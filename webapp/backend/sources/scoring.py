"""Phase 3.3's scoring persistence: identity, the row adapter, and supersession.

The roadmap line this implements: "score exactly once per (posting version,
profile version, scorer)". Everything here exists to make that sentence literally
true of the database rather than approximately true of the code.

WHAT IDENTIFIES A STORED SCORE

  profile_version_id   the candidate DATA the scorer applied, content-hashed from
                       `profile.json` by `candidate_profile`. Data only: the same
                       profile maps to the same row whatever scoring code ran.
  scorer_hash          the CODE. `rubric.RUBRIC_VERSION` (a deliberate human
                       statement) mixed with `rubric.scorer_source_digest()` (the
                       automatic one, over the pinned scoring surface's source
                       text). Mixing both is what makes a forgotten version bump
                       harmless: the digest moves whether or not the string does.
  input_hash           everything else the scorer read -- the scored version's
                       `version_hash`, the description's content identity, and
                       `is_aggregator`.
  score_hash           sha256({"input": input_hash, "scorer": scorer_hash}), which
                       is what makes migration 9's
                       `UNIQUE (posting_version_id, profile_version_id, score_hash)`
                       mean "one row per (version, profile, scorer, input)".

WHY `input_hash` IS NOT OPTIONAL. A posting scraped before its description
arrives is capped at `no_desc_cap_tier` by rule zero ("no 4/5 without a read
description"). The description then arrives -- which is a `descriptions` row, not
a new posting version, because the SOURCE said nothing new. Keyed on
(version, profile, scorer) alone, that posting is already scored and stays capped
forever. Keyed with the input, it is a different input, so it gets a NEW row and
the old one is SUPERSEDED. Still exactly one current score.

WHY SUPERSESSION RATHER THAN UPDATE. `uq_score_versions_current` enforces "at most
one un-superseded row per (posting_version, profile, scorer)", and the superseded
row keeps its tier, its rationale, and a pointer to its replacement. "Why did this
posting drop from 4 to 3 on the 4th" is then answerable from evidence instead of
from a changelog nobody wrote.

Conventions, identical to `runstore`'s: explicit `sqlite3.Connection`, no
transaction control, no `config.DB_PATH`, and the write step is a `writer.WriteOp`
so Phase 4 wires it with `writer.submit` and no scheduler or writer edit.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from . import runstore
from .writer import RunEvent

# `rubric.py` and `candidate_profile.py` live at the REPO ROOT, not in the
# `backend` package: `rubric.py` runs outside the webapp's dependency environment
# as a bare `uv run rubric.py` subprocess, so it cannot be moved under `backend/`
# without giving the scorer two dependency worlds. The path insert mirrors what
# `backend/tests/test_profile.py` and the benchmark harness already do.
_REPO_ROOT = os.path.dirname(  # <repo>/
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede this)
import rubric  # noqa: E402

__all__ = [
    "ScoreFeatureError",
    "ScoreOutcome",
    "ScorerIdentity",
    "ScoreWork",
    "WorkRow",
    "current_score_inputs",
    "description_for",
    "input_hash",
    "persist_scores",
    "row_from_version",
    "score_hash",
    "scorer_identity",
    "upsert_profile_version",
    "validate_features",
]

#: How many ids one prefetch statement carries. Mirrors `runstore._LOOKUP_CHUNK`.
_LOOKUP_CHUNK = 400

_SCORE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/score-version")


class ScoreFeatureError(ValueError):
    """A feature vector carried a key outside the stored-feature contract.

    Raised BEFORE the row is written, so an un-replayable score never reaches the
    database. See `candidate_profile.REQUIRED_SCORE_ROW_FEATURES`.
    """


# --------------------------------------------------------------------------- #
# Identity
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ScorerIdentity:
    rubric_version: str
    source_digest: str
    scorer_hash: str


def scorer_identity() -> ScorerIdentity:
    """The running scorer's identity: the declared version AND the source digest.

    Both, not either. `RUBRIC_VERSION` alone is a hand-maintained string, and a
    forgotten bump silently lets the anti-join reuse scores the new code would not
    have produced. The digest alone would churn the whole corpus on a comment. So
    the digest provides automatic invalidation and the string provides the human
    statement of intent, and `rubric.SCORER_SOURCE_DIGEST` (asserted by a test)
    forces that statement to be made.
    """
    digest = rubric.scorer_source_digest()
    return ScorerIdentity(
        rubric_version=rubric.RUBRIC_VERSION,
        source_digest=digest,
        scorer_hash=hashlib.sha256(
            runstore.canonical_json(
                {"rubric_version": rubric.RUBRIC_VERSION, "source_digest": digest}
            ).encode("utf-8")
        ).hexdigest(),
    )


def input_hash(*, version_hash: str, description_identity: str, is_aggregator: bool) -> str:
    """Digest of everything the scorer read other than the profile and itself.

    Three inputs, and each is here because leaving it out produces a stale score
    that nothing would ever revisit:

      version_hash          what the SOURCE said (title, company, location, salary,
                            posted date, remote, description digest).
      description_identity  the description body's own content identity, which
                            moves WITHOUT a new posting version: a description
                            fetched after the fact is a `descriptions` row, not a
                            source observation. `""` means no description, which is
                            itself a scored fact (rule zero caps the tier).
      is_aggregator         the ghost-listing cap's input. The same posting scored
                            as an aggregator mirror and as a direct listing is two
                            different tiers, so it is part of the input, not of the
                            scorer.
    """
    return hashlib.sha256(
        runstore.canonical_json(
            {
                "version_hash": version_hash,
                "description": description_identity,
                "is_aggregator": bool(is_aggregator),
            }
        ).encode("utf-8")
    ).hexdigest()


def score_hash(*, input_digest: str, scorer_hash: str) -> str:
    """The FULL key digest stored in `score_versions.score_hash`.

    Neither half alone works. Hashing the scorer's OUTPUT (the tier and rationale)
    would let a late-arriving description collide onto the old row whenever the
    tier happened not to move -- the row would then claim to describe an input it
    never saw. Hashing the INPUT alone would collide two scorer versions scoring
    the same input into one row, and `INSERT OR IGNORE` would keep whichever ran
    first.
    """
    return hashlib.sha256(
        runstore.canonical_json({"input": input_digest, "scorer": scorer_hash}).encode("utf-8")
    ).hexdigest()


def upsert_profile_version(
    conn: sqlite3.Connection, profile_doc, *, at: str | None = None
) -> str:
    """Ensure a `profile_versions` row for this profile document; return its id.

    `INSERT OR IGNORE` on a DETERMINISTIC id derived from the content hash, which
    is what makes `profile_versions.content_hash UNIQUE` dedupe instead of
    erroring on the second run (`candidate_profile.build_profile_version_row`
    documents the derivation). The row is content-addressed, so re-running this
    every pass costs one ignored insert.
    """
    row = candidate_profile.build_profile_version_row(
        profile_doc, created_at=at or runstore.utc_now_iso()
    )
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions "
        "(profile_version_id, content_hash, profile_json, rubric_hash, created_at) "
        "VALUES (:profile_version_id, :content_hash, :profile_json, :rubric_hash, :created_at)",
        row,
    )
    # The stored row wins on a content-hash collision with a different id -- there
    # cannot be one (the id is derived from the hash), but reading it back is what
    # makes that a fact rather than an assumption.
    stored = conn.execute(
        "SELECT profile_version_id FROM profile_versions WHERE content_hash=?",
        (row["content_hash"],),
    ).fetchone()
    return stored["profile_version_id"] if stored else row["profile_version_id"]


# --------------------------------------------------------------------------- #
# The row adapter
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class WorkRow:
    """One posting, resolved to the exact inputs the scorer needs."""

    posting_id: str
    posting_version_id: str
    version_hash: str
    namespace: str
    is_aggregator: bool
    row: Mapping[str, object]
    description: str | None
    description_identity: str

    @property
    def input_digest(self) -> str:
        return input_hash(
            version_hash=self.version_hash,
            description_identity=self.description_identity,
            is_aggregator=self.is_aggregator,
        )


def row_from_version(version_row: Mapping[str, object]) -> dict[str, object]:
    """`posting_versions` row -> the dict `rubric.score_row` reads.

    A fresh dict every time, deliberately: `score_row` MUTATES its argument
    (`salary`, `salary_min`, `salary_max`) when it recovers a pay band from the
    description, and handing it a `sqlite3.Row` or a shared dict would either fail
    or leak one posting's recovered band into the next.

    `source` is filled from `posting_versions.source`, which since Phase 3.1
    carries the NormalizedPosting NAMESPACE ("greenhouse:acme"), not a legacy CSV
    source string. That is exactly why `is_aggregator` is passed to the scorer
    explicitly rather than sniffed from this field.
    """
    return {
        "title": version_row["title"] or "",
        "company": version_row["company"] or "",
        "location": version_row["location"] or "",
        "salary": version_row["salary"] or "",
        "salary_min": version_row["salary_min"] or "",
        "salary_max": version_row["salary_max"] or "",
        "posted": version_row["posted"] or "",
        "remote": "true" if version_row["remote"] else "false",
        "source": version_row["source"] or "",
        "req_id": version_row["req_id"] or "",
        "flags": "",
    }


def description_for(
    conn: sqlite3.Connection, posting_ids: Sequence[str]
) -> dict[str, tuple[str | None, str]]:
    """`{posting_id: (body, content_identity)}` for the newest usable description.

    One statement per `_LOOKUP_CHUNK` ids, never one per posting: this runs on the
    scoring path, which is batched and must stay batched.

    A posting with no available description is simply absent from the result, and
    the caller scores it with `description_identity=""` -- which is a real input,
    not a missing one: rule zero caps an un-described posting's tier, and that cap
    has to be part of what the score is keyed on so the description's later arrival
    supersedes it.

    Phase 3.2 owns FETCHING descriptions; this only reads what is on file. The
    graph's enrichment stage is the seam where 3.2 plugs in.
    """
    found: dict[str, tuple[str | None, str]] = {}
    ids = list(dict.fromkeys(posting_ids))
    for start in range(0, len(ids), _LOOKUP_CHUNK):
        chunk = ids[start:start + _LOOKUP_CHUNK]
        sql = (
            "SELECT posting_id, body, content_hash FROM ("
            "  SELECT d.posting_id AS posting_id, d.body AS body, d.content_hash AS content_hash,"
            "         ROW_NUMBER() OVER ("
            "             PARTITION BY d.posting_id"
            "             ORDER BY d.fetched_at DESC, d.description_id DESC) AS rn"
            "    FROM descriptions d"
            f"   WHERE d.posting_id IN ({','.join('?' * len(chunk))})"
            "     AND d.fetch_status <> 'unavailable' AND d.body IS NOT NULL"
            ") WHERE rn = 1"
        )
        for row in conn.execute(sql, chunk):
            body = row["body"]
            identity = row["content_hash"] or hashlib.sha256(
                (body or "").encode("utf-8")
            ).hexdigest()
            found[row["posting_id"]] = (body, identity)
    return found


# --------------------------------------------------------------------------- #
# Selection and persistence
# --------------------------------------------------------------------------- #
def validate_features(features: Mapping[str, object], required: frozenset) -> dict:
    """Refuse to persist a vector carrying a key outside the closed contract.

    The contract is what makes a stored score REPLAYABLE
    (`rubric.reconstruct_tier`). A key the replayer has never heard of is either a
    contribution it will not sum or a cap it will not apply, and either way the
    replayed tier silently disagrees with the stored one. Failing the write turns
    "somebody added a dimension and forgot the replayer" into an error at the
    moment it happens rather than into a corpus of scores nobody can re-derive.
    """
    unknown = set(features) - set(required)
    if unknown:
        raise ScoreFeatureError(
            f"feature vector carries key(s) outside the stored-feature contract: "
            f"{sorted(unknown)}"
        )
    return dict(features)


def current_score_inputs(
    conn: sqlite3.Connection,
    posting_version_ids: Sequence[str],
    *,
    profile_version_id: str,
    scorer_hash: str,
) -> dict[str, tuple[str, str]]:
    """`{posting_version_id: (score_version_id, input_hash)}` for CURRENT scores.

    This is the "score exactly once" anti-join, and it is a set operation issued
    once per chunk -- never one probe per row, which on a 33,500-posting corpus is
    the difference between one query and thirty-four thousand.

    It rides `uq_score_versions_current`, whose leading column is
    `posting_version_id` precisely so this probe is a seek.

    It also SUBSUMES the dirty set, which is why the pass can select generously
    and still do no work: a posting whose content went A -> B -> A re-links a
    version that already has a current score row under this (profile, scorer), and
    the stored `input_hash` matches, so it is reused rather than rescored. The
    `input_hash` comparison is what keeps that safe -- a version that IS on file
    but was scored against a different description is NOT reusable.
    """
    found: dict[str, tuple[str, str]] = {}
    ids = list(dict.fromkeys(posting_version_ids))
    for start in range(0, len(ids), _LOOKUP_CHUNK):
        chunk = ids[start:start + _LOOKUP_CHUNK]
        rows = conn.execute(
            "SELECT score_version_id, posting_version_id, input_hash FROM score_versions "
            "WHERE profile_version_id=? AND scorer_hash=? AND superseded_at IS NULL "
            f"AND posting_version_id IN ({','.join('?' * len(chunk))})",
            (profile_version_id, scorer_hash, *chunk),
        )
        for row in rows:
            found[row["posting_version_id"]] = (
                row["score_version_id"], row["input_hash"] or "",
            )
    return found


@dataclass(frozen=True, slots=True)
class ScoreOutcome:
    scored: int = 0
    reused: int = 0
    superseded: int = 0
    blocked: int = 0
    score_version_ids: tuple[str, ...] = ()


def persist_scores(
    conn: sqlite3.Connection,
    items: Sequence[WorkRow],
    *,
    profile_version_id: str,
    scorer: ScorerIdentity,
    source_run_id: str | None = None,
    at: str,
) -> ScoreOutcome:
    """Score whatever needs it, supersede whatever it replaces, in one transaction.

    Four batched steps, in this exact order, and the order is forced by
    `uq_score_versions_current`:

      1. read the current rows for these versions (the anti-join above) and keep
         only the items whose `input_hash` differs;
      2. mark the rows being replaced superseded, with `superseded_by` still NULL.
         This FREES the partial unique index. It cannot be done after the insert
         (the index would reject the new row) and `superseded_by` cannot be set
         here, because the row it points at does not exist yet and the foreign key
         is checked immediately;
      3. insert the new rows;
      4. fill in `superseded_by` on the rows from step 2, now that their
         replacements exist.

    Every step is one statement or one `executemany`. Nothing here is per-row I/O.
    """
    if not items:
        return ScoreOutcome()

    current = current_score_inputs(
        conn, [i.posting_version_id for i in items],
        profile_version_id=profile_version_id, scorer_hash=scorer.scorer_hash,
    )

    work: list[WorkRow] = []
    reused = 0
    for item in items:
        existing = current.get(item.posting_version_id)
        if existing is not None and existing[1] == item.input_digest:
            reused += 1
            continue
        work.append(item)
    if not work:
        return ScoreOutcome(reused=reused)

    replaced = {
        item.posting_version_id: current[item.posting_version_id][0]
        for item in work
        if item.posting_version_id in current
    }

    inserts: list[tuple] = []
    blocked = 0
    for item in work:
        result = rubric.score_row_explained(
            dict(item.row), item.description, is_aggregator=item.is_aggregator
        )
        odds = rubric.hireability_explained(dict(item.row), item.description)
        features = validate_features(
            result.features, candidate_profile.REQUIRED_SCORE_ROW_FEATURES
        )
        odds_features = validate_features(
            odds.features, candidate_profile.REQUIRED_HIREABILITY_FEATURES
        )
        if candidate_profile.SCORE_ROW_BLOCKER_FEATURE in features:
            blocked += 1
        digest = score_hash(input_digest=item.input_digest, scorer_hash=scorer.scorer_hash)
        score_version_id = str(
            uuid.uuid5(
                _SCORE_NAMESPACE,
                runstore.canonical_json([item.posting_version_id, profile_version_id, digest]),
            )
        )
        inserts.append((
            score_version_id,
            item.posting_id,
            item.posting_version_id,
            profile_version_id,
            source_run_id,
            digest,
            scorer.scorer_hash,
            item.input_digest,
            result.tier,
            odds.label,
            odds.score,
            runstore.canonical_json({
                "why": result.why,
                "flags": list(result.flags),
                "odds_why": odds.why,
                "rubric_version": scorer.rubric_version,
                "scorer_source_digest": scorer.source_digest,
                "namespace": item.namespace,
                "is_aggregator": item.is_aggregator,
                "has_description": item.description is not None,
            }),
            runstore.canonical_json({"score_row": features, "hireability": odds_features}),
            at,
        ))

    if replaced:
        old_ids = sorted(replaced.values())
        conn.execute(
            "UPDATE score_versions SET superseded_at=?, superseded_by=NULL "
            f"WHERE score_version_id IN ({','.join('?' * len(old_ids))})",
            (at, *old_ids),
        )

    conn.executemany(
        "INSERT INTO score_versions "
        "(score_version_id, posting_id, posting_version_id, profile_version_id, "
        "source_run_id, score_hash, scorer_hash, input_hash, tier, odds, odds_score, "
        "rationale_json, features_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        inserts,
    )

    if replaced:
        new_by_version = {row[2]: row[0] for row in inserts}
        conn.executemany(
            "UPDATE score_versions SET superseded_by=? WHERE score_version_id=?",
            [(new_by_version[version_id], old_id)
             for version_id, old_id in replaced.items()],
        )

    return ScoreOutcome(
        scored=len(inserts),
        reused=reused,
        superseded=len(replaced),
        blocked=blocked,
        score_version_ids=tuple(row[0] for row in inserts),
    )


@dataclass(slots=True)
class ScoreWork:
    """One batch of scoring, as a `writer.WriteOp`.

    Not frozen, for `writer.MarkPresence`'s reason: the outcome only exists once
    the SQL has run, so `apply` publishes it by ASSIGNING `self.outcome` and
    `self.events` -- assignment, never append, so a busy-database rollback that
    replays the batch overwrites rather than doubles them.
    """

    run_uid: str
    at: str
    items: tuple[WorkRow, ...]
    profile_version_id: str
    scorer: ScorerIdentity
    source_run_id: str | None = None
    events: tuple[RunEvent, ...] = ()
    outcome: ScoreOutcome | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> ScoreOutcome:
        outcome = persist_scores(
            conn, self.items, profile_version_id=self.profile_version_id,
            scorer=self.scorer, source_run_id=self.source_run_id, at=self.at,
        )
        self.outcome = outcome
        self.events = (
            RunEvent(
                run_uid=self.run_uid,
                event_type="score.batch_scored",
                at=self.at,
                payload={
                    "selected": len(self.items),
                    "scored": outcome.scored,
                    "reused": outcome.reused,
                    "superseded": outcome.superseded,
                    "blocked": outcome.blocked,
                },
            ),
        )
        return outcome


# Re-exported so callers need not import `json` alongside this module for the
# `features_json` / `rationale_json` round trip.
def load_json(blob: str | None) -> object:
    return None if blob is None else json.loads(blob)
