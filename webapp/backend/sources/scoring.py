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
                       text) and `composition_digest()` (this module's own, over
                       the code that decides WHAT the scorer reads). Mixing the
                       first two is what makes a forgotten version bump harmless:
                       the digest moves whether or not the string does. The third
                       is there because the scorer's inputs are assembled HERE,
                       and assembling them differently is a different scorer even
                       when `rubric.py` is byte-identical (see
                       `composition_digest`).
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

AND WHY AN INPUT CAN COME BACK. Every id here is content-derived, so an input that
reverts (A -> B -> A on one posting version) does not describe a new row -- it
describes the row A already wrote, which is now superseded. `persist_scores`
RE-CURRENTS that row rather than re-minting it; re-minting is a primary key
violation, and a deterministic id means every retry violates it again. See
`persist_scores`' "REVERTING INPUTS".

Conventions, identical to `runstore`'s: explicit `sqlite3.Connection`, no
transaction control, no `config.DB_PATH`, and the write step is a `writer.WriteOp`
so Phase 4 wires it with `writer.submit` and no scheduler or writer edit.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import math
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
    "composition_digest",
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

#: "No row on file", which `dict.get(...)` alone cannot say here: a row that IS on
#: file and current has `superseded_at` of None, and the two mean opposite things.
_ABSENT = object()


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
    #: Defaulted so a test (or any caller) can name an identity by its hash alone.
    #: `scorer_identity()` always fills it in.
    composition_digest: str = ""


#: The functions that decide WHAT the scorer reads, in declared order. Labels are
#: hashed alongside the source text for `rubric._scorer_surface`'s two reasons:
#: renaming a member is a change even when its body is not, and two identical
#: bodies cannot cancel out.
_COMPOSITION_SURFACE_NAMES = ("row_from_version", "_score_one")


def composition_digest() -> str:
    """sha256 over the source of THIS module's scorer composition.

    `rubric.scorer_source_digest()` covers the scoring FUNCTIONS. It cannot cover
    how they are called, and how they are called is a scoring decision: the row
    `row_from_version` builds is the scorer's whole world, and `_score_one`
    decides what the odds axis sees of the fit pass's output. Phase 3.7's
    legacy/new differential found exactly that gap -- the odds call was reading a
    row with `flags=""`, so two `hireability` contributions were dead canonically
    and live under `rubric.cmd_score`, with `rubric.py` byte-identical on both
    sides. Fixing the composition changed every staffing/degree-gated canonical
    score without moving `scorer_hash`, which would have left the anti-join
    reusing scores the fixed code would not produce. So the composition is part of
    the scorer's identity, and it is hashed from source for the same reason the
    rubric surface is: nobody has to remember.

    No hand-maintained twin constant here, deliberately. `RUBRIC_VERSION` earns
    its keep as a human statement about the RUBRIC's meaning ("this is v3, the
    labels changed"); there is no equivalent statement to make about wiring, so a
    pinned digest would be maintenance with no decision attached to it.
    """
    payload = [
        [f"scoring.{name}", inspect.getsource(globals()[name])]
        for name in _COMPOSITION_SURFACE_NAMES
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def scorer_identity() -> ScorerIdentity:
    """The running scorer's identity: the declared version AND the two digests.

    All three, not any one. `RUBRIC_VERSION` alone is a hand-maintained string, and
    a forgotten bump silently lets the anti-join reuse scores the new code would
    not have produced. The digest alone would churn the whole corpus on a comment.
    So the digest provides automatic invalidation and the string provides the human
    statement of intent, and `rubric.SCORER_SOURCE_DIGEST` (asserted by a test)
    forces that statement to be made. `composition_digest()` closes the third gap:
    the same rubric wired to different inputs is a different scorer.
    """
    digest = rubric.scorer_source_digest()
    composition = composition_digest()
    return ScorerIdentity(
        rubric_version=rubric.RUBRIC_VERSION,
        source_digest=digest,
        composition_digest=composition,
        scorer_hash=hashlib.sha256(
            runstore.canonical_json(
                {
                    "rubric_version": rubric.RUBRIC_VERSION,
                    "source_digest": digest,
                    "composition_digest": composition,
                }
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

    `flags` starts EMPTY because there is nothing observed to put in it -- flags
    are scorer OUTPUT, not source data. `_score_one` fills it from `score_row`'s
    output before the odds pass reads it, which is the whole reason this dict must
    stay mutable and per-posting.
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


def _score_one(row: dict, description: str | None, *, is_aggregator: bool):
    """Both passes over ONE row, chained exactly as `rubric.cmd_score` chains them.

    Returns `(ScoreResult, OddsResult)`. The single shared, MUTABLE `row` is the
    whole point, and it carries two things from the fit pass into the odds pass
    that a second independent copy of the row would drop on the floor:

      FLAGS. `rubric._hireability_core` reads `r.get("flags")` to gate two
        contributions -- `staffing_w2` (`"Staffing/W2" in flags`) and
        `degree_gated` (`"degree-gated" in flags`). Neither is derived from the
        description or from any other input the odds pass is handed, so they fire
        only if the CALLER puts `score_row`'s output flags on the row first.
        `cmd_score` does (`r2["flags"] = ", ".join(flags)`); this module used not
        to, which made both contributions dead canonically and the "Lower bar"
        competition label unreachable for every canonically scored posting. The
        join is `", "` and the parse is a substring test, so the joined STRING is
        what is written here -- `_hireability_core` accepts a list too, but only
        the string is what legacy hands it, and the two paths must be the same
        path.
      THE RECOVERED PAY BAND. `score_row` MUTATES the row when it extracts a
        salary range from the description (`r["salary"]`, `r["salary_min"]`,
        `r["salary_max"]`), and the odds pass reads `r["salary_max"]` for its
        `comp_high_bar`/`comp_near_level` contributions. `cmd_score` scores `r`
        in place and then hands `dict(r)` to `hireability`, so legacy odds see
        the recovered band; two independent copies see the pre-scoring row and a
        posting whose comp is only stated in its description scores as though it
        stated no comp at all. Canonically that is every priced posting, since
        `posting_versions.salary_min`/`salary_max` are never populated
        (`runstore._link_source_version`) and the band exists only in the text.

    The caller owns the row and must pass a FRESH dict per posting -- it comes back
    mutated, and `WorkRow.row` is shared, read-only input.
    """
    result = rubric.score_row_explained(row, description, is_aggregator=is_aggregator)
    row["flags"] = ", ".join(result.flags)
    odds = rubric.hireability_explained(row, description)
    return result, odds


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
def validate_features(
    features: Mapping[str, object],
    required: frozenset,
    *,
    required_present: frozenset,
) -> dict:
    """Refuse to persist a vector that is not REPLAYABLE. Three checks, two directions.

    The contract is what makes a stored score replayable
    (`rubric.reconstruct_tier`), and a one-directional check only closes one of the
    two ways to break it:

      NO UNKNOWN KEY. A key the replayer has never heard of is either a
        contribution it will not sum or a cap it will not apply, and either way the
        replayed tier silently disagrees with the stored one.
      NO MISSING KEY. `required_present` is what the replayer INDEXES rather than
        probes -- `reconstruct_tier` reads `features["raw_score"]` directly, so a
        vector without it does not replay wrong, it raises `KeyError` years later
        on a row nothing can re-derive. Caps stay optional on purpose: a cap that
        did not fire has nothing to record, and `reconstruct_tier` reads each one
        through `if cap in features`.
      NO NON-NUMBER. Every value is summed, mins'd or compared by the replayer, so
        a string, a None or a NaN is a row that either raises or silently poisons
        an arithmetic answer (`NaN` propagates through `min` and never equals
        itself). `bool` is rejected too although it is an `int` subclass: `True` as
        a contribution means somebody stored a flag where a point value belongs.

    A BLOCKED vector is the other legal shape and is closed in both directions at
    once: exactly `{"blocker": <code>}`, whose value is a code string rather than a
    number. `reconstruct_tier` returns 0 on the key's presence alone, so a blocked
    vector carrying contributions as well would be describing two different things.

    Failing the write turns "somebody added a dimension and forgot the replayer"
    into an error at the moment it happens rather than into a corpus of scores
    nobody can re-derive.
    """
    keys = set(features)
    unknown = keys - set(required)
    if unknown:
        raise ScoreFeatureError(
            f"feature vector carries key(s) outside the stored-feature contract: "
            f"{sorted(unknown)}"
        )

    if candidate_profile.SCORE_ROW_BLOCKER_FEATURE in keys:
        extra = keys - candidate_profile.BLOCKED_SCORE_ROW_FEATURES
        if extra:
            raise ScoreFeatureError(
                f"a blocked feature vector carries nothing but "
                f"{sorted(candidate_profile.BLOCKED_SCORE_ROW_FEATURES)}, not {sorted(extra)}"
            )
        code = features[candidate_profile.SCORE_ROW_BLOCKER_FEATURE]
        if not isinstance(code, str) or not code:
            raise ScoreFeatureError(f"blocker must be a non-empty code string, got {code!r}")
        return dict(features)

    missing = set(required_present) - keys
    if missing:
        raise ScoreFeatureError(
            f"feature vector is missing key(s) the replayer requires: {sorted(missing)}"
        )

    for key in sorted(keys):
        value = features[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ScoreFeatureError(
                f"feature {key!r} must be a number, got {type(value).__name__} ({value!r})"
            )
        if not math.isfinite(value):
            raise ScoreFeatureError(f"feature {key!r} must be finite, got {value!r}")
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


def _score_version_id(
    *, posting_version_id: str, profile_version_id: str, score_digest: str
) -> str:
    """The row id a (version, profile, input, scorer) tuple ALWAYS lands on.

    Deterministic, and that is a property with consequences in both directions.
    It makes a re-run of the same work land on the row it already wrote instead of
    minting a second one -- and it means an input that REVERTS (A -> B -> A on one
    posting version) lands back on a row that already exists and is superseded. It
    is a plain function so `persist_scores` can find that row before the scorer
    runs, since none of the four inputs depends on the score.
    """
    return str(
        uuid.uuid5(
            _SCORE_NAMESPACE,
            runstore.canonical_json([posting_version_id, profile_version_id, score_digest]),
        )
    )


def _supersession_state(
    conn: sqlite3.Connection, score_version_ids: Sequence[str]
) -> dict[str, str | None]:
    """`{score_version_id: superseded_at}` for the ids already on file.

    Keyed on the PRIMARY KEY and chunked like every other lookup here, because the
    revert check runs once per batch and must not become one probe per row.
    """
    found: dict[str, str | None] = {}
    ids = list(dict.fromkeys(score_version_ids))
    for start in range(0, len(ids), _LOOKUP_CHUNK):
        chunk = ids[start:start + _LOOKUP_CHUNK]
        rows = conn.execute(
            "SELECT score_version_id, superseded_at FROM score_versions "
            f"WHERE score_version_id IN ({','.join('?' * len(chunk))})",
            chunk,
        )
        for row in rows:
            found[row["score_version_id"]] = row["superseded_at"]
    return found


@dataclass(frozen=True, slots=True)
class ScoreOutcome:
    """What one batch did, in terms that add up.

    `selected == scored + recurrent + reused + skipped` is the accounting the run
    report is read with, so the four are disjoint by construction:

      scored     rows INSERTED. New evidence, and the only rows the scorer ran for.
      recurrent  rows RE-CURRENTED: the input reverted to one this (version,
                 profile, scorer) was already scored under, so the superseded row
                 it maps to was made current again rather than re-inserted (which
                 is the primary key violation this counter exists to name). No
                 scorer call, no new row -- the stored tier, rationale and vector
                 are the ones that input produced when it was first seen.
      reused     the current row already matched the input; nothing was written.
      blocked    of the rows SCORED, how many the rubric blocked. Not disjoint
                 with the three above -- it is a breakdown of `scored`, and a
                 re-currented blocked row is not counted here because this pass
                 did not decide it.
    """

    scored: int = 0
    reused: int = 0
    superseded: int = 0
    blocked: int = 0
    recurrent: int = 0
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

    Five batched steps, in this exact order, and the order is forced by
    `uq_score_versions_current`:

      1. read the current rows for these versions (the anti-join above) and keep
         only the items whose `input_hash` differs; then split those by whether the
         row their input maps to ALREADY EXISTS (see REVERTING INPUTS below);
      2. mark the rows being replaced superseded, with `superseded_by` still NULL.
         This FREES the partial unique index. It cannot be done after the insert
         (the index would reject the new row) and `superseded_by` cannot be set
         here, because the row it points at does not exist yet and the foreign key
         is checked immediately;
      3. insert the new rows;
      4. re-current the reverted rows, clearing BOTH `superseded_at` and the now
         false `superseded_by` they still carry from the transition that retired
         them. After step 2 freed the index this is the only current row for its
         key, so the index sees exactly one at every commit boundary;
      5. fill in `superseded_by` on the rows from step 2, now that the rows that
         replaced them -- inserted or re-currented -- exist.

    REVERTING INPUTS. `score_version_id` is deterministic over (posting version,
    profile version, input, scorer), so an input that goes A -> B -> A on ONE
    posting version maps its third state back onto the row its first state wrote.
    The anti-join cannot see that row: it reads current rows only, and that one is
    superseded. Inserting anyway violates the primary key AND migration 9's
    `UNIQUE (posting_version_id, profile_version_id, score_hash)`, and because the
    id is deterministic every retry violates it again -- the pass is stuck, not
    merely failed. So a reverting input RE-CURRENTS its existing row instead. The
    stored tier, rationale and feature vector are the ones that exact input already
    produced, which is why re-currenting is not merely cheaper than rescoring but
    is the only answer consistent with the rest of this module: identical input,
    identical scorer, identical row.

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

    # The row each work item lands on, known BEFORE the scorer runs because none of
    # the id's four inputs depends on the score. One batched lookup then says which
    # of those rows already exist, which is the whole revert check.
    planned: list[tuple[WorkRow, str, str]] = []
    for item in work:
        digest = score_hash(input_digest=item.input_digest, scorer_hash=scorer.scorer_hash)
        planned.append((
            item,
            digest,
            _score_version_id(
                posting_version_id=item.posting_version_id,
                profile_version_id=profile_version_id,
                score_digest=digest,
            ),
        ))
    on_file = _supersession_state(conn, [row_id for _item, _digest, row_id in planned])

    fresh: list[tuple[WorkRow, str, str]] = []
    reverted: list[tuple[WorkRow, str]] = []
    for item, digest, row_id in planned:
        state = on_file.get(row_id, _ABSENT)
        if state is _ABSENT:
            fresh.append((item, digest, row_id))
        elif state is not None:
            reverted.append((item, row_id))
        else:
            # On file and already CURRENT under this exact input, which the
            # anti-join should have reused. Only reachable if a stored `input_hash`
            # disagrees with the id it is filed under -- a row to leave alone, not
            # one to replace, so it counts as reused rather than as work.
            reused += 1

    landing = [(item, row_id) for item, _digest, row_id in fresh]
    landing += list(reverted)
    if not landing:
        return ScoreOutcome(reused=reused)

    replaced = {
        item.posting_version_id: current[item.posting_version_id][0]
        for item, _row_id in landing
        if item.posting_version_id in current
    }

    inserts: list[tuple] = []
    blocked = 0
    for item, digest, score_version_id in fresh:
        result, odds = _score_one(
            dict(item.row), item.description, is_aggregator=item.is_aggregator
        )
        features = validate_features(
            result.features,
            candidate_profile.REQUIRED_SCORE_ROW_FEATURES,
            required_present=candidate_profile.REQUIRED_PRESENT_SCORE_ROW_FEATURES,
        )
        odds_features = validate_features(
            odds.features,
            candidate_profile.REQUIRED_HIREABILITY_FEATURES,
            required_present=candidate_profile.REQUIRED_PRESENT_HIREABILITY_FEATURES,
        )
        if candidate_profile.SCORE_ROW_BLOCKER_FEATURE in features:
            blocked += 1
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
                "scorer_composition_digest": scorer.composition_digest,
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

    if inserts:
        conn.executemany(
            "INSERT INTO score_versions "
            "(score_version_id, posting_id, posting_version_id, profile_version_id, "
            "source_run_id, score_hash, scorer_hash, input_hash, tier, odds, odds_score, "
            "rationale_json, features_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            inserts,
        )

    if reverted:
        # `superseded_by` is cleared with `superseded_at`, never separately: it
        # names the row that REPLACED this one, and this one is no longer replaced.
        # Leaving it would point the current row at its own predecessor's successor
        # and turn the supersession chain into a cycle. `created_at` and
        # `source_run_id` are left alone: the row was written then, by that run, and
        # this pass did not re-decide it.
        revived = sorted(row_id for _item, row_id in reverted)
        conn.execute(
            "UPDATE score_versions SET superseded_at=NULL, superseded_by=NULL "
            f"WHERE score_version_id IN ({','.join('?' * len(revived))})",
            revived,
        )

    if replaced:
        new_by_version = {item.posting_version_id: row_id for item, row_id in landing}
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
        recurrent=len(reverted),
        score_version_ids=tuple(row_id for _item, row_id in landing),
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
                    "recurrent": outcome.recurrent,
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
