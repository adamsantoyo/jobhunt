"""Phase 3.3's identity resolution: the normalization bridge and local matching.

Two jobs, both of which Phase 2 deliberately refused to do (`runstore`'s module
header: "Cross-source resolution is Phase 3's job ... and Phase 2 guessing at it
would produce merges nothing downstream could undo").

  THE NORMALIZATION BRIDGE. Migration 11 wrote every legacy URL alias as
    `alias_kind='url', namespace='legacy-url', value=<RAW url>`, while every
    canonical write since uses `namespace='url'` with a `contract.normalize_url`
    value. Those two namespaces never meet, so a scrape of a URL the legacy
    corpus already knows resolves to a BRAND NEW posting -- and the user's status,
    notes, and history stay attached to the old one. This is the Phase 4 cutover
    blocker. The bridge adds a normalized alias BESIDE the legacy one (nothing is
    rewritten, nothing is deleted), so the next canonical scrape resolves onto the
    legacy posting inside `runstore.write_records` with no special case anywhere.

  AGGREGATOR -> DIRECT LOCAL RESOLUTION. An aggregator's mirror of a board posting
    is a second posting with its own identity, because an aggregator rarely carries
    the board's requisition id. Matching them is what makes the ghost-listing caps
    (`undated-aggregator`, `needs-desc`) stop firing on a posting the board itself
    is reporting -- the match has to actually CHANGE the tier, or it is bookkeeping.

Everything here follows `runstore`'s conventions exactly: explicit
`sqlite3.Connection`, no transaction control, no `config.DB_PATH`, and the write
steps are `writer.WriteOp`s so Phase 4 wires them with `writer.submit` and no
scheduler edits.

THE RULES THIS MODULE WILL NOT BREAK

  NEVER GUESS. Two plausible candidates is not a match, it is an ambiguity, and it
    is archived as `identity_evidence` naming EVERY candidate. A wrong merge moves
    a user's notes onto someone else's job and nothing downstream can undo it.
  NOTHING IS DELETED OR REPOINTED. A resolved pair gets a `posting_redirects` row.
    The loser keeps its aliases and its versions, which is what makes the merge
    reversible and auditable.
  ONE POSTING, ONE INVALIDATION. A canonical match invalidates the SURVIVOR's
    score -- one row in `score_invalidations` -- and nothing else. The failure mode
    this rules out is a corpus-wide "rescore everything" triggered by a single
    match, which is the expensive answer given daily.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from . import runstore
from .contract import SourceCategory, normalize_url
from .writer import RunEvent

__all__ = [
    "BRIDGE_ALIAS_CONFIDENCE",
    "BridgeLegacyUrls",
    "DIRECT_CATEGORIES",
    "DirectIndex",
    "DirectObservation",
    "Observation",
    "LEGACY_URL_NAMESPACE",
    "MIN_TITLE_JACCARD",
    "ResolveAggregators",
    "ResolutionOutcome",
    "URL_NAMESPACE",
    "bridge_legacy_url_aliases",
    "build_direct_index",
    "company_key",
    "consume_invalidations",
    "direct_observations",
    "emit_invalidation",
    "locations_compatible",
    "normalized_title",
    "open_invalidations",
    "record_redirect",
    "resolve_local",
    "run_observations",
    "title_jaccard",
    "title_tokens",
]

#: Migration 11's alias namespace: `value` is the RAW legacy URL.
LEGACY_URL_NAMESPACE = "legacy-url"

#: The canonical alias namespace: `value` is `contract.normalize_url(url)`.
URL_NAMESPACE = "url"

#: Confidence recorded on a bridged alias. Deliberately 0.5 -- the same value
#: `runstore._insert_alias` gives a rank-1 URL claim, because that is exactly what
#: a bridged alias is: conservative secondary evidence, derived rather than
#: observed. Writing 1.0 would claim a requisition-grade certainty that a
#: normalization step cannot produce.
BRIDGE_ALIAS_CONFIDENCE = 0.5

#: Categories that count as "direct inventory" for local resolution. A startup
#: board (YC) is a first-party listing with its own requisition identity, not a
#: scrape of somebody else's, so it resolves aggregators the same way a DIRECT
#: board does.
DIRECT_CATEGORIES = frozenset({SourceCategory.DIRECT, SourceCategory.STARTUP_BOARD})

#: Title similarity floor for a local match. 0.75 of the token UNION, which is
#: strict: "Support Engineer" vs "Senior Support Engineer" scores 0.67 and does
#: NOT match, because a level difference is a different requisition. Exact
#: normalized equality is accepted separately, so a one-token title is not
#: penalised by the union denominator.
MIN_TITLE_JACCARD = 0.75

#: Shortest location token that may evidence compatibility. Two-letter state codes
#: are excluded on purpose: "CA" appears in half the corpus and would make every
#: California posting location-compatible with every other.
MIN_LOCATION_TOKEN = 3

_EVIDENCE_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/resolver-evidence"
)
_INVALIDATION_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/score-invalidation"
)

#: How many ids one prefetch statement carries. Mirrors `runstore._LOOKUP_CHUNK`
#: for the same reason: well under the oldest `SQLITE_MAX_VARIABLE_NUMBER` still
#: in the wild, and large enough that a batch is one or two statements.
_LOOKUP_CHUNK = 400

_TOKEN_RE = re.compile(r"[a-z0-9]+")

#: Legal-form and generic-suffix tokens dropped from a company key. A LOCAL, pure
#: normalizer: `scraper.canon_company` is deliberately NOT imported. That function
#: is part of the legacy CSV pipeline, it is not pure (it consults module state),
#: and coupling canonical identity resolution to it would make a legacy tweak
#: silently re-partition the canonical corpus.
_COMPANY_NOISE = frozenset({
    "inc", "incorporated", "llc", "llp", "ltd", "limited", "corp", "corporation",
    "co", "company", "plc", "gmbh", "ag", "sa", "nv", "bv", "oy", "ab", "as",
    "pty", "group", "holdings", "holding", "the",
})


# --------------------------------------------------------------------------- #
# Pure normalizers
# --------------------------------------------------------------------------- #
def _tokens(value: str | None) -> list[str]:
    text = unicodedata.normalize("NFKC", str(value or "")).lower()
    return _TOKEN_RE.findall(text)


def company_key(name: str | None) -> str:
    """Comparable form of a company name. Pure, and local by design (see above).

    NFKC-folded, lowercased, punctuation-split, legal-form tokens dropped, joined
    with single spaces. `"Acme Robotics, Inc."` and `"acme robotics"` agree;
    `"Acme"` and `"Acme Robotics"` do not, because dropping meaningful tokens is
    how two different employers get merged.
    """
    kept = [t for t in _tokens(name) if t not in _COMPANY_NOISE]
    return " ".join(kept)


def normalized_title(title: str | None) -> str:
    return " ".join(_tokens(title))


def title_tokens(title: str | None) -> frozenset[str]:
    return frozenset(_tokens(title))


def title_jaccard(left: str | None, right: str | None) -> float:
    """|A n B| / |A u B| over title tokens. 0.0 when either side is empty."""
    a, b = title_tokens(left), title_tokens(right)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def location_tokens(location: str | None) -> frozenset[str]:
    return frozenset(t for t in _tokens(location) if len(t) >= MIN_LOCATION_TOKEN)


def locations_compatible(left: str | None, right: str | None) -> bool:
    """True when the two locations do not CONTRADICT each other.

    Deliberately weaker than equality, in the permissive direction, because an
    aggregator routinely reports "San Francisco Bay Area" for a board's
    "San Francisco, CA" and one of the two sides is frequently blank. It is one of
    six conditions that must ALL hold, so a permissive location test cannot
    produce a match on its own -- and location is the field most likely to differ
    innocently between a board and its mirror.
    """
    a, b = location_tokens(left), location_tokens(right)
    if not a or not b:
        return True
    return bool(a & b)


# --------------------------------------------------------------------------- #
# The direct-inventory index
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Observation:
    """One posting as THIS RUN observed it, with the version row that described it."""

    posting_id: str
    namespace: str
    company: str
    title: str
    location: str

    @property
    def key(self) -> str:
        return company_key(self.company)


#: The name this type had when only direct inventory was indexed. Both sides of a
#: local match are now built from the same query, so the type is category-neutral.
DirectObservation = Observation


@dataclass(frozen=True, slots=True)
class DirectIndex:
    """Direct observations grouped by company key. Pure, in memory, per run.

    NOT a database index and not a corpus scan, which is the whole point: the
    roadmap's rule for this phase is "aggregators resolve locally against direct
    inventory", and a candidate an aggregator could plausibly be is one the run
    just saw. Adding a `postings`-wide index to answer it would tax every write in
    every run to serve a lookup that only aggregator rows make.
    """

    by_company: Mapping[str, tuple[Observation, ...]] = field(default_factory=dict)

    def candidates(self, company: str | None) -> tuple[Observation, ...]:
        return self.by_company.get(company_key(company), ())

    def __len__(self) -> int:
        return sum(len(v) for v in self.by_company.values())


def build_direct_index(observations: Iterable[Observation]) -> DirectIndex:
    """Group observations by company key. Pure."""
    grouped: dict[str, list[DirectObservation]] = {}
    for observation in observations:
        key = observation.key
        if key:
            grouped.setdefault(key, []).append(observation)
    return DirectIndex(by_company={k: tuple(v) for k, v in grouped.items()})


#: Everything one run positively observed, with the version row that described it.
#: Keyed on `run_postings`' primary key `(run_uid, posting_id)`, so the cost tracks
#: what the run delivered rather than how big the corpus has become.
_RUN_OBSERVATIONS_SQL = """
SELECT rp.posting_id AS posting_id,
       v.source AS namespace,
       v.company AS company,
       v.title AS title,
       v.location AS location
  FROM run_postings rp
  JOIN posting_versions v ON v.posting_version_id = rp.posting_version_id
 WHERE rp.run_uid = ? AND rp.present = 1 AND rp.posting_id > ?
 ORDER BY rp.posting_id
"""


def run_observations(conn: sqlite3.Connection, *, run_uid: str) -> list[Observation]:
    """Everything this run positively observed, in posting order.

    ONE run-scoped statement, keyed on `run_postings`' primary key. Both sides of
    local resolution are built from it: the direct-inventory index and the
    aggregator subject list. The category split is applied in Python rather than in
    SQL because a namespace's category lives in the adapter registry, not in the
    database -- there is no column that says "greenhouse:acme is direct", and
    inventing one would duplicate a declaration `SourceDescriptor` already owns.
    """
    return [
        Observation(
            posting_id=row["posting_id"],
            namespace=row["namespace"] or "",
            company=row["company"] or "",
            title=row["title"] or "",
            location=row["location"] or "",
        )
        for row in conn.execute(_RUN_OBSERVATIONS_SQL, (run_uid, ""))
    ]


def direct_observations(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    category_of: Callable[[str], SourceCategory],
) -> list[Observation]:
    """This run's DIRECT-inventory observations, in posting order."""
    return [
        observation
        for observation in run_observations(conn, run_uid=run_uid)
        if category_of(observation.namespace) in DIRECT_CATEGORIES
    ]


# --------------------------------------------------------------------------- #
# Local resolution
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ResolutionOutcome:
    """What local resolution decided about one aggregator posting.

    `matched` is set only when EXACTLY ONE candidate survived every condition.
    Two survivors is `ambiguous`, which is archived rather than resolved.
    """

    subject_posting_id: str
    matched: DirectObservation | None = None
    candidates: tuple[DirectObservation, ...] = ()

    @property
    def ambiguous(self) -> bool:
        return self.matched is None and len(self.candidates) > 1


def resolve_local(
    *,
    posting_id: str,
    company: str | None,
    title: str | None,
    location: str | None,
    index: DirectIndex,
) -> ResolutionOutcome:
    """Match one aggregator posting against the run's direct inventory. PURE.

    ALL of these must hold, and the caller checks the two it owns (the subject's
    category, and that the subject carries no rank-0 direct requisition alias --
    both need the database):

      same company key            an employer, not a fuzzy string
      title Jaccard >= 0.75       or exact normalized equality
      location compatible         shared >= 3-char token, or one side blank
      different postings          a posting never resolves to itself
      EXACTLY ONE survivor        two is an ambiguity, never a coin flip

    The conjunction is what makes this safe. Any one of these conditions alone
    matches wildly -- one employer posts forty roles, one title recurs across
    thirty employers -- and the cost of a wrong answer is a user's notes moving to
    someone else's job.
    """
    subject_title = normalized_title(title)
    survivors = [
        candidate
        for candidate in index.candidates(company)
        if candidate.posting_id != posting_id
        and (
            normalized_title(candidate.title) == subject_title
            or title_jaccard(title, candidate.title) >= MIN_TITLE_JACCARD
        )
        and locations_compatible(location, candidate.location)
    ]
    if len(survivors) == 1:
        return ResolutionOutcome(subject_posting_id=posting_id, matched=survivors[0],
                                 candidates=tuple(survivors))
    return ResolutionOutcome(subject_posting_id=posting_id, candidates=tuple(survivors))


# --------------------------------------------------------------------------- #
# Persistence primitives
# --------------------------------------------------------------------------- #
def _evidence(
    conn: sqlite3.Connection,
    *,
    posting_id: str,
    kind: str,
    payload: Mapping[str, object],
    at: str,
) -> str:
    """Persist one piece of identity evidence, deduped on its own content.

    The hash covers the disagreement/decision itself and NOT the observing run,
    for `runstore._record_conflict`'s reason: the same two postings looking like
    each other every morning is one standing fact, and hashing the run into it
    would append an identical row per run forever under
    `UNIQUE(evidence_hash)` + `INSERT OR IGNORE`.

    The SUBJECT posting IS hashed in, though, and that is not redundant with the
    payload: an ambiguity names N postings and files one row against each of them,
    with an identical payload. Without the subject in the hash, `UNIQUE` would keep
    the first row and silently drop the rest, so a posting caught in an ambiguity
    would carry no evidence that it was.
    """
    body = {"kind": kind, "posting_id": posting_id, **dict(payload)}
    evidence_hash = hashlib.sha256(runstore.canonical_json(body).encode("utf-8")).hexdigest()
    evidence_id = str(uuid.uuid5(_EVIDENCE_NAMESPACE, evidence_hash))
    conn.execute(
        "INSERT OR IGNORE INTO identity_evidence "
        "(evidence_id, posting_id, alias_id, evidence_kind, evidence_json, evidence_hash, "
        "observed_at) VALUES (?,?,NULL,?,?,?,?)",
        (evidence_id, posting_id, kind,
         runstore.canonical_json({**body, "first_observed_at": at}), evidence_hash, at),
    )
    return evidence_id


def record_redirect(
    conn: sqlite3.Connection, *, from_posting_id: str, to_posting_id: str, reason: str, at: str
) -> bool:
    """Point one posting at another. Returns True when a redirect was created.

    `INSERT OR IGNORE`, because `posting_redirects.from_posting_id` is the primary
    key: a posting redirects exactly once, and the FIRST decision is the one kept.
    Re-deciding it later would silently rewrite a merge a user may already have
    acted on.

    Nothing is deleted and no alias is repointed. The loser keeps every alias and
    every version it ever had, which is what makes the merge reversible and what
    lets the survivor's canonical-version selection see the loser's content state.
    """
    if from_posting_id == to_posting_id:
        return False
    cursor = conn.execute(
        "INSERT OR IGNORE INTO posting_redirects "
        "(from_posting_id, to_posting_id, reason, created_at) VALUES (?,?,?,?)",
        (from_posting_id, to_posting_id, reason, at),
    )
    return bool(cursor.rowcount)


def emit_invalidation(
    conn: sqlite3.Connection,
    *,
    posting_id: str,
    reason: str,
    evidence: Mapping[str, object] | None = None,
    run_uid: str | None,
    at: str,
) -> bool:
    """Queue ONE posting for rescoring. Returns True when a row was created.

    Single-posting by construction -- there is no bulk form of this function, and
    that is deliberate. The expensive failure mode of a resolver is a single match
    triggering a corpus-wide rescore; making the only available verb "invalidate
    this one posting" means that failure has to be written on purpose.

    Idempotent on (posting, reason, run): the id is derived from those three, so a
    pass that dies and is re-run queues the same work once rather than twice. Rows
    stay after consumption as evidence; the partial index only covers open ones.
    """
    invalidation_id = str(
        uuid.uuid5(_INVALIDATION_NAMESPACE,
                   runstore.canonical_json([posting_id, reason, run_uid or ""]))
    )
    cursor = conn.execute(
        "INSERT OR IGNORE INTO score_invalidations "
        "(invalidation_id, posting_id, reason, evidence_json, created_at, created_run_uid) "
        "VALUES (?,?,?,?,?,?)",
        (invalidation_id, posting_id, reason,
         None if evidence is None else runstore.canonical_json(dict(evidence)), at, run_uid),
    )
    return bool(cursor.rowcount)


def open_invalidations(
    conn: sqlite3.Connection, *, limit: int | None = None, after: str | None = None
) -> list[str]:
    """Posting ids with unconsumed invalidations, ordered and cursor-paginated.

    Same chunking contract as `runstore.dirty_posting_ids`: `after` resumes from
    the last id a caller processed, because `limit` alone re-returns the same
    first N (this is a query, not a queue).
    """
    sql = ("SELECT DISTINCT posting_id FROM score_invalidations "
           "WHERE consumed_at IS NULL AND posting_id > ? ORDER BY posting_id")
    params: list[object] = [after or ""]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [row["posting_id"] for row in conn.execute(sql, tuple(params))]


def consume_invalidations(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    at: str,
    posting_ids: Sequence[str] | None = None,
) -> int:
    """Mark invalidations consumed. `posting_ids=None` consumes every open row.

    Called ONLY when a pass completes. A pass that dies leaves its invalidations
    open, so the next pass picks the same work up -- the same restart-safety rule
    `runstore` applies to dirty emission, and the reason this is a separate step
    from the scoring itself.
    """
    if posting_ids is None:
        return conn.execute(
            "UPDATE score_invalidations SET consumed_at=?, consumed_run_uid=? "
            "WHERE consumed_at IS NULL",
            (at, run_uid),
        ).rowcount
    total = 0
    ids = list(dict.fromkeys(posting_ids))
    for start in range(0, len(ids), _LOOKUP_CHUNK):
        chunk = ids[start:start + _LOOKUP_CHUNK]
        total += conn.execute(
            "UPDATE score_invalidations SET consumed_at=?, consumed_run_uid=? "
            "WHERE consumed_at IS NULL AND posting_id IN "
            f"({','.join('?' * len(chunk))})",
            (at, run_uid, *chunk),
        ).rowcount
    return total


def _has_rank0_alias(conn: sqlite3.Connection, posting_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM posting_aliases WHERE posting_id=? AND alias_kind=? "
        "AND valid_to IS NULL LIMIT 1",
        (posting_id, runstore.SOURCE_REQ_ALIAS_KIND),
    ).fetchone() is not None


def _has_direct_rank0_alias(
    conn: sqlite3.Connection, posting_id: str, category_of: Callable[[str], SourceCategory]
) -> bool:
    """True when this posting already carries a DIRECT board's requisition id.

    Such a posting is not an unresolved aggregator mirror -- a board has claimed it
    by requisition, which is rank-0 authoritative evidence -- so nothing here may
    redirect it into something else.
    """
    return any(
        category_of(row["namespace"] or "") in DIRECT_CATEGORIES
        for row in conn.execute(
            "SELECT namespace FROM posting_aliases WHERE posting_id=? AND alias_kind=? "
            "AND valid_to IS NULL",
            (posting_id, runstore.SOURCE_REQ_ALIAS_KIND),
        )
    )


def _first_seen(conn: sqlite3.Connection, posting_id: str) -> str:
    row = conn.execute(
        "SELECT first_seen_at FROM postings WHERE posting_id=?", (posting_id,)
    ).fetchone()
    return "" if row is None else (row["first_seen_at"] or "")


def choose_survivor(conn: sqlite3.Connection, left: str, right: str) -> tuple[str, str]:
    """(survivor, loser) for two postings the bridge found to be the same thing.

    Precedence, in order, and it is the identity precedence the whole system runs
    on rather than a fresh rule invented here:

      1. a posting carrying a rank-0 `source_req` alias wins. That is a source's
         own requisition id, the authoritative claim; a posting evidenced only by
         a URL is the conservative secondary evidence (`contract.IdentityClaim`).
      2. otherwise the OLDER `first_seen_at` wins. The older posting is the one a
         user's status, notes, and history are most likely already attached to,
         and preserving those across the cutover is the point of the bridge.
      3. `posting_id` breaks the remaining tie, purely to make the order total. A
         merge decided by dict iteration order would not be reproducible, and an
         irreproducible merge cannot be reviewed.
    """
    left_req, right_req = _has_rank0_alias(conn, left), _has_rank0_alias(conn, right)
    if left_req != right_req:
        return (left, right) if left_req else (right, left)
    left_key = (_first_seen(conn, left), left)
    right_key = (_first_seen(conn, right), right)
    return (left, right) if left_key <= right_key else (right, left)


# --------------------------------------------------------------------------- #
# The bridge
# --------------------------------------------------------------------------- #
def bridge_legacy_url_aliases(
    conn: sqlite3.Connection, *, run_uid: str | None, at: str
) -> dict[str, object]:
    """Give every legacy URL alias a normalized twin. Non-destructive, idempotent.

    For each ACTIVE `legacy-url` alias, `normalize_url(value)` is computed and one
    of four things happens:

      AMBIGUOUS      the normalized value maps to more than one legacy posting
                     (two raw URLs that differ only in a tracking parameter, say,
                     recorded against different lineages). Bridging either one
                     would assert an identity the evidence does not support, so
                     NOTHING is bridged and the ambiguity is archived as
                     `normalization-bridge-ambiguous` naming every posting.
      BRIDGE         exactly one legacy posting and no active `url` alias for that
                     value: insert one, pointing at the legacy posting. This is the
                     Phase 4 unblocker -- the next canonical scrape of that URL now
                     resolves onto the legacy posting inside `write_records`, and
                     the user's job_state and notes survive the cutover.
      NOOP           the active `url` alias already points at the same posting.
      CANONICAL MATCH the active `url` alias points at a DIFFERENT posting. The
                     canonical corpus and the legacy corpus both know this URL and
                     disagree about which posting it is, which is precisely the
                     duplicate the cutover would otherwise ship. Survivor by
                     `choose_survivor`, `posting_redirects` loser -> survivor,
                     `canonical-match` evidence, and ONE `score_invalidations` row
                     for the survivor (its canonical version can now change, so its
                     stored tier is no longer trustworthy).

    Idempotent: the bridge insert is guarded by the same active-alias lookup that
    decides the branch, evidence is content-hashed with `INSERT OR IGNORE`, and
    redirects are primary-keyed on the loser. Running it every day is free after
    the first time.
    """
    legacy = conn.execute(
        "SELECT alias_id, posting_id, value FROM posting_aliases "
        "WHERE alias_kind=? AND namespace=? AND valid_to IS NULL ORDER BY alias_id",
        (URL_NAMESPACE, LEGACY_URL_NAMESPACE),
    ).fetchall()

    #: normalized value -> {posting_id: raw value}. Built first, because the
    #: ambiguity test is a property of the GROUP and cannot be decided one alias at
    #: a time.
    grouped: dict[str, dict[str, str]] = {}
    for row in legacy:
        normalized = normalize_url(row["value"])
        if not normalized:
            continue
        grouped.setdefault(normalized, {})[row["posting_id"]] = row["value"]

    bridged = ambiguous = matched = noop = 0
    invalidated: list[str] = []
    for normalized in sorted(grouped):
        postings = grouped[normalized]
        if len(postings) > 1:
            ambiguous += 1
            for posting_id in sorted(postings):
                _evidence(
                    conn,
                    posting_id=posting_id,
                    kind="normalization-bridge-ambiguous",
                    payload={
                        "normalized_url": normalized,
                        "raw_urls": sorted(postings.values()),
                        "candidate_posting_ids": sorted(postings),
                    },
                    at=at,
                )
            continue

        legacy_posting_id, raw = next(iter(postings.items()))
        existing = conn.execute(
            "SELECT alias_id, posting_id FROM posting_aliases "
            "WHERE alias_kind=? AND namespace=? AND value=? AND valid_to IS NULL",
            (URL_NAMESPACE, URL_NAMESPACE, normalized),
        ).fetchone()

        if existing is None:
            conn.execute(
                "INSERT INTO posting_aliases "
                "(alias_id, posting_id, alias_kind, namespace, value, url, req_id, "
                "provenance_json, confidence, valid_from, valid_to) "
                "VALUES (?,?,?,?,?,?,NULL,?,?,?,NULL)",
                (runstore.new_uid(), legacy_posting_id, URL_NAMESPACE, URL_NAMESPACE,
                 normalized, normalized,
                 runstore.canonical_json({"bridge": "legacy-url", "raw_url": raw,
                                          "run_uid": run_uid}),
                 BRIDGE_ALIAS_CONFIDENCE, at),
            )
            bridged += 1
            continue

        if existing["posting_id"] == legacy_posting_id:
            noop += 1
            continue

        survivor, loser = choose_survivor(conn, existing["posting_id"], legacy_posting_id)
        record_redirect(
            conn, from_posting_id=loser, to_posting_id=survivor,
            reason="normalization-bridge", at=at,
        )
        _evidence(
            conn,
            posting_id=survivor,
            kind="canonical-match",
            payload={
                "via": "normalization-bridge",
                "normalized_url": normalized,
                "raw_url": raw,
                "survivor_posting_id": survivor,
                "loser_posting_id": loser,
            },
            at=at,
        )
        if emit_invalidation(
            conn, posting_id=survivor, reason="canonical-match",
            evidence={"via": "normalization-bridge", "loser_posting_id": loser},
            run_uid=run_uid, at=at,
        ):
            invalidated.append(survivor)
        matched += 1

    return {
        "legacy_aliases": len(legacy),
        "bridged": bridged,
        "ambiguous": ambiguous,
        "canonical_matches": matched,
        "already_bridged": noop,
        "invalidated": tuple(invalidated),
    }


def resolve_aggregators(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    subjects: Sequence[tuple[str, str, str, str]],
    index: DirectIndex,
    category_of: Callable[[str], SourceCategory],
    at: str,
) -> dict[str, object]:
    """Resolve this run's aggregator postings against its direct inventory.

    `subjects` is `[(posting_id, company, title, location), ...]` -- already
    filtered by the caller to postings whose canonical namespace is an
    AGGREGATOR, because the caller is the one that computed the canonical version.

    A resolved subject gets a redirect into the direct posting, `canonical-match`
    evidence, and ONE invalidation for the SURVIVOR (the direct posting): its
    canonical version selection now also sees the aggregator's state map, which is
    exactly what makes the `undated-aggregator` and `needs-desc` caps stop firing.
    An ambiguous subject gets `canonical-match-ambiguous` evidence naming every
    candidate and is left alone.
    """
    matched: list[tuple[str, str]] = []
    ambiguous = 0
    invalidated: list[str] = []
    for posting_id, company, title, location in subjects:
        if _has_direct_rank0_alias(conn, posting_id, category_of):
            continue
        outcome = resolve_local(
            posting_id=posting_id, company=company, title=title, location=location,
            index=index,
        )
        if outcome.ambiguous:
            ambiguous += 1
            _evidence(
                conn,
                posting_id=posting_id,
                kind="canonical-match-ambiguous",
                payload={
                    "via": "aggregator-local-resolution",
                    "subject_posting_id": posting_id,
                    "subject": {"company": company, "title": title, "location": location},
                    "candidate_posting_ids": sorted(c.posting_id for c in outcome.candidates),
                    "candidates": [
                        {"posting_id": c.posting_id, "namespace": c.namespace,
                         "title": c.title, "location": c.location}
                        for c in sorted(outcome.candidates, key=lambda c: c.posting_id)
                    ],
                },
                at=at,
            )
            continue
        if outcome.matched is None:
            continue
        survivor = outcome.matched.posting_id
        record_redirect(
            conn, from_posting_id=posting_id, to_posting_id=survivor,
            reason="aggregator-local-resolution", at=at,
        )
        _evidence(
            conn,
            posting_id=survivor,
            kind="canonical-match",
            payload={
                "via": "aggregator-local-resolution",
                "survivor_posting_id": survivor,
                "loser_posting_id": posting_id,
                "direct_namespace": outcome.matched.namespace,
                "title_jaccard": round(title_jaccard(title, outcome.matched.title), 4),
            },
            at=at,
        )
        if emit_invalidation(
            conn, posting_id=survivor, reason="canonical-match",
            evidence={"via": "aggregator-local-resolution", "loser_posting_id": posting_id},
            run_uid=run_uid, at=at,
        ):
            invalidated.append(survivor)
        matched.append((posting_id, survivor))

    return {
        "subjects": len(subjects),
        "indexed_direct": len(index),
        "matched": tuple(matched),
        "ambiguous": ambiguous,
        "invalidated": tuple(invalidated),
    }


# --------------------------------------------------------------------------- #
# Writer operations
#
# Both implement `writer.WriteOp` (`events` plus `apply(conn) -> object | None`),
# so Phase 4 wires them with `writer.submit(...)` and no scheduler or writer edit.
# Neither is frozen, for `writer.MarkPresence`'s reason: the report only exists
# once the SQL has run, so `apply` publishes it by ASSIGNING `self.events` /
# `self.report` -- assignment, never append, so a busy-database rollback that
# replays the whole batch overwrites rather than doubles them.
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class BridgeLegacyUrls:
    """The normalization bridge for one run, as a single transaction."""

    run_uid: str
    at: str
    events: tuple[RunEvent, ...] = ()
    report: dict | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> dict:
        report = bridge_legacy_url_aliases(conn, run_uid=self.run_uid, at=self.at)
        self.report = report
        self.events = (
            RunEvent(
                run_uid=self.run_uid,
                event_type="resolve.legacy_urls_bridged",
                at=self.at,
                payload={k: v for k, v in report.items() if k != "invalidated"},
            ),
        )
        return report


@dataclass(slots=True)
class ResolveAggregators:
    """Aggregator -> direct local resolution for one run, as a single transaction."""

    run_uid: str
    at: str
    subjects: tuple[tuple[str, str, str, str], ...]
    index: DirectIndex
    category_of: Callable[[str], SourceCategory]
    events: tuple[RunEvent, ...] = ()
    report: dict | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> dict:
        report = resolve_aggregators(
            conn, run_uid=self.run_uid, subjects=self.subjects, index=self.index,
            category_of=self.category_of, at=self.at,
        )
        self.report = report
        self.events = (
            RunEvent(
                run_uid=self.run_uid,
                event_type="resolve.aggregators_resolved",
                at=self.at,
                payload={
                    "subjects": report["subjects"],
                    "indexed_direct": report["indexed_direct"],
                    "matched": len(report["matched"]),
                    "ambiguous": report["ambiguous"],
                },
            ),
        )
        return report
