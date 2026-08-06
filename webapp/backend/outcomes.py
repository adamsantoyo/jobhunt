"""Phase 5, W-5.2: recommendation snapshots + outcome-event capture.

Two append-only facts, kept in their own tables and never mutated:

  capture_snapshot        "what did we show, and in what order." One
                           `recommendation_snapshots` header plus one
                           `recommendation_snapshot_items` row per posting, with
                           tier/odds/source/role_family etc. DENORMALIZED at
                           capture time -- a point-in-time record of what the
                           visitor actually saw, independent of whatever the
                           live score does afterward.
  record_outcome_event     "what did the visitor do with it." Today just
                           'opened'; `OUTCOME_EVENT_KINDS` is meant to grow.

WHY NOT `recommendation_events` (migrations.py, migration 9). That table stays
VIRGIN by design: it would conflate three different facts (serving, opening,
terminal disposition) that each already have -- or, as of this module, now have
-- exactly one home. Serving is `recommendation_snapshot_items` (this module).
Opens are `outcome_events` (this module). Terminal outcomes (applied, passed,
...) are `state_events`, which already exists and already means that. One home
per fact, never two.

CONVENTIONS, matching `events.py` and `routers/state.py`: every public function
takes the caller's live `sqlite3.Connection`, does its own commit at the end (so
routers can call these directly, exactly like `routers/state.py._apply_state`
does), and touches nothing outside its own write. No UPDATE, no DELETE,
anywhere in this module -- append-only is enforced by never writing the SQL
keyword, not by a runtime guard (see `test_outcomes_contract.py`'s source scan).
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import uuid

from . import models
from .sources import graph, registry, runstore
from .sources import adapters  # noqa: F401 -- import-time install() side effect

# `rubric.py` and `candidate_profile.py` live at the REPO ROOT, not under
# `backend/` -- see sources/scoring.py's identical path-insert comment. Only
# `candidate_profile` is imported here (the profile SCHEMA/loader); `rubric.py`
# itself is deliberately never imported by this module -- see `_role_family`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede this)

#: Closed, extensible vocabulary for `record_outcome_event`'s `kind`. Only
#: 'opened' today; validated against this tuple so an unrecognized kind is a
#: clean ValueError (422 upstream) rather than a silently-stored typo.
OUTCOME_EVENT_KINDS = ("opened",)


# --------------------------------------------------------------------------- #
# Profile / role-family (best-effort; never raises)
# --------------------------------------------------------------------------- #
def _load_profile():
    """The running candidate profile, or None. `profile.json` is gitignored and
    genuinely absent in some environments (a fresh checkout, CI); capture must
    keep working with role_family/profile_version_id simply unresolved rather
    than crashing a snapshot over a missing file or a malformed one."""
    try:
        return candidate_profile.load_profile()
    except Exception:
        return None


def _current_profile_version_id(conn: sqlite3.Connection, profile) -> str | None:
    """The `profile_versions` row matching the running profile's content, if one
    has ever been written. Read-only: `scoring.upsert_profile_version` owns
    minting that row (once per scoring pass); a snapshot capture is not a
    scoring event and must not fabricate profile identity nobody has scored
    against yet. None when `profile` is None or its content hash has no match."""
    if profile is None:
        return None
    row = conn.execute(
        "SELECT profile_version_id FROM profile_versions WHERE content_hash=?",
        (profile.content_hash,),
    ).fetchone()
    return row["profile_version_id"] if row else None


def _role_family(title: str | None, description: str | None, profile) -> str | None:
    """Mirrors rubric.py:605-610's family derivation EXACTLY: lowercase title
    first ('t'), falling back to the first 1500 characters of a lowercase,
    backslash-stripped description ('d[:1500]') when the title matches no
    family's keywords. The backslash strip (`.replace("\\", "")`) is rubric.py's
    own, applied to the description only, before the [:1500] slice -- omitting
    it would let a backslash-split keyword ("hel\\pdesk") dodge a match that
    rubric.py itself would have caught. `rubric.py` is deliberately NOT imported
    to make this mirror: rubric.py is the scorer's pinned source surface
    (`scoring.composition_digest` hashes code that reads from it), and importing
    it here would couple the scorer's identity to this capture module's
    existence for no scoring benefit -- role family here is a display label,
    not a score input. `profile` is None when profile.json could not be loaded;
    role_family is then None, never a crash."""
    if profile is None:
        return None
    t = (title or "").lower()
    d = (description or "").lower().replace("\\", "")
    fam = next(
        (f for f, kws in profile.families.keywords.items() if any(k in t for k in kws)), None
    )
    if fam is None:
        fam = next(
            (f for f, kws in profile.families.keywords.items() if any(k in d[:1500] for k in kws)),
            None,
        )
    return fam


def _description_for(conn: sqlite3.Connection, posting_version_id: str | None) -> str | None:
    """The newest description body for a posting version, or None. Only looked
    up by `posting_version_id` (not `posting_id`): a description fetched for an
    OLDER version of the posting could belong to a materially different
    listing, and title-only role-family derivation (rubric.py's own fallback
    order) is an acceptable, documented degradation when this misses."""
    if posting_version_id is None:
        return None
    row = conn.execute(
        "SELECT body FROM descriptions WHERE posting_version_id=? "
        "ORDER BY fetched_at DESC, description_id DESC LIMIT 1",
        (posting_version_id,),
    ).fetchone()
    return row["body"] if row else None


# --------------------------------------------------------------------------- #
# Odds / source-category helpers
# --------------------------------------------------------------------------- #
def _split_odds(odds: str | None) -> tuple[str | None, str | None]:
    """(match_label, competition_label) from the combined "<match> / <competition>"
    string. Legacy values (Likely/Target/Reach) carry no " / " separator and
    split to (None, None), mirroring routers/analytics.py's `_competition_of`."""
    if not odds or " / " not in odds:
        return None, None
    match_label, _, competition_label = odds.partition(" / ")
    return match_label, competition_label


def _source_category(source: str | None) -> str | None:
    """The namespace's source category, or None when unregistered.

    Deliberately NOT `graph.registry_category`'s own default: that function
    reads an unregistered namespace as AGGREGATOR because canonical-version
    ranking needs a conservative-but-real category to rank against (see its
    docstring). This is analytics, not ranking -- fabricating "aggregator" for
    a namespace the registry has never heard of (a retired source key, a typo,
    a legacy string with no ':' namespace shape) would misreport what the
    snapshot actually saw. `sources.adapters` must already be imported for the
    registry to be populated; see this module's import of it, top of file."""
    if not source:
        return None
    source_key = source.split(":", 1)[0]
    if not source_key or not registry.is_registered(source_key):
        return None
    return str(graph.registry_category(source))


# --------------------------------------------------------------------------- #
# Enrichment: current score / latest version lookups
# --------------------------------------------------------------------------- #
def _latest_posting_version(conn: sqlite3.Connection, posting_id: str):
    return conn.execute(
        "SELECT posting_version_id, title, company, source, odds, tier, odds_score "
        "FROM posting_versions WHERE posting_id=? "
        "ORDER BY observed_at DESC, posting_version_id DESC LIMIT 1",
        (posting_id,),
    ).fetchone()


def _current_score(conn: sqlite3.Connection, posting_id: str, posting_version_id: str | None = None):
    """The current (`superseded_at IS NULL`) score, tiebroken `created_at DESC,
    score_version_id DESC` (mirrors canonical_reads._current_scores).

    `posting_version_id` given: the lookup is restricted to scores against THAT
    version -- a caller who names an explicit posting_version_id is making a
    claim about a specific version, and pairing it with a DIFFERENT version's
    winning score would silently misattribute which listing was actually
    scored (see `_enrich_item`'s posting_version_id/score_version_id pairing
    rule).

    `posting_version_id` omitted: matched by `score_versions.posting_id` OR by
    joining through `posting_versions` -- legacy-imported scores
    (`scorer_hash='legacy-import'`, migration 11) carry a NULL `posting_id`
    (migration 19 deliberately does not backfill it; see that migration's
    docstring), so a posting_id-only filter would silently miss every legacy
    score. Written as a `UNION ALL` of two independently index-usable branches
    (F11-lite) rather than the equivalent `posting_id=? OR posting_version_id
    IN (...)`: the OR form defeats index seeks on
    `idx_score_versions_current`-shaped indexes, forcing a full scan plus a
    temp b-tree per call; each UNION ALL branch instead seeks its own index and
    the shared `ORDER BY ... LIMIT 1` tiebreak is applied identically across
    both, so behavior is unchanged."""
    if posting_version_id is not None:
        return conn.execute(
            "SELECT score_version_id, posting_version_id, tier, odds, odds_score, scorer_hash "
            "FROM score_versions WHERE superseded_at IS NULL AND posting_version_id=? "
            "ORDER BY created_at DESC, score_version_id DESC LIMIT 1",
            (posting_version_id,),
        ).fetchone()
    return conn.execute(
        "SELECT score_version_id, posting_version_id, tier, odds, odds_score, scorer_hash "
        "FROM ("
        "  SELECT score_version_id, posting_version_id, tier, odds, odds_score, scorer_hash, "
        "         created_at "
        "  FROM score_versions WHERE superseded_at IS NULL AND posting_id=?"
        "  UNION ALL"
        "  SELECT sv.score_version_id, sv.posting_version_id, sv.tier, sv.odds, sv.odds_score, "
        "         sv.scorer_hash, sv.created_at "
        "  FROM score_versions sv "
        "  JOIN posting_versions pv ON pv.posting_version_id = sv.posting_version_id "
        "  WHERE sv.superseded_at IS NULL AND pv.posting_id=?"
        ") ORDER BY created_at DESC, score_version_id DESC LIMIT 1",
        (posting_id, posting_id),
    ).fetchone()


def _enrich_item(conn: sqlite3.Connection, item: dict, profile) -> dict:
    """Resolve one snapshot item's stored/derived fields. Precedence, per field:
    explicit item value > current score > latest posting version. `title`/
    `company` have no score-table source (score_versions carries neither) so
    their fallback is the latest version only; `score_version_id` has no
    version-table source so its fallback is the current score only.

    posting_version_id / score_version_id pairing (F3): these two must never
    describe DIFFERENT versions of the same posting. When the caller supplies
    posting_version_id explicitly, the score lookup is restricted to that
    version (`_current_score`'s `posting_version_id=` argument) -- so any score
    this item picks up is guaranteed to be scored against the named version.
    When the caller does NOT supply posting_version_id, the winning current
    score (if any) supplies BOTH score_version_id and posting_version_id --
    posting_version_id is read off the SCORE ROW, not off `_latest_posting_
    version`, so a posting whose newest version hasn't been scored yet still
    resolves to the version that was actually scored rather than a newer,
    unscored one. `_latest_posting_version` is consulted for posting_version_id
    only as the last resort, when no score exists at all."""
    posting_id = item["posting_id"]
    version_row = _latest_posting_version(conn, posting_id)
    explicit_posting_version_id = item.get("posting_version_id")
    score_row = _current_score(conn, posting_id, posting_version_id=explicit_posting_version_id)

    def pick(*, explicit, score_value=None, version_value=None):
        if explicit is not None:
            return explicit
        if score_value is not None:
            return score_value
        return version_value

    if explicit_posting_version_id is not None:
        posting_version_id = explicit_posting_version_id
    elif score_row is not None:
        posting_version_id = score_row["posting_version_id"]
    else:
        posting_version_id = version_row["posting_version_id"] if version_row else None
    title = pick(explicit=item.get("title"), version_value=version_row["title"] if version_row else None)
    company = pick(
        explicit=item.get("company"), version_value=version_row["company"] if version_row else None
    )
    source = pick(explicit=item.get("source"), version_value=version_row["source"] if version_row else None)
    score_version_id = pick(
        explicit=item.get("score_version_id"),
        score_value=score_row["score_version_id"] if score_row else None,
    )
    tier = pick(
        explicit=item.get("tier"),
        score_value=score_row["tier"] if score_row else None,
        version_value=version_row["tier"] if version_row else None,
    )
    odds = pick(
        explicit=item.get("odds"),
        score_value=score_row["odds"] if score_row else None,
        version_value=version_row["odds"] if version_row else None,
    )
    odds_score = pick(
        explicit=item.get("odds_score"),
        score_value=score_row["odds_score"] if score_row else None,
        version_value=version_row["odds_score"] if version_row else None,
    )

    match_label, competition_label = _split_odds(odds)
    description = _description_for(conn, posting_version_id)
    role_family = _role_family(title, description, profile)

    return {
        "posting_version_id": posting_version_id,
        "score_version_id": score_version_id,
        "tier": tier,
        "odds": odds,
        "odds_score": odds_score,
        "source": source,
        "source_category": _source_category(source),
        "match_label": match_label,
        "competition_label": competition_label,
        "role_family": role_family,
        "title": title,
        "company": company,
        # Not a stored item column -- only used by `capture_snapshot` to derive
        # the snapshot header's `scorer_hash` (F12). The score row that WON this
        # item's enrichment, not any explicit override, is what "scored it".
        "_scorer_hash": score_row["scorer_hash"] if score_row else None,
    }


# --------------------------------------------------------------------------- #
# recommendations ensure-step (idempotent, never UPDATEs)
# --------------------------------------------------------------------------- #
def _ensure_recommendation(
    conn: sqlite3.Connection, *, posting_id: str, profile_version_id: str,
    enriched: dict, captured_at: str,
) -> str:
    """Ensure exactly one `recommendations` row for (posting, profile, posting_
    version, score_version) and return its id. Idempotency key is deterministic
    (sha256 of ["queue-recommendation", posting_id, profile_version_id,
    posting_version_id, score_version_id or ""], mirroring scoring.py's
    `hashlib.sha256(runstore.canonical_json(...))` convention) so a posting
    served across many snapshots for the same profile AND the same version
    identity always resolves to the SAME recommendation row -- `INSERT ... ON
    CONFLICT(idempotency_key) DO NOTHING` followed by a SELECT, never an
    UPDATE. Version identity is baked into the key deliberately (F4): a claim
    made against posting_version_id=v1/score_version_id=s1 is a DIFFERENT claim
    than the same posting re-versioned or rescored, and freezing the original
    row across a re-version/rescore would let a stale claim silently keep
    speaking for a posting that has since changed underneath it. A new version
    or a new score therefore mints a NEW recommendations row -- append-only
    lineage of claims, not a single row mutated in place. The row this function
    writes on its first call for a given key is permanent; later calls for the
    SAME key only read it back."""
    idempotency_key = hashlib.sha256(
        runstore.canonical_json(
            [
                "queue-recommendation", posting_id, profile_version_id,
                enriched["posting_version_id"], enriched["score_version_id"] or "",
            ]
        ).encode("utf-8")
    ).hexdigest()
    recommendation_json = runstore.canonical_json(
        {
            "posting_id": posting_id,
            "posting_version_id": enriched["posting_version_id"],
            "score_version_id": enriched["score_version_id"],
            "tier": enriched["tier"],
            "odds": enriched["odds"],
            "odds_score": enriched["odds_score"],
            "source": enriched["source"],
            "source_category": enriched["source_category"],
            "match_label": enriched["match_label"],
            "competition_label": enriched["competition_label"],
            "role_family": enriched["role_family"],
            "title": enriched["title"],
            "company": enriched["company"],
        }
    )
    candidate_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO recommendations (recommendation_id, posting_id, posting_version_id, "
        "profile_version_id, score_version_id, idempotency_key, status, recommendation_json, "
        "created_at) VALUES (?,?,?,?,?,?,'active',?,?) "
        "ON CONFLICT(idempotency_key) DO NOTHING",
        (
            candidate_id, posting_id, enriched["posting_version_id"], profile_version_id,
            enriched["score_version_id"], idempotency_key, recommendation_json, captured_at,
        ),
    )
    row = conn.execute(
        "SELECT recommendation_id FROM recommendations WHERE idempotency_key=?",
        (idempotency_key,),
    ).fetchone()
    return row["recommendation_id"]


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def _derive_scorer_hash(enriched_items: list) -> str | None:
    """F12: the snapshot header's `scorer_hash` when not given explicitly.
    Every item's winning score row (`_enrich_item`'s `_scorer_hash`) must agree
    on a single, non-None hash for it to mean anything at the snapshot level --
    a mix of scorer versions, or no scores at all, honestly reports as None
    rather than picking one arbitrarily."""
    hashes = {e["_scorer_hash"] for e in enriched_items if e["_scorer_hash"] is not None}
    return next(iter(hashes)) if len(hashes) == 1 else None


def capture_snapshot(
    conn: sqlite3.Connection, *, surface: str, items: list, at: str | None = None,
    metadata: dict | None = None, scorer_hash: str | None = None, queue_size: int | None = None,
) -> dict:
    """Record one served queue: a `recommendation_snapshots` header plus one
    `recommendation_snapshot_items` row per item, in the given rank order.

    `items`: `[{"posting_id": str, "rank": int, ...optional overrides}]`. Ranks
    must be unique positive ints; posting_ids must be unique within the
    snapshot (also enforced by the DDL's `UNIQUE (snapshot_id, posting_id)`,
    but validated up front so a bad batch fails before any row is written,
    rather than partway through). An unknown `posting_id` is a ValueError. An
    explicit `posting_version_id` override must belong to that `posting_id`;
    an explicit `score_version_id` override must exist -- both checked up
    front, same reasoning (F2): a caller-supplied ghost id must fail clean,
    before any row is written, rather than reach the DB as an IntegrityError
    partway through the write. Empty `items` is valid -- a served-empty queue
    is real data.

    `queue_size` defaults to `len(items)` but may be given explicitly and
    larger, to record "showed 10 of 300" for a caller that only snapshots a
    partial view of a longer queue (F13). The DDL's CHECK only rejects
    negative; `queue_size >= len(items)` is intentionally NOT enforced here --
    a partial view is legal on its own terms.

    `scorer_hash` defaults to the derived value (F12, `_derive_scorer_hash`):
    when every item's winning score row agrees on one scorer_hash, that value;
    mixed scorer_hashes or no scores at all, None. An explicit `scorer_hash`
    always wins over derivation.

    A `recommendations` row is ensured per item only when both
    `posting_version_id` and the snapshot's `profile_version_id` resolve (see
    `recommendations`' NOT NULL columns, migrations.py); otherwise the item's
    `recommendation_id` is NULL. Never UPDATEs an existing `recommendations`
    row -- see `_ensure_recommendation`.

    The header insert and every item/`recommendations` write happen inside a
    try/except that rolls back on ANY failure (F2): without it, a later item's
    `IntegrityError` (a ghost id that slipped past up-front validation, or any
    other write failure) would leave the header row -- and any items already
    written for it -- sitting in an open, uncommitted transaction, silently
    corrupting the next unrelated write on this connection. Commits at the end
    on success, mirroring routers/state.py._apply_state.
    """
    captured_at = at or models.now_iso()

    if queue_size is not None and queue_size < 0:
        raise ValueError(f"queue_size must not be negative: {queue_size}")

    ranks_seen: set = set()
    postings_seen: set = set()
    for item in items:
        posting_id = item.get("posting_id")
        if not posting_id:
            raise ValueError("each item requires posting_id")
        rank = item.get("rank")
        if not isinstance(rank, int) or isinstance(rank, bool) or rank < 1:
            raise ValueError(f"rank must be a positive integer: {rank!r}")
        if rank in ranks_seen:
            raise ValueError(f"duplicate rank in snapshot: {rank}")
        ranks_seen.add(rank)
        if posting_id in postings_seen:
            raise ValueError(f"duplicate posting_id in snapshot: {posting_id}")
        postings_seen.add(posting_id)
        if conn.execute(
            "SELECT 1 FROM postings WHERE posting_id=?", (posting_id,)
        ).fetchone() is None:
            raise ValueError(f"unknown posting_id: {posting_id}")
        explicit_posting_version_id = item.get("posting_version_id")
        if explicit_posting_version_id is not None and conn.execute(
            "SELECT 1 FROM posting_versions WHERE posting_version_id=? AND posting_id=?",
            (explicit_posting_version_id, posting_id),
        ).fetchone() is None:
            raise ValueError(
                f"posting_version_id {explicit_posting_version_id!r} does not belong to "
                f"posting_id {posting_id!r}"
            )
        explicit_score_version_id = item.get("score_version_id")
        if explicit_score_version_id is not None and conn.execute(
            "SELECT 1 FROM score_versions WHERE score_version_id=?", (explicit_score_version_id,)
        ).fetchone() is None:
            raise ValueError(f"unknown score_version_id: {explicit_score_version_id}")

    profile = _load_profile()
    profile_version_id = _current_profile_version_id(conn, profile)
    enriched_items = [_enrich_item(conn, item, profile) for item in items]
    resolved_queue_size = queue_size if queue_size is not None else len(items)
    resolved_scorer_hash = (
        scorer_hash if scorer_hash is not None else _derive_scorer_hash(enriched_items)
    )

    snapshot_id = str(uuid.uuid4())
    metadata_json = runstore.canonical_json(metadata) if metadata is not None else None
    stored_items = []
    try:
        conn.execute(
            "INSERT INTO recommendation_snapshots "
            "(snapshot_id, surface, captured_at, profile_version_id, scorer_hash, queue_size, "
            "metadata_json) VALUES (?,?,?,?,?,?,?)",
            (
                snapshot_id, surface, captured_at, profile_version_id, resolved_scorer_hash,
                resolved_queue_size, metadata_json,
            ),
        )

        for item, enriched in zip(items, enriched_items):
            recommendation_id = None
            if enriched["posting_version_id"] is not None and profile_version_id is not None:
                recommendation_id = _ensure_recommendation(
                    conn, posting_id=item["posting_id"], profile_version_id=profile_version_id,
                    enriched=enriched, captured_at=captured_at,
                )
            conn.execute(
                "INSERT INTO recommendation_snapshot_items "
                "(snapshot_id, rank, recommendation_id, posting_id, posting_version_id, "
                "score_version_id, tier, odds, odds_score, source, source_category, match_label, "
                "competition_label, role_family, title, company) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    snapshot_id, item["rank"], recommendation_id, item["posting_id"],
                    enriched["posting_version_id"], enriched["score_version_id"], enriched["tier"],
                    enriched["odds"], enriched["odds_score"], enriched["source"],
                    enriched["source_category"], enriched["match_label"],
                    enriched["competition_label"], enriched["role_family"], enriched["title"],
                    enriched["company"],
                ),
            )
            stored_items.append(
                {
                    "snapshot_id": snapshot_id,
                    "rank": item["rank"],
                    "recommendation_id": recommendation_id,
                    "posting_id": item["posting_id"],
                    "posting_version_id": enriched["posting_version_id"],
                    "score_version_id": enriched["score_version_id"],
                    "tier": enriched["tier"],
                    "odds": enriched["odds"],
                    "odds_score": enriched["odds_score"],
                    "source": enriched["source"],
                    "source_category": enriched["source_category"],
                    "match_label": enriched["match_label"],
                    "competition_label": enriched["competition_label"],
                    "role_family": enriched["role_family"],
                    "title": enriched["title"],
                    "company": enriched["company"],
                }
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "snapshot_id": snapshot_id,
        "surface": surface,
        "captured_at": captured_at,
        "profile_version_id": profile_version_id,
        "scorer_hash": resolved_scorer_hash,
        "queue_size": resolved_queue_size,
        "metadata": metadata,
        "items": stored_items,
    }


def _posting_id_for_url(conn: sqlite3.Connection, url: str) -> str | None:
    """The posting the ACTIVE (`valid_to IS NULL`) alias row carrying `url`
    belongs to, newest-first tiebreak -- byte-identical to `canonical_reads.
    _posting_id_for_url`'s query.

    This is the bridge that actually covers the corpus (5.5 fix B1): every
    claim writes a `posting_aliases` row (`runstore._insert_alias`), while
    `job_state.posting_id` is only ever set by migration 19's one-shot
    backfill -- `ingest.py`'s own `job_state` INSERT does not populate it, so
    for anything ingested since, that column is NULL. `_capture_today_
    snapshot` resolves a served queue entry through this same join; resolving
    an OPEN any other way would leave the two sides of the 5.5 seam naming
    different postings (or, far more often, one naming a posting and the
    other naming nothing) and the derived-rank lookup in `record_outcome_
    event` would find no item to match."""
    row = conn.execute(
        "SELECT posting_id FROM posting_aliases WHERE url=? AND valid_to IS NULL "
        "ORDER BY valid_from DESC, alias_id DESC LIMIT 1",
        (url,),
    ).fetchone()
    return row["posting_id"] if row else None


def _resolve_seen_key(conn: sqlite3.Connection, url: str) -> str | None:
    """Mirrors routers/state.py's `_resolve_seen_key` exactly: the `jobs` cache
    wins for any present url, a dormant `job_state` row addressed by its own
    last-known url is the fallback, None if neither knows the url."""
    row = conn.execute("SELECT seen_key FROM jobs WHERE url=?", (url,)).fetchone()
    if row:
        return row["seen_key"]
    row = conn.execute("SELECT seen_key FROM job_state WHERE url=?", (url,)).fetchone()
    return row["seen_key"] if row else None


def record_outcome_event(
    conn: sqlite3.Connection, *, kind: str, url: str | None = None,
    posting_id: str | None = None, snapshot_id: str | None = None, rank: int | None = None,
    payload: dict | None = None, at: str | None = None, idempotency_key: str | None = None,
) -> dict:
    """Record one outcome event ('opened' today; `OUTCOME_EVENT_KINDS` is the
    closed vocabulary this validates against).

    Identity resolution: an explicit `posting_id` is used as given (and must
    exist). Otherwise, given a `url`, `seen_key` is resolved the same way
    `routers/state.py._resolve_seen_key` does, and `posting_id` is filled from
    the active `posting_aliases` row for that url (`_posting_id_for_url`),
    falling back to `job_state.posting_id` when the alias table does not know
    the url. The alias step is additive (5.5 fix B1's bridge, applied to the
    read side): every url that resolved before still resolves to the same
    posting, and the far larger population that only the alias table can name
    now resolves too. An unresolvable url is not an error -- "someone opened
    a job we no longer track" is still a real event -- it is simply stored
    with `url` as its only identifier; the DDL's CHECK constraint is
    satisfied by `url` alone.

    A given `snapshot_id` must reference an existing snapshot (checked up
    front for a clean ValueError rather than relying on the FK to fail the
    whole insert with a less specific error). When BOTH `snapshot_id` and
    `rank` are given, the pair must reference an existing snapshot item too
    (F15's composite FK backs this at the DDL level; checked here first for
    the same clean-ValueError reason) -- a `rank` with no matching item in that
    snapshot is not "the visitor opened rank 99 of an empty queue," it is a
    caller bug.

    RANK IS SERVER-DERIVED (5.5 fix B2). `rank` never has to leave the
    client, and normally does not: a caller that names a `snapshot_id` and no
    `rank` gets the rank looked up from the item matching (snapshot_id,
    resolved posting_id). This is the whole reason the 5.5 seam was dead --
    the queue renumbers ranks on every serve, so a rank the client captured
    at display time is a claim about a DIFFERENT serve than the day's stored
    snapshot, and validating that pair strictly rejected the exact payload
    the real client sends.

    Three rules, in the order they apply:

      * `rank` WITHOUT `snapshot_id` is ignored and stored NULL. A rank that
        names no snapshot points at nothing; storing it dangling would invite
        a later reader to pair it with whatever snapshot happens to be handy.
      * `snapshot_id` WITHOUT `rank`: the rank is derived. When no item in
        that snapshot matches this event's posting (the posting was in the
        served queue but unresolvable at capture time, or the visitor opened
        something the day's snapshot never contained), the event DEGRADES to
        an unattributed open -- stored with `snapshot_id` NULL and `rank`
        NULL, never rejected. An open is a real fact about the visitor even
        when the ranking side channel cannot place it, and 422-ing it would
        throw the fact away to protect a nullable column.
      * an EXPLICIT (`snapshot_id`, `rank`) pair is still validated exactly
        as before -- a caller making a specific claim about a specific served
        position must make a true one.

    `idempotency_key` is optional and, when given, deduplicates: a second call
    with the SAME key returns the FIRST call's stored event rather than
    inserting a new row (`INSERT ... ON CONFLICT(idempotency_key) DO NOTHING`
    followed by a read-back, never an UPDATE -- same convention as
    `_ensure_recommendation`). Without a key, there is no dedupe at all: every
    call inserts a new row, exactly as before F14. `idempotency_key` is
    nullable and UNIQUE at the DDL level; SQLite's UNIQUE permits any number of
    NULL rows, so keyless calls never collide with each other.

    On any failure during the write, the transaction is rolled back and the
    exception re-raised (F2) -- no partial write survives.

    Commits at the end on success, mirroring routers/state.py._apply_state.
    """
    if kind not in OUTCOME_EVENT_KINDS:
        raise ValueError(f"unknown outcome kind: {kind!r}")
    if posting_id is None and url is None:
        raise ValueError("record_outcome_event requires posting_id or url")

    seen_key = None
    if posting_id is not None:
        if conn.execute(
            "SELECT 1 FROM postings WHERE posting_id=?", (posting_id,)
        ).fetchone() is None:
            raise ValueError(f"unknown posting_id: {posting_id}")
    elif url is not None:
        seen_key = _resolve_seen_key(conn, url)
        # Alias join first, `job_state` bridge second -- the SAME order (and
        # the same queries) `routers/queueapi._posting_ids_for` uses when it
        # captures the day's snapshot, so the two ends of the 5.5 seam always
        # name the same posting for the same url.
        posting_id = _posting_id_for_url(conn, url)
        if posting_id is None and seen_key is not None:
            row = conn.execute(
                "SELECT posting_id FROM job_state WHERE seen_key=?", (seen_key,)
            ).fetchone()
            if row is not None and row["posting_id"] is not None:
                posting_id = row["posting_id"]

    # B2, rule 1: a rank naming no snapshot points at nothing.
    if snapshot_id is None:
        rank = None

    if snapshot_id is not None:
        if conn.execute(
            "SELECT 1 FROM recommendation_snapshots WHERE snapshot_id=?", (snapshot_id,)
        ).fetchone() is None:
            raise ValueError(f"unknown snapshot_id: {snapshot_id}")

        if rank is not None:
            # B2, rule 3: an explicit pair is a specific claim -- validated.
            if conn.execute(
                "SELECT 1 FROM recommendation_snapshot_items WHERE snapshot_id=? AND rank=?",
                (snapshot_id, rank),
            ).fetchone() is None:
                raise ValueError(
                    f"no snapshot item at snapshot_id={snapshot_id!r} rank={rank!r}"
                )
        else:
            # B2, rule 2: derive the rank from the item this snapshot holds
            # for this posting; degrade to unattributed when there is none.
            item = (
                conn.execute(
                    "SELECT rank FROM recommendation_snapshot_items "
                    "WHERE snapshot_id=? AND posting_id=?",
                    (snapshot_id, posting_id),
                ).fetchone()
                if posting_id is not None
                else None
            )
            if item is None:
                snapshot_id = None
            else:
                rank = item["rank"]

    event_id = str(uuid.uuid4())
    at_value = at or models.now_iso()
    payload_json = runstore.canonical_json(payload) if payload is not None else None
    try:
        conn.execute(
            "INSERT INTO outcome_events (outcome_event_id, kind, at, posting_id, seen_key, url, "
            "snapshot_id, rank, payload_json, idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(idempotency_key) DO NOTHING",
            (
                event_id, kind, at_value, posting_id, seen_key, url, snapshot_id, rank,
                payload_json, idempotency_key,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    if idempotency_key is not None:
        row = conn.execute(
            "SELECT * FROM outcome_events WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        return {
            "outcome_event_id": row["outcome_event_id"],
            "kind": row["kind"],
            "at": row["at"],
            "posting_id": row["posting_id"],
            "seen_key": row["seen_key"],
            "url": row["url"],
            "snapshot_id": row["snapshot_id"],
            "rank": row["rank"],
            "payload": json.loads(row["payload_json"]) if row["payload_json"] is not None else None,
        }

    return {
        "outcome_event_id": event_id,
        "kind": kind,
        "at": at_value,
        "posting_id": posting_id,
        "seen_key": seen_key,
        "url": url,
        "snapshot_id": snapshot_id,
        "rank": rank,
        "payload": payload,
    }
