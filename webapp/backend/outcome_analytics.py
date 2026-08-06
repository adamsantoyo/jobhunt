"""Phase 5, W-5.3: outcome analytics -- what happened to what we recommended
and what we applied to, sliced by source / match band / competition band /
role family / feature / rank, with explicit denominators and a `low_sample`
flag rather than a rate that quietly lies when n is tiny.

Pure read-only aggregation: every function here issues SELECTs only (no
INSERT/UPDATE/DELETE, no `conn.commit()`), exactly like `funnel.py`. It reads
two disjoint fact streams and never conflates them (same "one home per fact"
rule `outcomes.py`'s docstring states):

  application_outcomes    driven by `state_events` (field='status'), the
                           existing append-only log of what a HUMAN did with
                           a job after applying -- responded, phone-screened,
                           interviewed, offered. `job_state` supplies only
                           the durable posting_id/url bridge, never the
                           history itself (mirrors `funnel.py`).
  recommendation_outcomes driven by `recommendation_snapshot_items` /
                           `outcome_events` (W-5.2) -- what the SCORER showed
                           and whether the visitor opened or later applied to
                           it. Denominators are DISTINCT POSTINGS, not events
                           or snapshot appearances: a posting recommended in
                           five overlapping snapshots is one opportunity, not
                           five.

Both streams key off `posting_id` first and fall back to weaker identifiers
(a legacy `jobs` row by url, a bare `seen_key`) only when `posting_id` cannot
be resolved -- see `_application_identities` and `_recommended_postings`.
Anything left unresolved lands in a literal `"unknown"` cell rather than
being dropped: an analytics payload that silently discards the postings it
could not classify would overstate how well it understands its own data.
"""
from __future__ import annotations

import json
import os
import statistics
import sqlite3
import sys
from datetime import datetime

from . import outcomes

# `candidate_profile.py` lives at the repo root, not under `backend/` -- see
# `outcomes.py`'s identical path-insert comment (this module needs the same
# feature vocabulary constants it does, and importing `outcomes` first
# already performs this insert as a side effect, but it is repeated here,
# guarded the same way, so this module does not silently depend on import
# ORDER to find `candidate_profile`).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede this)

#: Mirrors `funnel.py`'s identical constant EXACTLY (kept local rather than
#: imported: `funnel.py` is a read-only file for this task, and this module's
#: definition of "responded" must not silently drift if that private name
#: ever moves). Passed is excluded on purpose -- that is the applicant giving
#: up, not the company responding.
#:
#: L5: sharing the STAGE NAMES with `funnel.py` does not mean sharing its
#: counting rule, and that divergence is deliberate. `funnel.py` counts a
#: posting as having reached a stage regardless of whether an Applied event
#: exists for it at all (a funnel stage is a state the posting is IN).
#: `_application_identities` only ever counts response-stage rows that
#: transition FROM 'Applied' (`old_value == "Applied"`) for an identity that
#: already has its own Applied event -- these are per-application RATES
#: (`n_responded / n_applied`), not funnel-style reached-counts, so an
#: identity can only be "responded" relative to an apply it is known to have
#: made.
_RESPONSE_STAGES = ("Phone screen", "Interview", "Offer", "Rejected")

_CHUNK = 400


def _parse(at: str) -> datetime:
    """Mirrors `funnel.py`'s `_parse`: state_events.at is local-naive ISO with
    two coexisting grains (bare 'YYYY-MM-DD' for backfilled rows, a full
    isoformat() string otherwise); both parse via fromisoformat."""
    return datetime.fromisoformat(at)


def _at_on_or_after(at: str, snapshot_at: str) -> bool:
    """True when event timestamp `at` is on-or-after `snapshot_at`,
    grain-aware (H1/M3): `recommendation_snapshots.captured_at` is always
    full-grain, but `state_events.at` (and, by the same bridging logic,
    `outcome_events.at` when compared against it) may be a bare
    'YYYY-MM-DD' backfilled row. A full-timestamp `at` a few seconds after a
    full-timestamp `snapshot_at` is unambiguous; a bare-date `at` has no
    time-of-day to compare, so it is compared at DATE grain against
    `snapshot_at`'s own date -- a same-day backfilled apply/open must count,
    not be silently dropped because midnight < some later time-of-day.
    Full-grain comparisons use `>=`, not `>`: an exact-timestamp tie (the
    apply/open landed at literally the same instant as the snapshot) counts
    as caused by it, not excluded by it."""
    if len(at) == 10:
        return _parse(at).date() >= _parse(snapshot_at).date()
    return _parse(at) >= _parse(snapshot_at)


def _days_between(at: str, snapshot_at: str) -> float:
    """Companion to `_at_on_or_after`: the day-count to report once that
    check has already passed. Bare-date `at` -> whole DATE-grain days
    (matches `_at_on_or_after`'s date-grain comparison -- a same-day apply is
    0 days, not a fraction derived from comparing midnight to a full
    timestamp); full-grain `at` -> fractional days, as before H1."""
    if len(at) == 10:
        return float((_parse(at).date() - _parse(snapshot_at).date()).days)
    return (_parse(at) - _parse(snapshot_at)).total_seconds() / 86400


def _median(values):
    return statistics.median(values) if values else None


def _chunks(ids):
    ordered = list(dict.fromkeys(i for i in ids if i is not None))
    for start in range(0, len(ordered), _CHUNK):
        yield ordered[start:start + _CHUNK]


def _in_clause(n: int) -> str:
    return ",".join("?" * n)


# --------------------------------------------------------------------------- #
# cell builders
# --------------------------------------------------------------------------- #
def _cell(key: str, rows: list, min_sample: int) -> dict:
    """One `cell` (application_outcomes shape). `rows` is this cell's list of
    per-identity outcome-fact dicts (see `_application_identities`); rates are
    per `n_applied` (this cell's own denominator), never per `n_responded` --
    an "interview rate" that quietly changed denominator between cells would
    make cross-cell comparison meaningless."""
    n_applied = len(rows)
    n_responded = sum(1 for r in rows if r["responded"])
    n_phone = sum(1 for r in rows if r["phone_screen"])
    n_interview = sum(1 for r in rows if r["interview"])
    n_offer = sum(1 for r in rows if r["offer"])
    days = [r["days_to_response"] for r in rows if r["days_to_response"] is not None]
    return {
        "key": key,
        "n_applied": n_applied,
        "n_responded": n_responded,
        "n_phone_screen": n_phone,
        "n_interview": n_interview,
        "n_offer": n_offer,
        "response_rate": (n_responded / n_applied) if n_applied else None,
        "interview_rate": (n_interview / n_applied) if n_applied else None,
        "offer_rate": (n_offer / n_applied) if n_applied else None,
        "median_days_to_response": _median(days),
        "low_sample": n_applied < min_sample,
    }


def _rcell(key: str, rows: list, min_sample: int) -> dict:
    """One `rcell` (recommendation_outcomes shape). `rows` is this cell's list
    of per-posting recommendation-fact dicts (see `_recommended_postings`);
    rates are per `n_recommended`."""
    n_recommended = len(rows)
    n_opened = sum(1 for r in rows if r["opened"])
    n_applied = sum(1 for r in rows if r["applied"])
    days = [r["days_to_apply"] for r in rows if r["applied"] and r["days_to_apply"] is not None]
    return {
        "key": key,
        "n_recommended": n_recommended,
        "n_opened": n_opened,
        "n_applied": n_applied,
        "open_rate": (n_opened / n_recommended) if n_recommended else None,
        "application_rate": (n_applied / n_recommended) if n_recommended else None,
        "median_days_to_apply": _median(days),
        "low_sample": n_recommended < min_sample,
    }


def _sort_cells(cells: list, count_key: str) -> list:
    """Known keys sorted by `count_key` DESC then key ASC; the literal
    `"unknown"` key (when present) always sorts last, regardless of its own
    count -- it is a catch-all bucket, not a competitive entrant."""
    known = sorted(
        (c for c in cells if c["key"] != "unknown"), key=lambda c: (-c[count_key], c["key"])
    )
    unknown = [c for c in cells if c["key"] == "unknown"]
    return known + unknown


def _group(rows: list, key_fn) -> dict:
    groups: dict = {}
    for r in rows:
        key = key_fn(r) or "unknown"
        groups.setdefault(key, []).append(r)
    return groups


# --------------------------------------------------------------------------- #
# application_outcomes: identity resolution + outcome facts from state_events
# --------------------------------------------------------------------------- #
def _seen_key_posting_ids(conn: sqlite3.Connection) -> dict:
    """seen_key -> resolved posting_id, per-STREAM (M1): an explicit
    `state_events.posting_id` value ANYWHERE in that seen_key's full event
    history wins over the `job_state.posting_id` bridge for every row of
    that stream (posting_id-first, per this task's brief). Resolved ONCE
    here, shared by BOTH `_application_identities` and
    `_posting_first_applied_at` -- before this fix the two paths each
    re-derived a seen_key's posting_id independently (one per-stream, one
    per-row), which could attribute the same seen_key's apply to two
    DIFFERENT postings when an explicit posting_id appeared on a later row
    than the one a per-row caller happened to look at."""
    event_rows = conn.execute(
        "SELECT seen_key, posting_id FROM state_events WHERE field='status'"
    ).fetchall()
    job_state_rows = conn.execute("SELECT seen_key, posting_id FROM job_state").fetchall()
    seen_key_pid = {r["seen_key"]: r["posting_id"] for r in job_state_rows if r["posting_id"]}

    by_seen_key: dict = {}
    for r in event_rows:
        by_seen_key.setdefault(r["seen_key"], []).append(r["posting_id"])

    resolved: dict = {}
    for seen_key, pids in by_seen_key.items():
        explicit = next((p for p in pids if p), None)
        resolved[seen_key] = explicit or seen_key_pid.get(seen_key)
    # A seen_key known only to job_state (no state_events row at all) still
    # resolves via the bridge, keeping the map complete for any caller.
    for seen_key, pid in seen_key_pid.items():
        resolved.setdefault(seen_key, pid)
    return resolved


def _application_identities(conn: sqlite3.Connection, profile) -> list:
    """One dict per DISTINCT applied identity (dedupe by posting_id when
    resolvable, else by bare seen_key -- F5.3's identity rule).

    Identity resolution, posting_id-first: a `state_events` row's own
    `posting_id` column wins when populated (it is, only where migration-11
    lineage was unique -- see this task's brief); otherwise `job_state.
    posting_id` for that row's `seen_key` (the durable, current bridge).
    Neither resolving is not an error -- a role state-tracked purely by
    `seen_key` is still a real applied identity, just one analytics cannot
    place a posting-scoped dimension on.

    Two different `seen_key`s that resolve to the SAME posting_id (a role
    re-tracked under a new key, or a test fixture asserting the dedupe rule
    directly) are merged into ONE identity here: their event rows are pooled
    and re-sorted by (at, id) before outcome facts are derived, so "first
    Applied at" and "first response at" are computed over the identity's
    FULL history, not just one key's slice of it.

    Each identity dict ALSO carries three keys this module's own cells never
    read -- `applied_at` (the identity's first Applied `at`), `posting_id`
    (the resolved posting, None for a seen_key-only identity), and
    `identity_key` (the stable `"pid:<id>"` / `"sk:<seen_key>"` label of the
    dedupe rule above). They exist so `ranking_metrics.py` can key this SAME
    computation by posting instead of maintaining a third private copy of
    "what counts as applied / as a response" (5.5 fix B6). Additive by
    construction: every pre-existing key keeps its exact meaning, and
    `_cell`/`_group` continue to read only the dimension and outcome keys.
    """
    event_rows = conn.execute(
        "SELECT id, seen_key, url, old_value, new_value, at, posting_id "
        "FROM state_events WHERE field='status' ORDER BY seen_key, at, id"
    ).fetchall()
    job_state_rows = conn.execute("SELECT seen_key, url, posting_id FROM job_state").fetchall()
    seen_key_url = {r["seen_key"]: r["url"] for r in job_state_rows}

    # Per-seen_key posting_id (M1): resolved by the SAME shared helper
    # `_posting_first_applied_at` uses, so the two endpoint families never
    # attribute the same seen_key's apply to different postings.
    seen_key_resolved_pid = _seen_key_posting_ids(conn)

    by_seen_key: dict = {}
    for r in event_rows:
        by_seen_key.setdefault(r["seen_key"], []).append(r)

    identity_groups: dict = {}
    for seen_key, rows in by_seen_key.items():
        pid = seen_key_resolved_pid.get(seen_key)
        identity = ("pid", pid) if pid else ("sk", seen_key)
        identity_groups.setdefault(identity, []).extend(rows)

    identities = []
    for identity, rows in identity_groups.items():
        rows = sorted(rows, key=lambda r: (r["at"], r["id"]))
        applied_ats = [r["at"] for r in rows if r["new_value"] == "Applied"]
        if not applied_ats:
            continue
        first_applied_at = min(applied_ats, key=_parse)
        reached = {r["new_value"] for r in rows}
        response_ats = [
            r["at"] for r in rows if r["old_value"] == "Applied" and r["new_value"] in _RESPONSE_STAGES
        ]
        responded = bool(response_ats)
        days_to_response = None
        if response_ats:
            first_response_at = min(response_ats, key=_parse)
            delta = (_parse(first_response_at) - _parse(first_applied_at)).total_seconds() / 86400
            if delta >= 0:
                days_to_response = delta

        pid = identity[1] if identity[0] == "pid" else None
        url = next((r["url"] for r in rows if r["url"]), None) or seen_key_url.get(rows[0]["seen_key"])
        source, source_category, match_band, competition_band, role_family = _attribute_dimensions(
            conn, pid, url, profile
        )

        identities.append(
            {
                # Additive (B6) -- see this function's docstring. `posting_id`
                # is None for a seen_key-only identity; `identity_key` names
                # which of the two dedupe rules produced this row.
                "applied_at": first_applied_at,
                "posting_id": pid,
                "identity_key": f"pid:{pid}" if pid else f"sk:{identity[1]}",
                "responded": responded,
                "phone_screen": "Phone screen" in reached,
                "interview": "Interview" in reached,
                "offer": "Offer" in reached,
                "days_to_response": days_to_response,
                "source": source,
                "source_category": source_category,
                "match_band": match_band,
                "competition_band": competition_band,
                "role_family": role_family,
            }
        )
    return identities


def _attribute_dimensions(conn: sqlite3.Connection, posting_id, url, profile):
    """(source, source_category, match_band, competition_band, role_family)
    for one applied identity. posting_id-first: when resolvable, delegate to
    `outcomes._enrich_item` -- the SAME current-score/latest-version/role-
    family precedence W-5.2 uses to describe a posting, reused rather than
    duplicated (per this task's brief). Falling back to the legacy `jobs`
    table by url (odds string, source column only -- role_family has no
    source there, so it stays unresolved) when no posting_id resolves at
    all. Neither resolving: every dimension is `None`, which callers turn
    into the literal `"unknown"` bucket."""
    if posting_id is not None:
        enriched = outcomes._enrich_item(conn, {"posting_id": posting_id}, profile)
        return (
            enriched["source"],
            enriched["source_category"],
            enriched["match_label"],
            enriched["competition_label"],
            enriched["role_family"],
        )
    if url is not None:
        row = conn.execute("SELECT odds, source FROM jobs WHERE url=?", (url,)).fetchone()
        if row is not None:
            match_label, competition_label = outcomes._split_odds(row["odds"])
            source = row["source"]
            return (source, outcomes._source_category(source), match_label, competition_label, None)
    return (None, None, None, None, None)


def _application_outcomes(conn: sqlite3.Connection, min_sample: int, profile) -> dict:
    identities = _application_identities(conn, profile)
    return {
        "n_applied_total": len(identities),
        "by_source": _sort_cells(
            [_cell(k, rows, min_sample) for k, rows in _group(identities, lambda r: r["source"]).items()],
            "n_applied",
        ),
        "by_source_category": _sort_cells(
            [
                _cell(k, rows, min_sample)
                for k, rows in _group(identities, lambda r: r["source_category"]).items()
            ],
            "n_applied",
        ),
        "by_match_band": _sort_cells(
            [
                _cell(k, rows, min_sample)
                for k, rows in _group(identities, lambda r: r["match_band"]).items()
            ],
            "n_applied",
        ),
        "by_competition_band": _sort_cells(
            [
                _cell(k, rows, min_sample)
                for k, rows in _group(identities, lambda r: r["competition_band"]).items()
            ],
            "n_applied",
        ),
        "by_role_family": _sort_cells(
            [
                _cell(k, rows, min_sample)
                for k, rows in _group(identities, lambda r: r["role_family"]).items()
            ],
            "n_applied",
        ),
    }


# --------------------------------------------------------------------------- #
# recommendation_outcomes: latest-item attribution + opened/applied matching
# --------------------------------------------------------------------------- #
def _latest_items_by_posting(conn: sqlite3.Connection) -> dict:
    """One row per DISTINCT recommended posting: its FIRST snapshot's
    `captured_at` (for "did the application come AFTER being recommended")
    and its LATEST snapshot item (for rank/band/role_family/source_category
    attribution -- documented choice, F5.3: a posting whose rank or bands
    changed across snapshots is described by the most recent view of it, not
    an arbitrary or a first one).

    Single query, `ORDER BY posting_id, captured_at, snapshot_id` so both the
    minimum (first row per group) and the maximum (last row per group) are
    available from one pass in Python -- no per-posting point query."""
    rows = conn.execute(
        "SELECT i.posting_id, i.rank, i.score_version_id, i.source_category, "
        "i.match_label, i.competition_label, i.role_family, s.captured_at, s.snapshot_id "
        "FROM recommendation_snapshot_items i "
        "JOIN recommendation_snapshots s ON s.snapshot_id = i.snapshot_id "
        "ORDER BY i.posting_id, s.captured_at, s.snapshot_id"
    ).fetchall()
    by_posting: dict = {}
    for r in rows:
        by_posting.setdefault(r["posting_id"], []).append(r)
    result = {}
    for posting_id, items in by_posting.items():
        result[posting_id] = {
            "first_snapshot_at": items[0]["captured_at"],
            "latest": items[-1],
        }
    return result


def _posting_first_opened_at(conn: sqlite3.Connection) -> dict:
    """posting_id -> earliest 'opened' outcome_events.at (M3/L6).

    posting_id match is primary; a url-or-seen_key-only event (posting_id
    NULL) is bridged the SAME way `outcomes._resolve_seen_key` resolves a
    url when WRITING an event (L6): `jobs.seen_key` wins first, falling back
    to a `job_state` row addressed by its own url; either bridged seen_key
    is then resolved to a posting_id through `job_state.posting_id` -- the
    same durable bridge `_application_identities` uses -- rather than left
    unmatched, per this task's brief ("keep it simple and documented").

    Returns first-`at`, not a plain set (M3): `open_rate` is recommendation-
    attributed, not lifetime -- an open that happened BEFORE a posting's
    first snapshot (e.g. the visitor found it some other way, long before
    the scorer ever surfaced it) must not count as caused by the
    recommendation. `_recommended_postings` compares this timestamp against
    each posting's first-snapshot `at`, grain-aware, via `_at_on_or_after`
    (same rule H1 applies to applies)."""
    rows = conn.execute(
        "SELECT posting_id, seen_key, url, at FROM outcome_events WHERE kind='opened'"
    ).fetchall()
    if not rows:
        return {}
    job_state_rows = conn.execute("SELECT seen_key, url, posting_id FROM job_state").fetchall()
    by_seen_key = {r["seen_key"]: r["posting_id"] for r in job_state_rows if r["posting_id"]}
    by_url = {r["url"]: r["posting_id"] for r in job_state_rows if r["posting_id"] and r["url"]}
    jobs_seen_key_by_url = {
        r["url"]: r["seen_key"]
        for r in conn.execute("SELECT url, seen_key FROM jobs").fetchall()
        if r["url"]
    }

    first_at: dict = {}
    for r in rows:
        pid = r["posting_id"]
        if pid is None:
            seen_key = r["seen_key"] or (jobs_seen_key_by_url.get(r["url"]) if r["url"] else None)
            pid = (by_seen_key.get(seen_key) if seen_key else None) or (
                by_url.get(r["url"]) if r["url"] else None
            )
        if not pid:
            continue
        if pid not in first_at or _parse(r["at"]) < _parse(first_at[pid]):
            first_at[pid] = r["at"]
    return first_at


def _posting_first_applied_at(conn: sqlite3.Connection) -> dict:
    """posting_id -> earliest 'Applied' event `at`, using the SAME per-STREAM
    identity resolution `_application_identities` uses (M1's shared
    `_seen_key_posting_ids`: an explicit posting_id anywhere in a seen_key's
    event stream wins over the job_state bridge for every row of that
    stream) -- not a per-row resolution, which could pick a DIFFERENT
    posting_id for the same seen_key than `_application_identities` does.
    Kept separate from `_application_identities`'s richer per-identity facts
    because recommendation attribution only ever needs the timestamp, keyed
    by posting_id -- every recommended posting already carries one
    (`recommendation_snapshot_items.posting_id` is NOT NULL), so there is no
    seen_key-only case to fall back to here the way `_application_identities`
    has to."""
    rows = conn.execute(
        "SELECT id, seen_key, new_value, at, posting_id FROM state_events "
        "WHERE field='status' AND new_value='Applied' ORDER BY seen_key, at, id"
    ).fetchall()
    if not rows:
        return {}
    seen_key_pid = _seen_key_posting_ids(conn)
    first_at: dict = {}
    for r in rows:
        pid = seen_key_pid.get(r["seen_key"])
        if not pid:
            continue
        if pid not in first_at or _parse(r["at"]) < _parse(first_at[pid]):
            first_at[pid] = r["at"]
    return first_at


def _feature_presence(conn: sqlite3.Connection, score_version_ids) -> dict:
    """score_version_id -> ({feature present in score_row}, {feature present
    in hireability}). Batched in IN-list chunks (corpus-scale precedent:
    `canonical_reads._chunks`) rather than one query per posting."""
    result = {}
    for chunk in _chunks(score_version_ids):
        rows = conn.execute(
            f"SELECT score_version_id, features_json FROM score_versions "
            f"WHERE score_version_id IN ({_in_clause(len(chunk))})",
            chunk,
        ).fetchall()
        for r in rows:
            if not r["features_json"]:
                result[r["score_version_id"]] = (set(), set())
                continue
            try:
                parsed = json.loads(r["features_json"])
            except (TypeError, ValueError):
                parsed = {}
            score_row = parsed.get("score_row") or {}
            hireability = parsed.get("hireability") or {}
            result[r["score_version_id"]] = (set(score_row), set(hireability))
    return result


def _recommended_postings(conn: sqlite3.Connection) -> list:
    """One dict per DISTINCT recommended posting_id: its latest-item
    attribution plus opened/applied outcome facts."""
    by_posting = _latest_items_by_posting(conn)
    first_opened = _posting_first_opened_at(conn)
    first_applied = _posting_first_applied_at(conn)

    postings = []
    for posting_id, info in by_posting.items():
        latest = info["latest"]
        first_snapshot_at = info["first_snapshot_at"]

        opened = False
        opened_at = first_opened.get(posting_id)
        if opened_at is not None and _at_on_or_after(opened_at, first_snapshot_at):
            opened = True

        applied = False
        days_to_apply = None
        applied_at = first_applied.get(posting_id)
        if applied_at is not None and _at_on_or_after(applied_at, first_snapshot_at):
            applied = True
            days_to_apply = _days_between(applied_at, first_snapshot_at)

        postings.append(
            {
                "posting_id": posting_id,
                "rank": latest["rank"],
                "match_band": latest["match_label"],
                "competition_band": latest["competition_label"],
                "role_family": latest["role_family"],
                "source_category": latest["source_category"],
                "score_version_id": latest["score_version_id"],
                "opened": opened,
                "applied": applied,
                "days_to_apply": days_to_apply,
            }
        )
    return postings


def _recommendation_outcomes(conn: sqlite3.Connection, min_sample: int) -> dict:
    postings = _recommended_postings(conn)
    n_snapshots = conn.execute("SELECT COUNT(*) AS n FROM recommendation_snapshots").fetchone()["n"]

    by_rank_groups = _group(postings, lambda p: str(p["rank"]))
    by_rank = sorted(
        (_rcell(k, rows, min_sample) for k, rows in by_rank_groups.items()),
        key=lambda c: int(c["key"]),
    )

    feature_ids = {p["score_version_id"] for p in postings if p["score_version_id"]}
    presence = _feature_presence(conn, feature_ids)

    def _feature_cells(vocab, index):
        """One cell per feature actually present on >=1 recommended posting's
        score (`raw_score`/`function_match`-shaped features: present on
        EVERY scored row, so their cell's `n_recommended` equals the scored
        population; `degree_gated`/blocker-shaped features: present only
        when that condition fired, so their cell is a strict subset -- both
        are legitimate "some rows have this feature" cells, not a bug),
        PLUS a trailing `"unknown"` cell (M2) for every recommended posting
        with no resolvable score_version_id (no score attributed at all, or
        a score_version_id absent from `score_versions.features_json`) --
        without it, `by_feature`'s denominators silently covered a SMALLER
        population than every other slice (by_rank, by_match_band, ...),
        which all include every recommended posting via `postings` directly.
        `nothing dropped` (this module's docstring) applies here too."""
        cells = []
        for feature in vocab:
            rows = [
                p
                for p in postings
                if p["score_version_id"]
                and p["score_version_id"] in presence
                and feature in presence[p["score_version_id"]][index]
            ]
            if rows:
                cells.append(_rcell(feature, rows, min_sample))
        unknown_rows = [
            p for p in postings if not p["score_version_id"] or p["score_version_id"] not in presence
        ]
        if unknown_rows:
            cells.append(_rcell("unknown", unknown_rows, min_sample))
        return _sort_cells(cells, "n_recommended")

    return {
        "n_snapshots": n_snapshots,
        "n_recommended_total": len(postings),
        "by_rank": by_rank,
        "by_match_band": _sort_cells(
            [_rcell(k, rows, min_sample) for k, rows in _group(postings, lambda p: p["match_band"]).items()],
            "n_recommended",
        ),
        "by_competition_band": _sort_cells(
            [
                _rcell(k, rows, min_sample)
                for k, rows in _group(postings, lambda p: p["competition_band"]).items()
            ],
            "n_recommended",
        ),
        "by_role_family": _sort_cells(
            [_rcell(k, rows, min_sample) for k, rows in _group(postings, lambda p: p["role_family"]).items()],
            "n_recommended",
        ),
        "by_source_category": _sort_cells(
            [
                _rcell(k, rows, min_sample)
                for k, rows in _group(postings, lambda p: p["source_category"]).items()
            ],
            "n_recommended",
        ),
        "by_feature": {
            "score_row": _feature_cells(candidate_profile.REQUIRED_SCORE_ROW_FEATURES, 0),
            "hireability": _feature_cells(candidate_profile.REQUIRED_HIREABILITY_FEATURES, 1),
        },
    }


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def outcome_analytics(conn: sqlite3.Connection, *, min_sample: int = 5, now: str | None = None) -> dict:
    """Read-only outcome analytics -- application outcomes (state_events) and
    recommendation outcomes (W-5.2 snapshots/events), sliced by source, match
    band, competition band, role family, feature, and rank.

    `min_sample` sets the `low_sample` threshold on every cell (`n < min_
    sample`); it does not filter or hide any cell -- a low-sample cell is
    still real data, just flagged as not-yet-trustworthy on its own.

    `now` is accepted for interface symmetry with other read modules
    (`funnel.py`'s ghost count takes one) but unused today: every metric here
    is defined by a completed pair of events (Applied -> response, snapshot
    -> apply), never by an open span measured against the current moment,
    so there is nothing for it to affect yet.
    """
    profile = outcomes._load_profile()
    return {
        "min_sample": min_sample,
        "application_outcomes": _application_outcomes(conn, min_sample, profile),
        "recommendation_outcomes": _recommendation_outcomes(conn, min_sample),
    }
