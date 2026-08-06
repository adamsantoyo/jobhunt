"""Task 5.5a: ranking-quality metrics over the served "today" queue.

Answers "is the Today queue any good", as opposed to `outcome_analytics.py`'s
"what happened to everything we ever recommended or applied to" (any surface,
all time) and `calibration.py`'s "do the odds bands themselves deserve
trust". Every fact this module reads about WHAT was shown and WHEN comes from
`recommendation_snapshot_items` / `recommendation_snapshots` rows with
`surface='today'` -- the point-in-time queue captures `routers/queueapi.py`'s
snapshot-on-serve writes, at most one per local day (see that router's
docstring). Facts about what the visitor DID (open, apply, respond) come from
`outcome_events` and `state_events`, resolved through the SAME posting_id
bridges `outcome_analytics.py` already established and tested -- imported
directly by name below, never re-derived, so "what counts as applied" and
"what counts as a response" have exactly one definition across the whole app.

Every cell carries explicit denominators and a `low_sample` flag
(`n < min_sample`) rather than a rate that quietly lies when n is tiny --
same convention `outcome_analytics.py` and `calibration.py` use. `min_sample`
never filters or hides a cell.

Two families are deliberately population-wide rather than scoped to
"served via the Today queue": `response_rate` and `ghost_rate`. The 5.5
contract states each as "... over applied postings" / "... over
n_applied_eligible" with no "served" qualifier (unlike `top10_application_
rate`, `time_to_application`, `stale_rate`, and `source_yield`, whose
contract text says "served" explicitly). Read literally, these two report
the site's whole applied-postings population (the same identity/response
rule `outcome_analytics.py` and `funnel.py` already define, re-exposed here
under this payload's explicit-denominator/low_sample convention for a
single self-contained ranking-quality view) rather than only the subset the
Today queue is provably responsible for. Recorded here prominently per the
dispatch instruction, since the contract text left this ambiguous and the
two readings produce materially different numbers. Population-wide is taken
literally in the denominator too (5.5 fix B6): both families count applied
identities that resolve to no posting_id at all, which the posting-keyed
families below necessarily cannot see.

Deterministic given the same database contents and the same injected `now`:
no clock reads, no randomness anywhere in this module.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from types import SimpleNamespace
from typing import Optional

from . import outcomes
from .outcome_analytics import (
    _application_identities,
    _at_on_or_after,
    _chunks,
    _days_between,
    _group,
    _in_clause,
    _median,
    _parse,
    _seen_key_posting_ids,
    _sort_cells,
)
from .ranking import QueuePolicy, freshness_of

__all__ = ["ranking_metrics"]


# --------------------------------------------------------------------------- #
# served population: everything ever shown in a surface='today' snapshot
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class _ServedPosting:
    posting_id: str
    #: MIN captured_at across every rank this posting was ever served at.
    first_served_at: str
    #: MIN captured_at among rank<=10 rows only; None if never top-10.
    first_top10_served_at: Optional[str]
    #: Descriptive fields from the LATEST item -- mirrors `outcome_analytics.
    #: _latest_items_by_posting`'s "most recent view of it" convention: a
    #: re-served posting's source/category is described by its newest
    #: snapshot, not an arbitrary or a first one.
    source: Optional[str]
    source_category: Optional[str]


def _today_served(conn: sqlite3.Connection) -> dict:
    """posting_id -> `_ServedPosting`, over every distinct posting ever served
    in a surface='today' `recommendation_snapshots` row.

    Single query, `ORDER BY posting_id, captured_at, snapshot_id` (identical
    shape to `outcome_analytics._latest_items_by_posting`) so both the
    earliest row (first_served_at / the top-10 subset's earliest) and the
    latest row (descriptive fields) come from one pass in Python."""
    rows = conn.execute(
        "SELECT i.posting_id, i.rank, i.source, i.source_category, "
        "s.captured_at, s.snapshot_id "
        "FROM recommendation_snapshot_items i "
        "JOIN recommendation_snapshots s ON s.snapshot_id = i.snapshot_id "
        "WHERE s.surface='today' "
        "ORDER BY i.posting_id, s.captured_at, s.snapshot_id"
    ).fetchall()
    by_posting: dict = {}
    for r in rows:
        by_posting.setdefault(r["posting_id"], []).append(r)

    result = {}
    for posting_id, items in by_posting.items():
        first_served_at = items[0]["captured_at"]
        top10 = [it["captured_at"] for it in items if it["rank"] <= 10]
        latest = items[-1]
        result[posting_id] = _ServedPosting(
            posting_id=posting_id,
            first_served_at=first_served_at,
            first_top10_served_at=top10[0] if top10 else None,
            source=latest["source"],
            source_category=latest["source_category"],
        )
    return result


def _today_snapshots_by_day(conn: sqlite3.Connection) -> dict:
    """local ISO date -> {"queue_size": int, "posting_ids": set} for every
    surface='today' snapshot captured that day. Under the snapshot-on-serve
    contract there is normally exactly one per local day; grouped (summed
    queue_size, unioned posting_ids) rather than assumed-singular so a day
    with more than one snapshot (pre-cutover fixtures, direct test writes)
    still gets an honest accounting instead of silently picking one."""
    snap_rows = conn.execute(
        "SELECT snapshot_id, captured_at, queue_size FROM recommendation_snapshots "
        "WHERE surface='today' ORDER BY captured_at"
    ).fetchall()
    if not snap_rows:
        return {}
    snapshot_ids = [r["snapshot_id"] for r in snap_rows]
    items_by_snapshot: dict = {}
    for chunk in _chunks(snapshot_ids):
        for r in conn.execute(
            f"SELECT snapshot_id, posting_id FROM recommendation_snapshot_items "
            f"WHERE snapshot_id IN ({_in_clause(len(chunk))})",
            chunk,
        ).fetchall():
            items_by_snapshot.setdefault(r["snapshot_id"], set()).add(r["posting_id"])

    by_day: dict = {}
    for r in snap_rows:
        day = r["captured_at"][:10]
        entry = by_day.setdefault(day, {"queue_size": 0, "posting_ids": set()})
        entry["queue_size"] += r["queue_size"]
        entry["posting_ids"] |= items_by_snapshot.get(r["snapshot_id"], set())
    return by_day


# --------------------------------------------------------------------------- #
# visitor-action facts: opens (any date), state_events (any date), applies
# --------------------------------------------------------------------------- #
def _posting_open_ats(conn: sqlite3.Connection) -> dict:
    """posting_id -> set of 'opened' `outcome_events.at` values, at their
    FULL stored grain.

    Resolution mirrors `outcome_analytics._posting_first_opened_at`'s bridge
    (posting_id direct; else seen_key -> `job_state.posting_id`; else url ->
    `jobs.seen_key` -> the same bridge) exactly, but collects EVERY event
    rather than only the first -- `queue_completion` asks "did an open
    happen on THIS day", which `_posting_first_opened_at`'s min-only return
    cannot answer, so this is a companion pass over the same rows rather
    than a second definition of the bridge itself.

    The timestamps are NOT truncated to dates (5.5 fix B5). They were, and
    that silently broke `source_yield`'s attribution rule: a truncated `at`
    is a length-10 string, which `_at_on_or_after` reads as a bare backfilled
    date and compares at DATE grain, so an open that happened at 08:00 --
    hours BEFORE the 09:00 serve that day -- counted as caused by the
    recommendation. Full grain here; `_open_days` derives the date-grain view
    `queue_completion` legitimately needs."""
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
    result: dict = {}
    for r in rows:
        pid = r["posting_id"]
        if pid is None:
            seen_key = r["seen_key"] or (jobs_seen_key_by_url.get(r["url"]) if r["url"] else None)
            pid = (by_seen_key.get(seen_key) if seen_key else None) or (
                by_url.get(r["url"]) if r["url"] else None
            )
        if not pid:
            continue
        result.setdefault(pid, set()).add(r["at"])
    return result


def _open_days(open_ats: dict) -> dict:
    """posting_id -> set of local ISO dates it was opened on. The date-grain
    projection of `_posting_open_ats`, for `queue_completion`'s "did an open
    happen on THIS day" question only -- every other consumer compares
    timestamps, and must see them at full grain (B5)."""
    return {pid: {at[:10] for at in ats} for pid, ats in open_ats.items()}


def _posting_state_event_dates(conn: sqlite3.Connection) -> dict:
    """posting_id -> set of local ISO dates with >=1 `state_events` row (ANY
    field, not just status transitions) for that posting's seen_key(s), via
    `outcome_analytics._seen_key_posting_ids`'s per-stream resolution.
    `queue_completion`'s "any state_event" action is deliberately broader
    than an Applied transition -- hiding, snoozing, or noting a job is still
    the visitor acting on it that day."""
    rows = conn.execute("SELECT seen_key, at FROM state_events").fetchall()
    if not rows:
        return {}
    seen_key_pid = _seen_key_posting_ids(conn)
    result: dict = {}
    for r in rows:
        pid = seen_key_pid.get(r["seen_key"])
        if not pid:
            continue
        result.setdefault(pid, set()).add(r["at"][:10])
    return result


def _applied_identities(conn: sqlite3.Connection) -> list:
    """Every applied identity, straight from `outcome_analytics.
    _application_identities` -- ONE definition of "what counts as applied /
    as a response" for the whole app (5.5 fix B6). Before this, a private
    per-posting reimplementation lived here; it agreed with the shared one
    only by inspection, which is exactly the drift this module's docstring
    promises not to allow."""
    return _application_identities(conn, outcomes._load_profile())


def _applied_by_posting(identities: list) -> dict:
    """posting_id -> its identity dict, for the posting-keyed families.

    An identity with no resolvable posting_id (`identity_key` "sk:...") is
    absent here on purpose: it cannot be linked to a SERVED posting at all,
    so top10 / time-to-application / stale / source-yield have nothing to say
    about it. It is still counted by the two population-wide families
    (`response_rate`, `ghost_rate`), which take the full identity list --
    "population-wide" means population-wide."""
    return {i["posting_id"]: i for i in identities if i["posting_id"]}


def _latest_posting_freshness(conn: sqlite3.Connection, posting_ids) -> dict:
    """posting_id -> (posted, first_seen) from each posting's newest
    `posting_versions` row (observed_at DESC, posting_version_id DESC
    tiebreak -- the same ordering `outcomes._latest_posting_version` uses
    for one posting, batched here over many at once via `_chunks`/
    `_in_clause`, per the 5.3 watch item on corpus-scale point queries)."""
    result = {}
    for chunk in _chunks(posting_ids):
        rows = conn.execute(
            f"SELECT posting_id, posted, first_seen FROM ("
            f"  SELECT posting_id, posted, first_seen, observed_at, posting_version_id, "
            f"         ROW_NUMBER() OVER (PARTITION BY posting_id "
            f"           ORDER BY observed_at DESC, posting_version_id DESC) AS rn "
            f"  FROM posting_versions WHERE posting_id IN ({_in_clause(len(chunk))})"
            f") WHERE rn=1",
            chunk,
        ).fetchall()
        for r in rows:
            result[r["posting_id"]] = (r["posted"], r["first_seen"])
    return result


# --------------------------------------------------------------------------- #
# metric families
# --------------------------------------------------------------------------- #
def _top10_application_rate(served: dict, applied: dict, min_sample: int) -> dict:
    """Distinct postings ever served at rank<=10 in a "today" snapshot ->
    Applied on/after that first top-10 serve, over n served-at-top10."""
    top10 = [p for p in served.values() if p.first_top10_served_at is not None]
    n = len(top10)
    n_applied = 0
    for p in top10:
        facts = applied.get(p.posting_id)
        if facts and _at_on_or_after(facts["applied_at"], p.first_top10_served_at):
            n_applied += 1
    return {
        "n_served_top10": n,
        "n_applied": n_applied,
        "rate": (n_applied / n) if n else None,
        "low_sample": n < min_sample,
    }


def _time_to_application(served: dict, applied: dict, min_sample: int) -> dict:
    """Median days first-serve (any rank) -> Applied, over postings served in
    a "today" snapshot AND applied to on/after that first serve."""
    n_served = len(served)
    days = []
    for p in served.values():
        facts = applied.get(p.posting_id)
        if facts and _at_on_or_after(facts["applied_at"], p.first_served_at):
            days.append(_days_between(facts["applied_at"], p.first_served_at))
    n_applied = len(days)
    return {
        "n_served": n_served,
        "n_applied": n_applied,
        "median_days": _median(days),
        "low_sample": n_applied < min_sample,
    }


def _response_rate(identities: list, min_sample: int) -> dict:
    """Funnel-defined response rate (Applied -> {Phone screen, Interview,
    Offer, Rejected}, `outcome_analytics._RESPONSE_STAGES`) over EVERY
    applied identity the system knows about -- see this module's docstring on
    why this family is population-wide rather than served-via-Today-scoped.

    The denominator includes identities that resolve to no posting_id
    (seen_key-only, `identity_key` "sk:..."): those are real applications
    whose posting the canonical graph simply cannot name, and dropping them
    would quietly report a rate over a SUBSET while calling it the
    population (5.5 fix B6)."""
    n_applied = len(identities)
    n_responded = sum(1 for f in identities if f["responded"])
    return {
        "n_applied": n_applied,
        "n_responded": n_responded,
        "rate": (n_responded / n_applied) if n_applied else None,
        "low_sample": n_applied < min_sample,
    }


def _stale_rate(
    conn: sqlite3.Connection, served: dict, opened: dict, applied: dict,
    policy: QueuePolicy, today: date, min_sample: int,
) -> dict:
    """Served postings NEVER opened and NEVER applied to (any timing -- total
    disengagement, not serve-attributed) whose current posting age is past
    `policy.stale_days` (basis posted, first_seen fallback -- `ranking.
    freshness_of`, the SAME function `build_queue` uses to exclude stale
    postings from the queue in the first place, reused rather than
    reimplemented), over n_served.

    `today` is the INJECTED `now`'s date, never `date.today()` -- this whole
    module is deterministic given the same database and the same `now` (see
    `ranking_metrics`' docstring), and this is the family that would silently
    stop being so first. `opened` and `applied` are membership checks only,
    so full-grain timestamps and posting-keyed identities both work here."""
    posting_ids = list(served.keys())
    freshness = _latest_posting_freshness(conn, posting_ids)
    n_served = len(served)
    n_stale = 0
    for pid in posting_ids:
        if pid in opened or pid in applied:
            continue
        posted, first_seen = freshness.get(pid, (None, None))
        job = SimpleNamespace(posted=posted, first_seen=first_seen)
        fresh = freshness_of(job, today, policy)
        if fresh.bucket == "stale":
            n_stale += 1
    return {
        "n_served": n_served,
        "n_stale_never_engaged": n_stale,
        "rate": (n_stale / n_served) if n_served else None,
        "low_sample": n_served < min_sample,
    }


def _ghost_rate(identities: list, ghost_days: int, now: datetime, min_sample: int) -> dict:
    """Applied identities >= `ghost_days` ago (maturity: old enough that "no
    response yet" is a real signal, not just impatience) with no response
    event, over n_applied_eligible. Population-wide, same denominator rule
    (and same seen_key-only inclusion) as `_response_rate`.

    NOT the same number as `routers/funnel.py`'s `ghosted.applied_no_
    response_14d`, and deliberately so (5.5 fix B10). That one asks "how many
    jobs are SITTING at status Applied and have been for >= 14 days" -- a
    current-state count that a later status change (to Rejected, or to
    Passed) removes from the tally. This one asks "of the applications old
    enough to judge (>= `ghost_days`, 21 by default), how many were never
    answered at all" -- a history count over `_RESPONSE_STAGES` transitions
    that no later edit un-counts. Different questions, different windows,
    both kept: the funnel tab reports pipeline hygiene, this reports
    response silence."""
    n_total = len(identities)
    n_eligible = 0
    n_ghosted = 0
    for facts in identities:
        applied_at = _parse(facts["applied_at"])
        age_days = (now - applied_at).total_seconds() / 86400
        if age_days >= ghost_days:
            n_eligible += 1
            if not facts["responded"]:
                n_ghosted += 1
    return {
        "n_applied_total": n_total,
        "n_applied_eligible": n_eligible,
        "n_ghosted": n_ghosted,
        "rate": (n_ghosted / n_eligible) if n_eligible else None,
        "low_sample": n_eligible < min_sample,
        "ghost_days": ghost_days,
    }


def _queue_completion(by_day: dict, open_dates: dict, state_dates: dict, min_sample: int) -> dict:
    """Per snapshot day: distinct same-day-served postings with a same-
    local-day action (an 'opened' outcome_event or any state_events row that
    day) / the day's QUEUE_SIZE.

    The denominator is `queue_size` -- what the visitor was SHOWN that day --
    never `n_served`, the count of snapshot items that resolved to a
    posting_id. The two differ whenever capture could not resolve every
    served entry (`queue_size >= len(items)` is `capture_snapshot`'s
    documented partial-view contract), and dividing by `n_served` would
    report "you finished your queue" off a fraction of the queue.

    A day with `n_served == 0` -- shown a queue, but not one item of it
    identifiable -- reports `rate: null`, not `0.0` (5.5 fix B7): zero
    completions out of zero KNOWN items is unmeasured, not a bad day, and a
    fabricated 0.0 would drag the median down for a capture failure. Same
    for a `queue_size == 0` day.

    `median_rate` is the median over days whose rate is non-null only, for
    that reason; `n_days` counts ALL reported days (including the null-rate
    ones), and `low_sample` compares that DAY count -- not any per-day
    posting count -- against `min_sample`."""
    per_day = []
    rates = []
    for day in sorted(by_day):
        info = by_day[day]
        queue_size = info["queue_size"]
        posting_ids = info["posting_ids"]
        n_served = len(posting_ids)
        n_completed = sum(
            1 for pid in posting_ids
            if day in open_dates.get(pid, ()) or day in state_dates.get(pid, ())
        )
        rate = (n_completed / queue_size) if (queue_size and n_served) else None
        if rate is not None:
            rates.append(rate)
        per_day.append(
            {
                "day": day,
                "queue_size": queue_size,
                "n_served": n_served,
                "n_completed": n_completed,
                "rate": rate,
            }
        )
    return {
        "by_day": per_day,
        "n_days": len(per_day),
        "median_rate": _median(rates),
        "low_sample": len(per_day) < min_sample,
    }


def _yield_cell(key: str, postings: list, opened: dict, applied: dict, min_sample: int) -> dict:
    """One source/category cell: n_recommended -> n_opened -> n_applied ->
    n_responded, each rate against ITS OWN stage's denominator
    (`response_rate` = n_responded / n_applied, not n_recommended -- an
    "interview rate" that quietly changed denominator between cells would
    make cross-cell comparison meaningless, same rule `outcome_analytics.
    _cell`'s docstring states). open/apply are recommendation-attributed
    (on/after the posting's first "today" serve), same stance `outcome_
    analytics._recommended_postings` takes for open_rate/application_rate."""
    n_recommended = len(postings)
    n_opened = 0
    n_applied = 0
    n_responded = 0
    for p in postings:
        open_ats = opened.get(p.posting_id)
        # `key=_parse`, not lexicographic min: the same mixed-grain rule
        # `outcome_analytics._posting_first_opened_at` applies (a bare
        # 'YYYY-MM-DD' backfilled row and a full timestamp must order by
        # instant, not by string).
        first_open = min(open_ats, key=_parse) if open_ats else None
        if first_open is not None and _at_on_or_after(first_open, p.first_served_at):
            n_opened += 1
        facts = applied.get(p.posting_id)
        if facts and _at_on_or_after(facts["applied_at"], p.first_served_at):
            n_applied += 1
            if facts["responded"]:
                n_responded += 1
    return {
        "key": key,
        "n_recommended": n_recommended,
        "n_opened": n_opened,
        "n_applied": n_applied,
        "n_responded": n_responded,
        "open_rate": (n_opened / n_recommended) if n_recommended else None,
        "application_rate": (n_applied / n_recommended) if n_recommended else None,
        "response_rate": (n_responded / n_applied) if n_applied else None,
        "low_sample": n_recommended < min_sample,
    }


def _source_yield(served: dict, opened: dict, applied: dict, min_sample: int) -> dict:
    by_source = _group(list(served.values()), lambda p: p.source)
    by_category = _group(list(served.values()), lambda p: p.source_category)
    return {
        "by_source": _sort_cells(
            [_yield_cell(k, ps, opened, applied, min_sample) for k, ps in by_source.items()],
            "n_recommended",
        ),
        "by_source_category": _sort_cells(
            [_yield_cell(k, ps, opened, applied, min_sample) for k, ps in by_category.items()],
            "n_recommended",
        ),
    }


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def ranking_metrics(
    conn: sqlite3.Connection, *, min_sample: int = 5, ghost_days: int = 21, now: str,
) -> dict:
    """Read-only ranking-quality metrics over the surface='today' snapshot
    history (see this module's docstring for the six families and the two
    deliberately population-wide ones).

    `now` is injected, never read from the clock (deterministic given the
    same database contents and the same `now`); it is the reference moment
    `stale_rate` ages postings against and `ghost_rate` ages applications
    against, and is echoed as `generated_at`. Required, not optional (unlike
    `calibration_report`'s `now=None`): every family but `response_rate`
    depends on it, whereas `calibration_report` only needs it for one arm.
    """
    served = _today_served(conn)
    identities = _applied_identities(conn)
    applied = _applied_by_posting(identities)
    open_ats = _posting_open_ats(conn)
    open_days = _open_days(open_ats)
    state_dates = _posting_state_event_dates(conn)
    by_day = _today_snapshots_by_day(conn)
    now_dt = _parse(now)
    policy = QueuePolicy()

    return {
        "generated_at": now,
        "min_sample": min_sample,
        "ghost_days": ghost_days,
        "top10_application_rate": _top10_application_rate(served, applied, min_sample),
        "time_to_application": _time_to_application(served, applied, min_sample),
        "response_rate": _response_rate(identities, min_sample),
        "stale_rate": _stale_rate(
            conn, served, open_ats, applied, policy, now_dt.date(), min_sample
        ),
        "ghost_rate": _ghost_rate(identities, ghost_days, now_dt, min_sample),
        "queue_completion": _queue_completion(by_day, open_days, state_dates, min_sample),
        "source_yield": _source_yield(served, open_ats, applied, min_sample),
    }
