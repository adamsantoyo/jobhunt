"""Application funnel metrics, derived entirely from the state_events log.

Read-only aggregation: response rate, per-stage conversion, time-in-stage, weekly
application pace, and ghost count. `job_state.status`/`updated_at` alone cannot answer
any of this (a single mutable row has no history) -- that's exactly what state_events
exists for. This module never writes.
"""
import sqlite3
import statistics
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends

from ..config import ADVANCED_STATUSES, STATUSES
from ..db import get_db

router = APIRouter()

# Events that count as a candidate "responding" to an application (leaving Applied
# toward any of these). Passed is excluded on purpose -- that's the applicant giving
# up, not the company responding.
_RESPONSE_STAGES = ("Phone screen", "Interview", "Offer", "Rejected")

# The positive progression tracked by stage_conversion. Terminal/negative outcomes
# (Rejected, Passed) are covered by `totals` and `response_rate`, not a conversion
# pair here -- "conversion" reads naturally as advancing the pipeline, not attrition.
_STAGE_ORDER = ("Applied", "Phone screen", "Interview", "Offer")

_GHOST_DAYS = 14


def _parse(at: str) -> datetime:
    """state_events.at is local-naive ISO, but two grains coexist: backfilled events
    derived from applied_date store a bare 'YYYY-MM-DD', everything else a full
    datetime isoformat() string. Both parse via fromisoformat (a date-only value lands
    at local midnight), and that ordering matches how the rows were inserted (see
    migrations.py's backfill docstring)."""
    return datetime.fromisoformat(at)


def _week_start(dt: datetime) -> str:
    """Monday-start local week containing `dt`, as an ISO date string."""
    return (dt.date() - timedelta(days=dt.weekday())).isoformat()


def _median(values):
    return statistics.median(values) if values else None


def _status_events_by_key(conn: sqlite3.Connection):
    """Every status-change event, oldest first within each seen_key. `id` breaks ties
    when two events share an `at` (e.g. a backfilled Applied+later-status pair minted
    in the same migration pass)."""
    rows = conn.execute(
        "SELECT seen_key, old_value, new_value, at FROM state_events "
        "WHERE field='status' ORDER BY seen_key, at, id"
    ).fetchall()
    by_key: dict[str, list] = {}
    for r in rows:
        by_key.setdefault(r["seen_key"], []).append(r)
    return by_key


@router.get("/funnel")
def get_funnel(conn: sqlite3.Connection = Depends(get_db)):
    by_key = _status_events_by_key(conn)

    reached: dict[str, set] = {s: set() for s in STATUSES}
    responded_keys: set = set()
    week_counts: dict[str, int] = {}
    time_spans: dict[str, list] = {s: [] for s in ADVANCED_STATUSES}

    for seen_key, events in by_key.items():
        for i, ev in enumerate(events):
            nv = ev["new_value"]
            if nv in reached:
                reached[nv].add(seen_key)
            if ev["old_value"] == "Applied" and nv in _RESPONSE_STAGES:
                responded_keys.add(seen_key)
            if nv == "Applied":
                week_counts_key = _week_start(_parse(ev["at"]))
                week_counts[week_counts_key] = week_counts.get(week_counts_key, 0) + 1
            # time-in-stage: pair this entry with the *next* status event for the same
            # role. No next event means the role is still in this stage (or this was
            # its last known state) -- an open-ended span, so it's excluded rather than
            # measured against "now" (that would keep growing every time the endpoint
            # is called, for data that hasn't actually changed).
            if nv in time_spans and i + 1 < len(events):
                start = _parse(ev["at"])
                end = _parse(events[i + 1]["at"])
                days = (end - start).total_seconds() / 86400
                if days >= 0:
                    time_spans[nv].append(days)

    totals = {
        "applied": len(reached["Applied"]),
        "responded": len(responded_keys),
        "phone_screen": len(reached["Phone screen"]),
        "interview": len(reached["Interview"]),
        "offer": len(reached["Offer"]),
        "rejected": len(reached["Rejected"]),
    }
    response_rate = (totals["responded"] / totals["applied"]) if totals["applied"] > 0 else None

    stage_conversion = []
    for frm, to in zip(_STAGE_ORDER, _STAGE_ORDER[1:]):
        entered_n = len(reached[frm])
        advanced_n = len(reached[frm] & reached[to])
        rate = (advanced_n / entered_n) if entered_n > 0 else None
        stage_conversion.append(
            {"from": frm, "to": to, "entered": entered_n, "advanced": advanced_n, "rate": rate}
        )

    time_in_stage = [
        {"status": s, "median_days": _median(time_spans[s]), "n": len(time_spans[s])}
        for s in ADVANCED_STATUSES
    ]

    apps_per_week = [
        {"week_start": wk, "count": c} for wk, c in sorted(week_counts.items())
    ]

    # Ghosted: currently sitting at Applied, with no response, for >= 14 days. Read
    # straight off job_state.applied_date (the durable, always-current column) rather
    # than re-deriving "last entered Applied" from the event stream -- same value,
    # simpler query, and correct even if a role re-entered Applied more than once.
    today = date.today()
    ghosted = 0
    for r in conn.execute(
        "SELECT applied_date FROM job_state WHERE status='Applied' AND applied_date IS NOT NULL"
    ).fetchall():
        try:
            applied_on = date.fromisoformat(str(r["applied_date"])[:10])
        except ValueError:
            continue
        if (today - applied_on).days >= _GHOST_DAYS:
            ghosted += 1

    return {
        "totals": totals,
        "response_rate": response_rate,
        "stage_conversion": stage_conversion,
        "time_in_stage": time_in_stage,
        "apps_per_week": apps_per_week,
        "ghosted": {"applied_no_response_14d": ghosted},
    }


@router.get("/activity")
def get_activity(conn: sqlite3.Connection = Depends(get_db)):
    """Today's pace: what got done today, this week's application count, and the
    daily-applied streak. Read events once, aggregate in Python (mirrors get_funnel).

    'done' is deliberately applied-or-passed only. Snoozing a role isn't a completed
    action -- it's deferring the decision -- so counting it toward "done today" made
    the old dashboard's done-count lie about how much real progress happened; this is
    the fix.
    """
    today = date.today()
    today_iso = today.isoformat()
    this_week_start = _week_start(datetime.combine(today, datetime.min.time()))

    rows = conn.execute(
        "SELECT seen_key, field, new_value, at FROM state_events "
        "WHERE field IN ('status', 'snoozed_until')"
    ).fetchall()

    applied_today: set = set()
    passed_today: set = set()
    snoozed_today: set = set()
    applied_dates: set = set()  # every local date with >=1 status->Applied event
    apps_this_week = 0

    for r in rows:
        at = _parse(r["at"])
        day = at.date().isoformat()
        field, nv = r["field"], r["new_value"]
        if field == "status" and nv == "Applied":
            applied_dates.add(day)
            if day == today_iso:
                applied_today.add(r["seen_key"])
            if _week_start(at) == this_week_start:
                apps_this_week += 1
        elif field == "status" and nv == "Passed" and day == today_iso:
            passed_today.add(r["seen_key"])
        elif field == "snoozed_until" and day == today_iso:
            # Only counts if snoozed INTO the future -- a snooze date in the past (or
            # today) isn't deferring anything.
            if nv and nv > today_iso:
                snoozed_today.add(r["seen_key"])

    done_today = applied_today | passed_today

    # Streak: consecutive local dates ending today with >=1 Applied event. If today
    # has none yet, fall back to yesterday's run (the day isn't over -- don't zero a
    # live streak mid-day); anything older than that is a broken streak (0).
    streak_days = 0
    if today_iso in applied_dates:
        cursor = today
    elif (today - timedelta(days=1)).isoformat() in applied_dates:
        cursor = today - timedelta(days=1)
    else:
        cursor = None
    while cursor is not None and cursor.isoformat() in applied_dates:
        streak_days += 1
        cursor -= timedelta(days=1)

    return {
        "today": {
            "applied": len(applied_today),
            "passed": len(passed_today),
            "snoozed": len(snoozed_today),
            "done": len(done_today),
        },
        "apps_this_week": apps_this_week,
        "streak_days": streak_days,
    }
