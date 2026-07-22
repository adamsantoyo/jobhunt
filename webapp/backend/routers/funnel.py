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
