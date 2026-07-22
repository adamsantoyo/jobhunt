"""Funnel endpoint golden-case tests (W6, finishing W5 scaffolding).

Parametrized data tables: event timeline in -> expected /api/funnel numbers out.

Expected values are derived from the endpoint's documented contract (see
routers/funnel.py's module docstring and inline comments), not guessed:
  - stage_conversion always contains exactly the three positive-progression pairs
    (Applied->Phone screen, Phone screen->Interview, Interview->Offer), each with
    entered/advanced counts and a null rate when entered==0 -- "empty-data safe"
    means no crash, not an empty list.
  - time_in_stage always contains one entry per ADVANCED_STATUS. A status reached
    with no *later* status event for that role is an open-ended span and is
    deliberately excluded from the median (median_days=None, n=0) rather than
    measured against "now" (see routers/funnel.py's time-in-stage comment) --
    every single-event case below therefore has an empty time-in-stage.
  - Dates are computed relative to a single `NOW` captured at module import, never
    hardcoded absolute dates, so this suite stays correct no matter what day it runs.

Scenarios:
  1. empty DB
  2. single Applied (no response for >14d -> ghosted)
  3. Applied->Rejected (single response)
  4. Applied->Phone Screen->Interview->Offer (multi-stage progression)
  5. two apps same week vs. across weeks
  6. backdated applied_date (before current week)
"""
import csv
import json
from datetime import datetime, time, timedelta
from types import SimpleNamespace

import pytest

from backend import config
from backend.config import ADVANCED_STATUSES
from backend.db import connect, init_db
from backend.identity import seen_key
from backend.ingest import ingest
from backend.routers.funnel import get_funnel

# Single "now" for the whole module: every relative timestamp below is derived from
# this one value, so setup and expected can never drift apart from separate
# datetime.now() calls, and week/ghost math stays internally consistent.
NOW = datetime.now()
MONDAY_THIS_WEEK = NOW.date() - timedelta(days=NOW.weekday())


def _week_start(dt: datetime) -> str:
    """Independent re-derivation of the Monday-start week (mirrors, but does not
    import, routers/funnel.py's _week_start -- a golden case should compute its
    expectation from the rule, not from the implementation under test)."""
    return (dt.date() - timedelta(days=dt.weekday())).isoformat()


def _days_between(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() / 86400


COLUMNS = ["tier", "odds", "odds_score", "odds_why", "new", "title", "company", "location",
           "salary", "salary_min", "salary_max", "posted", "first_seen", "remote", "source",
           "also_seen_on", "url", "req_id", "why", "flags", "desc_snippet"]


def row(**kw):
    """Helper to create a job CSV row with sensible defaults."""
    d = {c: "" for c in COLUMNS}
    d.update({"tier": "3", "odds": "Target", "odds_score": "0", "remote": "False"})
    d.update(kw)
    return d


def write_csv(path, rows):
    """Write job CSV fixture."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """Temporary repo fixture for test isolation."""
    root = tmp_path / "repo"
    results = root / "results"
    results.mkdir(parents=True)
    monkeypatch.setattr(config, "ROOT", root)
    monkeypatch.setattr(config, "RESULTS", results)
    return SimpleNamespace(root=root, results=results, db=tmp_path / "app.db")


def open_conn(repo):
    """Open a fresh connection with schema initialized."""
    conn = connect(repo.db)
    init_db(conn)
    return conn


def set_state(conn, url, **fields):
    """Seed a job_state row for a given URL."""
    sk = conn.execute("SELECT seen_key FROM jobs WHERE url=?", (url,)).fetchone()["seen_key"]
    base = {
        "status": "New",
        "notes": "",
        "follow_up_date": None,
        "applied_date": None,
        "starred": 0,
        "hidden": 0,
        "contact": "",
        "snoozed_until": None,
    }
    base.update(fields)
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, notes, follow_up_date, applied_date, "
        "starred, hidden, contact, snoozed_until, updated_at) "
        "VALUES (:seen_key, :url, :status, :notes, :follow_up_date, :applied_date, "
        ":starred, :hidden, :contact, :snoozed_until, :updated_at)",
        {
            "url": url,
            "seen_key": sk,
            "updated_at": fields.get("updated_at", NOW.isoformat()),
            **base,
        },
    )
    conn.commit()


def add_state_event(conn, seen_key, field, old_value, new_value, at, source="patch"):
    """Helper to manually insert a state_event (for pre-seeding timelines)."""
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
        "VALUES (?, NULL, ?, ?, ?, ?, ?)",
        (seen_key, field, old_value, new_value, at, source),
    )
    conn.commit()


def ingest_and_init(conn):
    """Run a minimal ingest to initialize jobs table."""
    ingest(conn)


# --------------------------------------------------------------------------- #
# Expected-shape builders -- shared by every case so "always 3 pairs" / "always
# 6 statuses" / "open spans excluded" are expressed once, not repeated per case.
# --------------------------------------------------------------------------- #

_STAGE_PAIRS = (("Applied", "Phone screen"), ("Phone screen", "Interview"), ("Interview", "Offer"))


def base_stage_conversion(entered_by_pair=None, advanced_by_pair=None):
    """Build the always-3-entries stage_conversion list. `entered_by_pair` /
    `advanced_by_pair` are {(from, to): count} overrides; anything unlisted is 0."""
    entered_by_pair = entered_by_pair or {}
    advanced_by_pair = advanced_by_pair or {}
    out = []
    for frm, to in _STAGE_PAIRS:
        entered = entered_by_pair.get((frm, to), 0)
        advanced = advanced_by_pair.get((frm, to), 0)
        rate = (advanced / entered) if entered > 0 else None
        out.append({"from": frm, "to": to, "entered": entered, "advanced": advanced, "rate": rate})
    return out


def base_time_in_stage(spans=None):
    """Build the always-len(ADVANCED_STATUSES)-entries time_in_stage list. `spans` is
    {status: [days, ...]} for statuses with a *closed* span (a later status event
    exists); anything unlisted has no closed span (median_days=None, n=0)."""
    spans = spans or {}
    out = []
    for s in ADVANCED_STATUSES:
        days = spans.get(s, [])
        median = sorted(days)[len(days) // 2] if len(days) % 2 else \
            (sorted(days)[len(days) // 2 - 1] + sorted(days)[len(days) // 2]) / 2 if days else None
        out.append({"status": s, "median_days": median, "n": len(days)})
    return out


def assert_funnel_matches(actual, expected):
    assert actual["totals"] == expected["totals"]
    if expected["response_rate"] is None:
        assert actual["response_rate"] is None
    else:
        assert actual["response_rate"] == pytest.approx(expected["response_rate"])

    assert len(actual["stage_conversion"]) == len(expected["stage_conversion"])
    for a, e in zip(actual["stage_conversion"], expected["stage_conversion"]):
        assert (a["from"], a["to"], a["entered"], a["advanced"]) == (e["from"], e["to"], e["entered"], e["advanced"])
        if e["rate"] is None:
            assert a["rate"] is None
        else:
            assert a["rate"] == pytest.approx(e["rate"])

    assert len(actual["time_in_stage"]) == len(expected["time_in_stage"])
    for a, e in zip(actual["time_in_stage"], expected["time_in_stage"]):
        assert (a["status"], a["n"]) == (e["status"], e["n"])
        if e["median_days"] is None:
            assert a["median_days"] is None
        else:
            assert a["median_days"] == pytest.approx(e["median_days"])

    assert actual["apps_per_week"] == expected["apps_per_week"]
    assert actual["ghosted"] == expected["ghosted"]


# --------------------------------------------------------------------------- #
# Golden-case fixtures: (name, setup_fn, expected_output)
# --------------------------------------------------------------------------- #

def case_empty_db():
    """Empty database -> zero totals, but the fixed-shape arrays are still fully
    populated (all pairs/statuses present with zero counts and null rates)."""
    def setup(repo, conn):
        pass

    expected = {
        "totals": {"applied": 0, "responded": 0, "phone_screen": 0, "interview": 0,
                    "offer": 0, "rejected": 0},
        "response_rate": None,  # No applications to respond to
        "stage_conversion": base_stage_conversion(),
        "time_in_stage": base_time_in_stage(),
        "apps_per_week": [],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="empty_db", setup=setup, expected=expected)


def case_single_applied_no_response():
    """Single Applied role, >14 days with no response -> ghosted. Only one event
    exists for this role (no later status event), so its Applied span is still open
    -- time_in_stage excludes it entirely."""
    applied_dt = NOW - timedelta(days=15)
    applied_at = applied_dt.isoformat()

    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(title="Support Engineer", company="Initech", location="San Francisco, CA",
                 url="https://example.com/job1")],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("Initech", "Support Engineer", "San Francisco, CA")
        add_state_event(conn, sk, "status", None, "Applied", applied_at, source="backfill")
        set_state(conn, "https://example.com/job1", status="Applied", applied_date=applied_at)

    expected = {
        "totals": {"applied": 1, "responded": 0, "phone_screen": 0, "interview": 0,
                    "offer": 0, "rejected": 0},
        "response_rate": 0.0,  # 0 responded / 1 applied
        "stage_conversion": base_stage_conversion(entered_by_pair={("Applied", "Phone screen"): 1}),
        "time_in_stage": base_time_in_stage(),  # open span, no later event -> excluded
        "apps_per_week": [{"week_start": _week_start(applied_dt), "count": 1}],
        "ghosted": {"applied_no_response_14d": 1},  # 15 days, over the 14d threshold
    }
    return SimpleNamespace(name="single_applied_no_response", setup=setup, expected=expected)


def case_applied_rejected():
    """Applied -> Rejected (one response). A closed Applied span exists (the
    Rejected event is the 'next' event), so time_in_stage has one measured entry."""
    applied_dt = NOW - timedelta(days=10)
    rejected_dt = applied_dt + timedelta(days=3, hours=4)
    applied_at = applied_dt.isoformat()
    rejected_at = rejected_dt.isoformat()

    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(title="Sales Engineer", company="WidgetCorp", location="New York, NY",
                 url="https://example.com/job2")],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("WidgetCorp", "Sales Engineer", "New York, NY")
        add_state_event(conn, sk, "status", None, "Applied", applied_at, source="backfill")
        add_state_event(conn, sk, "status", "Applied", "Rejected", rejected_at, source="patch")
        set_state(conn, "https://example.com/job2", status="Rejected",
                  applied_date=applied_at, updated_at=rejected_at)

    expected = {
        "totals": {"applied": 1, "responded": 1, "phone_screen": 0, "interview": 0,
                    "offer": 0, "rejected": 1},
        "response_rate": 1.0,  # 1 responded / 1 applied
        # Rejected is a terminal outcome, not a _STAGE_ORDER pair -- Applied->Phone
        # screen still shows entered=1 (this role reached Applied) but advanced=0
        # (it never reached Phone screen; it left via Rejected instead).
        "stage_conversion": base_stage_conversion(entered_by_pair={("Applied", "Phone screen"): 1}),
        "time_in_stage": base_time_in_stage(spans={"Applied": [_days_between(applied_dt, rejected_dt)]}),
        "apps_per_week": [{"week_start": _week_start(applied_dt), "count": 1}],
        "ghosted": {"applied_no_response_14d": 0},  # status moved past Applied
    }
    return SimpleNamespace(name="applied_rejected", setup=setup, expected=expected)


def case_multi_stage_progression():
    """Applied -> Phone Screen -> Interview -> Offer (full funnel progression).
    Fixed absolute timestamps: this scenario tests stage math, not date-relative
    behavior, so it doesn't need to move with NOW."""
    applied_dt = datetime(2026, 6, 20, 9, 0, 0)
    phone_dt = datetime(2026, 6, 25, 14, 0, 0)
    interview_dt = datetime(2026, 7, 2, 10, 0, 0)
    offer_dt = datetime(2026, 7, 10, 16, 0, 0)

    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(title="Platform Engineer", company="TechStartup", location="San Francisco, CA",
                 url="https://example.com/job3")],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("TechStartup", "Platform Engineer", "San Francisco, CA")
        add_state_event(conn, sk, "status", None, "Applied", applied_dt.isoformat(), source="backfill")
        add_state_event(conn, sk, "status", "Applied", "Phone screen", phone_dt.isoformat(), source="patch")
        add_state_event(conn, sk, "status", "Phone screen", "Interview", interview_dt.isoformat(), source="patch")
        add_state_event(conn, sk, "status", "Interview", "Offer", offer_dt.isoformat(), source="patch")
        set_state(conn, "https://example.com/job3", status="Offer",
                  applied_date=applied_dt.isoformat(), updated_at=offer_dt.isoformat())

    expected = {
        "totals": {"applied": 1, "responded": 1, "phone_screen": 1, "interview": 1,
                    "offer": 1, "rejected": 0},
        "response_rate": 1.0,
        "stage_conversion": base_stage_conversion(
            entered_by_pair={("Applied", "Phone screen"): 1, ("Phone screen", "Interview"): 1,
                              ("Interview", "Offer"): 1},
            advanced_by_pair={("Applied", "Phone screen"): 1, ("Phone screen", "Interview"): 1,
                               ("Interview", "Offer"): 1},
        ),
        "time_in_stage": base_time_in_stage(spans={
            "Applied": [_days_between(applied_dt, phone_dt)],
            "Phone screen": [_days_between(phone_dt, interview_dt)],
            "Interview": [_days_between(interview_dt, offer_dt)],
            # Offer has no later event -> excluded (open span), covered by the default.
        }),
        "apps_per_week": [{"week_start": _week_start(applied_dt), "count": 1}],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="multi_stage_progression", setup=setup, expected=expected)


def case_two_apps_same_week():
    """Two applications in the same week (same week_start bucket). Anchored to this
    week's Monday so the case is valid no matter what day the suite runs."""
    applied_dt_1 = datetime.combine(MONDAY_THIS_WEEK, time(9, 0)) + timedelta(days=1)   # Tue
    applied_dt_2 = datetime.combine(MONDAY_THIS_WEEK, time(10, 0)) + timedelta(days=2)  # Wed

    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(title="Support Tier 2", company="CompanyA", location="Seattle, WA",
                 url="https://example.com/job4a"),
             row(title="Support Tier 2", company="CompanyB", location="Seattle, WA",
                 url="https://example.com/job4b")],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 2}))
        ingest_and_init(conn)
        sk1 = seen_key("CompanyA", "Support Tier 2", "Seattle, WA")
        sk2 = seen_key("CompanyB", "Support Tier 2", "Seattle, WA")
        add_state_event(conn, sk1, "status", None, "Applied", applied_dt_1.isoformat(), source="backfill")
        add_state_event(conn, sk2, "status", None, "Applied", applied_dt_2.isoformat(), source="backfill")
        set_state(conn, "https://example.com/job4a", status="Applied", applied_date=applied_dt_1.isoformat())
        set_state(conn, "https://example.com/job4b", status="Applied", applied_date=applied_dt_2.isoformat())

    expected = {
        "totals": {"applied": 2, "responded": 0, "phone_screen": 0, "interview": 0,
                    "offer": 0, "rejected": 0},
        "response_rate": 0.0,
        "stage_conversion": base_stage_conversion(entered_by_pair={("Applied", "Phone screen"): 2}),
        "time_in_stage": base_time_in_stage(),
        "apps_per_week": [{"week_start": _week_start(applied_dt_1), "count": 2}],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="two_apps_same_week", setup=setup, expected=expected)


def case_two_apps_across_weeks():
    """Two applications more than a week apart -> two distinct week_start buckets.
    Any two timestamps >6 days apart necessarily fall in different Monday-start
    weeks, so this holds regardless of which weekday the suite runs on."""
    applied_dt_1 = NOW - timedelta(days=9)  # a week+ ago: guaranteed a different ISO week
    applied_dt_2 = NOW - timedelta(days=1)  # recent

    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(title="Backend Engineer", company="StartupX", location="Austin, TX",
                 url="https://example.com/job5a"),
             row(title="Backend Engineer", company="StartupY", location="Austin, TX",
                 url="https://example.com/job5b")],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 2}))
        ingest_and_init(conn)
        sk1 = seen_key("StartupX", "Backend Engineer", "Austin, TX")
        sk2 = seen_key("StartupY", "Backend Engineer", "Austin, TX")
        add_state_event(conn, sk1, "status", None, "Applied", applied_dt_1.isoformat(), source="backfill")
        add_state_event(conn, sk2, "status", None, "Applied", applied_dt_2.isoformat(), source="backfill")
        set_state(conn, "https://example.com/job5a", status="Applied", applied_date=applied_dt_1.isoformat())
        set_state(conn, "https://example.com/job5b", status="Applied", applied_date=applied_dt_2.isoformat())

    expected = {
        "totals": {"applied": 2, "responded": 0, "phone_screen": 0, "interview": 0,
                    "offer": 0, "rejected": 0},
        "response_rate": 0.0,
        "stage_conversion": base_stage_conversion(entered_by_pair={("Applied", "Phone screen"): 2}),
        "time_in_stage": base_time_in_stage(),
        "apps_per_week": sorted(
            [{"week_start": _week_start(applied_dt_1), "count": 1},
             {"week_start": _week_start(applied_dt_2), "count": 1}],
            key=lambda d: d["week_start"],
        ),
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="two_apps_across_weeks", setup=setup, expected=expected)


def case_backdated_applied():
    """Application with a backdated applied_date, later rejected -> one closed span
    whose length is exactly the applied->rejected gap."""
    applied_dt = NOW - timedelta(days=20)
    rejected_dt = NOW - timedelta(days=10)

    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(title="DevOps Engineer", company="CloudCorp", location="Remote",
                 url="https://example.com/job6")],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("CloudCorp", "DevOps Engineer", "Remote")
        add_state_event(conn, sk, "status", None, "Applied", applied_dt.isoformat(), source="backfill")
        add_state_event(conn, sk, "status", "Applied", "Rejected", rejected_dt.isoformat(), source="patch")
        set_state(conn, "https://example.com/job6", status="Rejected",
                  applied_date=applied_dt.isoformat(), updated_at=rejected_dt.isoformat())

    expected = {
        "totals": {"applied": 1, "responded": 1, "phone_screen": 0, "interview": 0,
                    "offer": 0, "rejected": 1},
        "response_rate": 1.0,
        "stage_conversion": base_stage_conversion(entered_by_pair={("Applied", "Phone screen"): 1}),
        "time_in_stage": base_time_in_stage(spans={"Applied": [_days_between(applied_dt, rejected_dt)]}),
        "apps_per_week": [{"week_start": _week_start(applied_dt), "count": 1}],
        "ghosted": {"applied_no_response_14d": 0},  # status moved past Applied (Rejected)
    }
    return SimpleNamespace(name="backdated_applied", setup=setup, expected=expected)


# Parametrize test cases
GOLDEN_CASES = [
    case_empty_db(),
    case_single_applied_no_response(),
    case_applied_rejected(),
    case_multi_stage_progression(),
    case_two_apps_same_week(),
    case_two_apps_across_weeks(),
    case_backdated_applied(),
]


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[c.name for c in GOLDEN_CASES])
def test_funnel_golden_cases(repo, case):
    conn = open_conn(repo)
    case.setup(repo, conn)
    actual = get_funnel(conn)
    assert_funnel_matches(actual, case.expected)
    conn.close()
