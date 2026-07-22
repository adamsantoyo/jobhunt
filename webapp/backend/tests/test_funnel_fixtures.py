"""Funnel endpoint golden-case test fixtures (W5 scaffolding).

Parametrized data tables: event timeline in → expected funnel numbers out.
Test logic deferred to W6 (Sonnet); stubs collect without errors.

Scenarios:
  1. empty DB
  2. single Applied (no response for >14d → ghosted)
  3. Applied→Rejected (single response)
  4. Applied→Phone Screen→Interview→Offer (multi-stage progression)
  5. two apps same week vs. across weeks
  6. backdated applied_date (before current week)
"""
import csv
import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from backend import config
from backend.db import connect, init_db
from backend.identity import seen_key
from backend.ingest import ingest
from backend.migrations import run_migrations


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
            "updated_at": fields.get("updated_at", "2026-07-22T00:00:00"),
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


# --------------------------------------------------------------------------- #
# Golden-case fixtures: (name, setup_fn, expected_output)
# --------------------------------------------------------------------------- #

def case_empty_db():
    """Empty database → all zeros/empty response."""
    def setup(repo, conn):
        pass
    expected = {
        "totals": {
            "applied": 0,
            "responded": 0,
            "phone_screen": 0,
            "interview": 0,
            "offer": 0,
            "rejected": 0,
        },
        "response_rate": None,  # No applications to respond to
        "stage_conversion": [],
        "time_in_stage": [],
        "apps_per_week": [],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="empty_db", setup=setup, expected=expected)


def case_single_applied_no_response():
    """Single Applied role, >14 days with no response → ghosted."""
    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(
                title="Support Engineer",
                company="Initech",
                location="San Francisco, CA",
                url="https://example.com/job1",
            )],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("Initech", "Support Engineer", "San Francisco, CA")
        # Applied 15 days ago, no response
        applied_at = (datetime.now() - timedelta(days=15)).isoformat()
        add_state_event(conn, sk, "status", None, "Applied", applied_at, source="backfill")
        set_state(conn, "https://example.com/job1", status="Applied", applied_date=applied_at)

    expected = {
        "totals": {
            "applied": 1,
            "responded": 0,
            "phone_screen": 0,
            "interview": 0,
            "offer": 0,
            "rejected": 0,
        },
        "response_rate": 0.0,  # 0 responded / 1 applied
        "stage_conversion": [],  # No conversions
        "time_in_stage": [
            {"status": "Applied", "median_days": 15, "n": 1}
        ],
        "apps_per_week": [
            {"week_start": (datetime.now() - timedelta(days=15)).date().isoformat(), "count": 1}
        ],
        "ghosted": {"applied_no_response_14d": 1},  # Over 14 days with no response
    }
    return SimpleNamespace(name="single_applied_no_response", setup=setup, expected=expected)


def case_applied_rejected():
    """Applied → Rejected (one response)."""
    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(
                title="Sales Engineer",
                company="WidgetCorp",
                location="New York, NY",
                url="https://example.com/job2",
            )],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("WidgetCorp", "Sales Engineer", "New York, NY")
        applied_at = "2026-07-15T10:00:00"
        rejected_at = "2026-07-18T14:00:00"
        add_state_event(conn, sk, "status", None, "Applied", applied_at, source="backfill")
        add_state_event(conn, sk, "status", "Applied", "Rejected", rejected_at, source="patch")
        set_state(
            conn,
            "https://example.com/job2",
            status="Rejected",
            applied_date=applied_at,
            updated_at=rejected_at,
        )

    expected = {
        "totals": {
            "applied": 1,
            "responded": 1,
            "phone_screen": 0,
            "interview": 0,
            "offer": 0,
            "rejected": 1,
        },
        "response_rate": 1.0,  # 1 responded / 1 applied
        "stage_conversion": [
            {"from": "Applied", "to": "Rejected", "entered": 1, "advanced": 1, "rate": 1.0}
        ],
        "time_in_stage": [
            {"status": "Applied", "median_days": 3, "n": 1}
        ],
        "apps_per_week": [
            # Week of July 15 (Monday July 14)
            {"week_start": "2026-07-14", "count": 1}
        ],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="applied_rejected", setup=setup, expected=expected)


def case_multi_stage_progression():
    """Applied → Phone Screen → Interview → Offer (full funnel progression)."""
    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(
                title="Platform Engineer",
                company="TechStartup",
                location="San Francisco, CA",
                url="https://example.com/job3",
            )],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("TechStartup", "Platform Engineer", "San Francisco, CA")
        applied_at = "2026-06-20T09:00:00"
        phone_at = "2026-06-25T14:00:00"
        interview_at = "2026-07-02T10:00:00"
        offer_at = "2026-07-10T16:00:00"

        add_state_event(conn, sk, "status", None, "Applied", applied_at, source="backfill")
        add_state_event(conn, sk, "status", "Applied", "Phone screen", phone_at, source="patch")
        add_state_event(conn, sk, "status", "Phone screen", "Interview", interview_at, source="patch")
        add_state_event(conn, sk, "status", "Interview", "Offer", offer_at, source="patch")

        set_state(
            conn,
            "https://example.com/job3",
            status="Offer",
            applied_date=applied_at,
            updated_at=offer_at,
        )

    expected = {
        "totals": {
            "applied": 1,
            "responded": 1,  # Responded = left Applied
            "phone_screen": 1,
            "interview": 1,
            "offer": 1,
            "rejected": 0,
        },
        "response_rate": 1.0,  # 1 responded / 1 applied
        "stage_conversion": [
            {"from": "Applied", "to": "Phone screen", "entered": 1, "advanced": 1, "rate": 1.0},
            {"from": "Phone screen", "to": "Interview", "entered": 1, "advanced": 1, "rate": 1.0},
            {"from": "Interview", "to": "Offer", "entered": 1, "advanced": 1, "rate": 1.0},
        ],
        "time_in_stage": [
            {"status": "Applied", "median_days": 5, "n": 1},
            {"status": "Phone screen", "median_days": 7, "n": 1},
            {"status": "Interview", "median_days": 8, "n": 1},
        ],
        "apps_per_week": [
            {"week_start": "2026-06-16", "count": 1}  # Week containing June 20
        ],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="multi_stage_progression", setup=setup, expected=expected)


def case_two_apps_same_week():
    """Two applications in the same week (same week_start in apps_per_week)."""
    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [
                row(
                    title="Support Tier 2",
                    company="CompanyA",
                    location="Seattle, WA",
                    url="https://example.com/job4a",
                ),
                row(
                    title="Support Tier 2",
                    company="CompanyB",
                    location="Seattle, WA",
                    url="https://example.com/job4b",
                ),
            ],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 2}))
        ingest_and_init(conn)
        # Both applied on Tuesday & Wednesday of the same week
        sk1 = seen_key("CompanyA", "Support Tier 2", "Seattle, WA")
        sk2 = seen_key("CompanyB", "Support Tier 2", "Seattle, WA")

        applied_at_1 = "2026-07-21T09:00:00"  # Tuesday
        applied_at_2 = "2026-07-22T10:00:00"  # Wednesday (same week, week starts Monday)

        add_state_event(conn, sk1, "status", None, "Applied", applied_at_1, source="backfill")
        add_state_event(conn, sk2, "status", None, "Applied", applied_at_2, source="backfill")

        set_state(conn, "https://example.com/job4a", status="Applied", applied_date=applied_at_1)
        set_state(conn, "https://example.com/job4b", status="Applied", applied_date=applied_at_2)

    expected = {
        "totals": {
            "applied": 2,
            "responded": 0,
            "phone_screen": 0,
            "interview": 0,
            "offer": 0,
            "rejected": 0,
        },
        "response_rate": 0.0,  # No responses yet
        "stage_conversion": [],
        "time_in_stage": [
            {"status": "Applied", "median_days": 1, "n": 2}  # Both ~1 day old
        ],
        "apps_per_week": [
            {"week_start": "2026-07-21", "count": 2}  # Same week (Monday is 2026-07-21)
        ],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="two_apps_same_week", setup=setup, expected=expected)


def case_two_apps_across_weeks():
    """Two applications across different weeks (different week_start values)."""
    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [
                row(
                    title="Backend Engineer",
                    company="StartupX",
                    location="Austin, TX",
                    url="https://example.com/job5a",
                ),
                row(
                    title="Backend Engineer",
                    company="StartupY",
                    location="Austin, TX",
                    url="https://example.com/job5b",
                ),
            ],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 2}))
        ingest_and_init(conn)
        sk1 = seen_key("StartupX", "Backend Engineer", "Austin, TX")
        sk2 = seen_key("StartupY", "Backend Engineer", "Austin, TX")

        applied_at_1 = "2026-07-10T09:00:00"  # Friday of week starting 2026-07-07
        applied_at_2 = "2026-07-21T10:00:00"  # Tuesday of week starting 2026-07-21

        add_state_event(conn, sk1, "status", None, "Applied", applied_at_1, source="backfill")
        add_state_event(conn, sk2, "status", None, "Applied", applied_at_2, source="backfill")

        set_state(conn, "https://example.com/job5a", status="Applied", applied_date=applied_at_1)
        set_state(conn, "https://example.com/job5b", status="Applied", applied_date=applied_at_2)

    expected = {
        "totals": {
            "applied": 2,
            "responded": 0,
            "phone_screen": 0,
            "interview": 0,
            "offer": 0,
            "rejected": 0,
        },
        "response_rate": 0.0,
        "stage_conversion": [],
        "time_in_stage": [
            {"status": "Applied", "median_days": 12, "n": 2}  # Median of (~12, ~1)
        ],
        "apps_per_week": [
            {"week_start": "2026-07-07", "count": 1},
            {"week_start": "2026-07-21", "count": 1},
        ],
        "ghosted": {"applied_no_response_14d": 0},  # Only 12 days for first, 1 day for second
    }
    return SimpleNamespace(name="two_apps_across_weeks", setup=setup, expected=expected)


def case_backdated_applied():
    """Application with backdated applied_date (before current week)."""
    # Compute expected week_start at definition time
    applied_date_obj = datetime.now() - timedelta(days=20)
    applied_date_str = applied_date_obj.isoformat()
    week_start_date = (applied_date_obj.date() - timedelta(days=applied_date_obj.weekday())).isoformat()

    def setup(repo, conn):
        write_csv(
            repo.results / "jobs_scored_2026-07-22.csv",
            [row(
                title="DevOps Engineer",
                company="CloudCorp",
                location="Remote",
                url="https://example.com/job6",
            )],
        )
        (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-22", "kept": 1}))
        ingest_and_init(conn)
        sk = seen_key("CloudCorp", "DevOps Engineer", "Remote")

        # Applied 20 days ago, rejected 10 days ago
        applied_at = applied_date_str
        rejected_at = (datetime.now() - timedelta(days=10)).isoformat()

        add_state_event(conn, sk, "status", None, "Applied", applied_at, source="backfill")
        add_state_event(conn, sk, "status", "Applied", "Rejected", rejected_at, source="patch")

        set_state(
            conn,
            "https://example.com/job6",
            status="Rejected",
            applied_date=applied_at,
            updated_at=rejected_at,
        )

    expected = {
        "totals": {
            "applied": 1,
            "responded": 1,
            "phone_screen": 0,
            "interview": 0,
            "offer": 0,
            "rejected": 1,
        },
        "response_rate": 1.0,
        "stage_conversion": [
            {"from": "Applied", "to": "Rejected", "entered": 1, "advanced": 1, "rate": 1.0}
        ],
        "time_in_stage": [
            {"status": "Applied", "median_days": 10, "n": 1}  # ~10 days from applied to rejected
        ],
        "apps_per_week": [
            # Week containing the backdated applied_at
            {"week_start": week_start_date, "count": 1}
        ],
        "ghosted": {"applied_no_response_14d": 0},
    }
    return SimpleNamespace(name="backdated_applied", setup=setup, expected=expected)


def ingest_and_init(conn):
    """Run a minimal ingest to initialize jobs table."""
    ingest(conn)


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
    """Golden-case test for /api/funnel endpoint (W6 implementation).

    Each case provides:
      - setup(repo, conn): initializes jobs, state_events, job_state for the scenario
      - expected: dict with expected funnel response structure

    Test logic: fetch /api/funnel, validate structure, compare against expected values.
    (Implementation deferred to W6; this is scaffolding only.)
    """
    conn = open_conn(repo)
    case.setup(repo, conn)

    # W6: Implement actual test logic here
    # For now, just verify the setup didn't error and the expected structure is valid.
    assert case.expected is not None
    assert "totals" in case.expected
    assert "response_rate" in case.expected
    assert "stage_conversion" in case.expected
    assert "time_in_stage" in case.expected
    assert "apps_per_week" in case.expected
    assert "ghosted" in case.expected

    conn.close()
    pytest.skip("Funnel endpoint implementation deferred to W6 (Sonnet)")
