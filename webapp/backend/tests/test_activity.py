"""GET /api/activity tests (W-A): today's pace, weekly application count, and streak.

Live-write-path cases (applied/snooze/passed today) go through quick_action, like
test_events.py -- exercising the real seeding of state_events, not a hand-rolled
row. Week-boundary and streak cases need backdated history a single test run can't
produce live, so those seed state_events directly (like test_funnel_fixtures.py's
add_state_event helper).
"""
import csv
from datetime import datetime, time, timedelta
from types import SimpleNamespace

import pytest

from backend import config
from backend.db import connect, init_db
from backend.identity import seen_key as compute_seen_key
from backend.ingest import ingest
from backend.models import QuickAction, url_to_b64
from backend.routers.funnel import get_activity
from backend.routers.state import quick_action

NOW = datetime.now()
TODAY = NOW.date()
MONDAY_THIS_WEEK = TODAY - timedelta(days=TODAY.weekday())

COLUMNS = ["tier", "odds", "odds_score", "odds_why", "new", "title", "company", "location",
           "salary", "salary_min", "salary_max", "posted", "first_seen", "remote", "source",
           "also_seen_on", "url", "req_id", "why", "flags", "desc_snippet"]


def row(**kw):
    d = {c: "" for c in COLUMNS}
    d.update({"tier": "3", "odds": "Target", "odds_score": "0", "remote": "False"})
    d.update(kw)
    return d


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    results = root / "results"
    results.mkdir(parents=True)
    monkeypatch.setattr(config, "ROOT", root)
    monkeypatch.setattr(config, "RESULTS", results)
    return SimpleNamespace(root=root, results=results, db=tmp_path / "app.db")


def open_conn(repo):
    conn = connect(repo.db)
    init_db(conn)
    return conn


def seed_job(repo, conn, *, title, company, location, url):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title=title, company=company, location=location, source="greenhouse", url=url),
    ])
    ingest(conn)
    return url


def add_event(conn, sk, field, old_value, new_value, at, source="backfill"):
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
        "VALUES (?, NULL, ?, ?, ?, ?, ?)",
        (sk, field, old_value, new_value, at, source),
    )
    conn.commit()


# --------------------------------------------------------------------------- #
# Empty DB
# --------------------------------------------------------------------------- #

def test_empty_db_all_zeros(repo):
    conn = open_conn(repo)
    result = get_activity(conn)
    assert result == {
        "today": {"applied": 0, "passed": 0, "snoozed": 0, "done": 0},
        "apps_this_week": 0,
        "streak_days": 0,
    }
    conn.close()


# --------------------------------------------------------------------------- #
# Today counters, via the real write path (quick_action)
# --------------------------------------------------------------------------- #

def test_applied_today_counts_applied_and_done(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn, title="Support Engineer", company="Initech", location="SF, CA", url="https://e/1")
    quick_action(url_to_b64(url), QuickAction(action="applied"), conn)

    result = get_activity(conn)
    assert result["today"]["applied"] == 1
    assert result["today"]["done"] == 1
    assert result["today"]["passed"] == 0
    conn.close()


def test_snooze_only_today_does_not_count_as_done(repo):
    """The conflation regression case: snoozing must not count toward done-today."""
    conn = open_conn(repo)
    url = seed_job(repo, conn, title="Analyst", company="WidgetCo", location="NY, NY", url="https://e/2")
    quick_action(url_to_b64(url), QuickAction(action="snooze", days=3), conn)

    result = get_activity(conn)
    assert result["today"]["snoozed"] == 1
    assert result["today"]["done"] == 0
    assert result["today"]["applied"] == 0
    conn.close()


def test_applied_then_passed_same_key_counts_done_once(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn, title="Engineer", company="StartupX", location="Remote", url="https://e/3")
    quick_action(url_to_b64(url), QuickAction(action="applied"), conn)
    quick_action(url_to_b64(url), QuickAction(action="pass"), conn)

    result = get_activity(conn)
    assert result["today"]["applied"] == 1
    assert result["today"]["passed"] == 1
    assert result["today"]["done"] == 1  # same seen_key: union, not sum
    conn.close()


# --------------------------------------------------------------------------- #
# apps_this_week: Monday-start week boundary
# --------------------------------------------------------------------------- #

def test_apps_this_week_counts_monday_not_prior_sunday(repo):
    conn = open_conn(repo)
    url1 = seed_job(repo, conn, title="A", company="LastWeekCo", location="L", url="https://e/4")
    url2 = seed_job(repo, conn, title="B", company="ThisWeekCo", location="L", url="https://e/5")
    sk1 = compute_seen_key("LastWeekCo", "A", "L")
    sk2 = compute_seen_key("ThisWeekCo", "B", "L")

    prior_sunday = datetime.combine(MONDAY_THIS_WEEK - timedelta(days=1), time(10, 0))
    this_monday = datetime.combine(MONDAY_THIS_WEEK, time(9, 0))

    add_event(conn, sk1, "status", "New", "Applied", prior_sunday.isoformat())
    add_event(conn, sk2, "status", "New", "Applied", this_monday.isoformat())

    result = get_activity(conn)
    assert result["apps_this_week"] == 1
    conn.close()


# --------------------------------------------------------------------------- #
# streak_days
# --------------------------------------------------------------------------- #

def test_streak_three_consecutive_days_ending_today(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn, title="A", company="C1", location="L", url="https://e/6")
    sk = compute_seen_key("C1", "A", "L")
    for i in range(3):  # today, yesterday, day before
        d = TODAY - timedelta(days=i)
        add_event(conn, sk, "status", "New", "Applied", datetime.combine(d, time(9, 0)).isoformat())

    result = get_activity(conn)
    assert result["streak_days"] == 3
    conn.close()


def test_streak_ending_yesterday_still_counts_full_run(repo):
    """No event today yet -- today isn't over, so the run ending yesterday still
    counts (doesn't zero mid-day)."""
    conn = open_conn(repo)
    url = seed_job(repo, conn, title="A", company="C2", location="L", url="https://e/7")
    sk = compute_seen_key("C2", "A", "L")
    for i in range(1, 4):  # yesterday, 2 days ago, 3 days ago
        d = TODAY - timedelta(days=i)
        add_event(conn, sk, "status", "New", "Applied", datetime.combine(d, time(9, 0)).isoformat())

    result = get_activity(conn)
    assert result["streak_days"] == 3
    conn.close()


def test_streak_resets_after_gap(repo):
    """Applied today and 2 days ago, but NOT yesterday -- the gap breaks the run;
    streak is just today's 1, not a bridge over the missing day."""
    conn = open_conn(repo)
    url = seed_job(repo, conn, title="A", company="C3", location="L", url="https://e/8")
    sk = compute_seen_key("C3", "A", "L")
    add_event(conn, sk, "status", "New", "Applied", datetime.combine(TODAY, time(9, 0)).isoformat())
    add_event(conn, sk, "status", "Interested", "Applied",
              datetime.combine(TODAY - timedelta(days=2), time(9, 0)).isoformat())

    result = get_activity(conn)
    assert result["streak_days"] == 1
    conn.close()
