"""state_events write-hook tests (W6).

Exercises events.record_field_events directly (normalization edge cases) plus the
two write paths that call it inside a real transaction: routers.state._apply_state
(patch + quick actions) and ingest.py's picks seeding. Router functions are called
directly against a connection (matching test_ingest.py's style) -- no HTTP layer.
"""
import csv
import json
from types import SimpleNamespace

import pytest

from backend import config
from backend.db import connect, init_db
from backend.events import record_field_events
from backend.identity import seen_key as compute_seen_key
from backend.ingest import ingest
from backend.models import QuickAction, StatePatch
from backend.routers.state import _apply_state, patch_state, quick_action

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


def seed_job(repo, conn, *, title="Support Engineer", company="Initech",
             location="San Francisco, CA", url="https://gh.io/a/1"):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title=title, company=company, location=location, source="greenhouse", url=url),
    ])
    ingest(conn)
    return url


def events_for(conn, sk, field=None):
    q = "SELECT * FROM state_events WHERE seen_key=?"
    params = [sk]
    if field:
        q += " AND field=?"
        params.append(field)
    return conn.execute(q + " ORDER BY id", params).fetchall()


# --------------------------------------------------------------------------- #
# events.record_field_events -- normalization edge cases (unit level, no DB writes
# beyond a bare state_events table).
# --------------------------------------------------------------------------- #

def test_record_field_events_writes_one_event_per_changed_field(repo):
    conn = open_conn(repo)
    n = record_field_events(
        conn, seen_key="sk1", url="https://e/1",
        old={"status": "New", "notes": ""},
        new={"status": "Applied", "notes": "hi", "contact": "x@y.com"},
        source="patch", at="2026-07-01T00:00:00",
    )
    # status and notes changed (New->Applied, ''->hi); contact is new (missing in old
    # == NULL) -> also counts as changed.
    assert n == 3
    rows = conn.execute("SELECT field, old_value, new_value FROM state_events ORDER BY field").fetchall()
    by_field = {r["field"]: (r["old_value"], r["new_value"]) for r in rows}
    assert by_field["status"] == ("New", "Applied")
    assert by_field["notes"] == (None, "hi")
    assert by_field["contact"] == (None, "x@y.com")
    conn.close()


def test_record_field_events_skips_untracked_fields(repo):
    conn = open_conn(repo)
    n = record_field_events(
        conn, seen_key="sk1", url=None,
        old={}, new={"needs_review": True, "review_dismissed": True},
        source="patch", at="2026-07-01T00:00:00",
    )
    assert n == 0
    assert conn.execute("SELECT COUNT(*) AS c FROM state_events").fetchone()["c"] == 0
    conn.close()


def test_record_field_events_empty_string_and_none_are_equivalent(repo):
    conn = open_conn(repo)
    n = record_field_events(
        conn, seen_key="sk1", url=None,
        old={"notes": None}, new={"notes": ""},
        source="patch", at="2026-07-01T00:00:00",
    )
    assert n == 0
    n2 = record_field_events(
        conn, seen_key="sk1", url=None,
        old={"notes": ""}, new={"notes": None},
        source="patch", at="2026-07-01T00:00:00",
    )
    assert n2 == 0
    conn.close()


def test_record_field_events_flag_normalization(repo):
    conn = open_conn(repo)
    # True/1/'1' all normalize to the same '1' -> no event when they "change" between
    # equivalent truthy representations.
    n = record_field_events(
        conn, seen_key="sk1", url=None,
        old={"starred": 1}, new={"starred": True},
        source="patch", at="2026-07-01T00:00:00",
    )
    assert n == 0
    n2 = record_field_events(
        conn, seen_key="sk1", url=None,
        old={"starred": 0}, new={"starred": True},
        source="patch", at="2026-07-01T00:00:00",
    )
    assert n2 == 1
    row_ = conn.execute("SELECT old_value, new_value FROM state_events WHERE field='starred'").fetchone()
    assert (row_["old_value"], row_["new_value"]) == ("0", "1")
    conn.close()


# --------------------------------------------------------------------------- #
# _apply_state (patch path) -- only actually-changed fields produce events, and
# nothing is written when the new value equals the old one.
# --------------------------------------------------------------------------- #

def test_patch_writes_only_changed_field_events(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")

    _apply_state(conn, url, {"notes": "first note", "contact": "recruiter@initech.com"})
    ev = events_for(conn, sk)
    fields = {r["field"] for r in ev}
    assert fields == {"notes", "contact"}
    assert len(ev) == 2

    # A second patch that only touches status must not re-emit notes/contact events.
    _apply_state(conn, url, {"status": "Applied"})
    ev2 = events_for(conn, sk)
    assert len(ev2) == 4  # +status, +applied_date (auto-filled)
    new_fields = {r["field"] for r in ev2[2:]}
    assert new_fields == {"status", "applied_date"}
    conn.close()


def test_patch_unchanged_value_writes_no_event(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")

    _apply_state(conn, url, {"status": "Interested"})
    count_after_first = len(events_for(conn, sk))
    assert count_after_first == 1

    # Re-applying the identical status must not write a second event.
    _apply_state(conn, url, {"status": "Interested"})
    assert len(events_for(conn, sk)) == count_after_first

    # Same for a bool flag re-sent as an equivalent representation.
    _apply_state(conn, url, {"starred": True})
    n = len(events_for(conn, sk))
    _apply_state(conn, url, {"starred": True})
    assert len(events_for(conn, sk)) == n
    conn.close()


def test_patch_applied_status_autofills_applied_date_event_once(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")

    _apply_state(conn, url, {"status": "Applied"})
    ev = events_for(conn, sk, field="applied_date")
    assert len(ev) == 1
    assert ev[0]["old_value"] is None and ev[0]["new_value"] is not None

    # Re-applying status Applied again must NOT re-fire applied_date (COALESCE keeps
    # the existing date, so old==new from the diff's point of view).
    _apply_state(conn, url, {"status": "Applied"})
    assert len(events_for(conn, sk, field="applied_date")) == 1
    conn.close()


def test_patch_via_router_records_source_patch(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")
    from backend.models import url_to_b64

    patch_state(url_to_b64(url), StatePatch(status="Applied"), conn)
    ev = events_for(conn, sk, field="status")
    assert len(ev) == 1
    assert ev[0]["source"] == "patch"
    conn.close()


# --------------------------------------------------------------------------- #
# Quick actions
# --------------------------------------------------------------------------- #

def test_quick_action_applied_records_status_and_applied_date(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")
    from backend.models import url_to_b64

    quick_action(url_to_b64(url), QuickAction(action="applied", applied_via="referral"), conn)
    ev = events_for(conn, sk)
    by_field = {r["field"]: r for r in ev}
    assert by_field["status"]["new_value"] == "Applied"
    assert by_field["status"]["source"] == "quick:applied"
    assert by_field["applied_via"]["new_value"] == "referral"
    assert "applied_date" in by_field
    conn.close()


def test_quick_action_star_and_unstar(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")
    from backend.models import url_to_b64

    quick_action(url_to_b64(url), QuickAction(action="star"), conn)
    ev = events_for(conn, sk, field="starred")
    assert len(ev) == 1 and ev[0]["new_value"] == "1" and ev[0]["source"] == "quick:star"

    quick_action(url_to_b64(url), QuickAction(action="unstar"), conn)
    ev2 = events_for(conn, sk, field="starred")
    assert len(ev2) == 2 and ev2[1]["old_value"] == "1" and ev2[1]["new_value"] == "0"
    assert ev2[1]["source"] == "quick:unstar"
    conn.close()


def test_quick_action_snooze_records_snoozed_until(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")
    from backend.models import url_to_b64

    quick_action(url_to_b64(url), QuickAction(action="snooze", days=5), conn)
    ev = events_for(conn, sk, field="snoozed_until")
    assert len(ev) == 1
    assert ev[0]["source"] == "quick:snooze"
    conn.close()


def test_quick_action_pass_records_status_and_unconditional_reason_event(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")
    from backend.models import url_to_b64

    quick_action(url_to_b64(url), QuickAction(action="pass", reason="not a fit"), conn)
    ev = events_for(conn, sk)
    by_field = {r["field"]: r for r in ev}
    assert by_field["status"]["new_value"] == "Passed"
    # pass_reason is not a job_state column -- it's an unconditional extra event, not
    # diffed against any prior value.
    assert by_field["pass_reason"]["new_value"] == "not a fit"
    assert by_field["pass_reason"]["old_value"] is None
    assert by_field["pass_reason"]["source"] == "quick:pass"
    conn.close()


def test_quick_action_pass_without_reason_writes_no_pass_reason_event(repo):
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")
    from backend.models import url_to_b64

    quick_action(url_to_b64(url), QuickAction(action="pass"), conn)
    ev = events_for(conn, sk, field="pass_reason")
    assert ev == []
    conn.close()


def test_quick_action_pass_reason_repeats_every_call_unconditionally(repo):
    """Unlike job_state-column fields, pass_reason is never diffed -- passing the
    same reason twice writes two events, one per call, since it isn't stored
    anywhere to diff against."""
    conn = open_conn(repo)
    url = seed_job(repo, conn)
    sk = compute_seen_key("Initech", "Support Engineer", "San Francisco, CA")
    from backend.models import url_to_b64

    quick_action(url_to_b64(url), QuickAction(action="pass", reason="stale posting"), conn)
    quick_action(url_to_b64(url), QuickAction(action="pass", reason="stale posting"), conn)
    ev = events_for(conn, sk, field="pass_reason")
    assert len(ev) == 2
    conn.close()


# --------------------------------------------------------------------------- #
# Picks seeding (ingest.py) -- source 'ingest:picks', new-row diff (old is all-NULL).
# --------------------------------------------------------------------------- #

def test_picks_seeding_writes_ingest_picks_events(repo):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support", company="PickCo", location="SF, CA", source="greenhouse", url="https://pick/1"),
    ])
    (repo.root / "picks.json").write_text(json.dumps([
        {"company": "PickCo", "title": "Support", "reason": "great fit", "url": "https://pick/1"},
    ]))
    conn = open_conn(repo)
    ingest(conn)

    sk = compute_seen_key("PickCo", "Support", "SF, CA")
    ev = events_for(conn, sk)
    by_field = {r["field"]: r for r in ev}
    assert by_field["status"]["old_value"] is None
    assert by_field["status"]["new_value"] == "Interested"
    assert by_field["status"]["source"] == "ingest:picks"
    assert by_field["starred"]["new_value"] == "1"
    assert by_field["notes"]["new_value"].startswith("[pick] ")
    conn.close()


def test_picks_seeding_no_event_when_state_already_exists(repo):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support", company="PickCo", location="SF, CA", source="greenhouse", url="https://pick/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    sk = compute_seen_key("PickCo", "Support", "SF, CA")
    _apply_state(conn, "https://pick/1", {"status": "Applied"})
    events_before = len(events_for(conn, sk))

    (repo.root / "picks.json").write_text(json.dumps([
        {"company": "PickCo", "title": "Support", "reason": "great fit", "url": "https://pick/1"},
    ]))
    ingest(conn)  # picks seeding must skip: seen_key already has state

    assert len(events_for(conn, sk)) == events_before
    conn.close()
