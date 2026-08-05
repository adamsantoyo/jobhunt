"""Phase 4, W-4.2: canonical read functions (`canonical_reads.py`).

Every database here is built under `tmp_path` via `backend.tests.test_source_
scheduler_fakes.make_connect` (fresh -> full canonical schema, per db.init_db),
and canonical rows are built through the REAL write path (`runstore.write_
records`, mirroring `test_source_graph.py`'s `deliver`/`record` helpers) so a
posting's identity, aliases, and `run_postings.source_state_json` are exactly
what production would write. Scores are inserted directly (bypassing the real
rubric/scorer) so tests can pin tier/odds/rationale values without a profile.json
dependency; their shape (rationale_json keys, superseded_at semantics) mirrors
both writers `canonical_reads.py`'s docstring cites: `scoring.persist_scores` and
migration 11's legacy backfill.

Nothing here can reach webapp/app.db (repo-root conftest.py fences JOBHUNT_DB).
"""
import json
import sqlite3
import uuid

import pytest

from backend import canonical_reads
from backend.db import connect, init_db
from backend.models import b64_to_url, url_to_b64
from backend.sources import runstore
from backend.sources.contract import NormalizedPosting
from backend.tests.test_source_scheduler_fakes import make_connect

AT = "2026-08-01T12:00:00+00:00"
AT2 = "2026-08-02T12:00:00+00:00"
AT3 = "2026-08-03T12:00:00+00:00"


# --------------------------------------------------------------------------- #
# Fixture helpers
# --------------------------------------------------------------------------- #
@pytest.fixture
def conn(tmp_path):
    c = make_connect(tmp_path)()
    try:
        yield c
    finally:
        c.close()


def record(namespace, *, url, title="Support Engineer", company="Acme Robotics",
           req_id=None, location="San Francisco, CA", posted="2026-07-01", salary=""):
    source_key, _, instance = namespace.partition(":")
    return NormalizedPosting(
        source_key=source_key, instance_key=instance, title=title, company=company,
        url=url, req_id=req_id, location=location, posted_date=posted, salary_text=salary,
    )


def deliver(conn, run_uid, deliveries, *, requested_at, status="succeeded", kind="daily"):
    """One committed run delivering `{namespace: [records]}`."""
    runstore.create_pipeline_run(
        conn, run_uid=run_uid, kind=kind, requested_at=requested_at, started_at=requested_at
    )
    for index, (namespace, records) in enumerate(deliveries.items()):
        attempt_id = f"{run_uid}-{index}"
        runstore.create_source_run(
            conn, source_run_id=attempt_id, run_uid=run_uid, source=namespace, attempt=1
        )
        runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt_id, records=records, recorded_at=requested_at
        )
        runstore.finish_source_run(
            conn, source_run_id=attempt_id, status="succeeded", finished_at=requested_at
        )
    conn.execute(
        "UPDATE pipeline_runs SET status=?, finished_at=? WHERE run_uid=?",
        (status, requested_at, run_uid),
    )
    return run_uid


def posting_id_for_url(conn, url):
    row = conn.execute(
        "SELECT posting_id FROM posting_aliases WHERE url=? ORDER BY valid_from DESC, alias_id DESC LIMIT 1",
        (url,),
    ).fetchone()
    return row["posting_id"] if row else None


def version_id_for(conn, namespace, posting_id):
    row = conn.execute(
        "SELECT posting_version_id FROM posting_versions WHERE posting_id=? AND source=? "
        "ORDER BY observed_at DESC, posting_version_id DESC LIMIT 1",
        (posting_id, namespace),
    ).fetchone()
    return row["posting_version_id"] if row else None


def ensure_profile(conn, profile_version_id="profile-1"):
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions (profile_version_id, content_hash, profile_json, created_at) "
        "VALUES (?,?,?,?)",
        (profile_version_id, profile_version_id, "{}", AT),
    )


def insert_score(conn, *, posting_version_id, posting_id, tier, odds, odds_score=50,
                  why="", flags=None, odds_why="", scorer_hash="scorer-1",
                  profile_version_id="profile-1", superseded_at=None, created_at=AT):
    ensure_profile(conn, profile_version_id)
    score_id = str(uuid.uuid4())
    rationale = {"why": why, "flags": [] if flags is None else flags, "odds_why": odds_why}
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, rationale_json, "
        "created_at, superseded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (score_id, posting_id, posting_version_id, profile_version_id, score_id, scorer_hash,
         tier, odds, odds_score, json.dumps(rationale), created_at, superseded_at),
    )
    return score_id


def insert_description(conn, posting_id, posting_version_id, body, *,
                        fetch_status="available", fetched_at=AT):
    desc_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO descriptions (description_id, posting_id, posting_version_id, provenance_hash, "
        "content_hash, fetch_status, body, fetched_at) VALUES (?,?,?,?,?,?,?,?)",
        (desc_id, posting_id, posting_version_id, desc_id, desc_id, fetch_status, body, fetched_at),
    )


def insert_legacy_current(conn, posting_id, *, title, company, location="SF", source="greenhouse",
                           tier=3, odds="Strong match / Standard", odds_score=70,
                           why="legacy why", flags="", odds_why="legacy odds why", observed_at=AT):
    """A migration-11-style posting version: no run_postings/source_state_json at
    all (the scheduler never touched this posting), tier/odds/why/flags stored
    directly on posting_versions AND mirrored into a score_versions row -- exactly
    what migration 11 writes."""
    version_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, observed_at, observed_at),
    )
    conn.execute(
        "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, version_hash, "
        "observed_at, title, company, location, source, tier, odds, odds_score, odds_why, why, flags, "
        "payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (version_id, posting_id, "legacy-current", version_id, observed_at,
         title, company, location, source, tier, odds, odds_score, odds_why, why, flags, "{}"),
    )
    alias_id = str(uuid.uuid4())
    url = f"https://legacy.example/{posting_id}"
    conn.execute(
        "INSERT INTO posting_aliases (alias_id, posting_id, alias_kind, namespace, value, url, "
        "provenance_json, confidence, valid_from) VALUES (?,?,'url','legacy-url',?,?,?,?,?)",
        (alias_id, posting_id, url, url, "{}", 1.0, observed_at),
    )
    insert_score(
        conn, posting_version_id=version_id, posting_id=posting_id, tier=tier, odds=odds,
        odds_score=odds_score, why=why, flags=flags, odds_why=odds_why,
        scorer_hash="legacy-import", profile_version_id="legacy-import", created_at=observed_at,
    )
    return version_id, url


# --------------------------------------------------------------------------- #
# Canonical version selection: category rank + redirect-awareness
# --------------------------------------------------------------------------- #
def test_canonical_version_prefers_direct_over_aggregator(conn):
    url = "https://acme.example/support-1"
    direct = record("greenhouse:acme", url=url, req_id=None, title="Support Engineer (board)")
    aggregator = record("jobspy:indeed", url=url, req_id=None, title="Support Engineer (mirror)")
    deliver(conn, "run-1", {"greenhouse:acme": [direct], "jobspy:indeed": [aggregator]}, requested_at=AT)
    conn.commit()

    pid = posting_id_for_url(conn, url)
    assert pid is not None
    direct_vid = version_id_for(conn, "greenhouse:acme", pid)
    agg_vid = version_id_for(conn, "jobspy:indeed", pid)
    insert_score(conn, posting_version_id=direct_vid, posting_id=pid, tier=4, odds="Strong match / Standard")
    insert_score(conn, posting_version_id=agg_vid, posting_id=pid, tier=1, odds="Weak match / High competition")
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    job = light[pid]
    assert job["title"] == "Support Engineer (board)"
    assert job["source"] == "greenhouse:acme"
    assert job["tier"] == 4
    assert job["odds"] == "Strong match / Standard"


def test_canonical_version_prefers_manual_over_aggregator(conn):
    url = "https://acme.example/support-2"
    manual = record("manual:me", url=url, req_id=None, title="Support Engineer (manual)")
    aggregator = record("jobspy:indeed", url=url, req_id=None, title="Support Engineer (mirror)")
    deliver(conn, "run-1", {"manual:me": [manual], "jobspy:indeed": [aggregator]}, requested_at=AT)
    conn.commit()

    pid = posting_id_for_url(conn, url)
    manual_vid = version_id_for(conn, "manual:me", pid)
    light = canonical_reads.build_light_rows(conn, [pid])
    assert light[pid]["title"] == "Support Engineer (manual)"
    assert light[pid]["source"] == "manual:me"


def test_canonical_version_follows_redirects(conn):
    """B starts AGGREGATOR-only; A (DIRECT) is separately delivered and then
    redirected into B. B's canonical version must upgrade to A's DIRECT content
    -- the whole point of resolving (graph.py's module docstring)."""
    url_a = "https://boards.example/greenhouse/req-1"
    url_b = "https://jobboard.example/mirror/1"
    rec_a = record("greenhouse:acme", url=url_a, req_id="req-1", title="Support Engineer (direct)")
    rec_b = record("jobspy:indeed", url=url_b, req_id=None, title="Support Engineer (aggregator)")
    deliver(conn, "run-1", {"greenhouse:acme": [rec_a]}, requested_at=AT)
    deliver(conn, "run-2", {"jobspy:indeed": [rec_b]}, requested_at=AT2)
    conn.commit()

    pid_a = posting_id_for_url(conn, url_a)
    pid_b = posting_id_for_url(conn, url_b)
    assert pid_a != pid_b

    conn.execute(
        "INSERT INTO posting_redirects (from_posting_id, to_posting_id, reason, created_at) "
        "VALUES (?,?,?,?)",
        (pid_a, pid_b, "test-merge", AT3),
    )
    conn.commit()

    direct_vid = version_id_for(conn, "greenhouse:acme", pid_a)
    chosen = canonical_reads._canonical_versions(conn, [pid_b])
    assert chosen[pid_b] == ("greenhouse:acme", direct_vid)


# --------------------------------------------------------------------------- #
# Fallback chain: legacy-current, and current-score supersession
# --------------------------------------------------------------------------- #
def test_legacy_current_fallback_when_no_state_map_exists(conn):
    pid = "legacy-posting-1"
    version_id, url = insert_legacy_current(
        conn, pid, title="Legacy Support Role", company="OldCo",
        tier=3, odds="Strong match / Standard", flags="reposted, near-duplicate",
    )
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    job = light[pid]
    assert job["title"] == "Legacy Support Role"
    assert job["company"] == "OldCo"
    assert job["tier"] == 3
    assert job["odds"] == "Strong match / Standard"
    assert job["flags"] == "reposted, near-duplicate"
    assert job["url"] == url
    assert job["posting_id"] == pid


def test_current_score_selection_ignores_superseded_rows(conn):
    """(a) A superseded row that would WIN on plain recency if `superseded_at
    IS NULL` were not enforced: it has a LATER created_at than the real
    current row. Mutation-verified by relaxing that WHERE clause (e.g.
    `superseded_at IS NULL` -> `1=1`) -- tier flips from 5 to 1."""
    url = "https://acme.example/support-3"
    rec = record("greenhouse:acme", url=url, req_id=None, title="Support Engineer")
    deliver(conn, "run-1", {"greenhouse:acme": [rec]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid = version_id_for(conn, "greenhouse:acme", pid)

    insert_score(conn, posting_version_id=vid, posting_id=pid, tier=5, odds="Strong match / Lower bar",
                 created_at=AT, superseded_at=None)
    insert_score(conn, posting_version_id=vid, posting_id=pid, tier=1, odds="Weak match / High competition",
                 created_at=AT3, superseded_at=AT2)
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    assert light[pid]["tier"] == 5
    assert light[pid]["odds"] == "Strong match / Lower bar"


def test_current_score_selection_prefers_freshest_created_at_over_row_order(conn):
    """(b) Two CURRENT rows (both `superseded_at IS NULL` -- legal: supersession
    is keyed by (posting_version_id, profile_version_id, scorer_hash), so a
    version scored under two profiles can have more than one current row) with
    DIFFERENT created_at. The freshest created_at must win. Mutation-verified
    by flipping the comparison direction (`<` instead of `>`) -- tier flips
    from 5 to 1."""
    url = "https://acme.example/support-tiebreak-order"
    rec = record("greenhouse:acme", url=url, req_id=None, title="Support Engineer")
    deliver(conn, "run-1", {"greenhouse:acme": [rec]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid = version_id_for(conn, "greenhouse:acme", pid)

    insert_score(conn, posting_version_id=vid, posting_id=pid, tier=1, odds="Weak match / High competition",
                 created_at=AT, superseded_at=None, profile_version_id="profile-old", scorer_hash="scorer-old")
    insert_score(conn, posting_version_id=vid, posting_id=pid, tier=5, odds="Strong match / Lower bar",
                 created_at=AT3, superseded_at=None, profile_version_id="profile-new", scorer_hash="scorer-new")
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    assert light[pid]["tier"] == 5
    assert light[pid]["odds"] == "Strong match / Lower bar"


def test_current_score_selection_tiebreak_on_equal_created_at_is_deterministic(conn):
    """Two current rows sharing the SAME created_at (the re-currenting case:
    `persist_scores` never touches created_at when it re-currents a reverted
    row), differing only by score_version_id -- the documented secondary key
    (`created_at DESC, score_version_id DESC`). Inserted in ASCENDING id order
    so a bug that fell back to "whichever row SQL returns first" would pick
    the WRONG (lexicographically smaller) row. Mutation-verified by dropping
    the score_version_id tiebreak (compare created_at alone) -- tier flips
    from 5 to 1."""
    url = "https://acme.example/support-tiebreak-id"
    rec = record("greenhouse:acme", url=url, req_id=None, title="Support Engineer")
    deliver(conn, "run-1", {"greenhouse:acme": [rec]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid = version_id_for(conn, "greenhouse:acme", pid)
    ensure_profile(conn, "profile-a")
    ensure_profile(conn, "profile-b")

    rationale = json.dumps({"why": "", "flags": [], "odds_why": ""})
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, rationale_json, "
        "created_at, superseded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        ("score-aaa", pid, vid, "profile-a", "score-aaa", "scorer-a", 1,
         "Weak match / High competition", 20, rationale, AT, None),
    )
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, rationale_json, "
        "created_at, superseded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        ("score-zzz", pid, vid, "profile-b", "score-zzz", "scorer-b", 5,
         "Strong match / Lower bar", 90, rationale, AT, None),
    )
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    assert light[pid]["tier"] == 5
    assert light[pid]["odds"] == "Strong match / Lower bar"


def test_flags_normalizes_list_and_string_rationale_shapes(conn):
    """The live scorer writes rationale.flags as a JSON list; migration 11's
    legacy backfill writes it as an already-joined string. Both must render as
    the same DTO string shape (JobLight.flags: str | None)."""
    url = "https://acme.example/support-4"
    rec = record("greenhouse:acme", url=url, req_id=None)
    deliver(conn, "run-1", {"greenhouse:acme": [rec]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid = version_id_for(conn, "greenhouse:acme", pid)
    insert_score(conn, posting_version_id=vid, posting_id=pid, tier=3, odds="Strong match / Standard",
                 flags=["reposted", "near-duplicate"])
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    assert light[pid]["flags"] == "reposted, near-duplicate"


# --------------------------------------------------------------------------- #
# NULL-column recovery: salary_min/salary_max/first_seen never populated by
# runstore._link_source_version for a scheduler-written posting_versions row
# --------------------------------------------------------------------------- #
def test_salary_and_first_seen_recovered_for_scheduler_written_postings(conn):
    """`runstore._link_source_version` never writes salary_min/salary_max/
    first_seen on a scheduler-written posting_versions row -- build_light_rows
    must recover them at read time (salary parsed from the record's own salary
    text, first_seen COALESCEd with postings.first_seen_at) so the comp
    histogram and first-seen dates are non-empty for a REAL scheduler fixture,
    not only for a hand-populated legacy-current row."""
    url = "https://acme.example/salary-1"
    rec = record("greenhouse:acme", url=url, req_id=None, salary="$120,000 - $150,000")
    deliver(conn, "run-1", {"greenhouse:acme": [rec]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid = version_id_for(conn, "greenhouse:acme", pid)
    insert_score(conn, posting_version_id=vid, posting_id=pid, tier=3, odds="Strong match / Standard")
    conn.commit()

    # Pin the fixture's starting state: the write path really does leave these
    # NULL, so this test would be vacuous against a writer that changed.
    row = conn.execute(
        "SELECT salary_min, salary_max, first_seen FROM posting_versions WHERE posting_version_id=?",
        (vid,),
    ).fetchone()
    assert (row["salary_min"], row["salary_max"], row["first_seen"]) == (None, None, None)

    light = canonical_reads.build_light_rows(conn, [pid])
    job = light[pid]
    assert job["salary_min"] == 120000
    assert job["salary_max"] == 150000
    assert job["first_seen"] is not None

    result = canonical_reads.analytics(conn)
    assert result["comp"]["buckets"], "comp histogram must be non-empty for a scheduler-written fixture"


# --------------------------------------------------------------------------- #
# job_state join: posting_id first, url fallback
# --------------------------------------------------------------------------- #
def test_state_join_prefers_posting_id_over_url(conn):
    url = "https://acme.example/support-5"
    rec = record("greenhouse:acme", url=url, req_id=None)
    deliver(conn, "run-1", {"greenhouse:acme": [rec]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)

    # Two job_state rows: one correctly linked by posting_id (status Applied), one
    # keyed by a URL that happens to equal this posting's display url but is
    # otherwise unrelated (status New) -- posting_id must win.
    conn.execute(
        "INSERT INTO job_state (seen_key, url, posting_id, status, updated_at) VALUES (?,?,?,?,?)",
        ("sk-by-pid", "https://unrelated.example/x", pid, "Applied", AT),
    )
    conn.execute(
        "INSERT INTO job_state (seen_key, url, posting_id, status, updated_at) VALUES (?,?,?,?,?)",
        ("sk-by-url", url, None, "New", AT),
    )
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    assert light[pid]["state"]["status"] == "Applied"


def test_state_join_falls_back_to_url_when_no_posting_id_link(conn):
    url = "https://acme.example/support-6"
    rec = record("greenhouse:acme", url=url, req_id=None)
    deliver(conn, "run-1", {"greenhouse:acme": [rec]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)

    conn.execute(
        "INSERT INTO job_state (seen_key, url, posting_id, status, updated_at) VALUES (?,?,?,?,?)",
        ("sk-url-only", url, None, "Interested", AT),
    )
    conn.commit()

    light = canonical_reads.build_light_rows(conn, [pid])
    assert light[pid]["state"]["status"] == "Interested"


def test_state_by_url_picks_most_recently_updated_row_deterministically(conn):
    """Two job_state rows share a url (no uniqueness constraint on `url`, only
    `seen_key`) -- the winner must be the most-recently-updated row, not
    whichever SQL happens to return first."""
    url = "https://acme.example/dup-state-1"
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) VALUES (?,?,?,?)",
        ("sk-old", url, "New", AT),
    )
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) VALUES (?,?,?,?)",
        ("sk-new", url, "Applied", AT2),
    )
    conn.commit()

    result = canonical_reads._state_by_url(conn, [url])
    assert result[url]["status"] == "Applied"


# --------------------------------------------------------------------------- #
# list_jobs: presence + min_tier
# --------------------------------------------------------------------------- #
def test_list_jobs_present_only_by_default(conn):
    url_present = "https://acme.example/present-1"
    url_absent = "https://acme.example/absent-1"
    deliver(conn, "run-1", {
        "greenhouse:acme": [record("greenhouse:acme", url=url_present, req_id=None)],
    }, requested_at=AT)
    conn.commit()
    pid_present = posting_id_for_url(conn, url_present)

    pid_absent = "absent-posting-1"
    insert_legacy_current(conn, pid_absent, title="Gone Role", company="GoneCo")
    conn.execute("UPDATE postings SET absent_since=? WHERE posting_id=?", (AT2, pid_absent))
    conn.commit()

    result = canonical_reads.list_jobs(conn)
    ids = {j["posting_id"] for j in result["jobs"]}
    assert pid_present in ids
    assert pid_absent not in ids


def test_list_jobs_min_tier_filter(conn):
    pid_low = "low-tier"
    pid_high = "high-tier"
    insert_legacy_current(conn, pid_low, title="Low", company="LowCo", tier=2)
    insert_legacy_current(conn, pid_high, title="High", company="HighCo", tier=4)
    conn.commit()

    result = canonical_reads.list_jobs(conn, min_tier=3)
    ids = {j["posting_id"] for j in result["jobs"]}
    assert pid_high in ids
    assert pid_low not in ids


def test_jobs_visible_after_commit_on_a_fresh_connection(conn, tmp_path):
    """Batch visibility: a plain read on a different connection sees a row once
    its writing transaction commits -- no caching layer, no polling needed."""
    url = "https://acme.example/visible-1"
    deliver(conn, "run-1", {"greenhouse:acme": [record("greenhouse:acme", url=url, req_id=None)]},
            requested_at=AT)
    conn.commit()

    other = connect(conn.execute("PRAGMA database_list").fetchone()["file"])
    try:
        result = canonical_reads.list_jobs(other)
        urls = {j["url"] for j in result["jobs"]}
        assert url in urls
    finally:
        other.close()


# --------------------------------------------------------------------------- #
# job_detail: full_desc, skill hits, 404
# --------------------------------------------------------------------------- #
def test_job_detail_full_desc_and_skill_hits(conn):
    url = "https://acme.example/detail-1"
    deliver(conn, "run-1", {"greenhouse:acme": [record("greenhouse:acme", url=url, req_id=None)]},
            requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid = version_id_for(conn, "greenhouse:acme", pid)
    insert_description(conn, pid, vid, "Looking for SQL and API troubleshooting experience.")
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES ('skills', ?)",
        (json.dumps(["sql", "api", "networking"]),),
    )
    conn.commit()

    detail = canonical_reads.job_detail(conn, url)
    assert detail is not None
    assert detail["full_desc"] == "Looking for SQL and API troubleshooting experience."
    assert set(detail["skill_hits"]) == {"sql", "api"}


def test_job_detail_unknown_url_returns_none(conn):
    assert canonical_reads.job_detail(conn, "https://nowhere.example/x") is None


# --------------------------------------------------------------------------- #
# followups
# --------------------------------------------------------------------------- #
def test_followups_splits_overdue_and_upcoming(conn):
    from datetime import date, timedelta
    today = date.today()
    overdue_date = (today - timedelta(days=5)).isoformat()
    upcoming_date = (today + timedelta(days=3)).isoformat()

    pid_overdue = "fu-overdue"
    pid_upcoming = "fu-upcoming"
    pid_hidden = "fu-hidden"
    insert_legacy_current(conn, pid_overdue, title="Overdue", company="A")
    insert_legacy_current(conn, pid_upcoming, title="Upcoming", company="B")
    insert_legacy_current(conn, pid_hidden, title="Hidden", company="C")

    conn.execute(
        "INSERT INTO job_state (seen_key, posting_id, status, follow_up_date, hidden, updated_at) "
        "VALUES (?,?,?,?,0,?)", ("sk-overdue", pid_overdue, "Applied", overdue_date, AT),
    )
    conn.execute(
        "INSERT INTO job_state (seen_key, posting_id, status, follow_up_date, hidden, updated_at) "
        "VALUES (?,?,?,?,0,?)", ("sk-upcoming", pid_upcoming, "Interview", upcoming_date, AT),
    )
    conn.execute(
        "INSERT INTO job_state (seen_key, posting_id, status, follow_up_date, hidden, updated_at) "
        "VALUES (?,?,?,?,1,?)", ("sk-hidden", pid_hidden, "Applied", overdue_date, AT),
    )
    conn.commit()

    result = canonical_reads.followups(conn)
    overdue_ids = {j["posting_id"] for j in result["overdue"]}
    upcoming_ids = {j["posting_id"] for j in result["upcoming"]}
    assert overdue_ids == {pid_overdue}
    assert upcoming_ids == {pid_upcoming}


# --------------------------------------------------------------------------- #
# changes: new / reposted / tier_changed / disappeared vs baseline
# --------------------------------------------------------------------------- #
def test_changes_new_tier_changed_and_disappeared(conn):
    url_stable = "https://acme.example/chg-stable"
    url_gone = "https://acme.example/chg-gone"
    url_new = "https://acme.example/chg-new"

    deliver(conn, "run-1", {
        "greenhouse:acme": [
            record("greenhouse:acme", url=url_stable, req_id="stable", title="Stable Role"),
            record("greenhouse:acme", url=url_gone, req_id="gone", title="Gone Role"),
        ],
    }, requested_at=AT)
    conn.commit()
    pid_stable = posting_id_for_url(conn, url_stable)
    pid_gone = posting_id_for_url(conn, url_gone)
    vid_stable_1 = version_id_for(conn, "greenhouse:acme", pid_stable)
    vid_gone = version_id_for(conn, "greenhouse:acme", pid_gone)
    insert_score(conn, posting_version_id=vid_stable_1, posting_id=pid_stable, tier=2,
                 odds="Weak match / Standard", created_at=AT)
    insert_score(conn, posting_version_id=vid_gone, posting_id=pid_gone, tier=3,
                 odds="Strong match / Standard", created_at=AT)
    conn.commit()

    # Run 2: stable role's content changes (new version -> tier bump), gone role
    # is not redelivered (absence pass would mark it absent), new role appears.
    deliver(conn, "run-2", {
        "greenhouse:acme": [
            record("greenhouse:acme", url=url_stable, req_id="stable", title="Stable Role (updated)"),
            record("greenhouse:acme", url=url_new, req_id="new", title="New Role"),
        ],
    }, requested_at=AT2)
    conn.execute("UPDATE postings SET absent_since=? WHERE posting_id=?", (AT2, pid_gone))
    conn.execute(
        "UPDATE run_postings SET present=0 WHERE run_uid=? AND posting_id=?", ("run-2", pid_gone)
    )
    conn.commit()

    pid_new = posting_id_for_url(conn, url_new)
    vid_stable_2 = version_id_for(conn, "greenhouse:acme", pid_stable)
    vid_new = version_id_for(conn, "greenhouse:acme", pid_new)
    insert_score(conn, posting_version_id=vid_stable_2, posting_id=pid_stable, tier=5,
                 odds="Strong match / Lower bar", created_at=AT2)
    insert_score(conn, posting_version_id=vid_new, posting_id=pid_new, tier=4,
                 odds="Strong match / Standard", created_at=AT2)
    conn.commit()

    result = canonical_reads.changes(conn)
    new_ids = {j["posting_id"] for j in result["new"]}
    assert new_ids == {pid_new}

    tier_changed_ids = {(tc["job"]["posting_id"], tc["from"], tc["to"]) for tc in result["tier_changed"]}
    assert (pid_stable, 2, 5) in tier_changed_ids

    disappeared_urls = {d["url"] for d in result["disappeared"]}
    assert url_gone in disappeared_urls
    disappeared_entry = next(d for d in result["disappeared"] if d["url"] == url_gone)
    assert disappeared_entry["title"] == "Gone Role"
    assert disappeared_entry["tier"] == 3


def test_changes_since_param_selects_baseline(conn):
    deliver(conn, "run-1", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/since-1", req_id="s1"),
    ]}, requested_at=AT)
    deliver(conn, "run-2", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/since-2", req_id="s2"),
    ]}, requested_at=AT2)
    deliver(conn, "run-3", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/since-3", req_id="s3"),
    ]}, requested_at=AT3)
    conn.commit()

    default = canonical_reads.changes(conn)
    assert default["baseline"] == AT2
    assert default["current"] == AT3

    since_run1 = canonical_reads.changes(conn, since=AT)
    assert since_run1["baseline"] == AT
    assert since_run1["current"] == AT3


def test_changes_since_accepts_run_uid_and_date_prefix(conn):
    """`since` must resolve THREE ways: an exact run_uid, an exact run_date
    (already covered by test_changes_since_param_selects_baseline), and a bare
    YYYY-MM-DD date matched as a PREFIX of a canonical run's full-ISO
    run_date. Mutation-verified by reverting `_resolve_baseline` to a plain
    `r["run_date"] == since` equality check -- both forms below then miss and
    silently fall back to the default (run-2) baseline."""
    deliver(conn, "run-1", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/since2-1", req_id="t1"),
    ]}, requested_at=AT)
    deliver(conn, "run-2", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/since2-2", req_id="t2"),
    ]}, requested_at=AT2)
    deliver(conn, "run-3", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/since2-3", req_id="t3"),
    ]}, requested_at=AT3)
    conn.commit()

    by_uid = canonical_reads.changes(conn, since="run-1")
    assert by_uid["baseline"] == AT
    assert by_uid["current"] == AT3

    by_date_prefix = canonical_reads.changes(conn, since=AT[:10])  # "2026-08-01"
    assert by_date_prefix["baseline"] == AT
    assert by_date_prefix["current"] == AT3


def test_changes_since_date_prefix_picks_newest_run_that_day(conn):
    """When several runs share a calendar day, a bare-date `since` must pick
    the NEWEST of them, not merely any run matching the prefix. A FIFTH run on
    a later, distinct day is included so the expected baseline (day2_late)
    differs from what the "no since" default fallback (the second-to-last
    run overall, day3) would give -- otherwise a broken `since` resolution
    could pass by coincidentally agreeing with the fallback."""
    day1 = "2026-08-10T08:00:00+00:00"
    day2_early = "2026-08-11T08:00:00+00:00"
    day2_late = "2026-08-11T20:00:00+00:00"
    day3 = "2026-08-12T08:00:00+00:00"
    day4 = "2026-08-13T08:00:00+00:00"
    deliver(conn, "run-a", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/pref-1", req_id="pf1"),
    ]}, requested_at=day1)
    deliver(conn, "run-b", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/pref-2", req_id="pf2"),
    ]}, requested_at=day2_early)
    deliver(conn, "run-c", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/pref-3", req_id="pf3"),
    ]}, requested_at=day2_late)
    deliver(conn, "run-d", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/pref-4", req_id="pf4"),
    ]}, requested_at=day3)
    deliver(conn, "run-e", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/pref-5", req_id="pf5"),
    ]}, requested_at=day4)
    conn.commit()

    result = canonical_reads.changes(conn, since="2026-08-11")
    assert result["current"] == day4
    assert result["baseline"] == day2_late


def test_run_membership_excludes_current_only_rows(conn):
    """Migration 13's `membership_kind='current-only'` rows assert "this
    posting's current state as of the backfill", not "this posting was a
    member of this run" -- `_run_membership` must exclude them exactly as
    `compat_job_history` does (migrations.py). Mutation-verified by dropping
    the `membership_kind='snapshot'` filter -- the posting reappears."""
    deliver(conn, "run-1", {"greenhouse:acme": [
        record("greenhouse:acme", url="https://acme.example/mk-1", req_id="mk1"),
    ]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, "https://acme.example/mk-1")

    conn.execute(
        "UPDATE run_postings SET membership_kind='current-only' WHERE run_uid='run-1' AND posting_id=?",
        (pid,),
    )
    conn.commit()

    membership = canonical_reads._run_membership(conn, "run-1")
    assert pid not in membership


def test_changes_reposted_derived_from_returned_at_within_window(conn):
    """No canonical scorer ever emits a `reposted` flag (sources/scoring.py),
    so `changes().reposted` cannot honestly read one from rationale/flags.
    Derived instead from `postings.returned_at` falling strictly inside the
    compared (baseline, current] window. Mutation-verified by reverting to the
    old `"reposted" in job["flags"]` substring check -- the posting drops out
    (insert_score's default flags never contain the word "reposted")."""
    url = "https://acme.example/repost-1"
    deliver(conn, "run-1", {"greenhouse:acme": [
        record("greenhouse:acme", url=url, req_id="rp1"),
    ]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid1 = version_id_for(conn, "greenhouse:acme", pid)
    insert_score(conn, posting_version_id=vid1, posting_id=pid, tier=3, odds="Strong match / Standard",
                 created_at=AT)
    conn.commit()

    # The posting went absent and came back strictly between run-1 (baseline)
    # and run-2 (current) -- Phase 2.4's sticky returned_at marker. Content is
    # unchanged, so run-2 re-links the SAME posting_version_id (and its
    # already-current score) rather than minting a new one.
    conn.execute("UPDATE postings SET returned_at=? WHERE posting_id=?", (AT2, pid))
    deliver(conn, "run-2", {"greenhouse:acme": [
        record("greenhouse:acme", url=url, req_id="rp1"),
    ]}, requested_at=AT3)
    conn.commit()

    result = canonical_reads.changes(conn)
    reposted_ids = {j["posting_id"] for j in result["reposted"]}
    assert pid in reposted_ids


def test_changes_reposted_excludes_returned_at_outside_window(conn):
    """`postings.returned_at` is STICKY (never cleared by a later absence), so
    a posting that returned once long before the baseline run must NOT read as
    reposted in every later `changes()` call. Mutation-verified by dropping
    the upper/lower window bound (treat any non-null returned_at as
    reposted) -- the posting incorrectly appears."""
    url = "https://acme.example/repost-2"
    deliver(conn, "run-1", {"greenhouse:acme": [
        record("greenhouse:acme", url=url, req_id="rp2"),
    ]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid1 = version_id_for(conn, "greenhouse:acme", pid)
    insert_score(conn, posting_version_id=vid1, posting_id=pid, tier=3, odds="Strong match / Standard",
                 created_at=AT)
    conn.commit()

    # Returned_at is from BEFORE the baseline run -- a stale, already-old
    # transition that the (baseline, current] window must not count.
    stale_return = "2020-01-01T00:00:00+00:00"
    conn.execute("UPDATE postings SET returned_at=? WHERE posting_id=?", (stale_return, pid))
    deliver(conn, "run-2", {"greenhouse:acme": [
        record("greenhouse:acme", url=url, req_id="rp2"),
    ]}, requested_at=AT2)
    conn.commit()

    result = canonical_reads.changes(conn)
    reposted_ids = {j["posting_id"] for j in result["reposted"]}
    assert pid not in reposted_ids


def test_changes_with_fewer_than_two_runs_is_empty(conn):
    result = canonical_reads.changes(conn)
    assert result == {
        "baseline": None, "current": None,
        "new": [], "reposted": [], "tier_changed": [], "disappeared": [],
    }


# --------------------------------------------------------------------------- #
# analytics: funnel / tiers / odds / matrix / by_source / comp, legacy odds safe
# --------------------------------------------------------------------------- #
def test_analytics_never_throws_on_legacy_single_word_odds(conn):
    pid_legacy = "an-legacy-odds"
    pid_new = "an-new-odds"
    insert_legacy_current(conn, pid_legacy, title="Legacy", company="A", tier=3, odds="Likely")
    insert_legacy_current(conn, pid_new, title="New", company="B", tier=3, odds="Strong match / Standard")
    conn.commit()

    result = canonical_reads.analytics(conn)
    assert result["odds"]["Standard"] == 1
    assert sum(result["odds"].values()) == 1  # the legacy "Likely" row excluded, not mis-bucketed
    assert result["matrix"]["3"]["Standard"] == 1
    assert result["tiers"]["3"] == 2  # both present jobs still counted by tier regardless of odds format


def test_analytics_funnel_by_source_and_comp_buckets(conn):
    pid_a = "an-a"
    pid_b = "an-b"
    insert_legacy_current(conn, pid_a, title="A", company="Acme", tier=4, odds="Strong match / Standard",
                           source="greenhouse")
    insert_legacy_current(conn, pid_b, title="B", company="Beta", tier=2, odds="Weak match / High competition",
                           source="jobspy:indeed")
    conn.execute(
        "UPDATE posting_versions SET salary_min=90000, salary_max=110000 WHERE posting_id=?", (pid_a,)
    )
    conn.commit()

    result = canonical_reads.analytics(conn)
    by_source = {row["source"]: row for row in result["by_source"]}
    assert by_source["greenhouse"]["kept"] == 1
    assert by_source["jobspy:indeed"]["kept"] == 1
    assert any(b["count"] == 1 for b in result["comp"]["buckets"])
    assert "statuses" in result and isinstance(result["statuses"], list)


def test_analytics_dormant_advanced_status_without_posting_id_not_double_counted(conn):
    """A present posting has TWO job_state rows: one correctly linked by
    posting_id (the one build_light_rows actually joins and counts in the main
    funnel loop) and a second, stale url-only row (posting_id NULL) with a
    DIFFERENT advanced status. The dormant-advanced pass must resolve the
    url-only row back to the same present posting and skip it, not double
    count it. Mutation-verified by disabling the url->posting_id alias
    resolution (`if pid is None and row["url"]:` -> `if False:`) -- the
    url-only row's status leaks into the funnel a second time."""
    url = "https://acme.example/dormant-1"
    deliver(conn, "run-1", {"greenhouse:acme": [
        record("greenhouse:acme", url=url, req_id="d1"),
    ]}, requested_at=AT)
    conn.commit()
    pid = posting_id_for_url(conn, url)
    vid = version_id_for(conn, "greenhouse:acme", pid)
    insert_score(conn, posting_version_id=vid, posting_id=pid, tier=3, odds="Strong match / Standard")

    conn.execute(
        "INSERT INTO job_state (seen_key, url, posting_id, status, updated_at) VALUES (?,?,?,?,?)",
        ("sk-pid", url, pid, "Applied", AT),
    )
    conn.execute(
        "INSERT INTO job_state (seen_key, url, posting_id, status, updated_at) VALUES (?,?,?,?,?)",
        ("sk-url-only", url, None, "Interview", AT),
    )
    conn.commit()

    result = canonical_reads.analytics(conn)
    assert result["funnel"].get("Applied") == 1
    assert "Interview" not in result["funnel"]


# --------------------------------------------------------------------------- #
# freshness
# --------------------------------------------------------------------------- #
def test_freshness_reports_stale_and_healthy_source_instances(conn):
    # Freshness is measured against wall-clock "now" (source_instance_freshness's
    # default `stale_after_seconds=86400`), so this test uses near-now timestamps
    # rather than the module's fixed AT/AT2 constants -- a fixed historical
    # timestamp would read as stale regardless of outcome.
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    recent = now.isoformat()
    older = (now - timedelta(minutes=5)).isoformat()

    deliver(conn, "run-1", {
        "greenhouse:acme": [record("greenhouse:acme", url="https://acme.example/fresh-1", req_id="f1")],
    }, requested_at=recent)
    conn.commit()

    # A second source with only failed attempts -- source_instance_freshness
    # must report it stale without any successful run on file.
    runstore.create_pipeline_run(conn, run_uid="run-fail", kind="daily", requested_at=older, started_at=older)
    runstore.create_source_run(
        conn, source_run_id="run-fail-0", run_uid="run-fail", source="jobspy:indeed", attempt=1
    )
    runstore.finish_source_run(conn, source_run_id="run-fail-0", status="failed", finished_at=older)
    conn.execute("UPDATE pipeline_runs SET status='partial' WHERE run_uid='run-fail'")
    conn.commit()

    result = canonical_reads.freshness(conn)
    by_name = {s["name"]: s for s in result["sources"]}
    assert by_name["greenhouse:acme"]["stale"] is False
    assert by_name["jobspy:indeed"]["stale"] is True
    assert "sweep" in result and "running" in result["sweep"]


def test_freshness_empty_database_never_throws(conn):
    result = canonical_reads.freshness(conn)
    assert result["latest_run"] is None
    assert result["sources"] == []
    assert result["sweep"]["running"] is False
