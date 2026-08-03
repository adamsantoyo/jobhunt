"""Migration runner tests (W6).

Two DB shapes are exercised:
  - fresh: a brand-new file, only ever touched via db.init_db() -> stamp path.
  - synthetic old-schema: a hand-built pre-Phase-0 DB (job_state keyed on url, with
    the retired needs_review/review_reason/review_dismissed columns and no
    schema_version/state_events/job_state_archive tables at all) -> the real
    migration path (1 -> 2 -> 3 -> 4).

Never touches webapp/app.db; every DB here lives under tmp_path.
"""
import glob
import json
import os
import sqlite3

import pytest

from backend.config import STATUSES
from backend.db import connect, init_db
from backend.identity import seen_key as compute_seen_key
from backend.migrations import MIGRATIONS, _migration_11_legacy_canonical_backfill, run_migrations

CANONICAL_TABLES_BY_VERSION = {
    5: {"profile_versions"},
    6: {"pipeline_runs", "source_runs", "run_events"},
    7: {"postings", "posting_aliases", "posting_redirects", "identity_evidence",
        "legacy_identity_map", "identity_migration_archive"},
    8: {"posting_versions", "descriptions"},
    9: {"score_versions", "llm_reviews", "recommendations", "recommendation_events"},
    10: {"run_postings"},
    12: {"legacy_artifact_imports"},
}
CANONICAL_VIEWS = {"compat_jobs", "compat_runs", "compat_job_history"}

# The pre-Phase-0 schema (job_state keyed on url, review_* columns present, no
# schema_version / state_events / job_state_archive). Captured from the DDL at
# commit 2223f88 (last commit before the Phase 0 migration work started).
OLD_DDL = """
CREATE TABLE jobs (
  url TEXT PRIMARY KEY, seen_key TEXT NOT NULL, tier INTEGER NOT NULL,
  odds TEXT, odds_score INTEGER, odds_why TEXT, is_new INTEGER NOT NULL DEFAULT 0,
  title TEXT, company TEXT, location TEXT, salary TEXT, salary_min INTEGER, salary_max INTEGER,
  posted TEXT, first_seen TEXT, remote INTEGER, source TEXT, also_seen_on TEXT, req_id TEXT,
  why TEXT, flags TEXT, desc_snippet TEXT, full_desc TEXT,
  latest_run TEXT, present INTEGER NOT NULL DEFAULT 1);
CREATE INDEX idx_jobs_seen_key ON jobs(seen_key);

CREATE TABLE job_state (
  url TEXT PRIMARY KEY, seen_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'New', notes TEXT DEFAULT '',
  follow_up_date TEXT, applied_date TEXT, starred INTEGER NOT NULL DEFAULT 0,
  hidden INTEGER NOT NULL DEFAULT 0, contact TEXT DEFAULT '', snoozed_until TEXT,
  needs_review INTEGER NOT NULL DEFAULT 0, review_reason TEXT,
  review_dismissed INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL);
CREATE INDEX idx_state_seen_key ON job_state(seen_key);

CREATE TABLE company_state (
  company TEXT PRIMARY KEY, contact TEXT DEFAULT '', notes TEXT DEFAULT '', updated_at TEXT NOT NULL);

CREATE TABLE runs (
  run_date TEXT PRIMARY KEY, kept INTEGER, new_this_run INTEGER,
  report_json TEXT, source_health_json TEXT, ingested_at TEXT NOT NULL);

CREATE TABLE job_history (
  url TEXT NOT NULL, run_date TEXT NOT NULL, seen_key TEXT NOT NULL,
  tier INTEGER NOT NULL, odds TEXT, present INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (url, run_date));

CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def _job_row(**kw):
    base = dict(url=None, seen_key=None, tier=3, present=1)
    base.update(kw)
    return base


def _insert_job(conn, **kw):
    r = _job_row(**kw)
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier, present) VALUES (:url, :seen_key, :tier, :present)",
        r,
    )


def _insert_old_state(conn, **kw):
    base = dict(
        url=None, seen_key=None, status="New", notes="", follow_up_date=None,
        applied_date=None, starred=0, hidden=0, contact="", snoozed_until=None,
        needs_review=0, review_reason=None, review_dismissed=0, updated_at="2026-01-01T00:00:00",
    )
    base.update(kw)
    conn.execute(
        "INSERT INTO job_state (url, seen_key, status, notes, follow_up_date, applied_date, "
        "starred, hidden, contact, snoozed_until, needs_review, review_reason, review_dismissed, "
        "updated_at) VALUES (:url,:seen_key,:status,:notes,:follow_up_date,:applied_date,:starred,"
        ":hidden,:contact,:snoozed_until,:needs_review,:review_reason,:review_dismissed,:updated_at)",
        base,
    )


def build_old_db(path):
    """A real on-disk sqlite file with the pre-Phase-0 schema, populated with rows
    that exercise every backfill branch plus one seen_key collision."""
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(OLD_DDL)
    conn.commit()

    sk_new = compute_seen_key("NewCo", "Widget Role", "Boise, ID")
    sk_applied = compute_seen_key("AppliedCo", "Support Engineer", "Austin, TX")
    sk_beyond = compute_seen_key("BeyondCo", "Platform Engineer", "Denver, CO")
    sk_other = compute_seen_key("OtherCo", "Sales Engineer", "Chicago, IL")
    sk_starred = compute_seen_key("StarCo", "IT Support", "Reno, NV")
    sk_collide = compute_seen_key("Acme", "Widget Engineer", "Remote")

    # status == 'New', nothing else set -> no events.
    _insert_job(conn, url="https://e/new", seen_key=sk_new)
    _insert_old_state(conn, url="https://e/new", seen_key=sk_new, status="New",
                       updated_at="2026-05-01T00:00:00")

    # applied_date set, status == 'Applied' -> one event.
    _insert_job(conn, url="https://e/applied", seen_key=sk_applied)
    _insert_old_state(conn, url="https://e/applied", seen_key=sk_applied, status="Applied",
                       applied_date="2026-06-01", updated_at="2026-06-01T00:00:00")

    # applied_date set, status beyond Applied -> two events.
    _insert_job(conn, url="https://e/beyond", seen_key=sk_beyond)
    _insert_old_state(conn, url="https://e/beyond", seen_key=sk_beyond, status="Offer",
                       applied_date="2026-05-01", updated_at="2026-05-20T00:00:00")

    # status != 'New', no applied_date -> one event (NULL -> status at updated_at).
    _insert_job(conn, url="https://e/other", seen_key=sk_other)
    _insert_old_state(conn, url="https://e/other", seen_key=sk_other, status="Rejected",
                       updated_at="2026-05-15T00:00:00")

    # starred=1 standalone event, layered on the 'New' no-status-event case.
    _insert_job(conn, url="https://e/star", seen_key=sk_starred)
    _insert_old_state(conn, url="https://e/star", seen_key=sk_starred, status="New", starred=1,
                       updated_at="2026-05-10T00:00:00")

    # seen_key collision: two old rows share sk_collide. One present job exists for
    # this seen_key so the winner's url resolves to it regardless of which row won;
    # the winner is decided by most-advanced status (Interview > Applied).
    _insert_job(conn, url="https://acme.example/widget", seen_key=sk_collide)
    _insert_old_state(conn, url="https://acme.example/widget-old", seen_key=sk_collide,
                       status="Applied", applied_date="2026-07-01", starred=0,
                       updated_at="2026-07-01T00:00:00")
    _insert_old_state(conn, url="orphaned:" + sk_collide, seen_key=sk_collide,
                       status="Interview", starred=1, needs_review=1, review_reason="ambiguous",
                       updated_at="2026-07-10T00:00:00")

    conn.commit()
    conn.close()
    return dict(
        sk_new=sk_new, sk_applied=sk_applied, sk_beyond=sk_beyond, sk_other=sk_other,
        sk_starred=sk_starred, sk_collide=sk_collide,
    )


def build_v4_db(path):
    build_old_db(path)
    conn = connect(path)
    import backend.migrations as migrations_mod
    original = list(migrations_mod.MIGRATIONS)
    migrations_mod.MIGRATIONS[:] = original[:4]
    try:
        run_migrations(conn, str(path))
    finally:
        migrations_mod.MIGRATIONS[:] = original
    conn.close()


def build_v10_db(path):
    build_v4_db(path)
    conn = connect(path)
    import backend.migrations as migrations_mod
    original = list(migrations_mod.MIGRATIONS)
    migrations_mod.MIGRATIONS[:] = original[:10]
    try:
        run_migrations(conn, str(path))
    finally:
        migrations_mod.MIGRATIONS[:] = original
    conn.close()


def build_v11_db(path):
    build_v10_db(path)
    conn = connect(path)
    import backend.migrations as migrations_mod
    original = list(migrations_mod.MIGRATIONS)
    migrations_mod.MIGRATIONS[:] = original[:11]
    try:
        run_migrations(conn, str(path))
    finally:
        migrations_mod.MIGRATIONS[:] = original
    conn.close()


def _objects(conn, object_type):
    return {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type=?", (object_type,)
    )}


def _canonical_structure(conn):
    names = set().union(*CANONICAL_TABLES_BY_VERSION.values()) | CANONICAL_VIEWS
    rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    return {(r["type"], r["name"], r["tbl_name"], r["sql"])
            for r in rows if r["tbl_name"] in names or r["name"] in names}


# --------------------------------------------------------------------------- #
# Fresh-DB stamp path
# --------------------------------------------------------------------------- #

def test_fresh_db_stamps_all_migrations_without_running_them(tmp_path):
    path = tmp_path / "fresh.db"
    conn = connect(path)
    init_db(conn)

    rows = conn.execute("SELECT version, name FROM schema_version ORDER BY version").fetchall()
    assert [r["version"] for r in rows] == [v for (v, _n, _fn) in MIGRATIONS]
    assert all(r["name"].endswith("(stamped)") for r in rows)

    # Baseline already reflects the latest schema: job_state keyed on seen_key, no
    # review_* columns, applied_via present.
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    assert cols == {"seen_key", "url", "status", "notes", "follow_up_date", "applied_date",
                     "starred", "hidden", "contact", "snoozed_until", "applied_via", "updated_at",
                     "posting_id"}
    event_cols = {r["name"] for r in conn.execute("PRAGMA table_info(state_events)")}
    assert "posting_id" in event_cols

    # No backfill/migration events on an empty, never-migrated-for-real DB.
    assert conn.execute("SELECT COUNT(*) AS c FROM state_events").fetchone()["c"] == 0
    assert _objects(conn, "table") >= set().union(*CANONICAL_TABLES_BY_VERSION.values())
    assert _objects(conn, "view") >= CANONICAL_VIEWS
    conn.close()


def test_canonical_migrations_create_expected_objects_version_by_version(tmp_path):
    path = tmp_path / "versions.db"
    build_v4_db(path)
    conn = connect(path)

    for version, _name, migration in MIGRATIONS[4:]:
        conn.execute("BEGIN IMMEDIATE")
        migration(conn)
        conn.commit()
        expected_tables = set().union(*(
            tables for at_version, tables in CANONICAL_TABLES_BY_VERSION.items()
            if at_version <= version
        ))
        assert _objects(conn, "table") >= expected_tables
        if version < 10:
            assert not (_objects(conn, "view") & CANONICAL_VIEWS)
        else:
            assert _objects(conn, "view") >= CANONICAL_VIEWS
    conn.close()


def test_v4_upgrade_matches_fresh_canonical_structure(tmp_path):
    fresh = connect(tmp_path / "fresh_equivalent.db")
    init_db(fresh)

    upgraded_path = tmp_path / "upgraded_equivalent.db"
    build_v4_db(upgraded_path)
    upgraded = connect(upgraded_path)
    assert [v for v, _name in run_migrations(upgraded, str(upgraded_path))] == list(range(5, 13))

    assert _canonical_structure(upgraded) == _canonical_structure(fresh)
    assert upgraded.execute("PRAGMA foreign_key_check").fetchall() == []
    assert fresh.execute("PRAGMA foreign_key_check").fetchall() == []
    fresh.close()
    upgraded.close()


def test_existing_init_db_backs_up_before_canonical_ddl(tmp_path):
    path = tmp_path / "v4_init.db"
    build_v4_db(path)
    conn = connect(path)

    init_db(conn)

    backups = glob.glob(str(path) + ".bak.v5-*")
    assert len(backups) == 1
    backup = sqlite3.connect(backups[0])
    tables = {r[0] for r in backup.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert "profile_versions" not in tables
    assert "pipeline_runs" not in tables
    backup.close()
    assert "profile_versions" in _objects(conn, "table")
    conn.close()


def test_nonempty_database_without_jobs_is_not_stamped_as_fresh(tmp_path):
    path = tmp_path / "nonempty_unknown.db"
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
    raw.commit()
    raw.close()

    conn = connect(path)
    with pytest.raises(RuntimeError, match="required jobs table is missing"):
        init_db(conn)
    assert not glob.glob(str(path) + ".bak.*")
    assert _objects(conn, "table") == {"unrelated"}
    conn.close()


def test_compatibility_views_expose_exact_legacy_columns(tmp_path):
    conn = connect(tmp_path / "compat.db")
    init_db(conn)
    expected = {
        "compat_jobs": ["url", "seen_key", "tier", "odds", "odds_score", "odds_why",
                        "is_new", "title", "company", "location", "salary", "salary_min",
                        "salary_max", "posted", "first_seen", "remote", "source",
                        "also_seen_on", "req_id", "why", "flags", "desc_snippet",
                        "full_desc", "latest_run", "present"],
        "compat_runs": ["run_date", "kept", "new_this_run", "report_json",
                        "source_health_json", "ingested_at"],
        "compat_job_history": ["url", "run_date", "seen_key", "tier", "odds", "present"],
    }
    for view, columns in expected.items():
        assert [r["name"] for r in conn.execute(f"PRAGMA table_info({view})")] == columns
        assert conn.execute(f"SELECT * FROM {view}").fetchall() == []
    conn.close()


def test_fresh_db_rerun_is_a_noop(tmp_path):
    path = tmp_path / "fresh2.db"
    conn = connect(path)
    init_db(conn)
    before = conn.execute("SELECT COUNT(*) AS c FROM schema_version").fetchone()["c"]
    result = run_migrations(conn, str(path))
    assert result == []
    after = conn.execute("SELECT COUNT(*) AS c FROM schema_version").fetchone()["c"]
    assert before == after == len(MIGRATIONS)
    conn.close()


# --------------------------------------------------------------------------- #
# Synthetic old-schema DB -> full migration flow
# --------------------------------------------------------------------------- #

@pytest.fixture
def old_db(tmp_path):
    path = tmp_path / "old.db"
    keys = build_old_db(path)
    return path, keys


def test_old_schema_migration_preserves_row_count_and_statuses(old_db):
    path, keys = old_db
    conn = connect(path)
    old_rows = conn.execute("SELECT seen_key, status FROM job_state").fetchall()
    old_count = len(old_rows)
    old_statuses_by_key = {}
    for r in old_rows:
        old_statuses_by_key.setdefault(r["seen_key"], []).append(r["status"])
    assert old_count == 7  # 5 singles + 2 colliding rows

    run_migrations(conn, str(path))

    new_rows = conn.execute("SELECT seen_key, status FROM job_state").fetchall()
    archived_rows = conn.execute("SELECT seen_key, status FROM job_state_archive").fetchall()

    # Row conservation: every old row is either the surviving winner or archived.
    assert len(new_rows) + len(archived_rows) == old_count
    assert len(new_rows) == 6   # 5 singles + 1 collision winner
    assert len(archived_rows) == 1

    # No status is lost: the winner's status and every archived row's status is one
    # of the statuses that existed pre-migration for that seen_key.
    new_by_key = {r["seen_key"]: r["status"] for r in new_rows}
    for sk, statuses in old_statuses_by_key.items():
        if len(statuses) == 1:
            assert new_by_key[sk] == statuses[0]
    # The collision: winner keeps the more-advanced status (Interview beats Applied).
    assert new_by_key[keys["sk_collide"]] == "Interview"
    archived = archived_rows[0]
    assert archived["seen_key"] == keys["sk_collide"]
    assert archived["status"] == "Applied"
    conn.close()


def test_null_url_collision_losers_are_archived_not_dropped(tmp_path):
    # The old TEXT PRIMARY KEY admits multiple NULL urls (SQLite quirk), so two
    # colliding rows can both have url=NULL. Losers must be excluded by row
    # identity, not url equality — NULL != NULL is false and would silently drop
    # the loser (V1 adversarial finding, Phase 0 verify).
    path = tmp_path / "nullurl.db"
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(OLD_DDL)
    sk = compute_seen_key("NullCo", "Support Engineer", "Remote")
    _insert_old_state(conn, url=None, seen_key=sk, status="Applied",
                      applied_date="2026-07-01", updated_at="2026-07-01T00:00:00")
    _insert_old_state(conn, url=None, seen_key=sk, status="Interview",
                      updated_at="2026-07-10T00:00:00")
    conn.commit()

    run_migrations(conn, str(path))

    live = conn.execute("SELECT * FROM job_state").fetchall()
    archived = conn.execute("SELECT * FROM job_state_archive").fetchall()
    assert len(live) == 1 and live[0]["status"] == "Interview"
    assert len(archived) == 1 and archived[0]["status"] == "Applied"
    conn.close()


def test_old_schema_migration_review_columns_dropped(old_db):
    path, _keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    assert "needs_review" not in cols
    assert "review_reason" not in cols
    assert "review_dismissed" not in cols
    assert "seen_key" in cols and cols_has_pk(conn)
    conn.close()


def cols_has_pk(conn):
    return any(r["pk"] == 1 and r["name"] == "seen_key"
               for r in conn.execute("PRAGMA table_info(job_state)"))


def test_old_schema_migration_winner_url_resolves_to_present_job(old_db):
    path, keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))
    row = conn.execute(
        "SELECT url FROM job_state WHERE seen_key=?", (keys["sk_collide"],)
    ).fetchone()
    # A present job exists for this seen_key -> winner's url is that job's url, not
    # either old row's url (neither the stale one nor the orphaned surrogate).
    assert row["url"] == "https://acme.example/widget"
    conn.close()


def test_old_schema_migration_backfill_event_counts_per_row(old_db):
    path, keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))

    def events_for(sk, source=None):
        q = "SELECT field, old_value, new_value, at FROM state_events WHERE seen_key=?"
        params = [sk]
        if source:
            q += " AND source=?"
            params.append(source)
        return conn.execute(q, params).fetchall()

    # 'New' + nothing else -> zero events.
    assert events_for(keys["sk_new"]) == []

    # applied_date + Applied -> exactly one event, NULL->Applied at applied_date.
    ev = events_for(keys["sk_applied"], "backfill")
    assert len(ev) == 1
    assert ev[0]["field"] == "status" and ev[0]["old_value"] is None
    assert ev[0]["new_value"] == "Applied" and ev[0]["at"] == "2026-06-01"

    # applied_date + status beyond Applied -> two events.
    ev = sorted(events_for(keys["sk_beyond"], "backfill"), key=lambda r: r["at"])
    assert len(ev) == 2
    assert (ev[0]["old_value"], ev[0]["new_value"], ev[0]["at"]) == (None, "Applied", "2026-05-01")
    assert (ev[1]["old_value"], ev[1]["new_value"], ev[1]["at"]) == ("Applied", "Offer", "2026-05-20T00:00:00")

    # status != New, no applied_date -> one event, NULL->status at updated_at.
    ev = events_for(keys["sk_other"], "backfill")
    assert len(ev) == 1
    assert (ev[0]["old_value"], ev[0]["new_value"], ev[0]["at"]) == (None, "Rejected", "2026-05-15T00:00:00")

    # starred=1 on top of 'New' (no status event) -> one standalone starred event.
    ev = events_for(keys["sk_starred"], "backfill")
    assert len(ev) == 1
    assert ev[0]["field"] == "starred"
    assert (ev[0]["old_value"], ev[0]["new_value"]) == (None, "1")

    # Collision: backfill runs BEFORE the rekey (migration 2 before migration 3), so
    # both old rows for sk_collide produced their own backfill events (row-scoped, not
    # seen_key-scoped) -- one status event for the Applied row, one status + one
    # starred event for the Interview row -- plus one 'migration'-source tombstone for
    # the archived loser.
    backfill_ev = events_for(keys["sk_collide"], "backfill")
    assert len(backfill_ev) == 3
    migration_ev = events_for(keys["sk_collide"], "migration")
    assert len(migration_ev) == 1
    assert migration_ev[0]["field"] == "archived"
    assert migration_ev[0]["new_value"] == "seen_key collision"
    conn.close()


def test_old_schema_migration_all_events_source_backfill_or_migration(old_db):
    path, _keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))
    sources = {r["source"] for r in conn.execute("SELECT DISTINCT source FROM state_events")}
    assert sources <= {"backfill", "migration"}
    conn.close()


def test_old_schema_migration_idempotent_second_run(old_db):
    path, _keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))
    state_before = conn.execute("SELECT * FROM job_state ORDER BY seen_key").fetchall()
    archive_before = conn.execute("SELECT * FROM job_state_archive ORDER BY seen_key").fetchall()
    events_before = conn.execute("SELECT COUNT(*) AS c FROM state_events").fetchone()["c"]

    result = run_migrations(conn, str(path))
    assert result == []

    state_after = conn.execute("SELECT * FROM job_state ORDER BY seen_key").fetchall()
    archive_after = conn.execute("SELECT * FROM job_state_archive ORDER BY seen_key").fetchall()
    events_after = conn.execute("SELECT COUNT(*) AS c FROM state_events").fetchone()["c"]

    assert [dict(r) for r in state_before] == [dict(r) for r in state_after]
    assert [dict(r) for r in archive_before] == [dict(r) for r in archive_after]
    assert events_before == events_after
    conn.close()


def test_legacy_runs_backfill_is_deterministic_and_does_not_invent_start_times(tmp_path):
    path = tmp_path / "legacy_runs.db"
    build_v4_db(path)
    conn = connect(path)
    health = {
        "zeta": {"rows": 8, "at": "2026-07-01T12:00:00", "refreshed": True},
        "alpha": {"rows": 3, "status": "done", "started_at": "2026-07-01T11:00:00"},
    }
    conn.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?)",
        ("2026-07-01", 11, 2, '{"ok":true}', json.dumps(health), "2026-07-01T12:05:00"),
    )
    conn.commit()

    run_migrations(conn, str(path))
    first_run = dict(conn.execute("SELECT * FROM pipeline_runs").fetchone())
    first_sources = [dict(r) for r in conn.execute(
        "SELECT * FROM source_runs ORDER BY source"
    )]
    assert first_run["requested_at"] is None
    assert first_run["started_at"] is None
    assert first_run["finished_at"] is None
    assert [r["source"] for r in first_sources] == ["alpha", "zeta"]
    assert first_sources[0]["started_at"] == "2026-07-01T11:00:00"
    assert first_sources[1]["started_at"] is None
    assert first_sources[1]["finished_at"] is None
    assert first_sources[1]["item_count"] == 8

    conn.execute("DELETE FROM schema_version WHERE version >= 6")
    conn.commit()
    run_migrations(conn, str(path))
    assert dict(conn.execute("SELECT * FROM pipeline_runs").fetchone()) == first_run
    assert [dict(r) for r in conn.execute(
        "SELECT * FROM source_runs ORDER BY source"
    )] == first_sources
    conn.close()


def test_malformed_source_health_does_not_abort_run_backfill(tmp_path):
    path = tmp_path / "malformed_health.db"
    build_v4_db(path)
    conn = connect(path)
    conn.executemany(
        "INSERT INTO runs VALUES (?,?,?,?,?,?)",
        [("2026-07-01", 1, 1, None, "not-json", "2026-07-01T00:00:00"),
         ("2026-07-02", 2, 1, None, "[]", "2026-07-02T00:00:00"),
         ("2026-07-03", 3, 1, None, '{"bad":"scalar"}', "2026-07-03T00:00:00")],
    )
    conn.commit()

    run_migrations(conn, str(path))
    assert conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0] == 4
    assert conn.execute("SELECT COUNT(*) FROM source_runs").fetchone()[0] == 0
    conn.close()


def test_nested_source_health_scalars_are_sanitized_not_bound(tmp_path):
    path = tmp_path / "nested_health.db"
    build_v4_db(path)
    conn = connect(path)
    health = json.dumps({"ats": {"rows": [], "deadline_at": {}, "status": ["bad"]}})
    conn.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?)",
        ("2026-07-04", 1, 1, None, health, "2026-07-04T12:00:00"),
    )
    conn.commit()

    run_migrations(conn, str(path))

    row = conn.execute("SELECT * FROM source_runs").fetchone()
    assert row["item_count"] is None
    assert row["deadline_at"] is None
    assert row["status"] == "imported"
    assert json.loads(row["metadata_json"])["rows"] == []
    conn.close()


def test_compat_runs_preserves_legacy_health_and_ingested_at(tmp_path):
    path = tmp_path / "compat_run_values.db"
    build_v4_db(path)
    conn = connect(path)
    health = '{"ats":{"rows":3}}'
    conn.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?)",
        ("2026-07-05", 3, 2, '{"date":"2026-07-05"}', health,
         "2026-07-05T14:30:00"),
    )
    conn.commit()

    run_migrations(conn, str(path))

    row = conn.execute("SELECT * FROM compat_runs WHERE run_date='2026-07-05'").fetchone()
    assert row["source_health_json"] == health
    assert row["ingested_at"] == "2026-07-05T14:30:00"
    conn.close()


def test_canonical_fk_policies_and_uniqueness(tmp_path):
    conn = connect(tmp_path / "constraints.db")
    init_db(conn)
    conn.execute("INSERT INTO postings VALUES ('p1','active','t0','t0',NULL)")
    conn.execute("INSERT INTO postings VALUES ('p2','active','t0','t0',NULL)")
    conn.execute(
        "INSERT INTO posting_aliases VALUES ('a1','p1','url','web','same',NULL,NULL,NULL,NULL,'t0',NULL)"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO posting_aliases VALUES ('a2','p2','url','web','same',NULL,NULL,NULL,NULL,'t0',NULL)"
        )
    conn.execute("UPDATE posting_aliases SET valid_to='t1' WHERE alias_id='a1'")
    conn.execute(
        "INSERT INTO posting_aliases VALUES ('a2','p2','url','web','same',NULL,NULL,NULL,NULL,'t1',NULL)"
    )
    conn.execute("INSERT INTO pipeline_runs (run_uid,kind,status) VALUES ('r1','full','done')")
    conn.execute(
        "INSERT INTO source_runs (source_run_id,run_uid,source,step,attempt,status) "
        "VALUES ('s1','r1','ats','scrape',1,'done')"
    )
    conn.execute(
        "INSERT INTO run_events VALUES ('e1','r1','s1',0,'started','t0',NULL)"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO run_events VALUES ('e2','r1',NULL,0,'duplicate','t1',NULL)"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM postings WHERE posting_id='p2'")
    conn.execute("DELETE FROM pipeline_runs WHERE run_uid='r1'")
    assert conn.execute("SELECT COUNT(*) FROM source_runs").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM run_events").fetchone()[0] == 0
    conn.close()


def test_canonical_composite_foreign_keys_reject_cross_owner_links(tmp_path):
    conn = connect(tmp_path / "composite_fks.db")
    init_db(conn)
    conn.executemany(
        "INSERT INTO pipeline_runs (run_uid,kind,status) VALUES (?,'full','done')",
        [("r1",), ("r2",)],
    )
    conn.execute(
        "INSERT INTO source_runs (source_run_id,run_uid,source,step,attempt,status) "
        "VALUES ('s1','r1','ats','scrape',1,'done')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO run_events VALUES ('e1','r2','s1',0,'bad','t0',NULL)")

    conn.executemany(
        "INSERT INTO postings VALUES (?,'active','t0','t0',NULL)", [("p1",), ("p2",)],
    )
    conn.execute(
        "INSERT INTO posting_versions "
        "(posting_version_id,posting_id,version_hash,observed_at,payload_json) "
        "VALUES ('v2','p2','h2','t0','{}')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO run_postings "
            "(run_uid,posting_id,posting_version_id,present,first_seen_in_run,recorded_at) "
            "VALUES ('r1','p1','v2',1,0,'t0')"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO run_postings "
            "(run_uid,posting_id,source_run_id,present,first_seen_in_run,recorded_at) "
            "VALUES ('r2','p1','s1',1,0,'t0')"
        )
    conn.close()


def test_posting_version_hash_is_scoped_to_posting(tmp_path):
    conn = connect(tmp_path / "version_hash_scope.db")
    init_db(conn)
    conn.execute("INSERT INTO postings VALUES ('p1','active','t0','t0',NULL)")
    conn.execute("INSERT INTO postings VALUES ('p2','active','t0','t0',NULL)")
    insert = (
        "INSERT INTO posting_versions "
        "(posting_version_id, posting_id, version_hash, observed_at, payload_json) "
        "VALUES (?,?,?,?,?)"
    )
    conn.execute(insert, ("v1", "p1", "same-content", "t0", "{}"))
    conn.execute(insert, ("v2", "p2", "same-content", "t0", "{}"))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, ("v3", "p1", "same-content", "t1", "{}"))
    conn.close()


def _build_backfill_v10(path):
    build_v10_db(path)
    conn = connect(path)
    conn.execute("DELETE FROM state_events")
    conn.execute("DELETE FROM job_state")
    conn.execute("DELETE FROM jobs")
    conn.execute("DELETE FROM job_history")

    conn.executemany(
        "INSERT INTO jobs (url, seen_key, tier, odds, odds_score, odds_why, is_new, title, "
        "company, location, salary, salary_min, salary_max, posted, first_seen, remote, source, "
        "also_seen_on, req_id, why, flags, desc_snippet, full_desc, latest_run, present) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("https://shared", "new-key", 4, "High", 88, "strong odds", 1, "New Role",
             "Shared Co", "Remote", "$150k", 140000, 160000, "2026-07-03", "2026-07-03",
             1, "ats", "board", "REQ-2", "excellent fit", '["visa"]', "snippet",
             "Full current description", "2026-07-03", 1),
            ("https://rich", "rich-key", 5, "Medium", 73, "credible", 0, "Platform Lead",
             "Rich Co", "SF", "$200k", 190000, 220000, "2026-07-02", "2026-07-02",
             0, "direct", None, "REQ-9", "rare match", '["onsite"]', "rich snippet",
             "Rich database body", "2026-07-03", 1),
        ],
    )
    conn.executemany(
        "INSERT INTO job_history (url, run_date, seen_key, tier, odds, present) VALUES (?,?,?,?,?,?)",
        [
            ("https://shared", "2026-07-01", "old-key", 3, "Low", 1),
            ("https://shared", "2026-07-03", "new-key", 4, "High", 1),
            ("https://same-a", "2026-07-01", "same-key", 2, "Low", 1),
            ("https://same-b", "2026-07-02", "same-key", 3, "Medium", 0),
            ("https://rich", "2026-07-03", "rich-key", 5, "Medium", 1),
            ("", "2026-07-02", "bad-key", 1, "Low", 1),
        ],
    )
    conn.executemany(
        "INSERT INTO job_state (seen_key, url, status, notes, follow_up_date, applied_date, "
        "starred, hidden, contact, snoozed_until, applied_via, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("same-key", "https://same-b", "Interested", "ambiguous", None, None,
             1, 0, "A", None, None, "2026-07-04T10:00:00"),
            ("new-key", "https://shared", "Applied", "mapped", "2026-07-10", "2026-07-04",
             0, 0, "B", None, "site", "2026-07-04T11:00:00"),
            ("missing-key", None, "New", "orphan", None, None,
             0, 1, "C", "2026-07-08", None, "2026-07-04T12:00:00"),
        ],
    )
    conn.executemany(
        "INSERT INTO state_events (id, seen_key, url, field, old_value, new_value, at, source) "
        "VALUES (?,?,?,?,?,?,?,?)",
        [
            (41, "same-key", "https://same-a", "status", None, "Interested",
             "2026-07-04T10:00:00", "patch"),
            (42, "new-key", "https://shared", "status", "Interested", "Applied",
             "2026-07-04T11:00:00", "quick:applied"),
            (43, "missing-key", None, "hidden", "0", "1",
             "2026-07-04T12:00:00", "patch"),
        ],
    )
    conn.commit()
    conn.close()


def test_v10_upgrade_matches_fresh_posting_link_structure(tmp_path):
    fresh = connect(tmp_path / "fresh_v11.db")
    init_db(fresh)
    path = tmp_path / "v10_structure.db"
    build_v10_db(path)
    upgraded = connect(path)
    assert run_migrations(upgraded, str(path)) == [
        (11, "legacy_canonical_backfill"),
        (12, "legacy_artifact_imports"),
    ]

    for table in ("job_state", "state_events"):
        fresh_columns = [(r["name"], r["type"], r["notnull"], r["pk"])
                         for r in fresh.execute(f"PRAGMA table_info({table})")]
        upgraded_columns = [(r["name"], r["type"], r["notnull"], r["pk"])
                            for r in upgraded.execute(f"PRAGMA table_info({table})")]
        assert upgraded_columns == fresh_columns
        fresh_fks = {(r["from"], r["table"], r["to"], r["on_delete"])
                     for r in fresh.execute(f"PRAGMA foreign_key_list({table})")}
        upgraded_fks = {(r["from"], r["table"], r["to"], r["on_delete"])
                        for r in upgraded.execute(f"PRAGMA foreign_key_list({table})")}
        assert upgraded_fks == fresh_fks
    assert upgraded.execute(
        "SELECT sql FROM sqlite_master WHERE name='uq_job_state_posting_id'"
    ).fetchone()[0] == fresh.execute(
        "SELECT sql FROM sqlite_master WHERE name='uq_job_state_posting_id'"
    ).fetchone()[0]
    upgraded.close()
    fresh.close()


def test_v11_upgrade_matches_fresh_legacy_import_ledger(tmp_path):
    fresh = connect(tmp_path / "fresh_v12.db")
    init_db(fresh)
    path = tmp_path / "v11_structure.db"
    build_v11_db(path)
    upgraded = connect(path)

    assert run_migrations(upgraded, str(path)) == [(12, "legacy_artifact_imports")]
    assert [tuple(r) for r in upgraded.execute("PRAGMA table_info(legacy_artifact_imports)")] == [
        tuple(r) for r in fresh.execute("PRAGMA table_info(legacy_artifact_imports)")
    ]
    upgraded_sql = upgraded.execute(
        "SELECT sql FROM sqlite_master WHERE name='legacy_artifact_imports'"
    ).fetchone()[0]
    fresh_sql = fresh.execute(
        "SELECT sql FROM sqlite_master WHERE name='legacy_artifact_imports'"
    ).fetchone()[0]
    assert upgraded_sql == fresh_sql
    assert upgraded.execute("PRAGMA foreign_key_check").fetchall() == []
    upgraded.close()
    fresh.close()


def test_legacy_canonical_backfill_preserves_lineages_history_content_and_state(tmp_path):
    path = tmp_path / "backfill.db"
    _build_backfill_v10(path)
    conn = connect(path)
    events_before = [dict(r) for r in conn.execute("SELECT * FROM state_events ORDER BY id")]

    run_migrations(conn, str(path))

    mappings = {(r["legacy_identity_value"], r["posting_id"]) for r in conn.execute(
        "SELECT legacy_identity_value, posting_id FROM legacy_identity_map "
        "WHERE legacy_identity_kind='lineage' AND namespace='legacy-db'"
    )}
    assert len(mappings) == 5
    same_ids = {posting_id for value, posting_id in mappings if "same-key" in value}
    assert len(same_ids) == 2

    shared_aliases = conn.execute(
        "SELECT pa.posting_id, pa.valid_to, m.legacy_identity_value "
        "FROM posting_aliases pa JOIN legacy_identity_map m ON m.posting_id=pa.posting_id "
        "WHERE pa.namespace='legacy-url' AND pa.value='https://shared' ORDER BY pa.valid_to"
    ).fetchall()
    assert len(shared_aliases) == 2
    assert sum(r["valid_to"] is None for r in shared_aliases) == 1
    assert "new-key" in next(r["legacy_identity_value"] for r in shared_aliases
                             if r["valid_to"] is None)

    assert conn.execute("SELECT COUNT(*) FROM job_history").fetchone()[0] == 6
    assert conn.execute("SELECT COUNT(*) FROM run_postings").fetchone()[0] == 5
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_migration_archive "
        "WHERE artifact='job_history' AND reason='malformed lineage'"
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM run_postings WHERE first_seen_in_run=1"
    ).fetchone()[0] == 5
    shared_old = conn.execute(
        "SELECT rp.posting_version_id, rp.present, rp.recorded_at FROM run_postings rp "
        "JOIN legacy_identity_map m ON m.posting_id=rp.posting_id "
        "WHERE m.legacy_identity_value LIKE '%old-key%'"
    ).fetchone()
    assert shared_old["posting_version_id"] is not None
    assert (shared_old["present"], shared_old["recorded_at"]) == (1, "2026-07-01")
    historical = conn.execute(
        "SELECT version_kind, tier, odds FROM posting_versions WHERE posting_version_id=?",
        (shared_old["posting_version_id"],),
    ).fetchone()
    assert tuple(historical) == ("legacy-history", 3, "Low")
    assert conn.execute(
        "SELECT title FROM compat_jobs WHERE url='https://shared'"
    ).fetchone()[0] == "New Role"
    current_rows = conn.execute(
        "SELECT url, present FROM compat_jobs ORDER BY url"
    ).fetchall()
    assert [tuple(r) for r in current_rows] == [
        ("https://rich", 1), ("https://shared", 1)
    ]

    rich = conn.execute(
        "SELECT * FROM posting_versions WHERE title='Platform Lead'"
    ).fetchone()
    payload = json.loads(rich["payload_json"])
    assert (rich["tier"], rich["odds"], rich["odds_score"], rich["odds_why"],
            rich["why"], rich["flags"]) == (
                5, "Medium", 73, "credible", "rare match", '["onsite"]'
            )
    assert payload["salary_min"] == 190000
    assert payload["full_desc"] == "Rich database body"
    score = conn.execute(
        "SELECT * FROM score_versions WHERE posting_version_id=?", (rich["posting_version_id"],)
    ).fetchone()
    assert (score["profile_version_id"], score["scorer_hash"], score["tier"],
            score["odds"], score["odds_score"]) == (
                "legacy-import", "legacy-import", 5, "Medium", 73
            )
    assert json.loads(score["rationale_json"])["flags"] == '["onsite"]'
    assert conn.execute(
        "SELECT body FROM descriptions WHERE posting_id=?", (rich["posting_id"],)
    ).fetchone()[0] == "Rich database body"
    assert dict(conn.execute(
        "SELECT profile_version_id, content_hash, profile_json FROM profile_versions "
        "WHERE profile_version_id='legacy-import'"
    ).fetchone()) == {
        "profile_version_id": "legacy-import", "content_hash": "legacy-import", "profile_json": "{}"
    }

    states = {r["seen_key"]: r["posting_id"] for r in conn.execute(
        "SELECT seen_key, posting_id FROM job_state"
    )}
    assert states["new-key"] is not None
    assert states["same-key"] is None
    assert states["missing-key"] is None
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_migration_archive WHERE artifact='job_state'"
    ).fetchone()[0] == 2
    assert conn.execute(
        "SELECT posting_id FROM state_events WHERE id=42"
    ).fetchone()[0] == states["new-key"]
    assert conn.execute("SELECT posting_id FROM state_events WHERE id=41").fetchone()[0] is None
    events_after = [dict(r) for r in conn.execute("SELECT * FROM state_events ORDER BY id")]
    for row in events_after:
        row.pop("posting_id")
    assert events_after == events_before
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_migration_archive WHERE artifact='state_event'"
    ).fetchone()[0] == 2
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()


def test_recycled_url_alias_closes_at_next_first_seen(tmp_path):
    path = tmp_path / "alias_bounds.db"
    _build_backfill_v10(path)
    conn = connect(path)
    run_migrations(conn, str(path))

    rows = conn.execute(
        "SELECT valid_from, valid_to FROM posting_aliases "
        "WHERE namespace='legacy-url' AND value='https://shared' ORDER BY valid_from"
    ).fetchall()
    assert [tuple(r) for r in rows] == [
        ("2026-07-01", "2026-07-03"), ("2026-07-03", None)
    ]
    conn.close()


def test_current_job_without_history_gets_visible_run_membership(tmp_path):
    path = tmp_path / "current_only.db"
    build_v10_db(path)
    conn = connect(path)
    conn.execute(
        "INSERT INTO jobs (url,seen_key,tier,title,latest_run,present) "
        "VALUES ('https://current','current-key',4,'Current Role','2026-07-06',1)"
    )
    conn.commit()

    run_migrations(conn, str(path))

    row = conn.execute("SELECT title, tier FROM compat_jobs WHERE url='https://current'").fetchone()
    assert tuple(row) == ("Current Role", 4)
    assert conn.execute(
        "SELECT COUNT(*) FROM run_postings rp JOIN posting_aliases a "
        "ON a.posting_id=rp.posting_id WHERE a.url='https://current'"
    ).fetchone()[0] == 1
    conn.close()


def test_conflicting_existing_history_mapping_rolls_back(tmp_path):
    path = tmp_path / "history_conflict.db"
    _build_backfill_v10(path)
    conn = connect(path)
    import backend.migrations as migrations_mod

    real_insert = migrations_mod._legacy_uid
    target_run = real_insert("pipeline_run", "2026-07-01")
    target_posting = migrations_mod._lineage_posting_id("https://shared", "old-key")
    conn.execute(
        "INSERT INTO postings VALUES (?, 'active', 't0', 't0', NULL)", (target_posting,)
    )
    conn.execute(
        "INSERT INTO pipeline_runs (run_uid,kind,status,legacy_run_date) "
        "VALUES (?,'imported','imported','2026-07-01')",
        (target_run,),
    )
    conn.execute(
        "INSERT INTO run_postings "
        "(run_uid,posting_id,present,first_seen_in_run,recorded_at) VALUES (?,?,0,0,'wrong')",
        (target_run, target_posting),
    )
    conn.commit()

    with pytest.raises(RuntimeError, match="mapping conflict"):
        run_migrations(conn, str(path))
    assert 11 not in {r["version"] for r in conn.execute("SELECT version FROM schema_version")}
    conn.close()


def test_binary_state_archive_payload_is_reversible(tmp_path):
    path = tmp_path / "binary_archive.db"
    build_v10_db(path)
    conn = connect(path)
    conn.execute(
        "INSERT INTO job_state (seen_key,notes,updated_at) VALUES ('missing',?,'t0')",
        (sqlite3.Binary(b"\x00\xff"),),
    )
    conn.commit()

    run_migrations(conn, str(path))

    payload = json.loads(conn.execute(
        "SELECT payload_json FROM identity_migration_archive "
        "WHERE artifact='job_state'"
    ).fetchone()[0])
    assert payload["notes"] == {"$type": "bytes", "base64": "AP8="}
    conn.close()


def test_migration_11_direct_rerun_is_idempotent(tmp_path):
    path = tmp_path / "rerun_v11.db"
    _build_backfill_v10(path)
    conn = connect(path)
    run_migrations(conn, str(path))
    tables = ("postings", "posting_aliases", "identity_evidence", "legacy_identity_map",
              "identity_migration_archive", "posting_versions", "descriptions",
              "score_versions", "pipeline_runs", "run_postings")
    before = {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
              for table in tables}
    state_before = [dict(r) for r in conn.execute("SELECT * FROM job_state ORDER BY seen_key")]
    events_before = [dict(r) for r in conn.execute("SELECT * FROM state_events ORDER BY id")]

    conn.execute("BEGIN IMMEDIATE")
    _migration_11_legacy_canonical_backfill(conn)
    conn.commit()

    assert {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables} == before
    assert [dict(r) for r in conn.execute("SELECT * FROM job_state ORDER BY seen_key")] == state_before
    assert [dict(r) for r in conn.execute("SELECT * FROM state_events ORDER BY id")] == events_before
    conn.close()


def test_url_alias_observation_tie_is_deterministic_and_archived(tmp_path):
    path = tmp_path / "alias_tie.db"
    _build_backfill_v10(path)
    conn = connect(path)
    conn.execute(
        "DELETE FROM job_history WHERE url='https://shared' AND seen_key='new-key'"
    )
    conn.execute(
        "UPDATE jobs SET first_seen='2026-07-01', latest_run='2026-07-01' "
        "WHERE url='https://shared'"
    )
    conn.commit()

    run_migrations(conn, str(path))

    aliases = conn.execute(
        "SELECT posting_id, valid_to FROM posting_aliases "
        "WHERE namespace='legacy-url' AND value='https://shared' ORDER BY posting_id"
    ).fetchall()
    assert len(aliases) == 2
    active_before = next(r["posting_id"] for r in aliases if r["valid_to"] is None)
    archive = conn.execute(
        "SELECT candidate_posting_ids_json FROM identity_migration_archive "
        "WHERE artifact='url_alias_ambiguity' AND locator='https://shared'"
    ).fetchone()
    assert json.loads(archive["candidate_posting_ids_json"]) == sorted(
        r["posting_id"] for r in aliases
    )

    conn.execute("BEGIN IMMEDIATE")
    _migration_11_legacy_canonical_backfill(conn)
    conn.commit()
    active_after = conn.execute(
        "SELECT posting_id FROM posting_aliases WHERE namespace='legacy-url' "
        "AND value='https://shared' AND valid_to IS NULL"
    ).fetchone()[0]
    assert active_after == active_before
    conn.close()


def test_migration_11_failure_rolls_back_columns_and_backfill(tmp_path):
    path = tmp_path / "rollback_v11.db"
    build_v10_db(path)
    conn = connect(path)
    conn.execute(
        "CREATE TRIGGER fail_lineage BEFORE INSERT ON legacy_identity_map "
        "BEGIN SELECT RAISE(ABORT, 'forced lineage failure'); END"
    )
    conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="forced lineage failure"):
        run_migrations(conn, str(path))

    assert 11 not in {r["version"] for r in conn.execute("SELECT version FROM schema_version")}
    assert "posting_id" not in {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    assert "posting_id" not in {r["name"] for r in conn.execute("PRAGMA table_info(state_events)")}
    assert conn.execute("SELECT COUNT(*) FROM postings").fetchone()[0] == 0
    conn.close()


def test_state_posting_fk_and_partial_uniqueness(tmp_path):
    conn = connect(tmp_path / "state_constraints.db")
    init_db(conn)
    conn.executemany(
        "INSERT INTO postings VALUES (?,'active','t0','t0',NULL)", [("p1",), ("p2",)]
    )
    conn.execute(
        "INSERT INTO job_state (seen_key, updated_at, posting_id) VALUES ('s1','t0','p1')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO job_state (seen_key, updated_at, posting_id) VALUES ('s2','t0','p1')"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO state_events (seen_key,field,at,source,posting_id) "
            "VALUES ('s1','status','t0','patch','missing')"
        )
    conn.execute(
        "INSERT INTO state_events (seen_key,field,at,source,posting_id) "
        "VALUES ('s1','status','t0','patch','p1')"
    )
    conn.execute(
        "INSERT INTO state_events (seen_key,field,at,source,posting_id) "
        "VALUES ('s1','notes','t1','patch','p1')"
    )
    assert conn.execute("SELECT COUNT(*) FROM state_events WHERE posting_id='p1'").fetchone()[0] == 2
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM postings WHERE posting_id='p1'")
    conn.close()


def test_old_schema_migration_creates_backup_file(old_db):
    path, _keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))
    backups = glob.glob(str(path) + ".bak.v1-*")
    assert len(backups) == 1
    assert os.path.getsize(backups[0]) > 0
    conn.close()


def test_online_backup_includes_committed_wal_rows(old_db):
    path, _keys = old_db
    conn = connect(path)
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('wal-proof', 'present')")
    conn.commit()

    run_migrations(conn, str(path))

    backup_path = glob.glob(str(path) + ".bak.v1-*")[0]
    restored = sqlite3.connect(backup_path)
    assert restored.execute(
        "SELECT value FROM app_settings WHERE key='wal-proof'"
    ).fetchone()[0] == "present"
    tables = {r[0] for r in restored.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert "schema_version" not in tables, "backup must precede migration bookkeeping"
    restored.close()
    conn.close()


def test_old_schema_migration_no_backup_on_already_migrated_rerun(old_db):
    path, _keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))
    first_backups = set(glob.glob(str(path) + ".bak.*"))
    run_migrations(conn, str(path))
    second_backups = set(glob.glob(str(path) + ".bak.*"))
    assert first_backups == second_backups  # no new backup on a no-op run
    conn.close()


# --------------------------------------------------------------------------- #
# Dry run
# --------------------------------------------------------------------------- #

def test_dry_run_reports_pending_without_persisting_anything(old_db):
    path, _keys = old_db
    conn = connect(path)

    result = run_migrations(conn, str(path), dry_run=True)
    assert [v for (v, _n) in result] == [v for (v, _n, _fn) in MIGRATIONS]

    # Nothing touched the real connection: no schema_version table, job_state still
    # keyed on url with the old review_* columns, no state_events/job_state_archive,
    # no backup file written.
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "schema_version" not in tables
    assert "state_events" not in tables
    assert "job_state_archive" not in tables
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    assert "needs_review" in cols
    row_count = conn.execute("SELECT COUNT(*) AS c FROM job_state").fetchone()["c"]
    assert row_count == 7  # untouched, still the old row-per-url shape
    assert glob.glob(str(path) + ".bak.*") == []

    # A real run afterward still sees all four as pending (dry run changed nothing).
    real_result = run_migrations(conn, str(path))
    assert [v for (v, _n) in real_result] == [v for (v, _n, _fn) in MIGRATIONS]
    conn.close()


def test_dry_run_on_fresh_db_does_not_mutate_it(tmp_path):
    path = tmp_path / "fresh_dry.db"
    conn = connect(path)
    init_db(conn)
    before = conn.execute("SELECT COUNT(*) AS c FROM schema_version").fetchone()["c"]

    run_migrations(conn, str(path), dry_run=True)

    after = conn.execute("SELECT COUNT(*) AS c FROM schema_version").fetchone()["c"]
    assert before == after == len(MIGRATIONS)  # already-stamped rows untouched
    conn.close()


def test_dry_run_validates_up_to_date_database_with_no_pending_migrations(tmp_path):
    path = tmp_path / "up_to_date_invalid_fk.db"
    conn = connect(path)
    init_db(conn)
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
    conn.execute("CREATE TABLE child (parent_id INTEGER REFERENCES parent(id))")
    conn.commit()
    conn.close()

    raw = sqlite3.connect(path)
    raw.execute("PRAGMA foreign_keys=OFF")
    raw.execute("INSERT INTO child (parent_id) VALUES (99)")
    raw.commit()
    raw.close()

    conn = connect(path)
    with pytest.raises(RuntimeError, match="foreign-key check failed"):
        run_migrations(conn, str(path), dry_run=True)
    assert conn.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == len(MIGRATIONS)
    conn.close()


def test_migration_refuses_connection_with_active_transaction(old_db):
    path, _keys = old_db
    conn = connect(path)
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('pending', 'write')")

    with pytest.raises(RuntimeError, match="no active transaction"):
        run_migrations(conn, str(path))

    assert glob.glob(str(path) + ".bak.*") == []
    conn.rollback()
    conn.close()


# --------------------------------------------------------------------------- #
# Migration ordering / bookkeeping
# --------------------------------------------------------------------------- #

def test_migrations_are_ordered_and_never_renumbered():
    versions = [v for (v, _n, _fn) in MIGRATIONS]
    assert versions == sorted(versions)
    assert versions == list(range(1, len(MIGRATIONS) + 1))


def test_kill_mid_migration_leaves_a_restorable_backup(old_db):
    """A migration that raises mid-way rolls back its own transaction, but the
    pre-migration backup taken before the first pending migration still exists and is
    a valid, restorable snapshot of the pre-migration state."""
    path, keys = old_db
    conn = connect(path)

    import backend.migrations as migrations_mod
    real_migration_3 = migrations_mod._migration_3_rekey_job_state

    def boom(conn):
        raise RuntimeError("simulated failure mid-migration")

    orig = list(migrations_mod.MIGRATIONS)
    migrations_mod.MIGRATIONS[:] = [
        (v, n, boom if v == 3 else fn) for (v, n, fn) in orig
    ]
    try:
        with pytest.raises(RuntimeError):
            run_migrations(conn, str(path))
    finally:
        migrations_mod.MIGRATIONS[:] = orig
    conn.close()

    # Migrations 1-2 committed; 3 rolled back and raised; 4 never ran.
    conn2 = connect(path)
    versions = {r["version"] for r in conn2.execute("SELECT version FROM schema_version")}
    assert versions == {1, 2}
    # job_state is still old-schema (migration 3 never completed).
    cols = {r["name"] for r in conn2.execute("PRAGMA table_info(job_state)")}
    assert "needs_review" in cols
    conn2.close()

    # The backup taken before migration 1 is a valid, restorable sqlite file with the
    # full pre-migration state (all 7 old rows, review columns intact).
    backups = glob.glob(str(path) + ".bak.v1-*")
    assert len(backups) == 1
    restored = sqlite3.connect(backups[0])
    restored.row_factory = sqlite3.Row
    restored_cols = {r["name"] for r in restored.execute("PRAGMA table_info(job_state)")}
    assert "needs_review" in restored_cols
    count = restored.execute("SELECT COUNT(*) AS c FROM job_state").fetchone()["c"]
    assert count == 7
    restored.close()


def test_ddl_from_failed_migration_rolls_back(old_db):
    path, _keys = old_db
    conn = connect(path)

    import backend.migrations as migrations_mod

    def create_then_boom(conn):
        conn.execute("CREATE TABLE must_rollback (id INTEGER PRIMARY KEY)")
        raise RuntimeError("after ddl")

    orig = list(migrations_mod.MIGRATIONS)
    migrations_mod.MIGRATIONS.append((13, "transactional_ddl_probe", create_then_boom))
    try:
        with pytest.raises(RuntimeError, match="after ddl"):
            run_migrations(conn, str(path))
    finally:
        migrations_mod.MIGRATIONS[:] = orig

    assert not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='must_rollback'"
    ).fetchone()
    versions = {r["version"] for r in conn.execute("SELECT version FROM schema_version")}
    assert versions == set(range(1, 13))
    conn.close()


def test_backup_validation_rejects_foreign_key_violation_before_migration(tmp_path):
    path = tmp_path / "invalid_fk.db"
    raw = sqlite3.connect(path)
    raw.executescript(OLD_DDL)
    raw.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
    raw.execute("CREATE TABLE child (parent_id INTEGER REFERENCES parent(id))")
    raw.execute("INSERT INTO child (parent_id) VALUES (99)")
    raw.commit()
    raw.close()

    conn = connect(path)
    with pytest.raises(RuntimeError, match="foreign-key check failed"):
        run_migrations(conn, str(path))

    assert not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    assert glob.glob(str(path) + ".bak.*") == []
    conn.close()


def test_backup_failure_aborts_before_schema_mutation(old_db, monkeypatch):
    path, _keys = old_db
    conn = connect(path)

    import backend.migrations as migrations_mod

    def backup_boom(*_args, **_kwargs):
        raise OSError("backup unavailable")

    monkeypatch.setattr(migrations_mod, "_backup", backup_boom)
    with pytest.raises(OSError, match="backup unavailable"):
        run_migrations(conn, str(path))

    assert not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    conn.close()
