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
import os
import sqlite3

import pytest

from backend.config import STATUSES
from backend.db import connect, init_db
from backend.identity import seen_key as compute_seen_key
from backend.migrations import MIGRATIONS, run_migrations

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
                     "starred", "hidden", "contact", "snoozed_until", "applied_via", "updated_at"}

    # No backfill/migration events on an empty, never-migrated-for-real DB.
    assert conn.execute("SELECT COUNT(*) AS c FROM state_events").fetchone()["c"] == 0
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


def test_old_schema_migration_creates_backup_file(old_db):
    path, _keys = old_db
    conn = connect(path)
    run_migrations(conn, str(path))
    backups = glob.glob(str(path) + ".bak.v1-*")
    assert len(backups) == 1
    assert os.path.getsize(backups[0]) > 0
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
