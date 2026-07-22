"""Forward-only schema migration runner + versioned migrations.

`db.init_db()` first applies the baseline schema (CREATE TABLE IF NOT EXISTS, which
never alters an existing table) and then calls `run_migrations()`, so every process
start converges the schema. Migrations are applied in version order, each in its own
transaction, recorded in `schema_version`. On a *fresh* database the baseline already
reflects the latest schema, so every migration is stamped as satisfied without being
executed (its old->new transform would be a no-op or, worse, misfire on an empty DB).

Design notes:
- The runner takes a live connection plus the on-disk path of that connection (for
  the pre-migration backup). `db.init_db()` derives the real path from the connection
  itself (PRAGMA database_list), never from a global config, so testing a copy never
  risks touching the canonical app.db.
- Migration functions are self-contained: they assume only that the *previous*
  version's schema exists. `state_events` is created with IF NOT EXISTS so migration 1
  is a harmless no-op when the baseline (or a prior run) already created the table.
- Timestamps stored in `state_events.at` are local-naive ISO (matching every other
  timestamp in this codebase: job_state.updated_at, applied_date). The funnel derives
  local weeks from them. `schema_version.applied_at` and backup stamps use UTC — pure
  audit metadata, unrelated to the user-facing timeline.
"""
import os
import shutil
import sqlite3
from datetime import datetime, timezone

from .config import STATUSES

# Single source of truth for the events log DDL. Referenced by both db.init_db()
# (baseline for fresh/existing DBs alike) and migration 1 (for callers that build an
# old-schema DB and invoke run_migrations directly, e.g. tests).
STATE_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS state_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seen_key TEXT NOT NULL,
  url TEXT,
  field TEXT NOT NULL,
  old_value TEXT,
  new_value TEXT,
  at TEXT NOT NULL,            -- ISO8601 local (matches job_state timestamps)
  source TEXT NOT NULL         -- 'patch' | 'quick:<action>' | 'backfill' | 'ingest:picks' | 'migration'
);
CREATE INDEX IF NOT EXISTS idx_events_seen_at ON state_events(seen_key, at);
CREATE INDEX IF NOT EXISTS idx_events_field_at ON state_events(field, at);
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _ensure_schema_version(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version ("
        "version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
    )


def _applied_versions(conn: sqlite3.Connection) -> set:
    return {r["version"] for r in conn.execute("SELECT version FROM schema_version")}


# --------------------------------------------------------------------------- #
# Migrations
# --------------------------------------------------------------------------- #
def _migration_1_state_events(conn: sqlite3.Connection) -> None:
    """Create the append-only state_events log (idempotent)."""
    conn.executescript(STATE_EVENTS_DDL)


def _migration_2_backfill(conn: sqlite3.Connection) -> None:
    """Reconstruct one plausible event stream per existing job_state row.

    'New' is the null origin (no event). An applied_date implies a NULL->'Applied'
    event at that date; a status past Applied additionally implies a later
    'Applied'->status transition at updated_at. A status other than New with no
    applied_date is a single NULL->status event at updated_at. starred=1 is a
    standalone NULL->'1' event at updated_at. Every event carries source 'backfill'.
    """
    applied_idx = STATUSES.index("Applied")
    rows = conn.execute(
        "SELECT seen_key, url, status, applied_date, updated_at, starred FROM job_state"
    ).fetchall()
    for r in rows:
        status = r["status"]
        applied_date = r["applied_date"]
        updated_at = r["updated_at"]
        idx = STATUSES.index(status) if status in STATUSES else None

        evs = []  # (field, old_value, new_value, at)
        if status == "New":
            pass
        elif applied_date and status == "Applied":
            evs.append(("status", None, "Applied", applied_date))
        elif applied_date and idx is not None and idx > applied_idx:
            evs.append(("status", None, "Applied", applied_date))
            evs.append(("status", "Applied", status, updated_at))
        else:
            evs.append(("status", None, status, updated_at))
        if r["starred"] == 1:
            evs.append(("starred", None, "1", updated_at))

        for field, old, new, at in evs:
            conn.execute(
                "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
                "VALUES (?,?,?,?,?,?,'backfill')",
                (r["seen_key"], r["url"], field, old, new, at),
            )


# Ordered (version, name, fn). Append new migrations here; never renumber.
MIGRATIONS = [
    (1, "state_events", _migration_1_state_events),
    (2, "backfill_state_events", _migration_2_backfill),
]


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def _backup(conn: sqlite3.Connection, db_path, version: int):
    """Copy the live DB file to <db_path>.bak.v<version>-<UTCstamp> before the first
    migration touches it. Checkpoints the WAL first so the copied .db file is a
    complete, self-restorable snapshot (recent commits otherwise live only in -wal).
    No-op when db_path is missing/:memory: (nothing on disk to preserve)."""
    if not db_path:
        return None
    path = str(db_path)
    if not os.path.isfile(path):
        return None
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = f"{path}.bak.v{version}-{stamp}"
    shutil.copy2(path, dest)
    return dest


def run_migrations(conn: sqlite3.Connection, db_path, *, dry_run=False, fresh=None):
    """Converge `conn` to the latest schema. Returns the list of (version, name)
    that were (or, for dry_run, would be) applied.

    Fresh DB (baseline already at latest): stamp every migration as satisfied without
    running it, names suffixed '(stamped)'. `fresh` defaults to auto-detection via the
    presence of the `jobs` table; db.init_db() passes it explicitly because it creates
    the baseline before calling here.

    Existing DB: for each unapplied version in order, back up the file before the first
    one, run its fn, record the schema_version row, and commit per migration. Any
    exception rolls that migration back and re-raises. A fully up-to-date DB is a no-op.

    dry_run=True: run every pending migration against the connection, then roll the
    whole thing back (no backup, no commit) and return what would have applied.
    """
    _ensure_schema_version(conn)
    if fresh is None:
        fresh = not _table_exists(conn, "jobs")
    applied = _applied_versions(conn)

    if fresh and not dry_run:
        stamped = []
        for version, name, _fn in MIGRATIONS:
            if version not in applied:
                conn.execute(
                    "INSERT INTO schema_version (version, name, applied_at) VALUES (?,?,?)",
                    (version, f"{name} (stamped)", _utc_now_iso()),
                )
                stamped.append((version, name))
        conn.commit()
        return stamped

    pending = [(v, n, fn) for (v, n, fn) in MIGRATIONS if v not in applied]

    if dry_run:
        # Trial-run everything, then discard so nothing is persisted.
        try:
            for version, name, fn in pending:
                fn(conn)
                conn.execute(
                    "INSERT INTO schema_version (version, name, applied_at) VALUES (?,?,?)",
                    (version, name, _utc_now_iso()),
                )
        finally:
            conn.rollback()
        return [(v, n) for (v, n, _fn) in pending]

    done = []
    backed_up = False
    for version, name, fn in pending:
        if not backed_up:
            _backup(conn, db_path, version)
            backed_up = True
        try:
            fn(conn)
            conn.execute(
                "INSERT INTO schema_version (version, name, applied_at) VALUES (?,?,?)",
                (version, name, _utc_now_iso()),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        done.append((version, name))
    return done
