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

# Archive of job_state rows that lose a seen_key collision during migration 3. A full
# copy of the old row (retired review columns included, so nothing is silently lost)
# plus the reason/time it was set aside. Referenced by db.init_db() (baseline for
# fresh/existing DBs) and migration 3 (for the direct-invocation test path).
JOB_STATE_ARCHIVE_DDL = """
CREATE TABLE IF NOT EXISTS job_state_archive (
  seen_key TEXT, url TEXT, status TEXT, notes TEXT,
  follow_up_date TEXT, applied_date TEXT, starred INTEGER, hidden INTEGER,
  contact TEXT, snoozed_until TEXT, needs_review INTEGER, review_reason TEXT,
  review_dismissed INTEGER, updated_at TEXT,
  archived_reason TEXT, archived_at TEXT);
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


def _collision_winner(rows):
    """Pick the surviving row for a colliding seen_key: most-advanced status (highest
    index in STATUSES), tie broken by latest updated_at, then url — fully deterministic
    so a re-derivation on the same data always keeps the same winner."""
    def key(r):
        st = r["status"]
        idx = STATUSES.index(st) if st in STATUSES else -1
        return (idx, r["updated_at"] or "", r["url"] or "")
    return max(rows, key=key)


def _winner_display_url(conn: sqlite3.Connection, seen_key: str, old_url):
    """Winner's display url: the present jobs row for this seen_key if one exists
    (dedupe guarantees <=1), else the old url — with an 'orphaned:' surrogate (the old
    orphan-parking PK, never a real address) collapsed to NULL, and any url a *different*
    present role now owns collapsed to NULL too (so the url-based join in read queries
    never misattributes this dormant state to that other role)."""
    row = conn.execute(
        "SELECT url FROM jobs WHERE seen_key=? AND present=1 LIMIT 1", (seen_key,)
    ).fetchone()
    if row is not None:
        return row["url"]
    if old_url and old_url.startswith("orphaned:"):
        return None
    if old_url and conn.execute(
        "SELECT 1 FROM jobs WHERE url=? AND present=1", (old_url,)
    ).fetchone() is not None:
        return None
    return old_url


def _archive_loser(conn: sqlite3.Connection, row, reason: str, at: str) -> None:
    """Preserve a collision loser verbatim (review columns included) and record a
    migration-source tombstone. The tombstone uses field 'archived' — out of band from
    the status timeline, so the funnel never mistakes it for a real transition."""
    conn.execute(
        "INSERT INTO job_state_archive (seen_key, url, status, notes, follow_up_date, "
        "applied_date, starred, hidden, contact, snoozed_until, needs_review, review_reason, "
        "review_dismissed, updated_at, archived_reason, archived_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (row["seen_key"], row["url"], row["status"], row["notes"], row["follow_up_date"],
         row["applied_date"], row["starred"], row["hidden"], row["contact"], row["snoozed_until"],
         row["needs_review"], row["review_reason"], row["review_dismissed"], row["updated_at"],
         reason, at),
    )
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
        "VALUES (?,?,?,?,?,?, 'migration')",
        (row["seen_key"], row["url"], "archived", row["status"], reason, at),
    )


def _migration_3_rekey_job_state(conn: sqlite3.Connection) -> None:
    """Re-key job_state on seen_key (was url) and drop the retired review columns.

    Rows are grouped by their seen_key COLUMN, which the old orphan-parking never
    rewrote (it only moved url onto an 'orphaned:<seen_key>' surrogate), so parked rows
    rejoin their real identity with no url parsing. Each group keeps one winner (see
    _collision_winner); the rest are copied to job_state_archive and each gets a
    'migration' tombstone event. The winner's url becomes its present-job address (or
    the old value, orphaned surrogate -> NULL). No status, note, or date is dropped:
    every old row is either the winner or archived intact."""
    conn.executescript(JOB_STATE_ARCHIVE_DDL)

    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    if "needs_review" not in cols:
        # Already the new schema (defensive: a re-run via the direct test path). No-op.
        return

    old_rows = conn.execute("SELECT * FROM job_state").fetchall()
    groups: dict = {}
    for r in old_rows:
        groups.setdefault(r["seen_key"], []).append(r)

    conn.execute("ALTER TABLE job_state RENAME TO _job_state_old")
    conn.execute(
        "CREATE TABLE job_state ("
        "seen_key TEXT PRIMARY KEY, url TEXT, "
        "status TEXT NOT NULL DEFAULT 'New', notes TEXT DEFAULT '', "
        "follow_up_date TEXT, applied_date TEXT, starred INTEGER NOT NULL DEFAULT 0, "
        "hidden INTEGER NOT NULL DEFAULT 0, contact TEXT DEFAULT '', snoozed_until TEXT, "
        "updated_at TEXT NOT NULL)"
    )

    at = _utc_now_iso()
    for seen_key, rows in groups.items():
        winner = _collision_winner(rows)
        url = _winner_display_url(conn, seen_key, winner["url"])
        conn.execute(
            "INSERT INTO job_state (seen_key, url, status, notes, follow_up_date, "
            "applied_date, starred, hidden, contact, snoozed_until, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (seen_key, url, winner["status"], winner["notes"], winner["follow_up_date"],
             winner["applied_date"], winner["starred"], winner["hidden"], winner["contact"],
             winner["snoozed_until"], winner["updated_at"]),
        )
        for loser in rows:
            if loser["url"] != winner["url"]:
                _archive_loser(conn, loser, "seen_key collision", at)

    conn.execute("DROP TABLE _job_state_old")


def _migration_4_applied_via(conn: sqlite3.Connection) -> None:
    """Add job_state.applied_via (nullable source picker for how an application was
    submitted). IF NOT EXISTS-style guard via PRAGMA check: a bare ALTER TABLE ADD
    COLUMN errors if the column is already there (e.g. a fresh baseline that already
    includes it, reached via the direct-invocation test path)."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    if "applied_via" not in cols:
        conn.execute("ALTER TABLE job_state ADD COLUMN applied_via TEXT")


# Ordered (version, name, fn). Append new migrations here; never renumber.
MIGRATIONS = [
    (1, "state_events", _migration_1_state_events),
    (2, "backfill_state_events", _migration_2_backfill),
    (3, "rekey_job_state_on_seen_key", _migration_3_rekey_job_state),
    (4, "job_state_applied_via", _migration_4_applied_via),
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


def _dry_run_pending(conn: sqlite3.Connection):
    """Trial-run every pending migration against a private in-memory copy of `conn`'s
    current state, then discard the copy.

    This does NOT run migrations against `conn` and roll back: SQLite auto-commits
    DDL (CREATE TABLE / ALTER TABLE) immediately, ahead of any enclosing transaction,
    regardless of the connection's isolation level -- migrations 1, 3, and 4 are all
    DDL, so a rollback() after running them on the real connection would silently fail
    to undo them, permanently mutating `conn` under a call named dry_run. Backing up
    onto a throwaway :memory: connection and mutating only that copy is the only way
    to make this truly inert."""
    tmp = sqlite3.connect(":memory:")
    tmp.row_factory = sqlite3.Row
    try:
        conn.backup(tmp)
        _ensure_schema_version(tmp)
        applied = _applied_versions(tmp)
        pending = [(v, n, fn) for (v, n, fn) in MIGRATIONS if v not in applied]
        for version, name, fn in pending:
            fn(tmp)
        return [(v, n) for (v, n, _fn) in pending]
    finally:
        tmp.close()


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

    dry_run=True: never touches `conn` (see _dry_run_pending) -- returns what would
    apply without any backup, commit, or persisted side effect on the real connection.
    """
    if dry_run:
        return _dry_run_pending(conn)

    _ensure_schema_version(conn)
    if fresh is None:
        fresh = not _table_exists(conn, "jobs")
    applied = _applied_versions(conn)

    if fresh:
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
