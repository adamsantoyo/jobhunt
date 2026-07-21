"""SQLite connection factory + schema DDL.

The `jobs`/`runs`/`job_history` tables are a rebuildable cache of pipeline output.
`job_state`/`company_state`/`app_settings` are user-owned and never clobbered.

WAL mode + a fresh connection per request keeps concurrent reads cheap and avoids
cross-thread connection sharing (FastAPI runs sync handlers in a threadpool).
"""
import sqlite3

from . import config

DDL = """
CREATE TABLE IF NOT EXISTS jobs (
  url TEXT PRIMARY KEY, seen_key TEXT NOT NULL, tier INTEGER NOT NULL,
  odds TEXT, odds_score INTEGER, odds_why TEXT, is_new INTEGER NOT NULL DEFAULT 0,
  title TEXT, company TEXT, location TEXT, salary TEXT, salary_min INTEGER, salary_max INTEGER,
  posted TEXT, first_seen TEXT, remote INTEGER, source TEXT, also_seen_on TEXT, req_id TEXT,
  why TEXT, flags TEXT, desc_snippet TEXT, full_desc TEXT,
  latest_run TEXT, present INTEGER NOT NULL DEFAULT 1);
CREATE INDEX IF NOT EXISTS idx_jobs_seen_key ON jobs(seen_key);
CREATE INDEX IF NOT EXISTS idx_jobs_tier_odds ON jobs(tier, odds);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company);

CREATE TABLE IF NOT EXISTS job_state (
  url TEXT PRIMARY KEY, seen_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'New', notes TEXT DEFAULT '',
  follow_up_date TEXT, applied_date TEXT, starred INTEGER NOT NULL DEFAULT 0,
  hidden INTEGER NOT NULL DEFAULT 0, contact TEXT DEFAULT '', snoozed_until TEXT,
  needs_review INTEGER NOT NULL DEFAULT 0, review_reason TEXT,
  review_dismissed INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_state_seen_key ON job_state(seen_key);

CREATE TABLE IF NOT EXISTS company_state (
  company TEXT PRIMARY KEY, contact TEXT DEFAULT '', notes TEXT DEFAULT '', updated_at TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS runs (
  run_date TEXT PRIMARY KEY, kept INTEGER, new_this_run INTEGER,
  report_json TEXT, source_health_json TEXT, ingested_at TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS job_history (
  url TEXT NOT NULL, run_date TEXT NOT NULL, seen_key TEXT NOT NULL,
  tier INTEGER NOT NULL, odds TEXT, present INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (url, run_date));
CREATE INDEX IF NOT EXISTS idx_hist_run ON job_history(run_date);
CREATE INDEX IF NOT EXISTS idx_hist_seen ON job_history(seen_key);

CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def connect(db_path=None):
    """Open a new connection with the required PRAGMAs. Row factory = sqlite3.Row."""
    path = str(db_path or config.DB_PATH)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_column(conn, table, column, decl):
    """Add a column to an existing table if missing (migration for populated DBs;
    CREATE IF NOT EXISTS alone never alters an existing table)."""
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db(conn):
    """Create the schema if it does not exist (idempotent), then run migrations."""
    conn.executescript(DDL)
    _ensure_column(conn, "job_state", "review_dismissed", "INTEGER NOT NULL DEFAULT 0")
    conn.commit()


def get_db():
    """FastAPI dependency: yields a request-scoped connection, always closed."""
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()
