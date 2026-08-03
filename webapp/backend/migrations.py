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
import base64
import hashlib
import json
import os
import sqlite3
import uuid
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

STATE_EVENTS_CANONICAL_DDL = STATE_EVENTS_DDL.replace(
    "  source TEXT NOT NULL         -- 'patch' | 'quick:<action>' | 'backfill' | 'ingest:picks' | 'migration'\n",
    "  source TEXT NOT NULL,        -- 'patch' | 'quick:<action>' | 'backfill' | 'ingest:picks' | 'migration'\n"
    "  posting_id TEXT REFERENCES postings(posting_id) ON DELETE RESTRICT\n",
)

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

PROFILE_VERSIONS_DDL = """
CREATE TABLE IF NOT EXISTS profile_versions (
    profile_version_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL UNIQUE,
    profile_json TEXT NOT NULL,
    rubric_hash TEXT,
    created_at TEXT NOT NULL
);
"""

RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_uid TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    profile_version_id TEXT REFERENCES profile_versions(profile_version_id) ON DELETE RESTRICT,
    legacy_run_date TEXT UNIQUE,
    legacy_ingested_at TEXT,
    source_health_json TEXT,
    requested_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    trigger TEXT,
    config_hash TEXT,
    scorer_hash TEXT,
    code_hash TEXT,
    kept_count INTEGER,
    new_count INTEGER,
    aggregate_report_json TEXT,
    error_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_requested
    ON pipeline_runs(status, requested_at);

CREATE TABLE IF NOT EXISTS source_runs (
    source_run_id TEXT PRIMARY KEY,
    run_uid TEXT NOT NULL REFERENCES pipeline_runs(run_uid) ON DELETE CASCADE,
    source TEXT NOT NULL,
    step TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    status TEXT NOT NULL,
    deadline_at TEXT,
    requested_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    item_count INTEGER,
    fetched_count INTEGER,
    accepted_count INTEGER,
    changed_count INTEGER,
    checkpoint_json TEXT,
    error_json TEXT,
    metadata_json TEXT,
    UNIQUE (run_uid, source, step, attempt),
    UNIQUE (source_run_id, run_uid)
);
CREATE INDEX IF NOT EXISTS idx_source_runs_run_status
    ON source_runs(run_uid, status);

CREATE TABLE IF NOT EXISTS run_events (
    run_event_id TEXT PRIMARY KEY,
    run_uid TEXT NOT NULL REFERENCES pipeline_runs(run_uid) ON DELETE CASCADE,
    source_run_id TEXT,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    event_type TEXT NOT NULL,
    at TEXT NOT NULL,
    payload_json TEXT,
    UNIQUE (run_uid, sequence),
    FOREIGN KEY (source_run_id, run_uid)
        REFERENCES source_runs(source_run_id, run_uid) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_run_events_source_run
    ON run_events(source_run_id);
"""

IDENTITY_DDL = """
CREATE TABLE IF NOT EXISTS postings (
    posting_id TEXT PRIMARY KEY,
    identity_status TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    retired_at TEXT
);

CREATE TABLE IF NOT EXISTS posting_aliases (
    alias_id TEXT PRIMARY KEY,
    posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    alias_kind TEXT NOT NULL,
    namespace TEXT NOT NULL,
    value TEXT NOT NULL,
    url TEXT,
    req_id TEXT,
    provenance_json TEXT,
    confidence REAL,
    valid_from TEXT NOT NULL,
    valid_to TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_posting_aliases_active
    ON posting_aliases(alias_kind, namespace, value) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_posting_aliases_posting
    ON posting_aliases(posting_id);

CREATE TABLE IF NOT EXISTS posting_redirects (
    from_posting_id TEXT PRIMARY KEY REFERENCES postings(posting_id) ON DELETE RESTRICT,
    to_posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (from_posting_id <> to_posting_id)
);

CREATE TABLE IF NOT EXISTS identity_evidence (
    evidence_id TEXT PRIMARY KEY,
    posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    alias_id TEXT REFERENCES posting_aliases(alias_id) ON DELETE RESTRICT,
    evidence_kind TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL UNIQUE,
    observed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legacy_identity_map (
    legacy_identity_kind TEXT NOT NULL,
    namespace TEXT NOT NULL,
    legacy_identity_value TEXT NOT NULL,
    posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    mapped_at TEXT NOT NULL,
    provenance_json TEXT,
    PRIMARY KEY (legacy_identity_kind, namespace, legacy_identity_value)
);

CREATE TABLE IF NOT EXISTS identity_migration_archive (
    archive_id TEXT PRIMARY KEY,
    artifact TEXT NOT NULL,
    locator TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    candidate_posting_ids_json TEXT,
    payload_hash TEXT NOT NULL,
    archived_at TEXT NOT NULL,
    UNIQUE (artifact, locator, payload_hash)
);
"""

CONTENT_DDL = """
CREATE TABLE IF NOT EXISTS posting_versions (
    posting_version_id TEXT PRIMARY KEY,
    posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    version_kind TEXT NOT NULL DEFAULT 'source',
    alias_id TEXT REFERENCES posting_aliases(alias_id) ON DELETE RESTRICT,
    source_run_id TEXT REFERENCES source_runs(source_run_id) ON DELETE RESTRICT,
    version_hash TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    title TEXT,
    company TEXT,
    location TEXT,
    salary TEXT,
    salary_min INTEGER,
    salary_max INTEGER,
    posted TEXT,
    remote INTEGER,
    source TEXT,
    req_id TEXT,
    tier INTEGER,
    odds TEXT,
    odds_score INTEGER,
    odds_why TEXT,
    is_new INTEGER,
    first_seen TEXT,
    also_seen_on TEXT,
    desc_snippet TEXT,
    latest_run TEXT,
    present INTEGER,
    why TEXT,
    flags TEXT,
    payload_json TEXT NOT NULL
    ,UNIQUE (posting_version_id, posting_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_posting_versions_posting_hash
    ON posting_versions(posting_id, version_hash);
CREATE INDEX IF NOT EXISTS idx_posting_versions_posting_observed
    ON posting_versions(posting_id, observed_at);

CREATE TABLE IF NOT EXISTS descriptions (
    description_id TEXT PRIMARY KEY,
    posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    posting_version_id TEXT REFERENCES posting_versions(posting_version_id) ON DELETE RESTRICT,
    alias_id TEXT REFERENCES posting_aliases(alias_id) ON DELETE RESTRICT,
    source_run_id TEXT REFERENCES source_runs(source_run_id) ON DELETE RESTRICT,
    provenance_hash TEXT NOT NULL UNIQUE,
    content_hash TEXT,
    fetch_status TEXT NOT NULL,
    body TEXT,
    fetched_at TEXT NOT NULL,
    metadata_json TEXT,
    CHECK (body IS NOT NULL OR fetch_status = 'unavailable')
);
CREATE INDEX IF NOT EXISTS idx_descriptions_posting_fetched
    ON descriptions(posting_id, fetched_at);
"""

DECISIONS_DDL = """
CREATE TABLE IF NOT EXISTS score_versions (
    score_version_id TEXT PRIMARY KEY,
    posting_version_id TEXT NOT NULL REFERENCES posting_versions(posting_version_id) ON DELETE RESTRICT,
    profile_version_id TEXT NOT NULL REFERENCES profile_versions(profile_version_id) ON DELETE RESTRICT,
    source_run_id TEXT REFERENCES source_runs(source_run_id) ON DELETE RESTRICT,
    score_hash TEXT NOT NULL,
    scorer_hash TEXT NOT NULL,
    config_hash TEXT,
    code_hash TEXT,
    tier INTEGER,
    odds TEXT,
    odds_score INTEGER,
    rationale_json TEXT,
    created_at TEXT NOT NULL
    ,UNIQUE (posting_version_id, profile_version_id, score_hash)
);
CREATE INDEX IF NOT EXISTS idx_score_versions_posting_profile
    ON score_versions(posting_version_id, profile_version_id, created_at);

CREATE TABLE IF NOT EXISTS llm_reviews (
    llm_review_id TEXT PRIMARY KEY,
    posting_version_id TEXT NOT NULL REFERENCES posting_versions(posting_version_id) ON DELETE RESTRICT,
    profile_version_id TEXT NOT NULL REFERENCES profile_versions(profile_version_id) ON DELETE RESTRICT,
    score_version_id TEXT REFERENCES score_versions(score_version_id) ON DELETE RESTRICT,
    source_run_id TEXT REFERENCES source_runs(source_run_id) ON DELETE RESTRICT,
    review_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    review_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (posting_version_id, profile_version_id, review_hash)
);

CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id TEXT PRIMARY KEY,
    posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    posting_version_id TEXT NOT NULL REFERENCES posting_versions(posting_version_id) ON DELETE RESTRICT,
    profile_version_id TEXT NOT NULL REFERENCES profile_versions(profile_version_id) ON DELETE RESTRICT,
    score_version_id TEXT REFERENCES score_versions(score_version_id) ON DELETE RESTRICT,
    llm_review_id TEXT REFERENCES llm_reviews(llm_review_id) ON DELETE RESTRICT,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    recommendation_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    retired_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_recommendations_posting_status
    ON recommendations(posting_id, status);

CREATE TABLE IF NOT EXISTS recommendation_events (
    recommendation_event_id TEXT PRIMARY KEY,
    recommendation_id TEXT NOT NULL REFERENCES recommendations(recommendation_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    event_type TEXT NOT NULL,
    at TEXT NOT NULL,
    payload_json TEXT,
    UNIQUE (recommendation_id, sequence)
);
"""

COMPATIBILITY_DDL = """
CREATE TABLE IF NOT EXISTS run_postings (
    run_uid TEXT NOT NULL REFERENCES pipeline_runs(run_uid) ON DELETE CASCADE,
    posting_id TEXT NOT NULL REFERENCES postings(posting_id) ON DELETE RESTRICT,
    posting_version_id TEXT REFERENCES posting_versions(posting_version_id) ON DELETE RESTRICT,
    source_run_id TEXT,
    present INTEGER NOT NULL DEFAULT 1 CHECK (present IN (0, 1)),
    first_seen_in_run INTEGER NOT NULL DEFAULT 0 CHECK (first_seen_in_run IN (0, 1)),
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (run_uid, posting_id),
    FOREIGN KEY (posting_version_id, posting_id)
        REFERENCES posting_versions(posting_version_id, posting_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_run_id, run_uid)
        REFERENCES source_runs(source_run_id, run_uid) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS idx_run_postings_posting
    ON run_postings(posting_id, run_uid);

CREATE VIEW IF NOT EXISTS compat_jobs AS
SELECT (SELECT a2.url FROM posting_aliases a2
    WHERE a2.posting_id = p.posting_id AND a2.valid_to IS NULL AND a2.url IS NOT NULL
    ORDER BY a2.valid_from DESC, a2.alias_id DESC LIMIT 1) AS url,
         p.posting_id AS seen_key, v.tier AS tier,
             v.odds AS odds, v.odds_score AS odds_score, v.odds_why AS odds_why,
             COALESCE(v.is_new, 0) AS is_new, v.title AS title,
             v.company AS company, v.location AS location,
             v.salary AS salary, v.salary_min AS salary_min, v.salary_max AS salary_max,
             v.posted AS posted, COALESCE(v.first_seen, p.first_seen_at) AS first_seen,
             v.remote AS remote, v.source AS source, v.also_seen_on AS also_seen_on,
             v.req_id AS req_id, v.why AS why, v.flags AS flags,
             COALESCE(v.desc_snippet, substr(d.body, 1, 500)) AS desc_snippet,
             d.body AS full_desc, v.latest_run AS latest_run,
             COALESCE(v.present, 1) AS present
FROM postings p
JOIN posting_versions v ON v.posting_version_id = (
    SELECT v2.posting_version_id FROM posting_versions v2
    WHERE v2.posting_id = p.posting_id AND v2.version_kind IN ('source', 'legacy-current')
    ORDER BY v2.observed_at DESC, v2.posting_version_id DESC LIMIT 1
)
LEFT JOIN descriptions d ON d.description_id = (
    SELECT d2.description_id FROM descriptions d2
    WHERE d2.posting_id = p.posting_id ORDER BY d2.fetched_at DESC, d2.description_id DESC LIMIT 1
)
;

CREATE VIEW IF NOT EXISTS compat_runs AS
SELECT legacy_run_date AS run_date, kept_count AS kept, new_count AS new_this_run,
             aggregate_report_json AS report_json, source_health_json AS source_health_json,
             legacy_ingested_at AS ingested_at
FROM pipeline_runs WHERE status IN ('done', 'imported');

CREATE VIEW IF NOT EXISTS compat_job_history AS
SELECT (SELECT a2.url FROM posting_aliases a2
                WHERE a2.posting_id = rp.posting_id AND a2.valid_from <= rp.recorded_at
                    AND (a2.valid_to IS NULL OR a2.valid_to > rp.recorded_at) AND a2.url IS NOT NULL
                ORDER BY a2.valid_from DESC, a2.alias_id DESC LIMIT 1) AS url,
                         pr.legacy_run_date AS run_date, rp.posting_id AS seen_key,
             v.tier AS tier, v.odds AS odds, rp.present AS present
FROM run_postings rp
JOIN pipeline_runs pr ON pr.run_uid = rp.run_uid
LEFT JOIN posting_versions v ON v.posting_version_id = rp.posting_version_id
WHERE pr.status IN ('done', 'imported');
"""

CANONICAL_DDL = "\n".join((
        PROFILE_VERSIONS_DDL,
        RUNS_DDL,
        IDENTITY_DDL,
        CONTENT_DDL,
        DECISIONS_DDL,
        COMPATIBILITY_DDL,
))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _database_has_tables(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1"
    ).fetchone() is not None


def _ensure_schema_version(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version ("
        "version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
    )


def _applied_versions(conn: sqlite3.Connection) -> set:
    return {r["version"] for r in conn.execute("SELECT version FROM schema_version")}


def _execute_ddl(conn: sqlite3.Connection, ddl: str) -> None:
    """Execute a DDL script without executescript's implicit transaction commit."""
    statement = ""
    for line in ddl.splitlines():
        statement += line + "\n"
        if sqlite3.complete_statement(statement):
            sql = statement.strip()
            if sql:
                conn.execute(sql)
            statement = ""
    if statement.strip():
        raise sqlite3.OperationalError("incomplete DDL statement")


def _validate_database(conn: sqlite3.Connection) -> None:
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise RuntimeError(f"database integrity check failed: {integrity}")
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        first = tuple(violations[0])
        raise RuntimeError(
            f"database foreign-key check failed: {len(violations)} violation(s), first={first}"
        )


# --------------------------------------------------------------------------- #
# Migrations
# --------------------------------------------------------------------------- #
def _migration_1_state_events(conn: sqlite3.Connection) -> None:
    """Create the append-only state_events log (idempotent)."""
    _execute_ddl(conn, STATE_EVENTS_DDL)


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
    _execute_ddl(conn, JOB_STATE_ARCHIVE_DDL)

    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    if "needs_review" not in cols:
        # Already the new schema (defensive: a re-run via the direct test path). No-op.
        return

    # rowid travels with each row so collision losers are excluded by identity, not
    # by url equality — the old PK allowed multiple NULL urls, and NULL != NULL is
    # false in Python, which would silently skip archiving such a loser.
    old_rows = conn.execute("SELECT rowid AS _rid, * FROM job_state").fetchall()
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
            if loser["_rid"] != winner["_rid"]:
                _archive_loser(conn, loser, "seen_key collision", at)

    # Zero-silent-loss invariant: every old row must be live or archived. Raising
    # here rolls the migration back and leaves the .bak restorable, turning any
    # future accounting bug into a loud failure instead of quiet data loss.
    live = conn.execute("SELECT COUNT(*) FROM job_state").fetchone()[0]
    archived = conn.execute(
        "SELECT COUNT(*) FROM job_state_archive WHERE archived_at = ?", (at,)
    ).fetchone()[0]
    if live + archived != len(old_rows):
        raise RuntimeError(
            f"job_state re-key dropped rows: {len(old_rows)} in, "
            f"{live} live + {archived} archived out"
        )

    conn.execute("DROP TABLE _job_state_old")


def _migration_4_applied_via(conn: sqlite3.Connection) -> None:
    """Add job_state.applied_via (nullable source picker for how an application was
    submitted). IF NOT EXISTS-style guard via PRAGMA check: a bare ALTER TABLE ADD
    COLUMN errors if the column is already there (e.g. a fresh baseline that already
    includes it, reached via the direct-invocation test path)."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    if "applied_via" not in cols:
        conn.execute("ALTER TABLE job_state ADD COLUMN applied_via TEXT")


def _migration_5_profile_versions(conn: sqlite3.Connection) -> None:
    _execute_ddl(conn, PROFILE_VERSIONS_DDL)


_LEGACY_RUN_NAMESPACE = uuid.UUID("fe5578bd-9624-5c99-96fd-27ab276f5c9f")


def _legacy_uid(kind: str, *parts) -> str:
    material = "\x1f".join(str(part) for part in (kind,) + parts)
    return str(uuid.uuid5(_LEGACY_RUN_NAMESPACE, material))


def _legacy_scalar(value, *, integer=False):
    if value is None or isinstance(value, (dict, list, tuple)):
        return None
    if integer:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return str(value)


def _migration_6_runs(conn: sqlite3.Connection) -> None:
    _execute_ddl(conn, RUNS_DDL)
    if not _table_exists(conn, "runs"):
        return

    for row in conn.execute(
        "SELECT rowid AS legacy_rowid, run_date, kept, new_this_run, report_json, "
        "source_health_json, ingested_at FROM runs ORDER BY run_date, rowid"
    ).fetchall():
        identity = row["run_date"] if row["run_date"] is not None else f"rowid:{row['legacy_rowid']}"
        run_uid = _legacy_uid("pipeline_run", identity)
        conn.execute(
            "INSERT OR IGNORE INTO pipeline_runs "
            "(run_uid, kind, status, legacy_run_date, legacy_ingested_at, source_health_json, "
            "trigger, kept_count, new_count, aggregate_report_json) "
            "VALUES (?, 'imported', 'imported', ?, ?, ?, 'legacy_migration', ?, ?, ?)",
            (run_uid, row["run_date"], row["ingested_at"], row["source_health_json"],
             row["kept"], row["new_this_run"], row["report_json"]),
        )
        try:
            health = json.loads(row["source_health_json"] or "")
        except (TypeError, ValueError):
            continue
        if not isinstance(health, dict):
            continue
        for source in sorted(health):
            details = health[source]
            if not isinstance(details, dict):
                continue
            step = _legacy_scalar(details.get("step")) or "legacy"
            try:
                attempt = max(1, int(details.get("attempt", 1)))
            except (TypeError, ValueError):
                attempt = 1
            status = _legacy_scalar(details.get("status")) or "imported"
            source_run_id = _legacy_uid("source_run", run_uid, source, step, attempt)
            metadata_json = json.dumps(details, sort_keys=True, separators=(",", ":"))
            conn.execute(
                "INSERT OR IGNORE INTO source_runs "
                "(source_run_id, run_uid, source, step, attempt, status, deadline_at, "
                "requested_at, started_at, finished_at, item_count, fetched_count, "
                "accepted_count, changed_count, checkpoint_json, error_json, metadata_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (source_run_id, run_uid, str(source), step, attempt, status,
                 _legacy_scalar(details.get("deadline_at")),
                 _legacy_scalar(details.get("requested_at")),
                 _legacy_scalar(details.get("started_at")),
                 _legacy_scalar(details.get("finished_at")),
                 _legacy_scalar(details.get("rows", details.get("count")), integer=True),
                 _legacy_scalar(details.get("fetched_count"), integer=True),
                 _legacy_scalar(details.get("accepted_count"), integer=True),
                 _legacy_scalar(details.get("changed_count"), integer=True),
                 json.dumps(details.get("checkpoint"), sort_keys=True)
                 if "checkpoint" in details else None,
                 json.dumps(details.get("error"), sort_keys=True)
                 if "error" in details else None, metadata_json),
            )


def _migration_7_identity(conn: sqlite3.Connection) -> None:
    _execute_ddl(conn, IDENTITY_DDL)


def _migration_8_content(conn: sqlite3.Connection) -> None:
    _execute_ddl(conn, CONTENT_DDL)


def _migration_9_decisions(conn: sqlite3.Connection) -> None:
    _execute_ddl(conn, DECISIONS_DDL)


def _migration_10_compatibility(conn: sqlite3.Connection) -> None:
    _execute_ddl(conn, COMPATIBILITY_DDL)


_LEGACY_LINEAGE_NAMESPACE = uuid.UUID("7ae26166-7694-5a9d-9eb2-c7031f0ccdfe")
_LEGACY_IMPORT_AT = "legacy-import"


def _canonical_json(value) -> str:
    def encode_special(item):
        if isinstance(item, bytes):
            return {"$type": "bytes", "base64": base64.b64encode(item).decode("ascii")}
        raise TypeError(f"Object of type {type(item).__name__} is not JSON serializable")

    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        default=encode_special,
    )


def _stable_hash(value) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _lineage_value(url: str, seen_key: str) -> str:
    return _canonical_json([url, seen_key])


def _lineage_posting_id(url: str, seen_key: str) -> str:
    return str(uuid.uuid5(_LEGACY_LINEAGE_NAMESPACE, _lineage_value(url, seen_key)))


def _archive_identity(conn, artifact, locator, payload, reason, candidates=None) -> str:
    payload_json = _canonical_json(payload)
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    archive_id = str(uuid.uuid5(
        _LEGACY_LINEAGE_NAMESPACE,
        _canonical_json(["archive", artifact, locator, payload_hash]),
    ))
    conn.execute(
        "INSERT OR IGNORE INTO identity_migration_archive "
        "(archive_id, artifact, locator, payload_json, reason, "
        "candidate_posting_ids_json, payload_hash, archived_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (archive_id, artifact, locator, payload_json, reason,
         _canonical_json(sorted(candidates)) if candidates else None,
         payload_hash, _LEGACY_IMPORT_AT),
    )
    return archive_id


def _observation_bounds(conn, url, seen_key):
    observed = [r[0] for r in conn.execute(
        "SELECT run_date FROM job_history WHERE url=? AND seen_key=? "
        "AND run_date IS NOT NULL AND trim(run_date)<>''",
        (url, seen_key),
    )]
    current = conn.execute(
        "SELECT first_seen, latest_run FROM jobs WHERE url=? AND seen_key=?",
        (url, seen_key),
    ).fetchone()
    if current:
        observed.extend(value for value in (current["first_seen"], current["latest_run"])
                        if value is not None and str(value).strip())
    if not observed:
        return _LEGACY_IMPORT_AT, _LEGACY_IMPORT_AT
    values = [str(value) for value in observed]
    return min(values), max(values)


def _ensure_posting_columns(conn):
    state_columns = {r["name"] for r in conn.execute("PRAGMA table_info(job_state)")}
    if "posting_id" not in state_columns:
        conn.execute(
            "ALTER TABLE job_state ADD COLUMN posting_id TEXT "
            "REFERENCES postings(posting_id) ON DELETE RESTRICT"
        )
    event_columns = {r["name"] for r in conn.execute("PRAGMA table_info(state_events)")}
    if "posting_id" not in event_columns:
        conn.execute(
            "ALTER TABLE state_events ADD COLUMN posting_id TEXT "
            "REFERENCES postings(posting_id) ON DELETE RESTRICT"
        )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_job_state_posting_id "
        "ON job_state(posting_id) WHERE posting_id IS NOT NULL"
    )
    version_columns = {r["name"] for r in conn.execute("PRAGMA table_info(posting_versions)")}
    if "version_kind" not in version_columns:
        conn.execute(
            "ALTER TABLE posting_versions ADD COLUMN version_kind TEXT NOT NULL DEFAULT 'source'"
        )
    for name, column_type in (
        ("is_new", "INTEGER"), ("first_seen", "TEXT"), ("also_seen_on", "TEXT"),
        ("desc_snippet", "TEXT"), ("latest_run", "TEXT"), ("present", "INTEGER"),
    ):
        if name not in version_columns:
            conn.execute(f"ALTER TABLE posting_versions ADD COLUMN {name} {column_type}")


def _migration_11_legacy_canonical_backfill(conn: sqlite3.Connection) -> None:
    _ensure_posting_columns(conn)
    _migration_6_runs(conn)
    for view in ("compat_jobs", "compat_runs", "compat_job_history"):
        conn.execute(f"DROP VIEW IF EXISTS {view}")
    _execute_ddl(conn, COMPATIBILITY_DDL)

    state_before = [dict(r) for r in conn.execute(
        "SELECT * FROM job_state ORDER BY seen_key"
    )]
    events_before = [dict(r) for r in conn.execute(
        "SELECT * FROM state_events ORDER BY id"
    )]
    for row in state_before:
        row.pop("posting_id", None)
    for row in events_before:
        row.pop("posting_id", None)

    lineage_rows = conn.execute(
        "SELECT url, seen_key FROM jobs UNION SELECT url, seen_key FROM job_history"
    ).fetchall()
    valid_lineages = []
    for row in lineage_rows:
        url, seen_key = row["url"], row["seen_key"]
        if url is None or seen_key is None or not str(url).strip() or not str(seen_key).strip():
            _archive_identity(
                conn, "lineage", _lineage_value(url, seen_key),
                {"url": url, "seen_key": seen_key}, "malformed lineage",
            )
            continue
        valid_lineages.append((url, seen_key))

    lineages = {}
    for url, seen_key in valid_lineages:
        posting_id = _lineage_posting_id(url, seen_key)
        lineage_value = _lineage_value(url, seen_key)
        first_seen, last_seen = _observation_bounds(conn, url, seen_key)
        lineage_hash = _stable_hash({"url": url, "seen_key": seen_key})
        lineages[(url, seen_key)] = {
            "posting_id": posting_id,
            "first_seen": first_seen,
            "last_seen": last_seen,
        }
        conn.execute(
            "INSERT OR IGNORE INTO postings "
            "(posting_id, identity_status, first_seen_at, created_at) "
            "VALUES (?, 'active', ?, ?)",
            (posting_id, first_seen, _LEGACY_IMPORT_AT),
        )
        conn.execute(
            "UPDATE postings SET first_seen_at = MIN(first_seen_at, ?) WHERE posting_id=?",
            (first_seen, posting_id),
        )
        conn.execute(
            "INSERT OR IGNORE INTO legacy_identity_map "
            "(legacy_identity_kind, namespace, legacy_identity_value, posting_id, mapped_at, "
            "provenance_json) VALUES ('lineage','legacy-db',?,?,?,?)",
            (lineage_value, posting_id, _LEGACY_IMPORT_AT,
             _canonical_json({"url": url, "seen_key": seen_key, "lineage_hash": lineage_hash})),
        )
        evidence_id = str(uuid.uuid5(
            _LEGACY_LINEAGE_NAMESPACE, _canonical_json(["evidence", lineage_value])
        ))
        conn.execute(
            "INSERT OR IGNORE INTO identity_evidence "
            "(evidence_id, posting_id, evidence_kind, evidence_json, evidence_hash, observed_at) "
            "VALUES (?,?,'legacy-lineage',?,?,?)",
            (evidence_id, posting_id,
             _canonical_json({"url": url, "seen_key": seen_key, "lineage_hash": lineage_hash}),
             lineage_hash, first_seen),
        )

    by_url = {}
    for (url, seen_key), lineage in lineages.items():
        by_url.setdefault(url, []).append((seen_key, lineage))
    for url, entries in by_url.items():
        entries.sort(key=lambda item: (
            item[1]["first_seen"], item[1]["last_seen"], item[0], item[1]["posting_id"]
        ))
        latest_at = entries[-1][1]["last_seen"]
        tied = [entry[1]["posting_id"] for entry in entries
                if entry[1]["last_seen"] == latest_at]
        if len(tied) > 1:
            _archive_identity(
                conn, "url_alias_ambiguity", url,
                {"url": url, "observed_at": latest_at},
                "multiple lineages share latest URL observation", tied,
            )
        for index, (seen_key, lineage) in enumerate(entries):
            valid_to = entries[index + 1][1]["first_seen"] if index + 1 < len(entries) else None
            alias_id = str(uuid.uuid5(
                _LEGACY_LINEAGE_NAMESPACE,
                _canonical_json(["url-alias", url, seen_key]),
            ))
            conn.execute(
                "INSERT OR IGNORE INTO posting_aliases "
                "(alias_id, posting_id, alias_kind, namespace, value, url, provenance_json, "
                "confidence, valid_from, valid_to) VALUES (?,?,'url','legacy-url',?,?,?,?,?,?)",
                (alias_id, lineage["posting_id"], url, url,
                 _canonical_json({"url": url, "seen_key": seen_key}), 1.0,
                 lineage["first_seen"], valid_to),
            )

    profile_id = "legacy-import"
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions "
        "(profile_version_id, content_hash, profile_json, created_at) VALUES (?,?,?,?)",
        (profile_id, "legacy-import", "{}", _LEGACY_IMPORT_AT),
    )

    current_versions = {}
    for row in conn.execute("SELECT * FROM jobs ORDER BY url").fetchall():
        lineage = lineages.get((row["url"], row["seen_key"]))
        if lineage is None:
            _archive_identity(
                conn, "jobs", f"url:{row['url']}", dict(row), "malformed lineage"
            )
            continue
        posting_id = lineage["posting_id"]
        payload = dict(row)
        payload_json = _canonical_json(payload)
        version_hash = _stable_hash({"posting_id": posting_id, "payload": payload})
        version_id = str(uuid.uuid5(
            _LEGACY_LINEAGE_NAMESPACE,
            _canonical_json(["posting-version", posting_id, version_hash]),
        ))
        observed_at = str(row["latest_run"] or row["first_seen"] or lineage["last_seen"])
        conn.execute(
            "INSERT OR IGNORE INTO posting_versions "
            "(posting_version_id, posting_id, version_kind, version_hash, observed_at, title, company, "
            "location, salary, salary_min, salary_max, posted, remote, source, req_id, tier, "
            "odds, odds_score, odds_why, is_new, first_seen, also_seen_on, desc_snippet, "
            "latest_run, present, why, flags, payload_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (version_id, posting_id, "legacy-current", version_hash, observed_at,
             row["title"], row["company"],
             row["location"], row["salary"], row["salary_min"], row["salary_max"],
             row["posted"], row["remote"], row["source"], row["req_id"], row["tier"],
             row["odds"], row["odds_score"], row["odds_why"], row["is_new"],
             row["first_seen"], row["also_seen_on"], row["desc_snippet"], row["latest_run"],
             row["present"], row["why"], row["flags"], payload_json),
        )
        current_versions[(row["url"], row["seen_key"])] = version_id
        if row["full_desc"] is not None and str(row["full_desc"]).strip():
            body = row["full_desc"]
            provenance_hash = _stable_hash({"posting_id": posting_id, "source": "jobs.full_desc"})
            description_id = str(uuid.uuid5(
                _LEGACY_LINEAGE_NAMESPACE,
                _canonical_json(["description", posting_id, provenance_hash]),
            ))
            conn.execute(
                "INSERT OR IGNORE INTO descriptions "
                "(description_id, posting_id, posting_version_id, provenance_hash, content_hash, "
                "fetch_status, body, fetched_at, metadata_json) "
                "VALUES (?,?,?,?,?,'available',?,?,?)",
                (description_id, posting_id, version_id, provenance_hash,
                 hashlib.sha256(body.encode("utf-8")).hexdigest(), body, observed_at,
                 _canonical_json({"source": "legacy jobs.full_desc"})),
            )
        rationale = {
            "odds_why": row["odds_why"], "why": row["why"], "flags": row["flags"]
        }
        score_material = {
            "tier": row["tier"], "odds": row["odds"], "odds_score": row["odds_score"],
            "rationale": rationale,
        }
        score_hash = _stable_hash({"posting_version_id": version_id, "score": score_material})
        score_id = str(uuid.uuid5(
            _LEGACY_LINEAGE_NAMESPACE,
            _canonical_json(["score-version", version_id, score_hash]),
        ))
        conn.execute(
            "INSERT OR IGNORE INTO score_versions "
            "(score_version_id, posting_version_id, profile_version_id, score_hash, scorer_hash, "
            "tier, odds, odds_score, rationale_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (score_id, version_id, profile_id, score_hash, "legacy-import", row["tier"],
             row["odds"], row["odds_score"], _canonical_json(rationale), observed_at),
        )

    history_rows = conn.execute(
        "SELECT rowid AS legacy_rowid, * FROM job_history ORDER BY run_date, rowid"
    ).fetchall()
    earliest_history = {}
    for row in history_rows:
        lineage = lineages.get((row["url"], row["seen_key"]))
        if lineage:
            earliest_history[lineage["posting_id"]] = min(
                earliest_history.get(lineage["posting_id"], row["run_date"]), row["run_date"]
            )
    history_accounted = 0
    for row in history_rows:
        locator = f"rowid:{row['legacy_rowid']}"
        lineage = lineages.get((row["url"], row["seen_key"]))
        if lineage is None:
            archive_id = _archive_identity(
                conn, "job_history", locator, dict(row), "malformed lineage"
            )
            if conn.execute(
                "SELECT 1 FROM identity_migration_archive WHERE archive_id=?", (archive_id,)
            ).fetchone() is None:
                raise RuntimeError(f"job_history row was not archived: {locator}")
            history_accounted += 1
            continue
        run_uid = _legacy_uid("pipeline_run", row["run_date"])
        conn.execute(
            "INSERT OR IGNORE INTO pipeline_runs "
            "(run_uid, kind, status, legacy_run_date, trigger) "
            "VALUES (?,'imported','imported',?,'legacy_migration')",
            (run_uid, row["run_date"]),
        )
        history_payload = {
            "url": row["url"], "run_date": row["run_date"],
            "seen_key": row["seen_key"], "tier": row["tier"],
            "odds": row["odds"], "present": row["present"],
        }
        history_hash = _stable_hash({
            "posting_id": lineage["posting_id"], "history": history_payload,
        })
        version_id = str(uuid.uuid5(
            _LEGACY_LINEAGE_NAMESPACE,
            _canonical_json(["history-version", lineage["posting_id"], history_hash]),
        ))
        conn.execute(
            "INSERT OR IGNORE INTO posting_versions "
            "(posting_version_id, posting_id, version_kind, version_hash, observed_at, "
            "tier, odds, payload_json) VALUES (?,?,?,?,?,?,?,?)",
            (version_id, lineage["posting_id"], "legacy-history", history_hash, row["run_date"],
             row["tier"], row["odds"], _canonical_json(history_payload)),
        )
        expected = (
            version_id, row["present"],
            int(row["run_date"] == earliest_history[lineage["posting_id"]]), row["run_date"],
        )
        conn.execute(
            "INSERT OR IGNORE INTO run_postings "
            "(run_uid, posting_id, posting_version_id, present, first_seen_in_run, recorded_at) "
            "VALUES (?,?,?,?,?,?)",
            (run_uid, lineage["posting_id"], *expected),
        )
        mapped = conn.execute(
            "SELECT posting_version_id, present, first_seen_in_run, recorded_at "
            "FROM run_postings WHERE run_uid=? AND posting_id=?",
            (run_uid, lineage["posting_id"]),
        ).fetchone()
        if mapped is None or tuple(mapped) != expected:
            raise RuntimeError(f"job_history row mapping conflict: {locator}")
        history_accounted += 1

    for row in conn.execute("SELECT * FROM jobs ORDER BY url").fetchall():
        lineage = lineages.get((row["url"], row["seen_key"]))
        version_id = current_versions.get((row["url"], row["seen_key"]))
        if lineage is None or version_id is None:
            continue
        run_date = row["latest_run"] or row["first_seen"] or lineage["last_seen"]
        run_uid = _legacy_uid("pipeline_run", run_date)
        conn.execute(
            "INSERT OR IGNORE INTO pipeline_runs "
            "(run_uid, kind, status, legacy_run_date, trigger) "
            "VALUES (?,'imported','imported',?,'legacy_migration')",
            (run_uid, run_date),
        )
        existing = conn.execute(
            "SELECT 1 FROM run_postings WHERE run_uid=? AND posting_id=?",
            (run_uid, lineage["posting_id"]),
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO run_postings "
                "(run_uid, posting_id, posting_version_id, present, first_seen_in_run, recorded_at) "
                "VALUES (?,?,?,?,?,?)",
                (run_uid, lineage["posting_id"], version_id, row["present"],
                 int(lineage["posting_id"] not in earliest_history), run_date),
            )

    by_seen_key = {}
    for (_url, seen_key), lineage in lineages.items():
        by_seen_key.setdefault(seen_key, set()).add(lineage["posting_id"])
    for row in conn.execute("SELECT rowid AS legacy_rowid, * FROM job_state").fetchall():
        candidates = sorted(by_seen_key.get(row["seen_key"], set()))
        if len(candidates) == 1:
            conn.execute("UPDATE job_state SET posting_id=? WHERE rowid=?",
                         (candidates[0], row["legacy_rowid"]))
        else:
            conn.execute("UPDATE job_state SET posting_id=NULL WHERE rowid=?", (row["legacy_rowid"],))
            _archive_identity(conn, "job_state", f"rowid:{row['legacy_rowid']}", dict(row),
                              "canonical lineage resolution was not unique", candidates)
    for row in conn.execute("SELECT * FROM state_events ORDER BY id").fetchall():
        candidates = sorted(by_seen_key.get(row["seen_key"], set()))
        if len(candidates) == 1:
            conn.execute("UPDATE state_events SET posting_id=? WHERE id=?",
                         (candidates[0], row["id"]))
        else:
            conn.execute("UPDATE state_events SET posting_id=NULL WHERE id=?", (row["id"],))
            _archive_identity(conn, "state_event", f"id:{row['id']}", dict(row),
                              "canonical lineage resolution was not unique", candidates)

    mapped_lineages = sum(
        conn.execute(
            "SELECT posting_id FROM legacy_identity_map WHERE legacy_identity_kind='lineage' "
            "AND namespace='legacy-db' AND legacy_identity_value=?",
            (_lineage_value(url, seen_key),),
        ).fetchone()[0] == lineage["posting_id"]
        for (url, seen_key), lineage in lineages.items()
    )
    if mapped_lineages != len(valid_lineages):
        raise RuntimeError(
            f"lineage accounting mismatch: {len(valid_lineages)} valid, {mapped_lineages} mapped"
        )
    if history_accounted != len(history_rows):
        raise RuntimeError(
            f"history accounting mismatch: {len(history_rows)} rows, {history_accounted} accounted"
        )
    state_after = [dict(r) for r in conn.execute("SELECT * FROM job_state ORDER BY seen_key")]
    events_after = [dict(r) for r in conn.execute("SELECT * FROM state_events ORDER BY id")]
    for row in state_after:
        row.pop("posting_id", None)
    for row in events_after:
        row.pop("posting_id", None)
    if state_after != state_before or events_after != events_before:
        raise RuntimeError("state or event content changed during canonical backfill")
    for lineage in lineages.values():
        first_seen = conn.execute(
            "SELECT first_seen_at FROM postings WHERE posting_id=?", (lineage["posting_id"],)
        ).fetchone()[0]
        if first_seen > lineage["first_seen"]:
            raise RuntimeError(f"posting first_seen moved later: {lineage['posting_id']}")


# Ordered (version, name, fn). Append new migrations here; never renumber.
MIGRATIONS = [
    (1, "state_events", _migration_1_state_events),
    (2, "backfill_state_events", _migration_2_backfill),
    (3, "rekey_job_state_on_seen_key", _migration_3_rekey_job_state),
    (4, "job_state_applied_via", _migration_4_applied_via),
    (5, "profile_versions", _migration_5_profile_versions),
    (6, "pipeline_runs", _migration_6_runs),
    (7, "posting_identity", _migration_7_identity),
    (8, "posting_content", _migration_8_content),
    (9, "canonical_decisions", _migration_9_decisions),
    (10, "canonical_compatibility", _migration_10_compatibility),
    (11, "legacy_canonical_backfill", _migration_11_legacy_canonical_backfill),
]


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def _backup(conn: sqlite3.Connection, db_path, version: int):
    """Create a consistent SQLite backup before the first pending migration."""
    if not db_path:
        return None
    path = str(db_path)
    if not os.path.isfile(path):
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    dest = f"{path}.bak.v{version}-{stamp}"
    backup = sqlite3.connect(dest)
    try:
        backup.execute("PRAGMA foreign_keys=ON")
        conn.backup(backup)
        _validate_database(backup)
    except Exception:
        backup.close()
        try:
            os.unlink(dest)
        except FileNotFoundError:
            pass
        raise
    backup.close()
    return dest


def _dry_run_pending(conn: sqlite3.Connection):
    """Trial-run every pending migration against a private in-memory copy of `conn`'s
    current state, then discard the copy.

    The private copy keeps dry-run inert even if a migration contains transaction-
    hostile behavior now or in the future. Each migration still runs in the same
    explicit transaction and validation path as a real migration."""
    tmp = sqlite3.connect(":memory:")
    tmp.row_factory = sqlite3.Row
    try:
        conn.backup(tmp)
        tmp.execute("PRAGMA foreign_keys=ON")
        _ensure_schema_version(tmp)
        applied = _applied_versions(tmp)
        pending = [(v, n, fn) for (v, n, fn) in MIGRATIONS if v not in applied]
        for version, name, fn in pending:
            tmp.execute("BEGIN IMMEDIATE")
            try:
                fn(tmp)
                tmp.execute(
                    "INSERT INTO schema_version (version, name, applied_at) VALUES (?,?,?)",
                    (version, name, _utc_now_iso()),
                )
                _validate_database(tmp)
                tmp.commit()
            except Exception:
                tmp.rollback()
                raise
        _validate_database(tmp)
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
    if conn.in_transaction:
        raise RuntimeError("migrations require a connection with no active transaction")

    if dry_run:
        return _dry_run_pending(conn)

    if fresh is None:
        fresh = not _database_has_tables(conn)

    if fresh:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _ensure_schema_version(conn)
            applied = _applied_versions(conn)
            stamped = []
            for version, name, _fn in MIGRATIONS:
                if version not in applied:
                    conn.execute(
                        "INSERT INTO schema_version (version, name, applied_at) VALUES (?,?,?)",
                        (version, f"{name} (stamped)", _utc_now_iso()),
                    )
                    stamped.append((version, name))
            _validate_database(conn)
            conn.commit()
            return stamped
        except Exception:
            conn.rollback()
            raise

    applied = _applied_versions(conn) if _table_exists(conn, "schema_version") else set()

    pending = [(v, n, fn) for (v, n, fn) in MIGRATIONS if v not in applied]
    if pending:
        _backup(conn, db_path, pending[0][0])

    done = []
    for version, name, fn in pending:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _ensure_schema_version(conn)
            fn(conn)
            conn.execute(
                "INSERT INTO schema_version (version, name, applied_at) VALUES (?,?,?)",
                (version, name, _utc_now_iso()),
            )
            _validate_database(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        done.append((version, name))
    return done
