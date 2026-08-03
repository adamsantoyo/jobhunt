"""Concrete read helpers for legacy-to-canonical pipeline audits."""


def read_schema_versions(conn):
    return conn.execute(
        "SELECT version,name,applied_at FROM schema_version ORDER BY version"
    ).fetchall()


def read_legacy_jobs(conn):
    return conn.execute("SELECT * FROM jobs ORDER BY url,seen_key").fetchall()


def read_legacy_runs(conn):
    return conn.execute("SELECT * FROM runs ORDER BY run_date").fetchall()


def read_legacy_history(conn):
    return conn.execute(
        "SELECT rowid AS legacy_rowid,* FROM job_history "
        "ORDER BY run_date,url,seen_key,legacy_rowid"
    ).fetchall()


def read_legacy_state(conn):
    return conn.execute("SELECT rowid AS legacy_rowid,* FROM job_state ORDER BY seen_key").fetchall()


def read_legacy_events(conn):
    return conn.execute("SELECT * FROM state_events ORDER BY id").fetchall()


def read_compat_jobs(conn):
    return conn.execute("SELECT * FROM compat_jobs ORDER BY seen_key,url").fetchall()


def read_compat_runs(conn):
    return conn.execute("SELECT * FROM compat_runs ORDER BY run_date").fetchall()


def read_compat_history(conn):
    return conn.execute(
        "SELECT * FROM compat_job_history ORDER BY run_date,seen_key,url"
    ).fetchall()


def read_lineage_mappings(conn):
    return conn.execute(
        "SELECT * FROM legacy_identity_map "
        "WHERE legacy_identity_kind='lineage' AND namespace='legacy-db' "
        "ORDER BY legacy_identity_value,posting_id"
    ).fetchall()


def read_postings(conn):
    return conn.execute("SELECT * FROM postings ORDER BY posting_id").fetchall()


def read_posting_aliases(conn):
    return conn.execute(
        "SELECT * FROM posting_aliases "
        "ORDER BY alias_kind,namespace,value,valid_from,alias_id"
    ).fetchall()


def read_posting_redirects(conn):
    return conn.execute(
        "SELECT * FROM posting_redirects ORDER BY from_posting_id"
    ).fetchall()


def read_run_postings(conn):
    return conn.execute(
        "SELECT * FROM run_postings ORDER BY run_uid,posting_id"
    ).fetchall()


def read_run_posting_ownership(conn):
    return conn.execute(
        "SELECT rp.*,pr.legacy_run_date,pr.status AS run_status "
        "FROM run_postings rp JOIN pipeline_runs pr ON pr.run_uid=rp.run_uid "
        "ORDER BY pr.legacy_run_date,rp.posting_id,rp.run_uid"
    ).fetchall()


def read_identity_archives(conn):
    return conn.execute(
        "SELECT * FROM identity_migration_archive ORDER BY artifact,locator,archive_id"
    ).fetchall()


def read_import_ledger(conn):
    return conn.execute(
        "SELECT * FROM legacy_artifact_imports "
        "ORDER BY artifact_kind,artifact_hash,import_id"
    ).fetchall()