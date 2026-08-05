"""The six validation phases plus the self-check synthetic-DB builder.

Imported only from `__main__.py`, AFTER the safety gate has already confirmed
`backend.config.DB_PATH` resolves inside the sandbox and `sys.path` has been
bootstrapped so `backend.*` (not `webapp.backend.*`) is the one and only
loaded copy of the backend package -- see `__main__.py`'s module docstring
for why that distinction matters. Every function here takes explicit sandbox
paths and never relies on `backend.config.DB_PATH` for anything it opens.
"""
from __future__ import annotations

import glob
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from .util import (
    Report,
    compare_json,
    copy_into_sandbox,
    dump_table,
    jsonable,
    require_within,
    row_accounting,
    run_async,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
WEBAPP_DIR = Path(__file__).resolve().parents[2]

# The six legacy read paths JOBHUNT_READS dispatches between legacy and
# canonical (backend/config.py's module docstring); a representative
# job-detail path is added at call time once a sample URL is known.
_FLAGGED_LIST_PATHS = ["/api/jobs", "/api/followups", "/api/changes", "/api/analytics", "/api/freshness"]

_V2_LIST_PATHS = ["/api/v2/jobs", "/api/v2/followups", "/api/v2/changes", "/api/v2/analytics", "/api/v2/freshness"]


# --------------------------------------------------------------------------- #
# Self-check synthetic database
# --------------------------------------------------------------------------- #
def build_selfcheck_snapshot(sandbox: Path, report: Report) -> Path:
    """A small synthetic legacy database at schema v4, seeded with a few rows
    of job_state/state_events/jobs data (via a genuine run of migrations 1-4,
    including a seen_key collision that exercises the archive path) -- the
    stand-in "prod snapshot" that every phase below then runs against."""
    dest = require_within(sandbox / "selfcheck-prod-snapshot.db", sandbox, "selfcheck snapshot")
    # N7: idempotency. `build_v4_db`/`_build_minimal_v4_db` both `CREATE TABLE`
    # (no `IF NOT EXISTS`) against `dest` directly -- reusing a snapshot file
    # left over from an earlier invocation in the SAME sandbox crashes with
    # "table jobs already exists" (this is exactly what happened when the
    # runbook was executed twice in one sandbox). Unlink it and any WAL/SHM
    # sidecars first so a rerun always starts from nothing, same as a fresh
    # sandbox would.
    for stale in (dest, Path(f"{dest}-wal"), Path(f"{dest}-shm")):
        if stale.exists():
            stale.unlink()
    try:
        from backend.tests.test_migrations import build_v4_db

        build_v4_db(dest)
        report.log(f"self-check: synthetic v4 snapshot built via tests.test_migrations.build_v4_db at {dest}")
    except ImportError as exc:
        report.log(f"self-check: tests.test_migrations not importable ({exc}); "
                    "falling back to a minimal reimplementation")
        _build_minimal_v4_db(dest)
        report.log(f"self-check: synthetic v4 snapshot built via minimal reimplementation at {dest}")

    # build_v4_db's fixture has zero legacy `runs` rows by design (it only
    # exercises the job_state migration path). A real prod snapshot always
    # has run history, and canonical_reads/legacy read paths derive "latest
    # run" from different sources -- without at least one `runs` row, migration
    # 11's backfill has nothing to attribute postings to and falls back to its
    # `legacy-import` sentinel, which legacy's own `_latest_run()` (reading the
    # empty `runs` table) can never agree with. One realistic row here makes
    # phase 3's changes/analytics/freshness "latest run" comparisons meaningful
    # instead of vacuously divergent on a fixture no production database
    # resembles.
    # build_v4_db's job rows also carry no first_seen/latest_run (that builder
    # only exercises the job_state migration path) -- migration 11's canonical
    # backfill correctly falls back to its `legacy-import` sentinel for a
    # posting with zero observation history, which is a real code path but
    # not one any production posting (ingest.py always sets first_seen) would
    # ever hit, so it is not a meaningful signal for self-check. Filling both
    # columns in lets migration 11 attribute every posting to the same real
    # run the legacy side sees, making the read-parity comparisons apples to
    # apples the way they would be against an actual snapshot.
    conn = sqlite3.connect(str(dest))
    try:
        conn.execute(
            "INSERT INTO runs (run_date, kept, new_this_run, report_json, source_health_json, ingested_at) "
            "VALUES (?,?,?,?,?,?)",
            ("2026-07-01", 6, 1, "{}", "{}", "2026-07-01T12:00:00"),
        )
        # is_new=1 too: with exactly one run in this fixture's whole history,
        # every posting genuinely IS first-seen in "the latest run" both ways
        # of computing it (legacy's stored column, canonical's fresh
        # first-seen-in-latest-run check) -- 0 would only be honest with a
        # second, later run for these jobs to have already appeared in.
        conn.execute("UPDATE jobs SET first_seen='2026-07-01', latest_run='2026-07-01', is_new=1")
        conn.commit()
    finally:
        conn.close()
    report.log("self-check: seeded one legacy `runs` row and backdated job first_seen/latest_run "
               "so run-history comparisons are meaningful")
    return dest


_FALLBACK_OLD_DDL = """
CREATE TABLE jobs (
  url TEXT PRIMARY KEY, seen_key TEXT NOT NULL, tier INTEGER NOT NULL,
  odds TEXT, odds_score INTEGER, present INTEGER NOT NULL DEFAULT 1);
CREATE TABLE job_state (
  url TEXT PRIMARY KEY, seen_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'New', notes TEXT DEFAULT '',
  follow_up_date TEXT, applied_date TEXT, starred INTEGER NOT NULL DEFAULT 0,
  hidden INTEGER NOT NULL DEFAULT 0, contact TEXT DEFAULT '', snoozed_until TEXT,
  needs_review INTEGER NOT NULL DEFAULT 0, review_reason TEXT,
  review_dismissed INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
CREATE TABLE company_state (company TEXT PRIMARY KEY, contact TEXT DEFAULT '',
  notes TEXT DEFAULT '', updated_at TEXT NOT NULL);
CREATE TABLE runs (run_date TEXT PRIMARY KEY, kept INTEGER, new_this_run INTEGER,
  report_json TEXT, source_health_json TEXT, ingested_at TEXT NOT NULL);
CREATE TABLE job_history (url TEXT NOT NULL, run_date TEXT NOT NULL, seen_key TEXT NOT NULL,
  tier INTEGER NOT NULL, odds TEXT, present INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (url, run_date));
CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def _build_minimal_v4_db(dest: Path) -> None:
    """Degraded fallback for build_selfcheck_snapshot: a hand-rolled pre-Phase-0
    DB with one job/job_state row, forward-migrated to v4 only. Used only if
    importing the real test builder fails."""
    from backend.db import connect
    from backend.identity import seen_key as compute_seen_key
    import backend.migrations as migrations_mod

    conn = sqlite3.connect(str(dest))
    conn.row_factory = sqlite3.Row
    conn.executescript(_FALLBACK_OLD_DDL)
    sk = compute_seen_key("ValCo", "Support Engineer", "Remote")
    conn.execute("INSERT INTO jobs (url, seen_key, tier, present) VALUES (?,?,?,1)",
                 ("https://val45.example/a", sk, 3))
    conn.execute(
        "INSERT INTO job_state (url, seen_key, status, applied_date, updated_at) VALUES (?,?,?,?,?)",
        ("https://val45.example/a", sk, "Applied", "2026-07-01", "2026-07-01T00:00:00"),
    )
    conn.commit()
    conn.close()

    conn = connect(dest)
    original = list(migrations_mod.MIGRATIONS)
    migrations_mod.MIGRATIONS[:] = original[:4]
    try:
        migrations_mod.run_migrations(conn, str(dest))
    finally:
        migrations_mod.MIGRATIONS[:] = original
    conn.close()


# --------------------------------------------------------------------------- #
# Phase 1: migrate
# --------------------------------------------------------------------------- #
def _current_schema_version(conn: sqlite3.Connection):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    if not exists:
        return None
    row = conn.execute("SELECT MAX(version) AS v FROM schema_version").fetchone()
    return row["v"] if row else None


def phase1_migrate(report: Report, sandbox: Path, snapshot: Path) -> Path:
    from backend.db import connect, init_db

    report.log("PHASE 1 (migrate): begin")
    dest = copy_into_sandbox(snapshot, sandbox / "phase1_migrated.db", sandbox, label="phase1")
    # N7: idempotency. `copy_into_sandbox` replaces `dest` itself on a rerun,
    # but a PRIOR run's auto-backup file (`db.init_db`'s own naming,
    # `<path>.bak.v<version>-<timestamp>`) sits beside it under a different
    # name and is never touched by that copy -- so a second run in the same
    # sandbox would accumulate two backup files and fail
    # `phase1_auto_backup_present`'s "exactly one" assertion for a reason that
    # has nothing to do with migration correctness. Clear ONLY files matching
    # the harness's own known backup pattern for this exact destination path
    # before migrating again.
    stale_backups = sorted(glob.glob(f"{dest}.bak.v*-*"))
    for stale in stale_backups:
        os.unlink(stale)
    if stale_backups:
        report.log(f"PHASE 1 (migrate): cleared {len(stale_backups)} stale backup file(s) "
                   f"from a prior run in this sandbox: {stale_backups}")
    conn = connect(dest)
    try:
        before = _current_schema_version(conn)
        t0 = time.perf_counter()
        init_db(conn)
        duration = time.perf_counter() - t0
        after = _current_schema_version(conn)
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk_violations = [dict(r) for r in conn.execute("PRAGMA foreign_key_check").fetchall()]
    finally:
        conn.close()
    backups = sorted(glob.glob(str(dest) + ".bak.v*-*"))

    data = {
        "migrated_db_path": str(dest),
        "duration_seconds": duration,
        "schema_version_before": before,
        "schema_version_after": after,
        "integrity_check": integrity,
        "foreign_key_violations": fk_violations,
        "auto_backup_files": backups,
    }
    report.phase("1_migrate", data)
    report.check("phase1_schema_before_is_4", before == 4, f"schema_version before init_db() = {before}")
    report.check("phase1_schema_after_is_20", after == 20, f"schema_version after init_db() = {after}")
    report.check("phase1_integrity_check_ok", integrity == "ok", f"PRAGMA integrity_check = {integrity!r}")
    report.check("phase1_foreign_key_check_clean", len(fk_violations) == 0,
                 f"{len(fk_violations)} violation(s)")
    report.check("phase1_auto_backup_present", len(backups) == 1,
                 f"{len(backups)} backup file(s): {backups}")
    report.log(f"PHASE 1 (migrate): done in {duration:.3f}s, {before}->{after}")
    return dest


# --------------------------------------------------------------------------- #
# Phase 2: state preservation
# --------------------------------------------------------------------------- #
def phase2_state_preservation(report: Report, sandbox: Path, snapshot: Path) -> None:
    from backend.db import connect, init_db
    from backend.routers import analytics as analytics_router
    from backend.routers import funnel as funnel_router
    from backend.routers import jobs as jobs_router

    report.log("PHASE 2 (state preservation): begin")
    dest = copy_into_sandbox(snapshot, sandbox / "phase2_state.db", sandbox, label="phase2")
    conn = connect(dest)
    try:
        pre_job_state = dump_table(conn, "job_state", "seen_key")
        pre_state_events = dump_table(conn, "state_events", "id")
        pre_endpoints = {
            "funnel": jsonable(funnel_router.get_funnel(conn=conn)),
            "activity": jsonable(funnel_router.get_activity(conn=conn)),
            "analytics": jsonable(analytics_router.analytics(conn=conn)),
            "jobs": jsonable(jobs_router.list_jobs(min_tier=None, conn=conn)),
        }
        init_db(conn)  # forward-migrate the SAME connection/file in place
        post_job_state = dump_table(conn, "job_state", "seen_key")
        post_state_events = dump_table(conn, "state_events", "id")
        post_endpoints = {
            "funnel": jsonable(funnel_router.get_funnel(conn=conn)),
            "activity": jsonable(funnel_router.get_activity(conn=conn)),
            "analytics": jsonable(analytics_router.analytics(conn=conn)),
            "jobs": jsonable(jobs_router.list_jobs(min_tier=None, conn=conn)),
        }
    finally:
        conn.close()

    job_state_acct = row_accounting(pre_job_state, post_job_state, "seen_key")
    state_events_acct = row_accounting(pre_state_events, post_state_events, "id")
    # B3: an EMPTY value-tolerance set. `compare_json`'s default tolerates
    # `seen_key` value drift because that key is documented as repurposed on
    # the CANONICAL side (canonical_reads.py, posting_id-as-seen_key). Here
    # both sides are legacy (pre- vs post-migration, same legacy endpoint) --
    # no repurposing has happened yet, so a seen_key drift here would be a
    # real defect, not a documented substitution. Comparing exactly is the
    # correct legacy-vs-legacy bar.
    endpoint_diffs = {name: compare_json(pre_endpoints[name], post_endpoints[name],
                                          tolerate_value_diff=frozenset())
                       for name in pre_endpoints}

    data = {
        "job_state_accounting": job_state_acct,
        "state_events_accounting": state_events_acct,
        "legacy_endpoint_diffs_pre_vs_post": endpoint_diffs,
    }
    report.phase("2_state_preservation", data)
    report.check("phase2_job_state_zero_silent_loss", not job_state_acct["missing_in_post"],
                 f"{len(job_state_acct['missing_in_post'])} row(s) missing post-migration")
    report.check("phase2_job_state_byte_identical", job_state_acct["value_diff_count"] == 0,
                 f"{job_state_acct['value_diff_count']} row(s) changed")
    # B3: `added_in_post` was computed by `row_accounting` all along but
    # nothing ever asserted it -- a spurious post-migration row (e.g. a stray
    # INSERT unrelated to the migration itself) was structurally invisible.
    report.check("phase2_job_state_no_added_rows", not job_state_acct["added_in_post"],
                 f"added={len(job_state_acct['added_in_post'])} "
                 f"pre_count={job_state_acct['pre_count']} post_count={job_state_acct['post_count']}")
    report.check("phase2_job_state_pre_post_count_equal",
                 job_state_acct["pre_count"] == job_state_acct["post_count"],
                 f"pre_count={job_state_acct['pre_count']} post_count={job_state_acct['post_count']}")
    report.check("phase2_state_events_zero_silent_loss", not state_events_acct["missing_in_post"],
                 f"{len(state_events_acct['missing_in_post'])} row(s) missing post-migration")
    report.check("phase2_state_events_byte_identical", state_events_acct["value_diff_count"] == 0,
                 f"{state_events_acct['value_diff_count']} row(s) changed")
    report.check("phase2_state_events_no_added_rows", not state_events_acct["added_in_post"],
                 f"added={len(state_events_acct['added_in_post'])} "
                 f"pre_count={state_events_acct['pre_count']} post_count={state_events_acct['post_count']}")
    report.check("phase2_state_events_pre_post_count_equal",
                 state_events_acct["pre_count"] == state_events_acct["post_count"],
                 f"pre_count={state_events_acct['pre_count']} post_count={state_events_acct['post_count']}")
    for name, d in endpoint_diffs.items():
        report.check(f"phase2_legacy_{name}_identical_pre_post", d["equal"],
                     f"{d['diff_count']} diff(s); missing_in_canonical={d['missing_in_canonical_count']} "
                     f"extra_in_canonical={d['extra_in_canonical_count']}")
    report.log("PHASE 2 (state preservation): done")


# --------------------------------------------------------------------------- #
# Phase 3: read parity (on the migrated copy)
# --------------------------------------------------------------------------- #
_FRESHNESS_ALLOWED_EXTRA = {
    "stale", "consecutive_failed_runs", "age_seconds", "last_attempt_status", "licenses_absence",
}


def phase3_read_parity(report: Report, sandbox: Path, snapshot: Path) -> None:
    from fastapi import HTTPException

    from backend import canonical_reads
    from backend.db import connect, init_db
    from backend.models import url_to_b64
    from backend.routers import analytics as analytics_router
    from backend.routers import changes as changes_router
    from backend.routers import jobs as jobs_router

    report.log("PHASE 3 (read parity): begin")
    dest = copy_into_sandbox(snapshot, sandbox / "phase3_parity.db", sandbox, label="phase3")
    conn = connect(dest)
    try:
        init_db(conn)

        legacy_jobs = jsonable(jobs_router.list_jobs(min_tier=None, conn=conn))
        canon_jobs = jsonable(canonical_reads.list_jobs(conn, min_tier=None))
        legacy_followups = jsonable(jobs_router.followups(conn=conn))
        canon_followups = jsonable(canonical_reads.followups(conn))
        legacy_changes = jsonable(changes_router.changes(since=None, conn=conn))
        canon_changes = jsonable(canonical_reads.changes(conn, since=None))
        legacy_analytics = jsonable(analytics_router.analytics(conn=conn))
        canon_analytics = jsonable(canonical_reads.analytics(conn))
        legacy_freshness = jsonable(analytics_router.freshness(conn=conn))
        canon_freshness = jsonable(canonical_reads.freshness(conn))

        # N11: sample from the UNION of legacy and canonical URLs, not just
        # legacy's. Sampling only legacy URLs made a canonical-only "ghost"
        # (canonical returns 200 for a URL legacy has never heard of --
        # would legacy 404 while canonical serves nulls) structurally
        # invisible: every sampled URL was, by construction, one legacy
        # already recognised.
        legacy_urls = [j["url"] for j in legacy_jobs.get("jobs", []) if "url" in j]
        canon_urls = [j["url"] for j in canon_jobs.get("jobs", []) if "url" in j]
        canonical_only_urls = sorted(set(canon_urls) - set(legacy_urls))
        sample_urls = legacy_urls[:5] + canonical_only_urls[:3]

        detail_samples = []
        for url in sample_urls:
            try:
                legacy_detail = jsonable(jobs_router.job_detail(url_to_b64(url), conn=conn))
                legacy_status = 200
            except HTTPException as exc:
                legacy_detail = None
                legacy_status = exc.status_code
            canon_detail = jsonable(canonical_reads.job_detail(conn, url))
            canon_status = 200 if canon_detail is not None else 404
            if legacy_status == 200 and canon_status == 200:
                cmp = compare_json(legacy_detail, canon_detail, allowed_extra={"posting_id"})
            else:
                # One or both sides 404: no dict-vs-dict structural walk is
                # possible, but a legacy-404/canonical-200 divergence (a
                # canonical-only ghost) is exactly the case N11 exists to
                # catch, so it is recorded as its own diff kind instead of
                # being silently skipped.
                equal = legacy_status == canon_status
                is_ghost = legacy_status == 404 and canon_status == 200
                kind = "canonical_only_ghost" if is_ghost else "status_mismatch"
                cmp = {
                    "equal": equal, "diff_count": 0 if equal else 1,
                    "samples": [] if equal else [{
                        "path": "$", "kind": kind,
                        "legacy": legacy_status, "canonical": canon_status,
                    }],
                    "legacy_len": None, "canonical_len": None,
                    "missing_in_canonical_count": 0,
                    "extra_in_canonical_count": 1 if is_ghost else 0,
                    "list_lens": [],
                }
            detail_samples.append({
                "url": url, "canonical_only": url in canonical_only_urls,
                "legacy_status": legacy_status, "canonical_status": canon_status, **cmp,
            })
    finally:
        conn.close()

    results = {
        "jobs": compare_json(legacy_jobs, canon_jobs, allowed_extra={"posting_id"}),
        "followups": compare_json(legacy_followups, canon_followups, allowed_extra={"posting_id"}),
        "changes": compare_json(legacy_changes, canon_changes, allowed_extra={"posting_id"}),
        "analytics": compare_json(legacy_analytics, canon_analytics, allowed_extra={"posting_id"}),
        "freshness": compare_json(legacy_freshness, canon_freshness, allowed_extra=_FRESHNESS_ALLOWED_EXTRA),
        "job_detail_sample": detail_samples,
        "job_detail_canonical_only_url_count": len(canonical_only_urls),
    }
    report.phase("3_read_parity", results)
    for name in ("jobs", "followups", "changes", "analytics", "freshness"):
        d = results[name]
        # N12: an endpoint where BOTH sides are empty (every list nested in
        # the response has legacy_len == canonical_len == 0) cannot possibly
        # produce a meaningful diff -- it is vacuously equal, not a proven
        # parity. Flagged in the phase data AND recorded as informational so
        # a bare PASS here is never mistaken for a real comparison (the
        # followups compare is empty-vs-empty on a corpus with no
        # overdue/upcoming jobs, for instance).
        list_lens = d.get("list_lens") or []
        vacuous = bool(list_lens) and all(
            e["legacy_len"] == 0 and e["canonical_len"] == 0 for e in list_lens
        )
        d["vacuous"] = vacuous
        report.check(
            f"phase3_{name}_parity", d["equal"],
            f"{d['diff_count']} diff(s); legacy_len={d['legacy_len']} canonical_len={d['canonical_len']} "
            f"missing_in_canonical={d['missing_in_canonical_count']} "
            f"extra_in_canonical={d['extra_in_canonical_count']}"
            + (" -- VACUOUS: both sides empty" if vacuous else ""),
            informational=vacuous,
        )
        # B4: a standalone lost-row check, independent of diff_count (which
        # mixes missing/extra rows in with in-row value_mismatch/
        # length_mismatch diffs). Before B4's compare_json fix this mattered
        # a great deal: 10 lost reposted rows and 21 lost freshness chips
        # each used to collapse into ONE length_mismatch diff.
        report.check(
            f"phase3_{name}_no_lost_rows",
            d["missing_in_canonical_count"] == 0 and d["extra_in_canonical_count"] == 0,
            f"missing_in_canonical={d['missing_in_canonical_count']} "
            f"extra_in_canonical={d['extra_in_canonical_count']}",
            informational=vacuous,
        )
    if detail_samples:
        detail_ok = all(d["equal"] for d in detail_samples)
        report.check("phase3_job_detail_sample_parity", detail_ok,
                     f"{len(detail_samples)} url(s) sampled "
                     f"({len(canonical_only_urls)} canonical-only)")
    else:
        report.check("phase3_job_detail_sample_parity", True, "no jobs present to sample -- vacuous",
                     informational=True)
    ghost_samples = [d for d in detail_samples if d.get("canonical_only")]
    ghost_diverge_count = sum(
        1 for d in ghost_samples if d["legacy_status"] == 404 and d["canonical_status"] == 200
    )
    report.check(
        "phase3_job_detail_no_canonical_only_ghosts", ghost_diverge_count == 0,
        f"{len(ghost_samples)} canonical-only url(s) sampled, "
        f"{ghost_diverge_count} legacy-404/canonical-200 divergence(s)",
        informational=not ghost_samples,
    )
    report.log("PHASE 3 (read parity): done")


# --------------------------------------------------------------------------- #
# Phase 4: API contracts
# --------------------------------------------------------------------------- #
def _v2_and_runs_status(dest: Path, sample_url: str | None) -> dict:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend import runservice as runservice_mod
    from backend.db import connect as db_connect
    from backend.db import get_db
    from backend.models import url_to_b64
    from backend.routers import readsv2, runsapi

    app = FastAPI()
    app.include_router(readsv2.router, prefix="/api")
    app.include_router(runsapi.router, prefix="/api")

    def _override():
        c = sqlite3.connect(str(dest), check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override
    app.state.run_service = runservice_mod.RunService(connect=lambda: db_connect(dest))

    client = TestClient(app)
    results = {}
    for path in _V2_LIST_PATHS:
        results[path] = client.get(path).status_code
    detail_url_b64 = url_to_b64(sample_url or "https://val45.example/nonexistent")
    results[f"/api/v2/jobs/{{url_b64}}"] = client.get(f"/api/v2/jobs/{detail_url_b64}").status_code
    results["/api/runs"] = client.get("/api/runs").status_code
    return results


def _reads_flag_subprocess(db_path: Path, reads_value: str, sample_url: str | None, sandbox: Path) -> dict:
    """The six flagged legacy paths, hit by a FRESH subprocess with
    JOBHUNT_READS=<reads_value> -- necessary because config.READS_SOURCE is
    read once at import time, so this can't be exercised in-process."""
    script_path = require_within(sandbox / "_val45_reads_probe.py", sandbox, "reads-probe script")
    paths = list(_FLAGGED_LIST_PATHS)
    detail_path = f"/api/jobs/{{url_b64}}"
    script = f"""
import json, sqlite3, sys
sys.path.insert(0, {str(WEBAPP_DIR)!r})
from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend.db import get_db
from backend.models import url_to_b64
from backend.routers import analytics as analytics_router
from backend.routers import changes as changes_router
from backend.routers import jobs as jobs_router

app = FastAPI()
for m in (jobs_router, changes_router, analytics_router):
    app.include_router(m.router, prefix="/api")

def _override():
    c = sqlite3.connect({str(db_path)!r}, check_same_thread=False)
    c.row_factory = sqlite3.Row
    try:
        yield c
    finally:
        c.close()

app.dependency_overrides[get_db] = _override
client = TestClient(app)
out = {{}}
for path in {paths!r}:
    out[path] = client.get(path).status_code
out[{detail_path!r}] = client.get(
    "/api/jobs/" + url_to_b64({(sample_url or "https://val45.example/nonexistent")!r})
).status_code
print(json.dumps(out))
"""
    script_path.write_text(script)
    env = dict(os.environ)
    env["JOBHUNT_READS"] = reads_value
    # N6: pin BOTH flags explicitly on every subprocess this harness spawns --
    # never let a shell-exported JOBHUNT_WRITES leak through unexamined, even
    # though this particular probe never touches a write path. This is the
    # same lesson 4.6/4.7's conftest fence exists for, applied to the
    # harness's own subprocess spawns.
    env["JOBHUNT_WRITES"] = "legacy"
    env["JOBHUNT_DB"] = str(db_path)
    env["JOBHUNT_SKIP_STARTUP_INGEST"] = "1"
    env["PYTHONHASHSEED"] = "0"  # N10: deterministic dict/set iteration order
    result = subprocess.run(
        ["uv", "run", "--frozen", "python", str(script_path)],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=90,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"reads-flag subprocess (JOBHUNT_READS={reads_value}) failed rc={result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    lines = [ln for ln in result.stdout.strip().splitlines() if ln.strip()]
    return json.loads(lines[-1])


#: N9: the three legacy write entry points JOBHUNT_WRITES gates, plus the one
#: read-only endpoint that must stay live under both values.
_WRITE_ENDPOINT_LABELS = (
    "POST /api/refresh/quick", "POST /api/sweep/full", "POST /api/ingest", "GET /api/sweep/progress",
)


def _writes_flag_subprocess(db_path: Path, writes_value: str | None, sandbox: Path) -> dict:
    """POST the three JOBHUNT_WRITES-gated legacy write endpoints plus GET
    /api/sweep/progress, in a FRESH subprocess -- necessary because
    config.WRITES_SOURCE, like config.READS_SOURCE, is read once at import
    (N9).

    Deliberately a BARE FastAPI app (the sweepapi router mounted directly,
    with no CsrfGuard middleware from main.py): the X-App/CSRF guard is out
    of scope for a router-only app, exactly as tests/test_write_flag.py's
    `sweep_app` fixture documents. Were this ever changed to mount the real
    `main.py` app instead, every POST below would need
    `headers={{"x-app": "jobhunt"}}` (config.CSRF_HEADER/CSRF_VALUE) -- noted
    here so that choice stays deliberate rather than a later silent 403.

    The legacy runner is stubbed (a real `runner.start()` would spawn a
    pipeline subprocess) except `/api/ingest`, which runs the REAL `ingest()`
    against an empty RESULTS directory -- a stub there would prove only that
    a stub was called, not that the legacy path still works untouched
    (mirrors test_write_flag.py's own docstring reasoning).
    """
    script_path = require_within(sandbox / "_val45_writes_probe.py", sandbox, "writes-probe script")
    results_dir = require_within(sandbox / "_val45_writes_results", sandbox, "writes-probe results dir")
    results_dir.mkdir(parents=True, exist_ok=True)
    script = f"""
import json, sqlite3, sys
from pathlib import Path
sys.path.insert(0, {str(WEBAPP_DIR)!r})
from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend import config
from backend.db import get_db
from backend.routers import sweepapi

config.RESULTS = Path({str(results_dir)!r})

class _FakeRunner:
    def __init__(self):
        self.starts = []
    async def start(self, kind):
        self.starts.append(kind)
        return True, None
    async def cancel(self):
        pass

async def _one_frame_stream():
    yield 'data: {{"type": "sync"}}\\n\\n'

# Bare app: router only, no CsrfGuard middleware -- no X-App header needed
# (see this function's docstring / test_write_flag.py's sweep_app fixture).
sweepapi.runner = _FakeRunner()
sweepapi.sse_stream = _one_frame_stream

app = FastAPI()
app.include_router(sweepapi.router, prefix="/api")

def _override():
    c = sqlite3.connect({str(db_path)!r}, check_same_thread=False)
    c.row_factory = sqlite3.Row
    try:
        yield c
    finally:
        c.close()

app.dependency_overrides[get_db] = _override
client = TestClient(app)
out = {{}}
out["POST /api/refresh/quick"] = client.post("/api/refresh/quick").status_code
out["POST /api/sweep/full"] = client.post("/api/sweep/full").status_code
out["POST /api/ingest"] = client.post("/api/ingest").status_code
out["GET /api/sweep/progress"] = client.get("/api/sweep/progress").status_code
print(json.dumps(out))
"""
    script_path.write_text(script)
    env = dict(os.environ)
    if writes_value is None:
        # The "default env" case: prove the shipped default (no JOBHUNT_WRITES
        # at all) is legacy -- explicitly POP rather than just not-set, so a
        # shell-exported JOBHUNT_WRITES can never silently leak into what is
        # supposed to be testing absence (N6's pin-explicitly rule applied to
        # "explicitly absent" as much as to "explicitly a value").
        env.pop("JOBHUNT_WRITES", None)
    else:
        env["JOBHUNT_WRITES"] = writes_value
    env["JOBHUNT_READS"] = "legacy"  # N6: pin the OTHER flag too, always
    env["JOBHUNT_DB"] = str(db_path)
    env["JOBHUNT_SKIP_STARTUP_INGEST"] = "1"
    env["PYTHONHASHSEED"] = "0"  # N10
    result = subprocess.run(
        ["uv", "run", "--frozen", "python", str(script_path)],
        cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=90,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"writes-flag subprocess (JOBHUNT_WRITES={writes_value!r}) failed rc={result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    lines = [ln for ln in result.stdout.strip().splitlines() if ln.strip()]
    return json.loads(lines[-1])


def phase4_api_contracts(report: Report, sandbox: Path, snapshot: Path) -> None:
    from backend.db import connect, init_db

    report.log("PHASE 4 (API contracts): begin")
    dest_unmig = copy_into_sandbox(snapshot, sandbox / "phase4_unmigrated.db", sandbox, label="phase4-unmig")
    dest_mig = copy_into_sandbox(snapshot, sandbox / "phase4_migrated.db", sandbox, label="phase4-mig")
    conn = connect(dest_mig)
    try:
        init_db(conn)
        row = conn.execute("SELECT url FROM jobs WHERE present=1 LIMIT 1").fetchone()
        sample_url = row["url"] if row else None
    finally:
        conn.close()

    unmig_v2 = _v2_and_runs_status(dest_unmig, sample_url)
    mig_v2 = _v2_and_runs_status(dest_mig, sample_url)
    unmig_flagged = _reads_flag_subprocess(dest_unmig, "canonical", sample_url, sandbox)
    mig_flagged = _reads_flag_subprocess(dest_mig, "canonical", sample_url, sandbox)
    # N9: JOBHUNT_WRITES probe -- default env must behave exactly like legacy
    # always has (202/200); JOBHUNT_WRITES=canonical must refuse the three
    # write entry points with 409 while leaving progress observable.
    writes_default = _writes_flag_subprocess(dest_mig, None, sandbox)
    writes_canonical = _writes_flag_subprocess(dest_mig, "canonical", sandbox)

    data = {
        "sample_url": sample_url,
        "unmigrated_v2_and_runs_status": unmig_v2,
        "migrated_v2_and_runs_status": mig_v2,
        "unmigrated_flagged_paths_status_canonical": unmig_flagged,
        "migrated_flagged_paths_status_canonical": mig_flagged,
        "writes_flag_default_env_status": writes_default,
        "writes_flag_canonical_status": writes_canonical,
    }
    report.phase("4_api_contracts", data)

    unmig_503_ok = all(v == 503 for v in unmig_v2.values())
    report.check("phase4_unmigrated_v2_and_runs_503", unmig_503_ok, f"{unmig_v2}")

    mig_detail_key = f"/api/v2/jobs/{{url_b64}}"
    mig_200_ok = all(
        v == 200 for k, v in mig_v2.items() if k != mig_detail_key
    ) and (mig_v2[mig_detail_key] == (200 if sample_url else 404))
    report.check("phase4_migrated_v2_and_runs_200", mig_200_ok, f"{mig_v2}")

    unmig_flag_503_ok = all(v == 503 for v in unmig_flagged.values())
    report.check("phase4_unmigrated_flagged_paths_503_under_canonical_flag", unmig_flag_503_ok,
                 f"{unmig_flagged}")

    detail_path = f"/api/jobs/{{url_b64}}"
    mig_flag_200_ok = all(
        v == 200 for k, v in mig_flagged.items() if k != detail_path
    ) and (mig_flagged[detail_path] == (200 if sample_url else 404))
    report.check("phase4_migrated_flagged_paths_200_under_canonical_flag", mig_flag_200_ok,
                 f"{mig_flagged}")

    default_ok = (
        writes_default.get("POST /api/refresh/quick") == 202
        and writes_default.get("POST /api/sweep/full") == 202
        and writes_default.get("POST /api/ingest") == 200
        and writes_default.get("GET /api/sweep/progress") == 200
    )
    report.check("phase4_writes_flag_default_env_legacy_intact", default_ok, f"{writes_default}")

    canonical_ok = (
        writes_canonical.get("POST /api/refresh/quick") == 409
        and writes_canonical.get("POST /api/sweep/full") == 409
        and writes_canonical.get("POST /api/ingest") == 409
        and writes_canonical.get("GET /api/sweep/progress") == 200
    )
    report.check("phase4_writes_flag_canonical_refuses_writes_progress_live", canonical_ok,
                 f"{writes_canonical}")
    report.log("PHASE 4 (API contracts): done")


# --------------------------------------------------------------------------- #
# Phase 5: cancel/restart (on the migrated copy, injected fake transports)
# --------------------------------------------------------------------------- #
def _new_migrated_copy(sandbox: Path, snapshot: Path, name: str):
    from backend.db import connect, init_db

    dest = copy_into_sandbox(snapshot, sandbox / name, sandbox, label=name)
    conn = connect(dest)
    try:
        init_db(conn)
    finally:
        conn.close()
    return dest


def _build_service(connect_factory, profile_doc, **kwargs):
    from backend import runservice
    from backend.sources.scheduler import SchedulerConfig
    from backend.sources.testing import FakeTransport, text_response
    from backend.tests.test_source_enrichment import PERMISSIVE_PROFILE
    from types import SimpleNamespace

    kwargs.setdefault("legacy_runner", SimpleNamespace(running=False))
    kwargs.setdefault("enrichment_transport", FakeTransport(default=text_response("A body.")))
    kwargs.setdefault("profile", PERMISSIVE_PROFILE)
    kwargs.setdefault("scheduler_config", SchedulerConfig(retry_base_delay_seconds=0.01, retry_jitter=0.0))
    return runservice.RunService(connect=connect_factory, profile_doc=profile_doc, **kwargs)


def _pipeline_run_status(connect_factory, run_uid) -> str | None:
    conn = connect_factory()
    try:
        row = conn.execute("SELECT status FROM pipeline_runs WHERE run_uid=?", (run_uid,)).fetchone()
    finally:
        conn.close()
    return row["status"] if row else None


def _load_profile_doc():
    with open(REPO_ROOT / "profile.json") as handle:
        return json.load(handle)


def _parse_sse_frames(body: str) -> tuple[list[int], list[dict]]:
    """SSE text -> (ids, events), dropping comment/heartbeat frames. Mirrors
    tests/test_runservice_api.py's `parse()` helper (B2)."""
    ids: list[int] = []
    events: list[dict] = []
    for frame in (f for f in body.split("\n\n") if f.strip()):
        if frame.startswith(":"):
            continue
        lines = frame.strip().split("\n")
        ids.append(int(lines[0].removeprefix("id:").strip()))
        events.append(json.loads(lines[1].removeprefix("data:").strip()))
    return ids, events


#: B1: the alias_kind `mark_absent_for_scope`/`_OWNED_BY_INSTANCE_SQL` bound
#: its candidate set to (`runstore.SOURCE_REQ_ALIAS_KIND` -- kept as a literal
#: here rather than imported so this module stays readable standalone; the two
#: must agree, and a mismatch here would make every owned-count query below
#: silently return 0, which is exactly the failure mode `phase5a_absence_
#: candidates_owned` exists to catch).
_SOURCE_REQ_ALIAS_KIND = "source_req"


def _max_owning_namespace(dest: Path) -> tuple[str | None, int]:
    """The `posting_aliases` namespace (source_key[:instance_key]) that owns
    the MOST postings in this copy, and its count -- B1's runtime pick, so the
    all-fail fake source/instances can be named to match a namespace that
    genuinely owns something instead of an arbitrary one that owns nothing."""
    from backend.db import connect as db_connect

    conn = db_connect(dest)
    try:
        row = conn.execute(
            "SELECT namespace, COUNT(*) AS n FROM posting_aliases "
            "WHERE alias_kind=? AND valid_to IS NULL GROUP BY namespace "
            "ORDER BY n DESC, namespace LIMIT 1",
            (_SOURCE_REQ_ALIAS_KIND,),
        ).fetchone()
    finally:
        conn.close()
    return (row["namespace"], int(row["n"])) if row else (None, 0)


def _seed_self_check_source_req_alias(dest: Path) -> None:
    """Self-check-only (B1): give ONE existing posting a `source_req` alias so
    `_max_owning_namespace` has something real to find.

    Migrated data (real prod snapshot OR the self-check synthetic DB run
    through the same migration 11 backfill) carries only `alias_kind='url'`
    rows -- no production posting has ever been claimed by a canonical
    scheduler run, so `owned` is legitimately 0 there (see phase5's
    NOT-APPLICABLE handling). That makes the absence-scope invariant
    untestable on any corpus this harness has ever actually seen unless the
    synthetic self-check DB is deliberately extended with one, which is what
    this does -- exercising the "pick the max-owning namespace, run an
    all-fail against it, prove nothing is marked absent" mechanism for real
    at least once, without touching the real snapshot's fidelity.
    """
    from backend.db import connect as db_connect

    conn = db_connect(dest)
    try:
        posting = conn.execute("SELECT posting_id FROM postings ORDER BY posting_id LIMIT 1").fetchone()
        if posting is None:
            return
        pid = posting["posting_id"]
        conn.execute(
            "INSERT OR IGNORE INTO posting_aliases "
            "(alias_id, posting_id, alias_kind, namespace, value, url, req_id, "
            "provenance_json, confidence, valid_from, valid_to) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,NULL)",
            (f"val45-selfcheck-seed-{pid}", pid, _SOURCE_REQ_ALIAS_KIND, "val45-selfcheck:seed",
             "val45-seed-value", None, "val45-seed-req", "{}", 1.0, "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
    finally:
        conn.close()


def phase5_cancel_restart(report: Report, sandbox: Path, snapshot: Path, *, self_check: bool = False) -> None:
    from backend.db import connect as db_connect
    from backend.sources.contract import RunKind
    from backend.tests.test_source_scheduler_fakes import (
        FakeAdapter, descriptor_for, fast, hanging, permanent_always, plan_of,
    )

    report.log("PHASE 5 (cancel/restart): begin")
    profile_doc = _load_profile_doc()
    data: dict = {}

    # (a) all-fail transport: must settle non-succeeded, license nothing, mark
    #     no pre-existing posting absent, and leave job_state byte-identical.
    dest_a = _new_migrated_copy(sandbox, snapshot, "phase5a_allfail.db")
    connect_a = lambda: db_connect(dest_a)  # noqa: E731

    # B1: pick the namespace that owns the MOST postings so the all-fail
    # source's identity genuinely covers something -- `mark_absent_for_scope`
    # bounds its candidate set to postings owned (via an active `source_req`
    # alias) by the exact source/instance under test, so a fake named
    # "val45-allfail" with instances "x"/"y" (owning nothing) made the "no new
    # absence" assertion vacuous: the candidate set was always empty,
    # regardless of whether the licensing/scoping logic was correct.
    namespace, owned = _max_owning_namespace(dest_a)
    if owned == 0 and self_check:
        _seed_self_check_source_req_alias(dest_a)
        namespace, owned = _max_owning_namespace(dest_a)
    if namespace is not None:
        fake_source_key, _sep, fake_instance = namespace.partition(":")
    else:
        fake_source_key, fake_instance = "val45-allfail", "x"
    fake_instances = (fake_instance,) if fake_instance else ("",)

    conn = connect_a()
    try:
        before_absent = conn.execute(
            "SELECT COUNT(*) FROM postings WHERE absent_since IS NOT NULL"
        ).fetchone()[0]
        before_job_state = dump_table(conn, "job_state", "seen_key")
    finally:
        conn.close()

    service_a = _build_service(
        connect_a, profile_doc,
        plan_factory=lambda kind, config: plan_of(
            FakeAdapter(fake_source_key, instances=fake_instances, body=permanent_always())
        ),
    )

    async def _all_fail_scenario():
        # start_run + wait MUST share one event loop -- RunService's run
        # supervisor is a background asyncio.Task on the loop start_run() was
        # called from; a second, separate asyncio.run() for wait() would await
        # it from a different (and by then closed-and-reopened) loop and never
        # observe completion. Every existing scheduler/runservice test drives
        # a whole scenario through exactly one `run(coro)` call for this reason.
        started = await service_a.start_run("daily")
        await service_a.wait(started["run_uid"])
        return started

    result_a = run_async(_all_fail_scenario())
    status_a = _pipeline_run_status(connect_a, result_a["run_uid"])

    conn = connect_a()
    try:
        after_absent = conn.execute(
            "SELECT COUNT(*) FROM postings WHERE absent_since IS NOT NULL"
        ).fetchone()[0]
        after_job_state = dump_table(conn, "job_state", "seen_key")
        presence_event = conn.execute(
            "SELECT payload_json FROM run_events WHERE run_uid=? AND event_type='run.presence_refreshed' "
            "ORDER BY sequence DESC LIMIT 1",
            (result_a["run_uid"],),
        ).fetchone()
    finally:
        conn.close()
    job_state_acct_a = row_accounting(before_job_state, after_job_state, "seen_key")
    licensed_sources = None
    if presence_event is not None and presence_event["payload_json"]:
        licensed_sources = json.loads(presence_event["payload_json"]).get("licensed_sources")

    # B1: the check is inert (never truly exercised) whenever `owned == 0` --
    # this IS expected on a real prod snapshot (migrated postings carry only
    # `alias_kind='url'` rows; no canonical scheduler run has ever claimed
    # one), so it must report NOT-APPLICABLE loudly rather than a bare PASS.
    inert = owned == 0
    data["a_all_fail"] = {
        "run_uid": result_a["run_uid"],
        "final_status": status_a,
        "fake_source_namespace": namespace,
        "absence_candidates_owned": owned,
        "absence_candidates_inert_not_applicable": inert,
        "absent_count_before": before_absent,
        "absent_count_after": after_absent,
        "licensed_sources": licensed_sources,
        "job_state_accounting": job_state_acct_a,
    }
    report.check(
        "phase5a_absence_candidates_owned", owned > 0,
        f"owned={owned} namespace={namespace!r}"
        + ("" if owned > 0 else
           " -- NOT-APPLICABLE: no source_req-owned postings on this corpus (migrated postings "
           "carry only alias_kind='url' rows); the absence-scope invariant below is untested here"),
        informational=inert,
    )
    # B1: a missing run row used to pass this vacuously (`None != "succeeded"`
    # is True). An all-fail run settles "partial" (scheduler.py: run_status is
    # "succeeded" only if every target succeeded or was skipped; "failed" is
    # reserved for a scheduler-internal bug or a writer failure) -- assert the
    # real set of non-succeeded terminal statuses instead of a negation of one.
    report.check("phase5a_all_fail_settles_non_succeeded",
                 status_a in {"failed", "partial", "cancelled"}, f"final status={status_a!r}")
    report.check("phase5a_all_fail_marks_no_new_absence", after_absent == before_absent,
                 f"absent count {before_absent} -> {after_absent} (owned={owned})",
                 informational=inert)
    # B1: a failed source must never be counted as licensed to mark absence.
    # Meaningful regardless of `owned` -- `successful_source_scopes` filters
    # on status='succeeded', which an all-fail run has none of, by
    # construction, independent of whose postings were in scope.
    report.check("phase5a_failed_source_licenses_nothing", licensed_sources == 0,
                 f"licensed_sources={licensed_sources!r} (from the run's run.presence_refreshed "
                 f"event; None means the event was never emitted)")
    report.check("phase5a_all_fail_job_state_byte_identical",
                 not job_state_acct_a["missing_in_post"] and job_state_acct_a["value_diff_count"] == 0,
                 f"missing={len(job_state_acct_a['missing_in_post'])} "
                 f"changed={job_state_acct_a['value_diff_count']}")
    # B3: `added_in_post` (computed all along, never asserted) is the
    # zero-silent-loss signal for spurious rows, not just missing ones.
    report.check("phase5a_job_state_no_added_rows", not job_state_acct_a["added_in_post"],
                 f"added={len(job_state_acct_a['added_in_post'])} "
                 f"pre_count={job_state_acct_a['pre_count']} post_count={job_state_acct_a['post_count']}")
    report.check("phase5a_job_state_pre_post_count_equal",
                 job_state_acct_a["pre_count"] == job_state_acct_a["post_count"],
                 f"pre_count={job_state_acct_a['pre_count']} post_count={job_state_acct_a['post_count']}")

    # (b) hang-then-cancel: must settle cancelled, never succeeded.
    dest_b = _new_migrated_copy(sandbox, snapshot, "phase5b_cancel.db")
    connect_b = lambda: db_connect(dest_b)  # noqa: E731
    service_b = _build_service(
        connect_b, profile_doc,
        plan_factory=lambda kind, config: plan_of(
            FakeAdapter("val45-slow", instances=("acme",), body=hanging(),
                        descriptor=descriptor_for("val45-slow", deadline=30.0))
        ),
    )

    async def _cancel_scenario():
        started = await service_b.start_run("daily")
        await service_b.cancel_run(started["run_uid"])
        await service_b.wait(started["run_uid"])
        return started

    result_b = run_async(_cancel_scenario())
    status_b = _pipeline_run_status(connect_b, result_b["run_uid"])
    data["b_hang_then_cancel"] = {"run_uid": result_b["run_uid"], "final_status": status_b}
    report.check("phase5b_cancel_settles_cancelled", status_b == "cancelled", f"final status={status_b!r}")
    report.check("phase5b_cancel_never_succeeded", status_b != "succeeded", f"final status={status_b!r}")

    # (c) orphan recovery: a run row left 'running' (simulated dead process,
    #     no live process involved) must be marked interrupted at recovery.
    from backend import runservice as runservice_mod

    dest_c = _new_migrated_copy(sandbox, snapshot, "phase5c_orphan.db")
    connect_c = lambda: db_connect(dest_c)  # noqa: E731
    conn = connect_c()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status, requested_at, started_at) "
            "VALUES ('val45-orphan', 'daily', 'running', '2026-08-04T00:00:00+00:00', "
            "'2026-08-04T00:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()
    recovery_report = runservice_mod.recover_orphans_if_canonical(connect_c)
    status_c = _pipeline_run_status(connect_c, "val45-orphan")
    data["c_orphan_recovery"] = {
        "recovered_run_uids": list(recovery_report.run_uids) if recovery_report else [],
        "final_status": status_c,
    }
    report.check("phase5c_orphan_marked_interrupted", status_c == "interrupted",
                 f"final status={status_c!r}")

    # (d) SSE/event replay: B2 exercises the REAL GET /api/runs/{uid}/events
    #     HTTP endpoint (not just the persistence layer directly), with both
    #     Last-Event-ID and ?after=, mirroring the TestClient pattern
    #     tests/test_runservice_api.py uses -- a `with TestClient(...) as
    #     client:` context so the background run task and `client.portal`
    #     share ONE event loop (a second, separate `asyncio.run()` would await
    #     the task from a different loop and never observe completion).
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import runsapi

    dest_d = _new_migrated_copy(sandbox, snapshot, "phase5d_replay.db")
    connect_d = lambda: db_connect(dest_d)  # noqa: E731
    service_d = _build_service(
        connect_d, profile_doc,
        plan_factory=lambda kind, config: plan_of(
            FakeAdapter("val45-replay", instances=("a", "b", "c"), body=fast(2))
        ),
    )
    app_d = FastAPI()
    app_d.include_router(runsapi.router, prefix="/api")
    app_d.state.run_service = service_d

    with TestClient(app_d) as client:
        run_uid_d = client.post("/api/runs", json={"kind": "daily"}).json()["run_uid"]
        client.portal.call(service_d.wait, run_uid_d)

        with client.stream("GET", f"/api/runs/{run_uid_d}/events") as resp_full:
            full_status = resp_full.status_code
            full_body = "".join(resp_full.iter_text())
        sequences, _full_events = _parse_sse_frames(full_body)
        mid = sequences[len(sequences) // 2] if sequences else -1
        expected_tail_sequences = [s for s in sequences if s > mid]

        with client.stream(
            "GET", f"/api/runs/{run_uid_d}/events", headers={"Last-Event-ID": str(mid)}
        ) as resp_lastid:
            lastid_status = resp_lastid.status_code
            lastid_body = "".join(resp_lastid.iter_text())
        lastid_ids, _ = _parse_sse_frames(lastid_body)

        with client.stream(
            "GET", f"/api/runs/{run_uid_d}/events", params={"after": mid}
        ) as resp_after:
            after_status = resp_after.status_code
            after_body = "".join(resp_after.iter_text())
        after_ids, _ = _parse_sse_frames(after_body)

    # B2: `sequences == sorted(sequences)` on an ORDER BY query can NEVER
    # fail -- SQLite already returns the rows in that order, so the assertion
    # was a tautology restating the query itself. `next_event_sequence` is
    # per-run and contiguous from 0 (runstore.py), so the real, falsifiable
    # invariant is equality with the literal contiguous range -- which also
    # subsumes duplicate-freedom (a range has none by construction). The
    # duplicate check is kept as an independent assertion anyway rather than
    # folded away, so a reader sees uniqueness verified on its own terms.
    gap_free = sequences == list(range(len(sequences)))
    dup_free = len(set(sequences)) == len(sequences)
    lastid_resume_matches = lastid_ids == expected_tail_sequences
    after_resume_matches = after_ids == expected_tail_sequences

    data["d_event_replay"] = {
        "run_uid": run_uid_d,
        "probe": "GET /api/runs/{run_uid}/events over real HTTP via TestClient "
                 "(not the persistence layer directly) -- B2",
        "event_count": len(sequences),
        "full_stream_http_status": full_status,
        "sequences_gap_free_contiguous_from_zero": gap_free,
        "sequences_duplicate_free": dup_free,
        "mid_cursor": mid,
        "last_event_id_http_status": lastid_status,
        "last_event_id_resume_matches_tail": lastid_resume_matches,
        "query_after_http_status": after_status,
        "query_after_resume_matches_tail": after_resume_matches,
    }
    report.check("phase5d_http_stream_status_200", full_status == 200, f"status={full_status}")
    report.check("phase5d_events_gap_free_contiguous_from_zero", gap_free,
                 f"{len(sequences)} event(s); sequences={sequences}")
    report.check("phase5d_events_no_duplicates", dup_free, f"{len(sequences)} event(s)")
    report.check("phase5d_last_event_id_resume_matches_tail", lastid_resume_matches,
                 f"mid_cursor={mid}, resumed_len={len(lastid_ids)}, "
                 f"expected_len={len(expected_tail_sequences)}")
    report.check("phase5d_query_after_resume_matches_tail", after_resume_matches,
                 f"mid_cursor={mid}, resumed_len={len(after_ids)}, "
                 f"expected_len={len(expected_tail_sequences)}")

    report.phase("5_cancel_restart", data)
    report.log("PHASE 5 (cancel/restart): done")


# --------------------------------------------------------------------------- #
# Phase 6: performance
# --------------------------------------------------------------------------- #
def _time_reps(fn, reps=20):
    """N12: 20 reps (was 5), p50/p95/max. Nearest-rank percentile -- adequate
    at this sample size, and simple enough that a Haiku-tier runner reading
    the report never has to trust an interpolation choice it can't see."""
    durations = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        durations.append(time.perf_counter() - t0)
    durations.sort()

    def _percentile(p):
        if not durations:
            return None
        idx = min(len(durations) - 1, int(round(p * (len(durations) - 1))))
        return durations[idx]

    return {
        "p50_seconds": _percentile(0.50),
        "p95_seconds": _percentile(0.95),
        "max_seconds": max(durations),
        "reps": durations,
    }


def phase6_performance(report: Report, sandbox: Path, snapshot: Path) -> None:
    from backend import canonical_reads
    from backend.db import connect, init_db
    from backend.routers import analytics as analytics_router
    from backend.routers import changes as changes_router
    from backend.routers import jobs as jobs_router

    report.log("PHASE 6 (performance): begin")
    dest = copy_into_sandbox(snapshot, sandbox / "phase6_migrate_timing.db", sandbox, label="phase6-migrate")
    conn = connect(dest)
    t0 = time.perf_counter()
    init_db(conn)
    migration_duration = time.perf_counter() - t0
    conn.close()

    dest_parity = copy_into_sandbox(snapshot, sandbox / "phase6_endpoints.db", sandbox, label="phase6-endpoints")
    conn = connect(dest_parity)
    init_db(conn)

    # N12: renamed from "endpoint_timings" -- these call the router/
    # canonical_reads FUNCTIONS directly against an already-open connection,
    # never through HTTP, so the numbers exclude FastAPI request routing,
    # dependency injection, and JSON response serialization entirely. Do not
    # read them as end-to-end HTTP latency.
    read_function_timings = {
        "jobs": {
            "legacy": _time_reps(lambda: jobs_router.list_jobs(min_tier=None, conn=conn)),
            "v2": _time_reps(lambda: canonical_reads.list_jobs(conn, min_tier=None)),
        },
        "followups": {
            "legacy": _time_reps(lambda: jobs_router.followups(conn=conn)),
            "v2": _time_reps(lambda: canonical_reads.followups(conn)),
        },
        "changes": {
            "legacy": _time_reps(lambda: changes_router.changes(since=None, conn=conn)),
            "v2": _time_reps(lambda: canonical_reads.changes(conn, since=None)),
        },
        "analytics": {
            "legacy": _time_reps(lambda: analytics_router.analytics(conn=conn)),
            "v2": _time_reps(lambda: canonical_reads.analytics(conn)),
        },
        "freshness": {
            "legacy": _time_reps(lambda: analytics_router.freshness(conn=conn)),
            "v2": _time_reps(lambda: canonical_reads.freshness(conn)),
        },
    }
    conn.close()

    data = {
        "migration_duration_seconds": migration_duration,
        "read_function_timings": read_function_timings,
    }
    # N8: these are measurement-only records (literal-True facts), not
    # asserted invariants -- marked informational so the DONE tally never
    # mistakes "a timing was recorded" for "a check passed".
    report.check("phase6_migration_duration_recorded", migration_duration >= 0,
                 f"{migration_duration:.3f}s", informational=True)
    for name in read_function_timings:
        t = read_function_timings[name]
        report.check(
            f"phase6_{name}_timings_recorded", True,
            f"legacy p50={t['legacy']['p50_seconds']:.4f}s p95={t['legacy']['p95_seconds']:.4f}s, "
            f"v2 p50={t['v2']['p50_seconds']:.4f}s p95={t['v2']['p95_seconds']:.4f}s",
            informational=True,
        )

    # Writer-contention probe: daily + aggregators lanes concurrently, real
    # (fast, non-hanging) fake adapters, on the SAME db file -- reusing the
    # exact concurrency pattern test_runservice.py's mutual-exclusivity test
    # already exercises for these two lanes.
    contention = _writer_contention_probe(sandbox, snapshot, report)
    data["writer_contention_probe"] = contention
    report.phase("6_performance", data)
    report.log("PHASE 6 (performance): done")


def _writer_contention_probe(sandbox: Path, snapshot: Path, report: Report) -> dict:
    try:
        from backend.db import connect as db_connect
        from backend.sources import writer as writer_module
        from backend.sources.contract import RunKind
        from backend.tests.test_source_scheduler_fakes import FakeAdapter, descriptor_for, fast, plan_of

        dest = _new_migrated_copy(sandbox, snapshot, "phase6_contention.db")
        connect_fn = lambda: db_connect(dest)  # noqa: E731
        profile_doc = _load_profile_doc()

        def plan_factory(kind, config):
            if str(kind) == "aggregators":
                return plan_of(FakeAdapter(
                    "val45-agg", instances=("a1", "a2", "a3"), body=fast(2),
                    descriptor=descriptor_for("val45-agg", run_kinds=frozenset({RunKind.AGGREGATORS})),
                ))
            return plan_of(FakeAdapter("val45-daily", instances=("d1", "d2", "d3"), body=fast(2)))

        service = _build_service(connect_fn, profile_doc, plan_factory=plan_factory)

        writers: list = []
        original_start = writer_module.SqliteWriter.start

        async def capturing_start(self):
            await original_start(self)
            writers.append(self)

        writer_module.SqliteWriter.start = capturing_start
        try:
            async def scenario():
                first = await service.start_run("daily")
                second = await service.start_run("aggregators")
                await service.wait(first["run_uid"])
                await service.wait(second["run_uid"])
                return first, second

            first, second = run_async(scenario(), timeout=60.0)
        finally:
            writer_module.SqliteWriter.start = original_start

        writer_stats = [
            {
                "busy_retries": w.stats.busy_retries,
                "transactions": w.stats.transactions,
                "unclosed_connections": w.stats.unclosed_connections,
                "drain_timeouts": w.stats.drain_timeouts,
            }
            for w in writers
        ]
        total_unclosed = sum(w["unclosed_connections"] for w in writer_stats)
        result = {
            "skipped": False,
            "daily_run_uid": first["run_uid"],
            "aggregators_run_uid": second["run_uid"],
            "daily_status": _pipeline_run_status(connect_fn, first["run_uid"]),
            "aggregators_status": _pipeline_run_status(connect_fn, second["run_uid"]),
            "writer_instances_observed": len(writers),
            "writer_stats": writer_stats,
            "total_unclosed_connections": total_unclosed,
        }
        report.check("phase6_writer_contention_probe_ran", True,
                     f"{len(writers)} writer instance(s) observed")
        # N12: daily + aggregators are separate lanes (independent writer
        # queues by design -- see the phase's own docstring), so a genuine
        # concurrent-writer probe must observe at least two DISTINCT writer
        # instances; one would mean the two runs serialized onto a single
        # writer and the "contention" this probes for never happened.
        report.check("phase6_writer_instances_observed_at_least_two", len(writers) >= 2,
                     f"{len(writers)} writer instance(s) observed")
        report.check("phase6_writer_stats_unclosed_connections_zero", total_unclosed == 0,
                     f"total unclosed_connections={total_unclosed}")
        return result
    except Exception as exc:  # noqa: BLE001 - an optional probe must not fail the harness
        report.log(f"PHASE 6: writer-contention probe SKIPPED ({exc!r})")
        report.check("phase6_writer_contention_probe_ran", False,
                     f"skipped: {exc!r} (not a validation failure -- see contract's explicit skip allowance)")
        return {"skipped": True, "reason": repr(exc)}


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run_all_phases(report: Report, sandbox: Path, snapshot: Path, *, self_check: bool = False) -> None:
    phase1_migrate(report, sandbox, snapshot)
    phase2_state_preservation(report, sandbox, snapshot)
    phase3_read_parity(report, sandbox, snapshot)
    phase4_api_contracts(report, sandbox, snapshot)
    phase5_cancel_restart(report, sandbox, snapshot, self_check=self_check)
    phase6_performance(report, sandbox, snapshot)
