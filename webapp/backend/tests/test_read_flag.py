"""Phase 4, wave 2, task 4.6: the `JOBHUNT_READS` read flag.

Covers (a)-(d) of the task's test matrix; (e) (the import-time error for an
invalid env value) lives in `test_read_flag_import.py` since it needs a fresh
process, not just a monkeypatched attribute.

`config.READS_SOURCE` is read once at import time in production (see config.py's
module docstring), but every router handler re-reads `config.READS_SOURCE` as an
attribute lookup on every request rather than capturing it into a local at import
-- so `monkeypatch.setattr(config, "READS_SOURCE", ...)` changes behavior for the
next request exactly as the env var would have changed it for the next process,
without needing a subprocess per test. All three dispatching routers (jobs.py,
changes.py, analytics.py) import the same `backend.config` and `backend.
canonical_reads` module objects, so one monkeypatch on either reaches all of them.

Fixture DBs:
- `legacy_app`: a full-schema db (`db.init_db` -- legacy tables AND the empty
  canonical tables it always creates) with data written ONLY through the legacy
  `jobs`/`job_state`/`runs` tables, exactly as the pre-4.6 routers expect.
- `canonical_app`: the same full schema, but data written ONLY through the real
  canonical write path (`runstore.write_records` via the `deliver`/`record`
  helpers from `test_canonical_reads.py`, plus `insert_score`) -- the legacy
  `jobs` table stays empty. A response that contains this data is proof the
  canonical path answered, not the legacy SQL (which would see nothing).
- `legacy_only_app`: raw `db.DDL` only, no canonical tables at all -- what
  `flag=canonical` must 503 against.

Never touches webapp/app.db (repo-root conftest.py fences JOBHUNT_DB).
"""
import sqlite3

import pytest

from backend import canonical_reads, config
from backend.db import DDL as LEGACY_DDL
from backend.db import connect, get_db, init_db
from backend.models import url_to_b64
from backend.tests.test_canonical_reads import AT, deliver, insert_score, posting_id_for_url, record

# --------------------------------------------------------------------------- #
# Legacy-table fixture helpers
# --------------------------------------------------------------------------- #
def insert_legacy_job(conn, url, *, seen_key=None, tier=3, odds="Strong match / Standard",
                       odds_score=70, title="Legacy Support Eng", company="LegacyCo",
                       location="SF", source="greenhouse", present=1,
                       salary_min=None, salary_max=None, full_desc=None,
                       latest_run=None, flags="", why="legacy why"):
    seen_key = seen_key or f"sk-{url}"
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier, odds, odds_score, odds_why, title, company, "
        "location, salary_min, salary_max, source, full_desc, latest_run, present, flags, why) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (url, seen_key, tier, odds, odds_score, "legacy odds why", title, company, location,
         salary_min, salary_max, source, full_desc, latest_run, present, flags, why),
    )
    return seen_key


def insert_legacy_run(conn, run_date, *, kept=1, new_this_run=1, ingested_at=AT):
    conn.execute(
        "INSERT INTO runs (run_date, kept, new_this_run, ingested_at) VALUES (?,?,?,?)",
        (run_date, kept, new_this_run, ingested_at),
    )


def insert_legacy_state(conn, seen_key, url, *, status="Applied", follow_up_date=None,
                         hidden=0, updated_at=AT):
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, follow_up_date, hidden, updated_at) "
        "VALUES (?,?,?,?,?,?)",
        (seen_key, url, status, follow_up_date, hidden, updated_at),
    )


def _boom(*_args, **_kwargs):
    raise AssertionError("canonical_reads was invoked while READS_SOURCE == 'legacy'")


def _jsonable(value):
    """Recursively `model_dump()` any pydantic models inside a plain dict/list,
    so a direct legacy-function call result (JobLight/JobFull objects nested in
    dicts) compares equal to the same data after an HTTP round trip (plain JSON
    dicts)."""
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump())
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


# --------------------------------------------------------------------------- #
# App builders
# --------------------------------------------------------------------------- #
def _build_app(extra_routers=()):
    pytest.importorskip("httpx")
    from fastapi import FastAPI

    from backend.routers import analytics as analytics_router
    from backend.routers import changes as changes_router
    from backend.routers import jobs as jobs_router

    app = FastAPI()
    for module in (jobs_router, changes_router, analytics_router, *extra_routers):
        app.include_router(module.router, prefix="/api")
    return app


def _client_for(db_path, extra_routers=()):
    from fastapi.testclient import TestClient

    app = _build_app(extra_routers)

    def _override():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override
    return TestClient(app)


@pytest.fixture
def legacy_app(tmp_path):
    """Full schema, data written only via legacy tables."""
    db_path = tmp_path / "legacy.db"
    conn = connect(db_path)
    init_db(conn)

    insert_legacy_run(conn, "2026-08-01")
    url = "https://legacy.example/job-1"
    seen_key = insert_legacy_job(conn, url, tier=4)
    insert_legacy_state(conn, seen_key, url, status="Applied", follow_up_date="2020-01-01")
    conn.commit()

    client = _client_for(db_path)
    yield client, conn, url
    conn.close()


@pytest.fixture
def canonical_app(tmp_path):
    """Full schema, data written only via the real canonical write path. The
    legacy `jobs` table is left empty on purpose (see module docstring)."""
    db_path = tmp_path / "canonical.db"
    conn = connect(db_path)
    init_db(conn)

    url = "https://canonical.example/kv-1"
    deliver(conn, "run-1", {
        "greenhouse:acme": [record("greenhouse:acme", url=url, title="Canonical Support Eng")],
    }, requested_at=AT)
    pid = posting_id_for_url(conn, url)
    vid_row = conn.execute(
        "SELECT posting_version_id FROM posting_versions WHERE posting_id=?", (pid,)
    ).fetchone()
    insert_score(conn, posting_version_id=vid_row["posting_version_id"], posting_id=pid,
                 tier=5, odds="Strong match / Standard")
    conn.commit()

    client = _client_for(db_path)
    yield client, conn, url, pid
    conn.close()


@pytest.fixture
def legacy_only_client(tmp_path):
    """Raw legacy DDL only -- no canonical tables at all."""
    db_path = tmp_path / "legacy_only.db"
    conn = connect(db_path)
    conn.executescript(LEGACY_DDL)
    conn.commit()
    conn.close()
    return _client_for(db_path)


@pytest.fixture
def out_of_scope_client(tmp_path):
    """A legacy-only db (canonical schema entirely absent) with funnel + config
    mounted alongside the scoped routers, to prove those two stay legacy no
    matter what READS_SOURCE says. Includes state_events/job_state-archive (what
    `db.init_db` always creates, canonical-gating aside -- funnel.py needs
    state_events) but deliberately NOT `CANONICAL_DDL` (fresh-only, gated), so
    the canonical schema stays absent here exactly like `legacy_only_client`."""
    from backend.migrations import JOB_STATE_ARCHIVE_DDL, STATE_EVENTS_CANONICAL_DDL
    from backend.routers import configapi, funnel

    db_path = tmp_path / "out_of_scope.db"
    conn = connect(db_path)
    conn.executescript(LEGACY_DDL)
    conn.executescript(STATE_EVENTS_CANONICAL_DDL)
    conn.executescript(JOB_STATE_ARCHIVE_DDL)
    conn.commit()
    conn.close()
    return _client_for(db_path, extra_routers=(funnel, configapi))


# --------------------------------------------------------------------------- #
# (a) flag=legacy (default): byte-identical to pre-4.6 legacy behavior, and
# canonical_reads is never invoked.
# --------------------------------------------------------------------------- #
def test_legacy_flag_list_jobs_matches_direct_legacy_call_and_skips_canonical(legacy_app, monkeypatch):
    client, conn, _url = legacy_app
    monkeypatch.setattr(config, "READS_SOURCE", "legacy")
    monkeypatch.setattr(canonical_reads, "list_jobs", _boom)

    from backend.routers.jobs import list_jobs as legacy_list_jobs
    direct = legacy_list_jobs(min_tier=None, conn=conn)

    res = client.get("/api/jobs")
    assert res.status_code == 200
    assert res.json() == _jsonable(direct)


def test_legacy_flag_followups_matches_direct_legacy_call_and_skips_canonical(legacy_app, monkeypatch):
    client, conn, _url = legacy_app
    monkeypatch.setattr(config, "READS_SOURCE", "legacy")
    monkeypatch.setattr(canonical_reads, "followups", _boom)

    from backend.routers.jobs import followups as legacy_followups
    direct = legacy_followups(conn=conn)

    res = client.get("/api/followups")
    assert res.status_code == 200
    assert res.json() == _jsonable(direct)


def test_legacy_flag_job_detail_matches_direct_legacy_call_and_skips_canonical(legacy_app, monkeypatch):
    client, conn, url = legacy_app
    monkeypatch.setattr(config, "READS_SOURCE", "legacy")
    monkeypatch.setattr(canonical_reads, "job_detail", _boom)

    from backend.routers.jobs import job_detail as legacy_job_detail
    direct = legacy_job_detail(url_to_b64(url), conn=conn)

    res = client.get(f"/api/jobs/{url_to_b64(url)}")
    assert res.status_code == 200
    assert res.json() == direct.model_dump()


def test_legacy_flag_changes_matches_direct_legacy_call_and_skips_canonical(legacy_app, monkeypatch):
    client, conn, _url = legacy_app
    monkeypatch.setattr(config, "READS_SOURCE", "legacy")
    monkeypatch.setattr(canonical_reads, "changes", _boom)

    from backend.routers.changes import changes as legacy_changes
    direct = legacy_changes(since=None, conn=conn)

    res = client.get("/api/changes")
    assert res.status_code == 200
    assert res.json() == _jsonable(direct)


def test_legacy_flag_analytics_matches_direct_legacy_call_and_skips_canonical(legacy_app, monkeypatch):
    client, conn, _url = legacy_app
    monkeypatch.setattr(config, "READS_SOURCE", "legacy")
    monkeypatch.setattr(canonical_reads, "analytics", _boom)

    from backend.routers.analytics import analytics as legacy_analytics
    direct = legacy_analytics(conn=conn)

    res = client.get("/api/analytics")
    assert res.status_code == 200
    assert res.json() == _jsonable(direct)


def test_legacy_flag_freshness_matches_direct_legacy_call_and_skips_canonical(legacy_app, monkeypatch):
    client, conn, _url = legacy_app
    monkeypatch.setattr(config, "READS_SOURCE", "legacy")
    monkeypatch.setattr(canonical_reads, "freshness", _boom)

    from backend.routers.analytics import freshness as legacy_freshness
    direct = legacy_freshness(conn=conn)

    res = client.get("/api/freshness")
    assert res.status_code == 200
    assert res.json() == _jsonable(direct)


# --------------------------------------------------------------------------- #
# (b) flag=canonical on a canonical fixture db: canonical payloads come back,
# proven by data that exists ONLY in the canonical tables (the legacy `jobs`
# table is empty in this fixture -- see canonical_app's docstring).
# --------------------------------------------------------------------------- #
def test_canonical_flag_list_jobs_returns_canonical_only_data(canonical_app, monkeypatch):
    client, _conn, _url, pid = canonical_app
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")

    res = client.get("/api/jobs")
    assert res.status_code == 200
    body = res.json()
    assert len(body["jobs"]) == 1
    assert body["jobs"][0]["posting_id"] == pid
    assert body["jobs"][0]["title"] == "Canonical Support Eng"
    assert body["jobs"][0]["tier"] == 5


def test_canonical_flag_job_detail_returns_canonical_only_data(canonical_app, monkeypatch):
    # NOTE: /api/jobs/{url_b64} declares `response_model=JobFull` (pre-existing,
    # untouched by 4.6), so FastAPI filters the canonical_reads dict down to
    # JobFull's own fields -- the add-only `posting_id` key does NOT survive
    # this particular route even under flag=canonical, unlike /api/jobs and
    # /api/v2/jobs/{url_b64} (readsv2, no response_model). Title/tier are the
    # sentinel here instead: both come only from the canonical write path.
    client, _conn, url, _pid = canonical_app
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")

    res = client.get(f"/api/jobs/{url_to_b64(url)}")
    assert res.status_code == 200
    body = res.json()
    assert "posting_id" not in body  # response_model=JobFull strips add-only fields
    assert body["title"] == "Canonical Support Eng"
    assert body["tier"] == 5
    assert "skill_hits" in body  # JobFull-shaped, from canonical_reads.job_detail


def test_canonical_flag_analytics_reflects_canonical_only_data(canonical_app, monkeypatch):
    client, _conn, _url, _pid = canonical_app
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")

    res = client.get("/api/analytics")
    assert res.status_code == 200
    body = res.json()
    assert body["tiers"] == {"5": 1}


def test_canonical_flag_followups_empty_without_state(canonical_app, monkeypatch):
    # No job_state row exists for the canonical posting, so followups should be
    # empty either way -- this pins that the canonical dispatch runs cleanly
    # (no 500) even with nothing to surface.
    client, _conn, _url, _pid = canonical_app
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")

    res = client.get("/api/followups")
    assert res.status_code == 200
    assert res.json() == {"overdue": [], "upcoming": []}


def test_canonical_flag_changes_response_shape(canonical_app, monkeypatch):
    client, _conn, _url, _pid = canonical_app
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")

    res = client.get("/api/changes")
    assert res.status_code == 200
    assert set(res.json().keys()) == {"baseline", "current", "new", "reposted", "tier_changed", "disappeared"}


def test_canonical_flag_freshness_response_shape(canonical_app, monkeypatch):
    client, _conn, _url, _pid = canonical_app
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")

    res = client.get("/api/freshness")
    assert res.status_code == 200
    assert "sources" in res.json()


# --------------------------------------------------------------------------- #
# (c) flag=canonical against a legacy-only db (no canonical schema) -> 503 for
# every scoped endpoint.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("method,path", [
    ("get", "/api/jobs"),
    ("get", "/api/followups"),
    ("get", f"/api/jobs/{url_to_b64('https://nowhere.example/x')}"),
    ("get", "/api/changes"),
    ("get", "/api/analytics"),
    ("get", "/api/freshness"),
])
def test_canonical_flag_503_on_legacy_only_db(legacy_only_client, monkeypatch, method, path):
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")
    res = getattr(legacy_only_client, method)(path)
    assert res.status_code == 503


# --------------------------------------------------------------------------- #
# (d) out-of-scope endpoints (/api/funnel, /api/config) unaffected by the flag,
# even against a db with no canonical schema at all.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("flag", ["legacy", "canonical"])
@pytest.mark.parametrize("path", ["/api/funnel", "/api/config"])
def test_out_of_scope_endpoints_unaffected_by_flag(out_of_scope_client, monkeypatch, flag, path):
    monkeypatch.setattr(config, "READS_SOURCE", flag)
    res = out_of_scope_client.get(path)
    assert res.status_code == 200
