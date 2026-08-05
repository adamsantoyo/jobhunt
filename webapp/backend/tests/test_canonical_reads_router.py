"""Phase 4, W-4.2: the /v2 router (`routers/readsv2.py`) -- HTTP wiring, 503 on a
non-canonical database, 404 on an unknown job, and DTO key-set parity with the
legacy /api/* endpoints (pinned from models.py / frontend/src/api/types.ts,
add-only extra keys like `posting_id` permitted).

Not mounted in main.py (the orchestrator does that once both wave-1 tasks land),
so every test here builds its own local FastAPI app, exactly as instructed by
the phase 4 spec ("your tests mount it on a local FastAPI app"). Never touches
webapp/app.db (repo-root conftest.py fences JOBHUNT_DB).
"""
import sqlite3

import pytest

from backend.db import DDL as LEGACY_DDL
from backend.db import connect, get_db, init_db
from backend.models import JobLight, JobState
from backend.tests.test_canonical_reads import (
    AT,
    deliver,
    ensure_profile,
    insert_legacy_current,
    insert_score,
    posting_id_for_url,
    record,
    version_id_for,
)


@pytest.fixture
def api(tmp_path):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import readsv2

    db_path = tmp_path / "readsv2_test.db"
    conn = connect(db_path)
    init_db(conn)

    app = FastAPI()
    app.include_router(readsv2.router, prefix="/api")

    def _override():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override
    try:
        yield TestClient(app), conn
    finally:
        app.dependency_overrides.pop(get_db, None)
        conn.close()


@pytest.fixture
def legacy_only_api(tmp_path):
    """A database with only the pre-canonical (legacy) tables -- no `postings`,
    `pipeline_runs`, etc. Every v2 route must 503 against it."""
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import readsv2

    db_path = tmp_path / "legacy_only.db"
    conn = connect(db_path)
    conn.executescript(LEGACY_DDL)
    conn.commit()
    conn.close()

    app = FastAPI()
    app.include_router(readsv2.router, prefix="/api")

    def _override():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override
    return TestClient(app)


# --------------------------------------------------------------------------- #
# 503 on a database without the canonical schema
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("path", [
    "/api/v2/jobs", "/api/v2/followups", "/api/v2/changes",
    "/api/v2/analytics", "/api/v2/freshness",
    "/api/v2/jobs/aHR0cHM6Ly94",
])
def test_v2_routes_503_without_canonical_schema(legacy_only_api, path):
    res = legacy_only_api.get(path)
    assert res.status_code == 503


# --------------------------------------------------------------------------- #
# 404 on an unknown job
# --------------------------------------------------------------------------- #
def test_job_detail_404_for_unknown_url(api):
    client, _conn = api
    from backend.models import url_to_b64

    res = client.get(f"/api/v2/jobs/{url_to_b64('https://nowhere.example/x')}")
    assert res.status_code == 404


def test_job_detail_404_for_malformed_b64(api):
    client, _conn = api
    res = client.get("/api/v2/jobs/not-valid-base64!!!")
    assert res.status_code == 404


# --------------------------------------------------------------------------- #
# DTO key-set parity (add-only extra keys permitted)
# --------------------------------------------------------------------------- #
def test_job_list_key_set_is_a_superset_of_legacy_joblight(api):
    client, conn = api
    deliver(conn, "run-1", {
        "greenhouse:acme": [record("greenhouse:acme", url="https://acme.example/kv-1", req_id="kv1")],
    }, requested_at=AT)
    conn.commit()

    res = client.get("/api/v2/jobs")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"run_date", "jobs"}
    assert len(body["jobs"]) == 1
    job_keys = set(body["jobs"][0].keys())
    assert set(JobLight.model_fields) <= job_keys
    assert "posting_id" in job_keys  # the spec's example add-only field
    assert job_keys - set(JobLight.model_fields) == {"posting_id"}  # add-only, nothing else


def test_job_detail_key_set_matches_jobfull(api):
    client, conn = api
    pid = "detail-kv-1"
    insert_legacy_current(conn, pid, title="X", company="Y")
    conn.commit()
    url = f"https://legacy.example/{pid}"

    from backend.models import url_to_b64
    res = client.get(f"/api/v2/jobs/{url_to_b64(url)}")
    assert res.status_code == 200
    body = res.json()
    expected = set(JobLight.model_fields) | {"full_desc", "skill_hits", "posting_id"}
    assert set(body.keys()) == expected


def test_state_sub_object_key_set_matches_jobstate(api):
    client, conn = api
    pid = "state-kv-1"
    insert_legacy_current(conn, pid, title="X", company="Y")
    conn.execute(
        "INSERT INTO job_state (seen_key, posting_id, status, updated_at) VALUES (?,?,?,?)",
        ("sk-kv-1", pid, "Applied", AT),
    )
    conn.commit()

    res = client.get("/api/v2/jobs")
    job = next(j for j in res.json()["jobs"] if j["posting_id"] == pid)
    assert set(job["state"].keys()) == set(JobState.model_fields)


def test_changes_response_key_set(api):
    client, conn = api
    res = client.get("/api/v2/changes")
    assert res.status_code == 200
    assert set(res.json().keys()) == {"baseline", "current", "new", "reposted", "tier_changed", "disappeared"}


def test_analytics_response_key_set(api):
    client, _conn = api
    res = client.get("/api/v2/analytics")
    assert res.status_code == 200
    assert set(res.json().keys()) == {
        "funnel", "tiers", "odds", "matrix", "by_source", "new_per_run",
        "comp", "followups", "statuses",
    }


def test_freshness_response_key_set(api):
    client, _conn = api
    res = client.get("/api/v2/freshness")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {
        "latest_run", "ingested_at", "kept", "new_this_run", "sources",
        "zero_row_sources", "stale_refresh_sources", "sweep",
    }
    assert set(body["sweep"].keys()) == {"running", "kind", "step", "done", "total"}


def test_followups_response_key_set(api):
    client, _conn = api
    res = client.get("/api/v2/followups")
    assert res.status_code == 200
    assert set(res.json().keys()) == {"overdue", "upcoming"}


# --------------------------------------------------------------------------- #
# min_tier query param wiring
# --------------------------------------------------------------------------- #
def test_jobs_min_tier_query_param(api):
    client, conn = api
    insert_legacy_current(conn, "mt-low", title="Low", company="A", tier=2)
    insert_legacy_current(conn, "mt-high", title="High", company="B", tier=4)
    conn.commit()

    res = client.get("/api/v2/jobs", params={"min_tier": 3})
    assert res.status_code == 200
    ids = {j["posting_id"] for j in res.json()["jobs"]}
    assert ids == {"mt-high"}
