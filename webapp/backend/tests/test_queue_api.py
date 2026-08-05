"""Task 5.1: GET /api/queue/today (`routers/queueapi.py`) -- HTTP wiring.

The router is not mounted in main.py (the Phase 5 integration session does
that, exactly as wave 4.1/4.2 handled readsv2), so every test builds its own
local FastAPI app. Never touches webapp/app.db (repo-root conftest.py fences
JOBHUNT_DB). Response-shape keys are pinned here: this payload is the contract
the Today frontend flip and 5.2's recommendation snapshots build against.
"""
import sqlite3

import pytest

from backend.db import DDL as LEGACY_DDL
from backend.db import connect, get_db, init_db
from backend.models import JobLight, today_iso, url_to_b64


def _make_app():
    from fastapi import FastAPI

    from backend.routers import queueapi

    app = FastAPI()
    app.include_router(queueapi.router, prefix="/api")
    return app


def _client_for(db_path):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = _make_app()

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
def api(tmp_path):
    db_path = tmp_path / "queue_test.db"
    conn = connect(db_path)
    init_db(conn)
    try:
        yield _client_for(db_path), conn
    finally:
        conn.close()


def insert_job(conn, url, *, tier=4, odds="Strong match / Lower bar",
               odds_score=90, company="Acme", title="Engineer",
               posted=None, first_seen=None, present=1, full_desc="a description"):
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier, odds, odds_score, is_new, title, "
        "company, posted, first_seen, remote, present, full_desc) "
        "VALUES (?,?,?,?,?,0,?,?,?,?,0,?,?)",
        (url, url, tier, odds, odds_score, title, company, posted,
         first_seen or today_iso(), present, full_desc),
    )
    conn.commit()


def test_happy_path_shape_and_ranking(api):
    client, conn = api
    insert_job(conn, "https://x.example/1", odds_score=95, company="c1")
    insert_job(conn, "https://x.example/2", odds_score=90, company="c2")
    insert_job(conn, "https://x.example/gone", present=0)

    res = client.get("/api/queue/today")
    assert res.status_code == 200
    body = res.json()
    # top-level contract, pinned
    assert set(body.keys()) == {
        "generated_for", "cap", "queue", "excluded", "excluded_counts", "considered",
    }
    assert body["generated_for"] == today_iso()
    assert body["cap"] == 10  # config.DEFAULT_DAILY_QUEUE_SIZE, no setting stored
    assert body["considered"] == 2  # present=0 never reaches the ranker
    assert [e["job"]["url"] for e in body["queue"]] == [
        "https://x.example/1", "https://x.example/2",
    ]
    # entry contract, pinned
    entry = body["queue"][0]
    assert set(entry.keys()) == {"job", "rank", "lane", "lane_rank", "evidence"}
    assert entry["rank"] == 1
    assert set(entry["evidence"].keys()) == {
        "lane", "lane_rank", "match_band", "competition_band", "odds_score",
        "tier", "freshness", "uncertainty", "why",
    }
    # the job payload is a full JobLight
    assert set(JobLight.model_fields.keys()) <= set(entry["job"].keys())


def test_cap_from_app_settings_and_query_override(api):
    client, conn = api
    for i in range(5):
        insert_job(conn, f"https://x.example/{i}", odds_score=90 - i,
                   company=f"c{i}")
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES ('daily_queue_size', '3')"
    )
    conn.commit()

    body = client.get("/api/queue/today").json()
    assert body["cap"] == 3
    assert len(body["queue"]) == 3
    assert body["excluded_counts"] == {"beyond-cap": 2}

    body = client.get("/api/queue/today?cap=1").json()
    assert body["cap"] == 1
    assert len(body["queue"]) == 1

    # cap=0 is legal: a finished day still gets its exclusion accounting
    body = client.get("/api/queue/today?cap=0").json()
    assert body["queue"] == []
    assert body["excluded_counts"] == {"beyond-cap": 5}

    assert client.get("/api/queue/today?cap=101").status_code == 422


def test_state_and_staleness_reach_exclusion_accounting(api):
    client, conn = api
    insert_job(conn, "https://x.example/applied", company="c1")
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) "
        "VALUES (?,?,?,?)",
        ("https://x.example/applied", "https://x.example/applied", "Applied",
         "2026-01-01T00:00:00"),
    )
    insert_job(conn, "https://x.example/stale", company="c2",
               posted="2020-01-01", first_seen="2020-01-01")
    insert_job(conn, "https://x.example/live", company="c3")
    conn.commit()

    body = client.get("/api/queue/today").json()
    assert [e["job"]["url"] for e in body["queue"]] == ["https://x.example/live"]
    assert body["excluded_counts"] == {"not-new": 1, "stale-posting": 1}
    (row,) = body["excluded"]
    assert row["url_b64"] == url_to_b64("https://x.example/stale")
    assert row["reason"] == "stale-posting"
    assert set(row.keys()) == {"url_b64", "title", "company", "reason", "detail"}


def test_canonical_flag_503_on_legacy_only_db(tmp_path, monkeypatch):
    from backend import config

    db_path = tmp_path / "legacy_only.db"
    conn = connect(db_path)
    conn.executescript(LEGACY_DDL)
    conn.commit()
    conn.close()

    client = _client_for(db_path)
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")
    res = client.get("/api/queue/today")
    assert res.status_code == 503


def test_canonical_flag_dispatches_to_canonical_reads(api, monkeypatch):
    """flag=canonical + canonical schema: the queue is built from
    canonical_reads.list_jobs, not the legacy jobs table -- a legacy-only row
    must not appear."""
    from backend import config

    client, conn = api
    insert_job(conn, "https://x.example/legacy-only")
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")
    res = client.get("/api/queue/today")
    assert res.status_code == 200
    body = res.json()
    assert body["queue"] == []
    assert body["considered"] == 0
