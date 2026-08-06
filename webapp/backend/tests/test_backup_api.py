"""Task 5.6: HTTP wiring for `routers/backupapi.py`.

NOT mounted in main.py (the orchestrator wires that), so every test builds its
own local FastAPI app + TestClient -- the pattern `test_calibration_api.py`
and `test_outcomes_api.py` establish. Nothing here touches webapp/app.db or
the real webapp/backups (repo-root conftest.py fences JOBHUNT_DB; the source
path and dest dir are also both overridden per-test to tmp_path).

No restore endpoint is exercised here because none exists, by decision (see
backupapi.py's module docstring) -- restore is CLI-only, covered in
test_backup.py.
"""
from __future__ import annotations

import pytest

from backend.db import connect, init_db
from backend.migrations import MIGRATIONS

LATEST_SCHEMA_VERSION = max(version for version, _name, _fn in MIGRATIONS)


@pytest.fixture
def api(tmp_path):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import backupapi

    db_path = tmp_path / "source.db"
    conn = connect(db_path)
    init_db(conn)
    conn.execute("INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/1', 'sk1', 1)")
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) "
        "VALUES ('sk1', 'https://x/1', 'Applied', '2026-08-01T00:00:00')"
    )
    conn.commit()

    dest_dir = tmp_path / "backups"

    app = FastAPI()
    app.include_router(backupapi.router, prefix="/api")
    app.dependency_overrides[backupapi.get_backup_source_path] = lambda: db_path
    app.dependency_overrides[backupapi.get_backup_dest_dir] = lambda: dest_dir
    try:
        yield TestClient(app), db_path, dest_dir
    finally:
        app.dependency_overrides.pop(backupapi.get_backup_source_path, None)
        app.dependency_overrides.pop(backupapi.get_backup_dest_dir, None)
        conn.close()


def test_post_backups_creates_and_returns_manifest_201(api):
    client, db_path, dest_dir = api

    resp = client.post("/api/backups")
    assert resp.status_code == 201
    manifest = resp.json()
    assert manifest["tables"]["jobs"] == 1
    assert manifest["tables"]["job_state"] == 1
    assert manifest["schema_version"] == LATEST_SCHEMA_VERSION
    assert (dest_dir / manifest["backup_file"]).is_file()
    assert (dest_dir / manifest["backup_file"]).with_suffix(".json").is_file()


def test_get_backups_lists_newest_first(api):
    client, db_path, dest_dir = api

    assert client.get("/api/backups").json() == []

    first = client.post("/api/backups").json()
    second = client.post("/api/backups").json()

    listed = client.get("/api/backups").json()
    assert len(listed) == 2
    assert [m["backup_file"] for m in listed] == sorted(
        [first["backup_file"], second["backup_file"]],
        key=lambda f: next(m["created_at"] for m in [first, second] if m["backup_file"] == f),
        reverse=True,
    )


def test_post_backups_missing_source_returns_404(tmp_path):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import backupapi

    app = FastAPI()
    app.include_router(backupapi.router, prefix="/api")
    app.dependency_overrides[backupapi.get_backup_source_path] = lambda: tmp_path / "nope.db"
    app.dependency_overrides[backupapi.get_backup_dest_dir] = lambda: tmp_path / "backups"
    client = TestClient(app)

    resp = client.post("/api/backups")
    assert resp.status_code == 404


def test_no_restore_endpoint_mounted(api):
    client, db_path, dest_dir = api
    resp = client.post("/api/backups/restore")
    assert resp.status_code == 404
