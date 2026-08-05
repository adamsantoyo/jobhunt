"""Phase 5, W-5.2: HTTP wiring for `routers/outcomesapi.py`.

Not mounted in main.py (the 5.3 consumer wires that), so every test here
builds its own local FastAPI app + `TestClient`, exactly the pattern
`test_canonical_reads_router.py`'s `api` fixture establishes. Nothing here
touches webapp/app.db (repo-root conftest.py fences JOBHUNT_DB).
"""
import pytest

from backend.db import connect, get_db, init_db
from backend.models import url_to_b64

AT = "2026-08-01T12:00:00"


@pytest.fixture
def api(tmp_path):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import outcomesapi

    db_path = tmp_path / "outcomes_api_test.db"
    conn = connect(db_path)
    init_db(conn)

    app = FastAPI()
    app.include_router(outcomesapi.router, prefix="/api")

    def _override():
        # F9: `backend.db.connect`, not a raw `sqlite3.connect` -- the raw form
        # skips `PRAGMA foreign_keys=ON` (db.py:50-60), which made every FK
        # failure invisible at the API layer (SQLite silently allows the
        # orphan insert instead of raising). Using the same connection factory
        # `get_db` itself uses means F2's validation now surfaces as the
        # intended 422, not an unguarded IntegrityError -- or worse, nothing.
        c = connect(db_path)
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


def insert_posting(conn, posting_id, at=AT):
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, at, at),
    )
    conn.commit()


# --------------------------------------------------------------------------- #
# POST /api/outcomes/snapshots
# --------------------------------------------------------------------------- #
def test_post_snapshot_happy_path(api):
    client, conn = api
    insert_posting(conn, "p1")
    conn.execute(
        "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, "
        "version_hash, observed_at, title, source, odds, tier, payload_json) "
        "VALUES ('v1','p1','source','v1',?, 'Support Engineer','greenhouse',"
        "'Strong match / Standard', 2, '{}')",
        (AT,),
    )
    conn.commit()

    resp = client.post(
        "/api/outcomes/snapshots",
        json={"surface": "sweep", "items": [{"posting_id": "p1", "rank": 1}]},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["surface"] == "sweep"
    assert body["queue_size"] == 1
    assert len(body["items"]) == 1
    assert body["items"][0]["posting_id"] == "p1"
    assert body["items"][0]["odds"] == "Strong match / Standard"

    row = conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshots"
    ).fetchone()
    assert row["c"] == 1


def test_post_snapshot_empty_items_is_valid(api):
    client, _conn = api
    resp = client.post("/api/outcomes/snapshots", json={"surface": "sweep", "items": []})
    assert resp.status_code == 201
    assert resp.json()["queue_size"] == 0


def test_post_snapshot_unknown_posting_id_is_422(api):
    client, _conn = api
    resp = client.post(
        "/api/outcomes/snapshots",
        json={"surface": "sweep", "items": [{"posting_id": "nope", "rank": 1}]},
    )
    assert resp.status_code == 422


def test_post_snapshot_duplicate_rank_is_422(api):
    client, conn = api
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    resp = client.post(
        "/api/outcomes/snapshots",
        json={
            "surface": "sweep",
            "items": [{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 1}],
        },
    )
    assert resp.status_code == 422


# --------------------------------------------------------------------------- #
# POST /api/outcomes/events
# --------------------------------------------------------------------------- #
def test_post_event_happy_path_with_posting_id(api):
    client, conn = api
    insert_posting(conn, "p1")

    resp = client.post("/api/outcomes/events", json={"kind": "opened", "posting_id": "p1"})
    assert resp.status_code == 201
    body = resp.json()
    assert body["posting_id"] == "p1"
    assert body["kind"] == "opened"

    row = conn.execute("SELECT COUNT(*) AS c FROM outcome_events").fetchone()
    assert row["c"] == 1


def test_post_event_happy_path_with_url_b64(api):
    client, conn = api
    conn.execute("INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/1', 'sk1', 1)")
    conn.commit()

    resp = client.post(
        "/api/outcomes/events",
        json={"kind": "opened", "url_b64": url_to_b64("https://x/1")},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["seen_key"] == "sk1"
    assert body["url"] == "https://x/1"


def test_post_event_bad_base64_is_404(api):
    client, _conn = api
    resp = client.post(
        "/api/outcomes/events", json={"kind": "opened", "url_b64": "not-valid-base64!!"},
    )
    assert resp.status_code == 404


def test_post_event_unknown_kind_is_422(api):
    client, conn = api
    insert_posting(conn, "p1")
    resp = client.post(
        "/api/outcomes/events", json={"kind": "clicked", "posting_id": "p1"},
    )
    assert resp.status_code == 422


def test_post_event_unknown_posting_id_is_422(api):
    client, _conn = api
    resp = client.post(
        "/api/outcomes/events", json={"kind": "opened", "posting_id": "nope"},
    )
    assert resp.status_code == 422


def test_post_event_neither_url_nor_posting_id_is_422(api):
    client, _conn = api
    resp = client.post("/api/outcomes/events", json={"kind": "opened"})
    assert resp.status_code == 422


def test_post_event_unknown_url_stores_url_only(api):
    client, conn = api
    resp = client.post(
        "/api/outcomes/events",
        json={"kind": "opened", "url_b64": url_to_b64("https://nowhere/never-seen")},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["posting_id"] is None
    assert body["url"] == "https://nowhere/never-seen"


# --------------------------------------------------------------------------- #
# F2: a ghost posting_version_id must fail clean at 422, never a bare 500 --
# and, per F9, the API's get_db override now runs with foreign_keys=ON so this
# would surface as an IntegrityError-turned-500 if the fix regressed.
# --------------------------------------------------------------------------- #
def test_get_db_override_connection_enforces_foreign_keys(api):
    """F9: the API's `get_db` override must use `backend.db.connect` (same
    PRAGMAs as production, including `foreign_keys=ON`), not a raw
    `sqlite3.connect` -- otherwise an FK violation that reaches the DB layer
    is invisible at the API layer (SQLite silently permits the orphan insert)
    instead of raising, and a passing test could hide a would-be-500 in
    production. Probed directly against the connection the fixture hands
    out (built the same way the override builds its own, post-fix), rather
    than through `outcomes.py` (which validates most of this itself, F2) --
    this exercises the CONNECTION's own enforcement, the thing F9 actually
    changed."""
    import sqlite3

    client, conn = api
    insert_posting(conn, "p1")
    # Pull the ACTUAL connection `get_db`'s override yields to a request --
    # not the fixture's own `conn` (which was always built via `connect()`
    # and would pass this probe regardless of what the override does).
    override = client.app.dependency_overrides[get_db]
    # Bind the generator itself, not just its yielded value: an unbound
    # `next(override())` gets garbage-collected immediately after this
    # statement (CPython refcounting), which runs its `finally: c.close()`
    # before the next line ever executes.
    override_gen = override()
    override_conn = next(override_gen)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            override_conn.execute(
                "INSERT INTO outcome_events (outcome_event_id, kind, at, posting_id, snapshot_id) "
                "VALUES ('fk-probe', 'opened', ?, 'p1', 'does-not-exist')", (AT,),
            )
    finally:
        override_conn.close()


def test_post_snapshot_ghost_posting_version_id_is_422(api):
    client, conn = api
    insert_posting(conn, "p1")
    resp = client.post(
        "/api/outcomes/snapshots",
        json={
            "surface": "sweep",
            "items": [{"posting_id": "p1", "rank": 1, "posting_version_id": "does-not-exist"}],
        },
    )
    assert resp.status_code == 422
    # No partial write survived: zero snapshot headers, and the connection has
    # no pending transaction (a second, unrelated write on the same connection
    # must succeed rather than inherit a stuck transaction).
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshots"
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshot_items"
    ).fetchone()["c"] == 0
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('probe', '1')")
    conn.commit()


# --------------------------------------------------------------------------- #
# F18: a non-serializable metadata/payload dict raises TypeError out of
# `runstore.canonical_json`; both routes must turn that into 422, not a bare
# 500. Real JSON bodies can never contain a non-str-keyed dict (JSON object
# keys are always strings), so this is verified by making the underlying call
# raise TypeError directly -- exercising the routes' except clause itself
# rather than hunting for an HTTP-reachable trigger that doesn't exist.
# --------------------------------------------------------------------------- #
def test_post_snapshot_type_error_from_capture_is_422(api, monkeypatch):
    client, _conn = api
    from backend.routers import outcomesapi

    def _boom(*args, **kwargs):
        raise TypeError("keys must be str, int, float, bool or None, not tuple")

    monkeypatch.setattr(outcomesapi.outcomes, "capture_snapshot", _boom)
    resp = client.post("/api/outcomes/snapshots", json={"surface": "sweep", "items": []})
    assert resp.status_code == 422


def test_post_event_type_error_from_record_is_422(api, monkeypatch):
    client, conn = api
    insert_posting(conn, "p1")
    from backend.routers import outcomesapi

    def _boom(*args, **kwargs):
        raise TypeError("keys must be str, int, float, bool or None, not tuple")

    monkeypatch.setattr(outcomesapi.outcomes, "record_outcome_event", _boom)
    resp = client.post("/api/outcomes/events", json={"kind": "opened", "posting_id": "p1"})
    assert resp.status_code == 422
