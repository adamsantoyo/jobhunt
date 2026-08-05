"""Phase 5, W-5.4: HTTP wiring for `routers/calibrationapi.py`.

NOT mounted in main.py (the orchestrator wires that), so every test builds its
own local FastAPI app + TestClient -- the pattern `test_outcomes_api.py` and
`test_canonical_reads_router.py` establish. Nothing here touches webapp/app.db
(repo-root conftest.py fences JOBHUNT_DB).
"""
from datetime import datetime, timezone

import pytest

from backend.db import connect, get_db, init_db

AT = "2026-08-01T12:00:00"


@pytest.fixture
def api(tmp_path):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import calibrationapi

    db_path = tmp_path / "calibration_api_test.db"
    conn = connect(db_path)
    init_db(conn)

    app = FastAPI()
    app.include_router(calibrationapi.router, prefix="/api")

    def _override():
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


def _apply(conn, seen_key, at, *, responded=False):
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
        "VALUES (?, NULL, 'status', NULL, 'Applied', ?, 'patch')",
        (seen_key, at),
    )
    if responded:
        conn.execute(
            "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
            "VALUES (?, NULL, 'status', 'Applied', 'Phone screen', ?, 'patch')",
            (seen_key, at),
        )
    conn.commit()


# --------------------------------------------------------------------------- #
# happy path
# --------------------------------------------------------------------------- #
def test_get_calibration_empty_db_is_gated(api):
    client, _conn = api
    resp = client.get("/api/calibration")
    assert resp.status_code == 200
    body = resp.json()
    assert body["gate"] == {
        "gated": True,
        "n_applications": 0,
        "n_responses": 0,
        "thresholds": {"min_applications": 50, "min_responses": 10},
    }
    assert body["active"] == "gated"
    assert "empirical" not in body
    assert "model" not in body


def test_get_calibration_counts_applications(api):
    client, conn = api
    for i in range(3):
        _apply(conn, f"sk{i}", f"2026-08-{1 + i:02d}T09:00:00", responded=(i == 0))
    resp = client.get("/api/calibration")
    assert resp.status_code == 200
    assert resp.json()["gate"]["n_applications"] == 3
    assert resp.json()["gate"]["n_responses"] == 1


def test_thresholds_are_query_parameters(api):
    client, conn = api
    for i in range(3):
        _apply(conn, f"sk{i}", f"2026-08-{1 + i:02d}T09:00:00", responded=(i < 2))
    resp = client.get("/api/calibration", params={"min_applications": 3, "min_responses": 2})
    assert resp.status_code == 200
    body = resp.json()
    assert body["gate"]["thresholds"] == {"min_applications": 3, "min_responses": 2}
    assert body["gate"]["gated"] is False
    assert body["active"] == "empirical"
    assert "empirical" in body


def test_response_is_stable_across_two_calls(api):
    client, conn = api
    for i in range(4):
        _apply(conn, f"sk{i}", f"2026-08-{1 + i:02d}T09:00:00", responded=(i < 2))
    params = {"min_applications": 4, "min_responses": 2}
    first = client.get("/api/calibration", params=params).json()
    second = client.get("/api/calibration", params=params).json()
    # `generated_at` is a real per-request timestamp now, so it is the ONE key
    # allowed to differ between two calls over an unchanged database.
    first.pop("generated_at")
    second.pop("generated_at")
    assert first == second


def test_generated_at_is_a_real_timestamp(api):
    """The router computes `now` per request -- the same division queueapi.py
    draws when it computes `today` at the router and hands it to clock-free
    ranking code. Before this, `generated_at` was null in every HTTP response."""
    client, _conn = api
    body = client.get("/api/calibration").json()
    stamp = body["generated_at"]
    assert isinstance(stamp, str)
    parsed = datetime.fromisoformat(stamp)
    # Naive, because state_events.at is naive and the maturity window subtracts
    # one from the other; an offset-aware value would raise TypeError there.
    assert parsed.tzinfo is None
    delta = abs((datetime.now(timezone.utc).replace(tzinfo=None) - parsed).total_seconds())
    assert delta < 300, f"generated_at {stamp} is not the current moment"


def test_generated_at_reaches_the_maturity_window(api):
    """`now` is not decoration: it is what the model arm measures response
    maturity against. A request against a database whose gate is open must
    therefore get a model section that ATTEMPTED the window (any reason other
    than "no-now", which is what a null `now` produces)."""
    client, conn = api
    for i in range(4):
        _apply(conn, f"sk{i}", f"2026-08-{1 + i:02d}T09:00:00", responded=(i < 2))
    body = client.get(
        "/api/calibration", params={"min_applications": 4, "min_responses": 2}
    ).json()
    assert body["model"]["reason"] != "no-now"
    assert body["model"]["maturity_days"] == 21


# --------------------------------------------------------------------------- #
# validation
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "params",
    [
        {"min_applications": 0},
        {"min_responses": 0},
        {"min_applications": -1},
        {"min_responses": -5},
        {"min_applications": "abc"},
        {"min_responses": "1.5"},
    ],
)
def test_invalid_thresholds_are_422(api, params):
    client, _conn = api
    assert client.get("/api/calibration", params=params).status_code == 422


def test_minimum_valid_thresholds_are_accepted(api):
    client, _conn = api
    resp = client.get("/api/calibration", params={"min_applications": 1, "min_responses": 1})
    assert resp.status_code == 200
    assert resp.json()["gate"]["thresholds"] == {"min_applications": 1, "min_responses": 1}


def test_absurdly_high_thresholds_gate_rather_than_422(api):
    """An unreachable threshold is not a malformed request. It is a request
    whose honest answer is "gated, 3 of 1000000" -- which the caller can read
    and act on, unlike a 422 that says only that the endpoint declined."""
    client, conn = api
    for i in range(3):
        _apply(conn, f"sk{i}", f"2026-08-{1 + i:02d}T09:00:00", responded=True)
    resp = client.get("/api/calibration", params={"min_applications": 1000000})
    assert resp.status_code == 200
    body = resp.json()
    assert body["gate"]["gated"] is True
    assert body["gate"]["n_applications"] == 3
    assert body["gate"]["thresholds"]["min_applications"] == 1000000
    assert "empirical" not in body

    assert client.get(
        "/api/calibration", params={"min_responses": 1000000}
    ).status_code == 200
