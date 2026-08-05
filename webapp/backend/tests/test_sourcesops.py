"""Phase 4.4: GET /api/sources/ops.

The contract (spec decision 8, pinned by the orchestrator so the frontend can be
built against it concurrently) is a flat list of per-source-instance rows plus a
`generated_at` stamp. Every field's provenance is asserted here:

  FROM `runstore.source_instance_freshness()`  last_success_at, age_seconds, stale,
                                                consecutive_failures (its
                                                `consecutive_failed_runs`),
                                                licenses_absence.
  FROM the registry                            category.
  FROM ONE MORE scan of `source_runs`          p50/p95 duration (ATTEMPT-level,
                                                `started_at -> finished_at`, never
                                                the gate-wait-inclusive
                                                `TargetResult.duration_seconds` --
                                                roadmap open item 6), last_rows,
                                                median_rows, row_anomaly,
                                                last_failure_at, last_error.

Two tests are written specifically to fail under the bug they guard against, and
were hand-mutated during development to confirm that (see their docstrings):
`test_duration_is_attempt_level_not_gate_wait_inclusive` and
`test_consecutive_failures_stops_at_the_first_success_not_at_the_oldest_failure`.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.db import connect as db_connect
from backend.routers import sourcesops
from backend.runservice import RunService
from backend.sources import registry, runstore
from backend.sources.contract import ConfigError, RunKind, SourceConfig
from backend.sources.scheduler import Scheduler, SchedulerConfig
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    fast,
    make_connect,
    permanent_always,
    plan_of,
)

FAST_RETRY = dict(retry_base_delay_seconds=0.01, retry_jitter=0.0)


def run(coro):
    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


def scheduler(connect, **config):
    return Scheduler(connect, config=SchedulerConfig(**{**FAST_RETRY, **config}))


def service_for(connect, *, source_config=None) -> RunService:
    return RunService(connect=connect, source_config=source_config or SourceConfig())


def app_for(service: RunService) -> FastAPI:
    app = FastAPI()
    app.include_router(sourcesops.router, prefix="/api")
    app.state.run_service = service
    return app


def legacy_database(tmp_path, name="legacy.db"):
    """A v4-shaped database: legacy tables only, no canonical schema."""
    from backend import db

    path = tmp_path / name
    conn = sqlite3.connect(path)
    try:
        conn.executescript(db.DDL)
        conn.commit()
    finally:
        conn.close()
    return lambda: db_connect(path)


def ops_rows(client) -> dict[str, dict]:
    body = client.get("/api/sources/ops").json()
    assert set(body) == {"sources", "generated_at", "config_error"}
    assert body["config_error"] is None
    return {row["source"]: row for row in body["sources"]}


# --------------------------------------------------------------------------- #
# Schema gate
# --------------------------------------------------------------------------- #
def test_503_on_a_database_without_canonical_schema(tmp_path):
    service = service_for(legacy_database(tmp_path))
    with TestClient(app_for(service)) as client:
        response = client.get("/api/sources/ops")
        assert response.status_code == 503
        assert "canonical run schema is not available" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# Shape + freshness fields mapped faithfully from source_instance_freshness()
# --------------------------------------------------------------------------- #
def test_freshness_fields_are_read_from_source_instance_freshness_faithfully(tmp_path):
    connect = make_connect(tmp_path)
    good = FakeAdapter("good", instances=("board",), body=fast(2))
    bad = FakeAdapter("bad", instances=("board",), body=fast(2))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(good, bad)))
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(
                FakeAdapter("good", instances=("board",), body=fast(2)),
                FakeAdapter("bad", instances=("board",), body=permanent_always()),
            ),
        )
    )

    conn = connect()
    try:
        expected = {row["source"]: row for row in runstore.source_instance_freshness(conn)}
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        actual = ops_rows(client)

    for source in ("good:board", "bad:board"):
        assert set(actual[source]) == {
            "source", "category", "last_success_at", "last_failure_at", "age_seconds",
            "stale", "consecutive_failures", "p50_duration_seconds", "p95_duration_seconds",
            "last_rows", "median_rows", "row_anomaly", "circuit_open", "last_error",
            "licenses_absence",
        }
        assert actual[source]["last_success_at"] == expected[source]["last_success_at"]
        # Each call samples `now()` independently, a moment apart, so the two
        # numbers drift by a few milliseconds; compare loosely rather than exactly.
        assert actual[source]["age_seconds"] == pytest.approx(
            expected[source]["age_seconds"], abs=1.0
        )
        assert actual[source]["stale"] == expected[source]["stale"]
        assert actual[source]["licenses_absence"] == expected[source]["licenses_absence"]
        assert actual[source]["consecutive_failures"] == expected[source]["consecutive_failed_runs"]

    assert actual["good:board"]["stale"] is False
    assert actual["good:board"]["consecutive_failures"] == 0
    assert actual["good:board"]["circuit_open"] is False
    assert actual["bad:board"]["consecutive_failures"] == 1
    assert actual["bad:board"]["licenses_absence"] is False


# --------------------------------------------------------------------------- #
# Category: registry-driven, both the "currently configured" and the
# "retired but the adapter still exists" paths, and the "no adapter at all" path.
# --------------------------------------------------------------------------- #
def test_category_falls_back_to_the_registry_for_a_source_no_longer_configured(tmp_path):
    """`greenhouse` is a real, always-registered adapter (see
    `sources/adapters/__init__.py`'s module-level `install()`), but this
    service's `SourceConfig()` configures no companies for it, so `greenhouse:acme`
    cannot appear in `_configured_categories`'s plan. Its category must still come
    from `registry.get("greenhouse")` -- the "retired source, adapter still known"
    path -- not read as unknown."""
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn, run_uid="r1", kind="daily", requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00",
        )
        runstore.create_source_run(
            conn, source_run_id="sr1", run_uid="r1", source="greenhouse:acme", attempt=1,
            requested_at="2026-08-01T00:00:00+00:00", started_at="2026-08-01T00:00:00+00:00",
            inventory_scope="complete",
        )
        runstore.finish_source_run(
            conn, source_run_id="sr1", status="succeeded",
            finished_at="2026-08-01T00:00:01+00:00", accepted_count=5,
        )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        rows = ops_rows(client)
    assert rows["greenhouse:acme"]["category"] == "direct"


def test_category_is_null_when_no_adapter_matches_the_source_at_all(tmp_path):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn, run_uid="r1", kind="daily", requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00",
        )
        runstore.create_source_run(
            conn, source_run_id="sr1", run_uid="r1", source="totally-unregistered:xyz",
            attempt=1, requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00", inventory_scope="partial",
        )
        runstore.finish_source_run(
            conn, source_run_id="sr1", status="succeeded",
            finished_at="2026-08-01T00:00:01+00:00", accepted_count=1,
        )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        rows = ops_rows(client)
    assert rows["totally-unregistered:xyz"]["category"] is None


def test_a_currently_configured_but_never_run_source_is_present_with_nulls(tmp_path):
    """A source the registry would plan today but that has never once appeared in
    `source_runs` must still be listed -- not silently absent -- with every
    history-derived field null. This is what "empty-history sources present with
    nulls" means: the row list is not merely `source_runs GROUP BY source`."""
    connect = make_connect(tmp_path)
    fake = FakeAdapter(
        "brand-new-board", instances=("only",), body=fast(1),
        descriptor=descriptor_for("brand-new-board"),
    )
    registry.register(fake)
    try:
        service = service_for(connect)
        with TestClient(app_for(service)) as client:
            rows = ops_rows(client)
    finally:
        registry.unregister("brand-new-board")

    row = rows["brand-new-board:only"]
    assert row["category"] == "direct"
    assert row["last_success_at"] is None
    assert row["last_failure_at"] is None
    assert row["age_seconds"] is None
    assert row["stale"] is None
    assert row["consecutive_failures"] == 0
    assert row["p50_duration_seconds"] is None
    assert row["p95_duration_seconds"] is None
    assert row["last_rows"] is None
    assert row["median_rows"] is None
    assert row["row_anomaly"] == {"flag": False, "ratio": None}
    assert row["circuit_open"] is False
    assert row["last_error"] is None
    assert row["licenses_absence"] is False


# --------------------------------------------------------------------------- #
# Attempt-level duration -- roadmap open item 6.
# --------------------------------------------------------------------------- #
def test_duration_is_attempt_level_not_gate_wait_inclusive(tmp_path):
    """The attempt was REQUESTED at T+0 (queued behind the gate for a long time --
    stands in for `TargetResult.duration_seconds`, which starts the clock at
    request/queue time), but did not actually START running until T+100s, and
    finished 2s later. The correct p50/p95 is 2s (`started_at -> finished_at`);
    a gate-wait-inclusive computation would report ~102s.

    Hand-mutated to confirm this fails under the bug: changing
    `sourcesops._duration_seconds` to read `row.get("requested_at")` instead of
    `row.get("started_at")` turned this assertion's `2.0` into `102.0` and the
    test failed, exactly as intended. Reverted after confirming.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn, run_uid="r1", kind="daily", requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00",
        )
        runstore.create_source_run(
            conn, source_run_id="sr1", run_uid="r1", source="slow:board", attempt=1,
            requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:01:40+00:00",  # +100s queue wait
            inventory_scope="partial",
        )
        runstore.finish_source_run(
            conn, source_run_id="sr1", status="succeeded",
            finished_at="2026-08-01T00:01:42+00:00",  # +2s of actual fetch
            accepted_count=3,
        )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        row = ops_rows(client)["slow:board"]
    assert row["p50_duration_seconds"] == 2.0
    assert row["p95_duration_seconds"] == 2.0


def test_duration_excludes_unattempted_rows_and_rows_missing_a_timestamp(tmp_path):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn, run_uid="r1", kind="daily", requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00",
        )
        # An unattempted (cancelled-before-fetch) row: must not enter the window.
        runstore.record_unattempted_source_run(
            conn, source_run_id="u1", run_uid="r1", source="mix:board", status="cancelled",
            requested_at="2026-08-01T00:00:00+00:00", finished_at="2026-08-01T00:00:00+00:00",
        )
        # A succeeded fetch attempt missing started_at (defensive: should not happen
        # in practice, but the spec is explicit that it must be excluded).
        runstore.create_source_run(
            conn, source_run_id="sr1", run_uid="r1", source="mix:board", attempt=1,
            requested_at="2026-08-01T00:00:00+00:00", started_at=None,
            inventory_scope="partial",
        )
        runstore.finish_source_run(
            conn, source_run_id="sr1", status="succeeded",
            finished_at="2026-08-01T00:00:05+00:00", accepted_count=4,
        )
        # A real, timestamped, succeeded attempt -- this is the only one that
        # should feed the duration window.
        runstore.create_source_run(
            conn, source_run_id="sr2", run_uid="r1", source="mix:board", attempt=2,
            requested_at="2026-08-01T00:00:10+00:00", started_at="2026-08-01T00:00:10+00:00",
            inventory_scope="partial",
        )
        runstore.finish_source_run(
            conn, source_run_id="sr2", status="succeeded",
            finished_at="2026-08-01T00:00:13+00:00", accepted_count=4,
        )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        row = ops_rows(client)["mix:board"]
    assert row["p50_duration_seconds"] == 3.0
    assert row["p95_duration_seconds"] == 3.0
    # last_rows still reads the most recent SUCCEEDED attempt (sr2), timestamped or not.
    assert row["last_rows"] == 4


# --------------------------------------------------------------------------- #
# last_rows / median_rows / row_anomaly
# --------------------------------------------------------------------------- #
def test_row_anomaly_flags_a_big_drop_only_once_the_median_floor_is_met(tmp_path):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn, run_uid="r1", kind="daily", requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00",
        )
        # 4 attempts of 20 rows each, then a 5th of 2 rows: ratio 2/20 = 0.1 < 0.5,
        # median (20) >= 10, so this SHOULD flag.
        for i, count in enumerate((20, 20, 20, 20, 2), start=1):
            sid = f"drop-{i}"
            at = f"2026-08-01T00:{i:02d}:00+00:00"
            runstore.create_source_run(
                conn, source_run_id=sid, run_uid="r1", source="dropping:board", attempt=i,
                requested_at=at, started_at=at, inventory_scope="partial",
            )
            runstore.finish_source_run(
                conn, source_run_id=sid, status="succeeded",
                finished_at=f"2026-08-01T00:{i:02d}:01+00:00", accepted_count=count,
            )
        # A second source with the same 90% drop, but a median under the floor
        # (2 -> below 10): must NOT flag.
        for i, count in enumerate((2, 2, 2, 2, 0), start=1):
            sid = f"tiny-{i}"
            at = f"2026-08-01T01:{i:02d}:00+00:00"
            runstore.create_source_run(
                conn, source_run_id=sid, run_uid="r1", source="tiny:board", attempt=i,
                requested_at=at, started_at=at, inventory_scope="partial",
            )
            runstore.finish_source_run(
                conn, source_run_id=sid, status="succeeded",
                finished_at=f"2026-08-01T01:{i:02d}:01+00:00", accepted_count=count,
            )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        rows = ops_rows(client)

    dropping = rows["dropping:board"]
    assert dropping["last_rows"] == 2
    assert dropping["median_rows"] == 20
    assert dropping["row_anomaly"]["flag"] is True
    assert dropping["row_anomaly"]["ratio"] == pytest.approx(0.1)

    tiny = rows["tiny:board"]
    assert tiny["last_rows"] == 0
    assert tiny["median_rows"] == 2
    assert tiny["row_anomaly"]["flag"] is False, "median below the floor must not flag"


# --------------------------------------------------------------------------- #
# last_failure_at / last_error
# --------------------------------------------------------------------------- #
def test_last_failure_at_and_last_error_come_from_the_newest_non_succeeded_attempt(tmp_path):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn, run_uid="r1", kind="daily", requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00",
        )
        runstore.create_source_run(
            conn, source_run_id="ok", run_uid="r1", source="flaky:board", attempt=1,
            requested_at="2026-08-01T00:00:00+00:00", started_at="2026-08-01T00:00:00+00:00",
            inventory_scope="partial",
        )
        runstore.finish_source_run(
            conn, source_run_id="ok", status="succeeded",
            finished_at="2026-08-01T00:00:01+00:00", accepted_count=1,
        )
        runstore.create_source_run(
            conn, source_run_id="bad", run_uid="r1", source="flaky:board", attempt=2,
            requested_at="2026-08-01T00:05:00+00:00", started_at="2026-08-01T00:05:00+00:00",
            inventory_scope="partial",
        )
        runstore.finish_source_run(
            conn, source_run_id="bad", status="failed",
            finished_at="2026-08-01T00:05:03+00:00",
            error={"type": "TimeoutError", "message": "board.example timed out"},
        )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        row = ops_rows(client)["flaky:board"]
    assert row["last_failure_at"] == "2026-08-01T00:05:03+00:00"
    # `last_error` is a compact DISPLAY STRING on the wire (fix 4.4 wave-2 review
    # finding 2), never the parsed `error_json` object -- "Type: message".
    assert row["last_error"] == "TimeoutError: board.example timed out"


# --------------------------------------------------------------------------- #
# consecutive_failures / circuit_open -- run-level, stops at the first success.
# --------------------------------------------------------------------------- #
def test_consecutive_failures_stops_at_the_first_success_not_at_the_oldest_failure(tmp_path):
    """Four runs, newest first: FAIL, FAIL, SUCCEED, FAIL. The correct count is 2
    (the two newest failures), stopping at the SUCCEED two runs back -- not 3 (which
    counting every failed run regardless of the success between them would give).

    `consecutive_failures` is computed by this router's own
    `_consecutive_failures` (fix 4.4 wave-2 review finding 4 -- see the module
    docstring for why this is no longer simply
    `fresh["consecutive_failed_runs"]`), but the "stop at the first success"
    rule this test pins is shared with `source_instance_freshness`'s version of
    it: no cancels/interrupts appear in this fixture, so the two algorithms
    agree here by construction. Hand-mutated to confirm this fails under the
    bug: changing `_consecutive_failures` to `break` on `succeeded` only when
    it is the newest attempt (i.e. losing the "any attempt in the run
    succeeded" / "stop at the first success" rule) turned `2` into `4` and the
    test failed. Reverted after confirming.
    """
    connect = make_connect(tmp_path)
    good = FakeAdapter("board", instances=("acme",), body=fast(1))
    bad = FakeAdapter("board", instances=("acme",), body=permanent_always())
    # oldest -> newest: fail, succeed, fail, fail
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(bad)))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(good)))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(bad)))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(bad)))

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        row = ops_rows(client)["board:acme"]
    assert row["consecutive_failures"] == 2
    assert row["circuit_open"] is False

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(bad)))
    with TestClient(app_for(service)) as client:
        row = ops_rows(client)["board:acme"]
    assert row["consecutive_failures"] == 3
    assert row["circuit_open"] is True, "circuit_open flips at CIRCUIT_OPEN_THRESHOLD=3"


def _insert_fetch_run(
    conn: sqlite3.Connection, *, run_uid: str, source: str, status: str, at: str,
    accepted_count: int | None = None, error: object = None,
) -> None:
    """One pipeline_runs row plus one attempt=1 fetch-step source_runs row for
    `source`, settled at `status`. A small helper so the cancel/failure history
    fixtures below (fix 4.4 wave-2 review finding 4) read as a plain timeline
    rather than repeating the four-call `runstore` dance per row."""
    runstore.create_pipeline_run(
        conn, run_uid=run_uid, kind="daily", requested_at=at, started_at=at,
    )
    source_run_id = f"sr-{run_uid}"
    runstore.create_source_run(
        conn, source_run_id=source_run_id, run_uid=run_uid, source=source, attempt=1,
        requested_at=at, started_at=at, inventory_scope="partial",
    )
    kwargs: dict = {"source_run_id": source_run_id, "status": status, "finished_at": at}
    if accepted_count is not None:
        kwargs["accepted_count"] = accepted_count
    if error is not None:
        kwargs["error"] = error
    runstore.finish_source_run(conn, **kwargs)


def test_consecutive_failures_excludes_cancelled_runs_between_real_failures(tmp_path):
    """Fix 4.4 wave-2 review finding 4: a cancelled run sitting between two real
    failures must not be counted as a third failure. History, newest run
    first: FAILED, CANCELLED, FAILED, SUCCEEDED. The correct count is 2 (the
    two genuine failures) -- the CANCELLED run is skipped entirely (neither
    counted nor a break in the streak), not 3, which
    `source_instance_freshness`'s `consecutive_failed_runs` (counting a
    cancelled run as a failure) would give.

    Mutation-verified: changing `_consecutive_failures` to also count a
    `"cancelled"`/`"interrupted"` final status (i.e. reverting to the
    `source_instance_freshness`-equivalent rule this DTO field deliberately
    diverges from) turns `2` into `3`. Reverted after confirming.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _insert_fetch_run(
            conn, run_uid="r1", source="cancelly:board", status="succeeded",
            at="2026-08-01T00:00:00+00:00", accepted_count=3,
        )
        _insert_fetch_run(
            conn, run_uid="r2", source="cancelly:board", status="failed",
            at="2026-08-01T01:00:00+00:00",
            error={"type": "TimeoutError", "message": "first failure"},
        )
        _insert_fetch_run(
            conn, run_uid="r3", source="cancelly:board", status="cancelled",
            at="2026-08-01T02:00:00+00:00",
        )
        _insert_fetch_run(
            conn, run_uid="r4", source="cancelly:board", status="failed",
            at="2026-08-01T03:00:00+00:00",
            error={"type": "TimeoutError", "message": "second failure"},
        )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        row = ops_rows(client)["cancelly:board"]
    assert row["consecutive_failures"] == 2
    assert row["circuit_open"] is False


def test_three_consecutive_cancels_never_open_the_circuit_on_a_healthy_source(tmp_path):
    """The user-facing case the finding names directly: a source that has never
    once genuinely failed, cancelled three times in a row by the user, must
    read `consecutive_failures == 0` / `circuit_open is False` -- not the
    false CIRCUIT OPEN alarm the pre-fix behaviour (via
    `source_instance_freshness.consecutive_failed_runs`, which counts
    `cancelled` the same as `failed`) would have painted.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _insert_fetch_run(
            conn, run_uid="r0", source="healthy-but-cancelled:board", status="succeeded",
            at="2026-08-01T00:00:00+00:00", accepted_count=3,
        )
        for i, at in enumerate(
            (
                "2026-08-01T01:00:00+00:00",
                "2026-08-01T02:00:00+00:00",
                "2026-08-01T03:00:00+00:00",
            ),
            start=1,
        ):
            _insert_fetch_run(
                conn, run_uid=f"cancel-{i}", source="healthy-but-cancelled:board",
                status="cancelled", at=at,
            )
        conn.commit()
    finally:
        conn.close()

    service = service_for(connect)
    with TestClient(app_for(service)) as client:
        row = ops_rows(client)["healthy-but-cancelled:board"]
    assert row["consecutive_failures"] == 0
    assert row["circuit_open"] is False


# --------------------------------------------------------------------------- #
# last_error serialization fallback ladder -- fix 4.4 wave-2 review finding 2.
# --------------------------------------------------------------------------- #
def test_last_error_serialization_fallback_ladder():
    """`sourcesops._format_error`, exercised directly against every shape the
    spec's fallback ladder names, rather than round-tripped through a full
    scheduler run per shape."""
    fmt = sourcesops._format_error
    assert fmt(json.dumps({"type": "TimeoutError", "message": "board timed out"})) == (
        "TimeoutError: board timed out"
    )
    assert fmt(json.dumps({"message": "just a message, no type"})) == "just a message, no type"
    assert fmt(json.dumps("already just a string")) == "already just a string"
    assert fmt(None) is None


# --------------------------------------------------------------------------- #
# An unrelated ConfigError must not 500 the endpoint -- fix 4.4 wave-2 review
# finding 5.
# --------------------------------------------------------------------------- #
def test_a_configerror_from_an_unrelated_source_does_not_500_the_endpoint(tmp_path):
    """A source that has nothing to do with the one this test cares about is
    misconfigured badly enough that `registry.plan_run()` raises `ConfigError`
    while `_configured_categories` asks the registry what it would plan today.
    That must degrade to history-derived rows (`config_error` reported,
    `category` null for anything only findable via `configured`), not 500 the
    whole panel over one unrelated source's mistake.

    Mutation-verified: removing the `except ConfigError` branch around
    `_configured_categories(service)` in `_ops_sync` turns this into an
    unhandled 500 (no `except` catches a `ConfigError` propagating out of
    `asyncio.to_thread(_ops_sync, service)`) instead of 200. Reverted after
    confirming.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn, run_uid="r1", kind="daily", requested_at="2026-08-01T00:00:00+00:00",
            started_at="2026-08-01T00:00:00+00:00",
        )
        runstore.create_source_run(
            conn, source_run_id="sr1", run_uid="r1", source="greenhouse:acme", attempt=1,
            requested_at="2026-08-01T00:00:00+00:00", started_at="2026-08-01T00:00:00+00:00",
            inventory_scope="complete",
        )
        runstore.finish_source_run(
            conn, source_run_id="sr1", status="succeeded",
            finished_at="2026-08-01T00:00:01+00:00", accepted_count=5,
        )
        conn.commit()
    finally:
        conn.close()

    fake = FakeAdapter(
        "broken-config-adapter", instances=("x",), body=fast(1),
        descriptor=descriptor_for("broken-config-adapter"),
    )

    def _broken_plan(config: SourceConfig):
        raise ConfigError(
            "broken-config-adapter: bogus config", source_key="broken-config-adapter",
        )

    fake.plan = _broken_plan  # instance-attribute override; `plan()` never runs
    registry.register(fake)
    try:
        service = service_for(connect)
        with TestClient(app_for(service)) as client:
            response = client.get("/api/sources/ops")
    finally:
        registry.unregister("broken-config-adapter")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"sources", "generated_at", "config_error"}
    assert isinstance(body["config_error"], str)
    assert "broken-config-adapter" in body["config_error"]

    rows = {row["source"]: row for row in body["sources"]}
    # `greenhouse:acme` still renders from history: `_category_fallback` never
    # calls `.plan()`, so it is unaffected by the unrelated adapter's error.
    assert rows["greenhouse:acme"]["category"] == "direct"


# --------------------------------------------------------------------------- #
# The cross-agent seam test the wave-2 review said was missing (fix 6): pin
# the exact response shape the frontend (a concurrent agent) is built against.
# --------------------------------------------------------------------------- #
def test_ops_response_shape_pins_the_frontend_contract(tmp_path):
    """Spec decision 8's shape, exact top-level keys, exact per-row keys, and
    the TYPE/nullability of every field, across three representative rows: a
    healthy source, a failed source, and a source that is currently configured
    but has never once run. Nothing here should need to change when the
    frontend (W-A, editing concurrently and not coordinating with this file)
    is built against this endpoint -- that is the whole point of pinning it.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # (a) healthy: one clean succeeded attempt, JUST NOW (so `age_seconds` is
        # well under `DEFAULT_STALE_AFTER_SECONDS` and `stale` reads False) --
        # nothing else in its history.
        now = datetime.now(timezone.utc).isoformat()
        _insert_fetch_run(
            conn, run_uid="r1", source="healthy:board", status="succeeded",
            at=now, accepted_count=5,
        )
        # (b) failed: most recent (only) attempt failed with a structured error.
        _insert_fetch_run(
            conn, run_uid="r2", source="failed:board", status="failed",
            at="2026-08-01T00:05:00+00:00",
            error={"type": "TimeoutError", "message": "board.example timed out"},
        )
        conn.commit()
    finally:
        conn.close()

    # (c) configured-never-run: a registered adapter the registry would plan
    # today, with no source_runs rows at all.
    fake = FakeAdapter(
        "never-run-board", instances=("only",), body=fast(1),
        descriptor=descriptor_for("never-run-board"),
    )
    registry.register(fake)
    try:
        service = service_for(connect)
        with TestClient(app_for(service)) as client:
            response = client.get("/api/sources/ops")
    finally:
        registry.unregister("never-run-board")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"sources", "generated_at", "config_error"}
    assert isinstance(body["generated_at"], str)
    assert body["config_error"] is None

    rows = {row["source"]: row for row in body["sources"]}
    row_keys = {
        "source", "category", "last_success_at", "last_failure_at", "age_seconds",
        "stale", "consecutive_failures", "p50_duration_seconds", "p95_duration_seconds",
        "last_rows", "median_rows", "row_anomaly", "circuit_open", "last_error",
        "licenses_absence",
    }

    healthy = rows["healthy:board"]
    failed = rows["failed:board"]
    never_run = rows["never-run-board:only"]

    for row in (healthy, failed, never_run):
        assert set(row) == row_keys
        assert isinstance(row["source"], str)
        assert row["category"] is None or isinstance(row["category"], str)
        assert row["last_success_at"] is None or isinstance(row["last_success_at"], str)
        assert row["last_failure_at"] is None or isinstance(row["last_failure_at"], str)
        assert row["age_seconds"] is None or isinstance(row["age_seconds"], (int, float))
        assert row["stale"] is None or isinstance(row["stale"], bool)
        assert isinstance(row["consecutive_failures"], int)
        assert row["p50_duration_seconds"] is None or isinstance(
            row["p50_duration_seconds"], (int, float)
        )
        assert row["p95_duration_seconds"] is None or isinstance(
            row["p95_duration_seconds"], (int, float)
        )
        assert row["last_rows"] is None or isinstance(row["last_rows"], int)
        assert row["median_rows"] is None or isinstance(row["median_rows"], (int, float))
        assert isinstance(row["row_anomaly"], dict)
        assert set(row["row_anomaly"]) == {"flag", "ratio"}
        assert isinstance(row["row_anomaly"]["flag"], bool)
        assert row["row_anomaly"]["ratio"] is None or isinstance(
            row["row_anomaly"]["ratio"], (int, float)
        )
        assert isinstance(row["circuit_open"], bool)
        assert row["last_error"] is None or isinstance(row["last_error"], str)
        assert isinstance(row["licenses_absence"], bool)

    # Per-fixture nullability that only makes sense pinned to the specific case:
    assert healthy["stale"] is False
    assert healthy["last_error"] is None
    assert healthy["circuit_open"] is False
    assert healthy["consecutive_failures"] == 0

    assert failed["stale"] is True
    assert failed["last_error"] == "TimeoutError: board.example timed out"
    assert failed["consecutive_failures"] == 1

    # `stale` is None (not False) for a source with no freshness row at all --
    # distinct from a source that IS fresh (`False`) or genuinely stale (`True`).
    assert never_run["stale"] is None
    assert never_run["last_success_at"] is None
    assert never_run["category"] == "direct"
    assert never_run["consecutive_failures"] == 0
