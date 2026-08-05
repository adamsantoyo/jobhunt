"""Phase 4.4: POST /api/sources/{source}/retry, and the `RunService.retry_source`
method behind it.

`retry_source` is the one new public method this wave's hard rules allow in
`runservice.py`. It reuses `start_run`'s supervisor machinery (`_supervise`,
`_ActiveRun`, the same `_EXCLUSION_GROUPS` conflict matrix) verbatim -- the only
thing it does differently is resolve a SINGLE-TARGET plan itself instead of
handing the scheduler a bare kind and letting it plan the whole registry. Three
things follow from that and are what this suite proves:

  SCOPED EVIDENCE   only the requested source's targets ever reach the scheduler,
                     so `source_runs`/`postings` for the run contain nothing else,
                     even when the full plan for that kind has other sources in it.
  KIND DERIVATION    the resolved kind is read back off the started run, not
                     asserted separately: a DIRECT/STARTUP_BOARD source resolves to
                     `full-direct`, an AGGREGATOR source resolves to `aggregators`.
                     NOT `daily` -- `daily` is the one `RunProfile` with
                     `dueness_filtered=True` (`run_profiles.RUN_PROFILES`), so a
                     retry that resolved to it would be silently dropped by
                     `filter_due` for a source that already succeeded recently
                     (wave-2 review finding 1, regression-tested below by
                     `test_two_consecutive_retries_of_a_fresh_succeeded_direct_source_both_fetch`).
  STAGE POLICY       falls out of reusing `_supervise` for free: `full-direct` is a
                     `STAGED_KINDS` member (enrichment + scoring run), `aggregators`
                     settles at fetch (`_supervise` skips them) -- exactly the same
                     rule `start_run` follows, asserted here because 4.4 promises
                     it explicitly.
"""
from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.db import connect as db_connect
from backend.routers import sourcesops
from backend.runservice import RunService
from backend.sources.contract import RunKind, SourceConfig
from backend.sources.scheduler import SchedulerConfig
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    fast,
    hanging,
    make_connect,
    plan_of,
)

IDLE_RUNNER = SimpleNamespace(running=False)


def run(coro):
    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


def build_service(connect, *, plans: dict[RunKind, list] | None = None, **kwargs) -> RunService:
    """`plans` maps `RunKind -> full plan`, so `_resolve_retry_target`'s two-kind
    probe (`FULL_DIRECT` then `AGGREGATORS`) sees a DIFFERENT plan per kind --
    unlike a single `plan_factory=lambda kind, config: plan` that ignores `kind`
    entirely, which would make every source resolve to whichever kind is tried
    first."""
    plans = plans or {}
    kwargs.setdefault("legacy_runner", IDLE_RUNNER)
    kwargs.setdefault(
        "scheduler_config", SchedulerConfig(retry_base_delay_seconds=0.01, retry_jitter=0.0)
    )
    kwargs.setdefault("source_config", SourceConfig())
    kwargs.setdefault("plan_factory", lambda kind, config: plans.get(kind, []))
    return RunService(connect=connect, **kwargs)


def app_for(service: RunService) -> FastAPI:
    app = FastAPI()
    app.include_router(sourcesops.router, prefix="/api")
    app.state.run_service = service
    return app


def legacy_database(tmp_path, name="legacy.db"):
    from backend import db

    path = tmp_path / name
    conn = sqlite3.connect(path)
    try:
        conn.executescript(db.DDL)
        conn.commit()
    finally:
        conn.close()
    return lambda: db_connect(path)


def source_run_sources(connect, run_uid: str) -> set[str]:
    conn = connect()
    try:
        return {
            row["source"]
            for row in conn.execute(
                "SELECT DISTINCT source FROM source_runs WHERE run_uid=?", (run_uid,)
            )
        }
    finally:
        conn.close()


def posting_version_sources(connect) -> set[str]:
    conn = connect()
    try:
        return {
            row["source"] for row in conn.execute("SELECT DISTINCT source FROM posting_versions")
        }
    finally:
        conn.close()


async def retry_and_settle(service: RunService, source: str) -> dict:
    """`retry_source` then `wait`, in ONE coroutine on ONE event loop.

    `retry_source` returns as soon as its supervisor task is CREATED, not
    finished -- the background run (fetch, then enrichment/scoring) keeps going
    on whatever event loop created that task. `run()` below wraps each call in
    its own `asyncio.run()`, which tears the loop down (and every task on it)
    the moment its own coroutine returns. Splitting `retry_source` and `wait`
    across two separate `run()` calls therefore strands the supervisor mid-run:
    `wait` finds nothing active (the record was never even added, or the task
    was cancelled before it could write anything) and returns immediately,
    against an all-but-empty database. `start_and_settle` in `test_runservice.py`
    is the same fix for `start_run`.
    """
    started = await service.retry_source(source)
    await service.wait(started["run_uid"])
    return started


# --------------------------------------------------------------------------- #
# RunService.retry_source, directly: evidence scoping + kind derivation + stages.
# --------------------------------------------------------------------------- #
def test_retry_runs_only_the_requested_source_evidence_scoped_to_it_alone(tmp_path):
    connect = make_connect(tmp_path)
    full_direct_plan = plan_of(
        FakeAdapter("good", instances=("acme",), body=fast(2)),
        FakeAdapter("other", instances=("beta",), body=fast(2)),
    )
    service = build_service(connect, plans={RunKind.FULL_DIRECT: full_direct_plan})

    started = run(retry_and_settle(service, "good:acme"))
    assert set(started) == {"run_uid", "source", "kind", "status"}
    assert started["source"] == "good:acme"
    assert started["kind"] == "full-direct"
    assert started["status"] == "running"

    assert source_run_sources(connect, started["run_uid"]) == {"good:acme"}
    detail = run(service.run_detail(started["run_uid"]))
    assert detail["status"] == "succeeded"
    assert {r["source"] for r in detail["source_runs"]} == {"good:acme"}
    # `other:beta`'s two postings never entered the plan at all -- only
    # `good:acme`'s versions are on record, proving the filter in
    # `_resolve_retry_target` reached the scheduler and not just the response.
    conn = connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM postings").fetchone()[0] == 2
    finally:
        conn.close()
    assert posting_version_sources(connect) == {"good:acme"}


def test_retry_of_a_direct_source_resolves_to_full_direct_and_runs_post_fetch_stages(tmp_path):
    from backend.sources.testing import FakeTransport, text_response
    from backend.tests.test_source_enrichment import PERMISSIVE_PROFILE

    connect = make_connect(tmp_path)
    plan = plan_of(FakeAdapter("good", instances=("acme",), body=fast(2)))
    service = build_service(
        connect,
        plans={RunKind.FULL_DIRECT: plan},
        profile=PERMISSIVE_PROFILE,
        enrichment_transport=FakeTransport(default=text_response("A description.")),
    )

    started = run(retry_and_settle(service, "good:acme"))
    assert started["kind"] == "full-direct"

    detail = run(service.run_detail(started["run_uid"]))
    assert detail["stages"]["enrichment"]["phase"] == "finished"
    assert detail["stages"]["scoring"]["phase"] == "finished"
    assert detail["settled"]["outcome"] == "succeeded"


def test_retry_of_an_aggregator_source_resolves_to_aggregators_and_settles_at_fetch(tmp_path):
    connect = make_connect(tmp_path)
    agg_descriptor = descriptor_for(
        "jobspy-fake", run_kinds=frozenset({RunKind.AGGREGATORS})
    )
    plan = plan_of(
        FakeAdapter(
            "jobspy-fake", instances=("indeed",), body=fast(2), descriptor=agg_descriptor
        )
    )
    service = build_service(connect, plans={RunKind.AGGREGATORS: plan})

    started = run(retry_and_settle(service, "jobspy-fake:indeed"))
    assert started["kind"] == "aggregators"

    detail = run(service.run_detail(started["run_uid"]))
    # An `aggregators` run never appends `stage.*` events at all -- see
    # `_supervise`'s skipped branch, which emits only `service.stages.skipped`
    # (asserted below) -- so the per-stage detail lives in `settled.stages`, not
    # in `detail["stages"]` (which `_stage_reports` builds from `stage.*` rows).
    assert detail["stages"] == {}
    assert detail["settled"]["stages"]["enrichment"]["status"] == "skipped"
    assert detail["settled"]["stages"]["scoring"]["status"] == "skipped"
    assert detail["settled"]["outcome"] == "succeeded"


def test_two_consecutive_retries_of_a_fresh_succeeded_direct_source_both_fetch(tmp_path):
    """Regression for wave-2 review finding 1.

    Before the fix, `_RETRY_CANDIDATE_KINDS` tried `DAILY` first -- the one
    `RunProfile` with `dueness_filtered=True` (`run_profiles.RUN_PROFILES`) --
    so a retry of a DIRECT/STARTUP_BOARD source that had JUST succeeded (well
    inside its `refresh_interval_seconds`, the default 6h from
    `descriptor_for`) resolved to `daily`, got silently dropped by
    `scheduler.filter_due` before it ever reached `_run_attempt` (no
    fetch-step `source_runs` row, no adapter call at all), and the run still
    settled "succeeded" against zero work -- the reviewer's repro. `full-direct`
    never dueness-filters (`RUN_PROFILES[RunKind.FULL_DIRECT].dueness_filtered
    is False`), so a second retry immediately after the first must still
    reach the adapter and leave real evidence, exactly like the first.

    Mutation-verified: temporarily reverting `_RETRY_CANDIDATE_KINDS` to
    `(RunKind.DAILY, RunKind.AGGREGATORS)` makes the second retry resolve to
    `daily`, `source_run_sources(connect, second["run_uid"])` come back empty,
    and `adapter.attempts["acme"]` stay at `1` -- while `run_detail(...)`
    still reports `"status": "succeeded"` for that empty run, exactly the
    silent no-op the finding described. Reverted after confirming.
    """
    connect = make_connect(tmp_path)
    plan = plan_of(FakeAdapter("good", instances=("acme",), body=fast(2)))
    adapter = plan[0][0]
    service = build_service(connect, plans={RunKind.FULL_DIRECT: plan})

    first = run(retry_and_settle(service, "good:acme"))
    assert first["kind"] == "full-direct"
    detail1 = run(service.run_detail(first["run_uid"]))
    assert detail1["status"] == "succeeded"
    assert source_run_sources(connect, first["run_uid"]) == {"good:acme"}
    assert adapter.attempts["acme"] == 1

    # Immediately retry again -- well inside the source's default 6h
    # `refresh_interval_seconds`, so a retry that resolved to `daily` would be
    # filtered out as "not due" by `filter_due` (see `scheduler.py`'s preflight).
    second = run(retry_and_settle(service, "good:acme"))
    assert second["kind"] == "full-direct"
    assert second["run_uid"] != first["run_uid"]
    detail2 = run(service.run_detail(second["run_uid"]))
    assert detail2["status"] == "succeeded"
    assert source_run_sources(connect, second["run_uid"]) == {"good:acme"}
    assert adapter.attempts["acme"] == 2


def test_unknown_source_is_UnknownSource(tmp_path):
    from backend.runservice import UnknownSource

    connect = make_connect(tmp_path)
    service = build_service(connect, plans={RunKind.FULL_DIRECT: [], RunKind.AGGREGATORS: []})
    with pytest.raises(UnknownSource):
        run(service.retry_source("nobody:home"))


# --------------------------------------------------------------------------- #
# POST /api/sources/{source}/retry -- status codes, via the router.
# --------------------------------------------------------------------------- #
def test_retry_happy_path_is_202_with_run_uid_source_and_kind(tmp_path):
    connect = make_connect(tmp_path)
    plan = plan_of(FakeAdapter("good", instances=("acme",), body=fast(2)))
    service = build_service(connect, plans={RunKind.FULL_DIRECT: plan})
    with TestClient(app_for(service)) as client:
        response = client.post("/api/sources/good:acme/retry")
        assert response.status_code == 202
        body = response.json()
        assert body == {"run_uid": body["run_uid"], "source": "good:acme", "kind": "full-direct"}
        client.portal.call(service.wait, body["run_uid"])


def test_retry_unknown_source_is_404(tmp_path):
    connect = make_connect(tmp_path)
    service = build_service(connect, plans={RunKind.FULL_DIRECT: [], RunKind.AGGREGATORS: []})
    with TestClient(app_for(service)) as client:
        response = client.post("/api/sources/nobody:home/retry")
        assert response.status_code == 404
        assert "nobody:home" in response.json()["detail"]


def test_retry_409_on_a_lane_conflict_naming_exactly_the_start_run_rule(tmp_path):
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter(
            "slow", instances=("acme",), body=hanging(), descriptor=descriptor_for("slow", deadline=30.0)
        ),
        FakeAdapter("other", instances=("beta",), body=fast(1)),
    )
    service = build_service(connect, plans={RunKind.FULL_DIRECT: plan})
    with TestClient(app_for(service)) as client:
        first = client.post("/api/sources/slow:acme/retry")
        assert first.status_code == 202
        # Both `slow:acme` and `other:beta` resolve to `full-direct`, the same
        # exclusion group ("direct") `start_run`'s `daily`/`full-direct` share --
        # so the second retry is refused for exactly the reason a concurrent
        # `POST /api/runs {"kind": "daily"}` (or `"full-direct"`) would be.
        conflict = client.post("/api/sources/other:beta/retry")
        assert conflict.status_code == 409
        assert first.json()["run_uid"] in conflict.json()["detail"]
        client.portal.call(service.cancel_run, first.json()["run_uid"])
        client.portal.call(service.wait, first.json()["run_uid"])


def test_retry_409_while_a_legacy_sweep_is_running(tmp_path):
    connect = make_connect(tmp_path)
    plan = plan_of(FakeAdapter("good", instances=("acme",), body=fast(1)))
    service = build_service(
        connect, plans={RunKind.FULL_DIRECT: plan}, legacy_runner=SimpleNamespace(running=True)
    )
    with TestClient(app_for(service)) as client:
        response = client.post("/api/sources/good:acme/retry")
        assert response.status_code == 409
        assert "legacy sweep" in response.json()["detail"]


def test_retry_503_on_a_database_without_canonical_schema(tmp_path):
    service = build_service(legacy_database(tmp_path), plans={RunKind.FULL_DIRECT: []})
    with TestClient(app_for(service)) as client:
        response = client.post("/api/sources/whatever:x/retry")
        assert response.status_code == 503
        assert "canonical run schema is not available" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# RunService.list_runs -- overlays an active run not yet visible in
# `pipeline_runs` (wave-2 review finding 3). Lives here rather than in
# `test_runservice.py` (owned by a different agent this wave) because this
# file already has `RunService` + scheduler-fake plumbing set up, and the
# retry endpoint is the concrete 202-then-poll path the finding names.
# --------------------------------------------------------------------------- #
def test_list_runs_overlays_an_active_run_whose_row_has_not_landed_in_pipeline_runs_yet(tmp_path):
    """`start_run`/`retry_source` return as soon as the run's supervisor TASK is
    created, not once the scheduler's writer has actually committed the
    `StartRun` op that creates the `pipeline_runs` row -- the same race
    `run_exists` already tolerates (see its docstring: "a UI that opens the
    event stream immediately after a 202 would otherwise be told 404 about a
    run it just started"). Before this fix, `list_runs` read `pipeline_runs`
    alone, so a client that called `POST .../retry` and immediately polled
    `GET /api/runs` could get back a list with no trace of the run it just
    started, and a UI built to mount one panel per listed run never mounted
    one.

    Constructed directly against the service's in-memory bookkeeping rather
    than trying to win a real race: an `_ActiveRun` record is inserted into
    `service._active` for a `run_uid` that deliberately has NO `pipeline_runs`
    row at all -- exactly the state a real run is in for the brief window
    between `Scheduler.start()` returning and its writer's first commit.
    `list_runs_sync` never touches `record.handle`, so a bare placeholder
    stands in for the real `RunHandle`.

    Mutation-verified: reverting `list_runs_sync` to the DB-only query (no
    `pending` merge over `self._active`) drops `run_uid` "phantom-run" from
    the result entirely. Reverted after confirming.
    """
    from types import SimpleNamespace as _SimpleNamespace

    from backend.runservice import _ActiveRun

    connect = make_connect(tmp_path)
    service = build_service(connect)

    record = _ActiveRun(
        run_uid="phantom-run",
        kind="full-direct",
        handle=_SimpleNamespace(),  # never read by list_runs_sync
        trigger="manual-retry",
        requested_at="2026-08-01T00:00:00+00:00",
    )
    service._active[record.run_uid] = record

    listing = run(service.list_runs(10))
    by_uid = {r["run_uid"]: r for r in listing}
    assert "phantom-run" in by_uid
    row = by_uid["phantom-run"]
    assert row["kind"] == "full-direct"
    assert row["status"] == "running"
    assert row["trigger"] == "manual-retry"
    assert row["requested_at"] == "2026-08-01T00:00:00+00:00"
    assert row["active"] is True
    # Everything the writer would have filled in later, and hasn't yet:
    assert row["started_at"] is None
    assert row["finished_at"] is None
    assert row["kept_count"] is None
    assert row["new_count"] is None
    assert row["error"] is None

    # Confirm there really is no `pipeline_runs` row for it -- this row came
    # from the in-memory overlay, not a DB row that happened to already exist.
    conn = connect()
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM pipeline_runs WHERE run_uid=?", ("phantom-run",)
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_list_runs_prefers_the_persisted_row_once_it_lands(tmp_path):
    """Once the real `pipeline_runs` row exists, `list_runs` must read it
    (not double-list the run) even while the in-memory record is still
    active -- the overlay applies only to runs `pipeline_runs` doesn't know
    about yet."""
    connect = make_connect(tmp_path)
    plan = plan_of(FakeAdapter("good", instances=("acme",), body=fast(2)))
    service = build_service(connect, plans={RunKind.FULL_DIRECT: plan})

    started = run(retry_and_settle(service, "good:acme"))
    listing = run(service.list_runs(10))
    matches = [r for r in listing if r["run_uid"] == started["run_uid"]]
    assert len(matches) == 1
    assert matches[0]["status"] == "succeeded"
