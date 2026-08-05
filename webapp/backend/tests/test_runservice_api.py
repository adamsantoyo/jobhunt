"""Phase 4.1: the /api/runs endpoints and the SSE replay stream.

Two harnesses, each chosen for what it can prove:

  TestClient      for the request/response endpoints, and for one end-to-end SSE
                  read over real HTTP (headers, framing, close). Used as a
                  context manager throughout, so every request and every
                  background run share ONE event loop -- a `TestClient` used
                  without `with` spins a fresh portal per request, and a run task
                  created in one of those would be stranded by the next.
  the generator   for stream semantics. `runsapi.event_stream` is deliberately a
                  module-level async generator: driving it directly makes replay,
                  resume, bridging and close deterministic, with no socket to race
                  and no timing tolerance to tune.

`httpx`'s ASGI transport buffers a whole response before returning it, so it
cannot be used to observe a live stream at all -- which is the other reason the
live-tail assertions live at the generator level.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend import runservice
from backend.db import connect as db_connect
from backend.routers import runsapi
from backend.sources.scheduler import SchedulerConfig
from backend.sources.testing import FakeTransport, text_response
from backend.tests.test_source_enrichment import PERMISSIVE_PROFILE
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    fast,
    hanging,
    make_connect,
    plan_of,
    slow,
)

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _REPO_ROOT not in sys.path:  # pragma: no cover - mirrors sources/scoring.py
    sys.path.insert(0, _REPO_ROOT)

IDLE_RUNNER = SimpleNamespace(running=False)


def run(coro):
    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


@pytest.fixture(scope="module")
def profile_doc():
    with open(os.path.join(_REPO_ROOT, "profile.json")) as handle:
        return json.load(handle)


def build_service(connect, profile_doc, *, plan=None, **kwargs):
    kwargs.setdefault("legacy_runner", IDLE_RUNNER)
    kwargs.setdefault("enrichment_transport", FakeTransport(default=text_response("A body.")))
    kwargs.setdefault("profile", PERMISSIVE_PROFILE)
    kwargs.setdefault(
        "scheduler_config", SchedulerConfig(retry_base_delay_seconds=0.01, retry_jitter=0.0)
    )
    if plan is not None:
        kwargs.setdefault("plan_factory", lambda kind, config: plan)
    return runservice.RunService(connect=connect, profile_doc=profile_doc, **kwargs)


def make_app(service):
    app = FastAPI()
    app.include_router(runsapi.router, prefix="/api")
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


def persisted(connect, run_uid):
    conn = connect()
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT sequence, event_type FROM run_events WHERE run_uid=? ORDER BY sequence",
                (run_uid,),
            )
        ]
    finally:
        conn.close()


def parse(frames):
    """SSE text -> (ids, events), dropping comment frames."""
    ids, events = [], []
    for frame in frames:
        if frame.startswith(":"):
            continue
        lines = frame.strip().split("\n")
        ids.append(int(lines[0].removeprefix("id:").strip()))
        events.append(json.loads(lines[1].removeprefix("data:").strip()))
    return ids, events


async def collect(service, run_uid, **kwargs):
    return [chunk async for chunk in runsapi.event_stream(service, run_uid, **kwargs)]


# --------------------------------------------------------------------------- #
# POST /api/runs
# --------------------------------------------------------------------------- #
def test_create_run_returns_202_with_the_run_identity(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(2)))
    )
    with TestClient(make_app(service)) as client:
        response = client.post("/api/runs", json={"kind": "daily"})
        assert response.status_code == 202
        body = response.json()
        assert set(body) == {"run_uid", "kind", "status"}
        assert body["kind"] == "daily"
        assert body["status"] == "running"
        client.portal.call(service.wait, body["run_uid"])

        listed = client.get("/api/runs").json()
        assert [r["run_uid"] for r in listed] == [body["run_uid"]]
        assert listed[0]["status"] == "succeeded"


def test_unknown_kind_is_400(tmp_path, profile_doc):
    service = build_service(make_connect(tmp_path), profile_doc, plan=[])
    with TestClient(make_app(service)) as client:
        response = client.post("/api/runs", json={"kind": "quick"})
        assert response.status_code == 400
        assert "unknown run kind" in response.json()["detail"]


@pytest.mark.parametrize("kind", ["llm-review", "manual-import"])
def test_deferred_kinds_are_501(tmp_path, profile_doc, kind):
    service = build_service(make_connect(tmp_path), profile_doc, plan=[])
    with TestClient(make_app(service)) as client:
        response = client.post("/api/runs", json={"kind": kind})
        assert response.status_code == 501
        assert "wave-3" in response.json()["detail"]


def test_a_legacy_database_answers_503_rather_than_crashing(tmp_path, profile_doc):
    service = build_service(legacy_database(tmp_path), profile_doc, plan=[])
    with TestClient(make_app(service)) as client:
        response = client.post("/api/runs", json={"kind": "daily"})
        assert response.status_code == 503
        assert "canonical run schema is not available" in response.json()["detail"]
        # Every other endpoint degrades the same way instead of raising an
        # OperationalError through the handler -- or, worse for the stream,
        # through a response whose 200 has already been sent.
        assert client.get("/api/runs").status_code == 503
        assert client.get("/api/runs/whatever").status_code == 503
        cancel = client.post("/api/runs/whatever/cancel")
        assert cancel.status_code == 503
        assert "canonical run schema is not available" in cancel.json()["detail"]
        stream = client.get("/api/runs/whatever/events")
        assert stream.status_code == 503
        assert "canonical run schema is not available" in stream.json()["detail"]


def test_a_real_bug_is_a_500_and_not_a_fake_503(tmp_path, profile_doc):
    """"Database unavailable" is a claim about the database, not a catch-all.

    A blanket `except Exception -> 503` would report every bug in this module as
    an outage: the page would say "try again later" forever and the traceback
    would never reach a log.
    """
    service = build_service(make_connect(tmp_path), profile_doc, plan=[])

    async def broken(limit):
        raise RuntimeError("a genuine bug, not a missing table")

    service.list_runs = broken
    with TestClient(make_app(service), raise_server_exceptions=False) as client:
        assert client.get("/api/runs").status_code == 500


def test_conflicting_kinds_are_409_and_name_the_blocking_run(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter(
            "slow",
            instances=("acme",),
            body=hanging(),
            descriptor=descriptor_for("slow", deadline=30.0),
        )
    )
    service = build_service(connect, profile_doc, plan=plan)
    with TestClient(make_app(service)) as client:
        first = client.post("/api/runs", json={"kind": "daily"}).json()
        conflict = client.post("/api/runs", json={"kind": "full-direct"})
        assert conflict.status_code == 409
        assert first["run_uid"] in conflict.json()["detail"]
        # A different lane is unaffected.
        second = client.post("/api/runs", json={"kind": "aggregators"})
        assert second.status_code == 202
        for run_uid in (first["run_uid"], second.json()["run_uid"]):
            assert client.post(f"/api/runs/{run_uid}/cancel").status_code == 202
            client.portal.call(service.wait, run_uid)


def test_a_running_legacy_sweep_is_named_in_the_409(tmp_path, profile_doc):
    service = build_service(
        make_connect(tmp_path), profile_doc, plan=[], legacy_runner=SimpleNamespace(running=True)
    )
    with TestClient(make_app(service)) as client:
        response = client.post("/api/runs", json={"kind": "daily"})
        assert response.status_code == 409
        assert "legacy sweep" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# GET /api/runs and /api/runs/{run_uid}
# --------------------------------------------------------------------------- #
def test_list_is_newest_first_and_honours_limit(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        for uid, at in (
            ("older", "2026-01-01T00:00:00+00:00"),
            ("newer", "2026-06-01T00:00:00+00:00"),
        ):
            conn.execute(
                "INSERT INTO pipeline_runs (run_uid, kind, status, requested_at, kept_count, "
                "new_count, error_json) VALUES (?,?,?,?,?,?,?)",
                (uid, "daily", "succeeded", at, 3, 1, '{"type":"X"}'),
            )
        conn.commit()
    finally:
        conn.close()
    service = build_service(connect, profile_doc, plan=[])
    with TestClient(make_app(service)) as client:
        rows = client.get("/api/runs").json()
        assert [r["run_uid"] for r in rows] == ["newer", "older"]
        assert rows[0]["kept_count"] == 3
        assert rows[0]["error"] == {"type": "X"}  # parsed, not a JSON string
        assert [r["run_uid"] for r in client.get("/api/runs?limit=1").json()] == ["newer"]
        assert client.get("/api/runs?limit=0").status_code == 422


def test_detail_carries_source_runs_stages_and_change_summary(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(2)))
    )
    with TestClient(make_app(service)) as client:
        run_uid = client.post("/api/runs", json={"kind": "daily"}).json()["run_uid"]
        client.portal.call(service.wait, run_uid)

        detail = client.get(f"/api/runs/{run_uid}").json()
        assert detail["status"] == "succeeded"
        assert detail["terminal"] is True
        assert [r["source"] for r in detail["source_runs"]] == ["gh:acme"]
        assert detail["source_runs"][0]["accepted_count"] == 2
        assert detail["stages"]["enrichment"]["phase"] == "finished"
        assert detail["stages"]["scoring"]["phase"] == "finished"
        assert detail["settled"]["outcome"] == "succeeded"
        assert detail["change_summary"]["changed"] == 2
        assert client.get("/api/runs/no-such-run").status_code == 404


# --------------------------------------------------------------------------- #
# POST /api/runs/{run_uid}/cancel
# --------------------------------------------------------------------------- #
def test_cancel_matrix(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter(
            "slow",
            instances=("acme",),
            body=hanging(),
            descriptor=descriptor_for("slow", deadline=30.0),
        )
    )
    service = build_service(connect, profile_doc, plan=plan)
    with TestClient(make_app(service)) as client:
        assert client.post("/api/runs/nope/cancel").status_code == 404

        run_uid = client.post("/api/runs", json={"kind": "daily"}).json()["run_uid"]
        accepted = client.post(f"/api/runs/{run_uid}/cancel")
        assert accepted.status_code == 202
        assert accepted.json() == {"run_uid": run_uid, "cancelling": True}
        client.portal.call(service.wait, run_uid)

        again = client.post(f"/api/runs/{run_uid}/cancel")
        assert again.status_code == 409
        assert "already finished" in again.json()["detail"]
        assert client.get(f"/api/runs/{run_uid}").json()["status"] == "cancelled"


# --------------------------------------------------------------------------- #
# GET /api/runs/{run_uid}/events -- over HTTP
# --------------------------------------------------------------------------- #
def test_the_stream_replays_a_finished_run_over_http_and_closes(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(2)))
    )
    with TestClient(make_app(service)) as client:
        run_uid = client.post("/api/runs", json={"kind": "daily"}).json()["run_uid"]
        client.portal.call(service.wait, run_uid)

        with client.stream("GET", f"/api/runs/{run_uid}/events") as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            assert response.headers["cache-control"] == "no-cache, no-transform"
            assert response.headers["x-accel-buffering"] == "no"
            body = "".join(response.iter_text())

    frames = [f + "\n\n" for f in body.split("\n\n") if f.strip()]
    ids, events = parse(frames)
    rows = persisted(connect, run_uid)
    assert ids == [r["sequence"] for r in rows]
    assert [e["event_type"] for e in events] == [r["event_type"] for r in rows]
    assert events[-1]["event_type"] == runservice.EVENT_RUN_SETTLED
    assert events[0]["payload"]["kind"] == "daily"


def test_last_event_id_resumes_over_http(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(2)))
    )
    with TestClient(make_app(service)) as client:
        run_uid = client.post("/api/runs", json={"kind": "daily"}).json()["run_uid"]
        client.portal.call(service.wait, run_uid)
        rows = persisted(connect, run_uid)
        cut = rows[len(rows) // 2]["sequence"]

        with client.stream(
            "GET", f"/api/runs/{run_uid}/events", headers={"Last-Event-ID": str(cut)}
        ) as response:
            body = "".join(response.iter_text())

    ids, _ = parse([f + "\n\n" for f in body.split("\n\n") if f.strip()])
    assert ids == [r["sequence"] for r in rows if r["sequence"] > cut]


def test_the_stream_for_an_unknown_run_is_404_not_an_empty_200(tmp_path, profile_doc):
    """Same answer as GET detail, for the same reason: an id that names nothing is
    a client error, and a 200 that closes immediately is indistinguishable from a
    run that has simply said nothing yet."""
    service = build_service(make_connect(tmp_path), profile_doc, plan=[])
    with TestClient(make_app(service)) as client:
        response = client.get("/api/runs/no-such-run/events")
        assert response.status_code == 404
        assert "no-such-run" in response.json()["detail"]


def test_a_run_that_has_not_reached_the_database_yet_still_streams(tmp_path, profile_doc):
    """The 404 check must not race the 202 it follows.

    `start_run` returns as soon as the run task exists; `StartRun` commits a
    moment later. A browser that opens the stream on the 202 is asking about a run
    that is live but has no `pipeline_runs` row yet, and it must be streamed, not
    refused.
    """
    connect = make_connect(tmp_path)
    service = build_service(connect, profile_doc, plan=[])
    service._active["fresh"] = runservice._ActiveRun(
        run_uid="fresh", kind="daily", handle=object()
    )

    async def scenario():
        assert await service.run_exists("fresh") is True
        assert await service.run_exists("never-existed") is False

    run(scenario())


def test_an_out_of_range_cursor_is_refused_before_the_headers_go_out(tmp_path, profile_doc):
    """`run_events.sequence` is an int64 column and a Python int is unbounded.

    A cursor past int64 cannot even be BOUND to the query: sqlite3 raises
    `OverflowError`, and inside the generator that arrives after the 200 has been
    sent -- a truncated success the client reads as "nothing more to say".
    """
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(1)))
    )
    beyond_int64 = str(2**64)
    with TestClient(make_app(service)) as client:
        run_uid = client.post("/api/runs", json={"kind": "daily"}).json()["run_uid"]
        client.portal.call(service.wait, run_uid)

        too_big = client.get(f"/api/runs/{run_uid}/events?after={beyond_int64}")
        assert too_big.status_code == 400
        assert client.get(f"/api/runs/{run_uid}/events?after=-5").status_code == 400
        assert client.get(f"/api/runs/{run_uid}/events?after=nonsense").status_code == 400

        # The header is replayed by the browser, not typed by a caller: an
        # unusable one is treated as absent, so the reconnect still works.
        with client.stream(
            "GET", f"/api/runs/{run_uid}/events", headers={"Last-Event-ID": beyond_int64}
        ) as response:
            assert response.status_code == 200
            body = "".join(response.iter_text())

    ids, _ = parse([f + "\n\n" for f in body.split("\n\n") if f.strip()])
    assert ids == [r["sequence"] for r in persisted(connect, run_uid)]


# --------------------------------------------------------------------------- #
# The stream generator: replay, resume, bridge, close
# --------------------------------------------------------------------------- #
def test_the_cursor_prefers_last_event_id_then_after_then_the_beginning():
    assert runsapi.resume_cursor("7", 2) == 7
    assert runsapi.resume_cursor(" 7 ", None) == 7
    assert runsapi.resume_cursor(None, 5) == 5
    assert runsapi.resume_cursor(None, None) == -1
    # A malformed header must not lose the query cursor, or a reconnect would
    # replay a whole run it has already shown.
    assert runsapi.resume_cursor("garbage", 4) == 4
    assert runsapi.resume_cursor("garbage", None) == -1
    # Neither may an unbindable one, and the clamp is the last line of defence
    # for a cursor that reached here without going through `parse_after`.
    assert runsapi.resume_cursor(str(2**64), 4) == 4
    assert runsapi.resume_cursor(str(-(2**64)), None) == -1
    assert runsapi.resume_cursor(None, 2**64) == runsapi.MAX_CURSOR
    assert runsapi.resume_cursor(None, -9) == runsapi.MIN_CURSOR


def test_parse_after_is_the_only_place_a_cursor_can_be_rejected():
    from fastapi import HTTPException

    assert runsapi.parse_after(None) is None
    assert runsapi.parse_after(" 12 ") == 12
    assert runsapi.parse_after(str(runsapi.MAX_CURSOR)) == runsapi.MAX_CURSOR
    for bad in ("abc", "", str(2**64), "-2"):
        with pytest.raises(HTTPException) as excinfo:
            runsapi.parse_after(bad)
        assert excinfo.value.status_code == 400


def test_the_after_cursor_replays_only_what_follows_it(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(2)))
    )

    async def scenario():
        started = await service.start_run("daily")
        await service.wait(started["run_uid"])
        return started["run_uid"], await collect(service, started["run_uid"], after=2)

    run_uid, frames = run(scenario())
    ids, events = parse(frames)
    rows = persisted(connect, run_uid)
    assert ids == [r["sequence"] for r in rows if r["sequence"] > 2]
    assert ids[0] == 3
    assert events[-1]["event_type"] == runservice.EVENT_RUN_SETTLED


def test_a_new_service_instance_replays_the_same_stream_after_a_restart(tmp_path, profile_doc):
    """The restart case: no live handle, no fan-out, no in-memory state at all.

    The second service shares only the database file, which is the whole claim --
    progress is canonical-DB-native, so a browser reconnecting after the server
    restarted sees byte-identical frames.
    """
    connect = make_connect(tmp_path)
    first = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(2)))
    )

    async def produce():
        started = await first.start_run("daily")
        await first.wait(started["run_uid"])
        return started["run_uid"], await collect(first, started["run_uid"])

    run_uid, live_frames = run(produce())

    restarted = build_service(connect, profile_doc, plan=[])
    assert restarted.is_active(run_uid) is False
    replayed = run(collect(restarted, run_uid))

    assert replayed == live_frames
    assert parse(replayed)[1][-1]["event_type"] == runservice.EVENT_RUN_SETTLED


def test_tailing_a_live_run_has_no_gaps_and_no_duplicates(tmp_path, profile_doc):
    """The bridge between replay and live tail, under continuous emission.

    The reader is started at the same moment as the run and drains until the
    stream closes on its own. Two sources trickling records keep commits landing
    throughout, so the reader crosses the replay/tail boundary repeatedly rather
    than once at a quiet moment.
    """
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter("gh", instances=("acme", "beta"), body=slow(6, per_record=0.01)),
        FakeAdapter("lever", instances=("acme",), body=slow(6, per_record=0.01)),
    )
    service = build_service(
        connect,
        profile_doc,
        plan=plan,
        scheduler_config=SchedulerConfig(
            batch_size=1, flush_interval_seconds=0.0, retry_base_delay_seconds=0.01
        ),
    )

    async def scenario():
        started = await service.start_run("daily")
        frames = await collect(service, started["run_uid"], heartbeat=0.05)
        return started["run_uid"], frames

    run_uid, frames = run(scenario())

    ids, events = parse(frames)
    rows = persisted(connect, run_uid)
    assert ids == sorted(set(ids)), "no duplicate and no out-of-order frames"
    assert ids == list(range(len(rows))), "no gaps: every persisted sequence was delivered"
    assert [e["event_type"] for e in events] == [r["event_type"] for r in rows]
    assert events[-1]["event_type"] == runservice.EVENT_RUN_SETTLED
    assert service.subscriber_count(run_uid) == 0, "the subscription must be released"


def test_two_concurrent_readers_see_the_same_stream(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(
        connect,
        profile_doc,
        plan=plan_of(FakeAdapter("gh", instances=("acme",), body=slow(4, per_record=0.01))),
    )

    async def scenario():
        started = await service.start_run("daily")
        uid = started["run_uid"]
        both = await asyncio.gather(
            collect(service, uid, heartbeat=0.05), collect(service, uid, heartbeat=0.05)
        )
        return uid, both

    run_uid, (left, right) = run(scenario())
    assert parse(left)[0] == parse(right)[0]
    assert parse(left)[0] == [r["sequence"] for r in persisted(connect, run_uid)]


def test_a_stream_closes_on_the_settled_event_and_ignores_nothing_before_it(
    tmp_path, profile_doc
):
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(1)))
    )

    async def scenario():
        started = await service.start_run("daily")
        await service.wait(started["run_uid"])
        return started["run_uid"], await collect(service, started["run_uid"])

    run_uid, frames = run(scenario())
    _, events = parse(frames)
    types = [e["event_type"] for e in events]
    assert types.count(runservice.EVENT_RUN_SETTLED) == 1
    assert types[-1] == runservice.EVENT_RUN_SETTLED
    assert "run.started" in types and "stage.scoring.finished" in types


def test_a_stream_for_a_run_nobody_owns_closes_instead_of_hanging(tmp_path, profile_doc):
    """A run interrupted by a process death has no settled event and never will.

    Its stream must still end -- otherwise every such row would pin a socket
    forever -- so liveness, not the terminal event alone, is what ends it.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status) VALUES ('ghost','daily','interrupted')"
        )
        conn.execute(
            "INSERT INTO run_events (run_event_id, run_uid, sequence, event_type, at) "
            "VALUES ('e0','ghost',0,'run.started','2026-08-04T00:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()
    service = build_service(connect, profile_doc, plan=[])

    frames = run(collect(service, "ghost"))
    ids, events = parse(frames)
    assert ids == [0]
    assert events[0]["event_type"] == "run.started"
    # And an id that exists nowhere gets an empty stream rather than a hang.
    assert run(collect(service, "not-a-run")) == []


def test_the_run_routes_are_mounted_on_the_real_app():
    from backend.main import app

    # From the generated schema rather than `app.routes`: an included router is
    # one opaque object there, and the schema is what actually enumerates paths.
    paths = set(app.openapi()["paths"])
    assert {
        "/api/runs",
        "/api/runs/{run_uid}",
        "/api/runs/{run_uid}/cancel",
        "/api/runs/{run_uid}/events",
    } <= paths


def test_the_csrf_guard_covers_the_run_mutations():
    """No lifespan context manager, and no request that reaches a handler: the
    guard answers before anything can open the configured database."""
    from backend.main import app

    client = TestClient(app)
    assert client.post("/api/runs", json={"kind": "daily"}).status_code == 403
    assert client.post("/api/runs/whatever/cancel").status_code == 403


class SettlesDuringTheRead:
    """A service whose run settles in the window between a read and a sample.

    Delegates everything; the only difference is that the FIRST `events_after`
    lets the run finish before it returns. That is exactly the interleaving the
    read-then-sample order loses the run on: the terminal event is committed and
    the record marked settled after the read took its snapshot, so a liveness
    sample taken afterwards reports a dead run whose settled event is on disk,
    unread, and the loop stops having delivered nothing.
    """

    def __init__(self, service, settle):
        self._service = service
        self._settle = settle
        self.reads = 0

    def subscribe(self, run_uid):
        return self._service.subscribe(run_uid)

    def unsubscribe(self, run_uid, queue):
        self._service.unsubscribe(run_uid, queue)

    def is_active(self, run_uid):
        return self._service.is_active(run_uid)

    async def events_after(self, run_uid, cursor, **kwargs):
        rows = await self._service.events_after(run_uid, cursor, **kwargs)
        self.reads += 1
        if self.reads == 1:
            self._settle()
        return rows


def test_a_terminal_event_that_lands_during_the_read_is_still_delivered(
    tmp_path, profile_doc
):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status) VALUES ('racy','daily','running')"
        )
        conn.commit()
    finally:
        conn.close()
    service = build_service(connect, profile_doc, plan=[])
    record = runservice._ActiveRun(run_uid="racy", kind="daily", handle=object())
    service._active["racy"] = record

    def settle():
        service._append_event_sync(
            "racy", runservice.EVENT_RUN_SETTLED, {"outcome": "succeeded"}, None
        )
        record.settled = True
        service._active.pop("racy", None)
        service._fanout.publish("racy")

    racing = SettlesDuringTheRead(service, settle)
    frames = run(collect(racing, "racy", heartbeat=0.05))

    _, events = parse(frames)
    assert [e["event_type"] for e in events] == [runservice.EVENT_RUN_SETTLED], (
        "a stream must never close while a persisted terminal event is unread"
    )
    assert service.subscriber_count("racy") == 0


def test_an_idle_live_run_gets_heartbeat_comments(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(connect, profile_doc, plan=[])
    record = runservice._ActiveRun(run_uid="idle", kind="daily", handle=object())
    service._active["idle"] = record

    async def scenario():
        stream = runsapi.event_stream(service, "idle", heartbeat=0.01)
        beats = [await asyncio.wait_for(stream.__anext__(), 2) for _ in range(2)]
        record.settled = True
        service._active.pop("idle", None)
        service._fanout.publish("idle")
        tail = [chunk async for chunk in stream]
        return beats, tail

    beats, tail = run(scenario())
    assert beats == [": heartbeat\n\n", ": heartbeat\n\n"]
    assert tail == []
    assert service.subscriber_count("idle") == 0
