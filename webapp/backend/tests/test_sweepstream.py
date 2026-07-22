"""SSE progress-stream tests (no pipeline subprocesses, no real results/).

Covers the properties the stream's design depends on:
  * the lifetime cap fires even while events stream continuously (the check is at
    the loop head, not in the idle-timeout branch)
  * a saturated subscriber is flagged and recycled, never silently dropped
  * `finished` counts completed runs exactly once, so a client can tell "a run
    ended while I was away" from "nothing happened"
  * a stream's first frame is always a sync snapshot of live state
  * cleanup rides on Starlette cancelling the generator at disconnect (there is
    no is_disconnected() poll; this test is what guards that assumption)
  * the pure-ASGI CSRF guard behaves exactly like the decorator it replaced
"""
import asyncio
import socket
import threading
import time

import pytest

from backend import sweeprunner
from backend.sweeprunner import BOOT, SUB_QUEUE_MAX, Runner


def fresh() -> Runner:
    """A Runner detached from the module singleton (no subprocess, no DB)."""
    return Runner()


# ------------------------------------------------------------------ counters
def test_sync_event_on_idle_runner_reports_no_run():
    r = fresh()
    ev = r._sync_event()
    assert ev["type"] == "sync"
    assert ev["running"] is False
    assert ev["last_error"] is None
    assert ev["boot"] == BOOT
    assert ev["finished"] == 0


def test_finished_counts_each_terminal_once_and_tracks_last_error():
    r = fresh()
    r._emit({"type": "start", "kind": "quick"})
    r._emit({"type": "log", "kind": "quick", "line": "x"})
    assert r.finished == 0, "non-terminal events must not count as completed runs"

    r._emit({"type": "done", "kind": "quick"})
    assert (r.finished, r.last_error) == (1, None)

    r._emit({"type": "error", "kind": "quick", "message": "step build failed (rc=1)"})
    assert (r.finished, r.last_error) == (2, "step build failed (rc=1)")

    # A later clean run clears the remembered failure.
    r._emit({"type": "done", "kind": "quick"})
    assert (r.finished, r.last_error) == (3, None)


def test_every_frame_carries_the_counters():
    r = fresh()
    sub, sync = r.subscribe()
    r._emit({"type": "log", "kind": "quick", "line": "hello"})
    ev = sub.q.get_nowait()
    assert sync["boot"] == ev["boot"] == BOOT
    assert ev["finished"] == 0


def test_idle_sync_after_a_failed_run_reports_the_error():
    """The client decides whether to SHOW it (only if its counter moved); the
    server's job is just to carry it."""
    r = fresh()
    r._emit({"type": "error", "kind": "full", "message": "FIXTURES FAILED"})
    ev = r._sync_event()
    assert ev["running"] is False
    assert ev["last_error"] == "FIXTURES FAILED"
    assert ev["finished"] == 1


def test_running_sync_carries_live_state_and_hides_stale_error():
    r = fresh()
    r._emit({"type": "error", "kind": "full", "message": "old failure"})
    r.running, r.kind, r.step, r.done, r.total = True, "full", "scrape:ats", 2, 9
    r.log.append("scraping greenhouse")
    ev = r._sync_event()
    assert (ev["running"], ev["kind"], ev["step"]) == (True, "full", "scrape:ats")
    assert (ev["done"], ev["total"], ev["line"]) == (2, 9, "scraping greenhouse")
    assert ev["last_error"] is None, "a live run must not surface the previous run's error"


# ------------------------------------------------------------------ overflow
def test_saturated_subscriber_is_flagged_not_grown_or_silently_dropped():
    r = fresh()
    sub, _ = r.subscribe()
    for i in range(SUB_QUEUE_MAX + 5000):
        r._emit({"type": "log", "kind": "quick", "line": f"line {i}"})
    assert sub.q.qsize() == SUB_QUEUE_MAX, "queue must stay bounded"
    assert sub.overflow is True, "overflow must be visible so the stream can recycle"


def test_subscribe_and_unsubscribe_manage_the_subscriber_set():
    r = fresh()
    assert r.subscribers == set()
    sub, _ = r.subscribe()
    assert sub in r.subscribers
    r.unsubscribe(sub)
    assert r.subscribers == set()


# ------------------------------------------------------------------ the stream
def collect(gen, stop_after=None, deadline_s=10):
    """Drain an async generator into a list of frames."""
    async def run():
        out = []
        t0 = time.monotonic()
        async for frame in gen:
            out.append(frame)
            if stop_after and len(out) >= stop_after:
                break
            if time.monotonic() - t0 > deadline_s:
                pytest.fail("stream did not end within the test deadline")
        return out, time.monotonic() - t0
    return asyncio.run(run())


def test_stream_opens_with_a_sync_frame(monkeypatch):
    r = fresh()
    monkeypatch.setattr(sweeprunner, "runner", r)
    frames, _ = collect(sweeprunner.sse_stream(), stop_after=1)
    assert frames[0].startswith("data: ")
    assert '"type": "sync"' in frames[0]


def test_lifetime_cap_fires_during_a_continuous_event_storm(monkeypatch):
    """F1: the deadline used to live in the queue-wait timeout branch, so a stream
    that never went idle never checked it. Events here arrive far faster than the
    heartbeat interval, so that branch is never taken."""
    r = fresh()
    monkeypatch.setattr(sweeprunner, "runner", r)
    monkeypatch.setattr(sweeprunner, "STREAM_MAX_SECS", 1)

    async def run():
        stop = False

        async def storm():
            i = 0
            while not stop:
                r._emit({"type": "log", "kind": "quick", "line": f"line {i}"})
                i += 1
                await asyncio.sleep(0.005)

        task = asyncio.create_task(storm())
        frames = []
        t0 = asyncio.get_running_loop().time()
        async for frame in sweeprunner.sse_stream():
            frames.append(frame)
        elapsed = asyncio.get_running_loop().time() - t0
        task.cancel()
        return frames, elapsed

    frames, elapsed = asyncio.run(asyncio.wait_for(run(), timeout=15))
    assert 0.9 <= elapsed <= 4.0, f"stream ended at {elapsed:.2f}s, expected ~1s cap"
    assert len(frames) > 10, "the storm should have been delivered, not just the cap"
    assert '"type": "bye"' in frames[-1] and '"reason": "deadline"' in frames[-1]
    assert r.subscribers == set(), "the finally must unsubscribe"


def test_overflowed_stream_recycles_with_bye(monkeypatch):
    r = fresh()
    monkeypatch.setattr(sweeprunner, "runner", r)

    async def run():
        gen = sweeprunner.sse_stream()
        first = await gen.__anext__()                 # the sync frame
        # Saturate before draining anything, so the loop head sees overflow.
        for i in range(SUB_QUEUE_MAX + 10):
            r._emit({"type": "log", "kind": "quick", "line": f"line {i}"})
        frames = [first]
        async for frame in gen:
            frames.append(frame)
        return frames

    frames = asyncio.run(asyncio.wait_for(run(), timeout=15))
    assert '"type": "bye"' in frames[-1] and '"reason": "overflow"' in frames[-1]
    assert r.subscribers == set()


# ------------------------------------------------- disconnect cleanup (e2e)
def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_client_disconnect_unsubscribes_without_any_poll(monkeypatch):
    """sse_stream has no is_disconnected() check on purpose: Starlette cancels the
    generator when the client goes away, and the finally does the cleanup. If a
    future Starlette/uvicorn stops doing that, this test is what notices."""
    uvicorn = pytest.importorskip("uvicorn")
    from fastapi import FastAPI
    from fastapi.responses import StreamingResponse

    r = fresh()
    monkeypatch.setattr(sweeprunner, "runner", r)

    app = FastAPI()

    @app.get("/p")
    async def progress():
        return StreamingResponse(sweeprunner.sse_stream(), media_type="text/event-stream")

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="critical"))
    th = threading.Thread(target=server.run, daemon=True)
    th.start()
    try:
        conn = None
        for _ in range(100):
            try:
                conn = socket.create_connection(("127.0.0.1", port), timeout=2)
                break
            except OSError:
                time.sleep(0.05)
        assert conn is not None, "test server never came up"
        conn.sendall(b"GET /p HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n")
        conn.recv(4096)                                  # headers + sync frame
        for _ in range(100):                             # subscriber registered
            if r.subscribers:
                break
            time.sleep(0.05)
        assert r.subscribers, "the stream never registered a subscriber"

        conn.close()                                     # client vanishes
        for _ in range(100):
            if not r.subscribers:
                break
            time.sleep(0.05)
        assert r.subscribers == set(), "disconnect did not unsubscribe within 5s"
    finally:
        server.should_exit = True
        th.join(timeout=10)


# ------------------------------------------------------------------ csrf guard
def test_csrf_guard_matches_the_decorator_it_replaced():
    """The pure-ASGI conversion must not change who gets a 403."""
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend import config
    from backend.main import app

    # No lifespan context manager: this must not trigger the startup ingest.
    client = TestClient(app)

    assert client.post("/api/sweep/cancel").status_code == 403
    assert client.post("/api/sweep/cancel", headers={config.CSRF_HEADER: "nope"}).status_code == 403
    # Header lookup stays case-insensitive, as it was through request.headers.
    for name in (config.CSRF_HEADER, config.CSRF_HEADER.upper(), config.CSRF_HEADER.lower()):
        res = client.post("/api/sweep/cancel", headers={name: config.CSRF_VALUE})
        assert res.status_code == 200, f"{name} should have been accepted"
    # GETs are never guarded (this one 404s past the guard, which is the point).
    assert client.get("/api/definitely-not-a-route").status_code == 404
