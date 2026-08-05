"""Phase 4, wave 3, task 4.7: the `JOBHUNT_WRITES` write flag.

The flag ships ahead of the cutover, so almost everything here is a pin on
"nothing changed yet": with `WRITES_SOURCE == "legacy"` (the default, and what
repo-root conftest.py pins the whole suite to) every legacy write path behaves
exactly as it did before this task existed. The `canonical` half describes what
the flip will do -- refuse the three legacy write entry points, skip the startup
ingest -- and, just as importantly, what it must NOT do: progress and cancel stay
live so a sweep already running when the flag flips remains observable and
stoppable.

Same testing convention as 4.6's read flag (see test_read_flag.py): production
reads `JOBHUNT_WRITES` once, at import (config.py's module docstring says why),
but the gate re-reads `config.WRITES_SOURCE` as an attribute on every call, so
`monkeypatch.setattr(config, "WRITES_SOURCE", ...)` changes the next request the
way the env var would have changed the next process -- no subprocess needed.
Matrix item (e), the invalid-value-at-import error, does need a fresh process and
follows test_read_flag_import.py's subprocess pattern at the bottom of this file.

The runner is stubbed throughout: these tests are about the gate in front of it,
and a real `runner.start()` would spawn pipeline subprocesses. The one exception
is /api/ingest under `legacy`, which runs the REAL ingest against a tmp database
and an empty RESULTS directory -- a stub there would have proven only that a stub
was called, not that the legacy path still works untouched.

Never touches webapp/app.db (repo-root conftest.py fences JOBHUNT_DB, and every
database here is a tmp_path file).
"""
from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from backend import config
from backend.db import connect, get_db, init_db
from backend.models import IngestReport
from backend.routers import sweepapi

WEBAPP_DIR = Path(__file__).resolve().parents[2]

WRITE_ENDPOINTS = [
    ("/api/refresh/quick", "quick"),
    ("/api/sweep/full", "full"),
    ("/api/ingest", None),
]


class FakeRunner:
    """Just the surface routers/sweepapi.py touches."""

    def __init__(self, start_result=None):
        self.start_result = start_result or (True, None)
        self.starts: list[str] = []
        self.cancels = 0

    async def start(self, kind):
        self.starts.append(kind)
        ok, detail = self.start_result
        return ok, (kind if detail is None else detail)

    async def cancel(self):
        self.cancels += 1


async def _one_frame_stream():
    """Stands in for sweeprunner.sse_stream: the real one holds the connection
    open for up to STREAM_MAX_SECS, and TestClient buffers a whole response."""
    yield "data: {\"type\": \"sync\"}\n\n"


@pytest.fixture
def sweep_app(tmp_path, monkeypatch):
    """(client, runner, ingest_calls, conn) with sweepapi mounted at /api.

    No CsrfGuard: this app is built from the router directly, so the X-App header
    is out of scope here (test_sweepstream.py already pins the guard itself).
    """
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    db_path = tmp_path / "writes.db"
    conn = connect(db_path)
    init_db(conn)
    conn.commit()

    # An empty RESULTS directory: the real ingest reads it, finds no scored CSVs
    # and returns an all-zero report, which is a genuine legacy round trip.
    results = tmp_path / "results"
    results.mkdir()
    monkeypatch.setattr(config, "RESULTS", results)

    fake_runner = FakeRunner()
    monkeypatch.setattr(sweepapi, "runner", fake_runner)
    monkeypatch.setattr(sweepapi, "sse_stream", _one_frame_stream)

    ingest_calls: list[object] = []
    real_ingest = sweepapi.ingest

    def counting_ingest(c):
        ingest_calls.append(c)
        return real_ingest(c)

    monkeypatch.setattr(sweepapi, "ingest", counting_ingest)

    app = FastAPI()
    app.include_router(sweepapi.router, prefix="/api")

    def _override():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override
    yield TestClient(app), fake_runner, ingest_calls, conn
    conn.close()


# --------------------------------------------------------------------------- #
# (1) flag=legacy (the default): every write path behaves exactly as before.
# --------------------------------------------------------------------------- #
def test_writes_source_defaults_to_legacy():
    """The shipped default, and the value conftest.py pins the suite to. If this
    ever reads 'canonical' the cutover happened by accident."""
    assert config.WRITES_SOURCE == "legacy"


@pytest.mark.parametrize("path,kind", [("/api/refresh/quick", "quick"), ("/api/sweep/full", "full")])
def test_legacy_flag_starts_the_run(sweep_app, monkeypatch, path, kind):
    client, runner, _ingest_calls, _conn = sweep_app
    monkeypatch.setattr(config, "WRITES_SOURCE", "legacy")

    res = client.post(path)

    assert res.status_code == 202
    assert res.json() == {"started": True, "kind": kind}
    assert runner.starts == [kind]


@pytest.mark.parametrize("path,kind", [("/api/refresh/quick", "quick"), ("/api/sweep/full", "full")])
def test_legacy_flag_keeps_the_runners_own_409(sweep_app, monkeypatch, path, kind):
    """The pre-existing "already running" refusal is also a 409. The gate must not
    replace, mask or duplicate it: under legacy the detail is still the runner's."""
    client, runner, _ingest_calls, _conn = sweep_app
    runner.start_result = (False, "a sweep is already running")
    monkeypatch.setattr(config, "WRITES_SOURCE", "legacy")

    res = client.post(path)

    assert res.status_code == 409
    assert res.json()["detail"] == "a sweep is already running"
    assert "JOBHUNT_WRITES" not in res.json()["detail"]
    assert runner.starts == [kind], "the request must still have reached the runner"


def test_legacy_flag_ingest_runs_the_real_ingest(sweep_app, monkeypatch):
    client, _runner, ingest_calls, conn = sweep_app
    monkeypatch.setattr(config, "WRITES_SOURCE", "legacy")

    from backend.ingest import ingest as real_ingest
    direct = real_ingest(conn)

    res = client.post("/api/ingest")

    assert res.status_code == 200
    assert res.json() == direct.model_dump()
    assert len(ingest_calls) == 1, "the handler must have called ingest itself"


def test_legacy_flag_progress_and_cancel_work(sweep_app, monkeypatch):
    client, runner, _ingest_calls, _conn = sweep_app
    monkeypatch.setattr(config, "WRITES_SOURCE", "legacy")

    progress = client.get("/api/sweep/progress")
    assert progress.status_code == 200

    cancel = client.post("/api/sweep/cancel")
    assert cancel.status_code == 200
    assert cancel.json() == {"cancelled": True}
    assert runner.cancels == 1


# --------------------------------------------------------------------------- #
# (2) flag=canonical: the three legacy write entry points refuse with a 409 that
# names the flag, and never reach the work behind them.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("path,_kind", WRITE_ENDPOINTS)
def test_canonical_flag_refuses_legacy_writes_with_409(sweep_app, monkeypatch, path, _kind):
    client, runner, ingest_calls, _conn = sweep_app
    monkeypatch.setattr(config, "WRITES_SOURCE", "canonical")

    res = client.post(path)

    assert res.status_code == 409
    detail = res.json()["detail"]
    assert "JOBHUNT_WRITES" in detail, "the refusal must name the flag to change"
    assert "canonical" in detail
    assert "—" not in detail and "–" not in detail
    assert detail == config.WRITE_GATE_DETAIL, "one refusal string across all gated paths"
    assert runner.starts == [], "a refused request must not start a sweep"
    assert ingest_calls == [], "a refused request must not ingest"


# --------------------------------------------------------------------------- #
# (3) flag=canonical: progress and cancel stay live on purpose, so a sweep that
# was already running when the flag flipped is still visible and still stoppable.
# --------------------------------------------------------------------------- #
def test_canonical_flag_leaves_progress_stream_live(sweep_app, monkeypatch):
    client, _runner, _ingest_calls, _conn = sweep_app
    monkeypatch.setattr(config, "WRITES_SOURCE", "canonical")

    res = client.get("/api/sweep/progress")

    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/event-stream")
    assert "sync" in res.text


def test_canonical_flag_leaves_cancel_live(sweep_app, monkeypatch):
    client, runner, _ingest_calls, _conn = sweep_app
    monkeypatch.setattr(config, "WRITES_SOURCE", "canonical")

    res = client.post("/api/sweep/cancel")

    assert res.status_code == 200
    assert res.json() == {"cancelled": True}
    assert runner.cancels == 1, "an in-flight legacy sweep must still be stoppable"


# --------------------------------------------------------------------------- #
# (4) flag=canonical: main.py's startup ingest is skipped, and says so.
# --------------------------------------------------------------------------- #
def _run_lifespan(monkeypatch, tmp_path, *, writes, skip_startup=False):
    """Drive the real lifespan with its I/O replaced, and report the ingest calls.

    Everything the gate does not decide is stubbed: a tmp connection instead of
    config.DB_PATH, no schema work, no canonical recovery, no runner/service
    teardown. What is left is exactly the branch under test.
    """
    from backend import main

    calls: list[object] = []
    conn = sqlite3.connect(tmp_path / "lifespan.db")

    def fake_ingest(c):
        calls.append(c)
        return IngestReport(rows=0, new=0, healed=0, needs_review=0,
                            descs_joined=0, runs_backfilled=0)

    async def noop_shutdown(*_args, **_kwargs):
        return None

    monkeypatch.setattr(main, "connect", lambda: conn)
    monkeypatch.setattr(main, "init_db", lambda _c: None)
    monkeypatch.setattr(main, "ingest", fake_ingest)
    monkeypatch.setattr(main, "recover_orphans_if_canonical", lambda: None)
    monkeypatch.setattr(main, "shutdown_default_service", noop_shutdown)
    monkeypatch.setattr(main.runner, "shutdown", noop_shutdown)
    monkeypatch.setattr(config, "WRITES_SOURCE", writes)
    monkeypatch.setattr(config, "SKIP_STARTUP_INGEST", skip_startup)

    async def go():
        async with main.lifespan(None):
            pass

    asyncio.run(go())
    return calls


def test_legacy_flag_startup_ingest_still_runs(monkeypatch, tmp_path, capsys):
    calls = _run_lifespan(monkeypatch, tmp_path, writes="legacy")

    assert len(calls) == 1
    assert "ingest ok" in capsys.readouterr().err


def test_canonical_flag_skips_startup_ingest_and_logs_the_flag(monkeypatch, tmp_path, capsys):
    calls = _run_lifespan(monkeypatch, tmp_path, writes="canonical")

    assert calls == []
    err = capsys.readouterr().err
    assert "JOBHUNT_WRITES" in err
    assert "ingest skipped" in err


def test_legacy_flag_skip_startup_ingest_stays_silent(monkeypatch, tmp_path, capsys):
    """The pre-existing JOBHUNT_SKIP_STARTUP_INGEST path logged nothing; adding
    the write-flag branch above it must not have changed that."""
    calls = _run_lifespan(monkeypatch, tmp_path, writes="legacy", skip_startup=True)

    assert calls == []
    assert capsys.readouterr().err == ""


# --------------------------------------------------------------------------- #
# (5) an invalid JOBHUNT_WRITES must fail at import, not lazily on first write.
# Needs a fresh process for the same reason test_read_flag_import.py does: the
# env var is consulted once, and backend.config is already in sys.modules here.
# --------------------------------------------------------------------------- #
def _run_import(tmp_path, writes_value):
    env = {
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(WEBAPP_DIR),
        "JOBHUNT_DB": str(tmp_path / "must-not-be-real.db"),
        "JOBHUNT_SKIP_STARTUP_INGEST": "1",
    }
    if writes_value is not None:
        env["JOBHUNT_WRITES"] = writes_value
    return subprocess.run(
        [sys.executable, "-c", "import backend.config"],
        env=env, capture_output=True, text=True, timeout=30,
    )


def test_invalid_jobhunt_writes_fails_at_import(tmp_path):
    proc = _run_import(tmp_path, "bogus")
    assert proc.returncode != 0
    assert "JOBHUNT_WRITES" in proc.stderr
    assert "bogus" in proc.stderr


def test_missing_jobhunt_writes_defaults_to_legacy(tmp_path):
    proc = _run_import(tmp_path, None)
    assert proc.returncode == 0, proc.stderr

    probe = subprocess.run(
        [sys.executable, "-c", "import backend.config as c; print(c.WRITES_SOURCE)"],
        env={
            "PATH": "/usr/bin:/bin",
            "PYTHONPATH": str(WEBAPP_DIR),
            "JOBHUNT_DB": str(tmp_path / "must-not-be-real.db"),
            "JOBHUNT_SKIP_STARTUP_INGEST": "1",
        },
        capture_output=True, text=True, timeout=30,
    )
    assert probe.stdout.strip() == "legacy", probe.stderr


@pytest.mark.parametrize("value", ["legacy", "canonical"])
def test_valid_jobhunt_writes_values_import_cleanly(tmp_path, value):
    proc = _run_import(tmp_path, value)
    assert proc.returncode == 0, proc.stderr
