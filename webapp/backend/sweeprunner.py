"""Quick-refresh + full-sweep background runners.

A single module-level Runner drives pipeline subprocesses via asyncio, fans SSE
events out to any number of subscribers, and re-ingests on success. All pipeline
commands run with cwd=ROOT via PIPELINE_PY (the pipeline's 3.14 venv).

Guards (full sweep): 45-min wall-clock cap; per-step attempt cap of 25 -> run
`sweep.py --skip <step>`; abort with no ingest if output contains FIXTURES FAILED;
refuse to start if an external sweep touched results/sweep_state.json in the last
3 minutes.
"""
import asyncio
import collections
import json
import os
import re
import signal
import time

from . import config

# Quick refresh: the reliable everyday path (ATS re-scrape + score + build).
QUICK_STEPS = [
    ("scrape:ats", ["scraper.py", "--only", "ats"]),
    ("desc:ats", ["rubric.py", "fetch", "--group", "ats"]),
    ("score", ["rubric.py", "score"]),
    ("build", ["build_tracker.py"]),
]

QUICK_STEP_TIMEOUT = 15 * 60      # 15 min per quick step
SWEEP_NEXT_TIMEOUT = 120          # per `sweep.py --next` call (sweep caps steps at 40s)
SWEEP_WALL_CAP = 45 * 60          # total full-sweep wall clock
SWEEP_STEP_ATTEMPT_CAP = 25       # attempts on the same step before --skip
EXTERNAL_SWEEP_WINDOW = 180       # seconds: a fresh sweep_state.json => external run
HEARTBEAT_SECS = 15
STREAM_MAX_SECS = 300             # recycle an SSE connection rather than pin its socket

_STEP_RE = re.compile(r"^>>>\s+(.*\S)\s*$")
_PROGRESS_RE = re.compile(r"\[(\d+)/(\d+) done\]\s*next:\s*(.+?)\s*$")


class Runner:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.running = False
        self.kind = None
        self.step = None
        self.done = 0
        self.total = 0
        self.events = []                                    # replay buffer for this run
        self.log = collections.deque(maxlen=500)            # last 500 raw output lines
        self.subscribers: set[asyncio.Queue] = set()
        self._proc = None
        self._cancel = False
        self._task = None
        # sweep_state.json mtime after OUR last write. Snapshot at boot so a restart
        # right after our own sweep doesn't read the still-fresh file as an external
        # run; a genuinely external sweep re-registers on its next --next write.
        self._own_state_mtime = 0.0
        self._mark_own_sweep_write()

    # ---------------------------------------------------------------- status
    def status(self) -> dict:
        return {
            "running": self.running,
            "kind": self.kind,
            "step": self.step,
            "done": self.done,
            "total": self.total,
        }

    # ---------------------------------------------------------------- events
    def _emit(self, ev: dict):
        ev = dict(ev)
        self.events.append(ev)
        for q in list(self.subscribers):
            try:
                q.put_nowait(ev)
            except Exception:
                pass

    async def subscribe(self):
        q: asyncio.Queue = asyncio.Queue()
        # Replay only while a run is live; a finished run's terminal events must not
        # resurface as a stale progress/error strip on every later page load.
        snapshot = list(self.events) if self.running else []
        self.subscribers.add(q)
        return q, snapshot

    def unsubscribe(self, q):
        self.subscribers.discard(q)

    # ------------------------------------------------ external sweep detection
    def _mark_own_sweep_write(self):
        """Record sweep_state.json's mtime right after WE drove sweep.py, so our own
        writes are never mistaken for an externally-running sweep."""
        try:
            self._own_state_mtime = (config.RESULTS / "sweep_state.json").stat().st_mtime
        except OSError:
            pass

    def external_sweep_active(self) -> bool:
        p = config.RESULTS / "sweep_state.json"
        if not p.exists():
            return False
        try:
            mtime = p.stat().st_mtime
        except OSError:
            return False
        if time.time() - mtime > EXTERNAL_SWEEP_WINDOW:
            return False
        # Fresh file: external only if it advanced past the state WE last wrote
        # (our own just-finished sweep keeps its mtime for the whole window).
        return mtime > self._own_state_mtime + 1e-6

    # ---------------------------------------------------------------- control
    async def start(self, kind: str):
        """Returns (ok, detail). Second start while running or external -> (False, msg)."""
        if self.running:
            return False, "a sweep is already running"
        if self.external_sweep_active():
            return False, "an external sweep appears to be running (results/sweep_state.json is fresh)"
        self.running = True
        self.kind = kind
        self.step = None
        self.done = 0
        self.total = 0
        self._cancel = False
        self.events = []
        self.log.clear()
        self._task = asyncio.create_task(self._run(kind))
        return True, kind

    async def cancel(self):
        self._cancel = True
        self._kill_proc_tree(self._proc)
        self._emit({"type": "log", "kind": self.kind, "line": "cancel requested"})

    async def shutdown(self):
        """Server teardown: make sure no pipeline subprocess outlives the app.
        Kill the current proc tree first (unblocks the read loop), then cancel and
        await the runner task so _run's finally clears the running flag."""
        self._cancel = True
        self._kill_proc_tree(self._proc)
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - teardown
                pass

    @staticmethod
    def _kill_proc_tree(proc):
        """Kill the spawned process AND its children (sweep.py runs the actual step as
        its own subprocess; killing only sweep.py would orphan a live scraper)."""
        if proc is None or proc.returncode is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    # ---------------------------------------------------------------- run body
    async def _run(self, kind: str):
        try:
            self._emit({"type": "start", "kind": kind})
            if kind == "quick":
                await self._run_quick()
            else:
                await self._run_full()
        except Exception as e:  # noqa: BLE001 - surface any failure as an SSE error
            self._emit({"type": "error", "kind": kind, "message": str(e)})
        finally:
            self.running = False
            self._proc = None

    async def _spawn(self, cmd, timeout, step_label):
        """Spawn PIPELINE_PY cmd, stream stdout->log/SSE. Returns (rc|None, lines).
        rc is None on timeout (process killed)."""
        proc = await asyncio.create_subprocess_exec(
            str(config.PIPELINE_PY), *cmd,
            cwd=str(config.ROOT),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,  # own process group so cancel/timeout kills grandchildren
        )
        self._proc = proc
        lines: list[str] = []

        async def _read():
            assert proc.stdout is not None
            async for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").rstrip("\n")
                lines.append(line)
                self.log.append(line)
                self._emit({"type": "log", "kind": self.kind, "step": step_label, "line": line})

        try:
            await asyncio.wait_for(asyncio.gather(_read(), proc.wait()), timeout=timeout)
        except asyncio.TimeoutError:
            self._kill_proc_tree(proc)
            await proc.wait()
            self._proc = None
            return None, lines
        except asyncio.CancelledError:
            # Task cancelled (server shutdown / runner.shutdown()): the child is in
            # its own session, so nothing else will reap it — kill the tree here.
            self._kill_proc_tree(proc)
            try:
                await proc.wait()
            except Exception:  # noqa: BLE001 - best-effort reap during teardown
                pass
            self._proc = None
            raise
        self._proc = None
        return proc.returncode, lines

    async def _run_quick(self):
        self.total = len(QUICK_STEPS)
        for i, (label, cmd) in enumerate(QUICK_STEPS):
            if self._cancel:
                self._emit({"type": "error", "kind": "quick", "message": "cancelled"})
                return
            self.step = label
            self.done = i
            self._emit({"type": "step", "kind": "quick", "step": label, "done": i, "total": self.total})
            rc, _ = await self._spawn(cmd, QUICK_STEP_TIMEOUT, label)
            if self._cancel:
                self._emit({"type": "error", "kind": "quick", "message": "cancelled"})
                return
            if rc is None:
                self._emit({"type": "error", "kind": "quick", "step": label,
                            "message": f"step {label} exceeded the 15m window and was killed"})
                return
            if rc != 0:
                self._emit({"type": "error", "kind": "quick", "step": label,
                            "message": f"step {label} failed (rc={rc})"})
                return
        self.done = self.total
        await self._ingest_and_done("quick")

    async def _run_full(self):
        t0 = time.time()
        attempts: dict[str, int] = {}
        steps_run = 0     # steps actually executed by THIS run
        did_reset = False
        while True:
            if self._cancel:
                self._emit({"type": "error", "kind": "full", "message": "cancelled"})
                return
            if time.time() - t0 > SWEEP_WALL_CAP:
                self._emit({"type": "error", "kind": "full",
                            "message": "wall-clock cap (45m) exceeded; stopping"})
                return

            rc, lines = await self._spawn(["sweep.py", "--next"], SWEEP_NEXT_TIMEOUT, self.step or "sweep")
            self._mark_own_sweep_write()
            text = "\n".join(lines)

            if "FIXTURES FAILED" in text:
                self._emit({"type": "error", "kind": "full", "step": "tests",
                            "message": "FIXTURES FAILED — tests gate failed; sweep aborted, no ingest"})
                return

            cur_step = None
            done = total = None
            nxt = None
            for ln in lines:
                ms = _STEP_RE.match(ln)
                if ms:
                    cur_step = ms.group(1).strip()
                mp = _PROGRESS_RE.search(ln)
                if mp:
                    done, total, nxt = int(mp.group(1)), int(mp.group(2)), mp.group(3).strip()

            if done is not None:
                self.done, self.total = done, total
            if cur_step:
                self.step = cur_step
                steps_run += 1
                attempts[cur_step] = attempts.get(cur_step, 0) + 1
                self._emit({"type": "step", "kind": "full", "step": cur_step,
                            "done": self.done, "total": self.total})

            # All steps complete?
            if "DONE — all steps complete." in text or nxt == "DONE":
                if steps_run == 0 and not did_reset:
                    # sweep_state.json is a stale all-done file from a PREVIOUS completed
                    # sweep — without a reset this run would be a silent no-op. A partial
                    # state (some steps pending) never hits this: --next runs a real step.
                    await self._spawn(["sweep.py", "--reset"], 60, "reset")
                    self._mark_own_sweep_write()
                    did_reset = True
                    self._emit({"type": "log", "kind": "full",
                                "line": "[info] previous sweep was complete; state reset for a fresh run"})
                    continue
                break

            # Persistently-stuck step -> skip past it.
            if cur_step and attempts.get(cur_step, 0) > SWEEP_STEP_ATTEMPT_CAP:
                await self._spawn(["sweep.py", "--skip", cur_step], 60, cur_step)
                self._mark_own_sweep_write()
                self._emit({"type": "skipped", "kind": "full", "step": cur_step,
                            "message": f"skipped stuck step {cur_step} after {SWEEP_STEP_ATTEMPT_CAP} attempts"})
                attempts[cur_step] = 0
                continue

            if rc is None:
                # --next itself hung past 120s (sweep caps steps at 40s). Loop; the
                # attempt counter will skip a genuinely stuck step.
                self._emit({"type": "log", "kind": "full", "step": cur_step,
                            "line": "[warn] sweep.py --next exceeded 120s; retrying"})
                continue

        self.step = self.step or "done"
        self.done = self.total
        self._emit({"type": "step", "kind": "full", "step": self.step,
                    "done": self.total, "total": self.total})
        await self._ingest_and_done("full")

    # ---------------------------------------------------------------- ingest
    def _do_ingest(self) -> dict:
        from .db import connect
        from .ingest import ingest as run_ingest
        conn = connect()
        try:
            rep = run_ingest(conn)
            return rep.model_dump()
        finally:
            conn.close()

    async def _ingest_and_done(self, kind: str):
        if self._cancel:
            self._emit({"type": "error", "kind": kind, "message": "cancelled"})
            return
        try:
            counts = await asyncio.to_thread(self._do_ingest)
        except Exception as e:  # noqa: BLE001
            self._emit({"type": "error", "kind": kind, "message": f"ingest failed: {e}"})
            return
        self._emit({"type": "ingested", "kind": kind, "message": "re-ingested",
                    "done": self.total, "total": self.total, "counts": counts})
        self._emit({"type": "done", "kind": kind})


# Module-level singleton (survives page reloads; progress endpoint reattaches).
runner = Runner()


async def sse_stream(request=None):
    """Async generator yielding SSE frames: snapshot replay + live tail + heartbeats.

    Ends on client disconnect, or after STREAM_MAX_SECS regardless. Both matter:
    a stream that outlives its client keeps a socket pinned, and browsers allow
    only ~6 per origin, so leaked streams eventually starve the whole app. The
    disconnect check is best-effort (BaseHTTPMiddleware can swallow the signal),
    so the lifetime cap is the backstop that guarantees the socket is released.
    EventSource reconnects on its own, and subscribe() replays the active run,
    so recycling the connection is invisible to a client that is still there.
    """
    q, snapshot = await runner.subscribe()
    deadline = asyncio.get_running_loop().time() + STREAM_MAX_SECS
    try:
        for ev in snapshot:
            yield f"data: {json.dumps(ev)}\n\n"
        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=HEARTBEAT_SECS)
                yield f"data: {json.dumps(ev)}\n\n"
            except asyncio.TimeoutError:
                if request is not None and await request.is_disconnected():
                    return
                if asyncio.get_running_loop().time() >= deadline:
                    return
                yield ": heartbeat\n\n"
    finally:
        runner.unsubscribe(q)
