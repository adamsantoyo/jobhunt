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
import uuid

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
SUB_QUEUE_MAX = 512               # per-subscriber backlog before we recycle its stream

_STEP_RE = re.compile(r"^>>>\s+(.*\S)\s*$")
_PROGRESS_RE = re.compile(r"\[(\d+)/(\d+) done\]\s*next:\s*(.+?)\s*$")

# Run counters restart with the process, so a client must not compare a count from
# one process against another's. Every frame carries this nonce to make that check
# a comparison instead of a guess.
BOOT = uuid.uuid4().hex[:8]


class _Sub:
    """One SSE subscriber. `overflow` means we stopped feeding it because it could
    not drain; sse_stream turns that into a recycle so it reattaches and resyncs
    rather than silently carrying a hole in its event history."""

    __slots__ = ("q", "overflow")

    def __init__(self):
        self.q: asyncio.Queue = asyncio.Queue(maxsize=SUB_QUEUE_MAX)
        self.overflow = False


class Runner:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.running = False
        self.kind = None
        self.step = None
        self.done = 0
        self.total = 0
        self.finished = 0                                   # runs completed since BOOT
        self.last_error = None                              # last completed run's failure, if any
        self.log = collections.deque(maxlen=500)            # last 500 raw output lines
        self.subscribers: set[_Sub] = set()
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
    def _stamp(self, ev: dict) -> dict:
        """Tag a frame with the run counters. A client folds these in and compares
        them, which is how it distinguishes "a run finished while I was away" from
        "nothing happened" without us ever replaying event history to it."""
        return {**ev, "boot": BOOT, "finished": self.finished}

    def _emit(self, ev: dict):
        if ev["type"] in ("done", "error"):
            # Exactly one terminal event per run, so this really is a completed-run
            # count. Clients invalidate their caches once per increment they observe.
            self.finished += 1
            self.last_error = ev.get("message") if ev["type"] == "error" else None
        ev = self._stamp(ev)
        for sub in list(self.subscribers):
            if sub.overflow:
                continue
            try:
                sub.q.put_nowait(ev)
            except asyncio.QueueFull:
                # Half-open or blocked client. Don't grow memory one stdout line at a
                # time for it, and don't drop silently either (an undetectable hole
                # would leave its strip wrong) -- flag it and let sse_stream recycle
                # the response so it comes back and resyncs.
                sub.overflow = True
            except Exception:  # noqa: BLE001 - a bad subscriber must not kill the run
                pass

    def _sync_event(self) -> dict:
        """The single catch-up frame every stream opens with: live state, not history.

        The client's reduce() folds every past log frame into one lastLine and every
        past step frame into the latest, so replaying a run verbatim cost O(run
        length) per reconnect to deliver what this one frame already carries.

        It reports no terminal events while idle -- a finished run must never
        resurface as a stale progress strip on a fresh page load. A client that WAS
        watching learns the run ended from the `finished` counter instead.
        """
        return self._stamp({
            "type": "sync",
            "running": self.running,
            "kind": self.kind,
            "step": self.step,
            "done": self.done,
            "total": self.total,
            "line": self.log[-1] if self.log else None,
            "last_error": None if self.running else self.last_error,
        })

    def subscribe(self) -> tuple[_Sub, dict]:
        """Not async on purpose: with no await between building the snapshot and
        registering, a new subscriber cannot miss an event emitted in between."""
        sub = _Sub()
        sync = self._sync_event()
        self.subscribers.add(sub)
        return sub, sync

    def unsubscribe(self, sub: _Sub):
        self.subscribers.discard(sub)

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
        # Ingest runs off-thread and emits nothing until it returns; say so, or the
        # strip sits on a stale line for the whole quiet stretch before `done`.
        self._emit({"type": "log", "kind": kind, "line": "ingesting…"})
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


async def sse_stream():
    """Async generator yielding SSE frames: one sync snapshot, then the live tail.

    Ends on subscriber overflow or after STREAM_MAX_SECS, both checked at the TOP of
    every iteration rather than inside the queue-wait timeout: during an active
    sweep _emit publishes a frame per stdout line, so the timeout branch is rarely
    taken and a check parked there would never run exactly when the run is long
    enough for it to matter. A stream that outlives its client keeps a socket
    pinned and browsers allow only ~6 per origin.

    There is deliberately no request.is_disconnected() poll. Starlette already
    cancels this generator on disconnect (StreamingResponse races the body against
    listen_for_disconnect), so it never returns True from in here -- measured at
    ~0.2s to cancellation under both BaseHTTPMiddleware and pure ASGI, with no poll
    ever observing anything. Cleanup rides on that cancellation reaching the finally
    below. The heartbeat is the fallback: on a half-open socket, or on an ASGI
    server advertising spec_version >= 2.4 (where StreamingResponse detects
    disconnects from a failing send instead), the next heartbeat write is what
    surfaces the dead peer. STREAM_MAX_SECS is the hard backstop when neither fires.

    Recycling needs no replay protocol. Every stream opens with a `sync` frame
    carrying live state plus the `finished` run counter, so a client that missed
    events while away learns what it missed by comparing counters. Deliberate
    recycles announce themselves with `bye` so the client reopens immediately
    instead of waiting out EventSource's retry and blinking its strip out.
    """
    loop = asyncio.get_running_loop()
    sub, sync = runner.subscribe()
    deadline = loop.time() + STREAM_MAX_SECS
    try:
        yield f"data: {json.dumps(sync)}\n\n"
        while True:
            now = loop.time()
            if now >= deadline or sub.overflow:
                reason = "overflow" if sub.overflow else "deadline"
                yield f"data: {json.dumps({'type': 'bye', 'reason': reason})}\n\n"
                return
            try:
                # Clamp to the deadline so a quiet stream ends on time instead of up
                # to HEARTBEAT_SECS late.
                ev = await asyncio.wait_for(sub.q.get(), timeout=min(HEARTBEAT_SECS, deadline - now))
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"    # comment frame: no event, keeps the socket warm
                continue
            yield f"data: {json.dumps(ev)}\n\n"
    finally:
        runner.unsubscribe(sub)
