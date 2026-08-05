"""Cancel-latency benchmark for `RunService` (Phase 4 spec decision 11 / roadmap
2.6 deferred note).

Builds a ~200-target `daily` plan of FAKE, NEVER-FINISHING adapters (`hanging()`
from the scheduler test fakes -- yields nothing, then sleeps for an hour; only a
cancel or a deadline stops it) through the REAL `RunService` + `Scheduler` +
`SqliteWriter` into a temporary, freshly migrated SQLite database built with
`backend.db.init_db` -- never `webapp/app.db`. Nothing here touches the network:
`ExecutionMode.ASYNC_INPROCESS` fake bodies, same as `bench_scheduler_32k.py`.

WHAT IS MEASURED

  cancel_to_settled_seconds   wall time from the START of
                               `RunService.cancel_run(run_uid)` to the moment
                               `RunService.wait(run_uid)` returns -- which is only
                               after the supervisor's `finally` has set
                               `record.settled = True`, which only happens after
                               the `service.run.settled` event has been appended
                               and committed (see `runservice._supervise`'s
                               docstring, decision 5). This is "cancel() ->
                               service.run.settled", not "cancel() -> the
                               scheduler's own `run.cancelled`": the post-fetch
                               stages (skipped, for a cancelled run, but still
                               evidenced by their own event) happen in between.

  unattempted_rows            `COUNT(*) FROM source_runs WHERE run_uid=? AND
                               step='unattempted'` -- targets whose plan entry the
                               run reports on (decision 5 in scheduler.py: "every
                               planned target appears in the run report exactly
                               once") but that never got as far as a fetch
                               attempt, because the cancel arrived while they were
                               still queued behind `SchedulerConfig
                               .max_concurrent_targets` (default 8). With ~200
                               hanging targets and cancel delivered once >= 8
                               attempts are already `running`, this number is
                               expected to land close to `total_targets -
                               max_concurrent_targets`: the handful already inside
                               `_run_attempt` settle as `cancelled` fetch-step
                               attempts instead (see `attempted_rows` below).

  attempted_rows               `COUNT(*) ... step='fetch'` for the same run --
                               the targets that DID reach `_run_attempt` before
                               the cancel (bounded by `max_concurrent_targets`),
                               each settling with `status='cancelled'` under the
                               normal fetch-attempt step rather than the
                               unattempted one. `attempted_rows +
                               unattempted_rows` accounts for every planned
                               target.

WHAT THIS DOES NOT MEASURE, STATED RATHER THAN IMPLIED: the fake bodies
(`hanging()`) yield NOTHING before sleeping, so no attempt ever produces a
record for the writer to persist, and the run's `SqliteWriter` therefore sits
on an EMPTY queue for the entire window between "gate full" and cancel. The
number reported here is cancel latency against a writer with nothing queued
to drain. The real worst case -- the one that actually matters for a UI's
"how long until cancel visibly takes effect" -- is cancelling MID-DRAIN of a
full writer queue, e.g. a genuine sweep where several fast direct sources are
committing batches of records at the moment cancel is requested; `aclose`
(see `runservice.py` decision 1) has to wait out that drain, or its own
timeout, before the writer releases. This benchmark does not construct that
case and its numbers should not be read as a bound on it -- a benchmark that
does (queued writes in flight, not just queued targets) is future work if that
distinction ever needs measuring on its own.

Invoke directly (the project's real interpreter, not any per-webapp venv --
see the GATES section of `plans/phase4-spec.md`):

    cd webapp && /Users/adamsantoyo/Documents/Projects/jobhunt/.venv/bin/python \
        -m backend.benchmarks.bench_cancel_latency

    # or, equivalently, from the repo root:
    /Users/adamsantoyo/Documents/Projects/jobhunt/.venv/bin/python \
        webapp/backend/benchmarks/bench_cancel_latency.py

Options:
    --out PATH        write the JSON metrics to PATH instead of stdout
    --runs N           how many independent trials to run (default 3, per the
                        spec's "run it 3 times, report the numbers")
    --targets N        planned target count per trial (default 200)

Not a pytest test: it lives outside `testpaths` in the root `pyproject.toml`
(`["test_sweep_state.py", "webapp/backend/tests"]`), so `pytest -q` never
collects or runs it, same as `bench_scheduler_32k.py`. Output is JSON: a list of
per-trial measurements plus a summary (min/median/max of each number across
trials). No threshold lives here -- this is a measurement, not a gate (spec
decision 11: "no CI assertion this wave").
"""
from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sqlite3
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

# Make `backend.*` importable regardless of cwd: this file lives at
# webapp/backend/benchmarks/bench_cancel_latency.py, so parents[2] is webapp/.
_WEBAPP_ROOT = Path(__file__).resolve().parents[2]
if str(_WEBAPP_ROOT) not in sys.path:
    sys.path.insert(0, str(_WEBAPP_ROOT))

from backend.runservice import RunService  # noqa: E402
from backend.sources.contract import RunKind, SourceConfig  # noqa: E402
from backend.sources.scheduler import SchedulerConfig  # noqa: E402
from backend.tests.test_source_scheduler_fakes import (  # noqa: E402
    FakeAdapter,
    descriptor_for,
    hanging,
    make_connect,
    plan_of,
)

#: Idle stand-in for `sweeprunner.runner` -- `RunService._legacy_running()` reads
#: `.running` off whatever is injected here; a benchmark must never depend on
#: (or be blocked by) the real legacy runner singleton.
IDLE_RUNNER = SimpleNamespace(running=False)

#: How long to wait for at least `max_concurrent_targets` attempts to actually
#: reach `status='running'` before declaring the run "mid-fetch" and cancelling.
#: Generous relative to how fast 8 fake, no-I/O tasks reach their gate (typically
#: sub-millisecond); exists only so a slow CI box degrades to "cancel a bit
#: later", never to a hang.
MID_FETCH_POLL_TIMEOUT_SECONDS = 5.0
MID_FETCH_POLL_INTERVAL_SECONDS = 0.005


def build_plan(target_count: int) -> list:
    """One source, `target_count` instances, all hanging (see module docstring).

    `per_host_concurrency` is raised to `target_count` so the run's OWN host gate
    cannot bind ahead of `SchedulerConfig.max_concurrent_targets` -- the default
    `descriptor_for` value (4) would otherwise throttle a single-source plan like
    this one well below the global gate this benchmark means to exercise, since
    every one of these targets shares one made-up host by construction. A real
    ~200-target sweep spreads that load across ~20 distinct source hosts instead,
    each with its own (tighter) host gate; this benchmark cares about the
    scheduler's STRUCTURAL cancel path, not host-level throttling, so the global
    gate is what is left binding.
    """
    descriptor = descriptor_for("cancel-bench", per_host_concurrency=target_count)
    adapter = FakeAdapter(
        "cancel-bench",
        instances=[f"inst-{i:03d}" for i in range(target_count)],
        body=hanging(),  # yields nothing, then sleeps for an hour -- see module docstring
        descriptor=descriptor,
    )
    return plan_of(adapter)


def _running_attempts(connect, run_uid: str) -> int:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM source_runs WHERE run_uid=? AND step='fetch' AND status='running'",
            (run_uid,),
        ).fetchone()
        return int(row[0])
    finally:
        conn.close()


def _source_run_counts(connect, run_uid: str) -> dict:
    conn = connect()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT step, COUNT(*) AS n FROM source_runs WHERE run_uid=? GROUP BY step",
            (run_uid,),
        ).fetchall()
        return {row["step"]: int(row["n"]) for row in rows}
    finally:
        conn.close()


async def measure_once(*, target_count: int, max_concurrent_targets: int, tmp_dir: Path) -> dict:
    connect = make_connect(tmp_dir, name="bench.db")
    plan = build_plan(target_count)
    service = RunService(
        connect=connect,
        source_config=SourceConfig(),
        legacy_runner=IDLE_RUNNER,
        scheduler_config=SchedulerConfig(max_concurrent_targets=max_concurrent_targets),
        plan_factory=lambda kind, config: plan,
        trigger="benchmark",
    )

    started = await service.start_run("daily")
    run_uid = started["run_uid"]

    # Wait for genuine mid-fetch: at least `max_concurrent_targets` attempts
    # already `running` (the gate is full), so the cancel lands with real
    # in-flight work AND real queued work both present.
    poll_deadline = time.monotonic() + MID_FETCH_POLL_TIMEOUT_SECONDS
    running = 0
    while time.monotonic() < poll_deadline:
        running = _running_attempts(connect, run_uid)
        if running >= max_concurrent_targets:
            break
        await asyncio.sleep(MID_FETCH_POLL_INTERVAL_SECONDS)

    cancel_started = time.perf_counter()
    await service.cancel_run(run_uid)
    await service.wait(run_uid)
    cancel_to_settled_seconds = time.perf_counter() - cancel_started

    detail = await service.run_detail(run_uid)
    counts = _source_run_counts(connect, run_uid)
    unattempted_rows = counts.get("unattempted", 0)
    attempted_rows = counts.get("fetch", 0)

    return {
        "target_count": target_count,
        "max_concurrent_targets": max_concurrent_targets,
        "running_attempts_at_cancel": running,
        "cancel_to_settled_seconds": round(cancel_to_settled_seconds, 4),
        "unattempted_rows": unattempted_rows,
        "attempted_rows": attempted_rows,
        "accounted_for": unattempted_rows + attempted_rows == target_count,
        "run_status": detail["status"] if detail else None,
        "settled_outcome": (detail or {}).get("settled", {}).get("outcome"),
    }


def _summary(values: list[float]) -> dict:
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "median": round(statistics.median(ordered), 4),
        "max": ordered[-1],
    }


async def run_benchmark(*, runs: int, target_count: int, max_concurrent_targets: int) -> dict:
    trials = []
    for _ in range(runs):
        tmp_dir = Path(tempfile.mkdtemp(prefix="jobhunt-bench-cancel-"))
        try:
            trials.append(
                await measure_once(
                    target_count=target_count,
                    max_concurrent_targets=max_concurrent_targets,
                    tmp_dir=tmp_dir,
                )
            )
        finally:
            import shutil  # noqa: PLC0415 - only needed on this path

            shutil.rmtree(tmp_dir, ignore_errors=True)

    return {
        "benchmark": "bench_cancel_latency",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "sqlite_version": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "runs": runs,
        "target_count": target_count,
        "max_concurrent_targets": max_concurrent_targets,
        "trials": trials,
        "summary": {
            "cancel_to_settled_seconds": _summary(
                [t["cancel_to_settled_seconds"] for t in trials]
            ),
            "unattempted_rows": _summary([t["unattempted_rows"] for t in trials]),
        },
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Cancel-latency benchmark for RunService (see module docstring)."
    )
    parser.add_argument("--out", type=Path, default=None, help="write JSON metrics here instead of stdout")
    parser.add_argument("--runs", type=int, default=3, help="independent trials (default 3)")
    parser.add_argument("--targets", type=int, default=200, help="planned targets per trial (default 200)")
    parser.add_argument(
        "--max-concurrent-targets", type=int, default=8,
        help="SchedulerConfig.max_concurrent_targets (default 8, the production default)",
    )
    args = parser.parse_args(argv)

    metrics = asyncio.run(
        run_benchmark(
            runs=args.runs, target_count=args.targets,
            max_concurrent_targets=args.max_concurrent_targets,
        )
    )
    payload = json.dumps(metrics, indent=2, sort_keys=False)

    if args.out:
        args.out.write_text(payload + "\n")
        print(f"[bench_cancel_latency] wrote {args.out}", file=sys.stderr)
    else:
        print(payload)


if __name__ == "__main__":
    main()
