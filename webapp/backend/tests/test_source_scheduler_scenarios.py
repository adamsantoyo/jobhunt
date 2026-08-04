"""Phase 2.6: adversarial scenario coverage for the scheduler/writer/runstore
safety story — fast/slow/failing/hanging adapters, cancellation, restart,
backpressure, partial success, and SQLite contention, plus the tracked
follow-up on SUBPROCESS isolation.

This suite is deliberately additive. `test_source_scheduler.py`,
`test_source_scheduler_persistence.py`, and `test_source_absence.py` already
cover most of the ground the roadmap item names (see each test's docstring for
which existing test it extends rather than duplicates). What lives here is
either genuinely new coverage or a scenario that needed several adapter kinds,
a real process boundary, or direct control of internal state to observe at all.

Every database is a fresh file under `tmp_path`; nothing here can reach
`webapp/app.db`. No test synchronises on a wall-clock sleep — every wait below
either polls a real committed row from an independent connection, or drives
internal state (a queue, a task) directly and deterministically.
"""
from __future__ import annotations

import asyncio
import contextlib
import gc
import json
import os
import sqlite3
import sys
import threading
import time
import warnings

import pytest

from backend.sources.contract import (
    Disposition,
    ExecutionMode,
    InventoryScope,
    RunKind,
    SourceTarget,
)
from backend.sources.scheduler import Scheduler, SchedulerConfig, recover_orphans
from backend.sources.testing import fixture_path
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    blocking,
    descriptor_for,
    fast,
    gated,
    hanging,
    make_connect,
    permanent_always,
    plan_of,
    scalar,
    slow,
    transient_then,
)

FAST_RETRY = dict(retry_base_delay_seconds=0.01, retry_jitter=0.0)


def run(coro):
    """Run one scenario with a hard ceiling, so a hang fails instead of wedging."""

    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


def scheduler(connect, **config):
    return Scheduler(connect, config=SchedulerConfig(**{**FAST_RETRY, **config}))


def rows(connect, sql, params=()):
    conn = connect()
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def report_of(connect, run_uid):
    return json.loads(
        scalar(connect, "SELECT aggregate_report_json FROM pipeline_runs WHERE run_uid=?", (run_uid,))
    )


def is_closed(conn) -> bool:
    try:
        conn.execute("SELECT 1")
    except sqlite3.ProgrammingError:
        return True
    return False


def leftover_tasks():
    return [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]


# =========================================================================== #
# 1. Adapter taxonomy: fast / slow / failing (each kind) / hanging, together
# =========================================================================== #
def test_fast_slow_transient_permanent_and_hanging_sources_settle_correctly_together(tmp_path):
    """Every fake adapter kind this suite is built on, run concurrently in one
    plan, so the mixed-kind interactions are exercised rather than inferred from
    pairwise tests. `test_hanging_source_is_contained_to_its_own_deadline` and
    `test_one_target_deadline_does_not_stall_the_others` (test_source_scheduler.py)
    each mix two kinds; this is all five kinds in one run.

    Asserts, in memory AND persisted: the run settles PARTIAL; each target's final
    status matches its kind; and the fast/slow/transient sources actually commit
    rows to an independent connection WHILE the hanging target is still pinned on
    its own deadline, not merely "eventually, once everything settles".
    """
    connect = make_connect(tmp_path)
    fast_src = FakeAdapter("fast", instances=("a",), body=fast(2))
    slow_src = FakeAdapter("slow", instances=("a",), body=slow(2, per_record=0.02))
    transient_src = FakeAdapter("transient", instances=("a",), body=transient_then(count=2))
    permanent_src = FakeAdapter("permanent", instances=("a",), body=permanent_always())
    hung_src = FakeAdapter(
        "hung",
        instances=("a",),
        body=hanging(before=1),
        descriptor=descriptor_for("hung", deadline=0.5),
    )

    async def scenario():
        sched = scheduler(connect, max_concurrent_targets=8, batch_size=1, flush_interval_seconds=0.0)
        handle = sched.start(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(fast_src, slow_src, transient_src, permanent_src, hung_src),
        )
        deadline = time.monotonic() + 0.4
        committed_sources = 0
        while time.monotonic() < deadline:
            committed_sources = scalar(
                connect,
                "SELECT COUNT(DISTINCT s.source) FROM run_postings rp "
                "JOIN source_runs s ON s.source_run_id = rp.source_run_id",
            )
            if committed_sources >= 1:
                break
            await asyncio.sleep(0.005)
        assert not handle.done, "the whole run settled before the hung target's own deadline fired"
        return committed_sources, await handle.wait()

    committed_before_hang_settles, result = run(scenario())

    assert committed_before_hang_settles >= 1, (
        "no other source committed a row while the hung target was still pinned"
    )
    assert result.status == "partial"
    expected = {
        "fast:a": "succeeded",
        "slow:a": "succeeded",
        "transient:a": "succeeded",
        "permanent:a": "failed",
        "hung:a": "timeout",
    }
    assert {t.source_run_key: t.status for t in result.targets} == expected

    # Persisted: the same shape survives independently in `source_runs`, keyed by
    # each source's LAST attempt (transient has two: failed then succeeded).
    persisted: dict[str, str] = {}
    for row in rows(connect, "SELECT source, attempt, status FROM source_runs ORDER BY source, attempt"):
        persisted[row["source"]] = row["status"]
    assert persisted == expected

    assert result.target("transient:a").attempts[0].disposition is Disposition.TRANSIENT
    assert len(result.target("transient:a").attempts) == 2
    assert result.target("permanent:a").attempts[0].disposition is Disposition.PERMANENT
    assert result.target("hung:a").error["type"] == "DeadlineExceeded"
    assert result.target("hung:a").accepted == 1, "the one record delivered before the deadline survives"


# =========================================================================== #
# 2. Cancellation
# =========================================================================== #
# Mid-run cancel (already-committed batches survive; in-flight rows reach terminal
# states; run terminal state correct) is covered by
# `test_cancellation_stops_cleanly_persists_evidence_and_orphans_no_tasks`.
# Cancel-while-queued-at-a-gate (terminal `unattempted` evidence) is covered by
# `test_a_target_cancelled_at_a_gate_records_terminal_evidence_beside_the_one_that_ran`
# and `test_targets_cancelled_while_queued_at_a_gate_still_appear_in_the_report`
# (test_source_scheduler.py). Both new below: cancel arriving during the writer
# drain step of cleanup, and double-cancel idempotency.
def test_a_run_cancelled_while_the_writer_is_draining_still_finishes_cleanup(tmp_path):
    """The run's cleanup path is a SEQUENCE of awaits — drain, presence pass,
    aclose — not just the last one. `test_a_run_task_cancelled_during_cleanup_still_closes_the_writer`
    (test_source_scheduler.py) proves this at the `aclose` await; this proves it at
    the FIRST one, `writer.drain()`, called right after every target settles.
    """
    base = make_connect(tmp_path)
    opened = []

    def connect():
        conn = base()
        opened.append(conn)
        return conn

    import backend.sources.scheduler as scheduler_module
    import backend.sources.writer as writer_module

    original_drain = writer_module.SqliteWriter.drain
    state = {"calls": 0}
    writers = []

    async def cancelling_drain(self, **kwargs):
        """Cancel the run task as its FIRST drain begins.

        Requested before delegating, so the CancelledError is delivered at an await
        inside the real `drain` — a wrapper that parked on its own sleep would absorb
        the cancellation itself, and the drain under test would never see it.
        """
        state["calls"] += 1
        if state["calls"] == 1:
            writers.append(self)
            asyncio.current_task().cancel()
        return await original_drain(self, **kwargs)

    async def scenario():
        adapter = FakeAdapter("src", instances=("a",), body=fast(3))
        handle = scheduler(connect).start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        with contextlib.suppress(asyncio.CancelledError):
            await handle._task
        await asyncio.sleep(0)
        return handle, leftover_tasks()

    writer_module.SqliteWriter.drain = cancelling_drain
    try:
        handle, leftovers = run(scenario())
    finally:
        writer_module.SqliteWriter.drain = original_drain

    assert writers, "the run never reached its writer drain"
    assert handle.run_uid not in scheduler_module._LIVE_RUNS, (
        "a run cancelled mid-drain is fenced off from orphan recovery forever"
    )
    assert writers[0].closed, "the steps after the drain never ran"
    assert leftovers == [], f"cleanup was cut short and orphaned {leftovers}"
    assert opened, "the test never observed a connection"
    assert all(is_closed(conn) for conn in opened), "the writer's connection leaked"
    assert handle._task.cancelling() == 0, (
        "a suppressed cancellation left the task's cancelling() count elevated"
    )


def test_a_drain_gives_up_on_a_commit_that_will_never_finish(tmp_path):
    """The one state no amount of reaping can rescue, and therefore the only thing
    that proves the drain's bound is load-bearing.

    A batch checked out of the queue is one `queue.join()` counts as unfinished until
    the writer calls `task_done()` — which it does only after its transaction
    returns. So while a commit is stuck on its worker thread, the writer task is
    ALIVE (nothing to reap), the queue is EMPTY (nothing to abandon), and
    `_unfinished_tasks` is pinned above zero. `join()` on that queue never returns.
    Only `asyncio.timeout` ends it, which is why the run below finishes at all.

    Sibling of `test_a_writer_task_that_dies_outside_a_commit_does_not_hang_the_run`
    (test_source_scheduler.py), which covers the case reaping DOES rescue.
    """
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    original = writer_module.SqliteWriter._commit_once
    stuck = threading.Event()
    calls = {"n": 0}

    def stalling_commit(self, batch):
        calls["n"] += 1
        if calls["n"] >= 2:
            # Never set until the run is over: a commit that will not come back.
            stuck.wait(timeout=TEST_TIMEOUT)
        return original(self, batch)

    writer_module.SqliteWriter._commit_once = stalling_commit
    writers = []
    original_start = writer_module.SqliteWriter.start

    async def capturing_start(self):
        await original_start(self)
        writers.append(self)

    writer_module.SqliteWriter.start = capturing_start

    async def scenario():
        adapter = FakeAdapter("src", instances=("a",), body=fast(6))
        sched = scheduler(
            connect,
            batch_size=1,
            flush_interval_seconds=0.0,
            max_ops_per_transaction=1,
            writer_drain_timeout_seconds=0.3,
        )
        started = time.monotonic()
        result = await sched.start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)).wait()
        elapsed = time.monotonic() - started
        # Only now: the commit thread has to stay stuck for the whole run, but it
        # must be released before the loop shuts its executor down, or the timing
        # measured here would be swamped by that wait.
        stuck.set()
        return result, elapsed

    try:
        result, elapsed = run(scenario())
    finally:
        stuck.set()
        writer_module.SqliteWriter._commit_once = original
        writer_module.SqliteWriter.start = original_start

    assert result.status in ("succeeded", "partial", "failed")
    assert elapsed < 10.0, "the run waited on a commit that was never going to return"
    assert writers, "the writer never started"
    stats = writers[0].stats
    assert stats.drain_timeouts >= 1, (
        "the drain returned without either emptying the queue or recording that it "
        "gave up — the bound is not doing anything"
    )
    # The close then declined to pull the connection out from under the stuck commit
    # thread rather than closing it and racing a live statement.
    assert stats.unclosed_connections >= 1
    # And the run says so where a caller can see it. The persisted report cannot
    # carry a close-time timeout — the writer has to still be open to commit that
    # row — so the in-memory result is the only place it can appear.
    assert result.writer_drain_timeouts >= 1


def test_a_drain_timeout_during_the_close_still_reaches_the_run_result(tmp_path):
    """Where a close-time timeout can be seen, and where it structurally cannot.

    The persisted report is built and handed to the writer BEFORE the close — it has
    to be, since only an open writer can commit it — so a drain that gives up during
    `aclose` can never appear in the database. The in-memory result is the only place
    left, which makes WHEN the counter is read part of the contract rather than an
    incidental line ordering. Simulated rather than provoked: a real close-time
    timeout is always preceded by one in the cleanup drain that caused it, so nothing
    else here can isolate the read order.
    """
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    original_drain = writer_module.SqliteWriter.drain
    original_aclose = writer_module.SqliteWriter.aclose

    async def timing_out_aclose(self, **kwargs):
        # Exactly what a give-up inside the close looks like from the outside.
        self.stats.drain_timeouts += 1
        return await original_aclose(self, **kwargs)

    writer_module.SqliteWriter.aclose = timing_out_aclose
    try:
        adapter = FakeAdapter("src", instances=("a",), body=fast(2))
        result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))
    finally:
        writer_module.SqliteWriter.aclose = original_aclose
        writer_module.SqliteWriter.drain = original_drain

    assert result.status == "succeeded"
    assert result.writer_drain_timeouts == 1, (
        "a drain that gave up during the close is invisible to the caller"
    )
    assert report_of(connect, result.run_uid)["writer"]["drain_timeouts"] == 0, (
        "the persisted report claimed to know about a timeout that happened after it "
        "was written"
    )


def test_a_cancel_delivered_inside_aclose_still_releases_the_task_and_connection(tmp_path):
    """The cancel lands INSIDE the real `aclose`, at its own first await.

    The scheduler-level cleanup tests park a monkeypatched wrapper, so the cancel
    they deliver never reaches `aclose`'s body; they pin the run's sequencing, not
    the writer's release. This one parks `drain` — which `aclose` itself awaits — so
    the CancelledError is raised inside `aclose`, exactly where a `finally` that was
    missing, or a release that sat after the awaits, would lose a task and a file
    handle for the life of the process.
    """
    base = make_connect(tmp_path)
    opened = []

    def connect():
        conn = base()
        opened.append(conn)
        return conn

    import backend.sources.writer as writer_module

    async def scenario():
        writer = writer_module.SqliteWriter(connect)
        await writer.start()
        parked = asyncio.Event()

        async def parking_drain(*, timeout=None):
            parked.set()
            await asyncio.sleep(3600)
            return True  # pragma: no cover - the cancel arrives first

        # Instance attribute, so nothing global is patched and the real `aclose`
        # under test is the production one.
        writer.drain = parking_drain
        closer = asyncio.create_task(writer.aclose(timeout=5.0))
        await parked.wait()
        closer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await closer
        await asyncio.sleep(0)
        return writer, leftover_tasks()

    writer, leftovers = run(scenario())

    assert writer.closed, "aclose was interrupted and never released what it owns"
    assert leftovers == [], f"the writer task outlived its own close: {leftovers}"
    assert opened, "the test never observed a connection"
    assert all(is_closed(conn) for conn in opened), "the sqlite connection leaked"


def test_a_cancel_absorbed_inside_aclose_is_un_counted_not_just_swallowed(tmp_path):
    """`aclose` swallows a cancellation delivered at its wait for the writer task —
    it has to, or the release in its `finally` would be the thing that got skipped.

    Swallowing the exception is not the whole job: asyncio counts cancellations
    separately, and a task left with an elevated `cancelling()` makes the NEXT
    `asyncio.timeout` in it re-raise a bare `CancelledError` instead of the
    `TimeoutError` its caller is catching. The swallow has to be declared with
    `uncancel()`.
    """
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    async def scenario():
        writer = writer_module.SqliteWriter(connect)
        await writer.start()

        async def cancelling_drain(*, timeout=None):
            # Requested here, delivered at aclose's own next await — which is the
            # wait for the writer task, inside the suppression under test.
            asyncio.current_task().cancel()
            return True

        writer.drain = cancelling_drain
        closer = asyncio.create_task(writer.aclose(timeout=5.0))
        with contextlib.suppress(asyncio.CancelledError):
            await closer
        await asyncio.sleep(0)
        return writer, closer

    writer, closer = run(scenario())

    assert writer.closed
    assert not closer.cancelled(), "aclose let the cancellation escape instead of absorbing it"
    assert closer.cancelling() == 0, (
        "the absorbed cancellation was never un-counted; an outer asyncio.timeout "
        "would now report CancelledError instead of TimeoutError"
    )


def test_cancelling_a_run_twice_is_idempotent(tmp_path):
    """`RunHandle.cancel()` sets an already-set `Event` and re-cancels already
    cancelled tasks; both are no-ops by construction, but the contract is worth
    pinning explicitly rather than trusting that asyncio never changes it."""
    connect = make_connect(tmp_path)

    async def scenario():
        gate = asyncio.Event()
        adapter = FakeAdapter("long", instances=("a",), body=gated(before=1, gate=gate, after=3))
        handle = scheduler(connect, batch_size=1, flush_interval_seconds=0.0).start(
            kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if scalar(connect, "SELECT COUNT(*) FROM run_postings") >= 1:
                break
            await asyncio.sleep(0.005)
        handle.cancel()
        handle.cancel()  # must not be a second decision, a crash, or a hang
        gate.set()
        return await handle.wait()

    result = run(scenario())
    assert result.status == "cancelled"
    assert scalar(connect, "SELECT status FROM pipeline_runs") == "cancelled"


def test_cancelling_before_the_run_task_ever_ran_twice_is_still_idempotent(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("never", instances=("a",), body=fast(5))

    async def scenario():
        handle = scheduler(connect).start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        handle.cancel()
        handle.cancel()
        return await handle.wait()

    result = run(scenario())
    assert result.status == "cancelled"
    assert adapter.attempts == {}


# =========================================================================== #
# 3. Restart / orphan recovery
# =========================================================================== #
# The hand-constructed-fixture path (`_simulate_crashed_run`) already covers
# recovery, resume, and unattempted-row attempt numbering exhaustively in
# test_source_scheduler_persistence.py. New below: a REAL scheduler run abandoned
# via a genuine process-death simulation (the event loop is destroyed with the run
# task still pending, never given the chance to run its own cleanup `finally` —
# which is exactly what distinguishes "the process died" from "the run was
# cancelled") rather than hand-written rows standing in for the shape a crash
# leaves.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_a_process_killed_mid_run_leaves_recoverable_evidence_and_resumes_cleanly(tmp_path):
    # Abandoning the loop mid-flight (below) is the whole point of this test, and
    # it necessarily leaves coroutines Python's GC will later report as "never
    # awaited" / "destroyed but pending" when it finalizes them — collateral of
    # the simulation, not a bug the run itself has. `gc.collect()` right after
    # `loop.close()` forces that finalization to happen inside this test's own
    # scope (and hence its own filtered warnings) instead of a later, unrelated
    # test's.
    connect = make_connect(tmp_path)
    captured: dict[str, str] = {}

    async def setup():
        good = FakeAdapter("good", instances=("a",), body=fast(2))
        gate = asyncio.Event()
        stuck = FakeAdapter("stuck", instances=("b",), body=gated(before=1, gate=gate, after=1))
        handle = scheduler(connect, batch_size=1, flush_interval_seconds=0.0).start(
            kind=RunKind.FULL_DIRECT, plan=plan_of(good, stuck)
        )
        captured["run_uid"] = handle.run_uid
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if scalar(connect, "SELECT status FROM source_runs WHERE source='good:a'") == "succeeded":
                break
            await asyncio.sleep(0.005)
        assert (
            scalar(connect, "SELECT status FROM source_runs WHERE source='good:a'") == "succeeded"
        ), "the healthy target never even finished before the kill"
        # No cancel, no await on the handle: the coroutine is simply abandoned,
        # exactly as a killed process would abandon it.

    # A fresh, throwaway event loop rather than `asyncio.run`: `asyncio.run` tears
    # its own loop down by CANCELLING and AWAITING every pending task, which would
    # run this run's cleanup `finally` after all and defeat the whole simulation.
    # `loop.close()` on a loop with pending tasks discards them without running
    # anything further — the only in-process way to imitate a process exit.
    loop = asyncio.new_event_loop()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # "Task was destroyed but it is pending" etc.
        loop.run_until_complete(setup())
        loop.close()
        # Force the abandoned tasks to be finalized NOW, inside this test's own
        # scope, rather than whenever the garbage collector next happens to run —
        # which could be during a later, unrelated test and would misattribute
        # these expected-and-harmless "was never awaited" warnings to it.
        gc.collect()

    assert scalar(connect, "SELECT status FROM pipeline_runs") == "running", (
        "the process death simulation did not actually skip cleanup"
    )
    assert scalar(connect, "SELECT status FROM source_runs WHERE source='stuck:b'") == "running"

    report = recover_orphans(connect)

    assert report.run_uids == (captured["run_uid"],)
    assert scalar(connect, "SELECT status FROM pipeline_runs") == "interrupted"
    assert scalar(connect, "SELECT status FROM source_runs WHERE source='good:a'") == "succeeded", (
        "recovery must not touch an attempt that had already terminated cleanly"
    )
    assert scalar(connect, "SELECT status FROM source_runs WHERE source='stuck:b'") == "interrupted"
    assert scalar(connect, "SELECT COUNT(*) FROM source_runs WHERE status='running'") == 0

    resumed_good = FakeAdapter("good", instances=("a",), body=fast(2))
    resumed_stuck = FakeAdapter("stuck", instances=("b",), body=fast(1))
    result = run(
        scheduler(connect, batch_size=1, flush_interval_seconds=0.0).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(resumed_good, resumed_stuck),
            resume_run_uid=captured["run_uid"],
        )
    )

    assert result.status == "succeeded"
    assert result.target("good:a").status == "skipped", "the already-succeeded target was re-run"
    assert resumed_good.attempts == {}
    assert result.target("stuck:b").status == "succeeded"
    attempts = {
        (row["source"], row["attempt"]): row["status"]
        for row in rows(connect, "SELECT source, attempt, status FROM source_runs ORDER BY source, attempt")
    }
    assert attempts == {
        ("good:a", 1): "succeeded",
        ("stuck:b", 1): "interrupted",
        ("stuck:b", 2): "succeeded",
    }, "resume must renumber the orphaned attempt rather than restart or reuse it"


# =========================================================================== #
# 4. Backpressure, and a writer that dies while producers are genuinely parked
# =========================================================================== #
# A slow writer parking a fast producer behind a small queue, with no lost
# committed batches and correct final counts, is already covered by
# `test_backpressure_bounds_memory_when_the_writer_is_slower_than_the_adapters`
# (test_source_scheduler.py). `test_a_writer_task_that_dies_outside_a_commit_does_not_hang_the_run`
# in the same file covers writer death, but with a single target and the default
# 64-deep queue, which never actually fills — that test proves the run does not
# hang, not that backpressure specifically was in play. New below: a queue small
# enough, and enough concurrent producers, that the queue is provably FULL (a
# producer genuinely parked in `submit()`, not merely a busy writer) at the
# instant the writer dies.
def test_a_writer_death_with_a_genuinely_full_queue_fails_the_run_without_hanging_any_producer(
    tmp_path,
):
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    original_start = writer_module.SqliteWriter.start
    writers: list = []

    async def capturing_start(self):
        await original_start(self)
        writers.append(self)

    async def scenario():
        adapters = [FakeAdapter(f"src{n}", instances=("a",), body=fast(200)) for n in range(4)]
        sched = Scheduler(
            connect,
            config=SchedulerConfig(
                batch_size=1,
                flush_interval_seconds=0.0,
                queue_size=1,
                max_ops_per_transaction=1,
                max_concurrent_targets=4,
                writer_drain_timeout_seconds=1.0,
                **FAST_RETRY,
            ),
        )
        handle = sched.start(kind=RunKind.FULL_DIRECT, plan=plan_of(*adapters))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if writers and writers[0]._queue.full():
                break
            await asyncio.sleep(0.001)
        assert writers and writers[0]._queue.full(), "the queue was never actually full"
        writers[0]._task.cancel()
        result = await handle.wait()
        await asyncio.sleep(0)
        return result, leftover_tasks(), writers[0].stats.dropped

    writer_module.SqliteWriter.start = capturing_start
    started = time.monotonic()
    try:
        result, leftovers, dropped = run(scenario())
    finally:
        writer_module.SqliteWriter.start = original_start
    elapsed = time.monotonic() - started

    assert result.status == "failed"
    assert result.error["stage"] == "writer"
    assert leftovers == [], (
        f"a producer parked on the dead writer's full queue was left orphaned: {leftovers}"
    )
    assert elapsed < 10.0, "the run did not finish promptly after the writer died under backpressure"
    assert dropped >= 1, "operations lost to the dead writer under backpressure were not counted"
    # `FinishRun` was itself submitted to the dead writer, so the terminal row is
    # never written — the same deliberate behaviour
    # `test_a_fatal_writer_failure_leaves_the_run_row_running_until_recovery`
    # (test_source_scheduler_persistence.py) pins: only startup orphan recovery,
    # never a second write path, may reconcile it.
    assert scalar(connect, "SELECT status FROM pipeline_runs") == "running"


# This was the 2.6 scenario suite's one production finding, fixed by the post-put
# death check in `SqliteWriter.submit` (2.6 follow-up). The test below is the
# regression test for it; the companion after it pins the `submit_soon` half of the
# same race.
def test_a_submit_parked_on_a_full_queue_when_the_writer_dies_must_see_writer_error(tmp_path):
    """The race, and why one check before the put cannot close it.

    `_reap`/`_abandon` drain the queue in a loop with no `await` inside it, so
    draining runs to completion in one uninterrupted step. That drain wakes a parked
    putter's future, but the woken task only runs once control returns to the event
    loop — which does not happen until `_abandon`'s loop has already returned, having
    found the queue empty and stopped. The putter then resumes, finds room, and
    completes its `put()` normally.

    So the producer's own pre-put check is worthless here: the writer was alive when
    it ran. Only a check AFTER the put can observe the death, which is what `submit`
    now makes — otherwise `submit()` returns with no exception and the op it just
    handed over sits in the queue uncounted, never to be committed, until some later
    `_reap()` happens to find it, by which point the caller has long since believed
    its data was handed off.
    """
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    async def scenario():
        writer = writer_module.SqliteWriter(connect, queue_size=1)

        async def dummy():
            await asyncio.sleep(3600)

        # Stands in for a writer task that `submit()`'s own pre-check still sees as
        # alive, so the queue genuinely fills before it dies — not a shortcut, the
        # real precondition the race depends on.
        task = asyncio.create_task(dummy())
        writer._task = task

        filler = writer_module.EmitEvents()
        writer._queue.put_nowait(filler)  # queue at capacity (maxsize=1)

        real_op = writer_module.EmitEvents()
        producer = asyncio.create_task(writer.submit(real_op))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not producer.done(), "the producer never actually parked on the full queue"

        task.cancel()
        await asyncio.sleep(0)  # let the cancellation land; task.done() becomes True

        # Exactly what `submit()`, `submit_soon()`, and `drain()` all call
        # internally the moment they next touch the writer — not a test shortcut.
        writer._reap()

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return producer, writer

    producer, writer = run(scenario())

    assert producer.done()
    assert producer.exception() is not None, (
        "a submit() parked on a full queue at the moment the writer died returned "
        "successfully, silently losing its op"
    )
    assert isinstance(producer.exception(), writer_module.WriterError), producer.exception()
    # And the op it handed over is accounted for rather than left in the queue for a
    # later caller to find: raising while the operation is still queued would say
    # "lost" about something that is merely late.
    assert writer._queue.qsize() == 0
    assert writer.stats.dropped >= 1


def test_submitting_to_a_closed_writer_is_refused_rather_than_silently_queued(tmp_path):
    """A cleanly stopped writer is as unable to commit as a dead one.

    Its queue is a container nobody will read again, and `drain()` on it returns
    without waiting for anything — so an accepted submit is a handoff to nowhere,
    reported as success. The two submit paths keep their usual split: the awaited one
    raises, the deferred one counts the loss, because its callers are `finally` blocks
    that must not be handed a new exception.
    """
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    async def scenario():
        writer = writer_module.SqliteWriter(connect)
        await writer.start()
        await writer.aclose()
        assert writer.closed
        dropped_before = writer.stats.dropped
        raised = None
        try:
            await writer.submit(writer_module.EmitEvents())
        except writer_module.WriterError as exc:
            raised = exc
        deferred = writer.submit_soon(writer_module.EmitEvents())
        return writer, raised, deferred, dropped_before

    writer, raised, deferred, dropped_before = run(scenario())

    assert raised is not None, "a closed writer accepted an operation it can never commit"
    assert "closed" in str(raised)
    assert deferred is None, "submit_soon queued an operation on a closed writer"
    assert writer.stats.dropped == dropped_before + 1


def test_a_deferred_submit_parked_on_a_full_queue_counts_its_loss_when_the_writer_dies(
    tmp_path,
):
    """The `submit_soon` half of the same race.

    A deferred put parks and is woken identically, but `submit_soon` deliberately
    never raises — its callers are `finally` blocks already handling a failure. The
    obligation it does carry is accounting: "the loss is counted in `stats.dropped`".
    Without a post-put check the operation is neither committed nor counted, and the
    writer's own report would understate what the run lost.
    """
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    async def scenario():
        writer = writer_module.SqliteWriter(connect, queue_size=1)

        async def dummy():
            await asyncio.sleep(3600)

        task = asyncio.create_task(dummy())
        writer._task = task
        writer._queue.put_nowait(writer_module.EmitEvents())  # at capacity

        deferred = writer.submit_soon(writer_module.EmitEvents())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert deferred is not None and not deferred.done(), (
            "the deferred put never actually parked on the full queue"
        )

        task.cancel()
        await asyncio.sleep(0)
        writer._reap()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return deferred, writer

    deferred, writer = run(scenario())

    assert deferred.done()
    assert deferred.exception() is None, "submit_soon must never raise at its caller"
    assert writer._queue.qsize() == 0, "the op was left queued for a writer that is gone"
    assert writer.stats.dropped >= 1, "an operation lost to the dead writer was not counted"


# =========================================================================== #
# 5. Partial success: absence licence scoping and last-known-good retention
# =========================================================================== #
# `test_source_absence.py` already covers LICENCE / SCOPE / INVENTORY /
# OBSERVATION / EVIDENCE for absence marking exhaustively, and
# `test_a_failed_source_marks_nothing_absent_and_keeps_last_known_good` there is
# the roadmap line verbatim. `test_partial_success_three_of_four_land_and_the_failure_records_evidence`
# (test_source_scheduler.py) covers the PARTIAL run-status + per-source-scope
# side. New below: the two combined in ONE run, with degraded freshness read
# back immediately afterwards, since the roadmap groups all three under one
# scenario and no existing test asserts all three from a single run.
def test_a_mixed_run_settles_partial_licenses_only_the_winners_and_degrades_freshness_for_the_loser(
    tmp_path,
):
    connect = make_connect(tmp_path)
    from backend.sources.scheduler import source_instance_freshness, successful_source_scopes

    healthy_one = FakeAdapter("healthy", instances=("a",), body=fast(2))
    healthy_two = FakeAdapter("healthy", instances=("b",), body=fast(2))
    broken = FakeAdapter("broken", instances=("x",), body=permanent_always())

    first = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(healthy_one, healthy_two, broken)
        )
    )
    assert first.status == "partial"

    second_broken = FakeAdapter("broken", instances=("x",), body=permanent_always())
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(second_broken)))
    assert second.status == "partial"

    conn = connect()
    try:
        scopes = {s["source"]: s for s in successful_source_scopes(conn, second.run_uid)}
        freshness = {row["source"]: row for row in source_instance_freshness(conn)}
    finally:
        conn.close()

    assert "broken:x" not in scopes, "a failed attempt must never licence absence marking"
    assert scalar(
        connect, "SELECT COUNT(*) FROM postings WHERE posting_id IN "
        "(SELECT posting_id FROM posting_aliases WHERE namespace='broken:x') "
        "AND absent_since IS NOT NULL"
    ) == 0, "the broken source's last-known-good postings were marked absent"

    degraded = freshness["broken:x"]
    assert degraded["licenses_absence"] is False
    assert degraded["consecutive_failed_runs"] >= 1
    healthy_fresh = [row for key, row in freshness.items() if key.startswith("healthy:")]
    assert all(row["licenses_absence"] is True for row in healthy_fresh)
    assert all(row["consecutive_failed_runs"] == 0 for row in healthy_fresh)


# =========================================================================== #
# 6. SQLite contention: an independent connection holding the write lock
# =========================================================================== #
# `test_a_reader_can_query_throughout_a_run_without_the_database_locking`
# (test_source_scheduler_persistence.py) already covers a concurrent READER
# throughout a run. New below: a concurrent, fully independent connection that
# actually holds the database's WRITE lock (a real external writer, not a
# reader) while the scheduler's own writer tries to commit — forcing SQLite's
# busy handler to make the writer wait, and, when the busy_timeout itself is
# exceeded, to raise back to Python — and `_commit`'s own retry is what has to
# absorb that without corrupting or losing anything.
def test_an_external_connection_holding_the_write_lock_forces_a_recorded_busy_retry(tmp_path):
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    original_init = writer_module.SqliteWriter.__init__

    def short_timeout_init(self, *args, **kwargs):
        # A busy_timeout short enough that the external hold below genuinely
        # outlasts ONE attempt but not two, so SQLite raises OperationalError to
        # Python on the first try (instead of resolving the contention invisibly
        # inside its own C-level wait) and `_commit`'s single retry then lands
        # cleanly once the lock has released.
        kwargs.setdefault("busy_timeout_ms", 200)
        return original_init(self, *args, **kwargs)

    hold_started = threading.Event()
    writers = []
    original_start = writer_module.SqliteWriter.start

    async def capturing_start(self):
        await original_start(self)
        writers.append(self)

    def hold_the_write_lock():
        """Hold the write lock until the writer has actually recorded a busy retry.

        Not for a fixed duration: a fixed hold has to be long enough that the first
        commit attempt meets the lock and short enough that the second does not,
        which is a window measured in tens of milliseconds and therefore a flake on a
        loaded machine. Releasing on the observed retry makes both halves certain —
        attempt one cannot succeed, attempt two cannot fail.
        """
        raw = sqlite3.connect(str(connect.path))
        try:
            raw.execute("BEGIN IMMEDIATE")
            hold_started.set()
            deadline = time.monotonic() + TEST_TIMEOUT
            while time.monotonic() < deadline:
                if writers and writers[0].stats.busy_retries >= 1:
                    return
                time.sleep(0.005)
        finally:
            raw.commit()
            raw.close()

    writer_module.SqliteWriter.__init__ = short_timeout_init
    writer_module.SqliteWriter.start = capturing_start
    holder = threading.Thread(target=hold_the_write_lock, daemon=True)
    holder.start()
    try:
        assert hold_started.wait(timeout=5), "the external connection never took the write lock"
        adapter = FakeAdapter("src", instances=("a", "b"), body=fast(3))
        result = run(
            # Orphan recovery on start opens its own connection and can itself
            # absorb the whole contention window on the default (generous)
            # busy_timeout before the writer under test ever attempts a commit —
            # which would prove nothing about `_commit`'s own retry. Disabled so
            # the writer's commit is what actually meets the lock.
            scheduler(
                connect, batch_size=1, flush_interval_seconds=0.0, recover_orphans_on_start=False
            ).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        )
    finally:
        writer_module.SqliteWriter.__init__ = original_init
        writer_module.SqliteWriter.start = original_start
        holder.join(timeout=TEST_TIMEOUT)

    assert result.status == "succeeded", "contention that resolves within the budget must not fail the run"
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 6, "no batch was lost to the contention"
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 6
    report = report_of(connect, result.run_uid)
    assert report["writer"]["busy_retries"] >= 1, (
        "the external lock never actually forced a retry — the scenario proves nothing"
    )


# =========================================================================== #
# 7. Hanging-adapter isolation (follow-up 1)
# =========================================================================== #
# PROVISIONAL, BY CONSTRUCTION. This test asserts that a limitation still EXISTS,
# so it is the one test in this suite that is supposed to fail one day: the day
# scheduler-side routing for `ExecutionMode.SUBPROCESS` lands, the stall it measures
# disappears and this test must be DELETED rather than repaired. It is here to keep
# the accepted Phase 2 limitation documented in executable form instead of in a
# comment nobody re-checks.
def test_a_blocking_in_process_adapter_freezes_the_loop_a_documented_phase_2_limitation(
    tmp_path,
):
    """Follow-up-1 VERDICT: NO, a blocking in-process adapter is NOT isolated —
    `ExecutionMode.SUBPROCESS` is descriptor metadata the scheduler never reads to
    change how a target is scheduled (confirmed by inspection: nothing in
    `scheduler.py` branches on `descriptor.execution`). Every target's fetch
    coroutine runs cooperatively on the SAME event loop regardless of what it
    declares. The isolation the mode promises exists only because the real JobSpy
    adapter hands its blocking work to an actual child process (see the next
    test); an adapter that does the blocking work in-process instead — a bug in
    that adapter, not something the scheduler could see coming — freezes the whole
    loop for as long as the block lasts, deadlines included, since a frozen loop
    cannot even notice its own clock ran out until control returns to it.

    Measured against the LOOP rather than against another target: an independent
    ticker task, started before the run and running throughout it, records when it
    is scheduled. A gap in its ticks is the freeze, directly. The earlier version of
    this test compared two targets' start stamps, which depended on which of two
    tasks asyncio happened to run first — an ordering nothing guarantees.
    """
    connect = make_connect(tmp_path)

    async def scenario():
        ticks: list[float] = []
        stop = asyncio.Event()

        async def ticker():
            while not stop.is_set():
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        watch = asyncio.create_task(ticker())
        await asyncio.sleep(0)  # the ticker is running before the run starts
        blocker = FakeAdapter(
            "blocker",
            instances=("a",),
            body=blocking(0.3),
            descriptor=descriptor_for(
                "blocker", execution=ExecutionMode.SUBPROCESS, deadline=5.0
            ),
        )
        healthy = FakeAdapter(
            "healthy", instances=("a",), body=fast(2), descriptor=descriptor_for("healthy", deadline=5.0)
        )
        result = await scheduler(connect, max_concurrent_targets=4).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(blocker, healthy)
        )
        stop.set()
        await watch
        return result, ticks

    result, ticks = run(scenario())

    assert result.status == "succeeded"
    assert len(ticks) >= 2, "the ticker never ran; the measurement is meaningless"
    worst = max(b - a for a, b in zip(ticks, ticks[1:]))
    assert worst >= 0.25, (
        f"the event loop's longest stall during the run was {worst:.3f}s, so a "
        "synchronously blocking in-process adapter no longer freezes it. That is "
        "GOOD NEWS: scheduler-side isolation for ExecutionMode.SUBPROCESS now "
        "exists, follow-up 1 is closed, and this test should be deleted rather "
        "than adjusted."
    )


def test_the_real_jobspy_subprocess_adapter_is_cancellable_through_the_scheduler_and_orphans_no_child(
    tmp_path, monkeypatch
):
    """Follow-up-1, the other half, against the REAL adapter (not a stand-in):
    when the adapter itself correctly hands its blocking work to a child process
    — as JobSpy does — the scheduler's ordinary deadline cancellation reaches
    across that process boundary, kills the child, and a healthy source sharing
    the run is genuinely unaffected. Contrast with the previous test: the
    isolation is real here only because the adapter provides it; the scheduler
    contributes nothing beyond the same cancellation every other target gets.
    """
    from backend.sources.adapters import jobspy

    procs: list = []
    real_exec = asyncio.create_subprocess_exec

    async def spy(*args, **kwargs):
        proc = await real_exec(*args, **kwargs)
        procs.append(proc)
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spy)

    connect = make_connect(tmp_path)
    fake_child = str(fixture_path("jobspy", "fake_child.py"))
    stream = str(fixture_path("jobspy", "child_stream.ndjson"))

    target = SourceTarget(
        source_key="jobspy",
        instance_key="indeed",
        label="JobSpy indeed",
        params={
            "site": "indeed",
            "search_terms": ("technical support engineer",),
            "searches": ({"location": "San Francisco Bay Area, CA", "is_remote": False},),
            "results_wanted": 25,
            "country_indeed": "USA",
            "hours_old": 720,
            "title_cap": 5,
            "child_command": (sys.executable, fake_child, "hang", stream),
        },
        inventory_scope=InventoryScope.PARTIAL,
        deadline_seconds=0.3,
    )
    healthy = FakeAdapter("healthy", instances=("a", "b"), body=fast(2))
    plan = [(jobspy.ADAPTER, target), *plan_of(healthy)]

    started = time.monotonic()
    result = run(
        scheduler(connect, max_concurrent_targets=4).run(kind=RunKind.FULL_DIRECT, plan=plan)
    )
    elapsed = time.monotonic() - started

    jobspy_result = result.target("jobspy:indeed")
    assert jobspy_result.status == "timeout"
    assert jobspy_result.fetched >= 1, "the child never streamed anything before the cancel"
    assert result.target("healthy:a").succeeded
    assert result.target("healthy:b").succeeded
    assert elapsed < 5.0, "a 0.3s subprocess deadline should not have cost the run more than this"

    assert len(procs) == 1
    assert procs[0].returncode is not None, "the child was never reaped after the deadline cancel"
    with pytest.raises(ProcessLookupError):
        os.kill(procs[0].pid, 0)
