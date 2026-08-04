"""Scheduling behaviour: concurrency, deadlines, retries, cancellation, backpressure.

The properties asserted here are the ones the Success Contract is written in
terms of:

  * "one failed source adds no more than its own deadline"
  * "at most two total attempts per source run"
  * "new/changed jobs appear incrementally before the whole run finishes"
    (the persistence half of that lives in `test_source_scheduler_persistence`)

Async paths use `asyncio.run` inside sync tests, matching `test_source_contract`
and `test_source_greenhouse`: the suite gains no plugin dependency.

Timing assertions are deliberately loose on the upper bound and tight on the
lower: a slow CI box may take longer than expected, but a scheduler that returns
*before* a deadline it was supposed to enforce is a real bug.
"""
import asyncio
import time

import pytest

from backend.sources.contract import (
    Disposition,
    InventoryScope,
    PayloadError,
    PermanentSourceError,
    RunKind,
    TransientSourceError,
)
from backend.sources.scheduler import Scheduler, SchedulerConfig
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    Probe,
    descriptor_for,
    fast,
    gated,
    hanging,
    make_connect,
    permanent_always,
    plan_of,
    raising,
    scalar,
    subprocess_like,
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


# --------------------------------------------------------------------------- #
# Bounded concurrency
# --------------------------------------------------------------------------- #
def test_global_concurrency_limit_is_actually_enforced(tmp_path):
    connect = make_connect(tmp_path)
    probe = Probe()
    adapter = FakeAdapter(
        "wide",
        instances=[f"board{n}" for n in range(12)],
        body=fast(2, hold=0.05),
        probe=probe,
    )

    result = run(
        scheduler(connect, max_concurrent_targets=3).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)
        )
    )

    assert len(result.targets) == 12
    assert probe.peak <= 3, f"observed {probe.peak} targets in flight, limit was 3"
    assert probe.peak == 3, "limit was never reached; the test would not detect a broken gate"
    assert result.peak_concurrency == probe.peak
    assert all(t.succeeded for t in result.targets)


def test_per_host_limit_binds_across_two_sources_sharing_a_host(tmp_path):
    """Two sources behind one API host share one ceiling, and the politer wins."""
    connect = make_connect(tmp_path)
    probe = Probe()
    host = "api.shared.example"
    polite = FakeAdapter(
        "polite",
        instances=[f"p{n}" for n in range(4)],
        body=fast(1, hold=0.05),
        descriptor=descriptor_for("polite", per_host_concurrency=2),
        host=host,
        probe=probe,
    )
    greedy = FakeAdapter(
        "greedy",
        instances=[f"g{n}" for n in range(4)],
        body=fast(1, hold=0.05),
        descriptor=descriptor_for("greedy", per_host_concurrency=8),
        host=host,
        probe=probe,
    )

    result = run(
        scheduler(connect, max_concurrent_targets=8).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(polite, greedy)
        )
    )

    assert probe.peak <= 2, f"per-host ceiling breached: {probe.peak} in flight on one host"
    assert result.peak_by_host[host] == 2
    assert len(result.succeeded_targets) == 8


def test_per_source_max_concurrent_targets_is_respected(tmp_path):
    connect = make_connect(tmp_path)
    probe = Probe()
    limited = FakeAdapter(
        "limited",
        instances=[f"a{n}" for n in range(6)],
        body=fast(1, hold=0.04),
        descriptor=descriptor_for("limited", max_concurrent_targets=2, per_host_concurrency=8),
        probe=probe,
    )
    other = FakeAdapter(
        "other",
        instances=[f"b{n}" for n in range(4)],
        body=fast(1, hold=0.04),
        descriptor=descriptor_for("other", per_host_concurrency=8),
        probe=probe,
    )

    run(
        scheduler(connect, max_concurrent_targets=8).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(limited, other)
        )
    )

    assert probe.per_source_peak["limited"] <= 2
    assert probe.per_source_peak["other"] >= 2, "the unlimited source must not be throttled too"


# --------------------------------------------------------------------------- #
# Deadlines
# --------------------------------------------------------------------------- #
def test_hanging_source_is_contained_to_its_own_deadline(tmp_path):
    """The Success Contract line, measured: a hang costs its deadline and nothing more."""
    connect = make_connect(tmp_path)
    stuck = FakeAdapter(
        "stuck",
        instances=("frozen",),
        body=hanging(before=2),
        descriptor=descriptor_for("stuck", deadline=0.3),
    )
    healthy = FakeAdapter("healthy", instances=("a", "b", "c"), body=fast(3))

    started = time.monotonic()
    result = run(
        scheduler(connect, max_concurrent_targets=8).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(stuck, healthy)
        )
    )
    elapsed = time.monotonic() - started

    hung = result.target("stuck:frozen")
    assert hung.status == "timeout"
    assert hung.error["type"] == "DeadlineExceeded"
    assert hung.error["retryable"] is False
    # One attempt only: a deadline is not a transient error.
    assert len(hung.attempts) == 1
    assert elapsed >= 0.3, "the deadline was not actually waited out"
    assert elapsed < 3.0, f"a 0.3s hang cost the run {elapsed:.2f}s"
    assert result.status == "partial"
    assert [t.source_run_key for t in result.succeeded_targets] == [
        "healthy:a",
        "healthy:b",
        "healthy:c",
    ]


def test_records_delivered_before_the_deadline_are_kept(tmp_path):
    connect = make_connect(tmp_path)
    stuck = FakeAdapter(
        "stuck",
        instances=("frozen",),
        body=hanging(before=4),
        descriptor=descriptor_for("stuck", deadline=0.25),
    )

    result = run(
        scheduler(connect, batch_size=2, flush_interval_seconds=0.01).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(stuck)
        )
    )

    target = result.target("stuck:frozen")
    assert target.status == "timeout"
    assert target.fetched == 4
    assert target.accepted == 4, "partial results from a timed-out source must survive"
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 4


def test_one_target_deadline_does_not_stall_the_others(tmp_path):
    connect = make_connect(tmp_path)
    stuck = FakeAdapter(
        "stuck",
        instances=("frozen",),
        body=hanging(),
        descriptor=descriptor_for("stuck", deadline=1.0),
    )
    quick = FakeAdapter("quick", instances=("a",), body=fast(2))

    async def scenario():
        sched = scheduler(connect, max_concurrent_targets=4)
        handle = sched.start(kind=RunKind.FULL_DIRECT, plan=plan_of(stuck, quick))
        # The healthy source must land while the hung one is still burning its
        # deadline. Poll a *separate* connection, which is the real question.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if scalar(connect, "SELECT COUNT(*) FROM run_postings") >= 2:
                break
            await asyncio.sleep(0.01)
        else:  # pragma: no cover - failure path
            pytest.fail("healthy source did not commit while another target was hung")
        early = scalar(connect, "SELECT COUNT(*) FROM run_postings")
        return early, await handle.wait()

    early, result = run(scenario())
    assert early >= 2
    assert result.target("quick:a").succeeded
    assert result.target("stuck:frozen").status == "timeout"


# --------------------------------------------------------------------------- #
# Retry classification
# --------------------------------------------------------------------------- #
def test_transient_failure_is_retried_exactly_once_and_can_succeed(tmp_path):
    connect = make_connect(tmp_path)
    flaky = FakeAdapter("flaky", instances=("board",), body=transient_then(count=3))

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(flaky)))

    target = result.target("flaky:board")
    assert target.status == "succeeded"
    assert [a.attempt for a in target.attempts] == [1, 2]
    assert target.attempts[0].status == "failed"
    assert target.attempts[0].disposition is Disposition.TRANSIENT
    assert flaky.attempts["board"] == 2
    assert scalar(
        connect, "SELECT COUNT(*) FROM source_runs WHERE source='flaky:board'"
    ) == 2


def test_transient_failure_stops_after_two_attempts(tmp_path):
    connect = make_connect(tmp_path)
    always = FakeAdapter(
        "always", instances=("board",), body=transient_then(succeed_on_attempt=99)
    )

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(always)))

    target = result.target("always:board")
    assert target.status == "failed"
    assert len(target.attempts) == 2, "two total attempts is the contract ceiling"
    assert always.attempts["board"] == 2


@pytest.mark.parametrize(
    "factory",
    [
        lambda: PermanentSourceError("404 board", status=404),
        lambda: PayloadError("envelope changed shape"),
    ],
)
def test_permanent_failures_are_never_retried(tmp_path, factory):
    connect = make_connect(tmp_path)
    broken = FakeAdapter("broken", instances=("board",), body=raising(factory))

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(broken)))

    target = result.target("broken:board")
    assert target.status == "failed"
    assert len(target.attempts) == 1
    assert target.attempts[0].disposition is Disposition.PERMANENT
    assert broken.attempts["board"] == 1


def test_unclassified_adapter_exception_is_treated_as_permanent(tmp_path):
    """A KeyError from a parser is a bug; retrying spends budget reproducing it."""
    connect = make_connect(tmp_path)
    buggy = FakeAdapter("buggy", instances=("board",), body=raising(lambda: KeyError("title")))

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(buggy)))

    target = result.target("buggy:board")
    assert target.status == "failed"
    assert len(target.attempts) == 1
    assert target.error["type"] == "KeyError"
    assert target.error["disposition"] == str(Disposition.PERMANENT)
    assert buggy.attempts["board"] == 1


def test_mid_stream_transient_failure_keeps_delivered_records_and_retries(tmp_path):
    connect = make_connect(tmp_path)
    flaky = FakeAdapter(
        "midstream",
        instances=("board",),
        body=transient_then(count=3, before=2),
    )

    result = run(
        scheduler(connect, batch_size=1, flush_interval_seconds=0.0).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(flaky)
        )
    )

    target = result.target("midstream:board")
    assert target.status == "succeeded"
    assert len(target.attempts) == 2
    # Attempt 1 delivered records 0 and 1; attempt 2 re-delivered 0..2. Identity
    # dedupe means three postings, not five.
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 3
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 3


def test_retry_is_skipped_when_the_target_budget_cannot_fund_it(tmp_path):
    """A transient failure raised late in the deadline must not buy a second full
    deadline. The per-target budget refuses the retry and records the refusal."""
    connect = make_connect(tmp_path)

    # Held at second-scale rather than at the smallest numbers that express the
    # property: the failure must land inside the deadline (0.53s of 1.0s) and the
    # backoff must then overrun the budget (0.53 + 0.67 > 1.0). Both margins are
    # ~0.5s of wall clock, so a stalled test box changes the timings without
    # changing which branch is taken.
    async def late_transient(adapter, target, ctx):
        await asyncio.sleep(0.53)
        raise TransientSourceError("slow failure")
        yield  # pragma: no cover - makes this an async generator

    late = FakeAdapter(
        "late",
        instances=("board",),
        body=late_transient,
        descriptor=descriptor_for("late", deadline=1.0),
    )

    started = time.monotonic()
    result = run(
        Scheduler(
            connect,
            config=SchedulerConfig(
                retry_base_delay_seconds=0.67,
                retry_jitter=0.0,
                attempt_budget_multiplier=1.0,
            ),
        ).run(kind=RunKind.FULL_DIRECT, plan=plan_of(late))
    )
    elapsed = time.monotonic() - started

    target = result.target("late:board")
    assert target.status == "failed"
    assert target.attempts[0].disposition is Disposition.TRANSIENT
    assert len(target.attempts) == 1, "the retry should have been refused, not taken"
    assert late.attempts["board"] == 1
    assert elapsed < 3.0
    skipped = [e for e in _events(connect) if e["event_type"] == "source.retry_skipped"]
    assert len(skipped) == 1
    import json as _json

    assert _json.loads(skipped[0]["payload_json"])["reason"] == "target budget exhausted"


def test_a_transient_failure_early_in_the_deadline_still_gets_its_retry(tmp_path):
    """The mirror of the budget test: the refusal must be about the budget, not a
    blanket ban on retrying transient errors."""
    connect = make_connect(tmp_path)
    flaky = FakeAdapter(
        "early",
        instances=("board",),
        body=transient_then(count=1),
        descriptor=descriptor_for("early", deadline=0.3),
    )

    result = run(
        Scheduler(
            connect,
            config=SchedulerConfig(
                retry_base_delay_seconds=0.01,
                retry_jitter=0.0,
                attempt_budget_multiplier=1.0,
            ),
        ).run(kind=RunKind.FULL_DIRECT, plan=plan_of(flaky))
    )

    assert result.target("early:board").status == "succeeded"
    assert flaky.attempts["board"] == 2
    assert [e["event_type"] for e in _events(connect) if "retry_skipped" in e["event_type"]] == []


def _events(connect):
    conn = connect()
    try:
        return conn.execute("SELECT * FROM run_events ORDER BY sequence").fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Partial success
# --------------------------------------------------------------------------- #
def test_partial_success_three_of_four_land_and_the_failure_records_evidence(tmp_path):
    connect = make_connect(tmp_path)
    ok = FakeAdapter("ok", instances=("a", "b", "c"), body=fast(2))
    dead = FakeAdapter("dead", instances=("x",), body=permanent_always())

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(ok, dead)))

    assert result.status == "partial"
    assert len(result.succeeded_targets) == 3
    assert len(result.failed_targets) == 1
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 6

    row = _source_run(connect, "dead:x")
    assert row["status"] == "failed"
    assert row["started_at"] and row["finished_at"]
    assert '"status": 404' in row["error_json"] or '"status":404' in row["error_json"]
    assert row["inventory_scope"] == str(InventoryScope.COMPLETE)
    # The healthy sources are still individually licensed for Phase 2.4.
    scopes = {
        r["source"]: r["inventory_scope"]
        for r in _rows(connect, "SELECT source, inventory_scope FROM source_runs WHERE status='succeeded'")
    }
    assert set(scopes) == {"ok:a", "ok:b", "ok:c"}


def test_a_failed_source_leaves_a_previous_run_s_data_intact(tmp_path):
    connect = make_connect(tmp_path)
    good = FakeAdapter("src", instances=("board",), body=fast(3))
    result_one = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(good)))
    before = _postings(connect)
    assert len(before) == 3

    broken = FakeAdapter("src", instances=("board",), body=permanent_always())
    result_two = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(broken)))

    assert result_one.status == "succeeded"
    assert result_two.status == "partial"
    assert _postings(connect) == before, "a failed source must not disturb last-known-good data"
    # Membership for the failed run is empty; nothing claims those postings were
    # seen. Absence marking (Phase 2.4) reads status + scope, both recorded.
    assert (
        scalar(
            connect,
            "SELECT COUNT(*) FROM run_postings WHERE run_uid=?",
            (result_two.run_uid,),
        )
        == 0
    )


# --------------------------------------------------------------------------- #
# Incremental visibility and backpressure
# --------------------------------------------------------------------------- #
def test_records_are_committed_before_the_run_finishes(tmp_path):
    connect = make_connect(tmp_path)

    async def scenario():
        gate = asyncio.Event()
        adapter = FakeAdapter(
            "trickle", instances=("board",), body=gated(before=3, gate=gate, after=2)
        )
        sched = scheduler(connect, batch_size=1, flush_interval_seconds=0.0)
        handle = sched.start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        deadline = time.monotonic() + 5
        seen = 0
        while time.monotonic() < deadline:
            seen = scalar(connect, "SELECT COUNT(*) FROM run_postings")
            if seen >= 3:
                break
            await asyncio.sleep(0.01)
        assert not handle.done, "the run finished before visibility could be observed"
        gate.set()
        return seen, await handle.wait()

    seen, result = run(scenario())
    assert seen >= 3, "no record was durable until the run ended"
    assert result.status == "succeeded"
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 5


def test_backpressure_bounds_memory_when_the_writer_is_slower_than_the_adapters(tmp_path):
    """A tiny queue plus a deliberately slow commit must park producers, not buffer."""
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    original = writer_module.SqliteWriter._commit_once
    original_submit = writer_module.SqliteWriter.submit
    depths = []
    parked = {"n": 0}

    def slow_commit(self, batch):
        depths.append(self._queue.qsize())
        time.sleep(0.005)
        return original(self, batch)

    async def counting_submit(self, op):
        # A full queue at submit time means this producer is about to park. That
        # is backpressure, measured at the only place it can be observed.
        if self._queue.full():
            parked["n"] += 1
        return await original_submit(self, op)

    writer_module.SqliteWriter._commit_once = slow_commit
    writer_module.SqliteWriter.submit = counting_submit
    try:
        adapters = [
            FakeAdapter(f"src{n}", instances=("board",), body=fast(60)) for n in range(4)
        ]
        result = run(
            scheduler(
                connect,
                batch_size=1,
                flush_interval_seconds=0.0,
                queue_size=4,
                max_ops_per_transaction=2,
                max_concurrent_targets=4,
            ).run(kind=RunKind.FULL_DIRECT, plan=plan_of(*adapters))
        )
    finally:
        writer_module.SqliteWriter._commit_once = original
        writer_module.SqliteWriter.submit = original_submit

    assert result.status == "succeeded"
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 240
    assert max(depths) <= 4, f"queue grew past its bound: {max(depths)}"
    assert _report(connect, result.run_uid)["writer"]["max_queue_depth"] <= 4
    assert parked["n"] >= 20, (
        "producers never parked; the writer kept up and the test proves nothing "
        f"(parked {parked['n']} times)"
    )


def test_a_deadline_firing_while_a_flush_is_parked_loses_no_delivered_record(tmp_path):
    """Backpressure plus a deadline is the narrow window where a batch can be in
    neither the buffer nor the queue. Every record pulled from the adapter must
    still be committed."""
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    original = writer_module.SqliteWriter._commit_once
    commits = {"n": 0}

    def stalling_commit(self, batch):
        commits["n"] += 1
        if commits["n"] >= 2:
            # Longer than the target's deadline, so the next submit parks on a
            # full queue and is still parked when the timeout fires.
            time.sleep(0.35)
        return original(self, batch)

    writer_module.SqliteWriter._commit_once = stalling_commit
    try:
        adapter = FakeAdapter(
            "parked",
            instances=("board",),
            body=fast(8),
            descriptor=descriptor_for("parked", deadline=0.12),
        )
        result = run(
            scheduler(
                connect,
                batch_size=1,
                flush_interval_seconds=0.0,
                queue_size=1,
                max_ops_per_transaction=1,
            ).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        )
    finally:
        writer_module.SqliteWriter._commit_once = original

    target = result.target("parked:board")
    assert target.status == "timeout", "the scenario did not reach the deadline"
    assert target.fetched >= 2, "the adapter never got far enough to park a flush"
    assert (
        scalar(connect, "SELECT COUNT(*) FROM run_postings") == target.fetched
    ), "a record delivered by the adapter was dropped"
    assert target.accepted == target.fetched


def test_batching_uses_far_fewer_transactions_than_records(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("bulk", instances=("board",), body=fast(400))

    result = run(
        scheduler(connect, batch_size=50, flush_interval_seconds=10.0).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)
        )
    )

    assert result.target("bulk:board").accepted == 400
    report = _report(connect, result.run_uid)
    assert report["writer"]["records"] == 400
    assert report["writer"]["transactions"] < 40, report["writer"]


# --------------------------------------------------------------------------- #
# Cancellation
# --------------------------------------------------------------------------- #
def test_cancellation_stops_cleanly_persists_evidence_and_orphans_no_tasks(tmp_path):
    connect = make_connect(tmp_path)

    async def scenario():
        gate = asyncio.Event()
        adapter = FakeAdapter(
            "long", instances=("a", "b"), body=gated(before=2, gate=gate, after=5)
        )
        baseline = len(asyncio.all_tasks())
        sched = scheduler(connect, batch_size=1, flush_interval_seconds=0.0)
        handle = sched.start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if scalar(connect, "SELECT COUNT(*) FROM run_postings") >= 4:
                break
            await asyncio.sleep(0.01)
        handle.cancel()
        result = await handle.wait()
        await asyncio.sleep(0)
        leftovers = [
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done()
        ]
        return result, baseline, leftovers, adapter

    result, baseline, leftovers, adapter = run(scenario())

    assert result.status == "cancelled"
    assert leftovers == [], f"cancellation orphaned {leftovers}"
    assert sorted(adapter.closed) == ["long:a", "long:b"], "generators were not unwound"
    # Evidence: every attempt is settled, none left 'running'.
    statuses = {r["source"]: r["status"] for r in _rows(connect, "SELECT source, status FROM source_runs")}
    assert statuses == {"long:a": "cancelled", "long:b": "cancelled"}
    assert scalar(connect, "SELECT status FROM pipeline_runs") == "cancelled"
    # The records delivered before the cancel are durable.
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") >= 4


def test_a_cancel_before_the_run_task_starts_spawns_no_targets(tmp_path):
    """`start()` returns before the run task has run, so the target tasks the
    cancel would cancel do not exist yet. The decision has to be carried by the
    flag, or the whole plan executes and is then labelled cancelled."""
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("never", instances=("a", "b", "c"), body=fast(5))

    async def scenario():
        handle = scheduler(connect).start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        handle.cancel()
        return await handle.wait()

    result = run(scenario())

    assert adapter.attempts == {}, "a run cancelled before it started still fetched"
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 0
    assert scalar(connect, "SELECT COUNT(*) FROM source_runs") == 0
    assert result.status == "cancelled"
    assert scalar(connect, "SELECT status FROM pipeline_runs") == "cancelled"
    # Every planned target is still accounted for, so the report reconciles
    # against the plan it describes.
    assert len(result.targets) == 3
    assert {t.status for t in result.targets} == {"cancelled"}
    assert all(t.attempts == () for t in result.targets)
    report = _report(connect, result.run_uid)
    assert (report["targets"], report["cancelled"]) == (3, 3)


def test_targets_cancelled_while_queued_at_a_gate_still_appear_in_the_report(tmp_path):
    """A target waiting on a semaphore is cancelled before it has an attempt to
    report. Dropping it would leave the run report claiming fewer targets than the
    plan it was built from."""
    connect = make_connect(tmp_path)

    async def scenario():
        gate = asyncio.Event()
        adapter = FakeAdapter(
            "queued", instances=("a", "b", "c", "d"), body=gated(before=2, gate=gate, after=1)
        )
        sched = scheduler(
            connect, max_concurrent_targets=1, batch_size=1, flush_interval_seconds=0.0
        )
        handle = sched.start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if scalar(connect, "SELECT COUNT(*) FROM run_postings") >= 2:
                break
            await asyncio.sleep(0.01)
        handle.cancel()
        gate.set()
        return await handle.wait(), adapter

    result, adapter = run(scenario())

    assert result.status == "cancelled"
    assert len(result.targets) == 4, "planned targets vanished from the run report"
    assert {t.status for t in result.targets} == {"cancelled"}
    # The single slot was held by one target; the other three never ran.
    assert list(adapter.attempts) == ["a"]
    queued = [t for t in result.targets if t.source_run_key != "queued:a"]
    assert all(t.attempts == () for t in queued)
    assert all(
        t.skipped_reason == "cancelled before the first attempt started" for t in queued
    )
    assert result.target("queued:a").attempts, "the running target lost its attempt evidence"


def test_a_writer_failure_is_not_recorded_as_an_adapter_failure(tmp_path):
    """A dead writer is ours. Attributing it to the source would put a
    persistence outage into that board's health history permanently."""
    import json

    import backend.sources.writer as writer_module

    connect = make_connect(tmp_path)
    original_submit = writer_module.SqliteWriter.submit

    async def refusing_submit(self, op):
        # Only the record path fails, so the attempt's own settling still lands
        # and the persisted classification can be read back.
        if isinstance(op, writer_module.RecordBatch):
            raise writer_module.WriterError("sqlite writer failed")
        return await original_submit(self, op)

    writer_module.SqliteWriter.submit = refusing_submit
    try:
        adapter = FakeAdapter("blamed", instances=("board",), body=fast(4))
        result = run(
            scheduler(connect, batch_size=1, flush_interval_seconds=0.0).run(
                kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)
            )
        )
    finally:
        writer_module.SqliteWriter.submit = original_submit

    target = result.target("blamed:board")
    assert target.status == "failed"
    assert target.error["stage"] == "writer"
    assert "source_key" not in target.error, "a writer outage was blamed on the source"
    assert len(target.attempts) == 1, "a writer failure is not the adapter's to retry"
    stored = json.loads(_source_run(connect, "blamed:board")["error_json"])
    assert stored["stage"] == "writer"
    assert "source_key" not in stored
    assert "unclassified adapter exception" not in stored.get("note", "")


def test_subprocess_mode_cancellation_reaches_the_adapter(tmp_path):
    """A SUBPROCESS adapter kills its child in `fetch`'s finally. The scheduler's
    obligation is that its deadline cancel gets there."""
    connect = make_connect(tmp_path)
    child = {}
    from backend.sources.contract import ExecutionMode

    adapter = FakeAdapter(
        "jobspy-like",
        instances=("indeed",),
        body=subprocess_like(child),
        descriptor=descriptor_for(
            "jobspy-like",
            deadline=0.2,
            execution=ExecutionMode.SUBPROCESS,
            inventory_scope=InventoryScope.PARTIAL,
        ),
    )

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert child == {"started": True, "terminated": True}
    target = result.target("jobspy-like:indeed")
    assert target.status == "timeout"
    assert target.accepted == 2
    row = _source_run(connect, "jobspy-like:indeed")
    assert row["inventory_scope"] == str(InventoryScope.PARTIAL)


# --------------------------------------------------------------------------- #
# Transport ownership
# --------------------------------------------------------------------------- #
class RecordingTransport:
    """Counts and timestamps every request the scheduler's pacer lets through."""

    def __init__(self) -> None:
        self.started_at: list[float] = []
        self.inflight = 0
        self.peak = 0

    async def send(self, request):
        from backend.sources.contract import HttpResponse

        self.started_at.append(time.monotonic())
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        try:
            await asyncio.sleep(0.005)
            return HttpResponse(status=200, url=request.url, content=b"{}")
        finally:
            self.inflight -= 1


def _paced_adapter(body, **descriptor_kwargs):
    from backend.sources.contract import TransportKind

    return FakeAdapter(
        "paced",
        instances=("a", "b", "c", "d"),
        body=body,
        descriptor=descriptor_for(
            "paced", transport=TransportKind.HTTP, **descriptor_kwargs
        ),
    )


def test_all_targets_of_a_source_share_one_pacer_configured_from_the_descriptor(tmp_path):
    """A per-target pacer would pace nothing, so identity is the property to assert.

    Checked structurally rather than by timing: the scheduler's obligation is to
    wire one `PacedTransport` per source key over the run's shared transport, and
    to take its limits from the descriptor.
    """
    from backend.sources.contract import HttpRequest
    from backend.sources.transport import PacedTransport

    connect = make_connect(tmp_path)
    transport = RecordingTransport()
    handed: list[object] = []

    async def one_request(adapter, target, ctx):
        handed.append(ctx.http())
        await ctx.http().send(HttpRequest(url="https://paced.example/api/jobs"))
        yield target.record(
            title="Support Engineer",
            company="Acme",
            url=f"https://paced.example/{target.instance_key}",
            req_id=target.instance_key,
        )

    adapter = _paced_adapter(
        one_request, per_host_concurrency=4, min_request_interval_seconds=0.03
    )
    sched = Scheduler(
        connect,
        config=SchedulerConfig(max_concurrent_targets=4, **FAST_RETRY),
        transport=transport,
    )
    result = run(sched.run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert len(result.succeeded_targets) == 4
    assert len(handed) == 4
    assert all(isinstance(t, PacedTransport) for t in handed)
    assert len({id(t) for t in handed}) == 1, "each target got its own pacer"
    pacer = handed[0]
    assert pacer._inner is transport
    assert pacer._per_host == 4
    assert pacer._min_interval == 0.03


def test_a_single_slot_host_serialises_and_paces_its_requests(tmp_path):
    from backend.sources.contract import HttpRequest

    connect = make_connect(tmp_path)
    transport = RecordingTransport()

    async def one_request(adapter, target, ctx):
        await ctx.http().send(HttpRequest(url="https://paced.example/api/jobs"))
        yield target.record(
            title="Support Engineer",
            company="Acme",
            url=f"https://paced.example/{target.instance_key}",
            req_id=target.instance_key,
        )

    adapter = _paced_adapter(
        one_request, per_host_concurrency=1, min_request_interval_seconds=0.03
    )
    sched = Scheduler(
        connect,
        config=SchedulerConfig(max_concurrent_targets=4, **FAST_RETRY),
        transport=transport,
    )
    run(sched.run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert transport.peak == 1
    times = sorted(transport.started_at)
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert len(gaps) == 3
    assert all(gap >= 0.025 for gap in gaps), f"pacing not applied: {gaps}"


def test_an_adapter_declaring_no_transport_is_given_none(tmp_path):
    connect = make_connect(tmp_path)
    seen = {}

    async def inspect(adapter, target, ctx):
        seen["has_transport"] = ctx.has_transport
        seen["deadline_at"] = ctx.deadline_at
        yield target.record(title="T", company="C", url="https://x.example/1", req_id="1")

    adapter = FakeAdapter("notransport", instances=("a",), body=inspect)
    sched = Scheduler(connect, config=SchedulerConfig(**FAST_RETRY), transport=RecordingTransport())
    run(sched.run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert seen["has_transport"] is False
    assert seen["deadline_at"] is not None


# --------------------------------------------------------------------------- #
# Degenerate and failure paths
# --------------------------------------------------------------------------- #
def test_a_run_with_no_targets_still_records_itself(tmp_path):
    connect = make_connect(tmp_path)
    result = run(scheduler(connect).run(kind=RunKind.DAILY, plan=[]))

    assert result.status == "succeeded"
    assert result.targets == ()
    row = _rows(connect, "SELECT status, started_at, finished_at FROM pipeline_runs")[0]
    assert row["status"] == "succeeded"
    assert row["started_at"] and row["finished_at"]


def test_a_writer_failure_fails_the_run_instead_of_hanging_it(tmp_path):
    """A fatal commit error must surface as a failed run, not a deadlocked one:
    producers parked on a full queue have to be released."""
    connect = make_connect(tmp_path)
    import backend.sources.writer as writer_module

    original = writer_module.SqliteWriter._commit_once
    calls = {"n": 0}

    def exploding(self, batch):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("disk on fire")
        return original(self, batch)

    writer_module.SqliteWriter._commit_once = exploding
    try:
        adapters = [
            FakeAdapter(f"src{n}", instances=("a",), body=fast(80)) for n in range(3)
        ]
        result = run(
            scheduler(
                connect,
                batch_size=1,
                flush_interval_seconds=0.0,
                queue_size=2,
                max_ops_per_transaction=1,
                max_concurrent_targets=3,
            ).run(kind=RunKind.FULL_DIRECT, plan=plan_of(*adapters))
        )
    finally:
        writer_module.SqliteWriter._commit_once = original

    assert result.status == "failed"
    assert result.error["stage"] == "writer"
    assert result.error["message"] == "disk on fire"


# --------------------------------------------------------------------------- #
# Hashes
# --------------------------------------------------------------------------- #
def test_every_run_records_config_and_code_hashes(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("hashed", instances=("a",), body=fast(1))

    first = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert first.config_hash == second.config_hash
    assert first.code_hash == second.code_hash
    assert first.config_hash.startswith("sha256:")
    rows = _rows(connect, "SELECT config_hash, code_hash FROM pipeline_runs")
    assert {r["config_hash"] for r in rows} == {first.config_hash}
    assert {r["code_hash"] for r in rows} == {first.code_hash}

    changed = FakeAdapter(
        "hashed",
        instances=("a",),
        body=fast(1),
        descriptor=descriptor_for("hashed", deadline=99.0),
    )
    third = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(changed)))
    assert third.code_hash != first.code_hash, "a descriptor change must change the code hash"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _rows(connect, sql, params=()):
    conn = connect()
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _source_run(connect, source):
    rows = _rows(
        connect,
        "SELECT * FROM source_runs WHERE source=? ORDER BY attempt DESC",
        (source,),
    )
    assert rows, f"no source_runs row for {source}"
    return rows[0]


def _postings(connect):
    return [tuple(r) for r in _rows(connect, "SELECT posting_id FROM postings ORDER BY posting_id")]


def _report(connect, run_uid):
    import json

    blob = scalar(connect, "SELECT aggregate_report_json FROM pipeline_runs WHERE run_uid=?", (run_uid,))
    return json.loads(blob)
