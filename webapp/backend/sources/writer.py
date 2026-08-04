"""The single SQLite writer queue.

One asyncio task owns one connection and is the only thing in the process that
writes canonical tables during a run. Everything else — sixteen adapters, dozens
of concurrent targets — hands it ordered operations through a bounded queue.

Why one writer, when SQLite in WAL mode tolerates concurrent readers:

  * WAL permits many readers but exactly one writer. N concurrent writers would
    serialise anyway, via `SQLITE_BUSY` retries, at the cost of unpredictable
    latency and a real risk of a partially-applied source under contention.
  * The queue is a total order, which is what makes foreign keys satisfiable
    without coordination: a target's `source_runs` row is enqueued before its
    first record batch, so `run_postings.source_run_id` and
    `run_events.source_run_id` always resolve.
  * Batching is only meaningful with one writer. Draining several queued
    operations into one transaction turns a per-record fsync into a per-batch
    fsync, which is the difference between a 60-second daily run and a 6-minute
    one.

Why the commit runs in a worker thread: `sqlite3` is blocking, and a commit that
blocks the event loop stalls every in-flight adapter *and* delays the
`asyncio.timeout` that enforces their deadlines. Handing each transaction to
`asyncio.to_thread` keeps deadline enforcement honest. Single ownership survives
because the writer task awaits each transaction before starting the next, so the
connection is never touched concurrently even though the thread may differ
(hence `check_same_thread=False`, which `db.connect` already sets).

Backpressure is a bounded `asyncio.Queue`. When the writer falls behind, a
producer's `await submit(...)` parks until there is room, which bounds memory to
`queue_size` batches instead of letting a fast adapter buffer a whole board in
RAM. A parked producer is still inside its own target deadline, so a pathological
writer stall costs that target its deadline and nothing else.

Durability before broadcast: `on_commit` fires only after the transaction that
contains those `run_events` has committed. Phase 4's SSE hook attaches here, so a
client can never be told about a state transition that a crash would erase.
"""
from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from . import runstore
from .contract import NormalizedPosting

__all__ = [
    "CreateSourceRun",
    "absorb_cancel",
    "EmitEvents",
    "FinishRun",
    "FinishSourceRun",
    "MarkPresence",
    "RecordBatch",
    "RecordUnattemptedSourceRun",
    "RunEvent",
    "SqliteWriter",
    "StartRun",
    "SummarizeChanges",
    "WriteOp",
    "WriterError",
    "WriterStats",
]


class WriterError(RuntimeError):
    """The writer task died. Every subsequent submit raises this."""


@contextlib.contextmanager
def absorb_cancel():
    """Swallow whatever the block raises, and un-count it if it was a cancellation.

    `contextlib.suppress(BaseException)` swallows the exception but not asyncio's
    BOOKKEEPING: a task that consumed a `CancelledError` keeps its `cancelling()`
    count elevated, and the next `asyncio.timeout` in that task — or an outer one
    that then sees a cancellation it did not raise — reports a bare `CancelledError`
    where its caller is catching `TimeoutError`. `uncancel()` is how a deliberate
    swallow is declared to asyncio.

    Used by every cleanup step that must finish even while its task is being
    cancelled from outside: the writer's own close, and the run's cleanup path in
    `scheduler._execute`. Non-cancellation exceptions are suppressed exactly as
    before, because a cleanup step that fails must not prevent the steps after it.

    `uncancel()` is keyed on CATCHING the `CancelledError`, not on the count moving
    while the block ran: the cancel is routinely REQUESTED before the block is
    entered and only DELIVERED inside it, and a before/after comparison sees no
    change in exactly that case. An `asyncio.timeout` inside the block does its own
    conversion and its own uncancel, and raises `TimeoutError` — a different type,
    which lands in the second clause and is not double-counted here.
    """
    try:
        yield
    except asyncio.CancelledError:
        task = asyncio.current_task()
        if task is not None:
            task.uncancel()
    except BaseException:  # noqa: BLE001 - deliberate: see the docstring
        pass


@dataclass(frozen=True, slots=True)
class RunEvent:
    """One `run_events` row, minus its sequence number.

    The sequence is assigned by the writer inside the transaction that persists
    the event, so UNIQUE(run_uid, sequence) holds by construction: there is only
    one allocator and it is the same task that commits.
    """

    run_uid: str
    event_type: str
    source_run_id: str | None = None
    payload: Mapping[str, object] | None = None
    at: str | None = None


class WriteOp(Protocol):
    """One unit of work applied inside the writer's transaction.

    `apply` must be re-runnable: a busy-database rollback replays the whole batch,
    so an op that mutated Python state on its first pass would double-count. This
    is why `RecordBatch` returns its outcome instead of appending to its sink.
    """

    events: tuple[RunEvent, ...]

    def apply(self, conn: sqlite3.Connection) -> object | None: ...


@dataclass(frozen=True, slots=True)
class StartRun:
    run_uid: str
    kind: str
    trigger: str | None
    requested_at: str
    started_at: str
    config_hash: str | None = None
    code_hash: str | None = None
    #: True when reopening an interrupted run rather than creating a new one.
    resume: bool = False
    events: tuple[RunEvent, ...] = ()

    def apply(self, conn: sqlite3.Connection) -> None:
        if self.resume:
            if not runstore.reopen_pipeline_run(
                conn, run_uid=self.run_uid, started_at=self.started_at
            ):
                raise WriterError(
                    f"run {self.run_uid!r} is not resumable (not in status 'interrupted')"
                )
            return
        runstore.create_pipeline_run(
            conn,
            run_uid=self.run_uid,
            kind=self.kind,
            trigger=self.trigger,
            requested_at=self.requested_at,
            started_at=self.started_at,
            config_hash=self.config_hash,
            code_hash=self.code_hash,
        )


@dataclass(frozen=True, slots=True)
class FinishRun:
    run_uid: str
    status: str
    finished_at: str
    kept_count: int | None = None
    new_count: int | None = None
    report: object = None
    error: object = None
    events: tuple[RunEvent, ...] = ()

    def apply(self, conn: sqlite3.Connection) -> None:
        runstore.finish_pipeline_run(
            conn,
            run_uid=self.run_uid,
            status=self.status,
            finished_at=self.finished_at,
            kept_count=self.kept_count,
            new_count=self.new_count,
            report=self.report,
            error=self.error,
        )


@dataclass(frozen=True, slots=True)
class CreateSourceRun:
    source_run_id: str
    run_uid: str
    source: str
    attempt: int
    requested_at: str
    started_at: str
    deadline_at: str | None = None
    inventory_scope: str | None = None
    metadata: object = None
    events: tuple[RunEvent, ...] = ()

    def apply(self, conn: sqlite3.Connection) -> None:
        runstore.create_source_run(
            conn,
            source_run_id=self.source_run_id,
            run_uid=self.run_uid,
            source=self.source,
            attempt=self.attempt,
            requested_at=self.requested_at,
            started_at=self.started_at,
            deadline_at=self.deadline_at,
            inventory_scope=self.inventory_scope,
            metadata=self.metadata,
        )


@dataclass(slots=True)
class RecordUnattemptedSourceRun:
    """One terminal `source_runs` row for a target that never attempted a fetch.

    Separate from `CreateSourceRun` because it is the opposite kind of row: created
    already settled, with no timing, under `UNATTEMPTED_SOURCE_RUN_STEP` so that it
    consumes no fetch attempt number (see `runstore.record_unattempted_source_run`).

    Not frozen, for the same reason `MarkPresence` is not: the insert is an
    `INSERT OR IGNORE`, and when it is ignored the row this op's events reference
    belongs to an earlier op, so the events are ASSIGNED away — assignment, never
    append, so a busy-database rollback that replays the batch cannot double them.
    Emitting them anyway would point `run_events.source_run_id` at an id no row
    carries, which the foreign key would refuse and the whole transaction would die
    on.
    """

    source_run_id: str
    run_uid: str
    source: str
    status: str
    requested_at: str | None = None
    finished_at: str | None = None
    inventory_scope: str | None = None
    error: object = None
    metadata: object = None
    events: tuple[RunEvent, ...] = ()

    def apply(self, conn: sqlite3.Connection) -> bool:
        inserted = runstore.record_unattempted_source_run(
            conn,
            source_run_id=self.source_run_id,
            run_uid=self.run_uid,
            source=self.source,
            status=self.status,
            requested_at=self.requested_at,
            finished_at=self.finished_at,
            inventory_scope=self.inventory_scope,
            error=self.error,
            metadata=self.metadata,
        )
        if not inserted:
            self.events = ()
        return inserted


@dataclass(frozen=True, slots=True)
class RecordBatch:
    """A flushed slice of one target's stream, plus the progress it implies.

    Records, counts, and the checkpoint that describes them commit together. That
    is stronger than the contract requires (a checkpoint only promises delivery,
    not commit) and it is what makes replay after a crash cheap rather than merely
    safe.
    """

    run_uid: str
    source_run_id: str
    records: tuple[NormalizedPosting, ...]
    recorded_at: str
    fetched_count: int
    checkpoint_json: str | None = None
    events: tuple[RunEvent, ...] = ()
    #: The owning target's running list of committed outcomes. The writer appends
    #: to it only after the transaction commits, so a rolled-back-and-retried batch
    #: cannot double-count. The scheduler reads it for live counts without a query.
    outcome_sink: list[runstore.RecordOutcome] | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> runstore.RecordOutcome:
        outcome = runstore.write_records(
            conn,
            run_uid=self.run_uid,
            source_run_id=self.source_run_id,
            records=self.records,
            recorded_at=self.recorded_at,
        )
        runstore.update_source_run_progress(
            conn,
            source_run_id=self.source_run_id,
            fetched_count=self.fetched_count,
            accepted_delta=outcome.accepted,
            # Phase 3.1: how many of this attempt's records moved their posting's
            # current source version, accumulated in the same transaction that
            # moved them. `source_runs.changed_count` has been NULL since
            # migration 6 waiting for exactly this.
            changed_delta=outcome.changed,
            checkpoint_json=self.checkpoint_json,
        )
        return outcome


@dataclass(frozen=True, slots=True)
class FinishSourceRun:
    source_run_id: str
    status: str
    finished_at: str
    fetched_count: int | None = None
    accepted_count: int | None = None
    checkpoint_json: str | None = None
    error: object = None
    metadata: object = None
    events: tuple[RunEvent, ...] = ()

    def apply(self, conn: sqlite3.Connection) -> None:
        runstore.finish_source_run(
            conn,
            source_run_id=self.source_run_id,
            status=self.status,
            finished_at=self.finished_at,
            fetched_count=self.fetched_count,
            accepted_count=self.accepted_count,
            checkpoint_json=self.checkpoint_json,
            error=self.error,
            metadata=self.metadata,
        )


@dataclass(slots=True)
class MarkPresence:
    """Phase 2.4's presence pass for one settled run, as a single transaction.

    One op rather than one per source, because the pass is a whole-run judgement:
    every licence has to be read from the same committed snapshot of `source_runs`,
    and the refresh that returns re-delivered postings to present has to land with
    the markings rather than beside them.

    Not frozen, and that is the point. The counts only exist once the SQL has run, so
    `apply` publishes them by ASSIGNING `self.events` and `self.report` — assignment,
    never append, so a busy-database rollback that replays the whole batch overwrites
    them instead of doubling them (`WriteOp.apply` must be re-runnable).

    The run-level report is what `FinishRun` carries into `aggregate_report_json`; the
    per-source events are what Phase 4 replays to show which board dropped what.
    """

    run_uid: str
    at: str
    events: tuple[RunEvent, ...] = ()
    #: The pass's report, or None when it has not run yet.
    report: dict | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> dict:
        report = runstore.apply_run_presence(conn, run_uid=self.run_uid, at=self.at)
        events = [
            RunEvent(
                run_uid=self.run_uid,
                event_type="run.presence_refreshed",
                at=self.at,
                payload={
                    "seen": report["seen"],
                    "returned": report["returned"],
                    "licensed_sources": report["licensed_sources"],
                },
            )
        ]
        for scope in report["sources"]:
            events.append(
                RunEvent(
                    run_uid=self.run_uid,
                    source_run_id=str(scope["source_run_id"]),
                    event_type="source.absence_marked",
                    at=self.at,
                    payload=dict(scope),
                )
            )
        self.events = tuple(events)
        self.report = report
        return report


@dataclass(slots=True)
class SummarizeChanges:
    """Phase 3.1's change accounting for one run, read inside the writer transaction.

    A read, not a write — but it has to be a writer op all the same, because the
    writer owns the only connection during a run and the counts have to be taken from
    the committed snapshot that every batch has landed in.

    Not frozen, for `MarkPresence`'s reason: the report only exists once the SQL has
    run, so `apply` publishes it by ASSIGNING `self.events`/`self.report`, never by
    appending, so a busy-database rollback that replays the batch overwrites rather
    than doubles them.

    The dirty IDS are deliberately NOT carried here: a run can dirty tens of thousands
    of postings, and the run report is not a work queue. Phase 3.2/3.3 ask
    `runstore.dirty_posting_ids(conn, run_uid)` for the list, which is recomputed from
    committed rows and therefore survives a restart between this run and their work.
    """

    run_uid: str
    at: str
    events: tuple[RunEvent, ...] = ()
    #: The run's change counts, or None when it has not run yet.
    report: dict | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> dict:
        report = runstore.change_summary(conn, self.run_uid)
        self.events = (
            RunEvent(
                run_uid=self.run_uid,
                event_type="run.changes_summarized",
                at=self.at,
                payload=dict(report),
            ),
        )
        self.report = report
        return report


@dataclass(frozen=True, slots=True)
class EmitEvents:
    """Events with no state change of their own (plan built, checkpoint discarded)."""

    events: tuple[RunEvent, ...] = ()

    def apply(self, conn: sqlite3.Connection) -> None:
        return None


@dataclass
class WriterStats:
    submitted: int = 0
    committed_ops: int = 0
    transactions: int = 0
    events: int = 0
    records: int = 0
    max_queue_depth: int = 0
    max_ops_per_transaction: int = 0
    busy_retries: int = 0
    #: Deferred submits refused because the writer had already failed, plus anything
    #: abandoned in the queue of a writer task that died. Non-zero means some evidence
    #: from a cancelled target did not land.
    dropped: int = 0
    #: Times `drain()` gave up before the queue emptied. Non-zero means the reported
    #: per-target counts may be short of what actually committed.
    drain_timeouts: int = 0
    #: Connections `aclose` declined to close because a commit was still running on a
    #: worker thread when its bound expired. Non-zero means one sqlite handle was
    #: deliberately leaked in preference to closing it under a live statement.
    unclosed_connections: int = 0


_SENTINEL = object()


class SqliteWriter:
    """The one writer. Construct, `await start()`, submit, `await aclose()`."""

    def __init__(
        self,
        connect: Callable[[], sqlite3.Connection],
        *,
        queue_size: int = 64,
        max_ops_per_transaction: int = 32,
        busy_timeout_ms: int = 10_000,
        on_commit: Callable[[Sequence[RunEvent]], None] | None = None,
        close_lock_timeout_seconds: float = 5.0,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        self._connect = connect
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._max_ops = max(1, max_ops_per_transaction)
        self._busy_timeout_ms = busy_timeout_ms
        #: Broadcast hook. Called AFTER the transaction commits, never before —
        #: "persist state transitions and progress before broadcasting".
        self._on_commit = on_commit
        self._conn: sqlite3.Connection | None = None
        self._task: asyncio.Task | None = None
        self._pending: set[asyncio.Task] = set()
        self._sequences: dict[str, int] = {}
        self._failure: BaseException | None = None
        #: True once the writer task has stopped on its own terms — the sentinel, or
        #: a recorded commit failure. It is what tells `_reap` that a finished task
        #: is a clean stop rather than a death nobody noticed.
        self._stopped = False
        #: Held by whichever thread is touching the connection: the worker thread for
        #: the duration of a transaction, the event-loop thread while closing. It is
        #: the handshake that makes `aclose` safe — `task.cancel()` does not stop a
        #: `to_thread` worker, and `check_same_thread=False` means sqlite3 will not
        #: stop the two of them from using one connection at once.
        self._commit_lock = threading.Lock()
        self._close_lock_timeout = max(0.0, close_lock_timeout_seconds)
        self.stats = WriterStats()

    # -- lifecycle --------------------------------------------------------- #
    async def start(self) -> None:
        if self._task is not None:
            return
        self._conn = await asyncio.to_thread(self._open)
        self._task = asyncio.create_task(self._loop(), name="sqlite-writer")

    def _open(self) -> sqlite3.Connection:
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        # A reader holding the DB (the API serving a request) must not turn into a
        # write failure; WAL plus a busy timeout makes contention a wait, not an
        # error. Verified by the contention test.
        conn.execute(f"PRAGMA busy_timeout={int(self._busy_timeout_ms)}")
        conn.execute("PRAGMA foreign_keys=ON")
        runstore.require_canonical_schema(conn)
        return conn

    async def aclose(self, *, drain: bool = True, timeout: float | None = None) -> None:
        """Flush everything already submitted, then stop and close the connection.

        `drain=True` waits for the deferred `submit_soon` puts first, so records a
        cancelled target managed to hand over still land. `drain=False` is the
        panic path: stop taking work and close.

        Every step is bounded by `timeout` and the release happens in a `finally`
        that contains no `await`, because this is the ONLY place the writer task and
        its sqlite connection are given up. The caller is a run's cleanup path that
        may itself be under cancellation, and a close that got skipped — or that was
        interrupted halfway — would leak a task and a file handle for the life of the
        process. `timeout=None` keeps the original unbounded waits, for a caller that
        owns the writer directly and has no run to protect.
        """
        task = self._task
        if task is None:
            return
        try:
            if drain:
                await self.drain(timeout=timeout)
            # put_nowait, not an awaited put: a queue that is still full here is one
            # nothing is consuming, and parking on it would be the unbounded wait
            # this method exists to rule out. The cancel below stops that writer.
            with contextlib.suppress(asyncio.QueueFull):
                self._queue.put_nowait(_SENTINEL)
            with absorb_cancel():
                # asyncio.wait rather than `await task`: it neither cancels the task
                # on timeout nor re-raises its exception, which `_reap` records.
                await asyncio.wait({task}, timeout=timeout)
        finally:
            self._reap()
            self._task = None
            if not task.done():
                # Synchronous, so the task cannot outlive this call even if we are
                # cancelled on this very line.
                task.cancel()
            conn, self._conn = self._conn, None
            if conn is not None:
                self._close_connection(conn)

    def _close_connection(self, conn: sqlite3.Connection) -> None:
        """Release the connection without racing a commit still on a worker thread.

        Closed inline rather than on a worker thread because an `await` in `aclose`'s
        `finally` can be interrupted by the cancellation that path exists to survive,
        and the connection would then never be released at all.

        Inline is only safe with the commit lock, though: `task.cancel()` above does
        NOT stop a `to_thread` worker, so a transaction can still be executing on this
        exact connection, and CPython's sqlite3 holds no per-connection lock to make
        that merely an error rather than a data race. The acquire is blocking and
        bounded — bounded because a stuck commit must not wedge the event loop in a
        `finally`, blocking because there is nothing here that may await. In the
        normal path the writer task has already finished and the lock is free, so this
        costs nothing.

        If the bound expires the connection is deliberately LEAKED rather than closed
        under a live statement: one file handle in a process that is already in
        trouble is a strictly better outcome than a use-after-free in a C extension,
        and `stats.unclosed_connections` says it happened.
        """
        if not self._commit_lock.acquire(timeout=self._close_lock_timeout):
            self.stats.unclosed_connections += 1
            return
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - teardown; a close failure is not actionable
            pass
        finally:
            self._commit_lock.release()

    async def drain(self, *, timeout: float | None = None) -> bool:
        """Block until everything submitted so far has committed. True if it did.

        Two waits, in order: the deferred `submit_soon` puts must reach the queue,
        then the queue must empty. After this returns True, every `RecordBatch`
        outcome_sink is complete, which is what lets the scheduler report exact
        per-target counts instead of counts that are short by the last batch.

        Bounded, and that bound is load-bearing. `_loop` records a failure for a
        commit that raised, which is the only failure it can see; a writer task that
        died some other way — cancelled with the loop parked on `queue.get`, killed
        by an error in the loop itself — leaves a queue nobody will ever consume, and
        an unbounded `join()` on it never returns. `_reap` converts that death into a
        recorded failure and abandons the queue; `timeout` is the backstop for
        everything else, including a commit that is merely pathologically slow.
        """
        if self._task is None:
            return True
        try:
            async with asyncio.timeout(timeout):
                self._reap()
                if self._pending:
                    await asyncio.gather(*tuple(self._pending), return_exceptions=True)
                # Again after the deferred puts land: they may have been parked on a
                # queue whose consumer had already died, and they are then part of
                # what has to be abandoned rather than waited for.
                self._reap()
                if self._task.done():
                    return False
                await self._queue.join()
        except TimeoutError:
            self.stats.drain_timeouts += 1
            return False
        return True

    def _reap(self) -> None:
        """Adopt a writer task that died without recording a failure, and give up on
        whatever is still queued for it.

        Every producer's contract is "submit raises once the writer is dead". A task
        that died outside `_commit` never set `_failure`, so without this the run
        would keep submitting into a queue nobody reads and then wait forever for it
        to empty. Abandoning the queue is what releases producers already parked in
        `put` and any `join()` waiting on it.
        """
        task = self._task
        if task is None or not task.done():
            return
        if self._failure is None and not self._stopped:
            if task.cancelled():
                self._failure = WriterError("sqlite writer task was cancelled")
            else:
                exc = task.exception()
                self._failure = exc if exc is not None else WriterError(
                    "sqlite writer task exited before the writer was closed"
                )
        if self._failure is not None:
            self._abandon()

    def _abandon(self) -> None:
        """Discard everything queued for a writer that will never consume it again.

        Counted as dropped, so "the writer died and N operations were lost" stays
        visible rather than inferred — the same accounting `_drain_and_drop` keeps
        for the failure the writer task noticed itself.
        """
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._queue.task_done()
            if item is not _SENTINEL:
                self.stats.dropped += 1

    # -- submission -------------------------------------------------------- #
    async def submit(self, op: WriteOp) -> None:
        """Enqueue, parking on a full queue. This is the backpressure point.

        Checked for a dead writer on BOTH sides of the put, because the put is an
        await and the writer can die while a producer is parked on it. Waking that
        producer is exactly what abandoning the queue does, and it wakes it into a
        free slot: `_abandon`'s loop runs to completion without yielding, finds the
        queue empty, and stops; only then does the woken putter resume and hand over
        an operation nothing will ever consume. Without the second check the caller
        is told the handoff succeeded. The check re-runs `_reap`, so the operation
        just enqueued is dropped and counted before the raise rather than sitting in
        the queue until some later caller notices it.
        """
        self._raise_if_failed()
        self.stats.submitted += 1
        await self._queue.put(op)
        self._raise_if_failed()
        self.stats.max_queue_depth = max(self.stats.max_queue_depth, self._queue.qsize())

    def submit_soon(self, op: WriteOp) -> asyncio.Task | None:
        """Enqueue from a context that cannot await — a cancelled target's `finally`.

        The put runs in its own task, so it survives the caller's cancellation;
        `aclose(drain=True)` waits for it. Ordering with respect to other submits is
        preserved because `asyncio.Queue` wakes blocked putters FIFO and tasks start
        in creation order.

        Deliberately does not raise on a dead writer: every caller is a `finally`
        block already handling a failure, and raising there would replace a
        recorded failure with an unrelated one. The loss is counted in `stats.dropped`.
        """
        self._reap()
        if self._failure is not None or self._stopped:
            self.stats.dropped += 1
            return None
        self.stats.submitted += 1
        task = asyncio.create_task(self._deferred_put(op))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return task

    async def _deferred_put(self, op: WriteOp) -> None:
        """A `submit_soon` put, plus the same post-put death check `submit` makes.

        A deferred put parks on a full queue exactly as an awaited one does, and is
        woken the same way by a queue being abandoned. `submit_soon` promises not to
        raise — its callers are `finally` blocks already handling a failure — so the
        loss is counted here instead, at the moment it happens rather than whenever
        the next `_reap` happens to find the operation still sitting in the queue.
        """
        await self._queue.put(op)
        self._reap()

    def _raise_if_failed(self) -> None:
        # A writer task that died outside `_commit` is a failure too; noticing it
        # here is what stops a producer from filling a queue nobody will read.
        self._reap()
        if self._failure is not None:
            raise WriterError("sqlite writer failed") from self._failure
        if self._stopped:
            # A cleanly stopped writer is as unable to commit as a dead one. Without
            # this, a late submit lands in a queue nobody will ever read again and
            # reports success — the same silent loss the post-put check exists to
            # prevent, arrived at by a different route.
            raise WriterError("sqlite writer is closed")

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    @property
    def closed(self) -> bool:
        """True once the writer task and its connection have been released.

        The run's cleanup path reads this to decide whether an interrupted `aclose`
        has to be retried; a writer that was never started reads as closed, which is
        the same statement about what it owns.
        """
        return self._task is None and self._conn is None

    # -- the loop ---------------------------------------------------------- #
    async def _loop(self) -> None:
        assert self._conn is not None
        while True:
            first = await self._queue.get()
            if first is _SENTINEL:
                self._queue.task_done()
                self._stopped = True
                return
            batch: list[WriteOp] = [first]
            while len(batch) < self._max_ops:
                try:
                    nxt = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if nxt is _SENTINEL:
                    # Put it back so the next loop iteration terminates cleanly
                    # after this batch commits. The put_nowait cannot fail: the
                    # get_nowait above just freed a slot.
                    self._queue.put_nowait(_SENTINEL)
                    self._queue.task_done()
                    break
                batch.append(nxt)

            self.stats.max_ops_per_transaction = max(
                self.stats.max_ops_per_transaction, len(batch)
            )
            try:
                events = await asyncio.to_thread(self._commit, batch)
            except BaseException as exc:  # noqa: BLE001 - recorded, surfaced to submitters
                self._failure = exc
                for _ in batch:
                    self._queue.task_done()
                await self._drain_and_drop()
                self._stopped = True
                return
            for _ in batch:
                self._queue.task_done()
            self.stats.transactions += 1
            self.stats.committed_ops += len(batch)
            if events and self._on_commit is not None:
                # After the commit, never before.
                self._on_commit(events)

    async def _drain_and_drop(self) -> None:
        """Keep consuming after a fatal commit failure, discarding everything.

        Without this, a producer already parked in `queue.put` would wait forever
        on a queue nobody reads — a hang, in the one situation where the caller
        most needs to see the error. Discards are counted, so "the writer died and
        N operations were lost" is visible rather than inferred.
        """
        while True:
            item = await self._queue.get()
            self._queue.task_done()
            if item is _SENTINEL:
                return
            self.stats.dropped += 1

    def _commit(self, batch: Sequence[WriteOp]) -> list[RunEvent]:
        """Apply a batch in one transaction. Retries once on a busy database.

        A rollback restores the event-sequence counters, so a retried batch reuses
        the same sequence numbers rather than leaving a hole (which an SSE client
        replaying `run_events` would read as a lost event).

        Runs on a worker thread, and holds `_commit_lock` for as long as it is
        touching the connection: `aclose` may be trying to close that connection on
        the event-loop thread, and cancelling the writer task does not stop this
        function. See `_close_connection`.
        """
        with self._commit_lock:
            return self._commit_locked(batch)

    def _commit_locked(self, batch: Sequence[WriteOp]) -> list[RunEvent]:
        attempts = 2
        last: sqlite3.OperationalError | None = None
        for attempt in range(attempts):
            snapshot = dict(self._sequences)
            try:
                return self._commit_once(batch)
            except sqlite3.OperationalError as exc:
                self._sequences = snapshot
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                last = exc
                self.stats.busy_retries += 1
                if attempt + 1 < attempts:
                    time.sleep(0.05)
            except BaseException:
                self._sequences = snapshot
                raise
        raise last  # pragma: no cover - defensive; loop above always sets `last`

    def _commit_once(self, batch: Sequence[WriteOp]) -> list[RunEvent]:
        conn = self._conn
        assert conn is not None
        conn.execute("BEGIN IMMEDIATE")
        try:
            emitted: list[RunEvent] = []
            rows: list[dict[str, object]] = []
            outcomes: list[tuple[RecordBatch, runstore.RecordOutcome]] = []
            records = 0
            for op in batch:
                result = op.apply(conn)
                if isinstance(op, RecordBatch):
                    records += len(op.records)
                    if op.outcome_sink is not None and isinstance(result, runstore.RecordOutcome):
                        outcomes.append((op, result))
                for event in getattr(op, "events", ()):
                    sequence = self._sequences.get(event.run_uid)
                    if sequence is None:
                        sequence = runstore.next_event_sequence(conn, event.run_uid)
                    self._sequences[event.run_uid] = sequence + 1
                    rows.append(
                        {
                            "run_uid": event.run_uid,
                            "source_run_id": event.source_run_id,
                            "sequence": sequence,
                            "event_type": event.event_type,
                            "at": event.at or runstore.utc_now_iso(),
                            "payload": event.payload,
                        }
                    )
                    emitted.append(event)
            runstore.append_run_events(conn, rows)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        # Python-side effects happen only after the transaction is durable, so a
        # busy-retry that replays this batch cannot double-count anything.
        for op, outcome in outcomes:
            op.outcome_sink.append(outcome)
        self.stats.records += records
        self.stats.events += len(emitted)
        return emitted


def collecting_hook(sink: list[RunEvent]) -> Callable[[Sequence[RunEvent]], None]:
    """A broadcast hook that just records. Phase 4 replaces it with SSE fan-out."""

    def _hook(events: Iterable[RunEvent]) -> None:
        sink.extend(events)

    return _hook
