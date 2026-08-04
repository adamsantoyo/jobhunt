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
import sqlite3
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from . import runstore
from .contract import NormalizedPosting

__all__ = [
    "CreateSourceRun",
    "EmitEvents",
    "FinishRun",
    "FinishSourceRun",
    "RecordBatch",
    "RunEvent",
    "SqliteWriter",
    "StartRun",
    "WriteOp",
    "WriterError",
    "WriterStats",
]


class WriterError(RuntimeError):
    """The writer task died. Every subsequent submit raises this."""


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
    #: Deferred submits refused because the writer had already failed. Non-zero
    #: means some evidence from a cancelled target did not land.
    dropped: int = 0


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

    async def aclose(self, *, drain: bool = True) -> None:
        """Flush everything already submitted, then stop and close the connection.

        `drain=True` waits for the deferred `submit_soon` puts first, so records a
        cancelled target managed to hand over still land. `drain=False` is the
        panic path: stop taking work and close.
        """
        if self._task is None:
            return
        if drain:
            await self.drain()
        try:
            await self._queue.put(_SENTINEL)
            await self._task
        finally:
            self._task = None
            conn, self._conn = self._conn, None
            if conn is not None:
                await asyncio.to_thread(conn.close)

    async def drain(self) -> None:
        """Block until everything submitted so far has committed.

        Two waits, in order: the deferred `submit_soon` puts must reach the queue,
        then the queue must empty. After this returns, every `RecordBatch`
        outcome_sink is complete, which is what lets the scheduler report exact
        per-target counts instead of counts that are short by the last batch.
        """
        if self._task is None:
            return
        if self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)
        await self._queue.join()

    # -- submission -------------------------------------------------------- #
    async def submit(self, op: WriteOp) -> None:
        """Enqueue, parking on a full queue. This is the backpressure point."""
        self._raise_if_failed()
        self.stats.submitted += 1
        await self._queue.put(op)
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
        if self._failure is not None:
            self.stats.dropped += 1
            return None
        self.stats.submitted += 1
        task = asyncio.create_task(self._queue.put(op))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return task

    def _raise_if_failed(self) -> None:
        if self._failure is not None:
            raise WriterError("sqlite writer failed") from self._failure

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    # -- the loop ---------------------------------------------------------- #
    async def _loop(self) -> None:
        assert self._conn is not None
        while True:
            first = await self._queue.get()
            if first is _SENTINEL:
                self._queue.task_done()
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
        """
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
