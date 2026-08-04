"""Bounded-concurrency scheduler for the canonical source engine (Phase 2.3).

The unit of scheduling is a `SourceTarget`, not a source: Greenhouse is one
adapter with ~32 boards, and each board gets its own deadline, its own attempt
rows, and its own failure domain. That is what makes "one failed source adds no
more than its own deadline" and Phase 2.4's absence scoping both true.

WHAT THIS OWNS (the adapter side of the table in `contract.py` owns the rest):

  concurrency   three nested gates, always acquired in the same order so they
                cannot deadlock: global -> per-source -> per-host. Per-host caps
                are computed from the whole plan up front, so two sources sharing
                an API host share one ceiling instead of each getting its own.
  pacing        one `PacedTransport` per host, shared by every target behind that
                host, which is the only arrangement in which
                `min_request_interval_seconds` means anything (a pacer with a
                single client paces nothing, and two pacers on one host pace half
                of it each).
  deadlines     one `asyncio.timeout` per attempt. A hanging adapter is cancelled
                at its deadline and the records it already delivered are kept.
  retries       at most two attempts, transient classification only, jittered.
  persistence   everything goes through the single writer queue, so records land
                incrementally and state transitions are durable before any
                broadcast.
  recovery      attempts left 'running' by a dead process are marked interrupted,
                and an interrupted run can be resumed as an explicit decision.
  presence      once every target has settled and committed, one whole-run pass
                (`MarkPresence` -> `runstore.apply_run_presence`) refreshes what was
                seen and marks absent what a successful COMPLETE enumeration proved
                gone. It runs before the run's own terminal row so its result is part
                of that run's evidence, and it is skipped entirely on a cancelled or
                failed run.

FIVE DECISIONS WORTH RATIFYING, because for each one the obvious implementation
is subtly wrong:

1. A DEADLINE IS NOT RETRIED. `TransientSourceError` buys one retry; a timeout
   does not. Retrying a timeout would make a hanging source cost `2 x deadline`,
   contradicting the Success Contract line it is meant to satisfy. A source that
   could not answer inside its budget will not answer inside a second one.
2. A TARGET HAS A HARD WALL-CLOCK BUDGET of `attempt_budget_multiplier x
   deadline` (default 2x) covering both attempts and the backoff between them.
   Without it, a transient failure raised one millisecond before the deadline
   would licence a second full deadline. The retry is skipped when the budget
   cannot fund it, and the skip is recorded as a run event.
3. CHECKPOINTS DO NOT CROSS RUNS. A stored cursor is offered back only when the
   same run is explicitly resumed. Resuming a COMPLETE-scope target mid-inventory
   in a *new* run would let it report success without enumerating the board, and
   Phase 2.4 would then mark every posting it skipped absent.
4. AN UNCLASSIFIED ADAPTER EXCEPTION IS PERMANENT. Only `TransientSourceError`
   opts into a retry. A `KeyError` from a parser is a bug, and retrying a bug
   spends the run's budget to reproduce it. A `WriterError` is not an adapter
   exception at all: it is recorded with `stage: writer` and carries no source
   key, because a persistence outage must not enter a source's health history.
5. CANCELLATION IS ABSORBED, NOT PROPAGATED, at the target boundary. A cancelled
   target catches its `CancelledError`, hands its evidence to the writer through
   the deferred path, and returns a `cancelled` result. Re-raising would abandon
   the attempt row in 'running' — exactly the orphan state restart recovery
   exists to clean up, manufactured on a path where we know the truth. A target
   cancelled before it ever ran — queued at a gate, or never spawned because the
   cancel arrived before the run's first tick — is reported as a cancelled target
   with no attempts, so the report still balances against the plan it describes.
   It DOES get a `source_runs` row (2.6), created already terminal, with no timing
   because nothing started and under `runstore.UNATTEMPTED_SOURCE_RUN_STEP` because
   it is not a fetch attempt: every attempt-numbering, absence-licensing and
   freshness query is bounded to the `fetch` step, so the row consumes no attempt
   number a resume would need. Without it, `source_runs` cannot distinguish a board
   that was never asked from a board that was never planned.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import random
import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone

from . import registry, run_profiles, runstore
from .contract import (
    CANONICAL_HASH_FIELDS,
    CHECKPOINT_VERSION,
    Checkpoint,
    Disposition,
    FetchContext,
    InboundPayload,
    InventoryScope,
    NormalizedPosting,
    RunKind,
    SourceAdapter,
    SourceConfig,
    SourceDescriptor,
    SourceError,
    SourceTarget,
    Transport,
    TransportKind,
)
from .runstore import (
    RecoveryReport,
    resumable_runs,
    resume_plan,
    source_instance_freshness,
    successful_source_scopes,
)
from .transport import PacedTransport
from .writer import (
    CreateSourceRun,
    EmitEvents,
    FinishRun,
    FinishSourceRun,
    MarkPresence,
    RecordBatch,
    RecordUnattemptedSourceRun,
    RunEvent,
    SqliteWriter,
    StartRun,
    SummarizeChanges,
    WriterError,
    absorb_cancel,
)

__all__ = [
    "AttemptOutcome",
    "RunHandle",
    "RunResult",
    "Scheduler",
    "SchedulerConfig",
    "TargetResult",
    "code_hash_for",
    "config_hash_for",
    "recover_orphans",
    "resumable_runs",
    "resume_plan",
    "source_instance_freshness",
    "successful_source_scopes",
]

#: Run outcomes that licence the Phase 2.4 presence pass. A cancelled run is excluded
#: by decision: cancellation stops targets mid-enumeration, and a COMPLETE-scope
#: target that settled before the cancel arrived would otherwise mark its instance's
#: unseen postings absent on the strength of a run whose other evidence is missing.
#: A failed run is excluded because the failure is either the writer (no evidence
#: landed) or the scheduler itself (the report cannot be trusted to describe the run).
#: `partial` IS included: per-source failure isolation is the whole design, and a
#: healthy board must still be able to retire its own closed requisitions on a day
#: another board 404s.
PRESENCE_PASS_RUN_STATUSES = frozenset({"succeeded", "partial"})


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    """Every knob, defaulted for the local single-user machine."""

    #: Targets in flight across the whole run.
    max_concurrent_targets: int = 8
    #: Records buffered per target before a flush. Small enough that a 60-second
    #: daily run shows results early; large enough that a 32K-row source does not
    #: pay 32K transactions.
    batch_size: int = 50
    #: Flush a partial buffer after this long, so a slow trickling source still
    #: becomes visible before it finishes.
    flush_interval_seconds: float = 0.5
    #: Bounded writer queue = bounded memory under backpressure.
    queue_size: int = 64
    max_ops_per_transaction: int = 32
    #: Base retry delay; the wait is `base * uniform(1 - jitter, 1 + jitter)`.
    #: Jitter matters because ~32 Greenhouse boards behind one host fail together
    #: when that host rate-limits, and un-jittered retries would re-stampede it.
    retry_base_delay_seconds: float = 0.25
    retry_jitter: float = 0.5
    max_attempts: int = 2
    #: Hard per-target wall-clock budget as a multiple of the target's deadline.
    attempt_budget_multiplier: float = 2.0
    #: How long a cancelled adapter's generator gets to unwind. This is the
    #: cancellation-propagation budget for SUBPROCESS adapters: their `finally` is
    #: where the child process is killed.
    stream_close_grace_seconds: float = 5.0
    #: Hard bound on every wait in the run's cleanup path (drain, presence pass,
    #: writer close). Generous, because exceeding it costs exact counts and possibly
    #: a committed presence pass: it is there to turn a writer that will never make
    #: progress into a finished run rather than a hung one, not to police a slow
    #: commit. Only a writer task that died in a way `_commit` could not see, or a
    #: transaction that has been stuck for half a minute, can reach it.
    writer_drain_timeout_seconds: float = 30.0
    recover_orphans_on_start: bool = True

    def __post_init__(self) -> None:
        if self.max_concurrent_targets < 1:
            raise ValueError("max_concurrent_targets must be >= 1")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.attempt_budget_multiplier < 1.0:
            raise ValueError("attempt_budget_multiplier must be >= 1.0")
        if not 0.0 <= self.retry_jitter < 1.0:
            raise ValueError("retry_jitter must be in [0, 1)")
        if self.writer_drain_timeout_seconds <= 0:
            raise ValueError("writer_drain_timeout_seconds must be > 0")


# --------------------------------------------------------------------------- #
# Results
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class AttemptOutcome:
    """One row of `source_runs`, as the scheduler saw it.

    Deliberately carries no accepted count: an attempt is settled the instant its
    stream ends, which can precede the commit of its last batch. The authoritative
    per-attempt count is `source_runs.accepted_count`, accumulated by the writer.
    """

    attempt: int
    source_run_id: str
    status: str
    disposition: Disposition
    fetched: int
    duration_seconds: float
    error: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class TargetResult:
    source_run_key: str
    source_key: str
    instance_key: str
    label: str
    inventory_scope: InventoryScope
    status: str
    attempts: tuple[AttemptOutcome, ...] = ()
    fetched: int = 0
    accepted: int = 0
    created: int = 0
    duplicates: int = 0
    conflicts: int = 0
    duration_seconds: float = 0.0
    error: Mapping[str, object] | None = None
    #: Set when a resume skipped this target because it already succeeded, or
    #: DAILY dueness filtering skipped it because it is still fresh.
    skipped_reason: str | None = None
    #: Structured evidence behind `skipped_reason`, populated for the DAILY
    #: not-due case (`last_success_at`, `age_seconds`, `refresh_interval_seconds`)
    #: so a Phase 4 UI can render "skipped (fresh)" without a second query.
    skip_detail: Mapping[str, object] | None = None
    #: Live accumulator the writer appends committed outcomes to. Counts above are
    #: recomputed from it once the writer has drained; before that they are zero.
    record_outcomes: list = field(default_factory=list, repr=False, compare=False)

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"

    @property
    def source_run_id(self) -> str | None:
        return self.attempts[-1].source_run_id if self.attempts else None


@dataclass(frozen=True, slots=True)
class RunResult:
    run_uid: str
    kind: str
    status: str
    requested_at: str
    started_at: str
    finished_at: str
    config_hash: str
    code_hash: str
    targets: tuple[TargetResult, ...] = ()
    peak_concurrency: int = 0
    peak_by_host: Mapping[str, int] = field(default_factory=dict)
    recovery: RecoveryReport | None = None
    error: Mapping[str, object] | None = None
    #: The Phase 2.5 run profile this run was scheduled under, mirrored here so
    #: 2.6b and Phase 4 can read them without re-parsing `report` (which also
    #: carries them, for persistence -- see `_run_report`).
    target_budget_seconds: float | None = None
    dueness_filtered: bool = False
    priority: str = "normal"
    #: Times the writer's drain gave up before its queue emptied, counted over the
    #: WHOLE run including the close. The persisted report carries the same counter
    #: as of the moment the run's terminal row was built, which is necessarily before
    #: the close; this field is the only place a close-time timeout can appear.
    #: Non-zero means some evidence this run produced may not have committed.
    writer_drain_timeouts: int = 0

    @property
    def succeeded_targets(self) -> tuple[TargetResult, ...]:
        return tuple(t for t in self.targets if t.succeeded)

    @property
    def failed_targets(self) -> tuple[TargetResult, ...]:
        return tuple(t for t in self.targets if t.status in ("failed", "timeout"))

    def target(self, source_run_key: str) -> TargetResult:
        for result in self.targets:
            if result.source_run_key == source_run_key:
                return result
        raise KeyError(source_run_key)

    @property
    def accepted(self) -> int:
        return sum(t.accepted for t in self.targets)

    @property
    def created(self) -> int:
        return sum(t.created for t in self.targets)


# --------------------------------------------------------------------------- #
# Hashes (Success Contract: "config/version hashes" on every run)
# --------------------------------------------------------------------------- #
def _digest(payload: object) -> str:
    return "sha256:" + hashlib.sha256(
        runstore.canonical_json(payload).encode("utf-8")
    ).hexdigest()


def config_hash_for(config: SourceConfig) -> str:
    """Hash of the configuration a run was planned from.

    Covers `companies`, `profile`, and `options` in full, because any of them can
    change which targets exist or what they query — and a checkpoint or an absence
    decision taken under one configuration is not valid under another.
    """
    return _digest(
        {
            "search_terms": list(config.search_terms),
            "companies": dict(config.companies),
            "profile": dict(config.profile),
            "options": dict(config.options),
        }
    )


def code_hash_for(plan: Sequence[tuple[SourceAdapter, SourceTarget]]) -> str:
    """Version hash of the adapter surface that produced a run.

    Descriptors, not source text: the descriptor is what the scheduler acts on
    (deadlines, concurrency, execution mode, inventory scope), so a change there
    is exactly a change that can alter a run's meaning. Two runs with the same
    code hash were scheduled under identical rules.
    """
    descriptors: dict[str, object] = {}
    for adapter, _target in plan:
        descriptor: SourceDescriptor = adapter.descriptor
        descriptors[descriptor.source_key] = {
            "category": str(descriptor.category),
            "run_kinds": sorted(str(k) for k in descriptor.run_kinds),
            "refresh_interval_seconds": descriptor.refresh_interval_seconds,
            "default_deadline_seconds": descriptor.default_deadline_seconds,
            "supports_checkpoint": descriptor.supports_checkpoint,
            "execution": str(descriptor.execution),
            "transport": str(descriptor.transport),
            "max_concurrent_targets": descriptor.max_concurrent_targets,
            "per_host_concurrency": descriptor.per_host_concurrency,
            "min_request_interval_seconds": descriptor.min_request_interval_seconds,
            "default_inventory_scope": str(descriptor.default_inventory_scope),
        }
    return _digest(
        {
            "descriptors": descriptors,
            "checkpoint_version": CHECKPOINT_VERSION,
            "canonical_hash_fields": list(CANONICAL_HASH_FIELDS),
        }
    )


# --------------------------------------------------------------------------- #
# Restart / resume API (Phase 4 wraps these in endpoints)
# --------------------------------------------------------------------------- #
def recover_orphans(
    connect: Callable[[], sqlite3.Connection],
    *,
    exclude_run_uids: Sequence[str] = (),
    actor: str = "scheduler-startup",
) -> RecoveryReport:
    """Mark every run/attempt a dead process left 'running' as interrupted.

    Opens, uses, and closes its own connection, so app startup can call it before
    any scheduler exists.
    """
    conn = connect()
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN IMMEDIATE")
        try:
            report = runstore.recover_orphans(
                conn, exclude_run_uids=exclude_run_uids, actor=actor
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        return report
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Internal per-run state
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class _Gates:
    global_gate: asyncio.Semaphore
    per_source: dict[str, asyncio.Semaphore]
    per_host: dict[str, asyncio.Semaphore]
    host_of: dict[str, str]
    inflight: int = 0
    peak: int = 0
    host_inflight: dict[str, int] = field(default_factory=dict)
    host_peak: dict[str, int] = field(default_factory=dict)


#: Run ids currently owned by a live scheduler in THIS process. Module-level
#: rather than per-instance so that two `Scheduler` objects in one process cannot
#: mark each other's in-flight runs interrupted during startup recovery.
_LIVE_RUNS: set[str] = set()


@dataclass(slots=True)
class _Preflight:
    recovery: RecoveryReport | None
    checkpoints: dict[str, Checkpoint]
    attempts: dict[str, int]
    completed: set[str]
    #: DAILY-only. `source_run_key` -> the evidence that excluded it, for kinds
    #: whose `RunProfile.dueness_filtered` is True. Empty for every other kind.
    not_due: dict[str, run_profiles.SkippedTarget] = field(default_factory=dict)


class RunHandle:
    """A started run. `run_uid` is known before any work happens, so a caller can
    cancel or query a run it has not yet awaited."""

    def __init__(self, run_uid: str, task: asyncio.Task, cancel: Callable[[], None]) -> None:
        self.run_uid = run_uid
        self._task = task
        self._cancel = cancel

    def cancel(self) -> None:
        """Request a clean stop.

        Cancels the target tasks, NOT the run task. The run keeps going just far
        enough to settle every attempt, write its own terminal row, and close the
        writer, so a cancelled run leaves committed records and complete evidence
        rather than the orphaned 'running' rows a hard cancel would.

        Valid before the run task has run at all: `start()` returns
        synchronously, so the set of target tasks is empty at that point. The
        flag is what carries the decision — the run's target-creation loop reads
        it before spawning each target, so a cancel that arrives first spawns
        nothing instead of cancelling nothing.
        """
        self._cancel()

    @property
    def done(self) -> bool:
        return self._task.done()

    def __await__(self):
        return self._task.__await__()

    async def wait(self) -> RunResult:
        return await self._task


# --------------------------------------------------------------------------- #
# The scheduler
# --------------------------------------------------------------------------- #
class Scheduler:
    """Runs a plan of (adapter, target) pairs under bounded concurrency.

    `connect` is a zero-argument callable returning a fresh `sqlite3.Connection`.
    There is deliberately no default: the scheduler must never be able to reach
    the live `app.db` implicitly, and every test passes a `tmp_path` factory.
    """

    def __init__(
        self,
        connect: Callable[[], sqlite3.Connection],
        *,
        config: SchedulerConfig | None = None,
        transport: Transport | None = None,
        event_hook: Callable[[Sequence[RunEvent]], None] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._connect = connect
        self.config = config or SchedulerConfig()
        self._transport = transport
        self._event_hook = event_hook
        self._rng = rng or random.Random()
        self._recovered = False

    # -- public API -------------------------------------------------------- #
    async def run(self, **kwargs) -> RunResult:
        return await self.start(**kwargs).wait()

    def start(
        self,
        *,
        kind: RunKind | str,
        config: SourceConfig | None = None,
        plan: Sequence[tuple[SourceAdapter, SourceTarget]] | None = None,
        run_uid: str | None = None,
        trigger: str = "manual",
        resume_run_uid: str | None = None,
        payloads: Mapping[str, Sequence[InboundPayload]] | None = None,
    ) -> RunHandle:
        """Begin a run and return immediately with a handle.

        `plan` overrides registry planning, which is how a test drives fake
        adapters without registering them process-wide (the registry is a global,
        and a test that mutated it would leak into the sixteen adapter suites).
        """
        source_config = config if config is not None else SourceConfig()
        run_kind = kind if isinstance(kind, RunKind) else RunKind(kind)
        work = list(plan) if plan is not None else registry.plan_run(source_config, run_kind)
        uid = resume_run_uid or run_uid or runstore.new_uid()
        _LIVE_RUNS.add(uid)

        cancel_event = asyncio.Event()
        tasks: set[asyncio.Task] = set()

        def _cancel() -> None:
            cancel_event.set()
            for task in tuple(tasks):
                task.cancel()

        task = asyncio.create_task(
            self._execute(
                run_uid=uid,
                run_kind=run_kind,
                config=source_config,
                plan=work,
                trigger=trigger,
                resume=resume_run_uid is not None,
                payloads=dict(payloads or {}),
                cancel_event=cancel_event,
                target_tasks=tasks,
            ),
            name=f"scheduler-run-{uid}",
        )
        return RunHandle(uid, task, _cancel)

    # -- the run ----------------------------------------------------------- #
    async def _execute(
        self,
        *,
        run_uid: str,
        run_kind: RunKind,
        config: SourceConfig,
        plan: Sequence[tuple[SourceAdapter, SourceTarget]],
        trigger: str,
        resume: bool,
        payloads: Mapping[str, Sequence[InboundPayload]],
        cancel_event: asyncio.Event,
        target_tasks: set[asyncio.Task],
    ) -> RunResult:
        requested_at = runstore.utc_now_iso()
        config_hash = config_hash_for(config)
        code_hash = code_hash_for(plan)
        profile = run_profiles.profile_for(run_kind)

        try:
            preflight: _Preflight = await asyncio.to_thread(
                self._preflight, run_uid, run_kind, plan, resume
            )
        except BaseException:
            _LIVE_RUNS.discard(run_uid)
            raise

        writer = SqliteWriter(
            self._connect,
            queue_size=self.config.queue_size,
            max_ops_per_transaction=self.config.max_ops_per_transaction,
            on_commit=self._event_hook,
            # How long the close may block the event loop waiting for a commit that
            # is still on a worker thread. Capped well below the drain bound: the
            # drain is an await and may take its full budget, whereas this one runs
            # in a `finally` that cannot await and therefore stalls the loop.
            close_lock_timeout_seconds=min(5.0, self.config.writer_drain_timeout_seconds),
        )
        try:
            await writer.start()
        except BaseException:
            _LIVE_RUNS.discard(run_uid)
            raise

        started_at = runstore.utc_now_iso()
        gates = self._build_gates(plan)
        transports = self._build_transports(plan)
        results: list[TargetResult] = []
        cleanup: set[asyncio.Task] = set()
        run_status = "succeeded"
        run_error: Mapping[str, object] | None = None
        drain_timeouts = 0

        try:
            await writer.submit(
                StartRun(
                    run_uid=run_uid,
                    kind=str(run_kind),
                    trigger=trigger,
                    requested_at=requested_at,
                    started_at=started_at,
                    config_hash=config_hash,
                    code_hash=code_hash,
                    resume=resume,
                    events=(
                        RunEvent(
                            run_uid=run_uid,
                            event_type="run.resumed" if resume else "run.started",
                            at=started_at,
                            payload={
                                "kind": str(run_kind),
                                "trigger": trigger,
                                "planned_targets": len(plan),
                                "config_hash": config_hash,
                                "code_hash": code_hash,
                                "target_budget_seconds": profile.target_budget_seconds,
                                "dueness_filtered": profile.dueness_filtered,
                                "priority": str(profile.priority),
                                "recovered_runs": (
                                    len(preflight.recovery.run_uids) if preflight.recovery else 0
                                ),
                                "recovered_attempts": (
                                    len(preflight.recovery.source_run_ids)
                                    if preflight.recovery
                                    else 0
                                ),
                            },
                        ),
                    ),
                )
            )

            # The cancel flag is read once per target, and the first read happens
            # after the `StartRun` submit above has been awaited: a cancel raised
            # before or during that await spawns no targets at all, rather than
            # cancelling an empty set of tasks and letting the whole plan run.
            for adapter, target in plan:
                if resume and target.source_run_key in preflight.completed:
                    results.append(
                        TargetResult(
                            source_run_key=target.source_run_key,
                            source_key=target.source_key,
                            instance_key=target.instance_key,
                            label=target.label,
                            inventory_scope=target.inventory_scope,
                            status="skipped",
                            skipped_reason="already succeeded in this run",
                        )
                    )
                    continue
                not_due = preflight.not_due.get(target.source_run_key)
                if not_due is not None:
                    results.append(
                        TargetResult(
                            source_run_key=target.source_run_key,
                            source_key=target.source_key,
                            instance_key=target.instance_key,
                            label=target.label,
                            inventory_scope=target.inventory_scope,
                            status="skipped",
                            skipped_reason="not due (fresh)",
                            skip_detail={
                                "last_success_at": not_due.last_success_at,
                                "age_seconds": not_due.age_seconds,
                                "refresh_interval_seconds": not_due.refresh_interval_seconds,
                            },
                        )
                    )
                    continue
                if cancel_event.is_set():
                    result = _not_started(target)
                    # Deferred, not awaited: this is evidence about a run that is
                    # already stopping, and a `submit` here could park on a full
                    # queue or raise a `WriterError` that would turn a cancelled run
                    # into a failed one. `aclose(drain=True)` still waits for it.
                    writer.submit_soon(
                        _unattempted_source_run(
                            run_uid=run_uid,
                            target=target,
                            requested_at=requested_at,
                            reason=str(result.skipped_reason),
                        )
                    )
                    results.append(result)
                    continue
                task = asyncio.create_task(
                    self._run_target(
                        run_uid=run_uid,
                        adapter=adapter,
                        target=target,
                        config=config,
                        writer=writer,
                        gates=gates,
                        requested_at=requested_at,
                        checkpoint=preflight.checkpoints.get(target.source_run_key),
                        attempt_offset=preflight.attempts.get(target.source_run_key, 0),
                        payloads=tuple(payloads.get(target.source_key, ())),
                        cleanup=cleanup,
                        transports=transports,
                    ),
                    name=f"target-{target.source_run_key}",
                )
                target_tasks.add(task)

            # `target_tasks` is snapshotted before the first await, so a task that
            # finishes (and is discarded from the set) cannot escape the gather.
            pending = tuple(target_tasks)
            gathered = await asyncio.gather(*pending, return_exceptions=True)
            target_tasks.clear()
            for item in gathered:
                if isinstance(item, TargetResult):
                    results.append(item)
                elif isinstance(item, asyncio.CancelledError):
                    continue
                elif isinstance(item, BaseException):
                    # A bug in the scheduler's own target runner, not an adapter.
                    run_status = "failed"
                    run_error = {"type": type(item).__name__, "message": str(item)}

            if cancel_event.is_set():
                run_status = "cancelled"
            elif run_status != "failed":
                run_status = (
                    "succeeded"
                    if all(t.status in ("succeeded", "skipped") for t in results)
                    else "partial"
                )
        except BaseException as exc:  # noqa: BLE001 - recorded on the run row, then re-raised
            run_status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
            run_error = {"type": type(exc).__name__, "message": str(exc)}
            raise
        finally:
            # Two nested `finally`s, because the two obligations have different
            # failure modes. Everything below can be interrupted — the run task
            # itself may be cancelled from outside at any of these awaits — but
            # `_LIVE_RUNS.discard` cannot be allowed to be: a run left in that set
            # is permanently excluded from startup orphan recovery, so its 'running'
            # rows would never be reconciled by anything, ever.
            try:
                drain_timeout = self.config.writer_drain_timeout_seconds
                if cleanup:
                    # Give cancelled adapter generators their unwind window; this is
                    # where a SUBPROCESS adapter kills its child. Suppressed, because
                    # a cancel delivered while they unwind must not skip the writer
                    # close below and leak a task plus a file handle.
                    with absorb_cancel():
                        await asyncio.wait(
                            cleanup, timeout=self.config.stream_close_grace_seconds
                        )
                # Everything submitted must commit before per-target counts are read:
                # a target settles its attempt as soon as its stream ends, which can
                # precede the commit of its final batch.
                #
                # BaseException, not Exception: this is the only path that closes the
                # writer and its connection, so it has to reach `aclose()` even if the
                # run task itself is being cancelled from outside. A cancelled drain
                # costs exact counts; a skipped `aclose()` leaks a task and a file
                # handle for the life of the process.
                with absorb_cancel():
                    await writer.drain(timeout=drain_timeout)
                results = [_with_record_totals(result) for result in results]

                finished_at = runstore.utc_now_iso()
                if writer.failure is not None and run_status != "cancelled":
                    run_status = "failed"
                    run_error = {
                        "type": type(writer.failure).__name__,
                        "message": str(writer.failure),
                        "stage": "writer",
                    }
                # Phase 2.4. After every target has settled and everything they
                # produced has committed, and before the run writes its own terminal
                # row, so the pass reads a complete snapshot of this run's attempts
                # and its result is part of the same run's evidence rather than a
                # later run's.
                absence = await self._settle_presence(
                    writer,
                    run_uid=run_uid,
                    at=finished_at,
                    run_status=run_status,
                    timeout=drain_timeout,
                )
                # Phase 3.1. Counted after the same drain, from the same committed
                # rows, and NOT gated on the run's outcome the way the presence pass
                # is: summarising what changed asserts nothing about what is missing,
                # so a cancelled or failed run's partial deliveries are still
                # legitimately described by it.
                changes = await self._settle_changes(
                    writer, run_uid=run_uid, at=finished_at, timeout=drain_timeout
                )
                report = _run_report(plan, results, gates, writer, profile, absence, changes)
                writer.submit_soon(
                    FinishRun(
                        run_uid=run_uid,
                        status=run_status,
                        finished_at=finished_at,
                        kept_count=sum(t.accepted for t in results),
                        new_count=sum(t.created for t in results),
                        report=report,
                        error=run_error,
                        events=(
                            RunEvent(
                                run_uid=run_uid,
                                event_type=f"run.{run_status}",
                                at=finished_at,
                                payload=report,
                            ),
                        ),
                    )
                )
                # One call, not a retry loop: `aclose` releases the writer task and
                # the connection in a `finally` that contains no `await`, so a cancel
                # arriving anywhere inside it still leaves the writer closed. There is
                # nothing for a second attempt to do — asserted by
                # `test_a_cancel_delivered_inside_aclose_still_releases_the_task_and_connection`.
                with absorb_cancel():
                    await writer.aclose(timeout=drain_timeout)
                # Read AFTER the close, so a drain that gave up while closing is
                # visible somewhere: the persisted report cannot carry it (the writer
                # has to commit that row before it can be closed), so the in-memory
                # result is where it lands.
                drain_timeouts = writer.stats.drain_timeouts
            finally:
                _LIVE_RUNS.discard(run_uid)

        return RunResult(
            run_uid=run_uid,
            kind=str(run_kind),
            status=run_status,
            requested_at=requested_at,
            started_at=started_at,
            finished_at=finished_at,
            config_hash=config_hash,
            code_hash=code_hash,
            targets=tuple(sorted(results, key=lambda t: (t.source_key, t.instance_key))),
            peak_concurrency=gates.peak,
            peak_by_host=dict(gates.host_peak),
            recovery=preflight.recovery,
            error=run_error,
            target_budget_seconds=profile.target_budget_seconds,
            dueness_filtered=profile.dueness_filtered,
            priority=str(profile.priority),
            writer_drain_timeouts=drain_timeouts,
        )

    # -- preflight --------------------------------------------------------- #
    def _preflight(
        self,
        run_uid: str,
        run_kind: RunKind,
        plan: Sequence[tuple[SourceAdapter, SourceTarget]],
        resume: bool,
    ) -> _Preflight:
        """One synchronous pass over the database before any target starts.

        Runs on a worker thread with its own short-lived connection. Orphan
        recovery, checkpoint loading, attempt-number continuation, and DAILY
        dueness all have to complete before the first attempt row is written,
        and doing them on the writer's connection would mean ordering them
        against a queue that is not running yet.
        """
        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("PRAGMA foreign_keys=ON")
            runstore.require_canonical_schema(conn)

            recovery: RecoveryReport | None = None
            if self.config.recover_orphans_on_start and not self._recovered:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    recovery = runstore.recover_orphans(
                        conn, exclude_run_uids=tuple(_LIVE_RUNS - {run_uid})
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                self._recovered = True

            checkpoints: dict[str, Checkpoint] = {}
            attempts: dict[str, int] = {}
            completed: set[str] = set()
            if resume:
                snapshot = runstore.resume_plan(conn, run_uid)
                if not snapshot["resumable"]:
                    raise ValueError(
                        f"run {run_uid!r} is not resumable (status {snapshot['status']!r}); "
                        "only an interrupted run may be resumed"
                    )
                attempts = runstore.max_attempt_by_source(conn, run_uid)
                completed = set(snapshot["completed_sources"])
                for _adapter, target in plan:
                    key = target.source_run_key
                    if key in completed:
                        continue
                    checkpoint = _safe_checkpoint(
                        runstore.latest_checkpoint_json(conn, run_uid=run_uid, source=key)
                    )
                    # A cursor comes back only when it still describes this exact
                    # query; `config_fingerprint` covers params and inventory scope.
                    if checkpoint is not None and checkpoint.is_valid_for(target):
                        checkpoints[key] = checkpoint

            not_due: dict[str, run_profiles.SkippedTarget] = {}
            if run_profiles.profile_for(run_kind).dueness_filtered and plan:
                # `source_instance_freshness` is the existing query helper the
                # roadmap says to reuse rather than writing new SQL; only the
                # `last_success_at` field feeds the pure decision in
                # `run_profiles.filter_due` below.
                wanted = sorted({target.source_run_key for _adapter, target in plan})
                now = datetime.now(timezone.utc)
                freshness = runstore.source_instance_freshness(
                    conn, at=now.isoformat(), sources=wanted
                )
                last_success_at = {
                    str(row["source"]): row["last_success_at"] for row in freshness
                }
                _due, skipped = run_profiles.filter_due(plan, last_success_at, now=now)
                not_due = {s.source_run_key: s for s in skipped}

            return _Preflight(
                recovery=recovery,
                checkpoints=checkpoints,
                attempts=attempts,
                completed=completed,
                not_due=not_due,
            )
        finally:
            conn.close()

    # -- gates ------------------------------------------------------------- #
    def _build_gates(self, plan: Sequence[tuple[SourceAdapter, SourceTarget]]) -> _Gates:
        """Compute every concurrency ceiling from the whole plan, once.

        Per-host limits are a `min` across every descriptor that touches the host:
        two sources behind the same API host share one budget and the politer of
        the two wins. Computing them up front rather than lazily on first use makes
        the ceiling independent of which target happened to start first.
        """
        host_limits: dict[str, int] = {}
        host_of: dict[str, str] = {}
        per_source: dict[str, asyncio.Semaphore] = {}
        for adapter, target in plan:
            descriptor = adapter.descriptor
            host = _host_key(target)
            host_of[target.source_run_key] = host
            limit = max(1, descriptor.per_host_concurrency)
            host_limits[host] = min(host_limits.get(host, limit), limit)
            if descriptor.max_concurrent_targets and descriptor.source_key not in per_source:
                per_source[descriptor.source_key] = asyncio.Semaphore(
                    descriptor.max_concurrent_targets
                )
        return _Gates(
            global_gate=asyncio.Semaphore(self.config.max_concurrent_targets),
            per_source=per_source,
            per_host={host: asyncio.Semaphore(limit) for host, limit in host_limits.items()},
            host_of=host_of,
        )

    def _build_transports(
        self, plan: Sequence[tuple[SourceAdapter, SourceTarget]]
    ) -> dict[str, Transport]:
        """One `PacedTransport` per HOST, shared by every target behind that host.

        Per host, not per source (2.6). `min_request_interval_seconds` and
        `per_host_concurrency` are statements about the machine being asked, so two
        sources sharing one API host have to share one pacer: a pacer each means the
        host sees the sum of their ceilings and none of their pacing, which is the
        opposite of what the descriptors asked for.

        The arithmetic is the same `min` across descriptors `_build_gates` uses, plus
        a `max` on the interval: politeness is a floor, so behind a shared host the
        politer concurrency and the longer interval both win. Built once, up front,
        so the policy cannot depend on which target happened to start first.

        A target that declares no host falls back to `source:<key>` exactly as the
        gates do, which degrades this to the pre-2.6 per-source pacer rather than to
        one global pacer over unrelated hosts.
        """
        if self._transport is None:
            return {}
        limits: dict[str, int] = {}
        intervals: dict[str, float] = {}
        for adapter, target in plan:
            descriptor = adapter.descriptor
            if descriptor.transport is TransportKind.NONE:
                continue
            host = _host_key(target)
            limit = max(1, descriptor.per_host_concurrency)
            limits[host] = min(limits.get(host, limit), limit)
            intervals[host] = max(
                intervals.get(host, 0.0), descriptor.min_request_interval_seconds
            )
        return {
            host: PacedTransport(
                self._transport,
                min_interval_seconds=intervals[host],
                per_host_concurrency=limit,
            )
            for host, limit in limits.items()
        }

    @contextlib.asynccontextmanager
    async def _hold(self, gates: _Gates, target: SourceTarget):
        """Acquire global -> per-source -> per-host, in that fixed order.

        A fixed order is what rules out deadlock between two targets that need the
        same pair of gates in opposite orders. The gate is held across a retry's
        backoff: the alternative — release and re-queue — can starve a target
        behind newly-arriving work, and the backoff is a quarter of a second.
        """
        host = gates.host_of.get(target.source_run_key) or _host_key(target)
        async with contextlib.AsyncExitStack() as stack:
            await stack.enter_async_context(gates.global_gate)
            source_gate = gates.per_source.get(target.source_key)
            if source_gate is not None:
                await stack.enter_async_context(source_gate)
            host_gate = gates.per_host.get(host)
            if host_gate is not None:
                await stack.enter_async_context(host_gate)
            gates.inflight += 1
            gates.peak = max(gates.peak, gates.inflight)
            gates.host_inflight[host] = gates.host_inflight.get(host, 0) + 1
            gates.host_peak[host] = max(gates.host_peak.get(host, 0), gates.host_inflight[host])
            try:
                yield
            finally:
                gates.inflight -= 1
                gates.host_inflight[host] -= 1

    # -- one target -------------------------------------------------------- #
    async def _run_target(
        self,
        *,
        run_uid: str,
        adapter: SourceAdapter,
        target: SourceTarget,
        config: SourceConfig,
        writer: SqliteWriter,
        gates: _Gates,
        requested_at: str,
        checkpoint: Checkpoint | None,
        attempt_offset: int,
        payloads: Sequence[InboundPayload],
        cleanup: set[asyncio.Task],
        transports: Mapping[str, Transport],
    ) -> TargetResult:
        descriptor = adapter.descriptor
        deadline = descriptor.deadline_for(target)
        budget = deadline * self.config.attempt_budget_multiplier
        outcomes: list[AttemptOutcome] = []
        sink: list[runstore.RecordOutcome] = []
        started = time.monotonic()

        try:
            await self._attempt_loop(
                run_uid=run_uid,
                adapter=adapter,
                target=target,
                config=config,
                writer=writer,
                gates=gates,
                requested_at=requested_at,
                checkpoint=checkpoint,
                attempt_offset=attempt_offset,
                payloads=payloads,
                cleanup=cleanup,
                transports=transports,
                deadline=deadline,
                budget=budget,
                outcomes=outcomes,
                sink=sink,
            )
        except asyncio.CancelledError:
            # Decision 5, applied to the gate as well as to the stream: a target
            # cancelled while queued behind a semaphore, or between attempts, has
            # no attempt of its own to settle, and dropping it here would leave a
            # planned target absent from the run report entirely.
            if not outcomes:
                # No attempt row exists for this target, so the only evidence in
                # `source_runs` that it was ever planned is this terminal row. The
                # deferred path is mandatory here: this task is cancelled and
                # cannot await anything.
                writer.submit_soon(
                    _unattempted_source_run(
                        run_uid=run_uid,
                        target=target,
                        requested_at=requested_at,
                        reason="cancelled before the first attempt started",
                    )
                )
            return TargetResult(
                source_run_key=target.source_run_key,
                source_key=target.source_key,
                instance_key=target.instance_key,
                label=target.label,
                inventory_scope=target.inventory_scope,
                status="cancelled",
                attempts=tuple(outcomes),
                fetched=outcomes[-1].fetched if outcomes else 0,
                duration_seconds=time.monotonic() - started,
                error={"type": "Cancelled", "message": "run cancelled"},
                skipped_reason=None if outcomes else "cancelled before the first attempt started",
                record_outcomes=sink,
            )

        last = outcomes[-1] if outcomes else None
        return TargetResult(
            source_run_key=target.source_run_key,
            source_key=target.source_key,
            instance_key=target.instance_key,
            label=target.label,
            inventory_scope=target.inventory_scope,
            status=last.status if last else "failed",
            attempts=tuple(outcomes),
            fetched=last.fetched if last else 0,
            duration_seconds=time.monotonic() - started,
            error=last.error if last else {"type": "NoAttempt", "message": "budget exhausted"},
            record_outcomes=sink,
        )

    async def _attempt_loop(
        self,
        *,
        run_uid: str,
        adapter: SourceAdapter,
        target: SourceTarget,
        config: SourceConfig,
        writer: SqliteWriter,
        gates: _Gates,
        requested_at: str,
        checkpoint: Checkpoint | None,
        attempt_offset: int,
        payloads: Sequence[InboundPayload],
        cleanup: set[asyncio.Task],
        transports: Mapping[str, Transport],
        deadline: float,
        budget: float,
        outcomes: list[AttemptOutcome],
        sink: list[runstore.RecordOutcome],
    ) -> None:
        """Hold the gates and run up to `max_attempts` attempts, appending each
        outcome to `outcomes` as it settles.

        Split from `_run_target` so that a cancellation arriving before the first
        attempt exists still has the outcomes accumulated so far to report."""
        async with self._hold(gates, target):
            budget_end = time.monotonic() + budget
            for index in range(self.config.max_attempts):
                attempt_no = attempt_offset + index + 1
                attempt_deadline = (
                    deadline if index == 0 else min(deadline, budget_end - time.monotonic())
                )
                if attempt_deadline <= 0:
                    break
                outcome = await self._run_attempt(
                    run_uid=run_uid,
                    adapter=adapter,
                    target=target,
                    config=config,
                    writer=writer,
                    requested_at=requested_at,
                    attempt=attempt_no,
                    deadline_seconds=attempt_deadline,
                    # A retry starts clean: `FetchContext` is single-use, and a
                    # half-advanced cursor from the failed attempt would resume
                    # past records that were never delivered.
                    checkpoint=checkpoint if index == 0 else None,
                    payloads=payloads,
                    sink=sink,
                    cleanup=cleanup,
                    transports=transports,
                )
                outcomes.append(outcome)
                if outcome.status in ("succeeded", "cancelled"):
                    break
                # Only a classified transient error buys the one retry (decisions
                # 1 and 4 in the module docstring).
                if outcome.disposition is not Disposition.TRANSIENT:
                    break
                if outcome.status == "timeout":
                    break
                if index + 1 >= self.config.max_attempts:
                    break
                backoff = self._backoff()
                if time.monotonic() + backoff >= budget_end:
                    writer.submit_soon(
                        EmitEvents(
                            events=(
                                RunEvent(
                                    run_uid=run_uid,
                                    source_run_id=outcome.source_run_id,
                                    event_type="source.retry_skipped",
                                    payload={
                                        "source": target.source_run_key,
                                        "reason": "target budget exhausted",
                                        "budget_seconds": budget,
                                    },
                                ),
                            )
                        )
                    )
                    break
                await asyncio.sleep(backoff)

    async def _run_attempt(
        self,
        *,
        run_uid: str,
        adapter: SourceAdapter,
        target: SourceTarget,
        config: SourceConfig,
        writer: SqliteWriter,
        requested_at: str,
        attempt: int,
        deadline_seconds: float,
        checkpoint: Checkpoint | None,
        payloads: Sequence[InboundPayload],
        sink: list[runstore.RecordOutcome],
        cleanup: set[asyncio.Task],
        transports: Mapping[str, Transport],
    ) -> AttemptOutcome:
        descriptor = adapter.descriptor
        source_run_id = runstore.new_uid()
        started_mono = time.monotonic()
        started_at = runstore.utc_now_iso()

        await writer.submit(
            CreateSourceRun(
                source_run_id=source_run_id,
                run_uid=run_uid,
                source=target.source_run_key,
                attempt=attempt,
                requested_at=requested_at,
                started_at=started_at,
                deadline_at=_iso_in(deadline_seconds),
                inventory_scope=str(target.inventory_scope),
                metadata={
                    "label": target.label,
                    "execution": str(descriptor.execution),
                    "category": str(descriptor.category),
                    "deadline_seconds": deadline_seconds,
                    "resumed": checkpoint is not None,
                },
                events=(
                    RunEvent(
                        run_uid=run_uid,
                        source_run_id=source_run_id,
                        event_type="source.started",
                        at=started_at,
                        payload={
                            "source": target.source_run_key,
                            "attempt": attempt,
                            "deadline_seconds": deadline_seconds,
                            "inventory_scope": str(target.inventory_scope),
                        },
                    ),
                ),
            )
        )

        ctx = FetchContext(
            config=config,
            transport=self._transport_for(descriptor, target, transports),
            resume_from=checkpoint,
            payloads=payloads,
            deadline_at=started_mono + deadline_seconds,
        )

        buffer: list[NormalizedPosting] = []
        #: A batch removed from the buffer whose `submit` has not returned yet. If
        #: the deadline fires while that submit is parked on a full queue, the
        #: records are in neither place, so the `finally` re-submits it. Without
        #: this, backpressure plus a timeout silently loses a batch.
        in_flight: RecordBatch | None = None
        fetched = 0
        last_flush = time.monotonic()
        status = "succeeded"
        disposition = Disposition.SUCCESS
        error: Mapping[str, object] | None = None
        cancelled = False
        stream = adapter.fetch(target, ctx)

        def build_batch() -> RecordBatch:
            return RecordBatch(
                run_uid=run_uid,
                source_run_id=source_run_id,
                records=tuple(buffer),
                recorded_at=runstore.utc_now_iso(),
                fetched_count=fetched,
                checkpoint_json=_checkpoint_json(ctx),
                outcome_sink=sink,
            )

        try:
            async with asyncio.timeout(deadline_seconds):
                async for record in stream:
                    fetched += 1
                    buffer.append(record)
                    if (
                        len(buffer) >= self.config.batch_size
                        or (time.monotonic() - last_flush) >= self.config.flush_interval_seconds
                    ):
                        in_flight = build_batch()
                        buffer = []
                        last_flush = time.monotonic()
                        # The backpressure point: a full writer queue parks the
                        # adapter here, inside its own deadline.
                        await writer.submit(in_flight)
                        in_flight = None
        except TimeoutError:
            status = "timeout"
            disposition = Disposition.TRANSIENT
            error = {
                "type": "DeadlineExceeded",
                "disposition": str(Disposition.TRANSIENT),
                "message": f"{target.source_run_key} exceeded its {deadline_seconds:g}s deadline",
                "source_key": target.source_key,
                "instance_key": target.instance_key,
                "retryable": False,
                "retry_policy": "deadline exhaustion is never retried",
            }
        except asyncio.CancelledError:
            status = "cancelled"
            disposition = Disposition.PERMANENT
            cancelled = True
            error = {
                "type": "Cancelled",
                "disposition": str(Disposition.PERMANENT),
                "message": "run cancelled",
            }
        except WriterError as exc:
            # Ours, not the adapter's. Stamping the source's key on a failed
            # commit would put a persistence outage into that source's health
            # history, where every later analysis would read it as a bad board.
            status = "failed"
            disposition = Disposition.PERMANENT
            error = {
                "type": type(exc).__name__,
                "disposition": str(Disposition.PERMANENT),
                "message": str(exc),
                "stage": "writer",
                "note": "persistence failure; not attributable to the adapter",
            }
        except SourceError as exc:
            status = "failed"
            disposition = exc.disposition
            error = exc.to_json_dict()
        except Exception as exc:  # noqa: BLE001 - an adapter bug, classified permanent
            status = "failed"
            disposition = Disposition.PERMANENT
            error = {
                "type": type(exc).__name__,
                "disposition": str(Disposition.PERMANENT),
                "message": str(exc),
                "source_key": target.source_key,
                "instance_key": target.instance_key,
                "note": "unclassified adapter exception; treated as permanent",
            }
        finally:
            # Unwinding the generator is the cancellation-propagation obligation
            # for SUBPROCESS adapters: their `finally` is where the child is
            # killed. A cancelled task cannot await, so the close is handed to the
            # run's cleanup set instead of being skipped.
            cancelled = await self._close_stream(stream, cancelled=cancelled, cleanup=cleanup) or cancelled

            # Both the final batch and the settling update go through the deferred
            # path. They are ordered behind everything this attempt already
            # submitted, they cannot block a target that is unwinding, and
            # `writer.aclose(drain=True)` waits for them.
            if in_flight is not None:
                writer.submit_soon(in_flight)
                in_flight = None
            if buffer:
                writer.submit_soon(build_batch())
                buffer = []
            finished_at = runstore.utc_now_iso()
            writer.submit_soon(
                FinishSourceRun(
                    source_run_id=source_run_id,
                    status=status,
                    finished_at=finished_at,
                    fetched_count=fetched,
                    # None => keep the accumulated count; see finish_source_run.
                    accepted_count=None,
                    checkpoint_json=_checkpoint_json(ctx),
                    error=error,
                    metadata={
                        "label": target.label,
                        "inventory_scope": str(target.inventory_scope),
                        "duration_seconds": round(time.monotonic() - started_mono, 6),
                        "attempt": attempt,
                    },
                    events=(
                        RunEvent(
                            run_uid=run_uid,
                            source_run_id=source_run_id,
                            event_type=f"source.{status}",
                            at=finished_at,
                            payload={
                                "source": target.source_run_key,
                                "attempt": attempt,
                                "fetched": fetched,
                                "error": error,
                            },
                        ),
                    ),
                )
            )

        return AttemptOutcome(
            attempt=attempt,
            source_run_id=source_run_id,
            status=status,
            disposition=disposition,
            fetched=fetched,
            duration_seconds=time.monotonic() - started_mono,
            error=error,
        )

    # -- presence ---------------------------------------------------------- #
    async def _settle_presence(
        self,
        writer: SqliteWriter,
        *,
        run_uid: str,
        at: str,
        run_status: str,
        timeout: float | None = None,
    ) -> dict | None:
        """Run the Phase 2.4 presence pass, or decline to and say so with `None`.

        Declining is the safe direction and is what the roadmap asks for on every
        path that is not a settled run: "failed/timed-out sources retain
        last-known-good records". A run that never reached the pass simply leaves
        every posting exactly as the last successful run left it.

        The submit and drain are suppressed as a pair. This runs inside `_execute`'s
        `finally`, where a raise would replace whatever outcome the run already has
        with a persistence error, and where the run task may itself be under
        cancellation. A pass that could not commit reports `None` — no marking is
        strictly better than a partial one. The drain is bounded for the same reason
        every other wait in that `finally` is: a writer that will never commit this
        op must cost the run a report field, not its ability to finish.
        """
        if run_status not in PRESENCE_PASS_RUN_STATUSES or writer.failure is not None:
            return None
        op = MarkPresence(run_uid=run_uid, at=at)
        drained = False
        with absorb_cancel():
            await writer.submit(op)
            drained = await writer.drain(timeout=timeout)
        if not drained or writer.failure is not None:
            # `MarkPresence.apply` publishes its report from INSIDE the transaction,
            # so the object carries a report whether or not that transaction went on
            # to commit. A rollback — a fatal commit error, a busy retry that then
            # failed — would otherwise have this return a description of markings
            # that are not in the database, into the run's own persisted evidence.
            # Re-checked here rather than only before the submit, because the failure
            # this is about happens during the commit, not before it.
            return None
        return op.report

    # -- changes ----------------------------------------------------------- #
    async def _settle_changes(
        self,
        writer: SqliteWriter,
        *,
        run_uid: str,
        at: str,
        timeout: float | None = None,
    ) -> dict | None:
        """Count what this run changed, or decline to and say so with `None`.

        Same shape and the same suppression rules as `_settle_presence`, for the same
        reason: this runs inside `_execute`'s `finally`, where a raise would replace
        the run's outcome with a persistence error and where the run task may itself
        be under cancellation.

        `None` means "not counted", never "nothing changed" — and it costs nothing,
        because the counts are derived, not stored: `runstore.change_summary` and
        `runstore.dirty_posting_ids` answer the same question from the committed rows
        at any later time, from any connection.
        """
        if writer.failure is not None:
            return None
        op = SummarizeChanges(run_uid=run_uid, at=at)
        drained = False
        with absorb_cancel():
            await writer.submit(op)
            drained = await writer.drain(timeout=timeout)
        if not drained or writer.failure is not None:
            return None
        return op.report

    # -- helpers ----------------------------------------------------------- #
    async def _close_stream(
        self, stream: object, *, cancelled: bool, cleanup: set[asyncio.Task]
    ) -> bool:
        """Unwind an adapter's generator. Returns True if closing was cancelled.

        Closing is what runs the adapter's `finally`, which for a SUBPROCESS
        adapter is where the child process is terminated. Skipping it would leave
        the child alive until interpreter shutdown collected the generator.
        """
        aclose = getattr(stream, "aclose", None)
        if aclose is None:
            return False
        if cancelled:
            task = asyncio.create_task(aclose())
            cleanup.add(task)
            task.add_done_callback(cleanup.discard)
            return False
        try:
            await asyncio.wait_for(aclose(), timeout=self.config.stream_close_grace_seconds)
        except asyncio.CancelledError:
            # The run was cancelled while we were unwinding. Report it so the
            # caller uses the deferred submit path instead of trying to await.
            return True
        except Exception:  # noqa: BLE001 - a failure to unwind is not the attempt's result
            return False
        return False

    def _transport_for(
        self,
        descriptor: SourceDescriptor,
        target: SourceTarget,
        transports: Mapping[str, Transport],
    ) -> Transport | None:
        """The run's pacer for this target's host, or None for a source that asked
        for no transport at all.

        A lookup rather than a construction: `_build_transports` computed the whole
        table from the plan before any target started, so pacing is a property of the
        run, not of whichever target happened to reach this line first.
        """
        if descriptor.transport is TransportKind.NONE or self._transport is None:
            return None
        return transports.get(_host_key(target))

    def _backoff(self) -> float:
        base = self.config.retry_base_delay_seconds
        if base <= 0:
            return 0.0
        jitter = self.config.retry_jitter
        return base * self._rng.uniform(1.0 - jitter, 1.0 + jitter)


# --------------------------------------------------------------------------- #
# Module helpers
# --------------------------------------------------------------------------- #
def _host_key(target: SourceTarget) -> str:
    """Per-host gate key. A target that declares no host falls back to its source,
    which degrades the host gate into a second per-source gate rather than into no
    gate at all."""
    return target.host or f"source:{target.source_key}"


def _not_started(target: SourceTarget) -> TargetResult:
    """A planned target the run never spawned, because a cancel arrived first.

    Every planned target appears in the run report exactly once, whatever the
    run's outcome: a report that silently omits the targets it never reached
    cannot be reconciled against the plan it claims to describe.
    """
    return TargetResult(
        source_run_key=target.source_run_key,
        source_key=target.source_key,
        instance_key=target.instance_key,
        label=target.label,
        inventory_scope=target.inventory_scope,
        status="cancelled",
        skipped_reason="cancelled before the target started",
        error={"type": "Cancelled", "message": "run cancelled before this target started"},
    )


def _unattempted_source_run(
    *, run_uid: str, target: SourceTarget, requested_at: str, reason: str
) -> RecordUnattemptedSourceRun:
    """The `source_runs` row for a planned target that never attempted a fetch.

    Terminal on creation and numbered under a step of its own, so it records what
    happened without joining the attempt sequence — see decision 5 in the module
    docstring and `runstore.record_unattempted_source_run`.
    """
    at = runstore.utc_now_iso()
    source_run_id = runstore.new_uid()
    return RecordUnattemptedSourceRun(
        source_run_id=source_run_id,
        run_uid=run_uid,
        source=target.source_run_key,
        status="cancelled",
        requested_at=requested_at,
        finished_at=at,
        inventory_scope=str(target.inventory_scope),
        error={"type": "Cancelled", "message": reason},
        metadata={"label": target.label, "reason": reason, "attempted": False},
        events=(
            RunEvent(
                run_uid=run_uid,
                source_run_id=source_run_id,
                event_type="source.cancelled",
                at=at,
                payload={
                    "source": target.source_run_key,
                    "reason": reason,
                    "attempted": False,
                },
            ),
        ),
    )


def _iso_in(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _checkpoint_json(ctx: FetchContext) -> str | None:
    checkpoint = ctx.checkpoint
    return checkpoint.to_json() if checkpoint is not None else None


def _safe_checkpoint(blob: str | None) -> Checkpoint | None:
    """A malformed or version-mismatched checkpoint means "start clean".

    Checkpoints are advisory, so refusing to parse one is always safe and always
    better than resuming from a cursor whose meaning is unknown.
    """
    try:
        return Checkpoint.from_json(blob)
    except (ValueError, TypeError):
        return None


def _totals(sink: Sequence[runstore.RecordOutcome]) -> runstore.RecordOutcome:
    total = runstore.RecordOutcome()
    for outcome in sink:
        total = total.merge(outcome)
    return total


def _with_record_totals(result: TargetResult) -> TargetResult:
    totals = _totals(result.record_outcomes)
    return replace(
        result,
        accepted=totals.accepted,
        created=totals.created,
        duplicates=totals.duplicates,
        conflicts=totals.conflicts,
    )


def _run_report(
    plan: Sequence[tuple[SourceAdapter, SourceTarget]],
    results: Sequence[TargetResult],
    gates: _Gates,
    writer: SqliteWriter,
    profile: run_profiles.RunProfile,
    absence: dict | None = None,
    changes: dict | None = None,
) -> dict[str, object]:
    return {
        # Phase 2.5: the run kind and the profile it was scheduled under,
        # persisted here (`aggregate_report_json`) so 2.6b and Phase 4 can
        # read them without re-deriving them from `pipeline_runs.kind`.
        "kind": str(profile.kind),
        "target_budget_seconds": profile.target_budget_seconds,
        "dueness_filtered": profile.dueness_filtered,
        "priority": str(profile.priority),
        "targets": len(plan),
        #: None means the presence pass did not run (cancelled run, failed run, or
        #: dead writer). That is a materially different statement from a pass that
        #: ran and marked nothing, and the report must not conflate them.
        "presence": absence,
        #: Phase 3.1's change accounting: how many of the postings this run observed
        #: moved to a different content version ("N changed"), split into first
        #: sightings and updates, beside the number of `posting_versions` rows the run
        #: actually minted. The gap between `changed` and `versions_created` is
        #: content that reverted to a state already on file. None means the count did
        #: not run (dead writer) — materially different from a count of zero, and the
        #: report must not conflate them. The dirty IDS are not here on purpose: they
        #: are recomputed on demand by `runstore.dirty_posting_ids(conn, run_uid)`.
        "changed": changes,
        "succeeded": sum(1 for t in results if t.status == "succeeded"),
        "failed": sum(1 for t in results if t.status in ("failed", "timeout")),
        "cancelled": sum(1 for t in results if t.status == "cancelled"),
        "skipped": sum(1 for t in results if t.status == "skipped"),
        #: The DAILY-dueness subset of "skipped", broken out with its evidence so
        #: a Phase 4 UI can render "skipped (fresh)" instead of these targets
        #: silently vanishing from the run's story. Every planned target still
        #: appears exactly once among `targets`/the result list; this is a view
        #: onto that same data, not a second source of truth.
        "skipped_not_due": [
            {"source": t.source_run_key, "label": t.label, **(t.skip_detail or {})}
            for t in results
            if t.status == "skipped" and t.skipped_reason == "not due (fresh)"
        ],
        "accepted": sum(t.accepted for t in results),
        "created": sum(t.created for t in results),
        "peak_concurrency": gates.peak,
        "peak_by_host": dict(gates.host_peak),
        "writer": {
            "transactions": writer.stats.transactions,
            "records": writer.stats.records,
            "max_queue_depth": writer.stats.max_queue_depth,
            "max_ops_per_transaction": writer.stats.max_ops_per_transaction,
            "busy_retries": writer.stats.busy_retries,
            "dropped": writer.stats.dropped,
            #: Non-zero means a drain gave up before the queue emptied, so the counts
            #: above and the per-target counts in this report may be short of what
            #: actually committed. Recorded rather than hidden: it is the only signal
            #: that distinguishes "nothing more was written" from "we stopped
            #: waiting to find out". Necessarily counts only the drains BEFORE this
            #: row was built — the writer has to still be open to commit it, so a
            #: timeout during the close appears in `RunResult.writer_drain_timeouts`
            #: and nowhere in the database.
            "drain_timeouts": writer.stats.drain_timeouts,
        },
    }
