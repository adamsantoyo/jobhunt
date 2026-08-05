"""Phase 4.1: the persisted run service.

Replaces the legacy in-memory `sweeprunner.Runner` for canonical runs. Nothing
here is in-memory-authoritative: a run's identity, its progress, its evidence and
its completion all live in `pipeline_runs` / `source_runs` / `run_events`, so a
browser that reconnects after a server restart replays the same stream from the
same rows. The only process-local state is (a) the handle needed to cancel a run
this process is currently executing and (b) a fan-out used to WAKE readers, never
to carry data.

THE SIX DECISIONS THAT SHAPE THIS MODULE

1. ONE WRITER AT A TIME, ENFORCED BY AWAITING THE RUN TASK. A canonical run is
   scheduler fetch -> enrichment -> scoring, strictly sequential. The guarantee is
   structural rather than a sleep or a poll: `Scheduler._execute` closes its
   `SqliteWriter` (`await writer.aclose(...)`) inside the `finally` of the run
   task, so the moment `await handle` returns or raises, that writer's task and
   connection are already released. Everything this module does afterwards --
   enrichment's per-row commits, the graph pass, its own `run_events` appends --
   therefore runs against a database with no other writer on it, which is also
   why `next_event_sequence` + `append_run_events` cannot race the writer's own
   sequence allocator.

   THE ONE CAVEAT, stated rather than glossed: `aclose` bounds how long it will
   block waiting for a commit that is still running on a worker thread, and when
   that bound expires it deliberately LEAKS the connection instead of closing it
   under a live statement (`writer._close_connection`). On that pathological path
   a commit can still be in flight when the stages below start, so the claim is
   "no other writer, except after a close that timed out" -- and that exception is
   COUNTED (`writer.WriterStats.unclosed_connections`) rather than silent, so it
   is observable in the run's own teardown numbers. `busy_timeout` on this
   module's connections is what turns such a straggler into a short wait rather
   than an error.

   AND IT IS PER-RUN, NOT GLOBAL. The lane matrix deliberately lets `aggregators`
   run alongside `daily`, so two runs -- two writers -- can legitimately be on the
   same database file at once; what decision 1 promises is that nothing else
   writes on behalf of THIS run while its stages do. `busy_timeout` (10s here,
   matching `SqliteWriter`'s) bounds that cross-lane contention, and 4.5
   validation is where it gets measured on copied production data instead of
   assumed.

2. THE FAN-OUT CARRIES NO PAYLOAD. `Scheduler(event_hook=...)` fires after the
   transaction containing those events commits, but the `RunEvent` objects it
   hands over do not carry the sequence the writer assigned them (the writer
   allocates it inside the transaction). Sequence IS the SSE cursor, so a stream
   built from the hook's objects would have to invent one. Instead the hook is a
   doorbell: subscribers are woken and then READ the persisted rows past their own
   cursor. That makes "live tail" and "replay after a restart" literally the same
   code path, which is the only way the no-gap/no-duplicate promise survives a
   reconnect.

3. STAGES RUN ON succeeded AND partial, NEVER ON failed OR cancelled. Same rule
   the scheduler's own presence pass uses, for the same reason: per-source failure
   isolation is the design, so one dead board must not stop a healthy one's
   postings from being described and scored. A failed run's evidence cannot be
   trusted and a cancelled run stopped mid-enumeration.

   "Cancelled" is a property of the RUN, not only of its fetch: a cancel that
   arrives after the fetch has already settled still stops everything after it.
   The flag is read at every stage boundary and cooperatively INSIDE both stages
   -- enrichment by cancelling the task that wraps it (its writes are per-row and
   a died-mid-pass enrichment self-heals on the next run), scoring by checking
   between graph ops (an unfinished pass row licences nothing; see decision 6). A
   cancelled run settles with outcome "cancelled" and never with "succeeded",
   because the API already answered 202 for the cancel.

4. A STAGE FAILURE IS EVIDENCE, NOT A CRASH. Each stage is wrapped: the error is
   appended as its own `run_events` row and repeated in the settled event's
   payload, and the next stage still runs. Scoring after a failed enrichment is
   deliberate -- it scores whatever descriptions already exist, which is strictly
   better than skipping the pass and leaving the run unscored.

5. THE CLIENT-FACING COMPLETION SIGNAL IS `service.run.settled`, NOT the
   scheduler's `run.succeeded`. The fetch phase finishing is not the run
   finishing: two more stages follow it. The settled event is appended only after
   every stage has settled, and the SSE endpoint closes the stream on it.

6. THE GRAPH PASS COMMITS PER OP, NOT ONCE AT THE END. `graph.run_pass` does no
   transaction control, so a caller that wrapped the whole pass in one
   transaction would hold the write lock for the pass's entire duration -- and on
   a real corpus that is seconds, during which every user-state edit and the
   concurrent `aggregators` lane's writer would sit on `busy_timeout` and then
   fail. So this module drives the same stage sequence itself
   (`OpenPass -> BridgeLegacyUrls -> ResolvePass -> ScoreGraphPass per page ->
   ClosePass`) and commits after each op, exactly as `SqliteWriter` would if the
   ops were submitted to it: each op's `.events` are appended in the SAME
   transaction as its `.apply`, so evidence and write are still atomic per op.

   That is safe because the pass is self-healing by design, not because the work
   is small: a `score_passes` row left `running` licences NOTHING (`graph`'s
   baseline rule requires a COMPLETED pass), invalidations are consumed only by
   `ClosePass`, and scores are keyed by (posting version, profile version,
   scorer), so a pass that dies between ops re-emits its remaining work on the
   next pass and rewrites nothing it already wrote. The visible payoff is the
   roadmap's: scored rows become readable after each committed page instead of
   after the whole pass.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from . import config as app_config
from .db import connect as db_connect
from .sources import enrichment as enrichment_module
from .sources import graph as graph_module
from .sources import registry as registry_module
from .sources import resolver as resolver_module
from .sources import runstore, scheduler as scheduler_module
from .sources import scoring as scoring_module
from .sources.contract import RunKind, SourceConfig
from .sources.scheduler import RunHandle, Scheduler, SchedulerConfig
from .sources.writer import RunEvent

__all__ = [
    "CanonicalSchemaUnavailable",
    "EVENT_ENRICHMENT_CANCELLED",
    "EVENT_ENRICHMENT_FAILED",
    "EVENT_ENRICHMENT_FINISHED",
    "EVENT_ENRICHMENT_STARTED",
    "EVENT_RUN_SETTLED",
    "EVENT_SCORING_CANCELLED",
    "EVENT_SCORING_FAILED",
    "EVENT_SCORING_FINISHED",
    "EVENT_SCORING_STARTED",
    "EVENT_STAGES_SKIPPED",
    "RunConflict",
    "RunService",
    "RunServiceError",
    "SUPPORTED_KINDS",
    "UnknownRunKind",
    "UnsupportedRunKind",
    "default_service",
    "recover_orphans_if_canonical",
    "reset_default_service",
    "shutdown_default_service",
]

# --------------------------------------------------------------------------- #
# Vocabulary
# --------------------------------------------------------------------------- #
#: Kinds this wave can execute. `llm-review` and `manual-import` are wave-3 scope
#: (their open questions are documented in `sources/llm_review_pass.py`); the API
#: answers 501 for them rather than pretending they are unknown.
SUPPORTED_KINDS: frozenset[str] = frozenset({"daily", "full-direct", "aggregators"})
DEFERRED_KINDS: frozenset[str] = frozenset({"llm-review", "manual-import"})

#: Kinds whose fetch is followed by enrichment + scoring. Aggregator runs feed the
#: resolver rather than the description/score path, so they settle at fetch.
STAGED_KINDS: frozenset[str] = frozenset({"daily", "full-direct"})

#: Fetch outcomes that licence the post-fetch stages. Mirrors
#: `scheduler.PRESENCE_PASS_RUN_STATUSES` deliberately -- see decision 3.
STAGE_RUN_STATUSES: frozenset[str] = frozenset({"succeeded", "partial"})

#: `daily` and `full-direct` both drive the direct-inventory writers, so they are
#: mutually exclusive with each other. `aggregators` is independent of both, and
#: exclusive only with itself.
_EXCLUSION_GROUPS: Mapping[str, str] = {
    "daily": "direct",
    "full-direct": "direct",
    "aggregators": "aggregators",
}

#: `pipeline_runs.status` values that mean "still going". Everything else --
#: including `interrupted`, written by orphan recovery -- is terminal.
_ACTIVE_STATUSES: frozenset[str] = frozenset({"running"})

EVENT_ENRICHMENT_STARTED = "stage.enrichment.started"
EVENT_ENRICHMENT_FINISHED = "stage.enrichment.finished"
EVENT_ENRICHMENT_FAILED = "stage.enrichment.failed"
#: A stage stopped because the run was cancelled. Distinct from `.failed`: nothing
#: went wrong, and the work it did not do is still eligible for the next run.
EVENT_ENRICHMENT_CANCELLED = "stage.enrichment.cancelled"
EVENT_SCORING_STARTED = "stage.scoring.started"
EVENT_SCORING_FINISHED = "stage.scoring.finished"
EVENT_SCORING_FAILED = "stage.scoring.failed"
EVENT_SCORING_CANCELLED = "stage.scoring.cancelled"
EVENT_STAGES_SKIPPED = "service.stages.skipped"
#: The terminal, client-facing event. The SSE stream closes on it.
EVENT_RUN_SETTLED = "service.run.settled"

#: Every event type this service appends itself (the scheduler owns `run.*`,
#: `source.*` and friends). Used by the run-detail read to pick stage reports out
#: of `run_events` without a second table.
SERVICE_EVENT_TYPES: frozenset[str] = frozenset(
    {
        EVENT_ENRICHMENT_STARTED,
        EVENT_ENRICHMENT_FINISHED,
        EVENT_ENRICHMENT_FAILED,
        EVENT_ENRICHMENT_CANCELLED,
        EVENT_SCORING_STARTED,
        EVENT_SCORING_FINISHED,
        EVENT_SCORING_FAILED,
        EVENT_SCORING_CANCELLED,
        EVENT_STAGES_SKIPPED,
        EVENT_RUN_SETTLED,
    }
)

#: How many `run_events` rows one read pulls. A reader that fills its page loops
#: again immediately rather than parking on the doorbell.
EVENT_PAGE_SIZE = 500


# --------------------------------------------------------------------------- #
# Errors (each maps to exactly one HTTP status in routers/runsapi.py)
# --------------------------------------------------------------------------- #
class RunServiceError(Exception):
    """Base for everything the API layer translates into a status code."""


class UnknownRunKind(RunServiceError):
    """A kind string that is not a `RunKind` at all -> 400."""


class UnsupportedRunKind(RunServiceError):
    """A real kind this wave does not execute yet -> 501."""


class RunConflict(RunServiceError):
    """Another run (canonical or legacy) owns this lane right now -> 409."""


class CanonicalSchemaUnavailable(RunServiceError):
    """The configured database has no canonical schema -> 503."""


class UnknownRun(RunServiceError):
    """No `pipeline_runs` row and no live handle -> 404."""


class UnknownSource(RunServiceError):
    """No candidate plan contains a target for this source -> 404 (4.4)."""


class _StageCancelled(Exception):
    """Internal: a stage stopped early because the run was cancelled.

    Never leaves this module. A stage that raises it is recorded as `cancelled`
    rather than `failed`, because nothing went wrong and the work it did not do is
    still eligible for the next run.
    """


# --------------------------------------------------------------------------- #
# Fan-out
# --------------------------------------------------------------------------- #
class _Fanout:
    """Per-run doorbell. Carries no data -- see decision 2.

    One coalescing slot per subscriber: a reader that has not consumed its token
    yet will re-read the table anyway, so a second token would buy nothing and a
    growing queue would only be a memory leak on a slow client.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = {}

    def subscribe(self, run_uid: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._subscribers.setdefault(run_uid, set()).add(queue)
        return queue

    def unsubscribe(self, run_uid: str, queue: asyncio.Queue) -> None:
        subscribers = self._subscribers.get(run_uid)
        if not subscribers:
            return
        subscribers.discard(queue)
        if not subscribers:
            self._subscribers.pop(run_uid, None)

    def publish(self, run_uid: str) -> None:
        for queue in tuple(self._subscribers.get(run_uid, ())):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass  # a token is already pending; the reader re-reads regardless

    def subscriber_count(self, run_uid: str) -> int:
        return len(self._subscribers.get(run_uid, ()))


# --------------------------------------------------------------------------- #
# In-flight bookkeeping
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class _ActiveRun:
    run_uid: str
    kind: str
    handle: RunHandle
    supervisor: asyncio.Task | None = None
    #: The task wrapping the stage currently in flight, when that stage can be
    #: stopped by cancelling it (enrichment). Scoring runs on a worker thread and
    #: is stopped cooperatively instead, so it never sets this.
    stage_task: asyncio.Task | None = None
    fetch_status: str | None = None
    cancel_requested: bool = False
    settled: bool = False
    stages: dict[str, Any] = field(default_factory=dict)
    #: What `start_run`/`retry_source` were called with, captured at dispatch
    #: time so `list_runs` can show a run that is active in this process but
    #: has not reached `pipeline_runs` yet (see `list_runs_sync`'s merge and
    #: `run_exists`'s docstring for the same race). `requested_at` here is this
    #: process's clock at dispatch, not the writer's own `runstore.utc_now_iso()`
    #: call inside the `StartRun` op -- the two are a few milliseconds apart at
    #: most, and once the row lands the persisted value takes over.
    trigger: str = "manual"
    requested_at: str = ""


# --------------------------------------------------------------------------- #
# The service
# --------------------------------------------------------------------------- #
class RunService:
    """Owns canonical run execution and the persisted event stream behind it.

    Every collaborator is injectable, because the production wiring (real
    `config.DB_PATH`, the adapter registry, an httpx transport, the real
    `profile.json`) is precisely what a test must not reach.
    """

    def __init__(
        self,
        *,
        connect: Callable[[], sqlite3.Connection] | None = None,
        scheduler_config: SchedulerConfig | None = None,
        scheduler_transport: Any = None,
        enrichment_transport: Any = None,
        transport_factory: Callable[[], Any] | None = None,
        source_config: SourceConfig | None = None,
        plan_factory: Callable[[RunKind, SourceConfig], Sequence[tuple[Any, Any]] | None]
        | None = None,
        profile_doc: Mapping[str, Any] | None = None,
        profile: Any = None,
        enrich: Callable[..., Any] | None = None,
        score: Callable[..., Any] | None = None,
        legacy_runner: Any = None,
        trigger: str = "api",
        enrichment_deadline_seconds: float | None = None,
        score_batch_size: int | None = None,
        on_score_op: Callable[[str, int], None] | None = None,
    ) -> None:
        self._connect_factory = connect
        self._scheduler_config = scheduler_config
        self._scheduler_transport = scheduler_transport
        self._enrichment_transport = enrichment_transport
        self._transport_factory = transport_factory
        self._source_config = source_config
        self._plan_factory = plan_factory
        self._profile_doc = profile_doc
        self._profile = profile
        self._enrich = enrich or enrichment_module.enrich_run
        # The default is this module's own per-op driver (decision 6), not
        # `graph.run_pass`: the difference is transaction shape, not stage order.
        # An injected `score` is a test seam and owns its own transactions.
        self._score = score or self._graph_pass
        #: An injected scorer keeps the plain `(conn, run_uid=, profile_doc=)`
        #: signature; only the built-in driver is handed the fan-out callback.
        self._score_is_default = score is None
        self._legacy_runner = legacy_runner
        self._trigger = trigger
        self._enrichment_deadline_seconds = enrichment_deadline_seconds
        self._score_batch_size = score_batch_size
        #: Test seam. Called on the scoring worker thread after each graph op
        #: COMMITS, with (op name, events appended). Production leaves it None.
        self._on_score_op = on_score_op
        self._active: dict[str, _ActiveRun] = {}
        self._fanout = _Fanout()
        self._adapters_installed = False

    # -- connections ------------------------------------------------------- #
    def connect(self) -> sqlite3.Connection:
        """A fresh connection to the configured database.

        Resolved per call, never captured at import: `config.DB_PATH` is what the
        repo-wide test fence rewrites, and a cached path would walk straight past
        it.

        `busy_timeout` matches `SqliteWriter`'s, and for the same reason. The
        conflict matrix lets an `aggregators` run share the process with a `daily`
        one, so two writers can legitimately be on this file at once; without the
        timeout the loser of a lock race raises instantly instead of waiting the
        few milliseconds the other transaction needs.
        """
        conn = self._connect_factory() if self._connect_factory is not None else db_connect()
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _read(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        conn = self.connect()
        try:
            conn.row_factory = sqlite3.Row
            return fn(conn)
        finally:
            conn.close()

    # -- schema gate ------------------------------------------------------- #
    def _require_canonical_schema_sync(self) -> None:
        try:
            self._read(runstore.require_canonical_schema)
        except CanonicalSchemaUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 - any shape of "wrong database"
            raise CanonicalSchemaUnavailable(str(exc)) from exc

    async def require_canonical_schema(self) -> None:
        await asyncio.to_thread(self._require_canonical_schema_sync)

    def has_canonical_schema(self) -> bool:
        try:
            self._require_canonical_schema_sync()
        except CanonicalSchemaUnavailable:
            return False
        return True

    # -- planning inputs --------------------------------------------------- #
    def source_config(self) -> SourceConfig:
        if self._source_config is not None:
            return self._source_config
        path = app_config.ROOT / "config.json"
        try:
            with open(path) as handle:
                doc = json.load(handle)
        except (OSError, ValueError):
            return SourceConfig()
        if not isinstance(doc, Mapping):
            return SourceConfig()
        return SourceConfig.from_mapping(doc)

    def profile_doc(self) -> Mapping[str, Any]:
        """The parsed `profile.json` document the graph pass scores against."""
        if self._profile_doc is None:
            with open(app_config.ROOT / "profile.json") as handle:
                self._profile_doc = json.load(handle)
        return self._profile_doc

    def profile(self) -> Any:
        """The compiled profile enrichment's prefilter reads."""
        if self._profile is None:
            import candidate_profile  # noqa: PLC0415 - repo-root module, path set by sources.scoring

            self._profile = candidate_profile.build_profile(self.profile_doc())
        return self._profile

    def _plan_for(self, run_kind: RunKind, source_config: SourceConfig):
        if self._plan_factory is not None:
            return self._plan_factory(run_kind, source_config)
        if not self._adapters_installed:
            from .sources.adapters import install  # noqa: PLC0415 - registry side effect

            install()
            self._adapters_installed = True
        return None  # let the scheduler plan from the registry

    # -- conflict matrix --------------------------------------------------- #
    def active_runs(self) -> dict[str, str]:
        """`run_uid -> kind` for every run this process is currently executing."""
        return {uid: record.kind for uid, record in self._active.items()}

    def is_active(self, run_uid: str) -> bool:
        record = self._active.get(run_uid)
        return record is not None and not record.settled

    def _legacy_running(self) -> bool:
        runner = self._legacy_runner
        if runner is None:
            from .sweeprunner import runner as module_runner  # noqa: PLC0415 - avoid import cycle

            runner = module_runner
        return bool(getattr(runner, "running", False))

    def _check_conflicts(self, kind: str) -> None:
        if self._legacy_running():
            raise RunConflict(
                "a legacy sweep is running; canonical runs are refused until it finishes"
            )
        group = _EXCLUSION_GROUPS[kind]
        for record in self._active.values():
            if record.settled:
                continue
            if _EXCLUSION_GROUPS.get(record.kind) == group:
                raise RunConflict(
                    f"run {record.run_uid} ({record.kind}) is already active in this lane"
                )

    @staticmethod
    def parse_kind(kind: Any) -> RunKind:
        if not isinstance(kind, str):
            raise UnknownRunKind(f"unknown run kind {kind!r}")
        if kind in DEFERRED_KINDS:
            raise UnsupportedRunKind(
                f"run kind {kind!r} is not wired yet (wave-3 scope); see plans/phase4-spec.md"
            )
        try:
            return RunKind(kind)
        except ValueError:
            raise UnknownRunKind(f"unknown run kind {kind!r}") from None

    # -- lifecycle --------------------------------------------------------- #
    async def start_run(self, kind: Any, *, trigger: str | None = None) -> dict[str, Any]:
        """Begin a canonical run and return before any work happens.

        Order of refusals is deliberate: a nonsense kind is a client bug (400/501)
        regardless of database or lane; a database without canonical schema makes
        the whole feature unavailable (503); a lane conflict is the only one that
        depends on what else is happening right now (409).
        """
        run_kind = self.parse_kind(kind)
        if str(run_kind) not in SUPPORTED_KINDS:  # pragma: no cover - parse_kind covers it
            raise UnsupportedRunKind(f"run kind {kind!r} is not supported")
        await self.require_canonical_schema()
        self._check_conflicts(str(run_kind))

        source_config = self.source_config()
        plan = self._plan_for(run_kind, source_config)
        effective_trigger = trigger or self._trigger
        scheduler = Scheduler(
            self.connect,
            config=self._scheduler_config,
            transport=self._scheduler_transport,
            event_hook=self._on_committed_events,
        )
        handle = scheduler.start(
            kind=run_kind,
            config=source_config,
            plan=plan,
            trigger=effective_trigger,
        )
        record = _ActiveRun(
            run_uid=handle.run_uid,
            kind=str(run_kind),
            handle=handle,
            trigger=effective_trigger,
            requested_at=runstore.utc_now_iso(),
        )
        self._active[handle.run_uid] = record
        record.supervisor = asyncio.create_task(
            self._supervise(record), name=f"runservice-{handle.run_uid}"
        )
        return {"run_uid": handle.run_uid, "kind": str(run_kind), "status": "running"}

    #: The run kinds a single-source retry is tried against, in order (4.4).
    #: `adapter.plan()` does not vary by kind -- only which sources are ELIGIBLE
    #: for a kind does (`SourceDescriptor.runs_in`) -- so `full-direct` and
    #: `daily` would resolve to the identical target list for a
    #: DIRECT/STARTUP_BOARD source anyway (every such adapter declares
    #: `run_kinds={DAILY, FULL_DIRECT}`). `full-direct` is tried first, and
    #: MUST be -- not `daily` -- because `daily` is the only `RunProfile` with
    #: `dueness_filtered=True` (`run_profiles.RUN_PROFILES`): a retry of a
    #: source that already succeeded today would resolve to `daily`, get
    #: silently dropped by `filter_due` before it ever reaches the scheduler
    #: (no `source_runs` row, no evidence, nothing to await), and the caller's
    #: 202 would settle "succeeded" against a run that did nothing (wave-2
    #: review finding 1). `full-direct` shares the exact same "direct"
    #: exclusion lane as `daily` (`_EXCLUSION_GROUPS`) and the same
    #: `STAGED_KINDS` membership (enrichment + scoring still run), so nothing
    #: about the retry's conflict behaviour or post-fetch stages changes --
    #: only dueness-filtering is avoided, which is the whole point of an
    #: explicit user-requested retry. AGGREGATOR sources (`jobspy`) declare
    #: membership in `{AGGREGATORS}` only, so they are found on the second
    #: try. MANUAL is deliberately absent: it declares only `MANUAL_IMPORT`, a
    #: wave-3-deferred kind (`DEFERRED_KINDS`), so a manual source can never
    #: appear in either candidate plan and correctly reads as "unknown source"
    #: to a retry request rather than raising `UnsupportedRunKind` for a kind
    #: nobody asked for.
    _RETRY_CANDIDATE_KINDS: tuple[RunKind, ...] = (RunKind.FULL_DIRECT, RunKind.AGGREGATORS)

    def _full_plan_for(
        self, run_kind: RunKind, source_config: SourceConfig
    ) -> Sequence[tuple[Any, Any]]:
        """Every `(adapter, target)` pair `run_kind` would plan, resolved eagerly.

        `_plan_for` answers `None` in production ("let the scheduler plan from
        the registry"), because `start_run` hands the scheduler a bare
        `run_kind` and lets it call `registry.plan_run` internally. A
        single-source retry cannot use that shortcut: it needs the concrete
        list *before* the scheduler starts, so it can filter it down to the one
        target the caller asked for. So this reuses `_plan_for` for the test
        seam (an injected `plan_factory`, exactly what `start_run` gets) and
        falls back to `registry.plan_run` itself -- not to the scheduler --
        when there is none.
        """
        plan = self._plan_for(run_kind, source_config)
        if plan is not None:
            return plan
        return registry_module.plan_run(source_config, run_kind)

    def _resolve_retry_target(
        self, source: str, source_config: SourceConfig
    ) -> tuple[RunKind, list[tuple[Any, Any]]] | None:
        """`(kind, [(adapter, target)])` for the one target named `source`.

        Tries each candidate kind's full plan in turn and keeps the first that
        contains a target whose `source_run_key` equals `source` exactly --
        this IS the category -> kind derivation (see `_RETRY_CANDIDATE_KINDS`),
        done by asking the registry what it would actually plan rather than by
        guessing a category from the source string. `None` means no candidate
        plan contains this source at all, which reads as "unknown source"
        whether that is a typo, a retired source, or one only reachable through
        a deferred kind (manual import).
        """
        for candidate in self._RETRY_CANDIDATE_KINDS:
            plan = self._full_plan_for(candidate, source_config)
            matched = [
                (adapter, target) for adapter, target in plan if target.source_run_key == source
            ]
            if matched:
                return candidate, matched
        return None

    async def retry_source(self, source: str, *, trigger: str | None = None) -> dict[str, Any]:
        """Begin a single-source run for exactly one source instance (4.4).

        Same shape as `start_run` (schema gate, then lane conflict, then a
        supervised run via the same `_supervise` machinery), except the plan
        handed to the scheduler is the single-target slice
        `_resolve_retry_target` found rather than the registry's full plan for
        the kind. Post-fetch stages therefore follow the resolved kind's own
        policy for free: a DIRECT/STARTUP_BOARD source resolves to
        `full-direct` (a `STAGED_KINDS` member, same lane as `daily`, but --
        unlike `daily` -- never dueness-filtered, so a retry of a source that
        already succeeded today still runs instead of being silently skipped)
        and gets enrichment + scoring exactly as a full `daily` run would; an
        AGGREGATOR source resolves to `aggregators` and settles at fetch, same
        as any other `aggregators` run.

        Refusal order mirrors `start_run`: schema availability (503) is
        checked first, before the source is even resolved -- resolving is moot
        on a database the feature is unavailable on. Then "is this source
        real" (404). Then the lane conflict the resolved kind belongs to
        (409), which can only be decided once the kind is known.
        """
        await self.require_canonical_schema()
        source_config = self.source_config()
        resolved = await asyncio.to_thread(self._resolve_retry_target, source, source_config)
        if resolved is None:
            raise UnknownSource(f"no source {source!r}")
        run_kind, plan = resolved
        self._check_conflicts(str(run_kind))

        effective_trigger = trigger or "manual-retry"
        scheduler = Scheduler(
            self.connect,
            config=self._scheduler_config,
            transport=self._scheduler_transport,
            event_hook=self._on_committed_events,
        )
        handle = scheduler.start(
            kind=run_kind,
            config=source_config,
            plan=plan,
            trigger=effective_trigger,
        )
        record = _ActiveRun(
            run_uid=handle.run_uid,
            kind=str(run_kind),
            handle=handle,
            trigger=effective_trigger,
            requested_at=runstore.utc_now_iso(),
        )
        self._active[handle.run_uid] = record
        record.supervisor = asyncio.create_task(
            self._supervise(record), name=f"runservice-{handle.run_uid}"
        )
        return {
            "run_uid": handle.run_uid,
            "source": source,
            "kind": str(run_kind),
            "status": "running",
        }

    def _deliver_cancel(self, record: _ActiveRun) -> None:
        """Set the flag, stop the fetch, stop the stage that is running now.

        The flag is the durable half: the fetch's own cancel only reaches target
        tasks, and by the time a cancel arrives the fetch may already be over. The
        supervisor reads the flag at every stage boundary and the graph driver
        reads it between ops, so a cancel that lands after the handle is done
        still stops everything that has not run yet.
        """
        record.cancel_requested = True
        record.handle.cancel()
        task = record.stage_task
        if task is not None and not task.done():
            task.cancel()

    async def cancel_run(self, run_uid: str) -> None:
        """Deliver a cancel to a live run.

        Raises `UnknownRun` when nothing knows the id and `RunConflict` when the
        run exists but this process cannot cancel it (already settled here, or
        owned by a process that is gone). A live record is answered without
        touching the database at all, which is also why the schema gate below sits
        after it: a live run cannot exist on a database that has no canonical
        schema, so only the lookup path needs gating (503 rather than a 500 from
        `no such table: pipeline_runs`).
        """
        record = self._active.get(run_uid)
        if record is not None and not record.settled:
            self._deliver_cancel(record)
            return
        await self.require_canonical_schema()
        row = await asyncio.to_thread(self._run_row, run_uid)
        if row is None:
            raise UnknownRun(f"no run {run_uid!r}")
        status = row.get("status")
        if status in _ACTIVE_STATUSES:
            raise RunConflict(
                f"run {run_uid!r} is not owned by this process; it will be reconciled at startup"
            )
        raise RunConflict(f"run {run_uid!r} already finished with status {status!r}")

    async def wait(self, run_uid: str) -> None:
        """Await a run's supervisor, stages included. Test and shutdown seam."""
        record = self._active.get(run_uid)
        if record is None or record.supervisor is None:
            return
        await asyncio.wait([record.supervisor])

    async def aclose(self, timeout: float = 5.0) -> None:
        """Best-effort clean stop for every run this process owns.

        A cancelled run still writes its own terminal rows (that is the whole
        point of `RunHandle.cancel`), so shutting down this way leaves complete
        evidence instead of the `running` rows startup recovery would otherwise
        have to reconcile.
        """
        records = [r for r in self._active.values() if not r.settled]
        for record in records:
            try:
                self._deliver_cancel(record)
            except Exception:  # noqa: BLE001 - teardown must not raise
                pass
        supervisors = [r.supervisor for r in records if r.supervisor is not None]
        if not supervisors:
            return
        try:
            await asyncio.wait(supervisors, timeout=timeout)
        except Exception:  # noqa: BLE001 - teardown must not raise
            pass

    # -- the supervisor ---------------------------------------------------- #
    async def _supervise(self, record: _ActiveRun) -> None:
        """Await the fetch, then run the post-fetch stages, then settle.

        `await record.handle` is the writer-closed barrier: the scheduler's run
        task closes its writer in a `finally` before the task completes, so this
        line returning is proof that no other writer is on the database. See
        decision 1.
        """
        stages: dict[str, Any] = {}
        try:
            try:
                result = await record.handle
                fetch_status = result.status
                fetch_error = dict(result.error) if result.error else None
            except asyncio.CancelledError:
                record.fetch_status = "cancelled"
                raise
            except BaseException as exc:  # noqa: BLE001 - recorded as evidence
                fetch_status = "failed"
                fetch_error = {"type": type(exc).__name__, "message": str(exc)}
            record.fetch_status = fetch_status

            licensed = (
                record.kind in STAGED_KINDS
                and fetch_status in STAGE_RUN_STATUSES
                and not record.cancel_requested
            )
            if licensed:
                stages["enrichment"] = await self._enrichment_stage(record)
                # Read again: the cancel may have arrived DURING enrichment, and
                # a stage never runs on a cancelled run (decision 3).
                if record.cancel_requested:
                    stages["scoring"] = await self._stage_not_started_because_cancelled(
                        record, EVENT_SCORING_CANCELLED
                    )
                else:
                    stages["scoring"] = await self._scoring_stage(record)
            else:
                reason = (
                    f"run kind {record.kind!r} has no post-fetch stages"
                    if record.kind not in STAGED_KINDS
                    else "the run was cancelled"
                    if record.cancel_requested
                    else f"fetch status {fetch_status!r} does not licence post-fetch stages"
                )
                skipped = {"status": "skipped", "reason": reason}
                stages = {"enrichment": dict(skipped), "scoring": dict(skipped)}
                await self._append_event(
                    record.run_uid,
                    EVENT_STAGES_SKIPPED,
                    {"reason": reason, "kind": record.kind, "fetch_status": fetch_status},
                )

            record.stages = stages
            failures = sorted(
                name for name, entry in stages.items() if entry.get("status") == "failed"
            )
            cancelled = sorted(
                name for name, entry in stages.items() if entry.get("status") == "cancelled"
            )
            # A cancelled run can never read as "succeeded": the API answered 202
            # to a cancel, and stages did not all run.
            outcome = (
                "cancelled"
                if record.cancel_requested or fetch_status == "cancelled"
                else "degraded"
                if failures
                else fetch_status
            )
            await self._append_event(
                record.run_uid,
                EVENT_RUN_SETTLED,
                {
                    "run_uid": record.run_uid,
                    "kind": record.kind,
                    "fetch_status": fetch_status,
                    "fetch_error": fetch_error,
                    "stages": stages,
                    "stage_failures": failures,
                    "stages_cancelled": cancelled,
                    # `degraded` says "the fetch was fine and something after it
                    # was not", which neither the fetch status nor a bare failure
                    # list says on its own.
                    "outcome": outcome,
                },
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - the service never dies of one run
            print(
                f"[runservice] run {record.run_uid} supervision failed: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
        finally:
            # Settled BEFORE the doorbell, and the terminal event is already
            # committed by now: an SSE reader that observes "not active" does one
            # more read and is guaranteed to see everything.
            record.settled = True
            self._active.pop(record.run_uid, None)
            self._fanout.publish(record.run_uid)

    # -- stages ------------------------------------------------------------ #
    async def _stage_not_started_because_cancelled(
        self, record: _ActiveRun, event_type: str
    ) -> dict[str, Any]:
        """Record a stage that never started because the run was cancelled."""
        reason = "the run was cancelled before this stage started"
        await self._append_event(
            record.run_uid, event_type, {"reason": reason, "started": False}
        )
        return {"status": "cancelled", "reason": reason}

    async def _enrichment_stage(self, record: _ActiveRun) -> dict[str, Any]:
        """Enrichment, in a task a cancel can actually stop.

        Cancelling the task is the whole cancellation mechanism here: enrichment
        takes a deadline but no cancel token, and it commits one `descriptions`
        row at a time, so a pass that dies mid-flight leaves committed rows and
        leaves the rest eligible for the next run. That makes "stop now" both
        possible and harmless.
        """
        await self._append_event(
            record.run_uid, EVENT_ENRICHMENT_STARTED, {"run_uid": record.run_uid}
        )
        task = asyncio.create_task(
            self._enrichment_pass(record), name=f"runservice-enrich-{record.run_uid}"
        )
        record.stage_task = task
        try:
            return await task
        except asyncio.CancelledError:
            # Ours (the stage task was cancelled by `_deliver_cancel`) or the
            # supervisor's own? `cancelling()` is what tells them apart, and a
            # cancellation aimed at the supervisor must keep propagating.
            current = asyncio.current_task()
            if not record.cancel_requested or (current is not None and current.cancelling()):
                raise
            reason = "the run was cancelled during enrichment"
            await self._append_event(
                record.run_uid,
                EVENT_ENRICHMENT_CANCELLED,
                {"reason": reason, "started": True},
            )
            return {"status": "cancelled", "reason": reason}
        finally:
            record.stage_task = None

    async def _enrichment_pass(self, record: _ActiveRun) -> dict[str, Any]:
        transport, owns_transport = self._enrichment_transport_for_run()
        conn = self.connect()
        try:
            report = await self._enrich(
                conn,
                record.run_uid,
                transport=transport,
                profile=self.profile(),
                deadline_seconds=self._enrichment_deadline_seconds,
            )
            payload = _enrichment_payload(report)
            await self._append_event(record.run_uid, EVENT_ENRICHMENT_FINISHED, payload)
            return {"status": "ok", "report": payload}
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - a stage failure is evidence
            error = {"type": type(exc).__name__, "message": str(exc)}
            await self._append_event(record.run_uid, EVENT_ENRICHMENT_FAILED, {"error": error})
            return {"status": "failed", "error": error}
        finally:
            conn.close()
            if owns_transport:
                await _aclose_transport(transport)

    async def _scoring_stage(self, record: _ActiveRun) -> dict[str, Any]:
        """The graph pass, on a worker thread, stopped cooperatively.

        A thread cannot be cancelled, so this stage deliberately does NOT register
        a `stage_task`: the driver reads `cancel_requested` between graph ops
        instead, which stops the pass at a committed boundary rather than in the
        middle of one.
        """
        await self._append_event(
            record.run_uid, EVENT_SCORING_STARTED, {"run_uid": record.run_uid}
        )
        loop = asyncio.get_running_loop()

        def notify() -> None:
            # From the worker thread: the fan-out's queues belong to the loop.
            loop.call_soon_threadsafe(self._fanout.publish, record.run_uid)

        try:
            report = await asyncio.to_thread(self._score_sync, record.run_uid, notify)
            await self._append_event(record.run_uid, EVENT_SCORING_FINISHED, report)
            return {"status": "ok", "report": report}
        except asyncio.CancelledError:
            raise
        except _StageCancelled:
            reason = "the run was cancelled during scoring"
            await self._append_event(
                record.run_uid, EVENT_SCORING_CANCELLED, {"reason": reason, "started": True}
            )
            return {"status": "cancelled", "reason": reason}
        except BaseException as exc:  # noqa: BLE001 - a stage failure is evidence
            error = {"type": type(exc).__name__, "message": str(exc)}
            await self._append_event(record.run_uid, EVENT_SCORING_FAILED, {"error": error})
            return {"status": "failed", "error": error}

    def _score_sync(
        self, run_uid: str, notify: Callable[[], None] | None = None
    ) -> dict[str, Any]:
        """The scoring pass on its own connection, on a worker thread.

        The graph layer does no transaction control by design. The default driver
        (`_graph_pass`) commits per op; the `commit`/`rollback` here is what an
        INJECTED `score` gets, and what closes out whatever op was in flight when
        one raised.
        """
        conn = self.connect()
        try:
            conn.row_factory = sqlite3.Row
            extra = {"notify": notify} if self._score_is_default else {}
            try:
                report = self._score(
                    conn, run_uid=run_uid, profile_doc=self.profile_doc(), **extra
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            return json.loads(runstore.canonical_json(report))
        finally:
            conn.close()

    # -- the graph pass, one transaction per op (decision 6) ---------------- #
    def _cancel_requested(self, run_uid: str) -> bool:
        record = self._active.get(run_uid)
        return record is not None and record.cancel_requested

    def _commit_op(
        self, conn: sqlite3.Connection, op: Any, *, notify: Callable[[], None] | None
    ) -> Any:
        """Apply one `writer.WriteOp` and its events in one short transaction.

        Deliberately the same shape as `writer._commit_once`, minus the batching:
        `apply` first, then that op's `events` (which only exist after `apply` has
        run -- the ops publish them by assignment), then one commit. Evidence and
        write are therefore atomic per op, and nothing else on this database waits
        longer than one op for the write lock.
        """
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = op.apply(conn)
            rows: list[dict[str, Any]] = []
            sequences: dict[str, int] = {}
            for event in getattr(op, "events", ()):
                sequence = sequences.get(event.run_uid)
                if sequence is None:
                    sequence = runstore.next_event_sequence(conn, event.run_uid)
                sequences[event.run_uid] = sequence + 1
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
            runstore.append_run_events(conn, rows)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        if rows and notify is not None:
            notify()  # after the commit, never before -- the readers re-read rows
        if self._on_score_op is not None:
            self._on_score_op(type(op).__name__, len(rows))
        return result

    def _graph_pass(
        self,
        conn: sqlite3.Connection,
        *,
        run_uid: str,
        profile_doc: Any,
        notify: Callable[[], None] | None = None,
        at: str | None = None,
    ) -> dict[str, Any]:
        """`graph.run_pass`'s stage sequence, with a commit after every op.

        The orchestration is replicated rather than reimplemented: the ops are the
        graph's own, in the graph's own order, and `graph.py` is untouched. What
        differs is only where the transaction boundaries fall -- see decision 6
        for why one big transaction is the wrong shape here and why chopping it up
        is safe.
        """
        stamp = at or runstore.utc_now_iso()
        identity = scoring_module.scorer_identity()
        batch_size = self._score_batch_size or graph_module.DEFAULT_BATCH_SIZE

        def stop_if_cancelled() -> None:
            if self._cancel_requested(run_uid):
                raise _StageCancelled(run_uid)

        opener = graph_module.OpenPass(
            run_uid=run_uid, at=stamp, profile_doc=profile_doc, scorer=identity
        )
        context = self._commit_op(conn, opener, notify=notify)

        stop_if_cancelled()
        bridge_report = self._commit_op(
            conn, resolver_module.BridgeLegacyUrls(run_uid=run_uid, at=stamp), notify=notify
        )

        stop_if_cancelled()
        resolve_report = self._commit_op(
            conn, graph_module.ResolvePass(run_uid=run_uid, at=stamp), notify=notify
        )

        totals = {"selected": 0, "scored": 0, "reused": 0, "recurrent": 0, "superseded": 0,
                  "blocked": 0, "skipped": 0}
        touched: list[str] = []
        cursor: str | None = None
        while True:
            stop_if_cancelled()
            page = graph_module.select_work(
                conn,
                run_uid=run_uid,
                mode=context.decision.mode,
                limit=batch_size,
                after=cursor,
            )
            if not page:
                break
            report = self._commit_op(
                conn,
                graph_module.ScoreGraphPass(
                    run_uid=run_uid, at=stamp, context=context, posting_ids=tuple(page)
                ),
                notify=notify,
            )
            for key in totals:
                totals[key] += int(report[key])
            touched.extend(report["posting_ids"])
            cursor = page[-1]

        summary = {
            "run_uid": run_uid,
            "pass_id": context.pass_id,
            "mode": str(context.decision.mode),
            "reasons": list(context.decision.reasons),
            "profile_version_id": context.identity.profile_version_id,
            "scorer_hash": context.scorer.scorer_hash,
            "rubric_version": context.scorer.rubric_version,
            "bridge": {k: v for k, v in bridge_report.items() if k != "invalidated"},
            "resolved": len(resolve_report["matched"]),
            "resolve_ambiguous": int(resolve_report["ambiguous"]),
            **totals,
        }
        stop_if_cancelled()
        return self._commit_op(
            conn,
            graph_module.ClosePass(
                run_uid=run_uid,
                at=stamp,
                context=context,
                report=summary,
                selected_posting_ids=tuple(dict.fromkeys(touched)),
            ),
            notify=notify,
        )

    def _enrichment_transport_for_run(self) -> tuple[Any, bool]:
        """Returns `(transport, this_call_owns_it)`.

        An injected transport belongs to whoever injected it; one built here is
        closed here.
        """
        if self._enrichment_transport is not None:
            return self._enrichment_transport, False
        if self._transport_factory is not None:
            return self._transport_factory(), True
        from .sources.transport import HttpxTransport  # noqa: PLC0415 - httpx is imported lazily

        return HttpxTransport(), True

    # -- events ------------------------------------------------------------ #
    def _on_committed_events(self, events: Sequence[RunEvent]) -> None:
        """Post-commit hook. Rings the doorbell once per run touched.

        Called on the event loop by the writer task, after its transaction
        committed -- so a woken reader is guaranteed to SEE the rows it is being
        told about.
        """
        for run_uid in dict.fromkeys(event.run_uid for event in events):
            self._fanout.publish(run_uid)

    async def _append_event(
        self,
        run_uid: str,
        event_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        source_run_id: str | None = None,
    ) -> int | None:
        sequence = await asyncio.to_thread(
            self._append_event_sync, run_uid, event_type, payload, source_run_id
        )
        if sequence is not None:
            self._fanout.publish(run_uid)
        return sequence

    def _append_event_sync(
        self,
        run_uid: str,
        event_type: str,
        payload: Mapping[str, Any] | None,
        source_run_id: str | None,
    ) -> int | None:
        conn = self.connect()
        try:
            conn.row_factory = sqlite3.Row
            # `run_events.run_uid` is an FK onto `pipeline_runs`. A run that died
            # in preflight has no row there, and inserting would fail rather than
            # record anything: no row means no evidence to attach, so say so by
            # returning None instead of raising into the supervisor.
            row = conn.execute(
                "SELECT 1 FROM pipeline_runs WHERE run_uid=?", (run_uid,)
            ).fetchone()
            if row is None:
                return None
            sequence = runstore.next_event_sequence(conn, run_uid)
            runstore.append_run_events(
                conn,
                [
                    {
                        "run_uid": run_uid,
                        "source_run_id": source_run_id,
                        "sequence": sequence,
                        "event_type": event_type,
                        "at": runstore.utc_now_iso(),
                        "payload": dict(payload) if payload is not None else None,
                    }
                ],
            )
            conn.commit()
            return sequence
        except sqlite3.Error as exc:  # noqa: BLE001 - evidence, never a crash
            print(
                f"[runservice] could not append {event_type} for {run_uid}: {exc}",
                file=sys.stderr,
            )
            return None
        finally:
            conn.close()

    # -- subscriptions ----------------------------------------------------- #
    def subscribe(self, run_uid: str) -> asyncio.Queue:
        return self._fanout.subscribe(run_uid)

    def unsubscribe(self, run_uid: str, queue: asyncio.Queue) -> None:
        self._fanout.unsubscribe(run_uid, queue)

    def subscriber_count(self, run_uid: str) -> int:
        return self._fanout.subscriber_count(run_uid)

    # -- reads ------------------------------------------------------------- #
    def events_after_sync(
        self, run_uid: str, after: int, *, limit: int = EVENT_PAGE_SIZE
    ) -> list[dict[str, Any]]:
        def _query(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute(
                "SELECT sequence, event_type, at, source_run_id, payload_json "
                "FROM run_events WHERE run_uid=? AND sequence>? "
                "ORDER BY sequence LIMIT ?",
                (run_uid, int(after), int(limit)),
            ).fetchall()
            return [
                {
                    "sequence": int(row["sequence"]),
                    "event_type": row["event_type"],
                    "at": row["at"],
                    "source_run_id": row["source_run_id"],
                    "payload": _load_json(row["payload_json"]),
                }
                for row in rows
            ]

        return self._read(_query)

    async def events_after(
        self, run_uid: str, after: int, *, limit: int = EVENT_PAGE_SIZE
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.events_after_sync, run_uid, after, limit=limit)

    def _run_row(self, run_uid: str) -> dict[str, Any] | None:
        def _query(conn: sqlite3.Connection):
            return conn.execute(
                "SELECT * FROM pipeline_runs WHERE run_uid=?", (run_uid,)
            ).fetchone()

        row = self._read(_query)
        return dict(row) if row is not None else None

    async def run_exists(self, run_uid: str) -> bool:
        """Is this a run at all? Live handle first, then `pipeline_runs`.

        The live check is not an optimisation: `start_run` returns as soon as the
        run task is created, and that task's `StartRun` op reaches the database a
        moment later, so a UI that opens the event stream immediately after a 202
        would otherwise be told 404 about a run it just started.
        """
        if self.is_active(run_uid):
            return True
        return (await asyncio.to_thread(self._run_row, run_uid)) is not None

    def list_runs_sync(self, limit: int = 20) -> list[dict[str, Any]]:
        """Recent runs, newest first -- persisted rows overlaid with any run this
        process has already started but whose `StartRun` op has not reached
        `pipeline_runs` yet.

        That gap is the same race `run_exists` guards against (see its
        docstring): `start_run`/`retry_source` return as soon as the run TASK
        is created, not once its first op has been written by the scheduler's
        writer, which drains on its own queue a moment later. Without this
        merge, a client that calls `POST /api/runs` (or a retry) and
        immediately polls `GET /api/runs` can get back a list with no trace of
        the run it just started -- the 202 beats the row -- and a UI built to
        mount a panel per listed run never mounts one (wave-2 review finding
        3). `run_exists`/the SSE endpoint already tolerate this window per-run;
        this is the same tolerance applied to the list.

        The synthetic row for a not-yet-persisted run carries only what the
        in-memory `_ActiveRun` record actually knows -- `run_uid`, `kind`,
        `status` (always `"running"`: a record this loop reaches is by
        definition not `settled`), `trigger`, `requested_at` (this process's
        clock at dispatch time, not the writer's) -- everything else
        (`started_at`, `finished_at`, `kept_count`, `new_count`, `error`) is
        `None` because the writer has not produced it yet. Once the real row
        lands, the next call reads it from `pipeline_runs` instead and this
        overlay no longer applies to that run.
        """

        def _query(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute(
                "SELECT run_uid, kind, status, trigger, requested_at, started_at, "
                "finished_at, kept_count, new_count, error_json FROM pipeline_runs "
                "ORDER BY COALESCE(requested_at, started_at, '') DESC, rowid DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            persisted = [
                {
                    "run_uid": row["run_uid"],
                    "kind": row["kind"],
                    "status": row["status"],
                    "trigger": row["trigger"],
                    "requested_at": row["requested_at"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "kept_count": row["kept_count"],
                    "new_count": row["new_count"],
                    "error": _load_json(row["error_json"]),
                    "active": self.is_active(row["run_uid"]),
                }
                for row in rows
            ]
            seen = {r["run_uid"] for r in persisted}
            pending = [
                {
                    "run_uid": record.run_uid,
                    "kind": record.kind,
                    "status": "running",
                    "trigger": record.trigger,
                    "requested_at": record.requested_at,
                    "started_at": None,
                    "finished_at": None,
                    "kept_count": None,
                    "new_count": None,
                    "error": None,
                    "active": True,
                }
                for record in self._active.values()
                if not record.settled and record.run_uid not in seen
            ]
            pending.sort(key=lambda r: r["requested_at"] or "", reverse=True)
            return (pending + persisted)[: int(limit)]

        return self._read(_query)

    async def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.list_runs_sync, limit)

    def run_detail_sync(self, run_uid: str) -> dict[str, Any] | None:
        def _query(conn: sqlite3.Connection) -> dict[str, Any] | None:
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE run_uid=?", (run_uid,)
            ).fetchone()
            if row is None:
                return None
            run = dict(row)
            detail: dict[str, Any] = {
                "run_uid": run["run_uid"],
                "kind": run["kind"],
                "status": run["status"],
                "trigger": run.get("trigger"),
                "requested_at": run.get("requested_at"),
                "started_at": run.get("started_at"),
                "finished_at": run.get("finished_at"),
                "kept_count": run.get("kept_count"),
                "new_count": run.get("new_count"),
                "config_hash": run.get("config_hash"),
                "code_hash": run.get("code_hash"),
                "scorer_hash": run.get("scorer_hash"),
                "profile_version_id": run.get("profile_version_id"),
                "report": _load_json(run.get("aggregate_report_json")),
                "error": _load_json(run.get("error_json")),
                "active": self.is_active(run_uid),
            }
            detail["source_runs"] = [
                {
                    "source_run_id": r["source_run_id"],
                    "source": r["source"],
                    "step": r["step"],
                    "attempt": r["attempt"],
                    "status": r["status"],
                    "requested_at": r["requested_at"],
                    "started_at": r["started_at"],
                    "finished_at": r["finished_at"],
                    "deadline_at": r["deadline_at"],
                    "item_count": r["item_count"],
                    "fetched_count": r["fetched_count"],
                    "accepted_count": r["accepted_count"],
                    "changed_count": r["changed_count"],
                    "inventory_scope": _optional_column(r, "inventory_scope"),
                    "error": _load_json(r["error_json"]),
                }
                for r in conn.execute(
                    "SELECT * FROM source_runs WHERE run_uid=? "
                    "ORDER BY source, step, attempt",
                    (run_uid,),
                ).fetchall()
            ]
            stages, settled = _stage_reports(conn, run_uid)
            detail["stages"] = stages
            detail["settled"] = settled
            detail["terminal"] = run["status"] not in _ACTIVE_STATUSES
            detail["change_summary"] = None
            if detail["terminal"]:
                try:
                    detail["change_summary"] = runstore.change_summary(conn, run_uid)
                except (LookupError, sqlite3.Error):
                    detail["change_summary"] = None
            return detail

        return self._read(_query)

    async def run_detail(self, run_uid: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self.run_detail_sync, run_uid)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _load_json(blob: Any) -> Any:
    if blob is None:
        return None
    try:
        return json.loads(blob)
    except (TypeError, ValueError):
        return blob


def _optional_column(row: sqlite3.Row, name: str) -> Any:
    return row[name] if name in row.keys() else None


def _enrichment_payload(report: Any) -> dict[str, Any]:
    """`EnrichmentReport` -> a JSON object.

    Field by field rather than `dataclasses.asdict`: two of the fields are
    `MappingProxyType`, which `asdict`'s deep copy cannot handle.
    """
    return {
        "run_uid": report.run_uid,
        "considered": report.considered,
        "already_described": report.already_described,
        "skipped_by_reason": dict(report.skipped_by_reason),
        "fetched": report.fetched,
        "available": report.available,
        "empty": report.empty,
        "failed": report.failed,
        "rows_written": report.rows_written,
        "peak_concurrency": report.peak_concurrency,
        "peak_by_host": dict(report.peak_by_host),
    }


async def _aclose_transport(transport: Any) -> None:
    closer = getattr(transport, "aclose", None)
    if closer is None:
        return
    try:
        result = closer()
        if asyncio.iscoroutine(result):
            await result
    except Exception:  # noqa: BLE001 - closing a transport must not fail a run
        pass


def _stage_reports(
    conn: sqlite3.Connection, run_uid: str
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Stage evidence, read back out of `run_events` (its only home)."""
    stages: dict[str, Any] = {}
    settled: dict[str, Any] | None = None
    rows = conn.execute(
        "SELECT sequence, event_type, at, payload_json FROM run_events "
        "WHERE run_uid=? AND (event_type LIKE 'stage.%' OR event_type LIKE 'service.%') "
        "ORDER BY sequence",
        (run_uid,),
    ).fetchall()
    for row in rows:
        event_type = row["event_type"]
        payload = _load_json(row["payload_json"])
        if event_type == EVENT_RUN_SETTLED:
            settled = payload
            continue
        if not event_type.startswith("stage."):
            continue
        parts = event_type.split(".")
        if len(parts) != 3:  # pragma: no cover - vocabulary is fixed above
            continue
        _, name, phase = parts
        entry = stages.setdefault(name, {})
        entry["phase"] = phase
        entry["at"] = row["at"]
        entry["sequence"] = int(row["sequence"])
        if phase == "finished":
            entry["report"] = payload
        elif phase == "failed":
            entry["error"] = (payload or {}).get("error", payload)
    return stages, settled


def recover_orphans_if_canonical(
    connect: Callable[[], sqlite3.Connection] | None = None,
) -> Any:
    """Startup recovery, but only on a database that has canonical tables.

    The live v4 database has none of them, so this returns None there and the app
    boots exactly as it did before. Callers still guard the call: a database that
    is canonical but locked must not take the server down.
    """
    factory = connect or db_connect
    conn = factory()
    try:
        conn.row_factory = sqlite3.Row
        runstore.require_canonical_schema(conn)
    except Exception:  # noqa: BLE001 - "not canonical" is the expected case
        return None
    finally:
        conn.close()
    return scheduler_module.recover_orphans(factory)


# --------------------------------------------------------------------------- #
# Process-wide default (what the API uses when nothing is injected)
# --------------------------------------------------------------------------- #
_DEFAULT_SERVICE: RunService | None = None


def default_service() -> RunService:
    global _DEFAULT_SERVICE
    if _DEFAULT_SERVICE is None:
        _DEFAULT_SERVICE = RunService()
    return _DEFAULT_SERVICE


def reset_default_service() -> None:
    """Drop the process-wide instance. Tests only."""
    global _DEFAULT_SERVICE
    _DEFAULT_SERVICE = None


async def shutdown_default_service(timeout: float = 5.0) -> None:
    if _DEFAULT_SERVICE is not None:
        await _DEFAULT_SERVICE.aclose(timeout=timeout)
