"""Fake adapters and database helpers shared by the scheduler test modules.

Contains no tests of its own; it is named `test_source_scheduler_fakes` so it
matches the same collection glob as the suites that import it.

Every fake is built on the real `contract` types, so a fake that violates an
invariant (sleeping for politeness, retrying internally, swallowing an error) is
as wrong here as it would be in `adapters/`. The scheduler under test cannot tell
these apart from Greenhouse.

No fake touches the network, and no test in either suite opens anything but a
`tmp_path` database.
"""
from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from backend.db import connect, init_db
from backend.sources.contract import (
    ExecutionMode,
    InventoryScope,
    PermanentSourceError,
    RunKind,
    SourceCategory,
    SourceConfig,
    SourceDescriptor,
    SourceTarget,
    TransientSourceError,
    TransportKind,
)

TEST_TIMEOUT = 15.0


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
def make_connect(tmp_path, name: str = "scheduler.db") -> Callable[[], sqlite3.Connection]:
    """Build a canonical database under tmp_path and return a connection factory.

    `init_db` is the only thing that ever creates schema in this codebase, and it
    is handed an explicit path here, so nothing in these suites can reach the real
    webapp/app.db.
    """
    path = tmp_path / name
    conn = connect(path)
    try:
        init_db(conn)
    finally:
        conn.close()

    def _connect() -> sqlite3.Connection:
        return connect(path)

    _connect.path = path  # type: ignore[attr-defined]
    return _connect


def read(connect_fn, sql: str, params: Sequence = ()) -> list[sqlite3.Row]:
    conn = connect_fn()
    try:
        return conn.execute(sql, tuple(params)).fetchall()
    finally:
        conn.close()


def scalar(connect_fn, sql: str, params: Sequence = ()):
    rows = read(connect_fn, sql, params)
    return rows[0][0] if rows else None


# --------------------------------------------------------------------------- #
# Concurrency probe
# --------------------------------------------------------------------------- #
@dataclass
class Probe:
    """Independent high-water-mark tracking, measured inside the adapter body.

    Deliberately not read from the scheduler's own counters: a bounded-concurrency
    test that trusts the component under test to report its own concurrency proves
    nothing.
    """

    inflight: int = 0
    peak: int = 0
    per_source: dict[str, int] = field(default_factory=dict)
    per_source_peak: dict[str, int] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)

    def enter(self, key: str, group: str) -> None:
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        self.per_source[group] = self.per_source.get(group, 0) + 1
        self.per_source_peak[group] = max(
            self.per_source_peak.get(group, 0), self.per_source[group]
        )
        self.order.append(key)

    def exit(self, group: str) -> None:
        self.inflight -= 1
        self.per_source[group] -= 1


# --------------------------------------------------------------------------- #
# The programmable adapter
# --------------------------------------------------------------------------- #
def descriptor_for(
    source_key: str,
    *,
    deadline: float = 5.0,
    per_host_concurrency: int = 4,
    max_concurrent_targets: int | None = None,
    supports_checkpoint: bool = False,
    execution: ExecutionMode = ExecutionMode.ASYNC_INPROCESS,
    inventory_scope: InventoryScope = InventoryScope.COMPLETE,
    min_request_interval_seconds: float = 0.0,
    transport: TransportKind = TransportKind.NONE,
    run_kinds: frozenset[RunKind] = frozenset({RunKind.FULL_DIRECT, RunKind.DAILY}),
    refresh_interval_seconds: int = 6 * 3600,
) -> SourceDescriptor:
    return SourceDescriptor(
        source_key=source_key,
        category=SourceCategory.DIRECT,
        run_kinds=run_kinds,
        refresh_interval_seconds=refresh_interval_seconds,
        default_deadline_seconds=deadline,
        supports_checkpoint=supports_checkpoint,
        execution=execution,
        transport=transport,
        max_concurrent_targets=max_concurrent_targets,
        per_host_concurrency=per_host_concurrency,
        min_request_interval_seconds=min_request_interval_seconds,
        default_inventory_scope=inventory_scope,
    )


class FakeAdapter:
    """An adapter whose `fetch` body is supplied by the test.

    `body` is an async generator function `(adapter, target, ctx) -> records`. It
    receives the adapter so a scenario can keep per-target attempt counters, which
    is how the "second attempt succeeds" cases are written without global state.
    """

    def __init__(
        self,
        source_key: str,
        *,
        instances: Sequence[str] = ("one",),
        body: Callable = None,
        descriptor: SourceDescriptor | None = None,
        host: str | None = None,
        inventory_scope: InventoryScope | None = None,
        probe: Probe | None = None,
    ) -> None:
        self.descriptor = descriptor or descriptor_for(source_key)
        self.source_key = source_key
        self.instances = tuple(instances)
        self.body = body
        self.host = host
        self.inventory_scope = inventory_scope or self.descriptor.default_inventory_scope
        self.probe = probe
        #: attempts observed per instance_key — the fakes' only mutable state.
        self.attempts: dict[str, int] = {}
        self.closed: list[str] = []

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        return [
            SourceTarget(
                source_key=self.source_key,
                instance_key=instance,
                params={"instance": instance},
                inventory_scope=self.inventory_scope,
                host=self.host,
            )
            for instance in self.instances
        ]

    def targets(self) -> list[SourceTarget]:
        return list(self.plan(SourceConfig()))

    async def fetch(self, target: SourceTarget, ctx):
        self.attempts[target.instance_key] = self.attempts.get(target.instance_key, 0) + 1
        if self.probe is not None:
            self.probe.enter(target.source_run_key, self.source_key)
        try:
            async for record in self.body(self, target, ctx):
                yield record
        finally:
            self.closed.append(target.source_run_key)
            if self.probe is not None:
                self.probe.exit(self.source_key)


def plan_of(*adapters: FakeAdapter) -> list[tuple[FakeAdapter, SourceTarget]]:
    return [(adapter, target) for adapter in adapters for target in adapter.targets()]


# --------------------------------------------------------------------------- #
# Record helper
# --------------------------------------------------------------------------- #
def posting(target: SourceTarget, n: int, *, company: str = "Acme", title: str | None = None):
    return target.record(
        title=title or f"Support Engineer {n}",
        company=company,
        url=f"https://{target.source_key}.example/{target.instance_key}/{n}",
        req_id=f"{n}",
        location="San Francisco, CA",
    )


# --------------------------------------------------------------------------- #
# Bodies
# --------------------------------------------------------------------------- #
def fast(count: int = 3, *, hold: float = 0.0):
    """Yields `count` records, optionally holding the slot for `hold` seconds."""

    async def _body(adapter, target, ctx):
        for n in range(count):
            yield posting(target, n)
        if hold:
            await asyncio.sleep(hold)

    return _body


def slow(count: int = 3, *, per_record: float = 0.02):
    async def _body(adapter, target, ctx):
        for n in range(count):
            await asyncio.sleep(per_record)
            yield posting(target, n)

    return _body


def hanging(*, before: int = 0):
    """Yields `before` records then never returns. Only a deadline stops it."""

    async def _body(adapter, target, ctx):
        for n in range(before):
            yield posting(target, n)
        await asyncio.sleep(3600)
        yield posting(target, 999)  # pragma: no cover - unreachable

    return _body


def raising(exc_factory: Callable[[], BaseException], *, after: int = 0):
    """Yields `after` records then raises. `after > 0` is the mid-stream case."""

    async def _body(adapter, target, ctx):
        for n in range(after):
            yield posting(target, n)
        raise exc_factory()

    return _body


def transient_then(count: int = 2, *, succeed_on_attempt: int = 2, before: int = 0):
    """Transient failure until `succeed_on_attempt`, then a clean stream."""

    async def _body(adapter, target, ctx):
        attempt = adapter.attempts[target.instance_key]
        if attempt < succeed_on_attempt:
            for n in range(before):
                yield posting(target, n)
            raise TransientSourceError(
                f"attempt {attempt} rate limited",
                source_key=target.source_key,
                instance_key=target.instance_key,
                status=429,
            )
        for n in range(count):
            yield posting(target, n)

    return _body


def permanent_always(*, before: int = 0):
    async def _body(adapter, target, ctx):
        for n in range(before):
            yield posting(target, n)
        raise PermanentSourceError(
            "board is gone",
            source_key=target.source_key,
            instance_key=target.instance_key,
            status=404,
        )

    return _body


def paged(*, pages: int = 3, per_page: int = 2, stop_after_page: int | None = None):
    """A checkpointing paginated source, the shape Workday/Amazon/iCIMS have.

    Resumes from `ctx.resume_from` when the scheduler supplies one, and marks a
    checkpoint after each page's records have been yielded — exactly the
    delivered-not-committed discipline `mark_checkpoint` documents.
    """

    async def _body(adapter, target, ctx):
        page = 0
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            page = int(ctx.resume_from.cursor.get("next_page", 0))
            emitted = ctx.resume_from.emitted
        while page < pages:
            for index in range(per_page):
                yield posting(target, page * per_page + index)
                emitted += 1
            page += 1
            ctx.mark_checkpoint({"next_page": page}, target=target, emitted=emitted)
            if stop_after_page is not None and page >= stop_after_page:
                raise TransientSourceError(
                    f"connection reset after page {page}",
                    source_key=target.source_key,
                    instance_key=target.instance_key,
                )

    return _body


def gated(*, before: int, gate: asyncio.Event, after: int = 1):
    """Yields `before` records, waits for the test to open the gate, then finishes.

    This is how "records are visible before the run completes" is asserted without
    a sleep: the test blocks the adapter until it has *observed* the committed rows
    from a separate connection.
    """

    async def _body(adapter, target, ctx):
        for n in range(before):
            yield posting(target, n)
        await gate.wait()
        for n in range(before, before + after):
            yield posting(target, n)

    return _body


def emitting(records: Sequence[dict]):
    """Yields exactly the records described, for identity/dedupe scenarios."""

    async def _body(adapter, target, ctx):
        for spec in records:
            yield target.record(**spec)

    return _body


def subprocess_like(child: dict, *, records: int = 2):
    """Stands in for the JobSpy SUBPROCESS adapter's cancellation contract.

    The adapter owns its child; the scheduler's only obligation is that its
    deadline cancel actually reaches the generator so this `finally` runs. The dict
    records whether it did.
    """

    async def _body(adapter, target, ctx):
        child["started"] = True
        try:
            for n in range(records):
                yield posting(target, n)
            await asyncio.sleep(3600)
        finally:
            child["terminated"] = True

    return _body
