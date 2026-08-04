"""Manual MCP import — the `ExecutionMode.PUSH` shape, no transport at all.

Supersedes the `mcp-dice` / `mcp-zip` arm of `scraper.py`: those rows were
never fetched by `scraper.py` itself, they were dropped into `results/raw.jsonl`
by an out-of-band MCP job-search tool call (Dice, ZipRecruiter, ...) using the
same `rec()` dict shape every other source used:

    {"title", "company", "location", "url", "source", "posted", "salary",
     "remote", "req_id"}, plus an optional `"_desc"` some rows carry.

This module is the reference PUSH adapter, and demonstrates what the reference
`ASYNC_INPROCESS` adapter (`greenhouse.py`) cannot:

  * There is no `Transport`. `SourceDescriptor.transport = TransportKind.NONE`
    means `ctx.http()` is never called (it would raise `ConfigError` if it
    were); `fetch()` instead reads `ctx.payloads`, the `InboundPayload`s the
    scheduler attached to this attempt from whatever the MCP importer handed
    it. See `contract.InboundPayload` — manual import is "a scraper whose
    transport already ran", not a special case of the contract.
  * `plan()` cannot enumerate origins the way `companies.<source_key>` lets
    Greenhouse enumerate boards: `config.json` has no `companies.manual` map,
    because there is nothing to plan against until rows actually arrive.
    `plan()` therefore returns exactly one fixed `SourceTarget` representing
    "the pushed batch" — the scheduler's one schedulable, one `source_runs`
    row, one deadline, one failure-isolation unit for whatever got pushed this
    run. This mirrors the `PushAdapter` test double in
    `tests/test_source_contract.py`.
  * A single push can (and in production does — `mcp-dice` and `mcp-zip` both
    land in the same `results/raw.jsonl`) mix rows from more than one MCP
    origin. The *target*'s `instance_key` cannot capture that, so identity
    namespacing happens per record instead: `parse_import_rows` reads each
    row's own `source` tag (`"mcp-dice"`, `"mcp-zip"`, ...) and stamps it as
    that record's `instance_key`, overriding the target's default. Two rows
    from different MCP tools therefore land in different namespaces
    (`manual:mcp-dice` vs `manual:mcp-zip`) even though they were pushed and
    scheduled together.
  * `InventoryScope.PARTIAL` unconditionally. Per the contract's own docstring:
    "an out-of-band drop" proves nothing about what it did not include, so a
    successful manual-import run must never license Phase 2.4 to mark anything
    absent. There is no COMPLETE variant of this source — every target this
    adapter ever plans is PARTIAL.

Preserved from the `rec()` shape: every field survives with the same meaning.
Not preserved: `scraper.py`'s cross-source `dedupe()` merge (`_alts`,
`salary_min`/`salary_max` derivation) — that is Phase 3 resolver work, not an
adapter concern. `_group` is dropped; it was `scraper.py`'s cache-file
bookkeeping, not posting data.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import Any

from ..contract import (
    ExecutionMode,
    FetchContext,
    InventoryScope,
    NormalizedPosting,
    PayloadError,
    RunKind,
    SourceCategory,
    SourceConfig,
    SourceDescriptor,
    SourceTarget,
    TransportKind,
    normalize_date,
)

__all__ = ["DESCRIPTOR", "ManualImportAdapter", "parse_import_rows"]

SOURCE_KEY = "manual"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.MANUAL,
    run_kinds=frozenset({RunKind.MANUAL_IMPORT}),
    # Not polled on a cadence at all: this source only ever runs when an MCP
    # import actually pushed rows, which the scheduler (Phase 2.5) triggers
    # out of band. 0 means "no minimum age gate" rather than "due every 6h
    # like a daily ATS board" — the DAILY/FULL_DIRECT due-check never
    # consults it because this descriptor is not in either run_kinds set.
    refresh_interval_seconds=0,
    # No network, just iterating whatever `ctx.payloads` already holds in
    # memory. Generous only because the batch's iteration itself is instant;
    # this is not a timeout tuned against an upstream host.
    default_deadline_seconds=10.0,
    # A push is delivered whole, once, by the importer. There is no cursor
    # into "the rest of the batch" to resume — the next run's payloads are a
    # brand new drop, not a continuation. Declaring True would be a lie the
    # scheduler would act on (same reasoning as Greenhouse's False).
    supports_checkpoint=False,
    execution=ExecutionMode.PUSH,
    transport=TransportKind.NONE,
    # Unused: TransportKind.NONE means no HttpRequest.host ever reaches a
    # limiter. Left at the type default rather than invented.
    per_host_concurrency=4,
    min_request_interval_seconds=0.0,
    # Only *some* rows carry `_desc` (an optional field this adapter maps to
    # `description`); declaring True here would tell Phase 3.2 to skip the
    # description fetch for every manual-import posting, including the ones
    # that have none.
    description_inline=False,
    default_inventory_scope=InventoryScope.PARTIAL,
)


def _row_date(row: Mapping[str, Any]) -> tuple[str | None, str]:
    """Pick the hashable date, and keep the raw string for provenance.

    Unlike Workday's `postedOn`, the MCP arm's `posted` field is sometimes
    already an absolute `YYYY-MM-DD` (Dice) and sometimes empty or relative
    (ZipRecruiter, or a tool that returned recency text). `normalize_date`
    already draws that line; this just keeps both forms per the contract's
    posted_date/posted_raw split.
    """
    raw = row.get("posted") or ""
    return normalize_date(raw), str(raw)


def parse_import_rows(
    payload: bytes | str | Sequence[Any], target: SourceTarget
) -> Iterator[NormalizedPosting]:
    """MCP import batch -> records. Pure: no I/O, no clock, no globals.

    The envelope is a JSON array of `rec()`-shaped row objects (bytes, text,
    or an already-decoded sequence all accepted, matching the reference
    parser). A malformed envelope — not JSON, or JSON that is not a list —
    raises `PayloadError`: the importer's output shape changed and this
    adapter is now broken. An individual row that is not an object, or is
    missing what identity requires (`title`, `url`, or the MCP origin tag in
    `source`), is skipped instead: one bad row must not blank the whole drop,
    and Phase 2.4 must never read that as "nothing was imported" (invariant 3
    is about the *target*, not about a single malformed row within it).

    The origin tag (`row["source"]`, e.g. `"mcp-dice"`) becomes the record's
    `instance_key`, overriding the target's own — this is what keeps two MCP
    tools pushed in the same batch in separate identity namespaces (see the
    module docstring). `req_id` is used when the row has one (Dice always
    does); ZipRecruiter rows routinely arrive with `req_id: ""`, which
    `NormalizedPosting` already folds to `None`, so identity degrades to the
    URL claim for exactly those rows — never treated as a global primary key,
    per the contract's `IdentityClaim` precedence.

    Yields lazily so `fetch` streams rather than materializing the batch.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"manual import {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
        raise PayloadError(
            f"manual import {target.instance_key}: expected a list of rows, got "
            f"{type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    for row in data:
        if not isinstance(row, Mapping):
            continue

        title = row.get("title")
        url = row.get("url")
        origin = row.get("source")
        if not title or not str(title).strip():
            continue
        if not url or not str(url).strip():
            continue
        if not origin or not str(origin).strip():
            # No MCP origin tag means no honest identity namespace: the
            # spec's "instance from the mcp source tag" has nothing to read.
            continue
        origin = str(origin).strip()

        posted_date, posted_raw = _row_date(row)
        req_id = row.get("req_id")
        description = row.get("_desc")

        yield target.record(
            title=str(title),
            company=str(row.get("company") or ""),
            url=str(url),
            location=str(row.get("location") or ""),
            instance_key=origin,
            req_id=str(req_id) if req_id else None,
            posted_date=posted_date,
            posted_raw=posted_raw,
            salary_text=str(row.get("salary") or ""),
            remote=bool(row.get("remote")),
            description=str(description) if description else None,
            extra={"mcp_source": origin},
        )


class ManualImportAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """Always one target: "the pushed batch". See the module docstring
        for why this cannot fan out per MCP origin the way `companies.<key>`
        lets other adapters fan out per board.

        `config` is accepted (and ignored) only to satisfy `SourceAdapter`;
        there is no `companies.manual` map to read, and that is not a missing
        feature — nothing about which MCP origins will show up in the next
        push is knowable ahead of the push itself.
        """
        return [
            SourceTarget(
                source_key=SOURCE_KEY,
                instance_key="",
                label="MCP manual import",
                inventory_scope=DESCRIPTOR.default_inventory_scope,
            )
        ]

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """No transport, no status check: just the pure parser over whatever
        the scheduler attached to this attempt as `ctx.payloads`.

        Zero payloads (or every payload parsing to zero rows) yields nothing
        and raises nothing — a run where the MCP importer had nothing to push
        is not a failure. This is safe specifically because the target is
        `InventoryScope.PARTIAL`: an empty yield here is never read as "this
        source's inventory is empty", only as "this drop was empty" — the
        distinction invariant 3 exists to protect for COMPLETE targets does
        not need protecting here, PARTIAL already forbids the inference.
        """
        for payload in ctx.payloads:
            for record in parse_import_rows(payload.content, target):
                yield record


ADAPTER = ManualImportAdapter()
