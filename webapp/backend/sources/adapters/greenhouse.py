"""Greenhouse job boards — the reference implementation of the contract.

The simplest of the sixteen shapes: one GET per company board returns the whole
board, no pagination, no search terms. Read it as the worked example for the
other fifteen. What it demonstrates:

  * `plan()` is pure — `companies.greenhouse` (a `{slug: display name}` map in
    `config.json`) becomes one `SourceTarget` per board, and nothing else.
  * `fetch()` is a transport shell — one request, one status check, then
    delegation to `parse_board`, which is pure and fixture-drivable.
  * a non-200 raises a classified error instead of returning `[]`, so a broken
    board can never be mistaken for an empty one.
  * `InventoryScope.COMPLETE` — the endpoint enumerates the entire board, so a
    successful run does license Phase 2.4 to mark the rest absent. Contrast the
    search-driven sources, which must be PARTIAL.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import Any

from ..contract import (
    ExecutionMode,
    FetchContext,
    HttpRequest,
    HttpResponse,
    InventoryScope,
    NormalizedPosting,
    PayloadError,
    RunKind,
    SourceCategory,
    SourceConfig,
    SourceDescriptor,
    SourceTarget,
    TransportKind,
    check_status,
    normalize_date,
)

__all__ = ["DESCRIPTOR", "GreenhouseAdapter", "board_url", "parse_board"]

SOURCE_KEY = "greenhouse"
API_HOST = "boards-api.greenhouse.io"
BOARD_URL_TEMPLATE = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Direct ATS boards are the daily-refresh backbone; four hours keeps a
    # 60-second daily run cheap while still catching same-day postings.
    refresh_interval_seconds=4 * 3600,
    # One small JSON GET. A board that has not answered in 20 seconds is down,
    # and the whole daily run has a 60-second target to hit.
    default_deadline_seconds=20.0,
    # No pagination to resume: the board arrives in one response, so a partial
    # result is not a thing that exists. Declaring True here would be a lie the
    # scheduler would act on.
    supports_checkpoint=False,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # ~32 boards behind one API host. Six in flight saturates it without
    # tripping Greenhouse's rate limiting.
    per_host_concurrency=6,
    min_request_interval_seconds=0.0,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)


def board_url(slug: str) -> str:
    return BOARD_URL_TEMPLATE.format(slug=slug)


def _posted_date(job: Mapping[str, Any]) -> tuple[str | None, str]:
    """Pick the hashable date, and keep the raw string for provenance.

    `first_published` is preferred over `updated_at` because `updated_at` moves
    whenever anyone touches the requisition — a recruiter re-tagging a job
    would otherwise mint a new posting version on the next run. When only
    `updated_at` exists it is used, truncated to a date so a timestamp bump
    within a day is not a material change.
    """
    raw = job.get("first_published") or job.get("updated_at") or ""
    return normalize_date(raw), str(raw or "")


def parse_board(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Iterator[NormalizedPosting]:
    """Greenhouse board JSON -> records. Pure: no I/O, no clock, no globals.

    A malformed envelope raises `PayloadError` (the API changed and this
    adapter is broken). An individual item missing a title or a URL is skipped:
    it cannot be identified or opened, and one bad row must not blank a board
    that Phase 2.4 would then mark entirely absent.

    Yields lazily so `fetch` streams rather than materializing the board.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"greenhouse board {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"greenhouse board {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    jobs = data.get("jobs")
    if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
        raise PayloadError(
            f"greenhouse board {target.instance_key}: 'jobs' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    company = str(target.param("company") or target.label or "")
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        title = job.get("title")
        url = job.get("absolute_url")
        if not title or not url:
            continue
        location = job.get("location")
        location_name = location.get("name") if isinstance(location, Mapping) else location
        posted_date, posted_raw = _posted_date(job)
        job_id = job.get("id")
        extra: dict[str, Any] = {}
        if job.get("internal_job_id") is not None:
            # Greenhouse groups one requisition posted to several locations
            # under a shared internal id. Kept as provenance for the Phase 3
            # resolver; deliberately not the identity, since each location is a
            # separately applicable posting.
            extra["internal_job_id"] = str(job["internal_job_id"])
        if job.get("requisition_id"):
            extra["customer_requisition_id"] = str(job["requisition_id"])
        if job.get("updated_at"):
            extra["updated_at"] = str(job["updated_at"])
        yield target.record(
            title=str(title),
            company=company,
            url=str(url),
            location=str(location_name or ""),
            # The board job id is the source-native requisition identity: it is
            # what the apply URL embeds and what survives title and location
            # edits. Namespaced by board, so two boards can never collide.
            req_id=str(job_id) if job_id is not None else None,
            posted_date=posted_date,
            posted_raw=posted_raw,
            extra=extra,
        )


class GreenhouseAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"anthropic": "Anthropic"}` -> one target per board.

        An unconfigured or empty `companies.greenhouse` plans zero targets,
        which is not an error — the scheduler simply has no Greenhouse work.
        """
        targets: list[SourceTarget] = []
        for slug, display_name in config.entries(SOURCE_KEY).items():
            slug = str(slug).strip()
            if not slug:
                continue
            name = str(display_name or slug)
            targets.append(
                SourceTarget(
                    source_key=SOURCE_KEY,
                    instance_key=slug,
                    label=name,
                    params={"slug": slug, "company": name},
                    inventory_scope=DESCRIPTOR.default_inventory_scope,
                    host=API_HOST,
                )
            )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """One request, one status check, then the pure parser.

        No retry, no sleep, no deadline branching — everything this method
        knows how to do on failure is raise (invariants 1-3).
        """
        slug = str(target.require("slug"))
        response: HttpResponse = await ctx.http().send(HttpRequest(url=board_url(slug)))
        check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
        for record in parse_board(response.content, target):
            yield record


ADAPTER = GreenhouseAdapter()
