"""Ashby job boards — one JSON GET per company, no pagination.

Supersedes `scraper.src_ashby`. Structurally identical to `greenhouse.py`
(read that module's docstring first; this one only calls out what differs):

  * the endpoint is `posting-api/job-board/{slug}` and returns `{"jobs": [...]}`,
    the same one-response-is-the-whole-board shape as Greenhouse, so
    `InventoryScope.COMPLETE` and `supports_checkpoint=False` apply for the
    same reason.
  * identity is the job's `id` (an Ashby-issued UUID), not a URL. The legacy
    scraper never captured it at all — `rec()` only stores a display url — so
    Phase 2.4 absence marking on that legacy data path was flying blind. This
    adapter treats `id` as the source-native requisition id (rank-0 identity
    claim); `jobUrl` is secondary evidence only (invariant: URL is never a
    global primary key).
  * `compensation.compensationTierSummaries[0].compensationTierSummary` is a
    human-readable salary string Ashby precomputes; there is no structured
    min/max to normalize, so it is carried through verbatim as `salary_text`,
    matching what `scraper.src_ashby` did.
  * a non-200 raises a classified error instead of `scraper.src_ashby`'s
    `return out` on any non-200, which made a blocked or deleted board
    indistinguishable from a genuinely empty one (contract invariant 3).
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

__all__ = ["DESCRIPTOR", "AshbyAdapter", "board_url", "parse_board"]

SOURCE_KEY = "ashby"
API_HOST = "api.ashbyhq.com"
BOARD_URL_TEMPLATE = "https://api.ashbyhq.com/posting-api/job-board/{slug}"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Same daily-refresh backbone role as Greenhouse: a direct ATS board.
    refresh_interval_seconds=4 * 3600,
    # One small JSON GET, same rationale as Greenhouse's 20s.
    default_deadline_seconds=20.0,
    # The whole board arrives in one response; there is no partial result to
    # resume, so declaring checkpoint support would be a lie the scheduler
    # would act on.
    supports_checkpoint=False,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # All boards share one API host; keep this conservative until real traffic
    # proves Ashby tolerates more (unlike Greenhouse, no measured baseline yet).
    per_host_concurrency=4,
    min_request_interval_seconds=0.0,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)


def board_url(slug: str) -> str:
    return BOARD_URL_TEMPLATE.format(slug=slug)


def _salary_text(job: Mapping[str, Any]) -> str:
    """First compensation tier summary string, or `""`.

    Ashby precomputes a human-readable range per tier (e.g. "$150K – $190K").
    There is no structured min/max in this envelope to normalize further, so
    the first tier is carried through verbatim, matching `scraper.src_ashby`.
    """
    compensation = job.get("compensation")
    if not isinstance(compensation, Mapping):
        return ""
    tiers = compensation.get("compensationTierSummaries")
    if not isinstance(tiers, Sequence) or isinstance(tiers, (str, bytes)) or not tiers:
        return ""
    first = tiers[0]
    if not isinstance(first, Mapping):
        return ""
    return str(first.get("compensationTierSummary") or "")


def parse_board(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Iterator[NormalizedPosting]:
    """Ashby board JSON -> records. Pure: no I/O, no clock, no globals.

    A malformed envelope raises `PayloadError` (the API changed and this
    adapter is broken). An individual item missing a title or a `jobUrl` is
    skipped rather than failing the whole board, the same rule Greenhouse
    applies: it cannot be identified or opened, and one bad row must not blank
    a board that Phase 2.4 would then mark entirely absent.

    Yields lazily so `fetch` streams rather than materializing the board.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"ashby board {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"ashby board {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    jobs = data.get("jobs")
    if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
        raise PayloadError(
            f"ashby board {target.instance_key}: 'jobs' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    company = str(target.param("company") or target.label or "")
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        title = job.get("title")
        url = job.get("jobUrl")
        if not title or not url:
            continue
        job_id = job.get("id")
        published_at = job.get("publishedAt") or ""
        extra: dict[str, Any] = {}
        if job.get("departmentName"):
            extra["department"] = str(job["departmentName"])
        if job.get("teamName"):
            extra["team"] = str(job["teamName"])
        if job.get("employmentType"):
            extra["employment_type"] = str(job["employmentType"])
        yield target.record(
            title=str(title),
            company=company,
            url=str(url),
            location=str(job.get("location") or ""),
            # Ashby issues a UUID per job posting; it is what the board and the
            # apply flow key on, and it survives title/location edits. The
            # legacy scraper never captured it, so this is a strict identity
            # improvement over `scraper.src_ashby`, not merely a port.
            req_id=str(job_id) if job_id else None,
            posted_date=normalize_date(published_at),
            posted_raw=str(published_at),
            salary_text=_salary_text(job),
            remote=bool(job.get("isRemote", False)),
            extra=extra,
        )


class AshbyAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"anthropic": "Anthropic"}` -> one target per board.

        An unconfigured or empty `companies.ashby` plans zero targets, which is
        not an error — the scheduler simply has no Ashby work.
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


ADAPTER = AshbyAdapter()
