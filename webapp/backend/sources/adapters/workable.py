"""Workable job boards — the widget-API adapter.

Structurally identical to Greenhouse (`greenhouse.py`): one GET per company
slug returns the whole board in a single JSON envelope, no pagination, no
search terms. What differs, and why:

  * identity — Workable's native requisition id is `shortcode` (the token
    embedded in every apply URL, e.g. `AB1234CD`), not `code` (an optional
    customer-assigned requisition number some boards never fill in). The
    former is what survives a title or location edit; the latter is kept as
    provenance only.
  * location — the widget API hands back `city`/`state`/`country` as separate
    fields rather than one string. `_location` joins city+state when either
    is present and falls back to country for remote-only postings that carry
    no city/state at all (a shape `scraper.src_workable`'s bare
    `f"{city}, {state}"` did not handle — it would have emitted `", "`).
  * `shortlink` over `url` — both are apply links; `shortlink` is the stable
    short-form Workable itself prefers, so it is tried first, matching
    `scraper.src_workable`'s `j.get("shortlink") or j.get("url")`.

Like Greenhouse: a non-200 raises a classified error instead of returning
`[]`, and `InventoryScope.COMPLETE` because the endpoint enumerates the
entire board.
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

__all__ = ["DESCRIPTOR", "WorkableAdapter", "widget_url", "parse_widget"]

SOURCE_KEY = "workable"
API_HOST = "apply.workable.com"
WIDGET_URL_TEMPLATE = "https://apply.workable.com/api/v1/widget/accounts/{slug}"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Direct ATS boards are the daily-refresh backbone; four hours keeps a
    # 60-second daily run cheap while still catching same-day postings.
    refresh_interval_seconds=4 * 3600,
    # One small JSON GET, same budget as Greenhouse.
    default_deadline_seconds=20.0,
    # No pagination to resume: the board arrives in one response.
    supports_checkpoint=False,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Every configured company shares apply.workable.com; six in flight is the
    # same headroom used for Greenhouse's shared boards-api host.
    per_host_concurrency=6,
    min_request_interval_seconds=0.0,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)


def widget_url(slug: str) -> str:
    return WIDGET_URL_TEMPLATE.format(slug=slug)


def _location(job: Mapping[str, Any]) -> str:
    """`city`+`state`, falling back to `country` when both are absent.

    `scraper.src_workable` always emitted `f"{city}, {state}"`, which produces
    a bare `", "` for a remote-only posting that carries neither. Joining only
    the non-empty parts and falling back to `country` keeps that case from
    manufacturing a location string that is nothing but punctuation.
    """
    city = str(job.get("city") or "").strip()
    state = str(job.get("state") or "").strip()
    parts = [p for p in (city, state) if p]
    if parts:
        return ", ".join(parts)
    return str(job.get("country") or "").strip()


def parse_widget(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Iterator[NormalizedPosting]:
    """Workable widget-account JSON -> records. Pure: no I/O, no clock, no globals.

    A malformed envelope raises `PayloadError` (the API changed and this
    adapter is broken). An individual item missing a title or a usable apply
    URL is skipped: it cannot be identified or opened, and one bad row must
    not blank a board that Phase 2.4 would then mark entirely absent.

    Yields lazily so `fetch` streams rather than materializing the board.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"workable account {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"workable account {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    jobs = data.get("jobs")
    if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
        raise PayloadError(
            f"workable account {target.instance_key}: 'jobs' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    company = str(target.param("company") or target.label or "")
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        title = job.get("title")
        url = job.get("shortlink") or job.get("url")
        if not title or not url:
            continue
        shortcode = job.get("shortcode")
        published_on = job.get("published_on") or ""
        extra: dict[str, Any] = {}
        if job.get("code"):
            # Optional customer-assigned requisition number. Kept as
            # provenance only: `shortcode` is what the apply URL embeds and
            # what survives a title or location edit, so it — not `code` —
            # is the identity.
            extra["customer_requisition_id"] = str(job["code"])
        if job.get("department"):
            extra["department"] = str(job["department"])
        if job.get("employment_type"):
            extra["employment_type"] = str(job["employment_type"])
        yield target.record(
            title=str(title),
            company=company,
            url=str(url),
            location=_location(job),
            req_id=str(shortcode) if shortcode else None,
            posted_date=normalize_date(published_on),
            posted_raw=str(published_on),
            remote=job.get("telecommuting") is True,
            extra=extra,
        )


class WorkableAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"seeq": "Seeq"}` -> one target per account slug.

        An unconfigured or empty `companies.workable` plans zero targets,
        which is not an error — the scheduler simply has no Workable work.
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
        response: HttpResponse = await ctx.http().send(
            HttpRequest(url=widget_url(slug), params={"details": "false"})
        )
        check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
        for record in parse_widget(response.content, target):
            yield record


ADAPTER = WorkableAdapter()
