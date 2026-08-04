"""Recruitee company boards.

Supersedes `scraper.src_recruitee`. Shaped exactly like `greenhouse.py`, with
one structural difference the reference adapter does not have to handle:
Recruitee is a per-tenant host (`{slug}.recruitee.com`), not one shared API
host, so `plan()` stamps a distinct `SourceTarget.host` per company rather
than reusing a module-level constant.

  * `plan()` is pure -- `companies.recruitee` (a `{slug: display name}` map in
    `config.json`, same shape as `companies.greenhouse`) becomes one
    `SourceTarget` per company board.
  * `fetch()` is a transport shell -- one GET to `/api/offers`, one status
    check, then delegation to `parse_offers`, which is pure and
    fixture-drivable.
  * a non-200 raises a classified error instead of returning `[]` (the legacy
    function's `if r.status_code != 200: return out`), so a broken or
    suspended board can never be mistaken for an empty one.
  * `InventoryScope.COMPLETE` -- `/api/offers` is not paginated and enumerates
    every published offer on the board in one response, so a successful run
    does license Phase 2.4 to mark the rest absent.
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

__all__ = ["ADAPTER", "DESCRIPTOR", "RecruiteeAdapter", "board_host", "board_url", "parse_offers"]

SOURCE_KEY = "recruitee"
BOARD_URL_TEMPLATE = "https://{slug}.recruitee.com/api/offers"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Direct ATS boards are the daily-refresh backbone; four hours keeps a
    # 60-second daily run cheap while still catching same-day postings.
    refresh_interval_seconds=4 * 3600,
    # One small JSON GET, no pagination. A board that has not answered in 20
    # seconds is down, and the whole daily run has a 60-second target to hit.
    default_deadline_seconds=20.0,
    # The whole board arrives in one response: no cursor exists to resume.
    # Declaring True here would be a lie the scheduler would act on.
    supports_checkpoint=False,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Each company is its own host (`{slug}.recruitee.com`), so there is no
    # shared-host ceiling to protect the way there is for Greenhouse's single
    # API host; the default per-host concurrency is plenty per tenant.
    per_host_concurrency=4,
    min_request_interval_seconds=0.0,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)


def board_host(slug: str) -> str:
    """`"edifecs"` -> `"edifecs.recruitee.com"`, the per-instance API host."""
    return f"{slug}.recruitee.com"


def board_url(slug: str) -> str:
    return BOARD_URL_TEMPLATE.format(slug=slug)


def parse_offers(
    payload: bytes | str | Mapping[str, Any], target: SourceTarget
) -> Iterator[NormalizedPosting]:
    """Recruitee `/api/offers` JSON -> records. Pure: no I/O, no clock, no globals.

    A malformed envelope raises `PayloadError` (the API changed and this
    adapter is broken). An individual offer missing a title or a `careers_url`
    is skipped: it cannot be identified or opened, and one bad row must not
    blank a board that Phase 2.4 would then mark entirely absent.

    Yields lazily so `fetch` streams rather than materializing the board.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"recruitee board {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"recruitee board {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    offers = data.get("offers")
    if not isinstance(offers, Sequence) or isinstance(offers, (str, bytes)):
        raise PayloadError(
            f"recruitee board {target.instance_key}: 'offers' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    company = str(target.param("company") or target.label or "")
    for offer in offers:
        if not isinstance(offer, Mapping):
            continue
        title = offer.get("title")
        url = offer.get("careers_url")
        if not title or not url:
            continue
        offer_id = offer.get("id")
        published_at = offer.get("published_at") or ""
        extra: dict[str, Any] = {}
        if offer.get("department"):
            extra["department"] = str(offer["department"])
        if offer.get("employment_type_code"):
            extra["employment_type_code"] = str(offer["employment_type_code"])
        if offer.get("updated_at"):
            extra["updated_at"] = str(offer["updated_at"])
        alt_urls: tuple[str, ...] = ()
        apply_url = offer.get("careers_apply_url")
        if apply_url and str(apply_url) != str(url):
            # Recruitee's apply-flow URL for the same offer; volunteered as a
            # mirror, never as identity (invariant: URL is secondary evidence).
            alt_urls = (str(apply_url),)
        yield target.record(
            title=str(title),
            company=company,
            url=str(url),
            location=str(offer.get("location") or ""),
            # The offer id is the source-native requisition identity: stable
            # across title/location edits, and namespaced by board below, so
            # two boards can never collide.
            req_id=str(offer_id) if offer_id is not None else None,
            posted_date=normalize_date(published_at),
            posted_raw=str(published_at),
            # Legacy strictness preserved on purpose: only a literal `True`
            # counts as remote. Recruitee has been seen sending non-boolean
            # truthy values in this field on malformed listings; treating them
            # as remote would be a silent misclassification, not a parse of
            # data the source actually asserted.
            remote=offer.get("remote") is True,
            alt_urls=alt_urls,
            extra=extra,
        )


class RecruiteeAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"edifecs": "Edifecs"}` -> one target per board.

        An unconfigured or empty `companies.recruitee` plans zero targets,
        which is not an error -- the scheduler simply has no Recruitee work.
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
                    # Per-instance host: each company is its own subdomain,
                    # unlike Greenhouse's single shared API host.
                    host=board_host(slug),
                )
            )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """One request, one status check, then the pure parser.

        No retry, no sleep, no deadline branching -- everything this method
        knows how to do on failure is raise (invariants 1-3).
        """
        slug = str(target.require("slug"))
        response: HttpResponse = await ctx.http().send(HttpRequest(url=board_url(slug)))
        check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
        for record in parse_offers(response.content, target):
            yield record


ADAPTER = RecruiteeAdapter()
