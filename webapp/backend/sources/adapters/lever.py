"""Lever job boards.

One GET per company account returns the account's whole posting list as a bare
JSON array (not wrapped in an object, unlike Greenhouse's `{"jobs": [...]}`).
No pagination, no search terms — the same "one board, one shape" story as
Greenhouse, so read `greenhouse.py` first; this module only calls out where
Lever's payload actually differs:

  * the envelope is a top-level JSON array, so `parse_postings` validates a
    list rather than a mapping with a `jobs` key;
  * `createdAt` is an epoch-millisecond integer, not an ISO string. Legacy
    `scraper.py` did `str(createdAt)[:10]`, which slices the first ten
    *digits* of the millisecond epoch (e.g. `1784019600000` -> `"1784019600"`)
    — not a date, and not even meaningful as text. This adapter converts the
    epoch properly via `datetime.fromtimestamp(ms / 1000, tz=UTC).date()`; see
    `decisions` in the handoff for why that is a deliberate deviation from the
    legacy field mapping rather than a preserved bug;
  * Lever hands every posting a source-native `id` (a UUID) that legacy
    ignored entirely (`rec()` was called with no `req_id=`). The contract
    wants source-native identity first, so this adapter uses `id` as `req_id`
    — a Lever posting's UUID survives title, location, and even
    `hostedUrl`-slug edits, whereas the URL does not.

`InventoryScope.COMPLETE` — the endpoint enumerates the entire account's
posting list, so a successful run does license Phase 2.4 to mark the rest
absent.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from datetime import datetime, timezone
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

__all__ = ["DESCRIPTOR", "LeverAdapter", "postings_url", "parse_postings"]

SOURCE_KEY = "lever"
API_HOST = "api.lever.co"
POSTINGS_URL_TEMPLATE = "https://api.lever.co/v0/postings/{slug}?mode=json"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Same daily-refresh backbone reasoning as Greenhouse: a direct ATS board,
    # cheap to poll, worth catching same-day postings on.
    refresh_interval_seconds=4 * 3600,
    # One small JSON GET, same budget as Greenhouse.
    default_deadline_seconds=20.0,
    # The whole account arrives in one response; there is no partial result to
    # resume from, so declaring checkpoint support would be a lie the
    # scheduler would act on.
    supports_checkpoint=False,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    per_host_concurrency=6,
    min_request_interval_seconds=0.0,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)


def postings_url(slug: str) -> str:
    return POSTINGS_URL_TEMPLATE.format(slug=slug)


def _posted_date(job: Mapping[str, Any]) -> tuple[str | None, str]:
    """Lever's epoch-millisecond `createdAt` -> `(iso_date_or_None, raw_str)`.

    `posted_raw` keeps the original epoch-ms value as provenance (it is not
    hashed, see `normalize_date`'s docstring), while `posted_date` only ever
    carries a real calendar date. A missing, non-numeric, or out-of-range
    epoch yields `(None, raw)` rather than raising: several Lever accounts
    omit `createdAt` on drafts synced into the public board, and one
    unparseable date must not blank the whole account (invariant 3 is about
    the *board*, not individual optional fields).
    """
    raw = job.get("createdAt")
    if raw is None or raw == "":
        return None, ""
    posted_raw = str(raw)
    try:
        epoch_ms = int(raw)
    except (TypeError, ValueError):
        return None, posted_raw
    try:
        iso_date = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None, posted_raw
    return normalize_date(iso_date), posted_raw


def parse_postings(
    payload: bytes | str | Sequence[Any], target: SourceTarget
) -> Iterator[NormalizedPosting]:
    """Lever postings JSON array -> records. Pure: no I/O, no clock, no globals.

    A malformed envelope (not JSON, or not a top-level array) raises
    `PayloadError` — the API changed and this adapter is broken. An individual
    item missing a title or an application URL is skipped: it cannot be
    identified or opened, and one bad row must not blank an account that
    Phase 2.4 would then mark entirely absent.

    Yields lazily so `fetch` streams rather than materializing the account.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"lever account {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Sequence) or isinstance(data, (str, bytes, Mapping)):
        raise PayloadError(
            f"lever account {target.instance_key}: expected a top-level JSON array, got "
            f"{type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    company = str(target.param("company") or target.label or "")
    for job in data:
        if not isinstance(job, Mapping):
            continue
        title = job.get("text")
        url = job.get("hostedUrl")
        if not title or not url:
            continue
        categories = job.get("categories")
        location = categories.get("location") if isinstance(categories, Mapping) else None
        posted_date, posted_raw = _posted_date(job)
        job_id = job.get("id")
        extra: dict[str, Any] = {}
        if isinstance(categories, Mapping):
            if categories.get("team"):
                extra["team"] = str(categories["team"])
            if categories.get("commitment"):
                extra["commitment"] = str(categories["commitment"])
            if categories.get("department"):
                extra["department"] = str(categories["department"])
        if job.get("workplaceType"):
            extra["workplace_type"] = str(job["workplaceType"])
        yield target.record(
            title=str(title),
            company=company,
            url=str(url),
            location=str(location or ""),
            # Lever's posting `id` is a UUID that is the source-native
            # requisition identity: it is embedded in `hostedUrl` and survives
            # title/location edits. Namespaced by account, so two accounts can
            # never collide. Legacy `scraper.py` never captured it.
            req_id=str(job_id) if job_id else None,
            posted_date=posted_date,
            posted_raw=posted_raw,
            remote=job.get("workplaceType") == "remote",
            extra=extra,
        )


class LeverAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"anthropic": "Anthropic"}` -> one target per Lever account.

        An unconfigured or empty `companies.lever` plans zero targets, which is
        not an error — the scheduler simply has no Lever work.
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
        response: HttpResponse = await ctx.http().send(HttpRequest(url=postings_url(slug)))
        check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
        for record in parse_postings(response.content, target):
            yield record


ADAPTER = LeverAdapter()
