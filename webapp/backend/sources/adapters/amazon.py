"""Amazon.jobs — a singleton, search-driven, paginated, checkpointed source.

Supersedes `scraper.src_amazon`. Unlike Greenhouse (one board, one request, the
whole inventory) Amazon.jobs has no board to enumerate: the only public entry
point is `search.json`, a keyword search over `profile.search_terms` at
`profile.employer_scrape_location`. That shapes every difference from the
reference adapter:

  * ONE TARGET, NOT ONE PER COMPANY. `companies.json` has no `amazon` block —
    there is exactly one Amazon, so `plan()` returns at most one
    `SourceTarget` with an empty `instance_key`, namespacing identity on the
    source key alone (see `NormalizedPosting.namespace`).
  * PARTIAL, NOT COMPLETE. A keyword search over one location radius proves
    nothing about postings it did not match. `InventoryScope.PARTIAL` is not
    optional here — getting it wrong would let Phase 2.4 mark real, unmatched
    Amazon postings absent.
  * PAGINATED AND CHECKPOINTED. Each search term pages `result_limit=100` at a
    time, capped at 500 results (`min(hits, 500)`, the legacy defensive
    ceiling), and the fan-out across terms is exactly the kind of long,
    interruptible enumeration `Checkpoint` exists for. The cursor is
    `{"term_index", "offset"}`: which term is in flight and how far into it.
  * NO SILENT STOP ON FAILURE. `scraper.src_amazon` wraps each page in
    `except Exception: break`, so a mid-run block or throttle quietly looks
    like "ran out of results". Here a non-200 or a broken envelope raises
    (invariants 1-3); only an actually empty page, or the declared `hits`
    ceiling, ends a term.
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import Any

from ..contract import (
    ConfigError,
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

__all__ = ["AmazonAdapter", "DESCRIPTOR", "parse_search_page", "search_url", "total_hits"]

SOURCE_KEY = "amazon"
SEARCH_HOST = "www.amazon.jobs"
SEARCH_URL = "https://www.amazon.jobs/en/search.json"
JOB_BASE_URL = "https://www.amazon.jobs"
COMPANY_NAME = "Amazon"
#: Default location filter when `profile.employer_scrape_location` is unset,
#: matching `scraper._emp_loc`'s fallback exactly.
DEFAULT_LOCATION = "California, United States"
#: One page of results.
PAGE_SIZE = 100
#: `min(hits, 500)` in the legacy loop: a defensive ceiling on how far one
#: search term is paged, independent of how large `hits` claims to be.
MAX_OFFSET = 500
RADIUS = "40km"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    refresh_interval_seconds=4 * 3600,
    # Up to `len(search_terms) * 5` requests against one host; a single
    # board's 20s budget would starve the later terms.
    default_deadline_seconds=90.0,
    # Long enumeration across many terms and pages is exactly what resuming
    # is for: a run cut off mid-term should not restart every prior term.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    per_host_concurrency=2,
    # Replaces `time.sleep(0.2)` between pages in `scraper.src_amazon`.
    min_request_interval_seconds=0.2,
    description_inline=False,
    # A keyword search can never license absence marking. See module docstring.
    default_inventory_scope=InventoryScope.PARTIAL,
)


def search_url() -> str:
    return SEARCH_URL


def total_hits(payload: bytes | str | Mapping[str, Any]) -> int:
    """The `hits` Amazon reports for a search page, or `0` if absent/unusable.

    Advisory only: used by `fetch` to decide whether another page of the
    current term is worth requesting. Never raises — a missing or malformed
    count degrades to "treat this term as exhausted", which is the safe
    direction (mirrors the legacy `min(d.get("hits", 0), 500)` default of 0).
    """
    if isinstance(payload, (bytes, bytearray, str)):
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return 0
    else:
        data = payload
    if not isinstance(data, Mapping):
        return 0
    hits = data.get("hits")
    if isinstance(hits, bool) or not isinstance(hits, (int, float)):
        return 0
    return int(hits)


def parse_search_page(
    payload: bytes | str | Mapping[str, Any], target: SourceTarget
) -> Iterator[NormalizedPosting]:
    """One `search.json` page -> records. Pure: no I/O, no clock, no globals.

    A malformed envelope raises `PayloadError` (the API changed and this
    adapter is broken). An individual item missing a title or a `job_path`
    is skipped: it cannot be identified or opened, and one bad row must not
    blank a page that Phase 2.4 would otherwise weigh as "no results".

    Yields lazily so `fetch` streams rather than materializing a page.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"amazon search page: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"amazon search page: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    jobs = data.get("jobs")
    if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
        raise PayloadError(
            "amazon search page: 'jobs' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    company = str(target.param("company") or COMPANY_NAME)
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        title = job.get("title")
        job_path = job.get("job_path")
        if not title or not job_path:
            continue
        city = str(job.get("city") or "").strip()
        state = str(job.get("state") or "").strip()
        location = ", ".join(part for part in (city, state) if part)
        # The requisition id (`id_icims`) is the source-native identity: it
        # survives title/location edits. When Amazon omits it (it sometimes
        # does for freshly-posted roles) the job path is the next-best
        # source-native handle — still namespaced by this target, never the
        # bare URL (contract: URL is secondary evidence, never identity).
        icims_id = job.get("id_icims")
        req_id = str(icims_id) if icims_id else str(job_path)
        posted_raw = str(job.get("posted_date") or "")
        yield target.record(
            title=str(title),
            company=company,
            url=JOB_BASE_URL + str(job_path),
            location=location,
            req_id=req_id,
            posted_date=normalize_date(posted_raw),
            posted_raw=posted_raw,
            extra={"job_path": str(job_path)},
        )


class AmazonAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`profile.search_terms` -> at most one target for all of Amazon.

        No search terms configured plans zero targets, which is not an
        error — the scheduler simply has no Amazon work, same as an
        unconfigured `companies.<source_key>` map for the per-company
        sources.
        """
        terms = config.search_terms
        if not terms:
            return []
        location = str(config.profile.get("employer_scrape_location") or DEFAULT_LOCATION)
        return [
            SourceTarget(
                source_key=SOURCE_KEY,
                instance_key="",
                label=COMPANY_NAME,
                params={
                    "search_terms": terms,
                    "location": location,
                    "company": COMPANY_NAME,
                },
                inventory_scope=DESCRIPTOR.default_inventory_scope,
                host=SEARCH_HOST,
            )
        ]

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Page every search term, streaming records and checkpointing per page.

        No retry, no sleep, no deadline branching — everything this method
        knows how to do on a failed page is raise (invariants 1-3). The
        cursor `{"term_index", "offset"}` names exactly where the next
        request must start, so a resumed run re-issues no request whose
        records were already yielded (beyond the replay-safe overlap the
        contract already tolerates).
        """
        terms = tuple(str(t) for t in (target.param("search_terms") or ()) if t)
        if not terms:
            raise ConfigError(
                "amazon: target has no search_terms", source_key=SOURCE_KEY, instance_key=target.instance_key
            )
        location = str(target.param("location") or "")

        term_index = 0
        offset = 0
        emitted = 0
        if ctx.resume_from is not None:
            cursor = ctx.resume_from.cursor
            term_index = max(0, int(cursor.get("term_index") or 0))
            offset = max(0, int(cursor.get("offset") or 0))
            emitted = max(0, int(ctx.resume_from.emitted or 0))

        while term_index < len(terms):
            term = terms[term_index]
            response: HttpResponse = await ctx.http().send(
                HttpRequest(
                    url=SEARCH_URL,
                    params={
                        "base_query": term,
                        "loc_query": location,
                        "result_limit": PAGE_SIZE,
                        "offset": offset,
                        "radius": RADIUS,
                    },
                )
            )
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            data = response.json(source_key=SOURCE_KEY, instance_key=target.instance_key)

            for record in parse_search_page(data, target):
                emitted += 1
                yield record

            # `parse_search_page` above already proved `data` is a mapping
            # with a `jobs` list (it validates before its first yield), so
            # reading the raw envelope here needs no further checking.
            raw_jobs = data.get("jobs") if isinstance(data, Mapping) else []
            next_offset = offset + PAGE_SIZE
            cap = min(total_hits(data), MAX_OFFSET)
            if raw_jobs and next_offset < cap:
                offset = next_offset
            else:
                term_index += 1
                offset = 0

            ctx.mark_checkpoint({"term_index": term_index, "offset": offset}, target=target, emitted=emitted)


ADAPTER = AmazonAdapter()
