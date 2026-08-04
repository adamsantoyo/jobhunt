"""Workday CXS company career sites — the search-driven, paginated shape.

Supersedes `scraper.src_workday`. Unlike Greenhouse's one-GET-returns-the-board
shape, a Workday CXS tenant exposes no "list everything" endpoint: the only way
in is `POST /wday/cxs/{tenant}/{site}/jobs` with a `searchText` query, paged 20
at a time. What this module demonstrates that the reference adapter does not:

  * `plan()` embeds `profile.search_terms` into the target's params rather than
    fanning out one target per term. The target stays per-tenant (one board),
    which is what failure isolation and absence scoping key on; the query
    fan-out is `fetch()`'s concern, not the scheduler's work list.
  * `InventoryScope.PARTIAL` — a keyword search proves nothing about what it
    did not match, so a successful run must never license marking anything
    absent (contrast Greenhouse's `COMPLETE`).
  * `supports_checkpoint=True` with an opaque cursor `{query_index, offset}`
    over the flattened `(term, variant)` query list, so a crash or deadline mid
    fan-out resumes at the exact page instead of re-walking every term.
  * `postedOn` is always Workday's relative recency text ("Posted 30+ Days
    Ago"); `normalize_date` correctly returns `None` for it (see the contract
    docstring), and the raw string survives in `posted_raw` only.

Preserved verbatim from `scraper.py`: the `{term} washington` query variant
issued before the bare term, the 20-item page size, and the 5-page-per-query
cap. Preserved as `descriptor.min_request_interval_seconds` rather than a
`time.sleep`: the 0.2s pacing between requests. Not preserved: the internal
`try/except: break` retry-by-silence and the cross-source `scraper.dedupe`
merge, which is Phase 3 resolver work.
"""
from __future__ import annotations

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

__all__ = [
    "DESCRIPTOR",
    "WorkdayAdapter",
    "build_queries",
    "jobs_url",
    "parse_page",
    "posting_url",
]

SOURCE_KEY = "workday"

#: Items requested per page. Matches `scraper.py`'s `limit`.
PAGE_SIZE = 20
#: Hard cap on pages walked for a single (term, variant) query, regardless of
#: how large `total` claims to be. Matches `scraper.py`'s `max_pages=5`; a
#: board that pages past this simply loses its tail for that query, same as
#: the legacy behaviour.
MAX_PAGES = 5

JOBS_URL_TEMPLATE = "https://{host}/wday/cxs/{tenant}/{site}/jobs"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    refresh_interval_seconds=4 * 3600,
    # Up to `len(search_terms) * 2` queries, each up to 5 pages: far more
    # expensive than Greenhouse's single GET, so this target gets a much
    # longer budget than the 20s reference default.
    default_deadline_seconds=60.0,
    # Pagination is real here (unlike Greenhouse's single-response board), and
    # a crash mid fan-out should resume at the page it reached.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Each tenant's host is company-specific (`{tenant}.wd#.myworkdayjobs.com`),
    # so per-host concurrency is really per-company; the default ceiling is
    # generous enough without a source-wide cap.
    per_host_concurrency=4,
    # Replaces `scraper.py`'s `time.sleep(0.2)` between requests.
    min_request_interval_seconds=0.2,
    description_inline=False,
    default_inventory_scope=InventoryScope.PARTIAL,
)


def jobs_url(host: str, tenant: str, site: str) -> str:
    return JOBS_URL_TEMPLATE.format(host=host, tenant=tenant, site=site)


def posting_url(host: str, site: str, external_path: str) -> str:
    return f"https://{host}/en-US/{site}{external_path}"


def _query_variants(term: str) -> tuple[str, str]:
    """`"support engineer"` -> `("support engineer washington", "support engineer")`.

    Preserves `scraper.py`'s exact query shape and order. Both variants share
    one identity space (dedup on `externalPath` within a run, invariant 5), so
    a tenant whose search text is effectively ignored still contributes only
    one copy of each posting.
    """
    return (f"{term} washington", term)


def build_queries(search_terms: Sequence[str]) -> tuple[str, ...]:
    """Flatten `profile.search_terms` into the ordered query list `fetch` walks.

    Pure and small enough to unit-test on its own; `fetch`'s checkpoint cursor
    is an index into exactly this list, so its order must be deterministic.
    """
    queries: list[str] = []
    for term in search_terms:
        if not term:
            continue
        queries.extend(_query_variants(term))
    return tuple(queries)


def _posted(job: Mapping[str, Any]) -> tuple[str | None, str]:
    """Workday's `postedOn` is relative recency text, never an absolute date.

    `normalize_date` returning `None` here is correct, not a bug: hashing a
    string that reads "Posted 3 Days Ago" today and "Posted 4 Days Ago"
    tomorrow would mint a bogus posting version on every run. The raw string
    survives in `posted_raw`, which is not hashed.
    """
    raw = job.get("postedOn") or ""
    return normalize_date(raw), str(raw)


def _req_id(job: Mapping[str, Any]) -> str | None:
    """The requisition id lives in `bulletFields[0]` when present at all.

    Some tenants omit `bulletFields` or ship it empty; when that happens the
    posting still has a URL, so identity degrades to the URL alias rather than
    the row being dropped (contrast a missing title/path, which is unusable).
    """
    fields = job.get("bulletFields")
    if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)) or not fields:
        return None
    first = fields[0]
    return str(first) if first else None


def parse_page(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Iterator[NormalizedPosting]:
    """One Workday CXS search response page -> records. Pure: no I/O, no clock.

    A malformed envelope (not JSON, no `jobPostings` list) raises
    `PayloadError`: the tenant's API shape changed and this adapter is broken.
    An individual posting missing a title or `externalPath` is skipped -- it
    cannot be identified or opened -- so one bad row never blanks the page.

    Reads `host`/`site`/`company` off `target.params`, the same params `plan()`
    embedded, so the URL a fixture-driven test builds is byte-identical to what
    a live `fetch()` would have produced.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"workday {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"workday {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    postings = data.get("jobPostings")
    if not isinstance(postings, Sequence) or isinstance(postings, (str, bytes)):
        raise PayloadError(
            f"workday {target.instance_key}: 'jobPostings' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    host = str(target.param("host") or "")
    site = str(target.param("site") or "")
    company = str(target.param("company") or target.label or "")

    for job in postings:
        if not isinstance(job, Mapping):
            continue
        title = job.get("title")
        path = job.get("externalPath")
        if not title or not path:
            continue
        posted_date, posted_raw = _posted(job)
        yield target.record(
            title=str(title),
            company=company,
            url=posting_url(host, site, str(path)),
            location=str(job.get("locationsText") or ""),
            # The requisition number is the source-native identity; namespaced
            # by tenant (`target.record` stamps `instance_key`), so two
            # tenants can never collide even if they share a req numbering
            # scheme.
            req_id=_req_id(job),
            posted_date=posted_date,
            posted_raw=posted_raw,
            extra={"external_path": str(path)},
        )


def _raw_posting_count(data: Mapping[str, Any]) -> int:
    """How many raw rows this page carried, independent of how many parsed.

    Used only for the pagination stop condition: a page with zero *raw* rows
    means the query is exhausted, even if some raw rows on an earlier page
    were unusable and skipped by `parse_page`. Never used to decide whether to
    raise -- that is `parse_page`'s job.
    """
    postings = data.get("jobPostings")
    if isinstance(postings, Sequence) and not isinstance(postings, (str, bytes)):
        return len(postings)
    return 0


class WorkdayAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`companies.workday.{key: {host, tenant, site, name}}` -> one target
        per tenant entry, with `profile.search_terms` embedded in its params.

        Deliberately does not validate `host`/`tenant`/`site` here: a target
        with a blank field is still buildable (mirrors `entries()` raising
        only when the whole map is the wrong shape), and `fetch()`'s
        `target.require(...)` is what raises `ConfigError` when a field that
        is actually needed to make a request turns out to be missing.
        """
        search_terms = tuple(config.search_terms)
        targets: list[SourceTarget] = []
        for key, entry in config.entries(SOURCE_KEY).items():
            key = str(key).strip()
            if not key:
                continue
            if not isinstance(entry, Mapping):
                raise ConfigError(
                    f"companies.workday.{key} must be an object, got {type(entry).__name__}",
                    source_key=SOURCE_KEY,
                    instance_key=key,
                )
            host = str(entry.get("host") or "").strip()
            tenant = str(entry.get("tenant") or "").strip()
            site = str(entry.get("site") or "").strip()
            name = str(entry.get("name") or key)
            targets.append(
                SourceTarget(
                    source_key=SOURCE_KEY,
                    instance_key=key,
                    label=name,
                    params={
                        "host": host,
                        "tenant": tenant,
                        "site": site,
                        "company": name,
                        "search_terms": search_terms,
                    },
                    inventory_scope=DESCRIPTOR.default_inventory_scope,
                    host=host or None,
                )
            )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Walk every `(term, variant)` query to its page cap, streaming as it goes.

        No retry, no sleep, no deadline branching (invariants 1-3, 8): a
        non-200 or a malformed body raises immediately and the scheduler
        decides what happens next. Checkpoints are marked after each page's
        records have been yielded, per `ctx.mark_checkpoint`'s contract.
        """
        host = str(target.require("host"))
        tenant = str(target.require("tenant"))
        site = str(target.require("site"))
        search_terms = tuple(target.param("search_terms") or ())
        queries = build_queries(search_terms)
        if not queries:
            raise ConfigError(
                f"{target.source_run_key}: no search terms configured",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            )
        url = jobs_url(host, tenant, site)

        start_qi, start_offset, emitted = 0, 0, 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            cursor = ctx.resume_from.cursor
            start_qi = int(cursor.get("query_index", 0))
            start_offset = int(cursor.get("offset", 0))
            emitted = int(ctx.resume_from.emitted)

        # In-run only: an efficiency measure (invariant 5), never relied on
        # for correctness. The two query variants per term overlap heavily,
        # and the writer dedupes on identity regardless.
        seen_paths: set[str] = set()

        for qi in range(start_qi, len(queries)):
            query_text = queries[qi]
            offset = start_offset if qi == start_qi else 0
            for _page_num in range(MAX_PAGES):
                response: HttpResponse = await ctx.http().send(
                    HttpRequest(
                        url=url,
                        method="POST",
                        json_body={
                            "appliedFacets": {},
                            "limit": PAGE_SIZE,
                            "offset": offset,
                            "searchText": query_text,
                        },
                    )
                )
                check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
                data = response.json(source_key=SOURCE_KEY, instance_key=target.instance_key)

                for record in parse_page(data, target):
                    path = record.extra.get("external_path", "")
                    if path:
                        if path in seen_paths:
                            continue
                        seen_paths.add(path)
                    yield record
                    emitted += 1

                raw_count = _raw_posting_count(data) if isinstance(data, Mapping) else 0
                next_offset = offset + PAGE_SIZE
                total_raw = data.get("total") if isinstance(data, Mapping) else None
                total = int(total_raw) if isinstance(total_raw, (int, float)) else None
                exhausted = raw_count == 0 or (total is not None and next_offset >= total)
                if exhausted:
                    break
                offset = next_offset
                ctx.mark_checkpoint({"query_index": qi, "offset": offset}, target=target, emitted=emitted)
            # Whether this query ended by exhaustion or by hitting MAX_PAGES,
            # the next unit of work is the following query from its start.
            ctx.mark_checkpoint({"query_index": qi + 1, "offset": 0}, target=target, emitted=emitted)


ADAPTER = WorkdayAdapter()
