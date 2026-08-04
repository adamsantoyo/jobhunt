"""Jibe / iCIMS Careers Cloud portals — including Costco's careers site.

Both the generic Jibe portals (`careers.amd.com`, ...) and Costco's careers
site are the same underlying platform: a `{base}/api/jobs` endpoint that wraps
every row as `{"data": {...}}` and answers `{"state": ..., "page": ...}` or
`{"keywords": ..., "page": ...}` queries identically. This is ONE adapter, not
two, because the difference between them is entirely in what query a target
sends and how it stops paging, not in how a payload is parsed:

  * GENERIC JIBE (`mode="keyword"`, the default): the portal has no facet that
    enumerates its whole inventory, so `plan()` fans a target out over
    `profile.search_terms` and `fetch()` walks up to `KEYWORD_PAGE_LIMIT` pages
    per term (`scraper.py::src_jibe`, pages 1..5).
  * COSTCO-STYLE (`mode="state"`): Costco's `keywords` search is ML-fuzzy and
    useless for precision (`scraper.py::src_costco`'s own docstring), so
    instead the target enumerates the `state` facet (all Washington postings)
    up to `STATE_PAGE_LIMIT` pages and title-filters at harvest instead of at
    the query. This is NOT a hardcoded company: it is a `companies.jibe` entry
    like any other, distinguished only by carrying `mode: "state"` and a
    `state` value instead of relying on `profile.search_terms`. A deployment
    that wants Costco enumerated adds
    `"costco": {"base": "https://careers.costco.com", "name": "Costco
    Wholesale", "mode": "state", "state": "Washington"}` to `companies.jibe`;
    this module never names Costco.

Both modes share the same title prefilter (`TITLE_PREFILTER`, verbatim from
`scraper.py::COSTCO_TECH`) applied by the pure parser, and the same envelope
parsing (`parse_jobs_page`). They differ in the URL a matched row resolves to:
generic Jibe's `apply_url` is an ATS login-gated link, not a public posting
page, so the slug page is the only usable canonical url; Costco's `apply_url`/
`canonical_url` genuinely are public posting pages and are preferred.

`InventoryScope.PARTIAL` for both modes, deliberately conservative even for
the state-facet enumeration. Two independent reasons either one of which
would be sufficient on its own:

  1. A run that never sees an empty page (Costco has more Washington postings
     than `STATE_PAGE_LIMIT * PAGE_SIZE`) has not enumerated the whole facet —
     it hit a depth cap, not the end of the inventory.
  2. Even a fully enumerated facet is filtered by `TITLE_PREFILTER` before a
     single record is yielded. A posting that is still live but had its title
     edited to no longer match (or a posting Phase 3 previously recorded from
     a title that used to match) is indistinguishable, from inside one run,
     from a posting that was actually closed. `InventoryScope.COMPLETE` would
     let Phase 2.4 mark that still-open posting absent purely because its
     title drifted, which is exactly the silent-deletion failure mode the
     contract's `InventoryScope` docstring warns about. PARTIAL means absence
     is never inferred from either mode, at the cost of never pruning stale
     Jibe/Costco postings automatically — acceptable, since nothing about this
     source licenses that inference safely.

Legacy: `scraper.py::src_jibe` (~line 411) and `scraper.py::src_costco` (~line
380). Preserved: the `{base}/api/jobs` endpoint, `limit=100`, `lang=en-us`,
the `{"data": {...}}` unwrap, the field mapping (`title`, `city`/`state`/
`full_location` -> `location`, `posted_date`|`create_date` -> `posted`), the
`req_id`-or-`slug` identity fallback, the url construction for each mode, and
`TITLE_PREFILTER`. Changed: a non-200 raises a classified error instead of
silently ending the walk (invariant 3); there is no `time.sleep` between pages
(`SourceDescriptor.min_request_interval_seconds` replaces it, invariant 2);
`posted_raw` keeps the full raw value rather than `scraper.py`'s `[:10]` slice
(more provenance, still excluded from the hash by `normalize_date`'s own
regex-anchored parsing, so this does not change what date gets hashed); the
in-run `seen` dedup set is dropped (invariant 5 makes it optional, and cutting
it keeps `fetch` a plain per-page stream); and cross-source dedupe/merging
(`scraper.py::dedupe`) is out of scope — Phase 3 resolver work.

`description` is deliberately NOT populated even though the list payload
carries one inline (`rubric.py::fetch_jibe_desc` reads `data.description` off
this same endpoint) — out of scope for this adapter's field mapping; a future
change can flip `description_inline` and wire it up.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

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
    "ADAPTER",
    "DESCRIPTOR",
    "JibeAdapter",
    "KEYWORD_PAGE_LIMIT",
    "PAGE_SIZE",
    "STATE_PAGE_LIMIT",
    "TITLE_PREFILTER",
    "job_page_url",
    "jobs_url",
    "matches_title_prefilter",
    "parse_jobs_page",
]

SOURCE_KEY = "jibe"

#: Title keyword prefilter, verbatim from `scraper.py::COSTCO_TECH`. Applies to
#: both modes: neither the generic Jibe keyword search nor Costco's state-facet
#: enumeration is otherwise scoped to roles this profile cares about.
TITLE_PREFILTER: tuple[str, ...] = (
    "engineer",
    "technician",
    "analyst",
    "program",
    "technolog",
    "systems",
    "support",
    "developer",
    "administrator",
    "network",
    "data",
    "security",
    "operations",
    "it ",
)

#: `scraper.py::src_jibe` -> `for page in range(1, 6)`.
KEYWORD_PAGE_LIMIT = 5
#: `scraper.py::src_costco` -> `for page in range(1, 15)`.
STATE_PAGE_LIMIT = 14
#: Matches both legacy functions' `limit: 100`.
PAGE_SIZE = 100

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    refresh_interval_seconds=4 * 3600,
    # A multi-page, potentially multi-term walk per target: more headroom than
    # a single-request board (Greenhouse) or a single offset-paged one
    # (SmartRecruiters).
    default_deadline_seconds=60.0,
    # Real pagination (and, for keyword mode, a term cursor) to resume.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Each target is its own employer's careers host, not a shared API host
    # (unlike Greenhouse's ~32 boards on one host), so no cross-target cap.
    max_concurrent_targets=None,
    per_host_concurrency=3,
    # Replaces `scraper.py`'s `time.sleep(0.2)` between pages (invariant 2).
    min_request_interval_seconds=0.2,
    description_inline=False,
    # Conservative for both modes; see module docstring.
    default_inventory_scope=InventoryScope.PARTIAL,
)


def jobs_url(base: str) -> str:
    return f"{base.rstrip('/')}/api/jobs"


def job_page_url(base: str, slug: str) -> str:
    return f"{base.rstrip('/')}/jobs/{slug}"


def matches_title_prefilter(title: str) -> bool:
    """`True` if `title` contains any `TITLE_PREFILTER` keyword, case-insensitively."""
    lowered = title.lower()
    return any(keyword in lowered for keyword in TITLE_PREFILTER)


def _posted(d: Mapping[str, Any]) -> tuple[str | None, str]:
    """`posted_date` falling back to `create_date` -> (hashable date, full raw).

    Matches `scraper.py`'s `d.get("posted_date") or d.get("create_date") or
    ""`. Unlike the legacy `[:10]` slice, `posted_raw` here is not truncated
    (more provenance, never hashed); `normalize_date`'s regex already anchors
    on the leading `YYYY-MM-DD`, so the hashable date is identical either way.
    """
    raw = d.get("posted_date") or d.get("create_date") or ""
    text = str(raw)
    return normalize_date(text), text


def _location(d: Mapping[str, Any]) -> str:
    """`full_location`, or `city`/`state` joined. Matches both legacy functions."""
    full = d.get("full_location")
    if full:
        return str(full)
    city = d.get("city") or ""
    state = d.get("state") or ""
    return ", ".join(x for x in (city, state) if x)


def _posting_url(d: Mapping[str, Any], *, base: str, slug: str, mode: str) -> str:
    """The canonical posting url, mode-dependent (see module docstring).

    `mode="state"` (Costco-style): `apply_url` and `canonical_url` are public
    posting pages, preferred over the constructed slug page.
    `mode="keyword"` (generic Jibe): `apply_url` is an ATS login-gated link,
    so only the constructed slug page is used, exactly as
    `scraper.py::src_jibe` does.
    """
    if mode == "state":
        return str(d.get("apply_url") or d.get("canonical_url") or job_page_url(base, slug))
    return job_page_url(base, slug)


def _decode(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Mapping[str, Any]:
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"jibe {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload
    if not isinstance(data, Mapping):
        raise PayloadError(
            f"jibe {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    return data


def _extract_jobs(data: Mapping[str, Any], target: SourceTarget) -> Sequence[Any]:
    """Validate and return the raw `jobs` list, unfiltered.

    Deliberately separate from `parse_jobs_page` so `fetch`'s pagination loop
    can check "did this page have any rows at all" (the legacy stopping
    condition) independently of the title prefilter — a page with 100 rows and
    zero matching titles must not be mistaken for the end of the walk.
    """
    jobs = data.get("jobs")
    if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
        raise PayloadError(
            f"jibe {target.instance_key}: 'jobs' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    return jobs


def parse_jobs_page(
    payload: bytes | str | Mapping[str, Any], target: SourceTarget
) -> Iterator[NormalizedPosting]:
    """One `{base}/api/jobs` page -> records. Pure: no I/O, no clock, no globals.

    A malformed envelope (not JSON, no `jobs` list at the top level) raises
    `PayloadError`: the API changed and this adapter is broken. An individual
    row is skipped, not raised, when it cannot be identified (`req_id` and
    `slug` both absent/empty) or its title fails `TITLE_PREFILTER` — one bad
    or irrelevant row must not blank a page that Phase 2.4 would then treat as
    proof the rest of the target is empty.

    `target.param("mode")` (`"keyword"` or `"state"`) selects the url
    construction rule; `target.param("base")` and `target.param("company")`
    supply the portal root and display name. Yields lazily so `fetch` streams
    rather than materializing a page.
    """
    data = _decode(payload, target)
    jobs = _extract_jobs(data, target)

    base = str(target.param("base") or "")
    company = str(target.param("company") or target.label or "")
    mode = str(target.param("mode") or "keyword")

    for wrap in jobs:
        if not isinstance(wrap, Mapping):
            continue
        d = wrap.get("data", wrap)
        if not isinstance(d, Mapping):
            continue
        title = d.get("title") or ""
        if not title or not matches_title_prefilter(str(title)):
            continue
        req_id = d.get("req_id")
        slug = d.get("slug")
        job_id = str(req_id) if req_id else (str(slug) if slug else "")
        if not job_id:
            continue
        slug_text = str(slug) if slug else job_id
        posted_date, posted_raw = _posted(d)
        yield target.record(
            title=str(title),
            company=company,
            url=_posting_url(d, base=base, slug=slug_text, mode=mode),
            location=_location(d),
            # Source-native requisition id first, falling back to the slug
            # only when no `req_id` was issued (both legacy functions do the
            # same `req_id or slug` fallback). Namespaced by portal via
            # `target.record` stamping `instance_key`, so two portals can
            # never collide even if they reuse slugs.
            req_id=job_id,
            posted_date=posted_date,
            posted_raw=posted_raw,
        )


class JibeAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`companies.jibe` -> one target per portal.

        Each entry needs `base`; `name` defaults to the slug. `mode: "state"`
        (Costco-style) additionally requires `state` and plans regardless of
        `profile.search_terms`. The default `mode: "keyword"` (generic Jibe)
        instead fans out over `profile.search_terms`, baked into the target's
        own `params` so a change in search terms changes
        `config_fingerprint()` and correctly invalidates a stale checkpoint
        (the same pattern `SourceTarget.config_fingerprint` documents). A
        keyword-mode entry with no configured search terms plans no target:
        `scraper.py`'s `for term in search_terms` is a no-op in that case, so
        scheduling a request-less run would be pure overhead.

        An unconfigured or empty `companies.jibe` plans zero targets, which is
        not an error — the scheduler simply has no Jibe work.
        """
        terms = tuple(config.search_terms)
        targets: list[SourceTarget] = []
        for slug, raw_entry in config.entries(SOURCE_KEY).items():
            slug = str(slug).strip()
            if not slug:
                continue
            if not isinstance(raw_entry, Mapping):
                raise ConfigError(
                    f"jibe.{slug}: entry must be an object, got {type(raw_entry).__name__}",
                    source_key=SOURCE_KEY,
                    instance_key=slug,
                )
            base = str(raw_entry.get("base") or "").strip()
            if not base:
                raise ConfigError(
                    f"jibe.{slug}: missing required 'base'", source_key=SOURCE_KEY, instance_key=slug
                )
            name = str(raw_entry.get("name") or slug)
            mode = str(raw_entry.get("mode") or "keyword").strip().lower()
            host = urlsplit(base).hostname

            if mode == "state":
                state = str(raw_entry.get("state") or "").strip()
                if not state:
                    raise ConfigError(
                        f"jibe.{slug}: mode 'state' requires a 'state' param",
                        source_key=SOURCE_KEY,
                        instance_key=slug,
                    )
                targets.append(
                    SourceTarget(
                        source_key=SOURCE_KEY,
                        instance_key=slug,
                        label=name,
                        params={"base": base, "company": name, "mode": "state", "state": state},
                        inventory_scope=InventoryScope.PARTIAL,
                        host=host,
                    )
                )
            elif mode == "keyword":
                if not terms:
                    continue
                targets.append(
                    SourceTarget(
                        source_key=SOURCE_KEY,
                        instance_key=slug,
                        label=name,
                        params={"base": base, "company": name, "mode": "keyword", "terms": terms},
                        inventory_scope=InventoryScope.PARTIAL,
                        host=host,
                    )
                )
            else:
                raise ConfigError(
                    f"jibe.{slug}: unknown mode {mode!r} (expected 'keyword' or 'state')",
                    source_key=SOURCE_KEY,
                    instance_key=slug,
                )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Dispatch to the mode-specific walk. See `_fetch_state`/`_fetch_keyword`."""
        mode = str(target.param("mode") or "keyword")
        if mode == "state":
            async for record in self._fetch_state(target, ctx):
                yield record
        else:
            async for record in self._fetch_keyword(target, ctx):
                yield record

    async def _fetch_state(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Costco-style: walk the `state` facet up to `STATE_PAGE_LIMIT` pages.

        No retry, no sleep, no deadline branching — a failed request is always
        a raise (invariants 1-3). Stops on the first empty page (positive
        assertion: the facet is exhausted) or after `STATE_PAGE_LIMIT` pages
        (a depth cap, not evidence of exhaustion — see module docstring on why
        this target is `InventoryScope.PARTIAL` regardless).
        """
        base = str(target.require("base"))
        state = str(target.require("state"))
        page = 1
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            page = max(1, int(ctx.resume_from.cursor.get("page", 1)))
            emitted = ctx.resume_from.emitted

        while page <= STATE_PAGE_LIMIT:
            response: HttpResponse = await ctx.http().send(
                HttpRequest(
                    url=jobs_url(base),
                    params={"state": state, "limit": PAGE_SIZE, "page": page, "lang": "en-us"},
                )
            )
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            data = response.json(source_key=SOURCE_KEY, instance_key=target.instance_key)
            jobs = _extract_jobs(_decode(data, target), target)
            if not jobs:
                return
            for record in parse_jobs_page(data, target):
                yield record
                emitted += 1
            page += 1
            # Marked after this page's records were yielded (and therefore
            # pulled by the consumer), never before (Checkpoint's "delivered,
            # not committed" contract).
            ctx.mark_checkpoint({"page": page}, target=target, emitted=emitted)

    async def _fetch_keyword(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Generic Jibe: walk each search term up to `KEYWORD_PAGE_LIMIT` pages.

        One term's failed request fails the whole target (invariants 1-3); a
        term that returns an empty page simply moves on to the next term, the
        same "positive assertion this term is exhausted" logic as the state
        walk. The cursor is `{"term_index", "page"}` so a crash or resume
        continues at the exact term and page it left off — the `Checkpoint
        {term_index?, page}` shape called out in this adapter's spec.
        """
        base = str(target.require("base"))
        terms = tuple(target.param("terms") or ())
        if not terms:
            return
        term_index = 0
        page = 1
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            term_index = max(0, int(ctx.resume_from.cursor.get("term_index", 0)))
            page = max(1, int(ctx.resume_from.cursor.get("page", 1)))
            emitted = ctx.resume_from.emitted

        while term_index < len(terms):
            term = terms[term_index]
            while page <= KEYWORD_PAGE_LIMIT:
                response = await ctx.http().send(
                    HttpRequest(
                        url=jobs_url(base),
                        params={"keywords": term, "limit": PAGE_SIZE, "page": page, "lang": "en-us"},
                    )
                )
                check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
                data = response.json(source_key=SOURCE_KEY, instance_key=target.instance_key)
                jobs = _extract_jobs(_decode(data, target), target)
                if not jobs:
                    break
                for record in parse_jobs_page(data, target):
                    yield record
                    emitted += 1
                page += 1
                ctx.mark_checkpoint(
                    {"term_index": term_index, "page": page}, target=target, emitted=emitted
                )
            term_index += 1
            page = 1
            ctx.mark_checkpoint({"term_index": term_index, "page": page}, target=target, emitted=emitted)


ADAPTER = JibeAdapter()
