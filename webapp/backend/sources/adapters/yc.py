"""Y Combinator jobs board — a single startup-board site crawled by facet, not paged.

Unlike every other adapter in this package, YC is not a multi-tenant ATS with
one target per company: it is one site (`ycombinator.com/jobs`) that lists
postings from every YC-backed startup at once. `plan()` therefore returns
exactly one singleton `SourceTarget` (empty `instance_key`, per
`NormalizedPosting.namespace`'s doc: "Singleton sources ... leave
`instance_key` empty and namespace on the source key alone").

What this module demonstrates beyond the reference adapters:

  * NO JSON API. The site server-renders an Inertia-style page whose entire
    prop tree — including the full `jobPostings` array — is embedded as
    HTML-escaped JSON in a `data-page="..."` attribute. `fetch()` still issues
    plain GETs; the payload just happens to be HTML wrapping JSON rather than
    JSON on the wire.
  * A DISCOVERED, NOT CONFIGURED, crawl frontier. `/jobs` itself does not
    enumerate every posting (see `decisions` below), so the same page also
    hands back `props.jobRoles` / `props.jobLocations`, and every
    `/jobs/role/{slug}` and `/jobs/location/{slug}` page is crawled in turn.
    The frontier is discovered from the site's own JSON, not read from
    `config.json` (there is nothing per-target to configure), which is why
    `parse_page` returns `(records, discovered_paths)` rather than just
    records — the two are peers, not because pagination has metadata the
    caller needs (SmartRecruiters' `totalFound`), but because the crawl
    frontier is itself part of what one page's payload contains.
  * CROSS-PATH IDENTITY COLLISION BY DESIGN. The same posting legitimately
    appears on `/jobs`, on its role page, and on its location page. `fetch()`
    suppresses same-run repeats with an in-memory `seen` set keyed on the
    record's own identity (req_id, falling back to the normalized URL) —
    an efficiency measure invariant 5 explicitly permits, never a substitute
    for the writer's cross-run dedupe.
  * `InventoryScope.COMPLETE` for the one target: the multi-path crawl exists
    specifically to enumerate the board in full (see `decisions`), unlike the
    keyword fan-outs (Eightfold, Workday, Amazon) that are inherently PARTIAL
    because they only ever see what a chosen search term happens to match.

Legacy source: `scraper.py::src_yc` / `_yc_page`. Preserved: the `data-page`
extraction, the `/jobs` + role + location path crawl, the title-cased
`/companies/{slug}/` company inference (falling back to `"YC startup"`), the
"remote" substring test against the location string, and per-run dedupe on
the posting's own id. Changed: a non-200 or an unparseable envelope raises
instead of `_yc_page`'s `except Exception: return {}` (invariant 3 — a
swallowed exception there made a blocked crawl indistinguishable from a
board with zero role/location facets); no `time.sleep` (there was none to
begin with — see `decisions`); the posting's native `id` is now surfaced as
`req_id` for identity, which `rec()` never did in `scraper.py`.
"""
from __future__ import annotations

import html
import json
import re
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
)

__all__ = ["DESCRIPTOR", "YcAdapter", "page_url", "parse_page"]

SOURCE_KEY = "yc"
SITE_HOST = "www.ycombinator.com"
SITE_ROOT = "https://www.ycombinator.com"
ROOT_PATH = "/jobs"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.STARTUP_BOARD,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # One site, refreshed on the same cadence as the direct ATS boards.
    refresh_interval_seconds=4 * 3600,
    # A full crawl visits `/jobs` plus one page per role and per location the
    # site currently declares — dozens of sequential HTML GETs against one
    # host, considerably more than a single Greenhouse or SmartRecruiters
    # request train.
    default_deadline_seconds=60.0,
    # Real, resumable pagination: the frontier (`paths`) and how far into it
    # the crawl got (`path_index`) survive a mid-run crash or cancellation.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Singleton target, one host: nothing else competes for this limiter.
    per_host_concurrency=4,
    # `scraper.py::_yc_page` had no `time.sleep` between requests (only
    # `src_builtin` does); preserved as-is rather than inventing a politeness
    # floor the legacy crawl never observed.
    min_request_interval_seconds=0.0,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)

_DATA_PAGE_RE = re.compile(r'data-page="([^"]*)"')
_COMPANY_URL_RE = re.compile(r"^/companies/([^/]+)/")


def page_url(path: str) -> str:
    return f"{SITE_ROOT}{path}"


# --------------------------------------------------------------------------- #
# Pure parsing
# --------------------------------------------------------------------------- #
def _extract_props(html_text: str, target: SourceTarget) -> Mapping[str, Any]:
    """Pull `props` out of the page's `data-page="<HTML-escaped JSON>"` attribute.

    Anything short of a well-formed `{"props": {...}}` envelope means the site
    changed and this adapter is broken (`PayloadError`, permanent): a missing
    attribute, unparseable JSON, or a non-object top level or `props` are all
    the same failure the way a non-list `jobs` is for Greenhouse.
    """
    match = _DATA_PAGE_RE.search(html_text)
    if not match:
        raise PayloadError(
            f"yc {target.instance_key or 'jobs'}: no data-page attribute found",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    try:
        payload = json.loads(html.unescape(match.group(1)))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PayloadError(
            f"yc {target.instance_key or 'jobs'}: data-page is not JSON: {exc}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        ) from exc
    if not isinstance(payload, Mapping):
        raise PayloadError(
            f"yc {target.instance_key or 'jobs'}: data-page is not an object",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    props = payload.get("props")
    if not isinstance(props, Mapping):
        raise PayloadError(
            f"yc {target.instance_key or 'jobs'}: 'props' is not an object",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    return props


def _facet_paths(entries: Any, template: str) -> list[str]:
    """`[{"slug": "engineering"}, ...]` -> `["/jobs/role/engineering", ...]`.

    Defensive, not strict: a missing or malformed `jobRoles`/`jobLocations`
    list means fewer facets to crawl, not a broken board — the postings this
    page itself carries are still real and must still surface (matching
    SmartRecruiters' `_page_meta` treating a missing `totalFound` as `0`
    rather than raising).
    """
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        return []
    paths: list[str] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        slug = str(entry.get("slug") or "").strip()
        if slug:
            paths.append(template.format(slug=slug))
    return paths


def _discover_paths(props: Mapping[str, Any]) -> tuple[str, ...]:
    """Every `/jobs/role/{slug}` and `/jobs/location/{slug}` this page names."""
    return tuple(
        _facet_paths(props.get("jobRoles"), "/jobs/role/{slug}")
        + _facet_paths(props.get("jobLocations"), "/jobs/location/{slug}")
    )


def _absolutize(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.startswith("/"):
        return f"{SITE_ROOT}{raw}"
    return raw


def _company_from_url(raw_url: Any) -> str:
    """`"/companies/example-inc/jobs/123-title"` -> `"Example Inc"`.

    Matched against the posting's own (relative) `url` field, exactly as
    `scraper.py::src_yc` does (`re.match(..., j.get("url") or "")`) — not
    against `applyUrl`, which routes off-site and carries no `/companies/`
    slug. Absent a match, `"YC startup"` stands in, matching the legacy
    fallback verbatim.
    """
    if not isinstance(raw_url, str):
        return "YC startup"
    match = _COMPANY_URL_RE.match(raw_url)
    if not match:
        return "YC startup"
    name = match.group(1).replace("-", " ").title().strip()
    return name or "YC startup"


def _build_record(job: Any, target: SourceTarget) -> NormalizedPosting | None:
    """One `jobPostings` entry -> a record, or `None` if it cannot be identified.

    A posting needs a title and at least one usable URL (`url` or
    `applyUrl`); missing both means nothing can be shown or opened, so it is
    skipped rather than emitted half-formed (same rule as every other
    adapter's title/url guard). A missing/`null` `id` degrades to no `req_id`
    rather than a skip — the URL identity claim still stands on its own.
    """
    if not isinstance(job, Mapping):
        return None
    title = job.get("title")
    if not title:
        return None

    raw_url = job.get("url")
    url_field = _absolutize(str(raw_url or ""))
    apply_field = _absolutize(str(job.get("applyUrl") or ""))
    primary_url = url_field or apply_field
    if not primary_url:
        return None
    alt_urls = tuple(u for u in (apply_field,) if u and u != primary_url)

    location = str(job.get("location") or "")
    raw_id = job.get("id")
    req_id = str(raw_id) if raw_id not in (None, "") else None

    return target.record(
        title=str(title),
        company=_company_from_url(raw_url),
        url=primary_url,
        location=location,
        req_id=req_id,
        remote="remote" in location.lower(),
        alt_urls=alt_urls,
    )


def parse_page(payload: bytes | str, target: SourceTarget) -> tuple[Iterator[NormalizedPosting], tuple[str, ...]]:
    """One YC jobs-board HTML page -> (records, newly discovered facet paths).

    Pure: no I/O, no clock. Records are yielded lazily (invariant 6); the
    discovered-paths tuple is returned eagerly because it comes from a
    sibling JSON key (`jobRoles`/`jobLocations`) the caller needs *before* it
    decides what to crawl next, not from consuming the postings themselves.

    A malformed envelope (see `_extract_props`) or a `jobPostings` that is not
    a list raises `PayloadError`: the site changed and this adapter is
    broken. An individual unusable posting is skipped, never the whole page
    (invariant 3 — one bad row must not blank a page Phase 2.4 would then
    treat as proof the rest of the board is empty).
    """
    html_text = payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else str(payload)
    props = _extract_props(html_text, target)
    postings = props.get("jobPostings")
    if not isinstance(postings, Sequence) or isinstance(postings, (str, bytes)):
        raise PayloadError(
            f"yc {target.instance_key or 'jobs'}: 'jobPostings' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    discovered = _discover_paths(props)

    def _records() -> Iterator[NormalizedPosting]:
        for job in postings:
            record = _build_record(job, target)
            if record is not None:
                yield record

    return _records(), discovered


class YcAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """Always exactly one target: YC has one board, not a company map.

        Unlike `companies.greenhouse`/`companies.smartrecruiters`, there is
        nothing per-instance to read from `config.json` — the whole point of
        the facet crawl is to reach every company on the one site. `config`
        is accepted (to satisfy `SourceAdapter`) but unused, matching how
        Greenhouse's `plan()` returns `[]` for no config: here there is
        always exactly one thing to plan, never zero.
        """
        return (
            SourceTarget(
                source_key=SOURCE_KEY,
                instance_key="",
                label="Y Combinator Jobs",
                params={},
                inventory_scope=DESCRIPTOR.default_inventory_scope,
                host=SITE_HOST,
            ),
        )

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Crawl `/jobs`, then every role and location path it (or any later
        page) names, streaming records as each page parses.

        No retry, no sleep, no deadline branching (invariants 1, 2, 8) — a
        non-200 or a malformed page raises and lets the scheduler decide.
        Resumes from `ctx.resume_from.cursor` (`{"path_index", "paths"}`)
        when valid for this target; an absent or invalid checkpoint starts
        clean at `/jobs`, which is always correct, merely slower.

        The crawl frontier (`paths`) grows as pages are visited: any page can
        in principle name role/location facets, not only `/jobs`, so newly
        seen paths are appended (never duplicated, via `known`) regardless of
        where in the walk they turn up. `seen` suppresses the cross-path
        repeats invariant 5 anticipates (the same posting is listed on
        `/jobs`, its role page, and its location page) as an efficiency
        measure only — replaying this target from an empty checkpoint would
        re-emit everything, which is expected and safe.
        """
        paths: list[str] = [ROOT_PATH]
        path_index = 0
        emitted = 0

        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            cursor_paths = ctx.resume_from.cursor.get("paths")
            if (
                isinstance(cursor_paths, Sequence)
                and not isinstance(cursor_paths, (str, bytes))
                and cursor_paths
            ):
                paths = [str(p) for p in cursor_paths]
            path_index = max(0, int(ctx.resume_from.cursor.get("path_index", 0)))
            emitted = ctx.resume_from.emitted

        known: set[str] = set(paths)
        seen: set[str] = set()

        while path_index < len(paths):
            path = paths[path_index]
            response: HttpResponse = await ctx.http().send(HttpRequest(url=page_url(path)))
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            records, discovered = parse_page(response.content, target)
            for new_path in discovered:
                if new_path not in known:
                    known.add(new_path)
                    paths.append(new_path)
            for record in records:
                identity_key = record.req_id or record.url_key
                if identity_key in seen:
                    continue
                seen.add(identity_key)
                emitted += 1
                yield record
            path_index += 1
            ctx.mark_checkpoint(
                {"path_index": path_index, "paths": list(paths)}, target=target, emitted=emitted
            )


ADAPTER = YcAdapter()
