"""Built In job-search listing pages.

The HTML-scraping worked example: there is no JSON API, so every other
adapter's "decode bytes, walk a `Mapping`" parsing shape does not apply here.
What this adapter demonstrates beyond `greenhouse.py` / `smartrecruiters.py`:

  * NO PER-COMPANY CONFIG. Built In is a keyword search engine, not a set of
    per-employer boards, so `plan()` does not read `companies.builtin` (there
    is no such key in `config.json`). It reads `profile.search_terms` and
    produces exactly one singleton target for the whole source -- the same
    shape the contract documents for YC (`NormalizedPosting.namespace`:
    "Singleton sources ... leave `instance_key` empty").
  * ONE TARGET, THREE NESTED CURSORS. A single `fetch()` call walks two fixed
    locales x every configured search term x up to `MAX_PAGES` result pages,
    because that whole space is one PARTIAL keyword search over one site, not
    independently schedulable units the way Greenhouse boards are. The
    checkpoint cursor is therefore `{locale_index, term_index, page}` rather
    than the single `offset` a one-dimensional pager needs.
  * `InventoryScope.PARTIAL` -- this is a keyword search over an unbounded
    catalog, capped at `MAX_PAGES` pages per (locale, term). Not seeing a
    posting proves nothing about whether it still exists, so a successful run
    must never license marking anything absent (contract `InventoryScope`
    docstring). This is also why `NormalizedPosting` with no `req_id` uses
    "builtin" as its own worked example in
    `test_identity_claims_degrade_to_url_only_without_req_id`.
  * PARSING IS A PURE REGEX WALK OVER TEXT, not a JSON envelope. There is no
    top-level shape to validate the way `{"jobs": [...]}` is validated
    elsewhere, so "malformed envelope" is defined as "this does not look like
    an HTML document at all" (see `parse_listing_page`); an HTML page with
    zero matching job cards is a legitimate empty search result, not an error
    (invariant 3 still holds: `check_status` is what turns a blocked/throttled
    response into a raise, never a quietly empty list).

Identity decision (documented per the task spec, which explicitly leaves this
adapter's author to decide and justify it): `req_id` is left `None`. The only
per-job handle Built In's HTML gives us is the `/job/...` URL path itself --
there is no separate structured id field the way Greenhouse's `job.id` or
SmartRecruiters' `job.id` sit next to (and independently of) the URL. Minting
a `req_id` equal to the URL path would not be independent evidence -- it
collapses to a second, differently-shaped copy of the same `url_key`, and
`normalize_url()` already gives us a stable alias to dedupe and to key on.
Treating a URL as a source-native requisition id is exactly the shortcut
`IdentityClaim`'s docstring warns adapters away from ("A URL is NEVER globally
unique"). Records therefore carry only the rank-1 URL identity claim.

Legacy source: `scraper.py::src_builtin`. Preserved: the two fixed locales
(Bay Area metro via `city`/`state`/`country`, and US-remote via
`remote=true`/`country`), the `search`/`page` query params, `max_pages=4`,
the nearest-preceding `/company/{slug}` anchor heuristic for company name, the
`$NNK-$NNK` salary regex and its 3000-before/500-after character window, and
the `remote = locale_is_remote or ("Remote" in chunk and "Hybrid" not in
chunk)` rule. Changed: a non-200 raises a classified error instead of
`break`-ing out with whatever was already collected (invariant 3); there is
no `time.sleep` -- politeness lives on
`SourceDescriptor.min_request_interval_seconds` (invariant 2); and there is no
in-adapter `seen` set for cross-page/cross-term dedupe -- suppressing
in-run duplicates is an efficiency option the writer's identity-based dedupe
makes unnecessary (invariant 5), and cross-source merging is Phase 3 resolver
work this adapter does not touch.
"""
from __future__ import annotations

import html
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
    normalize_text,
)

__all__ = [
    "DESCRIPTOR",
    "LOCALES",
    "MAX_PAGES",
    "BuiltInAdapter",
    "listings_url",
    "parse_listing_page",
]

SOURCE_KEY = "builtin"
API_HOST = "builtin.com"
BASE_URL = "https://builtin.com"
LISTINGS_URL_TEMPLATE = "https://builtin.com/jobs"

#: Matches `scraper.py`'s `max_pages=4` default. A hard cap, not a "until
#: exhausted" walk: Built In's search is effectively unbounded, so PARTIAL
#: scope (not COMPLETE) is what tells Phase 2.4 never to infer absence from
#: reaching it.
MAX_PAGES = 4

#: The two fixed locale passes `scraper.py` walks (2026-07-18 directive:
#: Bay Area metro + US-remote, not Seattle). `label` becomes the record's
#: `location`; `remote_default` seeds the remote flag before the per-card
#: text heuristic in `parse_listing_page` can raise it further.
LOCALES: tuple[Mapping[str, Any], ...] = (
    {
        "code": "sf-metro",
        "label": "San Francisco, CA (metro)",
        "remote_default": False,
        "params": {"city": "San Francisco", "state": "CA", "country": "USA"},
    },
    {
        "code": "remote-us",
        "label": "Remote, US",
        "remote_default": True,
        "params": {"remote": "true", "country": "USA"},
    },
)

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.STARTUP_BOARD,
    # Runs in the same daily sweep `scraper.py` gave it (unconditional unless
    # `--only` excludes it), and in `FULL_DIRECT` alongside every other
    # direct/startup-board source (Phase 2.5). `InventoryScope.PARTIAL` does
    # NOT carve a source out of `FULL_DIRECT` -- `amazon`, `eightfold`,
    # `jibe`, `phenom`, and `workday` are all PARTIAL and all run there too;
    # `FULL_DIRECT` means "every direct/startup source, dueness unfiltered",
    # not "only sources with an exhaustible inventory". Reserved exclusively
    # for the `AGGREGATORS` run is the JobSpy/meta-search arm, which this is
    # not.
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    refresh_interval_seconds=6 * 3600,
    # Up to 2 locales x N search terms x MAX_PAGES sequential requests, each
    # paced by min_request_interval_seconds below. Generous relative to
    # Greenhouse's single GET because this is many round trips, not one.
    default_deadline_seconds=90.0,
    # Real pagination to resume, across two dimensions beyond just the page
    # number -- see the module docstring's cursor shape.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    max_concurrent_targets=1,
    # One search-results host, one target: keep it polite. `scraper.py` notes
    # job *detail* pages 429 aggressively; listing pages are what this adapter
    # fetches instead, but the same host still deserves a light touch.
    per_host_concurrency=2,
    # Replaces `scraper.py`'s `time.sleep(0.3)` between requests (invariant 2).
    min_request_interval_seconds=0.3,
    description_inline=False,
    default_inventory_scope=InventoryScope.PARTIAL,
)

_CARD_RE = re.compile(r'<h2[^>]*><a[^>]+href="(/job/[^"]+)"[^>]*>([^<]+)</a>')
_COMPANY_RE = re.compile(r'href="/company/([^"/]+)"')
_SALARY_RE = re.compile(r"\$([\d,]+)K?\s*[-–]\s*\$?([\d,]+)K?")

#: Matches `scraper.py`'s `chunk = t[max(0, pos - 3000):pos + 500]` window
#: used to find the salary and the "Remote"/"Hybrid" text near a job card.
_CHUNK_BEFORE = 3000
_CHUNK_AFTER = 500


def listings_url() -> str:
    return LISTINGS_URL_TEMPLATE


def _company_from_slug(slug: str) -> str:
    """`"acme-robotics"` -> `"Acme Robotics"`, matching `scraper.py`'s
    `slug.replace("-", " ").title()`."""
    return slug.replace("-", " ").title() if slug else ""


def parse_listing_page(
    payload: bytes | str, target: SourceTarget, *, locale: Mapping[str, Any]
) -> Iterator[NormalizedPosting]:
    """One Built In `/jobs` listing page -> records. Pure: no I/O, no clock.

    `locale` carries the fixed metadata (`label`, `remote_default`) for
    whichever of `LOCALES` produced this page; it is plain data, not a second
    payload, so this stays a pure function of its inputs (invariant 7).

    A response that is not text at all, or that plainly is not an HTML
    document, raises `PayloadError`: Built In changed shape and this regex
    scraper is now broken. A page that *is* HTML but matches zero job cards is
    a legitimate empty search result -- the walk yields nothing and `fetch`
    moves on, exactly as an empty JSON `jobs` array does for Greenhouse
    (invariant 3: the distinction is `check_status`, not an empty return).

    A card whose title is empty once HTML entities are decoded and whitespace
    is collapsed is skipped: it cannot be identified, and one unusable card
    must not blank a page that Phase 2.4 would otherwise treat as proof the
    rest of the search is empty.

    Yields lazily so `fetch` streams rather than materializing the page.
    """
    if isinstance(payload, (bytes, bytearray)):
        text = payload.decode("utf-8", errors="replace")
    elif isinstance(payload, str):
        text = payload
    else:
        raise PayloadError(
            f"builtin listing page: expected HTML text, got {type(payload).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    if "<html" not in text.lower():
        raise PayloadError(
            "builtin listing page: response body is not an HTML document",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    cards = [(m.start(), m.group(1), m.group(2)) for m in _CARD_RE.finditer(text)]
    if not cards:
        return

    companies = [(m.start(), m.group(1)) for m in _COMPANY_RE.finditer(text)]
    location_label = str(locale.get("label") or "")
    remote_default = bool(locale.get("remote_default"))

    for pos, path, title_html in cards:
        title = normalize_text(html.unescape(title_html))
        if not title:
            continue
        company_slug = ""
        for company_pos, slug in reversed(companies):
            if company_pos < pos:
                company_slug = slug
                break
        chunk = text[max(0, pos - _CHUNK_BEFORE) : pos + _CHUNK_AFTER]
        salary_match = _SALARY_RE.search(chunk)
        salary_text = salary_match.group(0) if salary_match else ""
        remote = remote_default or ("Remote" in chunk and "Hybrid" not in chunk)
        yield target.record(
            title=title,
            company=_company_from_slug(company_slug),
            url=f"{BASE_URL}{path}",
            location=location_label,
            # No source-native requisition id exists to extract -- see the
            # module docstring's identity decision. URL-only identity is what
            # `identity_claims()` then produces.
            req_id=None,
            salary_text=salary_text,
            remote=remote,
            extra={"locale": str(locale.get("code") or "")},
        )


class BuiltInAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`profile.search_terms` -> one singleton target for the whole source.

        Built In is not per-company config the way `companies.greenhouse` is;
        it is one search engine walked with every configured term. No search
        terms configured plans zero targets, which is not an error -- the
        scheduler simply has no Built In work.
        """
        terms = tuple(config.search_terms)
        if not terms:
            return []
        return [
            SourceTarget(
                source_key=SOURCE_KEY,
                instance_key="",
                label="Built In",
                params={"search_terms": terms},
                inventory_scope=DESCRIPTOR.default_inventory_scope,
                host=API_HOST,
            )
        ]

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Walk `LOCALES` x search terms x pages, streaming as each page parses.

        No retry, no sleep, no deadline branching -- everything this method
        knows how to do on a failed request is raise (invariants 1-3).
        Resumes from `ctx.resume_from.cursor` when the checkpoint is valid for
        this exact target (`Checkpoint.is_valid_for`, which already discards a
        checkpoint whose `search_terms` changed since it was written, because
        that changes `target.config_fingerprint()`); an absent or invalid
        checkpoint starts at `(0, 0, 1)`, which is always correct, merely
        slower.

        A page with zero job cards ends the walk for the current
        `(locale, term)` pair and advances to the next one -- `scraper.py`'s
        `if not cards: break` -- rather than being treated as a failure.
        Reaching `MAX_PAGES` does the same. `ctx.mark_checkpoint()` is called
        after each page's records have been yielded (and therefore pulled by
        the consumer), never before (`Checkpoint`'s "delivered, not
        committed" contract).
        """
        terms = tuple(str(t) for t in (target.param("search_terms") or ()))
        if not terms:
            return

        locale_index = 0
        term_index = 0
        page = 1
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            cursor = ctx.resume_from.cursor
            locale_index = max(0, int(cursor.get("locale_index", 0)))
            term_index = max(0, int(cursor.get("term_index", 0)))
            page = max(1, int(cursor.get("page", 1)))
            emitted = ctx.resume_from.emitted

        while locale_index < len(LOCALES):
            if term_index >= len(terms):
                locale_index += 1
                term_index = 0
                page = 1
                continue

            locale = LOCALES[locale_index]
            term = terms[term_index]
            params: dict[str, Any] = dict(locale["params"])
            params["search"] = term
            params["page"] = page

            response: HttpResponse = await ctx.http().send(
                HttpRequest(url=listings_url(), params=params)
            )
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            records = list(parse_listing_page(response.content, target, locale=locale))

            if not records:
                term_index += 1
                page = 1
                ctx.mark_checkpoint(
                    {"locale_index": locale_index, "term_index": term_index, "page": page},
                    target=target,
                    emitted=emitted,
                )
                continue

            for record in records:
                yield record
                emitted += 1

            page += 1
            if page > MAX_PAGES:
                term_index += 1
                page = 1
            ctx.mark_checkpoint(
                {"locale_index": locale_index, "term_index": term_index, "page": page},
                target=target,
                emitted=emitted,
            )


ADAPTER = BuiltInAdapter()
