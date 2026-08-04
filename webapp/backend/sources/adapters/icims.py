"""iCIMS Attract job-search portals ({host}.icims.com).

The regex-over-HTML worked example: iCIMS Attract exposes no JSON API for the
public search widget, only a server-rendered results page
(`GET https://{host}.icims.com/jobs/search?in_iframe=1&pr={page}`), so this
adapter parses HTML the way `scraper.py` did rather than decoding JSON. What it
demonstrates beyond `greenhouse.py` and `smartrecruiters.py`:

  * `fetch()` walks pages `pr=0..5` (iCIMS Attract portals are small; six pages
    is the cap `scraper.py` used and is preserved here), stopping the walk the
    moment a page has zero job anchors — which is the "ran off the end of the
    board" signal for this source, distinct from a page whose anchors all turn
    out to be unusable (that page still counts as non-empty and pagination
    continues, matching `scraper.py`'s `found` tally).
  * `parse_page()` is pure per page and never makes the stop/continue call
    itself; `page_has_job_anchors()` is the small pure helper `fetch()` uses
    for that decision, kept separate so a page whose only anchors are
    duplicates or icon-only (empty title) still correctly continues pagination.
  * `fetch()` keeps a `seen` set of `req_id`s across the whole per-target walk
    and suppresses an already-seen id before yielding it again. This is
    `scraper.py`'s behaviour verbatim (its `seen` set spans the same `pr` loop)
    and is an efficiency measure, not a correctness one (invariant 5): a crash
    mid-walk and a scheduler-issued retry may still re-emit ids the writer has
    already seen, and that remains safe.
  * `InventoryScope.COMPLETE` — a small Attract portal's whole board fits
    inside the six-page cap, so a walk that runs to either an empty page or
    `pr=5` has enumerated it, licensing Phase 2.4 to mark the rest absent.

Legacy source: `scraper.py::src_icims`. Preserved: the URL and query shape,
the anchor/title/location regexes verbatim, the "Title " a11y-prefix strip,
the 900-character location lookahead window, the six-page cap, and the
in-walk `seen`-id suppression. Changed: a non-200 response raises a classified
error instead of silently ending the walk with whatever was already collected
(invariant 3); pagination pacing lives on
`SourceDescriptor.min_request_interval_seconds` instead of `time.sleep(0.2)`
(invariant 2); there is no internal retry on request failure (invariant 1);
`search_terms` is dropped from the signature — `scraper.py` accepted it but
never used it for iCIMS (the docstring says multi-word keyword search on this
source is unreliable, so it enumerates the whole board unfiltered), and this
adapter does the same.
"""
from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence

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
)

__all__ = [
    "DESCRIPTOR",
    "IcimsAdapter",
    "MAX_PAGES",
    "page_has_job_anchors",
    "parse_page",
    "search_url",
]

SOURCE_KEY = "icims"

#: `scraper.py`'s hard cap: `for pr in range(0, 6)`. iCIMS Attract portals used
#: by this config are small company career sites, not enterprise job boards,
#: so six pages has always been enough to reach the trailing empty page; kept
#: verbatim rather than re-derived, since raising it is a scope decision for
#: whoever next audits a board that turns out to be larger.
MAX_PAGES = 6

SEARCH_URL_TEMPLATE = "https://{host}.icims.com/jobs/search"

#: Verbatim from `scraper.py::src_icims`. Captures, in order: (1) the apply
#: URL with its query string stripped (the `[^"]*` after group 1 consumes any
#: `?...` before the closing quote), (2) the numeric job id -- iCIMS' own
#: requisition identity and this adapter's `req_id` -- and (3) the anchor's
#: raw inner HTML, from which the title is recovered. `re.S` so `(.*?)` spans
#: the newlines real iCIMS markup wraps a link's contents in.
JOB_ANCHOR_RE = re.compile(
    r'<a[^>]+href="(https://[^"]+/jobs/(\d+)/[^"]+/job)[^"]*"[^>]*>(.*?)</a>',
    re.S,
)

#: Verbatim from `scraper.py`. iCIMS renders a location as `US-<state>-<city>`
#: somewhere in the row markup following a job's anchor; the state code is
#: strictly uppercase (real state abbreviations, not the a11y span's case).
LOCATION_RE = re.compile(r"US-([A-Z]{2})-([A-Za-z .\-]+?)[<|&]")

#: How far past an anchor's end to look for its location, verbatim from
#: `scraper.py`'s `r.text[m.end(): m.end() + 900]`.
LOCATION_WINDOW_CHARS = 900

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
#: iCIMS renders an a11y-only "Title" table-header inside the same anchor as
#: the visible title text (e.g. `<span class="visually-hidden">Title </span>
#: Support Engineer`); stripped after tag removal, verbatim from `scraper.py`.
_TITLE_PREFIX_RE = re.compile(r"^Title\s+")


def search_url(host: str) -> str:
    return SEARCH_URL_TEMPLATE.format(host=host)


def _title_from_anchor_html(inner_html: str) -> str:
    """Anchor inner HTML -> visible title, or `""` for an icon-only anchor.

    Strips tags, collapses whitespace, then drops the leading "Title " a11y
    prefix. An anchor whose only content is a decorative icon (no text nodes
    at all) reduces to `""` here and is the "malformed anchor" `parse_page`
    skips: it cannot be identified by title, matching `scraper.py`'s
    `if title: out.append(...)`.
    """
    text = _TAG_RE.sub(" ", inner_html)
    text = _WS_RE.sub(" ", text).strip()
    return _TITLE_PREFIX_RE.sub("", text)


def _location_near(text: str, end_of_anchor: int) -> str:
    """`"City, ST"` from the `US-ST-City` segment within the next 900 chars, or `""`."""
    window = text[end_of_anchor : end_of_anchor + LOCATION_WINDOW_CHARS]
    match = LOCATION_RE.search(window)
    if not match:
        return ""
    state, city = match.group(1), match.group(2).strip()
    if not city:
        return ""
    return f"{city}, {state}"


def page_has_job_anchors(html: str) -> bool:
    """Whether this page carries at least one job anchor.

    This, not `len(list(parse_page(...)))`, is `fetch()`'s stop/continue
    signal. A page whose anchors are all duplicates or icon-only (title
    reduces to `""`) still legitimately reports results for this page and
    pagination continues -- exactly `scraper.py`'s `found` tally, which counts
    every regex match before any per-row skip decision.
    """
    return JOB_ANCHOR_RE.search(html) is not None


def _decode(payload: bytes | str, target: SourceTarget) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        try:
            return bytes(payload).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PayloadError(
                f"icims {target.instance_key}: body is not valid utf-8: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    # Defensive: the transport shell only ever hands parse_page a decoded
    # response body (str) or raw bytes. Anything else means this parser was
    # called with the wrong kind of payload -- the source-equivalent of
    # Greenhouse's "expected an object" envelope check -- and the source
    # changed shape enough that this adapter needs a human, not a retry.
    raise PayloadError(
        f"icims {target.instance_key}: expected HTML text, got {type(payload).__name__}",
        source_key=SOURCE_KEY,
        instance_key=target.instance_key,
    )


def parse_page(payload: bytes | str, target: SourceTarget) -> Iterator[NormalizedPosting]:
    """One iCIMS search-results page -> records. Pure: no I/O, no clock.

    Never raises for an ordinary page, including a genuinely empty one --
    "zero anchors" is a fact about this page's content, not a malformed
    envelope, so it yields nothing rather than raising (see
    `page_has_job_anchors` for how `fetch()` uses that fact to stop paging).
    `PayloadError` is reserved for a payload that is not decodable text at
    all, i.e. this parser was handed something other than an HTML response
    body.

    An anchor missing a usable title (icon-only, or a decorative row) is
    skipped -- it cannot be identified -- without failing the rest of the
    page, the same "one bad row must not blank the page" rule `greenhouse.py`
    and `smartrecruiters.py` follow. Duplicate suppression across anchors is
    deliberately not this function's job; see the module docstring.
    """
    text = _decode(payload, target)
    company = str(target.param("company") or target.label or "")
    for match in JOB_ANCHOR_RE.finditer(text):
        url, job_id, inner_html = match.groups()
        title = _title_from_anchor_html(inner_html)
        if not title:
            continue
        location = _location_near(text, match.end())
        yield target.record(
            title=title,
            company=company,
            url=url,
            location=location,
            # The numeric job id embedded in the apply URL is iCIMS' own
            # requisition identity: stable across title/location edits and
            # namespaced by host (target.record stamps instance_key), so two
            # portals can never collide.
            req_id=job_id,
        )


DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Direct ATS boards are the daily-refresh backbone, same reasoning as
    # Greenhouse and SmartRecruiters.
    refresh_interval_seconds=4 * 3600,
    # Up to six sequential full-HTML-page GETs per target (no parallelism
    # within one target: each page's URL depends on nothing from the last,
    # but the walk still only knows to stop after seeing a page, so it
    # cannot fan the requests out).
    default_deadline_seconds=60.0,
    # Real pagination to resume: a crash mid-walk should pick up at the next
    # page rather than re-fetching pages already yielded.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Each target is its own subdomain ({host}.icims.com), so per-host
    # concurrency never actually contends across targets; the default is
    # kept only because one target's own walk is already serial.
    per_host_concurrency=4,
    # Replaces scraper.py's `time.sleep(0.2)` between pages.
    min_request_interval_seconds=0.2,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)


class IcimsAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"careers-fhcrc": {"name": "Fred Hutchinson Cancer Center"}}` -> one target per host.

        An unconfigured or empty `companies.icims` plans zero targets, which
        is not an error -- the scheduler simply has no iCIMS work. A
        configured host whose entry is not an object is a config mistake
        worth failing loudly on rather than silently mis-scraping.
        """
        targets: list[SourceTarget] = []
        for host, entry in config.entries(SOURCE_KEY).items():
            host = str(host).strip()
            if not host:
                continue
            if not isinstance(entry, Mapping):
                raise ConfigError(
                    f"companies.icims.{host} must be an object, got {type(entry).__name__}",
                    source_key=SOURCE_KEY,
                    instance_key=host,
                )
            name = str(entry.get("name") or host)
            targets.append(
                SourceTarget(
                    source_key=SOURCE_KEY,
                    instance_key=host,
                    label=name,
                    params={"host": host, "company": name},
                    inventory_scope=DESCRIPTOR.default_inventory_scope,
                    host=f"{host}.icims.com",
                )
            )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Walk `pr=0..MAX_PAGES-1`, streaming records as each page parses.

        No retry, no sleep, no deadline branching -- everything this method
        knows how to do on a failed request is raise (invariants 1-3).
        Resumes from `ctx.resume_from.cursor["next_page"]` when the checkpoint
        is valid for this exact target (`Checkpoint.is_valid_for`); an
        invalid or absent checkpoint starts at page 0, which is always
        correct, merely slower. Stops on the first page with no job anchors,
        or after `MAX_PAGES` pages, whichever comes first.
        """
        host = str(target.require("host"))
        page = 0
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            page = max(0, int(ctx.resume_from.cursor.get("next_page", 0)))
            emitted = ctx.resume_from.emitted

        seen: set[str] = set()
        while page < MAX_PAGES:
            response: HttpResponse = await ctx.http().send(
                HttpRequest(
                    url=search_url(host),
                    params={"in_iframe": "1", "pr": page},
                )
            )
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            # Decoded here (not via `HttpResponse.text`, which silently
            # replaces bad bytes) so a body that is not valid UTF-8 -- the
            # host is serving something other than the HTML this adapter
            # understands -- raises `PayloadError` instead of being scraped
            # as mojibake, mirroring `greenhouse.py` handing `parse_board` the
            # raw response bytes rather than a pre-decoded string.
            text = _decode(response.content, target)
            if not page_has_job_anchors(text):
                return
            for record in parse_page(text, target):
                if record.req_id in seen:
                    continue
                seen.add(record.req_id or "")
                yield record
                emitted += 1
            page += 1
            # Marked after this page's records were yielded (and therefore
            # pulled by the consumer), never before (Checkpoint's "delivered,
            # not committed" contract).
            ctx.mark_checkpoint({"next_page": page}, target=target, emitted=emitted)


ADAPTER = IcimsAdapter()
