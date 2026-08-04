"""Eightfold PCSX family — Microsoft careers and the generic Eightfold boards.

Both `src_eightfold` and `src_microsoft` in `scraper.py` hit the same API
shape (`GET {base}/api/pcsx/search?domain=&query=&location=&start=&num=`), the
only differences being which `base`/`domain` are hard-coded (Microsoft) versus
config-driven (Eightfold) and a stricter WA-only location filter on the
Microsoft side. One adapter, one instance per configured company, is chosen
over two thin adapters for that reason: two adapters parsing byte-identical
JSON would be the same code wearing two names, and the Phase 3 resolver would
have to know they were secretly the same shape. `companies.eightfold` already
holds the multi-tenant map this needs — Microsoft becomes just another entry
(`{"microsoft": {"base": "https://apply.careers.microsoft.com", "domain":
"microsoft.com", "name": "Microsoft"}}`) rather than the special case it was
in `scraper.py`. See `EightfoldAdapter.plan` for what that entry needs, and
`decisions` below for what's not yet in `config.example.json`.

What this module demonstrates beyond the Greenhouse reference:

  * `InventoryScope.PARTIAL` — this is a keyword search (`profile.search_terms`
    fan out across every configured company), not a board walk. A successful
    run proves nothing about postings the search terms did not match, so
    Phase 2.4 must never infer absence from it.
  * checkpointing across a two-dimensional cursor (`term_index`, `start`) —
    pagination resumes mid-term, and exhausting one term's results (or its
    page cap) advances to the next rather than ending the run.
  * search terms are baked into `SourceTarget.params` at `plan()` time (not
    read from `FetchContext.config` inside `fetch`) so a change to
    `profile.search_terms` changes `config_fingerprint()` and invalidates any
    stale checkpoint instead of silently resuming into a different query.

Decisions (see also the dispatching agent's `decisions` field):

  * Location parsing (`_location_text`) prefers WA/CA metros uniformly across
    every instance, including Microsoft. `src_microsoft` used a stricter
    WA-only filter (`", WA," in l or l.endswith(", WA")`); `src_eightfold`
    used the looser WA-or-CA substring check this module keeps. Unifying loses
    nothing real: the profile's current lane preference already spans both
    metros (see `career-position-2026-07` memory), and a single filter is one
    thing to get right instead of two.
  * `postedTs` is converted with `datetime.fromtimestamp(ts, tz=timezone.utc)`,
    not the naive local-time `datetime.date.fromtimestamp(ts)` `scraper.py`
    uses. Parsing must be pure (invariant 7): the naive form makes the same
    payload hash differently depending on the machine's timezone, which is a
    purity violation the legacy script could get away with but this contract
    cannot.
  * `config.example.json` / `config.json` do not yet have a `microsoft` entry
    under `companies.eightfold`. `plan()` reads whatever is there — it does
    not hard-code Microsoft's `base`/`domain` the way `scraper.py` did — so
    Microsoft simply will not run until that entry is added. Adding it is a
    one-line config change, not a code change; this agent does not touch
    config files per its deliverable boundary.
  * A per-company entry missing `base` or `domain` is skipped in `plan()`
    rather than raising `ConfigError`, mirroring how `greenhouse.plan()`
    skips an empty slug: one malformed entry in a multi-tenant map should not
    abort planning for every other configured company. `fetch()` still raises
    (via `target.require`) if it is ever reached with either missing, so a
    hand-built target cannot silently no-op.
"""
from __future__ import annotations

import datetime
import json
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

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

__all__ = ["DESCRIPTOR", "EightfoldAdapter", "search_url", "parse_search_page"]

SOURCE_KEY = "eightfold"
MICROSOFT_INSTANCE_KEY = "microsoft"
MICROSOFT_LEGACY_SOURCE = "microsoft-careers"

#: Results per page. Matches both legacy callers' `num=10`.
PAGE_SIZE = 10
#: Pages fetched per search term before moving to the next term. The legacy
#: `src_eightfold` capped at 5 (`range(5)`), `src_microsoft` at 10
#: (`range(10)`, `start >= min(count, 100)`). The higher bound is kept
#: uniformly: it costs one extra request per exhausted term at worst, and a
#: PARTIAL-scope search should not under-cover a company relative to the
#: legacy behaviour it replaces.
MAX_PAGES_PER_TERM = 10
#: Fallback search location when neither the target's own `location` entry
#: nor `profile.employer_scrape_location` is configured. Matches
#: `scraper._emp_loc`'s hard-coded fallback.
DEFAULT_LOCATION = "California, United States"

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Employer career sites move slower than the pure ATS boards this fan-out
    # sits behind; six hours matches Greenhouse's daily-refresh cadence.
    refresh_interval_seconds=6 * 3600,
    # One target can run several search terms x up to 10 pages each against a
    # single employer host; considerably more work than one Greenhouse GET.
    default_deadline_seconds=45.0,
    # Pagination is real and resumable: (term_index, start) survives a
    # mid-run crash or deadline cancellation.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Each instance is its own host (apply.careers.microsoft.com,
    # apply.starbucks.com, fortive.eightfold.ai, ...), so the per-host cap
    # only ever throttles one company against itself.
    per_host_concurrency=4,
    # Replaces `time.sleep(0.2)` between pages in both legacy functions.
    min_request_interval_seconds=0.2,
    description_inline=False,
    # A keyword search over a subset of terms and one metro preference can
    # never license "everything else is absent" (invariant-adjacent: see
    # `InventoryScope.PARTIAL`'s docstring in contract.py).
    default_inventory_scope=InventoryScope.PARTIAL,
)


def search_url(base: str) -> str:
    return f"{base.rstrip('/')}/api/pcsx/search"


# --------------------------------------------------------------------------- #
# Pure parsing
# --------------------------------------------------------------------------- #
def _decode_envelope(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Mapping[str, Any]:
    if isinstance(payload, (bytes, bytearray, str)):
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"eightfold {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload
    if not isinstance(data, Mapping):
        raise PayloadError(
            f"eightfold {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    return data


def _extract_positions(data: Mapping[str, Any], target: SourceTarget) -> list[Any]:
    """`data.positions` -> a plain list, or `PayloadError` for a broken envelope.

    A missing/wrong-shaped `data` or `positions` means the PCSX API changed
    and this adapter is broken (raise). A `positions` list that is simply
    empty is a legitimate "no more results for this page" (return `[]`), which
    is how `fetch` knows to advance past the current search term.
    """
    section = data.get("data")
    if not isinstance(section, Mapping):
        raise PayloadError(
            f"eightfold {target.instance_key}: 'data' is not an object",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    positions = section.get("positions")
    if not isinstance(positions, Sequence) or isinstance(positions, (str, bytes)):
        raise PayloadError(
            f"eightfold {target.instance_key}: 'data.positions' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    return list(positions)


def _position_url(position: Mapping[str, Any], base: str) -> str:
    """`positionUrl` (absolute or base-relative) or a constructed job-id URL."""
    raw = position.get("positionUrl")
    if raw:
        raw = str(raw).strip()
        if raw.startswith("http://") or raw.startswith("https://"):
            return raw
        return f"{base.rstrip('/')}/{raw.lstrip('/')}"
    pid = position.get("id")
    if pid is None:
        return ""
    return f"{base.rstrip('/')}/careers/job/{pid}"


def _location_text(position: Mapping[str, Any]) -> str:
    """`standardizedLocations`/`locations` (list or bare string) -> display text.

    WA and CA metros are surfaced first when present (see module docstring's
    "Decisions" for why this is now uniform across every instance); otherwise
    the first two raw locations stand in, matching the legacy fallback.
    """
    locs = position.get("standardizedLocations") or position.get("locations") or []
    if isinstance(locs, str):
        locs = [locs]
    if not isinstance(locs, Sequence) or isinstance(locs, (bytes, bytearray)):
        return ""
    names = [str(loc) for loc in locs if loc]
    preferred = [loc for loc in names if ", WA" in loc or ", CA" in loc]
    return "; ".join(preferred or names[:2])


def _posted(position: Mapping[str, Any]) -> tuple[str | None, str]:
    """`postedTs` epoch seconds -> (hashable date, raw provenance string).

    UTC, not local time (see module docstring): a pure parser's output must
    not depend on the machine it runs on. A garbage/unparseable timestamp
    degrades to `(None, str(ts))` rather than raising — one bad field on one
    row must not blank the row (invariant 3 is about the target, not a field).
    """
    ts = position.get("postedTs")
    if ts is None:
        return None, ""
    try:
        date_str = datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc).date().isoformat()
    except (OSError, OverflowError, TypeError, ValueError):
        return None, str(ts)
    return normalize_date(date_str), str(ts)


def _build_record(position: Any, target: SourceTarget, *, base: str) -> NormalizedPosting | None:
    """One PCSX position -> a record, or `None` if it cannot be identified.

    A position needs a title and a stable id to be usable; missing either
    means it cannot be shown or re-identified on the next run, so it is
    skipped rather than emitted half-formed (same rule as Greenhouse's
    title/url check).
    """
    if not isinstance(position, Mapping):
        return None
    title = position.get("name")
    if not title:
        return None
    display_id = position.get("displayJobId")
    raw_id = position.get("id")
    req_id_value = display_id if display_id not in (None, "") else raw_id
    if req_id_value is None:
        return None
    url = _position_url(position, base)
    if not url:
        return None

    posted_date, posted_raw = _posted(position)
    company = str(target.param("company") or target.label or "")
    legacy_source = str(target.param("legacy_source") or SOURCE_KEY)
    extra: dict[str, Any] = {
        "legacy_source": legacy_source,
        "domain": str(target.param("domain") or ""),
    }
    locs_raw = position.get("standardizedLocations") or position.get("locations")
    if locs_raw:
        extra["locations_raw"] = list(locs_raw) if isinstance(locs_raw, (list, tuple)) else [locs_raw]

    return target.record(
        title=str(title),
        company=company,
        url=url,
        location=_location_text(position),
        req_id=str(req_id_value),
        posted_date=posted_date,
        posted_raw=posted_raw,
        remote=(position.get("workLocationOption") == "remote"),
        extra=extra,
    )


def parse_search_page(
    payload: bytes | str | Mapping[str, Any], target: SourceTarget
) -> Iterator[NormalizedPosting]:
    """One PCSX search-response page -> records. Pure: no I/O, no clock.

    Mirrors `greenhouse.parse_board`'s contract: a malformed envelope raises
    `PayloadError` (the API changed and this adapter is broken); an individual
    unusable row is skipped so one bad row cannot blank the page. Yields
    lazily, one page at a time — `fetch` is what strings pages together.
    """
    data = _decode_envelope(payload, target)
    base = str(target.require("base"))
    for position in _extract_positions(data, target):
        record = _build_record(position, target, base=base)
        if record is not None:
            yield record


class EightfoldAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`companies.eightfold` x `profile.search_terms` -> one target per company.

        A company with no `base`/`domain` is skipped (see module docstring);
        a run with no search terms configured plans zero targets, since a
        keyword search with no keywords cannot enumerate anything.
        """
        terms = config.search_terms
        if not terms:
            return []

        default_location = str(config.profile.get("employer_scrape_location") or DEFAULT_LOCATION)
        targets: list[SourceTarget] = []
        for slug, entry in config.entries(SOURCE_KEY).items():
            slug = str(slug).strip()
            if not slug or not isinstance(entry, Mapping):
                continue
            base = str(entry.get("base") or "").strip()
            domain = str(entry.get("domain") or "").strip()
            if not base or not domain:
                continue
            name = str(entry.get("name") or slug)
            location = str(entry.get("location") or default_location)
            legacy_source = MICROSOFT_LEGACY_SOURCE if slug == MICROSOFT_INSTANCE_KEY else SOURCE_KEY
            targets.append(
                SourceTarget(
                    source_key=SOURCE_KEY,
                    instance_key=slug,
                    label=name,
                    params={
                        "base": base,
                        "domain": domain,
                        "company": name,
                        "location": location,
                        # Baked in at plan time so a search-term change shows
                        # up in config_fingerprint() and invalidates stale
                        # checkpoints instead of silently resuming a stale run
                        # into a different query.
                        "terms": terms,
                        "legacy_source": legacy_source,
                    },
                    inventory_scope=DESCRIPTOR.default_inventory_scope,
                    host=urlsplit(base).hostname,
                )
            )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Walk every configured search term to its page cap or exhaustion.

        No retry, no sleep, no deadline branching (invariants 1, 2, 8) — a
        non-200 or a malformed page raises and lets the scheduler decide.
        Checkpointed after each page's records have been yielded, cursor
        `{"term_index", "start"}", so a crash or cancellation resumes exactly
        where the stream left off (replay-safe per invariant 5: the writer
        dedupes on identity).
        """
        base = str(target.require("base"))
        domain = str(target.require("domain"))
        location = str(target.param("location") or DEFAULT_LOCATION)
        terms = tuple(target.param("terms") or ())
        if not terms:
            return

        term_index = 0
        start = 0
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            cursor = ctx.resume_from.cursor
            term_index = max(0, int(cursor.get("term_index", 0)))
            start = max(0, int(cursor.get("start", 0)))
            emitted = ctx.resume_from.emitted

        pages_this_term = start // PAGE_SIZE
        url = search_url(base)

        while term_index < len(terms):
            term = terms[term_index]
            response: HttpResponse = await ctx.http().send(
                HttpRequest(
                    url=url,
                    params={
                        "domain": domain,
                        "query": term,
                        "location": location,
                        "start": start,
                        "num": PAGE_SIZE,
                        "sort_by": "relevance",
                    },
                )
            )
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            data = response.json(source_key=SOURCE_KEY, instance_key=target.instance_key)
            positions = _extract_positions(data, target)

            if not positions:
                term_index += 1
                start = 0
                pages_this_term = 0
                ctx.mark_checkpoint({"term_index": term_index, "start": start}, target=target, emitted=emitted)
                continue

            for position in positions:
                record = _build_record(position, target, base=base)
                if record is not None:
                    emitted += 1
                    yield record

            start += PAGE_SIZE
            pages_this_term += 1
            if pages_this_term >= MAX_PAGES_PER_TERM:
                term_index += 1
                start = 0
                pages_this_term = 0
            ctx.mark_checkpoint({"term_index": term_index, "start": start}, target=target, emitted=emitted)


ADAPTER = EightfoldAdapter()
