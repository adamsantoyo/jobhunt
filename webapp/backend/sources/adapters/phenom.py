"""Phenom People career sites — the search-term fan-out worked example.

Phenom-powered career sites (Seattle Children's, Rocket/Redfin, and many other
mid-size employers) expose no full-inventory listing endpoint: the only public
surface is a keyword search widget (`POST {base}/widgets` with a
`refineSearch` payload). What this adapter demonstrates beyond
`greenhouse.py` and `smartrecruiters.py`:

  * `fetch()` owns TWO nested loops instead of one: search terms fan out
    (`config.search_terms`, baked into the target's params at `plan()` time so
    a change to `profile.search_terms` invalidates any stored checkpoint via
    `SourceTarget.config_fingerprint()`) and, within each term, up to
    `PAGES_PER_TERM` offset pages. `parse_page()` stays pure and handles
    exactly one already-fetched page for one term; the fan-out, the paging
    cap, and the "empty page ends this term early" rule live in the transport
    shell because none of it is payload parsing.
  * `ctx.mark_checkpoint()` is called once per page with cursor
    `{"term_index", "from"}`, so a crash or scheduler-issued retry resumes at
    the exact (term, offset) pair rather than re-walking terms already
    exhausted. Replaying the same page is still safe (invariant 5): the same
    job commonly matches more than one search term, so cross-term duplicate
    emission is expected, not suppressed.
  * `InventoryScope.PARTIAL` — a keyword search proves nothing about jobs that
    do not match any configured term. A successful run must never license
    Phase 2.4 to mark unseen postings on this board absent.

Legacy source: `scraper.py::src_phenom`. Preserved: the `refineSearch`
request envelope (`lang`, `ddoKey`, `sortBy`, `all_fields`, `selected_fields`,
`locationData`, ...), `size=50` paging, the two-page-per-term cap
(`from` in `(0, size)`), and the field mapping (`title`;
`cityStateCountry`/`location`/`cityState` -> location; `applyUrl` or
`{base}/job/{jobId}` -> url; `postedDate`/`dateCreated` (first 10 chars) ->
posted; `jobId`/`jobSeqNo` -> req_id). Changed: a non-200 raises a classified
error instead of `scraper.py`'s bare `break` (which silently returned
whatever had been collected so far and made a blocked term indistinguishable
from an exhausted one — invariant 3); there is no in-adapter `try/except
Exception: break`; there is no `time.sleep` (`scraper.src_phenom` had none to
begin with, so `min_request_interval_seconds` stays at its default); and the
legacy `seen` set that suppressed cross-term duplicates within one call is
dropped — invariant 5 makes the writer responsible for that, not the adapter.
Cross-source dedupe (`scraper.dedupe`) is Phase 3 resolver work, out of scope
here.
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

__all__ = ["DESCRIPTOR", "PhenomAdapter", "parse_page", "widgets_url"]

SOURCE_KEY = "phenom"

#: Matches `scraper.py`'s `size` default and its two-page cap (`from` in
#: `(0, size)`). Phenom widgets accept larger pages, but the legacy scraper
#: never asked for more and there is no evidence it would help precision.
PAGE_SIZE = 50
PAGES_PER_TERM = 2


def widgets_url(base: str) -> str:
    return f"{base.rstrip('/')}/widgets"


def job_url(base: str, job_id: Any) -> str:
    return f"{base.rstrip('/')}/job/{job_id}"


DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # A direct company career site; same daily-refresh cadence as the other
    # ATS-shaped direct sources.
    refresh_interval_seconds=4 * 3600,
    # One target can issue up to `len(search_terms) * PAGES_PER_TERM`
    # sequential POSTs (profile ships 4 terms today: up to 8 requests). Well
    # above Greenhouse's single-GET 20s, short of SmartRecruiters' single-query
    # pagination budget scaled by the term fan-out.
    default_deadline_seconds=60.0,
    # Real pagination *and* a real term cursor to resume: a crash mid-walk
    # should pick up at the exact (term_index, from) pair rather than
    # re-running terms already exhausted.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # Each configured entry is its own employer's career site (its own host),
    # so this rarely binds; kept conservative like the other adapters absent a
    # measured baseline.
    per_host_concurrency=4,
    # `scraper.src_phenom` had no `time.sleep` between requests, so there is no
    # legacy politeness floor to preserve.
    min_request_interval_seconds=0.0,
    description_inline=False,
    # Keyword search only: no endpoint enumerates a Phenom site's full
    # inventory, so a successful run never licenses absence marking.
    default_inventory_scope=InventoryScope.PARTIAL,
)


def _search_payload(term: str, frm: int, *, size: int = PAGE_SIZE) -> dict[str, Any]:
    """The `refineSearch` request envelope, byte-for-byte what
    `scraper.src_phenom` posted. Kept exactly as the legacy scraper built it:
    several of these fields (`ddoKey`, `pageName`) are almost certainly load-
    bearing for which widget instance answers, and there is no upside to
    guessing which ones are safe to drop."""
    return {
        "lang": "en_us",
        "deviceType": "desktop",
        "country": "us",
        "pageName": "search-results",
        "ddoKey": "refineSearch",
        "sortBy": "",
        "from": frm,
        "jobs": True,
        "counts": True,
        "all_fields": ["category", "state", "city"],
        "size": size,
        "keywords": term,
        "global": True,
        "selected_fields": {},
        "locationData": {},
    }


def _job_list(data: Mapping[str, Any]) -> Sequence[Any] | None:
    """`data["refineSearch"]["data"]["jobs"]`, or `None` if any level is not
    the shape expected. `None` (rather than an empty list) is the malformed-
    envelope signal both `parse_page` and `_page_meta` key on."""
    refine = data.get("refineSearch")
    if not isinstance(refine, Mapping):
        return None
    inner = refine.get("data")
    if not isinstance(inner, Mapping):
        return None
    jobs = inner.get("jobs")
    if not isinstance(jobs, Sequence) or isinstance(jobs, (str, bytes)):
        return None
    return jobs


def _page_meta(data: Mapping[str, Any]) -> int:
    """Raw job count on this page, for the pagination stop decision only.

    Read only after `parse_page` has already validated the envelope in the
    same iteration (the established pattern from `smartrecruiters._page_meta`
    — see that module), so this never re-raises; a shape it cannot recognize
    is treated as `0`, ending the term's walk rather than looping forever.
    """
    jobs = _job_list(data)
    return len(jobs) if jobs is not None else 0


def _posted(job: Mapping[str, Any]) -> tuple[str | None, str]:
    """`postedDate` or `dateCreated` -> (hashable date, raw first-10-chars).

    `posted_raw` keeps `scraper.py`'s `[:10]` truncation; `posted_date` runs
    the full raw value through `normalize_date`, which anchors on the same
    `YYYY-MM-DD` prefix so the two never disagree.
    """
    raw = str(job.get("postedDate") or job.get("dateCreated") or "")
    return normalize_date(raw), raw[:10]


def parse_page(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Iterator[NormalizedPosting]:
    """One Phenom `refineSearch` page -> records. Pure: no I/O, no clock.

    A malformed envelope (not JSON, or missing `refineSearch.data.jobs`)
    raises `PayloadError`: the widget API changed and this adapter is broken.
    `scraper.src_phenom`'s `.get("refineSearch", {}).get("data", {}).get(
    "jobs", [])` chain swallowed that same failure into a silent empty list,
    which is indistinguishable from a term genuinely matching nothing
    (invariant 3) — this adapter raises instead, matching `greenhouse.py` and
    `smartrecruiters.py`. An individual item missing a title, or with neither
    an `applyUrl` nor an id to build a fallback apply URL from, is skipped: it
    cannot be identified or opened, and one bad row must not blank a page that
    Phase 2.4 would then treat as proof the rest of the board is empty.

    Pagination and term bookkeeping are deliberately not this function's job;
    it parses exactly the page it was handed.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"phenom {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"phenom {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    jobs = _job_list(data)
    if jobs is None:
        raise PayloadError(
            f"phenom {target.instance_key}: 'refineSearch.data.jobs' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    base = str(target.param("base") or "").rstrip("/")
    company = str(target.param("company") or target.label or "")
    for job in jobs:
        if not isinstance(job, Mapping):
            continue
        title = job.get("title")
        if not title:
            continue
        job_id = job.get("jobId") or job.get("jobSeqNo")
        apply_url = job.get("applyUrl")
        url = apply_url or (job_url(base, job_id) if job_id and base else None)
        if not url:
            continue
        location = job.get("cityStateCountry") or job.get("location") or job.get("cityState") or ""
        posted_date, posted_raw = _posted(job)
        extra: dict[str, Any] = {}
        job_seq_no = job.get("jobSeqNo")
        if job_seq_no is not None and str(job_seq_no) != str(job_id):
            # The board's own sequence number, distinct from the id used as
            # identity when both are present. Kept as Phase 3 provenance only,
            # never as identity (see module docstring / IdentityClaim).
            extra["job_seq_no"] = str(job_seq_no)
        yield target.record(
            title=str(title),
            company=company,
            url=str(url),
            location=str(location),
            # `jobId` (falling back to `jobSeqNo` when absent) is the
            # source-native requisition identity: it is what the apply URL
            # embeds and what survives title and location edits. Namespaced by
            # board (`target.record` stamps `instance_key`), so two boards can
            # never collide.
            req_id=str(job_id) if job_id is not None else None,
            posted_date=posted_date,
            posted_raw=posted_raw,
            extra=extra,
        )


class PhenomAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"seattlechildrens": {"base": "...", "name": "..."}}` -> one target
        per entry.

        `search_terms` is baked into the target's params (not read fresh from
        `ctx.config` inside `fetch`) so that `SourceTarget.config_fingerprint()`
        — and therefore `Checkpoint.is_valid_for()` — changes when
        `profile.search_terms` changes. Resuming a checkpoint taken under a
        different set of search terms would resume into a different result
        set, which is exactly what checkpoint scoping exists to prevent.

        An unconfigured or empty `companies.phenom` plans zero targets, which
        is not an error — the scheduler simply has no Phenom work. An entry
        that is present but not an object, or has no `base`, cannot be turned
        into a target at all, so it raises `ConfigError` rather than being
        silently skipped like a blank slug would be.
        """
        targets: list[SourceTarget] = []
        for slug, entry in config.entries(SOURCE_KEY).items():
            slug = str(slug).strip()
            if not slug:
                continue
            if not isinstance(entry, Mapping):
                raise ConfigError(
                    f"companies.{SOURCE_KEY}.{slug} must be an object with base/name, "
                    f"got {type(entry).__name__}",
                    source_key=SOURCE_KEY,
                    instance_key=slug,
                )
            base = str(entry.get("base") or "").strip().rstrip("/")
            if not base:
                raise ConfigError(
                    f"companies.{SOURCE_KEY}.{slug} is missing 'base'",
                    source_key=SOURCE_KEY,
                    instance_key=slug,
                )
            name = str(entry.get("name") or slug)
            targets.append(
                SourceTarget(
                    source_key=SOURCE_KEY,
                    instance_key=slug,
                    label=name,
                    params={
                        "base": base,
                        "company": name,
                        "search_terms": config.search_terms,
                    },
                    inventory_scope=DESCRIPTOR.default_inventory_scope,
                    host=urlsplit(base).hostname or None,
                )
            )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Walk every search term, up to `PAGES_PER_TERM` pages each, streaming
        records as each page parses.

        No retry, no sleep, no deadline branching — everything this method
        knows how to do on a failed request is raise (invariants 1-3). Resumes
        from `ctx.resume_from.cursor["term_index"]` / `["from"]` when the
        checkpoint is valid for this exact target (`Checkpoint.is_valid_for`,
        which also compares the baked-in `search_terms`); an invalid or absent
        checkpoint starts at term 0, offset 0, which is always correct, merely
        slower.

        A term's walk ends early — before `PAGES_PER_TERM` is reached — the
        moment a page comes back with zero raw jobs, matching
        `scraper.src_phenom`'s `if not jobs: break`. Reaching the page cap ends
        it too, even if the last page was full: `scraper.src_phenom` only ever
        tried `from` in `(0, size)`, never a third page.
        """
        base = str(target.require("base")).rstrip("/")
        terms = tuple(str(t) for t in (target.param("search_terms") or ()) if t)
        if not terms:
            return

        term_index = 0
        frm = 0
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            term_index = max(0, min(len(terms), int(ctx.resume_from.cursor.get("term_index", 0))))
            frm = max(0, int(ctx.resume_from.cursor.get("from", 0)))
            emitted = ctx.resume_from.emitted

        url = widgets_url(base)
        while term_index < len(terms):
            term = terms[term_index]
            response: HttpResponse = await ctx.http().send(
                HttpRequest(url=url, method="POST", json_body=_search_payload(term, frm))
            )
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            data = response.json(source_key=SOURCE_KEY, instance_key=target.instance_key)
            for record in parse_page(data, target):
                yield record
                emitted += 1
            raw_count = _page_meta(data)
            page_number = frm // PAGE_SIZE
            if raw_count == 0 or page_number + 1 >= PAGES_PER_TERM:
                term_index += 1
                frm = 0
            else:
                frm += PAGE_SIZE
            # Marked after this page's records were yielded (and therefore
            # pulled by the consumer), never before (Checkpoint's "delivered,
            # not committed" contract).
            ctx.mark_checkpoint({"term_index": term_index, "from": frm}, target=target, emitted=emitted)


ADAPTER = PhenomAdapter()
