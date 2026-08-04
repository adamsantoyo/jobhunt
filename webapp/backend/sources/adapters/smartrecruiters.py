"""SmartRecruiters company postings boards.

The pagination worked example: one company board is enumerated as a sequence
of offset-paged responses rather than Greenhouse's single GET, so this adapter
is where the contract's checkpoint machinery earns its keep. What it
demonstrates beyond `greenhouse.py`:

  * `fetch()` owns the pagination loop. `parse_page()` stays pure and handles
    exactly one already-fetched page; the loop, the offset arithmetic, and the
    stopping condition (`offset >= totalFound` or an empty page) live in the
    transport shell because they are unrelated to parsing a payload.
  * `ctx.mark_checkpoint()` is called once per page, after that page's records
    have been yielded, so a crash or a scheduler-issued retry can resume at
    the next offset without re-walking pages already delivered (though
    replaying them would still be safe — invariant 5).
  * `InventoryScope.COMPLETE` — a run that pages to `totalFound` (or to an
    empty page) has enumerated the whole board, so a successful run does
    license Phase 2.4 to mark the rest absent, exactly as for Greenhouse.

Legacy source: `scraper.py::src_smartrecruiters`. Preserved: the field
mapping (`name`->title, joined `location`->location, `location.remote`
->remote, `releasedDate[:10]`->posted, `id`->req_id, the constructed
`jobs.smartrecruiters.com` apply URL), the `limit=100` page size, and the
`offset += len(content)` / `offset >= totalFound` stopping rule. Changed:
a non-200 raises instead of silently ending the walk with whatever was
already collected (invariant 3), and there is no `time.sleep` — politeness
lives on `SourceDescriptor.min_request_interval_seconds` (invariant 2).
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

__all__ = ["DESCRIPTOR", "SmartRecruitersAdapter", "job_url", "parse_page", "postings_url"]

SOURCE_KEY = "smartrecruiters"
API_HOST = "api.smartrecruiters.com"
POSTINGS_URL_TEMPLATE = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"
JOB_URL_TEMPLATE = "https://jobs.smartrecruiters.com/{slug}/{id}"

#: Matches `scraper.py`'s page size. Larger pages mean fewer round trips per
#: board; SmartRecruiters accepts up to 100.
PAGE_SIZE = 100

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.DIRECT,
    run_kinds=frozenset({RunKind.DAILY, RunKind.FULL_DIRECT}),
    # Direct ATS boards are the daily-refresh backbone, same reasoning as
    # Greenhouse.
    refresh_interval_seconds=4 * 3600,
    # A single page covers most boards, but pagination means several round
    # trips are possible for a large one; more headroom than Greenhouse's
    # single-request 20s.
    default_deadline_seconds=45.0,
    # Real pagination to resume: a crash mid-walk should pick up at the last
    # completed offset rather than re-fetching pages already yielded.
    supports_checkpoint=True,
    execution=ExecutionMode.ASYNC_INPROCESS,
    transport=TransportKind.HTTP,
    # One shared API host across every SmartRecruiters company; keep it modest
    # since one target now issues several sequential requests instead of one.
    per_host_concurrency=4,
    min_request_interval_seconds=0.0,
    description_inline=False,
    default_inventory_scope=InventoryScope.COMPLETE,
)


def postings_url(slug: str) -> str:
    return POSTINGS_URL_TEMPLATE.format(slug=slug)


def job_url(slug: str, job_id: Any) -> str:
    return JOB_URL_TEMPLATE.format(slug=slug, id=job_id)


def _location_text(location: Mapping[str, Any]) -> str:
    """`{city, region, country}` -> `"San Francisco, CA, United States"`.

    Joins only the parts present, matching `scraper.py`'s
    `", ".join(x for x in [...] if x)`.
    """
    parts = (location.get("city"), location.get("region"), location.get("country"))
    return ", ".join(str(p) for p in parts if p)


def _posted(job: Mapping[str, Any]) -> tuple[str | None, str]:
    """`releasedDate` -> (hashable date, raw first-10-chars for provenance).

    `releasedDate` arrives as a full ISO-8601 timestamp
    (`"2026-07-14T09:00:00.000Z"`). `posted_raw` keeps `scraper.py`'s
    `[:10]` truncation; `posted_date` runs the full value through
    `normalize_date`, which anchors on the same `YYYY-MM-DD` prefix so the
    two never disagree.
    """
    raw = job.get("releasedDate") or ""
    text = str(raw)
    return normalize_date(text), text[:10]


def parse_page(payload: bytes | str | Mapping[str, Any], target: SourceTarget) -> Iterator[NormalizedPosting]:
    """One SmartRecruiters postings page -> records. Pure: no I/O, no clock.

    A malformed envelope (not JSON, no `content` list) raises `PayloadError`:
    the API changed and this adapter is broken. An individual item missing a
    name or an id is skipped — it cannot be identified or given a working
    apply URL, and one bad row must not blank a page that Phase 2.4 would then
    treat as proof the rest of the board is empty.

    Pagination bookkeeping (`offset`, `totalFound`, the stopping condition) is
    deliberately not this function's job; it parses exactly the page it was
    handed and nothing about what page comes next.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        from json import JSONDecodeError, loads

        try:
            data = loads(payload)
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"smartrecruiters postings {target.instance_key}: body is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise PayloadError(
            f"smartrecruiters postings {target.instance_key}: expected an object, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    content = data.get("content")
    if not isinstance(content, Sequence) or isinstance(content, (str, bytes)):
        raise PayloadError(
            f"smartrecruiters postings {target.instance_key}: 'content' is not a list",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    slug = str(target.param("slug") or target.instance_key or "")
    company = str(target.param("company") or target.label or "")
    for job in content:
        if not isinstance(job, Mapping):
            continue
        name = job.get("name")
        job_id = job.get("id")
        if not name or job_id is None:
            continue
        location = job.get("location")
        location_map = location if isinstance(location, Mapping) else {}
        posted_date, posted_raw = _posted(job)
        extra: dict[str, Any] = {}
        if job.get("refNumber"):
            # The customer's own requisition number, distinct from the
            # SmartRecruiters-internal `id`. Kept as Phase 3 provenance only,
            # never as identity (see module docstring / IdentityClaim).
            extra["ref_number"] = str(job["refNumber"])
        yield target.record(
            title=str(name),
            company=company,
            url=job_url(slug, job_id),
            location=_location_text(location_map),
            # The board's own posting id is the source-native requisition
            # identity: it is what the apply URL embeds and what survives
            # title and location edits. Namespaced by board (target.record
            # stamps instance_key), so two boards can never collide.
            req_id=str(job_id),
            posted_date=posted_date,
            posted_raw=posted_raw,
            remote=bool(location_map.get("remote", False)),
            extra=extra,
        )


def _page_meta(data: Mapping[str, Any]) -> tuple[int, int]:
    """`(items on this page, totalFound)`, defensively.

    Read only after `parse_page` has already validated `data["content"]` is a
    list, so this never raises; a missing or non-numeric `totalFound` is
    treated as `0`, matching `scraper.py`'s `d.get("totalFound", 0)` and
    stopping the walk after the current page rather than looping forever.
    """
    content = data.get("content")
    content_len = len(content) if isinstance(content, Sequence) and not isinstance(content, (str, bytes)) else 0
    total_found = data.get("totalFound")
    total = int(total_found) if isinstance(total_found, (int, float)) and not isinstance(total_found, bool) else 0
    return content_len, total


class SmartRecruitersAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`{"docusign": "DocuSign"}` -> one target per company slug.

        An unconfigured or empty `companies.smartrecruiters` plans zero
        targets, which is not an error — the scheduler simply has no
        SmartRecruiters work.
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
        """Walk the board page by page, streaming records as each page parses.

        No retry, no sleep, no deadline branching — everything this method
        knows how to do on a failed request is raise (invariants 1-3). Resumes
        from `ctx.resume_from.cursor["offset"]` when the checkpoint is valid
        for this exact target (`Checkpoint.is_valid_for`); an invalid or
        absent checkpoint starts at offset 0, which is always correct, merely
        slower.
        """
        slug = str(target.require("slug"))
        offset = 0
        emitted = 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            offset = max(0, int(ctx.resume_from.cursor.get("offset", 0)))
            emitted = ctx.resume_from.emitted

        while True:
            response: HttpResponse = await ctx.http().send(
                HttpRequest(
                    url=postings_url(slug),
                    params={"limit": PAGE_SIZE, "offset": offset},
                )
            )
            check_status(response, source_key=SOURCE_KEY, instance_key=target.instance_key)
            data = response.json(source_key=SOURCE_KEY, instance_key=target.instance_key)
            for record in parse_page(data, target):
                yield record
                emitted += 1
            content_len, total_found = _page_meta(data)
            offset += content_len
            # Marked after this page's records were yielded (and therefore
            # pulled by the consumer), never before (Checkpoint's "delivered,
            # not committed" contract).
            ctx.mark_checkpoint({"offset": offset}, target=target, emitted=emitted)
            if content_len == 0 or offset >= total_found:
                return


ADAPTER = SmartRecruitersAdapter()
