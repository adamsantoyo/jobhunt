"""Phase 3.2: cheap prefilter + bounded description fetching.

The roadmap line this implements: "Apply cheap location/title/blocker prefilter
before descriptions. Fetch descriptions only for new/changed plausible postings
with bounded host concurrency. Distinguish fetch failure from truly empty
description."

THE PUBLIC ENTRY POINT, exactly:

    async def enrich_run(
        conn: sqlite3.Connection,
        run_uid: str,
        *,
        transport: Transport,
        profile: Any,
        limit: int | None = None,
        max_concurrency: int = 8,
        per_host_concurrency: int = 2,
        fetch_timeout_seconds: float = 15.0,
        deadline_seconds: float | None = None,
        chunk_size: int = 200,
        now: Callable[[], str] | None = None,
    ) -> EnrichmentReport

It is a pipeline stage, not a runner: it takes an already-open connection (the
caller owns opening/closing it and, per this module's own commit discipline
below, does not need to wrap it in an outer transaction), a `run_uid` whose
dirty set (`runstore.dirty_posting_ids`) is the work list, an HTTP `transport`
(anything satisfying `contract.Transport` -- a bare `HttpxTransport`, a
`PacedTransport`, or a test double), and a validated candidate profile. It is
designed to be one node in Task 3.3's dependency graph: it does not import
anything from a graph module, and it raises nothing on an empty or unknown
`run_uid` (the report is simply all zeros).

FETCH_STATUS VOCABULARY, the `descriptions.fetch_status` values this module
ever writes (`FetchStatus` below is the enum; the column stores its `str()`):

  "available"    the fetch succeeded and the body, after whitespace
                 normalization, is non-empty. `body` holds the text.
  "empty"        the fetch succeeded (HTTP 200) but the body normalizes to the
                 empty string -- a real, positive observation that this posting
                 has no description, not a failure. `body` is `""` (never
                 `NULL`; the schema's CHECK constraint requires a non-NULL body
                 for every status except "unavailable").
  "unavailable"  the fetch could not be completed: a classified transport
                 error (after the one permitted retry), an HTTP status that
                 `contract.classify_status` scores PERMANENT or that stayed
                 TRANSIENT through both attempts, a timeout, a posting with no
                 usable URL, or a posting whose fetch was never attempted
                 because `deadline_seconds` ran out first. `body` is `NULL`.
                 `metadata_json.error` carries `{error_class, message, status}`
                 (see `contract.SourceError.to_json_dict`) and
                 `metadata_json.attempts` carries the attempt count (1 or 2),
                 which together are what "distinguish fetch failure from truly
                 empty description" means operationally: a reader can always
                 tell "we asked and got nothing" (status="empty", no error
                 evidence) from "we could not ask, or asking failed"
                 (status="unavailable", `metadata_json.error` populated).

PREFILTER. `prefilter_posting(posting, profile)` is a pure function: cheap
normalized fields in (`CheapPosting` -- title/company/location/remote, the
fields a source delivers WITHOUT a description fetch), a decision out
(`PrefilterDecision` -- fetch-worthy or not, plus a machine-readable
`category`/`reason`). No I/O, no clock, no randomness; the same inputs always
produce the same decision. Three exclusion categories are checked in this
fixed order, first match wins:

  "location"  `profile.location.non_us_patterns` (a non-US location is
              excluded outright -- the one location rule already live in
              rubric.py's scorer, mirrored here verbatim), then
              `profile.location.dc_pattern`, `.other_state_pattern`,
              `.socal_cities`, `.far_wa_cities` (each a positive-match
              exclusion: the location is confirmed to be a specific excluded
              region). A `remote=True` posting is NEVER excluded on location
              grounds -- the whole point of the "or US-remote" branch of the
              real location gate -- and an empty/unmatched location is left
              alone rather than guessed at.
  "title"     `profile.exclusions.people_management_pattern` against the title
              with `profile.exclusions.ic_manager_pattern` stripped first (the
              exact rubric.py idiom for telling "Engineering Manager" from
              "Technical Program Manager"), then: the title is checked against
              every family in `profile.families.keywords`, and if it matches a
              family that is NOT in `profile.families.in_scope`, that is an
              explicit off-focus signal computable from the title alone. A
              title matching NO family is left alone -- the real scorer falls
              back to the description in that case, and this prefilter cannot
              see the description yet, so guessing here would be the unsafe
              direction.
  "blocker"   `profile.employers.staffing_agencies` against the company field
              (the one employer-identity rule in rubric.py that never touches
              the description -- the rest of that section's blockers
              (`c2c_keywords`, `degree_required_pattern`, clearance,
              disqualifying skills, years-required) are description-only and
              therefore cannot be decided before a fetch).

Everything else -- no exclusion pattern matched, or the fields needed to decide
were empty/absent -- comes back fetch-worthy with `reason="not_excluded"`. That
default direction is the module's one hard requirement: uncertain means fetch.

WHY THIS MODULE DOES NOT IMPORT `runstore`'s WRITE HELPERS OR `writer.py`.
Nothing here participates in the ingest writer's batched transaction, and
`runstore.py`/`migrations.py` are being edited concurrently by Task 3.3. The
one exception is `runstore.dirty_posting_ids`, imported by name because the
roadmap names it explicitly as this phase's input work list and its signature
is frozen 3.1 API -- everything else this module needs (the current run's
cheap fields, the already-described check, the description write) is plain
SQL against `run_postings`/`posting_versions`/`descriptions`, owned by this
module alone, so a concurrent edit to `runstore.py` internals cannot change
this module's behavior.

COMMIT DISCIPLINE. Unlike `runstore.py` (whose functions never call
`commit()` because the writer owns one batched transaction), this module
commits once per description row, immediately after writing it. A fetch pass
can run for a while and holds no long-lived write transaction across an
`await` (an open write transaction spanning a slow network call would starve
any other writer on the same database file), and a process that dies mid-pass
leaves every description it already fetched committed rather than rolled
back -- exactly the "self-healing" posture `dirty_posting_ids` already has:
whatever this pass did not get to remains dirty-and-undescribed for the next
call to pick up.

IDEMPOTENCY. `descriptions.provenance_hash` is `sha256("posting_version:" +
posting_version_id)` (or, for the defensive fallback where a dirty id has no
resolvable version, `sha256("posting:" + posting_id + ":no-version")`) -- a
function of WHAT was fetched, not of WHEN or how many times. The write is
`INSERT ... ON CONFLICT(provenance_hash) DO UPDATE ... WHERE
descriptions.fetch_status = 'unavailable'`, so: a rerun that reaches a
posting whose description is already "available"/"empty" never gets here at
all (the already-described check below skips it before any fetch is
attempted); a rerun that reaches one still "unavailable" updates that same
row in place (never a second row) whether the outcome is the same failure
again or a fresh success. Either way, `UNIQUE(provenance_hash)` is never
violated and a rerun creates zero new rows for content already resolved.

BOUNDED CONCURRENCY. Two `asyncio.Semaphore`s gate every fetch: one global
(`max_concurrency`) and one per host (`per_host_concurrency`, keyed by
`contract.HttpRequest.host` -- the same hostname-lowercasing the scheduler's
own `PacedTransport` keys on). This module builds its own gates rather than
reusing `scheduler.py`'s private `_Gates` (which is not exported, and is
per-run scheduler state this module has no business reaching into); it reuses
only the public seam -- `contract.Transport`/`HttpRequest`/`check_status` and
`transport.PacedTransport` if the caller chooses to hand one in as
`transport`. `EnrichmentReport.peak_concurrency`/`peak_by_host` are the
proof-of-bound counters, populated the same way `scheduler.py` populates its
own (increment-then-max under the gate, decrement in a `finally`).

RETRY. At most one retry, and only for a TRANSIENT disposition (an
`asyncio.TimeoutError`, or a `contract.SourceError` -- raised either by the
transport itself or by `contract.check_status` classifying a non-2xx
response -- whose `.disposition is Disposition.TRANSIENT`). A PERMANENT error
or a second TRANSIENT one is terminal for that posting this pass. This
mirrors the scheduler's "one classified transient retry" restraint
(`contract.py`'s module docstring, invariant 1) without importing scheduler
code, which owns a materially different retry (deadline-budget-aware, whole
adapter attempts) that is not exposed as a reusable primitive.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from .contract import (
    Disposition,
    HttpRequest,
    JSONValue,
    SourceError,
    Transport,
    check_status,
    collapse_whitespace,
)
from .runstore import dirty_posting_ids

__all__ = [
    "REASON_DC_METRO",
    "REASON_FAR_WA_CITY",
    "REASON_NON_US_LOCATION",
    "REASON_NOT_EXCLUDED",
    "REASON_OFF_FOCUS_ROLE_TITLE",
    "REASON_OTHER_STATE",
    "REASON_PEOPLE_MANAGEMENT_TITLE",
    "REASON_SOCAL_CITY",
    "REASON_STAFFING_AGENCY_COMPANY",
    "CheapPosting",
    "EnrichmentReport",
    "FetchStatus",
    "PrefilterDecision",
    "enrich_run",
    "prefilter_posting",
]


# --------------------------------------------------------------------------- #
# Fetch status vocabulary
# --------------------------------------------------------------------------- #
class FetchStatus(StrEnum):
    """`descriptions.fetch_status`. See the module docstring for the exact
    meaning of each value and how it distinguishes failure from empty."""

    AVAILABLE = "available"
    EMPTY = "empty"
    UNAVAILABLE = "unavailable"


#: Statuses that make a `descriptions` row "usable" -- good enough that a later
#: run must not re-fetch it. Deliberately excludes UNAVAILABLE: a failed fetch
#: is not a description, and the posting stays eligible for a retry on the next
#: pass over the dirty set (or the next time this same run is re-driven).
_USABLE_STATUSES = (str(FetchStatus.AVAILABLE), str(FetchStatus.EMPTY))


# --------------------------------------------------------------------------- #
# Prefilter: pure, deterministic, no I/O
# --------------------------------------------------------------------------- #
REASON_NON_US_LOCATION = "non_us_location"
REASON_DC_METRO = "dc_metro"
REASON_OTHER_STATE = "other_state"
REASON_SOCAL_CITY = "socal_city"
REASON_FAR_WA_CITY = "far_wa_city"
REASON_PEOPLE_MANAGEMENT_TITLE = "people_management_title"
REASON_OFF_FOCUS_ROLE_TITLE = "off_focus_role_title"
REASON_STAFFING_AGENCY_COMPANY = "staffing_agency_company"
REASON_NOT_EXCLUDED = "not_excluded"


@dataclass(frozen=True, slots=True)
class CheapPosting:
    """The fields a source delivers WITHOUT a description fetch.

    Deliberately its own small type rather than `contract.NormalizedPosting`:
    that type requires `source_key`/`url` and performs its own (unrelated)
    validation at construction, which would make every prefilter unit test
    carry irrelevant required fields. Any object with these four attributes
    works -- a real pipeline stage builds one from `posting_versions` columns
    (see `_CheapRow`/`_cheap_fields` below); a test builds one directly.
    """

    title: str = ""
    company: str = ""
    location: str = ""
    remote: bool = False


@dataclass(frozen=True, slots=True)
class PrefilterDecision:
    """The prefilter's verdict. `category`/`reason` are always populated, even
    when `fetch_worthy` is True (`category=None`, `reason="not_excluded"`),
    so a caller can log "why" uniformly instead of treating the fetch-worthy
    case as reason-less."""

    fetch_worthy: bool
    category: str | None
    reason: str


#: Structural shape this module reads off `profile` for the prefilter. Not a
#: `typing.Protocol` enforced at runtime -- `profile` is duck-typed on purpose
#: (see the module docstring: `candidate_profile.Profile` satisfies this shape,
#: and so does a lightweight test double with the same attribute names) -- this
#: exists purely as documentation of exactly what is read, in one place.
#:
#:   profile.location.non_us_patterns      : Sequence[re.Pattern]
#:   profile.location.dc_pattern           : re.Pattern
#:   profile.location.other_state_pattern  : re.Pattern
#:   profile.location.socal_cities         : Sequence[str]
#:   profile.location.far_wa_cities        : Sequence[str]
#:   profile.exclusions.people_management_pattern : re.Pattern
#:   profile.exclusions.ic_manager_pattern         : re.Pattern
#:   profile.families.keywords       : Mapping[str, Sequence[str]]
#:   profile.families.in_scope       : Sequence[str]
#:   profile.employers.staffing_agencies : Sequence[str]


def _location_decision(posting: CheapPosting, profile: Any) -> PrefilterDecision | None:
    if posting.remote:
        return None
    loc = (posting.location or "").strip().lower()
    if not loc:
        return None
    prof_loc = profile.location
    if any(p.search(loc) for p in prof_loc.non_us_patterns):
        return PrefilterDecision(False, "location", REASON_NON_US_LOCATION)
    if prof_loc.dc_pattern.search(loc):
        return PrefilterDecision(False, "location", REASON_DC_METRO)
    if prof_loc.other_state_pattern.search(loc):
        return PrefilterDecision(False, "location", REASON_OTHER_STATE)
    if any(city.lower() in loc for city in prof_loc.socal_cities):
        return PrefilterDecision(False, "location", REASON_SOCAL_CITY)
    if any(city.lower() in loc for city in prof_loc.far_wa_cities):
        return PrefilterDecision(False, "location", REASON_FAR_WA_CITY)
    return None


def _title_decision(posting: CheapPosting, profile: Any) -> PrefilterDecision | None:
    title = (posting.title or "").strip().lower()
    if not title:
        return None
    excl = profile.exclusions
    stripped = excl.ic_manager_pattern.sub(" ", title)
    if excl.people_management_pattern.search(stripped):
        return PrefilterDecision(False, "title", REASON_PEOPLE_MANAGEMENT_TITLE)
    families = profile.families
    matched_family = next(
        (fam for fam, keywords in families.keywords.items() if any(kw.lower() in title for kw in keywords)),
        None,
    )
    if matched_family is not None and matched_family not in families.in_scope:
        return PrefilterDecision(False, "title", REASON_OFF_FOCUS_ROLE_TITLE)
    return None


def _blocker_decision(posting: CheapPosting, profile: Any) -> PrefilterDecision | None:
    company = (posting.company or "").strip().lower()
    if not company:
        return None
    if any(agency.lower() in company for agency in profile.employers.staffing_agencies):
        return PrefilterDecision(False, "blocker", REASON_STAFFING_AGENCY_COMPANY)
    return None


def prefilter_posting(posting: CheapPosting, profile: Any) -> PrefilterDecision:
    """Decide fetch-worthy vs skip from cheap fields alone. Pure; no I/O.

    Checks location, then title, then blocker, in that fixed order -- first
    exclusion wins. Anything not positively excluded is fetch-worthy: this is
    the conservative direction the roadmap asks for ("when uncertain, fetch").
    """
    for decide in (_location_decision, _title_decision, _blocker_decision):
        decision = decide(posting, profile)
        if decision is not None:
            return decision
    return PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class EnrichmentReport:
    """Structured counts, suitable for run evidence.

    Accounting invariant, true of every call:
        considered == already_described + sum(skipped_by_reason.values()) + fetched
        fetched    == available + empty + failed
        rows_written == fetched   (one write per attempted fetch, whatever the
                                    outcome -- a missing URL or an exhausted
                                    deadline still persists an "unavailable" row)
    """

    run_uid: str
    considered: int = 0
    already_described: int = 0
    skipped_by_reason: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))
    fetched: int = 0
    available: int = 0
    empty: int = 0
    failed: int = 0
    rows_written: int = 0
    peak_concurrency: int = 0
    peak_by_host: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def skipped_total(self) -> int:
        return sum(self.skipped_by_reason.values())

    @property
    def accounted_for(self) -> int:
        return self.already_described + self.skipped_total + self.fetched


# --------------------------------------------------------------------------- #
# Small self-contained helpers (deliberately not imported from runstore.py --
# see the module docstring for why)
# --------------------------------------------------------------------------- #
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


#: Chunk size for batched `IN (...)` lookups -- same rationale and same order of
#: magnitude as `runstore._LOOKUP_CHUNK`, redefined here rather than imported
#: so this module has no runtime dependency on runstore internals.
_LOOKUP_CHUNK = 400

_DESCRIPTION_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/description")


def _provenance_hash(*, posting_id: str, posting_version_id: str | None) -> str:
    """A function of WHAT is being described, not of when or how many times.

    Keyed on the posting_version alone when one is known: content identity
    already lives there (a material change mints a new version, so "this
    version's description" is a stable question across reruns). The
    `posting_id`-only fallback exists solely for the defensive case in
    `_cheap_fields` where a dirty id could not be resolved to a version at
    all; it should not occur against a canonical database.
    """
    key = f"posting_version:{posting_version_id}" if posting_version_id else f"posting:{posting_id}:no-version"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _description_id(provenance_hash: str) -> str:
    return str(uuid.uuid5(_DESCRIPTION_NAMESPACE, provenance_hash))


def _require_descriptions_table(conn: sqlite3.Connection) -> None:
    """Fail loudly if handed a database this module cannot write, the same
    posture `runstore.require_canonical_schema` takes for its own tables."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='descriptions'"
    ).fetchone()
    if row is None:
        raise RuntimeError("enrich_run requires the canonical 'descriptions' table")


# --------------------------------------------------------------------------- #
# Cheap fields: this run's title/company/location/remote/url per dirty posting
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _CheapRow:
    posting_id: str
    posting_version_id: str
    title: str
    company: str
    location: str
    remote: bool
    url: str | None


_CHEAP_FIELDS_SQL = """
SELECT rp.posting_id AS posting_id,
       rp.posting_version_id AS posting_version_id,
       pv.title AS title,
       pv.company AS company,
       pv.location AS location,
       pv.remote AS remote,
       pv.payload_json AS payload_json
  FROM run_postings rp
  JOIN posting_versions pv ON pv.posting_version_id = rp.posting_version_id
 WHERE rp.run_uid = ? AND rp.posting_id IN ({placeholders})
"""


def _extract_url(payload_json: str | None) -> str | None:
    """The raw fetch URL a source reported, from `posting_versions.payload_json`.

    There is no plain `url` column on `posting_versions` (Phase 3.1 kept the
    body-adjacent fields out of that table on purpose -- see `runstore
    ._link_source_version`'s docstring); the URL a version's content came from
    is inside its `payload_json.source.url`. A malformed or missing value
    degrades to `None` rather than raising: one posting with unreadable
    provenance must not fail a whole batch, and a `None` URL is handled by the
    caller as "cannot fetch" (an `unavailable` description), which is the safe
    outcome for evidence this module cannot make sense of.
    """
    if not payload_json:
        return None
    try:
        doc = json.loads(payload_json)
    except (TypeError, ValueError):
        return None
    source = doc.get("source") if isinstance(doc, dict) else None
    if not isinstance(source, dict):
        return None
    url = source.get("url")
    return url if isinstance(url, str) and url else None


def _cheap_fields(
    conn: sqlite3.Connection, run_uid: str, posting_ids: Sequence[str]
) -> dict[str, _CheapRow]:
    """This run's cheap fields for a batch of dirty posting ids.

    One statement per `_LOOKUP_CHUNK` ids, matching the write path's own
    batching discipline. `run_postings` carries exactly one row per (run_uid,
    posting_id) (Phase 3.1 invariant), so the join returns at most one row per
    posting id.
    """
    found: dict[str, _CheapRow] = {}
    ids = list(posting_ids)
    for start in range(0, len(ids), _LOOKUP_CHUNK):
        chunk = ids[start:start + _LOOKUP_CHUNK]
        sql = _CHEAP_FIELDS_SQL.format(placeholders=",".join("?" * len(chunk)))
        for row in conn.execute(sql, (run_uid, *chunk)):
            found[row["posting_id"]] = _CheapRow(
                posting_id=row["posting_id"],
                posting_version_id=row["posting_version_id"],
                title=row["title"] or "",
                company=row["company"] or "",
                location=row["location"] or "",
                remote=bool(row["remote"]),
                url=_extract_url(row["payload_json"]),
            )
    return found


def _already_described(conn: sqlite3.Connection, version_ids: Sequence[str]) -> set[str]:
    """Posting versions that already have a usable (available/empty) description."""
    found: set[str] = set()
    ids = [v for v in version_ids if v]
    for start in range(0, len(ids), _LOOKUP_CHUNK):
        chunk = ids[start:start + _LOOKUP_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        status_placeholders = ",".join("?" * len(_USABLE_STATUSES))
        sql = (
            f"SELECT DISTINCT posting_version_id FROM descriptions "
            f"WHERE posting_version_id IN ({placeholders}) "
            f"AND fetch_status IN ({status_placeholders})"
        )
        for row in conn.execute(sql, (*chunk, *_USABLE_STATUSES)):
            found.add(row["posting_version_id"])
    return found


def _iter_dirty_ids(
    conn: sqlite3.Connection, run_uid: str, *, limit: int | None, chunk_size: int
) -> list[str]:
    """Materialize (up to `limit`) dirty posting ids via `dirty_posting_ids`'s
    own cursor-paginated contract, so this module never issues the unbounded
    "no limit at all" query against a large dirty set in one shot."""
    out: list[str] = []
    after: str | None = None
    while True:
        remaining = None if limit is None else limit - len(out)
        if remaining is not None and remaining <= 0:
            return out
        take = chunk_size if remaining is None else min(chunk_size, remaining)
        batch = dirty_posting_ids(conn, run_uid, limit=take, after=after)
        if not batch:
            return out
        out.extend(batch)
        after = batch[-1]
        if len(batch) < take:
            return out


# --------------------------------------------------------------------------- #
# Fetch: bounded concurrency, one classified retry, failure vs empty
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _FetchOutcome:
    status: FetchStatus
    body: str | None
    attempts: int
    error: Mapping[str, JSONValue] | None


async def _fetch_description(transport: Transport, url: str, *, timeout: float) -> _FetchOutcome:
    """One posting's fetch, with the codebase's one-retry restraint.

    Reimplements the classification (not the deadline/budget bookkeeping) the
    scheduler applies to adapter attempts, because that code lives inside
    `Scheduler` and is not exposed as a reusable primitive. At most two
    attempts; the second happens only after a TRANSIENT disposition.
    """
    last_error: dict[str, JSONValue] | None = None
    attempt = 0
    for attempt in (1, 2):
        try:
            response = await asyncio.wait_for(transport.send(HttpRequest(url=url)), timeout=timeout)
        except TimeoutError:
            last_error = {
                "type": "Timeout",
                "disposition": str(Disposition.TRANSIENT),
                "message": f"no response within {timeout}s",
                "status": None,
            }
            if attempt == 1:
                continue
            break
        except SourceError as exc:
            last_error = exc.to_json_dict()
            if exc.disposition is Disposition.TRANSIENT and attempt == 1:
                continue
            break
        else:
            try:
                check_status(response, allow=(200,))
            except SourceError as exc:
                last_error = exc.to_json_dict()
                if exc.disposition is Disposition.TRANSIENT and attempt == 1:
                    continue
                break
            text = collapse_whitespace(response.text)
            status = FetchStatus.AVAILABLE if text else FetchStatus.EMPTY
            return _FetchOutcome(status=status, body=text, attempts=attempt, error=None)
    return _FetchOutcome(status=FetchStatus.UNAVAILABLE, body=None, attempts=attempt, error=last_error)


@dataclass(slots=True)
class _ConcurrencyGates:
    """This call's own bounded-concurrency state. Not shared with, or reused
    from, `scheduler.py`'s private `_Gates` -- see the module docstring."""

    global_sem: asyncio.Semaphore
    per_host_limit: int
    per_host: dict[str, asyncio.Semaphore] = field(default_factory=dict)
    inflight: int = 0
    peak: int = 0
    host_inflight: dict[str, int] = field(default_factory=dict)
    host_peak: dict[str, int] = field(default_factory=dict)

    def host_sem(self, host: str) -> asyncio.Semaphore:
        sem = self.per_host.get(host)
        if sem is None:
            sem = asyncio.Semaphore(self.per_host_limit)
            self.per_host[host] = sem
        return sem


@asynccontextmanager
async def _slot(gates: _ConcurrencyGates, host: str):
    async with gates.global_sem, gates.host_sem(host):
        gates.inflight += 1
        gates.peak = max(gates.peak, gates.inflight)
        gates.host_inflight[host] = gates.host_inflight.get(host, 0) + 1
        gates.host_peak[host] = max(gates.host_peak.get(host, 0), gates.host_inflight[host])
        try:
            yield
        finally:
            gates.inflight -= 1
            gates.host_inflight[host] -= 1


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
_UPSERT_SQL = """
INSERT INTO descriptions
    (description_id, posting_id, posting_version_id, alias_id, source_run_id,
     provenance_hash, content_hash, fetch_status, body, fetched_at, metadata_json)
VALUES (?,?,?,NULL,NULL,?,?,?,?,?,?)
ON CONFLICT(provenance_hash) DO UPDATE SET
    content_hash = excluded.content_hash,
    fetch_status = excluded.fetch_status,
    body = excluded.body,
    fetched_at = excluded.fetched_at,
    metadata_json = excluded.metadata_json
WHERE descriptions.fetch_status = 'unavailable'
"""


def _persist(
    conn: sqlite3.Connection,
    *,
    posting_id: str,
    posting_version_id: str | None,
    url: str | None,
    outcome: _FetchOutcome,
    fetched_at: str,
) -> None:
    provenance_hash = _provenance_hash(posting_id=posting_id, posting_version_id=posting_version_id)
    description_id = _description_id(provenance_hash)
    content_hash = hashlib.sha256(outcome.body.encode("utf-8")).hexdigest() if outcome.body else None
    metadata: dict[str, JSONValue] = {"url": url, "attempts": outcome.attempts}
    if outcome.error is not None:
        metadata["error"] = outcome.error
    if outcome.body is not None:
        metadata["content_length"] = len(outcome.body)
    conn.execute(
        _UPSERT_SQL,
        (
            description_id,
            posting_id,
            posting_version_id,
            provenance_hash,
            content_hash,
            str(outcome.status),
            outcome.body,
            fetched_at,
            _json_dumps(metadata),
        ),
    )
    conn.commit()


# --------------------------------------------------------------------------- #
# The entry point
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class _WorkItem:
    posting_id: str
    posting_version_id: str | None
    url: str | None
    host: str


async def enrich_run(
    conn: sqlite3.Connection,
    run_uid: str,
    *,
    transport: Transport,
    profile: Any,
    limit: int | None = None,
    max_concurrency: int = 8,
    per_host_concurrency: int = 2,
    fetch_timeout_seconds: float = 15.0,
    deadline_seconds: float | None = None,
    chunk_size: int = 200,
    now: Callable[[], str] | None = None,
) -> EnrichmentReport:
    """Prefilter and selectively fetch descriptions for one run's dirty set.

    See the module docstring for the exact vocabulary, the idempotency design,
    and the concurrency/retry policy. In order:

      1. Pull this run's dirty posting ids (`dirty_posting_ids`, cursor-paginated,
         bounded by `limit` when given).
      2. Batch-load each one's cheap fields (title/company/location/remote/url)
         from `run_postings`/`posting_versions`.
      3. Run `prefilter_posting` on each. Excluded postings are counted by
         `category:reason` in `EnrichmentReport.skipped_by_reason` and never
         touch the network or the `descriptions` table.
      4. Of the fetch-worthy postings, skip any whose current posting_version
         already has a usable (`available`/`empty`) description row
         (`already_described`).
      5. Fetch the rest, bounded by `max_concurrency` and
         `per_host_concurrency` (keyed on URL host), at most one classified
         transient retry each, `fetch_timeout_seconds` per attempt, and
         `deadline_seconds` (if given) as a hard stop on STARTING new fetches
         -- any posting reached after the deadline is persisted `unavailable`
         with `error.type == "DeadlineExceeded"` rather than silently dropped,
         so it remains eligible ("not usable yet") for the next call.
      6. Persist one `descriptions` row per fetch-worthy posting (idempotent
         upsert -- see the module docstring), committing immediately after
         each write.

    An unknown or empty `run_uid` returns an all-zero report rather than
    raising, matching `dirty_posting_ids`'s own "no such run" posture.
    """
    _require_descriptions_table(conn)
    clock = now or _utc_now_iso

    dirty_ids = _iter_dirty_ids(conn, run_uid, limit=limit, chunk_size=chunk_size)
    considered = len(dirty_ids)
    if not dirty_ids:
        return EnrichmentReport(run_uid=run_uid, considered=0)

    cheap = _cheap_fields(conn, run_uid, dirty_ids)

    skipped_by_reason: dict[str, int] = {}
    worklist: list[_WorkItem] = []
    for posting_id in dirty_ids:
        row = cheap.get(posting_id)
        if row is None:
            # Defensive only: a dirty id with no matching run_postings/posting_versions
            # join row should not occur against a canonical database (dirty_posting_ids
            # is itself derived from this run's run_postings rows). Treated as
            # fetch-worthy-but-unresolvable rather than silently dropped, so it still
            # shows up in the report and gets a (missing-url) description row.
            worklist.append(_WorkItem(posting_id=posting_id, posting_version_id=None, url=None, host=""))
            continue
        decision = prefilter_posting(
            CheapPosting(title=row.title, company=row.company, location=row.location, remote=row.remote),
            profile,
        )
        if not decision.fetch_worthy:
            key = f"{decision.category}:{decision.reason}"
            skipped_by_reason[key] = skipped_by_reason.get(key, 0) + 1
            continue
        host = HttpRequest(url=row.url).host if row.url else ""
        worklist.append(
            _WorkItem(posting_id=posting_id, posting_version_id=row.posting_version_id, url=row.url, host=host)
        )

    usable_versions = _already_described(conn, [w.posting_version_id for w in worklist])
    already_described = 0
    to_fetch: list[_WorkItem] = []
    for item in worklist:
        if item.posting_version_id is not None and item.posting_version_id in usable_versions:
            already_described += 1
        else:
            to_fetch.append(item)

    gates = _ConcurrencyGates(
        global_sem=asyncio.Semaphore(max(1, max_concurrency)),
        per_host_limit=max(1, per_host_concurrency),
    )
    deadline_at = None if deadline_seconds is None else time.monotonic() + deadline_seconds
    counts = {str(FetchStatus.AVAILABLE): 0, str(FetchStatus.EMPTY): 0, str(FetchStatus.UNAVAILABLE): 0}
    rows_written = 0

    async def run_one(item: _WorkItem) -> None:
        nonlocal rows_written
        if deadline_at is not None and time.monotonic() >= deadline_at:
            outcome = _FetchOutcome(
                status=FetchStatus.UNAVAILABLE,
                body=None,
                attempts=0,
                error={"type": "DeadlineExceeded", "disposition": str(Disposition.TRANSIENT),
                       "message": "enrichment deadline exceeded before this fetch started", "status": None},
            )
        elif not item.url:
            outcome = _FetchOutcome(
                status=FetchStatus.UNAVAILABLE,
                body=None,
                attempts=0,
                error={"type": "MissingURL", "disposition": str(Disposition.PERMANENT),
                       "message": "no fetchable url for this posting version", "status": None},
            )
        else:
            async with _slot(gates, item.host):
                outcome = await _fetch_description(transport, item.url, timeout=fetch_timeout_seconds)
        _persist(
            conn,
            posting_id=item.posting_id,
            posting_version_id=item.posting_version_id,
            url=item.url,
            outcome=outcome,
            fetched_at=clock(),
        )
        rows_written += 1
        counts[str(outcome.status)] += 1

    if to_fetch:
        await asyncio.gather(*(run_one(item) for item in to_fetch))

    return EnrichmentReport(
        run_uid=run_uid,
        considered=considered,
        already_described=already_described,
        skipped_by_reason=MappingProxyType(dict(skipped_by_reason)),
        fetched=len(to_fetch),
        available=counts[str(FetchStatus.AVAILABLE)],
        empty=counts[str(FetchStatus.EMPTY)],
        failed=counts[str(FetchStatus.UNAVAILABLE)],
        rows_written=rows_written,
        peak_concurrency=gates.peak,
        peak_by_host=MappingProxyType(dict(gates.host_peak)),
    )
