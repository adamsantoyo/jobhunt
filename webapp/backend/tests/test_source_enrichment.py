"""Phase 3.2: cheap prefilter + bounded description fetching.

The roadmap line under test: "Apply cheap location/title/blocker prefilter
before descriptions. Fetch descriptions only for new/changed plausible
postings with bounded host concurrency. Distinguish fetch failure from truly
empty description."

Two layers, tested separately:

  PREFILTER (`prefilter_posting`) is pure and needs no database at all. The
  fake profile below (`build_profile`) is a plain `SimpleNamespace` tree
  matching exactly the attributes `enrichment.py`'s module docstring says it
  reads -- never the real `profile.json`/`candidate_profile.Profile`, per this
  task's instruction that tests must use synthetic profiles. `enrichment.py`
  itself never imports `candidate_profile`; it duck-types `profile`, so a
  `SimpleNamespace` with the right attribute names is exactly as valid an
  input as a real `Profile`.

  ENRICHMENT (`enrich_run`) needs a real dirty run to work from, so these
  tests drive the actual scheduler (`FakeAdapter`/`emitting`/`plan_of`, the
  same fakes `test_source_posting_versions.py` uses) against a `tmp_path`
  database, then call `enrich_run` against a `FakeTransport` or a small
  concurrency-probing transport double. Nothing here touches the network or
  `webapp/app.db`.
"""
from __future__ import annotations

import asyncio
import json
import re
from types import SimpleNamespace

from backend.sources.contract import HttpResponse, InventoryScope, RunKind
from backend.sources.enrichment import (
    REASON_DC_METRO,
    REASON_FAR_WA_CITY,
    REASON_NON_US_LOCATION,
    REASON_NOT_EXCLUDED,
    REASON_OFF_FOCUS_ROLE_TITLE,
    REASON_OTHER_STATE,
    REASON_PEOPLE_MANAGEMENT_TITLE,
    REASON_SOCAL_CITY,
    REASON_STAFFING_AGENCY_COMPANY,
    CheapPosting,
    FetchStatus,
    PrefilterDecision,
    enrich_run,
    prefilter_posting,
)
from backend.sources.scheduler import Scheduler, SchedulerConfig
from backend.sources.testing import FakeTransport, text_response
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    emitting,
    make_connect,
    plan_of,
)

FAST_RETRY = dict(retry_base_delay_seconds=0.01, retry_jitter=0.0)


def run(coro):
    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


# --------------------------------------------------------------------------- #
# Synthetic profile -- a SimpleNamespace, never the real profile.json/Profile
# --------------------------------------------------------------------------- #
def build_profile(
    *,
    non_us=("nonusland",),
    dc=r"washington,\s*dc|district of columbia",
    other_state=r"\bnew york, ny\b",
    socal=("los angeles", "san diego"),
    far_wa=("spokane",),
    ic_manager_titles=("program", "product"),
    people_management=r"\bmanager\b|\bsupervisor\b",
    families=None,
    in_scope=None,
    staffing_agencies=("acme staffing", "staffing solutions"),
):
    if families is None:
        families = {
            "support": ("support engineer", "technical support"),
            "sales": ("account executive",),
        }
    if in_scope is None:
        in_scope = ("support",)
    ic_pattern = re.compile(
        r"\b(?:" + "|".join(re.escape(t) for t in ic_manager_titles) + r")\s+manager\b"
    )
    return SimpleNamespace(
        location=SimpleNamespace(
            non_us_patterns=[re.compile(p) for p in non_us],
            dc_pattern=re.compile(dc),
            other_state_pattern=re.compile(other_state),
            socal_cities=socal,
            far_wa_cities=far_wa,
        ),
        exclusions=SimpleNamespace(
            people_management_pattern=re.compile(people_management),
            ic_manager_pattern=ic_pattern,
        ),
        families=SimpleNamespace(keywords=families, in_scope=in_scope),
        employers=SimpleNamespace(staffing_agencies=staffing_agencies),
    )


PROFILE = build_profile()


# --------------------------------------------------------------------------- #
# Prefilter decision table
# --------------------------------------------------------------------------- #
def test_non_us_location_is_excluded():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="Somewhere, NonUsLand")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "location", REASON_NON_US_LOCATION)


def test_dc_metro_location_is_excluded():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="Washington, DC")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "location", REASON_DC_METRO)


def test_other_state_location_is_excluded():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="New York, NY")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "location", REASON_OTHER_STATE)


def test_socal_city_location_is_excluded():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="Los Angeles, CA")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "location", REASON_SOCAL_CITY)


def test_far_wa_city_location_is_excluded():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="Spokane, WA")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "location", REASON_FAR_WA_CITY)


def test_remote_posting_is_never_excluded_on_location():
    """The whole point of the real "Bay Area or US-remote" gate: remote wins
    over every location exclusion, including an outright non-US match."""
    posting = CheapPosting(
        title="Support Engineer", company="Acme", location="Somewhere, NonUsLand", remote=True
    )
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


def test_empty_location_is_uncertain_and_fetch_worthy():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


def test_bay_area_location_is_not_excluded():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="San Francisco, CA")
    decision = prefilter_posting(posting, PROFILE)
    assert decision.fetch_worthy is True


def test_people_management_title_is_excluded():
    posting = CheapPosting(title="Engineering Manager", company="Acme", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "title", REASON_PEOPLE_MANAGEMENT_TITLE)


def test_ic_manager_title_is_not_treated_as_people_management():
    """"Program Manager" strips to nothing under `ic_manager_pattern`, so it
    must NOT trip the people-management exclusion -- the exact rubric.py idiom
    this mirrors (`ic_manager_pattern.sub(" ", t)` before the search)."""
    posting = CheapPosting(title="Program Manager", company="Acme", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


def test_off_focus_family_title_is_excluded():
    posting = CheapPosting(title="Account Executive", company="Acme", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "title", REASON_OFF_FOCUS_ROLE_TITLE)


def test_in_scope_family_title_is_not_excluded():
    posting = CheapPosting(title="Support Engineer", company="Acme", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


def test_title_matching_no_family_is_uncertain_and_fetch_worthy():
    """The real scorer falls back to the description when the title alone
    matches no family; this prefilter cannot see the description, so it must
    not guess an exclusion here."""
    posting = CheapPosting(title="Widget Technician", company="Acme", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


def test_staffing_agency_company_is_a_blocker():
    posting = CheapPosting(title="Support Engineer", company="Random Staffing Solutions LLC", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(False, "blocker", REASON_STAFFING_AGENCY_COMPANY)


def test_ordinary_company_is_not_a_blocker():
    posting = CheapPosting(title="Support Engineer", company="Acme Corp", location="")
    decision = prefilter_posting(posting, PROFILE)
    assert decision == PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


def test_all_fields_empty_is_uncertain_and_fetch_worthy():
    decision = prefilter_posting(CheapPosting(), PROFILE)
    assert decision == PrefilterDecision(True, None, REASON_NOT_EXCLUDED)


def test_location_is_checked_before_title_before_blocker():
    """A posting that would be excluded on all three grounds is reported under
    the FIRST category the fixed order checks -- location."""
    posting = CheapPosting(
        title="Account Executive",
        company="Staffing Solutions Inc",
        location="Somewhere, NonUsLand",
    )
    decision = prefilter_posting(posting, PROFILE)
    assert decision.category == "location"
    assert decision.reason == REASON_NON_US_LOCATION


# --------------------------------------------------------------------------- #
# Database-backed enrichment tests
# --------------------------------------------------------------------------- #
def drive(connect_fn, specs, *, source="src", instance="a"):
    """One scheduler run delivering exactly `specs` (dicts for `target.record`).

    `InventoryScope.COMPLETE` + `FULL_DIRECT` is the simplest way to get a
    settled ('succeeded') run whose dirty set `enrich_run` can read -- the same
    shape `test_source_posting_versions.py` drives.
    """
    adapter = FakeAdapter(
        source, instances=(instance,), body=emitting(specs), inventory_scope=InventoryScope.COMPLETE
    )
    scheduler = Scheduler(connect_fn, config=SchedulerConfig(**FAST_RETRY))
    return run(scheduler.run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))


def cheap_rows_by_title(connect_fn, run_uid):
    """`{title: {"posting_id", "posting_version_id", "url"}}` for one run,
    read straight off the canonical tables `enrich_run` itself reads."""
    conn = connect_fn()
    try:
        rows = conn.execute(
            "SELECT rp.posting_id AS posting_id, rp.posting_version_id AS posting_version_id, "
            "pv.title AS title, pv.payload_json AS payload_json "
            "FROM run_postings rp JOIN posting_versions pv "
            "ON pv.posting_version_id = rp.posting_version_id WHERE rp.run_uid=?",
            (run_uid,),
        ).fetchall()
        out = {}
        for row in rows:
            payload = json.loads(row["payload_json"])
            out[row["title"]] = {
                "posting_id": row["posting_id"],
                "posting_version_id": row["posting_version_id"],
                "url": payload["source"]["url"],
            }
        return out
    finally:
        conn.close()


def descriptions(connect_fn):
    conn = connect_fn()
    try:
        return conn.execute("SELECT * FROM descriptions ORDER BY posting_id").fetchall()
    finally:
        conn.close()


def description_for(connect_fn, posting_id):
    conn = connect_fn()
    try:
        return conn.execute(
            "SELECT * FROM descriptions WHERE posting_id=?", (posting_id,)
        ).fetchone()
    finally:
        conn.close()


def seed_description(connect_fn, *, posting_id, posting_version_id, provenance_hash, body, fetched_at):
    conn = connect_fn()
    try:
        conn.execute(
            "INSERT INTO descriptions (description_id, posting_id, posting_version_id, "
            "provenance_hash, fetch_status, body, fetched_at) VALUES (?,?,?,?,?,?,?)",
            (f"seed-{provenance_hash}", posting_id, posting_version_id, provenance_hash,
             str(FetchStatus.AVAILABLE), body, fetched_at),
        )
        conn.commit()
    finally:
        conn.close()


PERMISSIVE_PROFILE = build_profile(
    non_us=("no-such-location-marker",),
    dc=r"no-such-dc-marker",
    other_state=r"no-such-state-marker",
    socal=(),
    far_wa=(),
    families={"support": ("support engineer",)},
    in_scope=("support",),
    staffing_agencies=("no-such-agency-marker",),
)


def test_prefilter_excluded_posting_is_never_fetched(tmp_path):
    """A posting the prefilter excludes must not reach the transport at all --
    proved by a `FakeTransport` with no route for its URL: any attempted fetch
    raises `AssertionError` inside `send`, which `asyncio.gather` would
    propagate and fail this test."""
    connect_fn = make_connect(tmp_path)
    result = drive(
        connect_fn,
        [
            dict(title="Support Engineer Keep", company="Acme", url="https://boards.example/keep",
                 req_id="1", location="San Francisco, CA"),
            dict(title="Support Engineer Skip", company="Acme", url="https://boards.example/skip",
                 req_id="2", location="Somewhere, NonUsLand"),
        ],
    )
    rows = cheap_rows_by_title(connect_fn, result.run_uid)
    transport = FakeTransport()
    transport.add(rows["Support Engineer Keep"]["url"], text_response("A real description body.", status=200))

    conn = connect_fn()
    try:
        report = run(enrich_run(conn, result.run_uid, transport=transport, profile=PROFILE))
    finally:
        conn.close()

    assert report.considered == 2
    assert report.fetched == 1
    assert report.available == 1
    assert report.skipped_by_reason == {"location:non_us_location": 1}
    assert transport.call_count == 1
    assert report.accounted_for == report.considered


def test_already_described_posting_is_skipped_without_a_fetch(tmp_path):
    connect_fn = make_connect(tmp_path)
    result = drive(
        connect_fn,
        [dict(title="Support Engineer", company="Acme", url="https://boards.example/x",
              req_id="1", location="San Francisco, CA")],
    )
    rows = cheap_rows_by_title(connect_fn, result.run_uid)
    posting_id = rows["Support Engineer"]["posting_id"]
    version_id = rows["Support Engineer"]["posting_version_id"]
    seed_description(
        connect_fn, posting_id=posting_id, posting_version_id=version_id,
        provenance_hash="manual-seed-1", body="A pre-existing description.", fetched_at="2020-01-01T00:00:00+00:00",
    )

    transport = FakeTransport()  # no routes at all: any send() raises

    conn = connect_fn()
    try:
        report = run(enrich_run(conn, result.run_uid, transport=transport, profile=PERMISSIVE_PROFILE))
    finally:
        conn.close()

    assert report.already_described == 1
    assert report.fetched == 0
    assert transport.call_count == 0
    rows_after = descriptions(connect_fn)
    assert len(rows_after) == 1
    assert rows_after[0]["body"] == "A pre-existing description."


def test_failure_vs_empty_and_retry_round_trip_through_the_real_table(tmp_path):
    connect_fn = make_connect(tmp_path)
    specs = [
        dict(title="Available Role", company="Acme", url="https://boards.example/available",
             req_id="1", location="San Francisco, CA"),
        dict(title="Empty Role", company="Acme", url="https://boards.example/empty",
             req_id="2", location="San Francisco, CA"),
        dict(title="Permanent Fail Role", company="Acme", url="https://boards.example/permfail",
             req_id="3", location="San Francisco, CA"),
        dict(title="Retry Then Success Role", company="Acme", url="https://boards.example/retrythensuccess",
             req_id="4", location="San Francisco, CA"),
        dict(title="Retry Twice Fail Role", company="Acme", url="https://boards.example/retrytwicefail",
             req_id="5", location="San Francisco, CA"),
    ]
    result = drive(connect_fn, specs)
    rows = cheap_rows_by_title(connect_fn, result.run_uid)

    transport = FakeTransport()
    transport.add(rows["Available Role"]["url"], text_response("A full job description here.", status=200))
    transport.add(rows["Empty Role"]["url"], text_response("   \n\t  ", status=200))
    transport.add(rows["Permanent Fail Role"]["url"], text_response("not found", status=404))
    transport.add(
        rows["Retry Then Success Role"]["url"],
        text_response("first attempt fails", status=503),
        text_response("second attempt succeeds with a real body.", status=200),
    )
    transport.add(
        rows["Retry Twice Fail Role"]["url"],
        text_response("first fail", status=503),
        text_response("second fail", status=503),
    )

    conn = connect_fn()
    try:
        report = run(
            enrich_run(
                conn, result.run_uid, transport=transport, profile=PERMISSIVE_PROFILE,
                fetch_timeout_seconds=5.0,
            )
        )
    finally:
        conn.close()

    assert report.considered == 5
    assert report.already_described == 0
    assert report.skipped_by_reason == {}
    assert report.fetched == 5
    assert report.available == 2  # Available Role + Retry Then Success Role
    assert report.empty == 1
    assert report.failed == 2  # Permanent Fail Role + Retry Twice Fail Role
    assert report.rows_written == 5
    assert report.accounted_for == report.considered

    # exactly one retry, never more, for each of the two transient cases; no
    # retry at all for the permanent failure or the two 200s.
    assert transport.call_count == 1 + 1 + 1 + 2 + 2

    available = description_for(connect_fn, rows["Available Role"]["posting_id"])
    assert available["fetch_status"] == "available"
    assert available["body"] == "A full job description here."
    assert available["content_hash"] is not None
    assert json.loads(available["metadata_json"])["attempts"] == 1

    empty = description_for(connect_fn, rows["Empty Role"]["posting_id"])
    assert empty["fetch_status"] == "empty"
    assert empty["body"] == ""  # not NULL -- genuinely empty, not unavailable
    assert json.loads(empty["metadata_json"])["attempts"] == 1

    permfail = description_for(connect_fn, rows["Permanent Fail Role"]["posting_id"])
    assert permfail["fetch_status"] == "unavailable"
    assert permfail["body"] is None
    perm_meta = json.loads(permfail["metadata_json"])
    assert perm_meta["attempts"] == 1
    assert perm_meta["error"]["status"] == 404

    retried_ok = description_for(connect_fn, rows["Retry Then Success Role"]["posting_id"])
    assert retried_ok["fetch_status"] == "available"
    assert retried_ok["body"] == "second attempt succeeds with a real body."
    assert json.loads(retried_ok["metadata_json"])["attempts"] == 2

    retried_fail = description_for(connect_fn, rows["Retry Twice Fail Role"]["posting_id"])
    assert retried_fail["fetch_status"] == "unavailable"
    assert retried_fail["body"] is None
    retry_meta = json.loads(retried_fail["metadata_json"])
    assert retry_meta["attempts"] == 2
    assert retry_meta["error"]["status"] == 503

    # the schema's own CHECK constraint, exercised for real rather than assumed:
    # every "unavailable" row landed with NULL body and every other status with
    # a non-NULL one, or the INSERT above would have raised IntegrityError.
    assert len(descriptions(connect_fn)) == 5


def test_idempotent_rerun_writes_zero_new_rows_when_already_described(tmp_path):
    connect_fn = make_connect(tmp_path)
    result = drive(
        connect_fn,
        [dict(title="Support Engineer", company="Acme", url="https://boards.example/idempotent",
              req_id="1", location="San Francisco, CA")],
    )
    rows = cheap_rows_by_title(connect_fn, result.run_uid)
    transport = FakeTransport()
    transport.add(rows["Support Engineer"]["url"], text_response("A stable description.", status=200))

    conn = connect_fn()
    try:
        first = run(enrich_run(conn, result.run_uid, transport=transport, profile=PERMISSIVE_PROFILE))
    finally:
        conn.close()
    assert first.fetched == 1
    assert first.available == 1
    after_first = descriptions(connect_fn)
    assert len(after_first) == 1

    # Second call: the transport has no route left queued beyond the single
    # response already consumed (FakeTransport repeats the last one), but the
    # already-described check must skip the fetch entirely regardless.
    conn = connect_fn()
    try:
        second = run(enrich_run(conn, result.run_uid, transport=transport, profile=PERMISSIVE_PROFILE))
    finally:
        conn.close()

    assert second.fetched == 0
    assert second.already_described == 1
    assert transport.call_count == 1  # unchanged from the first call
    after_second = descriptions(connect_fn)
    assert len(after_second) == 1
    assert after_second[0]["fetched_at"] == after_first[0]["fetched_at"]  # untouched, not rewritten


def test_idempotent_rerun_upgrades_an_unavailable_row_in_place(tmp_path):
    """A description stuck at 'unavailable' is NOT usable, so a rerun must
    retry it -- and when the retry succeeds, the SAME row (same
    provenance_hash) is updated to 'available' rather than a second row being
    inserted, which is exactly what `UNIQUE(provenance_hash)` plus the
    `ON CONFLICT ... WHERE fetch_status='unavailable'` upsert guarantees."""
    connect_fn = make_connect(tmp_path)
    result = drive(
        connect_fn,
        [dict(title="Support Engineer", company="Acme", url="https://boards.example/upgrade",
              req_id="1", location="San Francisco, CA")],
    )
    rows = cheap_rows_by_title(connect_fn, result.run_uid)
    url = rows["Support Engineer"]["url"]

    failing_transport = FakeTransport()
    failing_transport.add(url, text_response("gone", status=404))
    conn = connect_fn()
    try:
        first = run(enrich_run(conn, result.run_uid, transport=failing_transport, profile=PERMISSIVE_PROFILE))
    finally:
        conn.close()
    assert first.failed == 1
    assert descriptions(connect_fn)[0]["fetch_status"] == "unavailable"
    first_description_id = descriptions(connect_fn)[0]["description_id"]

    succeeding_transport = FakeTransport()
    succeeding_transport.add(url, text_response("Now it actually works.", status=200))
    conn = connect_fn()
    try:
        second = run(enrich_run(conn, result.run_uid, transport=succeeding_transport, profile=PERMISSIVE_PROFILE))
    finally:
        conn.close()

    assert second.already_described == 0  # the unavailable row was not usable
    assert second.fetched == 1
    assert second.available == 1
    rows_after = descriptions(connect_fn)
    assert len(rows_after) == 1  # updated in place, not duplicated
    assert rows_after[0]["description_id"] == first_description_id
    assert rows_after[0]["fetch_status"] == "available"
    assert rows_after[0]["body"] == "Now it actually works."


# --------------------------------------------------------------------------- #
# Bounded concurrency
# --------------------------------------------------------------------------- #
class ConcurrencyProbeTransport:
    """Independently measures true concurrency from the transport side, so the
    test can compare ground truth against what `enrich_run` itself reports."""

    def __init__(self, *, hold: float = 0.05):
        self._hold = hold
        self.calls = 0
        self.inflight = 0
        self.peak = 0
        self.host_inflight: dict[str, int] = {}
        self.host_peak: dict[str, int] = {}

    async def send(self, request) -> HttpResponse:
        host = request.host
        self.calls += 1
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        self.host_inflight[host] = self.host_inflight.get(host, 0) + 1
        self.host_peak[host] = max(self.host_peak.get(host, 0), self.host_inflight[host])
        try:
            await asyncio.sleep(self._hold)
            return HttpResponse(
                status=200, url=request.url,
                content=b"A concurrency-probe description body.",
                headers={"content-type": "text/plain"},
            )
        finally:
            self.inflight -= 1
            self.host_inflight[host] -= 1


def test_bounded_concurrency_holds_the_global_and_per_host_ceilings(tmp_path):
    connect_fn = make_connect(tmp_path)
    specs = []
    for host_n in range(2):
        for i in range(4):
            specs.append(
                dict(
                    title=f"Role {host_n}-{i}",
                    company="Acme",
                    url=f"https://host{host_n}.example/jobs/{i}",
                    req_id=f"{host_n}-{i}",
                    location="San Francisco, CA",
                )
            )
    result = drive(connect_fn, specs)

    probe = ConcurrencyProbeTransport(hold=0.05)
    conn = connect_fn()
    try:
        report = run(
            enrich_run(
                conn, result.run_uid, transport=probe, profile=PERMISSIVE_PROFILE,
                max_concurrency=3, per_host_concurrency=2,
            )
        )
    finally:
        conn.close()

    assert report.fetched == 8
    assert probe.calls == 8

    # The bound actually held, measured independently by the transport itself.
    assert probe.peak <= 3
    assert all(v <= 2 for v in probe.host_peak.values())

    # The bound was actually exercised, not trivially satisfied by accidental
    # serial execution -- with 4 postings sharing a host and a cap of 2, the
    # per-host semaphore must have been contended at least once.
    assert probe.peak >= 2
    assert any(v == 2 for v in probe.host_peak.values())

    # The module's own reported evidence matches transport-side ground truth.
    assert report.peak_concurrency == probe.peak
    assert dict(report.peak_by_host) == probe.host_peak


def test_report_counts_reconcile_with_rows_actually_written(tmp_path):
    connect_fn = make_connect(tmp_path)
    specs = [
        dict(title="Support Engineer One", company="Acme", url="https://boards.example/one",
             req_id="1", location="San Francisco, CA"),
        dict(title="Support Engineer Two", company="Acme", url="https://boards.example/two",
             req_id="2", location="San Francisco, CA"),
        dict(title="Account Executive Role", company="Acme", url="https://boards.example/three",
             req_id="3", location="San Francisco, CA"),
        dict(title="Excluded Location Role", company="Acme", url="https://boards.example/four",
             req_id="4", location="Somewhere, NonUsLand"),
    ]
    result = drive(connect_fn, specs)
    rows = cheap_rows_by_title(connect_fn, result.run_uid)

    transport = FakeTransport()
    transport.add(rows["Support Engineer One"]["url"], text_response("Body one.", status=200))
    transport.add(rows["Support Engineer Two"]["url"], text_response("Body two.", status=200))

    conn = connect_fn()
    try:
        report = run(enrich_run(conn, result.run_uid, transport=transport, profile=PROFILE))
    finally:
        conn.close()

    assert report.considered == 4
    assert report.accounted_for == report.considered
    assert report.rows_written == report.fetched
    assert report.rows_written == report.available + report.empty + report.failed

    total_rows = len(descriptions(connect_fn))
    assert total_rows == report.rows_written
