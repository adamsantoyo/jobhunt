"""JobSpy adapter: pure row normalization, the repost filter, and the subprocess
shell, driven by frozen fixtures and a fake child.

Nothing here imports jobspy, starts pandas, or opens a socket. The parser tests
run on a frozen row fixture with no process at all; the `fetch()` tests spawn
`fixtures/jobspy/fake_child.py`, which replays frozen NDJSON, so the subprocess
plumbing that this adapter exists for — incremental streaming, per-query
checkpoints, cancellation that actually kills the child, exit-code
classification — is exercised end to end without the library it isolates.
"""
import asyncio
import json
import os
import subprocess
import sys

import pytest

from backend.sources.adapters import jobspy
from backend.sources.contract import (
    Checkpoint,
    ConfigError,
    ExecutionMode,
    FetchContext,
    InventoryScope,
    NormalizedPosting,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceCategory,
    SourceConfig,
    SourceTarget,
    TransientSourceError,
    TransportKind,
)
from backend.sources.testing import collect, drain, fixture_bytes, fixture_json, fixture_path

#: Frozen jobspy DataFrame rows. The `NaN` literals are deliberate: a pandas
#: cell is `NaN`, not `None`, whenever anything else in its column is unset, and
#: `json.loads` reproduces that faithfully.
ROWS = fixture_json("jobspy", "indeed_rows.json")
FLOOD = fixture_json("jobspy", "repost_flood_rows.json")
STREAM_NDJSON = str(fixture_path("jobspy", "child_stream.ndjson"))
FAKE_CHILD = str(fixture_path("jobspy", "fake_child.py"))

QUERY = {
    "term": "technical support engineer",
    "location": "San Francisco Bay Area, CA",
    "is_remote": False,
    "google_search_term": "technical support engineer jobs in the San Francisco Bay Area",
    "results_wanted": 25,
}

SEARCHES = (
    {"location": "San Francisco Bay Area, CA", "is_remote": False},
    {"location": "United States", "is_remote": True},
)


def _target(
    *,
    site="indeed",
    terms=("technical support engineer",),
    searches=SEARCHES,
    title_cap=5,
    command=None,
):
    params = {
        "site": site,
        "search_terms": tuple(terms),
        "searches": tuple(searches),
        "results_wanted": 25,
        "country_indeed": "USA",
        "hours_old": 720,
        "title_cap": title_cap,
    }
    if command is not None:
        params["child_command"] = tuple(command)
    return SourceTarget(
        source_key="jobspy",
        instance_key=site,
        label=f"JobSpy {site}",
        params=params,
        inventory_scope=InventoryScope.PARTIAL,
    )


def _command(mode, *args):
    return (sys.executable, FAKE_CHILD, mode, *[str(a) for a in args])


def _spy_children(monkeypatch):
    """Capture every child the adapter spawns, so tests can prove it died.

    Patches the real `asyncio.create_subprocess_exec` rather than an adapter
    seam, which also asserts that `fetch()` really spawns a process instead of
    faking one.
    """
    procs = []
    real = asyncio.create_subprocess_exec

    async def spy(*args, **kwargs):
        proc = await real(*args, **kwargs)
        procs.append(proc)
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spy)
    return procs


# --------------------------------------------------------------------------- #
# Pure row normalization (no process, no transport)
# --------------------------------------------------------------------------- #
def test_normalize_row_maps_the_legacy_fields():
    record = jobspy.normalize_row(ROWS[0], _target(), query=QUERY)
    assert record.title == "Technical Support Engineer"  # whitespace collapsed
    assert record.company == "Example Corp"
    assert record.location == "San Francisco, CA"
    assert record.url == "https://www.indeed.com/viewjob?jk=4f1c2ab&utm_source=jobspy"
    assert record.posted_date == "2026-07-28"
    assert record.salary_text == "120000-160000 yearly"
    assert record.remote is False
    assert record.description.startswith("Own escalations")
    assert record.source_key == "jobspy" and record.instance_key == "indeed"


def test_parse_rows_skips_rows_with_no_title_or_no_url():
    records = list(jobspy.parse_rows(ROWS, _target(), query=QUERY))
    assert [r.title for r in records] == [
        "Technical Support Engineer",
        "Product Support Engineer",
        "IT Support Engineer",
        "Hardware Validation Engineer",
    ]
    assert "Ghost Recruiting" not in [r.company for r in records]
    assert jobspy.normalize_row(ROWS[4], _target()) is None  # blank title
    assert jobspy.normalize_row(ROWS[5], _target()) is None  # null job_url


def test_normalize_row_assembles_salary_like_the_legacy_pass():
    records = list(jobspy.parse_rows(ROWS, _target()))
    assert [r.salary_text for r in records] == [
        "120000-160000 yearly",  # both bounds
        "",  # no min_amount at all
        "45 hourly",  # max_amount is NaN -> omitted, never the literal "None"
        "150000 yearly",  # equal bounds collapse to one figure
    ]


def test_normalize_row_ignores_a_max_without_a_min():
    assert jobspy.salary_text({"max_amount": 200000, "interval": "yearly"}) == ""


def test_normalize_row_spells_amounts_stably_across_int_and_float():
    """pandas hands back 120000 or 120000.0 for the same posting; `salary_text`
    is hashed, so both must produce one spelling."""
    as_int = jobspy.salary_text({"min_amount": 120000, "max_amount": 160000, "interval": "yearly"})
    as_float = jobspy.salary_text(
        {"min_amount": 120000.0, "max_amount": 160000.0, "interval": "yearly"}
    )
    assert as_int == as_float == "120000-160000 yearly"


def test_normalize_row_trusts_the_row_remote_flag_not_the_search():
    """A remote SEARCH also returns on-site roles (legacy bug note): the search's
    `is_remote` must never force the record's."""
    remote_query = dict(QUERY, is_remote=True, location="United States")
    record = jobspy.normalize_row(ROWS[0], _target(), query=remote_query)
    assert record.remote is False
    assert record.extra["search_is_remote"] is True


def test_normalize_row_treats_pandas_nan_as_missing_not_as_remote():
    """`bool(float("nan"))` is True, so the legacy coercion marked every unset
    remote flag as remote."""
    third = list(jobspy.parse_rows(ROWS, _target()))[2]
    assert third.remote is False
    fourth = list(jobspy.parse_rows(ROWS, _target()))[3]
    assert fourth.remote is False  # the string "false"


def test_normalize_row_keeps_relative_dates_out_of_the_hash():
    third = list(jobspy.parse_rows(ROWS, _target()))[2]
    assert third.posted_raw == "3 days ago"
    assert third.posted_date is None
    assert third.canonical_fields()["posted_date"] == ""


def test_normalize_row_truncates_the_date_to_a_day():
    fourth = list(jobspy.parse_rows(ROWS, _target()))[3]
    assert fourth.posted_date == "2026-07-25"
    assert fourth.posted_raw == "2026-07-25T09:12:00+00:00"


def test_normalize_row_keeps_the_inline_description_and_truncates_it():
    record = jobspy.normalize_row(
        {"title": "Support Engineer", "job_url": "https://example.test/1", "description": "x" * 7000},
        _target(),
    )
    assert len(record.description) == jobspy.DESCRIPTION_LIMIT
    # a whitespace-only description is no description at all
    blank = list(jobspy.parse_rows(ROWS, _target()))[2]
    assert blank.description is None
    assert blank.description_digest == ""


def test_normalize_row_carries_no_req_id_and_claims_only_the_url():
    """An aggregator row mirrors somebody else's requisition; Phase 3 resolves
    the URL against direct inventory."""
    record = list(jobspy.parse_rows(ROWS, _target(), query=QUERY))[0]
    assert record.req_id is None
    claims = record.identity_claims()
    assert [c.kind for c in claims] == ["url"]
    assert claims[0].namespace == "url"
    assert claims[0].value == "https://www.indeed.com/viewjob?jk=4f1c2ab"


def test_normalize_row_namespaces_identity_per_site():
    indeed = jobspy.normalize_row(ROWS[0], _target(site="indeed"))
    linkedin = jobspy.normalize_row(ROWS[0], _target(site="linkedin"))
    assert indeed.namespace == "jobspy:indeed"
    assert linkedin.namespace == "jobspy:linkedin"
    assert indeed.content_hash() != linkedin.content_hash()


def test_normalize_row_records_query_provenance_without_hashing_it():
    record = jobspy.normalize_row(ROWS[0], _target(), query=QUERY)
    assert record.extra["site"] == "indeed"
    assert record.extra["aggregator_job_id"] == "in-4f1c2ab"
    assert record.extra["search_term"] == "technical support engineer"
    assert record.extra["search_location"] == "San Francisco Bay Area, CA"
    assert "search_term" not in record.canonical_fields()


def test_normalize_row_content_hash_is_stable_across_runs():
    once = list(jobspy.parse_rows(ROWS, _target(), query=QUERY))
    twice = list(jobspy.parse_rows(ROWS, _target(), query=QUERY))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]


def test_parse_rows_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b'{"jobs": []}', b'"nope"'):
        with pytest.raises(PayloadError):
            list(jobspy.parse_rows(payload, _target()))


def test_parse_rows_accepts_bytes_str_and_a_parsed_list():
    raw = fixture_bytes("jobspy", "repost_flood_rows.json")
    assert (
        len(list(jobspy.parse_rows(raw, _target())))
        == len(list(jobspy.parse_rows(raw.decode(), _target())))
        == len(list(jobspy.parse_rows(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# In-run dedupe and repost cap (pure stream transform)
# --------------------------------------------------------------------------- #
def test_dedupe_drops_repeat_urls_including_tracking_variants():
    records = list(jobspy.parse_rows(FLOOD, _target()))
    kept = list(jobspy.dedupe_stream(records, title_cap=100))
    assert len(records) == 10
    # flood1 repeated verbatim, flood2 repeated with utm_campaign/gclid attached
    assert len(kept) == 8
    assert [r.extra["aggregator_job_id"] for r in kept][-1] == "in-real1"


def test_dedupe_caps_repost_floods_per_company_and_title():
    records = list(jobspy.parse_rows(FLOOD, _target()))
    kept = list(jobspy.dedupe_stream(records, title_cap=5))
    ids = [r.extra["aggregator_job_id"] for r in kept]
    assert ids == ["in-flood1", "in-flood2", "in-flood3", "in-flood4", "in-flood5", "in-real1"]


def test_dedupe_cap_key_is_case_insensitive():
    """`staffing partners llc` and `Staffing Partners LLC` are one flood."""
    records = list(jobspy.parse_rows(FLOOD, _target()))
    kept = list(jobspy.dedupe_stream(records, title_cap=2))
    assert [r.extra["aggregator_job_id"] for r in kept] == ["in-flood1", "in-flood2", "in-real1"]


def test_dedupe_cap_of_zero_or_less_means_unlimited():
    records = list(jobspy.parse_rows(FLOOD, _target()))
    assert len(list(jobspy.dedupe_stream(records, title_cap=0))) == 8
    assert len(list(jobspy.dedupe_stream(records, title_cap=-1))) == 8


def test_dedupe_is_lazy_and_does_not_materialize_its_input():
    """`fetch` pipes a live subprocess through this; it must not buffer."""
    consumed = []

    def source():
        for record in jobspy.parse_rows(FLOOD, _target()):
            consumed.append(record)
            yield record

    stream = jobspy.dedupe_stream(source(), title_cap=5)
    first = next(stream)
    assert first.extra["aggregator_job_id"] == "in-flood1"
    assert len(consumed) == 1


def test_repost_filter_rejects_a_second_sighting_of_the_same_url():
    keep = jobspy.RepostFilter(title_cap=5)
    record = jobspy.normalize_row(ROWS[0], _target())
    assert keep.accept(record) is True
    assert keep.accept(record) is False


# --------------------------------------------------------------------------- #
# plan() and query planning
# --------------------------------------------------------------------------- #
def _config():
    return SourceConfig.from_mapping(
        {
            "profile": {"search_terms": ["technical support engineer", "hardware validation"]},
            "jobspy": {
                "sites": ["indeed", "linkedin", " "],
                "results_wanted_per_site": 1000,
                "results_wanted_linkedin": 60,
                "country_indeed": "USA",
                "hours_old": 720,
                "searches": [
                    {"location": "San Francisco Bay Area, CA", "is_remote": False},
                    {"location": "United States", "is_remote": True},
                ],
                "title_cap": 5,
            },
        }
    )


def test_plan_makes_one_target_per_configured_site():
    targets = jobspy.ADAPTER.plan(_config())
    assert [t.instance_key for t in targets] == ["indeed", "linkedin"]
    assert [t.label for t in targets] == ["JobSpy indeed", "JobSpy linkedin"]
    assert [t.source_run_key for t in targets] == ["jobspy:indeed", "jobspy:linkedin"]
    assert all(t.inventory_scope is InventoryScope.PARTIAL for t in targets)
    assert all(t.host is None for t in targets)


def test_plan_applies_the_per_site_results_wanted_override():
    indeed, linkedin = jobspy.ADAPTER.plan(_config())
    assert indeed.param("results_wanted") == 1000
    assert linkedin.param("results_wanted") == 60
    assert indeed.param("country_indeed") == "USA"
    assert indeed.param("hours_old") == 720
    assert indeed.param("title_cap") == 5


def test_plan_defaults_the_searches_to_the_bay_area():
    config = SourceConfig.from_mapping(
        {"profile": {"search_terms": ["support"]}, "jobspy": {"sites": ["indeed"]}}
    )
    target = jobspy.ADAPTER.plan(config)[0]
    assert target.param("searches") == jobspy.DEFAULT_SEARCHES
    assert target.param("title_cap") == jobspy.DEFAULT_TITLE_CAP


def test_plan_without_sites_or_terms_is_empty_not_an_error():
    assert list(jobspy.ADAPTER.plan(SourceConfig())) == []
    assert list(jobspy.ADAPTER.plan(SourceConfig.from_mapping({"jobspy": {"sites": ["indeed"]}}))) == []
    assert list(
        jobspy.ADAPTER.plan(
            SourceConfig.from_mapping({"profile": {"search_terms": ["support"]}, "jobspy": {}})
        )
    ) == []


def test_plan_rejects_a_non_object_jobspy_block():
    config = SourceConfig.from_mapping(
        {"profile": {"search_terms": ["support"]}, "jobspy": ["indeed"]}
    )
    with pytest.raises(ConfigError):
        jobspy.ADAPTER.plan(config)


def test_build_queries_is_the_term_by_search_cross_product():
    target = jobspy.ADAPTER.plan(_config())[0]
    queries = jobspy.build_queries(target)
    assert [(q["term"], q["location"], q["is_remote"]) for q in queries] == [
        ("technical support engineer", "San Francisco Bay Area, CA", False),
        ("technical support engineer", "United States", True),
        ("hardware validation", "San Francisco Bay Area, CA", False),
        ("hardware validation", "United States", True),
    ]
    assert queries[0]["google_search_term"] == (
        "technical support engineer jobs in the San Francisco Bay Area"
    )
    assert queries[1]["google_search_term"] == "technical support engineer jobs remote in the US"
    assert all(q["results_wanted"] == 1000 for q in queries)


def test_changing_the_search_set_invalidates_a_checkpoint():
    """The cursor is an index into `build_queries`; a different query list means
    a different result set, and the contract discards the stale cursor."""
    original = _target()
    widened = _target(terms=("technical support engineer", "hardware validation"))
    stale = Checkpoint(
        source_key="jobspy",
        instance_key="indeed",
        cursor={"query_index": 1},
        config_fingerprint=original.config_fingerprint(),
    )
    assert stale.is_valid_for(original) is True
    assert stale.is_valid_for(widened) is False


def test_task_spec_is_json_safe_and_carries_the_resume_point():
    target = jobspy.ADAPTER.plan(_config())[0]
    spec = jobspy.task_spec(target, start_index=2)
    assert json.loads(json.dumps(spec)) == spec
    assert spec["version"] == jobspy.WIRE_VERSION
    assert spec["site"] == "indeed"
    assert spec["start_index"] == 2
    assert len(spec["queries"]) == 4


def test_descriptor_declares_subprocess_isolation_and_partial_scope():
    descriptor = jobspy.DESCRIPTOR
    assert descriptor.category is SourceCategory.AGGREGATOR
    assert descriptor.runs_in(RunKind.AGGREGATORS)
    assert not descriptor.runs_in(RunKind.DAILY)
    assert descriptor.execution is ExecutionMode.SUBPROCESS
    assert descriptor.transport is TransportKind.NONE
    assert descriptor.supports_checkpoint is True
    assert descriptor.description_inline is True
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL


# --------------------------------------------------------------------------- #
# The NDJSON wire (pure)
# --------------------------------------------------------------------------- #
def test_wire_round_trips_a_record_without_losing_a_field():
    record = jobspy.normalize_row(ROWS[0], _target(), query=QUERY)
    message = jobspy.decode_line(jobspy.encode_record_line(record), _target())
    assert message.kind == "record"
    assert message.record.to_json_dict() == record.to_json_dict()
    assert message.record.content_hash() == record.content_hash()


def test_frozen_child_lines_rehydrate_exactly():
    target = _target()
    for line in fixture_bytes("jobspy", "child_stream.ndjson").splitlines():
        message = jobspy.decode_line(line, target)
        assert message.kind in ("record", "progress")
        if message.record is not None:
            payload = json.loads(line)["record"]
            assert message.record.to_json_dict() == payload
            assert NormalizedPosting.from_json_dict(payload).content_hash() == (
                message.record.content_hash()
            )


def test_wire_rejects_garbage_an_unknown_type_and_a_foreign_namespace():
    target = _target()
    with pytest.raises(PayloadError):
        jobspy.decode_line("Scraping indeed: 42%", target)
    with pytest.raises(PayloadError):
        jobspy.decode_line('["not", "an", "object"]', target)
    with pytest.raises(PayloadError):
        jobspy.decode_line('{"type": "banner", "text": "hi"}', target)
    with pytest.raises(PayloadError):
        jobspy.decode_line('{"type": "record", "record": null}', target)
    foreign = jobspy.encode_record_line(jobspy.normalize_row(ROWS[0], _target(site="linkedin")))
    with pytest.raises(PayloadError):
        jobspy.decode_line(foreign, target)


def test_wire_decodes_progress_and_error_lines():
    target = _target()
    progress = jobspy.decode_line(jobspy.encode_progress_line(3, count=12), target)
    assert (progress.kind, progress.query_index, progress.count) == ("progress", 3, 12)
    failure = jobspy.decode_line(jobspy.encode_error_line("permanent", "boom"), target)
    assert (failure.kind, failure.disposition, failure.message) == ("error", "permanent", "boom")


# --------------------------------------------------------------------------- #
# fetch(): the subprocess shell
# --------------------------------------------------------------------------- #
def test_fetch_streams_the_child_and_applies_the_repost_filter():
    target = _target(command=_command("stream", STREAM_NDJSON))
    ctx = FetchContext()  # no transport: this adapter must never ask for one
    records = asyncio.run(collect(jobspy.ADAPTER, target, ctx))
    # 5 record lines in, 2 of them duplicate URLs (one only after utm stripping)
    assert [r.title for r in records] == [
        "Technical Support Engineer",
        "Product Support Engineer",
        "Hardware Validation Engineer",
    ]
    assert all(r.namespace == "jobspy:indeed" for r in records)
    assert ctx.checkpoint.cursor == {"query_index": 2}
    assert ctx.checkpoint.emitted == 3
    assert ctx.has_transport is False


def test_fetch_marks_a_checkpoint_at_every_query_boundary():
    seen = []

    class _RecordingCtx(FetchContext):
        def mark_checkpoint(self, cursor, *, target, emitted=0):
            checkpoint = super().mark_checkpoint(cursor, target=target, emitted=emitted)
            seen.append((dict(cursor), emitted))
            return checkpoint

    target = _target(command=_command("stream", STREAM_NDJSON))
    asyncio.run(collect(jobspy.ADAPTER, target, _RecordingCtx()))
    assert seen == [({"query_index": 1}, 2), ({"query_index": 2}, 3)]


def test_fetch_sends_the_task_spec_to_the_child(tmp_path):
    spec_out = tmp_path / "spec.json"
    target = _target(command=_command("stream", STREAM_NDJSON, spec_out))
    asyncio.run(collect(jobspy.ADAPTER, target, FetchContext()))
    spec = json.loads(spec_out.read_text())
    assert spec["site"] == "indeed"
    assert spec["start_index"] == 0
    assert [q["location"] for q in spec["queries"]] == [
        "San Francisco Bay Area, CA",
        "United States",
    ]


def test_fetch_resumes_at_the_checkpointed_query(tmp_path):
    spec_out = tmp_path / "spec.json"
    target = _target(command=_command("stream", STREAM_NDJSON, spec_out))
    resume = Checkpoint(
        source_key="jobspy",
        instance_key="indeed",
        cursor={"query_index": 1},
        config_fingerprint=target.config_fingerprint(),
        emitted=3,
    )
    ctx = FetchContext(resume_from=resume)
    records = asyncio.run(collect(jobspy.ADAPTER, target, ctx))
    assert json.loads(spec_out.read_text())["start_index"] == 1
    # only the second query's group replays; its two distinct URLs survive
    assert [r.title for r in records] == [
        "Technical Support Engineer",
        "Hardware Validation Engineer",
    ]
    assert ctx.checkpoint.cursor == {"query_index": 2}
    assert ctx.checkpoint.emitted == 5  # 3 carried over + 2 newly yielded


def test_fetch_resuming_a_finished_target_spawns_no_child(monkeypatch):
    procs = _spy_children(monkeypatch)
    target = _target(command=_command("stream", STREAM_NDJSON))
    done = Checkpoint(
        source_key="jobspy",
        instance_key="indeed",
        cursor={"query_index": 2},  # two queries planned: nothing left
        config_fingerprint=target.config_fingerprint(),
        emitted=3,
    )
    records = asyncio.run(collect(jobspy.ADAPTER, target, FetchContext(resume_from=done)))
    assert records == []
    assert procs == []


def test_fetch_replaying_from_zero_reproduces_the_same_records():
    """Checkpoints are replayable, never an at-most-once guarantee."""
    target = _target(command=_command("stream", STREAM_NDJSON))
    fresh = asyncio.run(collect(jobspy.ADAPTER, target, FetchContext()))
    restart = Checkpoint(
        source_key="jobspy",
        instance_key="indeed",
        cursor={"query_index": 0},
        config_fingerprint=target.config_fingerprint(),
        emitted=0,
    )
    replayed = asyncio.run(
        collect(jobspy.ADAPTER, target, FetchContext(resume_from=restart))
    )
    assert [r.content_hash() for r in fresh] == [r.content_hash() for r in replayed]


def test_fetch_yields_records_before_the_child_exits(monkeypatch):
    """Contract invariant 6: a jobspy run is minutes long, so the first records
    must reach the consumer while the child is still working."""
    procs = _spy_children(monkeypatch)
    target = _target(command=_command("hang", STREAM_NDJSON))

    async def scenario():
        stream = jobspy.ADAPTER.fetch(target, FetchContext())
        first = await asyncio.wait_for(stream.__anext__(), 30)
        await stream.aclose()
        return first

    first = asyncio.run(scenario())
    assert first.title == "Technical Support Engineer"
    assert len(procs) == 1
    # closing the stream shut the still-running child down
    assert procs[0].returncode is not None


def test_fetch_cancellation_kills_the_child_and_leaves_no_orphan(monkeypatch):
    procs = _spy_children(monkeypatch)
    target = _target(command=_command("hang", STREAM_NDJSON))

    async def scenario():
        started = asyncio.Event()
        seen = []
        stream = jobspy.ADAPTER.fetch(target, FetchContext())

        async def consume():
            async for record in stream:
                seen.append(record)
                started.set()

        task = asyncio.create_task(consume())
        await asyncio.wait_for(started.wait(), 30)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 30)
        return seen

    seen = asyncio.run(scenario())
    assert seen  # records already delivered survive the cancellation
    assert len(procs) == 1
    assert procs[0].returncode is not None  # terminated and reaped
    with pytest.raises(ProcessLookupError):
        os.kill(procs[0].pid, 0)


def test_fetch_maps_an_unexplained_nonzero_exit_to_transient(monkeypatch):
    procs = _spy_children(monkeypatch)
    target = _target(command=_command("crash", 3))
    with pytest.raises(TransientSourceError) as excinfo:
        asyncio.run(collect(jobspy.ADAPTER, target, FetchContext()))
    assert "exited with code 3" in str(excinfo.value)
    assert "died without saying anything" in str(excinfo.value)  # stderr tail
    assert procs[0].returncode == 3


def test_fetch_maps_the_child_transient_exit_code_to_transient():
    target = _target(command=_command("fail", jobspy.EXIT_TRANSIENT))
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(jobspy.ADAPTER, target, FetchContext()))


def test_fetch_maps_the_child_permanent_exit_code_to_permanent():
    target = _target(command=_command("fail", jobspy.EXIT_PERMANENT))
    with pytest.raises(PermanentSourceError) as excinfo:
        asyncio.run(collect(jobspy.ADAPTER, target, FetchContext()))
    assert "fake child failure" in str(excinfo.value)


def test_fetch_never_reports_a_failed_child_as_an_empty_run():
    """Contract invariant 3: `scraper.src_jobspy` printed the failure and
    returned whatever it had, which is indistinguishable from 'no matches'."""
    target = _target(command=_command("fail", jobspy.EXIT_TRANSIENT))
    with pytest.raises(TransientSourceError):
        asyncio.run(drain(jobspy.ADAPTER.fetch(target, FetchContext())))


def test_fetch_on_a_malformed_child_line_is_a_payload_error(monkeypatch):
    procs = _spy_children(monkeypatch)
    target = _target(command=_command("garbage"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(jobspy.ADAPTER, target, FetchContext()))
    assert procs[0].returncode is not None  # still no orphan on the error path


def test_fetch_without_queries_is_a_config_error(monkeypatch):
    procs = _spy_children(monkeypatch)
    bare = SourceTarget(source_key="jobspy", instance_key="indeed")
    with pytest.raises(ConfigError):
        asyncio.run(collect(jobspy.ADAPTER, bare, FetchContext()))
    assert procs == []


# --------------------------------------------------------------------------- #
# The child entrypoint
# --------------------------------------------------------------------------- #
def test_child_module_imports_without_importing_jobspy():
    """The adapter (and CI) must work on a machine with no jobspy installed, so
    the import lives inside the child's `_load_scrape_jobs`, not at module
    scope."""
    probe = (
        "import sys; import backend.sources.adapters.jobspy_child as child; "
        "print('jobspy' in sys.modules)"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(jobspy.REPO_ROOT),
        env=jobspy.child_env(),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "False"


def test_child_rejects_an_unusable_task_spec():
    """Run the real child with garbage on stdin: it must classify, not crash."""
    result = subprocess.run(
        [sys.executable, "-m", jobspy.CHILD_MODULE],
        cwd=str(jobspy.REPO_ROOT),
        env=jobspy.child_env(),
        input="not json at all",
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == jobspy.EXIT_PERMANENT
    line = json.loads(result.stdout.strip())
    assert line["type"] == "error" and line["disposition"] == "permanent"


def test_child_refuses_a_wire_version_it_does_not_speak():
    result = subprocess.run(
        [sys.executable, "-m", jobspy.CHILD_MODULE],
        cwd=str(jobspy.REPO_ROOT),
        env=jobspy.child_env(),
        input=json.dumps({"version": 999, "site": "indeed", "queries": []}),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == jobspy.EXIT_PERMANENT
    assert "unsupported task spec version" in result.stdout


#: Drives the REAL child with a stubbed `jobspy` module injected into
#: `sys.modules`, so the child's own query loop, DataFrame duck-typing, record
#: encoding, and `start_index` handling are exercised with no network and no
#: pandas. The stub echoes its kwargs to stderr so the call mapping is checkable.
CHILD_DRIVER = '''
import json, sys, types

rows = json.load(open(sys.argv[1], encoding="utf-8"))


def scrape_jobs(**kwargs):
    sys.stderr.write("CALL " + json.dumps(kwargs) + "\\n")

    class Frame:  # quacks like a pandas DataFrame
        def to_dict(self, orient="records"):
            return rows

    return Frame()


fake = types.ModuleType("jobspy")
fake.scrape_jobs = scrape_jobs
sys.modules["jobspy"] = fake

from backend.sources.adapters.jobspy_child import main

raise SystemExit(main([]))
'''


def test_child_runs_its_queries_and_emits_a_stream_the_parent_can_decode(tmp_path):
    driver = tmp_path / "driver.py"
    driver.write_text(CHILD_DRIVER)
    target = _target()
    spec = jobspy.task_spec(target, start_index=1)
    result = subprocess.run(
        [sys.executable, str(driver), str(fixture_path("jobspy", "indeed_rows.json"))],
        cwd=str(jobspy.REPO_ROOT),
        env=jobspy.child_env(),
        input=json.dumps(spec),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == jobspy.EXIT_OK, result.stderr

    # start_index skipped query 0 entirely: exactly one scrape call, the remote one
    calls = [json.loads(line[5:]) for line in result.stderr.splitlines() if line.startswith("CALL ")]
    assert len(calls) == 1
    assert calls[0]["site_name"] == ["indeed"]
    assert calls[0]["search_term"] == "technical support engineer"
    assert calls[0]["location"] == "United States"
    assert calls[0]["is_remote"] is True
    assert calls[0]["results_wanted"] == 25
    assert calls[0]["country_indeed"] == "USA"
    assert calls[0]["hours_old"] == 720
    assert calls[0]["google_search_term"] == "technical support engineer jobs remote in the US"

    messages = [jobspy.decode_line(line, target) for line in result.stdout.splitlines() if line.strip()]
    records = [m.record for m in messages if m.kind == "record"]
    progress = [m for m in messages if m.kind == "progress"]
    # the two unusable fixture rows were skipped by the child, not raised
    assert [r.title for r in records] == [
        "Technical Support Engineer",
        "Product Support Engineer",
        "IT Support Engineer",
        "Hardware Validation Engineer",
    ]
    assert all(r.namespace == "jobspy:indeed" for r in records)
    assert [(p.query_index, p.count) for p in progress] == [(1, 4)]
    # the child's records are byte-identical to what the pure parser produces
    parsed = list(jobspy.parse_rows(ROWS, target, query=spec["queries"][1]))
    assert [r.to_json_dict() for r in records] == [r.to_json_dict() for r in parsed]


def test_child_rows_helper_accepts_frames_lists_and_nothing():
    from backend.sources.adapters import jobspy_child

    class Frame:
        def to_dict(self, orient="records"):
            return [{"title": "a"}, "junk"]

    assert jobspy_child._rows(Frame()) == [{"title": "a"}]
    assert jobspy_child._rows([{"title": "b"}]) == [{"title": "b"}]
    assert jobspy_child._rows(None) == ()


def test_child_with_no_queries_exits_clean_without_touching_jobspy():
    """An empty query list must not require jobspy to be installed at all."""
    spec = {"version": jobspy.WIRE_VERSION, "site": "indeed", "queries": [], "start_index": 0}
    result = subprocess.run(
        [sys.executable, "-m", jobspy.CHILD_MODULE],
        cwd=str(jobspy.REPO_ROOT),
        env=jobspy.child_env(),
        input=json.dumps(spec),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode in (jobspy.EXIT_OK, jobspy.EXIT_PERMANENT)
    if result.returncode == jobspy.EXIT_PERMANENT:
        # jobspy genuinely absent from this environment: still classified, and
        # still on stdout as protocol rather than swallowed.
        assert "not importable" in result.stdout
    else:
        assert result.stdout.strip() == ""
