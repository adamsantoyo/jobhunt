"""iCIMS adapter: parsing/transport split, driven by frozen fixtures.

Mirrors `test_source_greenhouse.py`'s structure and `smartrecruiters.py`'s
pagination-and-checkpoint test shape (see `test_source_contract.py`'s
`PagedAdapter`), adapted for an HTML regex source: parsing is exercised page
by page with no transport, and `fetch()`'s own concerns -- the pagination
loop, the empty-page stop condition, in-walk duplicate suppression, and
checkpoint round-trips -- are exercised separately through `FakeTransport`.
"""
import asyncio

import pytest

from backend.sources.adapters import icims
from backend.sources.contract import (
    Checkpoint,
    ConfigError,
    FetchContext,
    HttpResponse,
    InventoryScope,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceConfig,
    SourceTarget,
    TransientSourceError,
)
from backend.sources.testing import FakeTransport, collect, fixture_bytes, text_response

SEARCH_URL = icims.search_url("careers-fhcrc")


def _target(host="careers-fhcrc", name="Fred Hutchinson Cancer Center"):
    return SourceTarget(
        source_key="icims",
        instance_key=host,
        label=name,
        params={"host": host, "company": name},
        inventory_scope=InventoryScope.COMPLETE,
        host=f"{host}.icims.com",
    )


def _page(name):
    return fixture_bytes("icims", name)


def _page_text(name):
    return _page(name).decode("utf-8")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_page_from_frozen_fixture():
    records = list(icims.parse_page(_page("page0.html"), _target()))
    assert [r.req_id for r in records] == ["482910", "482911"]
    assert all(r.source_key == "icims" and r.instance_key == "careers-fhcrc" for r in records)
    assert all(r.company == "Fred Hutchinson Cancer Center" for r in records)


def test_parse_page_strips_tags_and_the_title_a11y_prefix():
    first = list(icims.parse_page(_page("page0.html"), _target()))[0]
    assert first.title == "Senior Research Scientist"


def test_parse_page_url_excludes_the_query_string():
    first = list(icims.parse_page(_page("page0.html"), _target()))[0]
    assert first.url == "https://careers-fhcrc.icims.com/jobs/482910/senior-research-scientist/job"


def test_parse_page_extracts_location_within_the_lookahead_window():
    first = list(icims.parse_page(_page("page0.html"), _target()))[0]
    assert first.location == "Seattle, WA"


def test_parse_page_missing_location_is_empty_not_an_error():
    second = list(icims.parse_page(_page("page0.html"), _target()))[1]
    assert second.location == ""


def test_parse_page_skips_the_icon_only_anchor_without_failing_the_page():
    """An anchor with no text content (title reduces to "") cannot be
    identified and must not blank the rest of the page."""
    records = list(icims.parse_page(_page("page0.html"), _target()))
    assert len(records) == 2
    assert "482912" not in [r.req_id for r in records]


def test_parse_page_never_scrapes_a_posted_date():
    """iCIMS' search-results HTML carries no reliable posting date; `scraper.py`
    never captured one for this source either."""
    records = list(icims.parse_page(_page("page0.html"), _target()))
    assert all(r.posted_date is None and r.posted_raw == "" for r in records)


def test_parse_page_identity_prefers_the_icims_job_id():
    first = list(icims.parse_page(_page("page0.html"), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "icims:careers-fhcrc",
        "482910",
    )
    assert claims[1].kind == "url"


def test_parse_page_content_hash_is_stable_across_runs():
    once = list(icims.parse_page(_page("page0.html"), _target()))
    twice = list(icims.parse_page(_page("page0.html"), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    # Same page, same bytes, different instance -> different identity namespace.
    other = list(icims.parse_page(_page("page0.html"), _target(host="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_page_accepts_bytes_and_str_identically():
    raw = _page("page0.html")
    assert (
        len(list(icims.parse_page(raw, _target())))
        == len(list(icims.parse_page(raw.decode(), _target())))
    )


def test_parse_page_rejects_a_payload_that_is_not_text():
    with pytest.raises(PayloadError):
        list(icims.parse_page(12345, _target()))


def test_parse_page_rejects_invalid_utf8_bytes():
    with pytest.raises(PayloadError):
        list(icims.parse_page(b"\xff\xfe not utf-8", _target()))


def test_parse_page_on_an_empty_results_page_yields_nothing_without_raising():
    """A genuinely empty board is a positive assertion, not a parse failure."""
    assert list(icims.parse_page(_page("empty.html"), _target())) == []


def test_page_has_job_anchors_distinguishes_results_from_no_results():
    assert icims.page_has_job_anchors(_page_text("page0.html")) is True
    assert icims.page_has_job_anchors(_page_text("empty.html")) is False


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_host_map():
    config = SourceConfig.from_mapping(
        {
            "companies": {
                "icims": {
                    "careers-fhcrc": {"name": "Fred Hutchinson Cancer Center"},
                    "jobs-avalara": {"name": "Avalara"},
                    "": {"name": "junk"},
                }
            }
        }
    )
    targets = icims.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["careers-fhcrc", "jobs-avalara"]
    assert [t.label for t in targets] == ["Fred Hutchinson Cancer Center", "Avalara"]
    assert [t.host for t in targets] == ["careers-fhcrc.icims.com", "jobs-avalara.icims.com"]
    assert all(t.inventory_scope is InventoryScope.COMPLETE for t in targets)
    assert targets[0].param("host") == "careers-fhcrc"


def test_plan_falls_back_to_the_host_when_name_is_missing():
    config = SourceConfig.from_mapping({"companies": {"icims": {"careers-fhcrc": {}}}})
    targets = icims.ADAPTER.plan(config)
    assert targets[0].label == "careers-fhcrc"


def test_plan_without_icims_config_is_empty_not_an_error():
    assert list(icims.ADAPTER.plan(SourceConfig())) == []


def test_plan_rejects_a_non_object_entry():
    config = SourceConfig.from_mapping({"companies": {"icims": {"careers-fhcrc": "Fred Hutch"}}})
    with pytest.raises(ConfigError):
        icims.ADAPTER.plan(config)


def test_descriptor_declares_daily_full_direct_and_checkpointing():
    descriptor = icims.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    # Real pagination: a crash mid-walk should resume, not restart.
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.COMPLETE


# --------------------------------------------------------------------------- #
# fetch(): pagination, dedup, and checkpoints
# --------------------------------------------------------------------------- #
def test_fetch_walks_pages_until_an_empty_page_and_streams_records():
    transport = FakeTransport().add(
        SEARCH_URL,
        text_response(_page_text("page0.html")),
        text_response(_page_text("page1.html")),
        text_response(_page_text("empty.html")),
    )
    records = asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))
    # page1's repeat of job 482910 is suppressed by the in-walk seen-id set.
    assert [r.req_id for r in records] == ["482910", "482911", "482920"]
    assert transport.call_count == 3
    assert [r.params["pr"] for r in transport.requests] == [0, 1, 2]
    assert all(r.params["in_iframe"] == "1" for r in transport.requests)


def test_fetch_stops_at_max_pages_even_when_every_page_has_results():
    """The six-page cap is a hard stop, not merely advisory."""
    transport = FakeTransport(default=lambda req: text_response(_page_text("page0.html")))
    records = asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == icims.MAX_PAGES
    # Every page repeats the same two jobs; the seen-id set collapses them.
    assert sorted(r.req_id for r in records) == ["482910", "482911"]


def test_fetch_deduplicates_a_repeated_job_id_within_the_walk():
    transport = FakeTransport().add(
        SEARCH_URL,
        text_response(_page_text("page0.html")),
        text_response(_page_text("page1.html")),
        text_response(_page_text("empty.html")),
    )
    records = asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))
    assert len([r for r in records if r.req_id == "482910"]) == 1


def test_fetch_marks_a_checkpoint_after_each_page():
    transport = FakeTransport().add(
        SEARCH_URL,
        text_response(_page_text("page0.html")),
        text_response(_page_text("empty.html")),
    )
    ctx = FetchContext(transport=transport)
    records = asyncio.run(collect(icims.ADAPTER, _target(), ctx))
    assert ctx.checkpoint is not None
    assert ctx.checkpoint.cursor["next_page"] == 1
    assert ctx.checkpoint.emitted == len(records) == 2


def test_fetch_resumes_from_a_valid_checkpoint_without_refetching_earlier_pages():
    transport = FakeTransport().add(
        SEARCH_URL,
        text_response(_page_text("page1.html")),
        text_response(_page_text("empty.html")),
    )
    target = _target()
    checkpoint = Checkpoint(
        source_key="icims",
        instance_key="careers-fhcrc",
        cursor={"next_page": 1},
        config_fingerprint=target.config_fingerprint(),
        emitted=2,
    )
    ctx = FetchContext(transport=transport, resume_from=checkpoint)
    records = asyncio.run(collect(icims.ADAPTER, target, ctx))
    # Resuming re-runs the seen-id set fresh, so a page revisited on resume
    # may re-emit an id already written by a prior attempt -- expected and
    # safe per invariant 5 (the writer dedupes on identity).
    assert [r.req_id for r in records] == ["482920", "482910"]
    assert transport.requests[0].params["pr"] == 1
    assert transport.call_count == 2


def test_fetch_ignores_a_checkpoint_with_a_stale_config_fingerprint():
    transport = FakeTransport().add(
        SEARCH_URL,
        text_response(_page_text("page0.html")),
        text_response(_page_text("empty.html")),
    )
    target = _target()
    stale = Checkpoint(
        source_key="icims",
        instance_key="careers-fhcrc",
        cursor={"next_page": 5},
        config_fingerprint="stale-fingerprint",
        emitted=99,
    )
    ctx = FetchContext(transport=transport, resume_from=stale)
    records = asyncio.run(collect(icims.ADAPTER, target, ctx))
    assert transport.requests[0].params["pr"] == 0
    assert [r.req_id for r in records] == ["482910", "482911"]


def test_fetch_on_a_missing_portal_is_permanent_not_empty():
    transport = FakeTransport().add(SEARCH_URL, text_response("not found", status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(SEARCH_URL, text_response("", status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_invalid_utf8_bytes_is_a_payload_error():
    transport = FakeTransport().add(
        SEARCH_URL,
        HttpResponse(status=200, url=SEARCH_URL, content=b"\xff\xfe garbage"),
    )
    with pytest.raises(PayloadError):
        asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce
    for a single-page (no-duplicate) walk."""
    transport = FakeTransport().add(
        SEARCH_URL,
        text_response(_page_text("page1.html")),
        text_response(_page_text("empty.html")),
    )
    fetched = asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(icims.parse_page(_page("page1.html"), _target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


def test_fetch_requires_the_host_param():
    transport = FakeTransport().add(SEARCH_URL, text_response(_page_text("empty.html")))
    bare = SourceTarget(source_key="icims", instance_key="careers-fhcrc")
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(icims.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_on_an_empty_portal_yields_nothing_and_does_not_raise():
    """An empty portal is a positive assertion, and only reachable via a 200."""
    transport = FakeTransport().add(SEARCH_URL, text_response(_page_text("empty.html")))
    records = asyncio.run(collect(icims.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 1
