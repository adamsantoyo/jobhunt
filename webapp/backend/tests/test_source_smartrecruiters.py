"""SmartRecruiters adapter: pagination/checkpoint worked example.

Parser tests run with no transport at all (frozen fixtures). `fetch()` tests
drive `FakeTransport` to exercise the offset-paging loop, checkpoint round
trips, resume, and replay-safety, mirroring the paginated adapter contract
tests in `test_source_contract.py`.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import smartrecruiters
from backend.sources.contract import (
    Checkpoint,
    ConfigError,
    FetchContext,
    InventoryScope,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceConfig,
    SourceTarget,
    TransientSourceError,
)
from backend.sources.testing import FakeTransport, collect, fixture_bytes, json_response, text_response

POSTINGS = smartrecruiters.postings_url("examplecorp")


def _target(slug="examplecorp", name="Example Corp"):
    return SourceTarget(
        source_key="smartrecruiters",
        instance_key=slug,
        label=name,
        params={"slug": slug, "company": name},
        inventory_scope=InventoryScope.COMPLETE,
        host=smartrecruiters.API_HOST,
    )


def _page1():
    return fixture_bytes("smartrecruiters", "page1.json")


def _page2():
    return fixture_bytes("smartrecruiters", "page2.json")


def _single_page():
    return fixture_bytes("smartrecruiters", "single_page.json")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_page_from_frozen_fixture():
    records = list(smartrecruiters.parse_page(_page1(), _target()))
    assert [r.req_id for r in records] == ["743999881234567", "743999881234568"]
    assert all(r.source_key == "smartrecruiters" and r.instance_key == "examplecorp" for r in records)
    assert all(r.company == "Example Corp" for r in records)


def test_parse_page_joins_location_parts_and_maps_remote():
    first = list(smartrecruiters.parse_page(_page1(), _target()))[0]
    assert first.location == "Austin, TX, US"
    assert first.remote is True


def test_parse_page_missing_location_is_empty_and_not_remote():
    second = list(smartrecruiters.parse_page(_page1(), _target()))[1]
    assert second.location == ""
    assert second.remote is False


def test_parse_page_builds_the_jobs_apply_url():
    first = list(smartrecruiters.parse_page(_page1(), _target()))[0]
    assert first.url == "https://jobs.smartrecruiters.com/examplecorp/743999881234567"
    assert first.url == smartrecruiters.job_url("examplecorp", 743999881234567)


def test_parse_page_truncates_released_date_and_normalizes_it():
    first = list(smartrecruiters.parse_page(_page1(), _target()))[0]
    assert first.posted_date == "2026-07-01"
    assert first.posted_raw == "2026-07-01"


def test_parse_page_keeps_unparseable_dates_out_of_the_hash():
    """Second page's second item has a non-ISO `releasedDate`."""
    second_page = list(smartrecruiters.parse_page(_page2(), _target()))
    support_specialist = [r for r in second_page if r.req_id == "743999881234571"][0]
    assert support_specialist.posted_date is None
    assert support_specialist.posted_raw == "Not a date"
    assert support_specialist.canonical_fields()["posted_date"] == ""


def test_parse_page_skips_row_with_blank_title():
    """Third item on page 1 has `"name": ""` and must not surface."""
    records = list(smartrecruiters.parse_page(_page1(), _target()))
    assert "743999881234569" not in [r.req_id for r in records]
    assert len(records) == 2


def test_parse_page_skips_row_missing_id():
    """First item on page 2 has no `id` at all and cannot be identified."""
    records = list(smartrecruiters.parse_page(_page2(), _target()))
    assert [r.req_id for r in records] == ["743999881234571"]


def test_parse_page_records_ref_number_as_provenance_not_identity():
    first = list(smartrecruiters.parse_page(_page1(), _target()))[0]
    assert first.extra["ref_number"] == "REQ-9001"
    assert "ref_number" not in first.canonical_fields()
    second = list(smartrecruiters.parse_page(_page1(), _target()))[1]
    assert "ref_number" not in second.extra


def test_parse_page_identity_prefers_the_smartrecruiters_posting_id():
    first = list(smartrecruiters.parse_page(_page1(), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "smartrecruiters:examplecorp",
        "743999881234567",
    )
    assert claims[1].kind == "url"


def test_parse_page_content_hash_is_stable_across_runs():
    once = list(smartrecruiters.parse_page(_page1(), _target()))
    twice = list(smartrecruiters.parse_page(_page1(), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    other = list(smartrecruiters.parse_page(_page1(), _target(slug="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_page_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"content": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(smartrecruiters.parse_page(payload, _target()))


def test_parse_page_accepts_bytes_str_and_mapping():
    raw = _page1()
    assert (
        len(list(smartrecruiters.parse_page(raw, _target())))
        == len(list(smartrecruiters.parse_page(raw.decode(), _target())))
        == len(list(smartrecruiters.parse_page(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_company_map():
    config = SourceConfig.from_mapping(
        {"companies": {"smartrecruiters": {"docusign": "DocuSign", "servicenow": "ServiceNow", "": "junk"}}}
    )
    targets = smartrecruiters.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["docusign", "servicenow"]
    assert [t.label for t in targets] == ["DocuSign", "ServiceNow"]
    assert all(t.host == smartrecruiters.API_HOST for t in targets)
    assert all(t.inventory_scope is InventoryScope.COMPLETE for t in targets)
    assert targets[0].param("slug") == "docusign"


def test_plan_without_smartrecruiters_config_is_empty_not_an_error():
    assert list(smartrecruiters.ADAPTER.plan(SourceConfig())) == []


def test_descriptor_declares_daily_and_full_direct_and_checkpoint_support():
    descriptor = smartrecruiters.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.COMPLETE


# --------------------------------------------------------------------------- #
# fetch(): pagination, checkpoints, resume, replay-safety
# --------------------------------------------------------------------------- #
def test_fetch_paginates_across_offsets_until_totalfound_is_reached():
    transport = FakeTransport().add(
        POSTINGS,
        json_response(json.loads(_page1())),
        json_response(json.loads(_page2())),
    )
    ctx = FetchContext(transport=transport)
    records = asyncio.run(collect(smartrecruiters.ADAPTER, _target(), ctx))
    assert [r.req_id for r in records] == ["743999881234567", "743999881234568", "743999881234571"]
    assert transport.call_count == 2
    assert transport.requests[0].params == {"limit": 100, "offset": 0}
    assert transport.requests[1].params == {"limit": 100, "offset": 3}
    # offset advanced by page-1's raw content length (3), not its yielded
    # record count (2): the skipped blank-title row still occupied a slot.
    assert ctx.checkpoint.cursor["offset"] == 5
    assert ctx.checkpoint.emitted == 3


def test_fetch_stops_without_a_trailing_request_once_totalfound_is_met():
    transport = FakeTransport().add(POSTINGS, json_response(json.loads(_single_page())))
    records = asyncio.run(collect(smartrecruiters.ADAPTER, _target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["900000000000001"]
    assert transport.call_count == 1


def test_fetch_on_a_missing_board_is_permanent_not_empty():
    transport = FakeTransport().add(POSTINGS, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(smartrecruiters.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(POSTINGS, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(smartrecruiters.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    transport = FakeTransport().add(POSTINGS, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(smartrecruiters.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_requires_the_slug_param():
    transport = FakeTransport().add(POSTINGS, json_response({"content": [], "totalFound": 0}))
    bare = SourceTarget(source_key="smartrecruiters", instance_key="examplecorp")
    with pytest.raises((PermanentSourceError, ConfigError)):
        asyncio.run(collect(smartrecruiters.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_on_an_empty_board_yields_nothing_and_does_not_raise():
    transport = FakeTransport().add(POSTINGS, json_response({"content": [], "totalFound": 0}))
    records = asyncio.run(collect(smartrecruiters.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 1


def test_fetch_yields_the_same_records_the_pure_parser_does():
    transport = FakeTransport().add(
        POSTINGS,
        json_response(json.loads(_page1())),
        json_response(json.loads(_page2())),
    )
    fetched = asyncio.run(collect(smartrecruiters.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(smartrecruiters.parse_page(_page1(), _target())) + list(
        smartrecruiters.parse_page(_page2(), _target())
    )
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


# -- checkpoints -------------------------------------------------------- #
def test_mid_stream_failure_keeps_earlier_records_and_last_checkpoint():
    transport = FakeTransport().add(
        POSTINGS,
        json_response(json.loads(_page1())),
        json_response({}, status=503),
    )
    target = _target()
    ctx = FetchContext(transport=transport)

    async def scenario():
        seen = []
        with pytest.raises(TransientSourceError):
            async for record in smartrecruiters.ADAPTER.fetch(target, ctx):
                seen.append(record)
        return seen

    seen = asyncio.run(scenario())
    assert [r.req_id for r in seen] == ["743999881234567", "743999881234568"]
    assert ctx.checkpoint is not None
    assert ctx.checkpoint.cursor["offset"] == 3
    assert ctx.checkpoint.emitted == 2
    assert ctx.checkpoint.is_valid_for(target)


def test_resume_from_checkpoint_continues_at_the_saved_offset():
    target = _target()
    resume = Checkpoint(
        source_key="smartrecruiters",
        instance_key="examplecorp",
        cursor={"offset": 3},
        config_fingerprint=target.config_fingerprint(),
        emitted=2,
    )
    transport = FakeTransport().add(POSTINGS, json_response(json.loads(_page2())))
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(smartrecruiters.ADAPTER, target, ctx))
    assert [r.req_id for r in records] == ["743999881234571"]
    assert transport.requests[0].params == {"limit": 100, "offset": 3}
    assert transport.call_count == 1
    assert ctx.checkpoint.emitted == 3
    assert ctx.checkpoint.cursor["offset"] == 5


def test_replayed_checkpoint_reemits_identical_records():
    """Invariant 5: replay is expected, and the writer dedupes on identity."""
    first = asyncio.run(
        collect(
            smartrecruiters.ADAPTER,
            _target(),
            FetchContext(
                transport=FakeTransport().add(
                    POSTINGS, json_response(json.loads(_page1())), json_response(json.loads(_page2()))
                )
            ),
        )
    )
    target = _target()
    stale = Checkpoint(
        source_key="smartrecruiters",
        instance_key="examplecorp",
        cursor={"offset": 0},
        config_fingerprint=target.config_fingerprint(),
        emitted=0,
    )
    second = asyncio.run(
        collect(
            smartrecruiters.ADAPTER,
            target,
            FetchContext(
                transport=FakeTransport().add(
                    POSTINGS, json_response(json.loads(_page1())), json_response(json.loads(_page2()))
                ),
                resume_from=stale,
            ),
        )
    )
    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]
    assert [r.identity_claims() for r in second] == [r.identity_claims() for r in first]


def test_stale_checkpoint_for_a_changed_target_is_ignored():
    target = _target()
    stale = Checkpoint(
        source_key="smartrecruiters",
        instance_key="examplecorp",
        cursor={"offset": 3},
        config_fingerprint="stale-fingerprint",
        emitted=99,
    )
    transport = FakeTransport().add(
        POSTINGS, json_response(json.loads(_page1())), json_response(json.loads(_page2()))
    )
    asyncio.run(collect(smartrecruiters.ADAPTER, target, FetchContext(transport=transport, resume_from=stale)))
    assert transport.requests[0].params == {"limit": 100, "offset": 0}
