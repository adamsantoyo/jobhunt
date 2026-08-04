"""Workable adapter: parsing/transport split, driven by a frozen fixture.

Every parser assertion here runs with no transport at all, mirroring
`test_source_greenhouse.py`: CI exercises payload handling without a network,
and the same fixture bytes replay identically forever.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import workable
from backend.sources.contract import (
    FetchContext,
    InventoryScope,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceConfig,
    SourceTarget,
    TransientSourceError,
)
from backend.sources.testing import FakeTransport, collect, fixture_bytes, json_response

ACCOUNT = workable.widget_url("seeq")


def _target(slug="seeq", name="Seeq"):
    return SourceTarget(
        source_key="workable",
        instance_key=slug,
        label=name,
        params={"slug": slug, "company": name},
        inventory_scope=InventoryScope.COMPLETE,
        host=workable.API_HOST,
    )


def _fixture():
    return fixture_bytes("workable", "account.json")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_widget_from_frozen_fixture():
    records = list(workable.parse_widget(_fixture(), _target()))
    assert [r.req_id for r in records] == ["AB1234CD", "EF5678GH", "IJ9012KL"]
    assert all(r.source_key == "workable" and r.instance_key == "seeq" for r in records)
    assert all(r.company == "Seeq" for r in records)


def test_parse_widget_normalizes_title_and_prefers_shortlink_over_url():
    first = list(workable.parse_widget(_fixture(), _target()))[0]
    assert first.title == "Senior Software Engineer, Platform"
    assert first.url == "https://apply.workable.com/j/AB1234CD"
    assert first.namespace == "workable:seeq"


def test_parse_widget_falls_back_to_url_when_shortlink_is_absent():
    second = list(workable.parse_widget(_fixture(), _target()))[1]
    assert second.url == "https://apply.workable.com/seeq/j/EF5678GH/"


def test_parse_widget_location_joins_city_and_state():
    first = list(workable.parse_widget(_fixture(), _target()))[0]
    assert first.location == "Seattle, WA"


def test_parse_widget_location_falls_back_to_country_without_city_or_state():
    """A remote-only posting with no city/state must not become `", "`."""
    second = list(workable.parse_widget(_fixture(), _target()))[1]
    assert second.location == "United States"


def test_parse_widget_location_uses_city_alone_when_state_is_absent():
    third = list(workable.parse_widget(_fixture(), _target()))[2]
    assert third.location == "Remote"


def test_parse_widget_remote_reads_telecommuting_strictly():
    records = list(workable.parse_widget(_fixture(), _target()))
    assert records[0].remote is False
    assert records[1].remote is True
    assert records[2].remote is False


def test_parse_widget_posted_date_is_absolute_and_raw_is_preserved():
    records = list(workable.parse_widget(_fixture(), _target()))
    assert records[0].posted_date == "2026-07-10"
    assert records[0].posted_raw == "2026-07-10"
    # A full timestamp is truncated to a date for the hash, but kept whole raw.
    assert records[1].posted_date == "2026-07-22"
    assert records[1].posted_raw == "2026-07-22T00:00:00.000Z"


def test_parse_widget_keeps_unparseable_dates_out_of_the_hash():
    third = list(workable.parse_widget(_fixture(), _target()))[2]
    assert third.posted_date is None
    assert third.posted_raw == "Recently"
    assert third.canonical_fields()["posted_date"] == ""


def test_parse_widget_skips_unusable_rows_without_failing_the_board():
    """A row with no title or no usable apply link cannot be identified."""
    records = list(workable.parse_widget(_fixture(), _target()))
    assert len(records) == 3
    assert "MN3456OP" not in [r.req_id for r in records]  # empty title
    assert "QR7890ST" not in [r.req_id for r in records]  # no shortlink/url


def test_parse_widget_records_provenance_without_hashing_it():
    first = list(workable.parse_widget(_fixture(), _target()))[0]
    assert first.extra["customer_requisition_id"] == "REQ-5001"
    assert first.extra["department"] == "Engineering"
    assert first.extra["employment_type"] == "Full-time"
    assert "customer_requisition_id" not in first.canonical_fields()


def test_parse_widget_content_hash_is_stable_across_runs():
    once = list(workable.parse_widget(_fixture(), _target()))
    twice = list(workable.parse_widget(_fixture(), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    # Same account, same bytes, different instance -> different identity namespace.
    other = list(workable.parse_widget(_fixture(), _target(slug="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_widget_identity_prefers_the_shortcode():
    first = list(workable.parse_widget(_fixture(), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "workable:seeq",
        "AB1234CD",
    )
    assert claims[1].kind == "url"


def test_parse_widget_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"jobs": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(workable.parse_widget(payload, _target()))


def test_parse_widget_accepts_bytes_str_and_mapping():
    raw = _fixture()
    assert (
        len(list(workable.parse_widget(raw, _target())))
        == len(list(workable.parse_widget(raw.decode(), _target())))
        == len(list(workable.parse_widget(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_company_map():
    config = SourceConfig.from_mapping(
        {"companies": {"workable": {"seeq": "Seeq", "acme": "Acme Corp", "": "junk"}}}
    )
    targets = workable.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["seeq", "acme"]
    assert [t.label for t in targets] == ["Seeq", "Acme Corp"]
    assert all(t.host == workable.API_HOST for t in targets)
    assert all(t.inventory_scope is InventoryScope.COMPLETE for t in targets)
    assert targets[0].param("slug") == "seeq"


def test_plan_without_workable_config_is_empty_not_an_error():
    assert list(workable.ADAPTER.plan(SourceConfig())) == []


def test_descriptor_declares_daily_and_full_direct():
    descriptor = workable.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    # A single-response board has nothing to resume.
    assert descriptor.supports_checkpoint is False
    assert descriptor.default_inventory_scope is InventoryScope.COMPLETE


# --------------------------------------------------------------------------- #
# fetch(): the thin transport shell
# --------------------------------------------------------------------------- #
def test_fetch_makes_exactly_one_request_and_streams_the_parsed_board():
    transport = FakeTransport()
    transport.add(ACCOUNT, json_response(json.loads(_fixture())))
    records = asyncio.run(collect(workable.ADAPTER, _target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["AB1234CD", "EF5678GH", "IJ9012KL"]
    assert transport.call_count == 1
    assert transport.urls() == ["https://apply.workable.com/api/v1/widget/accounts/seeq"]


def test_fetch_on_a_missing_account_is_permanent_not_empty():
    transport = FakeTransport().add(ACCOUNT, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(workable.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(ACCOUNT, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(workable.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    from backend.sources.testing import text_response

    transport = FakeTransport().add(ACCOUNT, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(workable.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce."""
    transport = FakeTransport().add(ACCOUNT, json_response(json.loads(_fixture())))
    fetched = asyncio.run(collect(workable.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(workable.parse_widget(_fixture(), _target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


def test_fetch_requires_the_slug_param():
    transport = FakeTransport().add(ACCOUNT, json_response({"jobs": []}))
    bare = SourceTarget(source_key="workable", instance_key="seeq")
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(workable.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_on_an_empty_account_yields_nothing_and_does_not_raise():
    """An empty account is a positive assertion, and only reachable via a 200."""
    transport = FakeTransport().add(ACCOUNT, json_response({"jobs": []}))
    records = asyncio.run(collect(workable.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []


def test_fetch_sends_details_false_as_a_query_param():
    transport = FakeTransport().add(ACCOUNT, json_response({"jobs": []}))
    asyncio.run(collect(workable.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.requests[0].params == {"details": "false"}
