"""Greenhouse reference adapter: parsing/transport split, driven by a frozen fixture.

Every parser assertion here runs with no transport at all, which is the point
of the split: CI exercises payload handling without a network, and the same
fixture bytes replay identically forever.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import greenhouse
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

BOARD = greenhouse.board_url("examplecorp")


def _target(slug="examplecorp", name="Example Corp"):
    return SourceTarget(
        source_key="greenhouse",
        instance_key=slug,
        label=name,
        params={"slug": slug, "company": name},
        inventory_scope=InventoryScope.COMPLETE,
        host=greenhouse.API_HOST,
    )


def _fixture():
    return fixture_bytes("greenhouse", "board.json")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_board_from_frozen_fixture():
    records = list(greenhouse.parse_board(_fixture(), _target()))
    assert [r.req_id for r in records] == ["4020123", "4020124", "4020125"]
    assert all(r.source_key == "greenhouse" and r.instance_key == "examplecorp" for r in records)
    assert all(r.company == "Example Corp" for r in records)


def test_parse_board_normalizes_title_location_and_url_alias():
    first = list(greenhouse.parse_board(_fixture(), _target()))[0]
    assert first.title == "Support Engineer, Platform"
    assert first.location == "San Francisco, CA"
    # the display url keeps its tracking param; the alias value does not
    assert first.url.endswith("?gh_src=abc123")
    assert first.url_key == "https://job-boards.greenhouse.io/examplecorp/jobs/4020123"
    assert first.namespace == "greenhouse:examplecorp"


def test_parse_board_prefers_first_published_over_updated_at():
    records = list(greenhouse.parse_board(_fixture(), _target()))
    assert records[0].posted_date == "2026-07-14"  # first_published, not updated_at
    assert records[0].extra["updated_at"] == "2026-07-21T18:04:33-04:00"
    assert records[1].posted_date == "2026-07-30"  # falls back to updated_at


def test_parse_board_keeps_unparseable_dates_out_of_the_hash():
    third = list(greenhouse.parse_board(_fixture(), _target()))[2]
    assert third.posted_date is None
    assert third.posted_raw == "Posted 30+ Days Ago"
    assert third.canonical_fields()["posted_date"] == ""


def test_parse_board_skips_unusable_rows_without_failing_the_board():
    """A row with no title or no url cannot be identified; the rest must survive."""
    records = list(greenhouse.parse_board(_fixture(), _target()))
    assert len(records) == 3
    assert "4020126" not in [r.req_id for r in records]  # empty title
    assert "4020127" not in [r.req_id for r in records]  # no absolute_url


def test_parse_board_missing_location_is_empty_not_an_error():
    second = list(greenhouse.parse_board(_fixture(), _target()))[1]
    assert second.location == ""


def test_parse_board_records_provenance_without_hashing_it():
    first = list(greenhouse.parse_board(_fixture(), _target()))[0]
    assert first.extra["internal_job_id"] == "4012345"
    assert first.extra["customer_requisition_id"] == "REQ-1234"
    assert "internal_job_id" not in first.canonical_fields()


def test_parse_board_content_hash_is_stable_across_runs():
    once = list(greenhouse.parse_board(_fixture(), _target()))
    twice = list(greenhouse.parse_board(_fixture(), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    # Same board, same bytes, different instance -> different identity namespace.
    other = list(greenhouse.parse_board(_fixture(), _target(slug="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_board_identity_prefers_the_board_job_id():
    first = list(greenhouse.parse_board(_fixture(), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "greenhouse:examplecorp",
        "4020123",
    )
    assert claims[1].kind == "url"


def test_parse_board_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"jobs": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(greenhouse.parse_board(payload, _target()))


def test_parse_board_accepts_bytes_str_and_mapping():
    raw = _fixture()
    assert (
        len(list(greenhouse.parse_board(raw, _target())))
        == len(list(greenhouse.parse_board(raw.decode(), _target())))
        == len(list(greenhouse.parse_board(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_company_map():
    config = SourceConfig.from_mapping(
        {"companies": {"greenhouse": {"anthropic": "Anthropic", "stripe": "Stripe", "": "junk"}}}
    )
    targets = greenhouse.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["anthropic", "stripe"]
    assert [t.label for t in targets] == ["Anthropic", "Stripe"]
    assert all(t.host == greenhouse.API_HOST for t in targets)
    assert all(t.inventory_scope is InventoryScope.COMPLETE for t in targets)
    assert targets[0].param("slug") == "anthropic"


def test_plan_without_greenhouse_config_is_empty_not_an_error():
    assert list(greenhouse.ADAPTER.plan(SourceConfig())) == []


def test_descriptor_declares_daily_and_full_direct():
    descriptor = greenhouse.DESCRIPTOR
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
    transport.add(BOARD, json_response(json.loads(_fixture())))
    records = asyncio.run(collect(greenhouse.ADAPTER, _target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["4020123", "4020124", "4020125"]
    assert transport.call_count == 1
    assert transport.urls() == ["https://boards-api.greenhouse.io/v1/boards/examplecorp/jobs"]


def test_fetch_on_a_missing_board_is_permanent_not_empty():
    transport = FakeTransport().add(BOARD, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(greenhouse.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(BOARD, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(greenhouse.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    from backend.sources.testing import text_response

    transport = FakeTransport().add(BOARD, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(greenhouse.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce."""
    transport = FakeTransport().add(BOARD, json_response(json.loads(_fixture())))
    fetched = asyncio.run(collect(greenhouse.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(greenhouse.parse_board(_fixture(), _target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


def test_fetch_requires_the_slug_param():
    transport = FakeTransport().add(BOARD, json_response({"jobs": []}))
    bare = SourceTarget(source_key="greenhouse", instance_key="examplecorp")
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(greenhouse.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_on_an_empty_board_yields_nothing_and_does_not_raise():
    """An empty board is a positive assertion, and only reachable via a 200."""
    transport = FakeTransport().add(BOARD, json_response({"jobs": [], "meta": {"total": 0}}))
    records = asyncio.run(collect(greenhouse.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
