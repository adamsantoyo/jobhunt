"""Recruitee adapter: parsing/transport split, driven by a frozen fixture.

Every parser assertion here runs with no transport at all, which is the point
of the split: CI exercises payload handling without a network, and the same
fixture bytes replay identically forever.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import recruitee
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

BOARD = recruitee.board_url("edifecs")


def _target(slug="edifecs", name="Edifecs"):
    return SourceTarget(
        source_key="recruitee",
        instance_key=slug,
        label=name,
        params={"slug": slug, "company": name},
        inventory_scope=InventoryScope.COMPLETE,
        host=recruitee.board_host(slug),
    )


def _fixture():
    return fixture_bytes("recruitee", "offers.json")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_offers_from_frozen_fixture():
    records = list(recruitee.parse_offers(_fixture(), _target()))
    assert [r.req_id for r in records] == ["812345", "812346", "812349"]
    assert all(r.source_key == "recruitee" and r.instance_key == "edifecs" for r in records)
    assert all(r.company == "Edifecs" for r in records)


def test_parse_offers_normalizes_title_and_location():
    first = list(recruitee.parse_offers(_fixture(), _target()))[0]
    assert first.title == "Senior Support Engineer"
    assert first.location == "Amsterdam, Netherlands"
    assert first.url == "https://edifecs.recruitee.com/o/senior-support-engineer"
    assert first.namespace == "recruitee:edifecs"


def test_parse_offers_remote_true_is_boolean_true():
    first = list(recruitee.parse_offers(_fixture(), _target()))[0]
    assert first.remote is True


def test_parse_offers_only_a_literal_true_counts_as_remote():
    """Offer 812349 sends `"remote": "yes"` -- a truthy non-boolean the legacy
    scraper's `is True` check deliberately does not treat as remote."""
    third = list(recruitee.parse_offers(_fixture(), _target()))[2]
    assert third.req_id == "812349"
    assert third.remote is False


def test_parse_offers_posted_date_from_published_at():
    first = list(recruitee.parse_offers(_fixture(), _target()))[0]
    assert first.posted_date == "2026-07-10"
    assert first.posted_raw == "2026-07-10T08:00:00.000Z"


def test_parse_offers_missing_published_at_yields_no_date():
    second = list(recruitee.parse_offers(_fixture(), _target()))[1]
    assert second.posted_date is None
    assert second.posted_raw == ""
    assert second.canonical_fields()["posted_date"] == ""


def test_parse_offers_skips_unusable_rows_without_failing_the_board():
    """A row with no title or no careers_url cannot be identified; the rest
    must survive."""
    records = list(recruitee.parse_offers(_fixture(), _target()))
    assert len(records) == 3
    ids = [r.req_id for r in records]
    assert "812347" not in ids  # empty title
    assert "812348" not in ids  # empty careers_url


def test_parse_offers_missing_location_is_empty_not_an_error():
    second = list(recruitee.parse_offers(_fixture(), _target()))[1]
    assert second.location == ""


def test_parse_offers_records_apply_url_as_alt_when_it_differs():
    first, second, _third = list(recruitee.parse_offers(_fixture(), _target()))
    assert first.alt_urls == ("https://edifecs.recruitee.com/o/senior-support-engineer/c/new",)
    assert second.alt_urls == ()  # no careers_apply_url on this offer


def test_parse_offers_records_provenance_without_hashing_it():
    first = list(recruitee.parse_offers(_fixture(), _target()))[0]
    assert first.extra["department"] == "Customer Support"
    assert first.extra["employment_type_code"] == "full_time"
    assert first.extra["updated_at"] == "2026-07-18T09:30:00.000Z"
    assert "department" not in first.canonical_fields()


def test_parse_offers_content_hash_is_stable_across_runs():
    once = list(recruitee.parse_offers(_fixture(), _target()))
    twice = list(recruitee.parse_offers(_fixture(), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    # Same board, same bytes, different instance -> different identity namespace.
    other = list(recruitee.parse_offers(_fixture(), _target(slug="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_offers_identity_prefers_the_offer_id():
    first = list(recruitee.parse_offers(_fixture(), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "recruitee:edifecs",
        "812345",
    )
    assert claims[1].kind == "url"


def test_parse_offers_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"offers": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(recruitee.parse_offers(payload, _target()))


def test_parse_offers_accepts_bytes_str_and_mapping():
    raw = _fixture()
    assert (
        len(list(recruitee.parse_offers(raw, _target())))
        == len(list(recruitee.parse_offers(raw.decode(), _target())))
        == len(list(recruitee.parse_offers(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_company_map():
    config = SourceConfig.from_mapping(
        {"companies": {"recruitee": {"edifecs": "Edifecs", "ampere": "Ampere Computing", "": "junk"}}}
    )
    targets = recruitee.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["edifecs", "ampere"]
    assert [t.label for t in targets] == ["Edifecs", "Ampere Computing"]
    assert all(t.inventory_scope is InventoryScope.COMPLETE for t in targets)
    assert targets[0].param("slug") == "edifecs"


def test_plan_sets_a_distinct_per_instance_host():
    """Recruitee has no shared API host like Greenhouse's `boards-api...`:
    each company is its own subdomain, and the scheduler's per-host limiter
    depends on that being reflected per target."""
    config = SourceConfig.from_mapping({"companies": {"recruitee": {"edifecs": "Edifecs", "ampere": "Ampere"}}})
    targets = recruitee.ADAPTER.plan(config)
    hosts = {t.instance_key: t.host for t in targets}
    assert hosts == {"edifecs": "edifecs.recruitee.com", "ampere": "ampere.recruitee.com"}


def test_plan_without_recruitee_config_is_empty_not_an_error():
    assert list(recruitee.ADAPTER.plan(SourceConfig())) == []


def test_descriptor_declares_daily_and_full_direct():
    descriptor = recruitee.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    # A single-response board has nothing to resume.
    assert descriptor.supports_checkpoint is False
    assert descriptor.default_inventory_scope is InventoryScope.COMPLETE


# --------------------------------------------------------------------------- #
# fetch(): the thin transport shell
# --------------------------------------------------------------------------- #
def test_fetch_makes_exactly_one_request_and_streams_the_parsed_offers():
    transport = FakeTransport()
    transport.add(BOARD, json_response(json.loads(_fixture())))
    records = asyncio.run(collect(recruitee.ADAPTER, _target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["812345", "812346", "812349"]
    assert transport.call_count == 1
    assert transport.urls() == ["https://edifecs.recruitee.com/api/offers"]


def test_fetch_on_a_missing_board_is_permanent_not_empty():
    transport = FakeTransport().add(BOARD, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(recruitee.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(BOARD, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(recruitee.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    from backend.sources.testing import text_response

    transport = FakeTransport().add(BOARD, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(recruitee.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce."""
    transport = FakeTransport().add(BOARD, json_response(json.loads(_fixture())))
    fetched = asyncio.run(collect(recruitee.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(recruitee.parse_offers(_fixture(), _target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


def test_fetch_requires_the_slug_param():
    transport = FakeTransport().add(BOARD, json_response({"offers": []}))
    bare = SourceTarget(source_key="recruitee", instance_key="edifecs")
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(recruitee.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_on_an_empty_board_yields_nothing_and_does_not_raise():
    """An empty board is a positive assertion, and only reachable via a 200."""
    transport = FakeTransport().add(BOARD, json_response({"offers": []}))
    records = asyncio.run(collect(recruitee.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
