"""Lever adapter: parsing/transport split, driven by a frozen fixture.

Mirrors `test_source_greenhouse.py`'s structure. The one Lever-specific block
worth calling out is the epoch-millisecond `createdAt` conversion tests: this
adapter deliberately deviates from `scraper.py`'s `str(createdAt)[:10]`, which
sliced the first ten *digits* of the millisecond epoch rather than producing a
date. See `lever._posted_date` for the corrected conversion.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import lever
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
from backend.sources.testing import FakeTransport, collect, fixture_bytes, json_response, text_response

ACCOUNT = lever.postings_url("examplecorp")


def _target(slug="examplecorp", name="Example Corp"):
    return SourceTarget(
        source_key="lever",
        instance_key=slug,
        label=name,
        params={"slug": slug, "company": name},
        inventory_scope=InventoryScope.COMPLETE,
        host=lever.API_HOST,
    )


def _fixture():
    return fixture_bytes("lever", "postings.json")


KEPT_IDS = [
    "6f3a1b2c-8d4e-4f2a-9c1d-0a1b2c3d4e5f",  # full posting
    "7a4b2c3d-9e5f-4a3b-8d2e-1b2c3d4e5f6a",  # no categories, no workplaceType
    "8b5c3d4e-0f6a-4b4c-9e3f-2c3d4e5f6a7b",  # onsite
    "be8f6071-3c9d-4e7f-8b6c-5f6a7b8c9d0e",  # createdAt is null
]


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_postings_from_frozen_fixture():
    records = list(lever.parse_postings(_fixture(), _target()))
    assert [r.req_id for r in records] == KEPT_IDS
    assert all(r.source_key == "lever" and r.instance_key == "examplecorp" for r in records)
    assert all(r.company == "Example Corp" for r in records)


def test_parse_postings_normalizes_title_location_and_url_alias():
    first = list(lever.parse_postings(_fixture(), _target()))[0]
    assert first.title == "Support Engineer, Platform"
    assert first.location == "San Francisco, CA"
    # the display url keeps its tracking param; the alias value does not
    assert first.url.endswith("?gh_src=abc123")
    assert first.url_key == (
        "https://jobs.lever.co/examplecorp/6f3a1b2c-8d4e-4f2a-9c1d-0a1b2c3d4e5f"
    )
    assert first.namespace == "lever:examplecorp"


def test_parse_postings_converts_epoch_milliseconds_correctly():
    """The deliberate deviation from legacy `str(createdAt)[:10]`.

    1784019600000 is 2026-07-14T09:00:00Z. Legacy would have produced the
    nonsense string "1784019600" (the first ten digits of the epoch, not a
    date); this adapter must produce the real calendar date.
    """
    records = list(lever.parse_postings(_fixture(), _target()))
    assert records[0].posted_date == "2026-07-14"
    assert records[0].posted_raw == "1784019600000"
    assert records[1].posted_date == "2026-07-30"


def test_parse_postings_null_created_at_keeps_the_hash_clean():
    fourth = list(lever.parse_postings(_fixture(), _target()))[3]
    assert fourth.req_id == "be8f6071-3c9d-4e7f-8b6c-5f6a7b8c9d0e"
    assert fourth.posted_date is None
    assert fourth.posted_raw == ""
    assert fourth.canonical_fields()["posted_date"] == ""


def test_parse_postings_skips_unusable_rows_without_failing_the_account():
    """A row with no title or no hostedUrl cannot be identified; the rest survive."""
    records = list(lever.parse_postings(_fixture(), _target()))
    assert len(records) == 4
    got_ids = [r.req_id for r in records]
    assert "9c6d4e5f-1a7b-4c5d-8f4a-3d4e5f6a7b8c" not in got_ids  # empty text
    assert "ad7e5f60-2b8c-4d6e-9a5b-4e5f6a7b8c9d" not in got_ids  # no hostedUrl


def test_parse_postings_missing_categories_is_empty_location_not_an_error():
    second = list(lever.parse_postings(_fixture(), _target()))[1]
    assert second.location == ""


def test_parse_postings_remote_flag_only_true_for_workplace_type_remote():
    records = list(lever.parse_postings(_fixture(), _target()))
    assert records[0].remote is True  # workplaceType == "remote"
    assert records[1].remote is False  # no workplaceType at all
    assert records[2].remote is False  # workplaceType == "onsite"


def test_parse_postings_records_provenance_without_hashing_it():
    first = list(lever.parse_postings(_fixture(), _target()))[0]
    assert first.extra["team"] == "Platform"
    assert first.extra["commitment"] == "Full-time"
    assert first.extra["department"] == "Engineering"
    assert first.extra["workplace_type"] == "remote"
    assert "team" not in first.canonical_fields()


def test_parse_postings_content_hash_is_stable_across_runs():
    once = list(lever.parse_postings(_fixture(), _target()))
    twice = list(lever.parse_postings(_fixture(), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    # Same account, same bytes, different instance -> different identity namespace.
    other = list(lever.parse_postings(_fixture(), _target(slug="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_postings_identity_prefers_the_lever_uuid():
    """Legacy `scraper.py` never captured `id`; the contract wants it as req_id."""
    first = list(lever.parse_postings(_fixture(), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "lever:examplecorp",
        "6f3a1b2c-8d4e-4f2a-9c1d-0a1b2c3d4e5f",
    )
    assert claims[1].kind == "url"


def test_parse_postings_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", json.dumps({"postings": []}).encode()):
        with pytest.raises(PayloadError):
            list(lever.parse_postings(payload, _target()))


def test_parse_postings_accepts_an_empty_array_without_raising():
    """An empty account is a positive assertion, and only reachable via valid JSON."""
    assert list(lever.parse_postings(b"[]", _target())) == []


def test_parse_postings_accepts_bytes_str_and_list():
    raw = _fixture()
    assert (
        len(list(lever.parse_postings(raw, _target())))
        == len(list(lever.parse_postings(raw.decode(), _target())))
        == len(list(lever.parse_postings(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_company_map():
    config = SourceConfig.from_mapping(
        {"companies": {"lever": {"outreach": "Outreach", "highspot": "Highspot", "": "junk"}}}
    )
    targets = lever.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["outreach", "highspot"]
    assert [t.label for t in targets] == ["Outreach", "Highspot"]
    assert all(t.host == lever.API_HOST for t in targets)
    assert all(t.inventory_scope is InventoryScope.COMPLETE for t in targets)
    assert targets[0].param("slug") == "outreach"


def test_plan_without_lever_config_is_empty_not_an_error():
    assert list(lever.ADAPTER.plan(SourceConfig())) == []


def test_descriptor_declares_daily_and_full_direct():
    descriptor = lever.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    # A single-response account has nothing to resume.
    assert descriptor.supports_checkpoint is False
    assert descriptor.default_inventory_scope is InventoryScope.COMPLETE


# --------------------------------------------------------------------------- #
# fetch(): the thin transport shell
# --------------------------------------------------------------------------- #
def test_fetch_makes_exactly_one_request_and_streams_the_parsed_postings():
    transport = FakeTransport()
    transport.add(ACCOUNT, json_response(json.loads(_fixture())))
    records = asyncio.run(collect(lever.ADAPTER, _target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == KEPT_IDS
    assert transport.call_count == 1
    assert transport.urls() == ["https://api.lever.co/v0/postings/examplecorp?mode=json"]


def test_fetch_on_a_missing_account_is_permanent_not_empty():
    transport = FakeTransport().add(ACCOUNT, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(lever.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(ACCOUNT, json_response([], status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(lever.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    transport = FakeTransport().add(ACCOUNT, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(lever.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce."""
    transport = FakeTransport().add(ACCOUNT, json_response(json.loads(_fixture())))
    fetched = asyncio.run(collect(lever.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(lever.parse_postings(_fixture(), _target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


def test_fetch_requires_the_slug_param():
    transport = FakeTransport().add(ACCOUNT, json_response([]))
    bare = SourceTarget(source_key="lever", instance_key="examplecorp")
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(lever.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_on_an_empty_account_yields_nothing_and_does_not_raise():
    """An empty account is a positive assertion, and only reachable via a 200."""
    transport = FakeTransport().add(ACCOUNT, json_response([]))
    records = asyncio.run(collect(lever.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
