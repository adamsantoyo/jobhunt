"""Ashby adapter: parsing/transport split, driven by a frozen fixture.

Mirrors `test_source_greenhouse.py`'s structure and coverage: pure parser
tests with no transport, `plan()` tests from a config mapping, and `fetch()`
tests via `FakeTransport` for the success/non-200/malformed-payload paths.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import ashby
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

BOARD = ashby.board_url("examplecorp")


def _target(slug="examplecorp", name="Example Corp"):
    return SourceTarget(
        source_key="ashby",
        instance_key=slug,
        label=name,
        params={"slug": slug, "company": name},
        inventory_scope=InventoryScope.COMPLETE,
        host=ashby.API_HOST,
    )


def _fixture():
    return fixture_bytes("ashby", "board.json")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_board_from_frozen_fixture():
    records = list(ashby.parse_board(_fixture(), _target()))
    assert [r.req_id for r in records] == [
        "5f2c9a10-8b3e-4c1a-9d4f-1a2b3c4d5e6f",
        "a1b2c3d4-e5f6-4789-9abc-def012345678",
        "11112222-3333-4444-5555-666677778888",
    ]
    assert all(r.source_key == "ashby" and r.instance_key == "examplecorp" for r in records)
    assert all(r.company == "Example Corp" for r in records)


def test_parse_board_normalizes_title_location_and_url():
    first = list(ashby.parse_board(_fixture(), _target()))[0]
    assert first.title == "Platform Support Engineer"
    assert first.location == "San Francisco, CA"
    assert first.url == "https://jobs.ashbyhq.com/examplecorp/5f2c9a10-8b3e-4c1a-9d4f-1a2b3c4d5e6f"
    assert first.namespace == "ashby:examplecorp"


def test_parse_board_reads_publishedat_prefix_as_the_absolute_date():
    first = list(ashby.parse_board(_fixture(), _target()))[0]
    assert first.posted_date == "2026-07-18"
    assert first.posted_raw == "2026-07-18T00:00:00.000Z"
    second = list(ashby.parse_board(_fixture(), _target()))[1]
    assert second.posted_date == "2026-07-22"
    assert second.posted_raw == "2026-07-22T00:00:00.000Z"


def test_parse_board_missing_publishedat_is_no_date_not_an_error():
    third = list(ashby.parse_board(_fixture(), _target()))[2]
    assert third.posted_date is None
    assert third.posted_raw == ""
    assert third.canonical_fields()["posted_date"] == ""


def test_parse_board_missing_compensation_is_empty_salary():
    second = list(ashby.parse_board(_fixture(), _target()))[1]
    assert second.salary_text == ""


def test_parse_board_reads_the_first_compensation_tier_summary():
    first = list(ashby.parse_board(_fixture(), _target()))[0]
    assert first.salary_text == "$150K – $190K"


def test_parse_board_reads_is_remote():
    records = list(ashby.parse_board(_fixture(), _target()))
    assert records[0].remote is False
    assert records[1].remote is True
    assert records[2].remote is False


def test_parse_board_skips_unusable_rows_without_failing_the_board():
    """A row with no title or no jobUrl cannot be identified; the rest survive."""
    records = list(ashby.parse_board(_fixture(), _target()))
    assert len(records) == 3
    ids = [r.req_id for r in records]
    assert "99990000-1111-2222-3333-444455556666" not in ids  # empty title
    assert "77778888-9999-0000-1111-222233334444" not in ids  # no jobUrl


def test_parse_board_missing_location_is_empty_not_an_error():
    """None of the surviving rows omit location in this fixture, but an absent
    key must not raise — assert directly against a hand-built minimal job."""
    minimal = {
        "jobs": [
            {
                "id": "abc",
                "title": "Bare Role",
                "jobUrl": "https://jobs.ashbyhq.com/examplecorp/abc",
            }
        ]
    }
    record = list(ashby.parse_board(minimal, _target()))[0]
    assert record.location == ""
    assert record.remote is False
    assert record.salary_text == ""
    assert record.posted_date is None


def test_parse_board_records_provenance_without_hashing_it():
    first = list(ashby.parse_board(_fixture(), _target()))[0]
    assert first.extra["department"] == "Customer Engineering"
    assert first.extra["team"] == "Platform"
    assert first.extra["employment_type"] == "FullTime"
    assert "department" not in first.canonical_fields()


def test_parse_board_content_hash_is_stable_across_runs():
    once = list(ashby.parse_board(_fixture(), _target()))
    twice = list(ashby.parse_board(_fixture(), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    # Same board, same bytes, different instance -> different identity namespace.
    other = list(ashby.parse_board(_fixture(), _target(slug="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_board_identity_prefers_the_ashby_job_id_over_url():
    first = list(ashby.parse_board(_fixture(), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "ashby:examplecorp",
        "5f2c9a10-8b3e-4c1a-9d4f-1a2b3c4d5e6f",
    )
    assert claims[1].kind == "url"


def test_parse_board_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"jobs": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(ashby.parse_board(payload, _target()))


def test_parse_board_accepts_bytes_str_and_mapping():
    raw = _fixture()
    assert (
        len(list(ashby.parse_board(raw, _target())))
        == len(list(ashby.parse_board(raw.decode(), _target())))
        == len(list(ashby.parse_board(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_company_map():
    config = SourceConfig.from_mapping(
        {"companies": {"ashby": {"anthropic": "Anthropic", "stripe": "Stripe", "": "junk"}}}
    )
    targets = ashby.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["anthropic", "stripe"]
    assert [t.label for t in targets] == ["Anthropic", "Stripe"]
    assert all(t.host == ashby.API_HOST for t in targets)
    assert all(t.inventory_scope is InventoryScope.COMPLETE for t in targets)
    assert targets[0].param("slug") == "anthropic"


def test_plan_without_ashby_config_is_empty_not_an_error():
    assert list(ashby.ADAPTER.plan(SourceConfig())) == []


def test_descriptor_declares_daily_and_full_direct():
    descriptor = ashby.DESCRIPTOR
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
    records = asyncio.run(collect(ashby.ADAPTER, _target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == [
        "5f2c9a10-8b3e-4c1a-9d4f-1a2b3c4d5e6f",
        "a1b2c3d4-e5f6-4789-9abc-def012345678",
        "11112222-3333-4444-5555-666677778888",
    ]
    assert transport.call_count == 1
    assert transport.urls() == ["https://api.ashbyhq.com/posting-api/job-board/examplecorp"]


def test_fetch_on_a_missing_board_is_permanent_not_empty():
    transport = FakeTransport().add(BOARD, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(ashby.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(BOARD, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(ashby.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    transport = FakeTransport().add(BOARD, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(ashby.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce."""
    transport = FakeTransport().add(BOARD, json_response(json.loads(_fixture())))
    fetched = asyncio.run(collect(ashby.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(ashby.parse_board(_fixture(), _target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


def test_fetch_requires_the_slug_param():
    transport = FakeTransport().add(BOARD, json_response({"jobs": []}))
    bare = SourceTarget(source_key="ashby", instance_key="examplecorp")
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(ashby.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_on_an_empty_board_yields_nothing_and_does_not_raise():
    """An empty board is a positive assertion, and only reachable via a 200."""
    transport = FakeTransport().add(BOARD, json_response({"jobs": []}))
    records = asyncio.run(collect(ashby.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
