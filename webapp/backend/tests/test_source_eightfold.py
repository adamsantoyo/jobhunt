"""Eightfold PCSX adapter (Microsoft careers + generic Eightfold boards).

Parsing/transport split, driven by frozen fixtures, mirroring
`test_source_greenhouse.py`'s structure. Additionally covers what Greenhouse's
single-response board does not need: search-term fan-out, pagination, and
checkpoint resume/replay.
"""
import asyncio
import datetime
import json

import pytest

from backend.sources.adapters import eightfold
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

BASE = "https://apply.starbucks.com"
DOMAIN = "starbucks.com"
URL = eightfold.search_url(BASE)


def _target(
    slug="starbucks",
    name="Starbucks",
    base=BASE,
    domain=DOMAIN,
    terms=("support engineer",),
    legacy_source="eightfold",
    location="California, United States",
):
    return SourceTarget(
        source_key="eightfold",
        instance_key=slug,
        label=name,
        params={
            "base": base,
            "domain": domain,
            "company": name,
            "location": location,
            "terms": terms,
            "legacy_source": legacy_source,
        },
        inventory_scope=InventoryScope.PARTIAL,
    )


def _fixture(name):
    return fixture_bytes("eightfold", name)


def _fixture_json(name):
    return json.loads(_fixture(name))


def _utc_date(ts):
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).date().isoformat()


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_search_page_from_frozen_fixture():
    records = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))
    assert [r.req_id for r in records] == ["REQ-9001", "9002", "REQ-9004"]
    assert all(r.source_key == "eightfold" and r.instance_key == "starbucks" for r in records)
    assert all(r.company == "Starbucks" for r in records)


def test_parse_search_page_skips_a_row_with_no_title():
    """A row with an empty title cannot be identified; the rest must survive."""
    records = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))
    assert len(records) == 3
    assert "REQ-9003" not in [r.req_id for r in records]


def test_parse_search_page_falls_back_to_id_when_displayJobId_is_absent():
    second = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))[1]
    assert second.req_id == "9002"  # no displayJobId on this row


def test_parse_search_page_prefers_wa_and_ca_metros():
    first, second, fourth = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))
    assert first.location == "Seattle, WA, United States; Redmond, WA, United States"
    assert second.location == "San Francisco, CA, United States"
    # neither of this row's two locations is WA/CA -> fall back to the first two raw entries
    assert fourth.location == "Austin, TX, United States; Denver, CO, United States"


def test_parse_search_page_accepts_a_bare_string_locations_field():
    second = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))[1]
    assert second.location == "San Francisco, CA, United States"


def test_parse_search_page_relative_position_url_is_joined_to_base():
    first = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))[0]
    assert first.url == "https://apply.starbucks.com/careers/job/9001?domain=starbucks.com"


def test_parse_search_page_constructs_a_fallback_url_without_positionUrl():
    second = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))[1]
    assert second.url == "https://apply.starbucks.com/careers/job/9002"


def test_parse_search_page_remote_flag_from_workLocationOption():
    first, second, fourth = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))
    assert first.remote is True  # workLocationOption == "remote"
    assert second.remote is False  # field absent
    assert fourth.remote is False


def test_parse_search_page_posted_date_is_utc_from_epoch_seconds():
    first, second, fourth = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))
    assert first.posted_date == _utc_date(1786060800)
    assert first.posted_raw == "1786060800"
    assert second.posted_date == _utc_date(1786147200)
    # no postedTs at all -> no date, and no fabricated raw string
    assert fourth.posted_date is None
    assert fourth.posted_raw == ""


def test_parse_search_page_records_legacy_source_and_domain_as_provenance():
    first = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))[0]
    assert first.extra["legacy_source"] == "eightfold"
    assert first.extra["domain"] == "starbucks.com"
    assert "internal_job_id" not in first.canonical_fields()
    assert "legacy_source" not in first.canonical_fields()


def test_parse_search_page_microsoft_instance_labels_legacy_source():
    target = _target(
        slug="microsoft",
        name="Microsoft",
        base="https://apply.careers.microsoft.com",
        domain="microsoft.com",
        legacy_source="microsoft-careers",
    )
    records = list(eightfold.parse_search_page(_fixture("page1.json"), target))
    assert all(r.extra["legacy_source"] == "microsoft-careers" for r in records)
    assert all(r.namespace == "eightfold:microsoft" for r in records)


def test_parse_search_page_content_hash_is_stable_across_runs():
    once = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))
    twice = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    other = list(eightfold.parse_search_page(_fixture("page1.json"), _target(slug="other", name="Other")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_search_page_identity_prefers_the_source_native_id():
    first = list(eightfold.parse_search_page(_fixture("page1.json"), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "eightfold:starbucks",
        "REQ-9001",
    )
    assert claims[1].kind == "url"


def test_parse_search_page_rejects_a_malformed_envelope():
    for payload in (
        b"<html>blocked</html>",
        b"[]",
        json.dumps({"data": "nope"}).encode(),
        json.dumps({"data": {"positions": "nope"}}).encode(),
        json.dumps({"nope": {}}).encode(),
    ):
        with pytest.raises(PayloadError):
            list(eightfold.parse_search_page(payload, _target()))


def test_parse_search_page_accepts_bytes_str_and_mapping():
    raw = _fixture("page1.json")
    assert (
        len(list(eightfold.parse_search_page(raw, _target())))
        == len(list(eightfold.parse_search_page(raw.decode(), _target())))
        == len(list(eightfold.parse_search_page(json.loads(raw), _target())))
    )


def test_parse_search_page_requires_base_param():
    bare = SourceTarget(source_key="eightfold", instance_key="starbucks", params={"domain": DOMAIN})
    with pytest.raises(ConfigError):
        list(eightfold.parse_search_page(_fixture("page1.json"), bare))


def test_parse_search_page_on_an_empty_page_yields_nothing_and_does_not_raise():
    assert list(eightfold.parse_search_page(_fixture("empty.json"), _target())) == []


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_companies_and_bakes_in_search_terms():
    config = SourceConfig.from_mapping(
        {
            "profile": {
                "search_terms": ["support engineer", "technical support"],
                "employer_scrape_location": "California, United States",
            },
            "companies": {
                "eightfold": {
                    "starbucks": {"base": "https://apply.starbucks.com", "domain": "starbucks.com", "name": "Starbucks"},
                    "fortive": {"base": "https://fortive.eightfold.ai", "domain": "fortive.com", "name": "Fortive"},
                    "microsoft": {
                        "base": "https://apply.careers.microsoft.com",
                        "domain": "microsoft.com",
                        "name": "Microsoft",
                    },
                    "nolocation": {"domain": "missingbase.example"},  # missing base -> skipped
                    "": {"base": "https://x.example", "domain": "x.example"},  # empty slug -> skipped
                }
            },
        }
    )
    targets = eightfold.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["starbucks", "fortive", "microsoft"]
    assert all(t.param("terms") == ("support engineer", "technical support") for t in targets)
    assert all(t.inventory_scope is InventoryScope.PARTIAL for t in targets)

    starbucks = targets[0]
    assert starbucks.param("legacy_source") == "eightfold"
    assert starbucks.param("location") == "California, United States"  # profile default

    microsoft = targets[2]
    assert microsoft.param("legacy_source") == "microsoft-careers"
    assert microsoft.param("domain") == "microsoft.com"
    assert microsoft.namespace == "eightfold:microsoft"


def test_plan_per_entry_location_overrides_the_profile_default():
    config = SourceConfig.from_mapping(
        {
            "profile": {"search_terms": ["support engineer"], "employer_scrape_location": "California, United States"},
            "companies": {
                "eightfold": {
                    "fortive": {
                        "base": "https://fortive.eightfold.ai",
                        "domain": "fortive.com",
                        "name": "Fortive",
                        "location": "Washington, United States",
                    }
                }
            },
        }
    )
    targets = eightfold.ADAPTER.plan(config)
    assert targets[0].param("location") == "Washington, United States"


def test_plan_without_search_terms_is_empty_not_an_error():
    config = SourceConfig.from_mapping(
        {"companies": {"eightfold": {"starbucks": {"base": "https://apply.starbucks.com", "domain": "starbucks.com"}}}}
    )
    assert list(eightfold.ADAPTER.plan(config)) == []


def test_plan_without_eightfold_config_is_empty_not_an_error():
    config = SourceConfig.from_mapping({"profile": {"search_terms": ["support engineer"]}})
    assert list(eightfold.ADAPTER.plan(config)) == []


def test_descriptor_declares_daily_and_full_direct_with_partial_scope():
    descriptor = eightfold.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL


# --------------------------------------------------------------------------- #
# fetch(): transport shell, pagination, checkpoints
# --------------------------------------------------------------------------- #
def test_fetch_walks_two_terms_to_exhaustion_and_streams_parsed_records():
    transport = FakeTransport()
    transport.add(
        URL,
        json_response(_fixture_json("page1.json")),  # term "a", start=0 -> 3 usable rows
        json_response(_fixture_json("empty.json")),  # term "a", start=10 -> ends term "a"
        json_response(_fixture_json("page2.json")),  # term "b", start=0 -> 1 row
        json_response(_fixture_json("empty.json")),  # term "b", start=10 -> ends term "b"
    )
    target = _target(terms=("term-a", "term-b"))
    ctx = FetchContext(transport=transport)
    records = asyncio.run(collect(eightfold.ADAPTER, target, ctx))
    assert [r.req_id for r in records] == ["REQ-9001", "9002", "REQ-9004", "REQ-9005"]
    assert transport.call_count == 4
    assert ctx.checkpoint.cursor == {"term_index": 2, "start": 0}
    assert ctx.checkpoint.emitted == 4


def test_fetch_records_stream_before_the_target_finishes():
    transport = FakeTransport()
    transport.add(
        URL,
        json_response(_fixture_json("page1.json")),
        json_response(_fixture_json("empty.json")),
    )
    target = _target(terms=("term-a",))
    ctx = FetchContext(transport=transport)

    async def scenario():
        stream = eightfold.ADAPTER.fetch(target, ctx)
        first = await stream.__anext__()
        assert transport.call_count == 1  # the empty terminator page has not been fetched yet
        rest = [record async for record in stream]
        return first, rest

    first, rest = asyncio.run(scenario())
    assert first.req_id == "REQ-9001"
    assert [r.req_id for r in rest] == ["9002", "REQ-9004"]


def test_fetch_request_params_carry_domain_query_and_location():
    transport = FakeTransport().add(URL, json_response(_fixture_json("empty.json")))
    target = _target(terms=("technical support",), location="Washington, United States")
    asyncio.run(collect(eightfold.ADAPTER, target, FetchContext(transport=transport)))
    sent = transport.requests[0]
    assert sent.params == {
        "domain": "starbucks.com",
        "query": "technical support",
        "location": "Washington, United States",
        "start": 0,
        "num": 10,
        "sort_by": "relevance",
    }


def test_fetch_on_a_dead_board_is_permanent_not_empty():
    transport = FakeTransport().add(URL, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(eightfold.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(URL, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(eightfold.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_a_malformed_payload_is_a_payload_error():
    transport = FakeTransport().add(URL, text_response("<html>blocked</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(eightfold.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_requires_base_and_domain_params():
    bare = SourceTarget(source_key="eightfold", instance_key="starbucks", params={"terms": ("x",)})
    transport = FakeTransport()
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(eightfold.ADAPTER, bare, FetchContext(transport=transport)))
    assert transport.call_count == 0


def test_fetch_with_no_search_terms_makes_no_requests_and_yields_nothing():
    transport = FakeTransport()
    target = _target(terms=())
    records = asyncio.run(collect(eightfold.ADAPTER, target, FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 0


def test_fetch_advances_to_the_next_term_at_the_page_cap_without_exhausting_it():
    """A term that never returns an empty page still yields to the next term
    after `MAX_PAGES_PER_TERM` pages, matching the legacy bounded loop."""

    def responder(request):
        start = request.params["start"]
        return json_response(
            {
                "data": {
                    "positions": [
                        {"id": 9200 + start, "displayJobId": f"REQ-{9200 + start}", "name": f"Support Eng {start}"}
                    ],
                    "count": 99999,
                }
            }
        )

    transport = FakeTransport().add(URL, responder)
    target = _target(terms=("evergreen",))
    ctx = FetchContext(transport=transport)
    records = asyncio.run(collect(eightfold.ADAPTER, target, ctx))
    assert transport.call_count == eightfold.MAX_PAGES_PER_TERM
    assert len(records) == eightfold.MAX_PAGES_PER_TERM
    assert ctx.checkpoint.cursor == {"term_index": 1, "start": 0}


def test_fetch_yields_the_same_records_the_pure_parser_does():
    transport = FakeTransport().add(
        URL, json_response(_fixture_json("page1.json")), json_response(_fixture_json("empty.json"))
    )
    target = _target(terms=("term-a",))
    fetched = asyncio.run(collect(eightfold.ADAPTER, target, FetchContext(transport=transport)))
    parsed = list(eightfold.parse_search_page(_fixture("page1.json"), target))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


# --------------------------------------------------------------------------- #
# Checkpoints: round-trip, resume, replay-safety
# --------------------------------------------------------------------------- #
def test_checkpoint_round_trips_and_is_scoped_to_the_target():
    target = _target(terms=("term-a", "term-b"))
    checkpoint = Checkpoint(
        source_key="eightfold",
        instance_key="starbucks",
        cursor={"term_index": 1, "start": 10},
        config_fingerprint=target.config_fingerprint(),
        emitted=13,
    )
    restored = Checkpoint.from_json(checkpoint.to_json())
    assert restored == checkpoint
    assert restored.is_valid_for(target)
    # A search-term change invalidates the checkpoint: the cursor would point
    # into a different query's result set.
    assert not restored.is_valid_for(_target(terms=("term-a", "term-b", "term-c")))


def test_resume_from_checkpoint_continues_mid_term_then_moves_on():
    transport = FakeTransport()
    transport.add(
        URL,
        json_response(_fixture_json("empty.json")),  # term "a" resumed at start=10 -> ends term "a"
        json_response(_fixture_json("page2.json")),  # term "b", start=0 -> 1 row
        json_response(_fixture_json("empty.json")),  # term "b", start=10 -> ends term "b"
    )
    target = _target(terms=("term-a", "term-b"))
    resume = Checkpoint(
        source_key="eightfold",
        instance_key="starbucks",
        cursor={"term_index": 0, "start": 10},
        config_fingerprint=target.config_fingerprint(),
        emitted=3,
    )
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(eightfold.ADAPTER, target, ctx))
    assert [r.req_id for r in records] == ["REQ-9005"]
    assert transport.requests[0].params["start"] == 10
    assert transport.requests[0].params["query"] == "term-a"
    assert ctx.checkpoint.emitted == 4


def test_replayed_checkpoint_re_emits_identical_records():
    """Invariant 5: replay is expected, and the writer dedupes on identity."""
    target = _target(terms=("term-a",))
    first_transport = FakeTransport().add(
        URL, json_response(_fixture_json("page1.json")), json_response(_fixture_json("empty.json"))
    )
    first = asyncio.run(collect(eightfold.ADAPTER, target, FetchContext(transport=first_transport)))

    stale = Checkpoint(
        source_key="eightfold",
        instance_key="starbucks",
        cursor={"term_index": 0, "start": 0},
        config_fingerprint=target.config_fingerprint(),
        emitted=0,
    )
    second_transport = FakeTransport().add(
        URL, json_response(_fixture_json("page1.json")), json_response(_fixture_json("empty.json"))
    )
    second = asyncio.run(
        collect(eightfold.ADAPTER, target, FetchContext(transport=second_transport, resume_from=stale))
    )
    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]
    assert [r.identity_claims() for r in second] == [r.identity_claims() for r in first]


def test_stale_checkpoint_for_a_changed_target_is_ignored_by_the_adapter():
    target = _target(terms=("term-a", "term-b"))
    stale = Checkpoint(
        source_key="eightfold",
        instance_key="starbucks",
        cursor={"term_index": 1, "start": 20},
        config_fingerprint="stale-fingerprint",
        emitted=99,
    )
    transport = FakeTransport().add(
        URL, json_response(_fixture_json("empty.json")), json_response(_fixture_json("empty.json"))
    )
    asyncio.run(collect(eightfold.ADAPTER, target, FetchContext(transport=transport, resume_from=stale)))
    # Started clean at term_index 0 / start 0, not from the stale cursor.
    assert transport.requests[0].params["start"] == 0
    assert transport.requests[0].params["query"] == "term-a"


def test_mid_stream_failure_keeps_earlier_records_and_last_checkpoint():
    transport = FakeTransport().add(
        URL, json_response(_fixture_json("page1.json")), json_response({}, status=503)
    )
    target = _target(terms=("term-a",))
    ctx = FetchContext(transport=transport)

    async def scenario():
        seen = []
        with pytest.raises(TransientSourceError):
            async for record in eightfold.ADAPTER.fetch(target, ctx):
                seen.append(record)
        return seen

    seen = asyncio.run(scenario())
    assert [r.req_id for r in seen] == ["REQ-9001", "9002", "REQ-9004"]
    assert ctx.checkpoint is not None
    assert ctx.checkpoint.cursor == {"term_index": 0, "start": 10}
    assert ctx.checkpoint.emitted == 3
