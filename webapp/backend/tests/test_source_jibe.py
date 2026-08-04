"""Jibe / iCIMS Careers Cloud portals (including Costco): parsing/transport
split, driven by frozen fixtures.

Two modes share one pure parser (`parse_jobs_page`) and one envelope shape;
what differs is entirely in `plan()`/`fetch()` — the query sent, the page cap,
and the url construction rule. See `jibe.py`'s module docstring for the full
rationale.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import jibe
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
from backend.sources.testing import FakeTransport, collect, fixture_bytes, json_response

JOBS_URL = jibe.jobs_url("https://careers.example.com")
COSTCO_URL = jibe.jobs_url("https://careers.costco.com")


def _keyword_target(base="https://careers.example.com", slug="example", name="Example Co", terms=("support engineer",)):
    return SourceTarget(
        source_key="jibe",
        instance_key=slug,
        label=name,
        params={"base": base, "company": name, "mode": "keyword", "terms": tuple(terms)},
        inventory_scope=InventoryScope.PARTIAL,
        host="careers.example.com",
    )


def _state_target(base="https://careers.costco.com", slug="costco", name="Costco Wholesale", state="Washington"):
    return SourceTarget(
        source_key="jibe",
        instance_key=slug,
        label=name,
        params={"base": base, "company": name, "mode": "state", "state": state},
        inventory_scope=InventoryScope.PARTIAL,
        host="careers.costco.com",
    )


def _page1():
    return fixture_bytes("jibe", "page1.json")


def _page2():
    return fixture_bytes("jibe", "page2.json")


# --------------------------------------------------------------------------- #
# Title prefilter
# --------------------------------------------------------------------------- #
def test_title_prefilter_matches_the_costco_tech_keyword_list():
    assert jibe.matches_title_prefilter("Support Engineer II")
    assert jibe.matches_title_prefilter("Systems Administrator")
    assert jibe.matches_title_prefilter("IT Support Specialist")  # "it " keyword
    assert jibe.matches_title_prefilter("Security Analyst")
    assert not jibe.matches_title_prefilter("Marketing Coordinator")
    assert not jibe.matches_title_prefilter("")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport) — keyword mode
# --------------------------------------------------------------------------- #
def test_parse_jobs_page_filters_by_title_and_identity_keyword_mode():
    records = list(jibe.parse_jobs_page(_page1(), _keyword_target()))
    # 5 rows in; "Marketing Coordinator" fails the title filter, "Data Analyst
    # Intern" matches the title but has no req_id or slug.
    assert [r.req_id for r in records] == ["R-2001", "network-technician-tacoma", "R-2004"]


def test_parse_jobs_page_keyword_mode_ignores_apply_url_uses_slug_page():
    """Generic Jibe: apply_url is an ATS login link, never the display url."""
    first = list(jibe.parse_jobs_page(_page1(), _keyword_target()))[0]
    assert first.url == "https://careers.example.com/jobs/support-engineer-seattle"
    assert first.company == "Example Co"
    assert first.namespace == "jibe:example"


def test_parse_jobs_page_state_mode_prefers_apply_url_then_canonical_url():
    """Costco-style: apply_url/canonical_url are real posting pages."""
    records = list(jibe.parse_jobs_page(_page1(), _state_target()))
    by_id = {r.req_id: r for r in records}
    assert by_id["R-2001"].url == "https://sso.example.com/apply?req=R-2001"
    # network-technician-tacoma has neither apply_url nor canonical_url -> slug page.
    assert by_id["network-technician-tacoma"].url == "https://careers.costco.com/jobs/network-technician-tacoma"
    # R-2004 has no slug either -> the id itself is used as the slug fallback.
    assert by_id["R-2004"].url == "https://careers.costco.com/jobs/R-2004"


def test_parse_jobs_page_state_mode_falls_back_to_canonical_url_without_apply_url():
    payload = {
        "jobs": [
            {
                "data": {
                    "req_id": "R-5001",
                    "slug": "data-engineer-olympia",
                    "title": "Data Engineer",
                    "canonical_url": "https://careers.costco.com/job/R-5001",
                }
            }
        ]
    }
    record = list(jibe.parse_jobs_page(payload, _state_target()))[0]
    assert record.url == "https://careers.costco.com/job/R-5001"


def test_parse_jobs_page_location_prefers_full_location_then_joins_city_state():
    records = list(jibe.parse_jobs_page(_page1(), _keyword_target()))
    by_id = {r.req_id: r for r in records}
    assert by_id["R-2001"].location == "Seattle, WA, United States"
    assert by_id["network-technician-tacoma"].location == "Tacoma, WA"
    assert by_id["R-2004"].location == ""  # no city/state/full_location at all


def test_parse_jobs_page_posted_date_prefers_posted_date_over_create_date():
    records = list(jibe.parse_jobs_page(_page1(), _keyword_target()))
    by_id = {r.req_id: r for r in records}
    assert by_id["R-2001"].posted_date == "2026-07-18"
    assert by_id["network-technician-tacoma"].posted_date == "2026-06-30"  # falls back to create_date


def test_parse_jobs_page_keeps_relative_posted_strings_out_of_the_hash():
    record = [r for r in jibe.parse_jobs_page(_page1(), _keyword_target()) if r.req_id == "R-2004"][0]
    assert record.posted_date is None
    assert record.posted_raw == "Recently Posted"
    assert record.canonical_fields()["posted_date"] == ""


def test_parse_jobs_page_req_id_falls_back_to_slug_when_req_id_is_absent():
    record = [r for r in jibe.parse_jobs_page(_page1(), _keyword_target()) if r.req_id == "network-technician-tacoma"][0]
    assert record.identity_claims()[0].value == "network-technician-tacoma"


def test_parse_jobs_page_skips_rows_with_no_usable_identity():
    records = list(jibe.parse_jobs_page(_page1(), _keyword_target()))
    assert "" not in [r.req_id for r in records]
    assert len(records) == 3


def test_parse_jobs_page_does_not_populate_description():
    """Deliberately out of scope for this adapter; see module docstring."""
    first = list(jibe.parse_jobs_page(_page1(), _keyword_target()))[0]
    assert first.description is None


def test_parse_jobs_page_accepts_a_row_without_the_data_wrapper():
    record = list(jibe.parse_jobs_page(fixture_bytes("jibe", "unwrapped_row.json"), _keyword_target()))[0]
    assert record.req_id == "R-4001"
    assert record.title == "Security Analyst"


def test_parse_jobs_page_accepts_bytes_str_and_mapping():
    raw = _page1()
    assert (
        len(list(jibe.parse_jobs_page(raw, _keyword_target())))
        == len(list(jibe.parse_jobs_page(raw.decode(), _keyword_target())))
        == len(list(jibe.parse_jobs_page(json.loads(raw), _keyword_target())))
    )


def test_parse_jobs_page_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"jobs": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(jibe.parse_jobs_page(payload, _keyword_target()))


def test_parse_jobs_page_content_hash_differs_by_portal_namespace():
    once = list(jibe.parse_jobs_page(_page1(), _keyword_target()))
    other = list(jibe.parse_jobs_page(_page1(), _keyword_target(slug="other", name="Other Co")))
    assert [r.content_hash() for r in once] != [r.content_hash() for r in other]


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_keyword_mode_bakes_search_terms_into_target_params():
    config = SourceConfig.from_mapping(
        {
            "profile": {"search_terms": ["support engineer", "network technician"]},
            "companies": {"jibe": {"amd": {"base": "https://careers.amd.com", "name": "AMD"}}},
        }
    )
    targets = jibe.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["amd"]
    target = targets[0]
    assert target.param("mode") == "keyword"
    assert target.param("terms") == ("support engineer", "network technician")
    assert target.inventory_scope is InventoryScope.PARTIAL
    assert target.host == "careers.amd.com"


def test_plan_keyword_mode_without_search_terms_plans_nothing():
    config = SourceConfig.from_mapping(
        {"companies": {"jibe": {"amd": {"base": "https://careers.amd.com", "name": "AMD"}}}}
    )
    assert list(jibe.ADAPTER.plan(config)) == []


def test_plan_state_mode_is_a_config_entry_not_a_hardcoded_company():
    """Costco is unconfigured by default; adding the entry is what turns it on."""
    empty = SourceConfig.from_mapping({"companies": {"jibe": {}}})
    assert list(jibe.ADAPTER.plan(empty)) == []

    config = SourceConfig.from_mapping(
        {
            "companies": {
                "jibe": {
                    "costco": {
                        "base": "https://careers.costco.com",
                        "name": "Costco Wholesale",
                        "mode": "state",
                        "state": "Washington",
                    }
                }
            }
        }
    )
    targets = jibe.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["costco"]
    target = targets[0]
    assert target.param("mode") == "state"
    assert target.param("state") == "Washington"
    assert target.param("company") == "Costco Wholesale"
    assert target.inventory_scope is InventoryScope.PARTIAL
    # State mode plans even with zero search terms configured.
    assert jibe.ADAPTER.plan(config)[0].instance_key == "costco"


def test_plan_state_mode_requires_a_state_param():
    config = SourceConfig.from_mapping(
        {"companies": {"jibe": {"costco": {"base": "https://careers.costco.com", "mode": "state"}}}}
    )
    with pytest.raises(ConfigError):
        jibe.ADAPTER.plan(config)


def test_plan_rejects_an_unknown_mode():
    config = SourceConfig.from_mapping(
        {"companies": {"jibe": {"x": {"base": "https://careers.x.example", "mode": "bogus"}}}}
    )
    with pytest.raises(ConfigError):
        jibe.ADAPTER.plan(config)


def test_plan_requires_base():
    config = SourceConfig.from_mapping({"companies": {"jibe": {"x": {"name": "X"}}}})
    with pytest.raises(ConfigError):
        jibe.ADAPTER.plan(config)


def test_plan_without_jibe_config_is_empty_not_an_error():
    assert list(jibe.ADAPTER.plan(SourceConfig())) == []


def test_plan_mixes_keyword_and_state_entries_in_one_config():
    config = SourceConfig.from_mapping(
        {
            "profile": {"search_terms": ["support engineer"]},
            "companies": {
                "jibe": {
                    "amd": {"base": "https://careers.amd.com", "name": "AMD"},
                    "costco": {
                        "base": "https://careers.costco.com",
                        "name": "Costco Wholesale",
                        "mode": "state",
                        "state": "Washington",
                    },
                }
            },
        }
    )
    targets = jibe.ADAPTER.plan(config)
    assert sorted(t.instance_key for t in targets) == ["amd", "costco"]
    modes = {t.instance_key: t.param("mode") for t in targets}
    assert modes == {"amd": "keyword", "costco": "state"}


def test_descriptor_declares_daily_and_full_direct_and_checkpointing():
    descriptor = jibe.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL
    assert descriptor.min_request_interval_seconds == 0.2


# --------------------------------------------------------------------------- #
# fetch(): state mode (Costco-style)
# --------------------------------------------------------------------------- #
def test_fetch_state_mode_paginates_until_an_empty_page():
    transport = FakeTransport()
    transport.add(COSTCO_URL, json_response(json.loads(_page1())), json_response(json.loads(_page2())), json_response({"jobs": []}))
    records = asyncio.run(collect(jibe.ADAPTER, _state_target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["R-2001", "network-technician-tacoma", "R-2004", "R-3001"]
    assert transport.call_count == 3
    first_request = transport.requests[0]
    assert first_request.params == {"state": "Washington", "limit": 100, "page": 1, "lang": "en-us"}
    assert transport.requests[1].params["page"] == 2


def test_fetch_state_mode_stops_at_the_page_cap_without_raising():
    transport = FakeTransport()
    transport.add(COSTCO_URL, json_response(json.loads(_page2())))  # same non-empty page forever
    records = asyncio.run(collect(jibe.ADAPTER, _state_target(), FetchContext(transport=transport)))
    assert transport.call_count == jibe.STATE_PAGE_LIMIT
    assert len(records) == jibe.STATE_PAGE_LIMIT  # one matching row per page


def test_fetch_state_mode_on_a_dead_portal_is_permanent_not_empty():
    transport = FakeTransport().add(COSTCO_URL, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(jibe.ADAPTER, _state_target(), FetchContext(transport=transport)))


def test_fetch_state_mode_on_throttling_is_transient():
    transport = FakeTransport().add(COSTCO_URL, json_response({}, status=503))
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(jibe.ADAPTER, _state_target(), FetchContext(transport=transport)))


def test_fetch_state_mode_requires_state_param():
    bare = SourceTarget(source_key="jibe", instance_key="costco", params={"base": "https://careers.costco.com", "mode": "state"})
    transport = FakeTransport().add(COSTCO_URL, json_response({"jobs": []}))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(jibe.ADAPTER, bare, FetchContext(transport=transport)))


# --------------------------------------------------------------------------- #
# fetch(): checkpoint round-trip, resume, replay-safety (state mode)
# --------------------------------------------------------------------------- #
def test_fetch_state_mode_checkpoints_after_each_page():
    transport = FakeTransport()
    transport.add(COSTCO_URL, json_response(json.loads(_page1())), json_response({"jobs": []}))
    ctx = FetchContext(transport=transport)
    asyncio.run(collect(jibe.ADAPTER, _state_target(), ctx))
    assert ctx.checkpoint.cursor["page"] == 2
    assert ctx.checkpoint.emitted == 3
    assert ctx.checkpoint.is_valid_for(_state_target())


def test_fetch_state_mode_resumes_from_checkpoint():
    resume = Checkpoint(
        source_key="jibe",
        instance_key="costco",
        cursor={"page": 2},
        config_fingerprint=_state_target().config_fingerprint(),
        emitted=3,
    )
    transport = FakeTransport()
    transport.add(COSTCO_URL, json_response(json.loads(_page2())), json_response({"jobs": []}))
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(jibe.ADAPTER, _state_target(), ctx))
    assert [r.req_id for r in records] == ["R-3001"]
    assert transport.requests[0].params["page"] == 2
    assert ctx.checkpoint.emitted == 4


def test_fetch_state_mode_replayed_checkpoint_re_emits_identical_records():
    target = _state_target()
    transport_a = FakeTransport()
    transport_a.add(COSTCO_URL, json_response(json.loads(_page1())), json_response({"jobs": []}))
    first = asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport_a)))

    stale = Checkpoint(
        source_key="jibe", instance_key="costco", cursor={"page": 1}, config_fingerprint=target.config_fingerprint(), emitted=0
    )
    transport_b = FakeTransport()
    transport_b.add(COSTCO_URL, json_response(json.loads(_page1())), json_response({"jobs": []}))
    second = asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport_b, resume_from=stale)))

    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]


def test_fetch_state_mode_stale_checkpoint_for_a_changed_target_is_ignored():
    target = _state_target(state="Washington")
    stale = Checkpoint(
        source_key="jibe", instance_key="costco", cursor={"page": 5}, config_fingerprint="stale", emitted=99
    )
    transport = FakeTransport().add(COSTCO_URL, json_response({"jobs": []}))
    asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport, resume_from=stale)))
    assert transport.requests[0].params["page"] == 1


# --------------------------------------------------------------------------- #
# fetch(): keyword mode (generic Jibe)
# --------------------------------------------------------------------------- #
def test_fetch_keyword_mode_moves_to_the_next_term_on_an_empty_page():
    transport = FakeTransport()
    transport.add(
        JOBS_URL,
        json_response(json.loads(_page1())),  # term 1, page 1
        json_response({"jobs": []}),  # term 1, page 2 -> empty, stop term 1
        json_response(json.loads(_page2())),  # term 2, page 1
        json_response({"jobs": []}),  # term 2, page 2 -> empty, stop term 2
    )
    target = _keyword_target(terms=("support engineer", "systems administrator"))
    records = asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["R-2001", "network-technician-tacoma", "R-2004", "R-3001"]
    assert transport.call_count == 4
    assert transport.requests[0].params == {"keywords": "support engineer", "limit": 100, "page": 1, "lang": "en-us"}
    assert transport.requests[2].params["keywords"] == "systems administrator"


def test_fetch_keyword_mode_stops_a_term_at_the_page_cap():
    transport = FakeTransport()
    transport.add(JOBS_URL, json_response(json.loads(_page2())))  # same non-empty page forever
    target = _keyword_target(terms=("support engineer",))
    records = asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport)))
    assert transport.call_count == jibe.KEYWORD_PAGE_LIMIT
    assert len(records) == jibe.KEYWORD_PAGE_LIMIT


def test_fetch_keyword_mode_with_no_terms_yields_nothing_without_a_request():
    target = SourceTarget(source_key="jibe", instance_key="amd", params={"base": "https://careers.amd.com", "mode": "keyword", "terms": ()})
    transport = FakeTransport()
    records = asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 0


def test_fetch_keyword_mode_on_a_failed_request_raises_mid_walk():
    transport = FakeTransport()
    transport.add(JOBS_URL, json_response(json.loads(_page1())), json_response({}, status=503))
    target = _keyword_target(terms=("support engineer", "network technician"))
    ctx = FetchContext(transport=transport)
    seen = []
    with pytest.raises(TransientSourceError):
        async def scenario():
            async for record in jibe.ADAPTER.fetch(target, ctx):
                seen.append(record)

        asyncio.run(scenario())
    assert [r.req_id for r in seen] == ["R-2001", "network-technician-tacoma", "R-2004"]
    # Partial progress survives the failure so the scheduler can persist it.
    assert ctx.checkpoint is not None
    assert ctx.checkpoint.cursor == {"term_index": 0, "page": 2}


# --------------------------------------------------------------------------- #
# fetch(): checkpoint round-trip, resume, replay-safety (keyword mode)
# --------------------------------------------------------------------------- #
def test_fetch_keyword_mode_checkpoints_term_index_between_terms():
    transport = FakeTransport()
    transport.add(
        JOBS_URL,
        json_response(json.loads(_page1())),
        json_response({"jobs": []}),
    )
    target = _keyword_target(terms=("support engineer",))
    ctx = FetchContext(transport=transport)
    asyncio.run(collect(jibe.ADAPTER, target, ctx))
    # One term, exhausted: term_index has advanced past the last term.
    assert ctx.checkpoint.cursor == {"term_index": 1, "page": 1}
    assert ctx.checkpoint.emitted == 3


def test_fetch_keyword_mode_resumes_mid_term_at_the_saved_page():
    target = _keyword_target(terms=("support engineer", "network technician"))
    resume = Checkpoint(
        source_key="jibe",
        instance_key="example",
        cursor={"term_index": 1, "page": 1},
        config_fingerprint=target.config_fingerprint(),
        emitted=3,
    )
    transport = FakeTransport()
    transport.add(JOBS_URL, json_response(json.loads(_page2())), json_response({"jobs": []}))
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(jibe.ADAPTER, target, ctx))
    assert [r.req_id for r in records] == ["R-3001"]
    # Resumed straight into term index 1 ("network technician"); term 0 is
    # never re-fetched.
    assert transport.requests[0].params["keywords"] == "network technician"
    assert transport.call_count == 2


def test_fetch_keyword_mode_replayed_checkpoint_re_emits_identical_records():
    target = _keyword_target(terms=("support engineer",))

    transport_a = FakeTransport()
    transport_a.add(JOBS_URL, json_response(json.loads(_page1())), json_response({"jobs": []}))
    first = asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport_a)))

    stale = Checkpoint(
        source_key="jibe", instance_key="example", cursor={"term_index": 0, "page": 1},
        config_fingerprint=target.config_fingerprint(), emitted=0,
    )
    transport_b = FakeTransport()
    transport_b.add(JOBS_URL, json_response(json.loads(_page1())), json_response({"jobs": []}))
    second = asyncio.run(collect(jibe.ADAPTER, target, FetchContext(transport=transport_b, resume_from=stale)))

    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]


def test_fetch_keyword_mode_stale_checkpoint_for_changed_search_terms_is_ignored():
    """Search terms are baked into target.params, so a changed profile
    invalidates config_fingerprint and the checkpoint is never resumed into."""
    old_target = _keyword_target(terms=("support engineer",))
    new_target = _keyword_target(terms=("support engineer", "network technician"))
    stale = Checkpoint(
        source_key="jibe", instance_key="example", cursor={"term_index": 0, "page": 3},
        config_fingerprint=old_target.config_fingerprint(), emitted=10,
    )
    assert not stale.is_valid_for(new_target)
    transport = FakeTransport()
    transport.add(JOBS_URL, json_response({"jobs": []}))
    asyncio.run(collect(jibe.ADAPTER, new_target, FetchContext(transport=transport, resume_from=stale)))
    assert transport.requests[0].params["page"] == 1
    assert transport.requests[0].params["keywords"] == "support engineer"


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce."""
    transport = FakeTransport()
    transport.add(COSTCO_URL, json_response(json.loads(_page1())), json_response({"jobs": []}))
    fetched = asyncio.run(collect(jibe.ADAPTER, _state_target(), FetchContext(transport=transport)))
    parsed = list(jibe.parse_jobs_page(_page1(), _state_target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]
