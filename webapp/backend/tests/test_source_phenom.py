"""Phenom adapter: the search-term fan-out worked example, driven by frozen
fixtures and a fake transport queued in exact request order.

`FakeTransport` routes on URL alone (query/body are invisible to the router),
and every Phenom request hits the same `{base}/widgets` URL regardless of
which term or page it is. That is exactly what `FakeTransport.add(url, *pages)`
is for: queue the responses in the order `fetch()` will actually request them
and let the queue drain one response per call.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import phenom
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
from backend.sources.testing import FakeTransport, collect, fixture_bytes, fixture_json, json_response

BASE = "https://careers.seattlechildrens.org"
WIDGETS = phenom.widgets_url(BASE)


def _target(
    slug="seattlechildrens",
    base=BASE,
    name="Seattle Children's",
    terms=("support engineer", "solutions engineer"),
):
    return SourceTarget(
        source_key="phenom",
        instance_key=slug,
        label=name,
        params={"base": base, "company": name, "search_terms": tuple(terms)},
        inventory_scope=InventoryScope.PARTIAL,
        host="careers.seattlechildrens.org",
    )


def _fixture(name):
    return fixture_bytes("phenom", name)


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_page_from_frozen_fixture():
    records = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))
    assert [r.req_id for r in records] == ["30012345", "30012346", "SEQ-30012348"]
    assert all(r.source_key == "phenom" and r.instance_key == "seattlechildrens" for r in records)
    assert all(r.company == "Seattle Children's" for r in records)


def test_parse_page_normalizes_title_and_prefers_citystatecountry_over_location():
    first = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[0]
    assert first.title == "Support Engineer II"
    assert first.location == "Seattle, WA, United States"  # not the sibling "location" field
    assert first.namespace == "phenom:seattlechildrens"


def test_parse_page_falls_back_to_location_then_citystate():
    second, third = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[1:3]
    assert second.location == "Seattle, WA"  # only "location" present
    assert third.location == "Bellevue, WA"  # only "cityState" present


def test_parse_page_builds_apply_url_when_none_is_supplied():
    second, third = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[1:3]
    # jobId present, no applyUrl -> {base}/job/{jobId}
    assert second.url == f"{BASE}/job/30012346"
    # no jobId at all, only jobSeqNo -> {base}/job/{jobSeqNo}
    assert third.url == f"{BASE}/job/SEQ-30012348"
    assert third.req_id == "SEQ-30012348"


def test_parse_page_prefers_apply_url_when_present():
    first = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[0]
    assert first.url == (
        "https://careers.seattlechildrens.org/job/30012345/support-engineer-ii?gh_src=abc"
    )


def test_parse_page_prefers_posted_date_over_date_created():
    first, second = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[:2]
    assert first.posted_date == "2026-07-20"  # postedDate wins over dateCreated
    assert second.posted_date == "2026-07-15"  # falls back to dateCreated


def test_parse_page_skips_unusable_rows_without_failing_the_page():
    """Empty title, and no applyUrl/id combination that can build a url, are skipped."""
    records = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))
    assert len(records) == 3
    assert "30012347" not in [r.req_id for r in records]  # empty title
    assert "Unlinkable Listing" not in [r.title for r in records]  # no id, no applyUrl


def test_parse_page_records_job_seq_no_provenance_without_hashing_it():
    first = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[0]
    assert first.extra["job_seq_no"] == "SEQ-30012345"
    assert "job_seq_no" not in first.canonical_fields()


def test_parse_page_omits_job_seq_no_when_equal_to_the_chosen_id():
    third = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[2]
    # jobId is absent, so job_id == jobSeqNo == "SEQ-30012348"; not distinct provenance.
    assert "job_seq_no" not in third.extra


def test_parse_page_content_hash_is_stable_across_runs():
    once = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))
    twice = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    # Same page, different instance -> different identity namespace.
    other = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target(slug="rocket", name="Rocket")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_page_identity_prefers_the_job_id():
    first = list(phenom.parse_page(_fixture("page_with_jobs.json"), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "phenom:seattlechildrens",
        "30012345",
    )
    assert claims[1].kind == "url"


def test_parse_page_empty_page_yields_nothing_and_does_not_raise():
    """An empty result set is a positive assertion, only reachable via a 200."""
    records = list(phenom.parse_page(_fixture("page_empty.json"), _target()))
    assert records == []


def test_parse_page_rejects_a_malformed_envelope():
    for payload in (
        b"<html>blocked</html>",
        b"[]",
        json.dumps({"refineSearch": "nope"}).encode(),
        json.dumps({"refineSearch": {"data": {"jobs": "nope"}}}).encode(),
        _fixture("page_malformed_missing_jobs.json"),
    ):
        with pytest.raises(PayloadError):
            list(phenom.parse_page(payload, _target()))


def test_parse_page_accepts_bytes_str_and_mapping():
    raw = _fixture("page_with_jobs.json")
    assert (
        len(list(phenom.parse_page(raw, _target())))
        == len(list(phenom.parse_page(raw.decode(), _target())))
        == len(list(phenom.parse_page(json.loads(raw), _target())))
    )


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_the_config_company_map_and_bakes_in_search_terms():
    config = SourceConfig.from_mapping(
        {
            "profile": {"search_terms": ["support engineer", "solutions engineer"]},
            "companies": {
                "phenom": {
                    "seattlechildrens": {"base": BASE, "name": "Seattle Children's"},
                    "rocket": {"base": "https://careers.rocket.com", "name": "Rocket (Redfin)"},
                }
            },
        }
    )
    targets = phenom.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["seattlechildrens", "rocket"]
    assert [t.label for t in targets] == ["Seattle Children's", "Rocket (Redfin)"]
    assert targets[0].host == "careers.seattlechildrens.org"
    assert targets[0].param("search_terms") == ("support engineer", "solutions engineer")
    assert all(t.inventory_scope is InventoryScope.PARTIAL for t in targets)


def test_plan_without_phenom_config_is_empty_not_an_error():
    assert list(phenom.ADAPTER.plan(SourceConfig())) == []


def test_plan_raises_configerror_for_an_entry_missing_base():
    config = SourceConfig.from_mapping({"companies": {"phenom": {"broken": {"name": "No Base"}}}})
    with pytest.raises(ConfigError):
        phenom.ADAPTER.plan(config)


def test_plan_raises_configerror_for_a_non_mapping_entry():
    config = SourceConfig.from_mapping({"companies": {"phenom": {"broken": "not-an-object"}}})
    with pytest.raises(ConfigError):
        phenom.ADAPTER.plan(config)


def test_plan_changing_search_terms_changes_the_config_fingerprint():
    """A stored checkpoint must invalidate when the search terms change."""
    without = _target(terms=())
    changed = _target(terms=("support engineer",))
    assert without.config_fingerprint() != changed.config_fingerprint()


def test_descriptor_declares_daily_full_direct_partial_scope_and_checkpoint_support():
    descriptor = phenom.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL


# --------------------------------------------------------------------------- #
# fetch(): the transport shell, pagination, and term fan-out
# --------------------------------------------------------------------------- #
def test_fetch_walks_two_pages_then_moves_to_the_next_term():
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_with_jobs.json")),  # term0 from=0
        json_response(fixture_json("phenom", "page_single_job.json")),  # term0 from=50 (cap reached)
        json_response(fixture_json("phenom", "page_empty.json")),  # term1 from=0 (empty -> stop)
    )
    target = _target(terms=("support engineer", "solutions engineer"))
    records = asyncio.run(collect(phenom.ADAPTER, target, FetchContext(transport=transport)))
    assert transport.call_count == 3
    assert [r.req_id for r in records] == ["30012345", "30012346", "SEQ-30012348", "30012399"]
    bodies = [r.json_body for r in transport.requests]
    assert [b["keywords"] for b in bodies] == ["support engineer", "support engineer", "solutions engineer"]
    assert [b["from"] for b in bodies] == [0, 50, 0]
    assert all(r.method == "POST" for r in transport.requests)


def test_fetch_stops_a_term_early_on_an_empty_first_page():
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_empty.json")),  # term0 from=0 -> empty, stop
        json_response(fixture_json("phenom", "page_single_job.json")),  # term1 from=0
        json_response(fixture_json("phenom", "page_empty.json")),  # term1 from=50
    )
    target = _target(terms=("no matches", "support engineer"))
    records = asyncio.run(collect(phenom.ADAPTER, target, FetchContext(transport=transport)))
    # term0 never tries a second page; term1's second page is empty and ends it.
    assert transport.call_count == 3
    assert [r.req_id for r in records] == ["30012399"]


def test_fetch_with_no_search_terms_yields_nothing_and_makes_no_requests():
    transport = FakeTransport()
    target = _target(terms=())
    records = asyncio.run(collect(phenom.ADAPTER, target, FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 0


def test_fetch_requires_the_base_param():
    transport = FakeTransport()
    bare = SourceTarget(source_key="phenom", instance_key="seattlechildrens", params={"search_terms": ("x",)})
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(phenom.ADAPTER, bare, FetchContext(transport=transport)))
    assert transport.call_count == 0


def test_fetch_on_a_blocked_board_is_permanent_not_empty():
    transport = FakeTransport().add(WIDGETS, json_response({}, status=403))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(phenom.ADAPTER, _target(terms=("x",)), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(WIDGETS, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(phenom.ADAPTER, _target(terms=("x",)), FetchContext(transport=transport)))


def test_fetch_on_a_malformed_payload_is_a_payload_error():
    from backend.sources.testing import text_response

    transport = FakeTransport().add(WIDGETS, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(phenom.ADAPTER, _target(terms=("x",)), FetchContext(transport=transport)))


def test_adapter_does_not_retry_a_failed_request():
    """Invariant 1: one failure is one request. Retry is the scheduler's call."""
    transport = FakeTransport().add(WIDGETS, json_response({}, status=503))
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(phenom.ADAPTER, _target(terms=("x",)), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport shell must add nothing the fixture path cannot reproduce."""
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_with_jobs.json")),
        json_response(fixture_json("phenom", "page_empty.json")),
    )
    target = _target(terms=("support engineer",))
    fetched = asyncio.run(collect(phenom.ADAPTER, target, FetchContext(transport=transport)))
    parsed = list(phenom.parse_page(_fixture("page_with_jobs.json"), target))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


# --------------------------------------------------------------------------- #
# Streaming
# --------------------------------------------------------------------------- #
def test_records_stream_before_the_target_finishes():
    """Success Contract: new jobs appear before the whole target completes."""
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_with_jobs.json")),
        json_response(fixture_json("phenom", "page_empty.json")),
    )
    target = _target(terms=("support engineer",))
    ctx = FetchContext(transport=transport)

    async def scenario():
        stream = phenom.ADAPTER.fetch(target, ctx)
        first = await stream.__anext__()
        assert transport.call_count == 1  # only page 1 fetched so far
        rest = [record async for record in stream]
        return first, rest

    first, rest = asyncio.run(scenario())
    assert first.req_id == "30012345"
    assert [r.req_id for r in rest] == ["30012346", "SEQ-30012348"]
    assert transport.call_count == 2


# --------------------------------------------------------------------------- #
# Checkpoints: mark, resume, replay-safety
# --------------------------------------------------------------------------- #
def test_checkpoint_advances_within_a_term_then_across_terms():
    """`mark_checkpoint` fires once per *page*, only after that page's records
    have all been pulled by the consumer (an async generator only advances
    when pulled), so its timing is checked against exact `__anext__()` steps
    rather than against `len(seen)` inside an `async for` body."""
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_with_jobs.json")),  # term0 from=0 -> 3 records
        json_response(fixture_json("phenom", "page_single_job.json")),  # term0 from=50 -> 1 record, cap
        json_response(fixture_json("phenom", "page_empty.json")),  # term1 from=0 -> empty
    )
    target = _target(terms=("support engineer", "solutions engineer"))
    ctx = FetchContext(transport=transport)

    async def scenario():
        stream = phenom.ADAPTER.fetch(target, ctx)
        seen = [await stream.__anext__() for _ in range(4)]
        # Pulling the 4th record required exhausting page 0 (3 records) and
        # starting page 1 (the 1 record above); the checkpoint reflects "page
        # 0 of term 0 done, on to page 1" — set before page 1 was requested.
        assert ctx.checkpoint.cursor == {"term_index": 0, "from": 50}
        assert ctx.checkpoint.emitted == 3
        rest = [record async for record in stream]
        return seen, rest

    seen, rest = asyncio.run(scenario())
    assert len(seen) == 4
    assert rest == []  # term 1's only page is empty
    # After page 1's cap-ending page and term 1's empty page, both terms are
    # exhausted: term_index has walked past the last one.
    assert ctx.checkpoint.cursor == {"term_index": 2, "from": 0}
    assert ctx.checkpoint.emitted == 4
    assert ctx.checkpoint.is_valid_for(target)


def test_resume_from_checkpoint_continues_at_the_exact_term_and_offset():
    resume = Checkpoint(
        source_key="phenom",
        instance_key="seattlechildrens",
        cursor={"term_index": 0, "from": 50},
        config_fingerprint=_target(terms=("support engineer", "solutions engineer")).config_fingerprint(),
        emitted=3,
    )
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_single_job.json")),  # term0 from=50
        json_response(fixture_json("phenom", "page_empty.json")),  # term1 from=0
    )
    target = _target(terms=("support engineer", "solutions engineer"))
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(phenom.ADAPTER, target, ctx))
    assert [r.req_id for r in records] == ["30012399"]
    assert transport.requests[0].json_body["keywords"] == "support engineer"
    assert transport.requests[0].json_body["from"] == 50
    assert ctx.checkpoint.emitted == 4


def test_resume_from_checkpoint_can_start_mid_term_fan_out():
    resume = Checkpoint(
        source_key="phenom",
        instance_key="seattlechildrens",
        cursor={"term_index": 1, "from": 0},
        config_fingerprint=_target(terms=("support engineer", "solutions engineer")).config_fingerprint(),
        emitted=2,
    )
    transport = FakeTransport().add(WIDGETS, json_response(fixture_json("phenom", "page_empty.json")))
    target = _target(terms=("support engineer", "solutions engineer"))
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(phenom.ADAPTER, target, ctx))
    # Resuming at term_index 1 must issue its first request for the *second*
    # term ("solutions engineer"), never re-walking term 0.
    assert transport.call_count == 1
    assert transport.requests[0].json_body["keywords"] == "solutions engineer"
    assert transport.requests[0].json_body["from"] == 0
    assert records == []


def test_replayed_checkpoint_re_emits_identical_records():
    """Invariant 5: replay is expected, and the writer dedupes on identity."""
    target = _target(terms=("support engineer",))
    first = asyncio.run(
        collect(
            phenom.ADAPTER,
            target,
            FetchContext(
                transport=FakeTransport().add(
                    WIDGETS,
                    json_response(fixture_json("phenom", "page_with_jobs.json")),
                    json_response(fixture_json("phenom", "page_empty.json")),
                )
            ),
        )
    )
    stale = Checkpoint(
        source_key="phenom",
        instance_key="seattlechildrens",
        cursor={"term_index": 0, "from": 0},
        config_fingerprint=target.config_fingerprint(),
        emitted=0,
    )
    second = asyncio.run(
        collect(
            phenom.ADAPTER,
            target,
            FetchContext(
                transport=FakeTransport().add(
                    WIDGETS,
                    json_response(fixture_json("phenom", "page_with_jobs.json")),
                    json_response(fixture_json("phenom", "page_empty.json")),
                ),
                resume_from=stale,
            ),
        )
    )
    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]
    assert [r.identity_claims() for r in second] == [r.identity_claims() for r in first]


def test_cross_term_duplicate_is_emitted_not_suppressed():
    """The same job matching two search terms yields twice; the writer dedupes."""
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_with_jobs.json")),  # term0 from=0
        json_response(fixture_json("phenom", "page_empty.json")),  # term0 from=50 -> stop early
        json_response(fixture_json("phenom", "page_duplicate_job.json")),  # term1 from=0
        json_response(fixture_json("phenom", "page_empty.json")),  # term1 from=50 -> stop
    )
    target = _target(terms=("support engineer", "solutions engineer"))
    records = asyncio.run(collect(phenom.ADAPTER, target, FetchContext(transport=transport)))
    req_ids = [r.req_id for r in records]
    assert req_ids.count("30012345") == 2
    dupes = [r for r in records if r.req_id == "30012345"]
    assert dupes[0].content_hash() == dupes[1].content_hash()
    assert dupes[0].identity_claims() == dupes[1].identity_claims()


def test_stale_checkpoint_for_a_changed_target_is_ignored_by_the_adapter():
    stale = Checkpoint(
        source_key="phenom",
        instance_key="seattlechildrens",
        cursor={"term_index": 1, "from": 50},
        config_fingerprint="stale-fingerprint",
        emitted=99,
    )
    transport = FakeTransport().add(
        WIDGETS,
        json_response(fixture_json("phenom", "page_with_jobs.json")),
        json_response(fixture_json("phenom", "page_empty.json")),
    )
    target = _target(terms=("support engineer",))
    asyncio.run(collect(phenom.ADAPTER, target, FetchContext(transport=transport, resume_from=stale)))
    assert transport.requests[0].json_body["from"] == 0
    assert transport.requests[0].json_body["keywords"] == "support engineer"
