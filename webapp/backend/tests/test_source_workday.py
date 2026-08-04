"""Workday CXS adapter: search-driven pagination and checkpoint resume.

Every parser assertion runs with no transport (fixture-driven, pure), and the
transport-shell assertions run against `FakeTransport` with no live network,
matching the Greenhouse reference suite's structure.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import workday
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

HOST = "tmobile.wd1.myworkdayjobs.com"
TENANT = "tmobile"
SITE = "External"
WORKDAY_URL = workday.jobs_url(HOST, TENANT, SITE)


def _target(
    key="tmobile",
    *,
    name="T-Mobile",
    host=HOST,
    tenant=TENANT,
    site=SITE,
    search_terms=("support engineer",),
):
    return SourceTarget(
        source_key="workday",
        instance_key=key,
        label=name,
        params={
            "host": host,
            "tenant": tenant,
            "site": site,
            "company": name,
            "search_terms": tuple(search_terms),
        },
        inventory_scope=InventoryScope.PARTIAL,
        host=host,
    )


def _fixture(name):
    return fixture_json("workday", name)


# --------------------------------------------------------------------------- #
# build_queries()
# --------------------------------------------------------------------------- #
def test_build_queries_preserves_the_washington_variant_and_order():
    assert workday.build_queries(["support engineer", "product support"]) == (
        "support engineer washington",
        "support engineer",
        "product support washington",
        "product support",
    )


def test_build_queries_drops_blank_terms_and_is_empty_for_none():
    assert workday.build_queries(["", "support engineer", ""]) == (
        "support engineer washington",
        "support engineer",
    )
    assert workday.build_queries([]) == ()


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_page_from_frozen_fixture():
    records = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))
    assert [r.req_id for r in records] == ["R-1001", None]
    assert all(r.source_key == "workday" and r.instance_key == "tmobile" for r in records)
    assert all(r.company == "T-Mobile" for r in records)


def test_parse_page_skips_row_missing_title_but_keeps_the_rest():
    records = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))
    assert len(records) == 2
    assert "R-1003" not in [r.req_id for r in records]  # blank title, unusable


def test_parse_page_skips_row_missing_external_path():
    payload = {
        "total": 1,
        "jobPostings": [{"title": "No Path", "bulletFields": ["R-9"]}],
    }
    assert list(workday.parse_page(payload, _target())) == []


def test_parse_page_missing_optional_fields_default_to_empty():
    second = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))[1]
    assert second.location == ""  # locationsText absent
    assert second.req_id is None  # bulletFields present but empty


def test_parse_page_builds_the_apply_url_from_host_and_site():
    first = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))[0]
    assert first.url == f"https://{HOST}/en-US/{SITE}/job/Remote/Support-Engineer-II_R-1001"
    assert first.namespace == "workday:tmobile"


def test_parse_page_relative_posted_on_normalizes_to_none():
    first = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))[0]
    assert first.posted_date is None
    assert first.posted_raw == "Posted 3 Days Ago"
    assert first.canonical_fields()["posted_date"] == ""


def test_parse_page_absolute_posted_on_would_normalize_if_ever_present():
    """Belt-and-suspenders: normalize_date itself handles an absolute date;
    Workday just never sends one (see module docstring)."""
    payload = {
        "total": 1,
        "jobPostings": [
            {
                "title": "Edge Case",
                "externalPath": "/job/x/Edge-Case_R-1",
                "postedOn": "2026-07-01T00:00:00-07:00",
                "bulletFields": ["R-1"],
            }
        ],
    }
    record = next(iter(workday.parse_page(payload, _target())))
    assert record.posted_date == "2026-07-01"


def test_parse_page_identity_prefers_req_id_then_url():
    first = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == (
        "source_req",
        "workday:tmobile",
        "R-1001",
    )
    assert claims[1].kind == "url"

    second = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))[1]
    assert len(second.identity_claims()) == 1  # no req_id: degrades to url only
    assert second.identity_claims()[0].kind == "url"


def test_parse_page_content_hash_is_stable_and_namespace_scoped():
    once = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))
    twice = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]
    other = list(workday.parse_page(fixture_bytes("workday", "page1.json"), _target(key="boeing", name="Boeing")))
    assert [r.content_hash() for r in other] != [r.content_hash() for r in once]


def test_parse_page_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"jobPostings": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(workday.parse_page(payload, _target()))


def test_parse_page_accepts_bytes_str_and_mapping():
    raw = fixture_bytes("workday", "page1.json")
    assert (
        len(list(workday.parse_page(raw, _target())))
        == len(list(workday.parse_page(raw.decode(), _target())))
        == len(list(workday.parse_page(json.loads(raw), _target())))
    )


def test_parse_page_ignores_non_mapping_rows():
    payload = {"total": 1, "jobPostings": ["not-a-job-object"]}
    assert list(workday.parse_page(payload, _target())) == []


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_config_and_embeds_search_terms():
    config = SourceConfig.from_mapping(
        {
            "profile": {"search_terms": ["support engineer", "product support"]},
            "companies": {
                "workday": {
                    "tmobile": {"host": HOST, "tenant": TENANT, "site": SITE, "name": "T-Mobile"},
                    "": {"host": "junk", "tenant": "junk", "site": "junk", "name": "junk"},
                }
            },
        }
    )
    targets = workday.ADAPTER.plan(config)
    assert [t.instance_key for t in targets] == ["tmobile"]
    assert targets[0].label == "T-Mobile"
    assert targets[0].param("host") == HOST
    assert targets[0].param("search_terms") == ("support engineer", "product support")
    assert targets[0].inventory_scope is InventoryScope.PARTIAL
    assert targets[0].host == HOST


def test_plan_defaults_name_to_the_config_key_when_absent():
    config = SourceConfig.from_mapping(
        {"companies": {"workday": {"boeing": {"host": HOST, "tenant": TENANT, "site": SITE}}}}
    )
    targets = workday.ADAPTER.plan(config)
    assert targets[0].label == "boeing"
    assert targets[0].param("company") == "boeing"


def test_plan_without_workday_config_is_empty_not_an_error():
    assert list(workday.ADAPTER.plan(SourceConfig())) == []


def test_plan_rejects_a_non_object_entry():
    config = SourceConfig.from_mapping({"companies": {"workday": {"tmobile": ["not", "an", "object"]}}})
    with pytest.raises(ConfigError):
        workday.ADAPTER.plan(config)


def test_descriptor_declares_daily_and_full_direct_with_checkpointing():
    descriptor = workday.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL


# --------------------------------------------------------------------------- #
# fetch(): pagination, query fan-out, and dedupe
# --------------------------------------------------------------------------- #
def _fan_out_responder():
    page1 = _fixture("page1.json")
    page2 = _fixture("page2.json")
    dup = _fixture("variant_duplicate.json")

    def responder(request):
        body = request.json_body
        query, offset = body["searchText"], body["offset"]
        if query == "support engineer washington":
            if offset == 0:
                return json_response(page1, url=WORKDAY_URL)
            if offset == 20:
                return json_response(page2, url=WORKDAY_URL)
            raise AssertionError(f"unexpected washington-variant offset {offset}")
        if query == "support engineer":
            if offset == 0:
                return json_response(dup, url=WORKDAY_URL)
            raise AssertionError(f"unexpected bare-variant offset {offset}")
        raise AssertionError(f"unexpected query {query!r}")

    return responder


def test_fetch_sends_the_washington_variant_before_the_bare_term():
    transport = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))
    bodies = [r.json_body["searchText"] for r in transport.requests]
    assert bodies[0] == "support engineer washington"
    assert "support engineer" in bodies[1:]


def test_fetch_paginates_within_a_query_until_total_is_reached():
    transport = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    records = asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["R-1001", None, "R-1004"]
    assert transport.call_count == 3  # washington@0, washington@20, bare@0


def test_fetch_dedupes_the_second_query_variant_within_the_run():
    """`variant_duplicate.json` repeats page1's two postings; they must not
    be yielded twice even though nothing forbids the writer from deduping
    them itself (invariant 5 makes this an efficiency measure, not a
    correctness one)."""
    transport = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    records = asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))
    paths = [r.extra["external_path"] for r in records]
    assert len(paths) == len(set(paths))


def test_fetch_yields_streaming_not_batched():
    """Success Contract: the first record is available before every page of
    every query has been requested."""
    transport = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    ctx = FetchContext(transport=transport)

    async def scenario():
        stream = workday.ADAPTER.fetch(_target(), ctx)
        first = await stream.__anext__()
        assert transport.call_count == 1
        rest = [r async for r in stream]
        return first, rest

    first, rest = asyncio.run(scenario())
    assert first.req_id == "R-1001"
    assert len(rest) == 2


def test_fetch_final_checkpoint_points_past_the_last_query():
    transport = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    ctx = FetchContext(transport=transport)
    records = asyncio.run(collect(workday.ADAPTER, _target(), ctx))
    assert ctx.checkpoint.cursor == {"query_index": 2, "offset": 0}
    assert ctx.checkpoint.emitted == len(records) == 3
    assert ctx.checkpoint.is_valid_for(_target())


def test_fetch_resumes_from_checkpoint_mid_query_without_refetching_page_one():
    """A checkpoint saved after washington@0 must resume at washington@20,
    never re-requesting offset 0.

    The bare-variant page reused here (`variant_duplicate.json`) happens to
    repeat page1's postings, and this resumed attempt legitimately re-emits
    them: in-run dedup is scoped to a single `fetch()` call (invariant 5), a
    fresh call has no memory of the crashed attempt's page1, and it is the
    identity-keyed writer -- not the adapter -- that is responsible for
    deduping across attempts.
    """
    page2 = _fixture("page2.json")
    dup = _fixture("variant_duplicate.json")

    def strict_responder(request):
        body = request.json_body
        query, offset = body["searchText"], body["offset"]
        if query == "support engineer washington" and offset == 20:
            return json_response(page2, url=WORKDAY_URL)
        if query == "support engineer" and offset == 0:
            return json_response(dup, url=WORKDAY_URL)
        raise AssertionError(f"unexpected resumed request: {query!r} offset={offset}")

    target = _target()
    resume = Checkpoint(
        source_key="workday",
        instance_key="tmobile",
        cursor={"query_index": 0, "offset": 20},
        config_fingerprint=target.config_fingerprint(),
        emitted=2,
    )
    transport = FakeTransport().add(WORKDAY_URL, strict_responder)
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(workday.ADAPTER, target, ctx))
    assert [r.req_id for r in records] == ["R-1004", "R-1001", None]
    assert ctx.checkpoint.cursor == {"query_index": 2, "offset": 0}
    assert ctx.checkpoint.emitted == 5


def test_replayed_checkpoint_re_emits_identical_records():
    """Invariant 5: replay is expected, and the writer dedupes on identity."""
    transport1 = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    first = asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport1)))

    target = _target()
    stale = Checkpoint(
        source_key="workday",
        instance_key="tmobile",
        cursor={"query_index": 0, "offset": 0},
        config_fingerprint=target.config_fingerprint(),
        emitted=0,
    )
    transport2 = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    second = asyncio.run(
        collect(workday.ADAPTER, target, FetchContext(transport=transport2, resume_from=stale))
    )
    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]


def test_stale_checkpoint_for_a_changed_target_is_ignored():
    """A checkpoint minted under different search terms has a different
    `config_fingerprint` and must not be honoured."""
    target = _target(search_terms=("support engineer",))
    stale = Checkpoint(
        source_key="workday",
        instance_key="tmobile",
        cursor={"query_index": 1, "offset": 20},
        config_fingerprint="stale-fingerprint-from-a-different-query-set",
        emitted=99,
    )
    transport = FakeTransport().add(WORKDAY_URL, _fan_out_responder())
    ctx = FetchContext(transport=transport, resume_from=stale)
    records = asyncio.run(collect(workday.ADAPTER, target, ctx))
    # Started clean, from washington@0, not from the stale cursor.
    assert transport.requests[0].json_body == {
        "appliedFacets": {},
        "limit": 20,
        "offset": 0,
        "searchText": "support engineer washington",
    }
    assert [r.req_id for r in records] == ["R-1001", None, "R-1004"]


def test_fetch_caps_pagination_at_max_pages_per_query():
    """A query whose `total` is never reached still stops after 5 pages and
    moves on to the next query, matching `scraper.py`'s `max_pages=5`."""

    def endless_page(offset):
        return {
            "total": 10_000,
            "jobPostings": [
                {
                    "title": f"Role {offset}",
                    "externalPath": f"/job/x/Role_{offset}",
                    "bulletFields": [f"R-{offset}"],
                }
            ],
        }

    def responder(request):
        body = request.json_body
        query, offset = body["searchText"], body["offset"]
        if query == "support engineer washington":
            return json_response(endless_page(offset), url=WORKDAY_URL)
        if query == "support engineer":
            return json_response({"total": 0, "jobPostings": []}, url=WORKDAY_URL)
        raise AssertionError(f"unexpected query {query!r}")

    transport = FakeTransport().add(WORKDAY_URL, responder)
    records = asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))
    assert len(records) == workday.MAX_PAGES  # 5 pages of 1 unique record each
    assert transport.call_count == workday.MAX_PAGES + 1  # + one empty bare-variant call


def test_fetch_on_a_query_with_no_results_yields_nothing_for_it():
    def responder(request):
        return json_response({"total": 0, "jobPostings": []}, url=WORKDAY_URL)

    transport = FakeTransport().add(WORKDAY_URL, responder)
    records = asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 2  # one request per variant, no pagination


# --------------------------------------------------------------------------- #
# fetch(): classified errors
# --------------------------------------------------------------------------- #
def test_fetch_on_a_missing_tenant_is_permanent_not_empty():
    transport = FakeTransport().add(WORKDAY_URL, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(WORKDAY_URL, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    from backend.sources.testing import text_response

    transport = FakeTransport().add(WORKDAY_URL, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_does_not_retry_a_failed_request():
    """Invariant 1: one failure is one request."""
    transport = FakeTransport().add(WORKDAY_URL, json_response({}, status=503))
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(workday.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_requires_host_tenant_and_site():
    transport = FakeTransport().add(WORKDAY_URL, json_response({"jobPostings": []}))
    for missing in ("host", "tenant", "site"):
        params = {"host": HOST, "tenant": TENANT, "site": SITE, "search_terms": ("support engineer",)}
        params[missing] = ""
        bare = SourceTarget(source_key="workday", instance_key="tmobile", params=params)
        with pytest.raises(PermanentSourceError):
            asyncio.run(collect(workday.ADAPTER, bare, FetchContext(transport=transport)))


def test_fetch_requires_search_terms():
    bare = SourceTarget(
        source_key="workday",
        instance_key="tmobile",
        params={"host": HOST, "tenant": TENANT, "site": SITE, "search_terms": ()},
    )
    transport = FakeTransport().add(WORKDAY_URL, json_response({"jobPostings": []}))
    with pytest.raises(ConfigError):
        asyncio.run(collect(workday.ADAPTER, bare, FetchContext(transport=transport)))
