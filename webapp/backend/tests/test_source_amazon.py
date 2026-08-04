"""Amazon.jobs adapter: search pagination, per-term checkpoint resume, and the
parsing/transport split, driven by frozen fixtures.

Every parser assertion runs with no transport at all (mirrors
`test_source_greenhouse.py`); the `fetch()` tests add a `FakeTransport` that
branches on request params, since every Amazon page shares one URL and only
the `base_query`/`offset` params distinguish one page from another.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import amazon
from backend.sources.contract import (
    Checkpoint,
    ConfigError,
    FetchContext,
    HttpRequest,
    InventoryScope,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceConfig,
    SourceTarget,
    TransientSourceError,
)
from backend.sources.testing import FakeTransport, collect, drain, fixture_bytes, fixture_json, json_response

PAGE1 = fixture_json("amazon", "page1.json")
PAGE2 = fixture_json("amazon", "page2.json")
EMPTY = fixture_json("amazon", "empty.json")


def _target(terms=("support engineer",), location="California, United States"):
    return SourceTarget(
        source_key="amazon",
        instance_key="",
        label="Amazon",
        params={"search_terms": tuple(terms), "location": location, "company": "Amazon"},
        inventory_scope=InventoryScope.PARTIAL,
        host=amazon.SEARCH_HOST,
    )


def _routed_transport(pages: dict[tuple[str, int], object]) -> FakeTransport:
    """One route for Amazon's single URL, branching on (base_query, offset).

    `FakeTransport` keys routes on the URL with query stripped, and every
    Amazon request hits the same path — so a real page must be selected from
    `request.params`, not from a queued sequence.
    """

    def responder(request: HttpRequest) -> object:
        key = (request.params["base_query"], request.params["offset"])
        body = pages.get(key)
        if body is None:
            raise AssertionError(f"no fixture routed for base_query/offset {key!r}")
        return json_response(body, url=request.url)

    return FakeTransport().add(amazon.SEARCH_URL, responder)


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_search_page_skips_unusable_rows_without_failing_the_page():
    records = list(amazon.parse_search_page(PAGE1, _target()))
    assert len(records) == 3
    assert [r.req_id for r in records] == ["2871234", "2871235", "/jobs/2871236/solutions-engineer"]


def test_parse_search_page_falls_back_to_job_path_when_id_icims_is_blank():
    third = list(amazon.parse_search_page(PAGE1, _target()))[2]
    assert third.req_id == "/jobs/2871236/solutions-engineer"
    assert third.extra["job_path"] == "/jobs/2871236/solutions-engineer"


def test_parse_search_page_joins_city_and_state_and_drops_empty_state():
    first, second, _ = list(amazon.parse_search_page(PAGE1, _target()))
    assert first.location == "Seattle, WA"
    assert second.location == "Bellevue"  # state was "" -> not ", "-joined in


def test_parse_search_page_builds_the_absolute_url():
    first = list(amazon.parse_search_page(PAGE1, _target()))[0]
    assert first.url == "https://www.amazon.jobs/jobs/2871234/support-engineer-ii"
    assert first.company == "Amazon"


def test_parse_search_page_keeps_relative_dates_out_of_the_hash():
    records = list(amazon.parse_search_page(PAGE1, _target()))
    assert records[0].posted_date is None  # "July 20, 2026" is not ISO
    assert records[0].posted_raw == "July 20, 2026"
    assert records[0].canonical_fields()["posted_date"] == ""
    # third row has no posted_date key at all
    assert records[2].posted_raw == ""
    assert records[2].posted_date is None


def test_parse_search_page_namespace_and_identity_claims():
    first = list(amazon.parse_search_page(PAGE1, _target()))[0]
    assert first.namespace == "amazon"  # singleton: no instance_key to append
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == ("source_req", "amazon", "2871234")
    assert claims[1].kind == "url"


def test_parse_search_page_content_hash_is_stable_across_runs():
    once = list(amazon.parse_search_page(PAGE1, _target()))
    twice = list(amazon.parse_search_page(PAGE1, _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]


def test_parse_search_page_rejects_a_malformed_envelope():
    for payload in (b"<html>blocked</html>", b"[]", json.dumps({"jobs": "nope"}).encode()):
        with pytest.raises(PayloadError):
            list(amazon.parse_search_page(payload, _target()))


def test_parse_search_page_accepts_bytes_str_and_mapping():
    raw = fixture_bytes("amazon", "page1.json")
    assert (
        len(list(amazon.parse_search_page(raw, _target())))
        == len(list(amazon.parse_search_page(raw.decode(), _target())))
        == len(list(amazon.parse_search_page(json.loads(raw), _target())))
    )


def test_parse_search_page_on_an_empty_page_yields_nothing():
    assert list(amazon.parse_search_page(EMPTY, _target())) == []


# --------------------------------------------------------------------------- #
# total_hits()
# --------------------------------------------------------------------------- #
def test_total_hits_reads_the_declared_count():
    assert amazon.total_hits(PAGE1) == 120
    assert amazon.total_hits(fixture_bytes("amazon", "page1.json")) == 120


def test_total_hits_defaults_to_zero_when_missing_or_malformed():
    assert amazon.total_hits({"jobs": []}) == 0
    assert amazon.total_hits(b"<html>blocked</html>") == 0
    assert amazon.total_hits([1, 2, 3]) == 0


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_produces_one_singleton_target_from_search_terms():
    config = SourceConfig.from_mapping(
        {
            "profile": {
                "search_terms": ["support engineer", "product support"],
                "employer_scrape_location": "Washington, United States",
            }
        }
    )
    targets = amazon.ADAPTER.plan(config)
    assert len(targets) == 1
    target = targets[0]
    assert target.instance_key == ""
    assert target.label == "Amazon"
    assert target.source_run_key == "amazon"
    assert target.param("search_terms") == ("support engineer", "product support")
    assert target.param("location") == "Washington, United States"
    assert target.inventory_scope is InventoryScope.PARTIAL
    assert target.host == amazon.SEARCH_HOST


def test_plan_falls_back_to_the_default_location():
    config = SourceConfig.from_mapping({"profile": {"search_terms": ["support engineer"]}})
    assert amazon.ADAPTER.plan(config)[0].param("location") == amazon.DEFAULT_LOCATION


def test_plan_without_search_terms_is_empty_not_an_error():
    assert list(amazon.ADAPTER.plan(SourceConfig())) == []
    assert list(amazon.ADAPTER.plan(SourceConfig.from_mapping({"profile": {}}))) == []


def test_descriptor_declares_daily_and_full_direct_and_checkpointing():
    descriptor = amazon.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL


# --------------------------------------------------------------------------- #
# fetch(): pagination within a term
# --------------------------------------------------------------------------- #
def test_fetch_pages_a_single_term_to_exhaustion():
    transport = _routed_transport(
        {
            ("support engineer", 0): PAGE1,
            ("support engineer", 100): PAGE2,
        }
    )
    ctx = FetchContext(transport=transport)
    records = asyncio.run(collect(amazon.ADAPTER, _target(), ctx))
    assert len(records) == 4  # 3 from page1 + 1 from page2
    assert transport.call_count == 2
    assert [r.params["offset"] for r in transport.requests] == [0, 100]
    # the term is exhausted (next_offset 200 >= min(hits=120, 500)) -> done sentinel
    assert ctx.checkpoint.cursor == {"term_index": 1, "offset": 0}
    assert ctx.checkpoint.emitted == 4


def test_fetch_stops_a_term_on_an_empty_page_even_if_hits_claims_more():
    transport = _routed_transport({("support engineer", 0): EMPTY})
    records = asyncio.run(collect(amazon.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 1


def test_fetch_advances_to_the_next_term_after_exhausting_the_previous_one():
    transport = _routed_transport(
        {
            ("support engineer", 0): PAGE1,
            ("support engineer", 100): PAGE2,
            ("product support", 0): EMPTY,
        }
    )
    ctx = FetchContext(transport=transport)
    records = asyncio.run(
        collect(amazon.ADAPTER, _target(terms=("support engineer", "product support")), ctx)
    )
    assert len(records) == 4
    assert transport.call_count == 3
    assert [r.params["base_query"] for r in transport.requests] == [
        "support engineer",
        "support engineer",
        "product support",
    ]
    # both terms exhausted -> term_index has walked past the end
    assert ctx.checkpoint.cursor == {"term_index": 2, "offset": 0}


def test_fetch_marks_a_checkpoint_after_every_page():
    seen_cursors = []
    transport = _routed_transport(
        {
            ("support engineer", 0): PAGE1,
            ("support engineer", 100): PAGE2,
        }
    )

    class _RecordingCtx(FetchContext):
        def mark_checkpoint(self, cursor, *, target, emitted=0):
            checkpoint = super().mark_checkpoint(cursor, target=target, emitted=emitted)
            seen_cursors.append(dict(cursor))
            return checkpoint

    ctx = _RecordingCtx(transport=transport)
    asyncio.run(collect(amazon.ADAPTER, _target(), ctx))
    assert seen_cursors == [{"term_index": 0, "offset": 100}, {"term_index": 1, "offset": 0}]


# --------------------------------------------------------------------------- #
# fetch(): checkpoint resume
# --------------------------------------------------------------------------- #
def test_fetch_resumes_mid_term_and_refetches_no_prior_page():
    target = _target()
    checkpoint = Checkpoint(
        source_key="amazon",
        instance_key="",
        cursor={"term_index": 0, "offset": 100},
        config_fingerprint=target.config_fingerprint(),
        emitted=3,
    )
    transport = _routed_transport({("support engineer", 100): PAGE2})
    ctx = FetchContext(transport=transport, resume_from=checkpoint)
    records = asyncio.run(collect(amazon.ADAPTER, target, ctx))
    assert len(records) == 1
    assert transport.call_count == 1
    assert transport.requests[0].params["offset"] == 100
    assert ctx.checkpoint.emitted == 4  # 3 carried over + 1 newly yielded


def test_fetch_resuming_a_completed_run_is_a_safe_no_op():
    target = _target()
    done = Checkpoint(
        source_key="amazon",
        instance_key="",
        cursor={"term_index": 1, "offset": 0},  # len(terms) == 1: fully done
        config_fingerprint=target.config_fingerprint(),
        emitted=4,
    )
    transport = _routed_transport({})
    records = asyncio.run(collect(amazon.ADAPTER, target, FetchContext(transport=transport, resume_from=done)))
    assert records == []
    assert transport.call_count == 0


def test_fetch_resuming_from_the_start_reproduces_the_same_records():
    """A checkpoint that resumes at (0, 0) is indistinguishable from a clean
    run: replaying it must be safe, per the contract's replayability rule."""
    target = _target()
    fresh = asyncio.run(
        collect(
            amazon.ADAPTER,
            target,
            FetchContext(
                transport=_routed_transport(
                    {("support engineer", 0): PAGE1, ("support engineer", 100): PAGE2}
                )
            ),
        )
    )
    restart = Checkpoint(
        source_key="amazon", instance_key="", cursor={"term_index": 0, "offset": 0},
        config_fingerprint=target.config_fingerprint(), emitted=0,
    )
    replayed = asyncio.run(
        collect(
            amazon.ADAPTER,
            target,
            FetchContext(
                transport=_routed_transport(
                    {("support engineer", 0): PAGE1, ("support engineer", 100): PAGE2}
                ),
                resume_from=restart,
            ),
        )
    )
    assert [r.content_hash() for r in fresh] == [r.content_hash() for r in replayed]


# --------------------------------------------------------------------------- #
# fetch(): transport shell, errors, config
# --------------------------------------------------------------------------- #
def test_fetch_yields_the_same_records_the_pure_parser_does():
    transport = _routed_transport(
        {("support engineer", 0): PAGE1, ("support engineer", 100): PAGE2}
    )
    fetched = asyncio.run(collect(amazon.ADAPTER, _target(), FetchContext(transport=transport)))
    parsed = list(amazon.parse_search_page(PAGE1, _target())) + list(
        amazon.parse_search_page(PAGE2, _target())
    )
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]


def test_fetch_on_a_blocked_search_is_permanent_not_empty():
    transport = FakeTransport().add(amazon.SEARCH_URL, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(amazon.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(amazon.SEARCH_URL, json_response({}, status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(amazon.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_html_interstitial_is_a_payload_error():
    from backend.sources.testing import text_response

    transport = FakeTransport().add(amazon.SEARCH_URL, text_response("<html>Access denied</html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(amazon.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_a_malformed_envelope_is_a_payload_error():
    transport = FakeTransport().add(amazon.SEARCH_URL, json_response({"jobs": "nope"}))
    with pytest.raises(PayloadError):
        asyncio.run(collect(amazon.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_requires_search_terms_on_the_target():
    bare = SourceTarget(source_key="amazon", instance_key="")
    transport = FakeTransport()
    with pytest.raises(ConfigError):
        asyncio.run(collect(amazon.ADAPTER, bare, FetchContext(transport=transport)))
    assert transport.call_count == 0


def test_fetch_a_non_200_never_swallows_and_never_returns_an_empty_list():
    """Contract invariant 3: distinguishable from a genuinely empty term."""
    transport = FakeTransport().add(amazon.SEARCH_URL, json_response({}, status=500))
    with pytest.raises(TransientSourceError):
        list(asyncio.run(drain(amazon.ADAPTER.fetch(_target(), FetchContext(transport=transport)))))
