"""YC adapter: discovered-frontier crawl worked example.

Parser tests run with no transport at all (frozen HTML fixtures). `fetch()`
tests drive `FakeTransport` to exercise the `/jobs` -> role -> location
crawl, cross-path in-run dedupe, checkpoint round trips, resume, and
replay-safety, mirroring the paginated-adapter contract tests in
`test_source_contract.py` and `test_source_smartrecruiters.py`.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import yc
from backend.sources.contract import (
    Checkpoint,
    FetchContext,
    InventoryScope,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceConfig,
    SourceTarget,
    TransientSourceError,
)
from backend.sources.testing import FakeTransport, collect, fixture_bytes, text_response

ROOT = yc.page_url("/jobs")
ROLE_ENGINEERING = yc.page_url("/jobs/role/engineering")
LOCATION_SF = yc.page_url("/jobs/location/san-francisco")


def _target():
    return SourceTarget(
        source_key="yc",
        instance_key="",
        label="Y Combinator Jobs",
        params={},
        inventory_scope=InventoryScope.COMPLETE,
        host=yc.SITE_HOST,
    )


def _root_html():
    return fixture_bytes("yc", "root.html")


def _role_html():
    return fixture_bytes("yc", "role_engineering.html")


def _location_html():
    return fixture_bytes("yc", "location_san_francisco.html")


def _transport_for_full_crawl():
    return (
        FakeTransport()
        .add(ROOT, text_response(_root_html().decode()))
        .add(ROLE_ENGINEERING, text_response(_role_html().decode()))
        .add(LOCATION_SF, text_response(_location_html().decode()))
    )


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_page_from_frozen_root_fixture():
    records, discovered = yc.parse_page(_root_html(), _target())
    records = list(records)
    assert [r.req_id for r in records] == ["100001", "100002", None]
    assert all(r.source_key == "yc" and r.instance_key == "" for r in records)
    assert discovered == ("/jobs/role/engineering", "/jobs/location/san-francisco")


def test_parse_page_namespaces_on_source_key_alone():
    records, _ = yc.parse_page(_root_html(), _target())
    first = list(records)[0]
    assert first.namespace == "yc"


def test_parse_page_infers_company_from_companies_url_slug():
    records, _ = yc.parse_page(_root_html(), _target())
    first = list(records)[0]
    assert first.company == "Exampleco"
    assert first.url == "https://www.ycombinator.com/companies/exampleco/jobs/100001-founding-engineer"


def test_parse_page_records_apply_url_as_an_alt_url():
    records, _ = yc.parse_page(_root_html(), _target())
    first = list(records)[0]
    assert first.alt_urls == ("https://apply.exampleco.com/100001",)


def test_parse_page_blank_apply_url_yields_no_alt_urls():
    records, _ = yc.parse_page(_root_html(), _target())
    second = list(records)[1]
    assert second.title == "Remote Support Engineer"
    # `url` is present and `applyUrl` is blank: company is still inferred
    # from `url`, and there is nothing distinct left over for `alt_urls`.
    assert second.company == "Anotherco"
    assert second.alt_urls == ()


def test_parse_page_falls_back_to_apply_url_when_url_is_blank():
    """The inverse of the above: no `url` at all, only `applyUrl`.

    `_company_from_url` is matched against the (absent) `url` field per
    legacy behaviour, so company inference degrades to the `"YC startup"`
    fallback even though the posting is still fully identifiable by URL.
    """
    payload = json.dumps(
        {
            "props": {
                "jobPostings": [
                    {
                        "id": 900001,
                        "title": "Apply-Only Listing",
                        "url": "",
                        "applyUrl": "https://apply.somestartup.com/900001",
                        "location": "Remote",
                    }
                ]
            }
        }
    ).replace('"', "&quot;")
    html_text = f'<div data-page="{payload}"></div>'
    records, _ = yc.parse_page(html_text, _target())
    only = list(records)[0]
    assert only.url == "https://apply.somestartup.com/900001"
    assert only.company == "YC startup"
    assert only.alt_urls == ()


def test_parse_page_marks_remote_from_location_substring():
    records, _ = yc.parse_page(_root_html(), _target())
    records = list(records)
    first, second = records[0], records[1]
    assert first.location == "San Francisco, CA"
    assert first.remote is False
    assert second.location == "Remote"
    assert second.remote is True


def test_parse_page_skips_row_with_blank_title():
    records, _ = yc.parse_page(_root_html(), _target())
    ids = [r.req_id for r in records]
    assert "100003" not in ids


def test_parse_page_skips_row_with_no_usable_url():
    records, _ = yc.parse_page(_root_html(), _target())
    ids = [r.req_id for r in records]
    assert "100004" not in ids


def test_parse_page_missing_id_keeps_the_row_with_no_req_id():
    """A posting with no `id` cannot carry a req_id claim, but URL identity
    still stands: it must not be dropped outright."""
    records, _ = yc.parse_page(_root_html(), _target())
    records = list(records)
    unidentified = [r for r in records if r.title == "Unidentified Role Posting"][0]
    assert unidentified.req_id is None
    assert unidentified.company == "Noidco"
    claims = unidentified.identity_claims()
    assert len(claims) == 1
    assert claims[0].kind == "url"


def test_parse_page_company_defaults_to_yc_startup_without_a_companies_url():
    from backend.sources.contract import SourceTarget as _T

    payload = json.dumps(
        {
            "props": {
                "jobPostings": [
                    {"id": 1, "title": "Mystery Role", "url": "/careers/mystery-role", "location": "Remote"}
                ]
            }
        }
    )
    html_text = f'<div data-page="{payload.replace(chr(34), "&quot;")}"></div>'
    records, _ = yc.parse_page(html_text, _target())
    only = list(records)[0]
    assert only.company == "YC startup"


def test_parse_page_defensive_when_facets_are_absent():
    records, discovered = yc.parse_page(_location_html(), _target())
    assert discovered == ()
    assert [r.req_id for r in records] == ["100001", "100005", "100006"]


def test_parse_page_identity_prefers_the_posting_id():
    records, _ = yc.parse_page(_root_html(), _target())
    first = list(records)[0]
    claims = first.identity_claims()
    assert (claims[0].kind, claims[0].namespace, claims[0].value) == ("source_req", "yc", "100001")
    assert claims[1].kind == "url"


def test_parse_page_content_hash_is_stable_across_runs():
    once = list(yc.parse_page(_root_html(), _target())[0])
    twice = list(yc.parse_page(_root_html(), _target())[0])
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]


def test_parse_page_rejects_a_missing_data_page_attribute():
    with pytest.raises(PayloadError):
        list(yc.parse_page(b"<html><body>no data here</body></html>", _target())[0])


def test_parse_page_rejects_unparseable_json_in_data_page():
    with pytest.raises(PayloadError):
        list(yc.parse_page(b'<div data-page="not json at all"></div>', _target())[0])


def test_parse_page_rejects_a_non_list_jobpostings():
    payload = json.dumps({"props": {"jobPostings": "nope"}}).replace('"', "&quot;")
    with pytest.raises(PayloadError):
        list(yc.parse_page(f'<div data-page="{payload}"></div>', _target())[0])


def test_parse_page_accepts_bytes_and_str():
    raw = _root_html()
    a = list(yc.parse_page(raw, _target())[0])
    b = list(yc.parse_page(raw.decode(), _target())[0])
    assert [r.to_json_dict() for r in a] == [r.to_json_dict() for r in b]


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_always_returns_exactly_one_singleton_target():
    targets = yc.ADAPTER.plan(SourceConfig())
    assert len(targets) == 1
    target = targets[0]
    assert target.source_key == "yc"
    assert target.instance_key == ""
    assert target.namespace == "yc"
    assert target.host == yc.SITE_HOST
    assert target.inventory_scope is InventoryScope.COMPLETE


def test_plan_is_config_independent():
    """Unlike the company-map adapters, an empty config still plans the board."""
    empty = yc.ADAPTER.plan(SourceConfig())
    configured = yc.ADAPTER.plan(SourceConfig.from_mapping({"companies": {"greenhouse": {"x": "X"}}}))
    assert len(empty) == len(configured) == 1


def test_descriptor_declares_daily_full_direct_and_checkpoint_support():
    descriptor = yc.DESCRIPTOR
    assert descriptor.runs_in(RunKind.DAILY)
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.AGGREGATORS)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.COMPLETE


# --------------------------------------------------------------------------- #
# fetch(): frontier crawl, cross-path dedupe
# --------------------------------------------------------------------------- #
def test_fetch_crawls_root_then_discovered_role_and_location_paths():
    transport = _transport_for_full_crawl()
    records = asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.urls() == [ROOT, ROLE_ENGINEERING, LOCATION_SF]
    # 100001 appears on all three pages, 100005 on two: each surfaces once.
    ids = [r.req_id for r in records]
    assert ids.count("100001") == 1
    assert ids.count("100005") == 1
    assert set(ids) >= {"100001", "100002", "100005", "100006"}
    assert None in ids  # the unidentified-id posting from root


def test_fetch_cross_path_dedupe_keeps_first_occurrence_only():
    transport = _transport_for_full_crawl()
    records = asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=transport)))
    founding_engineer = [r for r in records if r.req_id == "100001"]
    assert len(founding_engineer) == 1


def test_fetch_on_a_blocked_site_is_permanent_not_empty():
    transport = FakeTransport().add(ROOT, text_response("blocked", status=403))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(ROOT, text_response("", status=status))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_a_page_with_no_data_page_attribute_is_a_payload_error():
    transport = FakeTransport().add(ROOT, text_response("<html><body>nope</body></html>"))
    with pytest.raises(PayloadError):
        asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_an_empty_board_yields_nothing_and_does_not_raise():
    empty_page = (
        '<div data-page="{&quot;props&quot;: {&quot;jobPostings&quot;: []}}"></div>'
    )
    transport = FakeTransport().add(ROOT, text_response(empty_page))
    records = asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 1


def test_fetch_yields_the_same_records_the_pure_parser_does():
    transport = _transport_for_full_crawl()
    fetched = asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=transport)))

    root_records, _ = yc.parse_page(_root_html(), _target())
    role_records, _ = yc.parse_page(_role_html(), _target())
    location_records, _ = yc.parse_page(_location_html(), _target())
    seen: set[str] = set()
    expected = []
    for record in list(root_records) + list(role_records) + list(location_records):
        key = record.req_id or record.url_key
        if key in seen:
            continue
        seen.add(key)
        expected.append(record)
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in expected]


# -- checkpoints -------------------------------------------------------- #
def test_mid_crawl_failure_keeps_earlier_records_and_last_checkpoint():
    transport = (
        FakeTransport()
        .add(ROOT, text_response(_root_html().decode()))
        .add(ROLE_ENGINEERING, text_response("", status=503))
    )
    target = _target()
    ctx = FetchContext(transport=transport)

    async def scenario():
        seen = []
        with pytest.raises(TransientSourceError):
            async for record in yc.ADAPTER.fetch(target, ctx):
                seen.append(record)
        return seen

    seen = asyncio.run(scenario())
    assert [r.req_id for r in seen] == ["100001", "100002", None]
    assert ctx.checkpoint is not None
    assert ctx.checkpoint.cursor["path_index"] == 1
    assert ctx.checkpoint.cursor["paths"] == ["/jobs", "/jobs/role/engineering", "/jobs/location/san-francisco"]
    assert ctx.checkpoint.emitted == 3
    assert ctx.checkpoint.is_valid_for(target)


def test_resume_from_checkpoint_continues_at_the_saved_path_without_refetching_root():
    target = _target()
    resume = Checkpoint(
        source_key="yc",
        instance_key="",
        cursor={"path_index": 1, "paths": ["/jobs", "/jobs/role/engineering", "/jobs/location/san-francisco"]},
        config_fingerprint=target.config_fingerprint(),
        emitted=3,
    )
    transport = (
        FakeTransport()
        .add(ROLE_ENGINEERING, text_response(_role_html().decode()))
        .add(LOCATION_SF, text_response(_location_html().decode()))
    )
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(yc.ADAPTER, target, ctx))
    assert transport.urls() == [ROLE_ENGINEERING, LOCATION_SF]
    # 100001 was already emitted before the checkpoint in the "real" first
    # attempt; resuming past root, this fresh in-run `seen` set has not seen
    # it yet, so it re-surfaces here. That is expected: the writer, not the
    # adapter, is what dedupes across a resumed attempt (invariant 5).
    ids = [r.req_id for r in records]
    assert ids.count("100001") == 1
    assert "100006" in ids
    assert ctx.checkpoint.cursor["path_index"] == 3


def test_replayed_checkpoint_reemits_identical_records():
    """Invariant 5: replay is expected, and the writer dedupes on identity."""
    first = asyncio.run(collect(yc.ADAPTER, _target(), FetchContext(transport=_transport_for_full_crawl())))
    target = _target()
    stale = Checkpoint(
        source_key="yc",
        instance_key="",
        cursor={"path_index": 0, "paths": ["/jobs"]},
        config_fingerprint=target.config_fingerprint(),
        emitted=0,
    )
    second = asyncio.run(
        collect(
            yc.ADAPTER,
            target,
            FetchContext(transport=_transport_for_full_crawl(), resume_from=stale),
        )
    )
    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]


def test_stale_checkpoint_for_a_changed_target_is_ignored():
    target = _target()
    stale = Checkpoint(
        source_key="yc",
        instance_key="",
        cursor={"path_index": 2, "paths": ["/jobs", "/jobs/role/engineering", "/jobs/location/san-francisco"]},
        config_fingerprint="stale-fingerprint",
        emitted=99,
    )
    transport = _transport_for_full_crawl()
    asyncio.run(collect(yc.ADAPTER, target, FetchContext(transport=transport, resume_from=stale)))
    assert transport.urls()[0] == ROOT
