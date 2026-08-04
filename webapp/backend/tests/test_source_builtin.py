"""Built In HTML-scraping adapter: parsing/transport split, driven by frozen
fixtures.

Unlike the JSON-API adapters, there is no envelope shape to validate the way
`{"jobs": [...]}` is validated elsewhere -- `parse_listing_page` walks raw
HTML text with regexes, so "malformed" here means "not a page at all" rather
than "not the expected keys". Every assertion below runs with no live
network, per contract invariant 7.
"""
import asyncio

import pytest

from backend.sources.adapters import builtin
from backend.sources.contract import (
    Checkpoint,
    FetchContext,
    InventoryScope,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceCategory,
    SourceConfig,
    SourceTarget,
    TransientSourceError,
)
from backend.sources.testing import FakeTransport, collect, fixture_bytes, text_response

LISTINGS_URL = builtin.listings_url()
SF_METRO = builtin.LOCALES[0]
REMOTE_US = builtin.LOCALES[1]


def _target(terms=("support engineer",), instance="", label="Built In"):
    return SourceTarget(
        source_key="builtin",
        instance_key=instance,
        label=label,
        params={"search_terms": tuple(terms)},
        inventory_scope=InventoryScope.PARTIAL,
        host=builtin.API_HOST,
    )


def _page1() -> bytes:
    return fixture_bytes("builtin", "listing_page1.html")


def _empty_page() -> bytes:
    return fixture_bytes("builtin", "listing_empty.html")


# --------------------------------------------------------------------------- #
# Pure parsing (no transport)
# --------------------------------------------------------------------------- #
def test_parse_listing_page_from_frozen_fixture():
    records = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert [r.title for r in records] == ["Support Specialist", "Senior Support Engineer"]
    assert all(r.source_key == "builtin" for r in records)


def test_parse_listing_page_builds_the_full_site_url():
    records = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert records[1].url == "https://builtin.com/job/senior-support-engineer-acme-robotics-4471200"


def test_parse_listing_page_finds_the_nearest_preceding_company_anchor():
    records = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert records[1].company == "Acme Robotics"


def test_parse_listing_page_missing_company_and_salary_are_empty_not_an_error():
    """The first card has no `/company/` anchor before it anywhere in the
    document, and no dollar-figure salary badge nearby -- both must degrade
    to empty strings rather than raising."""
    records = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    first = records[0]
    assert first.company == ""
    assert first.salary_text == ""


def test_parse_listing_page_extracts_the_salary_window():
    records = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert records[1].salary_text == "$120K - $150K"


def test_parse_listing_page_skips_a_row_whose_title_is_blank_after_decoding():
    """The third card's link text is a bare `&nbsp;` entity; once decoded and
    whitespace-collapsed it is empty and cannot be identified, so it must not
    appear -- and must not blank the rest of the page (invariant 3)."""
    records = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert len(records) == 2
    assert "ghost-listing" not in " ".join(r.url for r in records)


def test_parse_listing_page_remote_flag_uses_locale_default_with_no_content_signal():
    """The second card ("Senior Support Engineer") has no work-mode badge
    nearby, so its remote flag is exactly the locale's default."""
    sf = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert sf[1].remote is False
    remote_us = list(builtin.parse_listing_page(_page1(), _target(), locale=REMOTE_US))
    assert remote_us[1].remote is True


def test_parse_listing_page_remote_flag_flips_from_content_even_off_the_remote_locale():
    """The first card ("Support Specialist") carries a "Remote" badge in its
    own chunk with no "Hybrid" nearby, so it is remote even under the
    non-remote locale."""
    sf = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert sf[0].remote is True


def test_parse_listing_page_stamps_the_locale_label_as_location():
    records = list(builtin.parse_listing_page(_page1(), _target(), locale=REMOTE_US))
    assert all(r.location == "Remote, US" for r in records)


def test_parse_listing_page_has_no_req_id_and_degrades_to_url_identity():
    """Per the module's documented identity decision: the only per-job handle
    is the URL path itself, so `req_id` stays `None` and `identity_claims()`
    carries only the rank-1 URL claim."""
    record = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))[1]
    assert record.req_id is None
    claims = record.identity_claims()
    assert len(claims) == 1
    assert claims[0].kind == "url"


def test_parse_listing_page_content_hash_is_stable_across_runs():
    once = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    twice = list(builtin.parse_listing_page(_page1(), _target(), locale=SF_METRO))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]


def test_parse_listing_page_on_a_genuinely_empty_result_set_yields_nothing_not_an_error():
    """A real Built In results page with zero matches is a positive assertion
    that the search is empty, not a failure (invariant 3)."""
    assert list(builtin.parse_listing_page(_empty_page(), _target(), locale=SF_METRO)) == []


def test_parse_listing_page_rejects_a_non_text_payload():
    with pytest.raises(PayloadError):
        list(builtin.parse_listing_page(12345, _target(), locale=SF_METRO))


def test_parse_listing_page_rejects_a_body_that_is_not_html():
    for payload in (b'{"error": "blocked"}', "", "plain text, no markup here"):
        with pytest.raises(PayloadError):
            list(builtin.parse_listing_page(payload, _target(), locale=SF_METRO))


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_expands_search_terms_into_one_singleton_target():
    config = SourceConfig.from_mapping(
        {"profile": {"search_terms": ["support engineer", "solutions engineer"]}}
    )
    targets = builtin.ADAPTER.plan(config)
    assert len(targets) == 1
    target = targets[0]
    assert target.instance_key == ""
    assert target.source_run_key == "builtin"
    assert target.param("search_terms") == ("support engineer", "solutions engineer")
    assert target.inventory_scope is InventoryScope.PARTIAL
    assert target.host == builtin.API_HOST


def test_plan_without_search_terms_is_empty_not_an_error():
    assert list(builtin.ADAPTER.plan(SourceConfig())) == []


def test_descriptor_declares_daily_partial_and_checkpointable():
    descriptor = builtin.DESCRIPTOR
    assert descriptor.category is SourceCategory.STARTUP_BOARD
    assert descriptor.runs_in(RunKind.DAILY)
    assert not descriptor.runs_in(RunKind.FULL_DIRECT)
    assert descriptor.supports_checkpoint is True
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL


# --------------------------------------------------------------------------- #
# fetch(): locale x term x page walk
# --------------------------------------------------------------------------- #
def _paged_responder(*, fail_on_page: int | None = None, fail_status: int = 500):
    """First page of every (locale, term) returns the 2-card fixture; the
    second page always comes back empty, ending that pair's walk exactly like
    `scraper.py`'s `if not cards: break`."""

    def responder(request):
        page = int(request.params["page"])
        if fail_on_page is not None and page == fail_on_page:
            return text_response("", status=fail_status, url=LISTINGS_URL)
        if page == 1:
            return text_response(_page1().decode(), url=LISTINGS_URL)
        return text_response(_empty_page().decode(), url=LISTINGS_URL)

    return responder


def test_fetch_walks_both_locales_streaming_records_and_stops_each_term_on_an_empty_page():
    transport = FakeTransport().add(LISTINGS_URL, _paged_responder())
    target = _target(terms=("support engineer",))
    records = asyncio.run(collect(builtin.ADAPTER, target, FetchContext(transport=transport)))

    # 2 records per locale x 2 locales; page 3/4 never requested because page
    # 2 already came back empty for both locales.
    assert len(records) == 4
    assert transport.call_count == 4
    assert [r.location for r in records] == [
        "San Francisco, CA (metro)",
        "San Francisco, CA (metro)",
        "Remote, US",
        "Remote, US",
    ]


def test_fetch_sends_the_locale_and_term_params_on_every_page():
    transport = FakeTransport().add(LISTINGS_URL, _paged_responder())
    target = _target(terms=("support engineer",))
    asyncio.run(collect(builtin.ADAPTER, target, FetchContext(transport=transport)))
    first_request = transport.requests[0]
    assert first_request.params["search"] == "support engineer"
    assert first_request.params["page"] == 1
    assert first_request.params["city"] == "San Francisco"


def test_fetch_on_a_blocked_response_is_classified_not_swallowed():
    transport = FakeTransport().add(LISTINGS_URL, text_response("", status=404, url=LISTINGS_URL))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(builtin.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_throttling_is_transient():
    for status in (429, 503):
        transport = FakeTransport().add(LISTINGS_URL, text_response("", status=status, url=LISTINGS_URL))
        with pytest.raises(TransientSourceError):
            asyncio.run(collect(builtin.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_on_a_non_html_body_is_a_payload_error():
    transport = FakeTransport().add(LISTINGS_URL, text_response('{"blocked": true}', url=LISTINGS_URL))
    with pytest.raises(PayloadError):
        asyncio.run(collect(builtin.ADAPTER, _target(), FetchContext(transport=transport)))


def test_fetch_without_search_terms_yields_nothing_and_makes_no_request():
    transport = FakeTransport().add(LISTINGS_URL, _paged_responder())
    bare = SourceTarget(source_key="builtin", instance_key="", params={})
    records = asyncio.run(collect(builtin.ADAPTER, bare, FetchContext(transport=transport)))
    assert records == []
    assert transport.call_count == 0


# --------------------------------------------------------------------------- #
# Checkpoints: {locale_index, term_index, page}
# --------------------------------------------------------------------------- #
def test_fetch_checkpoint_progresses_through_locale_and_term_rollover():
    transport = FakeTransport().add(LISTINGS_URL, _paged_responder())
    target = _target(terms=("support engineer",))
    ctx = FetchContext(transport=transport)
    records = asyncio.run(collect(builtin.ADAPTER, target, ctx))

    assert len(records) == 4
    # Walk ended after the second locale's empty page 2, having rolled over
    # locale_index once and term_index back to 0 for it.
    assert ctx.checkpoint is not None
    assert ctx.checkpoint.cursor == {"locale_index": 1, "term_index": 1, "page": 1}
    assert ctx.checkpoint.emitted == 4
    assert ctx.checkpoint.is_valid_for(target)


def test_fetch_resumes_from_a_valid_checkpoint_and_skips_already_walked_locales():
    transport = FakeTransport().add(LISTINGS_URL, _paged_responder())
    target = _target(terms=("support engineer",))
    resume = Checkpoint(
        source_key="builtin",
        instance_key="",
        cursor={"locale_index": 1, "term_index": 0, "page": 1},
        config_fingerprint=target.config_fingerprint(),
        emitted=2,
    )
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(builtin.ADAPTER, target, ctx))

    # Only the remote-US locale's two requests (page 1 with records, page 2
    # empty) fire; the sf-metro locale is not re-walked.
    assert transport.call_count == 2
    assert len(records) == 2
    assert all(r.location == "Remote, US" for r in records)
    assert ctx.checkpoint.emitted == 4


def test_fetch_replayed_checkpoint_re_emits_identical_records():
    """Resuming may re-deliver records already written; the writer dedupes on
    identity, so replay must reproduce byte-identical records (invariant 5)."""
    target = _target(terms=("support engineer",))
    stale = Checkpoint(
        source_key="builtin",
        instance_key="",
        cursor={"locale_index": 0, "term_index": 0, "page": 1},
        config_fingerprint=target.config_fingerprint(),
        emitted=0,
    )
    transport_a = FakeTransport().add(LISTINGS_URL, _paged_responder())
    fresh = asyncio.run(
        collect(builtin.ADAPTER, target, FetchContext(transport=transport_a))
    )
    transport_b = FakeTransport().add(LISTINGS_URL, _paged_responder())
    replayed = asyncio.run(
        collect(builtin.ADAPTER, target, FetchContext(transport=transport_b, resume_from=stale))
    )
    assert [r.to_json_dict() for r in fresh] == [r.to_json_dict() for r in replayed]


def test_stale_checkpoint_for_changed_search_terms_is_ignored():
    """A checkpoint recorded under one set of search terms must not resume a
    target whose terms have since changed -- `config_fingerprint` differs, so
    `is_valid_for` rejects it and the walk starts clean."""
    old_target = _target(terms=("support engineer",))
    new_target = _target(terms=("support engineer", "solutions engineer"))
    stale = Checkpoint(
        source_key="builtin",
        instance_key="",
        cursor={"locale_index": 1, "term_index": 0, "page": 1},
        config_fingerprint=old_target.config_fingerprint(),
        emitted=2,
    )
    assert not stale.is_valid_for(new_target)

    transport = FakeTransport().add(LISTINGS_URL, _paged_responder())
    ctx = FetchContext(transport=transport, resume_from=stale)
    records = asyncio.run(collect(builtin.ADAPTER, new_target, ctx))
    # Both locales x both terms walked from scratch: 4 (locale, term) pairs x
    # 2 records each.
    assert len(records) == 8


def test_fetch_mid_stream_failure_keeps_the_last_delivered_checkpoint():
    """A page-3 failure must not lose the checkpoint already advanced past
    pages 1-2 for the first locale."""
    transport = FakeTransport().add(LISTINGS_URL, _paged_responder(fail_on_page=1))
    target = _target(terms=("support engineer",))
    ctx = FetchContext(transport=transport)
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(builtin.ADAPTER, target, ctx))
    # Failed on the very first request: no checkpoint was ever marked.
    assert ctx.checkpoint is None
