"""Manual MCP import (`ExecutionMode.PUSH`): parsing/transport split, driven by
a frozen fixture and by `InboundPayload`s directly (there is no `Transport`).

Every parser assertion here runs with no transport and no `FakeTransport`
route at all, which is the point of the PUSH shape: the scheduler hands the
adapter payloads it already has in hand, not a URL to fetch.
"""
import asyncio
import json

import pytest

from backend.sources.adapters import manual
from backend.sources.contract import (
    ConfigError,
    ExecutionMode,
    FetchContext,
    InboundPayload,
    InventoryScope,
    PayloadError,
    RunKind,
    SourceCategory,
    SourceConfig,
    SourceTarget,
    TransportKind,
)
from backend.sources.testing import collect, fixture_bytes


def _target():
    return manual.ADAPTER.plan(SourceConfig())[0]


def _fixture():
    return fixture_bytes("manual", "batch.json")


def _payload(rows, locator="mcp://test/batch"):
    return InboundPayload(locator=locator, content=json.dumps(rows).encode("utf-8"))


# --------------------------------------------------------------------------- #
# Pure parsing (no transport, no payload wrapper)
# --------------------------------------------------------------------------- #
def test_parse_import_rows_from_frozen_fixture():
    records = list(manual.parse_import_rows(_fixture(), _target()))
    assert [r.company for r in records] == ["Cartesia", "Atoms", "Wonderschool"]
    assert all(r.source_key == "manual" for r in records)


def test_parse_import_rows_namespaces_identity_per_mcp_origin():
    """Two rows from different MCP tools in the same batch land in different
    identity namespaces, even though they were pushed and planned together."""
    records = list(manual.parse_import_rows(_fixture(), _target()))
    dice, zipr, dice2 = records
    assert dice.instance_key == "mcp-dice"
    assert zipr.instance_key == "mcp-zip"
    assert dice.namespace == "manual:mcp-dice"
    assert zipr.namespace == "manual:mcp-zip"
    assert dice2.namespace == "manual:mcp-dice"


def test_parse_import_rows_prefers_req_id_but_degrades_to_url_without_one():
    dice, zipr, _ = list(manual.parse_import_rows(_fixture(), _target()))

    dice_claims = dice.identity_claims()
    assert (dice_claims[0].kind, dice_claims[0].namespace, dice_claims[0].value) == (
        "source_req",
        "manual:mcp-dice",
        "c9a2dcc9-3728-4bd3-a6c6-e1aaeae4f2c0",
    )
    assert dice_claims[1].kind == "url"

    # ZipRecruiter row has req_id == "": NormalizedPosting folds that to None,
    # so identity degrades to the URL claim only. Never treated as global.
    assert zipr.req_id is None
    zip_claims = zipr.identity_claims()
    assert len(zip_claims) == 1
    assert zip_claims[0].kind == "url"


def test_parse_import_rows_absolute_date_survives_relative_recency_does_not():
    dice, zipr, relative = list(manual.parse_import_rows(_fixture(), _target()))
    assert dice.posted_date == "2026-07-03"
    assert dice.posted_raw == "2026-07-03"
    # Empty posted string -> no date, no raw text either.
    assert zipr.posted_date is None
    assert zipr.posted_raw == ""
    # "3 days ago" must never enter the hash, but survives as provenance.
    assert relative.posted_date is None
    assert relative.posted_raw == "3 days ago"
    assert relative.canonical_fields()["posted_date"] == ""


def test_parse_import_rows_maps_optional_desc_to_inline_description():
    _, zipr, dice2 = list(manual.parse_import_rows(_fixture(), _target()))
    assert zipr.description == "Robotics  support   role.\nSome whitespace noise."
    # description_digest is whitespace-insensitive, unlike the raw field.
    assert zipr.description_digest != ""
    assert dice2.description is None


def test_parse_import_rows_missing_location_is_empty_not_an_error():
    _, _, relative = list(manual.parse_import_rows(_fixture(), _target()))
    assert relative.location == ""


def test_parse_import_rows_skips_unusable_rows_without_failing_the_batch():
    """Missing title, missing url, missing MCP origin tag, and a non-object
    item must all be skipped, and must not blank the rows around them."""
    records = list(manual.parse_import_rows(_fixture(), _target()))
    assert len(records) == 3
    companies = {r.company for r in records}
    assert "Bio-Rad Laboratories" not in companies  # empty title
    assert "Graco, Inc." not in companies  # empty url
    assert "Crusoe" not in companies  # empty source/origin tag


def test_parse_import_rows_content_hash_is_stable_across_runs():
    once = list(manual.parse_import_rows(_fixture(), _target()))
    twice = list(manual.parse_import_rows(_fixture(), _target()))
    assert [r.content_hash() for r in once] == [r.content_hash() for r in twice]


def test_parse_import_rows_rejects_a_malformed_envelope():
    for payload in (b"not json at all", b'{"not": "a list"}', b'"just a string"'):
        with pytest.raises(PayloadError):
            list(manual.parse_import_rows(payload, _target()))


def test_parse_import_rows_accepts_bytes_str_and_decoded_list():
    raw = _fixture()
    assert (
        len(list(manual.parse_import_rows(raw, _target())))
        == len(list(manual.parse_import_rows(raw.decode(), _target())))
        == len(list(manual.parse_import_rows(json.loads(raw), _target())))
    )


def test_parse_import_rows_extra_records_the_mcp_origin_without_hashing_it():
    dice, _, _ = list(manual.parse_import_rows(_fixture(), _target()))
    assert dice.extra["mcp_source"] == "mcp-dice"
    assert "mcp_source" not in dice.canonical_fields()


# --------------------------------------------------------------------------- #
# plan()
# --------------------------------------------------------------------------- #
def test_plan_returns_exactly_one_target_for_the_pushed_batch():
    targets = manual.ADAPTER.plan(SourceConfig())
    assert len(targets) == 1
    target = targets[0]
    assert target.source_key == "manual"
    assert target.inventory_scope is InventoryScope.PARTIAL


def test_plan_ignores_config_there_is_nothing_to_enumerate():
    """Unlike Greenhouse, `config.json` has no `companies.manual` map: the
    target is fixed regardless of what config is handed in."""
    empty = manual.ADAPTER.plan(SourceConfig())
    with_unrelated_config = manual.ADAPTER.plan(
        SourceConfig.from_mapping({"companies": {"greenhouse": {"acme": "Acme"}}})
    )
    assert [t.source_run_key for t in empty] == [t.source_run_key for t in with_unrelated_config]


def test_descriptor_declares_manual_import_push_and_no_transport():
    descriptor = manual.DESCRIPTOR
    assert descriptor.category is SourceCategory.MANUAL
    assert descriptor.runs_in(RunKind.MANUAL_IMPORT)
    assert not descriptor.runs_in(RunKind.DAILY)
    assert not descriptor.runs_in(RunKind.FULL_DIRECT)
    assert descriptor.execution is ExecutionMode.PUSH
    assert descriptor.transport is TransportKind.NONE
    assert descriptor.supports_checkpoint is False
    assert descriptor.default_inventory_scope is InventoryScope.PARTIAL


# --------------------------------------------------------------------------- #
# fetch(): no transport, streams straight from ctx.payloads
# --------------------------------------------------------------------------- #
def test_fetch_streams_the_parsed_batch_from_inbound_payloads():
    ctx = FetchContext(payloads=[_payload(json.loads(_fixture()))])
    records = asyncio.run(collect(manual.ADAPTER, _target(), ctx))
    assert [r.company for r in records] == ["Cartesia", "Atoms", "Wonderschool"]


def test_fetch_never_touches_a_transport():
    """`ctx.http()` would raise on this context; `fetch` must never call it."""
    ctx = FetchContext(payloads=[_payload([{"title": "t", "company": "c", "url": "u", "source": "mcp-dice"}])])
    assert not ctx.has_transport
    records = asyncio.run(collect(manual.ADAPTER, _target(), ctx))
    assert len(records) == 1
    with pytest.raises(ConfigError):
        ctx.http()


def test_fetch_on_a_malformed_payload_raises_payload_error():
    ctx = FetchContext(payloads=[_payload({"not": "a list"})])
    with pytest.raises(PayloadError):
        asyncio.run(collect(manual.ADAPTER, _target(), ctx))


def test_fetch_with_no_payloads_yields_nothing_and_does_not_raise():
    """An MCP import run where nothing was pushed is not a failure: PARTIAL
    scope means an empty yield is never read as "this source is empty"."""
    ctx = FetchContext(payloads=[])
    records = asyncio.run(collect(manual.ADAPTER, _target(), ctx))
    assert records == []


def test_fetch_drains_multiple_payloads_in_one_run():
    """A run can carry more than one MCP drop; both must stream through."""
    ctx = FetchContext(
        payloads=[
            _payload([{"title": "A", "company": "Acme", "url": "https://a.example/1", "source": "mcp-dice"}]),
            _payload([{"title": "B", "company": "Beta", "url": "https://b.example/2", "source": "mcp-zip"}]),
        ]
    )
    records = asyncio.run(collect(manual.ADAPTER, _target(), ctx))
    assert [r.company for r in records] == ["Acme", "Beta"]
    assert [r.instance_key for r in records] == ["mcp-dice", "mcp-zip"]


def test_fetch_yields_the_same_records_the_pure_parser_does():
    """The transport-less shell must add nothing the fixture path cannot
    reproduce (there being no transport shell at all, this should be exact)."""
    ctx = FetchContext(payloads=[_payload(json.loads(_fixture()))])
    fetched = asyncio.run(collect(manual.ADAPTER, _target(), ctx))
    parsed = list(manual.parse_import_rows(_fixture(), _target()))
    assert [r.to_json_dict() for r in fetched] == [r.to_json_dict() for r in parsed]
