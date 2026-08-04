"""Contract-level tests: the guarantees the Phase 2.3 scheduler is built on.

No live network anywhere. Every adapter here is driven by `FakeTransport` or by
an `InboundPayload`, and the async paths are exercised with `asyncio.run` rather
than a plugin so the suite gains no new dependency.
"""
import asyncio
import json

import pytest

from backend.sources import registry
from backend.sources.contract import (
    CANONICAL_HASH_FIELDS,
    Checkpoint,
    ConfigError,
    Disposition,
    ExecutionMode,
    FetchContext,
    HttpRequest,
    HttpResponse,
    InboundPayload,
    InventoryScope,
    NormalizedPosting,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceAdapter,
    SourceCategory,
    SourceConfig,
    SourceDescriptor,
    SourceTarget,
    TransientSourceError,
    TransportKind,
    check_status,
    classify_status,
    normalize_date,
    normalize_text,
    normalize_url,
)
from backend.sources.testing import FakeTransport, collect, json_response

PAGED_KEY = "paged-test"
PAGED_URL = "https://paged.example/api/jobs"
PUSH_KEY = "manual-test"


# --------------------------------------------------------------------------- #
# Test doubles
# --------------------------------------------------------------------------- #
class PagedAdapter:
    """A paginated, checkpointing source. Stands in for Workday/Amazon/iCIMS."""

    descriptor = SourceDescriptor(
        source_key=PAGED_KEY,
        category=SourceCategory.DIRECT,
        run_kinds=frozenset({RunKind.FULL_DIRECT}),
        default_deadline_seconds=5.0,
        supports_checkpoint=True,
    )

    def plan(self, config: SourceConfig):
        return [
            SourceTarget(
                source_key=PAGED_KEY,
                instance_key=slug,
                label=str(name),
                params={"slug": slug},
            )
            for slug, name in config.entries(PAGED_KEY).items()
        ]

    async def fetch(self, target, ctx):
        page, emitted = 0, 0
        if ctx.resume_from is not None and ctx.resume_from.is_valid_for(target):
            page = int(ctx.resume_from.cursor.get("next_page", 0))
            emitted = ctx.resume_from.emitted
        while True:
            response = await ctx.http().send(HttpRequest(url=PAGED_URL, params={"page": page}))
            check_status(response, source_key=PAGED_KEY, instance_key=target.instance_key)
            jobs = response.json().get("jobs") or []
            if not jobs:
                return
            for job in jobs:
                yield target.record(
                    title=job["title"],
                    company="Acme",
                    url=job["url"],
                    req_id=str(job["id"]),
                    location="San Francisco, CA",
                )
                emitted += 1
            page += 1
            ctx.mark_checkpoint({"next_page": page}, target=target, emitted=emitted)


class PushAdapter:
    """Manual MCP import: no transport, records supplied from outside."""

    descriptor = SourceDescriptor(
        source_key=PUSH_KEY,
        category=SourceCategory.MANUAL,
        run_kinds=frozenset({RunKind.MANUAL_IMPORT}),
        execution=ExecutionMode.PUSH,
        transport=TransportKind.NONE,
        default_inventory_scope=InventoryScope.PARTIAL,
    )

    def plan(self, config: SourceConfig):
        return [
            SourceTarget(
                source_key=PUSH_KEY,
                instance_key="dice",
                inventory_scope=InventoryScope.PARTIAL,
            )
        ]

    async def fetch(self, target, ctx):
        for payload in ctx.payloads:
            for row in payload.json():
                yield target.record(
                    title=row["title"],
                    company=row["company"],
                    url=row["url"],
                    req_id=row.get("req_id"),
                )


def _page(*jobs):
    return json_response({"jobs": list(jobs)})


def _job(n):
    return {"id": 100 + n, "title": f"Support Engineer {n}", "url": f"https://acme.example/jobs/{100 + n}"}


def _paged_transport(pages, *, tail=None):
    transport = FakeTransport()
    responses = [_page(*page) for page in pages]
    responses.append(tail if tail is not None else _page())
    transport.add(PAGED_URL, *responses)
    return transport


def _target(instance="acme", **params):
    return SourceTarget(source_key=PAGED_KEY, instance_key=instance, params=params or {"slug": instance})


@pytest.fixture
def isolated_registry():
    """Snapshot and restore the process-wide registry around a test."""
    saved = dict(registry._REGISTRY)
    try:
        yield registry
    finally:
        registry._REGISTRY.clear()
        registry._REGISTRY.update(saved)


# --------------------------------------------------------------------------- #
# Normalization
# --------------------------------------------------------------------------- #
def test_normalize_text_folds_unicode_and_whitespace():
    assert normalize_text("  Support  Engineer\n") == "Support Engineer"
    assert normalize_text("Ｓupport") == "Support"
    assert normalize_text(None) == ""


def test_normalize_url_strips_tracking_sorts_query_and_lowercases_host():
    assert (
        normalize_url("HTTPS://Boards.Greenhouse.IO/acme/jobs/42/?gh_src=xyz&utm_source=x#apply")
        == "https://boards.greenhouse.io/acme/jobs/42"
    )
    # meaningful params survive and are ordered deterministically
    assert (
        normalize_url("https://x.example/careers/job/7?domain=b.com&a=1")
        == "https://x.example/careers/job/7?a=1&domain=b.com"
    )
    # default ports collapse; non-http passes through untouched
    assert normalize_url("https://x.example:443/a") == "https://x.example/a"
    assert normalize_url("withheld:abc") == "withheld:abc"
    assert normalize_url(None) == ""


def test_normalize_date_rejects_relative_recency_strings():
    assert normalize_date("2026-07-14T09:00:00-04:00") == "2026-07-14"
    assert normalize_date("2026-07-14") == "2026-07-14"
    # Workday's postedOn and Built In's card text must never enter the hash.
    assert normalize_date("Posted 30+ Days Ago") is None
    assert normalize_date("3 days ago") is None
    assert normalize_date("") is None
    assert normalize_date(None) is None
    assert normalize_date("2026-13-40") is None


# --------------------------------------------------------------------------- #
# NormalizedPosting
# --------------------------------------------------------------------------- #
def test_record_normalizes_on_construction():
    record = NormalizedPosting(
        source_key="greenhouse",
        instance_key="acme",
        title="  Support   Engineer ",
        company=" Acme  Inc ",
        url=" https://acme.example/jobs/1 ",
        location="San Francisco,   CA",
        req_id="  42 ",
        posted_date="2026-07-14T09:00:00Z",
        posted_raw=" 2026-07-14T09:00:00Z ",
    )
    assert record.title == "Support Engineer"
    assert record.company == "Acme Inc"
    assert record.location == "San Francisco, CA"
    assert record.req_id == "42"
    assert record.posted_date == "2026-07-14"
    assert record.namespace == "greenhouse:acme"


def test_record_requires_source_title_and_url():
    for kwargs in (
        {"source_key": "", "title": "t", "company": "c", "url": "u"},
        {"source_key": "s", "title": " ", "company": "c", "url": "u"},
        {"source_key": "s", "title": "t", "company": "c", "url": ""},
    ):
        with pytest.raises(PayloadError):
            NormalizedPosting(**kwargs)


def test_singleton_source_namespaces_on_source_key_alone():
    record = NormalizedPosting(source_key="yc", title="Founding SE", company="YC startup", url="https://y.example/1")
    assert record.namespace == "yc"


def test_canonical_hash_field_order_is_frozen():
    """Guard: changing this order or membership re-versions the whole corpus."""
    assert list(CANONICAL_HASH_FIELDS) == [
        "source_key",
        "namespace",
        "req_id",
        "url_key",
        "title",
        "company",
        "location",
        "posted_date",
        "salary",
        "remote",
        "description_digest",
    ]
    record = NormalizedPosting(source_key="s", title="t", company="c", url="https://x.example/1")
    assert list(record.canonical_fields()) == list(CANONICAL_HASH_FIELDS)


def test_content_hash_ignores_noise_but_tracks_material_change():
    base = NormalizedPosting(
        source_key="greenhouse",
        instance_key="acme",
        title="Support Engineer",
        company="Acme",
        url="https://acme.example/jobs/1",
        location="San Francisco, CA",
        posted_date="2026-07-14",
        req_id="42",
    )
    noisy = NormalizedPosting(
        source_key="greenhouse",
        instance_key="acme",
        title="  Support Engineer  ",
        company="Acme",
        url="https://acme.example/jobs/1?gh_src=tracking&utm_medium=email",
        location="San Francisco,  CA",
        posted_date="2026-07-14T23:59:59Z",
        posted_raw="Posted 3 days ago",
        req_id="42",
        alt_urls=("https://mirror.example/1",),
        extra={"updated_at": "2026-08-01T00:00:00Z", "debug": True},
    )
    assert noisy.content_hash() == base.content_hash()

    from dataclasses import replace

    assert replace(base, title="Senior Support Engineer").content_hash() != base.content_hash()
    assert replace(base, location="Oakland, CA").content_hash() != base.content_hash()
    assert replace(base, salary_text="$150k").content_hash() != base.content_hash()
    assert replace(base, remote=True).content_hash() != base.content_hash()
    assert replace(base, posted_date="2026-07-15").content_hash() != base.content_hash()
    assert replace(base, description="Do support things.").content_hash() != base.content_hash()


def test_description_digest_is_whitespace_insensitive():
    from dataclasses import replace

    record = NormalizedPosting(
        source_key="s", title="t", company="c", url="https://x.example/1", description="a  b\nc"
    )
    assert replace(record, description="a b c").content_hash() == record.content_hash()
    assert replace(record, description="a b d").content_hash() != record.content_hash()


def test_identity_claims_put_namespaced_req_id_before_url():
    record = NormalizedPosting(
        source_key="workday",
        instance_key="tmobile",
        title="Support Engineer",
        company="T-Mobile",
        url="https://tmobile.example/job/REQ-1?utm_source=x",
        req_id="REQ-1",
    )
    claims = record.identity_claims()
    assert [(c.kind, c.namespace, c.value, c.rank) for c in claims] == [
        ("source_req", "workday:tmobile", "REQ-1", 0),
        ("url", "url", "https://tmobile.example/job/REQ-1", 1),
    ]


def test_identity_claims_degrade_to_url_only_without_req_id():
    record = NormalizedPosting(
        source_key="builtin", title="Support Engineer", company="Acme", url="https://builtin.example/job/x"
    )
    claims = record.identity_claims()
    assert len(claims) == 1
    assert claims[0].kind == "url"


def test_record_json_round_trip_is_lossless():
    """Required by the JobSpy subprocess wire format and by fixture replay."""
    record = NormalizedPosting(
        source_key="jobspy-indeed",
        instance_key="indeed",
        title="Support Engineer",
        company="Acme",
        url="https://indeed.example/viewjob?jk=1",
        location="Remote",
        req_id=None,
        posted_date="2026-07-14",
        posted_raw="2026-07-14",
        salary_text="120000-150000 yearly",
        remote=True,
        description="Long description body",
        alt_urls=("https://mirror.example/1",),
        extra={"site": "indeed"},
    )
    wire = json.dumps(record.to_json_dict())
    restored = NormalizedPosting.from_json_dict(json.loads(wire))
    assert restored == record
    assert restored.content_hash() == record.content_hash()


# --------------------------------------------------------------------------- #
# Errors and classification
# --------------------------------------------------------------------------- #
def test_classify_status_table():
    assert classify_status(200) is Disposition.SUCCESS
    for code in (408, 425, 429, 500, 502, 503, 504):
        assert classify_status(code) is Disposition.TRANSIENT
    for code in (301, 400, 401, 403, 404, 410, 418):
        assert classify_status(code) is Disposition.PERMANENT


def test_check_status_raises_classified_errors():
    with pytest.raises(TransientSourceError) as transient:
        check_status(HttpResponse(status=503, url="https://x.example/a"), source_key="s")
    assert transient.value.disposition is Disposition.TRANSIENT
    assert transient.value.status == 503

    with pytest.raises(PermanentSourceError) as permanent:
        check_status(HttpResponse(status=404, url="https://x.example/a"), source_key="s")
    assert permanent.value.disposition is Disposition.PERMANENT

    ok = HttpResponse(status=204, url="https://x.example/a")
    assert check_status(ok, allow=(200, 204)) is ok


def test_source_error_serializes_for_source_runs_error_json():
    err = TransientSourceError("boom", source_key="greenhouse", instance_key="acme", status=503, url="u")
    payload = err.to_json_dict()
    assert payload["disposition"] == "transient"
    assert payload["source_key"] == "greenhouse"
    assert json.loads(json.dumps(payload)) == payload


def test_response_json_on_non_json_body_is_permanent():
    response = HttpResponse(status=200, url="https://x.example/a", content=b"<html>nope</html>")
    with pytest.raises(PayloadError):
        response.json(source_key="s")


# --------------------------------------------------------------------------- #
# Checkpoints
# --------------------------------------------------------------------------- #
def test_checkpoint_round_trips_through_json():
    target = _target()
    checkpoint = Checkpoint(
        source_key=PAGED_KEY,
        instance_key="acme",
        cursor={"next_page": 3, "term": "support engineer"},
        config_fingerprint=target.config_fingerprint(),
        emitted=57,
    )
    restored = Checkpoint.from_json(checkpoint.to_json())
    assert restored == checkpoint
    assert restored.cursor["next_page"] == 3
    assert restored.is_valid_for(target)


def test_checkpoint_json_is_none_safe_and_version_checked():
    assert Checkpoint.from_json(None) is None
    assert Checkpoint.from_json("  ") is None
    with pytest.raises(ValueError):
        Checkpoint.from_json(json.dumps({"version": 99}))


def test_checkpoint_is_invalid_when_target_config_changes():
    target = _target(slug="acme", terms=["a"])
    checkpoint = Checkpoint(
        source_key=PAGED_KEY,
        instance_key="acme",
        cursor={"next_page": 1},
        config_fingerprint=target.config_fingerprint(),
    )
    assert checkpoint.is_valid_for(target)
    assert not checkpoint.is_valid_for(_target(slug="acme", terms=["a", "b"]))
    assert not checkpoint.is_valid_for(_target(instance="other", slug="other"))


# --------------------------------------------------------------------------- #
# Streaming, failure, and resume
# --------------------------------------------------------------------------- #
def test_records_stream_before_the_source_finishes():
    """Success Contract: new jobs appear before the whole run completes."""
    adapter, target = PagedAdapter(), _target()
    transport = _paged_transport([[_job(1), _job(2)], [_job(3)]])
    ctx = FetchContext(transport=transport)

    async def scenario():
        stream = adapter.fetch(target, ctx)
        first = await stream.__anext__()
        # Exactly one page has been requested: the consumer has a record in hand
        # while pages 2 and 3 are still unfetched.
        assert transport.call_count == 1
        rest = [record async for record in stream]
        return first, rest

    first, rest = asyncio.run(scenario())
    assert first.title == "Support Engineer 1"
    assert [r.title for r in rest] == ["Support Engineer 2", "Support Engineer 3"]
    assert transport.call_count == 3  # two pages plus the empty terminator


def test_fast_source_completes_in_one_request():
    adapter, target = PagedAdapter(), _target()
    transport = _paged_transport([[_job(1)]])
    records = asyncio.run(collect(adapter, target, FetchContext(transport=transport)))
    assert [r.req_id for r in records] == ["101"]
    assert records[0].namespace == f"{PAGED_KEY}:acme"


def test_failing_source_raises_instead_of_yielding_nothing():
    """Invariant 3: a broken source must never look like an empty one."""
    adapter, target = PagedAdapter(), _target()

    transient = FakeTransport().add(PAGED_URL, json_response({}, status=503))
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(adapter, target, FetchContext(transport=transient)))

    permanent = FakeTransport().add(PAGED_URL, json_response({}, status=404))
    with pytest.raises(PermanentSourceError):
        asyncio.run(collect(adapter, target, FetchContext(transport=permanent)))

    exploding = FakeTransport().add(PAGED_URL, TransientSourceError("connection reset"))
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(adapter, target, FetchContext(transport=exploding)))


def test_adapter_does_not_retry_a_failed_request():
    """Invariant 1: one failure is one request. Retry is the scheduler's call."""
    adapter, target = PagedAdapter(), _target()
    transport = FakeTransport().add(PAGED_URL, json_response({}, status=503))
    with pytest.raises(TransientSourceError):
        asyncio.run(collect(adapter, target, FetchContext(transport=transport)))
    assert transport.call_count == 1


def test_mid_stream_failure_keeps_earlier_records_and_last_checkpoint():
    adapter, target = PagedAdapter(), _target()
    transport = _paged_transport([[_job(1), _job(2)]], tail=json_response({}, status=503))
    ctx = FetchContext(transport=transport)

    async def scenario():
        seen = []
        with pytest.raises(TransientSourceError):
            async for record in adapter.fetch(target, ctx):
                seen.append(record)
        return seen

    seen = asyncio.run(scenario())
    assert [r.req_id for r in seen] == ["101", "102"]
    # Partial progress survives the failure so the scheduler can persist it.
    assert ctx.checkpoint is not None
    assert ctx.checkpoint.cursor["next_page"] == 1
    assert ctx.checkpoint.emitted == 2
    assert ctx.checkpoint.is_valid_for(target)


def test_resume_from_checkpoint_continues_where_it_stopped():
    adapter, target = PagedAdapter(), _target()
    resume = Checkpoint(
        source_key=PAGED_KEY,
        instance_key="acme",
        cursor={"next_page": 1},
        config_fingerprint=target.config_fingerprint(),
        emitted=2,
    )
    transport = _paged_transport([[_job(3)]])
    ctx = FetchContext(transport=transport, resume_from=resume)
    records = asyncio.run(collect(adapter, target, ctx))
    assert [r.req_id for r in records] == ["103"]
    assert transport.requests[0].params == {"page": 1}
    assert ctx.checkpoint.emitted == 3


def test_replayed_checkpoint_re_emits_identical_records():
    """Invariant 5: replay is expected, and the writer dedupes on identity."""
    adapter, target = PagedAdapter(), _target()
    first = asyncio.run(
        collect(adapter, target, FetchContext(transport=_paged_transport([[_job(1), _job(2)]])))
    )
    stale = Checkpoint(
        source_key=PAGED_KEY,
        instance_key="acme",
        cursor={"next_page": 0},
        config_fingerprint=target.config_fingerprint(),
        emitted=2,
    )
    second = asyncio.run(
        collect(
            adapter,
            target,
            FetchContext(transport=_paged_transport([[_job(1), _job(2)]]), resume_from=stale),
        )
    )
    assert [r.content_hash() for r in second] == [r.content_hash() for r in first]
    assert [r.identity_claims() for r in second] == [r.identity_claims() for r in first]


def test_stale_checkpoint_for_a_changed_target_is_ignored_by_the_adapter():
    adapter, target = PagedAdapter(), _target(slug="acme", terms=["a"])
    stale = Checkpoint(
        source_key=PAGED_KEY,
        instance_key="acme",
        cursor={"next_page": 5},
        config_fingerprint="stale-fingerprint",
        emitted=99,
    )
    transport = _paged_transport([[_job(1)]])
    asyncio.run(collect(adapter, target, FetchContext(transport=transport, resume_from=stale)))
    assert transport.requests[0].params == {"page": 0}


def test_cancellation_propagates_and_preserves_partial_progress():
    """The scheduler enforces the deadline by cancelling; adapters must let it."""
    adapter, target = PagedAdapter(), _target()

    class HangingTransport(FakeTransport):
        async def send(self, request):
            if request.params and request.params.get("page") == 1:
                await asyncio.sleep(30)
            return await super().send(request)

    transport = HangingTransport()
    transport.add(PAGED_URL, _page(_job(1)), _page(_job(2)))
    ctx = FetchContext(transport=transport)

    async def scenario():
        seen = []
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                async for record in adapter.fetch(target, ctx):
                    seen.append(record)
        return seen

    seen = asyncio.run(scenario())
    assert [r.req_id for r in seen] == ["101"]
    assert ctx.checkpoint.cursor["next_page"] == 1


# --------------------------------------------------------------------------- #
# Manual import (no transport)
# --------------------------------------------------------------------------- #
def test_manual_import_uses_the_same_contract_without_a_transport():
    adapter = PushAdapter()
    target = adapter.plan(SourceConfig())[0]
    payload = InboundPayload(
        locator="mcp://dice/2026-08-03",
        content=json.dumps(
            [{"title": "Support Engineer", "company": "Acme", "url": "https://dice.example/j/1", "req_id": "D-1"}]
        ).encode("utf-8"),
    )
    ctx = FetchContext(payloads=[payload])
    records = asyncio.run(collect(adapter, target, ctx))
    assert [r.req_id for r in records] == ["D-1"]
    assert records[0].namespace == "manual-test:dice"
    # Absence can never be inferred from an out-of-band drop.
    assert target.inventory_scope is InventoryScope.PARTIAL


def test_context_without_transport_refuses_to_improvise_one():
    ctx = FetchContext()
    assert not ctx.has_transport
    with pytest.raises(ConfigError):
        ctx.http()


def test_inbound_payload_rejects_non_json():
    with pytest.raises(PayloadError):
        InboundPayload(locator="mcp://x", content=b"not json").json()


# --------------------------------------------------------------------------- #
# Descriptors, targets, config
# --------------------------------------------------------------------------- #
def test_descriptor_validation():
    for kwargs in (
        {"source_key": "", "run_kinds": frozenset({RunKind.DAILY})},
        {"source_key": "s", "run_kinds": frozenset()},
        {"source_key": "s", "run_kinds": frozenset({RunKind.DAILY}), "default_deadline_seconds": 0},
        {"source_key": "s", "run_kinds": frozenset({RunKind.DAILY}), "per_host_concurrency": 0},
        {"source_key": "s", "run_kinds": frozenset({RunKind.DAILY}), "max_concurrent_targets": 0},
        {"source_key": "s", "run_kinds": frozenset({RunKind.DAILY}), "refresh_interval_seconds": -1},
    ):
        with pytest.raises(ConfigError):
            SourceDescriptor(category=SourceCategory.DIRECT, **kwargs)


def test_descriptor_run_kind_membership_and_deadline_override():
    descriptor = PagedAdapter.descriptor
    assert descriptor.runs_in(RunKind.FULL_DIRECT)
    assert not descriptor.runs_in(RunKind.DAILY)
    assert descriptor.deadline_for(_target()) == 5.0
    assert descriptor.deadline_for(SourceTarget(source_key=PAGED_KEY, deadline_seconds=1.5)) == 1.5


def test_target_requires_params_explicitly():
    target = SourceTarget(source_key=PAGED_KEY, instance_key="acme", params={"slug": "acme"})
    assert target.require("slug") == "acme"
    assert target.source_run_key == "paged-test:acme"
    assert target.namespace == "paged-test:acme"
    with pytest.raises(ConfigError):
        target.require("host")
    with pytest.raises(ConfigError):
        SourceTarget(source_key="")
    with pytest.raises(ConfigError):
        SourceTarget(source_key="s", deadline_seconds=0)


def test_target_record_stamps_identity_from_the_target():
    target = SourceTarget(source_key="greenhouse", instance_key="acme")
    record = target.record(title="Support Engineer", company="Acme", url="https://a.example/1")
    assert (record.source_key, record.instance_key) == ("greenhouse", "acme")


def test_source_config_from_real_config_shape():
    config = SourceConfig.from_mapping(
        {
            "profile": {"search_terms": ["support engineer", ""], "employer_scrape_location": "California"},
            "companies": {"greenhouse": {"anthropic": "Anthropic"}},
            "jobspy": {"sites": ["indeed"], "title_cap": 5},
        }
    )
    assert config.search_terms == ("support engineer",)
    assert config.entries("greenhouse") == {"anthropic": "Anthropic"}
    assert config.entries("lever") == {}  # unconfigured is not an error
    assert config.option("jobspy")["title_cap"] == 5
    assert config.profile["employer_scrape_location"] == "California"
    with pytest.raises(ConfigError):
        SourceConfig.from_mapping({"companies": {"greenhouse": ["not", "a", "map"]}}).entries("greenhouse")


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
def test_registry_registers_and_resolves(isolated_registry):
    isolated_registry._REGISTRY.clear()
    adapter = PagedAdapter()
    isolated_registry.register(adapter)
    assert isolated_registry.get(PAGED_KEY) is adapter
    assert isolated_registry.is_registered(PAGED_KEY)
    assert isolated_registry.keys() == (PAGED_KEY,)


def test_registry_rejects_duplicate_source_keys(isolated_registry):
    isolated_registry._REGISTRY.clear()
    isolated_registry.register(PagedAdapter())
    with pytest.raises(ConfigError):
        isolated_registry.register(PagedAdapter())
    isolated_registry.register(PagedAdapter(), replace=True)


def test_registry_unknown_key_raises_config_error(isolated_registry):
    with pytest.raises(ConfigError):
        isolated_registry.get("no-such-source")


def test_plan_run_filters_by_run_kind_and_is_pure(isolated_registry):
    isolated_registry._REGISTRY.clear()
    isolated_registry.register(PagedAdapter())
    isolated_registry.register(PushAdapter())
    config = SourceConfig.from_mapping({"companies": {PAGED_KEY: {"acme": "Acme", "beta": "Beta"}}})

    full = isolated_registry.plan_run(config, RunKind.FULL_DIRECT)
    assert [t.source_run_key for _, t in full] == ["paged-test:acme", "paged-test:beta"]

    manual = isolated_registry.plan_run(config, RunKind.MANUAL_IMPORT)
    assert [t.source_run_key for _, t in manual] == ["manual-test:dice"]

    assert isolated_registry.plan_run(config, RunKind.AGGREGATORS) == []
    # Deterministic: planning twice yields the same plan.
    assert [t.source_run_key for _, t in isolated_registry.plan_run(config, RunKind.FULL_DIRECT)] == [
        t.source_run_key for _, t in full
    ]


def test_registered_adapters_satisfy_the_protocol():
    import backend.sources.adapters  # noqa: F401  (registers the built-ins)

    assert registry.keys()
    for adapter in registry.all_adapters():
        assert isinstance(adapter, SourceAdapter)
        assert isinstance(adapter.descriptor, SourceDescriptor)
        assert adapter.descriptor.source_key
