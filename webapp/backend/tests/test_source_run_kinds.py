"""Phase 2.5 run kinds: `run_profiles.filter_due` and its wiring into `Scheduler`.

Two layers, each tested at the layer where it is cheapest to get right:

  `run_profiles.filter_due`  a pure function -- no clock, no I/O, no event loop.
                              Tested directly with synthetic `now`/`last_success_at`
                              inputs, so "stale vs fresh vs never-succeeded" is
                              exact and instant rather than timing-dependent.
  `Scheduler`                 the wiring: a DAILY run actually skips what
                              `filter_due` says is not due, a FULL_DIRECT run
                              never does, and both persist their `RunProfile`
                              on the run. These need the real writer and a real
                              `tmp_path` database, same as `test_source_scheduler`.

The registry-level aggregator-isolation check needs no database at all: it is a
static property of the sixteen adapters' descriptors.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from backend.sources import registry, run_profiles
from backend.sources.contract import RunKind, SourceConfig
from backend.sources.run_profiles import Priority, RunProfile, filter_due, profile_for
from backend.sources.scheduler import Scheduler, SchedulerConfig
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    fast,
    make_connect,
    permanent_always,
    plan_of,
    scalar,
)

FAST_RETRY = dict(retry_base_delay_seconds=0.01, retry_jitter=0.0)
NOW = datetime(2026, 8, 3, 12, 0, 0, tzinfo=timezone.utc)


def run(coro):
    """Run one scenario with a hard ceiling, matching `test_source_scheduler`."""

    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


def scheduler(connect, **config):
    return Scheduler(connect, config=SchedulerConfig(**{**FAST_RETRY, **config}))


def _report(connect, run_uid):
    import json

    blob = scalar(connect, "SELECT aggregate_report_json FROM pipeline_runs WHERE run_uid=?", (run_uid,))
    return json.loads(blob)


# --------------------------------------------------------------------------- #
# `run_profiles.filter_due` -- pure, no scheduler, no database
# --------------------------------------------------------------------------- #
def _plan(source_key: str, *, refresh_interval_seconds: int, instance: str = "one"):
    adapter = FakeAdapter(
        source_key,
        instances=(instance,),
        descriptor=descriptor_for(source_key, refresh_interval_seconds=refresh_interval_seconds),
    )
    return plan_of(adapter)


def test_filter_due_fresh_source_is_skipped():
    plan = _plan("gh", refresh_interval_seconds=3600)
    key = plan[0][1].source_run_key
    last_success_at = {key: (NOW - timedelta(minutes=10)).isoformat()}

    due, skipped = filter_due(plan, last_success_at, now=NOW)

    assert due == []
    assert [s.source_run_key for s in skipped] == [key]
    assert skipped[0].age_seconds == pytest.approx(600.0)
    assert skipped[0].refresh_interval_seconds == 3600


def test_filter_due_stale_source_is_due():
    plan = _plan("gh", refresh_interval_seconds=3600)
    key = plan[0][1].source_run_key
    last_success_at = {key: (NOW - timedelta(hours=5)).isoformat()}

    due, skipped = filter_due(plan, last_success_at, now=NOW)

    assert [t.source_run_key for _a, t in due] == [key]
    assert skipped == []


def test_filter_due_never_succeeded_is_due():
    plan = _plan("gh", refresh_interval_seconds=3600)
    key = plan[0][1].source_run_key

    due, skipped = filter_due(plan, {}, now=NOW)

    assert [t.source_run_key for _a, t in due] == [key]
    assert skipped == []


def test_filter_due_unparseable_timestamp_is_due():
    """A malformed `last_success_at` folds into 'no successful history', not a
    crash and not a false skip -- see `run_profiles._age_seconds`."""
    plan = _plan("gh", refresh_interval_seconds=3600)
    key = plan[0][1].source_run_key

    due, skipped = filter_due(plan, {key: "not-a-timestamp"}, now=NOW)

    assert [t.source_run_key for _a, t in due] == [key]
    assert skipped == []


def test_filter_due_exactly_at_the_interval_boundary_is_due():
    """`age >= interval`, not `>`: a source due exactly on the hour must not
    wait for the clock to tick past it."""
    plan = _plan("gh", refresh_interval_seconds=3600)
    key = plan[0][1].source_run_key
    last_success_at = {key: (NOW - timedelta(seconds=3600)).isoformat()}

    due, skipped = filter_due(plan, last_success_at, now=NOW)

    assert [t.source_run_key for _a, t in due] == [key]
    assert skipped == []


def test_filter_due_is_pure_same_inputs_same_outputs():
    """No clock read inside: two calls with identical parameters -- including an
    identical `now` supplied by the caller, however far from wall-clock reality
    -- reproduce the identical split. That determinism is what makes this
    function unit-testable at all; a hidden `datetime.now()` would make this
    test flaky instead of tautological."""
    plan = _plan("gh", refresh_interval_seconds=3600)
    key = plan[0][1].source_run_key
    last_success_at = {key: (NOW - timedelta(minutes=10)).isoformat()}
    far_future_now = NOW + timedelta(days=3650)

    first = filter_due(plan, last_success_at, now=far_future_now)
    second = filter_due(plan, last_success_at, now=far_future_now)

    # Three and a half thousand days later, "fresh ten minutes before NOW" is
    # ancient -- both calls must agree it is due, with the identical age.
    assert [t.source_run_key for _a, t in first[0]] == [key]
    assert [t.source_run_key for _a, t in second[0]] == [key]
    assert first[1] == second[1] == []


def test_filter_due_multi_target_plan_splits_independently():
    fresh_adapter = FakeAdapter(
        "fresh-src",
        instances=("a",),
        descriptor=descriptor_for("fresh-src", refresh_interval_seconds=3600),
    )
    stale_adapter = FakeAdapter(
        "stale-src",
        instances=("b",),
        descriptor=descriptor_for("stale-src", refresh_interval_seconds=3600),
    )
    plan = plan_of(fresh_adapter, stale_adapter)
    fresh_key = fresh_adapter.targets()[0].source_run_key
    stale_key = stale_adapter.targets()[0].source_run_key
    last_success_at = {
        fresh_key: (NOW - timedelta(minutes=1)).isoformat(),
        stale_key: (NOW - timedelta(hours=10)).isoformat(),
    }

    due, skipped = filter_due(plan, last_success_at, now=NOW)

    assert [t.source_run_key for _a, t in due] == [stale_key]
    assert [s.source_run_key for s in skipped] == [fresh_key]


# --------------------------------------------------------------------------- #
# Run profiles: the one table
# --------------------------------------------------------------------------- #
def test_run_profiles_cover_every_run_kind():
    assert set(run_profiles.RUN_PROFILES) == set(RunKind)


def test_only_daily_dueness_filters():
    for kind in RunKind:
        expected = kind is RunKind.DAILY
        assert profile_for(kind).dueness_filtered is expected, kind


def test_daily_and_full_direct_target_budgets_match_the_roadmap():
    assert profile_for(RunKind.DAILY).target_budget_seconds == 60.0
    assert profile_for(RunKind.FULL_DIRECT).target_budget_seconds == 300.0


def test_llm_review_is_low_priority():
    assert profile_for(RunKind.LLM_REVIEW).priority is Priority.LOW
    for kind in RunKind:
        if kind is not RunKind.LLM_REVIEW:
            assert profile_for(kind).priority is Priority.NORMAL, kind


def test_run_profile_rejects_a_non_positive_target_budget():
    with pytest.raises(ValueError):
        RunProfile(
            kind=RunKind.DAILY,
            dueness_filtered=True,
            target_budget_seconds=0.0,
            priority=Priority.NORMAL,
        )


# --------------------------------------------------------------------------- #
# Wiring: a real Scheduler run, real writer, real tmp_path database
# --------------------------------------------------------------------------- #
def test_daily_run_skips_a_fresh_source_and_records_the_skip(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "gh", body=fast(2), descriptor=descriptor_for("gh", refresh_interval_seconds=3600)
    )

    first = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))
    assert first.target("gh:one").succeeded

    second = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))

    target = second.target("gh:one")
    assert target.status == "skipped"
    assert target.skipped_reason == "not due (fresh)"
    assert target.skip_detail is not None
    assert target.skip_detail["age_seconds"] < 3600
    assert target.skip_detail["refresh_interval_seconds"] == 3600
    assert adapter.attempts == {"one": 1}, "the not-due target must not be fetched again"

    report = _report(connect, second.run_uid)
    assert report["skipped_not_due"] == [
        {
            "source": "gh:one",
            "label": "one",
            "last_success_at": target.skip_detail["last_success_at"],
            "age_seconds": target.skip_detail["age_seconds"],
            "refresh_interval_seconds": 3600,
        }
    ]


def test_daily_run_reruns_a_stale_source(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "gh", body=fast(2), descriptor=descriptor_for("gh", refresh_interval_seconds=0.05)
    )

    first = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))
    assert first.target("gh:one").succeeded

    run(asyncio.sleep(0.2))

    second = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))

    assert second.target("gh:one").succeeded
    assert adapter.attempts == {"one": 2}, "a stale source must be fetched again"


def test_daily_run_fetches_a_source_with_no_successful_history(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "gh", body=fast(2), descriptor=descriptor_for("gh", refresh_interval_seconds=3600)
    )

    result = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))

    target = result.target("gh:one")
    assert target.succeeded
    assert target.skipped_reason is None
    assert adapter.attempts == {"one": 1}


def test_daily_run_still_fetches_a_source_whose_last_run_failed(tmp_path):
    """No successful run in history at all, regardless of how recently it was
    attempted, is always due -- a failed attempt buys no freshness."""
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "gh",
        body=permanent_always(),
        descriptor=descriptor_for("gh", refresh_interval_seconds=3600),
    )

    first = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))
    assert first.target("gh:one").status == "failed"

    second = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))

    target = second.target("gh:one")
    assert target.status == "failed"
    assert target.skipped_reason is None
    assert adapter.attempts == {"one": 2}, "a source with no successful history must always be due"


def test_dueness_is_ignored_for_full_direct(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "gh",
        body=fast(2),
        descriptor=descriptor_for("gh", refresh_interval_seconds=3600),
    )

    first = run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))
    assert first.target("gh:one").succeeded

    # Still well within the refresh interval -- a DAILY run would skip this.
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    target = second.target("gh:one")
    assert target.succeeded
    assert target.skipped_reason is None
    assert adapter.attempts == {"one": 2}, "FULL_DIRECT must not dueness-filter"


@pytest.mark.parametrize("kind", [RunKind.FULL_DIRECT, RunKind.AGGREGATORS, RunKind.MANUAL_IMPORT])
def test_dueness_is_ignored_for_every_non_daily_kind(tmp_path, kind):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "src",
        body=fast(1),
        descriptor=descriptor_for(
            "src", refresh_interval_seconds=3600, run_kinds=frozenset({RunKind.DAILY, kind})
        ),
    )
    run(scheduler(connect).run(kind=RunKind.DAILY, plan=plan_of(adapter)))

    result = run(scheduler(connect).run(kind=kind, plan=plan_of(adapter)))

    assert result.target("src:one").succeeded
    assert adapter.attempts == {"one": 2}


# --------------------------------------------------------------------------- #
# Run metadata: kind + target budget persisted on every run
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "kind,expected_budget,expected_dueness,expected_priority",
    [
        (RunKind.DAILY, 60.0, True, "normal"),
        (RunKind.FULL_DIRECT, 300.0, False, "normal"),
        (RunKind.AGGREGATORS, None, False, "normal"),
        (RunKind.LLM_REVIEW, None, False, "low"),
        (RunKind.MANUAL_IMPORT, None, False, "normal"),
    ],
)
def test_run_metadata_records_kind_and_target_budget(
    tmp_path, kind, expected_budget, expected_dueness, expected_priority
):
    connect = make_connect(tmp_path)

    result = run(scheduler(connect).run(kind=kind, plan=[]))

    assert result.status == "succeeded"
    assert result.target_budget_seconds == expected_budget
    assert result.dueness_filtered is expected_dueness
    assert result.priority == expected_priority

    report = _report(connect, result.run_uid)
    assert report["kind"] == str(kind)
    assert report["target_budget_seconds"] == expected_budget
    assert report["dueness_filtered"] is expected_dueness
    assert report["priority"] == expected_priority
    assert scalar(connect, "SELECT kind FROM pipeline_runs WHERE run_uid=?", (result.run_uid,)) == str(kind)


# --------------------------------------------------------------------------- #
# LLM_REVIEW: wiring only (task 3.6 owns the review body)
# --------------------------------------------------------------------------- #
def test_llm_review_plan_is_empty_today():
    """No adapter declares membership in LLM_REVIEW yet; `plan_run` must answer
    that plainly rather than erroring, per the roadmap's task 2.5 scope."""
    assert registry.plan_run(SourceConfig(), RunKind.LLM_REVIEW) == []


def test_llm_review_run_is_a_clean_noop(tmp_path):
    connect = make_connect(tmp_path)

    result = run(scheduler(connect).run(kind=RunKind.LLM_REVIEW, plan=[]))

    assert result.status == "succeeded"
    assert result.targets == ()
    assert result.priority == "low"
    assert result.target_budget_seconds is None
    report = _report(connect, result.run_uid)
    assert report["targets"] == 0
    assert report["skipped_not_due"] == []


# --------------------------------------------------------------------------- #
# Membership audit: AGGREGATORS is an independent failure domain
# --------------------------------------------------------------------------- #
def _keys_in(kind: RunKind) -> set[str]:
    # Importing the real package triggers `install()`, registering all sixteen
    # production adapters exactly once (idempotent) without mutating the
    # registry the way a test-local `registry.register` would.
    from backend.sources import adapters as _adapters

    return {
        a.descriptor.source_key for a in _adapters.ALL_ADAPTERS if a.descriptor.runs_in(kind)
    }


def test_aggregators_share_zero_targets_with_direct_kinds():
    aggregator_keys = _keys_in(RunKind.AGGREGATORS)
    daily_keys = _keys_in(RunKind.DAILY)
    full_direct_keys = _keys_in(RunKind.FULL_DIRECT)

    assert aggregator_keys == {"jobspy"}, "only the JobSpy arm is an AGGREGATOR source today"
    assert aggregator_keys.isdisjoint(daily_keys)
    assert aggregator_keys.isdisjoint(full_direct_keys)


def test_manual_import_shares_zero_targets_with_scheduled_kinds():
    manual_keys = _keys_in(RunKind.MANUAL_IMPORT)
    for kind in (RunKind.DAILY, RunKind.FULL_DIRECT, RunKind.AGGREGATORS, RunKind.LLM_REVIEW):
        assert manual_keys.isdisjoint(_keys_in(kind))


def test_every_direct_and_startup_board_source_is_in_full_direct():
    """Regression guard for the `builtin` misdeclaration this task fixed: every
    DIRECT/STARTUP_BOARD adapter must be reachable from FULL_DIRECT, whatever
    its `InventoryScope` -- PARTIAL does not carve a source out of the full
    sweep (see `builtin.py`'s `DESCRIPTOR` comment)."""
    from backend.sources import adapters as _adapters
    from backend.sources.contract import SourceCategory

    full_direct_keys = _keys_in(RunKind.FULL_DIRECT)
    for adapter in _adapters.ALL_ADAPTERS:
        if adapter.descriptor.category in (SourceCategory.DIRECT, SourceCategory.STARTUP_BOARD):
            assert adapter.descriptor.source_key in full_direct_keys, (
                f"{adapter.descriptor.source_key} is {adapter.descriptor.category} but not "
                "in FULL_DIRECT"
            )


def test_builtin_and_yc_are_startup_boards_in_both_daily_and_full_direct():
    daily_keys = _keys_in(RunKind.DAILY)
    full_direct_keys = _keys_in(RunKind.FULL_DIRECT)
    for key in ("builtin", "yc"):
        assert key in daily_keys
        assert key in full_direct_keys
