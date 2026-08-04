"""Phase 2.5 run kinds: per-kind scheduling policy and DAILY dueness filtering.

`registry.plan_run(config, run_kind)` answers ELIGIBILITY -- "which sources
declare membership in this kind" -- and its docstring explicitly defers
dueness to "a separate scheduler decision made against `source_runs` history
and `descriptor.refresh_interval_seconds`". This module is that decision,
plus the declarative "per-kind run profile" the roadmap asks for: one table
mapping each `RunKind` to whether it dueness-filters, its run-level
performance target, and its scheduling priority.

Division of responsibility, so this module stays pure and unit-testable
without a database or an event loop:

  the SCHEDULER   reads `runstore.source_instance_freshness()` (I/O, one
                   clock read for `now`) and hands this module the resulting
                   `last_success_at` map plus that same `now`.
  `filter_due`     HERE is pure: no I/O, no `datetime.now()` inside. Given the
  (this module)    same plan, the same freshness map, and the same `now`, it
                   always returns the same due/skipped split.

This mirrors the contract's own split between "the adapter expands config
into targets" and "the scheduler decides which targets run, and when" --
dueness is squarely on the scheduler side of that line, and living in its own
module (rather than inline in `scheduler.py`) is what keeps it testable
without spinning up the writer, the concurrency gates, or a real run.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from .contract import RunKind, SourceAdapter, SourceTarget

__all__ = [
    "RUN_PROFILES",
    "Priority",
    "RunProfile",
    "SkippedTarget",
    "filter_due",
    "profile_for",
]


class Priority(StrEnum):
    """Scheduling priority a run kind is launched at.

    Advisory today: nothing in `scheduler.py` yet reorders pending runs by
    priority, because one `Scheduler` executes one run at a time. It is
    recorded on the profile and persisted in run metadata (`_run_report`) so a
    later orchestrator that DOES juggle multiple pending runs -- e.g.
    deferring a `llm-review` run behind a `daily` one, per the roadmap's
    "optional low-priority review" -- has the fact available to act on,
    rather than a wiring change to make first.
    """

    NORMAL = "normal"
    LOW = "low"


@dataclass(frozen=True, slots=True)
class RunProfile:
    """Declarative policy for one `RunKind`. One table, not scattered
    conditionals -- see `RUN_PROFILES`.

    `target_budget_seconds` is a Success Contract PERFORMANCE TARGET recorded
    on the run for 2.6b to benchmark against. It is never a second deadline:
    the contract's one-deadline rule (`SourceDescriptor.default_deadline_seconds`
    / `SourceTarget.deadline_seconds`, enforced per attempt by the
    `asyncio.timeout` in `scheduler.Scheduler._run_attempt`) is untouched by
    this value, and nothing in `scheduler.py` cancels a run for missing it.
    """

    kind: RunKind
    dueness_filtered: bool
    target_budget_seconds: float | None
    priority: Priority

    def __post_init__(self) -> None:
        if self.target_budget_seconds is not None and self.target_budget_seconds <= 0:
            raise ValueError(f"{self.kind}: target_budget_seconds must be positive or None")


#: The one table the roadmap asks for. Every `RunKind` member must appear
#: exactly once, which the assertion below enforces at import time: a sixth
#: run kind added to the contract without a matching profile fails loudly here
#: instead of silently falling back to some default deep in the scheduler.
RUN_PROFILES: Mapping[RunKind, RunProfile] = {
    RunKind.DAILY: RunProfile(
        kind=RunKind.DAILY,
        # The only kind that dueness-filters. "Due direct sources,
        # changed-only enrichment/scoring" only means something if DAILY
        # actually skips sources that are still fresh.
        dueness_filtered=True,
        target_budget_seconds=60.0,
        priority=Priority.NORMAL,
    ),
    RunKind.FULL_DIRECT: RunProfile(
        kind=RunKind.FULL_DIRECT,
        dueness_filtered=False,
        target_budget_seconds=300.0,
        priority=Priority.NORMAL,
    ),
    RunKind.AGGREGATORS: RunProfile(
        kind=RunKind.AGGREGATORS,
        dueness_filtered=False,
        # The roadmap states no time target for this kind ("independent and
        # non-blocking" is a failure-domain property, not a duration); JobSpy's
        # own 600s per-target deadline is already the practical ceiling.
        target_budget_seconds=None,
        priority=Priority.NORMAL,
    ),
    RunKind.LLM_REVIEW: RunProfile(
        kind=RunKind.LLM_REVIEW,
        dueness_filtered=False,
        target_budget_seconds=None,
        # "Optional low-priority review of changed eligible postings" -- the
        # roadmap's own words for this kind. Task 3.6 owns the review body;
        # this profile only records that it should never contend with a
        # DAILY/FULL_DIRECT run for priority.
        priority=Priority.LOW,
    ),
    RunKind.MANUAL_IMPORT: RunProfile(
        kind=RunKind.MANUAL_IMPORT,
        dueness_filtered=False,
        target_budget_seconds=None,
        priority=Priority.NORMAL,
    ),
}

_missing_profiles = [kind for kind in RunKind if kind not in RUN_PROFILES]
if _missing_profiles:  # pragma: no cover - guards future RunKind additions
    raise RuntimeError(f"RUN_PROFILES is missing entries for {_missing_profiles!r}")


def profile_for(kind: RunKind) -> RunProfile:
    return RUN_PROFILES[kind]


# --------------------------------------------------------------------------- #
# DAILY dueness
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class SkippedTarget:
    """One planned target `filter_due` excluded because it is not due yet.

    Carries enough evidence that a caller can explain the skip (Phase 4's
    "skipped (fresh)") without a second query back into `source_runs`.
    """

    source_run_key: str
    source_key: str
    instance_key: str
    label: str
    last_success_at: str | None
    age_seconds: float | None
    refresh_interval_seconds: int


def filter_due(
    plan: Sequence[tuple[SourceAdapter, SourceTarget]],
    last_success_at: Mapping[str, str | None],
    *,
    now: datetime,
) -> tuple[list[tuple[SourceAdapter, SourceTarget]], list[SkippedTarget]]:
    """Split a DAILY-eligible plan into due and not-yet-due, against `now`.

    Pure: every input is a parameter, and nothing here reads the clock or the
    database. `last_success_at` is keyed by `SourceTarget.source_run_key` and
    holds the ISO timestamp of that source instance's most recent SUCCEEDED
    run (any inventory scope -- a PARTIAL success still proves the source
    answered), exactly the `last_success_at` field
    `runstore.source_instance_freshness()` reports; the caller is expected to
    build this map from that function's rows rather than new SQL.

    A source with no entry, or an entry whose timestamp will not parse, has no
    successful history as far as this function is concerned and is always
    due -- the roadmap's explicit rule, and the only safe default: a source
    that has never proven it can complete must never be silently skipped.
    """
    due: list[tuple[SourceAdapter, SourceTarget]] = []
    skipped: list[SkippedTarget] = []
    for adapter, target in plan:
        key = target.source_run_key
        interval = adapter.descriptor.refresh_interval_seconds
        stamp = last_success_at.get(key)
        age = _age_seconds(stamp, now)
        if age is None or age >= interval:
            due.append((adapter, target))
            continue
        skipped.append(
            SkippedTarget(
                source_run_key=key,
                source_key=target.source_key,
                instance_key=target.instance_key,
                label=target.label,
                last_success_at=stamp,
                age_seconds=age,
                refresh_interval_seconds=interval,
            )
        )
    return due, skipped


def _age_seconds(stamp: str | None, now: datetime) -> float | None:
    """Seconds between an ISO timestamp and `now`, or `None` if unusable.

    An unparseable or missing timestamp folds into "no successful history",
    same as a genuinely absent one -- a malformed row must never be treated as
    fresher than it is.
    """
    if not stamp:
        return None
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=now.tzinfo)
    return max(0.0, (now - parsed).total_seconds())
