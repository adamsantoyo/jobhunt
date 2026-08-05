"""Source ops read model + single-source retry (Phase 4.4, wave-2 contract 8).

Two endpoints:

  GET  /api/sources/ops           one row per source instance (a `source_runs.source`
                                   value -- `SourceTarget.source_run_key`, e.g.
                                   `"greenhouse:acme"`), shaped exactly per the
                                   orchestrator's pinned contract (spec decision 8) so
                                   the frontend built against it concurrently needs no
                                   coordination with this file.
  POST /api/sources/{source}/retry a single-source run via `RunService.retry_source`.

WHERE EACH FIELD COMES FROM

`last_success_at` / `age_seconds` / `stale` / `licenses_absence` come straight out of
`runstore.source_instance_freshness()` -- that function already computes them correctly
(RUN-level, not attempt-level: see its docstring), so this router reads its output keys
and maps them rather than re-deriving the same rule a second way.

`consecutive_failures` (and therefore `circuit_open`, which is just a threshold on it)
is DELIBERATELY NOT `source_instance_freshness`'s `consecutive_failed_runs`, even
though the two started out identical. That field counts every non-succeeded run
backward from the newest, which means a run whose fetch was CANCELLED (a user hit
"cancel") or INTERRUPTED (the process died mid-run, reconciled by
`scheduler.recover_orphans`) counts exactly the same as a run that genuinely FAILED.
Three user cancels in a row then paints `circuit_open` on a source that never once
actually failed a fetch (wave-2 review finding 4) -- a false alarm the operator has no
way to distinguish from a real one. So this module computes its OWN consecutive-failure
count in `_consecutive_failures`, over the same `_attempt_rows` scan `_source_metrics`
already needs: newest run backward, counting only a run whose final fetch attempt is
`failed` or `timeout`; a run whose final attempt is `cancelled` or `interrupted` is
SKIPPED -- neither counted as a failure nor treated as a break in the streak -- and a
run with any succeeded attempt still stops the count, exactly as
`source_instance_freshness` does. `source_instance_freshness`'s own
`consecutive_failed_runs` is left untouched (other callers may want the RUN-level
"was this source's most recent history uninterrupted good" reading); this DTO field
is a considered divergence, not a bug fix applied inconsistently.

Everything else -- `p50_duration_seconds` / `p95_duration_seconds` / `last_rows` /
`median_rows` / `row_anomaly` / `last_failure_at` / `last_error` / `consecutive_failures`
-- is NOT something `source_instance_freshness` carries (it is a per-RUN freshness view;
these are per-ATTEMPT statistics), so `_attempt_rows` runs one more scan of
`source_runs` and `_source_metrics` derives them. `p50`/`p95` are deliberately computed
over `started_at -> finished_at` on the ATTEMPT row, never `TargetResult.duration_seconds`
(which includes gate-queue wait) -- this is what roadmap open item 6 asked for.

`last_error` is a compact DISPLAY STRING on the wire, never the parsed `error_json`
object: `"{type}: {message}"` when both are present, `message` alone when `type` is
missing, the raw string unchanged when `error_json` was already bare text, and `None`
when there is no failure to report. See `_format_error`.

A `ConfigError` raised by `registry.plan_run()` while `_configured_categories` asks the
registry what it would plan today (an UNRELATED source's bad configuration -- a
`SourceTarget` naming the wrong `source_key`, for instance) must not 500 this whole
endpoint over one source's mistake. `_ops_sync` catches it, degrades to history alone
(`configured = {}`, so every row's category falls back to `_category_fallback`, which
never calls `.plan()` and is unaffected), and reports the error as a top-level add-only
`config_error: str | None` field rather than swallowing it silently.

EQP: NEITHER scan can use `idx_source_runs_run_status` (`(run_uid, status)`) -- both
need every row for a given `source` across every `run_uid`, and no index on this table
covers `source` alone (only `(run_uid, status)` and the `(run_uid, source, step,
attempt)`/`(source_run_id, run_uid)` UNIQUE constraints, per migrations.py). Adding one
is out of scope this wave (no migrations). Both scans are therefore full scans of
`source_runs`, exactly the shape `source_instance_freshness` already accepts for the
same reason (see its own call site) -- acceptable because this is a personal job
search's database, not a multi-tenant one: `source_runs` grows by (targets planned) per
run kind invocation, on the order of dozens of sources times a handful of attempts per
run, so even years of daily history sit in the tens of thousands of rows, not a size a
full table scan on a local SQLite file need worry about for an operator-facing panel
that is not on any hot path. If this ever becomes a real cost, the fix is an index on
`source_runs(source, requested_at)`, deferred to whichever wave needs it.
"""
from __future__ import annotations

import asyncio
import json
import math
import sqlite3
import statistics
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from ..runservice import CanonicalSchemaUnavailable, RunConflict, RunService, UnknownSource
from ..sources import registry as registry_module
from ..sources.adapters import install as install_adapters
from ..sources.contract import ConfigError, RunKind
from ..sources.runstore import SOURCE_RUN_STEP, source_instance_freshness, utc_now_iso
from .runsapi import get_run_service

router = APIRouter()

#: Attempts considered per source when computing duration percentiles and row
#: statistics -- bounds the cost of the per-source aggregation to a fixed amount of
#: work regardless of how much history a source has accumulated, and keeps a source
#: that failed for a month from single-handedly blowing out the recent-behaviour
#: numbers with a mountain of old data. "Recent" is defined by
#: `COALESCE(finished_at, started_at, requested_at)` descending -- see `_attempt_rows`.
DURATION_WINDOW = 20

#: `row_anomaly.flag` requires the window's median to be at least this many accepted
#: rows before ratio is trusted at all -- a source that normally returns 2 postings
#: flipping to 1 is not an anomaly worth surfacing, it is noise.
ANOMALY_MIN_MEDIAN_ROWS = 10
#: Below/above these ratios (last successful attempt's accepted_count / the window's
#: median), with the median floor above satisfied, `row_anomaly.flag` is True.
ANOMALY_LOW_RATIO = 0.5
ANOMALY_HIGH_RATIO = 2.0

#: `circuit_open` is `consecutive_failures >= CIRCUIT_OPEN_THRESHOLD`. Display-only
#: this wave -- nothing refuses to schedule a source because of it; see spec decision 8.
CIRCUIT_OPEN_THRESHOLD = 3

#: Attempt statuses that count as "not a success" for `last_failure_at`/`last_error`.
#: Excludes `succeeded` (obviously) and `running` (still in flight -- not evidence of
#: failure yet; a run left `running` by a dead process is reconciled to `interrupted`
#: at startup by `scheduler.recover_orphans`, at which point it lands here).
_FAILURE_STATUSES = frozenset({"failed", "timeout", "cancelled", "interrupted"})

#: The run kinds a source can be currently configured under. `DAILY`, not
#: `RunService._RETRY_CANDIDATE_KINDS`'s `FULL_DIRECT` (that tuple resolves a
#: retry's own kind, and deliberately avoids `DAILY` because it is the one
#: dueness-filtered kind -- see its docstring): every DIRECT/STARTUP_BOARD
#: adapter declares `run_kinds={DAILY, FULL_DIRECT}` together (checked against
#: every adapter module), so the two kinds are IDENTICAL for the purpose this
#: tuple serves -- "which sources would the registry plan at all" -- and
#: dueness-filtering is irrelevant to a category lookup. `DAILY` reads as the
#: more natural "what is this source normally run under" question. Trying both
#: `DAILY` and `AGGREGATORS` is how `category` is found for every source this
#: config could reach, without guessing a category from the source-key prefix.
_CONFIGURED_KINDS: tuple[RunKind, ...] = (RunKind.DAILY, RunKind.AGGREGATORS)


def _schema_gap(exc: BaseException) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=f"canonical run schema is not available on this database: {exc}",
    )


# --------------------------------------------------------------------------- #
# category: what the registry would currently plan, plus a fallback for a
# source that only exists in history any more (its company was removed from
# config.json, or its adapter key is no longer registered).
# --------------------------------------------------------------------------- #
def _configured_categories(service: RunService) -> dict[str, str]:
    """`source_run_key -> category` for every target the current config would plan.

    Installs the real adapters (idempotent -- see `sources/adapters/__init__.py`,
    which also does this at import time) and asks the registry directly, the same
    way `RunService._resolve_retry_target` does, rather than guessing a category
    from the source-key prefix. `manual` (`MANUAL_IMPORT`) is deliberately never
    planned here: a push source has no fetch step, so it cannot appear in
    `source_runs` under `step='fetch'` at all, and a category guess it cannot
    support would be actively misleading.
    """
    install_adapters()
    source_config = service.source_config()
    out: dict[str, str] = {}
    for kind in _CONFIGURED_KINDS:
        for adapter, target in registry_module.plan_run(source_config, kind):
            out[target.source_run_key] = str(adapter.descriptor.category)
    return out


def _category_fallback(source: str) -> str | None:
    """A category for a source no longer in the current plan, if its adapter still
    exists. `source_run_key` is `f"{source_key}:{instance_key}"` (or bare
    `source_key` with no instance) -- see `SourceTarget.source_run_key` -- and no
    `source_key` in this codebase contains a colon (checked against every adapter
    module), so splitting on the first one recovers it."""
    source_key = source.split(":", 1)[0]
    try:
        return str(registry_module.get(source_key).descriptor.category)
    except Exception:  # noqa: BLE001 - "no adapter" in any shape reads as unknown
        return None


# --------------------------------------------------------------------------- #
# The attempt-level scan (durations, rows, last failure) -- see module docstring.
# --------------------------------------------------------------------------- #
def _load_json(blob: Any) -> Any:
    if blob is None:
        return None
    try:
        return json.loads(blob)
    except (TypeError, ValueError):
        return blob


def _parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _recency_key(row: dict[str, Any]) -> str:
    return row.get("finished_at") or row.get("started_at") or row.get("requested_at") or ""


def _attempt_rows(conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    """Every fetch-step attempt, grouped by source, newest first.

    ONE full scan of `source_runs` (`step='fetch'`, every status, every run) -- see
    the module docstring's EQP note for why no index serves this and why the scan
    is acceptable here. `step=SOURCE_RUN_STEP` excludes `UNATTEMPTED_SOURCE_RUN_STEP`
    rows by construction (a target cancelled before it ever attempted a fetch is not
    an "attempt" for duration/row purposes).
    """
    rows = conn.execute(
        "SELECT source, run_uid, attempt, status, started_at, finished_at, "
        "requested_at, accepted_count, error_json FROM source_runs WHERE step=?",
        (SOURCE_RUN_STEP,),
    ).fetchall()
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_source.setdefault(row["source"], []).append(dict(row))
    for bucket in by_source.values():
        bucket.sort(key=_recency_key, reverse=True)
    return by_source


def _duration_seconds(row: dict[str, Any]) -> float | None:
    started, finished = row.get("started_at"), row.get("finished_at")
    if not started or not finished:
        return None
    try:
        return max(0.0, (_parse_instant(finished) - _parse_instant(started)).total_seconds())
    except ValueError:
        return None


def _format_error(blob: Any) -> str | None:
    """`error_json` -> a compact DISPLAY STRING (fix 4.4 wave-2 review finding 2).

    The pinned contract's `last_error` is a string, not the `{"type":...,
    "message":...}` dict `error_json` actually stores (`SourceError.to_json_dict`)
    -- a frontend that renders it as text (per spec decision 10: SSE/API payload
    text is rendered as text nodes, never interpolated) needs a string to render,
    not an object to reach into.

      {"type": T, "message": M}  -> "T: M"
      {"message": M} (no type)   -> M
      a bare JSON string          -> that string, unchanged
      anything else / absent      -> None
    """
    parsed = _load_json(blob)
    if parsed is None:
        return None
    if isinstance(parsed, str):
        return parsed
    if isinstance(parsed, dict):
        error_type = parsed.get("type")
        message = parsed.get("message")
        if error_type and message:
            return f"{error_type}: {message}"
        if message:
            return str(message)
        if error_type:
            return str(error_type)
    return str(parsed)


def _consecutive_failures(attempts: list[dict[str, Any]]) -> int:
    """Consecutive FAILED/TIMEOUT runs for one source, newest run backward,
    stopping at the first run with a succeeded attempt (fix 4.4 wave-2 review
    finding 4 -- see the module docstring for why this diverges on purpose from
    `source_instance_freshness`'s `consecutive_failed_runs`).

    `attempts` is one source's fetch-step rows, already newest-attempt-first
    (`_attempt_rows`). Grouped here by `run_uid` (a run can carry more than one
    attempt for a source when it retried); each run's OUTCOME is decided by
    its final (highest-`attempt`) row, except that any attempt in the run
    succeeding makes the whole run a success regardless of what a later retry
    did -- the same "any attempt succeeded" rule `source_instance_freshness`
    applies, so the "stop at the first success" behaviour matches exactly.

    A run whose final attempt is `cancelled` or `interrupted` is SKIPPED: it
    neither increments the count nor breaks the streak, so a good run behind a
    cancelled one is still found and the streak still stops there. Only
    `failed`/`timeout` finals increment the count.
    """
    runs: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for row in attempts:  # newest attempt first
        run_uid = row["run_uid"]
        if run_uid not in runs:
            runs[run_uid] = []
            order.append(run_uid)
        runs[run_uid].append(row)

    count = 0
    for run_uid in order:  # first-seen == newest, since `attempts` is newest-first
        run_attempts = runs[run_uid]
        if any(r["status"] == "succeeded" for r in run_attempts):
            break
        final = max(run_attempts, key=lambda r: r["attempt"])
        if final["status"] in ("failed", "timeout"):
            count += 1
        # "cancelled" / "interrupted" / anything else: skip -- neither counted
        # nor a stop, per the module docstring's divergence from
        # `source_instance_freshness`.
    return count


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile ("inclusive" method, `pct` in `[0, 100]`).

    The same algorithm `numpy.percentile`'s default uses, so a value here agrees
    with a spot-check run outside this codebase. `sorted_values` must already be
    sorted ascending and non-empty.
    """
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100)
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)


def _source_metrics(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    """Everything `source_instance_freshness` does not carry, for one source's
    attempt rows (already newest-first -- see `_attempt_rows`)."""
    failure = next((r for r in attempts if r["status"] in _FAILURE_STATUSES), None)
    last_failure_at = _recency_key(failure) or None if failure is not None else None
    last_error = _format_error(failure["error_json"]) if failure is not None else None
    consecutive_failures = _consecutive_failures(attempts)

    #: "the window": the most recent `DURATION_WINDOW` SUCCESSFUL attempts,
    #: regardless of whether they carry both timestamps. Durations further narrow
    #: to the subset that does (spec: "exclude ... rows without both timestamps");
    #: `last_rows`/`median_rows` do not need a duration to be meaningful.
    window = [r for r in attempts if r["status"] == "succeeded"][:DURATION_WINDOW]
    durations = sorted(
        d for d in (_duration_seconds(r) for r in window) if d is not None
    )
    p50 = _percentile(durations, 50) if durations else None
    p95 = _percentile(durations, 95) if durations else None

    row_counts = [r["accepted_count"] for r in window if r.get("accepted_count") is not None]
    last_rows = window[0].get("accepted_count") if window else None
    median_rows = statistics.median(row_counts) if row_counts else None

    ratio: float | None = None
    flag = False
    if last_rows is not None and median_rows is not None and median_rows > 0:
        ratio = last_rows / median_rows
        if median_rows >= ANOMALY_MIN_MEDIAN_ROWS and (
            ratio < ANOMALY_LOW_RATIO or ratio > ANOMALY_HIGH_RATIO
        ):
            flag = True

    return {
        "last_failure_at": last_failure_at,
        "last_error": last_error,
        "consecutive_failures": consecutive_failures,
        "p50": p50,
        "p95": p95,
        "last_rows": last_rows,
        "median_rows": median_rows,
        "row_anomaly": {"flag": flag, "ratio": ratio},
    }


def _empty_metrics() -> dict[str, Any]:
    """A fresh dict every call -- `row_anomaly` is nested and mutable, so a module-
    level constant reused across rows would let one row's caller mutate it and
    silently corrupt every other empty-history row's output."""
    return {
        "last_failure_at": None,
        "last_error": None,
        "consecutive_failures": 0,
        "p50": None,
        "p95": None,
        "last_rows": None,
        "median_rows": None,
        "row_anomaly": {"flag": False, "ratio": None},
    }


def _row_for(
    source: str,
    category: str | None,
    fresh: dict[str, Any] | None,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    #: `consecutive_failures` (and `circuit_open`, derived from it) comes from
    #: `metrics` -- this router's OWN `_consecutive_failures`, over `source_runs`
    #: directly -- never from `fresh["consecutive_failed_runs"]`. See the module
    #: docstring for why the two are deliberately different rules.
    if fresh is None:
        last_success_at = None
        age_seconds = None
        stale = None
        licenses_absence = False
    else:
        last_success_at = fresh["last_success_at"]
        age_seconds = fresh["age_seconds"]
        stale = bool(fresh["stale"])
        licenses_absence = bool(fresh["licenses_absence"])
    consecutive_failures = metrics["consecutive_failures"]
    return {
        "source": source,
        "category": category,
        "last_success_at": last_success_at,
        "last_failure_at": metrics["last_failure_at"],
        "age_seconds": age_seconds,
        "stale": stale,
        "consecutive_failures": consecutive_failures,
        "p50_duration_seconds": metrics["p50"],
        "p95_duration_seconds": metrics["p95"],
        "last_rows": metrics["last_rows"],
        "median_rows": metrics["median_rows"],
        "row_anomaly": metrics["row_anomaly"],
        "circuit_open": consecutive_failures >= CIRCUIT_OPEN_THRESHOLD,
        "last_error": metrics["last_error"],
        "licenses_absence": licenses_absence,
    }


def _ops_sync(service: RunService) -> dict[str, Any]:
    config_error: str | None = None
    try:
        configured = _configured_categories(service)
    except ConfigError as exc:
        # An unrelated source's bad configuration must not 500 the whole panel
        # (fix 4.4 wave-2 review finding 5) -- degrade to history alone. Every
        # row `source_instance_freshness`/`_attempt_rows` already knows about
        # still renders; `_category_fallback` (per-source, never calls
        # `.plan()`) is unaffected, so only sources that would ONLY have been
        # found via `configured` (never run, not in this history) go missing.
        configured = {}
        config_error = str(exc)
    conn = service.connect()
    try:
        conn.row_factory = sqlite3.Row
        freshness_by_source = {row["source"]: row for row in source_instance_freshness(conn)}
        attempts_by_source = _attempt_rows(conn)
    finally:
        conn.close()

    all_sources = sorted(set(configured) | set(freshness_by_source) | set(attempts_by_source))
    sources = [
        _row_for(
            source,
            configured.get(source) or _category_fallback(source),
            freshness_by_source.get(source),
            _source_metrics(attempts_by_source[source])
            if source in attempts_by_source
            else _empty_metrics(),
        )
        for source in all_sources
    ]
    return {"sources": sources, "generated_at": utc_now_iso(), "config_error": config_error}


@router.get("/sources/ops")
async def sources_ops(service: RunService = Depends(get_run_service)) -> Any:
    try:
        await service.require_canonical_schema()
    except CanonicalSchemaUnavailable as exc:
        raise _schema_gap(exc) from None
    return await asyncio.to_thread(_ops_sync, service)


# --------------------------------------------------------------------------- #
# POST /api/sources/{source}/retry
# --------------------------------------------------------------------------- #
@router.post("/sources/{source}/retry")
async def retry_source(source: str, service: RunService = Depends(get_run_service)) -> JSONResponse:
    try:
        started = await service.retry_source(source)
    except UnknownSource as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    except CanonicalSchemaUnavailable as exc:
        raise _schema_gap(exc) from None
    except RunConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return JSONResponse(
        {"run_uid": started["run_uid"], "source": started["source"], "kind": started["kind"]},
        status_code=202,
    )
