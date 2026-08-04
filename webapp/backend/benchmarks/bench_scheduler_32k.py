"""32K-row benchmark harness for the Phase 2 source scheduler (roadmap 2.6b).

Drives ~33,500 synthetic, deterministic postings spread across 15 fake
sources (one ~8K "large" source in the shape of a 40-board Greenhouse
account, the rest ranging 500-4,500 rows) through the REAL `Scheduler` +
`SqliteWriter` + `runstore` into a temporary, freshly migrated SQLite
database built with `backend.db.init_db` -- never `webapp/app.db`. Nothing
here touches the network: every adapter is a `FakeAdapter` (reused as-is
from `backend.tests.test_source_scheduler_fakes`, the module the scheduler
test suites already share) whose `fetch` body generates records from a
`random.Random` seeded on `(source_key, instance_key)`, so both the corpus
and every record's content hash are reproducible run over run and process
over process.

WHAT IS FAKE: the adapters' `plan`/`fetch` bodies (pure in-memory generators,
no I/O, no `TransportKind.HTTP`).
WHAT IS REAL: `Scheduler`, `SchedulerConfig` (production defaults, untouched),
`SqliteWriter`, `runstore`, and the on-disk schema/migrations via
`backend.db.init_db` -- the entire Phase 2 write path this benchmark exists
to guard.

It runs the corpus through the scheduler TWICE against the same database:

  1. "full_corpus_run" -- a fresh DB, every posting is new. This is the
     Phase 2 write-path baseline (batches, transactions, queue depth,
     wall time, rows/sec).
  2. "incremental_shaped_run" -- immediately after, the exact same plan is
     re-driven against the now-populated DB. The corpus did not change, so
     every posting's content hash is identical to run 1's; nothing new
     should be minted. This is the Phase 3 "changed-only" baseline anchor
     the roadmap asks for, measured today against the Phase 2 write path
     since scoring/description rescans do not exist yet.

Not a pytest test: it lives outside `testpaths` in the root `pyproject.toml`
(`["test_sweep_state.py", "webapp/backend/tests"]`), so `pytest -q` never
collects or runs it. Invoke directly:

    cd webapp && .venv-web/bin/python -m backend.benchmarks.bench_scheduler_32k

    # or, equivalently, from the repo root:
    webapp/.venv-web/bin/python webapp/backend/benchmarks/bench_scheduler_32k.py

Options:
    --out PATH     write the JSON metrics to PATH instead of stdout
    --keep-db      do not delete the temporary database afterward (its path
                   is printed to stderr for inspection)

Output is a single JSON object (see `run_benchmark`) carrying, for each of
the two runs: wall time, rows/sec (fetched and accepted), the writer's own
stats (transactions, records, max ops/transaction, max queue depth, busy
retries, dropped, drain timeouts), peak concurrency, and per-source /
overall attempt-duration spread (min/median/max) -- plus whole-process peak
RSS sampled after each run. It is deliberately just data: no threshold lives
here, so a later CI gate can diff two of these JSON blobs and decide what
"regression" means without this file changing.
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import platform
import random
import resource
import shutil
import statistics
import sqlite3
import sys
import tempfile
import time
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Make `backend.*` importable regardless of cwd: this file lives at
# webapp/backend/benchmarks/bench_scheduler_32k.py, so parents[2] is webapp/.
_WEBAPP_ROOT = Path(__file__).resolve().parents[2]
if str(_WEBAPP_ROOT) not in sys.path:
    sys.path.insert(0, str(_WEBAPP_ROOT))

from backend.sources.contract import (  # noqa: E402
    ExecutionMode,
    InventoryScope,
    RunKind,
    SourceCategory,
    SourceDescriptor,
    SourceTarget,
    TransportKind,
)
from backend.sources.scheduler import RunResult, Scheduler, SchedulerConfig  # noqa: E402
from backend.tests.test_source_scheduler_fakes import (  # noqa: E402
    FakeAdapter,
    make_connect,
    plan_of,
)

# --------------------------------------------------------------------------- #
# Deterministic synthetic content
# --------------------------------------------------------------------------- #
BASE_DATE = date(2026, 1, 1)  # fixed, not wall-clock-relative: reproducibility

TITLES = [
    "Support Engineer", "Customer Support Specialist", "Technical Support Engineer",
    "IT Support Analyst", "Platform Support Engineer", "Site Reliability Engineer",
    "Solutions Engineer", "Field Service Technician", "Help Desk Analyst",
    "Systems Administrator", "DevOps Engineer", "Cloud Support Engineer",
]
LOCATIONS = [
    "San Francisco, CA", "Remote", "New York, NY", "Austin, TX", "Seattle, WA",
    "Chicago, IL", "Boston, MA", "Denver, CO", "Atlanta, GA", "Remote - US",
]
COMPANY_ADJ = [
    "Northwind", "Vertex", "Bluepeak", "Redwood", "Ironclad", "Skyline", "Harbor",
    "Cobalt", "Lumen", "Fieldstone", "Anchor", "Meridian", "Silverline", "Granite",
    "Foxglove", "Cedarwood",
]
COMPANY_NOUN = [
    "Systems", "Robotics", "Health", "Analytics", "Labs", "Dynamics", "Networks",
    "Aerospace", "Biotech", "Logistics", "Security", "Cloud", "Materials", "Energy",
    "Media", "Financial",
]
LOREM_WORDS = (
    "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
    "incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis "
    "nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat"
).split()


def _seed_for(source_key: str, instance_key: str) -> int:
    """Stable cross-process seed. `hash()` is salted per-process; sha256 is not."""
    digest = hashlib.sha256(f"{source_key}:{instance_key}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _description(rng: random.Random, index: int) -> str:
    length = 40 + (index % 60)
    words = [rng.choice(LOREM_WORDS) for _ in range(length)]
    return " ".join(words).capitalize() + "."


def make_body(source_key: str, records_per_instance: int, *, description_inline: bool):
    """A deterministic, streamed, no-I/O `fetch` body -- the only thing faked."""

    async def _body(adapter, target: SourceTarget, ctx):
        rng = random.Random(_seed_for(source_key, target.instance_key))
        company = f"{rng.choice(COMPANY_ADJ)} {rng.choice(COMPANY_NOUN)}"
        for index in range(records_per_instance):
            title = rng.choice(TITLES)
            location = rng.choice(LOCATIONS)
            posted = BASE_DATE + timedelta(days=index % 90)
            remote = index % 3 == 0
            salary = "" if index % 5 == 0 else f"${80 + index % 60}k - ${130 + index % 60}k"
            description = _description(rng, index) if description_inline else None
            yield target.record(
                title=f"{title} {index}",
                company=company,
                url=f"https://{source_key}.example.test/{target.instance_key}/{index}",
                req_id=f"{source_key}-{target.instance_key}-{index}",
                location=location,
                posted_date=posted.isoformat(),
                salary_text=salary,
                remote=remote,
                description=description,
            )
            if index % 25 == 24:
                # Cooperative yield so 8-way concurrency actually interleaves
                # instead of one instance monopolising the loop -- the same
                # reason real adapters await a network call here.
                await asyncio.sleep(0)

    return _body


# --------------------------------------------------------------------------- #
# The corpus: a realistic source mix, >= 32,000 rows total
# --------------------------------------------------------------------------- #
@dataclasses.dataclass(frozen=True)
class CorpusSource:
    source_key: str
    category: SourceCategory
    inventory_scope: InventoryScope
    instances: int
    per_instance: int
    description_inline: bool = False

    @property
    def total(self) -> int:
        return self.instances * self.per_instance


CORPUS: tuple[CorpusSource, ...] = (
    # The one large ~8K source: shaped like a 40-board Greenhouse account.
    CorpusSource("greenhouse_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 40, 200),
    CorpusSource("lever_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 30, 150),
    CorpusSource("ashby_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 25, 140),
    CorpusSource("smartrecruiters_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 20, 150),
    CorpusSource("workable_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 20, 125),
    CorpusSource("recruitee_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 16, 125),
    CorpusSource("workday_fake", SourceCategory.DIRECT, InventoryScope.PARTIAL, 12, 150),
    CorpusSource("eightfold_fake", SourceCategory.DIRECT, InventoryScope.PARTIAL, 10, 150),
    CorpusSource("amazon_fake", SourceCategory.DIRECT, InventoryScope.PARTIAL, 10, 150),
    CorpusSource("icims_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 12, 100),
    CorpusSource("phenom_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 10, 100),
    CorpusSource("jibe_fake", SourceCategory.DIRECT, InventoryScope.COMPLETE, 8, 100, True),
    CorpusSource("builtin_fake", SourceCategory.AGGREGATOR, InventoryScope.PARTIAL, 8, 100),
    CorpusSource("yc_fake", SourceCategory.STARTUP_BOARD, InventoryScope.COMPLETE, 1, 500),
    CorpusSource("jobspy_fake", SourceCategory.AGGREGATOR, InventoryScope.PARTIAL, 1, 900, True),
)

TOTAL_ROWS = sum(c.total for c in CORPUS)
assert TOTAL_ROWS >= 32_000, f"corpus must be >= 32,000 rows, got {TOTAL_ROWS}"


def build_plan() -> list[tuple[FakeAdapter, SourceTarget]]:
    adapters = []
    for src in CORPUS:
        descriptor = SourceDescriptor(
            source_key=src.source_key,
            category=src.category,
            run_kinds=frozenset({RunKind.FULL_DIRECT}),
            refresh_interval_seconds=6 * 3600,
            # Generous relative to synthetic in-process generation, so the
            # benchmark measures throughput, not deadline enforcement.
            default_deadline_seconds=120.0,
            supports_checkpoint=False,
            execution=ExecutionMode.ASYNC_INPROCESS,
            transport=TransportKind.NONE,
            per_host_concurrency=4,
            min_request_interval_seconds=0.0,
            description_inline=src.description_inline,
            default_inventory_scope=src.inventory_scope,
        )
        adapter = FakeAdapter(
            src.source_key,
            instances=[f"inst-{i:03d}" for i in range(src.instances)],
            body=make_body(src.source_key, src.per_instance, description_inline=src.description_inline),
            descriptor=descriptor,
            host=f"{src.source_key}.example.test",
            inventory_scope=src.inventory_scope,
        )
        adapters.append(adapter)
    return plan_of(*adapters)


# --------------------------------------------------------------------------- #
# Measurement
# --------------------------------------------------------------------------- #
def peak_rss_mb() -> float:
    """Whole-process peak RSS since start, as a memory-growth proxy.

    `ru_maxrss` is monotonic non-decreasing for the process lifetime, so
    sampling it after run 1 and after run 2 shows whether the second pass
    grew the high-water mark -- a cheap unbounded-memory tripwire without a
    profiler. Units differ by platform: bytes on Darwin, KiB on Linux.
    """
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return round(usage / divisor, 2)


def fetch_report(connect, run_uid: str) -> dict:
    """The persisted `aggregate_report_json` for a run: the only place writer
    stats, peak concurrency, and the presence-pass summary are recorded."""
    conn = connect()
    try:
        row = conn.execute(
            "SELECT aggregate_report_json FROM pipeline_runs WHERE run_uid=?", (run_uid,)
        ).fetchone()
        if row is None or row["aggregate_report_json"] is None:
            return {}
        return json.loads(row["aggregate_report_json"])
    finally:
        conn.close()


def _presence_summary(presence: dict | None) -> dict | None:
    """The presence pass's own per-target breakdown (`presence["sources"]`) is
    223 entries long and duplicates what `duration_spread_by_source` already
    reports per source; drop it here so the JSON stays a few KB instead of a
    few MB, keeping only the run-level presence totals."""
    if presence is None:
        return None
    return {k: v for k, v in presence.items() if k != "sources"}


def _duration_stats(durations: Sequence[float]) -> dict:
    if not durations:
        return {"count": 0, "min_seconds": None, "median_seconds": None, "max_seconds": None}
    ordered = sorted(durations)
    return {
        "count": len(ordered),
        "min_seconds": round(ordered[0], 4),
        "median_seconds": round(statistics.median(ordered), 4),
        "max_seconds": round(ordered[-1], 4),
    }


def summarize_run(result: RunResult, elapsed: float, report: dict) -> dict:
    targets = result.targets
    rows_fetched = sum(t.fetched for t in targets)
    rows_accepted = sum(t.accepted for t in targets)
    rows_created = sum(t.created for t in targets)
    rows_duplicates = sum(t.duplicates for t in targets)

    by_source: dict[str, list[float]] = {}
    for t in targets:
        by_source.setdefault(t.source_key, []).append(t.duration_seconds)

    return {
        "run_uid": result.run_uid,
        "status": result.status,
        "wall_seconds": round(elapsed, 4),
        "targets_planned": len(targets),
        "targets_succeeded": sum(1 for t in targets if t.status == "succeeded"),
        "targets_failed": [t.source_run_key for t in targets if t.status in ("failed", "timeout")],
        "rows_fetched": rows_fetched,
        "rows_accepted": rows_accepted,
        "rows_created": rows_created,
        "rows_duplicates": rows_duplicates,
        "rows_per_second_fetched": round(rows_fetched / elapsed, 1) if elapsed > 0 else None,
        "rows_per_second_accepted": round(rows_accepted / elapsed, 1) if elapsed > 0 else None,
        "peak_concurrency": report.get("peak_concurrency"),
        "peak_by_host": report.get("peak_by_host"),
        "writer": report.get("writer"),
        "presence": _presence_summary(report.get("presence")),
        "duration_spread_overall": _duration_stats([t.duration_seconds for t in targets]),
        "duration_spread_by_source": {
            source: _duration_stats(durations) for source, durations in sorted(by_source.items())
        },
    }


async def run_once(connect, plan) -> tuple[RunResult, float]:
    scheduler = Scheduler(connect, config=SchedulerConfig())
    started = time.perf_counter()
    result = await scheduler.run(kind=RunKind.FULL_DIRECT, plan=plan, trigger="benchmark")
    elapsed = time.perf_counter() - started
    return result, elapsed


async def run_benchmark(*, keep_db: bool) -> dict:
    tmp_dir = Path(tempfile.mkdtemp(prefix="jobhunt-bench-scheduler-"))
    try:
        connect = make_connect(tmp_dir, name="bench.db")
        plan = build_plan()

        run1_result, run1_elapsed = await run_once(connect, plan)
        run1_report = fetch_report(connect, run1_result.run_uid)
        rss_after_run1 = peak_rss_mb()

        run2_result, run2_elapsed = await run_once(connect, plan)
        run2_report = fetch_report(connect, run2_result.run_uid)
        rss_after_run2 = peak_rss_mb()

        return {
            "benchmark": "bench_scheduler_32k",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "python_version": platform.python_version(),
            "sqlite_version": sqlite3.sqlite_version,
            "platform": platform.platform(),
            "corpus": {
                "sources": len(CORPUS),
                "total_rows": TOTAL_ROWS,
                "detail": [
                    {
                        "source_key": c.source_key,
                        "category": str(c.category),
                        "inventory_scope": str(c.inventory_scope),
                        "instances": c.instances,
                        "per_instance": c.per_instance,
                        "total": c.total,
                        "description_inline": c.description_inline,
                    }
                    for c in CORPUS
                ],
            },
            "scheduler_config": dataclasses.asdict(SchedulerConfig()),
            "db_path": str(connect.path) if keep_db else None,
            "peak_rss_mb_after_run1": rss_after_run1,
            "peak_rss_mb_after_run2": rss_after_run2,
            "peak_rss_mb_growth_run2_vs_run1": round(rss_after_run2 - rss_after_run1, 2),
            "full_corpus_run": summarize_run(run1_result, run1_elapsed, run1_report),
            "incremental_shaped_run": summarize_run(run2_result, run2_elapsed, run2_report),
        }
    finally:
        if keep_db:
            print(f"[bench_scheduler_32k] DB kept at: {tmp_dir}", file=sys.stderr)
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="32K-row benchmark for the source scheduler write path (see module docstring)."
    )
    parser.add_argument("--out", type=Path, default=None, help="write JSON metrics here instead of stdout")
    parser.add_argument(
        "--keep-db", action="store_true", help="do not delete the temp database; print its path to stderr"
    )
    args = parser.parse_args(argv)

    metrics = asyncio.run(run_benchmark(keep_db=args.keep_db))
    payload = json.dumps(metrics, indent=2, sort_keys=False)

    if args.out:
        args.out.write_text(payload + "\n")
        print(f"[bench_scheduler_32k] wrote {args.out}", file=sys.stderr)
    else:
        print(payload)


if __name__ == "__main__":
    main()
