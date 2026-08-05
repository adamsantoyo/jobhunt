"""Aggregate analytics + freshness (run/source-health/sweep status)."""
import json
import sqlite3

from fastapi import APIRouter, Depends

from .. import canonical_reads, config, read_dispatch
from ..config import ACTIVE_STATUSES, ADVANCED_STATUSES, DEFAULT_COMP_BAND, STATUSES
from ..db import get_db
from ..models import today_iso
from ..sweeprunner import runner

router = APIRouter()

#: The competition axis (Phase 3.5): jobs.odds/job_history.odds now stores the
#: combined "<match> / <competition>" string rubric.hireability() emits (see
#: rubric._hireability_core). Analytics breaks it down by the competition half
#: only -- the axis this dashboard has always charted as 3 columns -- rather
#: than by the full combined string, which would fragment into up to 15
#: match x competition cells.
_COMPETITION = ["High competition", "Standard", "Lower bar"]


def _competition_of(odds: str | None) -> str | None:
    """Extract the competition-axis component from a stored odds value.

    Legacy single-word values (Likely/Target/Reach, written by a scorer older
    than Phase 3.5 and still present in jobs/job_history until the next sweep)
    have no " / " separator and return None -- excluded from the competition
    breakdown rather than mis-bucketed under a guessed column."""
    if not odds or " / " not in odds:
        return None
    return odds.split(" / ", 1)[1]


def _comp_band(conn: sqlite3.Connection) -> list[int]:
    row = conn.execute("SELECT value FROM app_settings WHERE key='comp_band'").fetchone()
    if row:
        try:
            band = json.loads(row["value"])
            if isinstance(band, list) and len(band) == 2:
                return [int(band[0]), int(band[1])]
        except Exception:
            pass
    return list(DEFAULT_COMP_BAND)


@router.get("/analytics")
def analytics(conn: sqlite3.Connection = Depends(get_db)):
    if config.READS_SOURCE == "canonical":
        read_dispatch.require_canonical(conn)
        return canonical_reads.analytics(conn)
    # Funnel: every present job counts under COALESCE(status,'New') — matching how the
    # kanban derives a card's column — plus dormant advanced states (an applied job
    # that vanished from the feed still counts in the pipeline).
    funnel = {}
    for r in conn.execute(
        "SELECT COALESCE(s.status, 'New') AS st, COUNT(*) AS c "
        "FROM jobs j LEFT JOIN job_state s ON j.url = s.url "
        "WHERE j.present = 1 GROUP BY st"
    ).fetchall():
        funnel[r["st"]] = r["c"]
    ph = ",".join("?" for _ in ADVANCED_STATUSES)
    for r in conn.execute(
        f"SELECT s.status AS st, COUNT(*) AS c FROM job_state s LEFT JOIN jobs j ON s.url = j.url "
        f"WHERE (j.url IS NULL OR j.present = 0) AND s.status IN ({ph}) GROUP BY s.status",
        tuple(ADVANCED_STATUSES),
    ).fetchall():
        funnel[r["st"]] = funnel.get(r["st"], 0) + r["c"]

    tiers = {str(r["tier"]): r["c"] for r in conn.execute(
        "SELECT tier, COUNT(*) AS c FROM jobs WHERE present=1 GROUP BY tier").fetchall()}

    # Distribution over the competition axis only, accumulated (not assigned):
    # several distinct combined odds values ("Strong match / Standard",
    # "Weak match / Standard", ...) share one competition bucket.
    odds = {o: 0 for o in _COMPETITION}
    for r in conn.execute(
        "SELECT odds, COUNT(*) AS c FROM jobs WHERE present=1 AND odds IS NOT NULL GROUP BY odds").fetchall():
        comp = _competition_of(r["odds"])
        if comp in odds:
            odds[comp] += r["c"]

    matrix = {str(t): {o: 0 for o in _COMPETITION} for t in range(5, 0, -1)}
    for r in conn.execute(
        "SELECT tier, odds, COUNT(*) AS c FROM jobs WHERE present=1 GROUP BY tier, odds").fetchall():
        tkey = str(r["tier"])
        comp = _competition_of(r["odds"])
        if tkey in matrix and comp in matrix[tkey]:
            matrix[tkey][comp] += r["c"]

    by_source = [
        {"source": r["source"], "kept": r["kept"], "with_desc": r["with_desc"]}
        for r in conn.execute(
            "SELECT source, COUNT(*) AS kept, "
            "SUM(CASE WHEN full_desc IS NOT NULL AND full_desc!='' THEN 1 ELSE 0 END) AS with_desc "
            "FROM jobs WHERE present=1 GROUP BY source ORDER BY kept DESC"
        ).fetchall()
    ]

    new_per_run = [
        {"run_date": r["run_date"], "kept": r["kept"], "new_this_run": r["new_this_run"]}
        for r in conn.execute(
            "SELECT run_date, kept, new_this_run FROM runs ORDER BY run_date").fetchall()
    ]

    # Comp histogram: $10k buckets over salary midpoints of present jobs.
    bucket_counts: dict[int, int] = {}
    for r in conn.execute(
        "SELECT salary_min, salary_max FROM jobs WHERE present=1 AND (salary_min IS NOT NULL OR salary_max IS NOT NULL)"
    ).fetchall():
        lo_v, hi_v = r["salary_min"], r["salary_max"]
        if lo_v is not None and hi_v is not None:
            mid = (lo_v + hi_v) / 2
        else:
            mid = lo_v if lo_v is not None else hi_v
        bucket = int(mid // 10000) * 10000
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    comp_buckets = [
        {"lo": b, "hi": b + 10000, "count": bucket_counts[b]} for b in sorted(bucket_counts)
    ]

    today = today_iso()
    active_ph = ",".join("?" for _ in ACTIVE_STATUSES)
    overdue = conn.execute(
        f"SELECT COUNT(*) AS c FROM job_state WHERE follow_up_date IS NOT NULL AND follow_up_date < ? "
        f"AND status IN ({active_ph})",
        (today, *ACTIVE_STATUSES),
    ).fetchone()["c"]
    upcoming = conn.execute(
        "SELECT COUNT(*) AS c FROM job_state WHERE follow_up_date IS NOT NULL AND follow_up_date >= ?",
        (today,),
    ).fetchone()["c"]

    return {
        "funnel": funnel,
        "tiers": tiers,
        "odds": odds,
        "matrix": matrix,
        "by_source": by_source,
        "new_per_run": new_per_run,
        "comp": {"buckets": comp_buckets, "band": _comp_band(conn)},
        "followups": {"overdue": overdue, "upcoming": upcoming},
        "statuses": STATUSES,
    }


@router.get("/freshness")
def freshness(conn: sqlite3.Connection = Depends(get_db)):
    if config.READS_SOURCE == "canonical":
        read_dispatch.require_canonical(conn)
        return canonical_reads.freshness(conn)
    row = conn.execute(
        "SELECT run_date, ingested_at, kept, new_this_run, source_health_json, report_json "
        "FROM runs ORDER BY run_date DESC LIMIT 1"
    ).fetchone()

    sources = []
    zero_row_sources: list = []
    stale_refresh_sources: list = []
    latest_run = ingested_at = None
    kept = new_this_run = None

    if row is not None:
        latest_run = row["run_date"]
        ingested_at = row["ingested_at"]
        kept = row["kept"]
        new_this_run = row["new_this_run"]
        if row["source_health_json"]:
            try:
                sh = json.loads(row["source_health_json"])
                for name, meta in (sh or {}).items():
                    sources.append({
                        "name": name,
                        "rows": meta.get("rows"),
                        "refreshed": meta.get("refreshed"),  # may be absent -> None
                        "at": meta.get("at"),
                    })
            except Exception:
                pass
        if row["report_json"]:
            try:
                rep = json.loads(row["report_json"])
                zero_row_sources = rep.get("zero_row_sources", []) or []
                stale_refresh_sources = rep.get("stale_refresh_sources", []) or []
            except Exception:
                pass

    return {
        "latest_run": latest_run,
        "ingested_at": ingested_at,
        "kept": kept,
        "new_this_run": new_this_run,
        "sources": sources,
        "zero_row_sources": zero_row_sources,
        "stale_refresh_sources": stale_refresh_sources,
        "sweep": runner.status(),
    }
