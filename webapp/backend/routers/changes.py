"""Cross-run diff view: new / reposted / tier-changed / disappeared, by seen_key."""
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends

from ..db import get_db
from ..models import JOB_LIGHT_SQL, job_light_from_row, url_to_b64

router = APIRouter()


def _history_by_seen(conn: sqlite3.Connection, run_date: str):
    """seen_key -> {url, tier, odds} for one run (first url per seen_key wins)."""
    out = {}
    for r in conn.execute(
        "SELECT url, seen_key, tier, odds FROM job_history WHERE run_date=?", (run_date,)
    ).fetchall():
        out.setdefault(r["seen_key"], {"url": r["url"], "tier": r["tier"], "odds": r["odds"]})
    return out


def _job_light(conn: sqlite3.Connection, url: str):
    row = conn.execute(f"{JOB_LIGHT_SQL} WHERE j.url=?", (url,)).fetchone()
    return job_light_from_row(row) if row is not None else None


@router.get("/changes")
def changes(since: Optional[str] = None, conn: sqlite3.Connection = Depends(get_db)):
    runs = [r["run_date"] for r in conn.execute(
        "SELECT run_date FROM runs ORDER BY run_date").fetchall()]
    current = runs[-1] if runs else None
    if since and since in runs and since != current:
        baseline = since
    else:
        baseline = runs[-2] if len(runs) >= 2 else None

    empty = {"baseline": baseline, "current": current,
             "new": [], "reposted": [], "tier_changed": [], "disappeared": []}
    if current is None or baseline is None:
        return empty

    base = _history_by_seen(conn, baseline)
    curr = _history_by_seen(conn, current)
    base_keys = set(base)
    curr_keys = set(curr)

    # New: seen_key present now but not in the baseline.
    new_jobs = []
    for sk in curr_keys - base_keys:
        light = _job_light(conn, curr[sk]["url"])
        if light is not None:
            new_jobs.append(light)

    # Reposted: current jobs whose flags carry "reposted".
    reposted = []
    for r in conn.execute(
        f"{JOB_LIGHT_SQL} WHERE j.present=1 AND j.flags LIKE '%reposted%'").fetchall():
        reposted.append(job_light_from_row(r))

    # Tier changed: seen_key in both runs with a different tier.
    tier_changed = []
    for sk in curr_keys & base_keys:
        b_tier = base[sk]["tier"]
        c_tier = curr[sk]["tier"]
        if b_tier != c_tier:
            light = _job_light(conn, curr[sk]["url"])
            if light is not None:
                tier_changed.append({"job": light, "from": b_tier, "to": c_tier})

    # Disappeared: seen_key in baseline but not current. Baseline url joined to jobs
    # for display fields (may be null if that job never entered the jobs cache).
    disappeared = []
    for sk in base_keys - curr_keys:
        url = base[sk]["url"]
        jrow = conn.execute(
            "SELECT title, company, location FROM jobs WHERE url=?", (url,)).fetchone()
        disappeared.append({
            "url": url,
            "url_b64": url_to_b64(url),
            "title": jrow["title"] if jrow else None,
            "company": jrow["company"] if jrow else None,
            "location": jrow["location"] if jrow else None,
            "tier": base[sk]["tier"],
            "last_seen": baseline,
        })

    return {
        "baseline": baseline,
        "current": current,
        "new": new_jobs,
        "reposted": reposted,
        "tier_changed": tier_changed,
        "disappeared": disappeared,
    }
