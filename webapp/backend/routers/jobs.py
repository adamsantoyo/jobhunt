"""Job listing + detail endpoints."""
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from ..config import ACTIVE_STATUSES
from ..db import get_db
from ..models import (
    JOB_JOIN_SQL,
    JOB_LIGHT_SQL,
    JobFull,
    b64_to_url,
    date_plus,
    job_light_from_row,
    today_iso,
)

router = APIRouter()


def _latest_run(conn: sqlite3.Connection):
    row = conn.execute("SELECT MAX(run_date) AS d FROM runs").fetchone()
    return row["d"] if row else None


def _load_skills(conn: sqlite3.Connection) -> list[str]:
    row = conn.execute("SELECT value FROM app_settings WHERE key='skills'").fetchone()
    if not row:
        return []
    import json
    try:
        val = json.loads(row["value"])
        return [str(s) for s in val] if isinstance(val, list) else []
    except Exception:
        return []


@router.get("/jobs")
def list_jobs(min_tier: Optional[int] = None, conn: sqlite3.Connection = Depends(get_db)):
    sql = f"{JOB_LIGHT_SQL} WHERE j.present=1"
    params: tuple = ()
    if min_tier is not None:
        sql += " AND j.tier >= ?"
        params = (min_tier,)
    rows = conn.execute(sql, params).fetchall()
    jobs = [job_light_from_row(r) for r in rows]
    return {"run_date": _latest_run(conn), "jobs": jobs}


@router.get("/followups")
def followups(conn: sqlite3.Connection = Depends(get_db)):
    """Active-status jobs with a follow_up_date set, split into overdue (past due)
    and upcoming (due within the next 14 days), each ordered soonest-first. Items are
    full JobLight+state, so this only covers roles whose job row is still present --
    a role that both went dormant AND has a stale follow-up date has nothing live to
    render here; it's still counted in analytics' plain follow-up totals."""
    today = today_iso()
    horizon = date_plus(14)
    ph = ",".join("?" for _ in ACTIVE_STATUSES)
    rows = conn.execute(
        f"{JOB_LIGHT_SQL} WHERE j.present=1 AND s.follow_up_date IS NOT NULL "
        f"AND s.status IN ({ph}) ORDER BY s.follow_up_date ASC",
        tuple(ACTIVE_STATUSES),
    ).fetchall()
    overdue, upcoming = [], []
    for r in rows:
        fud = r["follow_up_date"]
        job = job_light_from_row(r)
        if fud < today:
            overdue.append(job)
        elif fud <= horizon:
            upcoming.append(job)
    return {"overdue": overdue, "upcoming": upcoming}


@router.get("/jobs/{url_b64}", response_model=JobFull)
def job_detail(url_b64: str, conn: sqlite3.Connection = Depends(get_db)):
    try:
        url = b64_to_url(url_b64)
    except Exception:
        raise HTTPException(status_code=404, detail="unknown job")
    row = conn.execute(f"{JOB_JOIN_SQL} WHERE j.url=?", (url,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="unknown job")

    light = job_light_from_row(row)
    full_desc = row["full_desc"]
    haystack = ((full_desc or "") + "\n" + (row["desc_snippet"] or "")).lower()
    skill_hits = []
    for skill in _load_skills(conn):
        s = skill.strip().lower()
        if s and s in haystack and skill not in skill_hits:
            skill_hits.append(skill)

    return JobFull(**light.model_dump(), full_desc=full_desc, skill_hits=skill_hits)
