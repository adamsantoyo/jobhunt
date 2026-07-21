"""Job listing + detail endpoints."""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..db import get_db
from ..models import JOB_JOIN_SQL, JOB_LIGHT_SQL, JobFull, b64_to_url, job_light_from_row

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
def list_jobs(conn: sqlite3.Connection = Depends(get_db)):
    rows = conn.execute(f"{JOB_LIGHT_SQL} WHERE j.present=1").fetchall()
    jobs = [job_light_from_row(r) for r in rows]
    return {"run_date": _latest_run(conn), "jobs": jobs}


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
