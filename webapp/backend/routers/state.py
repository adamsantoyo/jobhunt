"""User-owned state: job_state edits, quick actions, company notes, review list."""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..config import STATUSES
from ..db import get_db
from ..models import (
    JOB_JOIN_SQL,
    CompanyPatch,
    CompanyState,
    JobLight,
    JobState,
    QuickAction,
    StatePatch,
    b64_to_url,
    date_plus,
    job_light_from_row,
    now_iso,
    today_iso,
    url_to_b64,
)

router = APIRouter()

_STATE_DEFAULTS = {
    "status": "New", "notes": "", "follow_up_date": None, "applied_date": None,
    "starred": 0, "hidden": 0, "contact": "", "snoozed_until": None,
    "needs_review": 0, "review_reason": None,
}


def _state_dto(conn: sqlite3.Connection, url: str) -> JobState:
    row = conn.execute("SELECT * FROM job_state WHERE url=?", (url,)).fetchone()
    return JobState(
        status=row["status"],
        notes=row["notes"] or "",
        follow_up_date=row["follow_up_date"],
        applied_date=row["applied_date"],
        starred=bool(row["starred"]),
        hidden=bool(row["hidden"]),
        contact=row["contact"] or "",
        snoozed_until=row["snoozed_until"],
        needs_review=bool(row["needs_review"]),
        review_reason=row["review_reason"],
        updated_at=row["updated_at"],
    )


def _resolve_seen_key(conn: sqlite3.Connection, url: str):
    """seen_key for a state row: prefer existing state, then the jobs cache."""
    row = conn.execute("SELECT seen_key FROM job_state WHERE url=?", (url,)).fetchone()
    if row:
        return row["seen_key"]
    row = conn.execute("SELECT seen_key FROM jobs WHERE url=?", (url,)).fetchone()
    return row["seen_key"] if row else None


def _apply_state(conn: sqlite3.Connection, url: str, changes: dict) -> JobState:
    """Merge `changes` onto the existing (or default) state row and upsert.
    Every user edit clears needs_review. Applied with no applied_date -> today."""
    existing = conn.execute("SELECT * FROM job_state WHERE url=?", (url,)).fetchone()
    base = dict(_STATE_DEFAULTS)
    seen_key = _resolve_seen_key(conn, url) or ""
    if existing:
        for k in _STATE_DEFAULTS:
            base[k] = existing[k]
        seen_key = existing["seen_key"] or seen_key
    base.update(changes)

    if base.get("status") == "Applied" and not base.get("applied_date"):
        base["applied_date"] = today_iso()

    # A user edit always clears the review flag (never re-flag from an edit).
    base["needs_review"] = 0
    base["review_reason"] = None
    base["starred"] = 1 if base["starred"] else 0
    base["hidden"] = 1 if base["hidden"] else 0

    conn.execute(
        "INSERT INTO job_state (url, seen_key, status, notes, follow_up_date, applied_date, starred, hidden, "
        "contact, snoozed_until, needs_review, review_reason, updated_at) "
        "VALUES (:url,:seen_key,:status,:notes,:follow_up_date,:applied_date,:starred,:hidden,:contact,"
        ":snoozed_until,:needs_review,:review_reason,:updated_at) "
        "ON CONFLICT(url) DO UPDATE SET seen_key=excluded.seen_key, status=excluded.status, notes=excluded.notes, "
        "follow_up_date=excluded.follow_up_date, applied_date=excluded.applied_date, starred=excluded.starred, "
        "hidden=excluded.hidden, contact=excluded.contact, snoozed_until=excluded.snoozed_until, "
        "needs_review=excluded.needs_review, review_reason=excluded.review_reason, updated_at=excluded.updated_at",
        {"url": url, "seen_key": seen_key, **base, "updated_at": now_iso()},
    )
    conn.commit()
    return _state_dto(conn, url)


def _decode_or_404(url_b64: str) -> str:
    try:
        return b64_to_url(url_b64)
    except Exception:
        raise HTTPException(status_code=404, detail="unknown job")


def _known(conn: sqlite3.Connection, url: str) -> bool:
    if conn.execute("SELECT 1 FROM jobs WHERE url=?", (url,)).fetchone():
        return True
    return conn.execute("SELECT 1 FROM job_state WHERE url=?", (url,)).fetchone() is not None


@router.patch("/jobs/{url_b64}/state", response_model=JobState)
def patch_state(url_b64: str, body: StatePatch, conn: sqlite3.Connection = Depends(get_db)):
    url = _decode_or_404(url_b64)
    if not _known(conn, url):
        raise HTTPException(status_code=404, detail="unknown job")
    changes = body.model_dump(exclude_unset=True)
    if "status" in changes and changes["status"] not in STATUSES:
        raise HTTPException(status_code=422, detail=f"invalid status: {changes['status']}")
    return _apply_state(conn, url, changes)


@router.post("/jobs/{url_b64}/quick", response_model=JobState)
def quick_action(url_b64: str, body: QuickAction, conn: sqlite3.Connection = Depends(get_db)):
    url = _decode_or_404(url_b64)
    if not _known(conn, url):
        raise HTTPException(status_code=404, detail="unknown job")
    action = body.action
    if action == "applied":
        changes = {"status": "Applied"}  # applied_date auto-filled by _apply_state
    elif action == "snooze":
        days = body.days if body.days is not None else 3
        changes = {"snoozed_until": date_plus(days)}
    elif action == "pass":
        changes = {"status": "Passed"}
    elif action == "star":
        changes = {"starred": 1}
    elif action == "unstar":
        changes = {"starred": 0}
    else:
        raise HTTPException(status_code=422, detail=f"unknown action: {action}")
    return _apply_state(conn, url, changes)


@router.get("/companies", response_model=list[CompanyState])
def list_companies(conn: sqlite3.Connection = Depends(get_db)):
    rows = conn.execute("SELECT * FROM company_state ORDER BY company").fetchall()
    return [
        CompanyState(company=r["company"], contact=r["contact"] or "", notes=r["notes"] or "",
                     updated_at=r["updated_at"])
        for r in rows
    ]


@router.patch("/companies/{company}", response_model=CompanyState)
def patch_company(company: str, body: CompanyPatch, conn: sqlite3.Connection = Depends(get_db)):
    existing = conn.execute("SELECT * FROM company_state WHERE company=?", (company,)).fetchone()
    contact = existing["contact"] if existing else ""
    notes = existing["notes"] if existing else ""
    changes = body.model_dump(exclude_unset=True)
    if "contact" in changes:
        contact = changes["contact"] or ""
    if "notes" in changes:
        notes = changes["notes"] or ""
    now = now_iso()
    conn.execute(
        "INSERT INTO company_state (company, contact, notes, updated_at) VALUES (?,?,?,?) "
        "ON CONFLICT(company) DO UPDATE SET contact=excluded.contact, notes=excluded.notes, updated_at=excluded.updated_at",
        (company, contact, notes, now),
    )
    conn.commit()
    return CompanyState(company=company, contact=contact, notes=notes, updated_at=now)


def _synth_light(s: sqlite3.Row) -> JobLight:
    """Minimal JobLight for a review-flagged state whose job row is gone."""
    return JobLight(
        url=s["url"], url_b64=url_to_b64(s["url"]), seen_key=s["seen_key"], tier=0,
        is_new=False, remote=False, has_desc=False,
        state=JobState(
            status=s["status"], notes=s["notes"] or "", follow_up_date=s["follow_up_date"],
            applied_date=s["applied_date"], starred=bool(s["starred"]), hidden=bool(s["hidden"]),
            contact=s["contact"] or "", snoozed_until=s["snoozed_until"],
            needs_review=bool(s["needs_review"]), review_reason=s["review_reason"],
            updated_at=s["updated_at"],
        ),
    )


@router.get("/review", response_model=list[JobLight])
def review_list(conn: sqlite3.Connection = Depends(get_db)):
    rows = conn.execute("SELECT * FROM job_state WHERE needs_review=1").fetchall()
    out = []
    for s in rows:
        jrow = conn.execute(f"{JOB_JOIN_SQL} WHERE j.url=?", (s["url"],)).fetchone()
        out.append(job_light_from_row(jrow) if jrow is not None else _synth_light(s))
    return out
