"""User-owned state: job_state edits, quick actions, company notes, review list."""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..config import STATUSES
from ..db import get_db
from ..events import TRACKED_FIELDS, record_field_events
from ..models import (
    JOB_LIGHT_SQL,
    CompanyPatch,
    CompanyState,
    JobLight,
    JobState,
    QuickAction,
    ReconcileBody,
    ReviewItem,
    StatePatch,
    b64_to_url,
    date_plus,
    job_light_from_row,
    now_iso,
    today_iso,
    url_to_b64,
)

router = APIRouter()

# Whitelist of columns a PATCH may touch (also guards the SQL built from it).
_PATCHABLE = (
    "status", "notes", "follow_up_date", "applied_date", "starred", "hidden",
    "contact", "snoozed_until", "review_dismissed",
)


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


def _apply_state(conn: sqlite3.Connection, url: str, changes: dict,
                 source: str = "patch") -> JobState:
    """Upsert that touches ONLY the supplied fields, as one atomic statement — no
    read-modify-write, so concurrent edits of disjoint fields both survive.
    Every user edit clears the live needs_review flag; review_dismissed changes
    only when the patch names it. status->Applied with no applied_date supplied
    fills today while preserving any existing date (COALESCE).

    Before committing, one state_event is written per field that actually changed
    (diffed against the pre-write row), inside this same transaction so an event and
    the change it records commit together or not at all."""
    changes = {k: v for k, v in changes.items() if k in _PATCHABLE}
    for k in ("starred", "hidden", "review_dismissed"):
        if k in changes:
            changes[k] = 1 if changes[k] else 0
    auto_applied = changes.get("status") == "Applied" and "applied_date" not in changes

    # Pre-write row for event diffing (also supplies the preserved applied_date). Read
    # once, before the upsert. seen_key is derived data (jobs cache), never
    # user-mutated: this pre-read is not part of the lost-update race. Unset columns
    # take their DDL defaults on the INSERT branch (status 'New', notes '', flags 0).
    old_row = conn.execute("SELECT * FROM job_state WHERE url=?", (url,)).fetchone()
    seen_key = _resolve_seen_key(conn, url) or ""

    params = dict(changes)
    params.update(url=url, seen_key=seen_key, updated_at=now_iso(), today=today_iso())

    insert_cols = ["url", "seen_key", "updated_at"] + list(changes)
    insert_vals = [f":{c}" for c in insert_cols]
    set_parts = [f"{c}=excluded.{c}" for c in changes]
    set_parts += ["updated_at=excluded.updated_at", "needs_review=0", "review_reason=NULL"]
    if auto_applied:
        insert_cols.append("applied_date")
        insert_vals.append(":today")
        set_parts.append("applied_date=COALESCE(job_state.applied_date, :today)")

    conn.execute(
        f"INSERT INTO job_state ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals)}) "
        f"ON CONFLICT(url) DO UPDATE SET {', '.join(set_parts)}",
        params,
    )

    # Effective new values for the event diff. auto_applied's applied_date follows the
    # COALESCE above: an existing date is preserved (no event), else today (NULL->today).
    new_vals = {k: v for k, v in changes.items() if k in TRACKED_FIELDS}
    if auto_applied:
        prior = old_row["applied_date"] if old_row is not None else None
        new_vals["applied_date"] = prior or params["today"]
    old_vals = {k: old_row[k] for k in TRACKED_FIELDS} if old_row is not None else {}
    record_field_events(conn, seen_key=seen_key, url=url, old=old_vals,
                        new=new_vals, source=source, at=params["updated_at"])

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
    return _apply_state(conn, url, changes, source="patch")


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
    return _apply_state(conn, url, changes, source=f"quick:{action}")


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


@router.get("/review", response_model=list[ReviewItem])
def review_list(conn: sqlite3.Connection = Depends(get_db)):
    rows = conn.execute("SELECT * FROM job_state WHERE needs_review=1").fetchall()
    out = []
    for s in rows:
        jrow = conn.execute(f"{JOB_LIGHT_SQL} WHERE j.url=?", (s["url"],)).fetchone()
        job = job_light_from_row(jrow) if jrow is not None else _synth_light(s)
        # Live candidates: present jobs sharing the flagged row's seen_key. Ones
        # that already own state come through with state != null (UI disables Attach).
        cands = [
            job_light_from_row(r)
            for r in conn.execute(
                f"{JOB_LIGHT_SQL} WHERE j.present=1 AND j.seen_key=?", (s["seen_key"],)
            ).fetchall()
        ]
        out.append(ReviewItem(job=job, candidates=cands))
    return out


@router.post("/review/reconcile", response_model=JobState)
def reconcile(body: ReconcileBody, conn: sqlite3.Connection = Depends(get_db)):
    """Attach an orphaned/ambiguous state row to a user-chosen successor job.
    One guarded UPDATE (no check-then-act): a concurrent reconcile to the same
    target loses the race with a 409, never a PK-violation 500. seen_key is
    rewritten to the target's — the point is re-anchoring across identities."""
    from_url = _decode_or_404(body.from_url_b64)
    to_url = _decode_or_404(body.to_url_b64)
    if from_url == to_url:
        raise HTTPException(status_code=422, detail="source and target are the same job")
    cur = conn.execute(
        "UPDATE job_state SET url=:to, "
        "seen_key=(SELECT seen_key FROM jobs WHERE url=:to), "
        "needs_review=0, review_reason=NULL, review_dismissed=0, updated_at=:now "
        "WHERE url=:frm "
        "AND EXISTS (SELECT 1 FROM jobs WHERE url=:to AND present=1) "
        "AND NOT EXISTS (SELECT 1 FROM job_state WHERE url=:to)",
        {"to": to_url, "frm": from_url, "now": now_iso()},
    )
    if cur.rowcount == 0:
        if conn.execute("SELECT 1 FROM job_state WHERE url=?", (from_url,)).fetchone() is None:
            raise HTTPException(status_code=404, detail="no state at the source job")
        if conn.execute("SELECT 1 FROM jobs WHERE url=? AND present=1", (to_url,)).fetchone() is None:
            raise HTTPException(status_code=422, detail="target job is not present in the current run")
        raise HTTPException(status_code=409, detail="target job already has state")
    conn.commit()
    return _state_dto(conn, to_url)
