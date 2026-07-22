"""User-owned state: job_state edits, quick actions, company notes.

job_state is keyed on seen_key (role identity). Routes stay url-addressed for frontend
compat; each url is resolved to a seen_key before touching state. The review endpoints
are retired shims (empty list / 410) — see the bottom of this module."""
import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..config import DEFAULT_SNOOZE_DAYS, STATUSES
from ..db import get_db
from ..events import TRACKED_FIELDS, record_field_events
from ..models import (
    CompanyPatch,
    CompanyState,
    JobState,
    QuickAction,
    ReconcileBody,
    ReviewItem,
    StatePatch,
    b64_to_url,
    date_plus,
    now_iso,
    today_iso,
)

router = APIRouter()

# Whitelist of columns a PATCH may touch (also guards the SQL built from it).
# review_dismissed is deliberately absent: the DTO still accepts it (frontend compat)
# but it is dropped here, so it never reaches the SQL for a column that no longer exists.
_PATCHABLE = (
    "status", "notes", "follow_up_date", "applied_date", "starred", "hidden",
    "contact", "snoozed_until", "applied_via",
)


def _state_dto(conn: sqlite3.Connection, seen_key: str) -> JobState:
    row = conn.execute("SELECT * FROM job_state WHERE seen_key=?", (seen_key,)).fetchone()
    return JobState(
        status=row["status"],
        notes=row["notes"] or "",
        follow_up_date=row["follow_up_date"],
        applied_date=row["applied_date"],
        starred=bool(row["starred"]),
        hidden=bool(row["hidden"]),
        contact=row["contact"] or "",
        snoozed_until=row["snoozed_until"],
        applied_via=row["applied_via"],
        needs_review=False,   # retired; constant for frontend compat
        review_reason=None,
        updated_at=row["updated_at"],
    )


def _snooze_default_days(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value FROM app_settings WHERE key='snooze_default_days'").fetchone()
    if row is not None:
        try:
            return int(json.loads(row["value"]))
        except (TypeError, ValueError):
            pass
    return DEFAULT_SNOOZE_DAYS


def _resolve_seen_key(conn: sqlite3.Connection, url: str):
    """seen_key (job_state's key) for a url. The jobs cache is authoritative for any
    present/known url, so it wins; a dormant state row addressed by its own last-known
    url is the fallback. None when the url maps to neither."""
    row = conn.execute("SELECT seen_key FROM jobs WHERE url=?", (url,)).fetchone()
    if row:
        return row["seen_key"]
    row = conn.execute("SELECT seen_key FROM job_state WHERE url=?", (url,)).fetchone()
    return row["seen_key"] if row else None


def _apply_state(conn: sqlite3.Connection, url: str, changes: dict,
                 source: str = "patch", extra_events: list | None = None) -> JobState:
    """Upsert that touches ONLY the supplied fields, as one atomic statement — no
    read-modify-write, so concurrent edits of disjoint fields both survive.
    status->Applied with no applied_date supplied fills today while preserving any
    existing date (COALESCE).

    job_state is keyed on seen_key: the url is resolved to a seen_key (jobs cache, or a
    dormant state row) and the upsert conflicts on seen_key, so an edit made through
    any url that maps to a role lands on that role's single state row. url is refreshed
    to the address the edit came through (a present-job url), keeping display current.

    Before committing, one state_event is written per field that actually changed
    (diffed against the pre-write row), inside this same transaction so an event and
    the change it records commit together or not at all. `extra_events` are
    unconditional (seen_key, field, new_value) events for things that aren't job_state
    columns at all (e.g. a quick-pass reason) -- recorded, not diffed, in the same
    transaction."""
    changes = {k: v for k, v in changes.items() if k in _PATCHABLE}
    for k in ("starred", "hidden"):
        if k in changes:
            changes[k] = 1 if changes[k] else 0
    auto_applied = changes.get("status") == "Applied" and "applied_date" not in changes

    seen_key = _resolve_seen_key(conn, url) or ""
    # Pre-write row for event diffing (also supplies the preserved applied_date), keyed
    # on seen_key like the upsert below. Read once, before the write. Unset columns take
    # their DDL defaults on the INSERT branch (status 'New', notes '', flags 0).
    old_row = conn.execute("SELECT * FROM job_state WHERE seen_key=?", (seen_key,)).fetchone()

    params = dict(changes)
    params.update(url=url, seen_key=seen_key, updated_at=now_iso(), today=today_iso())

    insert_cols = ["seen_key", "url", "updated_at"] + list(changes)
    insert_vals = [f":{c}" for c in insert_cols]
    set_parts = [f"{c}=excluded.{c}" for c in changes]
    set_parts += ["url=excluded.url", "updated_at=excluded.updated_at"]
    if auto_applied:
        insert_cols.append("applied_date")
        insert_vals.append(":today")
        set_parts.append("applied_date=COALESCE(job_state.applied_date, :today)")

    conn.execute(
        f"INSERT INTO job_state ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals)}) "
        f"ON CONFLICT(seen_key) DO UPDATE SET {', '.join(set_parts)}",
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

    for field, new_value in (extra_events or []):
        conn.execute(
            "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
            "VALUES (?,?,?,?,?,?,?)",
            (seen_key, url, field, None, new_value, params["updated_at"], source),
        )

    conn.commit()
    return _state_dto(conn, seen_key)


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
    extra_events = None
    if action == "applied":
        changes = {"status": "Applied"}  # applied_date auto-filled by _apply_state
        if body.applied_via:
            changes["applied_via"] = body.applied_via
    elif action == "snooze":
        days = body.days if body.days is not None else _snooze_default_days(conn)
        changes = {"snoozed_until": date_plus(days)}
    elif action == "pass":
        changes = {"status": "Passed"}
        if body.reason:
            extra_events = [("pass_reason", body.reason)]
    elif action == "star":
        changes = {"starred": 1}
    elif action == "unstar":
        changes = {"starred": 0}
    else:
        raise HTTPException(status_code=422, detail=f"unknown action: {action}")
    return _apply_state(conn, url, changes, source=f"quick:{action}", extra_events=extra_events)


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


@router.get("/review", response_model=list[ReviewItem])
def review_list(conn: sqlite3.Connection = Depends(get_db)):
    """Retired: state follows a role by seen_key, so nothing is ever flagged for
    review. Kept as an always-empty endpoint so the built frontend's fetch is a no-op
    instead of a 404."""
    return []


@router.post("/review/reconcile")
def reconcile(body: ReconcileBody, conn: sqlite3.Connection = Depends(get_db)):
    """Retired: there are no orphaned state rows to reconcile once state is keyed on
    seen_key. 410 Gone tells the built frontend the operation no longer exists."""
    raise HTTPException(status_code=410, detail="reconcile retired: state is keyed on seen_key")
