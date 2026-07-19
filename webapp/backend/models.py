"""Pydantic DTOs + shared serialization helpers (row -> DTO, url <-> base64url)."""
from __future__ import annotations

import base64
from datetime import date, datetime, timedelta
from typing import Optional

from pydantic import BaseModel


def now_iso() -> str:
    return datetime.now().isoformat()


def today_iso() -> str:
    return date.today().isoformat()


def date_plus(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


# --------------------------------------------------------------------------- #
# url <-> base64url (no padding). Computed server-side; the frontend treats the
# result as an opaque id and never encodes/decodes it.
# --------------------------------------------------------------------------- #
def url_to_b64(url: str) -> str:
    return base64.urlsafe_b64encode((url or "").encode("utf-8")).decode("ascii").rstrip("=")


def b64_to_url(b64: str) -> str:
    pad = "=" * (-len(b64) % 4)
    return base64.urlsafe_b64decode((b64 + pad).encode("ascii")).decode("utf-8")


# --------------------------------------------------------------------------- #
# DTOs
# --------------------------------------------------------------------------- #
class JobState(BaseModel):
    status: str
    notes: str
    follow_up_date: Optional[str] = None
    applied_date: Optional[str] = None
    starred: bool
    hidden: bool
    contact: str
    snoozed_until: Optional[str] = None
    needs_review: bool
    review_reason: Optional[str] = None
    updated_at: str


class JobLight(BaseModel):
    url: str
    url_b64: str
    seen_key: str
    tier: int
    odds: Optional[str] = None
    odds_score: Optional[int] = None
    odds_why: Optional[str] = None
    is_new: bool
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    salary: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    posted: Optional[str] = None
    first_seen: Optional[str] = None
    remote: bool
    source: Optional[str] = None
    also_seen_on: Optional[str] = None
    req_id: Optional[str] = None
    why: Optional[str] = None
    flags: Optional[str] = None
    desc_snippet: Optional[str] = None
    has_desc: bool
    state: Optional[JobState] = None


class JobFull(JobLight):
    full_desc: Optional[str] = None
    skill_hits: list[str] = []


class IngestReport(BaseModel):
    rows: int
    new: int
    healed: int
    needs_review: int
    descs_joined: int
    runs_backfilled: int


class CompanyState(BaseModel):
    company: str
    contact: str
    notes: str
    updated_at: str


class ConfigOut(BaseModel):
    skills: list[str]
    comp_band: list[int]
    statuses: list[str]


# --------------------------------------------------------------------------- #
# Request bodies
# --------------------------------------------------------------------------- #
class StatePatch(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None
    follow_up_date: Optional[str] = None
    applied_date: Optional[str] = None
    starred: Optional[bool] = None
    hidden: Optional[bool] = None
    contact: Optional[str] = None
    snoozed_until: Optional[str] = None


class QuickAction(BaseModel):
    action: str
    days: Optional[int] = None


class CompanyPatch(BaseModel):
    contact: Optional[str] = None
    notes: Optional[str] = None


class ConfigPatch(BaseModel):
    skills: Optional[list[str]] = None
    comp_band: Optional[list[int]] = None


# --------------------------------------------------------------------------- #
# Row -> DTO helpers
# --------------------------------------------------------------------------- #
def _state_from_row(row) -> Optional[JobState]:
    """A LEFT JOIN with no match yields NULL status; a real row always has status."""
    status = row["status"] if "status" in row.keys() else None
    if status is None:
        return None
    return JobState(
        status=status,
        notes=row["notes"] or "",
        follow_up_date=row["follow_up_date"],
        applied_date=row["applied_date"],
        starred=bool(row["starred"]),
        hidden=bool(row["hidden"]),
        contact=row["contact"] or "",
        snoozed_until=row["snoozed_until"],
        needs_review=bool(row["needs_review"]),
        review_reason=row["review_reason"],
        updated_at=row["state_updated_at"] if "state_updated_at" in row.keys() else (row["updated_at"] or ""),
    )


# Shared SQL for "jobs LEFT JOIN job_state" used by every read endpoint. Selects
# all job columns plus the specific state columns (state.updated_at aliased so it
# never collides). A NULL status distinguishes "no state row" from a real row.
JOB_STATE_JOIN_COLS = (
    "s.status, s.notes, s.follow_up_date, s.applied_date, s.starred, s.hidden, "
    "s.contact, s.snoozed_until, s.needs_review, s.review_reason, s.updated_at AS state_updated_at"
)
JOB_JOIN_SQL = f"SELECT j.*, {JOB_STATE_JOIN_COLS} FROM jobs j LEFT JOIN job_state s ON j.url = s.url"


def job_light_from_row(row) -> JobLight:
    return JobLight(
        url=row["url"],
        url_b64=url_to_b64(row["url"]),
        seen_key=row["seen_key"],
        tier=row["tier"] if row["tier"] is not None else 0,
        odds=row["odds"],
        odds_score=row["odds_score"],
        odds_why=row["odds_why"],
        is_new=bool(row["is_new"]),
        title=row["title"],
        company=row["company"],
        location=row["location"],
        salary=row["salary"],
        salary_min=row["salary_min"],
        salary_max=row["salary_max"],
        posted=row["posted"],
        first_seen=row["first_seen"],
        remote=bool(row["remote"]),
        source=row["source"],
        also_seen_on=row["also_seen_on"],
        req_id=row["req_id"],
        why=row["why"],
        flags=row["flags"],
        desc_snippet=row["desc_snippet"],
        has_desc=bool(row["full_desc"]),
        state=_state_from_row(row),
    )
