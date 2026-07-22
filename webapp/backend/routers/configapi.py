"""App settings: skills list, comp band (display-only), and the goal-tracking knobs
(daily queue size, weekly app target, deadline, snooze default) that drive the funnel
and follow-up views. All UI-editable via PATCH."""
import json
import sqlite3
from datetime import date

from fastapi import APIRouter, Depends, HTTPException

from ..config import (
    DEFAULT_COMP_BAND,
    DEFAULT_DAILY_QUEUE_SIZE,
    DEFAULT_DEADLINE,
    DEFAULT_SKILLS,
    DEFAULT_SNOOZE_DAYS,
    DEFAULT_WEEKLY_APP_TARGET,
    STATUSES,
)
from ..db import get_db
from ..models import ConfigOut, ConfigPatch

router = APIRouter()


def _is_iso_date(val) -> bool:
    if not isinstance(val, str):
        return False
    try:
        date.fromisoformat(val)
        return True
    except ValueError:
        return False


def _get_skills(conn: sqlite3.Connection) -> list[str]:
    row = conn.execute("SELECT value FROM app_settings WHERE key='skills'").fetchone()
    if row:
        try:
            val = json.loads(row["value"])
            if isinstance(val, list):
                return [str(s) for s in val]
        except Exception:
            pass
    return list(DEFAULT_SKILLS)


def _get_comp_band(conn: sqlite3.Connection) -> list[int]:
    row = conn.execute("SELECT value FROM app_settings WHERE key='comp_band'").fetchone()
    if row:
        try:
            val = json.loads(row["value"])
            if isinstance(val, list) and len(val) == 2:
                return [int(val[0]), int(val[1])]
        except Exception:
            pass
    return list(DEFAULT_COMP_BAND)


def _get_int(conn: sqlite3.Connection, key: str, default: int) -> int:
    row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    if row is not None:
        try:
            val = int(json.loads(row["value"]))
            if val >= 1:
                return val
        except (TypeError, ValueError):
            pass
    return default


def _get_deadline(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT value FROM app_settings WHERE key='deadline'").fetchone()
    if row is not None:
        try:
            val = json.loads(row["value"])
            if _is_iso_date(val):
                return val
        except Exception:
            pass
    return DEFAULT_DEADLINE


def _set(conn: sqlite3.Connection, key: str, value):
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value)),
    )


def _config_out(conn: sqlite3.Connection) -> ConfigOut:
    return ConfigOut(
        skills=_get_skills(conn),
        comp_band=_get_comp_band(conn),
        statuses=STATUSES,
        daily_queue_size=_get_int(conn, "daily_queue_size", DEFAULT_DAILY_QUEUE_SIZE),
        weekly_app_target=_get_int(conn, "weekly_app_target", DEFAULT_WEEKLY_APP_TARGET),
        deadline=_get_deadline(conn),
        snooze_default_days=_get_int(conn, "snooze_default_days", DEFAULT_SNOOZE_DAYS),
    )


@router.get("/config", response_model=ConfigOut)
def get_config(conn: sqlite3.Connection = Depends(get_db)):
    return _config_out(conn)


@router.patch("/config", response_model=ConfigOut)
def patch_config(body: ConfigPatch, conn: sqlite3.Connection = Depends(get_db)):
    changes = body.model_dump(exclude_unset=True)
    if "skills" in changes and changes["skills"] is not None:
        _set(conn, "skills", [str(s) for s in changes["skills"]])
    if "comp_band" in changes and changes["comp_band"] is not None:
        band = changes["comp_band"]
        if not (isinstance(band, list) and len(band) == 2):
            raise HTTPException(status_code=422, detail="comp_band must be [lo, hi]")
        _set(conn, "comp_band", [int(band[0]), int(band[1])])
    for key in ("daily_queue_size", "weekly_app_target", "snooze_default_days"):
        if key in changes and changes[key] is not None:
            val = changes[key]
            if not (isinstance(val, int) and val >= 1):
                raise HTTPException(status_code=422, detail=f"{key} must be an int >= 1")
            _set(conn, key, val)
    if "deadline" in changes and changes["deadline"] is not None:
        deadline = changes["deadline"]
        if not _is_iso_date(deadline):
            raise HTTPException(status_code=422, detail="deadline must be an ISO date (YYYY-MM-DD)")
        _set(conn, "deadline", deadline)
    conn.commit()
    return _config_out(conn)
