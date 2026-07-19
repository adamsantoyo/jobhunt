"""App settings: skills list + comp band (UI-editable)."""
import json
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from ..config import DEFAULT_COMP_BAND, DEFAULT_SKILLS, STATUSES
from ..db import get_db
from ..models import ConfigOut, ConfigPatch

router = APIRouter()


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


def _set(conn: sqlite3.Connection, key: str, value):
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value)),
    )


@router.get("/config", response_model=ConfigOut)
def get_config(conn: sqlite3.Connection = Depends(get_db)):
    return ConfigOut(skills=_get_skills(conn), comp_band=_get_comp_band(conn), statuses=STATUSES)


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
    conn.commit()
    return ConfigOut(skills=_get_skills(conn), comp_band=_get_comp_band(conn), statuses=STATUSES)
