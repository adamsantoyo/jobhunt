"""Task 5.6: HTTP wiring for `backup.py`.

  POST /api/backups   create a validated backup of the live database, 201 +
                       manifest.
  GET  /api/backups    list existing backups' manifests, newest first.

NO restore endpoint, by decision (see backup.py's module docstring): restoring
over a live database is a deliberate manual file operation, not something an
API call can trigger. Use `python -m backend.backup restore` with the server
down instead.

Follows `routers/configapi.py`'s conventions (module-level `APIRouter()`,
plain-dict responses, FastAPI `Depends` for the injectable resources) with one
deliberate departure: `backup.create_backup`/`list_backups` manage their own
sqlite connections internally (a read-only source connection plus a separate
backup-target connection -- see `backup.py`), so this router does not pull a
request-scoped connection via `get_db`. What it injects instead is the SOURCE
PATH and DEST DIR themselves, each as its own `Depends`-overridable accessor --
the same test-injection shape `get_db` gives every other router, applied to
the two things this router's handlers actually need. Tests override both to
point at a temp DB and a temp directory; production leaves them at their
defaults (`config.DB_PATH`, `backup.DEFAULT_BACKUP_DIR`) so a real POST backs
up the live database into `webapp/backups`.

NOT mounted in main.py -- the orchestrator adds the one-line mount, exactly as
every other Phase 5 router was mounted at its integration wave. Tests build
their own local FastAPI app (see `tests/test_backup_api.py`).
"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException

from .. import backup, config

router = APIRouter()


def get_backup_source_path() -> Path:
    """The database to back up. Defaults to the live `config.DB_PATH`;
    overridden in tests to point at a temp DB so the suite never touches
    `webapp/app.db`."""
    return Path(config.DB_PATH)


def get_backup_dest_dir() -> Path:
    """Where backups + manifests live. Defaults to `backup.DEFAULT_BACKUP_DIR`
    (`webapp/backups`); overridden in tests to point at `tmp_path`."""
    return backup.DEFAULT_BACKUP_DIR


@router.post("/backups", status_code=201)
def create_backup_endpoint(
    source_path: Path = Depends(get_backup_source_path),
    dest_dir: Path = Depends(get_backup_dest_dir),
):
    try:
        return backup.create_backup(source_path, dest_dir)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"backup failed: {e}")


@router.get("/backups")
def list_backups_endpoint(dest_dir: Path = Depends(get_backup_dest_dir)):
    return backup.list_backups(dest_dir)
