"""Canonical read endpoints under /v2 -- DTO-shape-identical to the legacy /api/*
read endpoints (routers/jobs.py, changes.py, analytics.py), served from the
canonical posting + current-score tables via `canonical_reads`.

Not mounted anywhere yet: the orchestrator mounts this router (prefix="/api") in
main.py once both wave-1 tasks land, per the phase 4 spec ("Must NOT touch
main.py"). Tests in this package mount it on a local FastAPI app instead.

Every route calls `runstore.require_canonical_schema` first and turns its
RuntimeError into a 503 -- "live-DB gating, not code gating" (phase 4 spec,
architectural decision 3): this router exists unconditionally, but does nothing
useful against a database that predates the canonical schema.
"""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from .. import canonical_reads
from ..db import get_db
from ..models import b64_to_url
from ..sources import runstore

router = APIRouter()


def _require_canonical(conn: sqlite3.Connection) -> None:
    try:
        runstore.require_canonical_schema(conn)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None


@router.get("/v2/jobs")
def list_jobs(min_tier: int | None = None, conn: sqlite3.Connection = Depends(get_db)):
    _require_canonical(conn)
    return canonical_reads.list_jobs(conn, min_tier=min_tier)


@router.get("/v2/followups")
def followups(conn: sqlite3.Connection = Depends(get_db)):
    _require_canonical(conn)
    return canonical_reads.followups(conn)


@router.get("/v2/jobs/{url_b64}")
def job_detail(url_b64: str, conn: sqlite3.Connection = Depends(get_db)):
    _require_canonical(conn)
    try:
        url = b64_to_url(url_b64)
    except Exception:
        raise HTTPException(status_code=404, detail="unknown job") from None
    job = canonical_reads.job_detail(conn, url)
    if job is None:
        raise HTTPException(status_code=404, detail="unknown job")
    return job


@router.get("/v2/changes")
def changes(since: str | None = None, conn: sqlite3.Connection = Depends(get_db)):
    _require_canonical(conn)
    return canonical_reads.changes(conn, since=since)


@router.get("/v2/analytics")
def analytics(conn: sqlite3.Connection = Depends(get_db)):
    _require_canonical(conn)
    return canonical_reads.analytics(conn)


@router.get("/v2/freshness")
def freshness(conn: sqlite3.Connection = Depends(get_db)):
    _require_canonical(conn)
    return canonical_reads.freshness(conn)
