"""Phase 5, W-5.2/5.3: HTTP wiring for `outcomes.py`'s write paths and
`outcome_analytics.py`'s read path.

  POST /outcomes/snapshots   record one served queue (`outcomes.capture_snapshot`).
  POST /outcomes/events      record one visitor action (`outcomes.record_outcome_event`).
  GET  /outcomes/analytics   outcome analytics by source/band/family/feature/rank
                              (`outcome_analytics.outcome_analytics`) -- read-only,
                              no commit.

NOT mounted in main.py -- the 5.4+ consumer wires that, exactly like
routers/readsv2.py before Phase 4's integration. Every test here builds its
own local FastAPI app (see tests/test_canonical_reads_router.py's `api`
fixture for the established pattern), never touching webapp/app.db
(repo-root conftest.py fences JOBHUNT_DB).

Pydantic request models live in THIS file, not models.py: models.py is a
shared file this task does not own, and these DTOs are private to the two
POST routes below.
"""
from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from .. import outcomes
from ..db import get_db
from ..models import b64_to_url
from ..outcome_analytics import outcome_analytics

router = APIRouter()


class SnapshotItemIn(BaseModel):
    posting_id: str
    rank: int
    posting_version_id: str | None = None
    score_version_id: str | None = None
    tier: int | None = None
    odds: str | None = None
    odds_score: int | None = None
    source: str | None = None
    title: str | None = None
    company: str | None = None


class SnapshotIn(BaseModel):
    surface: str
    items: list[SnapshotItemIn] = []
    metadata: dict | None = None
    scorer_hash: str | None = None
    queue_size: int | None = None


class OutcomeEventIn(BaseModel):
    kind: str
    url_b64: str | None = None
    posting_id: str | None = None
    snapshot_id: str | None = None
    rank: int | None = None
    payload: dict | None = None
    idempotency_key: str | None = None


def _decode_or_404(url_b64: str) -> str:
    """Mirrors routers/state.py's `_decode_or_404`: a bad base64 payload reads
    as an unknown job, not a 500."""
    try:
        return b64_to_url(url_b64)
    except Exception:
        raise HTTPException(status_code=404, detail="unknown job")


@router.post("/outcomes/snapshots", status_code=201)
def post_snapshot(body: SnapshotIn, conn: sqlite3.Connection = Depends(get_db)):
    try:
        return outcomes.capture_snapshot(
            conn,
            surface=body.surface,
            items=[item.model_dump(exclude_unset=False) for item in body.items],
            metadata=body.metadata,
            scorer_hash=body.scorer_hash,
            queue_size=body.queue_size,
        )
    except (ValueError, TypeError) as exc:
        # TypeError surfaces from `runstore.canonical_json` (F18): non-
        # serializable `metadata` (e.g. a dict with mixed-type keys, which
        # `sort_keys=True` cannot compare) is a caller input error, not a
        # server fault.
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/outcomes/events", status_code=201)
def post_event(body: OutcomeEventIn, conn: sqlite3.Connection = Depends(get_db)):
    if not body.url_b64 and not body.posting_id:
        raise HTTPException(status_code=422, detail="url_b64 or posting_id is required")
    url = _decode_or_404(body.url_b64) if body.url_b64 else None
    try:
        return outcomes.record_outcome_event(
            conn,
            kind=body.kind,
            url=url,
            posting_id=body.posting_id,
            snapshot_id=body.snapshot_id,
            rank=body.rank,
            payload=body.payload,
            idempotency_key=body.idempotency_key,
        )
    except (ValueError, TypeError) as exc:
        # See post_snapshot's identical clause (F18): a non-serializable
        # `payload` is a caller input error, not a server fault.
        raise HTTPException(status_code=422, detail=str(exc))


@router.get("/outcomes/analytics")
def get_outcome_analytics(
    min_sample: int = Query(5, ge=0, le=1000),
    conn: sqlite3.Connection = Depends(get_db),
):
    return outcome_analytics(conn, min_sample=min_sample)
