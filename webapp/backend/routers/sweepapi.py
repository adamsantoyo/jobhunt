"""Sweep control: quick refresh, full sweep, SSE progress, cancel, manual ingest."""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from ..db import get_db
from ..ingest import ingest
from ..models import IngestReport
from ..sweeprunner import runner, sse_stream

router = APIRouter()


@router.post("/refresh/quick")
async def refresh_quick():
    ok, detail = await runner.start("quick")
    if not ok:
        raise HTTPException(status_code=409, detail=detail)
    return JSONResponse({"started": True, "kind": "quick"}, status_code=202)


@router.post("/sweep/full")
async def sweep_full():
    ok, detail = await runner.start("full")
    if not ok:
        raise HTTPException(status_code=409, detail=detail)
    return JSONResponse({"started": True, "kind": "full"}, status_code=202)


@router.get("/sweep/progress")
async def sweep_progress():
    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(sse_stream(), media_type="text/event-stream", headers=headers)


@router.post("/sweep/cancel")
async def sweep_cancel():
    await runner.cancel()
    return {"cancelled": True}


@router.post("/ingest", response_model=IngestReport)
def run_ingest(conn: sqlite3.Connection = Depends(get_db)):
    return ingest(conn)
