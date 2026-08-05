"""Sweep control: quick refresh, full sweep, SSE progress, cancel, manual ingest.

The three write entry points here (quick refresh, full sweep, manual ingest) are
the legacy pipeline's only HTTP-driven writers, so they are exactly what task
4.7's `JOBHUNT_WRITES` flag turns off at cutover: under `canonical` each refuses
with a 409 naming the flag (`_guard_legacy_writes`), while under the default
`legacy` the guard's condition is false and every handler below runs completely
unchanged. Progress and cancel are deliberately NOT guarded: a sweep already
running when the flag flips must stay observable and stoppable, and neither
endpoint writes anything of its own.
"""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from .. import config
from ..db import get_db
from ..ingest import ingest
from ..models import IngestReport
from ..sweeprunner import runner, sse_stream

router = APIRouter()


def _guard_legacy_writes() -> None:
    """409 when `JOBHUNT_WRITES=canonical` retires the legacy write paths.

    Read as an attribute on every call rather than captured at import, so tests
    can flip `config.WRITES_SOURCE` the way a restart would flip the env var
    (see test_write_flag.py; same convention as the 4.6 read flag).
    """
    if config.WRITES_SOURCE == "canonical":
        raise HTTPException(status_code=409, detail=config.WRITE_GATE_DETAIL)


@router.post("/refresh/quick")
async def refresh_quick():
    _guard_legacy_writes()
    ok, detail = await runner.start("quick")
    if not ok:
        raise HTTPException(status_code=409, detail=detail)
    return JSONResponse({"started": True, "kind": "quick"}, status_code=202)


@router.post("/sweep/full")
async def sweep_full():
    _guard_legacy_writes()
    ok, detail = await runner.start("full")
    if not ok:
        raise HTTPException(status_code=409, detail=detail)
    return JSONResponse({"started": True, "kind": "full"}, status_code=202)


@router.get("/sweep/progress")
async def sweep_progress():
    # no-transform stops an intermediary from buffering or rewriting the event
    # stream; X-Accel-Buffering is the nginx-specific form of the same instruction.
    headers = {"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"}
    return StreamingResponse(sse_stream(), media_type="text/event-stream", headers=headers)


@router.post("/sweep/cancel")
async def sweep_cancel():
    await runner.cancel()
    return {"cancelled": True}


@router.post("/ingest", response_model=IngestReport)
def run_ingest(conn: sqlite3.Connection = Depends(get_db)):
    _guard_legacy_writes()
    return ingest(conn)
