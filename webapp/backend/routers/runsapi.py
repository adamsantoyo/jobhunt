"""Canonical run control: start, list, inspect, cancel, and stream (Phase 4.1).

Five endpoints over `RunService`. The router itself holds no state and makes no
decisions: every refusal is a `RunServiceError` subclass raised by the service and
translated here into exactly one status code, so "what is a 409" is answered in
one place rather than in five handlers.

The SSE endpoint is the interesting one. Its cursor is `run_events.sequence` and
nothing else -- not a wall clock, not an in-memory counter -- which is what makes
these three cases the same code path:

  fresh connect        cursor -1, replay everything, then tail
  reconnect            cursor from `Last-Event-ID` (or `?after=`), replay the gap,
                       then tail
  after a restart      identical to the reconnect case, with no live handle
                       involved: the rows are the stream

`event_stream` is a module-level async generator rather than a closure so tests
can drive it directly, without a socket and without racing an HTTP client.

EVERY REFUSAL IS DECIDED BEFORE THE STREAMING RESPONSE IS CONSTRUCTED. Once
`StreamingResponse` is returned the status line and headers are already on the
wire, so an exception raised inside the generator can only truncate a 200 -- the
client sees a short, successful stream and has no way to tell it from a run that
simply had nothing more to say. The schema gate, the run-exists check and the
cursor validation therefore all run in the handler, where they can still be a
503, a 404 or a 400.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from .. import runservice
from ..runservice import (
    CanonicalSchemaUnavailable,
    RunConflict,
    RunService,
    UnknownRun,
    UnknownRunKind,
    UnsupportedRunKind,
)

router = APIRouter()

#: Comment frame cadence on an idle stream. Keeps the socket warm and is the
#: fallback that surfaces a half-open peer.
HEARTBEAT_SECONDS = 15.0

SSE_HEADERS = {
    # no-transform stops an intermediary from buffering or rewriting the event
    # stream; X-Accel-Buffering is the nginx-specific form of the same instruction.
    "Cache-Control": "no-cache, no-transform",
    "X-Accel-Buffering": "no",
}

#: `run_events.sequence` is a SQLite INTEGER, so a cursor outside int64 cannot
#: name a row and cannot even be bound to a query: sqlite3 raises `OverflowError`
#: on the way in. -1 is the exclusive "before the first event" cursor.
MIN_CURSOR = -1
MAX_CURSOR = 2**63 - 1


def _schema_unavailable(exc: BaseException) -> bool:
    """Is this exception the database saying "I have no canonical schema"?

    Two shapes, and only two: the service's own gate, and the raw
    `OperationalError` a read gets when it queries a table a legacy v4 database
    has never had. Anything else is a bug in this code and must surface as a 500
    rather than be dressed up as "database unavailable".
    """
    if isinstance(exc, CanonicalSchemaUnavailable):
        return True
    return isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc).lower()


def _schema_gap(exc: BaseException) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=f"canonical run schema is not available on this database: {exc}",
    )


class RunRequest(BaseModel):
    kind: str


def get_run_service(request: Request) -> RunService:
    """The app's service, or the process-wide default.

    `app.state.run_service` is how a test (and, later, a differently-wired app)
    substitutes a service pointed at its own database without touching the
    process-wide singleton the real server uses.
    """
    service = getattr(request.app.state, "run_service", None)
    return service if service is not None else runservice.default_service()


# --------------------------------------------------------------------------- #
# Control
# --------------------------------------------------------------------------- #
@router.post("/runs")
async def create_run(
    body: RunRequest, service: RunService = Depends(get_run_service)
) -> JSONResponse:
    try:
        started = await service.start_run(body.kind)
    except UnknownRunKind as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except UnsupportedRunKind as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from None
    except CanonicalSchemaUnavailable as exc:
        raise _schema_gap(exc) from None
    except RunConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return JSONResponse(started, status_code=202)


@router.get("/runs")
async def list_runs(
    limit: int = Query(20, ge=1, le=200), service: RunService = Depends(get_run_service)
) -> Any:
    try:
        return await service.list_runs(limit)
    except (CanonicalSchemaUnavailable, sqlite3.OperationalError) as exc:
        if not _schema_unavailable(exc):
            raise
        raise _schema_gap(exc) from None


@router.get("/runs/{run_uid}")
async def get_run(run_uid: str, service: RunService = Depends(get_run_service)) -> Any:
    try:
        detail = await service.run_detail(run_uid)
    except (CanonicalSchemaUnavailable, sqlite3.OperationalError) as exc:
        if not _schema_unavailable(exc):
            raise
        raise _schema_gap(exc) from None
    if detail is None:
        raise HTTPException(status_code=404, detail=f"no run {run_uid!r}")
    return detail


@router.post("/runs/{run_uid}/cancel")
async def cancel_run(
    run_uid: str, service: RunService = Depends(get_run_service)
) -> JSONResponse:
    try:
        await service.cancel_run(run_uid)
    except UnknownRun as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    except CanonicalSchemaUnavailable as exc:
        # Same gate as create, same code: on a legacy database the feature is
        # unavailable, which is not the same statement as "no such run".
        raise _schema_gap(exc) from None
    except RunConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return JSONResponse({"run_uid": run_uid, "cancelling": True}, status_code=202)


# --------------------------------------------------------------------------- #
# Stream
# --------------------------------------------------------------------------- #
def _frame(row: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "sequence": row["sequence"],
            "event_type": row["event_type"],
            "at": row["at"],
            "source_run_id": row["source_run_id"],
            "payload": row["payload"],
        },
        default=str,
    )
    return f"id: {row['sequence']}\ndata: {payload}\n\n"


def parse_after(after: str | None) -> int | None:
    """`?after=` -> a bindable cursor, or a 400. Never an exception mid-stream.

    Taken as a string rather than an `int` query parameter on purpose: FastAPI
    would answer 422 for `?after=abc` and accept `?after=<2**70>` (a Python int is
    unbounded) only for sqlite3 to raise `OverflowError` when it is bound, inside
    the generator, after the 200 has been sent. One parse, one status code, both
    decided here.
    """
    if after is None:
        return None
    try:
        value = int(after.strip())
    except (AttributeError, ValueError):
        raise HTTPException(
            status_code=400, detail=f"after must be an integer, got {after!r}"
        ) from None
    if not MIN_CURSOR <= value <= MAX_CURSOR:
        raise HTTPException(
            status_code=400,
            detail=f"after must be between {MIN_CURSOR} and {MAX_CURSOR}, got {value}",
        )
    return value


def resume_cursor(last_event_id: str | None, after: int | None) -> int:
    """`Last-Event-ID` wins, then `?after=`, then the beginning of the run.

    -1 rather than 0 because the cursor is exclusive and sequences start at 0: a
    fresh reader must be offered event 0, and a reader that has consumed event 0
    must not be offered it twice.

    A `Last-Event-ID` that is not a usable cursor -- unparseable, or a number no
    `run_events.sequence` could ever hold -- is treated as ABSENT rather than as
    an error. The header is replayed by the browser's own EventSource, not typed
    by a caller, so the useful answer to a corrupted one is "start again from what
    you asked for", not a failed reconnect. `?after=` is the caller's own input
    and is rejected instead (`parse_after`).
    """
    if last_event_id is not None:
        try:
            value = int(last_event_id.strip())
        except (AttributeError, ValueError):
            value = None
        if value is not None and MIN_CURSOR <= value <= MAX_CURSOR:
            return value
    if after is not None:
        return max(MIN_CURSOR, min(int(after), MAX_CURSOR))
    return -1


async def event_stream(
    service: RunService,
    run_uid: str,
    *,
    after: int = -1,
    heartbeat: float = HEARTBEAT_SECONDS,
) -> AsyncIterator[str]:
    """Replay persisted `run_events` past `after`, then live-tail, then close.

    Three properties, and the ordering that buys each of them:

    NO GAP. The subscription is taken BEFORE the first read. An event committed
    between the read and the subscribe would otherwise ring a doorbell nobody was
    holding, and the reader would then sit on `queue.get()` with unread rows on
    disk.

    NO DUPLICATE. `cursor` only ever moves forward and every read is
    `sequence > cursor`. The doorbell carries no payload, so there is nothing to
    de-duplicate against.

    CLEAN CLOSE, WITHOUT A LOST TAIL. Liveness is sampled BEFORE each read, never
    after. The service marks a run settled only after its terminal event has
    committed, so a read that follows an `is_active() == False` sample is
    guaranteed to see that event: settled-then-sampled means committed-then-read.
    Sampling after the read inverts exactly that -- a terminal event that commits
    in the window between the read and the sample is on disk, unread, and the loop
    has already decided to stop -- which is how a stream could close having
    delivered NOTHING for a run whose settled event was durably persisted.

    A run whose process died before settling (nothing more will ever be appended)
    ends on the same path: not active, one full read, close.
    """
    queue = service.subscribe(run_uid)
    cursor = after
    try:
        while True:
            active = service.is_active(run_uid)
            rows = await service.events_after(run_uid, cursor)
            for row in rows:
                cursor = row["sequence"]
                yield _frame(row)
                if row["event_type"] == runservice.EVENT_RUN_SETTLED:
                    return
            if len(rows) >= runservice.EVENT_PAGE_SIZE:
                continue  # a full page means there is more on disk already
            if not active:
                return  # sampled dead before this read: the read saw everything
            try:
                await asyncio.wait_for(queue.get(), timeout=heartbeat)
            except (asyncio.TimeoutError, TimeoutError):
                yield ": heartbeat\n\n"
    finally:
        service.unsubscribe(run_uid, queue)


@router.get("/runs/{run_uid}/events")
async def stream_run_events(
    run_uid: str,
    request: Request,
    after: str | None = Query(None),
    service: RunService = Depends(get_run_service),
) -> StreamingResponse:
    # Order: the caller's own input, then the database's availability, then the
    # existence of the thing being streamed. All three before the response is
    # constructed -- see the module docstring.
    cursor_after = parse_after(after)
    try:
        await service.require_canonical_schema()
    except CanonicalSchemaUnavailable as exc:
        raise _schema_gap(exc) from None
    if not await service.run_exists(run_uid):
        raise HTTPException(status_code=404, detail=f"no run {run_uid!r}")
    cursor = resume_cursor(request.headers.get("last-event-id"), cursor_after)
    return StreamingResponse(
        event_stream(service, run_uid, after=cursor),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )
