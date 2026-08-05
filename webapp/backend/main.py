"""FastAPI application: startup ingest, CSRF guard, /api routers, SPA static mount.

Bound to 127.0.0.1 only (see run.sh). Non-GET requests require the X-App header
(a lightweight local CSRF guard; there is no CORS middleware, so mutations are
same-origin only). The SPA in frontend/dist is served last with a catch-all that
returns index.html for client routes; if dist/ is missing the server stays up and
returns a helpful message instead of crashing.
"""
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from . import config
from .db import connect, init_db
from .ingest import ingest
from .routers import (
    analytics,
    calibrationapi,
    changes,
    configapi,
    funnel,
    jobs,
    outcomesapi,
    queueapi,
    readsv2,
    runsapi,
    sourcesops,
    sweepapi,
    state,
)
from .runservice import recover_orphans_if_canonical, shutdown_default_service
from .sweeprunner import runner


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = connect()
    try:
        init_db(conn)
        # The startup ingest is a legacy write, so 4.7's flag retires it with the
        # rest of them. Announced on stderr rather than skipped silently: unlike
        # JOBHUNT_SKIP_STARTUP_INGEST (asked for per-process, by whoever is
        # watching), this one can be inherited from a cutover-time environment,
        # and "the database stopped moving" must not be a mystery.
        if config.WRITES_SOURCE == "canonical":
            print(f"[startup] ingest skipped: {config.WRITE_GATE_DETAIL}", file=sys.stderr)
        elif not config.SKIP_STARTUP_INGEST:
            try:
                rep = ingest(conn)
                print(f"[startup] ingest ok: {rep.model_dump()}", file=sys.stderr)
            except Exception as e:  # keep the server alive even if ingest fails
                print(f"[startup] ingest FAILED: {e}", file=sys.stderr)
    finally:
        conn.close()
    # Canonical runs left 'running' by a dead process are reconciled here, before
    # any new run can start. Gated on the canonical schema being present and
    # wrapped besides: the live v4 database has none of those tables, and a boot
    # must never depend on this succeeding.
    try:
        report = recover_orphans_if_canonical()
        if report is not None and report.total:
            print(
                f"[startup] canonical recovery: {len(report.run_uids)} run(s), "
                f"{len(report.source_run_ids)} attempt(s) marked interrupted",
                file=sys.stderr,
            )
    except Exception as e:  # noqa: BLE001 - startup must survive a locked/odd database
        print(f"[startup] canonical recovery skipped: {e}", file=sys.stderr)
    yield
    # Teardown: never orphan a running pipeline subprocess (it lives in its own
    # session, so nothing else would reap it after the server exits).
    try:
        await runner.shutdown()
    except Exception as e:  # noqa: BLE001 - teardown must not block shutdown
        print(f"[shutdown] runner cleanup failed: {e}", file=sys.stderr)
    # Canonical runs get the same courtesy: a cancelled run writes its own
    # terminal rows, so stopping this way leaves evidence rather than orphans.
    try:
        await shutdown_default_service()
    except Exception as e:  # noqa: BLE001 - teardown must not block shutdown
        print(f"[shutdown] run service cleanup failed: {e}", file=sys.stderr)


app = FastAPI(title="JobHunt", lifespan=lifespan)


class CsrfGuard:
    """Non-GET requests must carry the X-App header (lightweight local CSRF guard).

    Pure ASGI rather than @app.middleware("http") on purpose. That decorator is
    sugar for BaseHTTPMiddleware, which per request builds an anyio task group and
    a pair of memory object streams, then proxies the response through them -- so
    every chunk of /api/sweep/progress takes an extra queue hop for the whole life
    of a 45-minute sweep. This guard reads one scope key and one header and needs
    none of that.

    It does NOT change disconnect behaviour, despite the folklore: measured on
    starlette 1.3.1 + uvicorn 0.51.0, a client disconnect cancels the streaming
    generator in ~0.2s under both styles, and Request.is_disconnected() never
    observes it under either (StreamingResponse's listen_for_disconnect consumes
    the http.disconnect first). sse_stream relies on that cancellation, not on a
    poll; see its docstring.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # lifespan/websocket scopes carry no "method"; only HTTP is guarded.
        if scope["type"] == "http" and scope["method"] not in ("GET", "HEAD"):
            if Headers(scope=scope).get(config.CSRF_HEADER) != config.CSRF_VALUE:
                res = JSONResponse({"detail": "missing or invalid app header"}, status_code=403)
                await res(scope, receive, send)
                return
        await self.app(scope, receive, send)


app.add_middleware(CsrfGuard)


# --- /api routers FIRST -----------------------------------------------------
for module in (
    jobs,
    state,
    analytics,
    changes,
    sweepapi,
    configapi,
    funnel,
    runsapi,
    readsv2,
    sourcesops,
    queueapi,
    outcomesapi,
    calibrationapi,
):
    app.include_router(module.router, prefix="/api")


# --- SPA static serving LAST ------------------------------------------------
DIST = config.WEBAPP_DIR / "frontend" / "dist"
INDEX = DIST / "index.html"

_MISSING_DIST_MSG = (
    "JobHunt backend is running, but the frontend has not been built.\n"
    "Run:  bash webapp/run.sh\n"
    "(that installs the venv, builds the Vite SPA into frontend/dist, and serves this URL)."
)


@app.get("/{full_path:path}")
async def spa(full_path: str):
    # Unknown /api/* paths get a JSON 404, never the SPA shell.
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="not found")
    if not INDEX.exists():
        return PlainTextResponse(_MISSING_DIST_MSG, status_code=200)
    # Serve a real static asset if the path points at one (guard traversal).
    if full_path:
        candidate = (DIST / full_path).resolve()
        try:
            candidate.relative_to(DIST.resolve())
            if candidate.is_file():
                return FileResponse(candidate)
        except (ValueError, OSError):
            pass
    # Otherwise return the SPA shell so client-side routing works.
    return FileResponse(INDEX)
