"""FastAPI application: startup ingest, CSRF guard, /api routers, SPA static mount.

Bound to 127.0.0.1 only (see run.sh). Non-GET requests require the X-App header
(a lightweight local CSRF guard; there is no CORS middleware, so mutations are
same-origin only). The SPA in frontend/dist is served last with a catch-all that
returns index.html for client routes; if dist/ is missing the server stays up and
returns a helpful message instead of crashing.
"""
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from . import config
from .db import connect, init_db
from .ingest import ingest
from .routers import analytics, changes, configapi, jobs, sweepapi, state


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = connect()
    try:
        init_db(conn)
        if not config.SKIP_STARTUP_INGEST:
            try:
                rep = ingest(conn)
                print(f"[startup] ingest ok: {rep.model_dump()}", file=sys.stderr)
            except Exception as e:  # keep the server alive even if ingest fails
                print(f"[startup] ingest FAILED: {e}", file=sys.stderr)
    finally:
        conn.close()
    yield


app = FastAPI(title="JobHunt", lifespan=lifespan)


@app.middleware("http")
async def csrf_guard(request: Request, call_next):
    if request.method not in ("GET", "HEAD"):
        if request.headers.get(config.CSRF_HEADER) != config.CSRF_VALUE:
            return JSONResponse({"detail": "missing or invalid app header"}, status_code=403)
    return await call_next(request)


# --- /api routers FIRST -----------------------------------------------------
for module in (jobs, state, analytics, changes, sweepapi, configapi):
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
