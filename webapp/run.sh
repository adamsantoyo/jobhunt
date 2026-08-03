#!/usr/bin/env bash
# Idempotent launcher for the JobHunt local web app.
#   locked root venv -> npm ci + build -> uvicorn 127.0.0.1:8000 -> open URL
# Safe to re-run; writes only generated environments/assets and webapp/app.db.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv is required (macOS: brew install uv)" >&2
  exit 1
fi

echo ">>> syncing locked Python environment"
(cd "$ROOT" && uv sync --frozen --all-groups --inexact)

# Build the SPA if the frontend project is present.
if [ -f "$HERE/frontend/package.json" ]; then
  echo ">>> building frontend"
  (
    cd "$HERE/frontend"
    if [ -f package-lock.json ]; then npm ci; else npm install; fi
    npm run build
  )
else
  echo ">>> frontend/ not present yet; the API will serve a placeholder page"
fi

PORT="${JOBHUNT_PORT:-8000}"
URL="http://127.0.0.1:$PORT"
# Open the browser shortly after the server comes up (best-effort, non-fatal).
(
  sleep 2
  if command -v open >/dev/null 2>&1; then open "$URL"
  elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$URL"
  fi
) >/dev/null 2>&1 &

echo ">>> serving $URL  (Ctrl-C to stop)"
# --timeout-graceful-shutdown: an open SSE progress stream would otherwise make
# Ctrl-C drain forever, so lifespan teardown (which kills any running pipeline
# subprocess) would never run.
cd "$HERE"
exec uv run --frozen python -m uvicorn backend.main:app --host 127.0.0.1 --port "$PORT" --timeout-graceful-shutdown 5
