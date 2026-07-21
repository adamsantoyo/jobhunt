#!/usr/bin/env bash
# Idempotent launcher for the JobHunt local web app.
#   venv (stable python) -> pip install -> npm ci + build -> uvicorn 127.0.0.1:8000 -> open URL
# Safe to re-run; only ever writes inside webapp/ (venv, node_modules, dist, app.db).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Prefer a stable interpreter; fall back through what's installed.
PY=""
for c in python3.13 python3.12 python3.14 python3; do
  if command -v "$c" >/dev/null 2>&1; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "error: no python interpreter found (tried python3.13/3.12/3.14/python3)" >&2
  exit 1
fi

VENV="$HERE/.venv-web"
if [ ! -d "$VENV" ]; then
  echo ">>> creating venv ($PY) at $VENV"
  "$PY" -m venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"

echo ">>> installing python requirements"
python -m pip install --upgrade pip >/dev/null
python -m pip install -r "$HERE/requirements.txt"

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

URL="http://127.0.0.1:8000"
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
exec python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --timeout-graceful-shutdown 5
