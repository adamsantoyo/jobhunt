"""Shared dispatch helper for task 4.6's read flag (`config.READS_SOURCE`).

Every legacy route with a canonical_reads equivalent -- /api/jobs, /api/jobs/
{url_b64}, /api/followups (routers/jobs.py), /api/changes (routers/changes.py),
/api/analytics, /api/freshness (routers/analytics.py) -- opens with the same
two-line guard:

    if config.READS_SOURCE == "canonical":
        read_dispatch.require_canonical(conn)
        return canonical_reads.<fn>(conn, ...)

`require_canonical` 503s when the connection lacks the canonical schema: the
flag is an explicit opt-in (phase 4 spec, wave 2 decision 9), so pointing it at
a database that cannot answer canonically must fail loudly, never fall back to
legacy silently. When the flag is "legacy" (the default), the guard's condition
is false and every handler's pre-existing body runs completely unchanged below
it -- that is what keeps flag=legacy byte-identical to pre-4.6 behavior.

Deliberately duplicates routers/readsv2.py's own `_require_canonical` (four
lines) rather than importing it: this module has no other reason to depend on
a router, and canonical_reads.py's own precedent (see its module docstring) is
to duplicate small, stable logic rather than bend an unrelated module's shape
to fit a read.
"""
from __future__ import annotations

import sqlite3

from fastapi import HTTPException

from .sources import runstore

__all__ = ["require_canonical"]


def require_canonical(conn: sqlite3.Connection) -> None:
    """Raise HTTPException(503) if `conn` lacks the canonical schema."""
    try:
        runstore.require_canonical_schema(conn)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
