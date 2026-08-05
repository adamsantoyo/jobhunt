"""Phase 5, W-5.4: HTTP wiring for `calibration.py`.

  GET /calibration   the calibration gate + (when open) empirical band rates
                     and the model-vs-baseline comparison.

The reference moment (`now`) is computed here per request, so `generated_at` in
the response is a real timestamp and the model arm's response-maturity window
has something to measure against -- see `_now_iso` below.

Read-only, no commit -- the same stance as GET /outcomes/analytics in
`outcomesapi.py`, whose conventions this file follows (module-level
`APIRouter`, `get_db` dependency, plain-dict responses straight from the
analysis module).

Reads canonical tables (`state_events`, `job_state`, `postings`,
`posting_versions`, `score_versions`) DIRECTLY and is therefore orthogonal to
the 4.6 jobs read-flag: that flag chooses between the legacy `jobs` table and
the canonical `postings` graph for JOB LISTING reads, a question this endpoint
never asks. No `require_canonical` guard here, deliberately -- gating outcome
evidence on a listing-path flag would make calibration disappear for reasons
that have nothing to do with calibration.

NOT mounted in main.py -- the orchestrator adds the one-line mount, exactly as
`routers/queueapi.py` and `routers/outcomesapi.py` were mounted at their
integration wave. Tests build their own local FastAPI app.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query

from ..calibration import calibration_report
from ..db import get_db

router = APIRouter()


def _now_iso() -> str:
    """The request's reference moment, computed HERE rather than in
    `calibration.py` -- the same division `queueapi.py` draws when it computes
    `today` at the router and hands it to a clock-free `ranking.build_queue`.
    The analysis module stays deterministic and testable at any moment in
    history; the router supplies the one input that cannot be.

    Naive UTC, deliberately. `state_events.at` is local-naive throughout this
    database, and the maturity window subtracts one from the other; an
    offset-aware value here would raise TypeError on the first mature-row
    check. The offset is dropped at the boundary rather than inside the
    comparison so this file is where the choice is visible.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


@router.get("/calibration")
def get_calibration(
    # ge=1 (not ge=0): a threshold of zero would open the gate on an empty
    # database, which is precisely the state this endpoint exists to refuse.
    # FastAPI turns a violation, or a non-integer, into 422.
    #
    # No upper bound: an absurdly high threshold is not a malformed request,
    # it is a request that gates. `min_applications=1000000` gets a normal 200
    # whose gate record says "gated, 42 of 1000000" -- the honest answer, and
    # one the caller can act on. A 422 there would be the endpoint refusing to
    # answer a question it can answer, and the response costs the same either
    # way (the work is bounded by the database, not by the threshold).
    min_applications: int = Query(50, ge=1),
    min_responses: int = Query(10, ge=1),
    conn: sqlite3.Connection = Depends(get_db),
):
    return calibration_report(
        conn,
        min_applications=min_applications,
        min_responses=min_responses,
        now=_now_iso(),
    )
