"""Task 5.1: GET /api/queue/today -- the server-side Today queue.

The impure boundary around `backend.ranking.build_queue`: assemble candidates
via the SAME read-flag dispatch every legacy route with a canonical equivalent
uses (task 4.6's two-line guard -- see `read_dispatch`'s docstring), compute
`today` once, and serialize the pure result. The READS flip therefore needs no
queue-side change: flag=canonical routes through `canonical_reads.list_jobs`
with the same 503-on-legacy-schema guard as every other canonical read.

`cap` semantics: the query param is the number of slots the caller wants FILLED
-- the client passes its remaining daily contract (daily_queue_size minus
done-today, exactly as `composeQueue` receives today); with no param the
configured `daily_queue_size` is used whole. Subtracting done-today stays a
client concern because done-today comes from /api/activity, which the client
already holds. The app_settings read duplicates configapi's two-line `_get_int`
rather than importing a private router helper (read_dispatch's precedent:
duplicate small stable logic over bending an unrelated module's shape).

NOT MOUNTED in `main.py` yet -- same pattern as `readsv2` in wave 4.1/4.2: the
Phase 5 integration session adds the one-line mount once both tracks land, and
until then the tests build their own local FastAPI app.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, Query

from .. import canonical_reads, config, ranking, read_dispatch
from ..db import get_db
from ..models import JOB_LIGHT_SQL, JobLight, job_light_from_row, today_iso

router = APIRouter()

#: Bound the requested cap: 0 is legal (a finished day still gets its exclusion
#: accounting), 100 is far past any usable daily contract.
_MAX_CAP = 100


def _configured_cap(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key='daily_queue_size'"
    ).fetchone()
    if row is not None:
        try:
            return max(0, min(_MAX_CAP, int(row["value"])))
        except (TypeError, ValueError):
            pass
    return config.DEFAULT_DAILY_QUEUE_SIZE


def _candidates(conn: sqlite3.Connection) -> list[JobLight]:
    if config.READS_SOURCE == "canonical":
        read_dispatch.require_canonical(conn)
        raw = canonical_reads.list_jobs(conn)["jobs"]
        # canonical rows are JobLight-dicts plus add-only keys (posting_id);
        # model_validate ignores the extras and type-checks the rest loudly.
        return [JobLight.model_validate(j) for j in raw]
    rows = conn.execute(f"{JOB_LIGHT_SQL} WHERE j.present=1").fetchall()
    return [job_light_from_row(r) for r in rows]


@router.get("/queue/today")
def today_queue(
    cap: Optional[int] = Query(default=None, ge=0, le=_MAX_CAP),
    conn: sqlite3.Connection = Depends(get_db),
):
    jobs = _candidates(conn)
    effective_cap = cap if cap is not None else _configured_cap(conn)
    today = today_iso()
    result = ranking.build_queue(jobs, cap=effective_cap, today=today)
    return {
        "generated_for": today,
        "cap": effective_cap,
        "queue": [
            {
                "job": entry.job,
                "rank": entry.rank,
                "lane": entry.lane,
                "lane_rank": entry.lane_rank,
                "evidence": entry.evidence,
            }
            for entry in result.entries
        ],
        "excluded": [
            {
                "url_b64": ex.url_b64,
                "title": ex.title,
                "company": ex.company,
                "reason": ex.reason,
                "detail": ex.detail,
            }
            for ex in result.excluded
        ],
        "excluded_counts": result.excluded_counts,
        "considered": result.considered,
    }
