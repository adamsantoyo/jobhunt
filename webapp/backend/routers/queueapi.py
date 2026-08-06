"""Task 5.1: GET /api/queue/today -- the server-side Today queue.
Task 5.5a adds GET /api/ranking/metrics and snapshot-on-serve.

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

Snapshot-on-serve (5.5 contract): the FIRST `/api/queue/today` response of the
local day captures a `surface="today"` `recommendation_snapshots` row via
`outcomes.capture_snapshot`, so `/api/ranking/metrics` and 5.5b's open-capture
have a stable `snapshot_id` to attribute against -- see `_capture_today_
snapshot`'s docstring for the once-per-day skip and the capture-failure
contract ("must never break queue serving"). The response's top-level
`snapshot_id` is nullable: the day's captured (or already-existing) snapshot
id on success, null when capture failed or was skipped and none exists yet.

The snapshot is of the DAY'S QUEUE, not of this request's slice of it (5.5 fix
B3): the captured queue is recomputed at the configured `daily_queue_size`,
ignoring whatever remaining-contract `cap` this particular request asked for,
while SERVING still honors the request's cap exactly as before. Otherwise the
day's stable record of "what we recommended" would be whatever cap the first
request of the day happened to carry -- a `cap=2` refresh at 5pm would pin the
whole day's ranking-quality denominator to two rows, and a `cap=0` request
(a finished day) would pin it to zero. `cap=0` still gets the day's
`snapshot_id` back; it just does not get to define it.

GET /api/ranking/metrics (5.5a): thin wiring around `ranking_metrics.
ranking_metrics` -- `now` computed HERE at the router (never inside the pure
analysis module), same division `routers/calibrationapi.py` draws. A database
that predates migration 21 has none of the tables it reads; that surfaces as
503, the same shape `routers/runsapi.py` gives a missing canonical run schema,
rather than a raw 500 (5.5 fix B8).

NOT MOUNTED in `main.py` yet -- same pattern as `readsv2` in wave 4.1/4.2: the
Phase 5 integration session adds the one-line mount once both tracks land, and
until then the tests build their own local FastAPI app.
"""
from __future__ import annotations

import sqlite3
import sys
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import canonical_reads, config, outcomes, ranking, read_dispatch
from ..db import get_db
from ..models import JOB_LIGHT_SQL, JobLight, job_light_from_row, now_iso, today_iso
from ..ranking_metrics import ranking_metrics as compute_ranking_metrics

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


def _today_snapshot_id_for(conn: sqlite3.Connection, day: str) -> Optional[str]:
    """The day's already-captured surface='today' snapshot_id, or None. A
    plain string-prefix match on `captured_at` (never a parse): a server-
    minted snapshot's `captured_at` defaults to `models.now_iso()` inside
    `outcomes.capture_snapshot`, always full-grain ISO, so the first 10
    characters are always that day's date."""
    row = conn.execute(
        "SELECT snapshot_id FROM recommendation_snapshots "
        "WHERE surface='today' AND substr(captured_at, 1, 10)=? "
        "ORDER BY captured_at ASC LIMIT 1",
        (day,),
    ).fetchone()
    return row["snapshot_id"] if row else None


def _posting_ids_for(conn: sqlite3.Connection, urls: list, seen_keys: list) -> tuple[dict, dict]:
    """(by_url, by_seen_key) posting_id lookups for one served queue.

    `by_url` comes from `posting_aliases` -- the ACTIVE (`valid_to IS NULL`)
    alias row carrying that url, newest-first tiebreak, exactly the join
    `canonical_reads._posting_id_for_url` uses. This is the primary bridge
    (5.5 fix B1): `posting_aliases` covers the WHOLE canonical corpus, because
    every claim writes one (`runstore._insert_alias`), whereas `job_state`
    only ever has a row for a job the VISITOR has touched. Resolving through
    job_state alone is why capture found a posting for 18 of 1095 jobs on the
    real database while the Today queue is, by construction, made of jobs
    nobody has acted on yet -- the two populations barely intersect, so the
    whole attribution thread downstream was dead on real data.

    `by_seen_key` is the `job_state.posting_id` bridge, kept as a FALLBACK
    only: a dormant row addressed by a seen_key whose url the alias table no
    longer carries still resolves.

    Both are single batched queries rather than a point query per entry (the
    5.3 watch item on corpus-scale point lookups). One IN-list each is enough
    without chunking: a served queue is bounded by `_MAX_CAP` (100), far under
    SQLite's parameter limit."""
    by_url: dict = {}
    if urls:
        rows = conn.execute(
            f"SELECT url, posting_id FROM ("
            f"  SELECT url, posting_id, ROW_NUMBER() OVER ("
            f"    PARTITION BY url ORDER BY valid_from DESC, alias_id DESC) AS rn "
            f"  FROM posting_aliases "
            f"  WHERE valid_to IS NULL AND url IS NOT NULL "
            f"    AND url IN ({','.join('?' * len(urls))})"
            f") WHERE rn=1",
            urls,
        ).fetchall()
        by_url = {r["url"]: r["posting_id"] for r in rows if r["posting_id"]}

    by_seen_key: dict = {}
    if seen_keys:
        rows = conn.execute(
            f"SELECT seen_key, posting_id FROM job_state "
            f"WHERE posting_id IS NOT NULL AND seen_key IN ({','.join('?' * len(seen_keys))})",
            seen_keys,
        ).fetchall()
        by_seen_key = {r["seen_key"]: r["posting_id"] for r in rows}
    return by_url, by_seen_key


def _snapshot_items(conn: sqlite3.Connection, entries: list) -> list:
    """`capture_snapshot` items for a served queue: one per entry whose
    posting_id resolves, in rank order.

    An entry that resolves to nothing (a legacy job never linked to the
    canonical graph) is simply left out; `capture_snapshot`'s `queue_size`
    still records the FULL displayed count (its own documented partial-view
    semantics: `queue_size >= len(items)` is legal), so a legacy-heavy queue
    is honestly under-represented in the snapshot's items rather than
    silently dropped from the day's capture entirely.

    Two entries resolving to the SAME posting_id (two urls aliasing one
    canonical posting -- rare, but the alias bridge makes it reachable in a
    way the job_state bridge did not) keep the BETTER-ranked one and drop the
    other. `capture_snapshot` rejects a duplicate posting_id for the whole
    batch, so without this a single aliased pair would cost the day its
    entire snapshot."""
    urls = [entry.job.url for entry in entries if entry.job.url]
    seen_keys = [entry.job.seen_key for entry in entries if entry.job.seen_key]
    by_url, by_seen_key = _posting_ids_for(conn, urls, seen_keys)

    items = []
    claimed: set = set()
    for entry in entries:
        posting_id = by_url.get(entry.job.url) or by_seen_key.get(entry.job.seen_key)
        if not posting_id or posting_id in claimed:
            continue
        claimed.add(posting_id)
        items.append({"posting_id": posting_id, "rank": entry.rank})
    return items


def _capture_today_snapshot(conn: sqlite3.Connection, entries_for_day) -> Optional[str]:
    """Best-effort snapshot-on-serve (5.5 contract). The FIRST `/api/queue/
    today` response of the local day becomes a `surface="today"` `recommendation_
    snapshots` row; a later same-day request (a shrunken remaining-contract
    cap, a page refresh) finds the existing row via `_today_snapshot_id_for`
    and echoes its id rather than capturing again -- "the day's snapshot is
    the first-served queue of that day" per the 5.5 contract's recorded
    decision.

    `entries_for_day` is a CALLABLE, not a list: it is invoked only when a
    capture is actually going to happen, so the common path (the day's
    snapshot already exists) never pays to rebuild the full-configured-cap
    queue B3 asks the capture to record.

    ONE timestamp source (5.5 fix B10): `captured_at` is read once and the
    day key is its own first ten characters, so a request that crosses local
    midnight between the two cannot file the row under the day BEFORE the one
    its `captured_at` claims -- which would leave the new day looking
    uncaptured and capture a second snapshot moments later.

    The existence check and the capture run inside one `BEGIN IMMEDIATE`
    (5.5 fix B4): two first-of-day requests arriving together would otherwise
    both read "no snapshot yet" and both write one, and the day would have
    two disagreeing records of what was recommended. IMMEDIATE takes the
    write lock at the check, so the loser blocks and then sees the winner's
    row. No migration and no unique index for this -- the transaction is the
    whole mechanism, and `capture_snapshot`'s own trailing `commit()` ends it.

    ANY failure here (a missing table on a pre-migration-21 database, a write
    conflict, an unexpected exception from `capture_snapshot`) is caught and
    reported to stderr, never raised -- queue serving must never break
    because the ranking-quality side channel could not write. Returns None on
    skip-due-to-failure; the caller still serves the queue, with a null
    `snapshot_id`.
    """
    began = False
    try:
        # A caller with a transaction already open (nothing in this router
        # does) cannot be handed a nested BEGIN; degrade to the unguarded
        # check rather than raising over it.
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
            began = True
        captured_at = now_iso()
        existing = _today_snapshot_id_for(conn, captured_at[:10])
        if existing is not None:
            if began:
                conn.rollback()  # nothing written; release the write lock
            return existing
        entries = entries_for_day()
        result = outcomes.capture_snapshot(
            conn, surface="today", items=_snapshot_items(conn, entries),
            at=captured_at, queue_size=len(entries),
        )
        return result["snapshot_id"]
    except Exception as exc:  # noqa: BLE001 - snapshot-on-serve must never break queue serving
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - a rollback failure must not mask `exc`
            pass
        print(f"[queue] snapshot-on-serve failed: {exc}", file=sys.stderr)
        return None


@router.get("/queue/today")
def today_queue(
    cap: Optional[int] = Query(default=None, ge=0, le=_MAX_CAP),
    conn: sqlite3.Connection = Depends(get_db),
):
    jobs = _candidates(conn)
    configured_cap = _configured_cap(conn)
    effective_cap = cap if cap is not None else configured_cap
    today = today_iso()
    result = ranking.build_queue(jobs, cap=effective_cap, today=today)

    def _day_queue() -> list:
        """The day's FULL queue for the snapshot (B3) -- the configured
        `daily_queue_size`, regardless of this request's remaining-contract
        `cap`. Rebuilt only when it differs from what was just served, and
        only when a capture is actually going to happen
        (`_capture_today_snapshot` calls this lazily)."""
        if configured_cap == effective_cap:
            return result.entries
        return ranking.build_queue(jobs, cap=configured_cap, today=today).entries

    # A daily_queue_size below 1 means the user asked for no queue: capturing
    # a fabricated max(1, ...) snapshot would invent a queue nobody saw
    # (seam L3), so the day simply records no snapshot.
    snapshot_id = _capture_today_snapshot(conn, _day_queue) if configured_cap >= 1 else None
    return {
        "generated_for": today,
        "cap": effective_cap,
        "snapshot_id": snapshot_id,
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


@router.get("/ranking/metrics")
def get_ranking_metrics(
    # ge=1 (not ge=0), same reasoning routers/calibrationapi.py's min_
    # applications/min_responses use: a threshold of zero would flag every
    # cell as never-low-sample, defeating the flag's purpose.
    min_sample: int = Query(5, ge=1),
    ghost_days: int = Query(21, ge=1),
    conn: sqlite3.Connection = Depends(get_db),
):
    try:
        return compute_ranking_metrics(
            conn, min_sample=min_sample, ghost_days=ghost_days, now=now_iso(),
        )
    except sqlite3.OperationalError as exc:
        # B8: every table this reads arrived in migration 21. On an older
        # database the first SELECT raises "no such table", which is the
        # database saying "not migrated", not a server fault -- so it answers
        # 503 like `routers/runsapi.py`'s canonical-run-schema gap, not 500.
        # Any OTHER OperationalError is a real bug and must still surface.
        if "no such table" not in str(exc).lower():
            raise
        raise HTTPException(
            status_code=503, detail="canonical tables not migrated"
        ) from None
