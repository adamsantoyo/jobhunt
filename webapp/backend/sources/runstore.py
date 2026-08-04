"""Canonical SQLite persistence for the Phase 2.3 scheduler.

Every function here is synchronous, takes an explicit `sqlite3.Connection`, and
performs no transaction control of its own: the caller (`writer.SqliteWriter`)
owns `BEGIN`/`COMMIT` so that a whole batch of state transitions and record
upserts lands atomically. Nothing in this module opens a connection, reads
`config.DB_PATH`, or knows what a run is *for* — that keeps the live `app.db`
structurally unreachable from here and makes every function testable against a
`tmp_path` database.

What this module is NOT:

  * It never touches the legacy `jobs` / `job_history` / `job_state` /
    `state_events` tables. Phase 4 owns the cutover; until then legacy reads stay
    authoritative and there is exactly one writer per table.
  * It scores nothing. `posting_versions.tier/odds/...` stay NULL on every row this
    module writes: a source version records what a source SAID, and what that is
    worth is Phase 3.4's judgement against a profile.
  * It marks a posting absent only through `apply_run_presence`, and only under the
    licence `source_runs.status = 'succeeded'` plus
    `source_runs.inventory_scope = 'complete'` grants. Absence is a reversible
    marking with evidence; no row is ever removed.

Identity, restated because this is where the rule is enforced (Phase 1 decision,
`contract.IdentityClaim`):

  rank 0  source-native requisition id, namespaced `source_key:instance_key`.
          Authoritative.
  rank 1  normalized URL, namespace `"url"`. Conservative secondary evidence.
          NEVER a global primary key.

Resolution, precisely:

  * A record that carries a requisition id is identified by that claim ALONE. If
    the claim is new, the record is a new posting — even when its URL is already
    an alias of something else. Letting the URL win there would merge two
    distinct requisitions that happen to share an address, which is exactly the
    "boards recycle URLs" failure the contract warns about, and the user's status
    and notes would follow the merge.
  * A record with no requisition id (aggregators routinely have none) is
    identified by its URL, because that is the only evidence it offers.
  * Every claim is then recorded as an alias of the resolved posting. A claim
    whose alias already points elsewhere is left untouched and the disagreement
    is written to `identity_evidence`.

The consequence, stated plainly: a board posting and an aggregator's mirror of it
converge on one posting when the board is seen first, and stay two postings with
recorded conflict evidence when the aggregator is seen first. That asymmetry is
deliberate. Cross-source resolution is Phase 3's job ("aggregators resolve
locally against direct inventory using aliases and similarity"), and Phase 2
guessing at it would produce merges nothing downstream could undo.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from .contract import IdentityClaim, JSONValue, NormalizedPosting

__all__ = [
    "DEFAULT_STALE_AFTER_SECONDS",
    "RecordOutcome",
    "RecoveryReport",
    "SOURCE_REQ_ALIAS_KIND",
    "SOURCE_RUN_STEP",
    "SOURCE_VERSION_KIND",
    "TERMINAL_SOURCE_RUN_STATUSES",
    "UNATTEMPTED_SOURCE_RUN_STEP",
    "append_run_events",
    "apply_run_presence",
    "canonical_json",
    "change_summary",
    "create_pipeline_run",
    "create_source_run",
    "dirty_posting_ids",
    "finish_pipeline_run",
    "finish_source_run",
    "latest_checkpoint_json",
    "mark_absent_for_scope",
    "max_attempt_by_source",
    "new_uid",
    "next_event_sequence",
    "posting_id_for_claim",
    "posting_version_id_for",
    "record_unattempted_source_run",
    "recover_orphans",
    "refresh_presence",
    "require_canonical_schema",
    "resumable_runs",
    "resume_plan",
    "reopen_pipeline_run",
    "source_instance_freshness",
    "successful_source_scopes",
    "update_source_run_progress",
    "utc_now_iso",
    "write_records",
]

#: `source_runs.step` for a scheduler fetch attempt. The column exists because the
#: legacy runner had several steps per source; the canonical engine has one, and
#: naming it explicitly keeps the UNIQUE(run_uid, source, step, attempt) key
#: meaningful if Phase 3 adds `describe`/`score` steps against the same run.
SOURCE_RUN_STEP = "fetch"

#: `source_runs.step` for a target that reached a terminal state WITHOUT ever
#: attempting a fetch — cancelled while queued at a gate, or never spawned because
#: the cancel arrived first. It is a separate step, not a fetch attempt, and that is
#: the whole point: every fetch-attempt query in this module is bounded to
#: `step = SOURCE_RUN_STEP`, so a row recorded here can never consume an attempt
#: number a resume still needs (`max_attempt_by_source`), licence absence
#: (`successful_source_scopes`), or degrade a source's freshness
#: (`source_instance_freshness`). The `attempt` column is 1 because the schema
#: requires `attempt >= 1`; it is a placeholder, never an attempt count.
UNATTEMPTED_SOURCE_RUN_STEP = "unattempted"

#: The alias kind that carries a source instance's own requisition identity. It is
#: the ONLY evidence that says "this posting belongs to `greenhouse:anthropic`",
#: because its namespace is the instance's `source_run_key`. A rank-1 URL alias lives
#: in the shared `"url"` namespace and names no instance at all, so it can never scope
#: absence — see `_owned_by_instance_sql`.
SOURCE_REQ_ALIAS_KIND = "source_req"

#: How old a source instance's last successful enumeration may get before
#: `source_instance_freshness` calls its data stale. One day: the daily run is the
#: cadence the Success Contract is written against, so a source that missed a full
#: day of them is degraded whatever its last attempt said.
DEFAULT_STALE_AFTER_SECONDS = 24 * 3600

#: Statuses that make a `source_runs` row immutable evidence. `finish_source_run`
#: refuses to write over any of them, which is how "never overwrite a failed
#: attempt's timing" is enforced structurally rather than by convention.
TERMINAL_SOURCE_RUN_STATUSES = frozenset(
    {"succeeded", "failed", "timeout", "cancelled", "interrupted", "skipped"}
)

#: `posting_versions.version_kind` for a version minted from what a source said.
#: Distinct from migration 11's one-time 'legacy-history'/'legacy-current' rows, and
#: from whatever kind a future scoring pass writes: every query in this module that
#: reads versions is bounded by it, so a later kind cannot silently become the answer
#: to "what content did we last see".
SOURCE_VERSION_KIND = "source"

#: Deterministic id namespaces. Derived rather than hard-coded so the derivation
#: is auditable; they are constants at runtime.
_POSTING_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/posting")
_EVIDENCE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/evidence")
_VERSION_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/posting-version"
)

#: How many posting ids one prefetch statement carries. Well under every SQLite build's
#: `SQLITE_MAX_VARIABLE_NUMBER` (999 on the oldest ones still in the wild), and large
#: enough that a scheduler batch is one or two statements rather than one per record.
_LOOKUP_CHUNK = 400


def utc_now_iso() -> str:
    """UTC ISO-8601. Run/attempt timing is audit metadata, like `schema_version`."""
    return datetime.now(timezone.utc).isoformat()


def new_uid() -> str:
    return str(uuid.uuid4())


def canonical_json(value: object) -> str:
    """Stable JSON for hashing and for `*_json` columns. `default=str` so an
    unexpected type in an adapter's `extra` degrades to a string instead of
    failing a whole batch."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _json_or_none(value: object) -> str | None:
    return None if value is None else canonical_json(value)


#: Migration 15's presence columns, named here rather than imported so that this
#: module keeps its "no knowledge of migrations" property and a schema drift shows up
#: as a clear scheduler-start error instead of an OperationalError three batches in.
_PRESENCE_COLUMNS = frozenset(
    {
        "last_seen_at",
        "last_seen_run_uid",
        "absent_since",
        "absent_run_uid",
        "absent_source_run_id",
        "returned_at",
    }
)

#: Migration 16's index, named here for the same reason as the presence columns: this
#: module knows nothing about `migrations.py`, so a drift has to surface as a clear
#: scheduler-start error rather than as a slow query nobody notices.
_VERSION_SOURCE_RUN_INDEX = "idx_posting_versions_source_run"


def require_canonical_schema(conn: sqlite3.Connection) -> None:
    """Fail loudly, once, if handed a database the scheduler cannot write.

    The scheduler deliberately does not run migrations: it is not allowed to
    mutate a schema it did not open, and `db.init_db()` is the one place that
    converges a database. This check turns "wrong database" into a clear error at
    run start rather than an FK failure three batches in.
    """
    required = {
        "pipeline_runs",
        "source_runs",
        "run_events",
        "postings",
        "posting_aliases",
        "posting_versions",
        "identity_evidence",
        "run_postings",
    }
    present = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('pipeline_runs','source_runs','run_events','postings','posting_aliases',"
            "'posting_versions','identity_evidence','run_postings')"
        )
    }
    missing = sorted(required - present)
    if missing:
        raise RuntimeError(
            "scheduler requires the canonical schema; missing tables: " + ", ".join(missing)
        )
    source_columns = {r["name"] for r in conn.execute("PRAGMA table_info(source_runs)")}
    membership_columns = {r["name"] for r in conn.execute("PRAGMA table_info(run_postings)")}
    if "inventory_scope" not in source_columns or "content_hash" not in membership_columns:
        raise RuntimeError(
            "scheduler requires schema version 14 "
            "(source_runs.inventory_scope, run_postings.content_hash)"
        )
    posting_columns = {r["name"] for r in conn.execute("PRAGMA table_info(postings)")}
    missing_presence = sorted(_PRESENCE_COLUMNS - posting_columns)
    if missing_presence:
        raise RuntimeError(
            "scheduler requires schema version 15; postings is missing: "
            + ", ".join(missing_presence)
        )
    # Migration 16's index. Checked here rather than left to run slowly, because its
    # absence is not a correctness bug that shows up as an error — it is a full scan
    # of `posting_versions` in every run's change accounting, which gets quietly
    # worse as the corpus grows. A wrong-schema database has to say so at run start.
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
        (_VERSION_SOURCE_RUN_INDEX,),
    ).fetchone() is None:
        raise RuntimeError(
            f"scheduler requires schema version 16 ({_VERSION_SOURCE_RUN_INDEX})"
        )
    if "source_state_json" not in membership_columns:
        raise RuntimeError(
            "scheduler requires schema version 18 (run_postings.source_state_json)"
        )


# --------------------------------------------------------------------------- #
# Runs and attempts
# --------------------------------------------------------------------------- #
def create_pipeline_run(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    kind: str,
    status: str = "running",
    trigger: str | None = None,
    requested_at: str,
    started_at: str | None = None,
    config_hash: str | None = None,
    code_hash: str | None = None,
    scorer_hash: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO pipeline_runs "
        "(run_uid, kind, status, trigger, requested_at, started_at, "
        "config_hash, code_hash, scorer_hash) VALUES (?,?,?,?,?,?,?,?,?)",
        (run_uid, kind, status, trigger, requested_at, started_at,
         config_hash, code_hash, scorer_hash),
    )


def reopen_pipeline_run(conn: sqlite3.Connection, *, run_uid: str, started_at: str) -> bool:
    """Put an interrupted run back into 'running' for an explicit resume.

    Only an `interrupted` run may be reopened: a run that finished, failed, or was
    cancelled has a settled outcome, and re-entering it would append attempts to a
    row whose counts already claim to be final. `started_at` is deliberately NOT
    rewritten — the original start is immutable evidence.
    """
    cursor = conn.execute(
        "UPDATE pipeline_runs SET status='running' WHERE run_uid=? AND status='interrupted'",
        (run_uid,),
    )
    return cursor.rowcount > 0


def finish_pipeline_run(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    status: str,
    finished_at: str,
    kept_count: int | None = None,
    new_count: int | None = None,
    report: object = None,
    error: object = None,
) -> None:
    conn.execute(
        "UPDATE pipeline_runs SET status=?, finished_at=?, kept_count=?, new_count=?, "
        "aggregate_report_json=?, error_json=? WHERE run_uid=?",
        (status, finished_at, kept_count, new_count,
         _json_or_none(report), _json_or_none(error), run_uid),
    )


def create_source_run(
    conn: sqlite3.Connection,
    *,
    source_run_id: str,
    run_uid: str,
    source: str,
    attempt: int,
    status: str = "running",
    step: str = SOURCE_RUN_STEP,
    deadline_at: str | None = None,
    requested_at: str | None = None,
    started_at: str | None = None,
    inventory_scope: str | None = None,
    metadata: object = None,
) -> None:
    """Append one attempt row. Never an upsert: a retry is a new row, so attempt 1's
    timing and failure survive verbatim next to attempt 2's."""
    conn.execute(
        "INSERT INTO source_runs "
        "(source_run_id, run_uid, source, step, attempt, status, deadline_at, requested_at, "
        "started_at, inventory_scope, metadata_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (source_run_id, run_uid, source, step, attempt, status, deadline_at, requested_at,
         started_at, inventory_scope, _json_or_none(metadata)),
    )


def record_unattempted_source_run(
    conn: sqlite3.Connection,
    *,
    source_run_id: str,
    run_uid: str,
    source: str,
    status: str,
    requested_at: str | None = None,
    finished_at: str | None = None,
    inventory_scope: str | None = None,
    error: object = None,
    metadata: object = None,
) -> bool:
    """Record a planned target that settled without ever attempting a fetch.

    A target cancelled while queued at a gate — or never spawned at all, because the
    cancel arrived before the run's first tick — has no timing of its own to record,
    but it is still part of the plan the run claims to describe. Without a row here
    it exists only in the in-memory run report, and a later reader of `source_runs`
    cannot tell "this board was never asked" from "this board was never planned".

    The row is terminal on creation (`started_at` stays NULL: nothing started) and is
    written under `UNATTEMPTED_SOURCE_RUN_STEP`, which is what keeps it out of the
    fetch-attempt sequence entirely. `attempt` is 1 only because the schema constrains
    the column to `>= 1`; no query reads it.

    `ON CONFLICT ... DO NOTHING` on the one constraint that may legitimately collide,
    so a run and a later resume of it that both cancel the same target before it
    starts keep one row rather than tripping UNIQUE(run_uid, source, step, attempt).
    The first row is the one kept: it names the earlier instant at which the target
    was known not to have run. Deliberately NOT `INSERT OR IGNORE`, which would also
    swallow a NOT NULL or CHECK violation and report it as "already existed" — a
    schema mismatch has to surface as a writer failure, not as silently missing
    evidence. Returns True when a row was actually inserted, which the caller needs
    in order to decide whether an event may reference `source_run_id`.
    """
    cursor = conn.execute(
        "INSERT INTO source_runs "
        "(source_run_id, run_uid, source, step, attempt, status, requested_at, "
        "finished_at, inventory_scope, error_json, metadata_json) "
        "VALUES (?,?,?,?,1,?,?,?,?,?,?) "
        "ON CONFLICT (run_uid, source, step, attempt) DO NOTHING",
        (source_run_id, run_uid, source, UNATTEMPTED_SOURCE_RUN_STEP, status,
         requested_at, finished_at, inventory_scope,
         _json_or_none(error), _json_or_none(metadata)),
    )
    return cursor.rowcount > 0


def update_source_run_progress(
    conn: sqlite3.Connection,
    *,
    source_run_id: str,
    fetched_count: int,
    accepted_delta: int,
    changed_delta: int = 0,
    checkpoint_json: str | None = None,
) -> None:
    """Publish in-flight progress for a still-running attempt.

    `accepted_count` accumulates in SQL rather than being assigned from a
    caller-side total: two batches for the same target can share one transaction,
    and an assignment computed before the first of them committed would report a
    stale number. `fetched_count` is a monotonic counter owned by the target loop,
    so assignment is correct for it.

    `changed_count` accumulates the same way, and Phase 3.1 is what finally fills the
    column migration 6 declared: how many DISTINCT POSTINGS this attempt's batches
    moved to different content. Distinct per batch, summed across batches, so an
    attempt that re-delivers one posting several times in one batch counts it once.
    The authoritative dirty SET is `dirty_posting_ids`, computed from committed
    membership rows; this column is per-attempt progress, not the set.

    Bounded by `status='running'`, so this can never touch a settled attempt. The
    checkpoint is written in the same transaction as the batch it describes, which
    makes "everything before this cursor was delivered" true of committed data
    rather than merely of delivered data.
    """
    conn.execute(
        # SQLite evaluates every SET expression against the pre-update row, so the
        # delta is added twice rather than item_count reading the new accepted_count.
        "UPDATE source_runs SET fetched_count=?, "
        "accepted_count=COALESCE(accepted_count, 0) + ?, "
        "item_count=COALESCE(accepted_count, 0) + ?, "
        "changed_count=COALESCE(changed_count, 0) + ?, "
        "checkpoint_json=COALESCE(?, checkpoint_json) "
        "WHERE source_run_id=? AND status='running'",
        (fetched_count, accepted_delta, accepted_delta, changed_delta,
         checkpoint_json, source_run_id),
    )


def finish_source_run(
    conn: sqlite3.Connection,
    *,
    source_run_id: str,
    status: str,
    finished_at: str,
    fetched_count: int | None = None,
    accepted_count: int | None = None,
    changed_count: int | None = None,
    checkpoint_json: str | None = None,
    error: object = None,
    metadata: object = None,
) -> bool:
    """Settle one attempt. Returns False if the row was already terminal.

    `accepted_count` defaults to whatever the batches accumulated rather than to a
    caller-supplied total. The caller settles an attempt as soon as its stream
    ends, which is *before* its last batch has necessarily committed, so a total
    computed at that moment would be short by one batch. The batches are ordered
    ahead of this update in the writer queue, so the accumulated value is exact by
    the time this runs.

    `changed_count` is accumulated by `update_source_run_progress` as batches commit,
    the way `accepted_count` is, so `None` here means "leave whatever is there", not
    "write NULL" — settling an attempt must not erase the count its batches built.
    A caller may still pass an explicit total to override it; nothing does.

    `metadata` is merged into whatever the attempt was created with rather than
    replacing it, so the execution mode and deadline recorded at start survive
    alongside the duration recorded at finish.
    """
    merged = _merged_metadata(conn, source_run_id, metadata)
    cursor = conn.execute(
        "UPDATE source_runs SET status=?, finished_at=?, "
        "fetched_count=COALESCE(?, fetched_count, 0), "
        "accepted_count=COALESCE(?, accepted_count, 0), "
        "item_count=COALESCE(?, item_count, accepted_count, 0), "
        "changed_count=COALESCE(?, changed_count), "
        "checkpoint_json=COALESCE(?, checkpoint_json), "
        "error_json=?, metadata_json=COALESCE(?, metadata_json) "
        "WHERE source_run_id=? AND status='running'",
        (status, finished_at, fetched_count, accepted_count, accepted_count, changed_count,
         checkpoint_json, _json_or_none(error), merged, source_run_id),
    )
    return cursor.rowcount > 0


def _load_metadata(blob: str | None) -> dict[str, JSONValue]:
    if not blob:
        return {}
    try:
        loaded = json.loads(blob)
    except (TypeError, ValueError):
        return {"unparsed_metadata_json": blob}
    return loaded if isinstance(loaded, dict) else {"metadata": loaded}


def _merged_metadata(
    conn: sqlite3.Connection, source_run_id: str, metadata: object
) -> str | None:
    if metadata is None:
        return None
    row = conn.execute(
        "SELECT metadata_json FROM source_runs WHERE source_run_id=?", (source_run_id,)
    ).fetchone()
    existing = _load_metadata(row["metadata_json"] if row else None)
    if isinstance(metadata, Mapping):
        existing.update(metadata)
    else:
        existing["metadata"] = metadata
    return canonical_json(existing)


def next_event_sequence(conn: sqlite3.Connection, run_uid: str) -> int:
    row = conn.execute(
        "SELECT MAX(sequence) FROM run_events WHERE run_uid=?", (run_uid,)
    ).fetchone()
    return 0 if row is None or row[0] is None else int(row[0]) + 1


def append_run_events(conn: sqlite3.Connection, events: Iterable[Mapping[str, object]]) -> int:
    """Insert pre-sequenced run events. The writer owns sequence allocation, so
    UNIQUE(run_uid, sequence) is satisfied by construction rather than by retry."""
    rows = [
        (
            event.get("run_event_id") or new_uid(),
            event["run_uid"],
            event.get("source_run_id"),
            int(event["sequence"]),
            event["event_type"],
            event["at"],
            _json_or_none(event.get("payload")),
        )
        for event in events
    ]
    if rows:
        conn.executemany(
            "INSERT INTO run_events "
            "(run_event_id, run_uid, source_run_id, sequence, event_type, at, payload_json) "
            "VALUES (?,?,?,?,?,?,?)",
            rows,
        )
    return len(rows)


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class RecordOutcome:
    """Per-batch accounting. Every delivered record lands in exactly one bucket."""

    received: int = 0
    #: run_postings rows this batch inserted (a posting new to *this run*).
    accepted: int = 0
    #: postings rows minted (a posting new to the corpus).
    created: int = 0
    #: records whose posting was already recorded in this run — the replay/fan-out
    #: case invariant 5 promises is safe.
    duplicates: int = 0
    #: records carrying no usable identity claim at all.
    skipped: int = 0
    #: rank-1 claims found pointing at a different posting. Evidence, not an error.
    conflicts: int = 0
    #: DISTINCT POSTINGS whose content state this batch moved — a first observation, a
    #: material change, a revert to a version already on file, or a new source
    #: reporting a posting already known. Counted per posting rather than per record
    #: so that a batch which sees one posting go A -> B -> A reports the one posting it
    #: moved rather than three changes and an inflated day's work. It is a live counter
    #: for progress reporting; the dirty SET is `dirty_posting_ids`, recomputed from
    #: committed rows.
    changed: int = 0

    def merge(self, other: RecordOutcome) -> RecordOutcome:
        return RecordOutcome(
            received=self.received + other.received,
            accepted=self.accepted + other.accepted,
            created=self.created + other.created,
            duplicates=self.duplicates + other.duplicates,
            skipped=self.skipped + other.skipped,
            conflicts=self.conflicts + other.conflicts,
            changed=self.changed + other.changed,
        )


def posting_id_for_claim(claim: IdentityClaim) -> str:
    """Deterministic posting id from the highest-rank claim.

    Deterministic rather than random so that a crash between two batches, or two
    processes racing on the same brand-new posting, converge on one id instead of
    two. Correctness does not depend on it — the alias lookup is what resolves
    identity — but it removes a whole class of duplicate-on-replay bugs.
    """
    return str(
        uuid.uuid5(
            _POSTING_NAMESPACE,
            canonical_json([claim.kind, claim.namespace, claim.value]),
        )
    )


def _active_alias(conn: sqlite3.Connection, claim: IdentityClaim):
    return conn.execute(
        "SELECT alias_id, posting_id FROM posting_aliases "
        "WHERE alias_kind=? AND namespace=? AND value=? AND valid_to IS NULL",
        (claim.kind, claim.namespace, claim.value),
    ).fetchone()


def _insert_alias(
    conn: sqlite3.Connection,
    *,
    claim: IdentityClaim,
    posting_id: str,
    source_run_id: str,
    at: str,
) -> str:
    """Insert one alias and return its id, so a source version can name the alias
    that evidenced the identity it was filed under."""
    alias_id = new_uid()
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id, posting_id, alias_kind, namespace, value, url, req_id, provenance_json, "
        "confidence, valid_from, valid_to) VALUES (?,?,?,?,?,?,?,?,?,?,NULL)",
        (
            alias_id,
            posting_id,
            claim.kind,
            claim.namespace,
            claim.value,
            claim.url,
            claim.req_id,
            canonical_json({"source_run_id": source_run_id, "rank": claim.rank}),
            # rank 0 is the source's own requisition id; rank 1 is a URL, which the
            # contract calls conservative secondary evidence. The gap is what the
            # Phase 3 resolver weighs when two claims disagree.
            1.0 if claim.rank == 0 else 0.5,
            at,
        ),
    )
    return alias_id


def _record_conflict(
    conn: sqlite3.Connection,
    *,
    posting_id: str,
    other_posting_id: str,
    claim: IdentityClaim,
    record: NormalizedPosting,
    source_run_id: str,
    at: str,
) -> None:
    """Persist a claim disagreement instead of resolving it.

    Two sources legitimately share a URL (an aggregator mirroring a board), so a
    rank-1 collision is evidence, not corruption. Re-pointing the alias here would
    merge two postings on the weakest possible signal; Phase 3's resolver decides,
    with similarity and the full alias graph in hand.

    The hash covers the disagreement itself — the claim, the two postings, and the
    record's canonical fields — and deliberately NOT the observing attempt. The
    same two boards disagreeing about the same URL every morning is one standing
    fact, and hashing the attempt into it would turn `UNIQUE(evidence_hash)` +
    `INSERT OR IGNORE` into an append of one identical row per run forever. The
    attempt is kept beside the hashed payload as provenance, where the ignored
    re-insert leaves it describing the first observation.
    """
    disagreement = {
        "kind": "alias-conflict",
        "claim": {
            "alias_kind": claim.kind,
            "namespace": claim.namespace,
            "value": claim.value,
            "rank": claim.rank,
        },
        "resolved_posting_id": posting_id,
        "conflicting_posting_id": other_posting_id,
        "record": record.canonical_fields(),
    }
    evidence_hash = hashlib.sha256(canonical_json(disagreement).encode("utf-8")).hexdigest()
    evidence_json = canonical_json(
        {**disagreement, "first_observed": {"source_run_id": source_run_id, "at": at}}
    )
    conn.execute(
        "INSERT OR IGNORE INTO identity_evidence "
        "(evidence_id, posting_id, alias_id, evidence_kind, evidence_json, evidence_hash, "
        "observed_at) VALUES (?,?,NULL,'alias-conflict',?,?,?)",
        (
            str(uuid.uuid5(_EVIDENCE_NAMESPACE, evidence_hash)),
            posting_id,
            evidence_json,
            evidence_hash,
            at,
        ),
    )


def _source_of_attempt(conn: sqlite3.Connection, source_run_id: str) -> str | None:
    row = conn.execute(
        "SELECT source FROM source_runs WHERE source_run_id=?", (source_run_id,)
    ).fetchone()
    return None if row is None else row["source"]


def _reattribute_membership(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    posting_id: str,
    source_run_id: str,
    source: str,
) -> None:
    """Move an existing membership row to the attempt re-delivering it.

    Bounded to attempts of the same target within the same run, so it can only
    ever move a row between attempt 1 and attempt 2 of one source. A row written
    by a different source (or a legacy row with no attempt at all, where the IN
    test is NULL and therefore never true) is left where it is.
    """
    conn.execute(
        "UPDATE run_postings SET source_run_id=? "
        "WHERE run_uid=? AND posting_id=? AND source_run_id IS NOT ? "
        "AND source_run_id IN "
        "  (SELECT source_run_id FROM source_runs WHERE run_uid=? AND source=?)",
        (source_run_id, run_uid, posting_id, source_run_id, run_uid, source),
    )

# --------------------------------------------------------------------------- #
# Source versions: Phase 3.1's material-change test
#
# The roadmap line this implements: "Hash normalized source records and create a
# posting version only for material changes. Emit dirty posting IDs; never scan all
# postings after each source."
#
# THE HASH is `NormalizedPosting.content_hash()` and nothing else. Its field list and
# field ORDER (`contract.CANONICAL_HASH_FIELDS`) are a frozen compatibility surface:
# every hash in `run_postings.content_hash` and `posting_versions.version_hash` was
# computed with it, so changing it re-versions the whole corpus. A second, local
# notion of "changed" defined here would be exactly that change, in disguise.
#
# CONTENT STATE IS PER SOURCE INSTANCE, and that is the load-bearing decision here.
# `CANONICAL_HASH_FIELDS` includes `source_key` and `namespace`, so two sources that
# describe ONE posting — an aggregator mirroring a board, which is precisely what
# Phase 3.3's cross-source resolution produces — can NEVER agree on a hash. A single
# "the posting's current version" therefore flips every time the two sources take
# turns, and the posting reads as changed forever: the daily run hands 3.2/3.3 a
# posting to re-describe and re-score that nobody has actually touched. So a posting's
# content state is a MAP, `{namespace: posting_version_id}`, one entry per observing
# source instance, and a record only ever moves ITS OWN entry.
#
#   * The map is recorded per observation in `run_postings.source_state_json`, so it
#     is evidence rather than derived cache, and the next run reads the last recorded
#     map instead of re-deriving one from version rows.
#   * A source that did not deliver in this run keeps its entry untouched — the state
#     is what each source last SAID, not what it said today — so an aggregator going
#     quiet cannot make a posting look changed.
#   * Merging a map is order-independent, so two sources delivering in one run reach
#     the same state whichever finishes first.
#
# A -> B -> A. `UNIQUE (posting_id, version_hash)` means content that reverts to a
# state already on file CANNOT get a second row, so a revert has no way to become the
# newest `observed_at` and a max(observed_at) rule would read the posting as "still B"
# forever. `posting_version_id_for` derives the id from (posting_id, content_hash), so
# `INSERT OR IGNORE` finds the row that is already there and the state map points back
# at it — mint and re-link are one operation. The version row keeps its ORIGINAL
# `observed_at` and `source_run_id`: it records when that content was FIRST seen, and
# rewriting either would destroy evidence to record something the state maps already
# record better, observation by observation.
#
# DIRTY is then a comparison of two states, recomputed from committed rows by
# `dirty_posting_ids`: a posting is dirty in a run when the state that run recorded
# differs from the state recorded by the last run whose dirty set was CONSUMED (see
# `_CONSUMED_RUN_STATUSES`). "No such previous run" — a first-ever sighting — counts
# as different. One rule covers first observation, material change, revert, and a new
# source joining; and none of first observation, alternating sources, or a run that
# died before anything could act on it can make it lie.
# --------------------------------------------------------------------------- #

#: Run statuses whose dirty set is assumed to have been CONSUMED by the phases
#: downstream of this one. Only a run that reached this state may serve as the
#: baseline the next run's changes are measured against.
#:
#: The alternative — treating every prior observation as a baseline regardless of how
#: its run ended — silently drops changes: a run that is cancelled or fails after its
#: batches commit has already recorded the new state, so the next healthy run would
#: compare against it, find nothing moved, and never emit the change to 3.2/3.3. The
#: work would be lost with no error anywhere. Measuring against the last CONSUMED run
#: instead makes the emission self-healing: whatever an interrupted run saw is
#: re-emitted by the next run that completes.
_CONSUMED_RUN_STATUSES = frozenset({"succeeded"})

#: SQL literal form of the above, for the composed queries below.
_CONSUMED_STATUS_SQL = ", ".join(f"'{status}'" for status in sorted(_CONSUMED_RUN_STATUSES))

#: A posting's last recorded content state. Chronological by the RUN's `requested_at`
#: (a run is the unit in which observations happen), then by the row's own
#: `recorded_at`, then by `run_uid` purely to make the order total. Rows that recorded
#: no state at all — legacy imports, anything written before migration 18 — are
#: skipped rather than treated as an empty state, so a legacy membership row cannot
#: erase what a source actually told us.
#:
#: Keyed on `idx_run_postings_posting (posting_id, run_uid)` plus the `pipeline_runs`
#: primary key: a seek per posting, issued ONCE PER BATCH rather than once per record.
_CURRENT_STATE_SQL = """
SELECT posting_id, source_state_json FROM (
    SELECT rp.posting_id AS posting_id,
           rp.source_state_json AS source_state_json,
           ROW_NUMBER() OVER (
               PARTITION BY rp.posting_id
               ORDER BY pr.requested_at DESC, rp.recorded_at DESC, rp.run_uid DESC
           ) AS rn
      FROM run_postings rp
      JOIN pipeline_runs pr ON pr.run_uid = rp.run_uid
     WHERE rp.posting_id IN ({placeholders})
       AND rp.source_state_json IS NOT NULL
) WHERE rn = 1
"""


def posting_version_id_for(posting_id: str, version_hash: str) -> str:
    """Deterministic `posting_versions.posting_version_id` from its content.

    Derived rather than random for the reason `posting_id_for_claim` is — two batches
    racing on the same brand-new content converge — plus one Phase 3.1 depends on
    structurally: `UNIQUE (posting_id, version_hash)` means the row may already exist,
    and a derived id lets the writer `INSERT OR IGNORE` it and then LINK it without
    reading anything back. That is the entire A->B->A mechanism, and it costs no
    round trip. It is also why the state map can store ids rather than hashes: for one
    posting the two are in bijection, and the id is the thing the membership row and
    every downstream join actually need.
    """
    return str(uuid.uuid5(_VERSION_NAMESPACE, canonical_json([posting_id, version_hash])))


def _load_state(blob: object) -> dict[str, str]:
    """Parse a stored state map. A malformed or non-object blob reads as no state.

    Degrading rather than raising is deliberate: the consequence of an unreadable
    state is that the posting looks changed once and is re-examined, which is the safe
    direction. Raising would take out the whole batch — and with it, other postings'
    perfectly good observations — over one bad row.
    """
    if not blob:
        return {}
    try:
        loaded = json.loads(blob) if isinstance(blob, (str, bytes)) else None
    except (TypeError, ValueError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {str(k): str(v) for k, v in loaded.items()}


def _current_states(
    conn: sqlite3.Connection, posting_ids: Sequence[str]
) -> dict[str, dict[str, str]]:
    """`{posting_id: {namespace: posting_version_id}}` for the ids given.

    One statement per `_LOOKUP_CHUNK` ids, never one per record: the write path is
    batched and must stay batched. Postings absent from the result have no recorded
    state, which is the "first observation" case.
    """
    found: dict[str, dict[str, str]] = {}
    ids = list(posting_ids)
    for start in range(0, len(ids), _LOOKUP_CHUNK):
        chunk = ids[start:start + _LOOKUP_CHUNK]
        sql = _CURRENT_STATE_SQL.format(placeholders=",".join("?" * len(chunk)))
        for row in conn.execute(sql, chunk):
            found[row["posting_id"]] = _load_state(row["source_state_json"])
    return found


def _link_source_version(
    conn: sqlite3.Connection,
    *,
    version_id: str,
    posting_id: str,
    alias_id: str | None,
    run_uid: str,
    source_run_id: str,
    record: NormalizedPosting,
    content_hash: str,
    at: str,
) -> None:
    """Ensure this content has a version row. Minting and re-linking are one call.

    `INSERT OR IGNORE` plus the derived id: genuinely new content inserts a row, and a
    revert to content already on file inserts nothing and leaves the row that was
    already there — which the caller then points its state map at either way. Foreign
    key violations are NOT swallowed by OR IGNORE, so a batch that referenced a
    missing posting or alias still fails loudly.

    What is deliberately NOT written here:
      * `tier`, `odds`, `is_new`, `why`, `flags` — scoring, which is 3.4's against a
        profile, not a property of what a source said.
      * `salary_min`/`salary_max` — parsing, likewise downstream; `salary` keeps the
        source's own text.
      * the description body — it can be kilobytes and belongs in `descriptions`
        (Phase 3.2). The hashed `description_digest` in the payload is what makes a
        rewritten body a material change without storing the body twice.
    """
    payload = {
        # The exact input to the hash, so a stored version can be re-verified without
        # re-fetching anything.
        "canonical": record.canonical_fields(),
        "content_hash": content_hash,
        "source": {
            "source_key": record.source_key,
            "instance_key": record.instance_key,
            "namespace": record.namespace,
            "url": record.url,
            "url_key": record.url_key,
            "alt_urls": list(record.alt_urls),
            "posted_raw": record.posted_raw,
            "has_description": record.description is not None,
            "extra": dict(record.extra),
        },
        # First observation of THIS content, matching the row's immutable
        # observed_at/source_run_id. Later observations live in `run_postings`.
        "first_observed": {"run_uid": run_uid, "source_run_id": source_run_id, "at": at},
    }
    conn.execute(
        "INSERT OR IGNORE INTO posting_versions "
        "(posting_version_id, posting_id, version_kind, alias_id, source_run_id, "
        "version_hash, observed_at, title, company, location, salary, posted, remote, "
        "source, req_id, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            version_id,
            posting_id,
            SOURCE_VERSION_KIND,
            alias_id,
            source_run_id,
            content_hash,
            at,
            record.title,
            record.company,
            record.location,
            record.salary_text,
            record.posted_date,
            int(record.remote),
            # The instance-scoped source string every other canonical table uses
            # (`source_runs.source`, alias namespaces, `SourceTarget.source_run_key`),
            # and the key of this posting's state map — so a version can be attributed
            # to the board that reported it rather than only to the software behind it.
            record.namespace,
            record.req_id,
            canonical_json(payload),
        ),
    )


def write_records(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    source_run_id: str,
    records: Sequence[NormalizedPosting],
    recorded_at: str,
) -> RecordOutcome:
    """Upsert a batch of records by identity claim and record run membership.

    Per record:
      1. resolve — the requisition claim alone when there is one, otherwise the
         URL claim (see the module docstring for why the URL may not override a
         requisition);
      2. mint — no hit means a new `postings` row with a deterministic id;
      3. attach — every claim with no active alias becomes one pointing at the
         resolved posting, so a URL first published by a board is already an alias
         when an aggregator later reports the same address;
      4. version — compare the record's version id against the entry THIS SOURCE
         INSTANCE holds in the posting's content state and, if they differ, mint (or
         re-link) a version and move that one entry (`_link_source_version`);
      5. record — one `run_postings` row per (run, posting), carrying the content
         hash, the whole content state after this observation, the version this
         record resolved to, and the `source_run_id` that observed it.

    Two passes over the batch, not one. Identity is resolved for the whole batch
    first, because only then are the posting ids known that the state lookup has to
    ask about, and that lookup is issued ONCE for the batch. A per-record `SELECT` for
    "what state is this posting in" would be one extra round trip per row on a
    33,500-row run — the write path is batched, and versioning it must not quietly
    un-batch it. Within pass two the in-memory `states` map is updated as versions are
    minted, so two sources delivering the same posting in one batch see each other's
    work exactly as two batches would — and reach the same state in either order,
    because each only ever writes its own entry.

    `run_postings.posting_version_id` names the version of the LAST record delivered
    for that posting in this run. With one membership row per (run, posting) and
    several sources able to describe one posting, some source has to be the one the
    single link names, and "the most recent observation" is the only rule that needs
    no adjudication between sources. It is a convenience link, not the content record:
    `source_state_json` on the same row is what says what every source had current,
    and it is what "changed" is computed from.

    `INSERT OR IGNORE` on `run_postings` is what makes delivering the same record
    twice a no-op (contract invariant 5): checkpoint replay and search-term fan-out
    both re-emit, and neither may inflate a count or duplicate a posting.

    One thing a re-emission does move: when the row already there was written by an
    EARLIER ATTEMPT OF THIS SAME TARGET, its `source_run_id` is re-pointed at the
    attempt re-delivering it. Attempt 1 can deliver 380 of 400 rows and then fail;
    without the re-point, the succeeding attempt 2 owns 20 rows, and Phase 2.4 —
    which joins `run_postings` to the SUCCEEDED attempt to learn what a complete
    inventory contained — would read the other 380 as unseen and mark live jobs
    absent. The re-point is scoped to the same `source_runs.source`: a posting a
    board reported and an aggregator then mirrored stays attributed to the board,
    because moving it to a PARTIAL-scope aggregator would cost the board's complete
    inventory a row it genuinely enumerated.

    The failed attempt's own row in `source_runs` is untouched by this: its counts,
    timing, and error stay verbatim. What moves is membership attribution, not
    evidence about the attempt.
    """
    received = accepted = created = duplicates = skipped = conflicts = 0
    #: Postings whose content state this batch moved. A SET, not a counter: a batch
    #: that sees one posting go A -> B -> A moved one posting, whatever the number of
    #: records and version rows involved.
    changed_postings: set[str] = set()
    #: Resolved lazily, on the first re-emission only, so a batch with no
    #: duplicates pays no extra query.
    source_name: str | None = None
    source_resolved = False

    # -- pass one: identity ------------------------------------------------ #
    #: (record, posting_id, alias_id, first_seen_in_run) per usable record.
    resolved: list[tuple[NormalizedPosting, str, str | None, int]] = []
    #: Postings this batch minted. They cannot have a membership row yet, so they are
    #: kept out of the state lookup entirely — which is every row of a first run.
    minted_here: set[str] = set()

    for record in records:
        received += 1
        claims = record.identity_claims()
        if not claims:
            skipped += 1
            continue

        # `identity_claims()` is rank-ordered, so claims[0] is the requisition
        # claim whenever the record has a req_id.
        resolving = claims[:1] if claims[0].rank == 0 else claims
        posting_id = None
        alias_id: str | None = None
        for claim in resolving:
            row = _active_alias(conn, claim)
            if row is not None:
                posting_id = row["posting_id"]
                alias_id = row["alias_id"]
                break

        first_seen_in_run = 0
        if posting_id is None:
            posting_id = posting_id_for_claim(claims[0])
            cursor = conn.execute(
                "INSERT OR IGNORE INTO postings "
                "(posting_id, identity_status, first_seen_at, created_at) "
                "VALUES (?, 'active', ?, ?)",
                (posting_id, recorded_at, recorded_at),
            )
            if cursor.rowcount:
                created += 1
                first_seen_in_run = 1
                minted_here.add(posting_id)

        for claim in claims:
            row = _active_alias(conn, claim)
            if row is None:
                new_alias_id = _insert_alias(
                    conn,
                    claim=claim,
                    posting_id=posting_id,
                    source_run_id=source_run_id,
                    at=recorded_at,
                )
                # The alias a version is filed under is the one that resolved the
                # record's identity: its highest-rank claim, whether that alias
                # already existed or was created just now.
                if alias_id is None and claim is claims[0]:
                    alias_id = new_alias_id
            elif row["posting_id"] != posting_id:
                conflicts += 1
                _record_conflict(
                    conn,
                    posting_id=posting_id,
                    other_posting_id=row["posting_id"],
                    claim=claim,
                    record=record,
                    source_run_id=source_run_id,
                    at=recorded_at,
                )

        resolved.append((record, posting_id, alias_id, first_seen_in_run))

    # -- pass two: content ------------------------------------------------- #
    states = _current_states(
        conn, sorted({pid for _, pid, _, _ in resolved} - minted_here)
    )

    for record, posting_id, alias_id, first_seen_in_run in resolved:
        content_hash = record.content_hash()
        version_id = posting_version_id_for(posting_id, content_hash)
        state = states.setdefault(posting_id, {})
        namespace = record.namespace
        if state.get(namespace) != version_id:
            # This source's entry moves, and ONLY this source's entry: what another
            # source last said about this posting is still what it last said.
            _link_source_version(
                conn,
                version_id=version_id,
                posting_id=posting_id,
                alias_id=alias_id,
                run_uid=run_uid,
                source_run_id=source_run_id,
                record=record,
                content_hash=content_hash,
                at=recorded_at,
            )
            state[namespace] = version_id
            changed_postings.add(posting_id)
        state_json = canonical_json(state)

        cursor = conn.execute(
            "INSERT OR IGNORE INTO run_postings "
            "(run_uid, posting_id, posting_version_id, source_run_id, present, "
            "first_seen_in_run, recorded_at, membership_kind, content_hash, "
            "source_state_json) VALUES (?,?,?,?,1,?,?,'snapshot',?,?)",
            (run_uid, posting_id, version_id, source_run_id, first_seen_in_run,
             recorded_at, content_hash, state_json),
        )
        if cursor.rowcount:
            accepted += 1
        else:
            duplicates += 1
            if not source_resolved:
                source_name = _source_of_attempt(conn, source_run_id)
                source_resolved = True
            if source_name is not None:
                _reattribute_membership(
                    conn,
                    run_uid=run_uid,
                    posting_id=posting_id,
                    source_run_id=source_run_id,
                    source=source_name,
                )
            # A run has ONE membership row per posting, so a re-delivery — a second
            # source describing it, a replayed batch, a retried attempt — updates the
            # row that is already there rather than adding one. Every column that
            # describes THIS observation moves together: the merged state, the
            # version and hash of the record that spoke last, and `recorded_at`,
            # which must not keep naming the first delivery while the rest of the row
            # describes the last one. Writing the same values twice is a no-op in
            # effect, which is what keeps a replayed batch idempotent.
            conn.execute(
                "UPDATE run_postings SET posting_version_id=?, content_hash=?, "
                "source_state_json=?, recorded_at=? WHERE run_uid=? AND posting_id=?",
                (version_id, content_hash, state_json, recorded_at, run_uid, posting_id),
            )

    return RecordOutcome(
        received=received,
        accepted=accepted,
        created=created,
        duplicates=duplicates,
        skipped=skipped,
        conflicts=conflicts,
        changed=len(changed_postings),
    )


# --------------------------------------------------------------------------- #
# Dirty postings: the Phase 3.2/3.3 handoff
#
# "Emit dirty posting IDs; never scan all postings after each source." Both halves
# are structural here:
#
#   EVIDENCE, not memory. The dirty set is recomputed from committed `run_postings`
#     rows, so a process that died after the batches committed and before anything
#     consumed them loses nothing: the next process asks the same question of the same
#     rows and gets the same answer. The writer's in-flight `RecordOutcome.changed` is
#     a live counter for reporting, never the source of truth.
#   RUN-SCOPED, never corpus-scoped. Every query below is anchored on `run_uid` (the
#     `run_postings` primary key) or on this run's `source_run_id`s (migration 16's
#     index). Nothing here touches a posting the run did not observe, so the cost
#     tracks what a run delivered rather than how big the corpus has become.
# --------------------------------------------------------------------------- #

#: The content state recorded by the last CONSUMED observation of this posting before
#: the one in hand. Chronological at every level — the run's `requested_at`, then the
#: row's `recorded_at`, then `run_uid` purely to make the order total — because a
#: baseline picked by anything else (an id's sort order, say) is not a statement about
#: time and would pick differently as ids change.
#:
#: The `pipeline_runs.status` bound is what makes emission self-healing across a run
#: that died: see `_CONSUMED_RUN_STATUSES`.
_PREVIOUS_CONSUMED_STATE_SQL = f"""(
    SELECT prev.source_state_json
      FROM run_postings prev
      JOIN pipeline_runs ppr ON ppr.run_uid = prev.run_uid
     WHERE prev.posting_id = rp.posting_id
       AND prev.source_state_json IS NOT NULL
       AND ppr.status IN ({_CONSUMED_STATUS_SQL})
       AND (ppr.requested_at, prev.recorded_at, prev.run_uid)
           < (?, rp.recorded_at, rp.run_uid)
     ORDER BY ppr.requested_at DESC, prev.recorded_at DESC, prev.run_uid DESC
     LIMIT 1
)"""

#: One run's dirty membership rows. `IS NOT` rather than `<>` on purpose: a posting
#: with no consumed previous observation compares against NULL, and that is a first
#: sighting — dirty by definition — where `<>` would yield NULL and drop the row.
#:
#: Rows that recorded no state of their own are excluded rather than counted as
#: changed: they are legacy membership rows, which describe an import rather than a
#: source observation and have no content this phase can reason about.
_DIRTY_ROWS_SQL = """
SELECT rp.posting_id AS posting_id, rp.first_seen_in_run AS first_seen_in_run
  FROM run_postings rp
 WHERE rp.run_uid = ?
   AND rp.present = 1
   AND rp.source_state_json IS NOT NULL
   AND rp.source_state_json IS NOT """ + _PREVIOUS_CONSUMED_STATE_SQL + "\n"


def _run_requested_at(conn: sqlite3.Connection, run_uid: str) -> str | None:
    row = conn.execute(
        "SELECT requested_at FROM pipeline_runs WHERE run_uid=?", (run_uid,)
    ).fetchone()
    return None if row is None else row["requested_at"]


def dirty_posting_ids(
    conn: sqlite3.Connection,
    run_uid: str,
    *,
    limit: int | None = None,
    after: str | None = None,
) -> list[str]:
    """Postings whose content changed in this run — 3.2's and 3.3's work list.

    Dirty means the content state this run recorded differs from the state recorded by
    the last run whose dirty set was consumed, which is four things at once: a posting
    seen for the first time, a posting whose content materially changed, a posting
    whose content reverted to a state already on file, and a posting a NEW source
    started reporting. Everything else — the overwhelming majority of a daily run — is
    not dirty and must not be re-described or re-scored.

    An unknown run answers `[]` rather than everything: a caller that mistypes a run id
    must not be handed the corpus.

    Chunking. Results are ordered by posting id, and `after` resumes from the last id a
    caller processed: `limit` alone re-returns the same first N, because this is a
    query rather than a queue — nothing here records that anyone consumed anything.

        seen = None
        while (batch := dirty_posting_ids(conn, run_uid, limit=500, after=seen)):
            handle(batch)
            seen = batch[-1]

    One-time cost on an existing database. `run_postings.source_state_json` is NULL for
    every row written before migration 18, so the first run after upgrading finds no
    previous state for anything and reports the whole corpus dirty, exactly once. That
    is the honest answer — no content baseline exists yet — and the run after it
    settles back to the genuine change rate.
    """
    requested_at = _run_requested_at(conn, run_uid)
    if requested_at is None:
        return []
    sql = f"SELECT posting_id FROM ({_DIRTY_ROWS_SQL})"
    params: list[object] = [run_uid, requested_at]
    if after is not None:
        sql += " WHERE posting_id > ?"
        params.append(after)
    sql += " ORDER BY posting_id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [row["posting_id"] for row in conn.execute(sql, tuple(params))]


def change_summary(conn: sqlite3.Connection, run_uid: str) -> dict[str, object]:
    """Counts for one run's change accounting, for the run report and for humans.

    The same predicate `dirty_posting_ids` selects on, counted instead of listed: a
    32,000-id list has no business in `aggregate_report_json`, and "N changed" is what
    a person reads.

    Three statements, each index-keyed on this run (asserted by an EXPLAIN QUERY PLAN
    test, with `ANALYZE` run first — several of these plans change once SQLite has
    statistics, and a plan that is only fast on a statistics-free database is not
    fast). Every row this run observed lands in exactly one of `changed`, `unchanged`,
    and `legacy_membership`, so the buckets can be read as a partition rather than as
    three unrelated numbers.
    """
    requested_at = _run_requested_at(conn, run_uid)
    if requested_at is None:
        raise LookupError(f"no pipeline_run {run_uid!r}")

    totals = conn.execute(
        "SELECT COUNT(*) AS observed, "
        "COALESCE(SUM(rp.posting_version_id IS NULL), 0) AS unversioned, "
        "COALESCE(SUM(rp.source_state_json IS NULL), 0) AS stateless, "
        "COALESCE(SUM(v.version_kind IS NOT NULL AND v.version_kind <> ?), 0) AS legacy "
        "FROM run_postings rp "
        "LEFT JOIN posting_versions v ON v.posting_version_id = rp.posting_version_id "
        "WHERE rp.run_uid = ? AND rp.present = 1",
        (SOURCE_VERSION_KIND, run_uid),
    ).fetchone()
    dirty = conn.execute(
        "SELECT COUNT(*) AS changed, "
        "COALESCE(SUM(first_seen_in_run), 0) AS first_seen "
        f"FROM ({_DIRTY_ROWS_SQL})",
        (run_uid, requested_at),
    ).fetchone()
    created = conn.execute(
        # `CROSS JOIN`, and in this order, on purpose — twice over.
        #
        # Not `source_run_id IN (SELECT ...)`: that form plans as a full scan of
        # `posting_versions` with a bloom filter as soon as `ANALYZE` has ever run, so
        # it is a corpus-sized cost lying in wait for the day somebody adds statistics
        # (measured: 2.3ms vs 0.55ms on an 8,000-version database, and the gap grows
        # with the corpus while the right plan's does not).
        #
        # And CROSS rather than a plain JOIN because CROSS is SQLite's documented way
        # to FIX the join order: this run's attempts are the selective side and
        # migration 16's index is the way into the versions, always. A plain JOIN gets
        # that right on any realistic database but is free to invert it on statistics
        # that say otherwise — which is the same silent dependence on stats, one step
        # further along. `INDEXED BY` pins the remaining degree of freedom: without it
        # SQLite reads `source_runs` through a covering-index SCAN whose cost grows
        # with every attempt ever run rather than with this run's. If that index is
        # ever dropped this query fails loudly, which is the intended failure mode for
        # a plan the correctness of the write path's cost model depends on.
        "SELECT COUNT(*) FROM source_runs sr INDEXED BY idx_source_runs_run_status "
        "CROSS JOIN posting_versions pv ON pv.source_run_id = sr.source_run_id "
        "WHERE sr.run_uid = ? AND pv.version_kind = ?",
        (run_uid, SOURCE_VERSION_KIND),
    ).fetchone()[0]

    observed = int(totals["observed"])
    legacy = int(totals["legacy"])
    changed = int(dirty["changed"])
    first_seen = int(dirty["first_seen"])
    return {
        "run_uid": run_uid,
        #: membership rows this run recorded — the denominator.
        "observed": observed,
        "changed": changed,
        #: dirty because the posting had never been seen before.
        "first_seen": first_seen,
        #: dirty because content moved under a posting already known.
        "updated": changed - first_seen,
        "unchanged": observed - changed - legacy,
        #: `posting_versions` rows this run's attempts minted. Lower than `changed`
        #: exactly when content reverted to a version already on file, so the gap
        #: between the two is the A->B->A case being visible rather than inferred.
        "versions_created": int(created),
        #: rows linking a version this phase did not write — migration 11's legacy
        #: imports. Counted in their own bucket rather than absorbed into `unchanged`,
        #: which would claim this run had observed content that had not moved.
        "legacy_membership": legacy,
        #: rows carrying no version at all. Zero for anything this module wrote.
        "unversioned": int(totals["unversioned"]),
    }


# --------------------------------------------------------------------------- #
# Restart, resume, and the Phase 2.4 handoff
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class RecoveryReport:
    at: str
    run_uids: tuple[str, ...] = ()
    source_run_ids: tuple[str, ...] = ()

    @property
    def total(self) -> int:
        return len(self.run_uids) + len(self.source_run_ids)


def recover_orphans(
    conn: sqlite3.Connection,
    *,
    at: str | None = None,
    reason: str = "orphaned by process exit",
    actor: str = "scheduler-startup",
    exclude_run_uids: Iterable[str] = (),
) -> RecoveryReport:
    """Mark every run/attempt left 'running' by a dead process as interrupted.

    `finished_at` is deliberately left NULL. The process died at an unknown
    instant; stamping recovery time as a finish time would put a fabricated
    duration into a table whose whole purpose is immutable timing evidence. The
    recovery instant goes into `metadata_json.interrupted` instead, where it is
    plainly recovery metadata and not a measurement.

    `exclude_run_uids` protects runs this process currently owns, so calling
    recovery at the start of run N cannot interrupt a concurrent run N-1.
    """
    stamp = at or utc_now_iso()
    excluded = {str(uid) for uid in exclude_run_uids}
    note = {"at": stamp, "by": actor, "reason": reason}

    source_run_ids: list[str] = []
    for row in conn.execute(
        "SELECT source_run_id, run_uid, metadata_json FROM source_runs WHERE status='running'"
    ).fetchall():
        if row["run_uid"] in excluded:
            continue
        metadata = _load_metadata(row["metadata_json"])
        metadata["interrupted"] = note
        conn.execute(
            "UPDATE source_runs SET status='interrupted', metadata_json=? "
            "WHERE source_run_id=? AND status='running'",
            (canonical_json(metadata), row["source_run_id"]),
        )
        source_run_ids.append(row["source_run_id"])

    run_uids: list[str] = []
    for row in conn.execute(
        "SELECT run_uid, error_json FROM pipeline_runs WHERE status='running'"
    ).fetchall():
        if row["run_uid"] in excluded:
            continue
        conn.execute(
            "UPDATE pipeline_runs SET status='interrupted', error_json=COALESCE(error_json, ?) "
            "WHERE run_uid=? AND status='running'",
            (canonical_json({"type": "Interrupted", **note}), row["run_uid"]),
        )
        run_uids.append(row["run_uid"])

    return RecoveryReport(at=stamp, run_uids=tuple(run_uids), source_run_ids=tuple(source_run_ids))


def latest_checkpoint_json(conn: sqlite3.Connection, *, run_uid: str, source: str) -> str | None:
    """Newest non-null checkpoint for one target inside one run.

    Scoped to a single run on purpose. Carrying a checkpoint across runs would let
    a COMPLETE-scope target resume mid-inventory and still report success, which
    would licence Phase 2.4 to mark everything it skipped absent. Resume is a
    within-run decision.
    """
    row = conn.execute(
        # `step`-bounded like every other fetch-attempt query here: a cursor describes
        # progress through an ENUMERATION, and a row of some other step against the
        # same source is not one, however recent its attempt number looks.
        "SELECT checkpoint_json FROM source_runs "
        "WHERE run_uid=? AND source=? AND step=? AND checkpoint_json IS NOT NULL "
        "ORDER BY attempt DESC LIMIT 1",
        (run_uid, source, SOURCE_RUN_STEP),
    ).fetchone()
    return row["checkpoint_json"] if row else None


def max_attempt_by_source(conn: sqlite3.Connection, run_uid: str) -> dict[str, int]:
    """Highest FETCH attempt number each target reached inside one run.

    Bounded to `step = SOURCE_RUN_STEP`, which is what a resume's numbering depends
    on: rows written under another step — `UNATTEMPTED_SOURCE_RUN_STEP` today, the
    describe/score steps Phase 3 adds to the same table tomorrow — are not fetch
    attempts, and counting them would make the next real attempt skip a number and
    claim an attempt that never happened.
    """
    return {
        row["source"]: int(row["n"])
        for row in conn.execute(
            "SELECT source, MAX(attempt) AS n FROM source_runs "
            "WHERE run_uid=? AND step=? GROUP BY source",
            (run_uid, SOURCE_RUN_STEP),
        )
    }


def successful_source_scopes(conn: sqlite3.Connection, run_uid: str) -> list[dict[str, object]]:
    """Exactly what Phase 2.4 needs to decide absence, and nothing more.

    A target may mark its unseen postings absent only if it appears here with
    `inventory_scope == 'complete'`. A failed, timed-out, interrupted, or PARTIAL
    target is absent from this list (or present as 'partial'), and its postings
    keep their last-known-good state with degraded freshness.

    The inventory a row licences is `run_postings` joined on its `source_run_id`,
    and that join is complete: `write_records` re-points rows first written by an
    earlier attempt of the same target onto the attempt that finished. Do not
    substitute `accepted_count` for it — that column counts the membership rows
    this attempt itself inserted, so on a retried target it is exactly the rows
    the earlier attempt had not already delivered.
    """
    return [
        {
            "source": row["source"],
            "source_run_id": row["source_run_id"],
            "inventory_scope": row["inventory_scope"],
            "finished_at": row["finished_at"],
            "accepted_count": row["accepted_count"],
        }
        for row in conn.execute(
            "SELECT source, source_run_id, inventory_scope, finished_at, accepted_count "
            "FROM source_runs WHERE run_uid=? AND status='succeeded' AND step=? "
            "ORDER BY source, attempt",
            (run_uid, SOURCE_RUN_STEP),
        )
    ]


# --------------------------------------------------------------------------- #
# Presence: Phase 2.4's absence marking
#
# The roadmap line this implements: "Mark postings absent only when that source
# completed successfully. Failed/timed-out sources retain last-known-good records and
# show degraded freshness."
#
# Three rules, each of which exists because breaking it silently deletes live jobs:
#
#   LICENCE. Only an attempt that both SUCCEEDED and declared
#     `inventory_scope='complete'` may mark anything. Partial success, failure,
#     timeout, cancellation, and interruption all mark nothing at all; those
#     postings keep their last-known-good state and their staleness shows up in
#     `source_instance_freshness` instead.
#   SCOPE. A licence covers only postings whose identity belongs to that exact
#     source instance, evidenced by an active `source_req` alias in the instance's
#     own namespace. `greenhouse:anthropic` enumerating its board says nothing
#     whatsoever about `greenhouse:acme`, about an aggregator's postings, or about a
#     manual import.
#   EVIDENCE. Nothing is deleted, nothing is hidden. A marking sets four columns on
#     `postings` naming the instant, the run, and the licensing attempt, and a
#     delivery in a later run clears `absent_since` while leaving that record beside
#     a `returned_at` stamp.
# --------------------------------------------------------------------------- #

#: Postings whose identity belongs to one source instance. `source_runs.source`,
#: `SourceTarget.source_run_key`, and `NormalizedPosting.namespace` are the same
#: string by construction, which is what makes this join exact rather than a guess.
_OWNED_BY_INSTANCE_SQL = (
    "SELECT posting_id FROM posting_aliases "
    "WHERE alias_kind=? AND namespace=? AND valid_to IS NULL"
)

#: What one attempt actually delivered. This is the join `successful_source_scopes`
#: documents: `write_records` re-points rows first written by an earlier attempt of
#: the same target onto the attempt that finished, so on a retried target this is the
#: whole inventory rather than only the rows attempt 2 happened to insert.
_DELIVERED_BY_ATTEMPT_SQL = (
    "SELECT posting_id FROM run_postings "
    "WHERE run_uid=? AND source_run_id=? AND present=1"
)

#: Everything any source positively observed in this run.
_SEEN_IN_RUN_SQL = "SELECT posting_id FROM run_postings WHERE run_uid=? AND present=1"


def refresh_presence(
    conn: sqlite3.Connection, *, run_uid: str, at: str
) -> dict[str, object]:
    """Record every positive observation this run made, and undo stale absences.

    A delivery is direct evidence that a posting exists; an absence is only ever an
    inference from not seeing one. So ANY source that delivered a posting — including
    a PARTIAL aggregator or a manual import — refreshes its `last_seen_*` and returns
    it to present. That asymmetry is the safe one: the failure mode of believing a
    delivery is a posting that lingers a run too long, and the failure mode of
    ignoring one is a live job the user never sees again.

    `last_seen_at` is the run's own `recorded_at` for that posting rather than the
    pass's timestamp, so it is a measurement of when delivery happened rather than of
    when this function ran.

    `absent_run_uid`, `absent_source_run_id`, and `returned_at` are deliberately not
    cleared: they are what makes the return transition legible afterwards. A posting
    that is present now and was absent before reads as `absent_since IS NULL AND
    returned_at IS NOT NULL`, with the previous absence still fully described.
    """
    returned = conn.execute(
        "SELECT COUNT(*) FROM postings WHERE absent_since IS NOT NULL "
        f"AND posting_id IN ({_SEEN_IN_RUN_SQL})",
        (run_uid,),
    ).fetchone()[0]
    cursor = conn.execute(
        "UPDATE postings SET "
        "last_seen_at=(SELECT MAX(rp.recorded_at) FROM run_postings rp "
        "              WHERE rp.posting_id=postings.posting_id "
        "                AND rp.run_uid=? AND rp.present=1), "
        "last_seen_run_uid=?, "
        "returned_at=CASE WHEN absent_since IS NULL THEN returned_at ELSE ? END, "
        "absent_since=NULL "
        f"WHERE posting_id IN ({_SEEN_IN_RUN_SQL})",
        (run_uid, run_uid, at, run_uid),
    )
    return {"seen": cursor.rowcount, "returned": int(returned)}


def mark_absent_for_scope(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    source: str,
    source_run_id: str,
    at: str,
) -> dict[str, object]:
    """Mark this instance's unseen postings absent. Caller checks the licence.

    The candidate set is `owned by this instance` minus `delivered by this attempt`
    minus `observed by anything else in this run`.

    The middle term is the licence's own inventory and is the reason a retried target
    is safe (2.3's re-point invariant). The third term is a guard, and it is
    load-bearing rather than redundant: `write_records` refuses to move a membership
    row ACROSS sources, so when an aggregator resolves a board's posting by URL and
    inserts the `run_postings` row first, that row keeps the aggregator's
    `source_run_id` even after the board re-delivers the same posting seconds later.
    The board's attempt-scoped inventory is then genuinely missing a posting the board
    genuinely enumerated, and without this clause a live job would be marked absent.
    The clause is stated as a refusal to contradict a positive observation, which is
    the same principle `refresh_presence` runs on, and `retained_positively_observed`
    in the returned report counts exactly the times it mattered.

    `absent_since IS NULL` bounds the UPDATE, so running the pass twice marks nothing
    a second time and cannot restamp a posting that was already absent — its
    `absent_since` stays the instant it actually went missing.
    """
    owned = conn.execute(
        f"SELECT COUNT(*) FROM ({_OWNED_BY_INSTANCE_SQL})",
        (SOURCE_REQ_ALIAS_KIND, source),
    ).fetchone()[0]
    inventory = conn.execute(
        f"SELECT COUNT(*) FROM ({_DELIVERED_BY_ATTEMPT_SQL})", (run_uid, source_run_id)
    ).fetchone()[0]
    retained = conn.execute(
        "SELECT COUNT(*) FROM postings WHERE absent_since IS NULL "
        f"AND posting_id IN ({_OWNED_BY_INSTANCE_SQL}) "
        f"AND posting_id NOT IN ({_DELIVERED_BY_ATTEMPT_SQL}) "
        f"AND posting_id IN ({_SEEN_IN_RUN_SQL})",
        (SOURCE_REQ_ALIAS_KIND, source, run_uid, source_run_id, run_uid),
    ).fetchone()[0]
    cursor = conn.execute(
        "UPDATE postings SET absent_since=?, absent_run_uid=?, absent_source_run_id=? "
        "WHERE absent_since IS NULL "
        f"AND posting_id IN ({_OWNED_BY_INSTANCE_SQL}) "
        f"AND posting_id NOT IN ({_SEEN_IN_RUN_SQL})",
        (at, run_uid, source_run_id, SOURCE_REQ_ALIAS_KIND, source, run_uid),
    )
    return {
        "source": source,
        "source_run_id": source_run_id,
        "owned": int(owned),
        "inventory": int(inventory),
        "marked_absent": cursor.rowcount,
        "retained_positively_observed": int(retained),
    }


def apply_run_presence(
    conn: sqlite3.Connection, *, run_uid: str, at: str | None = None
) -> dict[str, object]:
    """The whole Phase 2.4 pass for one settled run, in the caller's transaction.

    Refresh first, then mark: a positive observation is stronger evidence than an
    absence inference, so it is applied before anything is inferred. (The guard in
    `mark_absent_for_scope` makes the two orders equivalent; doing them in this order
    means the code does not depend on that.)

    Only the HIGHEST-numbered succeeded attempt of each source is honoured. The
    scheduler stops attempting a target the moment one succeeds, so a second
    succeeded attempt for one source can only come from a resume; treating the
    earlier one as authoritative there would licence it to mark the later attempt's
    whole inventory absent.
    """
    stamp = at or utc_now_iso()
    presence = refresh_presence(conn, run_uid=run_uid, at=stamp)

    licensed: dict[str, dict[str, object]] = {}
    for scope in successful_source_scopes(conn, run_uid):
        if scope["inventory_scope"] != "complete":
            continue
        # successful_source_scopes orders by (source, attempt), so a later attempt
        # of the same source overwrites an earlier one.
        licensed[str(scope["source"])] = scope

    sources = [
        mark_absent_for_scope(
            conn,
            run_uid=run_uid,
            source=str(scope["source"]),
            source_run_id=str(scope["source_run_id"]),
            at=stamp,
        )
        for scope in licensed.values()
    ]
    return {
        "at": stamp,
        "run_uid": run_uid,
        "seen": presence["seen"],
        "returned": presence["returned"],
        "licensed_sources": len(sources),
        "marked_absent": sum(int(s["marked_absent"]) for s in sources),
        "retained_positively_observed": sum(
            int(s["retained_positively_observed"]) for s in sources
        ),
        "sources": sources,
    }


def source_instance_freshness(
    conn: sqlite3.Connection,
    *,
    at: str | None = None,
    stale_after_seconds: float = DEFAULT_STALE_AFTER_SECONDS,
    sources: Sequence[str] | None = None,
) -> list[dict[str, object]]:
    """Per-source-instance freshness, computed entirely from `source_runs` evidence.

    This is the data contract behind "failed/timed-out sources ... show degraded
    freshness". Phase 4 owns the display; there is deliberately no stored freshness
    state to drift, because every field here is derived from attempt rows that are
    already immutable.

    A RUN, not an attempt, is the unit: a target that failed once and succeeded on
    its retry had a good run, and counting its two attempts as one failure and one
    success would make every retried source look permanently degraded. A run counts
    as successful for a source when ANY of that source's attempts in it succeeded.

    `licenses_absence` is the same test `apply_run_presence` applies, reported ahead
    of time: it is true when the most recent run's succeeding attempt declared
    COMPLETE scope. A source with `licenses_absence` false can never remove a posting
    from view, however stale it is — which is exactly the intended failure mode.
    """
    now = _parse_instant(at) if at else datetime.now(timezone.utc)
    rows = conn.execute(
        "SELECT source, run_uid, attempt, status, inventory_scope, "
        "  COALESCE(finished_at, started_at, requested_at) AS at "
        "FROM source_runs WHERE step=? ORDER BY source, attempt",
        (SOURCE_RUN_STEP,),
    ).fetchall()

    wanted = None if sources is None else set(sources)
    by_source: dict[str, dict[str, dict[str, object]]] = {}
    for row in rows:
        source = row["source"]
        if wanted is not None and source not in wanted:
            continue
        runs = by_source.setdefault(source, {})
        run = runs.setdefault(
            row["run_uid"],
            {"at": None, "succeeded": False, "complete": False, "last_status": None},
        )
        stamp = row["at"]
        if stamp and (run["at"] is None or stamp > run["at"]):
            run["at"] = stamp
        # Attempts arrive in ascending order, so the last one wins.
        run["last_status"] = row["status"]
        if row["status"] == "succeeded":
            run["succeeded"] = True
            run["complete"] = row["inventory_scope"] == "complete"

    report: list[dict[str, object]] = []
    for source in sorted(by_source):
        ordered = sorted(
            by_source[source].values(),
            key=lambda r: (r["at"] or "", ),
            reverse=True,
        )
        last_success_at = next((r["at"] for r in ordered if r["succeeded"]), None)
        last_complete_at = next(
            (r["at"] for r in ordered if r["succeeded"] and r["complete"]), None
        )
        consecutive = 0
        for run in ordered:
            if run["succeeded"]:
                break
            consecutive += 1
        newest = ordered[0]
        age = _age_seconds(last_success_at, now)
        report.append(
            {
                "source": source,
                "last_success_at": last_success_at,
                "last_complete_success_at": last_complete_at,
                "last_attempt_at": newest["at"],
                "last_attempt_status": newest["last_status"],
                "consecutive_failed_runs": consecutive,
                "runs_observed": len(ordered),
                "age_seconds": age,
                "stale": (
                    last_success_at is None
                    or consecutive > 0
                    or (age is not None and age > stale_after_seconds)
                ),
                "licenses_absence": bool(newest["succeeded"] and newest["complete"]),
            }
        )
    return report


def _parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _age_seconds(stamp: object, now: datetime) -> float | None:
    """Seconds between a stored timestamp and `now`, or None when unusable.

    An unparseable timestamp returns None rather than raising: freshness is a
    reporting query, and one malformed row must not take out the whole report.
    """
    if not isinstance(stamp, str) or not stamp.strip():
        return None
    try:
        return max(0.0, (now - _parse_instant(stamp)).total_seconds())
    except ValueError:
        return None


def resumable_runs(conn: sqlite3.Connection, *, limit: int = 20) -> list[dict[str, object]]:
    """Interrupted runs, newest first — the API-shaped input to a restart decision.

    Phase 4 turns this into an endpoint. It is a query, not an action: nothing here
    decides to resume anything.
    """
    return [
        {
            "run_uid": row["run_uid"],
            "kind": row["kind"],
            "status": row["status"],
            "requested_at": row["requested_at"],
            "started_at": row["started_at"],
            "interrupted_attempts": int(row["interrupted_attempts"] or 0),
            "succeeded_attempts": int(row["succeeded_attempts"] or 0),
        }
        for row in conn.execute(
            "SELECT r.run_uid, r.kind, r.status, r.requested_at, r.started_at, "
            "  (SELECT COUNT(*) FROM source_runs s "
            "     WHERE s.run_uid=r.run_uid AND s.step=? AND s.status='interrupted') "
            "     AS interrupted_attempts, "
            "  (SELECT COUNT(*) FROM source_runs s "
            "     WHERE s.run_uid=r.run_uid AND s.step=? AND s.status='succeeded') "
            "     AS succeeded_attempts "
            "FROM pipeline_runs r WHERE r.status='interrupted' "
            "ORDER BY COALESCE(r.started_at, r.requested_at) DESC LIMIT ?",
            (SOURCE_RUN_STEP, SOURCE_RUN_STEP, limit),
        )
    ]


def resume_plan(conn: sqlite3.Connection, run_uid: str) -> dict[str, object]:
    """Describe what resuming `run_uid` would do, without doing it.

    `completed` targets are skipped by a resume (their success is already recorded
    and their inventory scope already licensed); `pending` targets are re-attempted
    from their stored checkpoint when one is still valid.
    """
    run = conn.execute(
        "SELECT run_uid, kind, status, requested_at, started_at, config_hash, code_hash "
        "FROM pipeline_runs WHERE run_uid=?",
        (run_uid,),
    ).fetchone()
    if run is None:
        raise LookupError(f"no pipeline_run {run_uid!r}")

    completed: list[str] = []
    pending: list[str] = []
    for row in conn.execute(
        "SELECT source, MAX(status='succeeded') AS ok FROM source_runs "
        "WHERE run_uid=? AND step=? GROUP BY source ORDER BY source",
        (run_uid, SOURCE_RUN_STEP),
    ):
        (completed if row["ok"] else pending).append(row["source"])

    checkpoints = {
        row["source"]: row["checkpoint_json"]
        for row in conn.execute(
            "SELECT source, checkpoint_json FROM source_runs "
            "WHERE run_uid=? AND step=? AND checkpoint_json IS NOT NULL "
            "ORDER BY source, attempt",
            (run_uid, SOURCE_RUN_STEP),
        )
    }
    return {
        "run_uid": run["run_uid"],
        "kind": run["kind"],
        "status": run["status"],
        "requested_at": run["requested_at"],
        "started_at": run["started_at"],
        "config_hash": run["config_hash"],
        "code_hash": run["code_hash"],
        "resumable": run["status"] == "interrupted",
        "completed_sources": tuple(completed),
        "pending_sources": tuple(pending),
        "checkpoints": checkpoints,
        "max_attempt_by_source": max_attempt_by_source(conn, run_uid),
    }
