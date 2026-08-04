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
  * It never mints `posting_versions` rows. Creating a version on material change
    is Phase 3.1; Phase 2 only records `run_postings.content_hash` so 3.1 has
    something to diff against.
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
    "TERMINAL_SOURCE_RUN_STATUSES",
    "append_run_events",
    "apply_run_presence",
    "canonical_json",
    "create_pipeline_run",
    "create_source_run",
    "finish_pipeline_run",
    "finish_source_run",
    "latest_checkpoint_json",
    "mark_absent_for_scope",
    "max_attempt_by_source",
    "new_uid",
    "next_event_sequence",
    "posting_id_for_claim",
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

#: Deterministic id namespaces. Derived rather than hard-coded so the derivation
#: is auditable; they are constants at runtime.
_POSTING_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/posting")
_EVIDENCE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/evidence")


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
        "identity_evidence",
        "run_postings",
    }
    present = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('pipeline_runs','source_runs','run_events','postings','posting_aliases',"
            "'identity_evidence','run_postings')"
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


def update_source_run_progress(
    conn: sqlite3.Connection,
    *,
    source_run_id: str,
    fetched_count: int,
    accepted_delta: int,
    checkpoint_json: str | None = None,
) -> None:
    """Publish in-flight progress for a still-running attempt.

    `accepted_count` accumulates in SQL rather than being assigned from a
    caller-side total: two batches for the same target can share one transaction,
    and an assignment computed before the first of them committed would report a
    stale number. `fetched_count` is a monotonic counter owned by the target loop,
    so assignment is correct for it.

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
        "checkpoint_json=COALESCE(?, checkpoint_json) "
        "WHERE source_run_id=? AND status='running'",
        (fetched_count, accepted_delta, accepted_delta, checkpoint_json, source_run_id),
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

    `changed_count` stays NULL in Phase 2: deciding whether a posting materially
    changed requires the previous version, which Phase 3.1 owns. Writing a guess
    here would put a fabricated number into immutable evidence. `None` therefore
    means "leave whatever is there", not "write NULL" — otherwise settling an
    attempt would erase a count Phase 3.1 had already accumulated on it.

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

    def merge(self, other: RecordOutcome) -> RecordOutcome:
        return RecordOutcome(
            received=self.received + other.received,
            accepted=self.accepted + other.accepted,
            created=self.created + other.created,
            duplicates=self.duplicates + other.duplicates,
            skipped=self.skipped + other.skipped,
            conflicts=self.conflicts + other.conflicts,
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
) -> None:
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id, posting_id, alias_kind, namespace, value, url, req_id, provenance_json, "
        "confidence, valid_from, valid_to) VALUES (?,?,?,?,?,?,?,?,?,?,NULL)",
        (
            new_uid(),
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
      4. record — one `run_postings` row per (run, posting), carrying the content
         hash Phase 3.1 will diff and the `source_run_id` that observed it.

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
    #: Resolved lazily, on the first re-emission only, so a batch with no
    #: duplicates pays no extra query.
    source_name: str | None = None
    source_resolved = False

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
        for claim in resolving:
            row = _active_alias(conn, claim)
            if row is not None:
                posting_id = row["posting_id"]
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

        for claim in claims:
            row = _active_alias(conn, claim)
            if row is None:
                _insert_alias(
                    conn,
                    claim=claim,
                    posting_id=posting_id,
                    source_run_id=source_run_id,
                    at=recorded_at,
                )
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

        cursor = conn.execute(
            "INSERT OR IGNORE INTO run_postings "
            "(run_uid, posting_id, posting_version_id, source_run_id, present, "
            "first_seen_in_run, recorded_at, membership_kind, content_hash) "
            "VALUES (?,?,NULL,?,1,?,?,'snapshot',?)",
            (run_uid, posting_id, source_run_id, first_seen_in_run, recorded_at,
             record.content_hash()),
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

    return RecordOutcome(
        received=received,
        accepted=accepted,
        created=created,
        duplicates=duplicates,
        skipped=skipped,
        conflicts=conflicts,
    )


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
        "SELECT checkpoint_json FROM source_runs "
        "WHERE run_uid=? AND source=? AND checkpoint_json IS NOT NULL "
        "ORDER BY attempt DESC LIMIT 1",
        (run_uid, source),
    ).fetchone()
    return row["checkpoint_json"] if row else None


def max_attempt_by_source(conn: sqlite3.Connection, run_uid: str) -> dict[str, int]:
    return {
        row["source"]: int(row["n"])
        for row in conn.execute(
            "SELECT source, MAX(attempt) AS n FROM source_runs WHERE run_uid=? GROUP BY source",
            (run_uid,),
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
            "     WHERE s.run_uid=r.run_uid AND s.status='interrupted') AS interrupted_attempts, "
            "  (SELECT COUNT(*) FROM source_runs s "
            "     WHERE s.run_uid=r.run_uid AND s.status='succeeded') AS succeeded_attempts "
            "FROM pipeline_runs r WHERE r.status='interrupted' "
            "ORDER BY COALESCE(r.started_at, r.requested_at) DESC LIMIT ?",
            (limit,),
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
            "WHERE run_uid=? AND checkpoint_json IS NOT NULL ORDER BY source, attempt",
            (run_uid,),
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
