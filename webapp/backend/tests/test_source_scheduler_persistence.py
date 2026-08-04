"""Persistence behaviour: identity, evidence, restart, resume, and contention.

Companion to `test_source_scheduler`, which covers scheduling. Everything here is
about what ends up in SQLite and whether it can be trusted:

  * identity precedence — source requisition id first, URL only ever as a
    conservative secondary alias;
  * replay safety — the same record delivered twice is one posting;
  * immutable evidence — a settled attempt is never rewritten;
  * durability before broadcast — the commit precedes the hook;
  * restart — orphans become interrupted, and a resume is an explicit decision;
  * contention — a reader querying throughout a run never sees a locked database.

Every database is created under `tmp_path` by `make_connect`. Nothing here can
reach webapp/app.db.
"""
import asyncio
import json
import sqlite3
import threading
import time

import pytest

from backend.sources import runstore
from backend.sources.contract import Checkpoint, InventoryScope, RunKind
from backend.sources.scheduler import (
    Scheduler,
    SchedulerConfig,
    recover_orphans,
    resumable_runs,
    resume_plan,
    successful_source_scopes,
)
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    emitting,
    fast,
    make_connect,
    paged,
    plan_of,
    scalar,
    transient_then,
)

FAST_RETRY = dict(retry_base_delay_seconds=0.01, retry_jitter=0.0)


def run(coro):
    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


def scheduler(connect, **config):
    return Scheduler(connect, config=SchedulerConfig(**{**FAST_RETRY, **config}))


def rows(connect, sql, params=()):
    conn = connect()
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _at(value: str):
    """Parse a stored timestamp, so ordering assertions compare instants rather
    than strings whose formatting can vary."""
    from datetime import datetime

    return datetime.fromisoformat(value)


# --------------------------------------------------------------------------- #
# Identity
# --------------------------------------------------------------------------- #
def test_requisition_id_is_the_identity_and_the_url_is_only_an_alias(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "greenhouse-like",
        instances=("anthropic",),
        body=emitting(
            [
                {
                    "title": "Support Engineer",
                    "company": "Anthropic",
                    "url": "https://boards.example/anthropic/4020123?utm_source=x",
                    "req_id": "4020123",
                }
            ]
        ),
    )

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    aliases = rows(
        connect,
        "SELECT alias_kind, namespace, value, confidence FROM posting_aliases ORDER BY alias_kind",
    )
    assert [(a["alias_kind"], a["namespace"], a["value"]) for a in aliases] == [
        ("source_req", "greenhouse-like:anthropic", "4020123"),
        # Tracking parameters are stripped before the URL becomes an alias.
        ("url", "url", "https://boards.example/anthropic/4020123"),
    ]
    # Rank 0 is authoritative; rank 1 is explicitly weaker evidence.
    assert dict((a["alias_kind"], a["confidence"]) for a in aliases) == {
        "source_req": 1.0,
        "url": 0.5,
    }
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1


def test_a_url_only_record_still_gets_an_identity(tmp_path):
    """Aggregators frequently have no requisition id. They must still resolve."""
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "aggregator",
        instances=("indeed",),
        body=emitting(
            [{"title": "Support Engineer", "company": "Acme", "url": "https://x.example/a"}]
        ),
    )

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    aliases = rows(connect, "SELECT alias_kind, namespace FROM posting_aliases")
    assert [(a["alias_kind"], a["namespace"]) for a in aliases] == [("url", "url")]
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1


def test_an_aggregator_mirror_joins_the_board_posting_it_copied(tmp_path):
    """A URL-only record has nothing better than its URL, so the board's alias
    resolves it. This is the order Phase 3 prescribes: direct inventories first."""
    connect = make_connect(tmp_path)
    shared = "https://boards.example/acme/77"
    board = FakeAdapter(
        "board",
        instances=("acme",),
        body=emitting(
            [{"title": "Support Engineer", "company": "Acme", "url": shared, "req_id": "77"}]
        ),
    )
    aggregator = FakeAdapter(
        "aggregator",
        instances=("indeed",),
        body=emitting([{"title": "Support Engineer", "company": "Acme", "url": shared}]),
    )

    result = run(
        scheduler(connect, max_concurrent_targets=1).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(board, aggregator)
        )
    )

    assert result.status == "succeeded"
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1
    kinds = {r["alias_kind"] for r in rows(connect, "SELECT alias_kind FROM posting_aliases")}
    assert kinds == {"url", "source_req"}
    # One membership row: the same posting seen twice in one run is one row.
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 1
    assert scalar(connect, "SELECT COUNT(*) FROM identity_evidence") == 0
    # And it stays attributed to the board that enumerated it. Moving it to the
    # aggregator's PARTIAL-scope attempt would cost the board's complete
    # inventory a row it genuinely saw.
    owner = scalar(
        connect,
        "SELECT s.source FROM run_postings rp JOIN source_runs s "
        "ON s.source_run_id = rp.source_run_id",
    )
    assert owner == "board:acme"


def test_a_requisition_never_loses_its_identity_to_an_existing_url_alias(tmp_path):
    """The reverse order, stated honestly: the aggregator's URL is already an
    alias, and the board's requisition still wins. Two postings plus recorded
    conflict evidence, which is Phase 3's input — not a silent merge."""
    connect = make_connect(tmp_path)
    shared = "https://boards.example/acme/77"
    aggregator = FakeAdapter(
        "aggregator",
        instances=("indeed",),
        body=emitting([{"title": "Support Engineer", "company": "Acme", "url": shared}]),
    )
    board = FakeAdapter(
        "board",
        instances=("acme",),
        body=emitting(
            [{"title": "Support Engineer", "company": "Acme", "url": shared, "req_id": "77"}]
        ),
    )

    run(
        scheduler(connect, max_concurrent_targets=1).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(aggregator, board)
        )
    )

    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 2
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 2
    assert scalar(connect, "SELECT COUNT(*) FROM identity_evidence") == 1
    assert (
        scalar(connect, "SELECT COUNT(*) FROM posting_aliases WHERE alias_kind='url'") == 1
    ), "the URL alias must not be duplicated or re-pointed"


def test_a_conflicting_url_claim_is_recorded_as_evidence_not_repointed(tmp_path):
    """Two different requisitions advertising the same URL must not be merged."""
    connect = make_connect(tmp_path)
    shared = "https://boards.example/acme/shared"
    first = FakeAdapter(
        "board",
        instances=("acme",),
        body=emitting(
            [{"title": "Role A", "company": "Acme", "url": shared, "req_id": "111"}]
        ),
    )
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(first)))
    original = scalar(connect, "SELECT posting_id FROM posting_aliases WHERE alias_kind='url'")

    second = FakeAdapter(
        "board",
        instances=("acme",),
        body=emitting(
            [{"title": "Role B", "company": "Acme", "url": shared, "req_id": "222"}]
        ),
    )
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(second)))

    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 2
    # The URL alias still points where it did. Nothing was silently merged.
    assert (
        scalar(connect, "SELECT posting_id FROM posting_aliases WHERE alias_kind='url'")
        == original
    )
    evidence = rows(connect, "SELECT evidence_kind, evidence_json FROM identity_evidence")
    assert [e["evidence_kind"] for e in evidence] == ["alias-conflict"]
    payload = json.loads(evidence[0]["evidence_json"])
    assert payload["claim"]["rank"] == 1
    assert payload["conflicting_posting_id"] == original


def test_a_retry_hands_its_whole_inventory_to_the_attempt_that_finished(tmp_path):
    """Attempt 1 delivers part of the board and fails; attempt 2 redelivers all of
    it and succeeds. Phase 2.4 joins membership to the succeeded attempt, so every
    row that attempt enumerated has to be reachable from it — otherwise the rows
    attempt 1 happened to insert first read as unseen, and live jobs are marked
    absent."""
    connect = make_connect(tmp_path)
    flaky = FakeAdapter("flaky", instances=("board",), body=transient_then(count=5, before=3))

    result = run(
        scheduler(connect, batch_size=1, flush_interval_seconds=0.0).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(flaky)
        )
    )

    assert result.target("flaky:board").status == "succeeded"
    failed, succeeded = rows(
        connect,
        "SELECT source_run_id, attempt, status, fetched_count, accepted_count, error_json "
        "FROM source_runs WHERE source='flaky:board' ORDER BY attempt",
    )
    assert (failed["attempt"], failed["status"]) == (1, "failed")
    assert (succeeded["attempt"], succeeded["status"]) == (2, "succeeded")

    conn = connect()
    try:
        scopes = successful_source_scopes(conn, result.run_uid)
    finally:
        conn.close()
    assert [s["source"] for s in scopes] == ["flaky:board"]
    assert scopes[0]["source_run_id"] == succeeded["source_run_id"]

    total = scalar(
        connect, "SELECT COUNT(*) FROM run_postings WHERE run_uid=?", (result.run_uid,)
    )
    licensed = scalar(
        connect,
        "SELECT COUNT(*) FROM run_postings WHERE run_uid=? AND source_run_id=?",
        (result.run_uid, scopes[0]["source_run_id"]),
    )
    assert total == 5
    assert licensed == 5, "rows delivered by the failed attempt are invisible to Phase 2.4"
    assert (
        scalar(
            connect,
            "SELECT COUNT(*) FROM run_postings WHERE source_run_id=?",
            (failed["source_run_id"],),
        )
        == 0
    )

    # The failed attempt's own row is evidence and is untouched by the move: it
    # still records what it fetched, what it inserted, and why it failed.
    assert failed["fetched_count"] == 3
    assert failed["accepted_count"] == 3
    assert json.loads(failed["error_json"])["disposition"] == "transient"
    # And the succeeded attempt's count is still its own insertions, not the
    # inventory size — the join is what carries the inventory.
    assert succeeded["accepted_count"] == 2


def test_the_same_conflict_seen_every_run_is_one_evidence_row(tmp_path):
    """Alias conflicts recur daily by construction: the two boards that disagree
    today disagree tomorrow. One standing fact is one row, or `identity_evidence`
    grows by the size of the conflict set every run, forever."""
    connect = make_connect(tmp_path)
    shared = "https://boards.example/acme/shared"

    def board(req_id: str, title: str) -> FakeAdapter:
        return FakeAdapter(
            "board",
            instances=("acme",),
            body=emitting([{"title": title, "company": "Acme", "url": shared, "req_id": req_id}]),
        )

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(board("111", "Role A"))))
    conflicting = [
        run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(board("222", "Role B"))))
        for _ in range(3)
    ]

    evidence = rows(connect, "SELECT evidence_json, observed_at FROM identity_evidence")
    assert len(evidence) == 1, f"{len(evidence)} rows for one recurring conflict"
    payload = json.loads(evidence[0]["evidence_json"])
    assert payload["claim"]["rank"] == 1
    # The observing attempt is provenance beside the hashed disagreement, so the
    # row still says who saw it first.
    first_attempt = scalar(
        connect,
        "SELECT source_run_id FROM source_runs WHERE run_uid=?",
        (conflicting[0].run_uid,),
    )
    assert payload["first_observed"]["source_run_id"] == first_attempt
    assert "source_run_id" not in payload


def test_replaying_the_same_record_produces_one_posting(tmp_path):
    """Contract invariant 5: re-emission is expected, not an error."""
    connect = make_connect(tmp_path)
    spec = {
        "title": "Support Engineer",
        "company": "Acme",
        "url": "https://x.example/1",
        "req_id": "1",
    }
    adapter = FakeAdapter("dup", instances=("board",), body=emitting([spec, spec, spec]))

    result = run(
        scheduler(connect, batch_size=1, flush_interval_seconds=0.0).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)
        )
    )

    target = result.target("dup:board")
    assert target.fetched == 3
    assert target.accepted == 1
    assert target.duplicates == 2
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1
    assert scalar(connect, "SELECT COUNT(*) FROM posting_aliases") == 2


def test_a_second_run_reuses_the_posting_and_records_new_membership(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("src", instances=("board",), body=fast(2))

    first = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 2
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 4
    assert first.created == 2
    assert second.created == 0, "postings must not be re-minted on the second run"
    # first_seen_in_run marks the run that discovered each posting.
    flags = rows(
        connect,
        "SELECT run_uid, SUM(first_seen_in_run) AS n FROM run_postings GROUP BY run_uid",
    )
    assert {r["run_uid"]: r["n"] for r in flags} == {first.run_uid: 2, second.run_uid: 0}


def test_content_hash_is_stored_for_phase_three_to_diff(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("src", instances=("board",), body=fast(2))

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    hashes = [r["content_hash"] for r in rows(connect, "SELECT content_hash FROM run_postings")]
    assert len(hashes) == 2
    assert all(h and h.startswith("sha256:") for h in hashes)
    assert len(set(hashes)) == 2
    # Phase 2 records the hash and nothing else; versioning is Phase 3.1.
    assert scalar(connect, "SELECT COUNT(*) FROM posting_versions") == 0


def test_a_changed_record_changes_its_stored_hash(tmp_path):
    connect = make_connect(tmp_path)
    base = {
        "title": "Support Engineer",
        "company": "Acme",
        "url": "https://x.example/1",
        "req_id": "1",
    }
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("src", instances=("b",), body=emitting([base]))),
        )
    )
    moved = {**base, "title": "Senior Support Engineer"}
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("src", instances=("b",), body=emitting([moved]))),
        )
    )

    hashes = [
        r["content_hash"]
        for r in rows(connect, "SELECT content_hash FROM run_postings ORDER BY recorded_at")
    ]
    assert len(hashes) == 2
    assert hashes[0] != hashes[1], "a material change must be visible to Phase 3.1"
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1


def test_the_legacy_tables_are_never_written(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("src", instances=("a", "b"), body=fast(4))

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    for table in ("jobs", "job_history", "job_state", "state_events", "runs"):
        assert scalar(connect, f"SELECT COUNT(*) FROM {table}") == 0, table
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 8


# --------------------------------------------------------------------------- #
# Immutable evidence
# --------------------------------------------------------------------------- #
def test_each_attempt_is_its_own_row_and_a_settled_row_is_never_rewritten(tmp_path):
    connect = make_connect(tmp_path)
    flaky = FakeAdapter("flaky", instances=("board",), body=transient_then(count=2))

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(flaky)))

    attempts = rows(
        connect,
        "SELECT attempt, status, started_at, finished_at, error_json, step "
        "FROM source_runs WHERE source='flaky:board' ORDER BY attempt",
    )
    assert [a["attempt"] for a in attempts] == [1, 2]
    assert [a["status"] for a in attempts] == ["failed", "succeeded"]
    assert all(a["step"] == "fetch" for a in attempts)
    assert attempts[0]["error_json"] and attempts[1]["error_json"] is None
    assert _at(attempts[0]["started_at"]) <= _at(attempts[0]["finished_at"])
    assert _at(attempts[0]["finished_at"]) <= _at(attempts[1]["started_at"])
    before = tuple(attempts[0])

    # Directly re-settling a terminal row is refused, not applied.
    conn = connect()
    try:
        row = conn.execute(
            "SELECT source_run_id FROM source_runs WHERE source='flaky:board' AND attempt=1"
        ).fetchone()
        applied = runstore.finish_source_run(
            conn,
            source_run_id=row["source_run_id"],
            status="succeeded",
            finished_at="2099-01-01T00:00:00+00:00",
        )
        conn.commit()
    finally:
        conn.close()
    assert applied is False
    after = tuple(
        rows(
            connect,
            "SELECT attempt, status, started_at, finished_at, error_json, step "
            "FROM source_runs WHERE source='flaky:board' AND attempt=1",
        )[0]
    )
    assert after == before


def test_settling_an_attempt_keeps_a_changed_count_another_phase_wrote(tmp_path):
    """Phase 2 has nothing to say about `changed_count`, which is exactly why
    settling an attempt must leave it alone rather than assign NULL over it."""
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn,
            run_uid="run-1",
            kind=str(RunKind.FULL_DIRECT),
            requested_at="2026-08-03T00:00:00+00:00",
            started_at="2026-08-03T00:00:00+00:00",
        )
        runstore.create_source_run(
            conn,
            source_run_id="attempt-1",
            run_uid="run-1",
            source="src:a",
            attempt=1,
            requested_at="2026-08-03T00:00:00+00:00",
            started_at="2026-08-03T00:00:00+00:00",
        )
        # Stands in for the Phase 3.1 writer, the only thing that knows this number.
        conn.execute("UPDATE source_runs SET changed_count=7 WHERE source_run_id='attempt-1'")
        applied = runstore.finish_source_run(
            conn,
            source_run_id="attempt-1",
            status="succeeded",
            finished_at="2026-08-03T00:00:05+00:00",
            fetched_count=9,
        )
        conn.commit()
    finally:
        conn.close()

    assert applied is True
    assert scalar(connect, "SELECT changed_count FROM source_runs") == 7
    assert scalar(connect, "SELECT status FROM source_runs") == "succeeded"


def test_every_runstore_annotation_resolves():
    """An annotation naming something the module never imported is a NameError
    waiting for the first caller that introspects it."""
    import typing

    for name in dir(runstore):
        obj = getattr(runstore, name)
        if callable(obj) and getattr(obj, "__module__", None) == runstore.__name__:
            typing.get_type_hints(obj)


def test_attempt_metadata_keeps_both_start_time_and_finish_time_evidence(tmp_path):
    """Settling an attempt merges into its metadata; it does not replace it, so the
    execution mode and deadline recorded at start survive."""
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "src",
        instances=("board",),
        body=fast(2),
        descriptor=descriptor_for("src", deadline=7.5),
    )

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    metadata = json.loads(scalar(connect, "SELECT metadata_json FROM source_runs"))
    assert metadata["execution"] == "async-inprocess"
    assert metadata["category"] == "direct"
    assert metadata["deadline_seconds"] == 7.5
    assert metadata["resumed"] is False
    assert metadata["duration_seconds"] >= 0
    assert metadata["attempt"] == 1
    assert scalar(connect, "SELECT deadline_at FROM source_runs") is not None


def test_run_events_are_sequenced_and_persisted_before_the_broadcast_hook(tmp_path):
    connect = make_connect(tmp_path)
    observed = []

    def hook(events):
        # The hook is the broadcast seam. By the time it runs, the rows it
        # describes must already be readable from an independent connection.
        durable = scalar(connect, "SELECT COUNT(*) FROM run_events")
        observed.append((tuple(e.event_type for e in events), durable))

    adapter = FakeAdapter("src", instances=("a", "b"), body=fast(2))
    sched = Scheduler(connect, config=SchedulerConfig(**FAST_RETRY), event_hook=hook)
    result = run(sched.run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    delivered = 0
    for types, durable in observed:
        delivered += len(types)
        assert durable >= delivered, "an event was broadcast before it was durable"

    stored = rows(connect, "SELECT sequence, event_type FROM run_events ORDER BY sequence")
    assert [r["sequence"] for r in stored] == list(range(len(stored)))
    assert stored[0]["event_type"] == "run.started"
    assert stored[-1]["event_type"] == "run.succeeded"
    assert sum(1 for r in stored if r["event_type"] == "source.started") == 2
    assert sum(1 for r in stored if r["event_type"] == "source.succeeded") == 2
    assert delivered == len(stored)
    assert result.status == "succeeded"


def test_phase_two_four_can_read_which_sources_licensed_absence(tmp_path):
    connect = make_connect(tmp_path)
    complete = FakeAdapter("board", instances=("acme",), body=fast(2))
    partial = FakeAdapter(
        "search",
        instances=("bay-area",),
        body=fast(2),
        descriptor=descriptor_for("search", inventory_scope=InventoryScope.PARTIAL),
        inventory_scope=InventoryScope.PARTIAL,
    )
    broken = FakeAdapter(
        "dead",
        instances=("gone",),
        body=transient_then(succeed_on_attempt=99),
    )

    result = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(complete, partial, broken)
        )
    )

    conn = connect()
    try:
        scopes = successful_source_scopes(conn, result.run_uid)
    finally:
        conn.close()
    by_source = {s["source"]: s["inventory_scope"] for s in scopes}
    assert by_source == {"board:acme": "complete", "search:bay-area": "partial"}
    assert "dead:gone" not in by_source, "a failed source must never licence absence marking"
    assert all(s["finished_at"] for s in scopes)


# --------------------------------------------------------------------------- #
# Checkpoints
# --------------------------------------------------------------------------- #
def test_checkpoints_are_persisted_with_the_batch_they_describe(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "paged",
        instances=("board",),
        body=paged(pages=3, per_page=2),
        descriptor=descriptor_for("paged", supports_checkpoint=True),
    )

    run(
        scheduler(connect, batch_size=1, flush_interval_seconds=0.0).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)
        )
    )

    blob = scalar(connect, "SELECT checkpoint_json FROM source_runs WHERE source='paged:board'")
    checkpoint = Checkpoint.from_json(blob)
    assert checkpoint.cursor["next_page"] == 3
    assert checkpoint.emitted == 6
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 6


def test_a_checkpoint_is_durable_before_the_target_that_produced_it_finishes(tmp_path):
    """The cursor commits with the batch it describes, not at settle time, so a
    process killed mid-target resumes from real committed progress."""
    connect = make_connect(tmp_path)
    gate = asyncio.Event()

    async def two_pages_then_wait(adapter, target, ctx):
        for page in range(2):
            for index in range(2):
                yield target.record(
                    title=f"Role {page}{index}",
                    company="Acme",
                    url=f"https://x.example/{page}{index}",
                    req_id=f"{page}{index}",
                )
            ctx.mark_checkpoint({"next_page": page + 1}, target=target, emitted=(page + 1) * 2)
        await gate.wait()

    adapter = FakeAdapter(
        "paged",
        instances=("board",),
        body=two_pages_then_wait,
        descriptor=descriptor_for("paged", supports_checkpoint=True),
    )

    async def scenario():
        sched = scheduler(connect, batch_size=1, flush_interval_seconds=0.0)
        handle = sched.start(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter))
        deadline = time.monotonic() + 5
        blob = None
        while time.monotonic() < deadline:
            blob = scalar(connect, "SELECT checkpoint_json FROM source_runs")
            if blob:
                break
            await asyncio.sleep(0.01)
        assert not handle.done, "the target settled before the observation"
        gate.set()
        return blob, await handle.wait()

    blob, result = run(scenario())

    assert blob, "no checkpoint was durable while the target was still running"
    mid = Checkpoint.from_json(blob)
    assert mid.cursor["next_page"] >= 1
    # Everything the cursor claims was delivered is already committed.
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") >= mid.emitted
    assert result.status == "succeeded"


# --------------------------------------------------------------------------- #
# Restart and resume
# --------------------------------------------------------------------------- #
def _simulate_crashed_run(connect, *, run_uid, sources, checkpoints=None, succeeded=()):
    """Leave the database exactly as a killed process would: rows still 'running'.

    Written through the production `runstore` writers rather than hand-rolled SQL,
    so the fixture cannot drift from what the scheduler actually persists.
    """
    checkpoints = checkpoints or {}
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn,
            run_uid=run_uid,
            kind=str(RunKind.FULL_DIRECT),
            trigger="test",
            requested_at="2026-08-03T00:00:00+00:00",
            started_at="2026-08-03T00:00:00+00:00",
        )
        for index, source in enumerate(sources):
            source_run_id = f"crashed-{index}"
            runstore.create_source_run(
                conn,
                source_run_id=source_run_id,
                run_uid=run_uid,
                source=source,
                attempt=1,
                requested_at="2026-08-03T00:00:00+00:00",
                started_at="2026-08-03T00:00:00+00:00",
                inventory_scope="complete",
            )
            if source in checkpoints:
                runstore.update_source_run_progress(
                    conn,
                    source_run_id=source_run_id,
                    fetched_count=checkpoints[source]["emitted"],
                    accepted_delta=checkpoints[source]["emitted"],
                    checkpoint_json=checkpoints[source]["json"],
                )
            if source in succeeded:
                runstore.finish_source_run(
                    conn,
                    source_run_id=source_run_id,
                    status="succeeded",
                    finished_at="2026-08-03T00:00:05+00:00",
                )
        conn.commit()
    finally:
        conn.close()


def test_restart_marks_orphaned_attempts_interrupted_without_inventing_a_finish_time(tmp_path):
    connect = make_connect(tmp_path)
    _simulate_crashed_run(connect, run_uid="crashed", sources=["src:a", "src:b"])

    report = recover_orphans(connect)

    assert report.run_uids == ("crashed",)
    assert len(report.source_run_ids) == 2
    attempts = rows(connect, "SELECT status, finished_at, metadata_json FROM source_runs")
    assert {a["status"] for a in attempts} == {"interrupted"}
    for attempt in attempts:
        assert attempt["finished_at"] is None, "a fabricated finish time is fabricated evidence"
        note = json.loads(attempt["metadata_json"])["interrupted"]
        assert note["reason"] == "orphaned by process exit"
        assert note["at"] == report.at
    assert scalar(connect, "SELECT status FROM pipeline_runs") == "interrupted"
    assert scalar(connect, "SELECT finished_at FROM pipeline_runs") is None

    # Idempotent: a second recovery finds nothing left to do.
    assert recover_orphans(connect).total == 0


def test_a_scheduler_run_recovers_orphans_before_it_starts(tmp_path):
    connect = make_connect(tmp_path)
    _simulate_crashed_run(connect, run_uid="crashed", sources=["src:a"])
    adapter = FakeAdapter("src", instances=("a",), body=fast(1))

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert result.recovery is not None
    assert result.recovery.run_uids == ("crashed",)
    assert scalar(
        connect, "SELECT status FROM pipeline_runs WHERE run_uid='crashed'"
    ) == "interrupted"
    assert scalar(
        connect, "SELECT status FROM pipeline_runs WHERE run_uid=?", (result.run_uid,)
    ) == "succeeded"


def test_recovery_never_touches_a_run_the_caller_still_owns(tmp_path):
    connect = make_connect(tmp_path)
    _simulate_crashed_run(connect, run_uid="live", sources=["src:a"])

    report = recover_orphans(connect, exclude_run_uids=("live",))

    assert report.total == 0
    assert scalar(connect, "SELECT status FROM pipeline_runs") == "running"


def test_resumable_runs_and_resume_plan_describe_the_decision(tmp_path):
    connect = make_connect(tmp_path)
    checkpoint = Checkpoint(
        source_key="paged",
        instance_key="board",
        cursor={"next_page": 2},
        config_fingerprint="whatever",
        emitted=4,
    )
    _simulate_crashed_run(
        connect,
        run_uid="crashed",
        sources=["paged:board", "src:done"],
        checkpoints={"paged:board": {"json": checkpoint.to_json(), "emitted": 4}},
        succeeded=("src:done",),
    )
    recover_orphans(connect)

    conn = connect()
    try:
        listed = resumable_runs(conn)
        plan = resume_plan(conn, "crashed")
    finally:
        conn.close()

    assert [r["run_uid"] for r in listed] == ["crashed"]
    assert listed[0]["interrupted_attempts"] == 1
    assert listed[0]["succeeded_attempts"] == 1
    assert plan["resumable"] is True
    assert plan["completed_sources"] == ("src:done",)
    assert plan["pending_sources"] == ("paged:board",)
    assert plan["max_attempt_by_source"] == {"paged:board": 1, "src:done": 1}
    assert json.loads(plan["checkpoints"]["paged:board"])["cursor"] == {"next_page": 2}


def test_resuming_replays_from_the_checkpoint_and_appends_attempts(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "paged",
        instances=("board",),
        body=paged(pages=4, per_page=2),
        descriptor=descriptor_for("paged", supports_checkpoint=True),
    )
    target = adapter.targets()[0]
    checkpoint = Checkpoint(
        source_key="paged",
        instance_key="board",
        cursor={"next_page": 2},
        config_fingerprint=target.config_fingerprint(),
        emitted=4,
    )
    _simulate_crashed_run(
        connect,
        run_uid="crashed",
        sources=["paged:board", "src:done"],
        checkpoints={"paged:board": {"json": checkpoint.to_json(), "emitted": 4}},
        succeeded=("src:done",),
    )
    recover_orphans(connect)

    done = FakeAdapter("src", instances=("done",), body=fast(3))
    result = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(adapter, done),
            resume_run_uid="crashed",
        )
    )

    assert result.run_uid == "crashed"
    assert result.status == "succeeded"
    # The already-successful target is not re-run.
    assert result.target("src:done").status == "skipped"
    assert done.attempts == {}
    # The paged target resumed at page 2 and produced only pages 2 and 3.
    resumed = result.target("paged:board")
    assert resumed.status == "succeeded"
    assert resumed.fetched == 4
    attempts = rows(
        connect,
        "SELECT attempt, status FROM source_runs WHERE source='paged:board' ORDER BY attempt",
    )
    assert [(a["attempt"], a["status"]) for a in attempts] == [
        (1, "interrupted"),
        (2, "succeeded"),
    ]
    assert scalar(connect, "SELECT status FROM pipeline_runs WHERE run_uid='crashed'") == "succeeded"
    events = [r["event_type"] for r in rows(connect, "SELECT event_type FROM run_events ORDER BY sequence")]
    assert events[0] == "run.resumed"


def test_a_stale_checkpoint_is_not_handed_back_on_resume(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "paged",
        instances=("board",),
        body=paged(pages=2, per_page=2),
        descriptor=descriptor_for("paged", supports_checkpoint=True),
    )
    stale = Checkpoint(
        source_key="paged",
        instance_key="board",
        cursor={"next_page": 1},
        config_fingerprint="fingerprint-from-a-different-configuration",
        emitted=2,
    )
    _simulate_crashed_run(
        connect,
        run_uid="crashed",
        sources=["paged:board"],
        checkpoints={"paged:board": {"json": stale.to_json(), "emitted": 2}},
    )
    recover_orphans(connect)

    result = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(adapter), resume_run_uid="crashed"
        )
    )

    # Started clean: all four records, not the two after the stale cursor.
    assert result.target("paged:board").fetched == 4
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 4


def test_resuming_a_run_that_is_not_interrupted_is_refused(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("src", instances=("a",), body=fast(1))
    finished = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    with pytest.raises(ValueError, match="not resumable"):
        run(
            scheduler(connect).run(
                kind=RunKind.FULL_DIRECT,
                plan=plan_of(adapter),
                resume_run_uid=finished.run_uid,
            )
        )


# --------------------------------------------------------------------------- #
# End-to-end against a real adapter
# --------------------------------------------------------------------------- #
def test_the_real_greenhouse_adapter_runs_end_to_end_into_canonical_tables(tmp_path):
    """One check that the fakes have not drifted from a real adapter.

    Greenhouse is the reference implementation: it declares `TransportKind.HTTP`,
    so this also exercises the transport hand-off, `plan()` from a real
    `SourceConfig`, and a real record shape reaching `postings`. Frozen fixture,
    no network.
    """
    from backend.sources.adapters import greenhouse
    from backend.sources.contract import HttpResponse, SourceConfig
    from backend.sources.testing import FakeTransport, fixture_bytes

    connect = make_connect(tmp_path)
    config = SourceConfig.from_mapping(
        {"companies": {"greenhouse": {"anthropic": "Anthropic", "acme": "Acme"}}}
    )
    board = fixture_bytes("greenhouse", "board.json")
    transport = FakeTransport()
    for slug in ("anthropic", "acme"):
        transport.add(
            greenhouse.board_url(slug),
            HttpResponse(status=200, url=greenhouse.board_url(slug), content=board),
        )

    targets = greenhouse.ADAPTER.plan(config)
    plan = [(greenhouse.ADAPTER, target) for target in targets]
    sched = Scheduler(connect, config=SchedulerConfig(**FAST_RETRY), transport=transport)
    result = run(sched.run(kind=RunKind.DAILY, config=config, plan=plan))

    assert result.status == "succeeded"
    assert len(result.succeeded_targets) == 2
    assert result.accepted > 0
    # Two boards, same fixture: the requisition namespace keeps them apart, so
    # nothing is collapsed across boards.
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == result.accepted
    namespaces = {
        r["namespace"]
        for r in rows(connect, "SELECT namespace FROM posting_aliases WHERE alias_kind='source_req'")
    }
    assert namespaces == {"greenhouse:anthropic", "greenhouse:acme"}
    scopes = {r["source"]: r["inventory_scope"] for r in rows(
        connect, "SELECT source, inventory_scope FROM source_runs"
    )}
    assert scopes == {"greenhouse:anthropic": "complete", "greenhouse:acme": "complete"}
    assert transport.call_count == 2
    # Both boards were served the same fixture, so the second board's postings
    # advertise URLs already claimed by the first. That is the conflict case, and
    # it is recorded rather than merged: half the postings, one per board.
    per_board = result.accepted // 2
    assert scalar(connect, "SELECT COUNT(*) FROM identity_evidence") == per_board
    assert (
        scalar(connect, "SELECT COUNT(*) FROM posting_aliases WHERE alias_kind='url'")
        == per_board
    )


# --------------------------------------------------------------------------- #
# Contention
# --------------------------------------------------------------------------- #
def test_a_reader_can_query_throughout_a_run_without_the_database_locking(tmp_path):
    """WAL plus a single writer: a reader must never see SQLITE_BUSY, and must see
    the run's rows grow while the run is still in flight."""
    connect = make_connect(tmp_path)
    stop = threading.Event()
    errors: list[BaseException] = []
    samples: list[int] = []

    def reader():
        conn = connect()
        try:
            while not stop.is_set():
                try:
                    samples.append(
                        conn.execute("SELECT COUNT(*) FROM run_postings").fetchone()[0]
                    )
                    conn.execute(
                        "SELECT s.source, COUNT(rp.posting_id) FROM source_runs s "
                        "LEFT JOIN run_postings rp ON rp.source_run_id = s.source_run_id "
                        "GROUP BY s.source"
                    ).fetchall()
                except BaseException as exc:  # noqa: BLE001 - reported to the test
                    errors.append(exc)
                    return
                time.sleep(0.001)
        finally:
            conn.close()

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    try:
        adapters = [
            FakeAdapter(f"src{n}", instances=("a", "b"), body=fast(80)) for n in range(3)
        ]
        result = run(
            scheduler(
                connect,
                batch_size=5,
                flush_interval_seconds=0.0,
                max_concurrent_targets=6,
            ).run(kind=RunKind.FULL_DIRECT, plan=plan_of(*adapters))
        )
    finally:
        stop.set()
        thread.join(timeout=5)

    assert errors == [], f"reader hit {errors[0]!r} while the writer was committing"
    assert result.status == "succeeded"
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 480
    assert len(samples) > 5
    assert samples == sorted(samples), "a reader observed counts going backwards"
    assert max(samples) > 0
    report = json.loads(
        scalar(
            connect,
            "SELECT aggregate_report_json FROM pipeline_runs WHERE run_uid=?",
            (result.run_uid,),
        )
    )
    assert report["writer"]["busy_retries"] == 0


def test_the_scheduler_refuses_a_database_without_the_canonical_schema(tmp_path):
    path = tmp_path / "bare.db"
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE unrelated (x INTEGER)")
    raw.commit()
    raw.close()

    def connect():
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn

    adapter = FakeAdapter("src", instances=("a",), body=fast(1))
    with pytest.raises(RuntimeError, match="canonical schema"):
        run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))
