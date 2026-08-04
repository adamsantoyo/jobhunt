"""Phase 2.4: absence marking, presence refresh, and degraded freshness.

The single question this suite exists to answer is "can a run make a live posting
disappear?", and every test is a way of asking it. A bug here does not surface as a
crash or a wrong count; it surfaces as a job the user never sees again, months later,
with nothing in the database to say it was ever there. So the tests are written as
adversarial scenarios rather than as unit coverage:

  LICENCE      only a succeeded, COMPLETE-scope attempt may mark anything. Failure,
               timeout, PARTIAL scope, cancellation: nothing moves.
  SCOPE        a licence covers one source instance's own postings and nothing else.
  INVENTORY    the licence is spent against what the WINNING attempt delivered, which
               on a retried target means the rows attempt 1 inserted too (the 2.3
               re-point invariant).
  OBSERVATION  a posting another source delivered in the same run is never marked
               absent, whatever the attempt-scoped join says.
  EVIDENCE     nothing is deleted, and both the going and the coming back are legible
               afterwards.

Every database is created under `tmp_path` by `make_connect`. Nothing here can reach
webapp/app.db, and no migration runs anywhere but on those temporary files.
"""
import asyncio

from backend.sources import runstore
from backend.sources.contract import (
    ExecutionMode,
    InventoryScope,
    RunKind,
    SourceCategory,
    SourceDescriptor,
    TransportKind,
)
from backend.sources.scheduler import (
    Scheduler,
    SchedulerConfig,
    source_instance_freshness,
)
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    emitting,
    fast,
    hanging,
    make_connect,
    permanent_always,
    plan_of,
    posting,
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


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def emitting_indices(by_instance):
    """Yield `posting(target, n)` for each index this target's instance is given.

    Written against the same `posting()` helper the other scheduler suites use, so
    "the board dropped requisition 2" is expressed by leaving 2 out of a list rather
    than by hand-writing a record shape that could drift from the real one.
    """

    async def _body(adapter, target, ctx):
        for n in by_instance[target.instance_key]:
            yield posting(target, n)

    return _body


def presence_by_requisition(connect):
    """`{(namespace, req_id): postings row}` — the presence state, keyed the way the
    scenarios talk about it. Joining through the `source_req` alias is deliberate: it
    is the same evidence `mark_absent_for_scope` scopes on, so a test that keyed on
    posting_id could pass while the scoping was wrong."""
    return {
        (row["ns"], row["req"]): dict(row)
        for row in rows(
            connect,
            "SELECT a.namespace AS ns, a.value AS req, p.* FROM posting_aliases a "
            "JOIN postings p ON p.posting_id = a.posting_id "
            "WHERE a.alias_kind='source_req' AND a.valid_to IS NULL",
        )
    }


def absent_requisitions(connect):
    return {
        key
        for key, row in presence_by_requisition(connect).items()
        if row["absent_since"] is not None
    }


def presence_report(connect, run_uid):
    import json

    blob = scalar(
        connect,
        "SELECT aggregate_report_json FROM pipeline_runs WHERE run_uid=?",
        (run_uid,),
    )
    return json.loads(blob)["presence"]


def attempt_id(connect, run_uid, source, status="succeeded"):
    return scalar(
        connect,
        "SELECT source_run_id FROM source_runs "
        "WHERE run_uid=? AND source=? AND status=? ORDER BY attempt DESC LIMIT 1",
        (run_uid, source, status),
    )


def row_census(connect):
    """Every table absence could conceivably shrink. Absence is a marking, not a
    delete, so these counts may only ever grow."""
    return {
        table: scalar(connect, f"SELECT COUNT(*) FROM {table}")
        for table in (
            "postings",
            "posting_aliases",
            "run_postings",
            "identity_evidence",
            "source_runs",
            "pipeline_runs",
        )
    }


def partial_descriptor(source_key, category=SourceCategory.AGGREGATOR):
    """A source that can never license absence, however successful it is."""
    return SourceDescriptor(
        source_key=source_key,
        category=category,
        run_kinds=frozenset({RunKind.FULL_DIRECT, RunKind.DAILY}),
        default_deadline_seconds=5.0,
        execution=ExecutionMode.ASYNC_INPROCESS,
        transport=TransportKind.NONE,
        default_inventory_scope=InventoryScope.PARTIAL,
    )


# --------------------------------------------------------------------------- #
# The licence and its scope
# --------------------------------------------------------------------------- #
def test_a_complete_success_marks_only_the_unseen_postings_of_that_instance(tmp_path):
    """Two boards of one source. `acme` drops a requisition; `zenith` does not.

    `zenith`'s postings must be untouched. They belong to a different instance, and
    `acme` enumerating its own board is evidence about `acme` and about nothing else.
    """
    connect = make_connect(tmp_path)
    everything = {"acme": (0, 1, 2), "zenith": (0, 1, 2)}
    first = FakeAdapter("board", instances=("acme", "zenith"), body=emitting_indices(everything))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(first)))

    dropped = {"acme": (0, 1), "zenith": (0, 1, 2)}
    second = FakeAdapter("board", instances=("acme", "zenith"), body=emitting_indices(dropped))
    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(second)))

    assert result.status == "succeeded"
    assert absent_requisitions(connect) == {("board:acme", "2")}

    state = presence_by_requisition(connect)
    marked = state[("board:acme", "2")]
    assert marked["absent_run_uid"] == result.run_uid
    assert marked["absent_source_run_id"] == attempt_id(connect, result.run_uid, "board:acme")
    assert marked["returned_at"] is None
    # The sibling instance's identically-numbered requisition is a different posting
    # and stays present, with its own freshly refreshed observation.
    sibling = state[("board:zenith", "2")]
    assert sibling["absent_since"] is None
    assert sibling["last_seen_run_uid"] == result.run_uid

    report = presence_report(connect, result.run_uid)
    assert report["licensed_sources"] == 2
    assert report["marked_absent"] == 1
    by_source = {s["source"]: s for s in report["sources"]}
    assert by_source["board:acme"]["inventory"] == 2
    assert by_source["board:acme"]["owned"] == 3
    assert by_source["board:zenith"]["marked_absent"] == 0


def test_a_complete_success_never_marks_another_instance_or_source_absent(tmp_path):
    """The scoping test that bites: today's run plans ONE target.

    Yesterday's run enumerated a second board of the same source, a different source
    entirely, and an aggregator posting with no requisition identity at all. None of
    those appear in today's plan, so none of them is delivered today — which is
    exactly the shape in which unscoped absence marking retires the whole corpus on
    the strength of one board's inventory. Only `board:acme`'s own dropped
    requisition may move.
    """
    connect = make_connect(tmp_path)
    board = FakeAdapter(
        "board",
        instances=("acme", "zenith"),
        body=emitting_indices({"acme": (0, 1, 2), "zenith": (0, 1, 2)}),
    )
    other = FakeAdapter("other", instances=("x",), body=fast(2))
    aggregator = FakeAdapter(
        "aggregator",
        instances=("indeed",),
        body=emitting([{"title": "Mirrored Role", "company": "Acme",
                        "url": "https://agg.example/7"}]),
        descriptor=partial_descriptor("aggregator"),
        inventory_scope=InventoryScope.PARTIAL,
    )
    first = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(board, other, aggregator)
        )
    )
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 9

    today = FakeAdapter("board", instances=("acme",), body=emitting_indices({"acme": (0, 1)}))
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(today)))

    assert second.status == "succeeded"
    assert absent_requisitions(connect) == {("board:acme", "2")}
    # Counted over ALL postings, not just the ones with a requisition alias: the
    # aggregator's URL-only posting has no instance-scoped identity whatsoever, so no
    # licence can ever own it.
    assert scalar(connect, "SELECT COUNT(*) FROM postings WHERE absent_since IS NOT NULL") == 1

    state = presence_by_requisition(connect)
    for key in (("board:zenith", "0"), ("board:zenith", "2"), ("other:x", "1")):
        assert state[key]["absent_since"] is None
        assert state[key]["last_seen_run_uid"] == first.run_uid

    scope = presence_report(connect, second.run_uid)["sources"][0]
    assert scope["source"] == "board:acme"
    assert scope["owned"] == 3, "ownership is the instance's own requisition namespace"
    assert scope["marked_absent"] == 1


def test_a_failed_source_marks_nothing_absent_and_keeps_last_known_good(tmp_path):
    """The roadmap line, verbatim: failed sources retain last-known-good records."""
    connect = make_connect(tmp_path)
    healthy = FakeAdapter("board", instances=("acme",), body=fast(3))
    first = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(healthy)))

    broken = FakeAdapter("board", instances=("acme",), body=permanent_always())
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(broken)))

    assert second.target("board:acme").status == "failed"
    assert absent_requisitions(connect) == set()
    # Last-known-good means exactly this: the observation still names the run that
    # actually made it, not the run that failed to.
    for row in presence_by_requisition(connect).values():
        assert row["last_seen_run_uid"] == first.run_uid
        assert row["absent_since"] is None
    report = presence_report(connect, second.run_uid)
    assert report is not None, "a partial run still runs the pass; it just licences nothing"
    assert report["licensed_sources"] == 0
    assert report["marked_absent"] == 0


def test_a_timed_out_source_marks_nothing_absent(tmp_path):
    """A hanging board delivers zero records, which is indistinguishable from an
    empty board by row count alone. The status is what separates them."""
    connect = make_connect(tmp_path)
    healthy = FakeAdapter("board", instances=("acme",), body=fast(3))
    first = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(healthy)))

    stuck = FakeAdapter(
        "board",
        instances=("acme",),
        body=hanging(),
        descriptor=descriptor_for("board", deadline=0.05),
    )
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(stuck)))

    assert second.target("board:acme").status == "timeout"
    assert scalar(
        connect, "SELECT COUNT(*) FROM run_postings WHERE run_uid=?", (second.run_uid,)
    ) == 0
    assert absent_requisitions(connect) == set()
    assert presence_report(connect, second.run_uid)["licensed_sources"] == 0
    assert all(
        row["last_seen_run_uid"] == first.run_uid
        for row in presence_by_requisition(connect).values()
    )


def test_a_partial_scope_success_never_marks_anything_absent(tmp_path):
    """The run succeeded. The source succeeded. It still licences nothing, because a
    keyword search not matching a posting proves nothing about that posting."""
    connect = make_connect(tmp_path)
    descriptor = partial_descriptor("search")

    wide = FakeAdapter(
        "search",
        instances=("bay-area",),
        body=emitting_indices({"bay-area": (0, 1, 2)}),
        descriptor=descriptor,
        inventory_scope=InventoryScope.PARTIAL,
    )
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(wide)))

    narrow = FakeAdapter(
        "search",
        instances=("bay-area",),
        body=emitting_indices({"bay-area": (0,)}),
        descriptor=descriptor,
        inventory_scope=InventoryScope.PARTIAL,
    )
    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(narrow)))

    assert result.status == "succeeded"
    assert result.target("search:bay-area").status == "succeeded"
    assert absent_requisitions(connect) == set()
    report = presence_report(connect, result.run_uid)
    assert report["licensed_sources"] == 0
    assert report["seen"] == 1


def test_a_retry_marks_absence_from_the_whole_inventory_the_winning_attempt_delivered(
    tmp_path,
):
    """Regression against the 2.3 blocker, stated as its consequence.

    Attempt 1 delivers three of the board and fails; attempt 2 redelivers five and
    succeeds. The licensed inventory must be all five — if it were only the two rows
    attempt 2 itself inserted, the three attempt 1 had already written would read as
    unseen and three live jobs would be marked absent.
    """
    connect = make_connect(tmp_path)
    yesterday = FakeAdapter("flaky", instances=("board",), body=fast(6))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(yesterday)))

    today = FakeAdapter("flaky", instances=("board",), body=transient_then(count=5, before=3))
    result = run(
        scheduler(connect, batch_size=1, flush_interval_seconds=0.0).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(today)
        )
    )

    assert result.target("flaky:board").status == "succeeded"
    assert [r["status"] for r in rows(
        connect,
        "SELECT status FROM source_runs WHERE run_uid=? ORDER BY attempt",
        (result.run_uid,),
    )] == ["failed", "succeeded"]

    report = presence_report(connect, result.run_uid)
    scope = report["sources"][0]
    assert scope["source_run_id"] == attempt_id(connect, result.run_uid, "flaky:board")
    # The mandated join, asserted directly: the winning attempt owns all five rows.
    assert scope["inventory"] == 5
    assert scope["owned"] == 6
    assert scope["marked_absent"] == 1
    assert absent_requisitions(connect) == {("flaky:board", "5")}


# --------------------------------------------------------------------------- #
# Positive observation outranks inference
# --------------------------------------------------------------------------- #
def test_an_aggregator_never_marks_anything_absent_even_for_postings_it_aliases(tmp_path):
    """The aggregator resolves one of the board's postings by URL and succeeds while
    listing only that one. It must not retire the board's other requisition, and it
    must still count as having SEEN the one it did list."""
    connect = make_connect(tmp_path)
    board = FakeAdapter("board", instances=("acme",), body=emitting_indices({"acme": (0, 1)}))
    first = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(board)))

    mirrored = "https://board.example/acme/0"
    aggregator = FakeAdapter(
        "aggregator",
        instances=("indeed",),
        body=emitting([{"title": "Support Engineer 0", "company": "Acme", "url": mirrored}]),
        descriptor=partial_descriptor("aggregator"),
        inventory_scope=InventoryScope.PARTIAL,
    )
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(aggregator)))

    assert second.status == "succeeded"
    # It really did resolve onto the board's posting rather than minting its own.
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 2
    assert absent_requisitions(connect) == set()
    state = presence_by_requisition(connect)
    assert state[("board:acme", "0")]["last_seen_run_uid"] == second.run_uid
    assert state[("board:acme", "1")]["last_seen_run_uid"] == first.run_uid
    assert presence_report(connect, second.run_uid)["licensed_sources"] == 0


def test_a_manual_import_never_marks_anything_absent(tmp_path):
    """Manual import is a scraper whose transport already ran, and it is PARTIAL by
    the contract's own reasoning: rows arriving out of band say nothing about rows
    that did not."""
    connect = make_connect(tmp_path)
    board = FakeAdapter("board", instances=("acme",), body=emitting_indices({"acme": (0, 1, 2)}))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(board)))

    imported = FakeAdapter(
        "manual",
        instances=("dice",),
        body=emitting(
            [{"title": "Support Engineer", "company": "Acme", "url": "https://dice.example/9"}]
        ),
        descriptor=partial_descriptor("manual", category=SourceCategory.MANUAL),
        inventory_scope=InventoryScope.PARTIAL,
    )
    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(imported)))

    assert result.status == "succeeded"
    assert absent_requisitions(connect) == set()
    assert presence_report(connect, result.run_uid)["licensed_sources"] == 0


def test_a_positive_observation_in_the_same_run_outranks_an_absence_inference(tmp_path):
    """The membership-attribution hole, and the guard that closes it.

    `write_records` will not move a `run_postings` row across sources. So when an
    aggregator resolves a board's posting by URL and inserts that row FIRST, the row
    keeps the aggregator's `source_run_id` even though the board redelivers the same
    posting moments later. The board's attempt-scoped inventory is then genuinely
    short by a posting the board genuinely enumerated — and marking it absent would
    delete a live job on the strength of a bookkeeping detail.
    """
    connect = make_connect(tmp_path)
    board_body = emitting_indices({"acme": (0, 1)})
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=board_body)),
        )
    )

    mirrored = "https://board.example/acme/0"
    aggregator = FakeAdapter(
        "aggregator",
        instances=("indeed",),
        body=emitting([{"title": "Support Engineer 0", "company": "Acme", "url": mirrored}]),
        descriptor=partial_descriptor("aggregator"),
        inventory_scope=InventoryScope.PARTIAL,
    )
    board = FakeAdapter("board", instances=("acme",), body=board_body)
    # Serialised with the aggregator first, which is the order that produces the hole.
    result = run(
        scheduler(connect, max_concurrent_targets=1).run(
            kind=RunKind.FULL_DIRECT, plan=plan_of(aggregator, board)
        )
    )

    assert result.status == "succeeded"
    board_attempt = attempt_id(connect, result.run_uid, "board:acme")
    # The hole is real: the board enumerated two postings and owns one membership row.
    assert scalar(
        connect,
        "SELECT COUNT(*) FROM run_postings WHERE run_uid=? AND source_run_id=?",
        (result.run_uid, board_attempt),
    ) == 1

    assert absent_requisitions(connect) == set()
    scope = next(
        s for s in presence_report(connect, result.run_uid)["sources"]
        if s["source"] == "board:acme"
    )
    assert scope["inventory"] == 1
    assert scope["marked_absent"] == 0
    # The guard is measured, not merely asserted: this is the count of postings it
    # saved from a licence that would otherwise have retired them.
    assert scope["retained_positively_observed"] == 1


# --------------------------------------------------------------------------- #
# Reversibility and evidence
# --------------------------------------------------------------------------- #
def test_a_posting_that_returns_is_present_again_with_both_transitions_evidenced(tmp_path):
    connect = make_connect(tmp_path)
    full = FakeAdapter("board", instances=("acme",), body=fast(3))
    first = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(full)))

    short = FakeAdapter("board", instances=("acme",), body=fast(2))
    second = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(short)))

    gone = presence_by_requisition(connect)[("board:acme", "2")]
    assert gone["absent_since"] is not None
    assert gone["absent_run_uid"] == second.run_uid
    assert gone["returned_at"] is None
    assert gone["last_seen_run_uid"] == first.run_uid
    absent_attempt = gone["absent_source_run_id"]

    back = FakeAdapter("board", instances=("acme",), body=fast(3))
    third = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(back)))

    returned = presence_by_requisition(connect)[("board:acme", "2")]
    assert returned["absent_since"] is None, "a redelivered posting is present again"
    assert returned["returned_at"] is not None
    assert returned["last_seen_run_uid"] == third.run_uid
    # The absence that happened is still described. Clearing it would make the return
    # unfalsifiable: nothing left would say the posting had ever gone.
    assert returned["absent_run_uid"] == second.run_uid
    assert returned["absent_source_run_id"] == absent_attempt
    assert presence_report(connect, third.run_uid)["returned"] == 1
    assert absent_requisitions(connect) == set()


def test_absence_is_a_marking_and_never_removes_a_row(tmp_path):
    connect = make_connect(tmp_path)
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(4))),
        )
    )
    before = row_census(connect)

    result = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(1))),
        )
    )

    assert len(absent_requisitions(connect)) == 3
    after = row_census(connect)
    for table, count in before.items():
        assert after[table] >= count, f"{table} lost rows to an absence marking"
    assert after["postings"] == before["postings"]
    assert after["posting_aliases"] == before["posting_aliases"]
    # The absent postings keep their membership history from the run that saw them.
    assert scalar(
        connect, "SELECT COUNT(*) FROM run_postings WHERE run_uid=?", (result.run_uid,)
    ) == 1
    assert after["run_postings"] == before["run_postings"] + 1


def test_the_presence_pass_leaves_its_own_run_events(tmp_path):
    connect = make_connect(tmp_path)
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(3))),
        )
    )
    result = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(2))),
        )
    )

    events = [
        r["event_type"]
        for r in rows(
            connect,
            "SELECT event_type FROM run_events WHERE run_uid=? ORDER BY sequence",
            (result.run_uid,),
        )
    ]
    assert "run.presence_refreshed" in events
    assert "source.absence_marked" in events
    # The pass belongs to this run's evidence, so it lands before the run settles.
    assert events.index("source.absence_marked") < events.index("run.succeeded")


# --------------------------------------------------------------------------- #
# Paths that must not run the pass at all
# --------------------------------------------------------------------------- #
def test_a_cancelled_run_runs_no_presence_pass(tmp_path):
    """Cancellation stops targets mid-enumeration. A COMPLETE target that happened to
    settle first must not get to retire an instance on the strength of a run whose
    remaining evidence was never collected."""
    connect = make_connect(tmp_path)
    full = FakeAdapter("board", instances=("acme",), body=fast(3))
    first = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(full)))
    before = presence_by_requisition(connect)

    async def scenario():
        sched = scheduler(connect)
        handle = sched.start(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(0))),
        )
        handle.cancel()
        return await handle.wait()

    cancelled = run(scenario())

    assert cancelled.status == "cancelled"
    assert presence_report(connect, cancelled.run_uid) is None, (
        "None distinguishes 'the pass did not run' from 'it ran and marked nothing'"
    )
    assert absent_requisitions(connect) == set()
    after = presence_by_requisition(connect)
    assert {k: v["last_seen_run_uid"] for k, v in after.items()} == {
        k: v["last_seen_run_uid"] for k, v in before.items()
    }
    assert all(v["last_seen_run_uid"] == first.run_uid for v in after.values())


def test_an_interrupted_run_leaves_presence_untouched_until_it_is_resumed(tmp_path):
    """A process that dies mid-run never reaches the pass. Recovery marks its attempts
    interrupted, and interrupted attempts licence nothing — so the corpus is exactly
    where the last completed run left it."""
    connect = make_connect(tmp_path)
    first = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(3))),
        )
    )

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        runstore.create_pipeline_run(
            conn,
            run_uid="crashed",
            kind=str(RunKind.FULL_DIRECT),
            requested_at="2026-08-03T00:00:00+00:00",
            started_at="2026-08-03T00:00:00+00:00",
        )
        runstore.create_source_run(
            conn,
            source_run_id="crashed-0",
            run_uid="crashed",
            source="board:acme",
            attempt=1,
            requested_at="2026-08-03T00:00:00+00:00",
            started_at="2026-08-03T00:00:00+00:00",
            inventory_scope="complete",
        )
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        runstore.recover_orphans(conn)
        report = runstore.apply_run_presence(
            conn, run_uid="crashed", at="2026-08-03T01:00:00+00:00"
        )
        conn.commit()
    finally:
        conn.close()

    assert report["licensed_sources"] == 0
    assert report["marked_absent"] == 0
    assert absent_requisitions(connect) == set()
    assert all(
        row["last_seen_run_uid"] == first.run_uid
        for row in presence_by_requisition(connect).values()
    )


# --------------------------------------------------------------------------- #
# Idempotency
# --------------------------------------------------------------------------- #
def test_running_the_presence_pass_again_changes_nothing(tmp_path):
    """The scheduler runs the pass once per run, but a resume, a restart, or a Phase 4
    operator action could run it again over the same run. Nothing may double-mark, and
    nothing may flip — in particular `absent_since` must keep naming the instant the
    posting actually went missing rather than the instant of the latest rerun."""
    connect = make_connect(tmp_path)
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(3))),
        )
    )
    result = run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(1))),
        )
    )
    settled = presence_by_requisition(connect)
    assert len(absent_requisitions(connect)) == 2

    reports = []
    conn = connect()
    try:
        for stamp in ("2099-01-01T00:00:00+00:00", "2099-01-02T00:00:00+00:00"):
            conn.execute("BEGIN IMMEDIATE")
            reports.append(
                runstore.apply_run_presence(conn, run_uid=result.run_uid, at=stamp)
            )
            conn.commit()
    finally:
        conn.close()

    assert [r["marked_absent"] for r in reports] == [0, 0]
    assert [r["returned"] for r in reports] == [0, 0]
    assert presence_by_requisition(connect) == settled


# --------------------------------------------------------------------------- #
# Degraded freshness (the Phase 4 data contract)
# --------------------------------------------------------------------------- #
def test_freshness_reports_degraded_sources_from_source_run_evidence_alone(tmp_path):
    connect = make_connect(tmp_path)
    healthy = FakeAdapter("good", instances=("board",), body=fast(2))
    broken = FakeAdapter("bad", instances=("board",), body=fast(2))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(healthy, broken)))

    for _ in range(2):
        run(
            scheduler(connect).run(
                kind=RunKind.FULL_DIRECT,
                plan=plan_of(
                    FakeAdapter("good", instances=("board",), body=fast(2)),
                    FakeAdapter("bad", instances=("board",), body=permanent_always()),
                ),
            )
        )

    conn = connect()
    try:
        freshness = {row["source"]: row for row in source_instance_freshness(conn)}
    finally:
        conn.close()

    good = freshness["good:board"]
    assert good["consecutive_failed_runs"] == 0
    assert good["runs_observed"] == 3
    assert good["licenses_absence"] is True
    assert good["stale"] is False
    assert good["last_attempt_status"] == "succeeded"

    bad = freshness["bad:board"]
    assert bad["consecutive_failed_runs"] == 2
    assert bad["last_attempt_status"] == "failed"
    assert bad["licenses_absence"] is False, "a degraded source can never retire a posting"
    assert bad["stale"] is True
    # It DID succeed once, and that success is still on the record with its timestamp.
    assert bad["last_success_at"] is not None
    assert bad["last_complete_success_at"] == bad["last_success_at"]
    assert bad["last_attempt_at"] > bad["last_success_at"]


def test_freshness_counts_a_retried_run_as_one_good_run_not_a_failure(tmp_path):
    """A source that failed once and succeeded on its retry had a good run. Counting
    attempts instead of runs would show every retried board as permanently degraded."""
    connect = make_connect(tmp_path)
    flaky = FakeAdapter("flaky", instances=("board",), body=transient_then(count=2))
    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(flaky)))

    assert [r["status"] for r in rows(
        connect, "SELECT status FROM source_runs WHERE run_uid=? ORDER BY attempt",
        (result.run_uid,),
    )] == ["failed", "succeeded"]

    conn = connect()
    try:
        freshness = source_instance_freshness(conn)
    finally:
        conn.close()

    assert len(freshness) == 1
    assert freshness[0]["consecutive_failed_runs"] == 0
    assert freshness[0]["runs_observed"] == 1
    assert freshness[0]["licenses_absence"] is True
    assert freshness[0]["stale"] is False


def test_freshness_never_reports_a_partial_source_as_licensing_absence(tmp_path):
    connect = make_connect(tmp_path)
    search = FakeAdapter(
        "search",
        instances=("bay-area",),
        body=fast(2),
        descriptor=partial_descriptor("search"),
        inventory_scope=InventoryScope.PARTIAL,
    )
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(search)))

    conn = connect()
    try:
        freshness = source_instance_freshness(conn)
    finally:
        conn.close()

    assert freshness[0]["source"] == "search:bay-area"
    assert freshness[0]["consecutive_failed_runs"] == 0
    assert freshness[0]["last_success_at"] is not None
    assert freshness[0]["licenses_absence"] is False


def test_freshness_calls_an_old_success_stale_even_with_no_failures(tmp_path):
    connect = make_connect(tmp_path)
    run(
        scheduler(connect).run(
            kind=RunKind.FULL_DIRECT,
            plan=plan_of(FakeAdapter("board", instances=("acme",), body=fast(1))),
        )
    )

    conn = connect()
    try:
        fresh = source_instance_freshness(conn, stale_after_seconds=3600)[0]
        aged = source_instance_freshness(
            conn, at="2099-01-01T00:00:00+00:00", stale_after_seconds=3600
        )[0]
    finally:
        conn.close()

    assert fresh["stale"] is False
    assert aged["stale"] is True
    assert aged["consecutive_failed_runs"] == 0
    assert aged["age_seconds"] > 3600
