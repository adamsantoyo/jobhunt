"""Phase 3.1: source versions, the material-change test, and dirty emission.

The roadmap line under test: "Hash normalized source records and create a posting
version only for material changes. Emit dirty posting IDs; never scan all postings
after each source."

Five rules, each of which has a test that fails if the rule is reverted:

  MINT ON CHANGE ONLY. A first observation and a material change mint a version; an
    identical re-observation mints nothing. Over-minting re-describes and re-scores
    the whole corpus every night; under-minting means a posting whose salary, title,
    or description changed is never looked at again.
  A -> B -> A RE-LINKS. `UNIQUE (posting_id, version_hash)` forbids a second row for
    content already on file, so a revert points back at the original version instead.
    The revert is still a CHANGE — content moved — and the run after it must SETTLE.
  STATE IS PER SOURCE INSTANCE. `content_hash()` includes the source, so two sources
    describing one posting can never agree on a hash. A posting's content state is
    therefore a map, one entry per observing source, and a record moves only its own
    entry. Without this, a board and its mirror take turns and the posting is dirty
    forever — the most expensive possible answer, given daily.
  DIRTY IS DERIVED, AND SELF-HEALING. The dirty set is recomputed from committed rows,
    run-scoped, on any connection, after any restart — and it is measured against the
    last run whose dirty set was CONSUMED, so a change first seen by a run that was
    cancelled is re-emitted rather than lost.
  THE LEGACY VIEW STAYS LEGACY. Nothing this phase writes may appear in `compat_jobs`
    (that lives in `test_pipeline_audit.py`, with the parity audit it protects).

Every database is created under `tmp_path` by `make_connect`. Nothing here can reach
webapp/app.db.
"""
import asyncio
import json

import pytest

from backend.sources import runstore
from backend.sources.contract import InventoryScope, RunKind
from backend.sources.scheduler import Scheduler, SchedulerConfig
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    emitting,
    fast,
    make_connect,
    plan_of,
    scalar,
)

FAST_RETRY = dict(retry_base_delay_seconds=0.01, retry_jitter=0.0)

#: One posting, described three ways. The req_id is constant, so all three are the
#: same posting by rank-0 identity and any difference is a CONTENT difference.
STATE_A = {
    "title": "Support Engineer",
    "company": "Acme",
    "url": "https://x.example/1",
    "req_id": "R-1",
    "salary_text": "$120k",
}
STATE_B = {**STATE_A, "title": "Senior Support Engineer"}
STATE_C = {**STATE_A, "salary_text": "$150k"}


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


def drive(connect, specs, *, source="src", instance="b"):
    """One scheduler run delivering exactly `specs`. Returns the RunResult."""
    adapter = FakeAdapter(source, instances=(instance,), body=emitting(specs))
    return run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))


def drive_sources(connect, deliveries, **config):
    """One run in which several named sources each deliver their own records.

    `deliveries` is `[(source_key, [spec, ...]), ...]`, and the ORDER IS SIGNIFICANT
    to the tests that use it: the cross-source cases assert that the outcome does not
    depend on which source finishes last.
    """
    adapters = [
        FakeAdapter(source, instances=("i",), body=emitting(specs))
        for source, specs in deliveries
    ]
    return run(
        scheduler(connect, **config).run(kind=RunKind.FULL_DIRECT, plan=plan_of(*adapters))
    )


def dirty(connect, run_uid):
    """The dirty set, read on a CONNECTION THIS RUN NEVER TOUCHED.

    Every test that asserts dirtiness goes through here rather than through a
    scheduler return value, because that is the property Phase 3.2/3.3 depend on:
    the answer is in the database, not in the process that wrote it.
    """
    conn = connect()
    try:
        return runstore.dirty_posting_ids(conn, run_uid)
    finally:
        conn.close()


def summary(connect, run_uid):
    conn = connect()
    try:
        return runstore.change_summary(conn, run_uid)
    finally:
        conn.close()


def report_of(connect, run_uid):
    row = rows(
        connect, "SELECT aggregate_report_json FROM pipeline_runs WHERE run_uid=?", (run_uid,)
    )[0]
    return json.loads(row["aggregate_report_json"])


def versions(connect):
    return rows(
        connect,
        "SELECT posting_version_id, posting_id, version_kind, version_hash, observed_at, "
        "source_run_id, title, salary, req_id, source, remote, posted, tier, odds, "
        "payload_json FROM posting_versions "
        "ORDER BY observed_at, posting_version_id",
    )


# --------------------------------------------------------------------------- #
# Mint on change, and only on change
# --------------------------------------------------------------------------- #
def test_a_first_observation_mints_one_version_and_is_dirty(tmp_path):
    connect = make_connect(tmp_path)

    result = drive(connect, [STATE_A])

    stored = versions(connect)
    assert len(stored) == 1
    assert stored[0]["version_kind"] == "source"
    assert stored[0]["version_hash"].startswith("sha256:")
    # A source version says what a source said; what it is worth is 3.4's judgement.
    assert stored[0]["tier"] is None and stored[0]["odds"] is None
    posting_id = scalar(connect, "SELECT posting_id FROM postings")
    assert dirty(connect, result.run_uid) == [posting_id]
    assert summary(connect, result.run_uid) == {
        "run_uid": result.run_uid,
        "observed": 1,
        "changed": 1,
        "first_seen": 1,
        "updated": 0,
        "unchanged": 0,
        "versions_created": 1,
        "legacy_membership": 0,
        "unversioned": 0,
    }


def test_an_identical_re_observation_mints_nothing_and_is_not_dirty(tmp_path):
    connect = make_connect(tmp_path)

    first = drive(connect, [STATE_A])
    second = drive(connect, [STATE_A])

    assert scalar(connect, "SELECT COUNT(*) FROM posting_versions") == 1
    assert dirty(connect, second.run_uid) == []
    counts = summary(connect, second.run_uid)
    assert (counts["observed"], counts["changed"], counts["unchanged"]) == (1, 0, 1)
    assert counts["versions_created"] == 0
    # The second run still RECORDS the observation, linked to the unchanged version —
    # "not dirty" must never mean "not seen", or absence marking would delete it.
    linked = [r["posting_version_id"] for r in rows(
        connect, "SELECT posting_version_id FROM run_postings ORDER BY recorded_at"
    )]
    assert len(linked) == 2 and linked[0] == linked[1]
    assert dirty(connect, first.run_uid), "the first run's own dirtiness is unchanged"


def test_a_changed_record_mints_a_new_version_relinks_membership_and_is_dirty(tmp_path):
    connect = make_connect(tmp_path)

    first = drive(connect, [STATE_A])
    second = drive(connect, [STATE_B])

    stored = versions(connect)
    assert [v["title"] for v in stored] == ["Support Engineer", "Senior Support Engineer"]
    assert stored[0]["posting_id"] == stored[1]["posting_id"], "one posting, two versions"
    assert dirty(connect, second.run_uid) == [stored[0]["posting_id"]]
    counts = summary(connect, second.run_uid)
    assert (counts["changed"], counts["first_seen"], counts["updated"]) == (1, 0, 1)
    assert counts["versions_created"] == 1

    membership = rows(
        connect,
        "SELECT run_uid, posting_version_id, content_hash FROM run_postings "
        "ORDER BY recorded_at",
    )
    assert membership[0]["posting_version_id"] == stored[0]["posting_version_id"]
    assert membership[1]["posting_version_id"] == stored[1]["posting_version_id"]
    assert membership[1]["content_hash"] == stored[1]["version_hash"]
    assert first.run_uid == membership[0]["run_uid"]


def test_a_revert_relinks_the_first_version_and_is_still_a_change(tmp_path):
    """A -> B -> A. The third run's content already has a version row, and
    `UNIQUE (posting_id, version_hash)` means it cannot get a second one. It must
    still be detected as a change (content moved), the membership row must point back
    at the ORIGINAL version, and that original row's own evidence — when this content
    was first observed, and by which attempt — must not be rewritten to say it was
    observed later than it was.
    """
    connect = make_connect(tmp_path)

    first = drive(connect, [STATE_A])
    second = drive(connect, [STATE_B])
    before = versions(connect)
    third = drive(connect, [STATE_A])

    after = versions(connect)
    assert len(after) == 2, "a revert may not duplicate a version row"
    assert [tuple(v) for v in after] == [tuple(v) for v in before], (
        "the reverted-to version row is evidence of its FIRST observation and is immutable"
    )

    version_a = after[0]["posting_version_id"]
    membership = rows(
        connect,
        "SELECT run_uid, posting_version_id FROM run_postings ORDER BY recorded_at",
    )
    by_run = {r["run_uid"]: r["posting_version_id"] for r in membership}
    assert by_run[first.run_uid] == version_a
    assert by_run[third.run_uid] == version_a
    assert by_run[second.run_uid] != version_a

    posting_id = after[0]["posting_id"]
    assert dirty(connect, third.run_uid) == [posting_id], "a revert is a change"
    counts = summary(connect, third.run_uid)
    assert counts["changed"] == 1
    # The gap between the two is the whole A->B->A case, made visible rather than
    # inferred: content moved, but no row was minted for it.
    assert counts["versions_created"] == 0

    # And it SETTLES. A "current version" defined by MAX(observed_at) rather than by
    # linkage would read this posting as "currently B" forever — the reverted-to row
    # keeps its original, older `observed_at` — so every later run would re-link it,
    # re-count it as changed work, and hand 3.2/3.3 a posting that never stops
    # flapping. Both halves are asserted: the derived dirty verdict AND the attempt's
    # own count of the work it thought it had to do.
    fourth = drive(connect, [STATE_A])
    assert dirty(connect, fourth.run_uid) == []
    assert scalar(connect, "SELECT COUNT(*) FROM posting_versions") == 2
    assert scalar(
        connect,
        "SELECT changed_count FROM source_runs WHERE run_uid=? AND step='fetch'",
        (fourth.run_uid,),
    ) == 0


def test_three_distinct_states_keep_one_version_each(tmp_path):
    connect = make_connect(tmp_path)

    for spec in (STATE_A, STATE_B, STATE_C, STATE_B, STATE_A):
        last = drive(connect, [spec])

    assert scalar(connect, "SELECT COUNT(*) FROM posting_versions") == 3
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1
    assert scalar(connect, "SELECT COUNT(DISTINCT version_hash) FROM posting_versions") == 3
    assert dirty(connect, last.run_uid), "the last hop is A after B: a change"


# --------------------------------------------------------------------------- #
# Linkage
# --------------------------------------------------------------------------- #
def test_every_membership_row_links_the_version_current_for_that_observation(tmp_path):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("src", instances=("a", "b"), body=fast(3))

    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))
    run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    # Phase 2 wrote NULL here. Nothing with an identity may leave it NULL now: it is
    # what "which content did we see" is answered from.
    assert scalar(
        connect, "SELECT COUNT(*) FROM run_postings WHERE posting_version_id IS NULL"
    ) == 0
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 12
    # The link is consistent with the hash beside it, and with the posting it belongs
    # to — the composite foreign key would refuse a cross-posting link, so this is
    # asserting the join, not the constraint.
    assert scalar(
        connect,
        "SELECT COUNT(*) FROM run_postings rp JOIN posting_versions v "
        "  ON v.posting_version_id = rp.posting_version_id "
        "WHERE v.version_hash <> rp.content_hash OR v.posting_id <> rp.posting_id",
    ) == 0


#: One posting, published by a board and mirrored by an aggregator at the SAME URL
#: and with byte-identical fields. `content_hash()` still differs between them,
#: because the hash includes the source — which is exactly what makes a single
#: "current version" per posting untenable.
SHARED = {"title": "Support Engineer", "company": "Acme", "url": "https://x.example/shared"}


def test_alternating_sources_settle_instead_of_flapping_forever(tmp_path):
    """A board and its mirror take turns. From the run after each has spoken once,
    nothing is dirty.

    This is the case that a per-POSTING current version cannot get right at all: the
    two sources' hashes differ by construction, so "the version linked last" flips
    every run and the posting is re-emitted to 3.2/3.3 every single day, forever, for
    no change. Per-SOURCE state makes each turn a no-op after the first.
    """
    connect = make_connect(tmp_path)

    first = drive(connect, [SHARED], source="board")
    second = drive(connect, [SHARED], source="mirror")
    third = drive(connect, [SHARED], source="board")
    fourth = drive(connect, [SHARED], source="mirror")

    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1, (
        "the fixture must resolve both sources onto ONE posting, or it proves nothing"
    )
    posting_id = scalar(connect, "SELECT posting_id FROM postings")
    assert dirty(connect, first.run_uid) == [posting_id], "first sighting"
    assert dirty(connect, second.run_uid) == [posting_id], "a new source is new content"
    assert dirty(connect, third.run_uid) == [], "the board repeating itself is not news"
    assert dirty(connect, fourth.run_uid) == [], "and neither is the mirror"
    # Two versions, one per source, and no further minting once both are on file.
    assert scalar(connect, "SELECT COUNT(*) FROM posting_versions") == 2
    assert summary(connect, fourth.run_uid)["versions_created"] == 0


def test_the_order_two_sources_finish_in_does_not_change_the_state(tmp_path):
    """Board-then-mirror and mirror-then-board must leave the same state.

    A run's membership row can only name one version, so SOMETHING has to be the last
    word — but which source that is must not decide whether the posting is dirty. The
    state map merges per source, so both orders reach the same map, and the second run
    is not dirty even though the last word changed hands.
    """
    connect = make_connect(tmp_path)

    # One target at a time, so "who finished last" is the list order and not a race.
    first = drive_sources(
        connect, [("board", [SHARED]), ("mirror", [SHARED])], max_concurrent_targets=1
    )
    second = drive_sources(
        connect, [("mirror", [SHARED]), ("board", [SHARED])], max_concurrent_targets=1
    )

    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1
    states = [r["source_state_json"] for r in rows(
        connect, "SELECT source_state_json FROM run_postings ORDER BY recorded_at"
    )]
    assert states[0] == states[1], "the merged state must not depend on delivery order"
    assert len(dirty(connect, first.run_uid)) == 1
    assert dirty(connect, second.run_uid) == [], (
        "flipping which source spoke last is not a content change"
    )
    # The two versions are both still on file and each is attributed to its own source.
    assert sorted(v["source"] for v in versions(connect)) == ["board:i", "mirror:i"]


def test_a_source_that_goes_quiet_does_not_make_a_posting_look_changed(tmp_path):
    """State is what each source LAST said, not what it said today. An aggregator that
    stops covering a posting must not, by its silence, re-dirty it."""
    connect = make_connect(tmp_path)

    drive_sources(connect, [("board", [SHARED]), ("mirror", [SHARED])],
                  max_concurrent_targets=1)
    second = drive_sources(connect, [("board", [SHARED])])
    third = drive_sources(connect, [("board", [SHARED])])

    assert dirty(connect, second.run_uid) == []
    assert dirty(connect, third.run_uid) == []
    state = json.loads(scalar(
        connect,
        "SELECT source_state_json FROM run_postings WHERE run_uid=?", (third.run_uid,)
    ))
    assert sorted(state) == ["board:i", "mirror:i"], (
        "the quiet source's entry is carried forward, not dropped"
    )


def test_two_sources_describing_one_posting_in_one_run_link_the_last_word(tmp_path):
    """A board and an aggregator resolve to one posting by URL and disagree about its
    content. One run has ONE membership row per posting, so it names the version
    current after the last delivery, and the run is dirty either way.
    """
    connect = make_connect(tmp_path)
    board = {"title": "Support Engineer", "company": "Acme", "url": "https://x.example/1"}
    mirror = {**board, "title": "Support Engineer (Remote)"}
    plan = plan_of(
        FakeAdapter("board", instances=("b",), body=emitting([board])),
        FakeAdapter("mirror", instances=("m",), body=emitting([mirror])),
    )

    result = run(scheduler(connect, max_concurrent_targets=1).run(
        kind=RunKind.FULL_DIRECT, plan=plan
    ))

    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1
    assert scalar(connect, "SELECT COUNT(*) FROM run_postings") == 1
    stored = {v["version_hash"]: v["posting_version_id"] for v in versions(connect)}
    assert len(stored) == 2, "each source's content gets its own version"
    linked = rows(connect, "SELECT posting_version_id, content_hash FROM run_postings")[0]
    assert stored[linked["content_hash"]] == linked["posting_version_id"]
    assert len(dirty(connect, result.run_uid)) == 1


# --------------------------------------------------------------------------- #
# Dirty emission: derived, run-scoped, restart-safe
# --------------------------------------------------------------------------- #
def test_dirty_ids_are_scoped_to_their_run_and_survive_a_restart(tmp_path):
    """Two runs, two answers, both read afterwards from a connection that took no
    part in either. This is the whole contract with 3.2/3.3: they may run in a later
    process, after a crash, and still learn exactly what a given run changed.
    """
    connect = make_connect(tmp_path)
    stable = {"title": "Stable", "company": "Acme", "url": "https://x.example/s", "req_id": "S"}
    moving = {"title": "Moving", "company": "Acme", "url": "https://x.example/m", "req_id": "M"}

    first = drive(connect, [stable, moving])
    second = drive(connect, [stable, {**moving, "title": "Moved"}])

    ids_first = dirty(connect, first.run_uid)
    ids_second = dirty(connect, second.run_uid)
    assert len(ids_first) == 2, "every posting is new in the first run"
    assert len(ids_second) == 1
    moving_id = scalar(
        connect,
        "SELECT posting_id FROM posting_aliases WHERE alias_kind='source_req' AND value='M'",
    )
    assert ids_second == [moving_id]
    # Run-scoped, not corpus-scoped: the first run's dirty set is not the second's,
    # and asking about an unknown run answers nothing rather than everything.
    assert set(ids_second) < set(ids_first)
    assert dirty(connect, "no-such-run") == []


def test_a_change_a_cancelled_run_saw_is_re_emitted_to_the_next_good_run(tmp_path):
    """A run that dies after its batches commit must not swallow the change it saw.

    The batches of a cancelled or failed run are already committed — the writer
    commits as it goes, which is the whole point of the batching — so the new content
    IS on disk. If "previous observation" meant any prior row, the next healthy run
    would compare against that dead run's state, find nothing moved, and never emit
    the change: 3.2/3.3 would never describe or score it, and nothing anywhere would
    report an error. Measuring against the last CONSUMED run makes the emission
    self-healing.

    The cancellation is applied to the run's terminal status directly rather than by
    racing a real cancel, because what is under test is what a later run concludes
    from the rows on disk, and only a run whose batches DID commit can exercise it.
    """
    connect = make_connect(tmp_path)

    first = drive(connect, [STATE_A])
    interrupted = drive(connect, [STATE_B])
    conn = connect()
    try:
        conn.execute(
            "UPDATE pipeline_runs SET status='cancelled' WHERE run_uid=?", (interrupted.run_uid,)
        )
        conn.commit()
    finally:
        conn.close()
    third = drive(connect, [STATE_B])
    fourth = drive(connect, [STATE_B])

    posting_id = scalar(connect, "SELECT posting_id FROM postings")
    assert dirty(connect, first.run_uid) == [posting_id]
    assert dirty(connect, interrupted.run_uid) == [posting_id], (
        "the cancelled run's own summary is still honest about what it saw"
    )
    assert dirty(connect, third.run_uid) == [posting_id], (
        "B was never consumed, so the next completed run must emit it"
    )
    assert dirty(connect, fourth.run_uid) == [], "and then it settles"
    # No extra version rows were minted along the way: B already had one.
    assert scalar(connect, "SELECT COUNT(*) FROM posting_versions") == 2


def _traced(conn, fn):
    """Every SQL statement `fn` issues, with its parameters already substituted.

    Captured rather than copied into the test: a plan assertion against a
    hand-transcribed query proves something about the transcription, and drifts the
    first time the real statement changes.
    """
    seen: list[str] = []
    conn.set_trace_callback(seen.append)
    try:
        fn()
    finally:
        conn.set_trace_callback(None)
    return seen


def test_every_change_query_is_index_keyed_and_never_scans_a_table(tmp_path):
    """The roadmap's "never scan all postings after each source", asserted against the
    query planner rather than a stopwatch, for every statement on the change path: the
    per-batch state lookup, the dirty query, and all three statements `change_summary`
    issues. A single full scan of `run_postings` or `posting_versions` costs the whole
    corpus on every run, and it stays invisible in a toy database until the day it is
    not.

    Two things make this assertion mean something. The database is big enough that a
    scan is genuinely the expensive option — on a two-row table SQLite scans whatever
    it likes and any plan assertion is noise. And `ANALYZE` runs first: several of
    these plans change once `sqlite_stat1` exists (an `IN (subquery)` that seeks on a
    statistics-free database becomes a bloom-filtered full scan with statistics), so a
    query that is only fast without statistics is a regression waiting for whoever
    runs ANALYZE first.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        _write_runs(conn, runs=2, postings=600)
        run_uid = "run-1"
        # The real statements, captured with their parameters already substituted, so
        # this cannot drift from what the code actually runs.
        statements = _traced(conn, lambda: (
            runstore.change_summary(conn, run_uid),
            runstore.dirty_posting_ids(conn, run_uid),
            runstore.write_records(
                conn, run_uid=run_uid, source_run_id="attempt-1-0",
                records=_records(5, source_key="src0"),
                recorded_at="2026-08-02T02:00:00+00:00",
            ),
        ))
        conn.rollback()
        conn.execute("ANALYZE")
        plans = {
            sql: [row["detail"] for row in conn.execute("EXPLAIN QUERY PLAN " + sql)]
            for sql in statements
            if sql.lstrip().upper().startswith("SELECT")
        }
    finally:
        conn.close()

    # The state lookup, the dirty query, and the three summary statements.
    assert len(plans) >= 5, sorted(plans)
    for sql, details in plans.items():
        # SQLite names the ALIAS, not the table ("SEARCH rp ..."), so this asserts on
        # the verb. A "SCAN <table>" line means some table is read end to end; a
        # "SCAN (subquery-N)" line is the co-routine feeding a window function, and is
        # bounded by the rows that fed it.
        scans = [d for d in details if d.startswith("SCAN ") and "subquery" not in d]
        assert not scans, (sql, details)
        searches = [d for d in details if d.startswith("SEARCH ")]
        assert searches, (sql, details)
        assert all("USING INDEX" in d or "USING PRIMARY KEY" in d for d in searches), (
            sql, details
        )

    # And specifically: counting the versions a run minted rides migration 16's index
    # rather than the bloom-filtered full scan its `IN (subquery)` form plans.
    minted = [d for sql, d in plans.items() if "posting_versions pv" in sql]
    assert minted and any("idx_posting_versions_source_run" in d for d in minted[0]), minted


def test_the_run_report_says_how_many_postings_changed(tmp_path):
    """A human reading a run has to be able to see "N changed" without a query, the
    way 2.5 surfaced `skipped_not_due`. Persisted in `aggregate_report_json`, so it
    survives the process that produced it.
    """
    connect = make_connect(tmp_path)
    first = drive(connect, [STATE_A])
    second = drive(connect, [STATE_B])
    third = drive(connect, [STATE_B])

    assert report_of(connect, first.run_uid)["changed"] == summary(connect, first.run_uid)
    assert report_of(connect, second.run_uid)["changed"]["updated"] == 1
    assert report_of(connect, third.run_uid)["changed"] == {
        "run_uid": third.run_uid,
        "observed": 1,
        "changed": 0,
        "first_seen": 0,
        "updated": 0,
        "unchanged": 1,
        "versions_created": 0,
        "legacy_membership": 0,
        "unversioned": 0,
    }
    # The event is emitted for the same reason the presence pass emits one: Phase 4
    # replays `run_events` and must not have to re-derive this.
    payloads = [
        json.loads(r["payload_json"])
        for r in rows(
            connect,
            "SELECT payload_json FROM run_events WHERE run_uid=? AND event_type=?",
            (second.run_uid, "run.changes_summarized"),
        )
    ]
    assert len(payloads) == 1 and payloads[0]["changed"] == 1


def test_each_attempt_records_its_own_changed_count(tmp_path):
    """`source_runs.changed_count` has been NULL since migration 6. It is per-attempt
    evidence, accumulated in the same transactions as the records themselves, so a
    settled attempt still says what it changed after a restart.
    """
    connect = make_connect(tmp_path)
    drive(connect, [STATE_A])
    second = drive(connect, [STATE_B])

    counts = rows(
        connect,
        "SELECT run_uid, changed_count, accepted_count FROM source_runs "
        "WHERE step='fetch' ORDER BY requested_at",
    )
    assert [r["changed_count"] for r in counts] == [1, 1]
    third = drive(connect, [STATE_B])
    settled = rows(
        connect,
        "SELECT changed_count FROM source_runs WHERE run_uid=? AND step='fetch'",
        (third.run_uid,),
    )
    assert [r["changed_count"] for r in settled] == [0], (
        "an unchanged re-observation must record zero, not NULL and not one"
    )


# --------------------------------------------------------------------------- #
# The write path stays batched, and stays idempotent
# --------------------------------------------------------------------------- #
def _prepare_attempt(conn, *, run_uid="run-1", source_run_id="attempt-1", source="src:b"):
    runstore.create_pipeline_run(
        conn, run_uid=run_uid, kind="full_direct", requested_at="2026-08-04T00:00:00+00:00"
    )
    runstore.create_source_run(
        conn,
        source_run_id=source_run_id,
        run_uid=run_uid,
        source=source,
        attempt=1,
        inventory_scope="complete",
    )
    return run_uid, source_run_id


def _records(count, *, salary="$120k", source_key="src"):
    from backend.sources.contract import NormalizedPosting

    return [
        NormalizedPosting(
            source_key=source_key,
            instance_key="b",
            title=f"Support Engineer {n}",
            company="Acme",
            url=f"https://{source_key}.example/{n}",
            req_id=f"R-{n}",
            salary_text=salary,
        )
        for n in range(count)
    ]


def _write_runs(conn, *, runs=2, postings=600, sources=8):
    """Drive `runs` finished runs straight through `runstore`, for the tests that need
    a database big enough — and shaped enough like a real one — to plan against.

    `sources` matters as much as the row count: a real run fans out over hundreds of
    per-instance attempts, so `posting_versions.source_run_id` is selective and an
    index on it is worth using. A fixture that put every row under one attempt would
    make SQLite prefer a table scan for perfectly sound reasons and the plan assertions
    would be measuring the fixture rather than the query.
    """
    per_source = max(1, postings // sources)
    for index in range(runs):
        run_uid = f"run-{index}"
        runstore.create_pipeline_run(
            conn, run_uid=run_uid, kind="full_direct",
            requested_at=f"2026-08-{index + 1:02d}T00:00:00+00:00",
        )
        for source in range(sources):
            attempt = f"attempt-{index}-{source}"
            runstore.create_source_run(
                conn, source_run_id=attempt, run_uid=run_uid,
                source=f"src{source}:b", attempt=1,
            )
            runstore.write_records(
                conn, run_uid=run_uid, source_run_id=attempt,
                records=_records(per_source, source_key=f"src{source}"),
                recorded_at=f"2026-08-{index + 1:02d}T01:00:00+00:00",
            )
        conn.execute(
            "UPDATE pipeline_runs SET status='succeeded' WHERE run_uid=?", (run_uid,)
        )
    conn.commit()


def _count_lookups(conn, fn):
    seen: list[str] = []
    conn.set_trace_callback(seen.append)
    try:
        fn()
    finally:
        conn.set_trace_callback(None)
    return [sql for sql in seen if "ROW_NUMBER" in sql]


def test_the_state_lookup_is_one_statement_per_batch(tmp_path):
    """Requirement: no per-record SELECT storm. Counted structurally — a regression
    that moves the lookup inside the per-record loop makes this fail at 40 rather than
    at 1, however fast the machine running it happens to be.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        run_uid, attempt = _prepare_attempt(conn)
        batch = _records(40)
        runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt, records=batch, recorded_at="t0"
        )
        conn.commit()

        runstore.create_source_run(
            conn, source_run_id="attempt-2", run_uid=run_uid, source="src:b", attempt=2
        )
        lookups = _count_lookups(conn, lambda: runstore.write_records(
            conn, run_uid=run_uid, source_run_id="attempt-2", records=batch, recorded_at="t1"
        ))
    finally:
        conn.close()

    assert len(lookups) == 1, f"{len(lookups)} state lookups for one 40-record batch"


def test_a_batch_larger_than_the_lookup_chunk_is_still_a_handful_of_statements(tmp_path):
    """1,000 distinct postings in one batch: three lookups, not a thousand.

    The chunking exists because SQLite bounds the number of host parameters in one
    statement, and a batch bigger than `_LOOKUP_CHUNK` is the case where a naive
    single-statement implementation would fail outright and a naive per-record one
    would be a round trip per row. Both regressions land on this assertion.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        run_uid, attempt = _prepare_attempt(conn)
        batch = _records(1000)
        runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt, records=batch, recorded_at="t0"
        )
        conn.commit()
        runstore.create_source_run(
            conn, source_run_id="attempt-2", run_uid=run_uid, source="src:b", attempt=2
        )
        lookups = _count_lookups(conn, lambda: runstore.write_records(
            conn, run_uid=run_uid, source_run_id="attempt-2", records=batch, recorded_at="t1"
        ))
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM posting_versions").fetchone()[0] == 1000
        assert conn.execute(
            "SELECT COUNT(*) FROM run_postings WHERE source_state_json IS NULL"
        ).fetchone()[0] == 0
    finally:
        conn.close()

    expected = -(-1000 // runstore._LOOKUP_CHUNK)  # ceil, without importing math
    assert len(lookups) == expected == 3, f"{len(lookups)} lookups for 1,000 postings"


def test_a_replayed_batch_changes_nothing_the_second_time(tmp_path):
    """Delivering the same batch twice — checkpoint replay, search-term fan-out, a
    busy-database retry that re-applies the whole batch — must be a no-op for
    versions as it already is for postings and membership.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        run_uid, attempt = _prepare_attempt(conn)
        batch = _records(5)
        first = runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt, records=batch, recorded_at="t0"
        )
        second = runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt, records=batch, recorded_at="t0"
        )
        conn.commit()

        assert (first.changed, first.accepted) == (5, 5)
        assert (second.changed, second.accepted) == (0, 0)
        assert second.duplicates == 5
        assert conn.execute("SELECT COUNT(*) FROM posting_versions").fetchone()[0] == 5
        assert conn.execute("SELECT COUNT(*) FROM run_postings").fetchone()[0] == 5
        assert runstore.change_summary(conn, run_uid)["changed"] == 5
    finally:
        conn.close()


def test_one_posting_flapping_inside_one_batch_counts_as_one_change(tmp_path):
    """A -> B -> A within a single batch moved ONE posting, not three.

    Counting records rather than postings inflates every count built on it — the
    attempt's `changed_count`, the run's report, and any future rate-limiting on
    "how much changed today" — by however many times a fan-out happens to re-deliver
    the same posting. The orphan B version row is left on file deliberately: it is
    content a source really did report, and `UNIQUE (posting_id, version_hash)` means
    it costs one row however often it recurs.
    """
    from backend.sources.contract import NormalizedPosting

    connect = make_connect(tmp_path)
    conn = connect()
    try:
        run_uid, attempt = _prepare_attempt(conn)
        def record(title):
            return NormalizedPosting(
                source_key="src", instance_key="b", title=title, company="Acme",
                url="https://x.example/1", req_id="R-1",
            )
        outcome = runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt,
            records=[record("A"), record("B"), record("A")], recorded_at="t0",
        )
        conn.commit()

        assert outcome.received == 3
        assert outcome.changed == 1, "one posting moved, whatever the record count"
        assert conn.execute("SELECT COUNT(*) FROM posting_versions").fetchone()[0] == 2
        # The row that survives names the content the batch ended on.
        linked, state = conn.execute(
            "SELECT posting_version_id, source_state_json FROM run_postings"
        ).fetchone()
        assert json.loads(state) == {"src:b": linked}
        assert conn.execute(
            "SELECT title FROM posting_versions WHERE posting_version_id=?", (linked,)
        ).fetchone()[0] == "A"
    finally:
        conn.close()


def test_a_legacy_membership_row_is_counted_as_legacy_not_as_unchanged(tmp_path):
    """Migration 11's rows describe an import, not a source observation.

    They carry no per-source state and link a 'legacy-current' version, so they can be
    neither changed nor unchanged in this phase's sense. Counting them as `unchanged`
    would have the run report claim it observed content that had not moved, when it
    observed nothing of the kind.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        run_uid, attempt = _prepare_attempt(conn)
        runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt, records=_records(2),
            recorded_at="t0",
        )
        conn.execute(
            "INSERT INTO postings (posting_id,identity_status,first_seen_at,created_at) "
            "VALUES ('p-legacy','active','t0','t0')"
        )
        conn.execute(
            "INSERT INTO posting_versions (posting_version_id,posting_id,version_kind,"
            "version_hash,observed_at,payload_json) "
            "VALUES ('v-legacy','p-legacy','legacy-current','h','t0','{}')"
        )
        conn.execute(
            "INSERT INTO run_postings (run_uid,posting_id,posting_version_id,present,"
            "first_seen_in_run,recorded_at,membership_kind) "
            "VALUES (?,'p-legacy','v-legacy',1,0,'t0','current-only')",
            (run_uid,),
        )
        conn.commit()

        counts = runstore.change_summary(conn, run_uid)
        assert counts["observed"] == 3
        assert counts["changed"] == 2
        assert counts["legacy_membership"] == 1
        assert counts["unchanged"] == 0
        assert counts["observed"] == (
            counts["changed"] + counts["unchanged"] + counts["legacy_membership"]
        ), "the three buckets must partition what the run observed"
        assert "p-legacy" not in runstore.dirty_posting_ids(conn, run_uid)
    finally:
        conn.close()


def test_dirty_ids_chunk_with_a_cursor(tmp_path):
    """`limit` alone re-returns the same first N — this is a query, not a queue — so
    `after` is what a consumer chunks with. Pinned because 3.2/3.3 will chunk tens of
    thousands of ids and a cursor that silently did nothing would loop forever."""
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        run_uid, attempt = _prepare_attempt(conn)
        runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt, records=_records(10),
            recorded_at="t0",
        )
        conn.commit()

        everything = runstore.dirty_posting_ids(conn, run_uid)
        assert len(everything) == 10 == len(set(everything))
        assert everything == sorted(everything)
        assert runstore.dirty_posting_ids(conn, run_uid, limit=4) == everything[:4]
        assert runstore.dirty_posting_ids(conn, run_uid, limit=4) == everything[:4], (
            "without a cursor the same page comes back, and the docstring says so"
        )
        walked: list[str] = []
        cursor = None
        while page := runstore.dirty_posting_ids(conn, run_uid, limit=4, after=cursor):
            walked.extend(page)
            cursor = page[-1]
        assert walked == everything
    finally:
        conn.close()


def test_a_url_only_record_versions_under_its_url_identity(tmp_path):
    """Aggregators routinely publish no requisition id, so their records are
    identified by URL alone (rank 1). Versioning must follow whatever identity the
    resolver reached — a posting known only by its URL still has content, and content
    that moves under it is still a change.
    """
    connect = make_connect(tmp_path)
    listing = {"title": "Support Engineer", "company": "Acme", "url": "https://x.example/1"}

    first = drive(connect, [listing])
    second = drive(connect, [{**listing, "salary_text": "$150k"}])

    assert scalar(
        connect, "SELECT COUNT(*) FROM posting_aliases WHERE alias_kind='source_req'"
    ) == 0, "the fixture must have no requisition identity, or it proves nothing"
    assert scalar(connect, "SELECT COUNT(*) FROM postings") == 1
    stored = versions(connect)
    assert [v["salary"] for v in stored] == ["", "$150k"]
    assert stored[0]["req_id"] is None
    assert len(dirty(connect, first.run_uid)) == 1
    assert len(dirty(connect, second.run_uid)) == 1


def test_the_payload_records_the_exact_hash_input(tmp_path):
    """The stored version has to be re-verifiable: its payload carries the canonical
    fields the hash was taken over, in the frozen order, so a stored row can be
    checked against `content_hash()` without re-fetching anything.
    """
    connect = make_connect(tmp_path)
    drive(connect, [STATE_A])

    stored = versions(connect)[0]
    payload = json.loads(stored["payload_json"])
    assert payload["content_hash"] == stored["version_hash"]
    assert payload["canonical"]["title"] == "Support Engineer"
    assert payload["canonical"]["req_id"] == "R-1"
    assert payload["source"]["namespace"] == "src:b"
    assert stored["source_run_id"] is not None
    # The body is Phase 3.2's table, not this one's; the digest in the canonical
    # fields is what makes a rewritten description a material change.
    assert "description" not in payload["canonical"]
    assert payload["canonical"]["description_digest"] == ""


# --------------------------------------------------------------------------- #
# Phase 2 invariants that versioning must not have moved
# --------------------------------------------------------------------------- #
def test_absence_marking_still_works_and_a_returning_posting_is_dirty_only_if_changed(tmp_path):
    """The presence pass is untouched by versioning: a posting that stops being
    enumerated is still marked absent, and one that comes back unchanged returns to
    present WITHOUT being dirty — its content never moved, so nothing downstream
    needs to re-examine it.
    """
    connect = make_connect(tmp_path)
    keep = {"title": "Keeper", "company": "Acme", "url": "https://x.example/k", "req_id": "K"}
    goes = {"title": "Goer", "company": "Acme", "url": "https://x.example/g", "req_id": "G"}

    drive(connect, [keep, goes])
    second = drive(connect, [keep])
    absent = rows(
        connect, "SELECT posting_id, absent_since FROM postings WHERE absent_since IS NOT NULL"
    )
    assert len(absent) == 1
    assert dirty(connect, second.run_uid) == []

    third = drive(connect, [keep, goes])
    assert scalar(connect, "SELECT COUNT(*) FROM postings WHERE absent_since IS NOT NULL") == 0
    assert scalar(connect, "SELECT COUNT(*) FROM postings WHERE returned_at IS NOT NULL") == 1
    assert dirty(connect, third.run_uid) == [], (
        "a posting that returns with identical content has not changed"
    )
    assert report_of(connect, third.run_uid)["presence"]["returned"] == 1


def test_a_partial_source_still_versions_what_it_delivered(tmp_path):
    """Inventory scope governs ABSENCE, never content. A PARTIAL aggregator's
    delivery is a positive observation and versions exactly like a complete one.
    """
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "agg",
        instances=("a",),
        body=emitting([STATE_A]),
        descriptor=descriptor_for("agg", inventory_scope=InventoryScope.PARTIAL),
        inventory_scope=InventoryScope.PARTIAL,
    )

    result = run(scheduler(connect).run(kind=RunKind.FULL_DIRECT, plan=plan_of(adapter)))

    assert scalar(connect, "SELECT COUNT(*) FROM posting_versions") == 1
    assert len(dirty(connect, result.run_uid)) == 1
    assert scalar(connect, "SELECT COUNT(*) FROM postings WHERE absent_since IS NOT NULL") == 0


def test_the_scheduler_refuses_a_database_without_the_version_index(tmp_path):
    """`require_canonical_schema` is the "wrong database" guard. Migration 16's index
    is part of what the write path assumes, and its absence has to be a loud error at
    run start rather than a silent full scan per run.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute("DROP INDEX idx_posting_versions_source_run")
        conn.commit()
        with pytest.raises(RuntimeError, match="schema version 16"):
            runstore.require_canonical_schema(conn)
    finally:
        conn.close()


def test_the_legacy_tables_are_still_never_written(tmp_path):
    connect = make_connect(tmp_path)

    drive(connect, [STATE_A, STATE_B])

    for table in ("jobs", "job_history", "job_state", "state_events", "runs"):
        assert scalar(connect, f"SELECT COUNT(*) FROM {table}") == 0, table


def test_versions_are_deterministic_across_databases(tmp_path):
    """Two databases, the same record, the same version id. Determinism is what makes
    `INSERT OR IGNORE` a re-link rather than a race, and it is asserted here because
    nothing else would notice if the id became random.
    """
    one = make_connect(tmp_path, name="one.db")
    two = make_connect(tmp_path, name="two.db")

    drive(one, [STATE_A])
    drive(two, [STATE_A])

    left = versions(one)[0]
    right = versions(two)[0]
    assert left["posting_version_id"] == right["posting_version_id"]
    assert left["version_hash"] == right["version_hash"]
    assert left["posting_version_id"] == runstore.posting_version_id_for(
        left["posting_id"], left["version_hash"]
    )
