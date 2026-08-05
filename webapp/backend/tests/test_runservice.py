"""Phase 4.1: the persisted run service, at the service layer.

What these tests are really about is ORDERING and EVIDENCE. The endpoints in
`test_runservice_api` care about status codes; here the questions are:

  * did enrichment start only after the scheduler's writer was closed? (asserted
    positionally, from `run_events.sequence`: every stage event must sit after
    every fetch-phase event, which cannot be true unless the run task -- and
    therefore its writer's `aclose` -- had already returned)
  * do the stages run on `succeeded` AND `partial`, and never on `failed` or
    `cancelled`?
  * does a stage that blows up leave evidence and still let the run settle?
  * is `service.run.settled` always last?

Every database is created under `tmp_path` by the scheduler suite's
`make_connect`, and every run is driven by fake adapters through the scheduler's
`plan=` override, so nothing here touches the registry, the network, or app.db.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from backend import runservice
from backend.db import connect as db_connect
from backend.runservice import (
    CanonicalSchemaUnavailable,
    RunConflict,
    RunService,
    RunServiceError,
    UnknownRun,
    UnknownRunKind,
    UnsupportedRunKind,
)
from backend.sources.contract import RunKind
from backend.sources.scheduler import SchedulerConfig
from backend.sources.testing import FakeTransport, text_response
from backend.tests.test_source_enrichment import PERMISSIVE_PROFILE
from backend.tests.test_source_scheduler_fakes import (
    TEST_TIMEOUT,
    FakeAdapter,
    descriptor_for,
    fast,
    gated,
    hanging,
    make_connect,
    permanent_always,
    plan_of,
)

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _REPO_ROOT not in sys.path:  # pragma: no cover - mirrors sources/scoring.py
    sys.path.insert(0, _REPO_ROOT)


def run(coro):
    """One scenario with a hard ceiling, so a hang fails instead of wedging."""

    async def _guarded():
        return await asyncio.wait_for(coro, TEST_TIMEOUT)

    return asyncio.run(_guarded())


@pytest.fixture(scope="module")
def profile_doc():
    """The real `profile.json`, exactly as `graph.run_pass` wants it.

    The scoring stage is a real graph pass here, not a stub: "the graph pass
    report is persisted" is only worth asserting against the real report shape.
    Enrichment's prefilter gets the enrichment suite's synthetic permissive
    profile instead, so which postings are fetched stays a decision of this test
    rather than of the candidate's live profile.
    """
    with open(os.path.join(_REPO_ROOT, "profile.json")) as handle:
        return json.load(handle)


IDLE_RUNNER = SimpleNamespace(running=False)


def build_service(connect, profile_doc, *, plan=None, **kwargs):
    kwargs.setdefault("legacy_runner", IDLE_RUNNER)
    kwargs.setdefault("enrichment_transport", FakeTransport(default=text_response("A body.")))
    kwargs.setdefault("profile", PERMISSIVE_PROFILE)
    kwargs.setdefault(
        "scheduler_config", SchedulerConfig(retry_base_delay_seconds=0.01, retry_jitter=0.0)
    )
    if plan is not None:
        kwargs.setdefault("plan_factory", lambda kind, config: plan)
    return RunService(connect=connect, profile_doc=profile_doc, **kwargs)


def events(connect, run_uid=None):
    conn = connect()
    try:
        if run_uid is None:
            rows = conn.execute("SELECT * FROM run_events ORDER BY sequence").fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM run_events WHERE run_uid=? ORDER BY sequence", (run_uid,)
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def event_types(connect, run_uid=None):
    return [e["event_type"] for e in events(connect, run_uid)]


def payload_of(connect, run_uid, event_type):
    for row in events(connect, run_uid):
        if row["event_type"] == event_type:
            return json.loads(row["payload_json"]) if row["payload_json"] else None
    raise AssertionError(f"no {event_type} event for {run_uid}")


async def start_and_settle(service, kind):
    started = await service.start_run(kind)
    await service.wait(started["run_uid"])
    return started["run_uid"]


# --------------------------------------------------------------------------- #
# The happy path, and the ordering that proves single-writer discipline
# --------------------------------------------------------------------------- #
def test_a_daily_run_fetches_then_enriches_then_scores_then_settles(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("gh", instances=("acme",), body=fast(3))
    service = build_service(connect, profile_doc, plan=plan_of(adapter))

    run_uid = run(start_and_settle(service, "daily"))

    types = event_types(connect, run_uid)
    assert types[0] == "run.started"
    assert "run.succeeded" in types
    assert [t for t in types if t.startswith(("stage.", "service."))] == [
        "stage.enrichment.started",
        "stage.enrichment.finished",
        "stage.scoring.started",
        "stage.scoring.finished",
        "service.run.settled",
    ]
    assert types[-1] == "service.run.settled"
    # The graph pass's own ops are evidence too, and every one of them sits
    # between the scoring boundaries: they are what the scoring stage IS.
    opened = types.index("stage.scoring.started")
    finished = types.index("stage.scoring.finished")
    assert "score.pass_opened" in types[opened:finished]
    assert "score.pass_completed" in types[opened:finished]
    settled = payload_of(connect, run_uid, "service.run.settled")
    assert settled["fetch_status"] == "succeeded"
    assert settled["outcome"] == "succeeded"
    assert settled["stage_failures"] == []
    assert settled["stages"]["enrichment"]["status"] == "ok"
    assert settled["stages"]["scoring"]["status"] == "ok"


#: What the scheduler and its writer emit while the fetch phase is running.
FETCH_PHASE_PREFIXES = ("run.", "source.")
#: What this service and the graph pass it drives emit, all of it after the
#: fetch's writer has been closed.
POST_FETCH_PREFIXES = ("stage.", "service.", "score.", "resolve.")


def test_every_stage_event_sits_after_every_fetch_phase_event(tmp_path, profile_doc):
    """The writer-closed barrier, asserted positionally against a HELD-OPEN fetch.

    The service allocates its own sequences with `next_event_sequence` on its own
    connection. If any of that ran while the scheduler's writer was still open,
    the two allocators would interleave and a stage event could land at a lower
    sequence than a fetch event -- or collide outright on
    `UNIQUE(run_uid, sequence)`. `await handle` returning means the run task's
    `finally` already awaited `writer.aclose()`, which is what makes the partition
    below hold.

    THE GATE IS THE POINT. With a fast adapter the whole fetch is over in a
    millisecond, so a stage event appended BEFORE `await handle` still lands after
    every fetch event by luck of scheduling and the partition stays green. Here
    the fetch is pinned open until this test has SEEN fetch-phase rows committed,
    and the mid-fetch assertion below then looks straight at the window a
    premature append would occupy.

    It is not the whole pin, because a premature append can also lose the race
    with `StartRun` and be dropped for want of a `pipeline_runs` row -- a silent
    no-op this end-to-end shape cannot see.
    `test_no_stage_event_is_appended_before_the_fetch_handle_returns` closes that
    off deterministically; this one is the end-to-end ordering statement.
    """
    connect = make_connect(tmp_path)
    gate = asyncio.Event()
    plan = plan_of(
        FakeAdapter("gh", instances=("acme", "beta"), body=gated(before=2, gate=gate, after=1)),
        FakeAdapter("lever", instances=("acme",), body=gated(before=2, gate=gate, after=1)),
    )
    service = build_service(connect, profile_doc, plan=plan)

    async def scenario():
        started = await service.start_run("full-direct")
        run_uid = started["run_uid"]
        for _ in range(int(TEST_TIMEOUT / 0.01)):
            await asyncio.sleep(0.01)
            committed = event_types(connect, run_uid)
            if "run.started" in committed and any(
                t.startswith("source.") for t in committed
            ):
                break
        else:  # pragma: no cover - a fetch that never commits is a different bug
            raise AssertionError("the fetch phase committed nothing")
        # Mid-fetch, with the run demonstrably alive: no stage may have spoken.
        assert not [t for t in event_types(connect, run_uid) if t.startswith("stage.")]
        gate.set()
        await service.wait(run_uid)
        return run_uid

    run_uid = run(scenario())

    rows = events(connect, run_uid)
    sequences = [r["sequence"] for r in rows]
    assert sequences == list(range(len(rows))), "sequences must be dense and gap-free"
    for row in rows:
        assert row["event_type"].startswith(FETCH_PHASE_PREFIXES + POST_FETCH_PREFIXES), (
            f"unclassified event {row['event_type']!r}: the partition below only means "
            "something if every event belongs to exactly one phase"
        )
    fetch = [r["sequence"] for r in rows if r["event_type"].startswith(FETCH_PHASE_PREFIXES)]
    stages = [r["sequence"] for r in rows if r["event_type"].startswith(POST_FETCH_PREFIXES)]
    assert fetch and stages
    assert max(fetch) < min(stages)
    assert [t for t in event_types(connect, run_uid) if t.startswith(("stage.", "service."))] == [
        "stage.enrichment.started",
        "stage.enrichment.finished",
        "stage.scoring.started",
        "stage.scoring.finished",
        "service.run.settled",
    ]


class _ReleasableHandle:
    """A `RunHandle` whose await returns only when the test releases it.

    The writer-closed barrier IS "this await has returned", so holding the await
    open is the only way to look directly at the window a premature append would
    occupy, with no scheduling luck involved.
    """

    def __init__(self, result, release: asyncio.Event) -> None:
        self.run_uid = "held"
        self._result = result
        self._release = release

    def __await__(self):
        async def _held():
            await self._release.wait()
            return self._result

        return _held().__await__()

    def cancel(self):  # pragma: no cover - this run is never cancelled
        pass


def test_no_stage_event_is_appended_before_the_fetch_handle_returns(tmp_path, profile_doc):
    """The barrier itself, with the race taken out of the question.

    The end-to-end partition above can be satisfied by luck (a fast fetch) and can
    also MISS a premature append entirely: an append that beats `StartRun` finds
    no `pipeline_runs` row and returns None, writing nothing and raising nothing.
    So here the row exists BEFORE the supervisor starts -- an append at any point
    would land -- and the fetch is held open while this test watches the table.
    Anything the service appends before `await record.handle` returns shows up in
    that window, deterministically, on the first turn of the loop.
    """
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status) VALUES ('held','daily','running')"
        )
        conn.commit()
    finally:
        conn.close()
    service = build_service(connect, profile_doc, plan=[])

    async def scenario():
        release = asyncio.Event()
        record = runservice._ActiveRun(
            run_uid="held",
            kind="daily",
            handle=_ReleasableHandle(_stub_result("succeeded"), release),
        )
        service._active["held"] = record
        supervisor = asyncio.create_task(service._supervise(record))
        for _ in range(20):
            await asyncio.sleep(0.01)
            assert event_types(connect, "held") == [], (
                "the service may append nothing while the fetch's writer is still open"
            )
        release.set()
        await asyncio.wait_for(supervisor, TEST_TIMEOUT)

    run(scenario())

    assert [t for t in event_types(connect, "held") if t.startswith(("stage.", "service."))] == [
        "stage.enrichment.started",
        "stage.enrichment.finished",
        "stage.scoring.started",
        "stage.scoring.finished",
        "service.run.settled",
    ]


def test_the_graph_pass_report_is_persisted_in_the_scoring_event(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("gh", instances=("acme",), body=fast(2))
    service = build_service(connect, profile_doc, plan=plan_of(adapter))

    run_uid = run(start_and_settle(service, "daily"))

    report = payload_of(connect, run_uid, "stage.scoring.finished")
    assert report["run_uid"] == run_uid
    assert report["pass_id"]
    assert report["mode"] in ("full", "incremental")
    assert report["selected"] >= 2
    assert report["scored"] >= 2
    conn = connect()
    try:
        row = conn.execute(
            "SELECT status, scored FROM score_passes WHERE run_uid=?", (run_uid,)
        ).fetchone()
    finally:
        conn.close()
    assert row["status"] == "completed"
    assert row["scored"] == report["scored"]


def test_the_enrichment_report_is_persisted_and_describes_real_fetches(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("gh", instances=("acme",), body=fast(2))
    transport = FakeTransport(default=text_response("A real description body."))
    service = build_service(
        connect, profile_doc, plan=plan_of(adapter), enrichment_transport=transport
    )

    run_uid = run(start_and_settle(service, "daily"))

    report = payload_of(connect, run_uid, "stage.enrichment.finished")
    assert report["considered"] == 2
    assert report["fetched"] == 2
    assert report["available"] == 2
    assert transport.call_count == 2
    conn = connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM descriptions").fetchone()[0] == 2
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Which outcomes licence the stages
# --------------------------------------------------------------------------- #
def test_stages_run_on_a_partial_run(tmp_path, profile_doc):
    """Per-source failure isolation is the design: one dead board must not stop a
    healthy board's postings from being described and scored."""
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter("good", instances=("acme",), body=fast(2)),
        FakeAdapter(
            "bad",
            instances=("acme",),
            body=permanent_always(),
            descriptor=descriptor_for("bad", deadline=1.0),
        ),
    )
    service = build_service(connect, profile_doc, plan=plan)

    run_uid = run(start_and_settle(service, "daily"))

    types = event_types(connect, run_uid)
    assert "run.partial" in types
    assert "stage.enrichment.finished" in types
    assert "stage.scoring.finished" in types
    assert payload_of(connect, run_uid, "service.run.settled")["fetch_status"] == "partial"


def test_a_cancelled_run_settles_without_running_any_stage(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter(
        "slow",
        instances=("acme",),
        body=hanging(before=1),
        descriptor=descriptor_for("slow", deadline=30.0),
    )
    service = build_service(connect, profile_doc, plan=plan_of(adapter))

    async def scenario():
        started = await service.start_run("daily")
        # Let the target reach its first record, so the cancel lands mid-stream
        # rather than before the run's first tick.
        for _ in range(200):
            await asyncio.sleep(0.01)
            if adapter.attempts.get("acme"):
                break
        await service.cancel_run(started["run_uid"])
        await service.wait(started["run_uid"])
        return started["run_uid"]

    run_uid = run(scenario())

    types = event_types(connect, run_uid)
    assert "run.cancelled" in types
    assert not [t for t in types if t.startswith("stage.")]
    assert types[-2:] == ["service.stages.skipped", "service.run.settled"]
    settled = payload_of(connect, run_uid, "service.run.settled")
    assert settled["fetch_status"] == "cancelled"
    assert settled["outcome"] == "cancelled"
    assert settled["stages"]["enrichment"]["status"] == "skipped"


def test_an_aggregators_run_has_no_post_fetch_stages(tmp_path, profile_doc):
    """Aggregator observations feed the resolver, not the description/score path."""
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("agg", instances=("indeed",), body=fast(2))
    service = build_service(connect, profile_doc, plan=plan_of(adapter))

    run_uid = run(start_and_settle(service, "aggregators"))

    types = event_types(connect, run_uid)
    assert "run.succeeded" in types
    assert not [t for t in types if t.startswith("stage.")]
    skipped = payload_of(connect, run_uid, "service.stages.skipped")
    assert "no post-fetch stages" in skipped["reason"]


def test_stages_are_skipped_when_the_fetch_failed(tmp_path, profile_doc):
    """A failed fetch means either the writer died or the scheduler did; its
    report cannot be trusted, so nothing downstream may act on it.

    Driven through `_supervise` with a stub handle rather than through a real
    run: `failed` is exactly the outcome a fake adapter cannot manufacture (an
    adapter exception makes a target fail, which makes the RUN partial).
    """
    connect = make_connect(tmp_path)
    service = build_service(connect, profile_doc, plan=[])
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status) VALUES ('broken','daily','failed')"
        )
        conn.commit()
    finally:
        conn.close()

    async def scenario():
        record = runservice._ActiveRun(
            run_uid="broken",
            kind="daily",
            handle=_StubHandle(_stub_result("failed", error={"type": "WriterError"})),
        )
        service._active["broken"] = record
        await service._supervise(record)

    run(scenario())

    types = event_types(connect, "broken")
    assert types == ["service.stages.skipped", "service.run.settled"]
    settled = payload_of(connect, "broken", "service.run.settled")
    assert settled["fetch_status"] == "failed"
    assert settled["outcome"] == "failed"
    assert settled["fetch_error"] == {"type": "WriterError"}
    skipped = payload_of(connect, "broken", "service.stages.skipped")
    assert skipped["fetch_status"] == "failed"
    assert "does not licence" in skipped["reason"]


class _StubHandle:
    """Just enough `RunHandle` for `_supervise`: an awaitable that yields a result."""

    def __init__(self, result):
        self.run_uid = getattr(result, "run_uid", "stub")
        self._result = result

    def __await__(self):
        async def _done():
            return self._result

        return _done().__await__()

    def cancel(self):  # pragma: no cover - not reached by these tests
        pass


def _stub_result(status, *, error=None):
    return SimpleNamespace(status=status, error=error)


# --------------------------------------------------------------------------- #
# Stage failures are evidence, not crashes
# --------------------------------------------------------------------------- #
def test_a_failing_enrichment_stage_is_recorded_and_scoring_still_runs(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("gh", instances=("acme",), body=fast(2))

    async def boom(conn, run_uid, **kwargs):
        raise RuntimeError("transport exploded")

    service = build_service(connect, profile_doc, plan=plan_of(adapter), enrich=boom)

    run_uid = run(start_and_settle(service, "daily"))

    types = event_types(connect, run_uid)
    assert "stage.enrichment.failed" in types
    assert "stage.enrichment.finished" not in types
    # Scoring still ran: a failed description fetch is no reason to leave every
    # posting in the run unscored.
    assert "stage.scoring.finished" in types
    assert types[-1] == "service.run.settled"
    failed = payload_of(connect, run_uid, "stage.enrichment.failed")
    assert failed["error"] == {"type": "RuntimeError", "message": "transport exploded"}
    settled = payload_of(connect, run_uid, "service.run.settled")
    assert settled["stage_failures"] == ["enrichment"]
    assert settled["outcome"] == "degraded"
    assert settled["fetch_status"] == "succeeded"


def test_a_failing_scoring_stage_still_settles_the_run(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("gh", instances=("acme",), body=fast(1))

    def boom(conn, *, run_uid, profile_doc):
        raise ValueError("rubric is on fire")

    service = build_service(connect, profile_doc, plan=plan_of(adapter), score=boom)

    run_uid = run(start_and_settle(service, "daily"))

    types = event_types(connect, run_uid)
    assert types[-2:] == ["stage.scoring.failed", "service.run.settled"]
    settled = payload_of(connect, run_uid, "service.run.settled")
    assert settled["stage_failures"] == ["scoring"]
    assert settled["stages"]["enrichment"]["status"] == "ok"


# --------------------------------------------------------------------------- #
# The graph pass commits per op (decision 6)
# --------------------------------------------------------------------------- #
def user_state_write(path, key):
    """One user-owned edit, on its own connection, with a SHORT busy timeout.

    Short on purpose: `db.connect`'s own timeout would hide the defect this
    probes for by waiting the whole pass out. A quarter of a second is far longer
    than a committed op needs and far shorter than a corpus-sized pass, so it
    fails exactly when the pass is holding the write lock across ops.
    """
    conn = sqlite3.connect(path, timeout=0.25)
    try:
        conn.execute("PRAGMA busy_timeout=250")
        conn.execute(
            "INSERT OR REPLACE INTO job_state (seen_key, status, updated_at) VALUES (?,?,?)",
            (key, "Applied", "2026-08-04T00:00:00+00:00"),
        )
        conn.commit()
        return conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0]
    finally:
        conn.close()


def test_user_state_writes_succeed_between_graph_ops_during_a_scoring_pass(
    tmp_path, profile_doc
):
    """The whole reason the pass is chopped into per-op transactions.

    One transaction around the whole pass means the write lock is held for its
    entire duration, and on a real corpus that is seconds: every user-state edit
    the browser sends meanwhile answers 500 ("database is locked") and the
    concurrent `aggregators` lane's writer can fail outright. Here a user edit
    lands between every pair of ops, on a connection whose busy timeout is a
    quarter of a second, and each one also SEES the scores the previous pages
    committed -- which is the same property stated from the read side.
    """
    connect = make_connect(tmp_path)
    observed: list[tuple[str, str | None, int | None]] = []

    def edit_between_ops(op_name, appended):
        try:
            observed.append((op_name, None, user_state_write(connect.path, f"k{len(observed)}")))
        except sqlite3.OperationalError as exc:  # pragma: no cover - the defect
            observed.append((op_name, str(exc), None))

    service = build_service(
        connect,
        profile_doc,
        plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(6))),
        score_batch_size=2,
        on_score_op=edit_between_ops,
    )

    run_uid = run(start_and_settle(service, "daily"))

    assert payload_of(connect, run_uid, "service.run.settled")["outcome"] == "succeeded"
    assert [entry for entry in observed if entry[1] is not None] == [], (
        "every user-state write between two graph ops must succeed"
    )
    pages = [entry[2] for entry in observed if entry[0] == "ScoreGraphPass"]
    assert len(pages) >= 3, "the pass must have had several pages to commit between"
    assert pages == sorted(pages) and pages[0] > 0, (
        "each committed page is visible to another connection before the pass ends"
    )
    conn = connect()
    try:
        edits = conn.execute("SELECT COUNT(*) FROM job_state").fetchone()[0]
        scored = conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0]
    finally:
        conn.close()
    assert edits == len(observed)
    assert scored == 6


def test_a_pass_that_dies_between_ops_is_finished_by_the_next_one(tmp_path, profile_doc):
    """Per-op commits are only safe because an unfinished pass self-heals.

    A pass row left `running` licences no baseline, `ClosePass` is the only thing
    that consumes invalidations, and a score is keyed by (posting version, profile
    version, scorer) -- so the pass that died between ops keeps what it wrote and
    the next pass does the rest without rewriting any of it.
    """
    connect = make_connect(tmp_path)
    seen: list[str] = []

    def die_after_the_first_page(op_name, appended):
        seen.append(op_name)
        if op_name == "ScoreGraphPass" and seen.count("ScoreGraphPass") == 1:
            raise RuntimeError("the process died between ops")

    service = build_service(
        connect,
        profile_doc,
        plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(6))),
        score_batch_size=2,
        on_score_op=die_after_the_first_page,
    )

    run_uid = run(start_and_settle(service, "daily"))

    types = event_types(connect, run_uid)
    assert "stage.scoring.failed" in types
    assert "score.pass_completed" not in types
    conn = connect()
    try:
        row = conn.execute(
            "SELECT status FROM score_passes WHERE run_uid=?", (run_uid,)
        ).fetchone()
        partial = conn.execute("SELECT COUNT(DISTINCT posting_id) FROM score_versions").fetchone()[0]
    finally:
        conn.close()
    assert row["status"] == "running"
    assert 0 < partial < 6, "the committed page survived; the rest did not run"

    # The next pass, with nothing sabotaging it, completes what is left.
    healer = build_service(connect, profile_doc, plan=[], score_batch_size=2)
    report = healer._score_sync(run_uid)

    conn = connect()
    try:
        row = conn.execute(
            "SELECT status, scored FROM score_passes WHERE run_uid=?", (run_uid,)
        ).fetchone()
        covered = conn.execute("SELECT COUNT(DISTINCT posting_id) FROM score_versions").fetchone()[0]
        passes = conn.execute("SELECT COUNT(*) FROM score_passes").fetchone()[0]
    finally:
        conn.close()
    assert row["status"] == "completed"
    assert covered == 6
    assert passes == 1, "re-entering a run resumes its pass rather than forking a second"
    assert report["reused"] + report["scored"] >= 6


# --------------------------------------------------------------------------- #
# Kinds and the conflict matrix
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", ["daily", "full-direct", "aggregators"])
def test_supported_kinds_parse(kind):
    assert str(RunService.parse_kind(kind)) == kind


@pytest.mark.parametrize("kind", ["llm-review", "manual-import"])
def test_deferred_kinds_are_unsupported_not_unknown(kind):
    with pytest.raises(UnsupportedRunKind):
        RunService.parse_kind(kind)


@pytest.mark.parametrize("kind", ["", "quick", "DAILY", None, 7])
def test_unknown_kinds_raise_unknown(kind):
    with pytest.raises(UnknownRunKind):
        RunService.parse_kind(kind)


def test_direct_kinds_are_mutually_exclusive_and_aggregators_are_independent(
    tmp_path, profile_doc
):
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter(
            "slow",
            instances=("acme",),
            body=hanging(),
            descriptor=descriptor_for("slow", deadline=30.0),
        )
    )
    service = build_service(connect, profile_doc, plan=plan)

    async def scenario():
        first = await service.start_run("daily")
        errors = {}
        for kind in ("daily", "full-direct"):
            with pytest.raises(RunConflict) as excinfo:
                await service.start_run(kind)
            errors[kind] = str(excinfo.value)
        # Aggregators share no lane with the direct kinds.
        second = await service.start_run("aggregators")
        with pytest.raises(RunConflict):
            await service.start_run("aggregators")
        await service.cancel_run(first["run_uid"])
        await service.cancel_run(second["run_uid"])
        await service.wait(first["run_uid"])
        await service.wait(second["run_uid"])
        return errors

    errors = run(scenario())
    assert "already active in this lane" in errors["daily"]
    assert "already active in this lane" in errors["full-direct"]


def test_a_running_legacy_sweep_refuses_every_canonical_kind(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    service = build_service(
        connect, profile_doc, plan=[], legacy_runner=SimpleNamespace(running=True)
    )

    async def scenario():
        details = []
        for kind in ("daily", "full-direct", "aggregators"):
            with pytest.raises(RunConflict) as excinfo:
                await service.start_run(kind)
            details.append(str(excinfo.value))
        return details

    for detail in run(scenario()):
        assert "legacy sweep" in detail


def test_the_default_legacy_runner_is_the_module_singleton(tmp_path, profile_doc):
    from backend import sweeprunner

    connect = make_connect(tmp_path)
    service = build_service(connect, profile_doc, plan=[], legacy_runner=None)
    sweeprunner.runner.running = True
    try:
        with pytest.raises(RunConflict):
            run(service.start_run("daily"))
    finally:
        sweeprunner.runner.running = False


def test_a_settled_run_frees_its_lane(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("gh", instances=("acme",), body=fast(1))
    service = build_service(connect, profile_doc, plan=plan_of(adapter))

    async def scenario():
        first = await start_and_settle(service, "daily")
        second = await start_and_settle(service, "full-direct")
        return first, second

    first, second = run(scenario())
    assert first != second
    assert service.active_runs() == {}


# --------------------------------------------------------------------------- #
# The live-database gate
# --------------------------------------------------------------------------- #
def legacy_database(tmp_path, name="legacy.db"):
    """A v4-shaped database: legacy tables only, no canonical schema.

    Built from `db.DDL` directly rather than through `init_db`, because `init_db`
    is exactly the thing that would converge it -- and what this stands in for is
    the live app.db as it exists today, before any restart migrates it.
    """
    from backend import db

    path = tmp_path / name
    conn = sqlite3.connect(path)
    try:
        conn.executescript(db.DDL)
        conn.commit()
    finally:
        conn.close()
    return lambda: db_connect(path)


def test_starting_a_run_on_a_legacy_database_reports_the_schema_gap(tmp_path, profile_doc):
    service = build_service(legacy_database(tmp_path), profile_doc, plan=[])
    assert service.has_canonical_schema() is False
    with pytest.raises(CanonicalSchemaUnavailable) as excinfo:
        run(service.start_run("daily"))
    assert "pipeline_runs" in str(excinfo.value)


def test_the_schema_gate_is_checked_after_the_kind_but_before_the_lane(tmp_path, profile_doc):
    """An unknown kind is a client bug whatever the database holds."""
    service = build_service(legacy_database(tmp_path), profile_doc, plan=[])
    with pytest.raises(UnknownRunKind):
        run(service.start_run("nonsense"))
    with pytest.raises(UnsupportedRunKind):
        run(service.start_run("llm-review"))


def test_orphan_recovery_is_skipped_on_a_legacy_database(tmp_path):
    assert runservice.recover_orphans_if_canonical(legacy_database(tmp_path)) is None


def test_orphan_recovery_reconciles_a_run_left_running(tmp_path):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status, requested_at, started_at) "
            "VALUES ('orphan', 'daily', 'running', '2026-08-04T00:00:00+00:00', "
            "'2026-08-04T00:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()

    report = runservice.recover_orphans_if_canonical(connect)

    assert report is not None
    assert report.run_uids == ("orphan",)
    conn = connect()
    try:
        status = conn.execute(
            "SELECT status FROM pipeline_runs WHERE run_uid='orphan'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert status == "interrupted"


# --------------------------------------------------------------------------- #
# Cancel semantics
# --------------------------------------------------------------------------- #
def test_cancelling_an_unknown_run_is_not_found(tmp_path, profile_doc):
    service = build_service(make_connect(tmp_path), profile_doc, plan=[])
    with pytest.raises(UnknownRun):
        run(service.cancel_run("no-such-run"))


def test_cancelling_a_finished_run_conflicts(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    adapter = FakeAdapter("gh", instances=("acme",), body=fast(1))
    service = build_service(connect, profile_doc, plan=plan_of(adapter))

    async def scenario():
        run_uid = await start_and_settle(service, "daily")
        with pytest.raises(RunConflict) as excinfo:
            await service.cancel_run(run_uid)
        return str(excinfo.value)

    assert "already finished" in run(scenario())


def test_cancelling_on_a_legacy_database_reports_the_schema_gap(tmp_path, profile_doc):
    """No live record and no `pipeline_runs` table: the honest answer is "this
    feature is unavailable here", not an OperationalError out of the lookup."""
    service = build_service(legacy_database(tmp_path), profile_doc, plan=[])
    with pytest.raises(CanonicalSchemaUnavailable) as excinfo:
        run(service.cancel_run("whatever"))
    assert "pipeline_runs" in str(excinfo.value)


def test_a_cancel_delivered_after_the_fetch_skips_every_stage(tmp_path, profile_doc):
    """The stage gate reads the cancel flag, not only the fetch status.

    A cancel that arrives while the fetch is already settling leaves a run whose
    fetch status is `succeeded` and whose user has been told 202. Running two more
    stages on it would contradict both the answer and decision 3, so the boundary
    check is the flag. Driven through `_supervise` with a stub handle because the
    window it closes is, by construction, a race a real run cannot be held in.
    """
    connect = make_connect(tmp_path)
    service = build_service(connect, profile_doc, plan=[])
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status) VALUES ('late','daily','succeeded')"
        )
        conn.commit()
    finally:
        conn.close()

    async def scenario():
        record = runservice._ActiveRun(
            run_uid="late", kind="daily", handle=_StubHandle(_stub_result("succeeded"))
        )
        record.cancel_requested = True
        service._active["late"] = record
        await service._supervise(record)

    run(scenario())

    types = event_types(connect, "late")
    assert types == ["service.stages.skipped", "service.run.settled"]
    settled = payload_of(connect, "late", "service.run.settled")
    assert settled["fetch_status"] == "succeeded"
    assert settled["outcome"] == "cancelled"
    assert "cancelled" in payload_of(connect, "late", "service.stages.skipped")["reason"]


def test_a_cancel_during_enrichment_stops_it_and_leaves_scoring_unrun(tmp_path, profile_doc):
    """Cancel is not a fetch-phase-only word.

    The enrichment stage is wrapped in a task precisely so a cancel can stop it:
    its writes are per-row commits and whatever it did not describe stays eligible
    for the next run, so stopping mid-pass costs nothing and finishing a pass the
    user asked to abandon costs everything.
    """
    connect = make_connect(tmp_path)
    entered = asyncio.Event()

    async def enrichment_that_never_returns(conn, run_uid, **kwargs):
        entered.set()
        await asyncio.sleep(TEST_TIMEOUT * 10)
        raise AssertionError("cancelled enrichment must never complete")

    service = build_service(
        connect,
        profile_doc,
        plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(2))),
        enrich=enrichment_that_never_returns,
    )

    async def scenario():
        started = await service.start_run("daily")
        await asyncio.wait_for(entered.wait(), TEST_TIMEOUT / 2)
        await service.cancel_run(started["run_uid"])
        await service.wait(started["run_uid"])
        return started["run_uid"]

    run_uid = run(scenario())

    types = event_types(connect, run_uid)
    assert "run.succeeded" in types, "the fetch itself finished cleanly"
    assert "stage.enrichment.cancelled" in types
    assert "stage.enrichment.finished" not in types
    assert "stage.scoring.started" not in types
    assert "stage.scoring.cancelled" in types
    settled = payload_of(connect, run_uid, "service.run.settled")
    assert settled["fetch_status"] == "succeeded"
    assert settled["outcome"] == "cancelled"
    assert settled["outcome"] != "succeeded"
    assert settled["stages"]["enrichment"]["status"] == "cancelled"
    assert settled["stages"]["scoring"]["status"] == "cancelled"
    assert settled["stages_cancelled"] == ["enrichment", "scoring"]


def test_a_cancel_during_scoring_stops_the_pass_between_graph_ops(tmp_path, profile_doc):
    """A worker thread cannot be cancelled, so the pass reads the flag itself.

    It stops at a COMMITTED op boundary: the pages already scored stay, the pass
    row stays `running` (licencing nothing, so the next pass redoes the rest), and
    the run settles `cancelled` rather than `succeeded`.
    """
    connect = make_connect(tmp_path)
    state: dict[str, Any] = {"cancelled": False}

    def cancel_after_the_first_page(op_name, appended):
        if op_name == "ScoreGraphPass" and not state["cancelled"]:
            state["cancelled"] = True
            future = asyncio.run_coroutine_threadsafe(
                state["service"].cancel_run(state["run_uid"]), state["loop"]
            )
            future.result(timeout=TEST_TIMEOUT)

    service = build_service(
        connect,
        profile_doc,
        plan=plan_of(FakeAdapter("gh", instances=("acme",), body=fast(6))),
        score_batch_size=1,
        on_score_op=cancel_after_the_first_page,
    )
    state["service"] = service

    async def scenario():
        state["loop"] = asyncio.get_running_loop()
        started = await service.start_run("daily")
        state["run_uid"] = started["run_uid"]
        await service.wait(started["run_uid"])
        return started["run_uid"]

    run_uid = run(scenario())

    types = event_types(connect, run_uid)
    assert "stage.scoring.started" in types
    assert "stage.scoring.cancelled" in types
    assert "stage.scoring.finished" not in types
    assert "score.pass_completed" not in types
    settled = payload_of(connect, run_uid, "service.run.settled")
    assert settled["outcome"] == "cancelled"
    assert settled["stages"]["scoring"]["status"] == "cancelled"
    conn = connect()
    try:
        status = conn.execute(
            "SELECT status FROM score_passes WHERE run_uid=?", (run_uid,)
        ).fetchone()[0]
        scored = conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0]
    finally:
        conn.close()
    assert status == "running", "an unfinished pass licences nothing"
    assert 0 < scored < 6, "the pages that committed stayed; the rest did not run"


def test_cancelling_a_run_this_process_does_not_own_conflicts(tmp_path, profile_doc):
    """A `running` row with no live handle belongs to a dead process. Startup
    recovery reconciles it; a cancel cannot reach it."""
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO pipeline_runs (run_uid, kind, status) VALUES ('ghost','daily','running')"
        )
        conn.commit()
    finally:
        conn.close()
    service = build_service(connect, profile_doc, plan=[])
    with pytest.raises(RunConflict) as excinfo:
        run(service.cancel_run("ghost"))
    assert "not owned by this process" in str(excinfo.value)


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
def test_list_and_detail_describe_a_finished_run(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter("gh", instances=("acme",), body=fast(2)),
        FakeAdapter(
            "bad",
            instances=("acme",),
            body=permanent_always(),
            descriptor=descriptor_for("bad", deadline=1.0),
        ),
    )
    service = build_service(connect, profile_doc, plan=plan)

    async def scenario():
        run_uid = await start_and_settle(service, "daily")
        return run_uid, await service.list_runs(20), await service.run_detail(run_uid)

    run_uid, listing, detail = run(scenario())

    assert [r["run_uid"] for r in listing] == [run_uid]
    assert listing[0]["kind"] == "daily"
    assert listing[0]["status"] == "partial"
    assert listing[0]["trigger"] == "api"
    assert listing[0]["active"] is False

    assert detail["terminal"] is True
    sources = {r["source"]: r for r in detail["source_runs"]}
    assert set(sources) == {"gh:acme", "bad:acme"}
    assert sources["gh:acme"]["status"] == "succeeded"
    assert sources["gh:acme"]["fetched_count"] == 2
    assert sources["gh:acme"]["inventory_scope"] == "complete"
    assert sources["bad:acme"]["status"] == "failed"
    assert sources["bad:acme"]["error"]["type"] == "PermanentSourceError"
    assert detail["stages"]["enrichment"]["phase"] == "finished"
    assert detail["stages"]["scoring"]["report"]["run_uid"] == run_uid
    assert detail["settled"]["outcome"] == "partial"
    assert detail["change_summary"]["changed"] >= 2


def test_detail_is_none_for_an_unknown_run(tmp_path, profile_doc):
    service = build_service(make_connect(tmp_path), profile_doc, plan=[])
    assert run(service.run_detail("nope")) is None


def test_list_is_newest_first(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    conn = connect()
    try:
        for uid, at in (("older", "2026-01-01T00:00:00+00:00"), ("newer", "2026-06-01T00:00:00+00:00")):
            conn.execute(
                "INSERT INTO pipeline_runs (run_uid, kind, status, requested_at) VALUES (?,?,?,?)",
                (uid, "daily", "succeeded", at),
            )
        conn.commit()
    finally:
        conn.close()
    service = build_service(connect, profile_doc, plan=[])
    assert [r["run_uid"] for r in run(service.list_runs(10))] == ["newer", "older"]
    assert [r["run_uid"] for r in run(service.list_runs(1))] == ["newer"]


# --------------------------------------------------------------------------- #
# Housekeeping
# --------------------------------------------------------------------------- #
def test_aclose_cancels_everything_this_process_owns(tmp_path, profile_doc):
    connect = make_connect(tmp_path)
    plan = plan_of(
        FakeAdapter(
            "slow",
            instances=("acme",),
            body=hanging(),
            descriptor=descriptor_for("slow", deadline=30.0),
        )
    )
    service = build_service(connect, profile_doc, plan=plan)

    async def scenario():
        started = await service.start_run("daily")
        await service.aclose(timeout=TEST_TIMEOUT)
        return started["run_uid"]

    run_uid = run(scenario())
    assert service.active_runs() == {}
    conn = connect()
    try:
        status = conn.execute(
            "SELECT status FROM pipeline_runs WHERE run_uid=?", (run_uid,)
        ).fetchone()[0]
    finally:
        conn.close()
    assert status == "cancelled"


def test_the_default_service_is_a_process_singleton():
    runservice.reset_default_service()
    try:
        first = runservice.default_service()
        assert runservice.default_service() is first
    finally:
        runservice.reset_default_service()


def test_every_service_error_is_a_runservice_error():
    for error in (UnknownRunKind, UnsupportedRunKind, RunConflict, CanonicalSchemaUnavailable, UnknownRun):
        assert issubclass(error, RunServiceError)
