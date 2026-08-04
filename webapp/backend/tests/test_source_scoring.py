"""Phase 3.3: `scoring.persist_scores` AT THE DATABASE.

`test_scoring_features.py` covers the same contract as pure functions. This file
exists because a pure function nobody calls refuses nothing: both rules below were
reproducible against the write path while that file stayed green.

  A REVERTING INPUT RE-CURRENTS ITS OWN ROW. `score_version_id` is deterministic
    over (posting version, profile version, input, scorer), so an input that goes
    A -> B -> A on ONE posting version maps its third state back onto the row its
    first state wrote. The anti-join cannot see that row -- it reads current rows
    only, and that one is superseded -- so a plain INSERT violates the primary key
    AND migration 9's UNIQUE, and because the id is deterministic every RETRY
    violates it again. The pass is not slow or wrong, it is stuck.
  THE FEATURE CONTRACT IS ENFORCED WHERE THE ROW IS WRITTEN. Not merely available
    as a function: replacing both `validate_features` call sites with `dict(...)`
    must fail this file.

Every database is created under `tmp_path` by `make_connect`. Nothing here can
reach webapp/app.db.
"""
import dataclasses
import datetime
import hashlib
import json
import os
import sys

import pytest

from backend.sources import graph, runstore, scoring
from backend.sources.contract import NormalizedPosting, SourceCategory
from backend.tests.test_source_scheduler_fakes import make_connect

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede these)
import rubric  # noqa: E402

#: Three distinct write stamps: supersession is recorded with the time it happened,
#: and a single constant would let a wrong row pass a supersession assertion.
AT = "2026-08-04T12:00:00+00:00"
LATER = "2026-08-05T12:00:00+00:00"
LATEST = "2026-08-06T12:00:00+00:00"

#: Dated off the clock for `rubric.posting_age_days`' sake, exactly as
#: `test_source_graph.py` does: a literal would eventually cross the 30-day penalty
#: and then the 90-day ghost cap and fail these tests on a calendar.
TODAY = datetime.date.today().isoformat()

#: Two descriptions that are the SAME posting version scored under different
#: inputs: a rich one that clears the no-description cap and a thin one that does
#: not. Which tier each produces is not the point -- that they produce different
#: `input_hash` values on one posting version is.
DESC_A = (
    "Support enterprise customers on Azure, Intune, Entra ID, Active Directory, "
    "M365, ServiceNow, Kubernetes and observability tooling. 2 years of experience. "
    "Salary range $150,000 - $185,000 USD."
)
DESC_B = "Help customers with tickets. 6+ years of experience required."


def _identity(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


ID_A, ID_B = _identity(DESC_A), _identity(DESC_B)


def category_of(namespace: str) -> SourceCategory:
    return SourceCategory.DIRECT if namespace == "gh:acme" else SourceCategory.AGGREGATOR


@pytest.fixture(scope="module")
def profile_doc():
    with open(os.path.join(_REPO_ROOT, "profile.json")) as f:
        return json.load(f)


@pytest.fixture
def conn(tmp_path):
    connect = make_connect(tmp_path)
    c = connect()
    try:
        yield c
    finally:
        c.close()


@dataclasses.dataclass(frozen=True)
class Scene:
    """One delivered posting, resolved to the exact inputs `persist_scores` takes."""

    conn: object
    posting_id: str
    base: scoring.WorkRow
    profile_version_id: str
    scorer: scoring.ScorerIdentity

    def persist(self, *, description, identity, at=AT):
        item = dataclasses.replace(
            self.base, description=description, description_identity=identity
        )
        return scoring.persist_scores(
            self.conn, [item], profile_version_id=self.profile_version_id,
            scorer=self.scorer, at=at,
        )

    def input_hash(self, identity: str) -> str:
        return scoring.input_hash(
            version_hash=self.base.version_hash,
            description_identity=identity,
            is_aggregator=self.base.is_aggregator,
        )

    def rows(self):
        return self.conn.execute(
            "SELECT score_version_id, tier, input_hash, created_at, superseded_at, "
            "superseded_by FROM score_versions ORDER BY score_version_id"
        ).fetchall()

    def current(self):
        rows = [r for r in self.rows() if r["superseded_at"] is None]
        assert len(rows) <= 1, "uq_score_versions_current is supposed to make this impossible"
        return rows[0] if rows else None


@pytest.fixture
def scene(conn, profile_doc):
    """One committed run delivering one posting, plus the scoring identity."""
    runstore.create_pipeline_run(
        conn, run_uid="run-1", kind="daily", requested_at=AT, started_at=AT
    )
    runstore.create_source_run(
        conn, source_run_id="run-1-0", run_uid="run-1", source="gh:acme", attempt=1
    )
    runstore.write_records(
        conn, run_uid="run-1", source_run_id="run-1-0", recorded_at=AT,
        records=[NormalizedPosting(
            source_key="gh", instance_key="acme", title="Azure Support Engineer",
            company="Acme Robotics", url="https://acme.example/1", req_id="R-1",
            location="San Francisco, CA", posted_date=TODAY, salary_text="",
            description=None,
        )],
    )
    runstore.finish_source_run(
        conn, source_run_id="run-1-0", status="succeeded", finished_at=AT
    )
    conn.execute("UPDATE pipeline_runs SET status='succeeded' WHERE run_uid='run-1'")

    posting_id = conn.execute("SELECT posting_id FROM postings").fetchone()["posting_id"]
    rows, skipped = graph.build_work_rows(conn, [posting_id], category_of=category_of)
    assert not skipped and len(rows) == 1
    return Scene(
        conn=conn,
        posting_id=posting_id,
        base=rows[0],
        profile_version_id=scoring.upsert_profile_version(conn, profile_doc, at=AT),
        scorer=scoring.scorer_identity(),
    )


# --------------------------------------------------------------------------- #
# 1. A reverting input
# --------------------------------------------------------------------------- #
def test_a_reverting_input_re_currents_its_row_instead_of_re_minting_it(scene):
    """A -> B -> A on ONE posting version. The third call used to raise, forever.

    Not a hypothetical cycle: a description fetch that fails and is retried, an
    aggregator mirror that drops and restores a body, a `descriptions` row that
    reverts -- any of them puts an input back where it already was, and the input
    is what the score is keyed on. The pass aborted with an IntegrityError and
    every retry hit the same deterministic id, so the corpus stopped scoring.

    What must be true instead: no exception, exactly one current row at every step,
    the final current row is LITERALLY the row input A wrote (same id, same tier,
    same created_at -- it was not rescored, it was re-currented), and two rows in
    total. A third input state would be a third row; a re-mint would be a
    constraint violation; anything else would be a rewrite of stored evidence.
    """
    first = scene.persist(description=DESC_A, identity=ID_A, at=AT)
    assert (first.scored, first.reused, first.recurrent, first.superseded) == (1, 0, 0, 0)
    a_row = scene.current()
    assert a_row is not None and a_row["input_hash"] == scene.input_hash(ID_A)
    a_id, a_tier = a_row["score_version_id"], a_row["tier"]

    second = scene.persist(description=DESC_B, identity=ID_B, at=LATER)
    assert (second.scored, second.reused, second.recurrent, second.superseded) == (1, 0, 0, 1)
    b_row = scene.current()
    assert b_row["score_version_id"] != a_id
    assert b_row["input_hash"] == scene.input_hash(ID_B)
    b_id = b_row["score_version_id"]

    # The reverting call. No exception, and no third row.
    third = scene.persist(description=DESC_A, identity=ID_A, at=LATEST)
    assert (third.scored, third.reused, third.recurrent, third.superseded) == (0, 0, 1, 1)

    rows = {r["score_version_id"]: r for r in scene.rows()}
    assert set(rows) == {a_id, b_id}, "a reverting input must not mint a third row"

    revived = scene.current()
    assert revived["score_version_id"] == a_id
    assert revived["input_hash"] == scene.input_hash(ID_A)
    assert revived["tier"] == a_tier
    assert revived["created_at"] == AT, (
        "the row was re-currented, not rewritten: it was created when input A was "
        "first seen and that is a fact about the past"
    )
    assert revived["superseded_by"] is None, (
        "a current row that still names its successor turns the supersession chain "
        "into a cycle"
    )
    assert rows[b_id]["superseded_at"] == LATEST
    assert rows[b_id]["superseded_by"] == a_id

    # And the settled state is settled: a fourth call over the same input writes
    # nothing at all and reads as plain reuse.
    fourth = scene.persist(description=DESC_A, identity=ID_A, at=LATEST)
    assert (fourth.scored, fourth.reused, fourth.recurrent, fourth.superseded) == (0, 1, 0, 0)
    assert {r["score_version_id"] for r in scene.rows()} == {a_id, b_id}
    assert scene.current()["score_version_id"] == a_id


def test_the_reverted_row_keeps_the_vector_that_input_produced(scene):
    """Re-currenting is only correct because the row is the same row.

    Identical input and identical scorer produce an identical tier, rationale and
    feature vector -- so the stored ones already ARE the answer, and re-deriving
    them would be a slower way to write the same bytes. This asserts the equality
    the shortcut rests on rather than assuming it.
    """
    scene.persist(description=DESC_A, identity=ID_A, at=AT)
    stored = scene.conn.execute(
        "SELECT tier, odds, odds_score, rationale_json, features_json FROM score_versions"
    ).fetchone()
    before = tuple(stored)

    scene.persist(description=DESC_B, identity=ID_B, at=LATER)
    scene.persist(description=DESC_A, identity=ID_A, at=LATEST)

    after = tuple(scene.conn.execute(
        "SELECT tier, odds, odds_score, rationale_json, features_json FROM score_versions "
        "WHERE superseded_at IS NULL"
    ).fetchone())
    assert after == before

    features = json.loads(after[4])["score_row"]
    assert rubric.reconstruct_tier(features) == after[0], (
        "the re-currented row must still replay to its own tier"
    )


def test_two_reverting_postings_in_one_batch_stay_within_the_unique_index(conn, profile_doc):
    """The batch form, because every statement here is an `executemany`.

    One posting reverting is the reported bug; the fix has to hold when a page of
    them reverts together, since the supersede / insert / re-current / backfill
    ordering is what keeps the partial unique index seeing exactly one current row
    per key at every statement boundary -- not per posting.
    """
    runstore.create_pipeline_run(
        conn, run_uid="run-1", kind="daily", requested_at=AT, started_at=AT
    )
    runstore.create_source_run(
        conn, source_run_id="run-1-0", run_uid="run-1", source="gh:acme", attempt=1
    )
    runstore.write_records(
        conn, run_uid="run-1", source_run_id="run-1-0", recorded_at=AT,
        records=[
            NormalizedPosting(
                source_key="gh", instance_key="acme", title="Azure Support Engineer",
                company="Acme Robotics", url=f"https://acme.example/{n}", req_id=f"R-{n}",
                location="San Francisco, CA", posted_date=TODAY, salary_text="",
                description=None,
            )
            for n in range(1, 4)
        ],
    )
    runstore.finish_source_run(
        conn, source_run_id="run-1-0", status="succeeded", finished_at=AT
    )
    conn.execute("UPDATE pipeline_runs SET status='succeeded' WHERE run_uid='run-1'")

    posting_ids = [r["posting_id"] for r in conn.execute(
        "SELECT posting_id FROM postings ORDER BY posting_id"
    )]
    rows, skipped = graph.build_work_rows(conn, posting_ids, category_of=category_of)
    assert not skipped and len(rows) == 3
    profile_version_id = scoring.upsert_profile_version(conn, profile_doc, at=AT)
    scorer = scoring.scorer_identity()

    def persist(description, identity, at):
        items = [
            dataclasses.replace(row, description=description, description_identity=identity)
            for row in rows
        ]
        return scoring.persist_scores(
            conn, items, profile_version_id=profile_version_id, scorer=scorer, at=at
        )

    assert persist(DESC_A, ID_A, AT).scored == 3
    assert persist(DESC_B, ID_B, LATER).scored == 3
    reverted = persist(DESC_A, ID_A, LATEST)

    assert (reverted.scored, reverted.recurrent, reverted.superseded) == (0, 3, 3)
    assert conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0] == 6
    assert conn.execute(
        "SELECT COUNT(*) FROM score_versions WHERE superseded_at IS NULL"
    ).fetchone()[0] == 3
    assert conn.execute(
        "SELECT COUNT(*) FROM score_versions WHERE superseded_at IS NULL "
        "AND superseded_by IS NOT NULL"
    ).fetchone()[0] == 0


# --------------------------------------------------------------------------- #
# 2. The closed feature contract, on the write path
# --------------------------------------------------------------------------- #
def _mutate_score_row(monkeypatch, mutate):
    """Make the scorer emit a vector the contract must refuse."""
    real = rubric.score_row_explained

    def fake(row, desc, *, is_aggregator=None):
        result = real(row, desc, is_aggregator=is_aggregator)
        return dataclasses.replace(result, features=mutate(dict(result.features)))

    monkeypatch.setattr(rubric, "score_row_explained", fake)


@pytest.mark.parametrize("mutate,expected", [
    # The mutation the reviewer ran: an added dimension the replayer never hears of.
    (lambda f: {**f, "vibes_bonus": 2}, "vibes_bonus"),
    # Missing: `reconstruct_tier` INDEXES `raw_score`, so this row would not replay
    # wrong, it would raise on a row nothing can re-derive.
    (lambda f: {k: v for k, v in f.items() if k != "raw_score"}, "raw_score"),
    (lambda f: {k: v for k, v in f.items() if k != "function_match"}, "function_match"),
    # Non-numeric, and the NaN case that no exception would ever catch downstream.
    (lambda f: {**f, "raw_score": "not-a-number"}, "must be a number"),
    (lambda f: {**f, "raw_score": None}, "must be a number"),
    (lambda f: {**f, "function_match": True}, "must be a number"),
    (lambda f: {**f, "raw_score": float("nan")}, "finite"),
    (lambda f: {**f, "raw_score": float("inf")}, "finite"),
    # A blocked vector is closed in the other direction too.
    (lambda f: {"blocker": "non_us_location", **f}, "blocked feature vector"),
])
def test_an_unreplayable_vector_is_refused_by_the_write_itself(scene, monkeypatch,
                                                               mutate, expected):
    """THE MUTATION TEST. Replace both `validate_features` call sites in
    `persist_scores` with `dict(...)` and this test fails; nothing else in the suite
    does, which is how the enforcement went untested in the first place.

    The refusal has to happen at the write because that is the only place the score
    becomes permanent. A vector that cannot reproduce its tier is not a bad row to
    be cleaned up later -- `score_versions` is append-and-supersede, so it is a
    permanent claim about a tier nobody can check.
    """
    _mutate_score_row(monkeypatch, mutate)

    with pytest.raises(scoring.ScoreFeatureError, match=expected):
        scene.persist(description=DESC_A, identity=ID_A, at=AT)

    assert scene.conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0] == 0, (
        "the refusal must come BEFORE the insert, not after it"
    )
    scene.conn.rollback()


def test_the_odds_vector_is_validated_at_the_write_too(scene, monkeypatch):
    """Both call sites, not just the first one. The hireability vector has its own
    closed set (`REQUIRED_HIREABILITY_FEATURES`) and its own replay claim
    (`sum(features.values()) == score`), which an unknown key breaks identically.
    """
    real = rubric.hireability_explained

    def fake(row, desc):
        result = real(row, desc)
        return dataclasses.replace(result, features={**result.features, "vibes": 1})

    monkeypatch.setattr(rubric, "hireability_explained", fake)

    with pytest.raises(scoring.ScoreFeatureError, match="vibes"):
        scene.persist(description=DESC_A, identity=ID_A, at=AT)
    assert scene.conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0] == 0
    scene.conn.rollback()


def test_a_blocked_posting_still_persists_its_blocker_vector(scene):
    """The contract must not have been tightened into refusing a correct row.

    A blocked vector is `{"blocker": <code>}` -- no `raw_score`, no
    `function_match`, and a value that is a string. It is the one shape the
    required-present and numeric rules must both step around, so a blocked posting
    scoring cleanly is what proves they were written to the actual replay semantics
    rather than to the happy path.
    """
    off_target = dataclasses.replace(
        scene.base,
        row={**dict(scene.base.row), "location": "Austin, TX", "remote": "false"},
        description=DESC_A,
        description_identity=ID_A,
    )
    outcome = scoring.persist_scores(
        scene.conn, [off_target], profile_version_id=scene.profile_version_id,
        scorer=scene.scorer, at=AT,
    )
    assert (outcome.scored, outcome.blocked) == (1, 1)

    row = scene.current()
    assert row["tier"] == 0
    features = json.loads(scene.conn.execute(
        "SELECT features_json FROM score_versions"
    ).fetchone()[0])["score_row"]
    assert set(features) == candidate_profile.BLOCKED_SCORE_ROW_FEATURES
    assert features["blocker"] in candidate_profile.BLOCKER_CODES
    assert rubric.reconstruct_tier(features) == 0
