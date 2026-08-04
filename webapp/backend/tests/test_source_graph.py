"""Phase 3.3: the scoring graph — mode, canonical selection, and score-once.

The roadmap lines under test: "resolve -> enrich -> score as a graph over changed
inputs", "score exactly once per (posting version, profile version, scorer)", and
"never scan all postings after each source".

Six rules, each with a test that fails when it is reverted:

  MODE IS A DECISION WITH REASONS. `decide_mode` is pure and its whole decision
    table is pinned here. FULL when nothing has completed a pass on the baseline
    run, or when the profile or the scorer moved.
  THE BASELINE IS LICENSED, NOT ASSUMED. Only a COMPLETED pass on the run the
    dirty set is measured against licenses INCREMENTAL. A pass that died re-does
    its work rather than letting the next run's dirty set silently skip it.
  CANONICAL VERSION IS BY CATEGORY RANK. Direct and startup boards over manual
    over aggregators, ties broken lexicographically. After a relink the survivor
    also sees the state maps of postings redirecting into it -- which is what makes
    a canonical match change the TIER rather than just the bookkeeping.
  SCORE EXACTLY ONCE. A second pass over unchanged content mints zero rows.
  A CHANGED INPUT SUPERSEDES, IT DOES NOT OVERWRITE. A description arriving after
    the score gives a new row and marks the old one superseded; the partial unique
    index enforces that exactly one is current.
  NO QUERY ON THIS PATH SCANS A LARGE TABLE. Asserted against the query planner,
    with ANALYZE run first, on the real statements the code issues.

Every database is created under `tmp_path` by `make_connect`. Nothing here can
reach webapp/app.db.
"""
import datetime
import hashlib
import json
import os
import sqlite3
import sys

import pytest

from backend.sources import graph, resolver, runstore, scoring
from backend.sources.contract import NormalizedPosting, SourceCategory
from backend.tests.test_source_scheduler_fakes import make_connect

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402

AT = "2026-08-04T12:00:00+00:00"

#: Posting dates are computed from TODAY rather than pinned to a literal, because
#: `rubric.posting_age_days` measures against the real clock: a hard-coded date
#: would quietly cross the 30-day penalty and then the 90-day ghost cap, and these
#: tests would start failing on a calendar rather than on a code change.
TODAY = datetime.date.today().isoformat()

#: The namespaces these tests use, and what they are. Injected rather than taken
#: from the adapter registry: the graph's decisions are about CATEGORIES, and a
#: fixture that had to register real adapters would be testing the registry.
CATEGORIES = {
    "gh:acme": SourceCategory.DIRECT,
    "lever:acme": SourceCategory.DIRECT,
    "yc": SourceCategory.STARTUP_BOARD,
    "manual:dice": SourceCategory.MANUAL,
    "jobspy:indeed": SourceCategory.AGGREGATOR,
    "builtin": SourceCategory.AGGREGATOR,
}


def category_of(namespace: str) -> SourceCategory:
    return CATEGORIES.get(namespace, SourceCategory.AGGREGATOR)


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


# --------------------------------------------------------------------------- #
# Fixture helpers: runs, records, passes
# --------------------------------------------------------------------------- #
def record(namespace, *, title="Support Engineer", company="Acme Robotics",
           url="https://acme.example/1", req_id="R-1", location="San Francisco, CA",
           posted=None, salary="", description=None):
    posted = TODAY if posted is None else posted
    source_key, _, instance = namespace.partition(":")
    return NormalizedPosting(
        source_key=source_key, instance_key=instance, title=title, company=company,
        url=url, req_id=req_id, location=location, posted_date=posted,
        salary_text=salary, description=description,
    )


def deliver(conn, run_uid, deliveries, *, requested_at, status="succeeded"):
    """One committed run delivering `{namespace: [records]}`. Returns the run uid."""
    runstore.create_pipeline_run(
        conn, run_uid=run_uid, kind="daily", requested_at=requested_at,
        started_at=requested_at,
    )
    for index, (namespace, records) in enumerate(deliveries.items()):
        attempt_id = f"{run_uid}-{index}"
        runstore.create_source_run(
            conn, source_run_id=attempt_id, run_uid=run_uid, source=namespace, attempt=1,
        )
        runstore.write_records(
            conn, run_uid=run_uid, source_run_id=attempt_id, records=records,
            recorded_at=requested_at,
        )
        runstore.finish_source_run(
            conn, source_run_id=attempt_id, status="succeeded", finished_at=requested_at,
        )
    conn.execute("UPDATE pipeline_runs SET status=? WHERE run_uid=?", (status, run_uid))
    return run_uid


def run_pass(conn, run_uid, profile_doc, **kwargs):
    kwargs.setdefault("category_of", category_of)
    kwargs.setdefault("at", AT)
    return graph.run_pass(conn, run_uid=run_uid, profile_doc=profile_doc, **kwargs)


def current_scores(conn):
    return {
        r["posting_id"]: (r["tier"], r["input_hash"])
        for r in conn.execute(
            "SELECT posting_id, tier, input_hash FROM score_versions WHERE superseded_at IS NULL"
        )
    }


def score_rows(conn):
    return conn.execute(
        "SELECT score_version_id, posting_id, posting_version_id, tier, input_hash, "
        "superseded_at, superseded_by FROM score_versions ORDER BY created_at, score_version_id"
    ).fetchall()


def pass_row(conn, run_uid):
    return conn.execute(
        "SELECT * FROM score_passes WHERE run_uid=?", (run_uid,)
    ).fetchone()


def only_posting(conn):
    return conn.execute("SELECT posting_id FROM postings").fetchone()["posting_id"]


# --------------------------------------------------------------------------- #
# 1. decide_mode: the whole decision table, pure
# --------------------------------------------------------------------------- #
CURRENT = graph.PassIdentity(profile_version_id="profile-1", scorer_hash="scorer-1")


def _baseline(profile="profile-1", scorer="scorer-1", status=graph.PASS_COMPLETED):
    return graph.PassRecord(
        pass_id="p", run_uid="r", profile_version_id=profile, scorer_hash=scorer,
        mode="incremental", status=status,
    )


@pytest.mark.parametrize("baseline,mode,reasons", [
    (None, graph.PassMode.FULL, ("no_baseline_pass",)),
    (_baseline(), graph.PassMode.INCREMENTAL, ()),
    (_baseline(profile="profile-2"), graph.PassMode.FULL, ("profile_changed",)),
    (_baseline(scorer="scorer-2"), graph.PassMode.FULL, ("scorer_changed",)),
    (_baseline(profile="profile-2", scorer="scorer-2"), graph.PassMode.FULL,
     ("profile_changed", "scorer_changed")),
])
def test_the_mode_decision_table(baseline, mode, reasons):
    """Pure: no connection, no clock. Both identity reasons are reported when both
    apply, because "why did tonight's run rescore 33,000 postings" has to be
    answerable from the pass row.
    """
    decision = graph.decide_mode(CURRENT, baseline)
    assert decision.mode is mode
    assert decision.reasons == reasons


# --------------------------------------------------------------------------- #
# 2. Canonical version selection, pure
# --------------------------------------------------------------------------- #
def test_canonical_selection_prefers_direct_inventory_over_an_aggregator_mirror():
    """This IS the ghost-listing mechanism: an aggregator's undated mirror is only
    canonical when nothing better exists."""
    state = {"jobspy:indeed": "v-agg", "gh:acme": "v-board"}
    assert graph.select_canonical_version([state], category_of=category_of) == (
        "gh:acme", "v-board"
    )
    assert graph.select_canonical_version(
        [{"jobspy:indeed": "v-agg"}], category_of=category_of
    ) == ("jobspy:indeed", "v-agg")
    assert graph.select_canonical_version([], category_of=category_of) is None
    assert graph.select_canonical_version([{}], category_of=category_of) is None


def test_canonical_selection_ranks_manual_above_an_aggregator_and_below_a_board():
    """A human assertion outranks a scrape of somebody else's board, and loses to
    the board itself."""
    assert graph.select_canonical_version(
        [{"manual:dice": "v-manual", "jobspy:indeed": "v-agg"}], category_of=category_of
    ) == ("manual:dice", "v-manual")
    assert graph.select_canonical_version(
        [{"manual:dice": "v-manual", "yc": "v-yc"}], category_of=category_of
    ) == ("yc", "v-yc")


def test_a_tie_is_broken_lexicographically_and_never_by_iteration_order():
    """Two direct boards describing one posting is real (an ATS migration). A
    canonical version chosen by dict order would change the score for no reason and
    be impossible to review."""
    forward = {"gh:acme": "v-gh", "lever:acme": "v-lever"}
    reverse = {"lever:acme": "v-lever", "gh:acme": "v-gh"}
    assert graph.select_canonical_version([forward], category_of=category_of) == (
        "gh:acme", "v-gh"
    )
    assert graph.select_canonical_version([reverse], category_of=category_of) == (
        "gh:acme", "v-gh"
    )


def test_the_first_state_map_wins_for_a_namespace_it_mentions():
    """Maps arrive in priority order (own map first, then postings redirecting in),
    so a loser's stale entry can never override the survivor's live one for the same
    source."""
    own = {"gh:acme": "v-live"}
    merged = {"gh:acme": "v-stale", "jobspy:indeed": "v-agg"}
    assert graph.select_canonical_version([own, merged], category_of=category_of) == (
        "gh:acme", "v-live"
    )


# --------------------------------------------------------------------------- #
# 3. Score exactly once
# --------------------------------------------------------------------------- #
def test_a_second_pass_over_unchanged_content_mints_nothing(conn, profile_doc):
    """The literal "score exactly once" guarantee, plus its cost consequence: the
    second pass must find zero dirty postings AND zero work, or the daily run
    re-scores the corpus every night.
    """
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-01T00:00:00+00:00")
    first = run_pass(conn, "run-1", profile_doc)
    assert (first["mode"], first["scored"], first["reused"]) == ("full", 1, 0)

    deliver(conn, "run-2", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-02T00:00:00+00:00")
    assert runstore.dirty_posting_ids(conn, "run-2") == [], "content did not move"

    second = run_pass(conn, "run-2", profile_doc)
    assert second["mode"] == "incremental"
    assert second["reasons"] == []
    assert (second["selected"], second["scored"], second["reused"]) == (0, 0, 0)
    assert conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0] == 1


def test_a_revert_re_links_a_version_that_is_already_scored(conn, profile_doc):
    """A -> B -> A. The revert IS dirty (content moved), so the posting reaches the
    pass -- and the selection anti-join then finds a current score row for the
    re-linked version under this (profile, scorer) with a matching input, so it is
    REUSED rather than rescored. The anti-join subsumes dirty; dirty alone would
    rescore here for nothing.
    """
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme", title="Support Engineer")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)
    deliver(conn, "run-2", {"gh:acme": [record("gh:acme", title="Senior Support Engineer")]},
            requested_at="2026-08-02T00:00:00+00:00")
    run_pass(conn, "run-2", profile_doc)
    deliver(conn, "run-3", {"gh:acme": [record("gh:acme", title="Support Engineer")]},
            requested_at="2026-08-03T00:00:00+00:00")

    posting_id = only_posting(conn)
    assert runstore.dirty_posting_ids(conn, "run-3") == [posting_id]

    third = run_pass(conn, "run-3", profile_doc)
    assert (third["selected"], third["scored"], third["reused"]) == (1, 0, 1)
    assert conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0] == 2


def test_a_profile_edit_forces_a_full_pass_and_rescores_the_corpus(conn, profile_doc):
    """A different `profile_version_id` means every stored tier was produced against
    candidate preferences that no longer exist. An incremental pass would leave the
    untouched majority of the corpus scored against a profile that is gone.
    """
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme", req_id=f"R-{n}",
                                               url=f"https://acme.example/{n}")
                                        for n in range(3)]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)

    edited = json.loads(json.dumps(profile_doc))
    edited["location"]["bay_area_cities"].append("emeryville")

    deliver(conn, "run-2", {"gh:acme": [record("gh:acme", req_id=f"R-{n}",
                                               url=f"https://acme.example/{n}")
                                        for n in range(3)]},
            requested_at="2026-08-02T00:00:00+00:00")
    second = run_pass(conn, "run-2", edited)

    assert second["mode"] == "full"
    assert second["reasons"] == ["profile_changed"]
    assert second["scored"] == 3, "every posting is rescored, not just the changed ones"
    # A different profile is a different axis, not a supersession: both rows stay
    # current, one per profile version.
    assert second["superseded"] == 0
    assert conn.execute("SELECT COUNT(*) FROM score_versions").fetchone()[0] == 6


def test_a_scorer_edit_forces_a_full_pass(conn, profile_doc, monkeypatch):
    """Same argument for the CODE axis. `scorer_hash` mixes RUBRIC_VERSION with the
    pinned source digest, so this fires on an edit whether or not anyone bumped the
    string.
    """
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)

    deliver(conn, "run-2", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-02T00:00:00+00:00")
    moved = scoring.ScorerIdentity(
        rubric_version="rubric-testing-v99", source_digest="deadbeef",
        scorer_hash="scorer-hash-that-moved",
    )
    second = run_pass(conn, "run-2", profile_doc, scorer=moved)

    assert second["mode"] == "full"
    assert second["reasons"] == ["scorer_changed"]
    assert second["scored"] == 1


# --------------------------------------------------------------------------- #
# 4. The baseline licence (self-healing)
# --------------------------------------------------------------------------- #
def test_a_pass_that_died_is_redone_rather_than_trusted(conn, profile_doc):
    """SELF-HEALING, and the reason the baseline is licensed by a COMPLETED pass
    rather than by any pass at all.

    Run 2's pipeline_run SUCCEEDS but its scoring pass dies. Run 3's dirty set is
    therefore measured against run 2 and is EMPTY -- the content moved during run 2,
    not run 3. If any pass licensed INCREMENTAL, run 3 would find nothing to do and
    the change run 2 saw would be lost silently and forever. Because only a
    COMPLETED pass licenses it, run 3 goes FULL and scores it.
    """
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme", title="Support Engineer")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)

    deliver(conn, "run-2", {"gh:acme": [record("gh:acme", title="Cloud Support Engineer")]},
            requested_at="2026-08-02T00:00:00+00:00")
    opener = graph.OpenPass(
        run_uid="run-2", at=AT, profile_doc=profile_doc, scorer=scoring.scorer_identity()
    )
    opener.apply(conn)  # opened, never closed: the process died here
    assert pass_row(conn, "run-2")["status"] == graph.PASS_RUNNING

    deliver(conn, "run-3", {"gh:acme": [record("gh:acme", title="Cloud Support Engineer")]},
            requested_at="2026-08-03T00:00:00+00:00")
    assert runstore.dirty_posting_ids(conn, "run-3") == [], (
        "run 2 consumed the change as far as runstore is concerned"
    )
    assert graph.baseline_run_uid(conn, "run-3") == "run-2"
    assert graph.baseline_pass(conn, "run-3") is None, "a running pass licenses nothing"

    third = run_pass(conn, "run-3", profile_doc)
    assert third["mode"] == "full"
    assert third["reasons"] == ["no_baseline_pass"]
    assert third["scored"] == 1, "the change run 2 saw is re-emitted, not lost"


def test_a_completed_pass_on_the_baseline_run_licenses_incremental(conn, profile_doc):
    """The other side of the same rule: when the baseline run DID complete a pass
    under the same identity, the next run is incremental."""
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)
    assert pass_row(conn, "run-1")["status"] == graph.PASS_COMPLETED

    deliver(conn, "run-2", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-02T00:00:00+00:00")
    licence = graph.baseline_pass(conn, "run-2")
    assert licence is not None and licence.run_uid == "run-1"
    assert run_pass(conn, "run-2", profile_doc)["mode"] == "incremental"


def test_a_pass_row_records_what_it_did(conn, profile_doc):
    """The pass is evidence, not a log line: mode, reasons, and counts survive the
    process that produced them."""
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-01T00:00:00+00:00")
    report = run_pass(conn, "run-1", profile_doc)

    row = pass_row(conn, "run-1")
    assert row["mode"] == "full" and row["status"] == graph.PASS_COMPLETED
    assert (row["selected"], row["scored"], row["reused"]) == (1, 1, 0)
    assert row["finished_at"] == AT
    stored = json.loads(row["report_json"])
    assert stored["reasons"] == ["no_baseline_pass"]
    assert stored["scorer_hash"] == report["scorer_hash"]
    # And the run finally names the profile it was scored against (NULL on every
    # scheduler run since migration 6 declared the column).
    assert conn.execute(
        "SELECT profile_version_id FROM pipeline_runs WHERE run_uid='run-1'"
    ).fetchone()[0] == report["profile_version_id"]


# --------------------------------------------------------------------------- #
# 5. Description arrival supersedes
# --------------------------------------------------------------------------- #
def _add_description(conn, posting_id, body, *, at=AT):
    conn.execute(
        "INSERT INTO descriptions (description_id, posting_id, provenance_hash, "
        "content_hash, fetch_status, body, fetched_at) VALUES (?,?,?,?,'available',?,?)",
        (runstore.new_uid(), posting_id, runstore.new_uid(),
         hashlib.sha256(body.encode("utf-8")).hexdigest(), body, at),
    )


def test_a_description_arriving_later_supersedes_the_capped_score(conn, profile_doc):
    """The hole `input_hash` closes.

    A posting scraped before its description arrives is capped at
    `no_desc_cap_tier` by rule zero. The description then arrives -- which is a
    `descriptions` row, NOT a new posting version, because the source said nothing
    new. Keyed on (version, profile, scorer) alone, that posting is already scored
    and stays capped forever. Keyed with the INPUT, it is a different input, so it
    gets a new row and the old one is superseded.
    """
    # The title alone carries a tier-2 domain hit, so this row scores above the
    # no-description cap and rule zero has something to actually cap.
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme", title="Azure Support Engineer")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)
    posting_id = only_posting(conn)
    capped = current_scores(conn)[posting_id]
    stored = json.loads(conn.execute(
        "SELECT features_json FROM score_versions WHERE superseded_at IS NULL"
    ).fetchone()[0])
    assert "cap_no_desc" in stored["score_row"], "rule zero must actually have fired"

    _add_description(
        conn, posting_id,
        "Support enterprise customers on Azure, Intune, Entra ID, Active Directory, "
        "M365, ServiceNow, Kubernetes and observability tooling. 2 years of experience. "
        "Salary range $150,000 - $185,000 USD.",
    )
    # The description is not dirt: no source said anything, so the posting is only
    # in the work list because an invalidation put it there.
    resolver.emit_invalidation(
        conn, posting_id=posting_id, reason="description-arrived", run_uid="run-1", at=AT
    )
    deliver(conn, "run-2", {"gh:acme": [record("gh:acme", title="Azure Support Engineer")]},
            requested_at="2026-08-02T00:00:00+00:00")

    second = run_pass(conn, "run-2", profile_doc)
    assert (second["scored"], second["superseded"]) == (1, 1)

    rows = score_rows(conn)
    assert len(rows) == 2
    # Selected by SUPERSESSION, not by position: both rows carry the same
    # `created_at` (one pass wrote both), so `score_rows`' tiebreak falls through to
    # a content-derived uuid5 and which one sorts first is a coin flip that depends
    # on the description's hash. Unpacking the list would pass or fail on the
    # fixture text rather than on the behaviour.
    old = next(r for r in rows if r["superseded_at"] is not None)
    new = next(r for r in rows if r["superseded_at"] is None)
    assert old["superseded_at"] == AT and old["superseded_by"] == new["score_version_id"]
    assert new["superseded_by"] is None
    assert new["input_hash"] != old["input_hash"]
    assert new["posting_version_id"] == old["posting_version_id"], (
        "the SOURCE said nothing new -- this is the same posting version"
    )
    assert new["tier"] > capped[0], "the cap is gone, so the tier actually moved"
    assert len(current_scores(conn)) == 1


def test_exactly_one_current_score_is_enforced_by_the_index(conn, profile_doc):
    """`uq_score_versions_current` is the literal guarantee, and it is enforced by
    the DATABASE rather than by the code that happens to write it. A second current
    row for one (version, profile, scorer) is rejected.
    """
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)
    row = score_rows(conn)[0]

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
            "profile_version_id, score_hash, scorer_hash, input_hash, tier, created_at) "
            "SELECT 'duplicate', posting_id, posting_version_id, profile_version_id, "
            "'other-score-hash', scorer_hash, 'other-input', tier, created_at "
            "FROM score_versions WHERE score_version_id=?",
            (row["score_version_id"],),
        )
    conn.rollback()


# --------------------------------------------------------------------------- #
# 6. A canonical match changes the tier
# --------------------------------------------------------------------------- #
def test_resolving_an_aggregator_onto_a_board_moves_the_survivors_tier(conn, profile_doc):
    """The point of resolving at all.

    The aggregator's mirror is UNDATED, so the ghost-listing rule caps it however
    good the job is. The board's record of the same job is dated. After the match
    the survivor's canonical selection sees the board's DIRECT entry -- which
    outranks the aggregator's -- the undated cap stops firing, and the tier MOVES.
    A resolution that only rewrote bookkeeping would rescore to the same number.
    """
    body = ("Support enterprise customers on Azure, Intune, Entra ID, Active Directory, "
            "M365, ServiceNow, Kubernetes and observability tooling. 2 years of "
            "experience. Salary range $150,000 - $185,000 USD.")
    mirror = record(
        "jobspy:indeed", title="Azure Support Engineer", req_id=None,
        url="https://indeed.example.com/viewjob?jk=1", posted="",
    )
    board = record(
        "gh:acme", title="Azure Support Engineer", req_id="R-1",
        url="https://boards.example.com/acme/1",
    )

    deliver(conn, "run-1", {"jobspy:indeed": [mirror]},
            requested_at="2026-08-01T00:00:00+00:00")
    mirror_posting = only_posting(conn)
    _add_description(conn, mirror_posting, body)
    first = run_pass(conn, "run-1", profile_doc)
    assert first["scored"] == 1
    mirror_tier = current_scores(conn)[mirror_posting][0]
    mirror_features = json.loads(conn.execute(
        "SELECT features_json FROM score_versions WHERE superseded_at IS NULL"
    ).fetchone()[0])["score_row"]
    assert "cap_undated_aggregator" in mirror_features, (
        "an undated aggregator posting must be ghost-capped -- that is the premise"
    )

    deliver(conn, "run-2", {"gh:acme": [board], "jobspy:indeed": [mirror]},
            requested_at="2026-08-02T00:00:00+00:00")
    survivor_candidates = [
        r["posting_id"] for r in conn.execute(
            "SELECT posting_id FROM postings WHERE posting_id <> ?", (mirror_posting,)
        )
    ]
    assert len(survivor_candidates) == 1
    _add_description(conn, survivor_candidates[0], body)

    second = run_pass(conn, "run-2", profile_doc)

    assert second["resolved"] == 1
    survivor = conn.execute(
        "SELECT to_posting_id FROM posting_redirects WHERE from_posting_id=?",
        (mirror_posting,),
    ).fetchone()["to_posting_id"]
    assert survivor == survivor_candidates[0] != mirror_posting

    scores = current_scores(conn)
    assert survivor in scores
    assert scores[survivor][0] > mirror_tier, "the match has to move the tier"
    survivor_features = json.loads(conn.execute(
        "SELECT features_json FROM score_versions WHERE superseded_at IS NULL "
        "AND posting_id=?", (survivor,),
    ).fetchone()[0])["score_row"]
    assert "cap_undated_aggregator" not in survivor_features
    assert "cap_no_desc" not in survivor_features
    # The redirected posting is not deleted and not rescored: its identity now
    # resolves elsewhere.
    assert conn.execute(
        "SELECT COUNT(*) FROM postings WHERE posting_id=?", (mirror_posting,)
    ).fetchone()[0] == 1
    # One match, one invalidation -- and it is consumed by the pass that completed.
    assert conn.execute(
        "SELECT COUNT(*) FROM score_invalidations"
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM score_invalidations WHERE consumed_at IS NULL"
    ).fetchone()[0] == 0


def test_a_redirected_posting_is_skipped_rather_than_scored(conn, profile_doc):
    """Not deleted, not hidden: it simply stops being the thing a score is about."""
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme", req_id="R-1")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)
    survivor = only_posting(conn)

    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES ('loser','active',?,?)", (AT, AT),
    )
    resolver.record_redirect(
        conn, from_posting_id="loser", to_posting_id=survivor, reason="test", at=AT
    )
    rows, skipped = graph.build_work_rows(conn, ["loser", survivor], category_of=category_of)
    assert [r.posting_id for r in rows] == [survivor]
    assert skipped == ["loser"]


# --------------------------------------------------------------------------- #
# The enrichment slot
# --------------------------------------------------------------------------- #
def test_the_default_enrichment_stage_does_nothing_and_scores_anyway(conn, profile_doc):
    """`null_enrichment` is a correct stage, not a stub: a posting with no
    description scores with `description_identity=""`, which is a scored FACT (rule
    zero caps its tier) rather than a missing input.
    """
    assert graph.null_enrichment(conn, []) == {}
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme")]},
            requested_at="2026-08-01T00:00:00+00:00")
    report = run_pass(conn, "run-1", profile_doc, enrichment=graph.null_enrichment)
    assert report["scored"] == 1


def test_an_enrichment_stage_supplies_the_description_the_score_is_keyed_on(conn, profile_doc):
    """The seam Phase 3.2 plugs into, exercised with a stand-in enricher so the slot
    is proven wired rather than merely declared."""
    deliver(conn, "run-1", {"gh:acme": [record("gh:acme", title="Technical Support Engineer")]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)
    posting_id = only_posting(conn)
    before = current_scores(conn)[posting_id]

    body = ("Support enterprise customers on Azure, Intune, Entra ID, Active Directory, "
            "M365, ServiceNow, Kubernetes and observability tooling. 2 years of "
            "experience. Salary range $150,000 - $185,000 USD.")

    def enricher(conn_, items):
        return {
            item.posting_id: graph.EnrichmentOutcome(
                description=body, identity="digest-1", status="available"
            )
            for item in items
        }

    resolver.emit_invalidation(
        conn, posting_id=posting_id, reason="description-arrived", run_uid="run-1", at=AT
    )
    deliver(conn, "run-2", {"gh:acme": [record("gh:acme", title="Technical Support Engineer")]},
            requested_at="2026-08-02T00:00:00+00:00")
    report = run_pass(conn, "run-2", profile_doc, enrichment=enricher)

    assert (report["scored"], report["superseded"]) == (1, 1)
    assert current_scores(conn)[posting_id][0] > before[0]
    assert isinstance(enricher, graph.EnrichmentStage), "the slot is a protocol, not a base class"


# --------------------------------------------------------------------------- #
# Feature contract at the write boundary
# --------------------------------------------------------------------------- #
def test_a_persisted_vector_reconstructs_the_stored_tier(conn, profile_doc):
    """End to end: what the database holds is enough to re-derive what it claims."""
    import rubric

    deliver(conn, "run-1", {"gh:acme": [record("gh:acme", req_id=f"R-{n}",
                                               url=f"https://acme.example/{n}")
                                        for n in range(4)]},
            requested_at="2026-08-01T00:00:00+00:00")
    run_pass(conn, "run-1", profile_doc)

    rows = conn.execute(
        "SELECT tier, features_json FROM score_versions WHERE superseded_at IS NULL"
    ).fetchall()
    assert rows
    for row in rows:
        features = json.loads(row["features_json"])["score_row"]
        assert set(features) <= candidate_profile.REQUIRED_SCORE_ROW_FEATURES
        assert rubric.reconstruct_tier(features) == row["tier"]


# --------------------------------------------------------------------------- #
# Query plans
# --------------------------------------------------------------------------- #
#: Tables whose size grows with the corpus. A SCAN of any of them costs the whole
#: corpus on every pass, and stays invisible in a toy database until the day it is
#: not. The small ones (`pipeline_runs`, `score_passes`, `profile_versions`) are
#: deliberately excluded: they hold one row per run or per profile edit, and
#: forcing an index onto a ten-row table measures nothing.
LARGE_TABLES = (
    "run_postings", "posting_versions", "postings", "posting_aliases",
    "score_versions", "descriptions", "posting_redirects", "score_invalidations",
)


def _traced(conn, fn):
    """Every SQL statement `fn` issues, with its parameters already substituted.

    Captured rather than transcribed: a plan assertion against a hand-copied query
    proves something about the copy, and drifts the first time the real statement
    changes.
    """
    seen: list[str] = []
    conn.set_trace_callback(seen.append)
    try:
        fn()
    finally:
        conn.set_trace_callback(None)
    return seen


def _write_corpus(conn, *, runs=20, per_run=150, sources=6):
    """A database big enough -- and shaped enough like a real one -- to plan against.

    Both axes matter, for `test_source_posting_versions.py`'s reasons: `sources`
    keeps `posting_versions.source_run_id` selective, and `runs` keeps any single
    run a small slice of `run_postings` (with two runs, half the table matches and
    the post-ANALYZE planner is right to scan it).
    """
    namespaces = ["gh:acme", "lever:acme", "yc", "manual:dice", "jobspy:indeed", "builtin"]
    per_source = max(1, per_run // sources)
    for index in range(runs):
        run_uid = f"run-{index:02d}"
        deliver(
            conn, run_uid,
            {
                namespaces[s]: [
                    record(
                        namespaces[s], req_id=f"{s}-{n}",
                        url=f"https://{s}.example/{n}",
                        title=f"Support Engineer {n}",
                        company=f"Acme {n % 17}",
                        salary=f"${100 + index}k",
                    )
                    for n in range(per_source)
                ]
                for s in range(sources)
            },
            requested_at=f"2026-08-{index + 1:02d}T00:00:00+00:00",
        )
    conn.commit()


def test_no_query_on_the_scoring_path_scans_a_large_table(conn, profile_doc):
    """The roadmap's "never scan all postings after each source", asserted against
    the query planner rather than a stopwatch, on the REAL statements this phase
    issues: the corpus enumeration, the state lookup, the redirect lookups, the
    version and description prefetches, the score anti-join, and the resolver's
    alias queries.

    `ANALYZE` runs first, and that is load-bearing: several of these plans change
    once `sqlite_stat1` exists, so a query that is only fast on a statistics-free
    database is a regression waiting for whoever runs ANALYZE first.
    """
    _write_corpus(conn)
    scorer = scoring.scorer_identity()
    profile_version_id = scoring.upsert_profile_version(conn, profile_doc, at=AT)
    conn.commit()

    page = graph.select_work(conn, run_uid="run-19", mode=graph.PassMode.FULL, limit=200)
    assert page, "the corpus enumeration must actually return work"

    statements = _traced(conn, lambda: (
        graph.select_work(conn, run_uid="run-19", mode=graph.PassMode.FULL, limit=200),
        graph.select_work(conn, run_uid="run-19", mode=graph.PassMode.INCREMENTAL, limit=200),
        graph.build_work_rows(conn, page, category_of=category_of),
        scoring.current_score_inputs(
            conn, [f"v-{n}" for n in range(50)],
            profile_version_id=profile_version_id, scorer_hash=scorer.scorer_hash,
        ),
        resolver.direct_observations(conn, run_uid="run-19", category_of=category_of),
        resolver.open_invalidations(conn, limit=100),
        resolver.bridge_legacy_url_aliases(conn, run_uid="run-19", at=AT),
    ))
    conn.rollback()
    conn.execute("ANALYZE")

    plans = {}
    for sql in statements:
        if not sql.lstrip().upper().startswith("SELECT"):
            continue
        if not any(table in sql for table in LARGE_TABLES):
            continue
        plans[sql] = [row["detail"] for row in conn.execute("EXPLAIN QUERY PLAN " + sql)]

    assert len(plans) >= 7, sorted(plans)
    for sql, details in plans.items():
        # SQLite names the ALIAS, not the table ("SEARCH rp ..."), so this asserts
        # on the verb. A "SCAN (subquery-N)" line is the co-routine feeding a window
        # function and is bounded by the rows that fed it.
        scans = [d for d in details if d.startswith("SCAN ") and "subquery" not in d]
        assert not scans, (sql, details)
        searches = [d for d in details if d.startswith("SEARCH ")]
        assert searches, (sql, details)
        # "USING INDEX", "USING COVERING INDEX", and "USING PRIMARY KEY" all mean the
        # same thing here: the row was reached through a b-tree, not by reading the
        # table.
        assert all("INDEX" in d or "PRIMARY KEY" in d for d in searches), (sql, details)

    # And specifically: the "score exactly once" anti-join rides the partial unique
    # index whose leading column exists for it.
    anti_join = [d for sql, d in plans.items() if "score_versions" in sql]
    assert anti_join and any(
        "uq_score_versions_current" in line for line in anti_join[0]
    ), anti_join


def test_a_full_pass_reads_the_corpus_in_pages(conn, profile_doc):
    """Cursor pagination, not a materialised corpus: the FULL work list is read in
    `batch_size` pages in index order, and the pass still scores every posting
    exactly once.
    """
    _write_corpus(conn, runs=3, per_run=60, sources=6)
    report = run_pass(conn, "run-02", profile_doc, batch_size=25)

    postings = conn.execute("SELECT COUNT(*) FROM postings").fetchone()[0]
    assert report["mode"] == "full"
    assert report["selected"] == postings
    assert report["scored"] + report["skipped"] == postings
    assert conn.execute(
        "SELECT COUNT(*) FROM score_versions WHERE superseded_at IS NULL"
    ).fetchone()[0] == report["scored"]
