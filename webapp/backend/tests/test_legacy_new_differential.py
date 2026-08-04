"""Phase 3.7: legacy/new differential -- the same postings scored through TWO
call paths, asserted identical, with a reviewed allowlist for anything that is
allowed to differ.

THE TWO PATHS.

  LEGACY    `rubric.score_row(r, desc)` then `rubric.hireability(r2, desc)` with
            `r2["flags"]` set from `score_row`'s OWN output flags before
            `hireability` reads it -- exactly the two lines `rubric.cmd_score()`
            runs (`sweep.py` invokes `python rubric.py score`, i.e. this
            function, on every sweep).
  CANONICAL `scoring.persist_scores`, reached here through `graph.run_pass` over
            a `postings`/`posting_versions`/`score_versions` schema seeded with
            the SAME rows via `runstore.write_records` -- the path
            `webapp/backend/sources/scoring.py` documents as calling
            `rubric.score_row_explained(dict(item.row), ...)` and
            `rubric.hireability_explained(dict(item.row), ...)` on two SEPARATE
            copies of `item.row`, which always carries `flags=""`.

THE ALLOWLIST. `ALLOWLIST` below is a set of case names permitted to differ. It
is EMPTY, and additions require orchestrator signoff -- do not add a name here
to make a failing case pass; that defeats the point of the differential. Two
confirmed, real divergences between the two paths are demonstrated in their own
functions further down (`test_confirmed_divergence_*`), NOT folded into the main
corpus or the allowlist, because they are not "acceptable differences" to wave
through silently -- they are bugs, reported and left for the orchestrator/a human
to decide whether the wave-2 legacy path or the canonical path (or neither) is
correct.

WHY THE MAIN CORPUS AVOIDS THEM. Both divergences are real and reproducible (see
their tests), but folding rows that trigger them into the "must be identical"
loop would force EITHER a same-day unreviewed allowlist entry (against this
file's own rule) OR a corpus that can never assert 0 diffs. The main corpus
below is built to exercise everything else the roadmap line asks for --
representative tiers, labels, blockers, comp bands, staleness -- while staying
on the SAFE side of both known asymmetries (DIRECT-category postings only, no
`Staffing/W2`/`degree-gated` flags), which is what lets it make the strong claim
"identical for every posting" instead of "identical modulo known bugs".
"""
from __future__ import annotations

import datetime
import json
import os
import sys

import pytest

from backend.sources import graph, runstore
from backend.sources.contract import NormalizedPosting, SourceCategory
from backend.tests.test_source_scheduler_fakes import make_connect

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede these)
import rubric  # noqa: E402

AT = "2026-08-04T12:00:00+00:00"
TODAY = datetime.date.today().isoformat()

PROFILE_PATH = os.path.join(_REPO_ROOT, "profile.example.json")

#: Case names permitted to differ between the two paths. See the module
#: docstring: additions require orchestrator signoff, and the two currently-known
#: divergences are demonstrated separately rather than allowlisted here.
ALLOWLIST: frozenset[str] = frozenset()

#: All main-corpus postings are DIRECT, so `category_of` never has to decide an
#: aggregator/direct split -- that split is exactly what one of the two known
#: divergences is about (see `test_confirmed_divergence_undated_aggregator_source_string_sniff`).
NAMESPACE = "greenhouse:acme"


def category_of(namespace: str) -> SourceCategory:
    return SourceCategory.DIRECT


@pytest.fixture(scope="module")
def profile_doc():
    with open(PROFILE_PATH) as f:
        return json.load(f)


@pytest.fixture(autouse=True)
def _use_example_profile(profile_doc, monkeypatch):
    """Both paths must score against the SAME profile, and it must be
    `profile.example.json` (tracked, non-personal) rather than whatever
    `profile.json` happens to exist on this machine -- the canonical path
    already takes `profile_doc` explicitly (`graph.run_pass`'s argument), but
    the legacy path reads it through `rubric._rprofile()`'s process-wide cache,
    which this monkeypatch is what points at the same document instead of the
    real, gitignored `profile.json` (or nothing, on a clean checkout)."""
    monkeypatch.setattr(rubric, "_RPROFILE_CACHE", candidate_profile.build_profile(profile_doc))


@pytest.fixture
def conn(tmp_path):
    connect = make_connect(tmp_path)
    c = connect()
    try:
        yield c
    finally:
        c.close()


# --------------------------------------------------------------------------- #
# Row construction, shared between the two paths
# --------------------------------------------------------------------------- #
DESC_RICH = (
    "You will need example skill one and example skill two for this role. "
    "This role heavily uses exampletool and example platform name every single day, "
    "which is our exact daily stack for engineers on this team working across many projects. "
    "We value example industry term knowledge and example certification credentials too. "
    "The team ships features constantly and this description exists to pad the length past "
    "four hundred characters so the skills-match gate actually evaluates the resume overlap "
    "for this particular candidate profile, exercising the exact-stack bonus rule as well."
)
DESC_MODERATE_MATCH = (
    "We need example skill one experience and example skill two experience for "
    "this example title one role, shipping product improvements every single "
    "sprint with a cross-functional team that cares deeply about quality and "
    "reliability across the whole stack, with plenty of collaboration daily "
    "across many timezones and many stakeholders who all depend on this team "
    "to deliver consistently well past the four hundred character mark today."
)
DESC_NO_SKILLS_LONG = "Example Title One role. " * 20


class Posting:
    """One synthetic posting, described once and scored both ways from the same
    fields -- so a difference in the two results can only come from the
    SCORING PATH, never from a field that was populated differently."""

    def __init__(self, name, *, title="Example Title One", company="Acme Robotics",
                 location="Example Bay City One, CA", salary="", posted="",
                 remote=False, req_id=None, description=None):
        self.name = name
        self.title = title
        self.company = company
        self.location = location
        self.salary = salary
        self.posted = posted
        self.remote = remote
        self.req_id = req_id or name
        self.url = f"https://acme.example/{name}"
        self.description = description

    def legacy_row(self) -> dict:
        return {
            "title": self.title, "company": self.company, "location": self.location,
            "salary": self.salary, "salary_min": "", "salary_max": "",
            "posted": self.posted, "remote": "true" if self.remote else "false",
            "source": NAMESPACE, "req_id": self.req_id, "flags": "",
        }

    def normalized_posting(self) -> NormalizedPosting:
        source_key, _, instance = NAMESPACE.partition(":")
        return NormalizedPosting(
            source_key=source_key, instance_key=instance, title=self.title,
            company=self.company, url=self.url, req_id=self.req_id,
            location=self.location, posted_date=self.posted or None,
            salary_text=self.salary, remote=self.remote,
        )


#: The main corpus. Scenario names echo `test_scoring_goldens.py`'s where the
#: same situation applies (reusing the SHAPE of those rows, per the roadmap
#: line's "reuse golden rows where sensible") -- not the literal golden dicts,
#: because a golden row's `salary_min`/`salary_max` fields have no canonical
#: equivalent: `runstore.write_records` never parses `salary_text` into numeric
#: columns (see `runstore._link_source_version`'s docstring, "salary_min/
#: salary_max -- parsing, likewise downstream"), so `posting_versions.salary_min`
#: is always NULL and `scoring.row_from_version` always reads salary from the
#: TEXT column. Every comp-band case below therefore supplies salary as
#: parseable text, which both paths parse identically via `scraper.parse_salary`.
CORPUS = [
    Posting("tier5_target_employer_in_band_comp", company="Examplecorp",
            salary="$90,000 - $110,000", description=DESC_RICH),
    Posting("tier1_family_b_years_penalty", title="Example Title Three",
            description="6+ years of experience required for example title three "
                        "role on our team."),
    Posting("tier2_family_b_remote", title="Example Title Three",
            location="Remote - US", remote=True,
            description="Short desc under 400 chars, azure adjacent."),
    Posting("staff_capped_level_stretch", title="Staff Example Title One",
            description=DESC_RICH),
    Posting("stale_90d_ghost_cap", posted="2024-01-01", description=DESC_RICH),
    Posting("comp_in_band", salary="$90,000 - $100,000", description=DESC_RICH),
    Posting("low_comp_band", salary="$30,000 - $50,000", description=DESC_RICH),
    Posting("hireability_high_competition_employer", company="Bigtechco",
            description=DESC_RICH),
    Posting("hireability_comp_high_bar", salary="$160,000 - $180,000",
            description=DESC_RICH),
    Posting("hireability_senior_level_gap", title="Senior Example Title One",
            description=DESC_RICH),
    Posting("hireability_junior_level_gap", title="Junior Example Title One",
            description=DESC_RICH),
    Posting("hireability_weak_match_no_skills", description=DESC_NO_SKILLS_LONG),
    Posting("hireability_moderate_match", description=DESC_MODERATE_MATCH),
    Posting("no_description_cap", salary="$90,000 - $100,000", description=None),
    Posting("blocked_non_us_location", location="Examplestan", description=DESC_RICH),
    Posting("blocked_off_target_location", location="Austin, TX", description=DESC_RICH),
    Posting("blocked_active_clearance",
            description="must currently hold an active top secret clearance. "
                        "example title one role."),
    Posting("blocked_title_exclude", title="Example Title One example excluded term one",
            description=DESC_RICH),
    Posting("blocked_people_management", title="Example Title One Manager",
            description=DESC_RICH),
    Posting("blocked_years_required_too_high",
            description="8+ years of experience required for example title one. " + DESC_RICH),
    Posting("blocked_no_role_family", title="Totally Unrelated Title", description=DESC_RICH),
    Posting("blocked_off_focus_role", title="Example Title Four", description=DESC_RICH),
]

assert {p.name for p in CORPUS} & ALLOWLIST == set(), (
    "every corpus row is expected to match -- an allowlisted name belongs out "
    "of the main corpus, in its own test, not silently included here"
)


# --------------------------------------------------------------------------- #
# The two scoring paths
# --------------------------------------------------------------------------- #
def score_legacy(posting: Posting) -> dict:
    """Exactly `rubric.cmd_score`'s two scoring lines."""
    row = posting.legacy_row()
    tier, why, flags = rubric.score_row(dict(row), posting.description)
    row2 = dict(row)
    row2["tier"] = tier
    row2["why"] = why
    row2["flags"] = ", ".join(flags)
    odds_label, odds_score, odds_why = rubric.hireability(row2, posting.description)
    return {"tier": tier, "odds_label": odds_label, "odds_score": odds_score}


def score_canonical(conn, profile_doc, postings: list[Posting]) -> dict[str, dict]:
    """Every posting delivered in ONE run, scored in ONE pass, read back by name."""
    runstore.create_pipeline_run(conn, run_uid="run-1", kind="daily", requested_at=AT, started_at=AT)
    runstore.create_source_run(conn, source_run_id="run-1-0", run_uid="run-1", source=NAMESPACE, attempt=1)
    runstore.write_records(
        conn, run_uid="run-1", source_run_id="run-1-0", recorded_at=AT,
        records=[p.normalized_posting() for p in postings],
    )
    runstore.finish_source_run(conn, source_run_id="run-1-0", status="succeeded", finished_at=AT)
    conn.execute("UPDATE pipeline_runs SET status='succeeded' WHERE run_uid='run-1'")

    by_req = {
        row["req_id"]: row["posting_id"]
        for row in conn.execute(
            "SELECT req_id, posting_id FROM posting_aliases "
            "WHERE alias_kind='source_req' AND namespace=? AND valid_to IS NULL",
            (NAMESPACE,),
        )
    }
    for p in postings:
        if p.description is not None:
            posting_id = by_req[p.req_id]
            conn.execute(
                "INSERT INTO descriptions (description_id, posting_id, provenance_hash, "
                "content_hash, fetch_status, body, fetched_at) VALUES (?,?,?,?,'available',?,?)",
                (runstore.new_uid(), posting_id, runstore.new_uid(),
                 runstore.canonical_json(p.description), p.description, AT),
            )

    graph.run_pass(conn, run_uid="run-1", profile_doc=profile_doc, at=AT, category_of=category_of)

    results: dict[str, dict] = {}
    for p in postings:
        posting_id = by_req[p.req_id]
        row = conn.execute(
            "SELECT tier, odds, odds_score FROM score_versions "
            "WHERE posting_id=? AND superseded_at IS NULL", (posting_id,),
        ).fetchone()
        results[p.name] = {"tier": row["tier"], "odds_label": row["odds"], "odds_score": row["odds_score"]}
    return results


# --------------------------------------------------------------------------- #
# The differential
# --------------------------------------------------------------------------- #
def test_legacy_and_canonical_scoring_agree_over_the_corpus(conn, profile_doc):
    legacy = {p.name: score_legacy(p) for p in CORPUS}
    canonical = score_canonical(conn, profile_doc, CORPUS)

    assert set(legacy) == set(canonical) == {p.name for p in CORPUS}

    mismatches = []
    for name in legacy:
        if name in ALLOWLIST:
            continue
        if legacy[name] != canonical[name]:
            mismatches.append((name, legacy[name], canonical[name]))
    assert mismatches == [], (
        "legacy and canonical scoring disagree on postings not in ALLOWLIST -- "
        "additions to ALLOWLIST require orchestrator signoff (see module docstring); "
        f"mismatches: {mismatches}"
    )


def test_the_corpus_covers_a_representative_tier_and_label_spread():
    """The differential is only meaningful if the corpus actually varies -- this
    is the coverage-matrix half of the roadmap line, checked against the LEGACY
    path (which the canonical path was just proven identical to, above)."""
    tiers = set()
    match_labels = set()
    for p in CORPUS:
        result = score_legacy(p)
        tiers.add(result["tier"])
        # match_label is the part before " / " in the combined label.
        match_labels.add(result["odds_label"].split(" / ")[0])
    assert tiers >= {0, 1, 2, 3, 4, 5}, tiers
    assert match_labels >= {"Strong match", "Weak match", "Moderate match", "Unscored"}, match_labels


# --------------------------------------------------------------------------- #
# Confirmed divergences -- NOT allowlisted, NOT silently fixed, reported here.
# --------------------------------------------------------------------------- #
def test_confirmed_divergence_undated_aggregator_source_string_sniff(conn, profile_doc):
    """PRE-EXISTING, ALREADY-DOCUMENTED divergence (Phase 3.3;
    `test_scoring_features.py::test_the_undated_aggregator_cap_fires_on_the_explicit_flag_not_a_source_string`
    proves the same fact against the pure functions). Reconfirmed here end to end
    through both full call paths, not fixed: `rubric.cmd_score()` calls
    `score_row(r, desc)` with NO `is_aggregator` argument, so the undated-ghost cap
    falls back to sniffing `r["source"]` against the legacy prefix tuple
    `AGG_SOURCES = ("jobspy-", "mcp-", "builtin", "yc-jobs")`. Canonical
    `posting_versions.source` carries the NormalizedPosting NAMESPACE
    ("jobspy:indeed"), which matches none of those legacy prefixes -- so an
    undated posting from a canonically-named aggregator source scores UNCAPPED
    under the legacy path and CAPPED under the canonical path, which explicitly
    passes `is_aggregator=True` (`graph.build_work_rows`:
    `is_aggregator=category_of(namespace) is SourceCategory.AGGREGATOR`).
    """
    posting = Posting("undated_aggregator_divergence", posted="",
                       description="Working with exampletool and example platform "
                                   "name daily. " * 10)

    def category_of_aggregator(namespace: str) -> SourceCategory:
        return SourceCategory.AGGREGATOR

    legacy_row = dict(posting.legacy_row(), source="jobspy:indeed")
    legacy_tier, _why, _flags = rubric.score_row(legacy_row, posting.description)

    runstore.create_pipeline_run(conn, run_uid="run-1", kind="daily", requested_at=AT, started_at=AT)
    runstore.create_source_run(conn, source_run_id="run-1-0", run_uid="run-1", source="jobspy:indeed", attempt=1)
    runstore.write_records(
        conn, run_uid="run-1", source_run_id="run-1-0", recorded_at=AT,
        records=[NormalizedPosting(
            source_key="jobspy", instance_key="indeed", title=posting.title,
            company=posting.company, url=posting.url, req_id=posting.req_id,
            location=posting.location, posted_date=None,
        )],
    )
    runstore.finish_source_run(conn, source_run_id="run-1-0", status="succeeded", finished_at=AT)
    conn.execute("UPDATE pipeline_runs SET status='succeeded' WHERE run_uid='run-1'")
    posting_id = conn.execute(
        "SELECT posting_id FROM posting_aliases WHERE alias_kind='source_req' "
        "AND namespace='jobspy:indeed' AND value=?", (posting.req_id,),
    ).fetchone()["posting_id"]
    conn.execute(
        "INSERT INTO descriptions (description_id, posting_id, provenance_hash, "
        "content_hash, fetch_status, body, fetched_at) VALUES (?,?,?,?,'available',?,?)",
        (runstore.new_uid(), posting_id, runstore.new_uid(),
         runstore.canonical_json(posting.description), posting.description, AT),
    )
    graph.run_pass(conn, run_uid="run-1", profile_doc=profile_doc, at=AT,
                    category_of=category_of_aggregator)
    canonical_tier = conn.execute(
        "SELECT tier FROM score_versions WHERE posting_id=? AND superseded_at IS NULL",
        (posting_id,),
    ).fetchone()["tier"]

    assert legacy_tier != canonical_tier, (
        "if this now passes, the legacy source-string sniff and the canonical "
        "explicit-flag path have converged -- update this test's assertion and "
        "docstring, do not delete it"
    )
    assert canonical_tier < legacy_tier, "the canonical path is the one that caps correctly"


def test_confirmed_divergence_hireability_flags_dependent_features_are_dead_canonically():
    """NEWLY FOUND by this differential, NOT fixed here -- reported to the
    orchestrator.

    `rubric._hireability_core` reads `r.get("flags", "")` to decide TWO
    contributions: `staffing_w2` (`"Staffing/W2" in flags`) and `degree_gated`
    (`"degree-gated" in flags`). Neither is derived from the description or any
    other input `hireability_explained` is given directly -- both depend entirely
    on the CALLER having already put those exact strings into the row's `flags`
    field.

    `rubric.cmd_score()` (legacy) does this on purpose: it scores with
    `score_row` first, writes the OUTPUT flags onto the row (`r2["flags"] = ",
    ".join(flags)`), and only then calls `hireability(r2, desc)` -- so a
    staffing-agency company or a degree-gated posting gets its odds axis
    penalized/labeled accordingly.

    `scoring.persist_scores` (canonical) does NOT do this: it calls
    `rubric.score_row_explained(dict(item.row), ...)` and
    `rubric.hireability_explained(dict(item.row), ...)` on two INDEPENDENT
    copies of `item.row` (`webapp/backend/sources/scoring.py`), and
    `item.row` comes from `row_from_version`, which always sets `"flags": ""`
    (`webapp/backend/sources/scoring.py`'s `row_from_version` docstring: a
    FRESH dict every time). So under the canonical path, `staffing_w2` and
    `degree_gated` can NEVER fire -- they are dead code on that path, silently,
    for every posting ever scored canonically.

    This is a real behavioral difference between the two paths on ordinary
    staffing-agency and degree-gated postings, not a synthetic edge case. Per
    this task's constraints, it is pinned here rather than fixed (fixing it
    means deciding whether `scoring.py` should chain score's flags into the
    odds call, which is a `scoring.py` change out of this task's scope) and
    reported in the implementer's summary for the orchestrator to route.
    """
    posting = Posting("staffing_agency_lower_bar", company="Example Staffing Agency",
                       description=DESC_RICH)
    legacy = score_legacy(posting)
    assert legacy["odds_label"] == "Strong match / Lower bar"
    assert legacy["odds_score"] == 4

    # The canonical call pattern, reproduced directly (no database needed to show
    # the dead branch: it is a property of how the two functions are CALLED).
    canonical_row = posting.legacy_row()  # flags="" -- what row_from_version always sets
    canonical_odds = rubric.hireability_explained(dict(canonical_row), posting.description)

    assert canonical_odds.label != legacy["odds_label"], (
        "if this now matches, scoring.py started chaining score flags into the "
        "odds call -- update this test, do not delete it"
    )
    assert canonical_odds.competition_label == "Standard", (
        "canonically, staffing_w2 never fires: 'Lower bar' is unreachable"
    )
    assert canonical_odds.score != legacy["odds_score"]
