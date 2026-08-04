"""Phase 3.7: frozen scoring goldens for `rubric.score_row_explained` /
`rubric.hireability_explained`.

WHY THIS EXISTS AND WHY IT IS NOT `test_scoring_features.py` AGAIN. That file
proves UNIVERSAL properties over a generated grid (every vector reconstructs its
own tier, the feature set is closed). This file proves something narrower and
more literal: for a FROZEN set of representative rows, against a FROZEN profile,
the scorer produces EXACTLY these tiers, these labels, these scores, and these
feature vectors -- byte for byte. A property test can stay green while the
*substance* of a score quietly drifts (a reweighted rule that still sums
correctly, a relabeled cap that still reconstructs); a golden cannot.

THE PROFILE IS `profile.example.json`, NOT `profile.json`. The real profile is
gitignored personal data (target employers, comp band, resume keywords) and does
not exist on a clean checkout. `profile.example.json` is the tracked, scrubbed
document schema-shaped identically to the real one (see `candidate_profile.py`'s
module docstring and `test_profile.py`'s pattern of reading the real file --
that pattern is for tests that need the *real* candidate's data; goldens need
only a *valid* document, and a scrubbed one keeps this file free of anything
that must never be committed).

THE CHECKPOINT. `PINNED_PROFILE_CONTENT_HASH` and `PINNED_RUBRIC_VERSION` are
asserted against the loaded profile and the running scorer before a single
golden row is checked. When either fails, `profile.example.json` or the scoring
code changed shape -- which is not a bug this file exists to catch, it is a
DELIBERATE-CHANGE CHECKPOINT: every golden below was computed against the exact
byte content and code version pinned here, and a change to either invalidates
every expected value in this file at once. The fix is to inspect the diff,
confirm the new numbers are the intended ones (re-run the case by hand, the way
this file's rows were originally produced), and re-pin both the hash/version
constants and the affected golden rows in the same change -- not to silence the
failure.

TWO CALL PATTERNS, BOTH PINNED. `GOLDEN_CASES` calls `score_row_explained` and
`hireability_explained` on SEPARATE copies of one row -- the two functions scored
in ISOLATION, which is what makes each expected value attributable to the rubric
alone. `GOLDEN_CHAINED_CASES` scores the way both real callers do, through
`scoring._score_one`: one row, threaded from the fit pass into the odds pass so
that the flags and the recovered pay band `score_row` produced are what
`hireability` reads. Phase 3.7 found the canonical path NOT doing that (two
independent copies of a row whose `flags` was always `""`), which killed
`hireability`'s `staffing_w2` and `degree_gated` contributions canonically and
made "Lower bar" unreachable for every canonically scored posting; the chained
cases are the goldens for the fix, and the two `..._via_explicit_flags` tests at
the bottom stay because they pin the PURE gate the chaining feeds.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede these)
import rubric  # noqa: E402

from backend.sources import scoring  # noqa: E402

PROFILE_PATH = os.path.join(_REPO_ROOT, "profile.example.json")

#: THE CHECKPOINT. Recomputed by loading `profile.example.json` fresh and calling
#: `candidate_profile.profile_content_hash(doc)` / reading `rubric.RUBRIC_VERSION`.
#: If a future edit to `profile.example.json` or the scorer's pinned surface moves
#: either, `test_the_pinned_checkpoint_still_matches` fails FIRST and names which
#: one moved, before any golden row is even attempted.
PINNED_PROFILE_CONTENT_HASH = (
    "350d684e82ac40e2177403bfc4d94bdd9184e947e16970eadf1c3feeb8aeff86"
)
PINNED_RUBRIC_VERSION = "rubric-2026.08-v3"

CHECKPOINT_MESSAGE = (
    "\n\nThis is a DELIBERATE-CHANGE CHECKPOINT, not a bug to silence. {what} moved, "
    "which means every golden expected value in test_scoring_goldens.py was computed "
    "against a scorer or profile that no longer exists. To resolve: inspect the diff, "
    "confirm the new behavior is the intended one, recompute the golden rows by hand "
    "(score_row_explained/hireability_explained against profile.example.json), and "
    "re-pin {constant} together with the affected expected values in the SAME change."
)


@pytest.fixture(scope="module")
def profile_doc():
    with open(PROFILE_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def golden_profile(profile_doc):
    return candidate_profile.build_profile(profile_doc)


@pytest.fixture(autouse=True)
def _use_golden_profile(golden_profile, monkeypatch):
    """Every test in this file scores against `profile.example.json`, never
    whatever `profile.json` happens to be on this machine (or its absence)."""
    monkeypatch.setattr(rubric, "_RPROFILE_CACHE", golden_profile)


def test_the_pinned_checkpoint_still_matches(profile_doc, golden_profile):
    assert golden_profile.content_hash == PINNED_PROFILE_CONTENT_HASH, CHECKPOINT_MESSAGE.format(
        what="profile.example.json's content", constant="PINNED_PROFILE_CONTENT_HASH"
    )
    assert rubric.RUBRIC_VERSION == PINNED_RUBRIC_VERSION, CHECKPOINT_MESSAGE.format(
        what="rubric.RUBRIC_VERSION", constant="PINNED_RUBRIC_VERSION"
    )
    assert rubric.scorer_source_digest() == rubric.SCORER_SOURCE_DIGEST, CHECKPOINT_MESSAGE.format(
        what="the scorer's pinned source surface", constant="rubric.SCORER_SOURCE_DIGEST"
    )


# --------------------------------------------------------------------------- #
# Row construction
# --------------------------------------------------------------------------- #
def _row(title, *, company="Acme Robotics", location="Example Bay City One, CA",
         salary="", salary_min="", salary_max="", posted="", remote="false",
         source="greenhouse:acme", req_id="R-1", flags=""):
    return {
        "title": title, "company": company, "location": location,
        "salary": salary, "salary_min": salary_min, "salary_max": salary_max,
        "posted": posted, "remote": remote, "source": source, "req_id": req_id,
        "flags": flags,
    }


#: A description long enough to clear the `len(d) > 400` skills gate, carrying
#: both `exact_stack_patterns` ("exampletool", "example platform name") -- so it
#: drives `match_label` to "Strong match" via `exact_stack` -- and a tier-2 domain
#: hit via the same "exampletool" token, and 3 of the 4 `his_skills` patterns
#: ("example skill one", "example skill two", "exampletool"), which is
#: `skills_moderate` territory (>= 3, < 6) without ever reaching `skills_strong`
#: (profile.example.json only has 4 `his_skills` patterns, so 6 is unreachable
#: with this profile -- see `test_scoring_boundaries.py` for a profile built wide
#: enough to hit that boundary).
DESC_RICH = (
    "You will need example skill one and example skill two for this role. "
    "This role heavily uses exampletool and example platform name every single day, "
    "which is our exact daily stack for engineers on this team working across many projects. "
    "We value example industry term knowledge and example certification credentials too. "
    "The team ships features constantly and this description exists to pad the length past "
    "four hundred characters so the skills-match gate actually evaluates the resume overlap "
    "for this particular candidate profile, exercising the exact-stack bonus rule as well."
)
assert len(DESC_RICH) > 400

#: >400 chars, exactly TWO `his_skills` pattern hits (not >=3, not <=1) and
#: neither `exact_stack_patterns`, so neither `skills_moderate`/`skills_thin` nor
#: `exact_stack` fires -- the only description below that lands `match_label` on
#: the residual "Moderate match" branch.
DESC_MODERATE_MATCH = (
    "We need example skill one experience and example skill two experience for "
    "this example title one role, shipping product improvements every single "
    "sprint with a cross-functional team that cares deeply about quality and "
    "reliability across the whole stack, with plenty of collaboration daily "
    "across many timezones and many stakeholders who all depend on this team "
    "to deliver consistently well past the four hundred character mark today."
)
assert len(DESC_MODERATE_MATCH) > 400

#: >400 chars, zero `his_skills` hits -- `skills_thin` territory (<= 1).
DESC_NO_SKILLS_LONG = "Example Title One role. " * 20
assert len(DESC_NO_SKILLS_LONG) > 400


# --------------------------------------------------------------------------- #
# The golden corpus
#
# Each case: (name, row, description, score_kwargs, expected_score, expected_odds).
# `expected_score`/`expected_odds` are exactly what `test_scoring_features.py`
# proved every vector must satisfy (reconstructable, closed) -- this file adds
# the literal values a property test cannot pin.
# --------------------------------------------------------------------------- #
GOLDEN_CASES = [
    (
        "tier5_target_employer_in_band_comp",
        _row("Example Title One", company="Examplecorp", salary_min="90000", salary_max="110000"),
        DESC_RICH, {},
        dict(tier=5, why="core function match; domain: exampletool,example platform name; "
             "named-target employer; Bay Area; comp in band", flags=["target-co"],
             features={"function_match": 3, "domain_tier2": 2, "target_co": 1,
                       "comp_in_band": 1, "raw_score": 7}),
        dict(label="Strong match / Standard", match_label="Strong match",
             competition_label="Standard", score=2,
             features={"skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "tier1_family_b_years_penalty",
        _row("Example Title Three"),
        "6+ years of experience required for example title three role on our team.", {},
        dict(tier=1, why="Bay Area", flags=["6yrs-required"],
             features={"function_match": 2, "years_penalty": -1, "raw_score": 1}),
        dict(label="Unscored / Standard", match_label="Unscored", competition_label="Standard",
             score=-1, features={"years_high": -1}),
    ),
    (
        "tier2_family_b_remote_moderate",
        _row("Example Title Three", location="Remote - US", remote="true"),
        "Short desc under 400 chars, azure adjacent.", {},
        dict(tier=2, why="US-remote", flags=[],
             features={"function_match": 2, "raw_score": 2}),
        dict(label="Unscored / Standard", match_label="Unscored", competition_label="Standard",
             score=0, features={}),
    ),
    (
        "staff_capped_level_stretch",
        _row("Staff Example Title One"),
        DESC_RICH, {},
        dict(tier=2, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=["level-out"],
             features={"function_match": 3, "domain_tier2": 2, "staff_cap_delta": -3, "raw_score": 2}),
        dict(label="Level stretch / Standard", match_label="Level stretch",
             competition_label="Standard", score=-1,
             features={"staff_principal": -3, "skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "undated_aggregator_ghost_cap",
        _row("Example Title One", source="jobspy:indeed", posted=""),
        DESC_RICH, {"is_aggregator": True},
        dict(tier=3, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=["undated-aggregator"],
             features={"function_match": 3, "domain_tier2": 2, "raw_score": 5,
                       "cap_undated_aggregator": 3}),
        dict(label="Strong match / Standard", match_label="Strong match",
             competition_label="Standard", score=2,
             features={"skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "stale_90d_ghost_cap",
        _row("Example Title One", posted="2024-01-01"),
        DESC_RICH, {},
        dict(tier=3, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=["30d+", "stale-90d+"],
             features={"function_match": 3, "domain_tier2": 2, "stale_30d": -1, "raw_score": 4,
                       "cap_stale": 3}),
        dict(label="Strong match / Standard", match_label="Strong match",
             competition_label="Standard", score=2,
             features={"skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "no_description_cap",
        _row("Example Title One"),
        None, {},
        dict(tier=3, why="core function match; Bay Area", flags=["desc-unavailable"],
             features={"function_match": 3, "raw_score": 3}),
        dict(label="Unscored / Standard", match_label="Unscored", competition_label="Standard",
             score=0, features={}),
    ),
    (
        "comp_in_band",
        _row("Example Title One", salary_min="90000", salary_max="100000"),
        DESC_RICH, {},
        dict(tier=5, why="core function match; domain: exampletool,example platform name; "
             "Bay Area; comp in band", flags=[],
             features={"function_match": 3, "domain_tier2": 2, "comp_in_band": 1, "raw_score": 6}),
        dict(label="Strong match / Standard", match_label="Strong match",
             competition_label="Standard", score=2,
             features={"skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "low_comp_band",
        _row("Example Title One", salary_min="30000", salary_max="50000"),
        DESC_RICH, {},
        dict(tier=4, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=["low-comp"],
             features={"function_match": 3, "domain_tier2": 2, "low_comp": -1, "raw_score": 4}),
        dict(label="Strong match / Standard", match_label="Strong match",
             competition_label="Standard", score=3,
             features={"skills_moderate": 1, "exact_stack": 1, "comp_near_level": 1}),
    ),
    (
        "hireability_high_competition_employer",
        _row("Example Title One", company="Bigtechco"),
        DESC_RICH, {},
        dict(tier=5, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=[], features={"function_match": 3, "domain_tier2": 2, "raw_score": 5}),
        dict(label="Strong match / High competition", match_label="Strong match",
             competition_label="High competition", score=0,
             features={"high_competition": -2, "skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "hireability_comp_high_bar",
        _row("Example Title One", salary_min="160000", salary_max="180000"),
        DESC_RICH, {},
        dict(tier=5, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=[], features={"function_match": 3, "domain_tier2": 2, "raw_score": 5}),
        dict(label="Strong match / High competition", match_label="Strong match",
             competition_label="High competition", score=1,
             features={"skills_moderate": 1, "exact_stack": 1, "comp_high_bar": -1}),
    ),
    (
        "hireability_senior_level_gap",
        _row("Senior Example Title One"),
        DESC_RICH, {},
        dict(tier=5, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=[], features={"function_match": 3, "domain_tier2": 2, "raw_score": 5}),
        dict(label="Strong match / Standard", match_label="Strong match",
             competition_label="Standard", score=0,
             features={"senior": -2, "skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "hireability_junior_level_gap",
        _row("Junior Example Title One"),
        DESC_RICH, {},
        dict(tier=5, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=[], features={"function_match": 3, "domain_tier2": 2, "raw_score": 5}),
        dict(label="Strong match / Standard", match_label="Strong match",
             competition_label="Standard", score=4,
             features={"junior": 2, "skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "hireability_weak_match_no_skills",
        _row("Example Title One"),
        DESC_NO_SKILLS_LONG, {},
        dict(tier=3, why="core function match; Bay Area", flags=[],
             features={"function_match": 3, "raw_score": 3}),
        dict(label="Weak match / Standard", match_label="Weak match",
             competition_label="Standard", score=-1, features={"skills_thin": -1}),
    ),
    (
        "hireability_moderate_match",
        _row("Example Title One"),
        DESC_MODERATE_MATCH, {},
        dict(tier=3, why="core function match; Bay Area", flags=[],
             features={"function_match": 3, "raw_score": 3}),
        dict(label="Moderate match / Standard", match_label="Moderate match",
             competition_label="Standard", score=0, features={}),
    ),
    (
        "hireability_unscored_short_jd",
        _row("Example Title One"),
        "Example Title One. Great team.", {},
        dict(tier=3, why="core function match; Bay Area", flags=[],
             features={"function_match": 3, "raw_score": 3}),
        dict(label="Unscored / Standard", match_label="Unscored", competition_label="Standard",
             score=0, features={}),
    ),
]

#: `blocked()` rows: (name, row, description, expected blocker code).
#: `senior_design_verification` is not reachable here -- it requires a family
#: literally named "validation" in `families.keywords`, which
#: `profile.example.json` (deliberately generic: role_family_a/b/c) does not
#: define. `test_scoring_boundaries.py` and the real `profile.json`-driven
#: `test_scoring_features.py` cover it against real family names.
GOLDEN_BLOCKERS = [
    ("non_us_location", _row("Example Title One", location="Examplestan"), DESC_RICH),
    ("off_target_location", _row("Example Title One", location="Austin, TX"), DESC_RICH),
    ("active_clearance", _row("Example Title One"),
     "must currently hold an active top secret clearance. example title one role."),
    ("specialist_skill_or_title",
     _row("Example Title One example excluded term one"), DESC_RICH),
    ("specialist_skill_or_title", _row("Example Title One"),
     "this role needs example disqualifying skill one badly. " + DESC_RICH),
    ("people_management", _row("Example Title One Manager"), DESC_RICH),
    ("years_required_too_high", _row("Example Title One"),
     "8+ years of experience required for example title one. " + DESC_RICH),
    ("no_role_family_match", _row("Totally Unrelated Title"), DESC_RICH),
    ("off_focus_role", _row("Example Title Four"), DESC_RICH),
]


@pytest.mark.parametrize("name,row,desc,kwargs,expected_score,expected_odds", GOLDEN_CASES,
                          ids=[c[0] for c in GOLDEN_CASES])
def test_golden_row(name, row, desc, kwargs, expected_score, expected_odds):
    score = rubric.score_row_explained(dict(row), desc, **kwargs)
    odds = rubric.hireability_explained(dict(row), desc)

    assert score.tier == expected_score["tier"], name
    assert score.why == expected_score["why"], name
    assert score.flags == expected_score["flags"], name
    assert score.features == expected_score["features"], name
    assert rubric.reconstruct_tier(score.features) == score.tier, name

    assert odds.label == expected_odds["label"], name
    assert odds.match_label == expected_odds["match_label"], name
    assert odds.competition_label == expected_odds["competition_label"], name
    assert odds.score == expected_odds["score"], name
    assert odds.features == expected_odds["features"], name
    assert odds.label == f"{odds.match_label} / {odds.competition_label}", name

    # The replay contract this file also pins: every golden's identity fields
    # match the loaded (example) profile and the running rubric, not merely the
    # tier/label numbers.
    assert score.profile_hash == PINNED_PROFILE_CONTENT_HASH, name
    assert score.rubric_hash == PINNED_RUBRIC_VERSION, name
    assert odds.profile_hash == PINNED_PROFILE_CONTENT_HASH, name
    assert odds.rubric_hash == PINNED_RUBRIC_VERSION, name


#: >400 chars and a hard degree gate, with none of `degree_equivalent_exception`
#: ("equivalent") anywhere in it -- so `score_row` emits the `degree-gated` flag
#: that the odds pass's `degree_gated` contribution is gated on.
DESC_DEGREE_GATED = (
    "A bachelors degree required for this example title one role, no exceptions. " + DESC_RICH
)

#: A pay band stated ONLY in the description. `score_row` recovers it and writes
#: it back onto the row (`salary`, `salary_min`, `salary_max`), which is the other
#: thing the chained pattern carries into the odds pass -- here it is what makes
#: `comp_high_bar` fire.
DESC_PAY_BAND_IN_TEXT = (
    "The base pay range for this role is $160,000 - $180,000 per year. " + DESC_RICH
)

#: The chained corpus: same shape as `GOLDEN_CASES`, scored through
#: `scoring._score_one` instead of two isolated calls. Every case here is one
#: whose ODDS depend on something `score_row` produced, which is why none of them
#: could be expressed in `GOLDEN_CASES` above.
GOLDEN_CHAINED_CASES = [
    (
        "staffing_w2_lower_bar",
        _row("Example Title One", company="Example Staffing Agency"),
        DESC_RICH, {},
        dict(tier=4, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=["Staffing/W2"],
             features={"function_match": 3, "domain_tier2": 2, "staffing_w2": -1, "raw_score": 4}),
        dict(label="Strong match / Lower bar", match_label="Strong match",
             competition_label="Lower bar", score=4,
             features={"staffing_w2": 2, "skills_moderate": 1, "exact_stack": 1}),
    ),
    (
        "degree_gated_high_competition",
        _row("Example Title One"),
        DESC_DEGREE_GATED, {},
        dict(tier=4, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=["degree-gated"],
             features={"function_match": 3, "domain_tier2": 2, "degree_gated": -1, "raw_score": 4}),
        dict(label="Strong match / High competition", match_label="Strong match",
             competition_label="High competition", score=1,
             features={"skills_moderate": 1, "exact_stack": 1, "degree_gated": -1}),
    ),
    (
        "pay_band_recovered_from_description",
        _row("Example Title One"),
        DESC_PAY_BAND_IN_TEXT, {},
        dict(tier=5, why="core function match; domain: exampletool,example platform name; Bay Area",
             flags=["salary-from-desc"],
             features={"function_match": 3, "domain_tier2": 2, "raw_score": 5}),
        dict(label="Strong match / High competition", match_label="Strong match",
             competition_label="High competition", score=1,
             features={"skills_moderate": 1, "exact_stack": 1, "comp_high_bar": -1}),
    ),
]


@pytest.mark.parametrize("code,row,desc", GOLDEN_BLOCKERS,
                          ids=[f"{c}-{i}" for i, (c, _r, _d) in enumerate(GOLDEN_BLOCKERS)])
def test_golden_blocker(code, row, desc):
    score = rubric.score_row_explained(dict(row), desc)
    assert score.tier == 0
    assert score.features == {"blocker": code}
    assert rubric.reconstruct_tier(score.features) == 0


@pytest.mark.parametrize("name,row,desc,kwargs,expected_score,expected_odds",
                          GOLDEN_CHAINED_CASES, ids=[c[0] for c in GOLDEN_CHAINED_CASES])
def test_golden_chained_row(name, row, desc, kwargs, expected_score, expected_odds):
    """The goldens for the CHAINED call pattern -- what both real callers run.

    `scoring._score_one` is used rather than re-implementing the chaining here, so
    that a regression in the wiring fails these goldens instead of leaving them
    describing a call shape nothing performs.
    """
    scored_row = dict(row)
    score, odds = scoring._score_one(
        scored_row, desc, is_aggregator=kwargs.get("is_aggregator", False)
    )

    assert score.tier == expected_score["tier"], name
    assert score.why == expected_score["why"], name
    assert score.flags == expected_score["flags"], name
    assert score.features == expected_score["features"], name
    assert rubric.reconstruct_tier(score.features) == score.tier, name

    assert odds.label == expected_odds["label"], name
    assert odds.match_label == expected_odds["match_label"], name
    assert odds.competition_label == expected_odds["competition_label"], name
    assert odds.score == expected_odds["score"], name
    assert odds.features == expected_odds["features"], name

    # The chaining itself, pinned as the mechanism rather than assumed from the
    # numbers: the odds pass read a row carrying the fit pass's output flags.
    assert scored_row["flags"] == ", ".join(score.flags), name

    # And every case here EARNS its place: scored the isolated way `GOLDEN_CASES`
    # scores, the same row produces different odds. A case that does not is a case
    # that belongs in `GOLDEN_CASES`, not in this list.
    isolated = rubric.hireability_explained(dict(row), desc)
    assert isolated.features != odds.features, name


# --------------------------------------------------------------------------- #
# "Lower bar" and the desc-independent `degree_gated` odds contribution, at the
# level of the PURE function.
#
# Both are gated on `"...` in flags` inside `_hireability_core`. The chained
# goldens above pin that the real call paths populate those flags; these two pin
# the gate itself, given the flags directly -- so a regression can be localized
# to the wiring or to the rule rather than only to "the label moved".
# --------------------------------------------------------------------------- #
def test_golden_lower_bar_competition_label_via_explicit_flags():
    row = _row("Example Title One", flags="Staffing/W2")
    odds = rubric.hireability_explained(row, DESC_RICH)
    assert odds.competition_label == "Lower bar"
    assert odds.match_label == "Strong match"
    assert odds.label == "Strong match / Lower bar"
    assert odds.score == 4
    assert odds.features == {"staffing_w2": 2, "skills_moderate": 1, "exact_stack": 1}


def test_golden_degree_gated_odds_contribution_via_explicit_flags():
    row = _row("Example Title One", flags="degree-gated")
    odds = rubric.hireability_explained(row, DESC_RICH)
    assert odds.competition_label == "High competition"
    assert odds.features == {"skills_moderate": 1, "exact_stack": 1, "degree_gated": -1}


# --------------------------------------------------------------------------- #
# The coverage matrix the roadmap line asks for, asserted as a fact about the
# corpus above rather than left to be eyeballed from the case list.
# --------------------------------------------------------------------------- #
def test_the_golden_corpus_covers_every_tier_and_label():
    tiers = {c[4]["tier"] for c in GOLDEN_CASES} | {0}  # blockers contribute tier 0
    assert tiers == {0, 1, 2, 3, 4, 5}, tiers

    match_labels = {c[5]["match_label"] for c in GOLDEN_CASES}
    assert match_labels == {
        "Level stretch", "Strong match", "Moderate match", "Weak match", "Unscored",
    }, match_labels

    # "Lower bar" is no longer contributed by a hand-written literal here: a
    # chained golden reaches it the way a real posting does.
    competition_labels = {c[5]["competition_label"] for c in GOLDEN_CASES}
    competition_labels |= {c[5]["competition_label"] for c in GOLDEN_CHAINED_CASES}
    assert competition_labels == {"High competition", "Standard", "Lower bar"}, competition_labels

    blocker_codes = {c[0] for c in GOLDEN_BLOCKERS}
    assert blocker_codes == candidate_profile.BLOCKER_CODES - {"senior_design_verification"}

    assert any("cap_undated_aggregator" in c[4]["features"] for c in GOLDEN_CASES), (
        "no golden row exercised the aggregator ghost cap"
    )
    assert any("cap_stale" in c[4]["features"] for c in GOLDEN_CASES), (
        "no golden row exercised the stale ghost cap"
    )
    assert any("comp_in_band" in c[4]["features"] for c in GOLDEN_CASES), (
        "no golden row exercised the comp-in-band bonus"
    )
    assert any("low_comp" in c[4]["features"] for c in GOLDEN_CASES), (
        "no golden row exercised the low-comp penalty"
    )
