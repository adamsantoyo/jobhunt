"""Phase 3.7: boundary and property tests for `rubric.score_row_explained` /
`rubric.hireability_explained`.

WHY A SEPARATE PROFILE FROM THE GOLDENS. `test_scoring_goldens.py` pins exact
outputs against `profile.example.json` verbatim. This file needs to walk
`his_skills` hit counts up to 6 (`skills_strong`'s threshold), and
`profile.example.json` only carries 4 `his_skills` patterns -- 6 is
UNREACHABLE with that document. `BOUNDARY_PROFILE_DOC` is
`profile.example.json` with `skills.his_skills` extended to 8 patterns and
nothing else changed, built in-memory so this file stays independent of both
`profile.json` (gitignored, absent on a clean checkout) and the goldens'
document (whose hit-count ceiling this file exists to get past).

BOUNDARIES are exact-edge parametrizations: one test per threshold, values on
both sides of the line the rubric actually branches on (399/400/401, not
"a short description" and "a long one"). PROPERTIES are universal claims
checked over a bounded, fully enumerated grid (`itertools.product` over
explicit finite lists -- no `random`, so a failure always reproduces byte for
byte and no seed has to be remembered or pinned).
"""
from __future__ import annotations

import copy
import datetime
import itertools
import json
import os
import sys

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede these)
import rubric  # noqa: E402


# --------------------------------------------------------------------------- #
# The boundary profile
# --------------------------------------------------------------------------- #
def _boundary_profile_doc() -> dict:
    with open(os.path.join(_REPO_ROOT, "profile.example.json")) as f:
        doc = json.load(f)
    doc["skills"]["his_skills"] = [
        "example skill one", "example skill two", "example skill three",
        "example skill four", "example skill five", "example skill six",
        "example skill seven", "example skill eight",
    ]
    return doc


BOUNDARY_PROFILE_DOC = _boundary_profile_doc()
SKILL_TOKENS = tuple(BOUNDARY_PROFILE_DOC["skills"]["his_skills"])

#: The thresholds this file pins edges against, read from the document itself
#: rather than re-typed as magic numbers, so a future edit to
#: `profile.example.json`'s numeric fields cannot silently desync the boundary
#: this file exercises from the boundary the profile actually declares.
_COMP = BOUNDARY_PROFILE_DOC["comp"]
_EXPERIENCE = BOUNDARY_PROFILE_DOC["experience"]
_TIER_RULES = BOUNDARY_PROFILE_DOC["tier_rules"]


@pytest.fixture(scope="module")
def boundary_profile():
    return candidate_profile.build_profile(BOUNDARY_PROFILE_DOC)


@pytest.fixture(autouse=True)
def _use_boundary_profile(boundary_profile, monkeypatch):
    monkeypatch.setattr(rubric, "_RPROFILE_CACHE", boundary_profile)


def _row(title="Example Title One", **kw):
    base = {
        "title": title, "company": "Acme Robotics", "location": "Example Bay City One, CA",
        "salary": "", "salary_min": "", "salary_max": "", "posted": "", "remote": "false",
        "source": "greenhouse:acme", "req_id": "R-1", "flags": "",
    }
    base.update(kw)
    return base


def _padded(base: str, length: int) -> str:
    """`base` repeated and truncated to EXACTLY `length` characters."""
    text = (base * (length // len(base) + 2))[:length]
    assert len(text) == length
    return text


def _desc_with_hits(n: int, *, min_length: int = 420) -> str:
    """A description containing exactly the first `n` `SKILL_TOKENS`, long
    enough to clear the `len(d) > 400` skills gate."""
    body = " and ".join(SKILL_TOKENS[:n]) + " are the skills for this role. "
    while len(body) < min_length:
        body += "Extra padding text to reach the length threshold reliably today. "
    return body


# --------------------------------------------------------------------------- #
# Boundary: the len(description) == 400 skills gate
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("length,expect_gate_open", [(399, False), (400, False), (401, True)])
def test_the_400_char_skills_gate_edge(length, expect_gate_open):
    """The gate is `len(d) > 400`, so 400 itself is still CLOSED -- one character
    short of open. A description with zero skill hits is `match_label`
    "Unscored" with the gate closed and "Weak match" (`skills_thin`) the instant
    it opens, which is what makes the edge externally observable."""
    desc = _padded("Example Title One role with no skill keywords present here. ", length)
    odds = rubric.hireability_explained(_row(), desc)
    if expect_gate_open:
        assert odds.match_label == "Weak match"
        assert odds.features == {"skills_thin": -1}
    else:
        assert odds.match_label == "Unscored"
        assert odds.features == {}


# --------------------------------------------------------------------------- #
# Boundary: his_skills hit counts at 1 / 2 / 3 / 5 / 6
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hits,expected_feature,expected_match_label", [
    (1, {"skills_thin": -1}, "Weak match"),
    (2, {}, "Moderate match"),
    (3, {"skills_moderate": 1}, "Moderate match"),
    (5, {"skills_moderate": 1}, "Moderate match"),
    (6, {"skills_strong": 2}, "Strong match"),
])
def test_skills_hit_count_boundaries(hits, expected_feature, expected_match_label):
    """<=1 is thin, [3, 6) is moderate, >=6 is strong -- 2 is the honest gap where
    neither bonus nor penalty applies, which the `expected_feature == {}` case
    pins directly rather than leaving implicit."""
    odds = rubric.hireability_explained(_row(), _desc_with_hits(hits))
    assert odds.features == expected_feature, (hits, odds.features)
    assert odds.match_label == expected_match_label, (hits, odds.match_label)


def test_repeated_occurrences_of_one_pattern_count_once():
    """A hit is PATTERN membership, not occurrence count: `sum(1 for p in
    his_skills if p.search(d))` counts how many of the 8 patterns matched at
    least once, not how many times any of them appeared. A description that
    says the same single skill six times is still one hit."""
    desc = ("example skill one is used here. " * 15)
    assert len(desc) > 400
    odds = rubric.hireability_explained(_row(), desc)
    assert odds.features == {"skills_thin": -1}, odds.features
    assert odds.match_label == "Weak match"


# --------------------------------------------------------------------------- #
# Boundary: years thresholds (hireability)
# --------------------------------------------------------------------------- #
def _years_desc(years: int) -> str:
    return (
        f"{years}+ years of experience required for example title one role, standard team. "
        "Padding words here to lengthen the description well past four hundred "
        "characters for consistency across every boundary case tested here today."
    )


@pytest.mark.parametrize("years,expect_low,expect_high", [
    (2, True, False),
    (3, True, False),   # hireability_bonus_years_max edge: still bonus
    (4, False, False),  # one past the bonus edge: neither
    (5, False, False),  # one short of the penalty edge: neither
    (6, False, True),   # hireability_penalty_years_min edge: penalty begins
    (7, False, True),
])
def test_hireability_years_threshold_edges(years, expect_low, expect_high):
    assert _EXPERIENCE["hireability_bonus_years_max"] == 3
    assert _EXPERIENCE["hireability_penalty_years_min"] == 6
    odds = rubric.hireability_explained(_row(), _years_desc(years))
    assert ("years_low" in odds.features) == expect_low, (years, odds.features)
    assert ("years_high" in odds.features) == expect_high, (years, odds.features)


# --------------------------------------------------------------------------- #
# Boundary: years thresholds (score_row)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("years,expect_bonus,expect_penalty", [
    (3, True, False),   # score_bonus_years_max edge
    (4, False, False),
    (5, False, False),
    (6, False, True),   # score_penalty_years_low edge
    (7, False, True),   # score_penalty_years_high edge
    (8, False, False),  # one past score_penalty_years_high: neither (and blocked -- see below)
])
def test_score_row_years_required_threshold_edges(years, expect_bonus, expect_penalty):
    assert _EXPERIENCE["score_bonus_years_max"] == 3
    assert (_EXPERIENCE["score_penalty_years_low"], _EXPERIENCE["score_penalty_years_high"]) == (6, 7)
    assert _EXPERIENCE["blocker_years_min"] == 8
    score = rubric.score_row_explained(_row(), _years_desc(years))
    if years >= _EXPERIENCE["blocker_years_min"]:
        # 8 is also blocker_years_min's edge: the blocker takes precedence over
        # the penalty band entirely, which is why this parametrization stops at 8
        # rather than asserting a penalty that can never be observed there.
        assert score.features == {"blocker": "years_required_too_high"}
        return
    assert ("years_bonus" in score.features) == expect_bonus, (years, score.features)
    assert ("years_penalty" in score.features) == expect_penalty, (years, score.features)


@pytest.mark.parametrize("years,expect_penalty", [(7, False), (8, True), (9, True)])
def test_score_row_preferred_years_penalty_edge(years, expect_penalty):
    """`score_pref_penalty_years_min` gates the PREFERRED-years penalty, which is
    a distinct field from the required-years one above: 3+ required (safely under
    every required-years threshold) keeps this isolated to the preferred edge."""
    assert _EXPERIENCE["score_pref_penalty_years_min"] == 8
    desc = (
        f"3+ years of experience required. {years}+ years preferred is a plus for "
        "example title one role. Padding words here to lengthen the description "
        "well past four hundred characters for consistency across every case here."
    )
    score = rubric.score_row_explained(_row(), desc)
    assert ("years_pref_penalty" in score.features) == expect_penalty, (years, score.features)


# --------------------------------------------------------------------------- #
# Boundary: comp band edges
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hi,expect_high_bar,expect_near_level", [
    (69999, False, True),
    (70000, False, False),   # hireability_near_level edge: the open bound excludes it
    (149999, False, False),
    (150000, True, False),   # hireability_high_bar edge: the closed bound includes it
])
def test_hireability_comp_band_edges(hi, expect_high_bar, expect_near_level):
    assert _COMP["hireability_near_level"] == 70000
    assert _COMP["hireability_high_bar"] == 150000
    row = _row(salary_min="10000", salary_max=str(hi))
    odds = rubric.hireability_explained(row, _desc_with_hits(0))
    assert ("comp_high_bar" in odds.features) == expect_high_bar, (hi, odds.features)
    assert ("comp_near_level" in odds.features) == expect_near_level, (hi, odds.features)


@pytest.mark.parametrize("lo,hi,expect_in_band,expect_low_comp", [
    (79999, 100000, True, False),   # band_low is a HI >= test, not lo >=: lo below band_low still counts
    (80000, 100000, True, False),
    (100000, 120000, True, False),  # band_high edge
    (100000, 120001, True, False),  # lo <= band_high still holds even past band_high
    (59999, 59999, False, True),    # low_comp_threshold edge: strictly below
    (60000, 60000, False, False),   # at the threshold: neither band overlap nor low-comp
])
def test_score_row_comp_band_edges(lo, hi, expect_in_band, expect_low_comp):
    assert (_COMP["band_low"], _COMP["band_high"]) == (80000, 120000)
    assert _COMP["low_comp_threshold"] == 60000
    row = _row(salary_min=str(lo), salary_max=str(hi))
    score = rubric.score_row_explained(row, _desc_with_hits(0, min_length=420))
    assert ("comp_in_band" in score.features) == expect_in_band, (lo, hi, score.features)
    assert ("low_comp" in score.features) == expect_low_comp, (lo, hi, score.features)


# --------------------------------------------------------------------------- #
# Boundary: tier caps and the mid-computation clamp
# --------------------------------------------------------------------------- #
def test_staff_cap_clamp_edge():
    assert _TIER_RULES["staff_cap_tier"] == 2
    score = rubric.score_row_explained(_row("Staff Example Title One"), _desc_with_hits(6))
    assert score.tier == 2
    assert score.features["staff_cap_delta"] < 0
    assert "level-out" in score.flags


def test_func_cap_edge_min_func_and_tier():
    """`func_cap_min_func=3, func_cap_tier=4`: role_family_b's function weight is
    2 (< 3), and with enough OTHER contributions pushed to a raw score above the
    cap, the row is forced back down to tier 4 -- "5 requires Function 3"."""
    assert (_TIER_RULES["func_cap_min_func"], _TIER_RULES["func_cap_tier"]) == (3, 4)
    desc = (
        "Example Title Three role using exampletool and example platform name daily, "
        "example industry term and example certification too, shipping constantly for "
        "our team across many stakeholders, exceeding four hundred characters here for "
        "the length gate to ensure the skills evaluation logic runs as intended today."
    )
    row = _row("Example Title Three", company="Examplecorp", salary_min="90000", salary_max="100000")
    score = rubric.score_row_explained(row, desc)
    assert score.features["raw_score"] > 4
    assert score.features["cap_func"] == 4
    assert score.tier == 4
    assert "func-cap" in score.flags


def test_no_description_cap_edge():
    assert _TIER_RULES["no_desc_cap_tier"] == 3
    row = _row(salary_min="90000", salary_max="100000")
    score = rubric.score_row_explained(row, None)
    assert score.features["raw_score"] > 3
    assert score.features["cap_no_desc"] == 3
    assert score.tier == 3


@pytest.mark.parametrize("days_ago,expect_penalty,expect_cap", [
    (29, False, False),
    (30, False, False),   # stale_penalty_days edge: still closed (age > 30, not >=)
    (31, True, False),
    (89, True, False),
    (90, True, False),    # stale_cap_days edge: still closed
    (91, True, True),
])
def test_stale_penalty_and_cap_edges(days_ago, expect_penalty, expect_cap):
    assert _TIER_RULES["stale_penalty_days"] == 30
    assert (_TIER_RULES["stale_cap_days"], _TIER_RULES["stale_cap_tier"]) == (90, 3)
    posted = (datetime.date.today() - datetime.timedelta(days=days_ago)).isoformat()
    row = _row(posted=posted)
    score = rubric.score_row_explained(row, _desc_with_hits(6))
    assert ("stale_30d" in score.features) == expect_penalty, (days_ago, score.features)
    assert ("cap_stale" in score.features) == expect_cap, (days_ago, score.features)
    if expect_cap:
        assert score.features["cap_stale"] == 3


def test_undated_aggregator_cap_edge():
    """The cap only engages once the raw score exceeds it -- a row already AT or
    below `undated_aggregator_cap_tier` is untouched, which is why this needs a
    domain hit to push the raw score past 3 before the cap becomes observable."""
    assert _TIER_RULES["undated_aggregator_cap_tier"] == 3
    desc = "Working with exampletool and example platform name daily. " * 10
    row = _row(source="jobspy:indeed")
    capped = rubric.score_row_explained(row, desc, is_aggregator=True)
    uncapped = rubric.score_row_explained(row, desc, is_aggregator=False)
    assert capped.features["raw_score"] > 3
    assert capped.features["cap_undated_aggregator"] == 3
    assert capped.tier == 3
    assert uncapped.tier > capped.tier
    assert "cap_undated_aggregator" not in uncapped.features


# --------------------------------------------------------------------------- #
# Properties, over a bounded, fully enumerated (no `random`) grid
# --------------------------------------------------------------------------- #
TITLES = (
    "Example Title One", "Staff Example Title One", "Senior Example Title One",
    "Junior Example Title One", "Example Title Three", "Example Title Four",
)
LOCATIONS = ("Example Bay City One, CA", "Remote - US", "Austin, TX")
DESCRIPTIONS = (
    None,
    "",
    "Short description under the four hundred character skills gate.",
    "Example Title One role. " * 20,  # >400 chars, zero skill hits: skills_thin / Weak match
    _desc_with_hits(2),
    _desc_with_hits(6),
)
TODAY = datetime.date.today().isoformat()
POSTED = (None, TODAY, "2024-01-01")
AGGREGATOR = (False, True)

MATCH_LABELS = frozenset({
    "Level stretch", "Strong match", "Moderate match", "Weak match", "Unscored",
})
COMPETITION_LABELS = frozenset({"High competition", "Standard", "Lower bar"})


def _grid():
    for title, location, desc, posted, aggregator in itertools.product(
        TITLES, LOCATIONS, DESCRIPTIONS, POSTED, AGGREGATOR
    ):
        yield title, location, desc, posted, aggregator


def _grid_row(title, location, posted):
    return _row(title, location=location, posted=posted or "", remote="true" if location.startswith("Remote") else "false")


def test_property_numeric_contributions_sum_to_the_score_over_the_grid():
    """`sum(contributions) == raw_score` for score_row, `sum(features.values())
    == score` for hireability -- over every combination in the bounded grid, not
    a hand-picked few."""
    checked = 0
    for title, location, desc, posted, aggregator in _grid():
        row = _grid_row(title, location, posted)
        score = rubric.score_row_explained(dict(row), desc, is_aggregator=aggregator)
        odds = rubric.hireability_explained(dict(row), desc)

        if candidate_profile.SCORE_ROW_BLOCKER_FEATURE not in score.features:
            contributions = {
                k: v for k, v in score.features.items()
                if k in candidate_profile.SCORE_ROW_CONTRIBUTIONS
            }
            assert sum(contributions.values()) == score.features["raw_score"], (title, desc)
            assert rubric.reconstruct_tier(score.features) == score.tier, (title, desc)
        assert sum(odds.features.values()) == odds.score, (title, desc)
        checked += 1
    assert checked == len(TITLES) * len(LOCATIONS) * len(DESCRIPTIONS) * len(POSTED) * len(AGGREGATOR)


def test_property_label_is_always_the_join_of_two_closed_vocabularies():
    seen_match, seen_competition = set(), set()
    for title, location, desc, posted, _aggregator in _grid():
        row = _grid_row(title, location, posted)
        odds = rubric.hireability_explained(dict(row), desc)
        assert odds.match_label in MATCH_LABELS, (title, desc, odds.match_label)
        assert odds.competition_label in COMPETITION_LABELS, (title, desc, odds.competition_label)
        assert odds.label == f"{odds.match_label} / {odds.competition_label}", (title, desc)
        seen_match.add(odds.match_label)
        seen_competition.add(odds.competition_label)
    # The grid must actually exercise variety, or the property is checked
    # against nothing interesting. "Lower bar" is excluded: it depends on the
    # row's `flags` field, which no canonically-built row (`row_from_version`)
    # ever populates -- see test_scoring_goldens.py's module docstring.
    assert seen_match >= {"Strong match", "Unscored", "Weak match"}, seen_match
    assert seen_competition >= {"Standard"}, seen_competition


def test_property_scoring_is_deterministic():
    """The same input, scored twice, must be the identical result -- not merely
    an equal tier, the whole `ScoreResult`/`OddsResult` (features, why, flags,
    hashes)."""
    for title, location, desc, posted, aggregator in _grid():
        row = _grid_row(title, location, posted)
        first_score = rubric.score_row_explained(dict(row), desc, is_aggregator=aggregator)
        second_score = rubric.score_row_explained(dict(row), desc, is_aggregator=aggregator)
        assert first_score == second_score, (title, location, desc, posted, aggregator)

        first_odds = rubric.hireability_explained(dict(row), desc)
        second_odds = rubric.hireability_explained(dict(row), desc)
        assert first_odds == second_odds, (title, location, desc, posted, aggregator)


def test_property_profile_and_rubric_hashes_match_the_loaded_identity(boundary_profile):
    """Every `ScoreResult`/`OddsResult` names the profile and scorer that
    produced it. Both hashes must match the ACTUALLY LOADED profile's content
    hash and the running `RUBRIC_VERSION` -- not a copy of either recorded
    somewhere else that could drift out from under the check."""
    for title, location, desc, posted, aggregator in _grid():
        row = _grid_row(title, location, posted)
        score = rubric.score_row_explained(dict(row), desc, is_aggregator=aggregator)
        odds = rubric.hireability_explained(dict(row), desc)
        assert score.profile_hash == boundary_profile.content_hash, (title, desc)
        assert score.rubric_hash == rubric.RUBRIC_VERSION, (title, desc)
        assert odds.profile_hash == boundary_profile.content_hash, (title, desc)
        assert odds.rubric_hash == rubric.RUBRIC_VERSION, (title, desc)
