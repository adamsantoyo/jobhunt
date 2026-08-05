"""Phase 3.3: the stored feature vector is REPLAYABLE, and the scorer is hermetic.

Six closures the Phase 3.4 review deferred, each with the test that fails when it
is reverted:

  CLAMPS AND CAPS ARE IN THE VECTOR. `rubric.reconstruct_tier(features)` re-derives
    the tier from the vector alone, and `sum(contributions) == raw_score` holds for
    every scored row. Before this, the vector omitted `function_match` on any
    non-top family, the mid-computation staff clamp, the pre-clamp total, and every
    terminal cap -- so a stored score explained a tier it could not produce.
  THE FEATURE SET IS CLOSED IN BOTH DIRECTIONS. `scoring.validate_features` refuses
    a vector carrying a key the replayer has never heard of, one MISSING a key the
    replayer indexes, and one whose value is not a real number. (The write path's
    enforcement of it is `test_source_scoring.py`'s: this file is pure.)
  BLOCKER CODES ARE A NAMESPACE. `candidate_profile.BLOCKER_CODES` is asserted at
    emission, not merely spot-checked here.
  THE SCORER HAS A SOURCE DIGEST. Editing the pinned scoring surface fails this
    suite until a human updates the pin -- which is the moment to decide whether
    RUBRIC_VERSION moves with it.
  THE PROFILE OWNS THE LOCATION GATE AND THE TITLE EXCLUSIONS. Editing either one
    changes `profile_version_id`, which is what invalidates stored scores. They
    used to live in config.json, which nothing hashed.
  HIREABILITY IS REPLAYABLE TOO. Its vector sums to its score and its score
    composes to its label.

Nothing here touches a database or the network: `rubric` and `candidate_profile`
are pure given a profile document.
"""
import copy
import datetime
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


@pytest.fixture(scope="module")
def profile_doc():
    with open(os.path.join(_REPO_ROOT, "profile.json")) as f:
        return json.load(f)


# --------------------------------------------------------------------------- #
# Row generation
#
# A cross product rather than a handful of hand-written rows, because the property
# under test is universal ("every vector reconstructs its tier") and hand-written
# rows only ever cover the paths their author remembered. The axes are chosen to
# make each terminal cap and the mid-computation clamp actually fire.
# --------------------------------------------------------------------------- #
TITLES = [
    "Support Engineer",                    # top-weight family, no level modifier
    "Technical Support Engineer II",
    "Staff Technical Support Engineer",    # staff clamp
    "Senior Validation Engineer",          # stretch level
    "Technical Program Manager",           # off-focus / func cap territory
    "Systems Administrator",
    "Cloud Support Engineer",
]
LOCATIONS = [
    "San Francisco, CA",
    "Sunnyvale, CA",
    "Remote - US",
    "Austin, TX",
]
DESCRIPTIONS = [
    None,                                  # no description: rule-zero cap
    "",                                    # likewise, via a different path
    "You will support enterprise customers on Azure and Intune. 2+ years of "
    "experience required. Bachelor's degree or equivalent experience.",
    "Support Windows endpoints with Intune and Entra ID, Active Directory, M365, "
    "ServiceNow tickets. Salary range $150,000 - $185,000 USD. 3 years experience "
    "with hardware validation and semiconductor test equipment preferred.",
    # Straddles the `len(d) > 400` skills-overlap gate (rubric.py) ONLY once
    # backslashes are stripped first: 401 raw characters, 396 once the 5
    # trailing backslashes are removed. `_hireability_core` normalizes with
    # `.lower().replace("\\", "")` before measuring, so the real gate sees 396
    # (<=400, skills computation skipped, "Unscored"). A test that measured the
    # RAW string would see 401 (>400) and wrongly expect the skills-hit branch
    # to have run.
    "Assisting enterprise customers with break-fix tickets across a large fleet of "
    "endpoints, coordinating with vendors, documenting steps in the internal wiki, "
    "and escalating to engineering when a workaround is not sufficient for the issue "
    "at hand today, while also tracking recurring incidents in the queue and updating "
    "the knowledge base with resolution notes for the next on-call engineer on shift"
    + "\\" * 5,
]
POSTED = [None, "2026-08-01", "2025-01-01"]   # unknown / fresh / very stale

#: A posting date that is fresh whenever the suite runs. `rubric.posting_age_days`
#: measures against the real clock, so a row built to isolate ONE rule has to be
#: dated off the clock too or a literal quietly adds the 30-day penalty to it.
TODAY = datetime.date.today().isoformat()
AGGREGATOR = [False, True]


def _row(title, location, posted):
    return {
        "title": title, "company": "Acme Robotics", "location": location,
        "salary": "", "salary_min": "", "salary_max": "", "posted": posted or "",
        "source": "greenhouse:acme", "req_id": "R-1", "remote": "false", "flags": "",
    }


def _generated():
    for title in TITLES:
        for location in LOCATIONS:
            for desc in DESCRIPTIONS:
                for posted in POSTED:
                    for aggregator in AGGREGATOR:
                        yield title, location, desc, posted, aggregator


def _contributions(features):
    return {k: v for k, v in features.items()
            if k in candidate_profile.SCORE_ROW_CONTRIBUTIONS}


# --------------------------------------------------------------------------- #
# 1. Clamps and caps are in the vector
# --------------------------------------------------------------------------- #
def test_every_generated_row_reconstructs_its_own_tier_and_sums_to_its_raw_score():
    """The two invariants that make a stored score evidence rather than annotation.

    A vector that cannot reproduce its own tier is a story about a number. Both
    halves are asserted together because they fail to different mutations: dropping
    `raw_score` or a terminal cap breaks the reconstruction, and dropping
    `function_match` or `staff_cap_delta` breaks the sum while leaving the
    reconstruction accidentally correct.
    """
    checked = {"blocked": 0, "scored": 0, "with_cap": 0, "with_staff_clamp": 0}
    for title, location, desc, posted, aggregator in _generated():
        result = rubric.score_row_explained(
            _row(title, location, posted), desc, is_aggregator=aggregator
        )
        label = (title, location, desc, posted, aggregator, result.features)

        assert set(result.features) <= candidate_profile.REQUIRED_SCORE_ROW_FEATURES, label
        assert rubric.reconstruct_tier(result.features) == result.tier, label

        if candidate_profile.SCORE_ROW_BLOCKER_FEATURE in result.features:
            checked["blocked"] += 1
            assert result.tier == 0, label
            assert set(result.features) == {"blocker"}, label
            continue

        checked["scored"] += 1
        assert sum(_contributions(result.features).values()) == result.features["raw_score"], label
        if any(c in result.features for c in candidate_profile.SCORE_ROW_CAP_FEATURES):
            checked["with_cap"] += 1
        if result.features.get("staff_cap_delta", 0) < 0:
            checked["with_staff_clamp"] += 1

    # The generator has to actually exercise the paths, or the property above is
    # true of nothing interesting.
    assert checked["blocked"] > 0 and checked["scored"] > 0, checked
    assert checked["with_cap"] > 0, "no generated row hit a terminal cap"
    assert checked["with_staff_clamp"] > 0, "no generated row hit the staff clamp"


def test_the_staff_clamp_is_recorded_as_a_negative_contribution():
    """The clamp moves the RUNNING TOTAL, so it replays as a contribution."""
    good = ("You will support enterprise customers on Azure and Intune. 2+ years of "
            "experience required. Kubernetes and observability tooling.")
    clamped = rubric.score_row_explained(
        _row("Staff Technical Support Engineer", "San Francisco, CA", "2026-08-01"), good
    )
    assert "staff_cap_delta" in clamped.features
    assert clamped.features["staff_cap_delta"] < 0
    assert sum(_contributions(clamped.features).values()) == clamped.features["raw_score"]

    plain = rubric.score_row_explained(
        _row("Support Engineer", "San Francisco, CA", "2026-08-01"), good
    )
    assert "staff_cap_delta" not in plain.features


def test_the_staff_clamp_is_recorded_even_when_it_costs_nothing():
    """PRESENCE, not magnitude, is the contract -- and only this row proves it.

    "The rule fired and changed nothing" is a different fact from "the rule did not
    fire", and only the first one explains why a Staff title scored the same as an
    IC one. The distinction is invisible to the test above and to every generated
    row, because in all of them the clamp binds: emitting the key only when
    `capped != score` would leave the whole suite green. So this row is built to
    reach the clamp ALREADY at or below `staff_cap_tier` -- a Staff title whose
    years penalty has taken the running total down to the cap -- where a
    conditional emission is the only thing that can drop the key.
    """
    at_the_cap = rubric.score_row_explained(
        _row("Staff Support Engineer", "San Francisco, CA", TODAY),
        "You will be helping customers with their tickets. 6+ years of experience required.",
    )
    assert "level-out" in at_the_cap.flags, "the clamp must actually have fired"
    assert at_the_cap.features["staff_cap_delta"] == 0, (
        "a clamp that cost nothing is still a clamp that fired"
    )
    assert sum(_contributions(at_the_cap.features).values()) == at_the_cap.features["raw_score"]
    assert rubric.reconstruct_tier(at_the_cap.features) == at_the_cap.tier


def test_function_match_is_emitted_for_every_scored_row(profile_doc, monkeypatch):
    """It is the score's starting value, so omitting it makes the sum unprovable.

    It used to be emitted ONLY when the family was the top-weighted one, which is
    exactly the case where its absence is least visible: on today's profile every
    in-scope family happens to weigh 3, so the omission was invisible and the sum
    happened to work out. The second half of this test lowers one in-scope family's
    weight -- a one-line profile.json edit anyone could make tomorrow -- and shows
    the vector still reconstructs.
    """
    desc = "Supporting enterprise customers on Azure. 2+ years of experience."
    for title in TITLES:
        result = rubric.score_row_explained(_row(title, "San Francisco, CA", "2026-08-01"), desc)
        if candidate_profile.SCORE_ROW_BLOCKER_FEATURE in result.features:
            continue
        assert "function_match" in result.features, title

    lowered = copy.deepcopy(profile_doc)
    lowered["families"]["function_weight"]["validation"] = 1
    monkeypatch.setattr(rubric, "_RPROFILE_CACHE", candidate_profile.build_profile(lowered))
    result = rubric.score_row_explained(
        _row("Senior Validation Engineer", "San Francisco, CA", "2026-08-01"), desc
    )
    assert result.features["function_match"] == 1
    assert sum(_contributions(result.features).values()) == result.features["raw_score"]
    assert rubric.reconstruct_tier(result.features) == result.tier

    # And a below-top family that scores high enough is exactly what the RUBRIC
    # func cap exists for ("5 requires Function 3"), so the fourth terminal cap is
    # exercised too rather than being the one nothing ever reaches.
    rich = (
        "Support enterprise customers on Azure, Intune, Entra ID, Active Directory, "
        "M365, ServiceNow, Kubernetes, Datadog and observability tooling. Hardware "
        "validation and semiconductor test experience. 2 years of experience. "
        "Salary range $150,000 - $185,000 USD."
    )
    capped = rubric.score_row_explained(
        _row("Validation Engineer", "San Francisco, CA", "2026-08-01"), rich
    )
    assert capped.features["raw_score"] > 4
    assert capped.features["cap_func"] == 4
    assert capped.tier == 4
    assert rubric.reconstruct_tier(capped.features) == capped.tier


def test_the_undated_aggregator_cap_fires_on_the_explicit_flag_not_a_source_string():
    """The material bug this phase fixed.

    The cap used to gate on `r["source"].startswith(("jobspy-", "mcp-", "builtin",
    "yc-jobs"))` -- legacy CSV source strings. Canonical `posting_versions.source`
    carries the NormalizedPosting namespace ("jobspy:indeed"), which matches none of
    them, so for every canonically-scored row the ghost-listing cap silently never
    fired. Same row, same undated posting, two answers.
    """
    desc = ("Support enterprise customers on Azure, Intune, Entra ID, Active Directory, "
            "M365, ServiceNow, Kubernetes and observability tooling. 2+ years required. "
            "Salary range $150,000 - $185,000 USD.")
    row = _row("Technical Support Engineer", "San Francisco, CA", None)
    row["source"] = "jobspy:indeed"  # the canonical namespace, not a legacy string

    sniffed = rubric.score_row_explained(dict(row), desc)          # legacy path
    declared = rubric.score_row_explained(dict(row), desc, is_aggregator=True)

    assert "cap_undated_aggregator" not in sniffed.features, (
        "the legacy string sniff cannot see a canonical namespace -- that is the bug"
    )
    assert "cap_undated_aggregator" in declared.features
    assert declared.tier < sniffed.tier
    assert "undated-aggregator" in declared.flags
    assert rubric.reconstruct_tier(declared.features) == declared.tier


def test_emitting_features_does_not_change_the_tier():
    """`score_row()` throws the vector away and must return what it always returned."""
    for title, location, desc, posted, aggregator in _generated():
        plain = rubric.score_row(_row(title, location, posted), desc, is_aggregator=aggregator)
        explained = rubric.score_row_explained(
            _row(title, location, posted), desc, is_aggregator=aggregator
        )
        assert plain == (explained.tier, explained.why, explained.flags)


# --------------------------------------------------------------------------- #
# 2. The feature set is closed
# --------------------------------------------------------------------------- #
def _validate_score_row(features):
    return scoring.validate_features(
        features,
        candidate_profile.REQUIRED_SCORE_ROW_FEATURES,
        required_present=candidate_profile.REQUIRED_PRESENT_SCORE_ROW_FEATURES,
    )


def test_a_vector_with_an_unknown_key_is_refused_before_it_is_stored():
    """An un-replayable score must never reach the database.

    The failure this rules out: somebody adds a scored dimension, the vector grows
    a key `reconstruct_tier` ignores, and every score written from that day forward
    silently disagrees with its own replay.

    `test_source_scoring.py` proves the same refusal on the WRITE PATH, which is
    the claim that matters -- a pure function nobody calls refuses nothing.
    """
    good = {"function_match": 3, "raw_score": 3}
    assert _validate_score_row(good) == good

    with pytest.raises(scoring.ScoreFeatureError) as excinfo:
        _validate_score_row({**good, "vibes_bonus": 2})
    assert "vibes_bonus" in str(excinfo.value)


@pytest.mark.parametrize("features,expected", [
    ({"function_match": 3}, "raw_score"),
    ({"raw_score": 3}, "function_match"),
    ({}, "function_match"),
])
def test_a_vector_missing_a_key_the_replayer_indexes_is_refused(features, expected):
    """The other direction, and the one the closed set alone never covered.

    A closed set says which keys MAY appear; on its own it accepts `{}`. The
    consequence is not a wrong tier, it is `reconstruct_tier` raising `KeyError`
    years later on a row nothing can re-derive -- asserted below, so the rule is
    justified by the failure it prevents rather than by its own restatement.
    """
    with pytest.raises(scoring.ScoreFeatureError, match=expected):
        _validate_score_row(features)

    if "raw_score" not in features:
        with pytest.raises(KeyError):
            rubric.reconstruct_tier(features)


@pytest.mark.parametrize("value", [
    "not-a-number", None, True, False, float("nan"), float("inf"), float("-inf"), [3],
])
def test_a_vector_whose_value_is_not_a_real_number_is_refused(value):
    """Every stored value is summed, mins'd or compared on replay.

    A string raises, a None raises, a NaN does neither -- it propagates silently
    through `min` and never equals itself, which is the worst of the three. `bool`
    is rejected although it is an `int` subclass: `True` where a point value belongs
    means somebody stored a flag, and it would sum as 1.
    """
    with pytest.raises(scoring.ScoreFeatureError):
        _validate_score_row({"function_match": 3, "raw_score": value})


def test_a_blocked_vector_is_closed_in_both_directions():
    """A blocked row's vector is exactly `{"blocker": <code>}`.

    It never reached a score, so there is no total to reconstruct and no cap to
    apply -- `reconstruct_tier` returns 0 on the key's presence alone. A vector
    carrying a blocker AND contributions describes two different things, and the
    blocker's value is a code string rather than a number, so it is the one value
    the numeric rule must not be applied to.
    """
    blocked = {"blocker": "non_us_location"}
    assert _validate_score_row(blocked) == blocked

    with pytest.raises(scoring.ScoreFeatureError):
        _validate_score_row({**blocked, "raw_score": 3})
    with pytest.raises(scoring.ScoreFeatureError):
        _validate_score_row({"blocker": 7})


def test_the_hireability_vector_may_legitimately_be_empty():
    """No rule fired is a real answer there: the odds vector replays as a plain sum,
    so an empty vector scores 0. Requiring a key would reject a correct row.
    """
    assert scoring.validate_features(
        {}, candidate_profile.REQUIRED_HIREABILITY_FEATURES,
        required_present=candidate_profile.REQUIRED_PRESENT_HIREABILITY_FEATURES,
    ) == {}
    with pytest.raises(scoring.ScoreFeatureError):
        scoring.validate_features(
            {"senior": -1, "vibes": 1}, candidate_profile.REQUIRED_HIREABILITY_FEATURES,
            required_present=candidate_profile.REQUIRED_PRESENT_HIREABILITY_FEATURES,
        )


def test_the_contract_covers_exactly_the_weights_plus_the_computed_keys():
    """The two sets are related by construction, not by a copied list."""
    assert candidate_profile.SCORE_ROW_CONTRIBUTIONS == (
        candidate_profile.REQUIRED_SCORE_ROW_WEIGHTS
        | candidate_profile.NON_WEIGHTED_SCORE_ROW_CONTRIBUTIONS
    )
    assert candidate_profile.REQUIRED_SCORE_ROW_FEATURES == frozenset(
        candidate_profile.SCORE_ROW_CONTRIBUTIONS
        | candidate_profile.SCORE_ROW_SCALAR_FEATURES
        | set(candidate_profile.SCORE_ROW_CAP_FEATURES)
        | {candidate_profile.SCORE_ROW_BLOCKER_FEATURE}
    )
    # The cap order is a contract: `reconstruct_tier` replays them in it.
    assert candidate_profile.SCORE_ROW_CAP_FEATURES == (
        "cap_func", "cap_no_desc", "cap_stale", "cap_undated_aggregator"
    )
    # Required-present is a SUBSET of may-appear, and it is exactly the two keys
    # `_score_row_core` emits unconditionally. Requiring a conditional key (a cap,
    # or `staff_cap_delta`) would reject correct vectors; requiring neither of these
    # accepts vectors that cannot replay.
    assert candidate_profile.REQUIRED_PRESENT_SCORE_ROW_FEATURES == {
        "function_match", "raw_score"
    }
    assert (candidate_profile.REQUIRED_PRESENT_SCORE_ROW_FEATURES
            <= candidate_profile.REQUIRED_SCORE_ROW_FEATURES)
    assert candidate_profile.REQUIRED_PRESENT_HIREABILITY_FEATURES == frozenset()


# --------------------------------------------------------------------------- #
# 3. Blocker codes are a namespace
# --------------------------------------------------------------------------- #
def test_every_blocker_the_scorer_emits_is_in_the_declared_namespace():
    """Membership is asserted AT EMISSION; this proves the assertion is reachable
    and that the declared set is neither short nor padded with codes nothing emits.
    """
    emitted = set()
    for title, location, desc, posted, aggregator in _generated():
        features = rubric.score_row_explained(
            _row(title, location, posted), desc, is_aggregator=aggregator
        ).features
        code = features.get(candidate_profile.SCORE_ROW_BLOCKER_FEATURE)
        if code is not None:
            emitted.add(code)
    assert emitted, "no generated row was blocked"
    assert emitted <= candidate_profile.BLOCKER_CODES
    assert len(candidate_profile.BLOCKER_CODES) == 9


def test_an_undeclared_blocker_code_fails_at_emission(monkeypatch):
    """Revert the membership assertion and this test stops failing -- which is the
    point: a typo'd code is a new silent category in a stored, queryable namespace.
    """
    monkeypatch.setattr(
        candidate_profile, "BLOCKER_CODES", frozenset({"non_us_location"})
    )
    with pytest.raises(AssertionError):
        rubric.score_row_explained(
            _row("Manager, Technical Support Engineer", "San Jose, CA", "2026-08-01"),
            "You will support enterprise customers. 2+ years required.",
        )


# --------------------------------------------------------------------------- #
# 4. The scorer's source digest
# --------------------------------------------------------------------------- #
def test_the_pinned_scorer_source_digest_still_matches_the_code():
    """The RUBRIC_VERSION tripwire.

    When this fails, the pinned scoring surface changed. That is not a broken test:
    it is the suite asking for a decision. Update `rubric.SCORER_SOURCE_DIGEST`, and
    in the SAME edit decide whether `RUBRIC_VERSION` moves -- a shape change (a new
    dimension, a new blocker, a changed clamp or cap, a changed feature key set)
    means it does.

    Correctness does not depend on anyone getting that right: `scorer_hash` mixes in
    the COMPUTED digest, so a forgotten bump still invalidates every stored score.
    This assertion exists so the forgetting is noticed.
    """
    assert rubric.scorer_source_digest() == rubric.SCORER_SOURCE_DIGEST


def test_the_digest_moves_when_the_pinned_surface_moves(monkeypatch):
    """Otherwise the tripwire is decoration."""
    before = rubric.scorer_source_digest()

    def replacement(features):  # a different body for a pinned member
        return 0

    monkeypatch.setattr(rubric, "reconstruct_tier", replacement)
    assert rubric.scorer_source_digest() != before


def test_scorer_hash_carries_both_the_version_and_the_digest(monkeypatch):
    """A forgotten RUBRIC_VERSION bump must still invalidate stored scores."""
    baseline = scoring.scorer_identity()
    assert baseline.rubric_version == rubric.RUBRIC_VERSION
    assert baseline.source_digest == rubric.SCORER_SOURCE_DIGEST

    monkeypatch.setattr(rubric, "scorer_source_digest", lambda: "deadbeef")
    moved_digest = scoring.scorer_identity()
    assert moved_digest.scorer_hash != baseline.scorer_hash

    monkeypatch.undo()
    monkeypatch.setattr(rubric, "RUBRIC_VERSION", "rubric-testing-v99")
    moved_version = scoring.scorer_identity()
    assert moved_version.scorer_hash != baseline.scorer_hash


def test_scorer_hash_carries_the_composition_digest_as_well(monkeypatch):
    """The rubric is only half the scorer; the other half is what it is fed.

    `rubric.scorer_source_digest()` cannot see a change to how `scoring.py` builds
    the row or threads the fit pass's output into the odds pass -- and Phase 3.7's
    differential found a real bug of exactly that shape (the odds pass reading
    `flags=""`, so `staffing_w2`/`degree_gated` were dead canonically). Fixing it
    changed stored scores with `rubric.py` byte-identical, which the anti-join
    would have happily ignored had `scorer_hash` not covered the composition.
    """
    baseline = scoring.scorer_identity()
    assert baseline.composition_digest == scoring.composition_digest()

    monkeypatch.setattr(scoring, "composition_digest", lambda: "deadbeef")
    assert scoring.scorer_identity().scorer_hash != baseline.scorer_hash


@pytest.mark.parametrize("member", scoring._COMPOSITION_SURFACE_NAMES)
def test_the_composition_digest_moves_when_the_composition_moves(monkeypatch, member):
    """Otherwise that half of the identity is decoration, exactly as above.

    Parametrized over EVERY name in `_COMPOSITION_SURFACE_NAMES` rather than just
    `row_from_version`: a tuple member that never moved the digest on its own would
    sit there as decoration, indistinguishable from a member that is genuinely
    load-bearing. This monkeypatches each named function in turn (one at a time,
    the others left alone) and asserts the digest moves -- proving `member` is
    actually read by `composition_digest`, not merely listed.
    """
    before = scoring.composition_digest()

    def replacement(*args, **kwargs):  # a different body than whichever `member` has
        return None

    monkeypatch.setattr(scoring, member, replacement)
    assert scoring.composition_digest() != before, member


def test_the_composition_surface_is_exactly_row_from_version_and_score_one():
    """Pins the SET the parametrized test above walks, not just each member in
    isolation -- a member silently dropped from the tuple would still pass every
    per-member check above (there would just be one fewer of them), so that test
    alone cannot catch a shrunk tuple.

    `_score_one` MUST stay in this set: it is the function that copies the fit
    pass's flags string and recovered pay band onto the row before the odds pass
    reads it (see `_score_one`'s docstring). Phase 3.7's legacy/new differential
    found that omitting this composition step left `staffing_w2`/`degree_gated`
    dead canonically while `rubric.py` was byte-identical on both sides --
    `rubric.scorer_source_digest()` could not see that bug because nothing in
    `rubric.py` changed. Only `_score_one` in this tuple makes `scorer_hash` move
    when that wiring regresses, which is why dropping it from
    `_COMPOSITION_SURFACE_NAMES` must fail this test (confirmed by mutation:
    removing it from the tuple in `scoring.py` and re-running this suite fails
    here before the fix is reverted).
    """
    assert set(scoring._COMPOSITION_SURFACE_NAMES) == {"row_from_version", "_score_one"}


# --------------------------------------------------------------------------- #
# 5 + 6. Dead fields gone; config.json's scoring inputs folded in
# --------------------------------------------------------------------------- #
def test_the_dead_seattle_era_location_fields_are_gone(profile_doc):
    """They were read by nobody, and dead profile data is worse than none: it reads
    as a live rule, and editing it changes `profile_version_id` -- invalidating every
    stored score -- while changing no score at all.
    """
    assert set(profile_doc["location"]) == {"non_us_patterns", "bay_area_cities"}
    for dead in ("other_state_pattern", "socal_cities", "far_wa_cities", "dc_pattern"):
        assert dead not in profile_doc["location"]
    assert not hasattr(rubric, "_metro")
    assert not hasattr(rubric, "_bay")
    assert not hasattr(rubric, "_title_excl")
    assert not hasattr(rubric, "_profile")


def test_the_scorer_reads_no_config_json(monkeypatch, profile_doc):
    """Hermetic: record + description + profile.json, and nothing else.

    `load_cfg()` is booby-trapped for the duration. If any scoring path still
    reaches config.json, this raises instead of quietly scoring with a value no
    profile version covers.
    """
    def explode():
        raise AssertionError("the scorer read config.json")

    monkeypatch.setattr(rubric, "load_cfg", explode)
    for title, location, desc, posted, aggregator in _generated():
        rubric.score_row_explained(
            _row(title, location, posted), desc, is_aggregator=aggregator
        )
        rubric.hireability_explained(_row(title, location, posted), desc)


@pytest.mark.parametrize("edit", [
    lambda d: d["location"]["bay_area_cities"].append("emeryville"),
    lambda d: d["location"]["bay_area_cities"].remove(d["location"]["bay_area_cities"][0]),
    lambda d: d["exclusions"]["title_exclude"].append("apprentice"),
    lambda d: d["exclusions"]["title_exclude"].remove(d["exclusions"]["title_exclude"][0]),
])
def test_editing_a_folded_field_changes_the_profile_version(profile_doc, edit):
    """THE FOLD, stated as the property that motivated it.

    These two lists used to live in `config.json`, which nothing hashes into
    `profile_version_id`. Editing them changed the location gate and a whole class
    of title blockers for every posting while leaving the profile version -- the
    identity a stored score is filed under -- byte-identical. The corpus was never
    re-scored, and no audit could see the change. Now an edit mints a new profile
    version, which is exactly what forces a FULL pass.
    """
    before = candidate_profile.build_profile_version_row(profile_doc)
    mutated = copy.deepcopy(profile_doc)
    edit(mutated)
    after = candidate_profile.build_profile_version_row(mutated)
    assert after["content_hash"] != before["content_hash"]
    assert after["profile_version_id"] != before["profile_version_id"]


def test_the_folded_lists_actually_drive_the_scorer(profile_doc):
    """Not just hashed -- READ. A fold that hashed the data without using it would
    pass the test above and change nothing about any score.
    """
    mutated = copy.deepcopy(profile_doc)
    mutated["location"]["bay_area_cities"] = ["san francisco"]
    narrow = candidate_profile.build_profile(mutated)
    assert not any(p.search("sunnyvale, ca") for p in narrow.location.bay_area_cities)

    mutated2 = copy.deepcopy(profile_doc)
    mutated2["exclusions"]["title_exclude"] = ["engineer"]
    narrow2 = candidate_profile.build_profile(mutated2)
    assert any(p.search("support engineer") for p in narrow2.exclusions.title_exclude)


@pytest.mark.parametrize("path,value", [
    (("location", "bay_area_cities"), []),
    (("exclusions", "title_exclude"), []),
])
def test_an_emptied_folded_list_is_a_load_time_error(profile_doc, path, value):
    """An empty list here is never "no rule": an empty `bay_area_cities` reduces the
    whole location gate to US-remote-only, and an empty `title_exclude` disables a
    blocker. Both silently, for every row.
    """
    mutated = copy.deepcopy(profile_doc)
    mutated[path[0]][path[1]] = value
    with pytest.raises(candidate_profile.ProfileValidationError, match="must be non-empty"):
        candidate_profile.build_profile(mutated)


def test_a_version_1_profile_is_rejected_rather_than_up_converted(profile_doc):
    """It is missing rules the scorer now requires, and defaulting them is exactly
    the silent fallback this loader exists to prevent."""
    mutated = copy.deepcopy(profile_doc)
    mutated["schema_version"] = 1
    with pytest.raises(candidate_profile.ProfileValidationError, match="unsupported schema_version"):
        candidate_profile.build_profile(mutated)


# --------------------------------------------------------------------------- #
# Hireability
# --------------------------------------------------------------------------- #
def test_hireability_vectors_sum_to_their_score_and_compose_to_their_label(profile_doc):
    """The odds axis has no clamp, no cap, and no blocker, so its vector is a plain
    sum. Its label is no longer a score-vs-threshold comparison (profile.json's
    hireability_labels is gone as of schema_version 3): match_label and
    competition_label are pure functions of WHICH named features fired, and
    `label` is their " / "-joined combination -- so this test recomputes the
    same decision the scorer makes, from the same vector, independently of
    `rubric._hireability_core`'s internals.
    """
    candidate_profile.build_profile(profile_doc)  # profile.json itself must still validate
    seen_labels = set()
    for title, location, desc, posted, _aggregator in _generated():
        row = _row(title, location, posted)
        result = rubric.hireability_explained(row, desc)
        label = (title, desc, result.features)
        assert set(result.features) <= candidate_profile.REQUIRED_HIREABILITY_FEATURES, label
        assert sum(result.features.values()) == result.score, label

        # Must match rubric.py's own normalization (`_hireability_core`) exactly:
        # backslashes are stripped BEFORE the `len(d) > 400` skills-overlap gate
        # is measured, so a test that skipped this `.replace` would compute a
        # different length than the scorer did whenever a description carries
        # backslashes near that boundary (see the backslash corpus row above).
        d = (desc or "").lower().replace("\\", "")
        if "staff_principal" in result.features:
            expected_match = "Level stretch"
        elif "skills_strong" in result.features or "exact_stack" in result.features:
            expected_match = "Strong match"
        elif "skills_thin" in result.features:
            expected_match = "Weak match"
        elif len(d) <= 400:
            expected_match = "Unscored"
        else:
            expected_match = "Moderate match"
        if any(k in result.features for k in ("high_competition", "comp_high_bar", "degree_gated")):
            expected_competition = "High competition"
        elif "staffing_w2" in result.features:
            expected_competition = "Lower bar"
        else:
            expected_competition = "Standard"

        assert result.match_label == expected_match, label
        assert result.competition_label == expected_competition, label
        assert result.label == f"{expected_match} / {expected_competition}", label
        seen_labels.add(result.label)
    assert len(seen_labels) > 1, "the generated rows never varied the odds label"
