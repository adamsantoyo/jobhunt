#!/usr/bin/env python3
"""Validated, versioned candidate-scoring profile loader.

`profile.json` (repo root, tracked) holds every candidate-specific number and
list rubric.py's scorer uses: location gates, role families, exclusions,
skills, target employers, comp bands, level calibration, competition
modifiers, and the named weight tables `score_row`/`hireability` apply by
name. This module parses, validates, and compiles that document into a frozen
`Profile`, and provides the hashing helpers Phase 3.3 uses to mint
`profile_versions` rows.

Stdlib-only (frozen dataclasses, not pydantic): rubric.py runs OUTSIDE the
webapp's dependency environment, invoked as a bare `uv run rubric.py ...`
subprocess by `webapp/backend/sweeprunner.py`. Pulling in pydantic here would
mean two dependency worlds for one scoring path.

Validation is strict and fails loudly, on purpose (rubric.py Phase 3.4
directive: "a malformed profile fails loudly at startup, never silently
scores with defaults"):
  - unknown top-level or section keys are rejected
  - wrong types are rejected
  - every regex-bearing field is compiled at load time; a bad pattern raises
    `ProfileValidationError` naming the exact field path
  - `weights.score_row` / `weights.hireability` must contain EXACTLY the
    feature names rubric.py's scorer emits -- not a subset, not a superset.
    Adding a new scored dimension to rubric.py without a matching weight
    entry (or vice versa) is a load-time error, not a silent 0.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Mapping

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PROFILE_PATH = os.path.join(HERE, "profile.json")

# The exact feature/weight names rubric.score_row / rubric.hireability emit.
# profile.json's weights.score_row / weights.hireability tables are validated
# against these sets EXACTLY (see _validate_weight_table). Changing what
# score_row or hireability scores means updating BOTH the code and this set
# in the same change -- that coupling is the point.
REQUIRED_SCORE_ROW_WEIGHTS = frozenset({
    "tpm_senior", "tpm_ii_stretch", "stretch_level", "years_penalty",
    "years_bonus", "years_pref_penalty", "domain_tier2", "domain_tier1",
    "target_co", "stale_30d", "comp_in_band", "low_comp", "c2c",
    "staffing_w2", "degree_gated",
})
REQUIRED_HIREABILITY_WEIGHTS = frozenset({
    "staff_principal", "senior", "junior", "years_low", "years_high",
    "high_competition", "staffing_w2", "skills_strong", "skills_moderate",
    "skills_thin", "exact_stack", "comp_high_bar", "comp_near_level",
    "degree_gated",
})

_TOP_LEVEL_KEYS = frozenset({
    "schema_version", "location", "families", "domain", "skills", "targets",
    "competition", "employers", "exclusions", "level", "comp", "experience",
    "tier_rules", "hireability_labels", "weights",
})


class ProfileValidationError(ValueError):
    """profile.json failed validation. The message names the offending field
    path so a bad hand-edit fails loudly instead of silently scoring with a
    stale or default value."""


def _fail(path: str, msg: str):
    raise ProfileValidationError(f"{path}: {msg}")


def _require(cond: bool, path: str, msg: str):
    if not cond:
        _fail(path, msg)


def _nonempty(seq, path):
    """Reject an empty list/tuple/dict at load time. An empty collection here
    is never a legitimate "no rule" state -- it silently changes scorer
    behavior instead (see the call sites: an empty ic_manager_titles compiles
    a pattern that strips every "<word> manager" title, an empty
    exact_stack_patterns makes the `all()` bonus check vacuously true and
    fires on every long JD, etc.). A malformed/emptied-out profile.json must
    fail loudly, not silently disable or invert a rule."""
    _require(len(seq) > 0, path, "must be non-empty")
    return seq


def _expect_object(d, path) -> dict:
    _require(isinstance(d, dict), path, f"expected an object, got {type(d).__name__}")
    return d


def _expect_keys(d: dict, allowed: frozenset, path: str):
    """Every section is a closed set of keys: nothing unknown, nothing missing.
    This is what makes an unrecognized field in profile.json a load-time error
    instead of a silently-ignored typo."""
    have = set(d.keys())
    unknown = have - allowed
    missing = allowed - have
    _require(not unknown, path, f"unknown key(s) {sorted(unknown)}; allowed: {sorted(allowed)}")
    _require(not missing, path, f"missing required key(s) {sorted(missing)}")


def _str(v, path) -> str:
    _require(isinstance(v, str), path, f"expected a string, got {type(v).__name__}")
    return v


def _int(v, path) -> int:
    _require(isinstance(v, int) and not isinstance(v, bool), path, f"expected an integer, got {type(v).__name__}")
    return v


def _str_tuple(v, path) -> tuple:
    _require(isinstance(v, list), path, f"expected a list, got {type(v).__name__}")
    for i, item in enumerate(v):
        _require(isinstance(item, str), f"{path}[{i}]", f"expected a string, got {type(item).__name__}")
    return tuple(v)


def _compile(pattern: str, path: str) -> re.Pattern:
    _str(pattern, path)
    try:
        return re.compile(pattern)
    except re.error as e:
        _fail(path, f"invalid regex ({e}): {pattern!r}")


def _compile_list(values, path: str) -> tuple:
    _require(isinstance(values, list), path, f"expected a list of regex pattern strings, got {type(values).__name__}")
    return tuple(_compile(v, f"{path}[{i}]") for i, v in enumerate(values))


def _word_boundary_list(values, path: str) -> tuple:
    """Plain literal strings, each compiled with a `\\bword\\b`-style wrap
    (mirrors the pre-refactor call-time `re.search(r"\\b" + re.escape(c) +
    r"\\b", ...)` idiom, precompiled once at load instead of once per call)."""
    _require(isinstance(values, list), path, f"expected a list of strings, got {type(values).__name__}")
    out = []
    for i, v in enumerate(values):
        _require(isinstance(v, str), f"{path}[{i}]", f"expected a string, got {type(v).__name__}")
        out.append(re.compile(r"\b" + re.escape(v) + r"\b"))
    return tuple(out)


def _keyword_manager_pattern(titles: tuple, path: str) -> re.Pattern:
    words = "|".join(re.escape(t) for t in titles)
    return _compile(r"\b(?:" + words + r")\s+manager\b", path)


def _validate_weight_table(table, required: frozenset, path: str) -> Mapping[str, int]:
    _expect_object(table, path)
    have = set(table.keys())
    missing = required - have
    extra = have - required
    _require(not missing, path, f"missing required weight key(s) {sorted(missing)} "
             "(the scorer emits this feature but profile.json has no weight for it)")
    _require(not extra, path, f"unknown weight key(s) {sorted(extra)} "
             "(not a feature name the scorer emits -- typo, or the scorer changed and this is stale)")
    return MappingProxyType({k: _int(v, f"{path}.{k}") for k, v in table.items()})


# --------------------------------------------------------------------------- #
# Sections
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class LocationProfile:
    non_us_patterns: tuple
    other_state_pattern: re.Pattern
    socal_cities: tuple
    far_wa_cities: tuple
    dc_pattern: re.Pattern


@dataclass(frozen=True, slots=True)
class FamiliesProfile:
    keywords: Mapping[str, tuple]
    in_scope: tuple
    function_weight: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class DomainProfile:
    tier2_patterns: tuple
    tier1_patterns: tuple


@dataclass(frozen=True, slots=True)
class SkillsProfile:
    his_skills: tuple
    exact_stack_patterns: tuple


@dataclass(frozen=True, slots=True)
class TargetsProfile:
    employer_patterns: tuple


@dataclass(frozen=True, slots=True)
class CompetitionProfile:
    high_competition_patterns: tuple


@dataclass(frozen=True, slots=True)
class EmployersProfile:
    staffing_agencies: tuple
    c2c_keywords: tuple


@dataclass(frozen=True, slots=True)
class ExclusionsProfile:
    disqualifying_skills: tuple
    clearance_required_pattern: re.Pattern
    clearance_condition_pattern: re.Pattern
    clearance_exception_pattern: re.Pattern
    people_management_pattern: re.Pattern
    ic_manager_pattern: re.Pattern
    degree_required_pattern: re.Pattern
    degree_equivalent_exception: str


@dataclass(frozen=True, slots=True)
class LevelProfile:
    hireability_staff_principal_pattern: re.Pattern
    hireability_senior_pattern: re.Pattern
    hireability_junior_pattern: re.Pattern
    score_senior_pattern: re.Pattern
    staff_cap_pattern: re.Pattern
    tpm_ii_pattern: re.Pattern
    validation_design_keywords: tuple
    support_bigtech_companies: tuple


@dataclass(frozen=True, slots=True)
class CompProfile:
    hireability_high_bar: int
    hireability_near_level: int
    band_low: int
    band_high: int
    low_comp_threshold: int


@dataclass(frozen=True, slots=True)
class ExperienceProfile:
    hireability_bonus_years_max: int
    hireability_penalty_years_min: int
    blocker_years_min: int
    score_penalty_years_low: int
    score_penalty_years_high: int
    score_bonus_years_max: int
    score_pref_penalty_years_min: int


@dataclass(frozen=True, slots=True)
class TierRulesProfile:
    staff_cap_tier: int
    func_cap_min_func: int
    func_cap_tier: int
    no_desc_cap_tier: int
    stale_penalty_days: int
    stale_cap_days: int
    stale_cap_tier: int
    undated_aggregator_cap_tier: int


@dataclass(frozen=True, slots=True)
class HireabilityLabelsProfile:
    likely_threshold: int
    reach_threshold: int


@dataclass(frozen=True, slots=True)
class WeightsProfile:
    hireability: Mapping[str, int]
    score_row: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class Profile:
    """The fully validated, compiled candidate-scoring profile. `raw` is the
    original parsed JSON (unmodified), kept for hashing/storage -- it is what
    `profile_content_hash()` and `build_profile_version_row()` hash, never
    the compiled dataclass."""
    schema_version: int
    location: LocationProfile
    families: FamiliesProfile
    domain: DomainProfile
    skills: SkillsProfile
    targets: TargetsProfile
    competition: CompetitionProfile
    employers: EmployersProfile
    exclusions: ExclusionsProfile
    level: LevelProfile
    comp: CompProfile
    experience: ExperienceProfile
    tier_rules: TierRulesProfile
    hireability_labels: HireabilityLabelsProfile
    weights: WeightsProfile
    raw: Mapping

    @property
    def content_hash(self) -> str:
        return profile_content_hash(self.raw)


def _build_location(doc, path) -> LocationProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"non_us_patterns", "other_state_pattern", "socal_cities",
                                "far_wa_cities", "dc_pattern"}), path)
    return LocationProfile(
        non_us_patterns=_nonempty(_compile_list(d["non_us_patterns"], f"{path}.non_us_patterns"), f"{path}.non_us_patterns"),
        other_state_pattern=_compile(d["other_state_pattern"], f"{path}.other_state_pattern"),
        socal_cities=_str_tuple(d["socal_cities"], f"{path}.socal_cities"),
        far_wa_cities=_str_tuple(d["far_wa_cities"], f"{path}.far_wa_cities"),
        dc_pattern=_compile(d["dc_pattern"], f"{path}.dc_pattern"),
    )


def _build_families(doc, path) -> FamiliesProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"keywords", "in_scope", "function_weight"}), path)
    kw_doc = _expect_object(d["keywords"], f"{path}.keywords")
    _require(len(kw_doc) > 0, f"{path}.keywords", "must be non-empty")
    keywords = {}
    for fam, kws in kw_doc.items():
        keywords[fam] = _str_tuple(kws, f"{path}.keywords.{fam}")
    in_scope = _str_tuple(d["in_scope"], f"{path}.in_scope")
    _require(len(in_scope) > 0, f"{path}.in_scope", "must be non-empty")
    for fam in in_scope:
        _require(fam in keywords, f"{path}.in_scope", f"references unknown family {fam!r}")
    fw_doc = _expect_object(d["function_weight"], f"{path}.function_weight")
    _require(len(fw_doc) > 0, f"{path}.function_weight", "must be non-empty")
    # function_weight must name EXACTLY the families keywords defines -- not a
    # subset (a family missing its weight would silently score with the base
    # weight of 1 instead of its intended value) and not a superset (a stale
    # weight entry for a renamed/removed family is a load-time error, not
    # dead data).
    kw_keys = set(keywords.keys())
    fw_keys = set(fw_doc.keys())
    missing = kw_keys - fw_keys
    extra = fw_keys - kw_keys
    _require(not missing, f"{path}.function_weight",
             f"missing weight(s) for family key(s) {sorted(missing)} defined in {path}.keywords")
    _require(not extra, f"{path}.function_weight",
             f"unknown family key(s) {sorted(extra)} not defined in {path}.keywords")
    function_weight = {}
    for fam, w in fw_doc.items():
        function_weight[fam] = _int(w, f"{path}.function_weight.{fam}")
    return FamiliesProfile(
        keywords=MappingProxyType(keywords),
        in_scope=in_scope,
        function_weight=MappingProxyType(function_weight),
    )


def _build_domain(doc, path) -> DomainProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"tier2_patterns", "tier1_patterns"}), path)
    return DomainProfile(
        tier2_patterns=_compile_list(d["tier2_patterns"], f"{path}.tier2_patterns"),
        tier1_patterns=_compile_list(d["tier1_patterns"], f"{path}.tier1_patterns"),
    )


def _build_skills(doc, path) -> SkillsProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"his_skills", "exact_stack_patterns"}), path)
    return SkillsProfile(
        his_skills=_compile_list(d["his_skills"], f"{path}.his_skills"),
        exact_stack_patterns=_nonempty(
            _compile_list(d["exact_stack_patterns"], f"{path}.exact_stack_patterns"), f"{path}.exact_stack_patterns"),
    )


def _build_targets(doc, path) -> TargetsProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"employer_patterns"}), path)
    return TargetsProfile(employer_patterns=_nonempty(
        _compile_list(d["employer_patterns"], f"{path}.employer_patterns"), f"{path}.employer_patterns"))


def _build_competition(doc, path) -> CompetitionProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"high_competition_employers"}), path)
    return CompetitionProfile(
        high_competition_patterns=_word_boundary_list(d["high_competition_employers"], f"{path}.high_competition_employers"),
    )


def _build_employers(doc, path) -> EmployersProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"staffing_agencies", "c2c_keywords"}), path)
    return EmployersProfile(
        staffing_agencies=_str_tuple(d["staffing_agencies"], f"{path}.staffing_agencies"),
        c2c_keywords=_str_tuple(d["c2c_keywords"], f"{path}.c2c_keywords"),
    )


def _build_exclusions(doc, path) -> ExclusionsProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({
        "disqualifying_skills", "clearance_required_pattern", "clearance_condition_pattern",
        "clearance_exception_pattern", "people_management_pattern", "ic_manager_titles",
        "degree_required_pattern", "degree_equivalent_exception",
    }), path)
    ic_titles = _nonempty(_str_tuple(d["ic_manager_titles"], f"{path}.ic_manager_titles"), f"{path}.ic_manager_titles")
    return ExclusionsProfile(
        disqualifying_skills=_str_tuple(d["disqualifying_skills"], f"{path}.disqualifying_skills"),
        clearance_required_pattern=_compile(d["clearance_required_pattern"], f"{path}.clearance_required_pattern"),
        clearance_condition_pattern=_compile(d["clearance_condition_pattern"], f"{path}.clearance_condition_pattern"),
        clearance_exception_pattern=_compile(d["clearance_exception_pattern"], f"{path}.clearance_exception_pattern"),
        people_management_pattern=_compile(d["people_management_pattern"], f"{path}.people_management_pattern"),
        ic_manager_pattern=_keyword_manager_pattern(ic_titles, f"{path}.ic_manager_titles"),
        degree_required_pattern=_compile(d["degree_required_pattern"], f"{path}.degree_required_pattern"),
        degree_equivalent_exception=_str(d["degree_equivalent_exception"], f"{path}.degree_equivalent_exception"),
    )


def _build_level(doc, path) -> LevelProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({
        "hireability_staff_principal_pattern", "hireability_senior_pattern", "hireability_junior_pattern",
        "score_senior_pattern", "staff_cap_pattern", "tpm_ii_pattern",
        "validation_design_keywords", "support_bigtech_companies",
    }), path)
    return LevelProfile(
        hireability_staff_principal_pattern=_compile(d["hireability_staff_principal_pattern"], f"{path}.hireability_staff_principal_pattern"),
        hireability_senior_pattern=_compile(d["hireability_senior_pattern"], f"{path}.hireability_senior_pattern"),
        hireability_junior_pattern=_compile(d["hireability_junior_pattern"], f"{path}.hireability_junior_pattern"),
        score_senior_pattern=_compile(d["score_senior_pattern"], f"{path}.score_senior_pattern"),
        staff_cap_pattern=_compile(d["staff_cap_pattern"], f"{path}.staff_cap_pattern"),
        tpm_ii_pattern=_compile(d["tpm_ii_pattern"], f"{path}.tpm_ii_pattern"),
        validation_design_keywords=_str_tuple(d["validation_design_keywords"], f"{path}.validation_design_keywords"),
        support_bigtech_companies=_str_tuple(d["support_bigtech_companies"], f"{path}.support_bigtech_companies"),
    )


def _build_comp(doc, path) -> CompProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({
        "hireability_high_bar", "hireability_near_level", "band_low", "band_high", "low_comp_threshold",
    }), path)
    return CompProfile(
        hireability_high_bar=_int(d["hireability_high_bar"], f"{path}.hireability_high_bar"),
        hireability_near_level=_int(d["hireability_near_level"], f"{path}.hireability_near_level"),
        band_low=_int(d["band_low"], f"{path}.band_low"),
        band_high=_int(d["band_high"], f"{path}.band_high"),
        low_comp_threshold=_int(d["low_comp_threshold"], f"{path}.low_comp_threshold"),
    )


def _build_experience(doc, path) -> ExperienceProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({
        "hireability_bonus_years_max", "hireability_penalty_years_min", "blocker_years_min",
        "score_penalty_years_low", "score_penalty_years_high", "score_bonus_years_max",
        "score_pref_penalty_years_min",
    }), path)
    return ExperienceProfile(
        hireability_bonus_years_max=_int(d["hireability_bonus_years_max"], f"{path}.hireability_bonus_years_max"),
        hireability_penalty_years_min=_int(d["hireability_penalty_years_min"], f"{path}.hireability_penalty_years_min"),
        blocker_years_min=_int(d["blocker_years_min"], f"{path}.blocker_years_min"),
        score_penalty_years_low=_int(d["score_penalty_years_low"], f"{path}.score_penalty_years_low"),
        score_penalty_years_high=_int(d["score_penalty_years_high"], f"{path}.score_penalty_years_high"),
        score_bonus_years_max=_int(d["score_bonus_years_max"], f"{path}.score_bonus_years_max"),
        score_pref_penalty_years_min=_int(d["score_pref_penalty_years_min"], f"{path}.score_pref_penalty_years_min"),
    )


def _build_tier_rules(doc, path) -> TierRulesProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({
        "staff_cap_tier", "func_cap_min_func", "func_cap_tier", "no_desc_cap_tier",
        "stale_penalty_days", "stale_cap_days", "stale_cap_tier", "undated_aggregator_cap_tier",
    }), path)
    return TierRulesProfile(
        staff_cap_tier=_int(d["staff_cap_tier"], f"{path}.staff_cap_tier"),
        func_cap_min_func=_int(d["func_cap_min_func"], f"{path}.func_cap_min_func"),
        func_cap_tier=_int(d["func_cap_tier"], f"{path}.func_cap_tier"),
        no_desc_cap_tier=_int(d["no_desc_cap_tier"], f"{path}.no_desc_cap_tier"),
        stale_penalty_days=_int(d["stale_penalty_days"], f"{path}.stale_penalty_days"),
        stale_cap_days=_int(d["stale_cap_days"], f"{path}.stale_cap_days"),
        stale_cap_tier=_int(d["stale_cap_tier"], f"{path}.stale_cap_tier"),
        undated_aggregator_cap_tier=_int(d["undated_aggregator_cap_tier"], f"{path}.undated_aggregator_cap_tier"),
    )


def _build_hireability_labels(doc, path) -> HireabilityLabelsProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"likely_threshold", "reach_threshold"}), path)
    return HireabilityLabelsProfile(
        likely_threshold=_int(d["likely_threshold"], f"{path}.likely_threshold"),
        reach_threshold=_int(d["reach_threshold"], f"{path}.reach_threshold"),
    )


def _build_weights(doc, path) -> WeightsProfile:
    d = _expect_object(doc, path)
    _expect_keys(d, frozenset({"hireability", "score_row"}), path)
    return WeightsProfile(
        hireability=_validate_weight_table(d["hireability"], REQUIRED_HIREABILITY_WEIGHTS, f"{path}.hireability"),
        score_row=_validate_weight_table(d["score_row"], REQUIRED_SCORE_ROW_WEIGHTS, f"{path}.score_row"),
    )


def build_profile(doc) -> Profile:
    """Validate and compile a parsed profile.json document into a `Profile`.
    Raises `ProfileValidationError` naming the offending field on any problem.
    Pure function of `doc` -- no file I/O, so tests can hand it a mutated
    in-memory dict to exercise a specific validation failure."""
    d = _expect_object(doc, "profile")
    _expect_keys(d, _TOP_LEVEL_KEYS, "profile")
    schema_version = _int(d["schema_version"], "profile.schema_version")
    _require(schema_version == 1, "profile.schema_version", f"unsupported schema_version {schema_version} (only 1 is known)")
    return Profile(
        schema_version=schema_version,
        location=_build_location(d["location"], "profile.location"),
        families=_build_families(d["families"], "profile.families"),
        domain=_build_domain(d["domain"], "profile.domain"),
        skills=_build_skills(d["skills"], "profile.skills"),
        targets=_build_targets(d["targets"], "profile.targets"),
        competition=_build_competition(d["competition"], "profile.competition"),
        employers=_build_employers(d["employers"], "profile.employers"),
        exclusions=_build_exclusions(d["exclusions"], "profile.exclusions"),
        level=_build_level(d["level"], "profile.level"),
        comp=_build_comp(d["comp"], "profile.comp"),
        experience=_build_experience(d["experience"], "profile.experience"),
        tier_rules=_build_tier_rules(d["tier_rules"], "profile.tier_rules"),
        hireability_labels=_build_hireability_labels(d["hireability_labels"], "profile.hireability_labels"),
        weights=_build_weights(d["weights"], "profile.weights"),
        raw=MappingProxyType(json.loads(json.dumps(d))),  # deep, JSON-only copy
    )


# --------------------------------------------------------------------------- #
# Loading + hashing
# --------------------------------------------------------------------------- #
_CACHE: dict = {}


def load_profile(path: str | None = None) -> Profile:
    """Memoized like rubric.load_cfg(): parsed and validated once per distinct
    path per process. A malformed file raises ProfileValidationError (or
    json.JSONDecodeError for malformed JSON) the first time it is touched --
    there is no lazy/partial load that could silently fall back to defaults."""
    resolved = os.path.abspath(path) if path else DEFAULT_PROFILE_PATH
    if resolved not in _CACHE:
        with open(resolved) as f:
            doc = json.load(f)
        _CACHE[resolved] = build_profile(doc)
    return _CACHE[resolved]


def profile_content_hash(doc) -> str:
    """Stable digest of a profile document: sha256 over canonical JSON
    (sorted keys, no incidental whitespace) so key order in the source file
    never affects the hash. Mirrors contract.py's `_stable_digest`.

    Accepts a plain dict or a `Profile.raw` (a `MappingProxyType`, which
    `json.dumps` cannot serialize directly) -- `dict(...)` unwraps only the
    outer proxy; the nested values are already plain dict/list because `raw`
    is built via a JSON round-trip."""
    payload = dict(doc) if isinstance(doc, MappingProxyType) else doc
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# Fixed namespace for deriving a deterministic profile_version_id from a
# content hash, so repeat calls with the same document are idempotent (this
# matters because profile_versions.content_hash is UNIQUE -- an INSERT OR
# IGNORE keyed on a stable id, not a fresh uuid4 each time, is what makes that
# constraint actually dedupe instead of erroring on the second sweep).
_PROFILE_VERSION_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "jobhunt:profile_versions")


def build_profile_version_row(profile_doc, *,
                               profile_version_id: str | None = None,
                               created_at: str | None = None) -> dict:
    """Pure helper: returns a dict shaped exactly for
    `INSERT INTO profile_versions (profile_version_id, content_hash,
    profile_json, rubric_hash, created_at) VALUES (...)`. Does NOT touch any
    database -- Phase 3.3 owns deciding when a profile_versions row actually
    gets written (e.g. once per sweep, or only on content-hash change).

    Profile identity is candidate DATA ONLY: `profile_version_id` and
    `content_hash` are derived solely from `profile_doc`, so the same
    candidate profile always maps to the same row regardless of which
    version of the scoring code (rubric.RUBRIC_VERSION) is running. Scorer
    identity is a separate axis and lives in `score_versions.scorer_hash`
    (NOT NULL there, added by Phase 3.3) -- a profile_versions row keyed on
    both would collide two different rubric versions run against the same
    profile.json into one content_hash, and INSERT OR IGNORE would silently
    keep whichever rubric_hash got there first. `rubric_hash` here is always
    None; the column stays nullable for exactly this reason."""
    content_hash = profile_content_hash(profile_doc)
    payload = dict(profile_doc) if isinstance(profile_doc, MappingProxyType) else profile_doc
    profile_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if profile_version_id is None:
        profile_version_id = str(uuid.uuid5(_PROFILE_VERSION_NAMESPACE, content_hash))
    if created_at is None:
        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return {
        "profile_version_id": profile_version_id,
        "content_hash": content_hash,
        "profile_json": profile_json,
        "rubric_hash": None,
        "created_at": created_at,
    }
