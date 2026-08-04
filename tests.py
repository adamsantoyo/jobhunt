#!/usr/bin/env python3
"""Regression fixtures for the rubric engine. Every bug this system has shipped
lives here as a test. Run before every sweep; non-zero exit = do not sweep.
Usage: python3 tests.py"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rubric import score_row, years_required, salary_from_desc, posting_age_days, dedupe_by_url
from scraper import parse_salary, canon_company

FAILS = []
def check(name, cond, detail=""):
    if cond: print(f"  ok  {name}")
    else: print(f" FAIL {name}  {detail}"); FAILS.append(name)

def row(title, company="TestCo", loc="San Francisco, CA", salary=""):
    return {"title": title, "company": company, "location": loc, "salary": salary,
            "salary_min": "", "salary_max": "", "source": "test", "url": "u", "remote": "False"}

GOOD_DESC = "You will support enterprise customers on Azure and Intune. 2+ years of experience required. Bachelor's degree or equivalent experience."

print("== parse_salary ==")
check("hourly 2-digit", parse_salary("31-64 hourly") == (64480, 133120), parse_salary("31-64 hourly"))
check("$/hour", parse_salary("$85 - $92 per hour") == (176800, 191360), parse_salary("$85 - $92 per hour"))
check("k-suffix pair", parse_salary("120-150k annually") == (120000, 150000), parse_salary("120-150k annually"))
check("decimal k", parse_salary("$47.5K - $95K") == (47500, 95000), parse_salary("$47.5K - $95K"))
check("plain annual", parse_salary("80200.0-130700.0 yearly") == (80200, 130700), parse_salary("80200.0-130700.0 yearly"))
check("401k ignored", parse_salary("401(k) match plus $95,000-$120,000") == (95000, 120000))

print("== salary_from_desc ==")
check("USD format", salary_from_desc("compensation: USD 129,000.00 - 171,000.00 per year plus benefits") == (129000, 171000))
check("hourly desc", salary_from_desc("pay rate is $85 - $92 per hour depending on experience") == (176800, 191360))
check("401k not a band", salary_from_desc("we offer a 401(k) and great benefits") == (None, None))

print("== years_required ==")
check("company age ignored", years_required("celebrating 25 years in business. 2+ years of support experience required.") == (2, None))
check("preferred split", years_required("5+ years of experience required. 8+ years preferred.") == (5, 8))
check("escaped-plus handled upstream", years_required("8+ years of program management experience")[0] == 8)
check("non-experience 'of N years' ignored", years_required("benefits vest over a period of 5 years") == (None, None),
     years_required("benefits vest over a period of 5 years"))

print("== posting_age_days ==")
check("iso date", posting_age_days("2026-07-01") is not None and posting_age_days("2026-07-01") >= 0)
check("days-ago text", posting_age_days("Posted 30+ Days Ago") == 30)
check("posted today", posting_age_days("Posted Today") == 0)
check("empty is unknown", posting_age_days("") is None)

print("== canon_company ==")
check("amazon.com", canon_company("Amazon.com") == canon_company("Amazon"))
check("crusoe case", canon_company("CRUSOE") == canon_company("Crusoe"))
check("boeing", canon_company("The Boeing Company") == canon_company("Boeing"))

print("== score_row blockers/levels ==")
t, w, f = score_row(row("Technical Support Engineer II", "Datadog", "San Francisco, CA"), GOOD_DESC)
check("'II' support role not people-mgr-blocked", t > 0, (t, f))
t, _, _ = score_row(row("Manager, Technical Support Engineer", "CoreWeave", "San Jose, CA"), GOOD_DESC)
check("Manager-comma blocked", t == 0)
t, _, _ = score_row(row("Sr. Manager of Programs", "Wipro", "Oakland, CA"), GOOD_DESC)
check("Manager-of blocked", t == 0)
t, _, _ = score_row(row("Technical Program Manager", "Ring", "Palo Alto, CA"), GOOD_DESC)
check("TPM excluded from output", t == 0, t)
t, _, _ = score_row(row("Internal Support Engineer"), GOOD_DESC)
check("'Internal' not blocked by intern", t > 0, t)
t, _, _ = score_row(row("International Support Engineer"), GOOD_DESC)
check("'International' not blocked by intern", t > 0, t)
t, _, _ = score_row(row("Software Engineering Intern"), GOOD_DESC)
check("Intern blocked", t == 0)
t, _, f = score_row(row("Staff Technical Support Engineer"), GOOD_DESC)
check("Staff capped at 2", 0 < t <= 2 and "level-out" in f, (t, f))
t, _, _ = score_row(row("Manufacturing Supervisor", "SpaceX"), GOOD_DESC)
check("Supervisor blocked", t == 0)
t, _, _ = score_row(row("Engineering Manager, Silicon Assembly", "SpaceX"), GOOD_DESC)
check("mid-title Manager blocked", t == 0)
t, _, _ = score_row(row("Production Manager", "SpaceX"), GOOD_DESC)
check("Production Manager blocked", t == 0)
t, _, _ = score_row(row("Support Engineer"), "must currently hold an active top secret clearance. azure support role.")
check("clearance-hold blocked", t == 0)
t, _, _ = score_row(row("Support Engineer"), "ability to obtain a security clearance. azure support. 2+ years of experience.")
check("clearance-obtain allowed", t > 0)
t, _, _ = score_row(row("Support Engineer"), "8\\+ years of experience required in support.")
check("escaped 8+ blocked", t == 0)
t, _, f = score_row(row("Support Engineer"), "3+ years of experience required. 10+ years preferred is ideal.")
check("preferred-10 soft not block", t > 0 and any("preferred" in x for x in f), (t, f))

print("== score_row location (Bay Area + US-remote ONLY) ==")
t, _, f = score_row(row("Support Engineer", loc="San Francisco, CA"), GOOD_DESC)
check("San Francisco passes", t > 0 and "off-target-loc" not in f, (t, f))
t, _, f = score_row(row("Support Engineer", loc="Sunnyvale, CA"), GOOD_DESC)
check("Sunnyvale passes", t > 0 and "off-target-loc" not in f, (t, f))
r_ = row("Support Engineer", loc="Remote, US"); r_["remote"] = "True"
t, _, f = score_row(r_, GOOD_DESC)
check("US-remote (flag) passes", t > 0 and "off-target-loc" not in f, (t, f))
t, _, f = score_row(row("Support Engineer", loc="Remote - Anywhere in the US"), GOOD_DESC)
check("remote-in-location passes", t > 0 and "off-target-loc" not in f, (t, f))
t, _, f = score_row(row("Support Engineer", loc="Seattle, WA"), GOOD_DESC)
check("Seattle now off-target", t == 0 and "off-target-loc" in f, (t, f))
t, _, f = score_row(row("Support Engineer", loc="Austin, TX"), GOOD_DESC)
check("other-state office off-target", t == 0 and "off-target-loc" in f, (t, f))
t, _, _ = score_row(row("Support Engineer", loc="Bengaluru, KA"), GOOD_DESC)
check("Bengaluru blocked non-US", t == 0)
t, _, f = score_row(row("Support Engineer", loc="Fremont, NE"), GOOD_DESC)
check("Fremont NE not rescued as Bay city", t == 0 and "off-target-loc" in f, (t, f))
t, _, f = score_row(row("Support Engineer", loc="San Francisco, CA | New York City, NY | Seattle, WA"), GOOD_DESC)
check("multi-location incl SF passes", t > 0 and "off-target-loc" not in f, (t, f))
t, _, f = score_row(row("Support Engineer", loc="New York City, NY; San Francisco, CA; Seattle, WA"), GOOD_DESC)
check("multi-location NYC-first incl SF passes", t > 0 and "off-target-loc" not in f, (t, f))

print("== score_row staleness ==")
r_ = row("Support Engineer"); r_["posted"] = "2024-12-27"
t, _, f = score_row(r_, GOOD_DESC)
check(">90d capped at 3", t <= 3 and "stale-90d+" in f, (t, f))
r_ = row("Support Engineer"); r_["posted"] = "Posted 45 days ago"
t, _, f = score_row(r_, GOOD_DESC)
check(">30d penalized", "30d+" in f, f)
r_ = row("Technical Support Engineer", company="Okta"); r_["source"] = "jobspy-indeed"
t, _, f = score_row(r_, "Support enterprise customers on Azure and Intune identity workflows. 1+ years of experience required. salary range $90,000 - $110,000.")
check("undated aggregator capped at 3", t <= 3 and "undated-aggregator" in f, (t, f))

print("== dedupe_by_url ==")
a = {"url": "u1", "tier": 5, "source": "resolved-greenhouse"}
b = {"url": "u1", "tier": 5, "source": "greenhouse"}
c = {"url": "u2", "tier": 3, "source": "greenhouse"}
dd = dedupe_by_url([a, b, c])
check("same-url collapsed", len(dd) == 2, len(dd))
check("direct record preferred", next(x for x in dd if x["url"] == "u1")["source"] == "greenhouse")
check("higher tier wins", dedupe_by_url([{"url": "u", "tier": 3, "source": "s"}, {"url": "u", "tier": 4, "source": "s"}])[0]["tier"] == 4)

print("== score_row domain/EEO ==")
t1, w1, _ = score_row(row("Support Engineer"), "We are an equal opportunity employer regardless of gender identity or sexual orientation. General helpdesk duties. 2+ years of experience.")
check("EEO not domain", "domain" not in w1, w1)
t2, w2, _ = score_row(row("Support Engineer"), "Manage Intune and Entra identity and access workflows. 2+ years of experience.")
check("real domain scores", "domain" in w2 and t2 > t1, (t1, t2))
_, w3, _ = score_row(row("Program Manager"), "track record of tracking central programs. 3+ years of experience.")
check("track != rack, central != entra", "domain" not in w3, w3)

print("== score_row employment/desc ==")
t, _, f = score_row(row("Support Engineer", company="Apex Systems"), GOOD_DESC)
check("staffing flagged not dropped", t >= 1 and "Staffing/W2" in f, (t, f))
t, _, f = score_row(row("Support Engineer"), "this is a corp to corp position. azure. 2+ years of experience.")
check("C2C penalized", "C2C" in f)
t, _, f = score_row(row("Technical Support Engineer", company="Okta"), "")
check("no-desc caps at 3", t <= 3 and "desc-unavailable" in f, (t, f))
t, w, f = score_row(row("Support Engineer"), "hands-on Azure support role. salary range $90,000 - $110,000. 2+ years of experience.")
check("salary-from-desc fires", "salary-from-desc" in f and "comp in band" in w, (t, w, f))

print("== unrelated-role exclusion ==")
# a non-support role that only matches a domain keyword must be EXCLUDED, not kept as tier 2
t, why, f = score_row(row("Infrastructure Engineer", loc="San Francisco, CA"),
                      "Build cloud infrastructure on Azure. Python, Terraform. 3 years.")
check("Infrastructure Engineer w/ azure keyword excluded", t == 0 and "role-family" in why, (t, why))
t, _, _ = score_row(row("Silicon Design Verification Engineer", loc="San Jose, CA"),
                    "Own RTL verification and datacenter silicon bring-up.")
check("Silicon Design Verification excluded", t == 0, t)
t, _, f = score_row(row("Technical Support Engineer", loc="San Francisco, CA"),
                    "Support customers on Azure and Intune. 2 years.")
check("real support role still kept", t > 0, (t, f))

print("== in-scope gate (only support/validation/techops survive) ==")
STRONG = "Support enterprise customers on Azure and Intune identity. 2+ years. salary range $110,000 - $140,000."
t, _, _ = score_row(row("Technical Program Manager", loc="San Francisco, CA"), STRONG)
check("TPM excluded", t == 0, t)
t, _, _ = score_row(row("Solutions Engineer", loc="San Francisco, CA"), STRONG)
check("Solutions Engineer excluded", t == 0, t)
t, _, f = score_row(row("Technical Support Engineer", loc="San Francisco, CA"), STRONG)
check("Support Engineer survives", t > 0, (t, f))
t, _, f = score_row(row("Hardware Validation Engineer", loc="San Jose, CA"), "Validate firmware and BIOS on servers. 2 years.")
check("Hardware validation survives", t > 0, (t, f))

print("== hireability (match/competition axis) ==")
from rubric import hireability, hireability_explained
r_ = row("IT Support Specialist II", company="Acme Co"); r_["salary_max"]="80000"; r_["flags"]=""
lab, sc, _ = hireability(r_, "Support Windows endpoints with Intune and Entra ID, Active Directory, M365, ServiceNow tickets. 2+ years.")
check("junior/near-level, short JD scores Unscored / Standard", lab == "Unscored / Standard" and sc >= 3, (lab, sc))
r_ = row("Senior Support Engineer", company="Anthropic"); r_["salary_max"]="250000"; r_["flags"]=""
lab, sc, _ = hireability(r_, "Dedicated enterprise support for our API. 5+ years experience.")
check("elite+senior+highcomp scores Unscored / High competition", lab == "Unscored / High competition" and sc <= -2, (lab, sc))
r_ = row("Support Engineer", company="MidCo"); r_["salary_max"]="120000"; r_["flags"]=""
lab, _, _ = hireability(r_, "Provide product support and troubleshoot SaaS REST API issues. 3 years.")
check("mid role (no rule fires) scores Unscored / Standard", lab == "Unscored / Standard", lab)
# word-boundary: an elite-company substring must not tag an unrelated employer
r_ = row("Support Engineer", company="Metabase"); r_["salary_max"]="120000"; r_["flags"]=""
res_ = hireability_explained(r_, "short")
check("Metabase not flagged as Meta", res_.competition_label != "High competition" and "high-competition" not in res_.why, (res_.competition_label, res_.why))
# staff/principal titles are a level stretch regardless of skills evidence
r_ = row("Staff Support Engineer", company="Acme Co")
res_ = hireability_explained(r_, "Support customers with their tickets.")
check("staff title scores Level stretch / Standard", res_.label == "Level stretch / Standard", res_.label)
# a long JD hitting his exact daily stack is a strong match, independent of competition
LONG_STACK_DESC = ("You will provide technical support and troubleshoot issues for enterprise "
    "customers. Manage Intune and Entra ID device policies, Active Directory, Microsoft 365, "
    "ServiceNow tickets, escalation handling, incident response, PowerShell automation, SSO "
    "configuration, and endpoint management across the fleet. REST API integration and SaaS "
    "platform support round out the role. 2+ years of experience required.")
r_ = row("Support Engineer", company="Acme Co")
res_ = hireability_explained(r_, LONG_STACK_DESC)
check("exact-stack long JD scores Strong match / Standard", res_.label == "Strong match / Standard", res_.label)

print("== resolver ==")
from rubric import _title_sim, build_registry
import json as _json, os as _os
check("sim exact", _title_sim("Cloud Support Engineer", "Cloud Support Engineer") == 1.0)
check("sim reordered", _title_sim("Support Engineer, Amazon Leo", "Amazon Leo Support Engineer") >= 0.99)
check("sim near-variant", _title_sim("Technical Support Engineer, Axon 911", "Technical Support Engineer - Axon 911") >= 0.72)
check("sim rejects different role", _title_sim("Cloud Support Engineer", "Senior Accountant") < 0.3)
cfg = _json.load(open(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "config.json")))
reg = build_registry(cfg)
check("registry: spacex", canon_company("SpaceX") in reg and reg[canon_company("SpaceX")][0] == "greenhouse")
check("registry: costco", reg.get("costco wholesale", ("",))[0] == "costco")
check("registry: fred hutch", canon_company("Fred Hutchinson Cancer Center") in reg)
check("registry: starbucks eightfold", reg.get(canon_company("Starbucks"), ("",))[0] == "eightfold")

print("== profile.py: validation, hashing, feature vectors (Phase 3.4) ==")
import candidate_profile as _prof_mod
import rubric as _rubric_mod
from rubric import score_row_explained, hireability_explained, RUBRIC_VERSION

_PROFILE_DOC = _json.load(open(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "profile.json")))

def _mutated(fn):
    """Deep-copy the real profile.json, let fn(doc) mutate it in place, return it."""
    d = _json.loads(_json.dumps(_PROFILE_DOC))
    fn(d)
    return d

def _rejects(name, mutate_fn, needle=""):
    d = _mutated(mutate_fn)
    try:
        _prof_mod.build_profile(d)
        check(name, False, "expected ProfileValidationError, got none")
    except _prof_mod.ProfileValidationError as e:
        check(name, needle in str(e), str(e))

# -- validation failures --
check("valid profile.json loads clean", _prof_mod.build_profile(_PROFILE_DOC) is not None)

# profile.example.json (tracked, scrubbed placeholder) must independently pass
# the same validator as the real (gitignored) profile.json -- this is what
# keeps CI green without the real candidate data ever being committed.
_EXAMPLE_PROFILE_PATH = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "profile.example.json")
_EXAMPLE_PROFILE_DOC = _json.load(open(_EXAMPLE_PROFILE_PATH))
check("profile.example.json loads clean through the validator",
      _prof_mod.build_profile(_EXAMPLE_PROFILE_DOC) is not None)
_rejects("unknown top-level key rejected", lambda d: d.update(bogus_field=1), "unknown key")
_rejects("missing top-level key rejected", lambda d: d.pop("comp"), "missing required key")
_rejects("bad regex reported by field name", lambda d: d["location"].__setitem__(
    "non_us_patterns", ["[unclosed"] + d["location"]["non_us_patterns"][1:]), "location.non_us_patterns")
_rejects("wrong type rejected", lambda d: d["comp"].__setitem__("band_low", "99000"), "expected an integer")
_rejects("missing score_row weight rejected", lambda d: d["weights"]["score_row"].pop("c2c"), "missing required weight")
_rejects("extra score_row weight rejected", lambda d: d["weights"]["score_row"].__setitem__("nonsense", 1), "unknown weight key")
_rejects("missing hireability weight rejected", lambda d: d["weights"]["hireability"].pop("senior"), "missing required weight")
_rejects("unknown family in in_scope rejected", lambda d: d["families"].__setitem__("in_scope", ["ghost_family"]), "unknown family")

# -- empty-collection guards (an empty list here silently changes scorer
# behavior instead of raising: e.g. an empty ic_manager_titles compiles
# `\b(?:)\s+manager\b`, which matches every "<word> manager" title and
# silently disables the people-management blocker; an empty
# exact_stack_patterns makes the `all()` bonus check vacuously true and fires
# on every long JD) --
_rejects("empty ic_manager_titles rejected", lambda d: d["exclusions"].__setitem__("ic_manager_titles", []),
         "must be non-empty")
_rejects("empty exact_stack_patterns rejected", lambda d: d["skills"].__setitem__("exact_stack_patterns", []),
         "must be non-empty")
_rejects("empty families.function_weight rejected", lambda d: d["families"].__setitem__("function_weight", {}),
         "must be non-empty")
_rejects("empty families.in_scope rejected", lambda d: d["families"].__setitem__("in_scope", []),
         "must be non-empty")
_rejects("empty families.keywords rejected", lambda d: d["families"].__setitem__("keywords", {}),
         "must be non-empty")
_rejects("empty location.non_us_patterns rejected", lambda d: d["location"].__setitem__("non_us_patterns", []),
         "must be non-empty")
_rejects("empty targets.employer_patterns rejected", lambda d: d["targets"].__setitem__("employer_patterns", []),
         "must be non-empty")

# -- families.function_weight keys must exactly match families.keywords keys --
_rejects("function_weight missing a keyword family rejected",
         lambda d: d["families"]["function_weight"].pop("tam"), "missing weight")
_rejects("function_weight extra unknown family rejected",
         lambda d: d["families"]["function_weight"].__setitem__("ghost_family", 1), "unknown family key")

# -- hash stability --
h1 = _prof_mod.profile_content_hash(_PROFILE_DOC)
h2 = _prof_mod.profile_content_hash(_json.loads(_json.dumps(_PROFILE_DOC)))
check("same doc -> same content hash", h1 == h2)
reordered = dict(reversed(list(_PROFILE_DOC.items())))
check("key-order independent hash", _prof_mod.profile_content_hash(reordered) == h1)
mutated_doc = _mutated(lambda d: d["comp"].__setitem__("band_low", 1))
check("changed doc -> different content hash", _prof_mod.profile_content_hash(mutated_doc) != h1)

row_ver = _prof_mod.build_profile_version_row(_PROFILE_DOC)
check("version row has all five columns", set(row_ver) == {"profile_version_id", "content_hash", "profile_json", "rubric_hash", "created_at"})
check("version row content_hash matches profile_content_hash", row_ver["content_hash"] == h1)
check("version row rubric_hash is None (scorer identity lives in score_versions.scorer_hash)", row_ver["rubric_hash"] is None)
row_ver2 = _prof_mod.build_profile_version_row(_PROFILE_DOC)
check("version row id/hash idempotent across calls", row_ver["profile_version_id"] == row_ver2["profile_version_id"]
      and row_ver["content_hash"] == row_ver2["content_hash"])

# -- feature-vector correctness (hand-computed) --
res = score_row_explained(row("Support Engineer"), "General duties. 5 years experience needed for this role.")
# Phase 3.3 widened the vector so it can be REPLAYED (rubric.reconstruct_tier):
# it now also carries `raw_score` and, when they fire, the terminal caps. So the
# two assertions below moved onto the CONTRIBUTIONS SUBSET -- the keys that are
# summed into the score -- instead of onto the whole dict. Same claim about the
# same row, stated against the part of the vector that is a sum.
_contribs = {k: v for k, v in res.features.items() if k in _prof_mod.SCORE_ROW_CONTRIBUTIONS}
check("score_row_explained: plain-family-only row contributions == {function_match: 3}", _contribs == {"function_match": 3}, res.features)
check("score_row_explained: contributions sum to raw_score", sum(_contribs.values()) == res.features["raw_score"], res.features)
check("score_row_explained: contributions sum to tier when no clamp or cap engaged", sum(_contribs.values()) == res.tier, (res.features, res.tier))
check("score_row_explained: vector reconstructs its own tier", _rubric_mod.reconstruct_tier(res.features) == res.tier, (res.features, res.tier))
check("score_row_explained: vector keys are inside the stored-feature contract",
      set(res.features) <= _prof_mod.REQUIRED_SCORE_ROW_FEATURES, sorted(set(res.features) - _prof_mod.REQUIRED_SCORE_ROW_FEATURES))
check("score_row_explained: carries profile_hash + rubric_hash", res.profile_hash == h1 and res.rubric_hash == RUBRIC_VERSION)

blocked_res = score_row_explained(row("Manager, Technical Support Engineer", "CoreWeave", "San Jose, CA"), GOOD_DESC)
check("score_row_explained: blocked row features == {blocker: code}", blocked_res.features == {"blocker": "people_management"}, blocked_res.features)
check("score_row_explained: blocked row tier is 0", blocked_res.tier == 0)

hire_r = row("IT Support Specialist II", company="Acme Co"); hire_r["salary_max"] = "80000"; hire_r["flags"] = ""
hres = hireability_explained(hire_r, "Support Windows endpoints with Intune and Entra ID, Active Directory, M365, ServiceNow tickets. 2+ years.")
check("hireability_explained: hand-computed features", hres.features == {"junior": 2, "comp_near_level": 1}, hres.features)
check("hireability_explained: features sum to score", sum(hres.features.values()) == hres.score, (hres.features, hres.score))
check("hireability_explained: score composes to match/competition label", hres.label == "Unscored / Standard" and hres.score >= 3, (hres.label, hres.score))

print()
print(f"{'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILURES: ' + ', '.join(FAILS)}")
sys.exit(1 if FAILS else 0)
