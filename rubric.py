#!/usr/bin/env python3
"""
Rubric engine — description-aware scoring per RUBRIC.md.
Usage:
  python3 rubric.py fetch --group ats|workday|amazon|indeed|builtin1|builtin2|microsoft
  python3 rubric.py score
Descriptions cache: results/descriptions.jsonl  {url, desc}
Output: results/jobs_scored_<date>.csv with tier, why, flags
"""
import argparse, csv, datetime, glob, hashlib, html as htmlmod, json, os, re, sys, time
from dataclasses import dataclass as _dataclass
import requests
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import parse_salary, canon_company, norm, dedupe, VERIFY
import candidate_profile

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
JH = {**UA, "Accept": "application/json"}
DESC = os.path.join(HERE, "results", "descriptions.jsonl")

# Code-version identifier for the scoring ALGORITHM (independent of
# profile.json's content hash, which versions the candidate-preference DATA
# rubric applies). Bump this string whenever score_row's or hireability's
# logic changes shape: a new dimension, a new blocker rule, a changed
# clamp/cap, a changed feature-vector key set. Do NOT bump it for a
# profile.json data edit (new skill regex, adjusted comp band, added target
# employer) -- that produces a new profile_version_id, not a new rubric_hash.
# A score_versions row keys on (posting_version_id, profile_version_id,
# score_hash); rubric_hash is what lets a stored score be traced back to the
# exact scoring code that produced it even when the profile and the posting
# are both unchanged. Mirrors CANONICAL_HASH_FIELDS' bump discipline in
# webapp/backend/sources/contract.py.
#
# v2 (Phase 3.3): the feature vector became replayable (function_match always
# emitted, staff_cap_delta, raw_score, the four terminal caps), the scorer
# became hermetic (config.json's profile.bay_area / profile.title_exclude moved
# into profile.json), and the undated-aggregator cap took an explicit
# `is_aggregator` input instead of sniffing legacy CSV source strings.
# v3 (Phase 3.5): hireability's label stopped being a score-vs-threshold
# comparison against profile.json's (removed) hireability_labels and became a
# deterministic function of the SAME feature vector the score is summed from --
# match_label (fit quality: level gap, skills overlap) and competition_label
# (applicant-pool pressure), combined into `label` as "<match> / <competition>".
# s, the feature vector, and score_row()/tier are byte-identical to v2; only how
# a label is READ OFF an unchanged vector changed. Honest match/competition
# vocabulary replaces the probabilistic-sounding Likely/Target/Reach until real
# outcomes exist to calibrate a prediction against.
RUBRIC_VERSION = "rubric-2026.08-v3"

_RPROFILE_CACHE = None
def _rprofile():
    """Memoized validated candidate profile (profile.json). Raises
    profile.ProfileValidationError loudly on first use if the file is
    malformed -- never silently falls back to defaults."""
    global _RPROFILE_CACHE
    if _RPROFILE_CACHE is None:
        _RPROFILE_CACHE = candidate_profile.load_profile()
    return _RPROFILE_CACHE

_CFG_CACHE = None
def load_cfg():
    global _CFG_CACHE
    if _CFG_CACHE is None:
        with open(os.path.join(HERE, "config.json")) as f:
            _CFG_CACHE = json.load(f)
    return _CFG_CACHE

def load_candidates():
    """Single scoring path: read the raw harvest directly (no legacy pre-filter),
    dedupe with canonical company + field-preserving merge."""
    rawp = os.path.join(HERE, "results", "raw.jsonl")
    rows, seen = [], set()
    for l in open(rawp):
        if not l.strip(): continue
        r = json.loads(l)
        if not r.get("title") or not r.get("url") or r["url"] in seen: continue
        seen.add(r["url"])
        r.setdefault("salary_min", ""); r.setdefault("salary_max", "")
        r.setdefault("also_seen_on", ""); r["remote"] = str(r.get("remote", False))
        rows.append(r)
    return dedupe(rows)

def _clean_text(t):
    """Entity/nbsp cleanup for cached descriptions (older cache entries kept raw
    &nbsp;/&amp; from double-escaped ATS payloads)."""
    t = htmlmod.unescape(t or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()

def load_desc():
    d = {}
    if os.path.exists(DESC):
        with open(DESC) as f:
            for l in f:
                if l.strip():
                    j = json.loads(l); d[j["url"]] = _clean_text(j["desc"])
    return d

def save_desc(new):
    have = load_desc()
    fresh = {u: t for u, t in new.items() if u not in have and t}
    with open(DESC, "a") as f:
        for u, t in fresh.items():
            f.write(json.dumps({"url": u, "desc": t[:6000]}) + "\n")
    print(f"cached {len(fresh)} new descriptions (total {len(have)+len(fresh)})")

def strip_html(t):
    # unescape twice: Greenhouse-style payloads arrive double-escaped, leaving
    # literal &nbsp;/&amp; after a single pass
    t = htmlmod.unescape(htmlmod.unescape(t or ""))
    t = re.sub(r"<[^>]+>", " ", t).replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()

# ---------------- description fetchers ----------------
def fetch_ats(cands, cfg):
    out = {}
    urls_needed = {r["url"] for r in cands}
    inv_g = cfg["companies"]["greenhouse"]
    for slug in inv_g:
        try:
            r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true", headers=UA, timeout=15, verify=VERIFY)
            for j in r.json().get("jobs", []):
                if j.get("absolute_url") in urls_needed:
                    out[j["absolute_url"]] = strip_html(j.get("content", ""))
        except Exception as e: print("gh", slug, str(e)[:50], file=sys.stderr)
    for slug in cfg["companies"]["lever"]:
        try:
            r = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json", headers=UA, timeout=15, verify=VERIFY)
            for j in r.json():
                if j.get("hostedUrl") in urls_needed:
                    out[j["hostedUrl"]] = strip_html(j.get("descriptionPlain", "") or j.get("description", ""))
        except Exception as e: print("lv", slug, str(e)[:50], file=sys.stderr)
    for slug in cfg["companies"]["ashby"]:
        try:
            r = requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true", headers=UA, timeout=15, verify=VERIFY)
            for j in r.json().get("jobs", []):
                if j.get("jobUrl") in urls_needed:
                    out[j["jobUrl"]] = strip_html(j.get("descriptionHtml", "") or j.get("descriptionPlain", ""))
        except Exception as e: print("as", slug, str(e)[:50], file=sys.stderr)
    for slug in cfg["companies"].get("smartrecruiters", {}):
        for r_ in cands:
            if r_["source"] == "smartrecruiters" and f"/{slug}/" in r_["url"]:
                try:
                    d = requests.get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{r_['req_id']}", headers=UA, timeout=12, verify=VERIFY).json()
                    secs = (d.get("jobAd") or {}).get("sections") or {}
                    out[r_["url"]] = strip_html(" ".join(s.get("text", "") for s in secs.values() if isinstance(s, dict)))
                except Exception: pass
    for slug in cfg["companies"].get("workable", {}):
        try:
            r = requests.get(f"https://apply.workable.com/api/v1/widget/accounts/{slug}?details=true", headers=UA, timeout=15, verify=VERIFY)
            for j in r.json().get("jobs", []):
                u = j.get("shortlink") or j.get("url")
                if u in urls_needed:
                    out[u] = strip_html(j.get("description", ""))
        except Exception: pass
    for slug in cfg["companies"].get("recruitee", {}):
        try:
            r = requests.get(f"https://{slug}.recruitee.com/api/offers", headers=UA, timeout=15, verify=VERIFY)
            for j in r.json().get("offers", []):
                if j.get("careers_url") in urls_needed:
                    out[j["careers_url"]] = strip_html(j.get("description", ""))
        except Exception: pass
    return out

def fetch_workday(cands, cfg):
    out = {}
    hosts = {e["host"]: e for e in cfg["companies"]["workday"].values()}
    rows = [r for r in cands if r["source"] == "workday"]
    for r_ in rows:
        m = re.match(r"https://([^/]+)/en-US/([^/]+)(/job/.+)$", r_["url"])
        if not m: continue
        host, site, path = m.groups()
        e = hosts.get(host)
        if not e: continue
        try:
            d = requests.get(f"https://{host}/wday/cxs/{e['tenant']}/{site}{path}", headers=JH, timeout=12, verify=VERIFY).json()
            out[r_["url"]] = strip_html((d.get("jobPostingInfo") or {}).get("jobDescription", ""))
        except Exception: pass
        time.sleep(0.15)
    return out

def fetch_amazon(cands, cfg):
    out = {}
    urls_needed = {r["url"] for r in cands if r["source"] == "amazon-jobs"}
    for term in cfg["profile"]["search_terms"]:
        for offset in range(0, 500, 100):
            try:
                r = requests.get("https://www.amazon.jobs/en/search.json",
                                 params={"base_query": term, "loc_query": "Seattle, WA", "result_limit": 100,
                                         "offset": offset, "radius": "40km"}, headers=UA, timeout=15, verify=VERIFY)
                jobs = r.json().get("jobs", [])
                if not jobs: break
                for j in jobs:
                    u = "https://www.amazon.jobs" + (j.get("job_path") or "")
                    if u in urls_needed:
                        out[u] = strip_html(" ".join([j.get("description_short") or j.get("description") or "",
                                                      "BASIC QUALS: " + (j.get("basic_qualifications") or ""),
                                                      "PREFERRED: " + (j.get("preferred_qualifications") or "")]))
            except Exception: break
    return out

def fetch_indeed(cands, cfg):
    """Harvest Indeed descriptions captured at scrape time (src_jobspy stashes them in
    the raw row's `_desc`). No re-scrape: re-running the search returned a drifting result
    set, so many rows never matched and stayed description-less. Falls back silently for
    rows scraped before `_desc` existed (their descriptions are cached the normal way)."""
    out = {}
    for r in cands:
        if r["source"] == "jobspy-indeed" and r.get("_desc"):
            out[r["url"]] = re.sub(r"\s+", " ", r["_desc"])[:6000]
    return out

def fetch_builtin(cands, quarter):
    out = {}
    have = load_desc()
    rows = [r for r in cands if r["source"] == "builtin" and r["url"] not in have and _relevant(r["title"])]
    import concurrent.futures
    rows = rows[:55] if quarter else rows  # always take the next 55 uncached
    def one(r_):
        try:
            resp = requests.get(r_["url"], headers=UA, timeout=10, verify=VERIFY)
            if resp.status_code == 429:
                time.sleep(1.2); resp = requests.get(r_["url"], headers=UA, timeout=10, verify=VERIFY)
            if resp.status_code != 200: return None
            for block in re.findall(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', resp.text, re.S):
                try: d = json.loads(block)
                except Exception: continue
                if d.get("@type") == "JobPosting":
                    return (r_["url"], strip_html(d.get("description", "")))
        except Exception: return None
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        for res in ex.map(one, rows):
            if res: out[res[0]] = res[1]
    return out

def fetch_microsoft(cands):
    out = {}
    rows = [r for r in cands if r["source"] == "microsoft-careers"]
    for r_ in rows:
        pid = r_["url"].rstrip("/").split("/")[-1].split("?")[0]
        try:
            d = requests.get(f"https://apply.careers.microsoft.com/api/apply/v2/jobs/{pid}?domain=microsoft.com",
                             headers=JH, timeout=12, verify=VERIFY).json()
            jd = d.get("job_description") or ""
            if jd: out[r_["url"]] = strip_html(jd)[:6000]
        except Exception: pass
        time.sleep(0.2)
    return out

def _relevant(title):
    """Cheap family prefilter so per-row desc fetchers only pull plausible rows."""
    t = (title or "").lower()
    return any(k in t for fam in _rprofile().families.keywords.values() for k in fam)

def _ldjson_desc(url):
    try:
        r = requests.get(url, headers=UA, timeout=12, verify=VERIFY)
        if r.status_code != 200: return ""
        for block in re.findall(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', r.text, re.S):
            try: d = json.loads(block)
            except Exception: continue
            if isinstance(d, dict) and d.get("@type") == "JobPosting":
                return strip_html(d.get("description", ""))
    except Exception: pass
    return ""

def fetch_icims_desc(cands):
    """iCIMS job pages need in_iframe=1; description lives in the JobContent div."""
    out = {}
    for r_ in [r for r in cands if r["source"] == "icims" and _relevant(r["title"])]:
        try:
            rr = requests.get(r_["url"] + "?in_iframe=1", headers=UA, timeout=12, verify=VERIFY)
            if rr.status_code == 200:
                t = rr.text
                a = t.find("iCIMS_JobContent")
                b = t.find("iCIMS_Footer")
                seg = t[a:b] if a > -1 else t
                d = strip_html(seg)
                if len(d) > 200: out[r_["url"]] = d[:6000]
        except Exception: pass
        time.sleep(0.25)
    return out

def fetch_eightfold_desc(cands):
    out = {}
    for r_ in [r for r in cands if r["source"] == "eightfold" and _relevant(r["title"])]:
        m = re.match(r"(https://[^/]+)/careers/job/(\d+)\?domain=([^&]+)", r_["url"])
        if not m: continue
        base, pid, dom = m.groups()
        try:
            d = requests.get(f"{base}/api/apply/v2/jobs/{pid}?domain={dom}", headers=JH, timeout=12, verify=VERIFY).json()
            jd = d.get("job_description") or ""
            if jd: out[r_["url"]] = strip_html(jd)[:6000]
        except Exception: pass
        time.sleep(0.2)
    return out

def fetch_phenom_desc(cands):
    out = {}
    for r_ in [r for r in cands if r["source"] == "phenom" and _relevant(r["title"])]:
        d = _ldjson_desc(r_["url"])
        if d: out[r_["url"]] = d
        time.sleep(0.2)
    return out

def fetch_jibe_desc(cands, cfg):
    """Jibe portals return full descriptions inline on the list API — one paged
    enumeration per portal covers every candidate row (matched by company+req_id)."""
    want = {(r["company"], r["req_id"]): r["url"] for r in cands
            if r["source"] == "jibe" and _relevant(r["title"])}
    out = {}
    if not want: return out
    for _, e in cfg["companies"].get("jibe", {}).items():
        for page in range(1, 16):
            try:
                r = requests.get(f"{e['base']}/api/jobs",
                                 params={"limit": 100, "page": page, "lang": "en-us"},
                                 headers=UA, timeout=20, verify=VERIFY)
                if r.status_code != 200: break
                jobs = r.json().get("jobs", [])
                if not jobs: break
                for wrap in jobs:
                    d = wrap.get("data", wrap)
                    key = (e["name"], str(d.get("req_id") or d.get("slug") or ""))
                    if key in want and d.get("description"):
                        out[want[key]] = strip_html(d["description"])
                time.sleep(0.2)
            except Exception:
                break
    return out

def fetch_yc_desc(cands):
    """YC job pages (and the ATS pages they link out to) usually embed ld+json."""
    out = {}
    for r_ in [r for r in cands if r["source"] == "yc-jobs" and _relevant(r["title"])]:
        d = _ldjson_desc(r_["url"])
        if d: out[r_["url"]] = d
        time.sleep(0.2)
    return out

def fetch_costco_desc(cands):
    out = {}
    for r_ in [r for r in cands if r["source"] == "costco" and _relevant(r["title"])]:
        slug = r_["url"].rstrip("/").split("/")[-1].split("?")[0]
        got = ""
        for probe in (f"https://careers.costco.com/api/jobs/{slug}?lang=en-us",
                      f"https://careers.costco.com/api/job?slug={slug}&lang=en-us"):
            try:
                rr = requests.get(probe, headers=JH, timeout=12, verify=VERIFY)
                if rr.status_code == 200 and "json" in rr.headers.get("content-type", ""):
                    dd = rr.json()
                    node = dd.get("job", dd)
                    node = node.get("data", node) if isinstance(node, dict) else {}
                    got = strip_html(node.get("description", "") or node.get("job_description", ""))
                    if got: break
            except Exception: continue
        if not got: got = _ldjson_desc(r_["url"])
        if got: out[r_["url"]] = got
        time.sleep(0.2)
    return out

# ---------------- hireability (odds axis, orthogonal to fit tier) ----------------
# His resume vocabulary, high-competition employer list, staffing agencies, domain
# keywords, target employers, location gates, level calibration, comp bands, and the
# point value of every rule below live in profile.json (repo root), not here -- see
# candidate_profile.py for the validated/compiled loader. rubric.py only applies them by name.
#
# ScoreResult / OddsResult are the "explained" companions to score_row()/hireability():
# same computation, plus the feature vector that fired (named rule -> point contribution,
# or {"blocker": <code>} when the row was excluded) and the two hashes needed to make a
# stored score reproducible -- which profile.json produced it (profile_hash) and which
# version of this scoring code produced it (rubric_hash, see RUBRIC_VERSION above).
@_dataclass(frozen=True)
class ScoreResult:
    tier: int
    why: str
    flags: list
    features: dict
    profile_hash: str
    rubric_hash: str

@_dataclass(frozen=True)
class OddsResult:
    label: str
    match_label: str
    competition_label: str
    score: int
    why: str
    features: dict
    profile_hash: str
    rubric_hash: str

def _hireability_core(r, desc):
    """Shared implementation behind hireability() and hireability_explained().
    Returns an OddsResult; hireability() unpacks it to the legacy 3-tuple so
    every existing caller (tests.py, sweeprunner.py's cmd_score) is untouched."""
    prof = _rprofile()
    W = prof.weights.hireability
    t = (r["title"] or "").lower(); company = (r["company"] or "").lower()
    d = (desc or "").lower().replace("\\", "")
    flags = r.get("flags", "") if isinstance(r.get("flags"), str) else ", ".join(r.get("flags") or [])
    s, why, features = 0, [], {}
    # level gap vs his ~4yrs IC-I/II profile
    if prof.level.hireability_staff_principal_pattern.search(t):
        s += W["staff_principal"]; features["staff_principal"] = W["staff_principal"]
        why.append("staff/principal level")
    elif prof.level.hireability_senior_pattern.search(t):
        s += W["senior"]; features["senior"] = W["senior"]
        why.append("senior level")
    elif prof.level.hireability_junior_pattern.search(t):
        s += W["junior"]; features["junior"] = W["junior"]
        why.append("junior/associate level")
    # years required
    yreq, _ = years_required(desc)
    if yreq and yreq <= prof.experience.hireability_bonus_years_max:
        s += W["years_low"]; features["years_low"] = W["years_low"]; why.append(f"asks {yreq}yrs")
    elif yreq and yreq >= prof.experience.hireability_penalty_years_min:
        s += W["years_high"]; features["years_high"] = W["years_high"]; why.append(f"asks {yreq}yrs")
    # applicant-pool size proxy
    if any(p.search(company) for p in prof.competition.high_competition_patterns):
        s += W["high_competition"]; features["high_competition"] = W["high_competition"]
        why.append("high-competition employer")  # word-boundary: 'meta' must not match 'Metabase'
    # employment type: staffing/vendor W2 is a lower hiring bar (and a real vendor-to-FTE path)
    if "Staffing/W2" in flags:
        s += W["staffing_w2"]; features["staffing_w2"] = W["staffing_w2"]
        why.append("staffing/vendor (lower bar)")
    # resume/JD keyword overlap — only meaningful on a substantial JD; a short summary
    # snippet lacking keywords is not evidence of a poor match, so don't penalize it.
    if len(d) > 400:
        hits = sum(1 for p in prof.skills.his_skills if p.search(d))
        if hits >= 6:
            s += W["skills_strong"]; features["skills_strong"] = W["skills_strong"]; why.append(f"{hits} skills match")
        elif hits >= 3:
            s += W["skills_moderate"]; features["skills_moderate"] = W["skills_moderate"]; why.append(f"{hits} skills match")
        elif hits <= 1:
            s += W["skills_thin"]; features["skills_thin"] = W["skills_thin"]; why.append("thin skills match")
        # exact daily-stack bonus (his literal admin tools)
        if all(p.search(d) for p in prof.skills.exact_stack_patterns):
            s += W["exact_stack"]; features["exact_stack"] = W["exact_stack"]; why.append("his exact stack")
    elif d:
        why.append("short JD (skills unscored)")
    # comp expectation vs a below-market current comp and the target band
    try: hi = int(r.get("salary_max") or 0)
    except (TypeError, ValueError): hi = 0
    if hi >= prof.comp.hireability_high_bar:
        s += W["comp_high_bar"]; features["comp_high_bar"] = W["comp_high_bar"]; why.append("comp implies higher bar")
    elif 0 < hi < prof.comp.hireability_near_level:
        s += W["comp_near_level"]; features["comp_near_level"] = W["comp_near_level"]; why.append("comp near his level")
    if "degree-gated" in flags:
        s += W["degree_gated"]; features["degree_gated"] = W["degree_gated"]; why.append("hard degree gate")
    # Two honest labels, read off the SAME feature vector `s` was summed from --
    # no new inputs, no score-vs-threshold comparison. This replaced a "Likely /
    # Target / Reach" prediction the tool had no outcome data to calibrate
    # (roadmap: "replace user-facing probabilistic labels with honest
    # match/competition labels until outcomes support calibration").
    #
    # match_label: fit quality (level gap, skills overlap), most-decisive rule
    # first. "Unscored" is not a 5th quality tier -- it is the honest admission
    # that the len(d) > 400 gate above never ran, so no skills verdict exists to
    # report (rather than reporting "Moderate match" on missing evidence).
    if "staff_principal" in features:
        match_label = "Level stretch"
    elif "skills_strong" in features or "exact_stack" in features:
        match_label = "Strong match"
    elif "skills_thin" in features:
        match_label = "Weak match"
    elif len(d) <= 400:
        match_label = "Unscored"
    else:
        match_label = "Moderate match"
    # competition_label: applicant-pool pressure, independent of fit quality.
    if "high_competition" in features or "comp_high_bar" in features or "degree_gated" in features:
        competition_label = "High competition"
    elif "staffing_w2" in features:
        competition_label = "Lower bar"
    else:
        competition_label = "Standard"
    # Combined string is the one legacy surfaces keep reading unchanged in shape
    # (CSV `odds` column, jobs.odds, job_history.odds, score_versions.odds).
    label = f"{match_label} / {competition_label}"
    return OddsResult(label=label, match_label=match_label, competition_label=competition_label,
                       score=s, why="; ".join(why[:4]), features=features,
                       profile_hash=prof.content_hash, rubric_hash=RUBRIC_VERSION)

def hireability(r, desc):
    """Odds of actually landing it, scored ORTHOGONALLY to the fit tier. Returns
    (label, score, reasons). Positive = more winnable. This is a heuristic proxy, not a
    prediction: it models applicant-pool size, level gap, resume/JD keyword overlap,
    employment-type bar, and comp expectation — none of which the fit rubric touches."""
    res = _hireability_core(r, desc)
    return res.label, res.score, res.why

def hireability_explained(r, desc):
    """Same computation as hireability(), plus the feature vector that fired and
    the profile/rubric hashes -- what Phase 3.3's persistence layer stores
    alongside a score."""
    return _hireability_core(r, desc)

# ---------------- scoring ----------------
# THE SCORER IS HERMETIC. Its only inputs are the record, the description, and
# profile.json (via `_rprofile()`). It reads config.json nowhere.
#
# It used to. `_bay()` and `_title_excl()` pulled `profile.bay_area` and
# `profile.title_exclude` out of config.json, and nothing hashed config.json into
# `profile_version_id` -- so editing either list silently changed the location
# gate and a whole class of title blockers for every posting while leaving the
# profile version a stored score is filed under byte-identical. The corpus was
# never re-scored and no audit could see the change. Both lists now live in
# profile.json as `location.bay_area_cities` and `exclusions.title_exclude`
# (schema_version 2), where the content hash covers them. config.json itself is
# untouched -- other consumers may still read those keys; the scorer does not.
#
# `_metro()`/`seattle_metro` went with them: the 2026-07-18 "bay area and remote
# only" directive left that path unreachable, and unreachable code that reads
# configuration reads as a live rule.

def posting_age_days(posted):
    """Days since posting, from ISO dates or 'Posted 30+ Days Ago' strings. None if unknown."""
    p = (posted or "").strip().lower()
    if not p: return None
    if "today" in p or "yesterday" in p or "just posted" in p: return 0
    m = re.search(r"(\d+)\+?\s*days? ago", p)
    if m: return int(m.group(1))
    m = re.search(r"\d{4}-\d{2}-\d{2}", p)
    if m:
        try: return (datetime.date.today() - datetime.date.fromisoformat(m.group(0))).days
        except ValueError: return None
    return None

def years_required(d):
    """Context-aware: only counts years stated as an experience requirement, in-sentence.
    Returns (required_years, preferred_years). Ignores company-age phrases."""
    req, pref = [], []
    for sent in re.split(r"[.;\n•]", (d or "").lower()):
        if re.search(r"compan(y|ies)|business|history|founded|celebrat|industry leader|in business", sent):
            continue
        if not re.search(r"experience|background|track record|working (in|with)|required|prefer|minimum|at least", sent):
            continue
        for m in re.finditer(r"(\d{1,2})\s*\+?\s*(?:or more\s+)?years?", sent):
            y = int(m.group(1))
            if y > 20: continue
            if re.search(r"prefer|nice to have|ideal(ly)?|a plus|bonus", sent):
                pref.append(y)
            else:
                req.append(y)
    return (max(req) if req else None), (max(pref) if pref else None)

def salary_from_desc(d):
    """Extract WA-mandated pay bands from description text. Windows around currency
    markers, 401(k) stripped, hourly annualized by parse_salary."""
    d2 = re.sub(r"401\s*\(?k\)?", " ", d or "", flags=re.I)
    for m in re.finditer(r"usd|\$", d2, re.I):
        win = d2[m.start(): m.start() + 90]
        lo, hi = parse_salary(win)
        if lo and hi and hi >= lo and lo >= 30000:
            return lo, hi
    return None, None

def _score_row_core(r, desc, *, is_aggregator=None):
    """Shared implementation behind score_row() and score_row_explained().
    Returns (tier, why, flags, features) -- the 3-tuple callers already know,
    plus a features dict of {named_rule: point_contribution} for every rule
    that fired, or {"blocker": <code>} for the single rule that excluded the
    row. Every literal that used to live in a module-level constant here
    (skills/domain/location/family/exclusion word lists, level and comp
    thresholds, and the point value of every rule) now comes from
    profile.json via `prof` -- rubric.py only supplies the shape of the
    algorithm. The final `max(1, min(5, score))` clamp is the one exception:
    the 1-5 output scale is the rubric's structural contract (consumers like
    the tracker key off it), not a candidate preference, so it stays literal.

    THE FEATURE VECTOR IS REPLAYABLE (Phase 3.3). `reconstruct_tier(features)`
    re-derives this function's `tier` from the vector alone, and
    `sum(contributions) == features["raw_score"]` holds for every scored row.
    That is what makes a stored score auditable rather than merely annotated: a
    tier that cannot be re-derived from what was recorded is a number with a
    story attached, not evidence. Three additions bought it:

      * `function_match` is emitted ALWAYS, not only when the family is the
        top-weighted one. It is the score's STARTING VALUE, so a vector that
        omitted it on a lower-weighted family could not sum to the total.
      * `staff_cap_delta` records the negative delta of the mid-computation
        `min(score, staff_cap_tier)` clamp. It moves the running total, so it
        replays as a contribution rather than as a terminal cap.
      * `raw_score` records the total BEFORE `max(1, min(5, ...))`, and each of
        the four terminal caps records THE TIER ITS RULE FORCED, at exactly the
        point its `flags` entry is appended.

    Emitting features must never change the tier: `score_row()` throws the
    vector away and must return what it always returned.

    `is_aggregator` is an EXPLICIT INPUT, and that is a correctness fix, not
    ergonomics. The undated-ghost-listing cap used to gate on
    `r["source"].startswith(AGG_SOURCES)`, where AGG_SOURCES is a tuple of legacy
    CSV source strings ("jobspy-", "mcp-", "builtin", "yc-jobs"). Canonical
    `posting_versions.source` carries the NormalizedPosting NAMESPACE instead
    ("jobspy:indeed", "yc", "manual:dice"), which matches none of them -- so for
    every canonically-scored row the cap silently never fired and undated
    aggregator postings were free to reach tier 4/5. The caller now states the
    fact it knows (Phase 3.3 reads it from the source registry's
    `SourceCategory`); passing None keeps the legacy string sniff for the CSV
    path, which is the only caller that still has legacy source strings.
    """
    prof = _rprofile()
    W = prof.weights.score_row
    t = (r["title"] or "").lower()
    d = (desc or "").lower().replace("\\", "")
    company = (r["company"] or "").lower()
    loc = (r["location"] or "").lower()
    why, flags, features = [], [], {}
    if is_aggregator is None:
        is_aggregator = (r.get("source") or "").startswith(AGG_SOURCES)

    def blocked(reason, flags_, code):
        # Asserted AT EMISSION, not in a test: the blocker code is a stored,
        # queryable namespace (Phase 4 groups run reports by it), and a typo
        # would quietly create a category nothing has ever heard of.
        assert code in candidate_profile.BLOCKER_CODES, f"unknown blocker code {code!r}"
        return 0, reason, flags_, {"blocker": code}

    # location gate — Bay Area or US-remote ONLY (2026-07-18 directive: "bay area and remote only")
    if any(p.search(loc) for p in prof.location.non_us_patterns):
        return blocked("non-US location", ["blocker"], "non_us_location")
    # A Bay Area city name counts as local only when a CA marker is also present. This both
    # rejects cross-state collisions ("Fremont, NE") AND keeps multi-location postings that
    # list SF alongside other cities ("San Francisco, CA | New York City, NY | Seattle, WA").
    has_bay_city = any(p.search(loc) for p in prof.location.bay_area_cities)
    ca_marker = bool(re.search(r",\s*ca\b|\bcalif", loc)) or "bay area" in loc
    bay_area = has_bay_city and ca_marker
    is_remote = str(r.get("remote", "")).strip().lower() == "true" \
        or bool(re.search(r"\bremote\b|work from home|\bwfh\b|\banywhere\b|\bnationwide\b|\bvirtual\b", loc))
    if not (bay_area or is_remote):
        return blocked("not Bay Area or US-remote", ["off-target-loc"], "off_target_location")
    # family / function
    fam = next((f for f, kws in prof.families.keywords.items() if any(k in t for k in kws)), None)
    if not fam:
        fam = next((f for f, kws in prof.families.keywords.items() if any(k in d[:1500] for k in kws)), None)
    func = prof.families.function_weight.get(fam, 1)
    top_func = max(prof.families.function_weight.values())
    # blockers — clearance judged per sentence so "able to obtain" elsewhere can't mask a hold-requirement
    for sent in re.split(r"[.;\n]", d):
        if prof.exclusions.clearance_required_pattern.search(sent):
            if prof.exclusions.clearance_condition_pattern.search(sent) \
               and not prof.exclusions.clearance_exception_pattern.search(sent):
                return blocked("requires active clearance", ["blocker"], "active_clearance")
    # title excludes are word-boundary matches from profile.json
    # exclusions.title_exclude ("intern" no longer swallows "Internal"/"International")
    if any(k in d for k in prof.exclusions.disqualifying_skills) \
       or any(p.search(t) for p in prof.exclusions.title_exclude):
        return blocked("specialist/level blocker", ["blocker"], "specialist_skill_or_title")
    # people-management: any "Manager"/"Supervisor" in the title once IC manager
    # phrases (Program/Project/Product/Account/... Manager) are removed
    if prof.exclusions.people_management_pattern.search(prof.exclusions.ic_manager_pattern.sub(" ", t)):
        return blocked("people-management role", ["blocker"], "people_management")
    yrs_req, yrs_pref = years_required(d)
    if yrs_req and yrs_req >= prof.experience.blocker_years_min:
        return blocked(f"requires {yrs_req}+ years", ["blocker"], "years_required_too_high")
    score = func
    # Emitted unconditionally: `func` IS the score's starting value, so a vector
    # that recorded it only for the top-weighted family could not sum to the
    # total for any other family. The `why` line stays conditional -- it is the
    # human-facing claim "this is a core function match", which a weight of 1 is
    # not.
    features["function_match"] = func
    if func == top_func:
        why.append("core function match")
    # level
    senior = bool(prof.level.score_senior_pattern.search(t))
    if fam == "tpm" and senior:
        score += W["tpm_senior"]; flags.append("too-senior"); features["tpm_senior"] = W["tpm_senior"]
    elif fam == "tpm" and prof.level.tpm_ii_pattern.search(t):
        score += W["tpm_ii_stretch"]; flags.append("tpm-ii-stretch"); features["tpm_ii_stretch"] = W["tpm_ii_stretch"]
    elif fam == "validation" and senior and any(k in t for k in prof.level.validation_design_keywords):
        return blocked("senior design/verification role", ["blocker"], "senior_design_verification")
    elif senior and fam in ("validation", "techops"):
        score += W["stretch_level"]; flags.append("stretch-level"); features["stretch_level"] = W["stretch_level"]
    elif senior and fam == "support" and any(b in company for b in prof.level.support_bigtech_companies):
        score += W["stretch_level"]; flags.append("stretch-level"); features["stretch_level"] = W["stretch_level"]
    if yrs_req and prof.experience.score_penalty_years_low <= yrs_req <= prof.experience.score_penalty_years_high:
        score += W["years_penalty"]; flags.append(f"{yrs_req}yrs-required"); features["years_penalty"] = W["years_penalty"]
    elif yrs_req and yrs_req <= prof.experience.score_bonus_years_max:
        score += W["years_bonus"]; why.append(f"asks {yrs_req}yrs"); features["years_bonus"] = W["years_bonus"]
    if yrs_pref and yrs_pref >= prof.experience.score_pref_penalty_years_min:
        score += W["years_pref_penalty"]; flags.append(f"{yrs_pref}yrs-preferred"); features["years_pref_penalty"] = W["years_pref_penalty"]
    # domain (word-boundary regex; EEO boilerplate stripped)
    d_clean = re.sub(r"(equal (employment )?opportunity|eeo|gender identity|sexual orientation)[^.]*\.", " ", d)
    hits2 = [p for p in prof.domain.tier2_patterns if p.search(d_clean) or p.search(t)]
    hits1 = [p for p in prof.domain.tier1_patterns if p.search(d_clean) or p.search(t)]
    dom = W["domain_tier2"] if hits2 else (W["domain_tier1"] if hits1 else 0)
    if hits2:
        why.append("domain: " + ",".join(h.pattern.replace("\\b", "").replace("\\", "") for h in hits2[:3]))
        features["domain_tier2"] = W["domain_tier2"]
    elif hits1:
        why.append("domain: " + ",".join(h.pattern.replace("\\b", "").replace("\\", "") for h in hits1[:2]))
        features["domain_tier1"] = W["domain_tier1"]
    score += dom
    # No support/validation/techops/tpm/tam role family = it doesn't relate — exclude it from
    # the output entirely (a lone domain keyword like "azure" on a Data/Silicon Engineer role
    # is not a support job). Previously these were kept as tier-2 noise.
    if fam is None:
        return blocked("no role-family match", ["skip"], "no_role_family_match")
    if fam not in prof.families.in_scope:
        return blocked("off-focus role (tpm/solutions/account-mgmt)", ["skip"], "off_focus_role")
    if any(p.search(company) for p in prof.targets.employer_patterns) and fam:
        score += W["target_co"]; flags.append("target-co"); why.append("named-target employer")
        features["target_co"] = W["target_co"]
    why.append("Bay Area" if bay_area else "US-remote")
    if prof.level.staff_cap_pattern.search(t):
        # Principal/Staff: "Out (cap at 2)". Recorded as the DELTA it applied to
        # the running total (<= 0), not as a terminal cap: everything after this
        # line still adds to `score`, so the clamp is a contribution and has to
        # replay as one. Emitted even when it is 0 -- "the rule fired and cost
        # nothing" is a different fact from "the rule did not fire".
        capped = min(score, prof.tier_rules.staff_cap_tier)
        features["staff_cap_delta"] = capped - score
        score = capped
        flags.append("level-out")
    # logistics: posting age (>30d = -1 per RUBRIC dimension 6)
    age = posting_age_days(r.get("posted"))
    if age is not None and age > prof.tier_rules.stale_penalty_days:
        score += W["stale_30d"]; flags.append("30d+"); features["stale_30d"] = W["stale_30d"]
    # comp — posting field first, then WA pay-transparency extraction from description
    lo, hi = parse_salary(r.get("salary") or "")
    if not lo:
        try:
            lo = float(r.get("salary_min") or 0) or None
            hi = float(r.get("salary_max") or 0) or None
        except ValueError:
            lo = hi = None
    if not lo and desc:
        lo, hi = salary_from_desc(desc)
        if lo:
            r["salary"] = f"${lo:,}-${hi:,} (from description)"
            flags.append("salary-from-desc")
    if lo: r["salary_min"], r["salary_max"] = int(lo), int(hi or lo)
    if lo and hi and lo <= prof.comp.band_high and hi >= prof.comp.band_low:
        score += W["comp_in_band"]; why.append("comp in band"); features["comp_in_band"] = W["comp_in_band"]
    elif hi and hi < prof.comp.low_comp_threshold:
        score += W["low_comp"]; flags.append("low-comp"); features["low_comp"] = W["low_comp"]
    # employment type
    if any(k in d for k in prof.employers.c2c_keywords):
        score += W["c2c"]; flags.append("C2C"); features["c2c"] = W["c2c"]
    elif any(s in company for s in prof.employers.staffing_agencies):
        score += W["staffing_w2"]; flags.append("Staffing/W2"); features["staffing_w2"] = W["staffing_w2"]
    if not desc:
        flags.append("desc-unavailable")
    if prof.exclusions.degree_required_pattern.search(d) and prof.exclusions.degree_equivalent_exception not in d:
        score += W["degree_gated"]; flags.append("degree-gated"); features["degree_gated"] = W["degree_gated"]
    # The raw total, recorded before the structural clamp. Two rows that clamp to
    # the same tier from 7 and from 5 are not the same row, and a vector that kept
    # only the tier could re-derive neither.
    features["raw_score"] = score
    tier = max(1, min(5, score))  # structural: the rubric's 1-5 output scale, not a preference
    # RUBRIC tier mapping enforced: 5 requires Function 3 (fam is None already excluded above)
    #
    # Each cap below records THE TIER IT FORCED, at exactly the point its `flags`
    # entry is appended, and `reconstruct_tier` mins them back in the declared
    # order (candidate_profile.SCORE_ROW_CAP_FEATURES).
    if func < prof.tier_rules.func_cap_min_func and tier > prof.tier_rules.func_cap_tier:
        tier = prof.tier_rules.func_cap_tier
        features["cap_func"] = tier; flags.append("func-cap")
    if not desc and tier > prof.tier_rules.no_desc_cap_tier:
        tier = prof.tier_rules.no_desc_cap_tier  # rule zero: no 4/5 without a read description
        features["cap_no_desc"] = tier; flags.append("needs-desc")
    # ghost-listing guards: very old or undated aggregator postings can't be 4/5
    if age is not None and age > prof.tier_rules.stale_cap_days:
        tier = min(tier, prof.tier_rules.stale_cap_tier)
        features["cap_stale"] = prof.tier_rules.stale_cap_tier; flags.append("stale-90d+")
    elif age is None and is_aggregator:
        if tier > prof.tier_rules.undated_aggregator_cap_tier:
            tier = prof.tier_rules.undated_aggregator_cap_tier
            features["cap_undated_aggregator"] = tier; flags.append("undated-aggregator")
    return tier, "; ".join(why[:6]), flags, features


def reconstruct_tier(features):
    """Re-derive `_score_row_core`'s tier from a stored feature vector alone.

    PURE, and deliberately ignorant of profile.json: every number it needs was
    recorded by the scorer at the instant it decided. That is what makes it a
    replay rather than a re-run -- a profile edit changes future scores, not the
    reconstruction of a score already stored under an older profile version.

    The three steps mirror the scorer exactly: a blocked row is tier 0 and has no
    total; otherwise the raw total is clamped to the structural 1-5 scale and then
    each terminal cap that fired is applied, in `SCORE_ROW_CAP_FEATURES` order.
    `min` is used rather than assignment because a cap only ever lowers -- the
    scorer's `tier = cap` forms are already guarded by `tier > cap`.
    """
    if candidate_profile.SCORE_ROW_BLOCKER_FEATURE in features:
        return 0
    tier = max(1, min(5, features["raw_score"]))
    for cap in candidate_profile.SCORE_ROW_CAP_FEATURES:
        if cap in features:
            tier = min(tier, features[cap])
    return tier

def score_row(r, desc, *, is_aggregator=None):
    """Rubric scoring per RUBRIC.md. Mutates r's salary fields when a band is
    recovered from the description (WA pay-transparency extraction)."""
    tier, why, flags, _features = _score_row_core(r, desc, is_aggregator=is_aggregator)
    return tier, why, flags

def score_row_explained(r, desc, *, is_aggregator=None):
    """Same computation as score_row(), plus the feature vector that fired
    (or the blocker name) and the profile/rubric hashes -- what Phase 3.3's
    persistence layer stores alongside a score so a tier is reproducible and
    auditable after profile.json or RUBRIC_VERSION later change."""
    tier, why, flags, features = _score_row_core(r, desc, is_aggregator=is_aggregator)
    prof = _rprofile()
    return ScoreResult(tier=tier, why=why, flags=flags, features=features,
                        profile_hash=prof.content_hash, rubric_hash=RUBRIC_VERSION)


# ---------------- scorer identity: the RUBRIC_VERSION tripwire ----------------
# RUBRIC_VERSION is a hand-maintained string, and a hand-maintained string is
# exactly as reliable as the person editing it remembers to be. The failure it
# guards against is silent and expensive: change a clamp, forget the bump, and
# every stored score keeps claiming it was produced by a scorer that no longer
# exists -- while the "score exactly once" anti-join happily reuses rows the new
# code would not have produced.
#
# So the scorer's IDENTITY is derived from its SOURCE TEXT, over a pinned surface
# named below, and used two ways:
#
#   AUTOMATIC. Phase 3.3's `scorer_hash` mixes in the COMPUTED digest, so any
#     edit to the pinned surface invalidates every stored score whether or not
#     RUBRIC_VERSION moved. Correctness does not depend on anyone remembering.
#   DELIBERATE. `SCORER_SOURCE_DIGEST` below pins the digest as a constant, and a
#     test asserts the computed value still equals it. Editing the scorer
#     therefore FAILS THE SUITE until a human updates the pin -- and updating the
#     pin is the moment to decide whether RUBRIC_VERSION should move too.
#
# The surface is pinned by NAME rather than by module scan: a digest over the
# whole module would churn on a docstring in a description fetcher and cost a
# full corpus re-score for nothing.
_SCORER_SURFACE_NAMES = (
    "ScoreResult",
    "OddsResult",
    "_hireability_core",
    "hireability",
    "hireability_explained",
    "posting_age_days",
    "years_required",
    "salary_from_desc",
    "_score_row_core",
    "reconstruct_tier",
    "score_row",
    "score_row_explained",
)


def _scorer_surface():
    """(label, object) for every pinned member, in declared order.

    `scraper.parse_salary` is included because the scorer CALLS it to turn a
    salary string into a band, so a change there changes tiers exactly as a
    change here would. Reading its source is all this does -- scraper.py itself
    is not modified.
    """
    import scraper as _scraper_mod
    members = [(f"rubric.{name}", globals()[name]) for name in _SCORER_SURFACE_NAMES]
    members.append(("scraper.parse_salary", _scraper_mod.parse_salary))
    return members


def scorer_source_digest():
    """sha256 over the source text of the pinned scoring surface.

    Labels are hashed alongside the text so that renaming a function is a change
    even when its body is untouched, and so that two members with identical
    bodies cannot cancel out.
    """
    import inspect
    payload = [[label, inspect.getsource(obj)] for label, obj in _scorer_surface()]
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


#: The pinned value of `scorer_source_digest()`. Asserted by
#: `webapp/backend/tests/test_scoring_features.py`. When that assertion fails,
#: the scorer changed: update this constant, and decide in the same edit whether
#: RUBRIC_VERSION moves with it.
SCORER_SOURCE_DIGEST = "eb9073d937869ea2995f49dcf920d48373a53428ad1bf57f919dd9cb4aa94127"

def load_picks():
    """Human picks (picks.json) + LLM-confirmed picks (picks_llm.json, written by
    llm_review.py). Human entries win on URL collision."""
    picks, seen_urls = [], set()
    for fname in ("picks.json", "picks_llm.json"):
        p = os.path.join(HERE, fname)
        if os.path.exists(p):
            with open(p) as f:
                for e in json.load(f):
                    if e.get("url") and e["url"] in seen_urls: continue
                    if e.get("url"): seen_urls.add(e["url"])
                    picks.append(e)
    return picks

# ---------------- aggregator -> canonical source resolver ----------------
RES = os.path.join(HERE, "results", "resolutions.jsonl")
AGG_SOURCES = ("jobspy-", "mcp-", "builtin", "yc-jobs")

def _title_sim(a, b):
    ta, tb = set(norm(a).split()), set(norm(b).split())
    if not ta or not tb: return 0.0
    return len(ta & tb) / len(ta | tb)

def load_resolutions():
    res = {}
    if os.path.exists(RES):
        for l in open(RES):
            if l.strip():
                j = json.loads(l); res[j["agg_url"]] = j
    return res

def build_registry(cfg):
    reg = {}
    C = cfg["companies"]
    for section in ("greenhouse", "lever", "ashby", "smartrecruiters", "workable", "recruitee"):
        for slug, name in C.get(section, {}).items():
            reg[canon_company(name)] = (section, slug)
    for _, e in C.get("workday", {}).items(): reg[canon_company(e["name"])] = ("workday", e)
    for _, e in C.get("eightfold", {}).items(): reg[canon_company(e["name"])] = ("eightfold", e)
    for host, e in C.get("icims", {}).items(): reg[canon_company(e["name"])] = ("icims", host)
    for _, e in C.get("jibe", {}).items(): reg[canon_company(e["name"])] = ("jibe", e)
    reg["microsoft"] = ("microsoft", None)
    reg["amazon"] = ("amazon", None)
    reg["costco wholesale"] = ("costco", None); reg["costco"] = ("costco", None)
    return reg

_board_cache = {}

def _company_postings(ats, handle, title_hint):
    """Candidate postings [(title, url, desc, location)] from a company's direct ATS."""
    key = (ats, json.dumps(handle, default=str))
    full_list = ats in ("greenhouse", "lever", "ashby")
    if full_list and key in _board_cache:
        return _board_cache[key]
    rows = []
    try:
        if ats == "greenhouse":
            r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{handle}/jobs?content=true", headers=UA, timeout=15, verify=VERIFY)
            for j in r.json().get("jobs", []):
                rows.append((j.get("title", ""), j.get("absolute_url", ""), strip_html(j.get("content", "")), (j.get("location") or {}).get("name", "")))
        elif ats == "lever":
            for j in requests.get(f"https://api.lever.co/v0/postings/{handle}?mode=json", headers=UA, timeout=15, verify=VERIFY).json():
                rows.append((j.get("text", ""), j.get("hostedUrl", ""), strip_html(j.get("descriptionPlain", "")), (j.get("categories") or {}).get("location", "")))
        elif ats == "ashby":
            for j in requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{handle}", headers=UA, timeout=15, verify=VERIFY).json().get("jobs", []):
                rows.append((j.get("title", ""), j.get("jobUrl", ""), strip_html(j.get("descriptionHtml", "")), j.get("location", "")))
        elif ats == "workday":
            e = handle
            r = requests.post(f"https://{e['host']}/wday/cxs/{e['tenant']}/{e['site']}/jobs",
                              json={"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": title_hint},
                              headers=JH, timeout=15, verify=VERIFY)
            for j in r.json().get("jobPostings", []):
                rows.append((j.get("title", ""), f"https://{e['host']}/en-US/{e['site']}{j.get('externalPath', '')}", "", j.get("locationsText", "")))
        elif ats == "eightfold":
            e = handle
            r = requests.get(f"{e['base']}/api/pcsx/search", params={"domain": e["domain"], "query": title_hint, "num": 10}, headers=JH, timeout=15, verify=VERIFY)
            for p in r.json().get("data", {}).get("positions", []):
                locs = p.get("standardizedLocations") or []
                rows.append((p.get("name", ""), f"{e['base']}/careers/job/{p.get('id')}?domain={e['domain']}", "", "; ".join(locs[:2])))
        elif ats == "icims":
            r = requests.get(f"https://{handle}.icims.com/jobs/search", params={"ss": "1", "searchKeyword": title_hint}, headers=UA, timeout=15, verify=VERIFY)
            for m in re.finditer(r'<a[^>]+href="(https://[^"]+/jobs/\d+/[^"]+/job)[^"]*"[^>]*>(.*?)</a>', r.text, re.S):
                rows.append((re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group(2))).strip(), m.group(1), "", ""))
        elif ats == "microsoft":
            r = requests.get("https://apply.careers.microsoft.com/api/pcsx/search",
                             params={"domain": "microsoft.com", "query": title_hint, "location": "Washington, United States", "num": 10},
                             headers=JH, timeout=15, verify=VERIFY)
            for p in r.json().get("data", {}).get("positions", []):
                purl = p.get("positionUrl") or f"/careers/job/{p.get('id')}"
                rows.append((p.get("name", ""), f"https://apply.careers.microsoft.com{purl}", "", ""))
        elif ats == "amazon":
            r = requests.get("https://www.amazon.jobs/en/search.json",
                             params={"base_query": title_hint, "loc_query": "Seattle, WA", "result_limit": 20, "radius": "40km"},
                             headers=UA, timeout=15, verify=VERIFY)
            for j in r.json().get("jobs", []):
                rows.append((j.get("title", ""), "https://www.amazon.jobs" + (j.get("job_path") or ""), strip_html(j.get("description_short") or ""), f"{j.get('city', '')}, {j.get('state', '')}"))
        elif ats == "costco":
            r = requests.get("https://careers.costco.com/api/jobs", params={"keywords": title_hint, "limit": 20, "lang": "en-us"}, headers=UA, timeout=15, verify=VERIFY)
            for w in r.json().get("jobs", []):
                d = w.get("data", w)
                rows.append((d.get("title", ""), f"https://careers.costco.com/jobs/{d.get('slug', '')}", "", d.get("full_location", "")))
        elif ats == "jibe":
            r = requests.get(f"{handle['base']}/api/jobs", params={"keywords": title_hint, "limit": 20, "lang": "en-us"}, headers=UA, timeout=15, verify=VERIFY)
            for w in r.json().get("jobs", []):
                d = w.get("data", w)
                rows.append((d.get("title", ""), f"{handle['base']}/jobs/{d.get('slug', '')}",
                             strip_html(d.get("description") or ""), d.get("full_location", "")))
    except Exception:
        pass
    if full_list:
        _board_cache[key] = rows
    return rows

def cmd_resolve():
    """Rewrite tier>=4 aggregator rows to their canonical ATS posting: fresh link,
    full description, ghost-listing check. Cached in resolutions.jsonl."""
    cfg = load_cfg(); reg = build_registry(cfg)
    scored = sorted(glob.glob(os.path.join(HERE, "results", "jobs_scored_*.csv")))
    if not scored:
        print("run score first"); return
    rows = list(csv.DictReader(open(scored[-1])))
    res = load_resolutions()
    targets = [r for r in rows if (int(r["tier"]) >= 4 or "tier5-proposed" in r["flags"])
               and r["source"].startswith(AGG_SOURCES) and r["url"] not in res]
    print(f"resolver: {len(targets)} aggregator rows to attempt")
    new, desc_new = [], {}
    for r in targets:
        cc = canon_company(r["company"])
        hit = reg.get(cc) or next((v for k, v in reg.items() if k and len(k) > 3 and (k in cc or cc in k)), None)
        if not hit: continue
        ats, handle = hit
        best, bsim = None, 0.0
        for ct, cu, cd, cl in _company_postings(ats, handle, r["title"]):
            s = _title_sim(r["title"], ct)
            if s > bsim: best, bsim = (ct, cu, cd, cl), s
        if best and bsim >= 0.72:
            entry = {"agg_url": r["url"], "canonical_url": best[1], "ats": ats,
                     "matched_title": best[0], "sim": round(bsim, 2)}
            res[r["url"]] = entry; new.append(entry)
            if best[2]: desc_new[best[1]] = best[2]
            print(f"  + {r['company'][:20]} | {r['title'][:42]} -> {ats} sim={bsim:.2f}")
    with open(RES, "a") as f:
        for e in new: f.write(json.dumps(e) + "\n")
    if desc_new: save_desc(desc_new)
    # Upgrade pinned pick URLs to their canonical posting PER STORE — never merge the LLM
    # store back into picks.json, which would relabel LLM picks as human picks (provenance).
    changed = 0
    for fname in ("picks.json", "picks_llm.json"):
        path = os.path.join(HERE, fname)
        if not os.path.exists(path):
            continue
        try:
            entries = json.load(open(path))
        except Exception:
            continue
        touched = False
        for p in entries:
            if p.get("url") in res:
                p["url"] = res[p["url"]]["canonical_url"]; changed += 1; touched = True
        if touched:
            json.dump(entries, open(path, "w"), indent=1)
    print(f"resolved {len(new)} new (total {len(res)}); pick links upgraded: {changed}")

SEEN = os.path.join(HERE, "results", "seen.jsonl")

def _seen_key(r):
    city = (re.split(r"[,(;]", r["location"] or "") or [""])[0]
    return f"{canon_company(r['company'])}|{norm(r['title'])}|{norm(city)}"

def dedupe_by_url(rows):
    """The resolver can leave an aggregator original and its resolved copy pointing
    at the same canonical URL — keep one, preferring higher tier then the direct
    (non-resolved) record, which carries posted dates and salary fields."""
    best = {}
    for r2 in rows:
        cur = best.get(r2["url"])
        rank = (r2["tier"], 0 if str(r2["source"]).startswith("resolved-") else 1)
        if cur is None or rank > (cur["tier"], 0 if str(cur["source"]).startswith("resolved-") else 1):
            best[r2["url"]] = r2
    return list(best.values())

def cmd_score():
    cands = load_candidates(); descs = load_desc()
    picks = load_picks()
    stamp = datetime.date.today().isoformat()
    seen = {}  # key -> {first_seen, url}; ledger is append-only, last write wins
    if os.path.exists(SEEN):
        with open(SEEN) as f:
            for l in f:
                if l.strip():
                    j = json.loads(l)
                    seen[j["key"]] = {"first_seen": j["first_seen"], "url": j.get("url", "")}
    resolutions = load_resolutions()
    out, ledger_new, blocked = [], [], {}
    for r in cands:
        hit = resolutions.get(r["url"])
        if hit:  # aggregator row resolved to its canonical ATS posting
            r["_alts"] = sorted(set((r.get("_alts") or []) + [r["url"]]))
            r["url"] = hit["canonical_url"]
            r["source"] = "resolved-" + hit["ats"]
        urls = [r["url"]] + (r.get("_alts") or [])
        desc = next((descs[u] for u in urls if descs.get(u)), "")
        tier, why, flags = score_row(r, desc)
        if tier == 0:
            blocked[why] = blocked.get(why, 0) + 1
            continue
        # tier 5 reserved for judgment-confirmed picks (RUBRIC.md standard #2).
        # URL-pinned picks match ONLY by URL (same req posts in many cities);
        # title fallback applies only to picks that never got pinned.
        pick = next((p for p in picks if p.get("url") and p["url"] in urls), None) or \
               next((p for p in picks if not p.get("url")
                     and p["company"].lower() in (r["company"] or "").lower()
                     and (r["title"] or "").lower().strip().startswith(p["title"].lower())), None)
        if pick:
            tier = 5; why = pick["reason"]
            if not desc: flags = list(flags) + ["desc-not-cached"]
        elif tier >= 5:
            tier = 4; flags = list(flags) + ["tier5-proposed"]
        if tier >= 4 and (r.get("source") or "").startswith(AGG_SOURCES):
            flags = list(flags) + ["unresolved-aggregator"]  # ghost risk: no canonical ATS record found
        k = _seen_key(r)
        ent = seen.get(k)
        if ent is None:
            ent = {"first_seen": stamp, "url": r["url"]}
            seen[k] = ent; ledger_new.append({"key": k, **ent})
        elif ent["first_seen"] != stamp and ent.get("url") and ent["url"] != r["url"]:
            flags = list(flags) + ["reposted"]  # same company|title|city under a fresh URL
            ent["url"] = r["url"]
            ledger_new.append({"key": k, "first_seen": ent["first_seen"], "url": r["url"]})
        r2 = dict(r); r2["tier"] = tier; r2["why"] = why; r2["flags"] = ", ".join(flags)
        r2["new"] = "NEW" if ent["first_seen"] == stamp else ""
        r2["first_seen"] = ent["first_seen"]
        r2["desc_snippet"] = (desc or "")[:400]
        odds_label, odds_score, odds_why = hireability(r2, desc)
        r2["odds"] = odds_label; r2["odds_score"] = odds_score; r2["odds_why"] = odds_why
        out.append(r2)
    out = dedupe_by_url(out)
    out.sort(key=lambda x: (-x["tier"], x["company"], x["title"]))
    path = os.path.join(HERE, "results", f"jobs_scored_{stamp}.csv")
    cols = ["tier","odds","odds_score","odds_why","new","title","company","location","salary","salary_min","salary_max","posted","first_seen",
            "remote","source","also_seen_on","url","req_id","why","flags","desc_snippet"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore"); w.writeheader(); w.writerows(out)
    with open(SEEN, "a") as f:
        for e in ledger_new:
            f.write(json.dumps(e) + "\n")
    # coverage report
    from collections import Counter
    src_counts = Counter(r["source"].split(":")[0] for r in out)
    desc_counts = Counter(r["source"].split(":")[0] for r in out if r["desc_snippet"])
    new_rows = [r for r in out if r["new"]]
    health = {}
    shp = os.path.join(HERE, "results", "source_health.json")
    if os.path.exists(shp):
        try: health = json.load(open(shp))
        except Exception: health = {}
    report = {"date": stamp, "candidates_after_dedupe": len(cands), "kept": len(out),
              "new_this_run": len(new_rows),
              "new_by_tier": dict(Counter(r["tier"] for r in new_rows)),
              "tiers": dict(Counter(r["tier"] for r in out)),
              "salary_recovered_from_desc": sum(1 for r in out if "salary-from-desc" in r["flags"]),
              "stale_30d": sum(1 for r in out if "30d+" in r["flags"]),
              "ghost_risk": sum(1 for r in out if any(g in r["flags"] for g in ("stale-90d+", "undated-aggregator", "unresolved-aggregator"))),
              "blocked": blocked,
              "by_source": {s: {"kept": src_counts[s], "with_desc": desc_counts.get(s, 0)} for s in src_counts},
              "source_health": health,
              "zero_row_sources": sorted(g for g, v in health.items() if not v.get("rows")),
              "stale_refresh_sources": sorted(g for g, v in health.items() if v.get("refreshed") is False)}
    with open(os.path.join(HERE, "results", "run_report.json"), "w") as f:
        json.dump(report, f, indent=1)
    print("tiers:", report["tiers"])
    print("kept:", len(out), "| new:", report["new_this_run"], "new_by_tier:", report["new_by_tier"],
          "| salary-from-desc:", report["salary_recovered_from_desc"])
    print("stale>30d:", report["stale_30d"], "| ghost-risk:", report["ghost_risk"])
    if report["zero_row_sources"]:
        print("WARNING zero-row sources:", ", ".join(report["zero_row_sources"]))
    if report["stale_refresh_sources"]:
        print("WARNING kept-stale (empty refresh rejected):", ", ".join(report["stale_refresh_sources"]))
    print("blocked:", blocked)
    print("wrote", path)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["fetch", "score", "resolve"])
    ap.add_argument("--group", default="")
    a = ap.parse_args()
    if a.cmd == "score": return cmd_score()
    if a.cmd == "resolve": return cmd_resolve()
    cfg = load_cfg(); cands = load_candidates()
    g = a.group
    if g == "ats": save_desc(fetch_ats(cands, cfg))
    elif g == "workday": save_desc(fetch_workday(cands, cfg))
    elif g == "amazon": save_desc(fetch_amazon(cands, cfg))
    elif g == "indeed": save_desc(fetch_indeed(cands, cfg))
    elif g.startswith("builtin"): save_desc(fetch_builtin(cands, int(g[-1])))
    elif g == "microsoft": save_desc(fetch_microsoft(cands))
    elif g == "icims": save_desc(fetch_icims_desc(cands))
    elif g == "eightfold": save_desc(fetch_eightfold_desc(cands))
    elif g == "phenom": save_desc(fetch_phenom_desc(cands))
    elif g == "costco": save_desc(fetch_costco_desc(cands))
    elif g == "jibe": save_desc(fetch_jibe_desc(cands, cfg))
    elif g == "yc": save_desc(fetch_yc_desc(cands))
    else: print("unknown group")

if __name__ == "__main__":
    main()
