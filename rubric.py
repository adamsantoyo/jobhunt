#!/usr/bin/env python3
"""
Rubric engine — description-aware scoring per RUBRIC.md.
Usage:
  python3 rubric.py fetch --group ats|workday|amazon|indeed|builtin1|builtin2|microsoft
  python3 rubric.py score
Descriptions cache: results/descriptions.jsonl  {url, desc}
Output: results/jobs_scored_<date>.csv with tier, why, flags
"""
import argparse, csv, datetime, glob, html as htmlmod, json, os, re, sys, time
import requests
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import parse_salary, canon_company, norm, dedupe, VERIFY

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
JH = {**UA, "Accept": "application/json"}
DESC = os.path.join(HERE, "results", "descriptions.jsonl")

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
    return any(k in t for fam in FAMILY.values() for k in fam)

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
# His resume vocabulary — overlap with a JD proxies both ATS keyword match and how well
# he can speak to the role in an interview. More overlap => better odds.
# One regex per distinct skill concept — synonyms are OR'd inside a single entry so a JD
# can't score the same concept twice (e.g. "REST API" must not count both api and rest-api).
HIS_SKILLS = [r"product support", r"technical support", r"troubleshoot", r"help ?desk", r"service ?now",
 r"rest(ful)? apis?|\bapi\b", r"\bsaas\b", r"escalation", r"incident", r"ticket",
 r"\bentra\b|azure ad|active directory", r"\bintune\b", r"autopilot",
 r"microsoft 365|\bm365\b|office 365", r"defender", r"powershell", r"\bpython\b", r"sharepoint",
 r"power platform", r"azure devops|\bado\b", r"\bfirmware\b", r"bios|uefi", r"device driver",
 r"\bimaging\b", r"endpoint", r"\bmdm\b", r"\biam\b", r"single sign|\bsso\b", r"\bazure\b",
 r"\bwindows\b", r"hardware (validation|test)", r"\blab\b"]
# Employers whose support/ops reqs draw very large applicant pools -> lower odds despite good fit.
HIGH_COMPETITION = ["anthropic","openai","google","alphabet","meta","facebook","apple","nvidia","amazon",
 "aws","microsoft","netflix","databricks","stripe","datadog","pinterest","airbnb","snowflake","uber","lyft",
 "coinbase","salesforce","linkedin","roblox","tesla","spacex","figma","notion","rippling","ramp","brex",
 "plaid","scale ai","cohere","reddit","dropbox","twitch","robinhood","instacart","doordash","nvidia"]

def hireability(r, desc):
    """Odds of actually landing it, scored ORTHOGONALLY to the fit tier. Returns
    (label, score, reasons). Positive = more winnable. This is a heuristic proxy, not a
    prediction: it models applicant-pool size, level gap, resume/JD keyword overlap,
    employment-type bar, and comp expectation — none of which the fit rubric touches."""
    t = (r["title"] or "").lower(); company = (r["company"] or "").lower()
    d = (desc or "").lower().replace("\\", "")
    flags = r.get("flags", "") if isinstance(r.get("flags"), str) else ", ".join(r.get("flags") or [])
    s, why = 0, []
    # level gap vs his ~4yrs IC-I/II profile
    if re.search(r"\b(staff|principal|distinguished|director|head)\b", t):
        s -= 3; why.append("staff/principal level")
    elif re.search(r"\b(senior|sr\.?|lead|iii|iv)\b|\blevel ?[3-9]\b|\bl[3-9]\b|\bgrade ?[4-9]\b", t):
        s -= 2; why.append("senior level")
    elif re.search(r"\b(junior|associate|entry|tier ?1|level ?1|\bi\b|\bii\b|tier ?2|level ?2)\b", t):
        s += 2; why.append("junior/associate level")
    # years required
    yreq, _ = years_required(desc)
    if yreq and yreq <= 3: s += 1; why.append(f"asks {yreq}yrs")
    elif yreq and yreq >= 6: s -= 1; why.append(f"asks {yreq}yrs")
    # applicant-pool size proxy
    if any(re.search(r"\b" + re.escape(c) + r"\b", company) for c in HIGH_COMPETITION):
        s -= 2; why.append("high-competition employer")  # word-boundary: 'meta' must not match 'Metabase'
    # employment type: staffing/vendor W2 is a lower hiring bar (and a real vendor-to-FTE path)
    if "Staffing/W2" in flags: s += 2; why.append("staffing/vendor (lower bar)")
    # resume/JD keyword overlap — only meaningful on a substantial JD; a short summary
    # snippet lacking keywords is not evidence of a poor match, so don't penalize it.
    if len(d) > 400:
        hits = sum(1 for p in HIS_SKILLS if re.search(p, d))
        if hits >= 6: s += 2; why.append(f"{hits} skills match")
        elif hits >= 3: s += 1; why.append(f"{hits} skills match")
        elif hits <= 1: s -= 1; why.append("thin skills match")
        # exact daily-stack bonus (his literal admin tools)
        if re.search(r"\bintune\b", d) and re.search(r"\bentra\b|azure ad|active directory", d):
            s += 1; why.append("his exact stack")
    elif d:
        why.append("short JD (skills unscored)")
    # comp expectation vs a below-market current comp and the target band
    try: hi = int(r.get("salary_max") or 0)
    except (TypeError, ValueError): hi = 0
    if hi >= 200000: s -= 1; why.append("comp implies higher bar")
    elif 0 < hi < 95000: s += 1; why.append("comp near his level")
    if "degree-gated" in flags: s -= 1; why.append("hard degree gate")
    label = "Likely" if s >= 3 else ("Reach" if s <= -2 else "Target")
    return label, s, "; ".join(why[:4])

# ---------------- scoring ----------------
STAFFING = ["apex systems","kforce","k-tek","teksystems","insight global","robert half","horizontal talent",
 "vaco","galactic minds","hmg america","vdart","sgs consulting","agreeya","smart folks","maven companies",
 "skylineit","allwyn","inspyr","benchmark it","team red dog","wipro","capgemini","infosys","tata consultancy",
 "cognizant","hcl","ltimindtree","randstad","aerotek","collabera","diverse lynx","mastech","kelly","adecco",
 "experis","modis","akkodis","cybercoders","motion recruitment","jobot","talentburst","russell tobin","eteam",
 "mindlance","artech","pyramid consulting","intelliswift","compunnel","softworld","staffing","recruiting","talent"]
DOMAIN2 = [r"identity (and|&) access", r"identity management", r"\biam\b", r"\bentra\b", r"\bintune\b",
 r"endpoint (management|security|devices)", r"\bm365\b", r"microsoft 365", r"office 365", r"\bazure\b",
 r"active directory", r"single sign[- ]on", r"\bsso\b", r"\bmdm\b", r"device management", r"cloud support",
 r"data ?center", r"hardware (validation|test|qualification)", r"\blabs?\b", r"laboratory", r"\bracks?\b",
 r"\bsilicon\b", r"\bsku\b", r"server (hardware|fleet|platform)", r"\bfleet\b",
 r"\bllms?\b", r"large language model", r"generative ai", r"product support", r"developer support",
 r"rest(ful)? apis?", r"api support"]
DOMAIN1 = [r"servicenow", r"\bitsm\b", r"\bitil\b", r"healthcare", r"hospital", r"health[- ]tech",
 r"manufacturing", r"test floor", r"\bfactory\b", r"government cloud", r"govcloud", r"citizenship",
 r"u\.?s\.? citizen", r"public sector", r"\bgis\b", r"arcgis", r"escalation", r"incident management",
 r"\bsaas\b"]
NON_US = [r"\bchina\b", r"\bcanada\b", r"\bindia\b", r"\buk\b", r"united kingdom", r"\bmexico\b", r"\blatam\b",
 r"bengaluru", r"bangalore", r"hyderabad", r"chennai", r"mumbai", r"\bpune\b", r"noida", r"gurgaon",
 r"berlin", r"paris", r"london", r"dublin", r"madrid", r"barcelona", r"amsterdam", r"warsaw", r"krakow",
 r"toronto", r"vancouver,? b\.?c\.?", r"montreal", r"ontario", r"british columbia", r"tokyo", r"sydney",
 r"\baustralia\b", r"\bperth\b", r"melbourne", r"brisbane", r"adelaide", r"auckland", r"new zealand",
 r"singapore", r"tel aviv", r"s[aã]o paulo", r"mexico city", r"bogot[aá]", r"costa rica", r"heredia",
 # gaps exposed by global company boards (AMD, AppsFlyer, Datadog, Qualcomm) 2026-07-18
 r"taiwan", r"taipei", r"hsinchu", r"malaysia", r"penang", r"\bpoland\b", r"gdansk", r"\bisrael\b",
 r"herzliya", r"argentina", r"buenos aires", r"thailand", r"bangkok", r"\bkorea\b", r"seoul",
 r"\bjapan\b", r"\bosaka\b", r"beijing", r"shanghai", r"shenzhen", r"\bvietnam\b", r"philippines",
 r"\bmanila\b", r"indonesia", r"jakarta", r"\bbrazil\b", r"colombia", r"\bchile\b", r"\bperu\b",
 r"ukraine", r"\bkyiv\b", r"hungary", r"budapest", r"czech", r"prague", r"romania", r"bucharest",
 r"\bfinland\b", r"helsinki", r"\bsweden\b", r"stockholm", r"\bnorway\b", r"\boslo\b", r"denmark",
 r"copenhagen", r"switzerland", r"zurich", r"\bgeneva\b", r"austria", r"vienna", r"\bitaly\b",
 r"\bmilan\b", r"\brome\b", r"\bgreece\b", r"athens", r"turkey", r"istanbul", r"\buae\b", r"dubai",
 r"abu dhabi", r"saudi", r"riyadh", r"\bqatar\b", r"\bdoha\b", r"hong kong", r"south africa",
 r"johannesburg", r"cape town", r"\begypt\b", r"\bcairo\b", r"\bkenya\b", r"nairobi", r"\bnigeria\b",
 r"\blagos\b", r"lisbon", r"portugal", r"\bspain\b", r"\bmunich\b", r"germany", r"\bfrance\b",
 r"netherlands", r"ireland", r"\bbelgium\b", r"brussels",
 # multi-region tags: a bare "remote" must not read as US-remote when the region is foreign
 r"\bemea\b", r"\bapac\b", r"\bmena\b", r"\bcemea\b", r"europe", r"asia[\s-]?pacific",
 r"middle east", r"\banz\b"]
# California carved out entirely — Bay Area is the active target metro, SoCal the long-term home
OTHER_STATE = r"\b(alabama|alaska|arizona|arkansas|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|west virginia|wisconsin|wyoming|district of columbia|atlanta|austin|boston|denver|chicago|herndon|reston)\b"
# two-letter state abbreviations after a separator ("Bastrop, TX", "Remote - CO") — WA and CA excluded
ST_ABBR = re.compile(r"[,\-–]\s*(al|ak|az|ar|co|ct|de|dc|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wv|wi|wy)\b")
CA_PAT = re.compile(r",\s*ca\b|\bcalifornia\b")
SOCAL = ["los angeles", "san diego", "irvine", "santa monica", "long beach", "orange county", "pasadena",
         "burbank", "el segundo", "hawthorne", "carlsbad", "anaheim", "costa mesa", "torrance",
         "culver city", "riverside", "san bernardino", "ventura", "santa barbara"]
# named-target employers (career-position doc 2026-07-18): AI/platform support + silicon/aerospace lanes
TARGET_CO = [r"\banthropic\b", r"\bpylon\b", r"\bdatadog\b", r"\bappsflyer\b", r"\bnvidia\b",
             r"\bamd\b", r"\bqualcomm\b", r"annapurna", r"\bspacex\b", r"blue origin"]
# WA cities far outside the 45-min-from-Redmond commute range (relocation -1, not exclusion)
FAR_WA = ["spokane", "vancouver, wa", "pasco", "kennewick", "richland", "yakima", "wenatchee",
          "bellingham", "walla walla", "moses lake", "pullman", "longview", "port angeles"]
DC_PAT = r"district of columbia|washington,?\s*d\.?c\.?|\bwashington dc\b"
DISQ_SKILL = ["systemverilog","uvm","rtl","asic design","fpga development","rf design","signal integrity","kernel development",
 "device driver development","cpa","registered nurse"," rn ","journeyman","electrician license","pe license","phd required"]
FAMILY = {
 "support": ["support engineer","technical support","escalation engineer","supportability","support analyst","customer engineer","application support","product support","support specialist","developer support"],
 "techops": ["technical operations","operations engineer","technology operations","ops engineer","site operations","it operations"],
 "validation": ["validation","hardware test","test engineer","qualification","sku"],
 "tpm": ["technical program manager","program manager","tpm","technical project manager"],
 "tam": ["technical account manager","solutions engineer","customer success engineer","forward deployed"],
}
# The ONLY in-scope role families (2026-07-19). Anything outside this set — TPM, solutions
# engineer, account management — is excluded from the output entirely, not just down-ranked.
# Widen this tuple to re-open those lanes.
IN_SCOPE_FAMILIES = ("support", "techops", "validation")
# config-driven (single source of truth: config.json profile), memoized
_P = _METRO_C = _BAY_C = _EXCL_C = None
def _profile():
    global _P
    if _P is None: _P = load_cfg()["profile"]
    return _P
def _metro():
    global _METRO_C
    if _METRO_C is None: _METRO_C = [m.lower() for m in _profile()["seattle_metro"]]
    return _METRO_C
def _bay():
    global _BAY_C
    if _BAY_C is None: _BAY_C = [m.lower() for m in _profile().get("bay_area", [])]
    return _BAY_C
def _title_excl():
    global _EXCL_C
    if _EXCL_C is None:
        _EXCL_C = [re.compile(r"\b" + re.escape(x.strip()) + r"\b") for x in _profile().get("title_exclude", [])]
    return _EXCL_C
# IC roles titled "Manager" that are NOT people management
_MGR_IC = re.compile(r"\b(?:program|project|product|account|engagement|escalation|incident|release|case)\s+manager\b")

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

def score_row(r, desc):
    """Rubric scoring per RUBRIC.md. Mutates r's salary fields when a band is
    recovered from the description (WA pay-transparency extraction)."""
    t = (r["title"] or "").lower()
    d = (desc or "").lower().replace("\\", "")
    company = (r["company"] or "").lower()
    loc = (r["location"] or "").lower()
    why, flags = [], []
    # location gate — Bay Area or US-remote ONLY (2026-07-18 directive: "bay area and remote only")
    if any(re.search(p, loc) for p in NON_US):
        return 0, "non-US location", ["blocker"]
    # A Bay Area city name counts as local only when a CA marker is also present. This both
    # rejects cross-state collisions ("Fremont, NE") AND keeps multi-location postings that
    # list SF alongside other cities ("San Francisco, CA | New York City, NY | Seattle, WA").
    has_bay_city = any(re.search(r"\b" + re.escape(c) + r"\b", loc) for c in _bay())
    ca_marker = bool(re.search(r",\s*ca\b|\bcalif", loc)) or "bay area" in loc
    bay_area = has_bay_city and ca_marker
    is_remote = str(r.get("remote", "")).strip().lower() == "true" \
        or bool(re.search(r"\bremote\b|work from home|\bwfh\b|\banywhere\b|\bnationwide\b|\bvirtual\b", loc))
    if not (bay_area or is_remote):
        return 0, "not Bay Area or US-remote", ["off-target-loc"]
    # family / function
    fam = next((f for f, kws in FAMILY.items() if any(k in t for k in kws)), None)
    if not fam:
        fam = next((f for f, kws in FAMILY.items() if any(k in d[:1500] for k in kws)), None)
    func = 3 if fam in ("support", "techops", "validation") else (2 if fam in ("tpm", "tam") else 1)
    # blockers — clearance judged per sentence so "able to obtain" elsewhere can't mask a hold-requirement
    for sent in re.split(r"[.;\n]", d):
        if re.search(r"ts/?sci|top secret|secret clearance|security clearance", sent):
            if re.search(r"requir|must|condition of employment|currently hold|active", sent) \
               and not re.search(r"obtain|eligib", sent):
                return 0, "requires active clearance", ["blocker"]
    # title excludes are word-boundary matches from config.json profile.title_exclude
    # ("intern" no longer swallows "Internal"/"International")
    if any(k in d for k in DISQ_SKILL) or any(p.search(t) for p in _title_excl()):
        return 0, "specialist/level blocker", ["blocker"]
    # people-management: any "Manager"/"Supervisor" in the title once IC manager
    # phrases (Program/Project/Product/Account/... Manager) are removed
    if re.search(r"\bmanager\b|\bsupervisor\b", _MGR_IC.sub(" ", t)):
        return 0, "people-management role", ["blocker"]
    yrs_req, yrs_pref = years_required(d)
    if yrs_req and yrs_req >= 8:
        return 0, f"requires {yrs_req}+ years", ["blocker"]
    score = func
    if func == 3: why.append("core function match")
    # level
    senior = bool(re.search(r"\b(senior|sr\.?|staff|lead|iii|iv)\b", t))
    if fam == "tpm" and senior:
        score -= 2; flags.append("too-senior")
    elif fam == "tpm" and re.search(r"\bii\b", t):
        score -= 1; flags.append("tpm-ii-stretch")
    elif fam == "validation" and senior and any(k in t for k in ["design", "verification"]):
        return 0, "senior design/verification role", ["blocker"]
    elif senior and fam in ("validation", "techops"):
        score -= 1; flags.append("stretch-level")
    elif senior and fam == "support" and any(b in company for b in ["microsoft", "amazon", "google", "meta", "apple"]):
        score -= 1; flags.append("stretch-level")
    if yrs_req and 6 <= yrs_req <= 7: score -= 1; flags.append(f"{yrs_req}yrs-required")
    elif yrs_req and yrs_req <= 3: score += 1; why.append(f"asks {yrs_req}yrs")
    if yrs_pref and yrs_pref >= 8: score -= 1; flags.append(f"{yrs_pref}yrs-preferred")
    # domain (word-boundary regex; EEO boilerplate stripped)
    d_clean = re.sub(r"(equal (employment )?opportunity|eeo|gender identity|sexual orientation)[^.]*\.", " ", d)
    hits2 = [p for p in DOMAIN2 if re.search(p, d_clean) or re.search(p, t)]
    hits1 = [p for p in DOMAIN1 if re.search(p, d_clean) or re.search(p, t)]
    dom = 2 if hits2 else (1 if hits1 else 0)
    if hits2: why.append("domain: " + ",".join(h.replace("\\b", "").replace("\\", "") for h in hits2[:3]))
    elif hits1: why.append("domain: " + ",".join(h.replace("\\b", "").replace("\\", "") for h in hits1[:2]))
    score += dom
    # No support/validation/techops/tpm/tam role family = it doesn't relate — exclude it from
    # the output entirely (a lone domain keyword like "azure" on a Data/Silicon Engineer role
    # is not a support job). Previously these were kept as tier-2 noise.
    if fam is None:
        return 0, "no role-family match", ["skip"]
    if fam not in IN_SCOPE_FAMILIES:
        return 0, "off-focus role (tpm/solutions/account-mgmt)", ["skip"]
    if any(re.search(p, company) for p in TARGET_CO) and fam:
        score += 1; flags.append("target-co"); why.append("named-target employer")
    why.append("Bay Area" if bay_area else "US-remote")
    if re.search(r"\bstaff\b", t):
        score = min(score, 2); flags.append("level-out")  # Principal/Staff: "Out (cap at 2)"
    # logistics: posting age (>30d = -1 per RUBRIC dimension 6)
    age = posting_age_days(r.get("posted"))
    if age is not None and age > 30:
        score -= 1; flags.append("30d+")
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
    if lo and hi and lo <= 150000 and hi >= 99000: score += 1; why.append("comp in band")
    elif hi and hi < 85000: score -= 1; flags.append("low-comp")
    # employment type
    if "c2c" in d or "corp to corp" in d or "third party" in d:
        score -= 2; flags.append("C2C")
    elif any(s in company for s in STAFFING):
        score -= 1; flags.append("Staffing/W2")
    if not desc:
        flags.append("desc-unavailable")
    if re.search(r"bachelor'?s? degree (is )?required", d) and "equivalent" not in d:
        score -= 1; flags.append("degree-gated")
    tier = max(1, min(5, score))
    # RUBRIC tier mapping enforced: 5 requires Function 3 (fam is None already excluded above)
    if func < 3 and tier > 4:
        tier = 4; flags.append("func-cap")
    if not desc and tier > 3:
        tier = 3; flags.append("needs-desc")  # rule zero: no 4/5 without a read description
    # ghost-listing guards: very old or undated aggregator postings can't be 4/5
    if age is not None and age > 90:
        tier = min(tier, 3); flags.append("stale-90d+")
    elif age is None and (r.get("source") or "").startswith(AGG_SOURCES):
        if tier > 3: tier = 3; flags.append("undated-aggregator")
    return tier, "; ".join(why[:6]), flags

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
