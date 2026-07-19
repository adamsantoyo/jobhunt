#!/usr/bin/env python3
"""
Job Hunt Scraper — multi-source active job aggregator (non-MCP arm).
Sources: ATS public APIs (Greenhouse, Lever, Ashby, SmartRecruiters, Workable),
Workday CXS, Microsoft careers (Eightfold PCSX), Amazon.jobs, JobSpy (Indeed/
Glassdoor/ZipRecruiter/Google Jobs).

Usage:
  python3 scraper.py                     # full run
  python3 scraper.py --only ats,workday  # run only listed groups
Output: results/raw.jsonl (append cache, one group per tag) + results/source_health.json.
Scoring lives entirely in rubric.py (rubric.py score reads raw.jsonl directly).
TLS verification is on by default; JOBHUNT_INSECURE=1 disables it for intercepting-proxy sandboxes.
"""
import argparse, concurrent.futures, csv, datetime, json, os, re, sys, time
import requests

# TLS verification on by default; set JOBHUNT_INSECURE=1 only in sandboxes with intercepting proxies
VERIFY = os.environ.get("JOBHUNT_INSECURE", "").lower() not in ("1", "true")
if not VERIFY:
    requests.packages.urllib3.disable_warnings()

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
JH = {**UA, "Accept": "application/json", "Content-Type": "application/json"}

def load_config():
    with open(os.path.join(HERE, "config.json")) as f:
        return json.load(f)

def rec(title, company, location, url, source, posted=None, salary=None, remote=False, req_id=None):
    return {"title": (title or "").strip(), "company": (company or "").strip(),
            "location": (location or "").strip(), "url": url, "source": source,
            "posted": posted or "", "salary": salary or "", "remote": bool(remote),
            "req_id": req_id or ""}

def get(url, params=None, timeout=15):
    return requests.get(url, params=params, headers=UA, timeout=timeout, verify=VERIFY)

# ---------------- ATS sources ----------------
def src_greenhouse(slug, name):
    out = []
    r = get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    if r.status_code != 200: return out
    for j in r.json().get("jobs", []):
        out.append(rec(j.get("title"), name, (j.get("location") or {}).get("name"),
                       j.get("absolute_url"), "greenhouse",
                       posted=(j.get("updated_at") or "")[:10], req_id=str(j.get("id"))))
    return out

def src_lever(slug, name):
    out = []
    r = get(f"https://api.lever.co/v0/postings/{slug}?mode=json")
    if r.status_code != 200: return out
    for j in r.json():
        cat = j.get("categories") or {}
        out.append(rec(j.get("text"), name, cat.get("location"), j.get("hostedUrl"),
                       "lever", posted=str(j.get("createdAt", ""))[:10],
                       remote=(j.get("workplaceType") == "remote")))
    return out

def src_ashby(slug, name):
    out = []
    r = get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
    if r.status_code != 200: return out
    for j in r.json().get("jobs", []):
        comp = ""
        c = j.get("compensation") or {}
        tiers = c.get("compensationTierSummaries") or []
        if tiers: comp = tiers[0].get("compensationTierSummary", "")
        out.append(rec(j.get("title"), name, j.get("location"), j.get("jobUrl"),
                       "ashby", posted=(j.get("publishedAt") or "")[:10],
                       salary=comp, remote=j.get("isRemote", False)))
    return out

def src_smartrecruiters(slug, name):
    out, offset = [], 0
    while True:
        r = get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings",
                params={"limit": 100, "offset": offset})
        if r.status_code != 200: break
        d = r.json(); content = d.get("content", [])
        for j in content:
            loc = j.get("location") or {}
            loctxt = ", ".join(x for x in [loc.get("city"), loc.get("region"), loc.get("country")] if x)
            out.append(rec(j.get("name"), name, loctxt,
                           f"https://jobs.smartrecruiters.com/{slug}/{j.get('id')}",
                           "smartrecruiters", posted=(j.get("releasedDate") or "")[:10],
                           remote=loc.get("remote", False), req_id=str(j.get("id"))))
        offset += len(content)
        if offset >= d.get("totalFound", 0) or not content: break
    return out

def src_recruitee(slug, name):
    out = []
    r = get(f"https://{slug}.recruitee.com/api/offers")
    if r.status_code != 200: return out
    for j in r.json().get("offers", []):
        out.append(rec(j.get("title"), name, j.get("location"), j.get("careers_url"),
                       "recruitee", posted=(j.get("published_at") or "")[:10],
                       remote=(j.get("remote") is True)))
    return out

def src_workable(slug, name):
    out = []
    r = get(f"https://apply.workable.com/api/v1/widget/accounts/{slug}?details=false")
    if r.status_code != 200: return out
    for j in r.json().get("jobs", []):
        out.append(rec(j.get("title"), name, f"{j.get('city','')}, {j.get('state','')}",
                       j.get("shortlink") or j.get("url"), "workable",
                       posted=(j.get("published_on") or "")[:10],
                       remote=(j.get("telecommuting") is True)))
    return out

# ---------------- Workday CXS ----------------
def src_workday(entry, search_terms, max_pages=5):
    out, host, tenant, site, name = [], entry["host"], entry["tenant"], entry["site"], entry["name"]
    url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    seen = set()
    for term in search_terms:
        for q in (f"{term} washington", term):
            offset = 0
            for _ in range(max_pages):
                try:
                    r = requests.post(url, json={"appliedFacets": {}, "limit": 20,
                                                 "offset": offset, "searchText": q},
                                      headers=JH, timeout=15, verify=VERIFY)
                    if r.status_code != 200: break
                    d = r.json(); posts = d.get("jobPostings", [])
                    if not posts: break
                    for j in posts:
                        path = j.get("externalPath", "")
                        if path in seen: continue
                        seen.add(path)
                        out.append(rec(j.get("title"), name, j.get("locationsText"),
                                       f"https://{host}/en-US/{site}{path}", "workday",
                                       posted=j.get("postedOn", ""), req_id=(j.get("bulletFields") or [""])[0]))
                    offset += 20
                    if offset >= d.get("total", 0): break
                    time.sleep(0.2)
                except Exception:
                    break
    return out

# ---------------- Microsoft (Eightfold PCSX) ----------------
_EMP_LOC = None
def _emp_loc():
    """Location filter for employer-own APIs (Microsoft/Amazon), config-driven so it
    tracks the same geography policy as the rest of the pipeline instead of a hard-coded WA."""
    global _EMP_LOC
    if _EMP_LOC is None:
        try:
            _EMP_LOC = load_config()["profile"].get("employer_scrape_location", "California, United States")
        except Exception:
            _EMP_LOC = "California, United States"
    return _EMP_LOC

def src_microsoft(search_terms):
    out, seen = [], set()
    base = "https://apply.careers.microsoft.com/api/pcsx/search"
    for term in search_terms:
        start = 0
        for _ in range(10):
            try:
                r = get(base, params={"domain": "microsoft.com", "query": term,
                                      "location": _emp_loc(),
                                      "start": start, "num": 10, "sort_by": "relevance"})
                if r.status_code != 200: break
                d = r.json().get("data", {})
                pos = d.get("positions", [])
                if not pos: break
                for p in pos:
                    pid = p.get("displayJobId") or p.get("id")
                    if pid in seen: continue
                    seen.add(pid)
                    locs = p.get("standardizedLocations") or p.get("locations") or []
                    if isinstance(locs, str): locs = [locs]
                    wa = [l for l in locs if ", WA," in l or l.endswith(", WA")]
                    loctxt = "; ".join(wa or locs[:2])
                    posted = ""
                    if p.get("postedTs"):
                        posted = datetime.date.fromtimestamp(int(p["postedTs"])).isoformat()
                    purl = p.get("positionUrl") or f"/careers/job/{p.get('id')}"
                    out.append(rec(p.get("name"), "Microsoft", loctxt,
                                   f"https://apply.careers.microsoft.com{purl}",
                                   "microsoft-careers", posted=posted,
                                   remote=(p.get("workLocationOption") == "remote"),
                                   req_id=str(pid)))
                start += 10
                if start >= min(d.get("count", 0), 100): break
                time.sleep(0.2)
            except Exception:
                break
    return out

# ---------------- Amazon.jobs ----------------
def src_amazon(search_terms):
    out, seen = [], set()
    for term in search_terms:
        offset = 0
        for _ in range(5):
            try:
                r = get("https://www.amazon.jobs/en/search.json",
                        params={"base_query": term, "loc_query": _emp_loc(),
                                "result_limit": 100, "offset": offset, "radius": "40km"})
                if r.status_code != 200: break
                d = r.json(); jobs = d.get("jobs", [])
                if not jobs: break
                for j in jobs:
                    jid = j.get("id_icims") or j.get("job_path")
                    if jid in seen: continue
                    seen.add(jid)
                    out.append(rec(j.get("title"), "Amazon",
                                   f"{j.get('city','')}, {j.get('state','')}",
                                   "https://www.amazon.jobs" + (j.get("job_path") or ""),
                                   "amazon-jobs", posted=j.get("posted_date", ""), req_id=str(jid)))
                offset += 100
                if offset >= min(d.get("hits", 0), 500): break
                time.sleep(0.2)
            except Exception:
                break
    return out

# ---------------- JobSpy (Indeed / Glassdoor / ZipRecruiter / Google) ----------------
def src_jobspy(cfg):
    try:
        from jobspy import scrape_jobs
    except ImportError:
        print("  [jobspy] not installed — skipping", file=sys.stderr)
        return []
    out = []
    jcfg = cfg["jobspy"]
    # Bay Area + US-remote only (2026-07-18 directive). Each search = (location, is_remote).
    searches = jcfg.get("searches") or [{"location": "San Francisco Bay Area, CA", "is_remote": False}]
    def s(v):
        if v is None or (isinstance(v, float) and v != v): return ""
        return str(v)
    for term in cfg["profile"]["search_terms"]:
        for site in jcfg["sites"]:
            for sc in searches:
                loc = sc.get("location", "San Francisco Bay Area, CA")
                is_rem = bool(sc.get("is_remote"))
                gterm = f"{term} jobs {'remote in the US' if is_rem else 'in the San Francisco Bay Area'}"
                try:
                    wanted = jcfg.get(f"results_wanted_{site}", jcfg["results_wanted_per_site"])
                    df = scrape_jobs(site_name=[site], search_term=term, google_search_term=gterm,
                                     location=loc, is_remote=is_rem, results_wanted=wanted,
                                     hours_old=jcfg.get("hours_old"), country_indeed=jcfg["country_indeed"])
                    for _, r_ in df.iterrows():
                        sal = ""
                        if s(r_.get("min_amount")):
                            sal = f"{r_.get('min_amount')}-{r_.get('max_amount')} {s(r_.get('interval'))}"
                        # Trust jobspy's per-row remote flag; a remote SEARCH also returns on-site
                        # roles, so is_rem must NOT force remote=True (that polluted the remote bucket).
                        rc = rec(s(r_.get("title")), s(r_.get("company")), s(r_.get("location")),
                                 s(r_.get("job_url")), f"jobspy-{site}",
                                 posted=s(r_.get("date_posted"))[:10], salary=sal,
                                 remote=bool(r_.get("is_remote")))
                        # capture the description at scrape time so rubric.fetch_indeed needn't
                        # re-scrape Indeed (results drift between passes -> missed descriptions)
                        dtxt = s(r_.get("description"))
                        if dtxt: rc["_desc"] = dtxt[:6000]
                        out.append(rc)
                    print(f"  [jobspy] {site} '{term}' [{loc}{' remote' if is_rem else ''}]: {len(df)}")
                except Exception as e:
                    print(f"  [jobspy] {site} '{term}' [{loc}] FAIL: {str(e)[:80]}", file=sys.stderr)
    # De-dup within the run and drop reposting floods: aggregator queries overlap heavily
    # (~40% dup URLs) and staffing agencies repost one job dozens of times. Keep first-seen
    # URLs, and at most JOBSPY_TITLE_CAP copies of any one company+title.
    seen_urls, title_ct, cleaned = set(), {}, []
    cap = jcfg.get("title_cap", 5)
    for r_ in out:
        u = r_.get("url")
        if not u or u in seen_urls:
            continue
        seen_urls.add(u)
        k = ((r_.get("company") or "").lower(), (r_.get("title") or "").lower())
        title_ct[k] = title_ct.get(k, 0) + 1
        if title_ct[k] > cap:
            continue
        cleaned.append(r_)
    print(f"  [jobspy] deduped {len(out)} -> {len(cleaned)} rows (dropped dups + reposting floods)")
    return cleaned

# ---------------- iCIMS / Eightfold / Phenom ----------------
def src_icims(host, name, search_terms=None):
    """iCIMS Attract portals ({host}.icims.com). Multi-word keyword search is
    unreliable, so enumerate the whole (small) board and let the rubric filter."""
    out, seen = [], set()
    for pr in range(0, 6):
        try:
            r = get(f"https://{host}.icims.com/jobs/search",
                    params={"in_iframe": "1", "pr": pr}, timeout=15)
            if r.status_code != 200: break
            found = 0
            for m in re.finditer(r'<a[^>]+href="(https://[^"]+/jobs/(\d+)/[^"]+/job)[^"]*"[^>]*>(.*?)</a>', r.text, re.S):
                url, jid, ttl = m.groups()
                found += 1
                if jid in seen: continue
                seen.add(jid)
                title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", ttl)).strip()
                title = re.sub(r"^Title\s+", "", title)  # a11y table-header prefix
                seg = r.text[m.end(): m.end() + 900]
                lm = re.search(r"US-([A-Z]{2})-([A-Za-z .\-]+?)[<|&]", seg)
                loc = f"{lm.group(2).strip()}, {lm.group(1)}" if lm else ""
                if title:
                    out.append(rec(title, name, loc, url, "icims", req_id=jid))
            if not found: break
            time.sleep(0.2)
        except Exception:
            break
    return out

def src_eightfold(entry, search_terms):
    """Eightfold PCSX public search (same API family as Microsoft careers)."""
    out, seen = [], set()
    base, dom, name = entry["base"], entry["domain"], entry["name"]
    for term in search_terms:
        start = 0
        for _ in range(5):
            try:
                r = get(f"{base}/api/pcsx/search", params={"domain": dom, "query": term,
                        "location": entry.get("location", "United States"), "start": start, "num": 10}, timeout=15)
                if r.status_code != 200: break
                pos = r.json().get("data", {}).get("positions", [])
                if not pos: break
                for p in pos:
                    pid = p.get("id")
                    if pid in seen: continue
                    seen.add(pid)
                    locs = p.get("standardizedLocations") or p.get("locations") or []
                    if isinstance(locs, str): locs = [locs]
                    # target metros first: WA (current) and CA (Bay Area / SoCal lanes)
                    wa = [l for l in locs if ", WA" in l or ", CA" in l]
                    posted = ""
                    if p.get("postedTs"):
                        posted = datetime.date.fromtimestamp(int(p["postedTs"])).isoformat()
                    out.append(rec(p.get("name"), name, "; ".join(wa or locs[:2]),
                                   f"{base}/careers/job/{pid}?domain={dom}", "eightfold",
                                   posted=posted, req_id=str(p.get("displayJobId") or pid)))
                start += 10
                time.sleep(0.2)
            except Exception:
                break
    return out

def src_phenom(entry, search_terms, size=50):
    """Phenom People career sites expose a public /widgets refineSearch API."""
    out, seen = [], set()
    base, name = entry["base"], entry["name"]
    for term in search_terms:
        for frm in (0, size):
            payload = {"lang": "en_us", "deviceType": "desktop", "country": "us",
                       "pageName": "search-results", "ddoKey": "refineSearch", "sortBy": "",
                       "from": frm, "jobs": True, "counts": True,
                       "all_fields": ["category", "state", "city"], "size": size,
                       "keywords": term, "global": True, "selected_fields": {}, "locationData": {}}
            try:
                r = requests.post(f"{base}/widgets", json=payload, headers=JH, timeout=15, verify=VERIFY)
                if r.status_code != 200: break
                jobs = r.json().get("refineSearch", {}).get("data", {}).get("jobs", [])
                if not jobs: break
                for j in jobs:
                    jid = j.get("jobId") or j.get("jobSeqNo")
                    if jid in seen: continue
                    seen.add(jid)
                    url = j.get("applyUrl") or f"{base}/job/{jid}"
                    out.append(rec(j.get("title"), name,
                                   j.get("cityStateCountry") or j.get("location") or j.get("cityState"),
                                   url, "phenom",
                                   posted=str(j.get("postedDate") or j.get("dateCreated") or "")[:10],
                                   req_id=str(jid)))
            except Exception:
                break
    return out

COSTCO_TECH = ["engineer", "technician", "analyst", "program", "technolog", "systems", "support",
               "developer", "administrator", "network", "data", "security", "operations", "it "]

def src_costco(search_terms=None):
    """Costco careers API. Keyword search is ML-fuzzy and useless for precision,
    so enumerate all Washington jobs (state facet) and title-filter at harvest."""
    out, seen = [], set()
    for page in range(1, 15):
        try:
            r = get("https://careers.costco.com/api/jobs",
                    params={"state": "Washington", "limit": 100, "page": page, "lang": "en-us"}, timeout=15)
            if r.status_code != 200: break
            jobs = r.json().get("jobs", [])
            if not jobs: break
            for wrap in jobs:
                d = wrap.get("data", wrap)
                jid = d.get("req_id") or d.get("slug")
                title = d.get("title") or ""
                if jid in seen or not any(k in title.lower() for k in COSTCO_TECH):
                    continue
                seen.add(jid)
                city = d.get("city") or ""
                state = d.get("state") or ""
                loc = d.get("full_location") or ", ".join(x for x in [city, state] if x)
                url = d.get("apply_url") or d.get("canonical_url") or \
                      f"https://careers.costco.com/jobs/{d.get('slug', '')}"
                out.append(rec(title, "Costco Wholesale", loc, url, "costco",
                               posted=(d.get("posted_date") or d.get("create_date") or "")[:10],
                               req_id=str(jid)))
            time.sleep(0.2)
        except Exception:
            break
    return out

def src_jibe(entry, search_terms):
    """Generic Jibe / iCIMS Careers Cloud portal ({base}/api/jobs) — same API family
    as Costco's. Keyword search is ML-fuzzy, so title-filter at harvest. The list
    response carries full descriptions inline; rubric.fetch_jibe_desc caches them."""
    out, seen = [], set()
    base, name = entry["base"], entry["name"]
    for term in search_terms:
        for page in range(1, 6):
            try:
                r = get(f"{base}/api/jobs",
                        params={"keywords": term, "limit": 100, "page": page, "lang": "en-us"}, timeout=20)
                if r.status_code != 200: break
                jobs = r.json().get("jobs", [])
                if not jobs: break
                for wrap in jobs:
                    d = wrap.get("data", wrap)
                    jid = str(d.get("req_id") or d.get("slug") or "")
                    title = d.get("title") or ""
                    if not jid or jid in seen or not any(k in title.lower() for k in COSTCO_TECH):
                        continue
                    seen.add(jid)
                    loc = d.get("full_location") or ", ".join(x for x in [d.get("city") or "", d.get("state") or ""] if x)
                    # apply_url is an ATS login link on these portals; the slug page is canonical
                    url = f"{base}/jobs/{d.get('slug', jid)}"
                    out.append(rec(title, name, loc, url, "jibe",
                                   posted=str(d.get("posted_date") or d.get("create_date") or "")[:10],
                                   req_id=jid))
                time.sleep(0.2)
            except Exception:
                break
    return out

# ---------------- Startup boards: YC + Built In ----------------
def _yc_page(path):
    import html as _html
    try:
        r = get(f"https://www.ycombinator.com{path}", timeout=15)
        if r.status_code != 200: return {}
        m = re.search(r'data-page="([^"]+)"', r.text)
        return json.loads(_html.unescape(m.group(1))).get("props", {}) if m else {}
    except Exception:
        return {}

def src_yc():
    out, seen = [], set()
    root = _yc_page("/jobs")
    paths = ["/jobs"]
    for role in root.get("jobRoles", []):
        paths.append(f"/jobs/role/{role['slug']}")
    for locn in root.get("jobLocations", []):
        paths.append(f"/jobs/location/{locn['slug']}")
    for p in paths:
        props = root if p == "/jobs" else _yc_page(p)
        for j in props.get("jobPostings", []):
            if j.get("id") in seen: continue
            seen.add(j.get("id"))
            url = j.get("url") or j.get("applyUrl") or ""
            if url.startswith("/"): url = "https://www.ycombinator.com" + url
            company = ""
            mm = re.match(r"/companies/([^/]+)/", j.get("url") or "")
            if mm: company = mm.group(1).replace("-", " ").title()
            out.append(rec(j.get("title"), company or "YC startup", j.get("location"), url,
                           "yc-jobs", remote=("remote" in (j.get("location") or "").lower())))
    return out

def src_builtin(search_terms, max_pages=4):
    """Parse Built In listing pages directly (job detail pages rate-limit at 429)."""
    out, seen = [], set()
    # Bay Area metro + US-remote startup listings (2026-07-18 directive)
    locales = [{"params": {"city": "San Francisco", "state": "CA", "country": "USA"}, "loc": "San Francisco, CA (metro)", "remote": False},
               {"params": {"remote": "true", "country": "USA"}, "loc": "Remote, US", "remote": True}]
    for lc in locales:
        for term in search_terms:
            for page in range(1, max_pages + 1):
                try:
                    r = get("https://builtin.com/jobs", params={**lc["params"], "search": term, "page": page}, timeout=15)
                    if r.status_code != 200: break
                    t = r.text
                    cards = [(m.start(), m.group(1), m.group(2)) for m in
                             re.finditer(r'<h2[^>]*><a[^>]+href="(/job/[^"]+)"[^>]*>([^<]+)</a>', t)]
                    if not cards: break
                    comp_pos = [(m.start(), m.group(1)) for m in re.finditer(r'href="/company/([^"/]+)"', t)]
                    for pos, path, title in cards:
                        if path in seen: continue
                        seen.add(path)
                        company = ""
                        for cp, cslug in reversed(comp_pos):
                            if cp < pos:
                                company = cslug.replace("-", " ").title(); break
                        chunk = t[max(0, pos - 3000):pos + 500]
                        sal = ""
                        ms = re.search(r'\$([\d,]+)K?\s*[-–]\s*\$?([\d,]+)K?', chunk)
                        if ms: sal = ms.group(0)
                        remote = lc["remote"] or ("Remote" in chunk and "Hybrid" not in chunk)
                        out.append(rec(title, company, lc["loc"], f"https://builtin.com{path}",
                                       "builtin", salary=sal, remote=remote))
                    time.sleep(0.3)
                except Exception:
                    break
    return out

# ---------------- Shared normalization / parsing ----------------
def norm(s): return re.sub(r"[^a-z0-9 ]", "", (s or "").lower()).strip()

def parse_salary(s):
    """Salary text -> annual (lo, hi). Handles hourly rates, k-suffix, 2-digit numbers.
    '31-64 hourly' -> (64480, 133120); '$47.5K - $95K' -> (47500, 95000)."""
    if not s: return None, None
    txt = str(s).lower().replace(",", "")
    txt = re.sub(r"401\s*\(?k\)?", " ", txt)
    hourly = bool(re.search(r"hour|hourly|/hr\b|\bhr\b", txt))
    raw = re.findall(r"\$?\s*(\d{1,7}(?:\.\d+)?)\s*(k)?", txt)
    nums = []
    any_k = any(k for _, k in raw)
    for m, k in raw:
        try: v = float(m)
        except ValueError: continue
        if k: v *= 1000
        elif hourly and v < 500: v *= 2080
        elif any_k and 20 <= v <= 999: v *= 1000   # "120-150k" -> both thousands
        nums.append(v)
    vals = [v for v in nums if 25000 <= v <= 900000]
    if not vals: return None, None
    return int(min(vals)), int(max(vals))

COMPANY_CANON = {"amazoncom": "amazon", "amazon web services": "amazon", "aws": "amazon",
 "the boeing company": "boeing", "microsoft corporation": "microsoft", "t mobile": "tmobile",
 "tmobile usa": "tmobile", "meta platforms": "meta", "crusoe energy": "crusoe",
 "salesforce inc": "salesforce", "ntt data inc": "ntt data"}

def canon_company(c):
    x = norm((c or "").replace(".com", " ").replace(",", " "))
    x = re.sub(r"\b(inc|llc|corp|corporation|co|ltd|group|holdings|company)\b", " ", x)
    x = re.sub(r"^the\s+", "", re.sub(r"\s+", " ", x).strip())
    return COMPANY_CANON.get(x, x)

def _src_prio(source):
    prio = {"greenhouse": 0, "lever": 0, "ashby": 0, "workday": 0, "microsoft": 0,
            "amazon": 0, "smartrecruiters": 0, "workable": 0, "recruitee": 0}
    return prio.get(source.split("-")[0].split(":")[0], 9)

def dedupe(rows):
    """Canonical-company dedupe. Prefers direct-ATS record; merges non-empty fields
    (salary/posted survive from aggregator mirrors); tracks mirror urls in _alts."""
    def key(r_):
        city = (re.split(r"[,(;]", r_["location"] or "") or [""])[0]
        return (canon_company(r_["company"]), norm(r_["title"]), norm(city))
    best = {}
    for r_ in rows:
        k = key(r_)
        if k not in best:
            r_.setdefault("_alts", [])
            best[k] = r_
            continue
        a = best[k]
        hi, lo_ = (r_, a) if _src_prio(r_["source"]) < _src_prio(a["source"]) else (a, r_)
        merged = dict(lo_)
        for kk, vv in hi.items():
            if vv not in ("", None, []): merged[kk] = vv
        merged["_alts"] = sorted(set(a.get("_alts", []) + [a["url"], r_["url"]]) - {merged.get("url")})
        merged["also_seen_on"] = ", ".join(sorted({a["source"], r_["source"]} - {merged["source"]}))
        best[k] = merged
    return list(best.values())

# ---------------- Main ----------------
RAW = os.path.join(HERE, "results", "raw.jsonl")
HEALTH = os.path.join(HERE, "results", "source_health.json")

def _record_health(group, n, refreshed=True):
    """Per-group harvest counts so zero-row sources (markup change, block, outage)
    surface in the run report instead of failing silently (RUBRIC standard #5).
    refreshed=False means an empty refresh was rejected and prior data was kept."""
    h = {}
    if os.path.exists(HEALTH):
        try:
            with open(HEALTH) as f: h = json.load(f)
        except Exception: h = {}
    h[group] = {"rows": n, "refreshed": refreshed,
                "at": datetime.datetime.now().isoformat(timespec="seconds")}
    with open(HEALTH, "w") as f:
        json.dump(h, f, indent=1)
    if n == 0 and refreshed:
        print(f"[{group}] WARNING: 0 rows harvested — source may be broken or blocking", file=sys.stderr)

def save_raw(group, rows, allow_empty=False):
    """Replace prior rows of this group in the resumable raw cache. A swallowed adapter
    error returns [] and looks identical to 'no jobs', so an empty refresh KEEPS the
    last-known-good group data unless allow_empty=True — a transient outage must not wipe
    the cache and still report success. Written atomically (os.replace) so a crash
    mid-write can't truncate the file."""
    os.makedirs(os.path.dirname(RAW), exist_ok=True)
    all_old = []
    if os.path.exists(RAW):
        with open(RAW) as f:
            all_old = [json.loads(l) for l in f if l.strip()]
    prior = [r_ for r_ in all_old if r_.get("_group") == group]
    if not rows and prior and not allow_empty:
        _record_health(group, len(prior), refreshed=False)
        print(f"[{group}] refresh returned 0 rows — KEPT {len(prior)} cached (possible transient failure)",
              file=sys.stderr)
        return
    old = [r_ for r_ in all_old if r_.get("_group") != group]
    for r_ in rows: r_["_group"] = group
    tmp = RAW + ".tmp"
    with open(tmp, "w") as f:
        for r_ in old + rows:
            f.write(json.dumps(r_) + "\n")
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, RAW)
    _record_health(group, len(rows))
    print(f"[{group}] cached {len(rows)} rows (raw total {len(old)+len(rows)})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="comma list: ats,workday,microsoft,amazon,icims,eightfold,phenom,costco,jibe,yc,builtin,jobspy")
    ap.add_argument("--wd-tenant", default="", help="run only these workday tenant slugs (comma)")
    ap.add_argument("--js-site", default="", help="jobspy: only this site")
    ap.add_argument("--js-terms", default="", help="jobspy: term indices e.g. 0,1")
    args = ap.parse_args()
    only = set(x for x in args.only.split(",") if x)
    def enabled(g): return not only or g in only

    cfg = load_config()
    terms = cfg["profile"]["search_terms"]
    t0 = time.time()

    if enabled("ats"):
        jobs_ats = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
            futs = []
            for kind, fn in [("greenhouse", src_greenhouse), ("lever", src_lever),
                             ("ashby", src_ashby), ("smartrecruiters", src_smartrecruiters),
                             ("workable", src_workable), ("recruitee", src_recruitee)]:
                for slug, name in cfg["companies"].get(kind, {}).items():
                    futs.append(ex.submit(fn, slug, name))
            for f in concurrent.futures.as_completed(futs):
                try: jobs_ats.extend(f.result())
                except Exception as e: print("  [ats] worker fail:", str(e)[:80], file=sys.stderr)
        save_raw("ats", jobs_ats)

    if enabled("workday"):
        sel = set(x for x in args.wd_tenant.split(",") if x)
        entries = {k: v for k, v in cfg["companies"]["workday"].items() if not sel or k in sel}
        jobs_wd = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = [ex.submit(src_workday, e, terms) for e in entries.values()]
            for f in concurrent.futures.as_completed(futs):
                try: jobs_wd.extend(f.result())
                except Exception as e: print("  [workday] fail:", str(e)[:80], file=sys.stderr)
        save_raw("workday:" + (args.wd_tenant or "all"), jobs_wd)

    if enabled("icims"):
        rows_i = []
        for host, e in cfg["companies"].get("icims", {}).items():
            rows_i += src_icims(host, e["name"], terms)
        save_raw("icims", rows_i)
    if enabled("eightfold"):
        rows_e = []
        for _, e in cfg["companies"].get("eightfold", {}).items():
            rows_e += src_eightfold(e, terms)
        save_raw("eightfold", rows_e)
    if enabled("phenom"):
        rows_p = []
        for _, e in cfg["companies"].get("phenom", {}).items():
            rows_p += src_phenom(e, terms)
        save_raw("phenom", rows_p)
    if enabled("microsoft"):
        save_raw("microsoft", src_microsoft(terms))
    if enabled("amazon"):
        save_raw("amazon", src_amazon(terms))
    if enabled("costco"):
        save_raw("costco", src_costco(terms))
    if enabled("jibe"):
        rows_j = []
        for _, e in cfg["companies"].get("jibe", {}).items():
            rows_j += src_jibe(e, terms)
        save_raw("jibe", rows_j)
    if enabled("yc"):
        save_raw("yc", src_yc())
    if enabled("builtin"):
        save_raw("builtin", src_builtin(terms))
    if enabled("jobspy"):
        jcfg = cfg["jobspy"]
        if args.js_site: jcfg["sites"] = [args.js_site]
        if args.js_terms:
            idx = [int(i) for i in args.js_terms.split(",")]
            cfg["profile"]["search_terms"] = [terms[i] for i in idx if i < len(terms)]
        tag = f"jobspy:{args.js_site or 'all'}:{args.js_terms or 'all'}"
        save_raw(tag, src_jobspy(cfg))
    print(f"({time.time()-t0:.0f}s)")

if __name__ == "__main__":
    main()
