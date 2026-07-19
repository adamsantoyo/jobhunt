#!/usr/bin/env python3
"""
ATS discovery: given company name guesses, probe public ATS APIs and Workday
tenants, print config-ready JSON. Usage: python3 discover.py slug1 slug2 ...
(or edit CANDIDATES below and run with no args)
"""
import argparse, concurrent.futures, json, os, re, sys
import requests
requests.packages.urllib3.disable_warnings()
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
JH = {**UA, "Accept": "application/json", "Content-Type": "application/json"}

# Seattle-area employer candidates: slug guesses per company
CANDIDATES = {
 "Zillow": ["zillow", "zillowgroup"], "Redfin": ["redfin"], "Expedia": ["expedia", "expediagroup"],
 "Smartsheet": ["smartsheet"], "DocuSign": ["docusign"], "F5": ["f5", "f5networks"],
 "Alaska Airlines": ["alaskaair", "alaskaairlines"], "Starbucks": ["starbucks"], "REI": ["rei"],
 "PitchBook": ["pitchbook", "pitchbookdata"], "Avalara": ["avalara"], "Remitly": ["remitly"],
 "Rover": ["rover", "roverdotcom"], "Amperity": ["amperity"], "Truveta": ["truveta"],
 "Qualtrics": ["qualtrics"], "Snowflake": ["snowflake"], "Databricks": ["databricks"],
 "Stripe": ["stripe"], "Dropbox": ["dropbox"], "Okta": ["okta", "auth0"],
 "NVIDIA": ["nvidia"], "Intel": ["intel"], "AMD": ["amd"], "Qualcomm": ["qualcomm"],
 "Apple": ["apple"], "Oracle": ["oracle"], "Stoke Space": ["stokespace", "stoke-space"],
 "Kymeta": ["kymeta", "kymetacorp"], "Echodyne": ["echodyne", "echodyne-corp"],
 "Zap Energy": ["zapenergy", "zap-energy", "zapenergyinc"], "First Mode": ["firstmode"],
 "Absci": ["absci"], "Fortive": ["fortive"], "Fluke": ["fluke"], "Terex": ["terex"],
 "PACCAR": ["paccar"], "Weyerhaeuser": ["weyerhaeuser"], "Philips": ["philips"],
 "Crane Aerospace": ["craneae", "crane-aerospace-electronics", "craneaerospace"],
 "Bungie": ["bungie"], "Nintendo": ["nintendo", "nintendoofamerica"], "Valve": ["valvesoftware", "valve"],
 "Wizards of the Coast": ["wizardsofthecoast", "hasbro"], "Electronic Arts": ["ea", "electronicarts"],
 "Providence": ["providence", "providencehealth"], "Seattle Children's": ["seattlechildrens"],
 "UW Medicine": ["uw", "uwmedicine", "universityofwashington"], "Kaiser Permanente": ["kaiserpermanente", "kp"],
 "Fred Hutch": ["fredhutch", "fredhutchinsoncancercenter", "fhcc"],
 "Costco": ["costco", "costcowholesale"], "Slalom": ["slalom"], "Launch Consulting": ["launchconsulting", "launch-consulting-group", "launchcg"],
 "Getty Images": ["gettyimages"], "Chewy": ["chewy", "chewycom"], "SAP Concur": ["concur", "sap"],
 "ExtraHop": ["extrahop"], "WatchGuard": ["watchguard", "watchguard-technologies"],
 "Icertis": ["icertis"], "98point6": ["98point6"], "Panopto": ["panopto"],
 "Convoy": ["convoy"], "Flexport": ["flexport"], "Brex": ["brex"], "Gusto": ["gusto"],
 "Airbnb": ["airbnb"], "Uber": ["uber"], "Lyft": ["lyft"], "Pinterest": ["pinterest", "pinterestcareers"],
 "Twitch": ["twitch"], "Roblox": ["roblox"], "Unity": ["unity", "unity3d", "unitytechnologies"],
 "ServiceNow": ["servicenow"], "Workiva": ["workiva"], "Seeq": ["seeq"], "Outreach": ["outreach"],
 "Karat": ["karat"], "SeekOut": ["seekout"], "Textio": ["textio"], "Statsig": ["statsig"],
 "Temporal": ["temporal", "temporal-technologies", "temporaltechnologies"], "Chef/Progress": ["chef"],
 "Tableau": ["tableau"], "Esper": ["esper", "esperio"], "Auth0": ["auth0"],
 "SanMar": ["sanmar"], "Darigold": ["darigold"], "Symetra": ["symetra"], "PEMCO": ["pemco"],
 "Puget Sound Energy": ["pse", "pugetsoundenergy"], "Premera": ["premera", "premerablue"],
 "Regence": ["regence", "cambiahealth", "cambia"], "Accolade": ["accolade"],
 "Edifecs": ["edifecs"], "Vulcan": ["vulcan"], "PATH": ["path"], "Gates Foundation": ["gatesfoundation"],
}

def probe_boards(slug):
    hits = {}
    apis = {
        "greenhouse": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
        "lever": f"https://api.lever.co/v0/postings/{slug}?mode=json&limit=100",
        "ashby": f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
        "smartrecruiters": f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1",
        "workable": f"https://apply.workable.com/api/v1/widget/accounts/{slug}?details=false",
        "recruitee": f"https://{slug}.recruitee.com/api/offers",
    }
    for kind, url in apis.items():
        try:
            r = requests.get(url, headers=UA, timeout=8, verify=False)
            if r.status_code != 200: continue
            d = r.json()
            n = 0
            if kind == "greenhouse": n = len(d.get("jobs", []))
            elif kind == "lever": n = len(d) if isinstance(d, list) else 0
            elif kind == "ashby": n = len(d.get("jobs", []))
            elif kind == "smartrecruiters": n = d.get("totalFound", 0)
            elif kind == "workable": n = len(d.get("jobs", []))
            elif kind == "recruitee": n = len(d.get("offers", []))
            if n > 0: hits[kind] = n
        except Exception:
            pass
    return hits

def _probe_wd_host(slug, wd):
    host = f"{slug}.{wd}.myworkdayjobs.com"
    try:
        r = requests.get(f"https://{host}/", headers=UA, timeout=5, verify=False, allow_redirects=True)
        if r.status_code != 200: return None
        m = re.search(r"/(?:[a-z]{2}-[A-Z]{2}/)?([A-Za-z0-9_\-]+)/?$", r.url)
        site = m.group(1) if m else None
        if not site or site.lower() in ("login",): return None
        cr = requests.post(f"https://{host}/wday/cxs/{slug}/{site}/jobs",
                           json={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""},
                           headers=JH, timeout=8, verify=False)
        if cr.status_code == 200:
            return {"host": host, "tenant": slug, "site": site, "total": cr.json().get("total", 0)}
    except Exception:
        return None
    return None

def probe_workday(slug):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futs = [ex.submit(_probe_wd_host, slug, wd) for wd in ["wd1", "wd5", "wd3", "wd12", "wd501"]]
        for f in concurrent.futures.as_completed(futs):
            r = f.result()
            if r: return r
    return None

def discover(name, slugs):
    found = []
    for slug in slugs:
        b = probe_boards(slug)
        for kind, n in b.items():
            found.append((kind, slug, n))
        wd = probe_workday(slug)
        if wd:
            found.append(("workday", slug, wd))
    return name, found

HITS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "discover_hits.jsonl")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slice", default="", help="e.g. 0:20 — slice of candidate list")
    ap.add_argument("--finalize", action="store_true", help="build config additions from hits file")
    ap.add_argument("names", nargs="*")
    args = ap.parse_args()

    if args.finalize:
        by_name = {}
        with open(HITS) as f:
            for line in f:
                h = json.loads(line)
                by_name.setdefault(h["name"], []).append((h["kind"], h["slug"], h["info"]))
        cfg_add = {"greenhouse": {}, "lever": {}, "ashby": {}, "smartrecruiters": {}, "workable": {}, "recruitee": {}, "workday": {}}
        for name, found in by_name.items():
            best = sorted(found, key=lambda x: -(x[2]["total"] if isinstance(x[2], dict) else x[2]))[0]
            kind, slug, info = best
            if kind == "workday":
                cfg_add["workday"][slug] = {"host": info["host"], "tenant": info["tenant"], "site": info["site"], "name": name}
            else:
                cfg_add[kind][slug] = name
        print(json.dumps(cfg_add, indent=1))
        return

    cands = CANDIDATES
    if args.names:
        cands = {s: [s] for s in args.names}
    items = list(cands.items())
    if args.slice:
        a, b = args.slice.split(":")
        items = items[int(a):int(b)]
    os.makedirs(os.path.dirname(HITS), exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(discover, n, s): n for n, s in items}
        with open(HITS, "a") as out:
            for f in concurrent.futures.as_completed(futs):
                name, found = f.result()
                for kind, slug, info in found:
                    print(f"{name}: {kind} slug={slug} -> {info}")
                    out.write(json.dumps({"name": name, "kind": kind, "slug": slug, "info": info}) + "\n")

if __name__ == "__main__":
    main()
