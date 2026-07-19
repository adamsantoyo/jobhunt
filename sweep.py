#!/usr/bin/env python3
"""Sweep orchestrator. Each shell call runs `python3 sweep.py --next` until DONE.
Designed for ~45s execution windows: one chunk per call, state persisted between calls.
  --next    run the next pending step (fixture gate runs first, aborts sweep on failure)
  --status  show step states
  --reset   clear state for a fresh weekly run (keeps seen.jsonl ledger)
"""
import json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "results", "sweep_state.json")

STEPS = [
    ("tests",            [sys.executable, "tests.py"]),
    ("scrape:ats",       [sys.executable, "scraper.py", "--only", "ats"]),
    ("scrape:microsoft", [sys.executable, "scraper.py", "--only", "microsoft"]),
    ("scrape:amazon",    [sys.executable, "scraper.py", "--only", "amazon"]),
    ("scrape:yc",        [sys.executable, "scraper.py", "--only", "yc"]),
    ("scrape:builtin",   [sys.executable, "scraper.py", "--only", "builtin"]),
    ("scrape:wd1",       [sys.executable, "scraper.py", "--only", "workday", "--wd-tenant", "tmobile,nordstrom"]),
    ("scrape:wd2",       [sys.executable, "scraper.py", "--only", "workday", "--wd-tenant", "boeing,salesforce,blueorigin"]),
    ("scrape:wd3",       [sys.executable, "scraper.py", "--only", "workday", "--wd-tenant", "expedia,ffive,intel,nvidia,zillow"]),
    ("scrape:indeed01",  [sys.executable, "scraper.py", "--only", "jobspy", "--js-site", "indeed", "--js-terms", "0,1"]),
    ("scrape:indeed23",  [sys.executable, "scraper.py", "--only", "jobspy", "--js-site", "indeed", "--js-terms", "2,3"]),
    ("scrape:indeed4",   [sys.executable, "scraper.py", "--only", "jobspy", "--js-site", "indeed", "--js-terms", "4"]),
    ("scrape:linkedin0", [sys.executable, "scraper.py", "--only", "jobspy", "--js-site", "linkedin", "--js-terms", "0"]),
    ("scrape:linkedin2", [sys.executable, "scraper.py", "--only", "jobspy", "--js-site", "linkedin", "--js-terms", "2"]),
    ("scrape:icims",     [sys.executable, "scraper.py", "--only", "icims"]),
    ("scrape:eightfold", [sys.executable, "scraper.py", "--only", "eightfold"]),
    ("scrape:phenom",    [sys.executable, "scraper.py", "--only", "phenom"]),
    ("scrape:costco",    [sys.executable, "scraper.py", "--only", "costco"]),
    ("scrape:jibe",      [sys.executable, "scraper.py", "--only", "jibe"]),
    ("desc:ats",         [sys.executable, "rubric.py", "fetch", "--group", "ats"]),
    ("desc:workday",     [sys.executable, "rubric.py", "fetch", "--group", "workday"]),
    ("desc:amazon",      [sys.executable, "rubric.py", "fetch", "--group", "amazon"]),
    ("desc:indeed",      [sys.executable, "rubric.py", "fetch", "--group", "indeed"]),
    ("desc:microsoft",   [sys.executable, "rubric.py", "fetch", "--group", "microsoft"]),
    ("desc:icims",       [sys.executable, "rubric.py", "fetch", "--group", "icims"]),
    ("desc:eightfold",   [sys.executable, "rubric.py", "fetch", "--group", "eightfold"]),
    ("desc:phenom",      [sys.executable, "rubric.py", "fetch", "--group", "phenom"]),
    ("desc:costco",      [sys.executable, "rubric.py", "fetch", "--group", "costco"]),
    ("desc:jibe",        [sys.executable, "rubric.py", "fetch", "--group", "jibe"]),
    ("desc:builtin1",    [sys.executable, "rubric.py", "fetch", "--group", "builtin1"]),
    ("desc:builtin2",    [sys.executable, "rubric.py", "fetch", "--group", "builtin2"]),
    ("desc:yc",          [sys.executable, "rubric.py", "fetch", "--group", "yc"]),
    ("score-pre",        [sys.executable, "rubric.py", "score"]),
    ("resolve",          [sys.executable, "rubric.py", "resolve"]),
    ("score",            [sys.executable, "rubric.py", "score"]),
    ("build",            [sys.executable, "build_tracker.py"]),
]

def load():
    return json.load(open(STATE)) if os.path.exists(STATE) else {}

def save(st):
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump(st, open(STATE, "w"), indent=1)

def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "--next"
    st = load()
    if arg == "--reset":
        save({}); print("state reset"); return
    if arg == "--status":
        for name, _ in STEPS:
            print(f"  {st.get(name, {}).get('status', 'pending'):9s} {name}")
        return
    # --next
    for name, cmd in STEPS:
        if st.get(name, {}).get("status") == "done":
            continue
        t0 = time.time()
        print(f">>> {name}")
        p = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True, timeout=40)
        out = (p.stdout + p.stderr).strip()
        print(out[-1200:])
        ok = p.returncode == 0
        st[name] = {"status": "done" if ok else "failed", "secs": round(time.time() - t0), "rc": p.returncode}
        save(st)
        if name == "tests" and not ok:
            print("FIXTURES FAILED — sweep aborted per rubric gate."); sys.exit(2)
        remaining = [n for n, _ in STEPS if st.get(n, {}).get("status") != "done"]
        print(f"[{len(STEPS)-len(remaining)}/{len(STEPS)} done] next: {remaining[0] if remaining else 'DONE'}")
        return
    print("DONE — all steps complete.")

if __name__ == "__main__":
    main()
