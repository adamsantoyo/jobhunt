#!/usr/bin/env python3
"""Sweep orchestrator. Each shell call runs `python3 sweep.py --next` until DONE.
Designed for ~45s execution windows: one chunk per call, state persisted between calls.
  --next        run the next pending step (fixture gate runs first, aborts sweep on failure)
  --status      show step states
  --reset       clear state for a fresh weekly run (keeps seen.jsonl ledger)
  --skip NAME   mark a persistently-stuck step done so --next moves past it
"""
import json, os, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "results", "sweep_state.json")
STEP_TIMEOUT = 25

def _py(*a):
    return [sys.executable, *a]

def _chunks(seq, n):
    return [seq[i:i + n] for i in range(0, len(seq), n)]

def _dynamic_scrape_steps():
    """Derive jobspy term-index and Workday tenant coverage from config so no search
    term or tenant is ever silently skipped (was hard-coded, diverged from config)."""
    try:
        cfg = json.load(open(os.path.join(HERE, "config.json")))
    except Exception:
        cfg = {"profile": {"search_terms": []}, "companies": {}}
    n_terms = len(cfg.get("profile", {}).get("search_terms", []))
    idx = list(range(n_terms))
    wd = list(cfg.get("companies", {}).get("workday", {}).keys())
    steps = []
    for ch in _chunks(wd, 3):
        steps.append((f"scrape:wd_{ch[0]}", _py("scraper.py", "--only", "workday", "--wd-tenant", ",".join(ch))))
    for ch in _chunks(idx, 2):  # Indeed: cover EVERY term
        steps.append((f"scrape:indeed_{'_'.join(map(str, ch))}",
                      _py("scraper.py", "--only", "jobspy", "--js-site", "indeed", "--js-terms", ",".join(map(str, ch)))))
    for ch in _chunks(idx, 2):  # LinkedIn: best-effort (rate-limits), but still cover every term
        steps.append((f"scrape:linkedin_{'_'.join(map(str, ch))}",
                      _py("scraper.py", "--only", "jobspy", "--js-site", "linkedin", "--js-terms", ",".join(map(str, ch)))))
    return steps

STEPS = [
    ("tests",            _py("tests.py")),
    ("scrape:ats",       _py("scraper.py", "--only", "ats")),
    ("scrape:microsoft", _py("scraper.py", "--only", "microsoft")),
    ("scrape:amazon",    _py("scraper.py", "--only", "amazon")),
    ("scrape:yc",        _py("scraper.py", "--only", "yc")),
    ("scrape:builtin",   _py("scraper.py", "--only", "builtin")),
    *_dynamic_scrape_steps(),
    ("scrape:icims",     _py("scraper.py", "--only", "icims")),
    ("scrape:eightfold", _py("scraper.py", "--only", "eightfold")),
    ("scrape:phenom",    _py("scraper.py", "--only", "phenom")),
    ("scrape:costco",    _py("scraper.py", "--only", "costco")),
    ("scrape:jibe",      _py("scraper.py", "--only", "jibe")),
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
    if not os.path.exists(STATE):
        return {}
    with open(STATE) as f:
        return json.load(f)

def save(st):
    state_dir = os.path.dirname(STATE)
    os.makedirs(state_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".sweep_state.", suffix=".tmp", dir=state_dir)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(st, f, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, STATE)
        dir_fd = os.open(state_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "--next"
    st = load()
    if arg == "--reset":
        save({}); print("state reset"); return
    if arg == "--status":
        for name, _ in STEPS:
            print(f"  {st.get(name, {}).get('status', 'pending'):9s} {name}")
        return
    if arg == "--skip":
        name = sys.argv[2] if len(sys.argv) > 2 else ""
        if name not in {n for n, _ in STEPS}:
            print(f"unknown step: {name}"); sys.exit(1)
        st[name] = {**st.get(name, {}), "status": "done", "skipped": True, "rc": None}
        save(st); print(f"skipped {name}"); return
    # --next
    for name, cmd in STEPS:
        if st.get(name, {}).get("status") == "done":
            continue
        started_at = time.time()
        print(f">>> {name}")
        timed_out = False
        try:
            p = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True, timeout=STEP_TIMEOUT)
            out = (p.stdout + p.stderr).strip()
            rc = p.returncode
        except subprocess.TimeoutExpired as e:
            timed_out = True
            partial = "".join(s for s in (e.stdout, e.stderr) if isinstance(s, str))
            out = (partial.strip() + f"\n[timeout] step exceeded the {STEP_TIMEOUT}s window; marked failed, retried next call").strip()
            rc = -9
        print(out[-1200:])
        ok = rc == 0
        finished_at = time.time()
        previous = st.get(name, {})
        attempts = list(previous.get("attempts") or [])
        attempts.append({
            "attempt": len(attempts) + 1,
            "started_at": started_at,
            "finished_at": finished_at,
            "secs": round(finished_at - started_at, 3),
            "rc": rc,
            "timed_out": timed_out,
        })
        st[name] = {
            **previous,
            "status": "done" if ok else "failed",
            "secs": round(finished_at - started_at),
            "rc": rc,
            "attempts": attempts,
        }
        save(st)
        if name == "tests" and not ok:
            print("FIXTURES FAILED — sweep aborted per rubric gate."); sys.exit(2)
        remaining = [n for n, _ in STEPS if st.get(n, {}).get("status") != "done"]
        print(f"[{len(STEPS)-len(remaining)}/{len(STEPS)} done] next: {remaining[0] if remaining else 'DONE'}")
        return
    print("DONE — all steps complete.")

if __name__ == "__main__":
    main()
