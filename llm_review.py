#!/usr/bin/env python3
"""LLM judgment pass per RUBRIC.md tier-5 standard: the machine rubric proposes,
an LLM pass over the full description confirms (or downgrades).

Reads the latest jobs_scored CSV + cached descriptions, sends the borderline set
(tier5-proposed, tier 4, tier 3 with a description) to Claude in batches, then:
  - writes results/llm_review.jsonl        {url, tier, why, confidence}  (append cache)
  - writes picks_llm.json                  LLM-confirmed tier-5s (rubric.load_picks merges
                                           them; human picks.json wins on collision)
  - rewrites the CSV in place              tiers adjusted, why/flags annotated

Run AFTER a sweep completes (not a sweep step — batches take minutes):
  python3 llm_review.py            # review up to --limit rows, apply results
  python3 llm_review.py --limit 40
Then rebuild: python3 build_tracker.py  (and rescore next sweep to elevate picks)

Auth: Anthropic SDK credentials (ANTHROPIC_API_KEY or `ant auth login` profile);
falls back to the local `claude` CLI in headless mode when no SDK credentials.
Model: claude-sonnet-5 (override with JOBHUNT_LLM_MODEL).
"""
import argparse, csv, glob, json, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REVIEW = os.path.join(HERE, "results", "llm_review.jsonl")
PICKS_LLM = os.path.join(HERE, "picks_llm.json")
MODEL = os.environ.get("JOBHUNT_LLM_MODEL", "claude-sonnet-5")
BATCH = 8

SCHEMA = {
    "type": "object",
    "properties": {
        "verdicts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "tier": {"type": "integer"},
                    "why": {"type": "string"},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                },
                "required": ["url", "tier", "why", "confidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["verdicts"],
    "additionalProperties": False,
}


def load_rubric_text():
    with open(os.path.join(HERE, "RUBRIC.md")) as f:
        return f.read()


def latest_scored():
    paths = sorted(glob.glob(os.path.join(HERE, "results", "jobs_scored_*.csv")))
    if not paths:
        sys.exit("no jobs_scored CSV — run the sweep first")
    return paths[-1]


def load_reviewed():
    done = {}
    if os.path.exists(REVIEW):
        with open(REVIEW) as f:
            for l in f:
                if l.strip():
                    j = json.loads(l)
                    done[j["url"]] = j
    return done


def load_descs():
    d = {}
    p = os.path.join(HERE, "results", "descriptions.jsonl")
    if os.path.exists(p):
        with open(p) as f:
            for l in f:
                if l.strip():
                    j = json.loads(l)
                    d[j["url"]] = j["desc"]
    return d


def pick_targets(rows, done, descs, limit):
    """Borderline set, priority order: tier5-proposed, tier 4, tier 3 with desc."""
    def has_desc(r):
        return bool(r.get("desc_snippet")) or bool(descs.get(r["url"]))
    prio = []
    prio += [r for r in rows if "tier5-proposed" in r["flags"] and r["url"] not in done]
    prio += [r for r in rows if r["tier"] == "4" and "tier5-proposed" not in r["flags"]
             and r["url"] not in done and has_desc(r)]
    prio += [r for r in rows if r["tier"] == "3" and r["url"] not in done and has_desc(r)]
    return prio[:limit]


def build_prompt(rubric_text, batch, descs):
    jobs = []
    for r in batch:
        desc = descs.get(r["url"]) or r.get("desc_snippet") or ""
        jobs.append({
            "url": r["url"], "title": r["title"], "company": r["company"],
            "location": r["location"], "salary": r.get("salary", ""),
            "posted": r.get("posted", ""), "machine_tier": int(r["tier"]),
            "flags": r["flags"], "description": desc[:2500],
        })
    return f"""You are the judgment pass for a job-search rubric. Score each job below for THIS candidate, strictly per the rubric. The machine keyword-scorer already produced machine_tier; your job is to confirm or correct it by actually reading the description.

<rubric>
{rubric_text}
</rubric>

Rules:
- tier is 1-5 per the rubric's tier mapping. Assign 5 ONLY when the description clearly satisfies the tier-5 bar (function 3, right level, domain overlap, no blockers, comp workable) — tier 5 means "apply today".
- "why" must be ONE line naming the candidate's specific matching experience (e.g. "Intune/Entra endpoint work = your prior IAM/endpoint role"). For downgrades, one line naming the disqualifier instead.
- Respect blockers absolutely: people management, 8+ years required, active clearance held, EE design skills, non-US.
- confidence: high only if the description gave you enough to be sure.

Jobs:
{json.dumps(jobs, indent=1)}

Return a verdict for every job, keyed by its exact url."""


def call_sdk(prompt):
    import anthropic
    client = anthropic.Anthropic()
    response = client.messages.create(
        model=MODEL,
        max_tokens=16000,
        output_config={"format": {"type": "json_schema", "schema": SCHEMA}},
        messages=[{"role": "user", "content": prompt}],
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("model refused the request")
    text = next(b.text for b in response.content if b.type == "text")
    return json.loads(text)["verdicts"]


def call_cli(prompt):
    """Headless `claude -p` fallback for machines authenticated via Claude Code."""
    p = subprocess.run(
        ["claude", "-p", prompt + '\n\nRespond with ONLY a JSON object: {"verdicts": [...]} — no prose, no code fences.'],
        capture_output=True, text=True, timeout=600)
    if p.returncode != 0:
        raise RuntimeError(f"claude CLI failed: {p.stderr[:200]}")
    m = re.search(r"\{.*\}", p.stdout, re.S)
    if not m:
        raise RuntimeError(f"no JSON in CLI output: {p.stdout[:200]}")
    return json.loads(m.group(0))["verdicts"]


def review(prompt):
    try:
        return call_sdk(prompt)
    except ImportError:
        pass
    except Exception as e:
        if "auth" not in str(e).lower() and "api_key" not in str(e).lower() and "credential" not in str(e).lower():
            raise
    return call_cli(prompt)


def apply_verdicts(csv_path, rows, done):
    """Rewrite the scored CSV with LLM adjustments; emit picks_llm.json."""
    llm_picks, changed = [], 0
    for r in rows:
        v = done.get(r["url"])
        if not v:
            continue
        cur = int(r["tier"])
        flags = [f.strip() for f in r["flags"].split(",") if f.strip()]
        if "llm-reviewed" not in flags:
            flags.append("llm-reviewed")
        if v["tier"] >= 5 and v["confidence"] == "high":
            # the pick is recorded below, so the gate is satisfied — elevate now
            # so the rebuilt tracker shows it (score runs keep it 5 via picks_llm.json)
            flags.append("llm-5-confirmed")
            r["tier"] = "5"
            llm_picks.append({"company": r["company"], "title": r["title"],
                              "reason": v["why"], "url": r["url"], "source": "llm"})
        elif v["tier"] < cur:
            flags.append("llm-downgraded")
            r["tier"] = str(max(1, v["tier"]))
        elif v["tier"] == 4 and cur == 3:
            r["tier"] = "4"  # LLM read the description, so the rule-zero bar is met
        r["why"] = v["why"] or r["why"]
        r["flags"] = ", ".join(dict.fromkeys(flags))
        changed += 1
    rows.sort(key=lambda x: (-int(x["tier"]), x["company"], x["title"]))
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)
    if llm_picks:
        existing = []
        if os.path.exists(PICKS_LLM):
            with open(PICKS_LLM) as f:
                existing = json.load(f)
        urls = {p["url"] for p in existing}
        existing += [p for p in llm_picks if p["url"] not in urls]
        with open(PICKS_LLM, "w") as f:
            json.dump(existing, f, indent=1)
    return changed, len(llm_picks)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=80, help="max rows to review this run")
    ap.add_argument("--apply-only", action="store_true", help="re-apply cached verdicts, no API calls")
    a = ap.parse_args()

    csv_path = latest_scored()
    with open(csv_path) as f:
        rows = list(csv.DictReader(f))
    done = load_reviewed()
    descs = load_descs()

    if not a.apply_only:
        targets = pick_targets(rows, done, descs, a.limit)
        print(f"{len(targets)} rows to review ({len(done)} cached) via {MODEL}")
        rubric_text = load_rubric_text()
        for i in range(0, len(targets), BATCH):
            batch = targets[i:i + BATCH]
            try:
                verdicts = review(build_prompt(rubric_text, batch, descs))
            except Exception as e:
                print(f"batch {i//BATCH}: FAILED {str(e)[:120]}", file=sys.stderr)
                continue
            with open(REVIEW, "a") as f:
                for v in verdicts:
                    if v.get("url"):
                        done[v["url"]] = v
                        f.write(json.dumps(v) + "\n")
            print(f"batch {i//BATCH + 1}/{(len(targets)+BATCH-1)//BATCH}: {len(verdicts)} verdicts")

    changed, n_picks = apply_verdicts(csv_path, rows, done)
    print(f"applied {changed} verdicts to {os.path.basename(csv_path)}; "
          f"{n_picks} new LLM-confirmed picks -> picks_llm.json")
    print("next: python3 build_tracker.py  (rescore on next sweep elevates confirmed picks to tier 5)")


if __name__ == "__main__":
    main()
