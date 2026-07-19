# jobhunt

A no-frills job-scraping and scoring pipeline. It pulls listings straight from where jobs
originate — ATS platforms (Greenhouse, Lever, Ashby, SmartRecruiters, Workable, Recruitee),
Workday CXS, employer APIs (Microsoft Eightfold, Amazon), iCIMS/Phenom/Jibe portals, and
aggregators (Indeed/LinkedIn via [JobSpy](https://github.com/speedyapply/JobSpy)) — then scores
every listing against a personal fit rubric and emits a filterable Excel tracker.

Two scoring axes, kept orthogonal:

- **Tier (1–5)** — *fit*: how well the role matches your experience, level, domain, comp, and location.
- **Odds (Likely / Target / Reach)** — *hireability*: seniority gap, applicant-pool size, résumé↔JD
  keyword overlap, employment-type bar, and comp expectation. A heuristic, not a prediction.

## Architecture

```
config.json          your profile (search terms, target metros) + company→ATS registry
scraper.py           all sources → results/raw.jsonl (resumable cache) + source_health.json
rubric.py            description fetchers + the scoring engine → jobs_scored_<date>.csv + run_report.json
llm_review.py        LLM judgment pass over borderline tiers (run after a sweep) → picks_llm.json
sweep.py             orchestrator: one step per --next call (tests gate → scrape → desc → score → build)
build_tracker.py     Excel tracker; Status/Notes columns survive rebuilds (keyed by apply URL)
discover.py          auto-locates a company's ATS (probes ATS APIs + Workday tenants)
tests.py             regression fixtures for the rubric engine (run before every sweep)
```

## Setup

```bash
python3 -m venv .venv
./.venv/bin/pip install python-jobspy openpyxl requests
# optional: the LLM judgment pass (llm_review.py) uses the Anthropic SDK when
# ANTHROPIC_API_KEY is set, and otherwise falls back to the local `claude` CLI:
./.venv/bin/pip install anthropic
cp config.example.json config.json      # then edit the "profile" block for yourself
```

Write your own `RUBRIC.md` (the scoring standard the LLM pass reads) describing your
experience, level, domain, comp, and location preferences.

## Run a sweep

```bash
./.venv/bin/python sweep.py --next     # repeat until DONE (tests gate → scrape → desc → score → build)
./.venv/bin/python sweep.py --status   # progress
./.venv/bin/python sweep.py --reset    # fresh run (keeps the seen.jsonl ledger)
```

Then, optionally, the LLM judgment pass over borderline tiers:

```bash
./.venv/bin/python llm_review.py       # needs ANTHROPIC_API_KEY, or falls back to the `claude` CLI
./.venv/bin/python build_tracker.py    # rebuild with adjusted tiers
```

## Notes

- Dice and ZipRecruiter don't scrape reliably from a sandbox; pull them via their MCP
  connectors and fold the rows in with a small integration script.
- The location gate keeps only your configured metro (on-site) plus US-remote; everything
  else is excluded. Adjust `profile.bay_area` and the gate in `rubric.score_row` for your area.
- `config.json`, `RUBRIC.md`, and everything under `results/` are personal and git-ignored.
