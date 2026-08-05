"""Task 4.5: the copied-prod-DB validation harness.

A self-contained CLI that a low-capability runner agent executes MECHANICALLY
against a SANDBOX COPY of the production database, producing a JSON report plus
a human-readable log. The harness never concludes go/no-go for the 4.7 cutover;
it reports facts (durations, counts, diffs, status codes) for a human reviewer
to interpret. It is built and self-checked here against SYNTHETIC data only --
it never touches the real snapshot.

SAFETY GATE (non-negotiable): on startup, before any database is opened, the
harness verifies that `backend.config.DB_PATH` (i.e. the `JOBHUNT_DB` the
process was launched with) resolves to a path inside `--sandbox`. If it does
not, the harness refuses to run: clear stderr message, nonzero exit, nothing
opened. Every database file the harness opens, creates, copies, or migrates --
the working copies, the auto-backup files `db.init_db` creates next to them --
lives under `--sandbox`. It never opens the repo's `webapp/app.db`. Migrations
run ONLY on copies inside the sandbox; this is a standing project invariant
(see `plans/codebase-map.md`), and a violation here is a critical defect.

Environment fact this module is designed around: in this machine's shell
sandbox, SQLite cannot open database files located inside the project tree
(locking is denied, SQLITE_CANTOPEN). All paths under `--sandbox` work
normally. `--sandbox` MUST therefore be a directory outside the repo tree, and
`JOBHUNT_DB` MUST point somewhere inside it -- both are the runner's
responsibility (the RUNBOOK below sets them explicitly on every invocation).

USAGE
-----
    uv run --frozen python -m webapp.backend.validation45 \\
        --sandbox <dir> --snapshot <file> --out <report.json>

    uv run --frozen python -m webapp.backend.validation45 \\
        --self-check --sandbox <dir> --out <report.json>

`--snapshot` names a pristine copy of the production database at schema
version 4 (required unless `--self-check`, which builds its own synthetic v4
database inside the sandbox and ignores `--snapshot` if given). The harness
copies that snapshot within the sandbox once per phase that needs a fresh
start; the snapshot file itself is opened read-only at most, never migrated,
never written.

The JSON report is written to `--out`. A companion human-readable log is
written alongside it (same path with a `.log` suffix) and every log line is
also echoed to stdout as the harness runs, so a runner that only captures
stdout still gets the full narrative.

Report shape:
    {
      "phases": {"1_migrate": {...}, "2_state_preservation": {...}, ...},
      "environment": {"python_version": ..., "sqlite_version": ..., "timestamp": ...},
      "verdict_inputs": [{"check": str, "pass": bool, "detail": str}, ...]
    }

Exit codes: 0 = the harness ran to completion (individual checks may still
have failed -- read `verdict_inputs`, the harness does not decide go/no-go
for you). 2 = usage error or the safety gate refused to start. 1 = the
harness itself crashed partway through (a bug in the harness, or a database
too damaged to open at all) -- the report, if written, records the failure
under `verdict_inputs` too.

RUNBOOK (the exact commands the runner agent executes; every command below is
parameterized ONLY by <SANDBOX> -- a single directory outside the repo tree
that the runner creates once and reuses for every step, e.g.
`/Users/adamsantoyo/.claude/jobs/<job-id>/tmp/val45`. Every command below is
run with the repo root as the current working directory, so `uv run --frozen`
finds `pyproject.toml`/`uv.lock`. `PYTHONHASHSEED=0` (N10) makes dict/set
iteration order -- and therefore anything downstream that walks one without
sorting first -- identical across reruns, so two runs of this RUNBOOK against
the same snapshot produce the same diff counts. A FRESH sandbox directory per
invocation is still RECOMMENDED (it is simpler to reason about and leaves no
cleanup burden) but is no longer REQUIRED: N7 made every phase, and
`--self-check` in particular, safe to rerun against a sandbox that already
has a previous run's files in it.)

Step 0 -- self-check (run this FIRST, always, before touching the real
snapshot; if this does not come back all-green, stop and escalate rather than
proceeding to step 1):

    JOBHUNT_DB=<SANDBOX>/selfcheck/selfcheck-work.db \\
    JOBHUNT_SKIP_STARTUP_INGEST=1 \\
    PYTHONHASHSEED=0 \\
    uv run --frozen python -m webapp.backend.validation45 \\
      --self-check \\
      --sandbox <SANDBOX>/selfcheck \\
      --out <SANDBOX>/selfcheck/selfcheck-report.json

Step 1 -- the real validation run, against the copied production snapshot
(the runner places the pristine v4 snapshot at `<SANDBOX>/prod-snapshot.db`
before this step; the harness only ever reads it, and only ever writes copies
of it elsewhere under `<SANDBOX>`):

    JOBHUNT_DB=<SANDBOX>/work.db \\
    JOBHUNT_SKIP_STARTUP_INGEST=1 \\
    PYTHONHASHSEED=0 \\
    uv run --frozen python -m webapp.backend.validation45 \\
      --sandbox <SANDBOX> \\
      --snapshot <SANDBOX>/prod-snapshot.db \\
      --out <SANDBOX>/validation45-report.json

Step 2 -- hand `<SANDBOX>/validation45-report.json` (and its sibling `.log`)
to the human reviewer for the go/no-go call. Nothing further for the runner
to do; it must not delete `<SANDBOX>/prod-snapshot.db` or attempt to
interpret the report itself.
"""
