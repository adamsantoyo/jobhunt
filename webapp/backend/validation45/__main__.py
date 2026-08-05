"""CLI entry point: `uv run --frozen python -m webapp.backend.validation45 ...`.

Import discipline (this is why this file has almost no imports at module
level, unlike everything else in the codebase): the rest of the codebase
imports the backend package as the top-level name `backend` (pytest's
`pythonpath = ["webapp"]`, and `webapp/run.sh`'s `cd webapp && python -m
uvicorn backend.main:app`). This module is loaded as `webapp.backend.
validation45.__main__` (the contract's required invocation spelling), which
is a DIFFERENT module identity than `backend.validation45.__main__` would be.
If this process were to import both `webapp.backend.config` (by relative
import, following its own dotted name) AND `backend.config` (absolute, the
spelling every test fixture and fake this harness reuses is written against),
Python would load the backend package TWICE under two names, each with its
own independent copy of every module-level singleton -- `config.DB_PATH`
included, which is exactly the value the safety gate below depends on. Two
copies of that value silently diverging is precisely the kind of bug this
harness exists to rule out, not commit.

So: this file prepends `webapp/` to `sys.path` itself (mirroring pytest's
`pythonpath` setting) and, from that point on, EVERY import anywhere in this
package -- here and in `util.py`/`phases.py` -- is spelled `backend.*`,
never `webapp.backend.*` or a relative `from . import`. That keeps exactly
one copy of the backend package loaded for the life of the process, the same
one `backend.tests.*`'s fakes and fixtures already assume.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
WEBAPP_DIR = _THIS_FILE.parents[2]  # .../webapp
REPO_ROOT = _THIS_FILE.parents[3]  # repo root


def _bootstrap_sys_path() -> None:
    webapp_dir = str(WEBAPP_DIR)
    if webapp_dir not in sys.path:
        sys.path.insert(0, webapp_dir)


def _parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="python -m webapp.backend.validation45",
        description="Task 4.5 copied-prod-DB validation harness. See the package "
                     "docstring (webapp/backend/validation45/__init__.py) for the RUNBOOK.",
    )
    parser.add_argument("--sandbox", required=True,
                         help="Directory OUTSIDE the repo tree that every database file "
                              "this harness touches must live under.")
    parser.add_argument("--out", required=True,
                         help="Path to write the JSON report to (a sibling .log file is "
                              "written alongside it).")
    parser.add_argument("--snapshot",
                         help="Pristine copy of the production DB at schema v4, required "
                              "unless --self-check. Must resolve inside --sandbox.")
    parser.add_argument("--self-check", action="store_true",
                         help="Build a synthetic v4 database inside the sandbox and run "
                              "every phase against it instead of a real snapshot.")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    sandbox = Path(args.sandbox).resolve()
    sandbox.mkdir(parents=True, exist_ok=True)

    # --- SAFETY GATE, before anything else is imported from backend.* ------
    _bootstrap_sys_path()
    from backend import config as backend_config  # noqa: E402 - gate must run first

    db_path = Path(backend_config.DB_PATH).resolve()
    try:
        db_path.relative_to(sandbox)
    except ValueError:
        print(
            f"SAFETY GATE FAILED: JOBHUNT_DB={db_path} does not resolve inside "
            f"--sandbox {sandbox}. Refusing to open any database. Set JOBHUNT_DB to a "
            f"path under the sandbox before invoking this module (see the RUNBOOK in "
            f"webapp/backend/validation45/__init__.py).",
            file=sys.stderr,
        )
        return 2

    # N6: the harness exists to observe TRUE legacy behavior on a copied prod
    # DB -- if either flag is flipped to "canonical" (an exported shell var
    # leaking in, e.g. the JOBHUNT_READS shell-leak lesson from 4.6/4.7),
    # every downstream "legacy" comparison would silently be comparing
    # canonical against canonical. Refuse loudly instead of failing later, by
    # accident, deep in phase 3 or 4.
    if backend_config.READS_SOURCE != "legacy":
        print(
            f"SAFETY GATE FAILED: config.READS_SOURCE={backend_config.READS_SOURCE!r} "
            f"is not 'legacy' (JOBHUNT_READS leaked in from the shell?). The harness "
            f"must observe true legacy read behavior; refusing to start. Unset "
            f"JOBHUNT_READS or set it to 'legacy' before invoking this module.",
            file=sys.stderr,
        )
        return 2
    if backend_config.WRITES_SOURCE != "legacy":
        print(
            f"SAFETY GATE FAILED: config.WRITES_SOURCE={backend_config.WRITES_SOURCE!r} "
            f"is not 'legacy' (JOBHUNT_WRITES leaked in from the shell?). Refusing to "
            f"start. Unset JOBHUNT_WRITES or set it to 'legacy' before invoking this "
            f"module.",
            file=sys.stderr,
        )
        return 2
    # --- gate passed: everything from here on is confirmed sandbox-scoped --

    from .util import Report
    from .phases import build_selfcheck_snapshot, run_all_phases

    out_path = Path(args.out).resolve()
    report = Report(out_path)

    import sqlite3
    from datetime import datetime, timezone

    report.environment = {
        "python_version": sys.version,
        "sqlite_version": sqlite3.sqlite_version,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    report.log(f"validation45 starting: sandbox={sandbox} out={out_path} "
               f"self_check={args.self_check}")
    report.log(f"safety gate passed: JOBHUNT_DB={db_path} is inside sandbox {sandbox}")
    report.check("safety_gate_db_path_inside_sandbox", True, f"{db_path} inside {sandbox}",
                 informational=True)
    report.check(
        "safety_gate_reads_writes_legacy", True,
        f"READS_SOURCE={backend_config.READS_SOURCE!r} WRITES_SOURCE={backend_config.WRITES_SOURCE!r}",
        informational=True,
    )

    if not args.self_check and not args.snapshot:
        report.log("ERROR: --snapshot is required unless --self-check is given")
        report.check("harness_completed_without_fatal_error", False, "missing --snapshot")
        report.write()
        return 2

    try:
        if args.self_check:
            snapshot_path = build_selfcheck_snapshot(sandbox, report)
        else:
            from .util import require_within

            snapshot_path = require_within(args.snapshot, sandbox, "snapshot")
        run_all_phases(report, sandbox, snapshot_path, self_check=args.self_check)
    except Exception as exc:  # noqa: BLE001 - a crash must still produce a report
        report.log(f"FATAL: harness crashed: {exc!r}")
        report.check("harness_completed_without_fatal_error", False, repr(exc))
        report.write()
        raise
    else:
        report.check("harness_completed_without_fatal_error", True, "")

    report.write()
    # N8: split the tally so a reader cannot mistake a measurement-only
    # record (a timing, the safety-gate facts) for an asserted invariant that
    # happened to pass -- see Report.check's `informational` doc.
    informational = [c for c in report.verdict_inputs if c["informational"]]
    asserted = [c for c in report.verdict_inputs if not c["informational"]]
    asserted_passed = sum(1 for c in asserted if c["pass"])
    asserted_failed = len(asserted) - asserted_passed
    report.log(
        f"DONE: {asserted_passed} asserted passed, {len(informational)} informational "
        f"recorded, {asserted_failed} failed. Report: {out_path}  Log: {report.log_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
