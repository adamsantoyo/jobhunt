"""Paths and constants for the JobHunt webapp backend.

Environment overrides (used by tests and throwaway smoke runs):
- JOBHUNT_ROOT: pipeline repo root (defaults to the real repo two levels up).
- JOBHUNT_DB:   sqlite database path (defaults to webapp/app.db).
- JOBHUNT_SKIP_STARTUP_INGEST=1: skip running ingest() on app startup.
- JOBHUNT_READS: "legacy" (default) or "canonical" -- task 4.6's temporary read
  flag. Selects, for the SAME /api/* read paths (jobs/followups/jobs/{id}/
  changes/analytics/freshness), between the existing materialized-table queries
  ("legacy") and `canonical_reads`'s canonical-schema queries ("canonical"),
  dispatched at the top of each router handler (see `read_dispatch.py`). Any
  other value is a configuration error, not a silent fallback, so it raises at
  import time here -- consistent with this module's own read-at-import style.

These are read at import time on purpose so a freshly-spawned process (a test, a
smoke script, the sweep runner's ingest thread) picks up the environment it was
launched with. Tests that need a custom RESULTS/ROOT monkeypatch the module
attributes directly and pass their own connection to ingest(). READS_SOURCE
dispatch tests instead monkeypatch `config.READS_SOURCE` directly at runtime
(see test_read_flag.py) since the env var itself is only consulted once, at
process boot -- monkeypatching os.environ after this module has already
imported has no effect.
"""
from pathlib import Path
import os

ROOT = Path(os.environ.get("JOBHUNT_ROOT", Path(__file__).resolve().parents[2]))
RESULTS = ROOT / "results"
DB_PATH = Path(os.environ.get("JOBHUNT_DB", Path(__file__).resolve().parents[1] / "app.db"))
PIPELINE_PY = ROOT / ".venv" / "bin" / "python"

# The webapp directory itself (independent of JOBHUNT_ROOT) — used to locate the
# built SPA in frontend/dist regardless of where the pipeline root is pointed.
WEBAPP_DIR = Path(__file__).resolve().parents[1]

CSRF_HEADER = "x-app"
CSRF_VALUE = "jobhunt"

STATUSES = ["New", "Interested", "Applied", "Phone screen", "Interview", "Offer", "Rejected", "Passed"]
# Statuses that represent real engagement with a role; a job in one of these
# still counts in the funnel even after it disappears from the latest run.
ADVANCED_STATUSES = STATUSES[2:]  # Applied .. Passed
# Statuses that are "active" for the purposes of overdue follow-up surfacing.
ACTIVE_STATUSES = ["Applied", "Phone screen", "Interview"]

# Generic placeholder; the real band lives in app_settings (seeded once, UI-editable).
DEFAULT_COMP_BAND = [80000, 160000]
DEFAULT_SKILLS = [
    "technical support", "product support", "customer support", "troubleshooting",
    "sql", "api", "networking",
]

# Goal settings (D4): also seeded once into app_settings, UI-editable via /api/config.
# Values here are only the fallback used until a row exists / when one is malformed.
DEFAULT_DAILY_QUEUE_SIZE = 10
DEFAULT_WEEKLY_APP_TARGET = 15
DEFAULT_DEADLINE = "2027-02-01"
DEFAULT_SNOOZE_DAYS = 3

SKIP_STARTUP_INGEST = os.environ.get("JOBHUNT_SKIP_STARTUP_INGEST") == "1"

# Task 4.6's temporary read flag -- see module docstring. Explicit opt-in only;
# an unrecognized value is a misconfiguration, never a silent legacy fallback.
_READS_SOURCE_CHOICES = ("legacy", "canonical")
READS_SOURCE = os.environ.get("JOBHUNT_READS", "legacy")
if READS_SOURCE not in _READS_SOURCE_CHOICES:
    raise RuntimeError(
        f"invalid JOBHUNT_READS={READS_SOURCE!r}: must be one of {_READS_SOURCE_CHOICES}"
    )
