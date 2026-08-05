"""Phase 4, task 4.6, matrix item (e): an invalid `JOBHUNT_READS` value must
fail at import time, not lazily on first request.

`config.py` reads `JOBHUNT_READS` at module import (same style as every other
env var it reads -- see its module docstring), so the only way to observe the
failure honestly is a fresh process: monkeypatching `os.environ` in-process
after `backend.config` has already imported would prove nothing (the module is
already loaded and cached in `sys.modules`). `webapp` is added to `PYTHONPATH`
to match this repo's own `pyproject.toml` (`[tool.pytest.ini_options]
pythonpath = ["webapp"]`), and `JOBHUNT_DB` is pointed at a tmp path so the
subprocess can never reach the real webapp/app.db even though it runs outside
this session's own env-var fence (conftest.py's fence is a pytest fixture, not
inherited by a child process).
"""
import subprocess
import sys
from pathlib import Path

import pytest

WEBAPP_DIR = Path(__file__).resolve().parents[2]


def _run_import(tmp_path, reads_value):
    env = {
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(WEBAPP_DIR),
        "JOBHUNT_DB": str(tmp_path / "must-not-be-real.db"),
        "JOBHUNT_SKIP_STARTUP_INGEST": "1",
    }
    if reads_value is not None:
        env["JOBHUNT_READS"] = reads_value
    return subprocess.run(
        [sys.executable, "-c", "import backend.config"],
        env=env, capture_output=True, text=True, timeout=30,
    )


def test_invalid_jobhunt_reads_fails_at_import(tmp_path):
    proc = _run_import(tmp_path, "bogus")
    assert proc.returncode != 0
    assert "JOBHUNT_READS" in proc.stderr
    assert "bogus" in proc.stderr


def test_missing_jobhunt_reads_defaults_to_legacy(tmp_path):
    proc = _run_import(tmp_path, None)
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("value", ["legacy", "canonical"])
def test_valid_jobhunt_reads_values_import_cleanly(tmp_path, value):
    proc = _run_import(tmp_path, value)
    assert proc.returncode == 0, proc.stderr
