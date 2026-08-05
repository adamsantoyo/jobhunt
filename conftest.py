"""Repo-wide test fences.

The only thing here is a structural guarantee that the suite cannot touch the
user's live database. Nothing in `backend.sources` can reach it by construction —
every function takes an explicit connection and every test passes a `tmp_path`
factory — but the rest of the backend resolves `config.DB_PATH` at import time,
and `db.connect()` with no argument opens exactly that file. One test that forgets
its own path, one helper that calls `connect()` bare, and the suite is writing to
`webapp/app.db`: the file that holds the user's job history, notes, and statuses.

So the path is pointed at a per-session temporary directory before anything is
imported that might read it, and the real file is then unreachable by name rather
than merely unused by convention.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

#: Set before collection so that modules reading `JOBHUNT_DB` at import time — which
#: `backend.config` does, deliberately — never see the real path at all. A session
#: fixture would be too late for them.
_FENCE_DIR = Path(tempfile.mkdtemp(prefix="jobhunt-test-db-"))
_FENCE_DB = _FENCE_DIR / "must-not-be-the-real-app.db"
os.environ["JOBHUNT_DB"] = str(_FENCE_DB)

#: Pinned for the same reason, before collection, beside the `JOBHUNT_DB` fence
#: above: `backend.config` also reads `JOBHUNT_READS` at import time (4.6's read
#: flag, `{legacy, canonical}`, default `legacy`), and the suite's tests are
#: written and pinned against `legacy` behaviour explicitly (spec decision 9:
#: "Flag=legacy must remain byte-identical legacy behaviour (test-pinned)"). A
#: developer's shell exporting `JOBHUNT_READS=canonical` for their own manual
#: testing must not change what the suite means — a real repro: the suite had
#: exactly one failure under `JOBHUNT_READS=canonical` before this pin existed.
#: Pinning here, not merely relying on the default, makes the suite's behaviour
#: independent of whatever is in the developer's environment.
os.environ["JOBHUNT_READS"] = "legacy"


@pytest.fixture(scope="session", autouse=True)
def fence_the_live_database():
    """Keep `config.DB_PATH` pointed away from `webapp/app.db` for the whole session.

    Belt and braces with the environment variable above: a test that reloads or
    monkeypatches `backend.config` still ends up fenced, and the assertion below
    fails loudly the moment the default path leaks back in.
    """
    try:
        from backend import config
    except ImportError:  # pragma: no cover - suites that never import the backend
        yield
        return

    live = Path(__file__).resolve().parent / "webapp" / "app.db"
    config.DB_PATH = _FENCE_DB
    assert Path(config.DB_PATH).resolve() != live.resolve(), (
        "the test session is pointed at the live app.db"
    )
    yield
    assert Path(config.DB_PATH).resolve() != live.resolve(), (
        "a test repointed config.DB_PATH at the live app.db"
    )
