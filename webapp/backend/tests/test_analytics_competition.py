"""Recommended-finding 3: `/api/analytics`'s competition axis.

`jobs.odds`/`job_history.odds` stores the combined "<match> / <competition>"
string (Phase 3.5); the dashboard only ever charts the competition half, folded
down to 3 buckets (`analytics._COMPETITION`). Two properties matter and neither
was covered before this file:

  PARSING. `_competition_of` must split a combined string on " / " and return
    the second half, return None for a legacy single-word value (no separator),
    and return None for a null odds value -- never raise, never guess.
  ACCUMULATION. The odds/matrix aggregations GROUP BY the full odds string, not
    the competition half, so two distinct combined labels that share one
    competition bucket ("Strong match / Standard", "Weak match / Standard")
    must SUM into that bucket rather than overwrite each other, and a legacy row
    (no " / ") must be dropped from both the flat distribution and the matrix,
    not silently mis-bucketed.

Follows `test_migrations.py`'s pattern for a throwaway on-disk DB (`db.connect` +
`db.init_db`) and `test_sweepstream.py`'s `TestClient(app)` pattern (no lifespan
context manager, so the startup ingest never runs). Never touches webapp/app.db.
"""
import sqlite3

import pytest

from backend import db
from backend.routers.analytics import _competition_of


# --------------------------------------------------------------------------- #
# _competition_of: pure parsing, no DB
# --------------------------------------------------------------------------- #
def test_competition_of_splits_a_combined_string():
    assert _competition_of("Strong match / High competition") == "High competition"
    assert _competition_of("Weak match / Lower bar") == "Lower bar"


def test_competition_of_returns_none_for_a_legacy_single_word_value():
    """Likely/Target/Reach predate the " / " combined format and have no
    separator -- excluded from the breakdown rather than mis-bucketed under a
    guessed column."""
    for legacy in ("Likely", "Target", "Reach"):
        assert _competition_of(legacy) is None


def test_competition_of_returns_none_for_null_odds():
    assert _competition_of(None) is None


# --------------------------------------------------------------------------- #
# The router: odds/matrix aggregation accumulates, and drops legacy rows
# --------------------------------------------------------------------------- #
@pytest.fixture()
def analytics_client(tmp_path):
    """A TestClient wired to a throwaway on-disk DB, never webapp/app.db."""
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend.db import get_db
    from backend.main import app

    db_path = tmp_path / "analytics_test.db"
    conn = db.connect(db_path)
    db.init_db(conn)

    def _override_get_db():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override_get_db
    try:
        # No `with TestClient(app)`, deliberately: that would run `lifespan` and
        # trigger the startup ingest against config.DB_PATH (see
        # test_sweepstream.py's csrf test for the same reasoning).
        yield TestClient(app), conn
    finally:
        app.dependency_overrides.pop(get_db, None)
        conn.close()


def _insert_job(conn, url, tier, odds):
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier, odds, present) VALUES (?, ?, ?, ?, 1)",
        (url, url, tier, odds),
    )


def test_odds_and_matrix_accumulate_by_parsed_competition_and_drop_legacy_rows(analytics_client):
    client, conn = analytics_client

    # Two distinct combined labels, same tier, same competition bucket ("Standard").
    _insert_job(conn, "https://x/1", 3, "Strong match / Standard")
    _insert_job(conn, "https://x/2", 3, "Weak match / Standard")
    # A third row in a different competition bucket, so the buckets are not
    # indistinguishable by coincidence.
    _insert_job(conn, "https://x/3", 4, "Strong match / Lower bar")
    # A legacy single-word row: must not land in ANY bucket.
    _insert_job(conn, "https://x/4", 3, "Likely")
    conn.commit()

    res = client.get("/api/analytics")
    assert res.status_code == 200
    body = res.json()

    # Flat distribution: the two "Standard" rows summed, not overwritten.
    assert body["odds"]["Standard"] == 2
    assert body["odds"]["Lower bar"] == 1
    assert body["odds"]["High competition"] == 0

    # The legacy row is excluded, not mis-bucketed: total across buckets is 3,
    # not 4 (there are 4 present jobs).
    assert sum(body["odds"].values()) == 3

    # Matrix: tier 3 accumulates both "Standard" rows under its own row.
    assert body["matrix"]["3"]["Standard"] == 2
    assert body["matrix"]["3"]["Lower bar"] == 0
    assert body["matrix"]["4"]["Lower bar"] == 1
    # The legacy row (tier 3, "Likely") must not have leaked into tier 3's total
    # by inflating any bucket beyond the two combined-string rows.
    assert sum(body["matrix"]["3"].values()) == 2
