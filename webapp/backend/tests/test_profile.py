"""Phase 3.4: exercise `candidate_profile.build_profile_version_row()` against
a real `profile_versions` table.

`candidate_profile.py` lives at the repo root (stdlib-only: rubric.py runs
outside the webapp's dependency environment as a bare `uv run` subprocess),
so it is not importable via the `backend.*` package this test tree normally
uses. This file adds the repo root to `sys.path` itself rather than depending
on `rubric.py`'s import-time `sys.path.insert`, since nothing here imports
`rubric`.

`build_profile_version_row()` is a pure function -- Phase 3.4 does not wire
any runtime DB write into the scoring pipeline (that is Phase 3.3's call).
This test only proves the row it returns is shaped correctly for
`profile_versions` and that its deterministic id/hash make repeated inserts
idempotent, exactly as `profile_versions.content_hash UNIQUE` requires.
Never touches webapp/app.db: every connection here is a tmp_path fixture,
same as every other test in this tree (see root conftest.py's fence).
"""
import json
import os
import sys

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import candidate_profile  # noqa: E402  (path insert must precede this)

from backend.db import connect, init_db  # noqa: E402


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "profile-test.db"


@pytest.fixture
def conn(db_path):
    c = connect(db_path)
    init_db(c)
    yield c
    c.close()


@pytest.fixture
def profile_doc():
    with open(os.path.join(_REPO_ROOT, "profile.json")) as f:
        return json.load(f)


def _insert(conn, row):
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions "
        "(profile_version_id, content_hash, profile_json, rubric_hash, created_at) "
        "VALUES (:profile_version_id, :content_hash, :profile_json, :rubric_hash, :created_at)",
        row,
    )
    conn.commit()


def test_build_profile_version_row_round_trips_through_the_real_table(conn, profile_doc):
    row = candidate_profile.build_profile_version_row(profile_doc)
    _insert(conn, row)

    stored = conn.execute(
        "SELECT * FROM profile_versions WHERE profile_version_id = ?", (row["profile_version_id"],)
    ).fetchone()
    assert stored is not None
    assert stored["content_hash"] == row["content_hash"]
    # profile identity is candidate DATA ONLY -- scorer identity lives in
    # score_versions.scorer_hash (NOT NULL there), not here.
    assert stored["rubric_hash"] is None
    assert json.loads(stored["profile_json"]) == json.loads(row["profile_json"])
    # profile_json round-trips to the same canonical document the row was built from
    assert json.loads(stored["profile_json"]) == json.loads(
        json.dumps(profile_doc, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    )


def test_repeated_calls_are_idempotent_against_the_unique_constraint(conn, profile_doc):
    """profile_versions.content_hash is UNIQUE. build_profile_version_row() must
    derive the same profile_version_id for the same content every time, so a
    second sweep over an unchanged profile is an INSERT OR IGNORE no-op, not a
    UNIQUE-constraint violation."""
    row1 = candidate_profile.build_profile_version_row(profile_doc)
    row2 = candidate_profile.build_profile_version_row(profile_doc)
    assert row1["profile_version_id"] == row2["profile_version_id"]
    assert row1["content_hash"] == row2["content_hash"]

    _insert(conn, row1)
    _insert(conn, row2)  # must not raise IntegrityError

    count = conn.execute(
        "SELECT COUNT(*) AS n FROM profile_versions WHERE content_hash = ?", (row1["content_hash"],)
    ).fetchone()["n"]
    assert count == 1


def test_a_real_edit_produces_a_different_row(conn, profile_doc):
    edited = json.loads(json.dumps(profile_doc))
    edited["comp"]["band_low"] = profile_doc["comp"]["band_low"] + 1

    row_a = candidate_profile.build_profile_version_row(profile_doc)
    row_b = candidate_profile.build_profile_version_row(edited)
    assert row_a["profile_version_id"] != row_b["profile_version_id"]
    assert row_a["content_hash"] != row_b["content_hash"]

    _insert(conn, row_a)
    _insert(conn, row_b)
    count = conn.execute("SELECT COUNT(*) AS n FROM profile_versions").fetchone()["n"]
    assert count == 2


def test_explicit_profile_version_id_is_honored(conn, profile_doc):
    row = candidate_profile.build_profile_version_row(
        profile_doc, profile_version_id="fixed-id-123", created_at="2026-01-01T00:00:00+00:00"
    )
    assert row["profile_version_id"] == "fixed-id-123"
    assert row["created_at"] == "2026-01-01T00:00:00+00:00"
    _insert(conn, row)
    stored = conn.execute(
        "SELECT * FROM profile_versions WHERE profile_version_id = 'fixed-id-123'"
    ).fetchone()
    assert stored is not None


def test_loaded_profile_matches_the_on_disk_document(profile_doc):
    """Sanity check that the repo's tracked profile.json is itself valid --
    load_profile() must not raise, and its content_hash must match hashing
    the raw document directly."""
    prof = candidate_profile.load_profile(os.path.join(_REPO_ROOT, "profile.json"))
    assert prof.content_hash == candidate_profile.profile_content_hash(profile_doc)
    # Bumped 1 -> 2 by Phase 3.3: four dead Seattle-era location fields removed,
    # and config.json's profile.bay_area / profile.title_exclude folded in as
    # location.bay_area_cities / exclusions.title_exclude. Bumped 2 -> 3 by
    # Phase 3.5: hireability_labels removed (labels are now derived from the
    # feature vector, not a score-vs-threshold comparison). Pinned deliberately
    # -- a schema bump must be a decision, not a diff nobody noticed.
    assert prof.schema_version == candidate_profile.SCHEMA_VERSION == 3
