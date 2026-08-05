"""Phase 5, W-5.2: contract pins binding on the future 5.3 consumer.

These pins are deliberately blunt and deliberately narrow: they exist to keep
`outcomes.py` / migration 21 from drifting out from under whatever 5.3 builds
on top, not to exercise enrichment logic (see test_outcomes.py for that) or
HTTP wiring (see test_outcomes_api.py). Every database here lives under
tmp_path (repo-root conftest.py additionally fences JOBHUNT_DB), so nothing
here can reach webapp/app.db.
"""
import sqlite3

import pytest

import backend.migrations as migrations_mod
import backend.outcomes as outcomes
from backend.db import connect, init_db
from backend.migrations import MIGRATIONS, run_migrations

AT = "2026-08-01T12:00:00"

EXPECTED_COLUMNS = {
    "recommendation_snapshots": {
        "snapshot_id", "surface", "captured_at", "profile_version_id", "scorer_hash",
        "queue_size", "metadata_json",
    },
    "recommendation_snapshot_items": {
        "snapshot_id", "rank", "recommendation_id", "posting_id", "posting_version_id",
        "score_version_id", "tier", "odds", "odds_score", "source", "source_category",
        "match_label", "competition_label", "role_family", "title", "company",  # F16
    },
    "outcome_events": {
        "outcome_event_id", "kind", "at", "posting_id", "seen_key", "url", "snapshot_id",
        "rank", "payload_json", "idempotency_key",  # F14
    },
}


def _columns(conn, table):
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _insert_posting(conn, posting_id, at=AT):
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, at, at),
    )


def _insert_version(conn, posting_id, version_id, *, observed_at=AT, title="Support Engineer",
                     source="greenhouse", odds="Strong match / Standard", tier=2):
    conn.execute(
        "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, "
        "version_hash, observed_at, title, source, odds, tier, payload_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (version_id, posting_id, "source", version_id, observed_at, title, source, odds, tier, "{}"),
    )


def _ensure_profile_row(conn, profile_version_id, content_hash=None, at=AT):
    """Mirrors test_outcomes.py:246's pattern: a `profile_versions` row + a
    monkeypatched `_load_profile` is what actually mints `recommendations`
    rows in a test database that has no real `profile.json`."""
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions (profile_version_id, content_hash, "
        "profile_json, created_at) VALUES (?,?,?,?)",
        (profile_version_id, content_hash or profile_version_id, "{}", at),
    )


class _FakeFamilies:
    def __init__(self, keywords):
        self.keywords = keywords


class _FakeProfile:
    def __init__(self, keywords, content_hash="contract-fake-profile-hash"):
        self.families = _FakeFamilies(keywords)
        self.content_hash = content_hash


# --------------------------------------------------------------------------- #
# 1. MIGRATIONS[-1] shape
# --------------------------------------------------------------------------- #
def test_migrations_last_entry_is_21_outcome_snapshots():
    version, name, fn = MIGRATIONS[-1]
    assert (version, name) == (21, "outcome_snapshots")
    assert fn is migrations_mod._migration_21_outcome_snapshots


# --------------------------------------------------------------------------- #
# 2. Fresh + upgraded DBs both reach the exact expected column sets
# --------------------------------------------------------------------------- #
def test_fresh_db_has_outcome_tables_with_expected_columns(tmp_path):
    conn = connect(tmp_path / "fresh.db")
    init_db(conn)
    for table, expected in EXPECTED_COLUMNS.items():
        assert _columns(conn, table) == expected, table
    conn.close()


def test_upgraded_v20_db_has_outcome_tables_with_expected_columns(tmp_path):
    path = tmp_path / "v20_upgrade.db"
    conn = connect(path)
    init_db(conn)  # fresh -> stamped at the latest version, tables already present

    # Roll back to "as if migration 21 had never run": drop its tables/indexes and
    # its schema_version row, exactly like test_migrations.py's own migration-20
    # upgrade test does for that migration.
    for table in EXPECTED_COLUMNS:
        conn.execute(f"DROP TABLE {table}")
    conn.execute("DELETE FROM schema_version WHERE version >= 21")
    conn.commit()

    applied = run_migrations(conn, str(path))
    assert applied == [(21, "outcome_snapshots")]

    for table, expected in EXPECTED_COLUMNS.items():
        assert _columns(conn, table) == expected, table
    conn.close()


# --------------------------------------------------------------------------- #
# 3. Append-only semantics
# --------------------------------------------------------------------------- #
def test_capturing_overlapping_snapshots_never_mutates_prior_rows(tmp_path, monkeypatch):
    conn = connect(tmp_path / "append_only.db")
    init_db(conn)
    # F6: mint ACTUAL recommendations rows -- without a resolvable profile,
    # `profile_version_id` is always None and every assertion below about
    # recommendations rows would be vacuously true (0 rows, 0 == 0). A real
    # `profile.json` may or may not exist in this environment, so the profile
    # is faked exactly like test_outcomes.py:246's established pattern:
    # monkeypatch the loader and insert the matching `profile_versions` row.
    monkeypatch.setattr(
        outcomes, "_load_profile",
        lambda: _FakeProfile({}, content_hash="contract-append-only-hash"),
    )
    _ensure_profile_row(conn, "pv-append-only", content_hash="contract-append-only-hash")
    _insert_posting(conn, "p1")
    _insert_posting(conn, "p2")
    _insert_version(conn, "p1", "v1")
    _insert_version(conn, "p2", "v2")
    conn.commit()

    first = outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
        at="2026-08-01T10:00:00",
    )
    first_items_before = conn.execute(
        "SELECT * FROM recommendation_snapshot_items WHERE snapshot_id=? ORDER BY rank",
        (first["snapshot_id"],),
    ).fetchall()
    recs_before = {
        r["recommendation_id"]: dict(r)
        for r in conn.execute("SELECT * FROM recommendations").fetchall()
    }
    # The fake profile resolves and both postings are versioned, so this is
    # no longer a trivial `>= 0`: exactly one recommendations row per posting.
    assert len(recs_before) == 2

    second = outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
        at="2026-08-02T10:00:00",
    )

    assert second["snapshot_id"] != first["snapshot_id"]

    # Two independent header rows and two independent item sets.
    headers = conn.execute("SELECT snapshot_id FROM recommendation_snapshots").fetchall()
    assert {r["snapshot_id"] for r in headers} == {first["snapshot_id"], second["snapshot_id"]}

    first_items_after = conn.execute(
        "SELECT * FROM recommendation_snapshot_items WHERE snapshot_id=? ORDER BY rank",
        (first["snapshot_id"],),
    ).fetchall()
    assert [dict(r) for r in first_items_after] == [dict(r) for r in first_items_before]

    second_items = conn.execute(
        "SELECT * FROM recommendation_snapshot_items WHERE snapshot_id=? ORDER BY rank",
        (second["snapshot_id"],),
    ).fetchall()
    assert len(second_items) == 2

    # F7: the FULL row dict, byte-identical -- not just "same count" or "same
    # created_at" -- across two captures of the same (posting, profile,
    # version) identity. This is the pin that catches a smuggled `ON CONFLICT
    # (idempotency_key) DO UPDATE`: an UPDATE could leave row COUNT and
    # posting_id unchanged while still silently rewriting created_at,
    # recommendation_json, or status out from under the first capture.
    recs_after = {
        r["recommendation_id"]: dict(r)
        for r in conn.execute("SELECT * FROM recommendations").fetchall()
    }
    assert recs_after == recs_before
    by_posting = {}
    for row in recs_after.values():
        by_posting.setdefault(row["posting_id"], []).append(row)
    for posting_id, rows in by_posting.items():
        assert len(rows) == 1, f"{posting_id} has {len(rows)} recommendations rows"

    conn.close()


# --------------------------------------------------------------------------- #
# 4. outcome_events CHECK constraint
# --------------------------------------------------------------------------- #
def test_outcome_events_check_rejects_all_identifiers_null(tmp_path):
    conn = connect(tmp_path / "check.db")
    init_db(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outcome_events (outcome_event_id, kind, at, posting_id, seen_key, url) "
            "VALUES ('e1', 'opened', ?, NULL, NULL, NULL)",
            (AT,),
        )
    conn.close()


# --------------------------------------------------------------------------- #
# 5. No UPDATE/DELETE anywhere in the module (blunt, deliberately so)
# --------------------------------------------------------------------------- #
def _sql_string_literals(module) -> list[str]:
    """Every string literal in `module`'s source, EXCEPT docstrings (the first
    Expr-statement string in the module body, and in each class/function body)
    -- prose describing what the code does (e.g. "never an UPDATE") must not
    trip a scan meant to catch SQL the code actually EXECUTES. `ast`-based
    rather than a raw substring/regex scan on source text (F8): the original
    scan (`"UPDATE " not in source`) is case-sensitive on this codebase's
    upper-case-keyword convention and missed a lower-cased or formatted query
    (`"delete from outcome_events"`) entirely -- a real gap, not a hypothetical
    one, since nothing enforces keyword casing in an f-string at runtime."""
    import ast
    import inspect
    import textwrap

    source = textwrap.dedent(inspect.getsource(module))
    tree = ast.parse(source)
    docstring_nodes: set = set()

    def _mark_docstring(body):
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                and isinstance(body[0].value.value, str):
            docstring_nodes.add(id(body[0].value))

    _mark_docstring(tree.body)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            _mark_docstring(node.body)

    literals = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and id(node) not in docstring_nodes:
            literals.append(node.value)
    return literals


def test_outcomes_module_exposes_no_update_or_delete_sql():
    """Append-only is enforced by never writing the SQL keyword, not by a
    runtime guard, so this scans the module's own non-docstring string
    literals (its SQL) for a stray UPDATE/DELETE, case-insensitively and
    word-boundary-matched -- catching `"delete from ..."` and any oddly-cased
    or formatted variant a plain `"UPDATE " not in source` substring check
    would miss, while leaving prose in docstrings (which legitimately
    discusses UPDATE/DELETE as concepts) alone."""
    import re

    pattern = re.compile(r"\b(update|delete)\b", re.IGNORECASE)
    offenders = [lit for lit in _sql_string_literals(outcomes) if pattern.search(lit)]
    assert offenders == [], f"outcomes.py must never UPDATE/DELETE a stored row: {offenders!r}"
