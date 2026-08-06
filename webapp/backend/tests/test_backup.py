"""Task 5.6: `backend/backup.py` -- create/list/restore/verify + CLI smoke.

Every DB here lives under `tmp_path` (repo-root `conftest.py` also fences
`JOBHUNT_DB` for the whole session, belt and braces). Nothing in this file
touches `webapp/app.db` or writes into the real `webapp/backups`.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend import backup
from backend.config import WEBAPP_DIR
from backend.db import connect, init_db
from backend.migrations import MIGRATIONS

LATEST_SCHEMA_VERSION = max(version for version, _name, _fn in MIGRATIONS)


def _seed(conn):
    """A handful of rows across a few tables so row-count accounting is
    non-trivial (not every table zero)."""
    conn.execute("INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/1', 'sk1', 1)")
    conn.execute("INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/2', 'sk2', 2)")
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) "
        "VALUES ('sk1', 'https://x/1', 'Applied', '2026-08-01T00:00:00')"
    )
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) "
        "VALUES ('sk2', 'https://x/2', 'New', '2026-08-02T00:00:00')"
    )
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES ('skills', '[\"sql\"]')"
    )
    conn.commit()


@pytest.fixture
def source_db(tmp_path):
    db_path = tmp_path / "source.db"
    conn = connect(db_path)
    init_db(conn)
    _seed(conn)
    yield db_path, conn
    conn.close()


def _conn_db_path(conn):
    """The filesystem path SQLite itself reports for `conn`'s main database --
    an oracle independent of whatever path string the caller happened to pass
    in, used to prove which physical file a given call actually touched."""
    for row in conn.execute("PRAGMA database_list").fetchall():
        if row["name"] == "main":
            return row["file"]
    return None


def _independent_counts(db_path):
    """Row counts computed with a fresh connection, independent of anything
    `backup.py` itself does -- the round-trip oracle."""
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {
            r["name"]: conn.execute(f'SELECT COUNT(*) AS c FROM "{r["name"]}"').fetchone()["c"]
            for r in rows
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# create_backup
# --------------------------------------------------------------------------- #
def test_create_backup_manifest_accounting_exact(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"

    manifest = backup.create_backup(db_path, dest_dir)

    expected_counts = _independent_counts(db_path)
    assert manifest["tables"] == expected_counts
    assert manifest["tables"]["jobs"] == 2
    assert manifest["tables"]["job_state"] == 2
    assert manifest["tables"]["app_settings"] == 1
    assert manifest["schema_version"] == LATEST_SCHEMA_VERSION
    assert manifest["source_path"] == str(db_path.resolve())
    assert manifest["app_version"] is None
    assert manifest["created_at"]
    # Microsecond precision (not just to-the-second) -- required for
    # list_backups to order same-second backups correctly.
    assert "." in manifest["created_at"]

    backup_path = dest_dir / manifest["backup_file"]
    assert backup_path.is_file()
    assert (dest_dir / manifest["backup_file"]).with_suffix(".json").is_file()
    on_disk = json.loads((dest_dir / manifest["backup_file"]).with_suffix(".json").read_text())
    assert on_disk["tables"] == expected_counts
    assert on_disk["schema_version"] == LATEST_SCHEMA_VERSION


def test_create_backup_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        backup.create_backup(tmp_path / "does-not-exist.db", tmp_path / "backups")


def test_create_backup_default_dest_dir_is_webapp_backups():
    assert backup.DEFAULT_BACKUP_DIR == WEBAPP_DIR / "backups"


def test_create_backup_refuses_source_with_no_tables(tmp_path):
    empty_db = tmp_path / "empty.db"
    sqlite3.connect(str(empty_db)).close()  # zero-byte file: a valid, empty SQLite db

    with pytest.raises(RuntimeError):
        backup.create_backup(empty_db, tmp_path / "backups")


def test_create_backup_refuses_source_with_no_schema_version(tmp_path):
    raw_db = tmp_path / "unmigrated.db"
    conn = sqlite3.connect(str(raw_db))
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO widgets DEFAULT VALUES")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError):
        backup.create_backup(raw_db, tmp_path / "backups")


# Mutant M3 ("manifest counts from source not backup") and M12 ("schema_version
# read from source") both leave the manifest looking identical to a correct
# run in every other test here, because the source and the freshly-copied
# backup hold the same data at that instant -- src vs dst is otherwise an
# equivalent mutant. A connect-spy that records which physical file each
# oracle call actually touched is what makes the distinction observable.
def test_create_backup_manifest_oracle_reads_backup_not_source(source_db, tmp_path, monkeypatch):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"

    row_count_calls = []
    schema_version_calls = []
    orig_row_counts = backup._row_counts
    orig_schema_version = backup._schema_version

    def spy_row_counts(c):
        row_count_calls.append(_conn_db_path(c))
        return orig_row_counts(c)

    def spy_schema_version(c):
        schema_version_calls.append(_conn_db_path(c))
        return orig_schema_version(c)

    monkeypatch.setattr(backup, "_row_counts", spy_row_counts)
    monkeypatch.setattr(backup, "_schema_version", spy_schema_version)

    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    # _row_counts is called exactly once (for the manifest) and it must be
    # against the just-written BACKUP file, never the source.
    assert len(row_count_calls) == 1
    assert row_count_calls[0] and Path(row_count_calls[0]).name == backup_path.name

    # _schema_version is called at least once for the manifest; whichever
    # call's value ends up in the manifest is the LAST one made (an earlier
    # call, if any, is the pre-flight "is the source even migrated" guard
    # against the source itself) -- and that last call must be against the
    # backup, not the source.
    assert schema_version_calls
    assert Path(schema_version_calls[-1]).name == backup_path.name
    assert manifest["schema_version"] == LATEST_SCHEMA_VERSION


# Mutant M7 ("source opened read-write"): with no data actually altered in
# any of these tests, an md5-unchanged assertion alone can't distinguish a
# read-write open from a read-only one. A connect-spy pinning the exact
# `mode=ro` URI is the deterministic kill; the md5 check is belt and braces.
def test_create_backup_opens_source_read_only(source_db, tmp_path, monkeypatch):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"

    calls = []
    orig_connect = backup.sqlite3.connect

    def spy_connect(*args, **kwargs):
        calls.append((args, kwargs))
        return orig_connect(*args, **kwargs)

    monkeypatch.setattr(backup.sqlite3, "connect", spy_connect)

    before_md5 = hashlib.md5(db_path.read_bytes()).hexdigest()
    backup.create_backup(db_path, dest_dir)
    after_md5 = hashlib.md5(db_path.read_bytes()).hexdigest()
    assert before_md5 == after_md5  # source bytes untouched by the backup

    expected_uri = f"file:{Path(db_path)}?mode=ro"
    source_calls = [(a, kw) for a, kw in calls if a and a[0] == expected_uri]
    assert source_calls, f"expected a connect({expected_uri!r}, uri=True) call; got {calls!r}"
    _, kwargs = source_calls[0]
    assert kwargs.get("uri") is True


# Mutant M9 ("no cleanup of partial backup file"): the earlier failure paths
# (missing source, corrupt source) all fail BEFORE backup_path exists, so a
# removed cleanup call would never be exercised by them. This injects a
# failure AFTER the .db file is fully written and validated -- while writing
# its manifest sidecar -- which is exactly the gap the fix closes.
def test_create_backup_cleans_up_on_manifest_write_failure(source_db, tmp_path, monkeypatch):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"

    real_write_text = Path.write_text

    def failing_write_text(self, *args, **kwargs):
        if self.suffix == ".json":
            raise OSError("simulated failure writing the manifest sidecar")
        return real_write_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", failing_write_text)

    with pytest.raises(OSError):
        backup.create_backup(db_path, dest_dir)

    # The backup .db file was fully written and validated before the
    # manifest write failed -- it must not be left orphaned with no manifest
    # to account for it.
    assert list(dest_dir.glob("*.db")) == []
    assert list(dest_dir.glob("*.json")) == []


# H1: FK-corruption fixture. state_events.posting_id references
# postings(posting_id); with FK enforcement off the insert succeeds even
# though it violates the constraint, and only PRAGMA foreign_key_check (via
# _validate_database) catches it -- row-count/schema_version accounting alone
# sees a perfectly normal-looking database.
def test_create_backup_catches_fk_corruption_in_source(tmp_path):
    db_path = tmp_path / "fk-corrupt-source.db"
    conn = connect(db_path)
    init_db(conn)
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, "
        "at, source, posting_id) VALUES "
        "('sk1', 'https://x/1', 'status', 'New', 'Applied', "
        "'2026-08-01T00:00:00', 'patch', 'nonexistent-posting-id')"
    )
    conn.commit()
    conn.close()

    dest_dir = tmp_path / "backups"
    with pytest.raises(RuntimeError):
        backup.create_backup(db_path, dest_dir)

    # No half-written backup left behind despite the copy having succeeded
    # (the corruption is only caught by post-copy FK validation).
    assert list(dest_dir.glob("*")) == []


# --------------------------------------------------------------------------- #
# list_backups
# --------------------------------------------------------------------------- #
def test_list_backups_empty_dir_returns_empty_list(tmp_path):
    assert backup.list_backups(tmp_path / "nope") == []


def test_list_backups_newest_first(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"

    m1 = backup.create_backup(db_path, dest_dir)
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/3', 'sk3', 1)"
    )
    conn.commit()
    m2 = backup.create_backup(db_path, dest_dir)

    listed = backup.list_backups(dest_dir)
    assert len(listed) == 2
    assert [m["created_at"] for m in listed] == sorted(
        [m1["created_at"], m2["created_at"]], reverse=True
    )
    # Second backup has one more job row than the first.
    by_file = {m["backup_file"]: m for m in listed}
    assert by_file[m2["backup_file"]]["tables"]["jobs"] == 3
    assert by_file[m1["backup_file"]]["tables"]["jobs"] == 2


# H3 / M4: three backups whose created_at all land in the same wall-clock
# SECOND (but at increasing microsecond offsets, as real rapid-fire backups
# would) must still list newest first. Without microsecond precision in
# created_at, all three would tie on a second-granularity timestamp and the
# "newest first" assertion above would pass vacuously regardless of sort
# direction (equal keys compare equal either way) -- this is also what makes
# M4 ("list oldest-first") a real, non-vacuous kill.
def test_list_backups_same_second_orders_newest_first(source_db, tmp_path, monkeypatch):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"

    base = datetime(2026, 8, 5, 12, 0, 0, 0, tzinfo=timezone.utc)
    ticks = iter(
        [
            base, base,
            base + timedelta(microseconds=100), base + timedelta(microseconds=100),
            base + timedelta(microseconds=200), base + timedelta(microseconds=200),
        ]
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(ticks)

    monkeypatch.setattr(backup, "datetime", _FrozenDateTime)

    m1 = backup.create_backup(db_path, dest_dir)
    m2 = backup.create_backup(db_path, dest_dir)
    m3 = backup.create_backup(db_path, dest_dir)

    # All three really do land in the same second -- the tie this test exists
    # to exercise.
    assert m1["created_at"][:19] == m2["created_at"][:19] == m3["created_at"][:19]
    assert len({m1["backup_file"], m2["backup_file"], m3["backup_file"]}) == 3

    listed = backup.list_backups(dest_dir)
    assert [m["backup_file"] for m in listed] == [
        m3["backup_file"],
        m2["backup_file"],
        m1["backup_file"],
    ]


# L7: a manifest file that fails to parse must surface as a visible error
# entry in the listing, not vanish silently.
def test_list_backups_surfaces_unparseable_manifest_as_error_entry(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    backup.create_backup(db_path, dest_dir)

    bad = dest_dir / "zzz-corrupt.json"
    bad.write_text("{not valid json")

    listed = backup.list_backups(dest_dir)
    errors = [m for m in listed if "error" in m]
    assert len(errors) == 1
    assert errors[0]["manifest_path"] == str(bad)


# --------------------------------------------------------------------------- #
# restore
# --------------------------------------------------------------------------- #
def test_restore_into_fresh_path_green(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    dest_path = tmp_path / "restored.db"
    result = backup.restore(backup_path, dest_path)

    assert dest_path.is_file()
    assert result["schema_version"] == manifest["schema_version"]
    assert result["tables"] == manifest["tables"]
    assert result["row_count_total"] == sum(manifest["tables"].values())


def test_restore_refuses_existing_dest(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    dest_path = tmp_path / "already-here.db"
    dest_path.write_bytes(b"pre-existing content")

    with pytest.raises(FileExistsError):
        backup.restore(backup_path, dest_path)

    # untouched
    assert dest_path.read_bytes() == b"pre-existing content"


def test_restore_missing_backup_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        backup.restore(tmp_path / "nope.db", tmp_path / "dest.db")


def test_restore_missing_manifest_raises(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]
    manifest_path = backup_path.with_suffix(".json")
    manifest_path.unlink()

    with pytest.raises(FileNotFoundError):
        backup.restore(backup_path, tmp_path / "dest.db")


def test_restore_corrupted_backup_fails_validation_loudly(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    # Truncate mid-file: a valid SQLite header followed by garbage/nothing is
    # not a coherent database image.
    raw = backup_path.read_bytes()
    backup_path.write_bytes(raw[: len(raw) // 2])

    dest_path = tmp_path / "dest.db"
    with pytest.raises(RuntimeError):
        backup.restore(backup_path, dest_path)
    # A failed restore must not leave a half-written destination behind.
    assert not dest_path.exists()


def test_restore_non_database_file_fails_validation_loudly(tmp_path):
    dest_dir = tmp_path / "backups"
    dest_dir.mkdir()
    fake_backup = dest_dir / "fake-20260101T000000000000Z.db"
    fake_backup.write_bytes(b"not a sqlite database, just some bytes")
    fake_manifest = fake_backup.with_suffix(".json")
    fake_manifest.write_text(json.dumps({"schema_version": LATEST_SCHEMA_VERSION, "tables": {}}))

    dest_path = tmp_path / "dest.db"
    with pytest.raises(RuntimeError):
        backup.restore(fake_backup, dest_path)
    assert not dest_path.exists()


# H2: a dangling symlink at dest_path must be refused outright, not silently
# written through. `Path.exists()` alone follows symlinks and reports False
# for a dangling one, which would let `sqlite3.connect` create-and-write the
# symlink's target -- a write that escapes the caller-intended dest_path
# entirely.
def test_restore_refuses_dangling_symlink_dest(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    escape_target = tmp_path / "escaped-outside-intended-dest.db"
    dest_path = tmp_path / "dangling-link.db"
    dest_path.symlink_to(escape_target)  # target does not exist -- dangling

    with pytest.raises(FileExistsError):
        backup.restore(backup_path, dest_path)

    # The write never happened through the symlink.
    assert not escape_target.exists()
    assert dest_path.is_symlink()


# H2: cleanup after a failure must remove the real file, not just unlink a
# symlink pointing at it (which would silently orphan the real data).
def test_remove_partial_resolves_symlink_and_removes_real_file(tmp_path):
    real_file = tmp_path / "real-partial.db"
    real_file.write_bytes(b"partial data that must not be orphaned")
    link = tmp_path / "link-to-real.db"
    link.symlink_to(real_file)

    backup._remove_partial(link)

    assert not link.exists()
    assert not link.is_symlink()
    assert not real_file.exists()


# Mutant M10 ("restore validates source not dest"): src and dst hold
# identical data in every other test here, so a mutant that validates
# `backup_path` instead of the freshly-restored `dest_path` is otherwise
# unobservable. A spy on `_check_against_manifest` pins which connection it
# actually receives.
def test_restore_validates_destination_not_source(source_db, tmp_path, monkeypatch):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]
    dest_path = tmp_path / "restored.db"

    seen = []
    orig = backup._check_against_manifest

    def spy(c, m):
        seen.append(_conn_db_path(c))
        return orig(c, m)

    monkeypatch.setattr(backup, "_check_against_manifest", spy)

    backup.restore(backup_path, dest_path)

    assert len(seen) == 1
    assert seen[0] and Path(seen[0]).name == dest_path.name
    assert Path(seen[0]).name != backup_path.name


# --------------------------------------------------------------------------- #
# verify
# --------------------------------------------------------------------------- #
def test_verify_good_backup_passes(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    result = backup.verify(backup_path)
    assert result["schema_version"] == manifest["schema_version"]
    assert result["tables"] == manifest["tables"]


def test_verify_corrupted_backup_fails_loudly(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    raw = backup_path.read_bytes()
    backup_path.write_bytes(raw[:200])  # truncate hard, well inside the header

    with pytest.raises(RuntimeError):
        backup.verify(backup_path)


def test_verify_row_count_mismatch_fails_loudly(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    manifest_path = dest_dir / f"{manifest['backup_file'][:-3]}.json"

    tampered = dict(manifest)
    tampered["tables"] = dict(manifest["tables"])
    tampered["tables"]["jobs"] = manifest["tables"]["jobs"] + 1
    manifest_path.write_text(json.dumps(tampered))

    with pytest.raises(RuntimeError):
        backup.verify(dest_dir / manifest["backup_file"])


def test_verify_schema_version_mismatch_fails_loudly(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    manifest_path = dest_dir / f"{manifest['backup_file'][:-3]}.json"

    tampered = dict(manifest)
    tampered["schema_version"] = manifest["schema_version"] + 1
    manifest_path.write_text(json.dumps(tampered))

    with pytest.raises(RuntimeError):
        backup.verify(dest_dir / manifest["backup_file"])


# H1: FK-corruption fixture. state_events.posting_id references
# postings(posting_id); with FK enforcement off, inserting a row with a
# posting_id that does not exist succeeds anyway. Bumping the manifest's
# state_events count to match means row-count accounting alone now agrees
# with the corrupted backup -- only integrity/FK validation (PRAGMA
# foreign_key_check, via _validate_database) can still catch it.
def test_verify_catches_fk_corruption_manifest_counts_would_miss(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]
    manifest_path = backup_path.with_suffix(".json")

    bconn = sqlite3.connect(str(backup_path))
    bconn.execute("PRAGMA foreign_keys=OFF")
    bconn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, "
        "at, source, posting_id) VALUES "
        "('sk1', 'https://x/1', 'status', 'New', 'Applied', "
        "'2026-08-01T00:00:00', 'patch', 'nonexistent-posting-id')"
    )
    bconn.commit()
    bconn.close()

    tampered = dict(manifest)
    tampered["tables"] = dict(manifest["tables"])
    tampered["tables"]["state_events"] = tampered["tables"].get("state_events", 0) + 1
    manifest_path.write_text(json.dumps(tampered))

    with pytest.raises(RuntimeError):
        backup.verify(backup_path)


# M-1: a manifest missing a required accounting key must fail hard, not
# silently default (e.g. treating an absent "tables" key as {} and comparing
# equal to a since-emptied backup, or an absent "schema_version" as None and
# comparing equal to a pre-schema_version backup).
def test_verify_manifest_missing_required_key_raises(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    manifest_path = dest_dir / f"{manifest['backup_file'][:-3]}.json"

    stripped = dict(manifest)
    del stripped["schema_version"]
    manifest_path.write_text(json.dumps(stripped))

    with pytest.raises(RuntimeError):
        backup.verify(dest_dir / manifest["backup_file"])


# L2: the manifest must actually be the sidecar for the backup it is being
# checked against -- a manifest naming a different backup_file is a binding
# mismatch, not a value to trust.
def test_verify_manifest_backup_file_binding_mismatch_raises(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    m1 = backup.create_backup(db_path, dest_dir)
    m2 = backup.create_backup(db_path, dest_dir)

    with pytest.raises(RuntimeError):
        backup.verify(
            dest_dir / m1["backup_file"],
            dest_dir / f"{m2['backup_file'][:-3]}.json",
        )


# --------------------------------------------------------------------------- #
# Round-trip equality: row counts + spot content, independent of the manifest.
# --------------------------------------------------------------------------- #
def test_round_trip_row_counts_and_spot_content(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "backups"
    manifest = backup.create_backup(db_path, dest_dir)
    backup_path = dest_dir / manifest["backup_file"]

    dest_path = tmp_path / "restored.db"
    backup.restore(backup_path, dest_path)

    assert _independent_counts(dest_path) == _independent_counts(db_path)

    restored = connect(dest_path)
    try:
        row = restored.execute(
            "SELECT status, url FROM job_state WHERE seen_key='sk1'"
        ).fetchone()
        assert row["status"] == "Applied"
        assert row["url"] == "https://x/1"
        row2 = restored.execute(
            "SELECT tier FROM jobs WHERE seen_key='sk2'"
        ).fetchone()
        assert row2["tier"] == 2
        settings_row = restored.execute(
            "SELECT value FROM app_settings WHERE key='skills'"
        ).fetchone()
        assert json.loads(settings_row["value"]) == ["sql"]
    finally:
        restored.close()


# --------------------------------------------------------------------------- #
# CLI smoke test
# --------------------------------------------------------------------------- #
def test_cli_smoke_create_list_restore_verify(source_db, tmp_path):
    db_path, conn = source_db
    dest_dir = tmp_path / "cli-backups"

    def run(*args):
        proc = subprocess.run(
            [sys.executable, "-m", "backend.backup", *args],
            cwd=str(WEBAPP_DIR),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
        return json.loads(proc.stdout)

    created = run("create", str(db_path), "--dest-dir", str(dest_dir))
    assert created["tables"]["jobs"] == 2

    listed = run("list", "--dest-dir", str(dest_dir))
    assert len(listed) == 1
    assert listed[0]["backup_file"] == created["backup_file"]

    dest_path = tmp_path / "cli-restored.db"
    backup_path = dest_dir / created["backup_file"]
    restored = run("restore", str(backup_path), str(dest_path))
    assert restored["tables"] == created["tables"]
    assert dest_path.is_file()

    verified = run("verify", str(backup_path))
    assert verified["tables"] == created["tables"]

    # restore CLI also refuses an existing dest (non-zero exit, no traceback
    # needed -- json error on stderr, dest untouched).
    proc = subprocess.run(
        [sys.executable, "-m", "backend.backup", "restore", str(backup_path), str(dest_path)],
        cwd=str(WEBAPP_DIR),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 1
    err = json.loads(proc.stderr)
    assert "existing" in err["error"]
