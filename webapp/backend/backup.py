"""Task 5.6: consistent SQLite backup, restore, and standalone verification for
`app.db`.

Reuses migrations.py's proven machinery rather than reimplementing it:
`sqlite3.Connection.backup` for a page-level consistent copy (the same
mechanism `migrations._backup` uses for its pre-migration safety copy) and
`migrations._validate_database` (`PRAGMA integrity_check` + `PRAGMA
foreign_key_check`) for post-copy validation. On top of that this module adds
the sidecar manifest (schema_version + per-table row counts, so accounting can
be checked exactly, not just "integrity_check said ok") and the fresh-
destination-only restore contract.

Four operations:
- `create_backup(db_path=config.DB_PATH, dest_dir=DEFAULT_BACKUP_DIR)`:
  timestamped `<stem>-<stamp>.db` copy + `<stem>-<stamp>.json` manifest sidecar
  in `dest_dir`. Returns the manifest dict (plus `backup_path`/`manifest_path`
  for convenience).
- `list_backups(dest_dir=DEFAULT_BACKUP_DIR)`: every manifest in `dest_dir`,
  newest `created_at` first.
- `restore(backup_path, dest_path, manifest_path=None)`: copies `backup_path`
  into `dest_path` via the same `Connection.backup` mechanism, but ONLY if
  `dest_path` does not already exist -- restoring over a live database stays a
  deliberate manual file operation (stop the server, move files, restart),
  never something an API call or a stray CLI invocation can do by accident.
  Validates the copy (integrity, foreign keys, schema_version, exact per-table
  row-count accounting against the manifest) before returning; a fresh file
  written by a failed restore is cleaned up rather than left half-verified on
  disk.
- `verify(backup_path, manifest_path=None)`: the same checks as the tail of
  `restore`, run directly against `backup_path` with nothing copied anywhere --
  for checking a backup is sound without spending a restore's disk space.

All four raise loudly (FileNotFoundError / FileExistsError / RuntimeError) on
any failure -- there is no return-a-falsy-value path. A CLI at the bottom
(`python -m backend.backup {create|list|restore|verify}`, run from `webapp/`)
wraps all four for use with the server down.

`dest_dir` defaults to `webapp/backups` (via `config.WEBAPP_DIR`, independent
of CWD) but is a plain parameter throughout, so tests point it at `tmp_path`
and never touch the real directory.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .migrations import _validate_database

DEFAULT_BACKUP_DIR = config.WEBAPP_DIR / "backups"


def _table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]


def _row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        name: conn.execute(f'SELECT COUNT(*) AS c FROM "{name}"').fetchone()["c"]
        for name in _table_names(conn)
    }


def _schema_version(conn: sqlite3.Connection):
    """Max applied migration version, or None if the DB predates schema_version
    (or is a freshly-backed-up empty file with no tables at all)."""
    try:
        row = conn.execute("SELECT MAX(version) AS v FROM schema_version").fetchone()
    except sqlite3.OperationalError:
        return None
    return row["v"] if row is not None else None


def _open_ro(path: Path) -> sqlite3.Connection:
    """Read-only connection to an existing file. `mode=ro` refuses to create the
    file if it is missing, unlike a bare `sqlite3.connect`, so a typo'd path
    fails loudly instead of silently backing up an empty database. Read-only is
    also a safety property in its own right: the source (the live app.db, in
    production) is never opened in a mode that could write to it."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _refuse_if_present(path: Path, what: str) -> None:
    """Refuses to let a write proceed onto `path` if anything is already there
    -- including a symlink whose target does not (yet) exist. `Path.exists()`
    alone follows symlinks and reports False for a dangling one, which would
    let `sqlite3.connect(str(path))` create-and-write through it: a write to
    wherever the symlink points, silently escaping the intended backups/
    directory. Checking `is_symlink()` too closes that gap regardless of
    whether the link is dangling."""
    if path.exists() or path.is_symlink():
        raise FileExistsError(
            f"refusing to write {what}: {path} already exists or is a symlink "
            "(remove it yourself first if that is really what you want)"
        )


def _remove_partial(path: Path) -> None:
    """Removes a freshly-written, not-yet-committed file, resolving through a
    symlink first if `path` happens to be one. A plain `path.unlink()` on a
    symlink removes only the link, leaving whatever real file it points at
    orphaned on disk -- silently "cleaned up" from the caller's point of view
    while the actual partial data stays behind."""
    if path.is_symlink():
        target = path.resolve()
        path.unlink()
        if target != path and target.exists():
            target.unlink()
    elif path.exists():
        path.unlink()


def create_backup(db_path=None, dest_dir=None) -> dict:
    """Consistent, validated, timestamped backup of `db_path` (default
    `config.DB_PATH`) into `dest_dir` (default `DEFAULT_BACKUP_DIR`). Returns
    the manifest dict; the same dict is also written to the sidecar JSON file.
    """
    src_path = Path(db_path) if db_path is not None else Path(config.DB_PATH)
    if not src_path.is_file():
        raise FileNotFoundError(f"source database not found: {src_path}")

    out_dir = Path(dest_dir) if dest_dir is not None else DEFAULT_BACKUP_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    stem = src_path.stem
    backup_name = f"{stem}-{stamp}.db"
    manifest_name = f"{stem}-{stamp}.json"
    backup_path = out_dir / backup_name
    manifest_path = out_dir / manifest_name
    _refuse_if_present(backup_path, "backup")
    _refuse_if_present(manifest_path, "manifest")

    src = _open_ro(src_path)
    if not _table_names(src):
        src.close()
        raise RuntimeError(
            f"refusing to back up an empty/unrecognized database (no tables): {src_path}"
        )
    if _schema_version(src) is None:
        src.close()
        raise RuntimeError(
            f"refusing to back up a database with no schema_version "
            f"(not a migrated app.db): {src_path}"
        )

    dst = None
    manifest = None
    try:
        dst = sqlite3.connect(str(backup_path))
        dst.row_factory = sqlite3.Row
        dst.execute("PRAGMA foreign_keys=ON")
        src.backup(dst)
        _validate_database(dst)
        # Deliberately read the accounting back off `dst` (the just-written
        # backup file), never `src` (the live source): the manifest has to
        # describe what actually landed in the backup, and this is what lets
        # `verify()`/`restore()` re-check a backup standalone with no
        # dependence on the source still existing or still matching.
        schema_version = _schema_version(dst)
        tables = _row_counts(dst)
        dst.close()
        dst = None
        src.close()
        src = None

        manifest = {
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "schema_version": schema_version,
            "tables": tables,
            "source_path": str(src_path.resolve()),
            # No app-version concept exists in this codebase (checked: no
            # pyproject/VERSION file carries one) -- kept as an explicit null
            # field rather than omitted, so a future version string has a
            # stable place to land without changing the manifest shape.
            "app_version": None,
            "backup_file": backup_name,
        }
        # Writing the manifest lives inside this try/except (not after it) so
        # a failure here -- disk full, permissions -- still triggers cleanup
        # of the just-validated backup_path .db file below, rather than
        # leaving it orphaned on disk with no manifest to account for it.
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    except Exception:
        if dst is not None:
            dst.close()
        if src is not None:
            src.close()
        _remove_partial(backup_path)
        _remove_partial(manifest_path)
        raise

    manifest = dict(manifest)
    manifest["manifest_path"] = str(manifest_path)
    manifest["backup_path"] = str(backup_path)
    return manifest


def list_backups(dest_dir=None) -> list[dict]:
    """Every manifest in `dest_dir`, newest `created_at` first. Empty list (not
    an error) if `dest_dir` does not exist yet."""
    out_dir = Path(dest_dir) if dest_dir is not None else DEFAULT_BACKUP_DIR
    if not out_dir.is_dir():
        return []
    manifests = []
    for path in sorted(out_dir.glob("*.json")):
        try:
            manifest = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError) as e:
            # Not parseable as this module's manifest -- surface it as an
            # explicit error entry rather than silently dropping it (a stray
            # or corrupted manifest should be visible, not invisible).
            manifests.append({"manifest_path": str(path), "error": str(e)})
            continue
        manifest["manifest_path"] = str(path)
        backup_file = manifest.get("backup_file")
        if backup_file:
            manifest["backup_path"] = str(out_dir / backup_file)
        manifests.append(manifest)
    # Sort newest first by created_at (microsecond precision), with the
    # backup filename (which carries the same microsecond stamp) as a
    # secondary key -- belt and braces against any created_at tie ordering
    # ambiguously. Entries with neither (error entries) sort last.
    manifests.sort(
        key=lambda m: (m.get("created_at") or "", m.get("backup_file") or ""),
        reverse=True,
    )
    return manifests


def _mismatch_detail(expected: dict, actual: dict) -> str:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    mismatched = {
        t: {"expected": expected.get(t), "actual": actual.get(t)}
        for t in sorted(set(expected) | set(actual))
        if expected.get(t) != actual.get(t)
    }
    return f"mismatched={mismatched} missing_tables={missing} extra_tables={extra}"


def _load_manifest(backup_path: Path, manifest_path) -> tuple[Path, dict]:
    resolved = Path(manifest_path) if manifest_path is not None else backup_path.with_suffix(".json")
    if not resolved.is_file():
        raise FileNotFoundError(f"manifest not found: {resolved}")
    try:
        manifest = json.loads(resolved.read_text())
    except json.JSONDecodeError as e:
        raise RuntimeError(f"manifest is not valid JSON: {resolved}: {e}") from e
    expected_file = manifest.get("backup_file")
    if expected_file is not None and expected_file != backup_path.name:
        raise RuntimeError(
            f"manifest/backup mismatch: {resolved} names backup_file={expected_file!r}, "
            f"but the backup being checked is {backup_path.name!r}"
        )
    return resolved, manifest


def _check_against_manifest(conn: sqlite3.Connection, manifest: dict) -> dict:
    """Runs the shared validation tail (integrity/FK, schema_version, exact
    per-table row-count accounting) against an already-open connection.
    Returns the accounting dict on success; raises RuntimeError on any
    mismatch, always naming the discrepancy."""
    _validate_database(conn)
    missing = [k for k in ("schema_version", "tables") if k not in manifest]
    if missing:
        raise RuntimeError(f"manifest missing required key(s): {missing}")
    actual_version = _schema_version(conn)
    expected_version = manifest["schema_version"]
    if actual_version != expected_version:
        raise RuntimeError(
            f"schema_version mismatch: backup has {actual_version!r}, "
            f"manifest says {expected_version!r}"
        )
    actual_tables = _row_counts(conn)
    expected_tables = manifest["tables"]
    if actual_tables != expected_tables:
        raise RuntimeError(
            f"row-count accounting mismatch: {_mismatch_detail(expected_tables, actual_tables)}"
        )
    return {
        "schema_version": actual_version,
        "tables": actual_tables,
        "row_count_total": sum(actual_tables.values()),
    }


def restore(backup_path, dest_path, manifest_path=None) -> dict:
    """Restores `backup_path` into `dest_path`, which must NOT already exist
    (fresh-destination only -- see module docstring). Validates the restored
    copy against the sidecar manifest (or `manifest_path` if given) and
    returns the full accounting. On any failure the freshly-written
    `dest_path` is removed; `backup_path` and any pre-existing `dest_path` are
    never touched."""
    backup_path = Path(backup_path)
    dest_path = Path(dest_path)
    if not backup_path.is_file():
        raise FileNotFoundError(f"backup file not found: {backup_path}")
    # `exists()` alone follows symlinks and reports False for a dangling one,
    # which would let the connect() below create-and-write through it --
    # writing to wherever the symlink points, not to dest_path. Refuse both
    # an existing destination and a symlinked one, dangling or not.
    if dest_path.exists() or dest_path.is_symlink():
        raise FileExistsError(
            f"refusing to restore over an existing path: {dest_path} "
            "(restore only ever writes a fresh destination -- move or delete "
            "it yourself first if that is really what you want)"
        )
    manifest_path, manifest = _load_manifest(backup_path, manifest_path)

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    src = None
    dst = None
    try:
        src = _open_ro(backup_path)
        dst = sqlite3.connect(str(dest_path))
        dst.row_factory = sqlite3.Row
        dst.execute("PRAGMA foreign_keys=ON")
        src.backup(dst)
        # Validates dst -- the just-restored destination -- never src (the
        # backup file being restored from). That is the entire point of a
        # restore's post-copy check: prove what actually landed at dest_path,
        # not what was already known-good about the backup.
        accounting = _check_against_manifest(dst, manifest)
    except RuntimeError as e:
        # A genuine validation failure (integrity/FK/schema_version/row-count
        # mismatch) from _check_against_manifest / _validate_database.
        if dst is not None:
            dst.close()
        if src is not None:
            src.close()
        _remove_partial(dest_path)
        raise RuntimeError(f"restore failed validation: {e}") from e
    except Exception as e:
        # Anything else (I/O error, a non-database file tripping sqlite3's
        # own DatabaseError, etc.) is a restore failure but not, strictly, a
        # validation failure -- don't misreport it as one.
        if dst is not None:
            dst.close()
        if src is not None:
            src.close()
        _remove_partial(dest_path)
        raise RuntimeError(f"restore failed: {e}") from e
    dst.close()
    src.close()

    return {
        "restored_path": str(dest_path.resolve()),
        "backup_path": str(backup_path.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        **accounting,
    }


def verify(backup_path, manifest_path=None) -> dict:
    """Runs the same checks `restore` runs (integrity, foreign keys,
    schema_version, exact row-count accounting) directly against
    `backup_path`, without copying it anywhere. Returns the accounting dict on
    success; raises RuntimeError naming the discrepancy on any failure."""
    backup_path = Path(backup_path)
    if not backup_path.is_file():
        raise FileNotFoundError(f"backup file not found: {backup_path}")
    manifest_path, manifest = _load_manifest(backup_path, manifest_path)

    conn = None
    try:
        conn = _open_ro(backup_path)
        accounting = _check_against_manifest(conn, manifest)
    except RuntimeError as e:
        if conn is not None:
            conn.close()
        raise RuntimeError(f"backup failed validation: {e}") from e
    except Exception as e:
        if conn is not None:
            conn.close()
        raise RuntimeError(f"backup verification failed: {e}") from e
    conn.close()

    return {
        "backup_path": str(backup_path.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        **accounting,
    }


# --------------------------------------------------------------------------- #
# CLI -- `python -m backend.backup {create|list|restore|verify}`, run from
# `webapp/` so `backend` resolves as a package with the server down.
# --------------------------------------------------------------------------- #
def _cli(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m backend.backup")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="create a validated backup + manifest")
    p_create.add_argument("db_path", nargs="?", default=None, help="default: config.DB_PATH")
    p_create.add_argument("--dest-dir", default=None, help="default: webapp/backups")

    p_list = sub.add_parser("list", help="list backups newest first")
    p_list.add_argument("--dest-dir", default=None, help="default: webapp/backups")

    p_restore = sub.add_parser("restore", help="restore into a FRESH destination path")
    p_restore.add_argument("backup_path")
    p_restore.add_argument("dest_path")
    p_restore.add_argument("--manifest", default=None, help="default: <backup>.json")

    p_verify = sub.add_parser("verify", help="validate a backup file standalone")
    p_verify.add_argument("backup_path")
    p_verify.add_argument("--manifest", default=None, help="default: <backup>.json")

    args = parser.parse_args(argv)
    try:
        if args.cmd == "create":
            result = create_backup(args.db_path, args.dest_dir)
        elif args.cmd == "list":
            result = list_backups(args.dest_dir)
        elif args.cmd == "restore":
            result = restore(args.backup_path, args.dest_path, args.manifest)
        elif args.cmd == "verify":
            result = verify(args.backup_path, args.manifest)
        else:  # pragma: no cover - argparse enforces choices via subparsers
            raise ValueError(f"unknown command: {args.cmd}")
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
