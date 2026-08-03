"""Explicit importer for the two supported legacy identity artifacts."""
import argparse
import base64
import hashlib
import json
import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path


ARTIFACTS = {
    "resolutions.jsonl": "resolutions",
    "seen.jsonl": "seen",
}
_IMPORT_NAMESPACE = uuid.UUID("ea7d01a8-a4aa-5be4-897a-ce2e2163ed13")


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _hash_json(value) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_current_schema(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT 1 FROM schema_version WHERE version=12"
    ).fetchone()
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='legacy_artifact_imports'"
    ).fetchone()
    if row is None or table is None:
        raise RuntimeError("legacy artifact import requires schema version 12")


def _raw_payload(raw_line: bytes, parsed=None) -> dict:
    payload = {"raw_base64": base64.b64encode(raw_line).decode("ascii")}
    if parsed is not None:
        payload["parsed"] = parsed
    return payload


def _archive(conn, kind, artifact_hash, line_number, payload, reason, candidates=()) -> None:
    locator = f"{artifact_hash}:{line_number}"
    payload_json = _canonical_json(payload)
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    archive_id = str(uuid.uuid5(
        _IMPORT_NAMESPACE, _canonical_json(["archive", kind, locator, payload_hash])
    ))
    conn.execute(
        "INSERT OR IGNORE INTO identity_migration_archive "
        "(archive_id,artifact,locator,payload_json,reason,candidate_posting_ids_json,"
        "payload_hash,archived_at) VALUES (?,?,?,?,?,?,?,?)",
        (archive_id, f"legacy-{kind}", locator, payload_json, reason,
         _canonical_json(sorted(set(candidates))) if candidates else None,
         payload_hash, _now()),
    )


def _alias_candidates(conn, url) -> list[str]:
    if not isinstance(url, str) or not url.strip():
        return []
    return [row[0] for row in conn.execute(
        "SELECT DISTINCT posting_id FROM posting_aliases "
        "WHERE namespace='legacy-url' AND valid_to IS NULL AND (value=? OR url=?) "
        "ORDER BY posting_id",
        (url, url),
    )]


def _insert_evidence(conn, posting_id, evidence_kind, payload, observed_at) -> str:
    evidence_json = _canonical_json(payload)
    evidence_hash = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
    existing = conn.execute(
        "SELECT posting_id FROM identity_evidence WHERE evidence_hash=?",
        (evidence_hash,),
    ).fetchone()
    if existing is not None:
        return "duplicate" if existing[0] == posting_id else "conflict"
    evidence_id = str(uuid.uuid5(_IMPORT_NAMESPACE, f"evidence:{evidence_hash}"))
    conn.execute(
        "INSERT OR IGNORE INTO identity_evidence "
        "(evidence_id,posting_id,evidence_kind,evidence_json,evidence_hash,observed_at) "
        "VALUES (?,?,?,?,?,?)",
        (evidence_id, posting_id, evidence_kind, evidence_json, evidence_hash, observed_at),
    )
    return "inserted"


def _redirect_target(conn, posting_id) -> tuple[str | None, bool]:
    """Return terminal posting and whether an existing redirect cycle was found."""
    seen = set()
    current = posting_id
    while current not in seen:
        seen.add(current)
        row = conn.execute(
            "SELECT to_posting_id FROM posting_redirects WHERE from_posting_id=?", (current,)
        ).fetchone()
        if row is None:
            return current, False
        current = row[0]
    return None, True


def _resolution_payload(record, artifact_hash, line_number) -> dict:
    return {
        "artifact_hash": artifact_hash,
        "resolution": {key: record.get(key) for key in (
            "agg_url", "canonical_url", "ats", "matched_title", "sim"
        )},
    }


def _import_resolution(conn, record, raw_line, artifact_hash, line_number) -> str:
    agg_url = record.get("agg_url")
    canonical_url = record.get("canonical_url")
    if not isinstance(agg_url, str) or not agg_url.strip() or not isinstance(
        canonical_url, str
    ) or not canonical_url.strip():
        _archive(conn, "resolutions", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "malformed resolution")
        return "malformed"

    agg_candidates = _alias_candidates(conn, agg_url)
    canonical_candidates = _alias_candidates(conn, canonical_url)
    candidates = sorted(set(agg_candidates + canonical_candidates))
    if len(agg_candidates) != 1 or len(canonical_candidates) != 1:
        _archive(conn, "resolutions", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "URL resolution was not unique", candidates)
        return "archived"

    agg_posting = agg_candidates[0]
    canonical_posting = canonical_candidates[0]
    agg_target, agg_cycle = _redirect_target(conn, agg_posting)
    canonical_target, canonical_cycle = _redirect_target(conn, canonical_posting)
    if agg_cycle or canonical_cycle or agg_target is None or canonical_target is None:
        _archive(conn, "resolutions", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "existing redirect cycle", candidates)
        return "archived"
    if canonical_target == agg_posting and agg_posting != canonical_posting:
        _archive(conn, "resolutions", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "resolution would create redirect cycle", candidates)
        return "archived"

    existing_redirect = None
    if agg_posting != canonical_target:
        existing_redirect = conn.execute(
            "SELECT to_posting_id FROM posting_redirects WHERE from_posting_id=?",
            (agg_posting,),
        ).fetchone()
        if existing_redirect is not None and agg_target != canonical_target:
            _archive(conn, "resolutions", artifact_hash, line_number,
                     _raw_payload(raw_line, record), "existing redirect conflicts", candidates)
            return "archived"

    payload = _resolution_payload(record, artifact_hash, line_number)
    evidence_outcome = _insert_evidence(
        conn, canonical_target, "legacy-resolution", payload, _now()
    )
    if evidence_outcome == "conflict":
        _archive(conn, "resolutions", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "resolution evidence ownership conflicts",
                 candidates)
        return "archived"

    redirect_inserted = False
    if agg_posting != canonical_target and existing_redirect is None:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO posting_redirects "
            "(from_posting_id,to_posting_id,reason,created_at) VALUES (?,?,'legacy resolver',?)",
            (agg_posting, canonical_target, _now()),
        )
        redirect_inserted = cursor.rowcount == 1
    return "mapped" if evidence_outcome == "inserted" or redirect_inserted else "duplicate"


def _lineage_candidates(conn, url, seen_key) -> list[str]:
    exact_value = _canonical_json([url, seen_key])
    exact = [row[0] for row in conn.execute(
        "SELECT posting_id FROM legacy_identity_map "
        "WHERE legacy_identity_kind='lineage' AND namespace='legacy-db' "
        "AND legacy_identity_value=? ORDER BY posting_id",
        (exact_value,),
    )]
    if exact:
        candidates = exact
    else:
        candidates = set()
        for row in conn.execute(
            "SELECT posting_id,evidence_json FROM identity_evidence "
            "WHERE evidence_kind='legacy-lineage'"
        ):
            try:
                evidence = json.loads(row["evidence_json"])
            except (TypeError, ValueError):
                continue
            if isinstance(evidence, dict) and evidence.get("seen_key") == seen_key:
                candidates.add(row["posting_id"])

    resolved = set()
    for candidate in candidates:
        target, cycle = _redirect_target(conn, candidate)
        if cycle or target is None:
            return []
        resolved.add(target)
    return sorted(resolved)


def _isoish(value) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        if "T" in value or " " in value:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _parsed_instant(value):
    if not _isoish(value):
        return None
    if "T" not in value and " " not in value:
        parsed = datetime.combine(date.fromisoformat(value), datetime.min.time())
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _import_seen(conn, record, raw_line, artifact_hash, line_number) -> str:
    seen_key = record.get("key")
    url = record.get("url")
    first_seen = record.get("first_seen")
    if (not isinstance(seen_key, str) or not seen_key.strip()
            or not isinstance(url, str) or not url.strip()
            or (first_seen is not None and not _isoish(first_seen))):
        _archive(conn, "seen", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "malformed seen record")
        return "malformed"

    candidates = _lineage_candidates(conn, url, seen_key)
    if len(candidates) != 1:
        _archive(conn, "seen", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "seen identity was not unique", candidates)
        return "archived"

    payload = {
        "artifact_hash": artifact_hash,
        "seen": {"key": seen_key, "first_seen": first_seen, "url": url},
    }
    evidence_outcome = _insert_evidence(
        conn, candidates[0], "legacy-seen", payload, first_seen or _now()
    )
    if evidence_outcome == "conflict":
        _archive(conn, "seen", artifact_hash, line_number,
                 _raw_payload(raw_line, record), "seen evidence ownership conflicts", candidates)
        return "archived"
    if first_seen is not None:
        current = conn.execute(
            "SELECT first_seen_at FROM postings WHERE posting_id=?", (candidates[0],)
        ).fetchone()[0]
        incoming_at = _parsed_instant(first_seen)
        current_at = _parsed_instant(current)
        if current_at is None or (incoming_at is not None and incoming_at < current_at):
            conn.execute(
                "UPDATE postings SET first_seen_at=? WHERE posting_id=?",
                (first_seen, candidates[0]),
            )
    return "mapped" if evidence_outcome == "inserted" else "duplicate"


def _import_line(conn, kind, raw_line, artifact_hash, line_number) -> str:
    try:
        record = json.loads(raw_line)
    except (UnicodeDecodeError, json.JSONDecodeError):
        _archive(conn, kind, artifact_hash, line_number, _raw_payload(raw_line),
                 "malformed JSON line")
        return "malformed"
    if not isinstance(record, dict):
        _archive(conn, kind, artifact_hash, line_number, _raw_payload(raw_line, record),
                 "JSON line is not an object")
        return "malformed"
    if kind == "resolutions":
        return _import_resolution(conn, record, raw_line, artifact_hash, line_number)
    return _import_seen(conn, record, raw_line, artifact_hash, line_number)


def _persisted_summary(row) -> dict:
    return {
        "artifact_kind": row["artifact_kind"],
        "artifact_hash": row["artifact_hash"],
        "status": row["status"],
        "processed": row["processed_count"],
        "mapped": row["mapped_count"],
        "duplicate": row["duplicate_count"],
        "malformed": row["malformed_count"],
        "archived": row["archived_count"],
    }


def import_legacy_artifacts(conn: sqlite3.Connection, results_path) -> dict:
    """Import supported artifacts into an already-current connection atomically."""
    if conn.in_transaction:
        raise RuntimeError("legacy artifact import requires a connection with no active transaction")
    _require_current_schema(conn)
    results = Path(results_path)
    summaries = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        for filename, kind in ARTIFACTS.items():
            path = results / filename
            if not path.is_file():
                continue
            content = path.read_bytes()
            artifact_hash = hashlib.sha256(content).hexdigest()
            existing = conn.execute(
                "SELECT * FROM legacy_artifact_imports "
                "WHERE artifact_kind=? AND artifact_hash=? AND status='succeeded'",
                (kind, artifact_hash),
            ).fetchone()
            if existing is not None:
                summaries.append(_persisted_summary(existing))
                continue

            import_id = str(uuid.uuid5(_IMPORT_NAMESPACE, f"{kind}:{artifact_hash}"))
            idempotency_key = f"{kind}:{artifact_hash}"
            started_at = _now()
            conn.execute(
                "INSERT INTO legacy_artifact_imports "
                "(import_id,artifact_kind,artifact_path,artifact_hash,idempotency_key,status,"
                "started_at) VALUES (?,?,?,?,?,'running',?)",
                (import_id, kind, str(path), artifact_hash, idempotency_key, started_at),
            )
            counts = {name: 0 for name in (
                "processed", "mapped", "duplicate", "malformed", "archived"
            )}
            for line_number, raw_line in enumerate(content.splitlines(), 1):
                if not raw_line.strip():
                    continue
                counts["processed"] += 1
                outcome = _import_line(conn, kind, raw_line, artifact_hash, line_number)
                counts[outcome] += 1
            accounted = sum(counts[name] for name in (
                "mapped", "duplicate", "malformed", "archived"
            ))
            if counts["processed"] != accounted:
                raise RuntimeError(
                    f"legacy import accounting mismatch: {counts['processed']} processed, "
                    f"{accounted} accounted"
                )
            conn.execute(
                "UPDATE legacy_artifact_imports SET status='succeeded',finished_at=?,"
                "processed_count=?,mapped_count=?,duplicate_count=?,malformed_count=?,"
                "archived_count=?,counts_json=? WHERE import_id=?",
                (_now(), counts["processed"], counts["mapped"], counts["duplicate"],
                 counts["malformed"], counts["archived"], _canonical_json(counts), import_id),
            )
            summaries.append({
                "artifact_kind": kind, "artifact_hash": artifact_hash,
                "status": "succeeded", **counts,
            })
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"artifacts": summaries}


def audit_legacy_artifacts(conn: sqlite3.Connection, results_path) -> dict:
    """Report persisted ledger state and prospective ambiguity without writes."""
    _require_current_schema(conn)
    results = Path(results_path)
    ledger = [_persisted_summary(row) for row in conn.execute(
        "SELECT * FROM legacy_artifact_imports ORDER BY started_at,import_id"
    )]
    artifacts = []
    for filename, kind in ARTIFACTS.items():
        path = results / filename
        if not path.is_file():
            continue
        content = path.read_bytes()
        artifact_hash = hashlib.sha256(content).hexdigest()
        counts = {"processed": 0, "malformed": 0, "unknown": 0, "ambiguous": 0}
        for raw_line in content.splitlines():
            if not raw_line.strip():
                continue
            counts["processed"] += 1
            try:
                record = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError):
                counts["malformed"] += 1
                continue
            if not isinstance(record, dict):
                counts["malformed"] += 1
                continue
            if kind == "resolutions":
                urls = (record.get("agg_url"), record.get("canonical_url"))
                if any(not isinstance(url, str) or not url.strip() for url in urls):
                    counts["malformed"] += 1
                    continue
                sizes = [len(_alias_candidates(conn, url)) for url in urls]
                if 0 in sizes:
                    counts["unknown"] += 1
                elif any(size > 1 for size in sizes):
                    counts["ambiguous"] += 1
            else:
                seen_key, url, first_seen = (
                    record.get("key"), record.get("url"), record.get("first_seen")
                )
                if (not isinstance(seen_key, str) or not seen_key.strip()
                        or not isinstance(url, str) or not url.strip()
                        or (first_seen is not None and not _isoish(first_seen))):
                    counts["malformed"] += 1
                    continue
                size = len(_lineage_candidates(conn, url, seen_key))
                if size == 0:
                    counts["unknown"] += 1
                elif size > 1:
                    counts["ambiguous"] += 1
        persisted = conn.execute(
            "SELECT status,processed_count,mapped_count,duplicate_count,malformed_count,"
            "archived_count FROM legacy_artifact_imports "
            "WHERE artifact_kind=? AND artifact_hash=?",
            (kind, artifact_hash),
        ).fetchone()
        artifacts.append({
            "artifact_kind": kind,
            "artifact_hash": artifact_hash,
            **counts,
            "persisted": dict(persisted) if persisted is not None else None,
        })
    return {"ledger": ledger, "artifacts": artifacts}


def _read_only_connection(db_path) -> sqlite3.Connection:
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Import legacy identity artifacts")
    parser.add_argument("--db", required=True)
    parser.add_argument("--results", required=True)
    parser.add_argument("--audit-only", action="store_true")
    args = parser.parse_args(argv)

    if args.audit_only:
        conn = _read_only_connection(args.db)
        try:
            report = audit_legacy_artifacts(conn, args.results)
        finally:
            conn.close()
    else:
        from ..db import connect
        conn = connect(args.db)
        try:
            report = import_legacy_artifacts(conn, args.results)
        finally:
            conn.close()
    print(_canonical_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())