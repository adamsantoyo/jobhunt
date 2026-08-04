"""Read-only cutover-readiness audit for the canonical pipeline schema."""
import argparse
import base64
import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

from . import repositories


_REQUIRED_TABLES = {
    "schema_version", "jobs", "runs", "job_history", "job_state", "state_events",
    "postings", "posting_aliases", "posting_redirects", "legacy_identity_map",
    "identity_migration_archive", "pipeline_runs", "posting_versions", "run_postings",
    "legacy_artifact_imports",
}
_REQUIRED_VIEWS = {"compat_jobs", "compat_runs", "compat_job_history"}
_JOB_FIELDS = (
    "url", "seen_key", "tier", "odds", "odds_score", "odds_why", "is_new",
    "title", "company", "location", "salary", "salary_min", "salary_max", "posted",
    "first_seen", "remote", "source", "also_seen_on", "req_id", "why", "flags",
    "desc_snippet", "full_desc", "latest_run", "present",
)
_RUN_FIELDS = (
    "run_date", "kept", "new_this_run", "report_json", "source_health_json", "ingested_at",
)
_IMPORT_STATUSES = {"running", "succeeded", "failed"}
_IMPORT_KINDS = {"resolutions", "seen"}


def _require_current_schema(conn):
    objects = defaultdict(set)
    for row in conn.execute(
        "SELECT type,name FROM sqlite_master WHERE type IN ('table','view')"
    ):
        objects[row["type"]].add(row["name"])
    if not _REQUIRED_TABLES <= objects["table"] or not _REQUIRED_VIEWS <= objects["view"]:
        raise RuntimeError("pipeline audit requires the complete schema version 13 database")
    row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    if row is None or row[0] is None or int(row[0]) < 13:
        # v12 predates run_postings.membership_kind; auditing it would report
        # history-parity blockers that read as data loss rather than a stale schema.
        raise RuntimeError("pipeline audit requires schema version 13 or newer")


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _instant(value):
    if not _text(value):
        return None
    try:
        if "T" not in value and " " not in value:
            parsed = datetime.combine(date.fromisoformat(value), datetime.min.time())
        else:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _earliest(left, right):
    left_at, right_at = _instant(left), _instant(right)
    if left_at is None or right_at is None:
        return min(left, right)
    return left if left_at <= right_at else right


def _lineage_value(url, seen_key):
    return json.dumps([url, seen_key], separators=(",", ":"), ensure_ascii=True)


def _lineage_pair(value):
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    if (not isinstance(parsed, list) or len(parsed) != 2
            or not _text(parsed[0]) or not _text(parsed[1])):
        return None
    return parsed[0], parsed[1]


def _canonical_json(value):
    def encode_special(item):
        if isinstance(item, bytes):
            return {"$type": "bytes", "base64": base64.b64encode(item).decode("ascii")}
        raise TypeError
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      default=encode_special)


def _payload_hash(value):
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _archive_index(rows):
    by_artifact = defaultdict(Counter)
    for row in rows:
        by_artifact[row["artifact"]][(row["locator"], row["payload_hash"])] += 1
    return by_artifact


def _count_accounted(keys, available):
    remaining = available.copy()
    accounted = 0
    for key in keys:
        if remaining[key] > 0:
            remaining[key] -= 1
            accounted += 1
    return accounted


def _parity(expected_rows, actual_rows, key_fields, fields):
    expected_keys = [tuple(row[field] for field in key_fields) for row in expected_rows]
    actual_keys = [tuple(row[field] for field in key_fields) for row in actual_rows]
    expected = {key: row for key, row in zip(expected_keys, expected_rows)}
    actual = {key: row for key, row in zip(actual_keys, actual_rows)}
    shared = sorted(set(expected) & set(actual), key=repr)
    mismatches = {
        field: sum(expected[key].get(field) != actual[key].get(field) for key in shared)
        for field in fields
    }
    return {
        "missing_canonical": len(set(expected) - set(actual)),
        "unexpected_canonical": len(set(actual) - set(expected)),
        "duplicate_expected_key_count": len(expected_keys) - len(set(expected_keys)),
        "duplicate_actual_key_count": len(actual_keys) - len(set(actual_keys)),
        "field_mismatches": {field: count for field, count in mismatches.items() if count},
    }


def _redirect_cycle_count(rows):
    redirects = {row["from_posting_id"]: row["to_posting_id"] for row in rows}
    visited = set()
    cycles = set()
    for start in sorted(redirects):
        path = []
        positions = {}
        current = start
        while current in redirects and current not in visited:
            if current in positions:
                cycles.add(frozenset(path[positions[current]:]))
                break
            positions[current] = len(path)
            path.append(current)
            current = redirects[current]
        visited.update(path)
    return len(cycles)


def _identity_report(legacy_jobs, legacy_history, lineage_map, postings, aliases, redirects):
    active_aliases = defaultdict(set)
    active_urls = defaultdict(set)
    all_urls = defaultdict(set)
    posting_ids = set(postings)
    orphan_aliases = 0
    for row in aliases:
        if row["posting_id"] not in posting_ids:
            orphan_aliases += 1
        if row["url"] is not None:
            all_urls[row["url"]].add(row["posting_id"])
        if row["valid_to"] is None:
            active_aliases[(row["alias_kind"], row["namespace"], row["value"])].add(
                row["posting_id"]
            )
            if row["url"] is not None:
                active_urls[row["url"]].add(row["posting_id"])

    observed = defaultdict(list)
    for row in legacy_jobs:
        pair = (row["url"], row["seen_key"])
        for field in ("first_seen", "latest_run"):
            if _text(row[field]):
                observed[pair].append(row[field])
    for row in legacy_history:
        if _text(row["run_date"]):
            observed[(row["url"], row["seen_key"])].append(row["run_date"])
    moved_later = set()
    for pair, values in observed.items():
        posting_id = lineage_map.get(pair)
        posting = postings.get(posting_id)
        observed_first = values[0]
        for value in values[1:]:
            observed_first = _earliest(observed_first, value)
        if (posting is not None
                and _earliest(posting["first_seen_at"], observed_first)
                != posting["first_seen_at"]):
            moved_later.add(posting_id)

    return {
        "active_alias_conflict_count": sum(len(ids) > 1 for ids in active_aliases.values()),
        "redirect_cycle_count": _redirect_cycle_count(redirects),
        "ambiguous_active_url_count": sum(len(ids) > 1 for ids in active_urls.values()),
        "recycled_url_count": sum(len(ids) > 1 for ids in all_urls.values()),
        "orphan_alias_count": orphan_aliases,
        "first_seen_moved_later_count": len(moved_later),
    }


def _imports_report(rows):
    by_status = Counter()
    by_kind = Counter()
    violations = 0
    incomplete = 0
    for row in rows:
        status = row["status"] if row["status"] in _IMPORT_STATUSES else "other"
        kind = row["artifact_kind"] if row["artifact_kind"] in _IMPORT_KINDS else "other"
        by_status[status] += 1
        if status != "succeeded":
            incomplete += 1
        by_kind[kind] += 1
        accounted = sum(row[field] for field in (
            "mapped_count", "duplicate_count", "malformed_count", "archived_count"
        ))
        if row["processed_count"] != accounted:
            violations += 1
    return {
        "ledger_count": len(rows),
        "counts_by_status": dict(sorted(by_status.items())),
        "counts_by_artifact_kind": dict(sorted(by_kind.items())),
        "accounting_violation_count": violations,
        "incomplete_count": incomplete,
    }


def _has_parity_failure(section):
    return bool(
        section["missing_canonical"]
        or section["unexpected_canonical"]
        or section["duplicate_expected_key_count"]
        or section["duplicate_actual_key_count"]
        or section["field_mismatches"]
    )


def build_audit_report(conn):
    """Build a JSON-safe aggregate report without mutating the connection."""
    _require_current_schema(conn)
    integrity_rows = [row[0] for row in conn.execute("PRAGMA integrity_check")]
    integrity = "ok" if integrity_rows == ["ok"] else "failed"
    foreign_key_violations = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    schema_version = max(row["version"] for row in repositories.read_schema_versions(conn))

    legacy_jobs = repositories.read_legacy_jobs(conn)
    legacy_runs = repositories.read_legacy_runs(conn)
    legacy_history = repositories.read_legacy_history(conn)
    legacy_state = repositories.read_legacy_state(conn)
    legacy_events = repositories.read_legacy_events(conn)
    compat_jobs = repositories.read_compat_jobs(conn)
    compat_runs = repositories.read_compat_runs(conn)
    compat_history = repositories.read_compat_history(conn)
    mapping_rows = repositories.read_lineage_mappings(conn)
    posting_rows = repositories.read_postings(conn)
    alias_rows = repositories.read_posting_aliases(conn)
    redirect_rows = repositories.read_posting_redirects(conn)
    run_posting_rows = repositories.read_run_posting_ownership(conn)
    archive_rows = repositories.read_identity_archives(conn)
    import_rows = repositories.read_import_ledger(conn)

    postings = {row["posting_id"]: row for row in posting_rows}
    mapping_pairs = [(_lineage_pair(row["legacy_identity_value"]), row["posting_id"])
                     for row in mapping_rows]
    grouped_mappings = defaultdict(set)
    for pair, posting_id in mapping_pairs:
        if pair is not None:
            grouped_mappings[pair].add(posting_id)
    ambiguous_mapping_count = sum(len(ids) != 1 for ids in grouped_mappings.values())
    lineage_map = {pair: next(iter(ids)) for pair, ids in grouped_mappings.items() if len(ids) == 1}
    legacy_pairs = {
        (row["url"], row["seen_key"])
        for row in list(legacy_jobs) + list(legacy_history)
    }
    valid_pairs = {pair for pair in legacy_pairs if _text(pair[0]) and _text(pair[1])}
    malformed_pairs = legacy_pairs - valid_pairs
    archives = _archive_index(archive_rows)
    malformed_lineage_keys = [
        (_lineage_value(*pair), _payload_hash({"url": pair[0], "seen_key": pair[1]}))
        for pair in malformed_pairs
    ]
    malformed_lineage_accounted = _count_accounted(
        malformed_lineage_keys, archives["lineage"]
    )
    mapped_valid_pairs = {
        pair for pair in valid_pairs
        if pair in lineage_map and lineage_map[pair] in postings
    }
    extra_mapping_count = sum(
        pair is None or pair not in valid_pairs for pair, _posting_id in mapping_pairs
    )
    missing_mapping_count = len(valid_pairs - mapped_valid_pairs)
    orphan_mapping_count = sum(
        pair in valid_pairs and posting_id not in postings
        for pair, posting_id in mapping_pairs if pair is not None
    )
    mapped_posting_ids = {lineage_map[pair] for pair in mapped_valid_pairs}
    posting_collision_count = len(mapped_valid_pairs) - len(mapped_posting_ids)
    lineage_unexplained = (
        missing_mapping_count + extra_mapping_count + posting_collision_count
        + ambiguous_mapping_count
        + len(malformed_pairs) - malformed_lineage_accounted
    )

    malformed_jobs = [row for row in legacy_jobs
                      if not _text(row["url"]) or not _text(row["seen_key"])]
    malformed_job_keys = [(f"url:{row['url']}", _payload_hash(dict(row))) for row in malformed_jobs]
    malformed_jobs_accounted = _count_accounted(malformed_job_keys, archives["jobs"])
    expected_job_rows = []
    for row in legacy_jobs:
        pair = (row["url"], row["seen_key"])
        if pair not in lineage_map:
            continue
        expected = {field: row[field] for field in _JOB_FIELDS}
        expected["seen_key"] = lineage_map[pair]
        expected_job_rows.append(expected)
    actual_job_rows = [{field: row[field] for field in _JOB_FIELDS} for row in compat_jobs]
    jobs_parity = _parity(expected_job_rows, actual_job_rows, ("seen_key",), _JOB_FIELDS)
    current_unexplained = (
        max(0, len(malformed_jobs) - malformed_jobs_accounted)
        + jobs_parity["missing_canonical"] + jobs_parity["unexpected_canonical"]
    )

    expected_run_rows = [{field: row[field] for field in _RUN_FIELDS} for row in legacy_runs]
    actual_run_rows = [{field: row[field] for field in _RUN_FIELDS} for row in compat_runs]
    runs_parity = _parity(expected_run_rows, actual_run_rows, ("run_date",), _RUN_FIELDS)

    malformed_history = [row for row in legacy_history
                         if not _text(row["url"]) or not _text(row["seen_key"])]
    malformed_history_keys = [
        (f"rowid:{row['legacy_rowid']}", _payload_hash(dict(row))) for row in malformed_history
    ]
    malformed_history_accounted = _count_accounted(
        malformed_history_keys, archives["job_history"]
    )
    expected_history_rows = []
    for row in legacy_history:
        pair = (row["url"], row["seen_key"])
        if pair not in lineage_map:
            continue
        expected_history_rows.append({
            "run_date": row["run_date"], "posting_id": lineage_map[pair],
            "tier": row["tier"], "odds": row["odds"], "present": row["present"],
        })
    actual_history_rows = [{
        "run_date": row["run_date"], "posting_id": row["seen_key"],
        "tier": row["tier"], "odds": row["odds"], "present": row["present"],
    } for row in compat_history]
    history_parity = _parity(
        expected_history_rows, actual_history_rows, ("run_date", "posting_id"),
        ("posting_id", "tier", "odds", "present"),
    )
    history_unexplained = (
        max(0, len(malformed_history) - malformed_history_accounted)
        + history_parity["missing_canonical"] + history_parity["unexpected_canonical"]
    )

    posting_ids_by_seen_key = defaultdict(set)
    for (url, seen_key), posting_id in lineage_map.items():
        if (url, seen_key) in valid_pairs:
            posting_ids_by_seen_key[seen_key].add(posting_id)

    def state_event_conservation(rows, artifact, locator_field, locator_prefix):
        unmapped = [row for row in rows if row["posting_id"] is None]
        invalid = sum(
            row["posting_id"] is not None
            and posting_ids_by_seen_key[row["seen_key"]] != {row["posting_id"]}
            for row in rows
        )
        keys = [
            (f"{locator_prefix}:{row[locator_field]}", _payload_hash(dict(row)))
            for row in unmapped
        ]
        archived = _count_accounted(keys, archives[artifact])
        return {
            "row_count": len(rows),
            "mapped_count": len(rows) - len(unmapped),
            "unmapped_count": len(unmapped),
            "unmapped_archive_count": archived,
            "invalid_mapping_count": invalid,
            "unexplained_count": len(unmapped) - archived + invalid,
        }

    state_conservation = state_event_conservation(
        legacy_state, "job_state", "legacy_rowid", "rowid"
    )
    event_conservation = state_event_conservation(
        legacy_events, "state_event", "id", "id"
    )
    identity = _identity_report(
        legacy_jobs, legacy_history, lineage_map, postings, alias_rows, redirect_rows
    )
    imports = _imports_report(import_rows)

    owned_postings = {row["posting_id"] for row in mapping_rows}
    owned_postings.update(row["posting_id"] for row in alias_rows)
    owned_postings.update(row["posting_id"] for row in conn.execute(
        "SELECT DISTINCT posting_id FROM posting_versions"
    ))
    owned_postings.update(row["posting_id"] for row in run_posting_rows)
    orphan_posting_count = len(set(postings) - owned_postings)

    expected_memberships = {
        (row["run_date"], lineage_map[(row["url"], row["seen_key"])])
        for row in legacy_history if (row["url"], row["seen_key"]) in lineage_map
    }
    expected_memberships.update(
        ((row["latest_run"] or row["first_seen"]), lineage_map[(row["url"], row["seen_key"])])
        for row in legacy_jobs if (row["url"], row["seen_key"]) in lineage_map
    )
    actual_memberships = {
        (row["legacy_run_date"], row["posting_id"])
        for row in run_posting_rows
        # LEGACY runs only. `expected_memberships` is built from `job_history` and
        # `jobs`, both keyed by a legacy run DATE, so it can only ever describe runs
        # that came from the legacy database. A canonical run — anything the scheduler
        # executes — has no `legacy_run_date` at all, and counting its memberships
        # here reports every posting the scheduler ever discovered as an unexplained
        # membership: not data loss, just a Phase 1 parity check being asked a Phase 2
        # question. Orphan detection above deliberately still counts these rows, since
        # a scheduler-discovered posting IS owned by its run.
        if row["legacy_run_date"] is not None
    }
    unexplained_membership_count = len(actual_memberships - expected_memberships)

    conservation = {
        "lineage": {
            "legacy_distinct_valid_count": len(valid_pairs),
            "legacy_db_mapping_count": len(mapping_rows),
            "mapped_posting_count": len(mapped_posting_ids),
            "malformed_count": len(malformed_pairs),
            "malformed_archive_count": malformed_lineage_accounted,
            "unexplained_count": lineage_unexplained,
            "ambiguous_mapping_count": ambiguous_mapping_count,
            "orphan_posting_count": orphan_posting_count,
        },
        "current_jobs": {
            "legacy_count": len(legacy_jobs),
            "compat_count": len(compat_jobs),
            "malformed_archive_count": malformed_jobs_accounted,
            "unexplained_count": current_unexplained,
        },
        "history": {
            "legacy_count": len(legacy_history),
            "compat_count": len(compat_history),
            "malformed_archive_count": malformed_history_accounted,
            "unexplained_count": history_unexplained,
        },
        "state": state_conservation,
        "events": event_conservation,
        "run_postings": {
            "row_count": len(run_posting_rows),
            "unexplained_count": unexplained_membership_count,
        },
    }
    parity = {"current_jobs": jobs_parity, "runs": runs_parity, "history": history_parity}

    blockers = []
    if integrity != "ok":
        blockers.append("database_integrity_failed")
    if foreign_key_violations:
        blockers.append("foreign_key_violations")
    for name in ("lineage", "current_jobs", "history", "state", "events", "run_postings"):
        if conservation[name]["unexplained_count"]:
            blockers.append(f"{name}_conservation")
    for name, section in parity.items():
        if _has_parity_failure(section):
            blockers.append(f"{name}_parity")
    for field, code in (
        ("active_alias_conflict_count", "active_alias_conflicts"),
        ("redirect_cycle_count", "redirect_cycles"),
        ("ambiguous_active_url_count", "ambiguous_active_urls"),
        ("orphan_alias_count", "orphan_aliases"),
        ("first_seen_moved_later_count", "first_seen_moved_later"),
    ):
        if identity[field]:
            blockers.append(code)
    if imports["accounting_violation_count"]:
        blockers.append("import_accounting_violations")
    if imports["incomplete_count"]:
        blockers.append("incomplete_imports")
    if orphan_posting_count:
        blockers.append("orphan_postings")

    return {
        "database": {
            "integrity_check": integrity,
            "foreign_key_violation_count": foreign_key_violations,
            "schema_version": schema_version,
        },
        "conservation": conservation,
        "parity": parity,
        "identity": identity,
        "imports": imports,
        "readiness": {"ready": not blockers, "blockers": sorted(set(blockers))},
    }


def _read_only_connection(db_path):
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


class _PrivateArgumentParser(argparse.ArgumentParser):
    def error(self, _message):
        self.exit(2, "pipeline audit failed\n")


def main(argv=None):
    parser = _PrivateArgumentParser(description="Audit canonical pipeline cutover readiness")
    parser.add_argument("--db", required=True)
    args = parser.parse_args(argv)
    try:
        conn = _read_only_connection(args.db)
        try:
            report = build_audit_report(conn)
        finally:
            conn.close()
    except Exception:
        parser.exit(1, "pipeline audit failed\n")
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())