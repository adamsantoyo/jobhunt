import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from backend.db import connect, init_db
from backend.pipeline import repositories
from backend.pipeline.audit import build_audit_report
from backend.pipeline.audit import _parity
from backend.migrations import _migration_11_legacy_canonical_backfill


PRIVATE_URL = "https://private.example/person"
PRIVATE_KEY = "private-seen-key"
PRIVATE_TITLE = "PERSONAL JOB TITLE"
PRIVATE_COMPANY = "PRIVATE COMPANY"
PRIVATE_NOTES = "PERSONAL NOTES"


def _fresh_db(tmp_path):
    path = tmp_path / "audit.db"
    conn = connect(path)
    init_db(conn)
    return conn, path


def _backfilled_db(tmp_path, *, malformed=False, unmapped=False):
    conn, path = _fresh_db(tmp_path)
    conn.execute(
        "INSERT INTO jobs "
        "(url,seen_key,tier,odds,odds_score,odds_why,is_new,title,company,location,salary,"
        "salary_min,salary_max,posted,first_seen,remote,source,also_seen_on,req_id,why,flags,"
        "desc_snippet,full_desc,latest_run,present) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (PRIVATE_URL, PRIVATE_KEY, 4, "High", 90, "private rationale", 1,
         PRIVATE_TITLE, PRIVATE_COMPANY, "Private Place", "$1", 1, 2, "2026-07-01",
         "2026-07-01", 1, "private source", "private mirror", "private req",
         "private why", "private flags", "private snippet", "private description",
         "2026-07-01", 1),
    )
    conn.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?)",
        ("2026-07-01", 1, 1, '{"private":"report"}',
         '{"private":"health"}', "2026-07-01T01:00:00"),
    )
    conn.execute(
        "INSERT INTO job_history VALUES (?,?,?,?,?,?)",
        (PRIVATE_URL, "2026-07-01", PRIVATE_KEY, 4, "High", 1),
    )
    conn.execute(
        "INSERT INTO job_state (seen_key,url,status,notes,updated_at) VALUES (?,?,?,?,?)",
        (PRIVATE_KEY, PRIVATE_URL, "New", PRIVATE_NOTES, "2026-07-01T02:00:00"),
    )
    conn.execute(
        "INSERT INTO state_events (seen_key,url,field,new_value,at,source) VALUES (?,?,?,?,?,?)",
        (PRIVATE_KEY, PRIVATE_URL, "status", "New", "2026-07-01T02:00:00", "migration"),
    )
    if malformed:
        conn.execute("INSERT INTO jobs (url,seen_key,tier) VALUES (NULL,'',1)")
        conn.execute(
            "INSERT INTO job_history (url,run_date,seen_key,tier) VALUES ('','2026-06-01','',1)"
        )
    if unmapped:
        conn.execute(
            "INSERT INTO job_state (seen_key,status,notes,updated_at) VALUES (?,?,?,?)",
            ("unmapped-private-key", "New", "unmapped private notes", "2026-07-01"),
        )
        conn.execute(
            "INSERT INTO state_events (seen_key,field,new_value,at,source) VALUES (?,?,?,?,?)",
            ("unmapped-private-key", "status", "New", "2026-07-01", "migration"),
        )
    _migration_11_legacy_canonical_backfill(conn)
    conn.commit()
    return conn, path


def test_repository_reads_are_deterministic(tmp_path):
    conn, _ = _fresh_db(tmp_path)
    conn.executemany(
        "INSERT INTO jobs (url,seen_key,tier,title,company) VALUES (?,?,?,?,?)",
        [
            ("https://two", "key-two", 2, "Second", "Private Two"),
            ("https://one", "key-one", 1, "First", "Private One"),
        ],
    )
    conn.commit()

    rows = repositories.read_legacy_jobs(conn)

    assert [row["url"] for row in rows] == ["https://one", "https://two"]
    assert all(isinstance(row, sqlite3.Row) for row in rows)
    conn.close()


def test_fully_backfilled_database_is_ready_and_json_safe(tmp_path):
    conn, _ = _backfilled_db(tmp_path)

    report = build_audit_report(conn)
    encoded = json.dumps(report, sort_keys=True)

    assert report["readiness"] == {"ready": True, "blockers": []}
    assert report["database"] == {
        "integrity_check": "ok", "foreign_key_violation_count": 0, "schema_version": 14,
    }
    for private_value in (
        PRIVATE_URL, PRIVATE_KEY, PRIVATE_TITLE, PRIVATE_COMPANY, PRIVATE_NOTES,
        "private rationale", "private description",
    ):
        assert private_value not in encoded
    conn.close()


def test_current_field_mismatch_is_categorized_without_content_leakage(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute(
        "UPDATE posting_versions SET tier=1 WHERE version_kind='legacy-current'"
    )
    conn.commit()

    report = build_audit_report(conn)
    encoded = json.dumps(report)

    assert report["parity"]["current_jobs"]["field_mismatches"] == {"tier": 1}
    assert "current_jobs_parity" in report["readiness"]["blockers"]
    assert PRIVATE_TITLE not in encoded and PRIVATE_COMPANY not in encoded
    conn.close()


def test_current_missing_and_unexpected_rows_are_counted(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute(
        "UPDATE posting_versions SET version_kind='other' WHERE version_kind='legacy-current'"
    )
    conn.execute("INSERT INTO postings VALUES ('extra','active','2026-01-01','t0',NULL)")
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id,posting_id,alias_kind,namespace,value,url,valid_from) "
        "VALUES ('extra-alias','extra','url','native','extra','https://extra','2026-01-01')"
    )
    conn.execute(
        "INSERT INTO posting_versions "
        "(posting_version_id,posting_id,version_kind,version_hash,observed_at,tier,payload_json) "
        "VALUES ('extra-version','extra','source','extra-hash','2026-01-01',1,'{}')"
    )
    conn.commit()

    parity = build_audit_report(conn)["parity"]["current_jobs"]

    assert parity["missing_canonical"] == 1
    assert parity["unexpected_canonical"] == 1
    conn.close()


def test_run_and_history_field_parity_are_categorized(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute("UPDATE pipeline_runs SET kept_count=9 WHERE legacy_run_date='2026-07-01'")
    conn.execute("UPDATE run_postings SET present=0")
    conn.commit()

    parity = build_audit_report(conn)["parity"]

    assert parity["runs"]["field_mismatches"] == {"kept": 1}
    assert parity["history"]["field_mismatches"] == {"present": 1}
    conn.close()


def test_malformed_legacy_rows_are_accounted_by_archives(tmp_path):
    conn, _ = _backfilled_db(tmp_path, malformed=True)

    report = build_audit_report(conn)

    assert report["conservation"]["lineage"]["malformed_archive_count"] == 2
    assert report["conservation"]["current_jobs"]["malformed_archive_count"] == 1
    assert report["conservation"]["history"]["malformed_archive_count"] == 1
    assert report["readiness"]["ready"] is True
    conn.close()


def test_one_archive_cannot_account_for_two_identical_malformed_jobs(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute("INSERT INTO jobs (url,seen_key,tier) VALUES (NULL,'',1)")
    conn.execute("INSERT INTO jobs (url,seen_key,tier) VALUES (NULL,'',1)")
    conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    _migration_11_legacy_canonical_backfill(conn)
    conn.commit()

    report = build_audit_report(conn)

    assert report["conservation"]["current_jobs"]["malformed_archive_count"] == 1
    assert report["conservation"]["current_jobs"]["unexplained_count"] == 1
    assert "current_jobs_conservation" in report["readiness"]["blockers"]
    conn.close()


def test_unmapped_state_and_events_require_archives(tmp_path):
    conn, _ = _backfilled_db(tmp_path, unmapped=True)
    accepted = build_audit_report(conn)

    assert accepted["conservation"]["state"]["unmapped_count"] == 1
    assert accepted["conservation"]["events"]["unmapped_count"] == 1
    assert accepted["readiness"]["ready"] is True

    conn.execute("DELETE FROM identity_migration_archive WHERE artifact='state_event'")
    conn.commit()
    rejected = build_audit_report(conn)
    assert rejected["conservation"]["events"]["unexplained_count"] == 1
    assert "events_conservation" in rejected["readiness"]["blockers"]
    conn.close()


def test_stale_archive_payload_does_not_account_changed_state(tmp_path):
    conn, _ = _backfilled_db(tmp_path, unmapped=True)
    conn.execute(
        "UPDATE job_state SET notes='changed after archive' "
        "WHERE seen_key='unmapped-private-key'"
    )
    conn.commit()

    report = build_audit_report(conn)

    assert report["conservation"]["state"]["unexplained_count"] == 1
    assert "state_conservation" in report["readiness"]["blockers"]
    assert "changed after archive" not in json.dumps(report)
    conn.close()


def test_ambiguous_lineage_encodings_and_orphan_mapping_block(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    posting_id = conn.execute("SELECT posting_id FROM postings").fetchone()[0]
    conn.execute("INSERT INTO postings VALUES ('other','active','t0','t0',NULL)")
    encoded = json.dumps([PRIVATE_URL, PRIVATE_KEY], indent=1)
    conn.execute(
        "INSERT INTO legacy_identity_map VALUES ('lineage','legacy-db',?,'other','t0',NULL)",
        (encoded,),
    )
    conn.commit()

    report = build_audit_report(conn)

    assert report["conservation"]["lineage"]["ambiguous_mapping_count"] == 1
    assert report["readiness"]["ready"] is False
    assert posting_id != "other"
    conn.close()


def test_orphan_posting_and_hidden_run_membership_block(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute("INSERT INTO postings VALUES ('orphan','active','t0','t0',NULL)")
    conn.execute(
        "INSERT INTO pipeline_runs (run_uid,kind,status,legacy_run_date) "
        "VALUES ('running-extra','full','running','2099-01-01')"
    )
    posting_id = conn.execute("SELECT posting_id FROM postings WHERE posting_id<>'orphan'").fetchone()[0]
    conn.execute(
        "INSERT INTO run_postings "
        "(run_uid,posting_id,present,first_seen_in_run,recorded_at) "
        "VALUES ('running-extra',?,1,0,'2099-01-01')",
        (posting_id,),
    )
    conn.commit()

    report = build_audit_report(conn)

    assert report["conservation"]["lineage"]["orphan_posting_count"] == 1
    assert report["conservation"]["run_postings"]["unexplained_count"] == 1
    assert {"orphan_postings", "run_postings_conservation"} <= set(
        report["readiness"]["blockers"]
    )
    conn.close()


def test_identity_conflicts_cycles_ambiguity_and_orphans_are_detected(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    posting_id = conn.execute("SELECT posting_id FROM postings").fetchone()[0]
    conn.execute("INSERT INTO postings VALUES ('second','active','2026-01-01','t0',NULL)")
    conn.execute("DROP INDEX uq_posting_aliases_active")
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id,posting_id,alias_kind,namespace,value,url,valid_from) "
        "VALUES ('conflict','second','url','legacy-url',?,?,?)",
        (PRIVATE_URL, PRIVATE_URL, "2026-01-01"),
    )
    conn.executemany(
        "INSERT INTO posting_redirects VALUES (?,?,?,'t0')",
        [(posting_id, "second", "cycle"), ("second", posting_id, "cycle")],
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id,posting_id,alias_kind,namespace,value,url,valid_from) "
        "VALUES ('orphan','missing','url','native','orphan','https://orphan','t0')"
    )
    conn.commit()

    report = build_audit_report(conn)

    assert report["identity"]["active_alias_conflict_count"] == 1
    assert report["identity"]["ambiguous_active_url_count"] == 1
    assert report["identity"]["redirect_cycle_count"] == 1
    assert report["identity"]["orphan_alias_count"] == 1
    assert report["database"]["foreign_key_violation_count"] == 1
    assert {"active_alias_conflicts", "ambiguous_active_urls", "redirect_cycles",
            "orphan_aliases", "foreign_key_violations"} <= set(
                report["readiness"]["blockers"]
            )
    conn.close()


def test_first_seen_regression_blocks_and_recycled_url_is_reported(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute("UPDATE postings SET first_seen_at='2026-08-01'")
    conn.execute("INSERT INTO postings VALUES ('retired','retired','2025-01-01','t0','t1')")
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id,posting_id,alias_kind,namespace,value,url,valid_from,valid_to) "
        "VALUES ('retired-alias','retired','url','legacy-url','retired-value',?,?,'2026-01-01')",
        (PRIVATE_URL, "2025-01-01"),
    )
    conn.commit()

    report = build_audit_report(conn)

    assert report["identity"]["first_seen_moved_later_count"] == 1
    assert report["identity"]["recycled_url_count"] == 1
    assert "first_seen_moved_later" in report["readiness"]["blockers"]
    assert "recycled_urls" not in report["readiness"]["blockers"]
    conn.close()


def test_import_ledger_aggregates_and_blocks_accounting_violations(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.executemany(
        "INSERT INTO legacy_artifact_imports "
        "(import_id,artifact_kind,artifact_path,artifact_hash,idempotency_key,status,started_at,"
        "finished_at,processed_count,mapped_count,duplicate_count,malformed_count,archived_count) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("i1", "seen", "/private/seen", "h1", "seen:h1", "succeeded", "t0", "t1",
             2, 1, 1, 0, 0),
            ("i2", "resolutions", "/private/resolutions", "h2", "resolutions:h2",
             "succeeded", "t0", "t1", 3, 1, 0, 0, 0),
        ],
    )
    conn.commit()

    report = build_audit_report(conn)

    assert report["imports"]["counts_by_status"] == {"succeeded": 2}
    assert report["imports"]["counts_by_artifact_kind"] == {"resolutions": 1, "seen": 1}
    assert report["imports"]["accounting_violation_count"] == 1
    assert report["readiness"]["blockers"] == ["import_accounting_violations"]
    assert "/private" not in json.dumps(report)
    conn.close()


def test_incomplete_import_blocks_even_when_accounting_balances(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute(
        "INSERT INTO legacy_artifact_imports "
        "(import_id,artifact_kind,artifact_path,artifact_hash,idempotency_key,status,"
        "started_at,processed_count,mapped_count,duplicate_count,malformed_count,archived_count) "
        "VALUES ('running','seen','private','h','seen:h','running','t0',1,1,0,0,0)"
    )
    conn.commit()

    report = build_audit_report(conn)

    assert report["imports"]["incomplete_count"] == 1
    assert "incomplete_imports" in report["readiness"]["blockers"]
    conn.close()


def test_duplicate_parity_keys_are_reported():
    rows = [{"id": "same", "value": 1}, {"id": "same", "value": 1}]
    report = _parity(rows, rows[:1], ("id",), ("value",))
    assert report["duplicate_expected_key_count"] == 1
    assert report["duplicate_actual_key_count"] == 0


def test_timezone_equivalent_first_seen_does_not_false_block(tmp_path):
    conn, _ = _backfilled_db(tmp_path)
    conn.execute("UPDATE jobs SET first_seen='2025-12-31T19:00:00-05:00'")
    conn.execute("UPDATE postings SET first_seen_at='2026-01-01T00:00:00+00:00'")
    conn.commit()

    report = build_audit_report(conn)

    assert report["identity"]["first_seen_moved_later_count"] == 0
    conn.close()


def test_read_only_audit_does_not_mutate_database(tmp_path):
    conn, path = _backfilled_db(tmp_path)
    before = conn.total_changes
    conn.close()
    uri = path.resolve().as_uri() + "?mode=ro"
    read_only = sqlite3.connect(uri, uri=True)
    read_only.row_factory = sqlite3.Row

    report = build_audit_report(read_only)

    assert report["readiness"]["ready"] is True
    assert read_only.total_changes == 0
    read_only.close()
    verify = sqlite3.connect(path)
    assert verify.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    verify.close()
    assert before > 0


def test_cli_emits_compact_private_content_free_json(tmp_path):
    conn, path = _backfilled_db(tmp_path)
    conn.close()
    webapp = Path(__file__).resolve().parents[2]

    result = subprocess.run(
        [sys.executable, "-m", "backend.pipeline.audit", "--db", str(path)],
        cwd=webapp, capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == result.stdout.strip() + "\n"
    assert " " not in result.stdout
    parsed = json.loads(result.stdout)
    assert parsed["readiness"]["ready"] is True
    for private_value in (PRIVATE_URL, PRIVATE_KEY, PRIVATE_TITLE, PRIVATE_COMPANY, PRIVATE_NOTES):
        assert private_value not in result.stdout


def test_cli_errors_do_not_echo_private_arguments(capsys):
    try:
        from backend.pipeline import audit

        audit.main(["--results", "/private/PERSONAL CONTENT"])
    except SystemExit as exc:
        assert exc.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "pipeline audit failed\n"
