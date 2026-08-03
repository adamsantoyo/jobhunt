import base64
import hashlib
import json

import pytest

from backend.db import connect, init_db
from backend.pipeline import legacy_import
from backend.pipeline.legacy_import import audit_legacy_artifacts, import_legacy_artifacts


def _jsonl(path, rows):
    path.write_bytes(b"".join(
        row if isinstance(row, bytes) else json.dumps(row).encode("utf-8") + b"\n"
        for row in rows
    ))


def _seed_posting(conn, posting_id, url, seen_key=None, *, alias_value=None, first_seen="2026-02-01"):
    conn.execute(
        "INSERT INTO postings VALUES (?,'active',?,?,NULL)",
        (posting_id, first_seen, "2026-02-01"),
    )
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id,posting_id,alias_kind,namespace,value,url,valid_from) "
        "VALUES (?,?,'url','legacy-url',?,?,?)",
        (f"alias-{posting_id}-{alias_value or url}", posting_id, alias_value or url, url, first_seen),
    )
    if seen_key is not None:
        lineage_value = json.dumps([url, seen_key], separators=(",", ":"))
        evidence = json.dumps(
            {"url": url, "seen_key": seen_key, "lineage_hash": f"hash-{posting_id}"},
            sort_keys=True, separators=(",", ":"),
        )
        conn.execute(
            "INSERT INTO legacy_identity_map VALUES ('lineage','legacy-db',?,?,?,?)",
            (lineage_value, posting_id, first_seen, evidence),
        )
        conn.execute(
            "INSERT INTO identity_evidence "
            "(evidence_id,posting_id,evidence_kind,evidence_json,evidence_hash,observed_at) "
            "VALUES (?,?,'legacy-lineage',?,?,?)",
            (f"lineage-{posting_id}", posting_id, evidence, f"lineage-hash-{posting_id}", first_seen),
        )


@pytest.fixture
def imported_db(tmp_path):
    path = tmp_path / "app.db"
    conn = connect(path)
    init_db(conn)
    yield conn, path
    conn.close()


def test_resolution_same_posting_maps_evidence_and_duplicate_line(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "p1", "https://agg", "key-1")
    conn.execute(
        "INSERT INTO posting_aliases "
        "(alias_id,posting_id,alias_kind,namespace,value,url,valid_from) "
        "VALUES ('alias-canon','p1','url','legacy-url','https://canon','https://canon','t0')"
    )
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    resolution = {
        "agg_url": "https://agg", "canonical_url": "https://canon",
        "ats": "greenhouse", "matched_title": "Role", "sim": 0.9,
    }
    _jsonl(results / "resolutions.jsonl", [resolution, resolution])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["mapped"] == 1
    assert report["duplicate"] == 1
    evidence = conn.execute(
        "SELECT posting_id,evidence_json FROM identity_evidence "
        "WHERE evidence_kind='legacy-resolution'"
    ).fetchone()
    assert evidence["posting_id"] == "p1"
    payload = json.loads(evidence["evidence_json"])
    assert payload["artifact_hash"] == report["artifact_hash"]
    assert payload["resolution"] == resolution
    assert conn.execute("SELECT COUNT(*) FROM posting_redirects").fetchone()[0] == 0


def test_resolution_distinct_postings_creates_redirect(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "aggregator", "https://agg")
    _seed_posting(conn, "canonical", "https://canon")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "resolutions.jsonl", [{
        "agg_url": "https://agg", "canonical_url": "https://canon",
        "ats": "workday", "matched_title": "Engineer", "sim": 0.8,
    }])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["mapped"] == 1
    redirect = conn.execute("SELECT * FROM posting_redirects").fetchone()
    assert (redirect["from_posting_id"], redirect["to_posting_id"], redirect["reason"]) == (
        "aggregator", "canonical", "legacy resolver"
    )
    assert conn.execute(
        "SELECT posting_id FROM identity_evidence WHERE evidence_kind='legacy-resolution'"
    ).fetchone()[0] == "canonical"


def test_resolution_existing_identical_redirect_with_new_evidence_counts_mapped(
    imported_db, tmp_path
):
    conn, _ = imported_db
    _seed_posting(conn, "aggregator", "https://agg")
    _seed_posting(conn, "canonical", "https://canon")
    conn.execute(
        "INSERT INTO posting_redirects VALUES "
        "('aggregator','canonical','legacy resolver','t0')"
    )
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "resolutions.jsonl", [{
        "agg_url": "https://agg", "canonical_url": "https://canon",
        "ats": "workday", "matched_title": "Engineer", "sim": 0.8,
    }])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["duplicate"] == 0
    assert report["mapped"] == 1
    assert conn.execute(
        "SELECT to_posting_id,reason FROM posting_redirects WHERE from_posting_id='aggregator'"
    ).fetchone()[:] == ("canonical", "legacy resolver")


def test_resolution_rejects_redirect_cycle(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "aggregator", "https://agg")
    _seed_posting(conn, "canonical", "https://canon")
    conn.execute(
        "INSERT INTO posting_redirects VALUES ('canonical','aggregator','existing','t0')"
    )
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "resolutions.jsonl", [{
        "agg_url": "https://agg", "canonical_url": "https://canon",
    }])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["archived"] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM posting_redirects WHERE from_posting_id='aggregator'"
    ).fetchone()[0] == 0


def test_resolution_accepts_existing_chain_to_canonical(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "aggregator", "https://agg")
    _seed_posting(conn, "middle", "https://middle")
    _seed_posting(conn, "canonical", "https://canon")
    conn.executemany(
        "INSERT INTO posting_redirects VALUES (?,?,?,'t0')",
        [("aggregator", "middle", "existing"), ("middle", "canonical", "existing")],
    )
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "resolutions.jsonl", [{
        "agg_url": "https://agg", "canonical_url": "https://canon",
    }])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["mapped"] == 1
    assert conn.execute(
        "SELECT posting_id FROM identity_evidence WHERE evidence_kind='legacy-resolution'"
    ).fetchone()[0] == "canonical"
    assert conn.execute("SELECT COUNT(*) FROM posting_redirects").fetchone()[0] == 2


def test_resolution_evidence_ownership_conflict_archives(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "aggregator", "https://agg")
    _seed_posting(conn, "canonical", "https://canon")
    _seed_posting(conn, "wrong", "https://wrong")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    record = {"agg_url": "https://agg", "canonical_url": "https://canon"}
    _jsonl(results / "resolutions.jsonl", [record])
    content = (results / "resolutions.jsonl").read_bytes()
    artifact_hash = hashlib.sha256(content).hexdigest()
    payload = legacy_import._resolution_payload(record, artifact_hash, 1)
    evidence_json = legacy_import._canonical_json(payload)
    evidence_hash = hashlib.sha256(evidence_json.encode()).hexdigest()
    conn.execute(
        "INSERT INTO identity_evidence "
        "(evidence_id,posting_id,evidence_kind,evidence_json,evidence_hash,observed_at) "
        "VALUES (?,?,?,?,?,?)",
        ("conflict-evidence", "wrong", "legacy-resolution", evidence_json,
         evidence_hash, "t0"),
    )
    conn.commit()

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["archived"] == 1
    assert conn.execute("SELECT COUNT(*) FROM posting_redirects").fetchone()[0] == 0


def test_resolution_conflicting_redirect_is_archived_without_overwrite(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "aggregator", "https://agg")
    _seed_posting(conn, "canonical", "https://canon")
    _seed_posting(conn, "other", "https://other")
    conn.execute(
        "INSERT INTO posting_redirects VALUES ('aggregator','other','native','t0')"
    )
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "resolutions.jsonl", [{
        "agg_url": "https://agg", "canonical_url": "https://canon",
        "ats": None, "matched_title": None, "sim": None,
    }])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["archived"] == 1
    assert conn.execute(
        "SELECT to_posting_id FROM posting_redirects WHERE from_posting_id='aggregator'"
    ).fetchone()[0] == "other"
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_evidence WHERE evidence_kind='legacy-resolution'"
    ).fetchone()[0] == 0


def test_resolution_unknown_and_ambiguous_urls_archive_candidates(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "p1", "https://ambiguous", alias_value="alias-one")
    _seed_posting(conn, "p2", "https://ambiguous", alias_value="alias-two")
    _seed_posting(conn, "canonical", "https://canon")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "resolutions.jsonl", [
        {"agg_url": "https://missing", "canonical_url": "https://canon"},
        {"agg_url": "https://ambiguous", "canonical_url": "https://canon"},
    ])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["archived"] == 2
    archives = conn.execute(
        "SELECT candidate_posting_ids_json FROM identity_migration_archive "
        "WHERE artifact='legacy-resolutions' ORDER BY locator"
    ).fetchall()
    candidates = [json.loads(row[0]) for row in archives]
    assert ["canonical"] in candidates
    assert ["canonical", "p1", "p2"] in candidates


def test_seen_exact_and_fallback_unique_map_and_only_move_first_seen_earlier(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "exact", "https://exact", "exact-key", first_seen="2026-02-01")
    _seed_posting(conn, "fallback", "https://old-url", "fallback-key", first_seen="2026-03-01")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "seen.jsonl", [
        {"key": "exact-key", "first_seen": "2026-01-01", "url": "https://exact"},
        {"key": "fallback-key", "first_seen": "2026-02-15", "url": "https://new-url"},
    ])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["mapped"] == 2
    assert dict(conn.execute(
        "SELECT posting_id,first_seen_at FROM postings WHERE posting_id IN ('exact','fallback')"
    ).fetchall()[0])
    first_seen = dict(conn.execute(
        "SELECT posting_id,first_seen_at FROM postings WHERE posting_id IN ('exact','fallback')"
    ).fetchall())
    assert first_seen == {"exact": "2026-01-01", "fallback": "2026-02-15"}

    _jsonl(results / "seen.jsonl", [
        {"key": "exact-key", "first_seen": "2026-04-01", "url": "https://exact"},
    ])
    import_legacy_artifacts(conn, results)
    assert conn.execute(
        "SELECT first_seen_at FROM postings WHERE posting_id='exact'"
    ).fetchone()[0] == "2026-01-01"


def test_seen_follows_redirect_and_normalizes_timezone_order(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(
        conn, "old", "https://old", "redirected-key",
        first_seen="2026-01-01T00:30:00+00:00",
    )
    _seed_posting(
        conn, "current", "https://current", first_seen="2026-01-01T00:30:00+00:00",
    )
    conn.execute("INSERT INTO posting_redirects VALUES ('old','current','merge','t0')")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "seen.jsonl", [{
        "key": "redirected-key", "url": "https://old",
        "first_seen": "2025-12-31T20:00:00-05:00",
    }])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["mapped"] == 1
    evidence = conn.execute(
        "SELECT posting_id FROM identity_evidence WHERE evidence_kind='legacy-seen'"
    ).fetchone()
    assert evidence[0] == "current"
    assert conn.execute(
        "SELECT first_seen_at FROM postings WHERE posting_id='current'"
    ).fetchone()[0] == "2026-01-01T00:30:00+00:00"


def test_seen_ambiguous_and_unknown_archive_candidate_ids(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "p1", "https://one", "shared-key")
    _seed_posting(conn, "p2", "https://two", "shared-key")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "seen.jsonl", [
        {"key": "shared-key", "first_seen": "2026-01-01", "url": "https://other"},
        {"key": "missing-key", "first_seen": "2026-01-01", "url": "https://missing"},
    ])

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["archived"] == 2
    candidates = [row[0] for row in conn.execute(
        "SELECT candidate_posting_ids_json FROM identity_migration_archive "
        "WHERE artifact='legacy-seen'"
    )]
    assert json.dumps(["p1", "p2"], separators=(",", ":")) in candidates
    assert None in candidates


def test_malformed_lines_are_reversibly_archived_and_blank_lines_excluded(imported_db, tmp_path):
    conn, _ = imported_db
    results = tmp_path / "results"
    results.mkdir()
    content = b"\nnot json\n[1,2]\n{\"agg_url\": null}\n\xff\n   \n"
    (results / "resolutions.jsonl").write_bytes(content)

    report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["processed"] == report["malformed"] == 4
    assert report["mapped"] == report["duplicate"] == report["archived"] == 0
    payloads = [json.loads(row[0]) for row in conn.execute(
        "SELECT payload_json FROM identity_migration_archive "
        "WHERE artifact='legacy-resolutions'"
    )]
    raw_values = {base64.b64decode(payload["raw_base64"]) for payload in payloads}
    assert raw_values == {b"not json", b"[1,2]", b'{"agg_url": null}', b"\xff"}


def test_exact_byte_hash_idempotent_rerun_and_changed_file_new_import(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "p1", "https://one", "key-one")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    first = b'{"key":"key-one","first_seen":"2026-01-01","url":"https://one"}\n'
    (results / "seen.jsonl").write_bytes(first)

    first_report = import_legacy_artifacts(conn, results)["artifacts"][0]
    rerun_report = import_legacy_artifacts(conn, results)["artifacts"][0]

    assert first_report["artifact_hash"] == hashlib.sha256(first).hexdigest()
    assert rerun_report == first_report
    assert conn.execute("SELECT COUNT(*) FROM legacy_artifact_imports").fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_evidence WHERE evidence_kind='legacy-seen'"
    ).fetchone()[0] == 1

    changed = first + b"\n"
    (results / "seen.jsonl").write_bytes(changed)
    changed_report = import_legacy_artifacts(conn, results)["artifacts"][0]
    assert changed_report["artifact_hash"] == hashlib.sha256(changed).hexdigest()
    assert conn.execute("SELECT COUNT(*) FROM legacy_artifact_imports").fetchone()[0] == 2


def test_accounting_equality_and_forced_failure_roll_back_whole_import(
    imported_db, tmp_path, monkeypatch
):
    conn, _ = imported_db
    _seed_posting(conn, "p1", "https://one", "key-one")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "seen.jsonl", [
        {"key": "key-one", "first_seen": "2026-01-01", "url": "https://one"},
        {"key": "missing", "first_seen": "2026-01-01", "url": "https://missing"},
    ])
    real_import_line = legacy_import._import_line
    calls = 0

    def fail_second(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("forced failure")
        return real_import_line(*args)

    monkeypatch.setattr(legacy_import, "_import_line", fail_second)
    with pytest.raises(RuntimeError, match="forced failure"):
        import_legacy_artifacts(conn, results)

    assert conn.execute("SELECT COUNT(*) FROM legacy_artifact_imports").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_evidence WHERE evidence_kind='legacy-seen'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT first_seen_at FROM postings WHERE posting_id='p1'"
    ).fetchone()[0] == "2026-02-01"


def test_audit_only_read_only_connection_does_not_mutate(imported_db, tmp_path):
    conn, db_path = imported_db
    _seed_posting(conn, "p1", "https://one", "shared-key")
    _seed_posting(conn, "p2", "https://two", "shared-key")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "seen.jsonl", [
        {"key": "shared-key", "first_seen": "2026-01-01", "url": "https://other"},
    ])
    before = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("legacy_artifact_imports", "identity_evidence", "identity_migration_archive")
    }
    audit_conn = legacy_import._read_only_connection(db_path)
    try:
        report = audit_legacy_artifacts(audit_conn, results)
    finally:
        audit_conn.close()

    assert report["artifacts"][0]["ambiguous"] == 1
    assert report["artifacts"][0]["persisted"] is None
    assert report["ledger"] == []
    assert {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in before
    } == before


def test_audit_uses_same_seen_validation_as_import(imported_db, tmp_path):
    conn, _ = imported_db
    _seed_posting(conn, "p1", "https://one", "key-one")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "seen.jsonl", [{
        "key": "key-one", "url": "https://one", "first_seen": "not-a-date",
    }])

    report = audit_legacy_artifacts(conn, results)["artifacts"][0]

    assert report["processed"] == report["malformed"] == 1
    assert report["unknown"] == report["ambiguous"] == 0


def test_import_allows_future_schema_versions(imported_db, tmp_path):
    conn, _ = imported_db
    conn.execute(
        "INSERT INTO schema_version VALUES (13,'future test','2026-08-03T00:00:00')"
    )
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()

    assert import_legacy_artifacts(conn, results) == {"artifacts": []}


def test_cli_output_contains_counts_and_hash_only_not_job_content(imported_db, tmp_path, capsys):
    conn, db_path = imported_db
    _seed_posting(conn, "p1", "https://private.example/person", "private-key")
    conn.commit()
    results = tmp_path / "results"
    results.mkdir()
    _jsonl(results / "seen.jsonl", [{
        "key": "private-key", "first_seen": "2026-01-01",
        "url": "https://private.example/person", "name": "PERSONAL CONTENT",
    }])

    assert legacy_import.main(["--db", str(db_path), "--results", str(results)]) == 0
    output = capsys.readouterr().out
    parsed = json.loads(output)
    assert parsed["artifacts"][0]["processed"] == 1
    assert "PERSONAL CONTENT" not in output
    assert "private.example" not in output
    assert "private-key" not in output