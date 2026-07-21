"""Ingest unit tests (synthetic fixtures only, never the real results/).

Covers the contract's required list:
  * fresh ingest on synthetic fixtures
  * re-ingest preserves an edited status/note
  * URL-rewrite migration (state follows seen_key when unambiguous)
  * ambiguous rewrite -> needs_review, no data loss
  * ingest never clears status (disappeared job keeps its state)
  * backfill creates runs + history for multiple CSVs
  * description streaming join fills only-missing and skips malformed lines
"""
import csv
import json
from types import SimpleNamespace

import pytest

from backend import config
from backend.db import connect, init_db
from backend.identity import seen_key
from backend.ingest import ingest

COLUMNS = ["tier", "odds", "odds_score", "odds_why", "new", "title", "company", "location",
           "salary", "salary_min", "salary_max", "posted", "first_seen", "remote", "source",
           "also_seen_on", "url", "req_id", "why", "flags", "desc_snippet"]


def row(**kw):
    d = {c: "" for c in COLUMNS}
    d.update({"tier": "3", "odds": "Target", "odds_score": "0", "remote": "False"})
    d.update(kw)
    return d


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_descs(path, entries):
    """entries: list of raw strings (written verbatim) or (url, desc) tuples."""
    with open(path, "w", encoding="utf-8") as f:
        for e in entries:
            if isinstance(e, str):
                f.write(e + "\n")
            else:
                f.write(json.dumps({"url": e[0], "desc": e[1]}) + "\n")


@pytest.fixture
def repo(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    results = root / "results"
    results.mkdir(parents=True)
    monkeypatch.setattr(config, "ROOT", root)
    monkeypatch.setattr(config, "RESULTS", results)
    return SimpleNamespace(root=root, results=results, db=tmp_path / "app.db")


def open_conn(repo):
    conn = connect(repo.db)
    init_db(conn)
    return conn


def set_state(conn, url, **fields):
    sk = conn.execute("SELECT seen_key FROM jobs WHERE url=?", (url,)).fetchone()["seen_key"]
    base = {"status": "New", "notes": "", "follow_up_date": None, "applied_date": None,
            "starred": 0, "hidden": 0, "contact": "", "snoozed_until": None,
            "needs_review": 0, "review_reason": None}
    base.update(fields)
    conn.execute(
        "INSERT INTO job_state (url, seen_key, status, notes, follow_up_date, applied_date, starred, hidden, "
        "contact, snoozed_until, needs_review, review_reason, updated_at) VALUES "
        "(:url,:seen_key,:status,:notes,:follow_up_date,:applied_date,:starred,:hidden,:contact,"
        ":snoozed_until,:needs_review,:review_reason,'2026-07-19T00:00:00')",
        {"url": url, "seen_key": sk, **base},
    )
    conn.commit()


# --------------------------------------------------------------------------- #
def test_fresh_ingest(repo):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://gh.io/a/1", new="NEW", tier="5", odds="Likely"),
        row(title="IT Support", company="BuiltCo", location="Seattle, WA",
            source="builtin", url="https://builtin.com/b/2", desc_snippet="snippet only"),
    ])
    write_descs(repo.results / "descriptions.jsonl", [
        ("https://gh.io/a/1", "Full description for the greenhouse job."),
    ])
    (repo.results / "run_report.json").write_text(json.dumps({"date": "2026-07-19", "kept": 2}))

    conn = open_conn(repo)
    rep = ingest(conn)

    assert rep.rows == 2
    assert rep.new == 1
    assert rep.runs_backfilled == 1
    assert rep.descs_joined == 1

    present = conn.execute("SELECT COUNT(*) AS c FROM jobs WHERE present=1").fetchone()["c"]
    assert present == 2
    gh = conn.execute("SELECT full_desc, tier, is_new FROM jobs WHERE url='https://gh.io/a/1'").fetchone()
    assert gh["full_desc"] == "Full description for the greenhouse job."
    assert gh["tier"] == 5 and gh["is_new"] == 1
    bi = conn.execute("SELECT full_desc FROM jobs WHERE url='https://builtin.com/b/2'").fetchone()
    assert bi["full_desc"] is None  # builtin has no desc line -> falls back to snippet

    # run_report captured for the matching date.
    run = conn.execute("SELECT report_json FROM runs WHERE run_date='2026-07-19'").fetchone()
    assert json.loads(run["report_json"])["kept"] == 2
    conn.close()


def test_reingest_preserves_status_and_note(repo):
    csv_path = repo.results / "jobs_scored_2026-07-19.csv"
    write_csv(csv_path, [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://gh.io/a/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    set_state(conn, "https://gh.io/a/1", status="Applied", notes="called the recruiter",
              applied_date="2026-07-19")

    # Re-ingest the identical CSV.
    rep = ingest(conn)
    assert rep.needs_review == 0
    st = conn.execute("SELECT status, notes, applied_date FROM job_state WHERE url='https://gh.io/a/1'").fetchone()
    assert st["status"] == "Applied"
    assert st["notes"] == "called the recruiter"
    assert st["applied_date"] == "2026-07-19"
    conn.close()


def test_url_rewrite_migration_unambiguous(repo):
    # Run 1: job at U1.
    write_csv(repo.results / "jobs_scored_2026-07-18.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://old.example/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    set_state(conn, "https://old.example/1", status="Applied", notes="my note")

    # Run 2: same seen_key, url rewritten to U2; U1 gone.
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://canonical.example/2"),
    ])
    rep = ingest(conn)
    assert rep.healed == 1
    assert rep.needs_review == 0

    # State migrated to the new url; old url has no state row; status/note intact.
    assert conn.execute("SELECT 1 FROM job_state WHERE url='https://old.example/1'").fetchone() is None
    st = conn.execute(
        "SELECT status, notes, needs_review, seen_key FROM job_state WHERE url='https://canonical.example/2'"
    ).fetchone()
    assert st is not None
    assert st["status"] == "Applied"
    assert st["notes"] == "my note"
    assert st["needs_review"] == 0
    assert st["seen_key"] == seen_key("Initech", "Support Engineer", "San Francisco, CA")
    conn.close()


def test_ambiguous_rewrite_flags_review_without_loss(repo):
    write_csv(repo.results / "jobs_scored_2026-07-18.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://old.example/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    set_state(conn, "https://old.example/1", status="Applied", notes="precious note")

    # Run 2: TWO present jobs share the same seen_key -> ambiguous.
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://cand.example/A"),
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="lever", url="https://cand.example/B"),
    ])
    rep = ingest(conn)
    assert rep.healed == 0
    assert rep.needs_review == 1

    st = conn.execute(
        "SELECT status, notes, needs_review, review_reason FROM job_state WHERE url='https://old.example/1'"
    ).fetchone()
    assert st is not None                       # never deleted
    assert st["status"] == "Applied"            # never overwritten
    assert st["notes"] == "precious note"
    assert st["needs_review"] == 1
    assert "https://cand.example/A" in st["review_reason"]
    assert "https://cand.example/B" in st["review_reason"]
    # No state was fabricated on either candidate.
    assert conn.execute("SELECT COUNT(*) AS c FROM job_state").fetchone()["c"] == 1
    conn.close()


def test_ingest_never_clears_status_when_job_disappears(repo):
    write_csv(repo.results / "jobs_scored_2026-07-18.csv", [
        row(title="Zeta Role", company="SoloCorp", location="Nowhere, NV",
            source="greenhouse", url="https://solo.example/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    set_state(conn, "https://solo.example/1", status="Applied", notes="keep me")

    # Run 2: a completely different job; the old seen_key has zero candidates.
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Other Role", company="OtherCo", location="Austin, TX",
            source="greenhouse", url="https://other.example/9"),
    ])
    rep = ingest(conn)
    assert rep.healed == 0
    assert rep.needs_review == 0  # disappearance is NOT a review case

    st = conn.execute(
        "SELECT status, notes, needs_review FROM job_state WHERE url='https://solo.example/1'"
    ).fetchone()
    assert st["status"] == "Applied"
    assert st["notes"] == "keep me"
    assert st["needs_review"] == 0
    conn.close()


def test_backfill_creates_runs_and_history_for_multiple_csvs(repo):
    write_csv(repo.results / "jobs_scored_2026-07-18.csv", [
        row(title="A", company="C1", location="X, CA", source="greenhouse", url="https://e/1"),
        row(title="B", company="C2", location="Y, CA", source="greenhouse", url="https://e/2", new="NEW"),
    ])
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="A", company="C1", location="X, CA", source="greenhouse", url="https://e/1"),
    ])
    conn = open_conn(repo)
    rep = ingest(conn)

    assert rep.runs_backfilled == 2
    run_dates = [r["run_date"] for r in conn.execute("SELECT run_date FROM runs ORDER BY run_date").fetchall()]
    assert run_dates == ["2026-07-18", "2026-07-19"]

    h18 = conn.execute("SELECT COUNT(*) AS c FROM job_history WHERE run_date='2026-07-18'").fetchone()["c"]
    h19 = conn.execute("SELECT COUNT(*) AS c FROM job_history WHERE run_date='2026-07-19'").fetchone()["c"]
    assert h18 == 2 and h19 == 1

    run18 = conn.execute("SELECT kept, new_this_run FROM runs WHERE run_date='2026-07-18'").fetchone()
    assert run18["kept"] == 2 and run18["new_this_run"] == 1
    conn.close()


def test_description_join_fills_only_missing_and_skips_malformed(repo):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="A", company="C1", location="X, CA", source="greenhouse", url="https://e/1"),
        row(title="B", company="C2", location="Y, CA", source="ashby", url="https://e/2"),
    ])
    write_descs(repo.results / "descriptions.jsonl", [
        "this line is not valid json {{{",                    # malformed -> skipped
        ("https://e/1", "desc one"),
        ("https://not-wanted/999", "ignored, url not present"),
        ("https://e/2", "desc two"),
    ])
    conn = open_conn(repo)
    rep = ingest(conn)
    assert rep.descs_joined == 2  # only the two wanted urls; malformed line did not crash

    d1 = conn.execute("SELECT full_desc FROM jobs WHERE url='https://e/1'").fetchone()["full_desc"]
    d2 = conn.execute("SELECT full_desc FROM jobs WHERE url='https://e/2'").fetchone()["full_desc"]
    assert d1 == "desc one" and d2 == "desc two"

    # Second run: e/1 already has a full_desc, so it must NOT be re-fetched/overwritten.
    write_descs(repo.results / "descriptions.jsonl", [
        ("https://e/1", "SHOULD NOT OVERWRITE"),
        ("https://e/2", "desc two"),
    ])
    rep2 = ingest(conn)
    assert rep2.descs_joined == 0  # both already have descriptions -> nothing missing
    d1b = conn.execute("SELECT full_desc FROM jobs WHERE url='https://e/1'").fetchone()["full_desc"]
    assert d1b == "desc one"
    conn.close()


# --------------------------------------------------------------------------- #
# Review-fix suite: partial patches, recycled urls, durable dismiss, reconcile,
# same-day history replacement, non-dict jsonl lines.
# --------------------------------------------------------------------------- #

def test_partial_patch_only_touches_supplied_fields(repo):
    from backend.routers.state import _apply_state

    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://gh.io/a/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)

    _apply_state(conn, "https://gh.io/a/1", {"notes": "first note"})
    _apply_state(conn, "https://gh.io/a/1", {"status": "Applied"})
    _apply_state(conn, "https://gh.io/a/1", {"contact": "recruiter@initech.com"})

    st = conn.execute("SELECT * FROM job_state WHERE url='https://gh.io/a/1'").fetchone()
    # Every partial edit survived every later edit of a different field.
    assert st["notes"] == "first note"
    assert st["status"] == "Applied"
    assert st["contact"] == "recruiter@initech.com"
    # status -> Applied auto-filled applied_date.
    assert st["applied_date"] is not None

    # An explicitly-set applied_date is preserved by COALESCE on a later re-apply.
    _apply_state(conn, "https://gh.io/a/1", {"applied_date": "2026-07-01"})
    _apply_state(conn, "https://gh.io/a/1", {"status": "Applied"})
    st = conn.execute("SELECT applied_date FROM job_state WHERE url='https://gh.io/a/1'").fetchone()
    assert st["applied_date"] == "2026-07-01"
    conn.close()


def _recycle_fixture(repo, run2_rows):
    """Run 1: role T1 at U with saved state; run 2: U recycled to a different role."""
    write_csv(repo.results / "jobs_scored_2026-07-18.csv", [
        row(title="Original Role", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://recycled.example/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    set_state(conn, "https://recycled.example/1", status="Applied", notes="precious")
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", run2_rows)
    return conn


def test_recycled_url_parks_state_when_role_gone(repo):
    conn = _recycle_fixture(repo, [
        row(title="Totally Different Role", company="OtherCo", location="Austin, TX",
            source="greenhouse", url="https://recycled.example/1"),
    ])
    rep = ingest(conn)
    assert rep.healed == 0
    assert rep.needs_review == 0  # old role fully gone -> dormant, not review

    # The recycled url shows NO inherited state.
    assert conn.execute(
        "SELECT 1 FROM job_state WHERE url='https://recycled.example/1'").fetchone() is None
    jrow = conn.execute(
        "SELECT seen_key FROM jobs WHERE url='https://recycled.example/1'").fetchone()
    assert jrow["seen_key"] == seen_key("OtherCo", "Totally Different Role", "Austin, TX")

    # The old state survives, parked on the orphan surrogate, forensically annotated.
    sk1 = seen_key("Initech", "Original Role", "San Francisco, CA")
    st = conn.execute(
        "SELECT status, notes, needs_review, review_reason FROM job_state WHERE url=?",
        ("orphaned:" + sk1,)).fetchone()
    assert st is not None
    assert st["status"] == "Applied" and st["notes"] == "precious"
    assert st["needs_review"] == 0
    assert "recycled" in (st["review_reason"] or "")
    conn.close()


def test_recycled_url_migrates_state_to_moved_role(repo):
    conn = _recycle_fixture(repo, [
        row(title="Totally Different Role", company="OtherCo", location="Austin, TX",
            source="greenhouse", url="https://recycled.example/1"),
        # The ORIGINAL role moved to a new url (same seen_key).
        row(title="Original Role", company="Initech", location="San Francisco, CA",
            source="lever", url="https://moved.example/2"),
    ])
    rep = ingest(conn)
    assert rep.healed == 1
    assert rep.needs_review == 0
    assert conn.execute(
        "SELECT 1 FROM job_state WHERE url='https://recycled.example/1'").fetchone() is None
    st = conn.execute(
        "SELECT status, notes, needs_review FROM job_state WHERE url='https://moved.example/2'").fetchone()
    assert st["status"] == "Applied" and st["notes"] == "precious"
    assert st["needs_review"] == 0
    conn.close()


def test_recycled_url_ambiguous_flags_review(repo):
    conn = _recycle_fixture(repo, [
        row(title="Totally Different Role", company="OtherCo", location="Austin, TX",
            source="greenhouse", url="https://recycled.example/1"),
        row(title="Original Role", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://cand.example/A"),
        row(title="Original Role", company="Initech", location="San Francisco, CA",
            source="lever", url="https://cand.example/B"),
    ])
    rep = ingest(conn)
    assert rep.needs_review == 1
    assert conn.execute(
        "SELECT 1 FROM job_state WHERE url='https://recycled.example/1'").fetchone() is None
    sk1 = seen_key("Initech", "Original Role", "San Francisco, CA")
    st = conn.execute(
        "SELECT status, needs_review, review_reason FROM job_state WHERE url=?",
        ("orphaned:" + sk1,)).fetchone()
    assert st["status"] == "Applied"
    assert st["needs_review"] == 1
    assert "https://cand.example/A" in st["review_reason"]
    assert "https://cand.example/B" in st["review_reason"]
    conn.close()


def _ambiguous_fixture(repo):
    """Saved state whose url vanished; two present jobs share its seen_key."""
    write_csv(repo.results / "jobs_scored_2026-07-18.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://old.example/1"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    set_state(conn, "https://old.example/1", status="Applied", notes="precious note")
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://cand.example/A"),
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="lever", url="https://cand.example/B"),
    ])
    return conn


def test_durable_dismiss_survives_reingest(repo):
    from backend.routers.state import _apply_state

    conn = _ambiguous_fixture(repo)
    rep = ingest(conn)
    assert rep.needs_review == 1

    _apply_state(conn, "https://old.example/1", {"review_dismissed": True})
    st = conn.execute(
        "SELECT needs_review, review_dismissed FROM job_state WHERE url='https://old.example/1'").fetchone()
    assert st["needs_review"] == 0 and st["review_dismissed"] == 1

    # Re-ingest the SAME ambiguous situation: stays dismissed, not re-flagged.
    rep2 = ingest(conn)
    assert rep2.needs_review == 0
    st = conn.execute(
        "SELECT needs_review, review_dismissed FROM job_state WHERE url='https://old.example/1'").fetchone()
    assert st["needs_review"] == 0 and st["review_dismissed"] == 1

    # The ambiguity later resolves to ONE candidate: migration resets the dismissal.
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support Engineer", company="Initech", location="San Francisco, CA",
            source="greenhouse", url="https://cand.example/A"),
    ])
    rep3 = ingest(conn)
    assert rep3.healed == 1
    st = conn.execute(
        "SELECT status, needs_review, review_dismissed FROM job_state WHERE url='https://cand.example/A'").fetchone()
    assert st["status"] == "Applied"
    assert st["needs_review"] == 0 and st["review_dismissed"] == 0
    conn.close()


def test_reconcile_moves_state_and_rejects_bad_targets(repo):
    from fastapi import HTTPException

    from backend.models import ReconcileBody, url_to_b64
    from backend.routers.state import reconcile

    conn = _ambiguous_fixture(repo)
    ingest(conn)
    b64 = url_to_b64

    # from == to
    with pytest.raises(HTTPException) as e:
        reconcile(ReconcileBody(from_url_b64=b64("https://old.example/1"),
                                to_url_b64=b64("https://old.example/1")), conn)
    assert e.value.status_code == 422
    # unknown source
    with pytest.raises(HTTPException) as e:
        reconcile(ReconcileBody(from_url_b64=b64("https://nostate.example/x"),
                                to_url_b64=b64("https://cand.example/A")), conn)
    assert e.value.status_code == 404
    # target not present
    with pytest.raises(HTTPException) as e:
        reconcile(ReconcileBody(from_url_b64=b64("https://old.example/1"),
                                to_url_b64=b64("https://gone.example/z")), conn)
    assert e.value.status_code == 422

    # Happy path: state moves onto the chosen candidate, re-anchored and cleared.
    st = reconcile(ReconcileBody(from_url_b64=b64("https://old.example/1"),
                                 to_url_b64=b64("https://cand.example/A")), conn)
    assert st.status == "Applied" and st.needs_review is False
    assert conn.execute("SELECT 1 FROM job_state WHERE url='https://old.example/1'").fetchone() is None
    moved = conn.execute(
        "SELECT notes, seen_key, review_dismissed FROM job_state WHERE url='https://cand.example/A'").fetchone()
    assert moved["notes"] == "precious note"
    assert moved["seen_key"] == seen_key("Initech", "Support Engineer", "San Francisco, CA")
    assert moved["review_dismissed"] == 0

    # target already has state -> 409
    set_state(conn, "https://cand.example/B", status="Interested")
    with pytest.raises(HTTPException) as e:
        reconcile(ReconcileBody(from_url_b64=b64("https://cand.example/A"),
                                to_url_b64=b64("https://cand.example/B")), conn)
    assert e.value.status_code == 409
    conn.close()


def test_same_day_reingest_replaces_history_snapshot(repo):
    csv_path = repo.results / "jobs_scored_2026-07-19.csv"
    write_csv(csv_path, [
        row(title="A", company="C1", location="X, CA", source="greenhouse", url="https://e/1"),
        row(title="B", company="C2", location="Y, CA", source="greenhouse", url="https://e/2"),
    ])
    conn = open_conn(repo)
    ingest(conn)
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM job_history WHERE run_date='2026-07-19'").fetchone()["c"] == 2

    # Same-day rewrite shrinks the run to one job: the old snapshot must not linger.
    write_csv(csv_path, [
        row(title="A", company="C1", location="X, CA", source="greenhouse", url="https://e/1"),
    ])
    ingest(conn)
    hist = conn.execute(
        "SELECT url FROM job_history WHERE run_date='2026-07-19'").fetchall()
    assert [h["url"] for h in hist] == ["https://e/1"]
    conn.close()


def test_non_dict_json_lines_do_not_abort_ingest(repo):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="A", company="C1", location="X, CA", source="greenhouse", url="https://e/1"),
    ])
    write_descs(repo.results / "descriptions.jsonl", [
        "null",          # valid JSON, not an object
        "[1, 2, 3]",     # valid JSON, not an object
        "42",            # valid JSON, not an object
        ("https://e/1", "the real desc"),
    ])
    conn = open_conn(repo)
    rep = ingest(conn)  # must not raise
    assert rep.descs_joined == 1
    d = conn.execute("SELECT full_desc FROM jobs WHERE url='https://e/1'").fetchone()["full_desc"]
    assert d == "the real desc"
    conn.close()


def test_picks_seeding_only_when_no_state(repo):
    write_csv(repo.results / "jobs_scored_2026-07-19.csv", [
        row(title="Support", company="PickCo", location="SF, CA", source="greenhouse", url="https://pick/1"),
        row(title="Other", company="NoPick", location="SF, CA", source="greenhouse", url="https://pick/2"),
    ])
    (repo.root / "picks.json").write_text(json.dumps([
        {"company": "PickCo", "title": "Support", "reason": "great fit", "url": "https://pick/1"},
    ]))
    conn = open_conn(repo)
    ingest(conn)
    st = conn.execute("SELECT status, starred, notes FROM job_state WHERE url='https://pick/1'").fetchone()
    assert st is not None
    assert st["status"] == "Interested"
    assert st["starred"] == 1
    assert st["notes"].startswith("[pick] ")
    # A job without a pick gets no state row.
    assert conn.execute("SELECT 1 FROM job_state WHERE url='https://pick/2'").fetchone() is None
    conn.close()
