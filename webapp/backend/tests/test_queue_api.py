"""Task 5.1: GET /api/queue/today (`routers/queueapi.py`) -- HTTP wiring.
Task 5.5a: snapshot-on-serve + GET /api/ranking/metrics.

The router is not mounted in main.py (the Phase 5 integration session does
that, exactly as wave 4.1/4.2 handled readsv2), so every test builds its own
local FastAPI app. Never touches webapp/app.db (repo-root conftest.py fences
JOBHUNT_DB). Response-shape keys are pinned here: this payload is the contract
the Today frontend flip and 5.2's recommendation snapshots build against.
"""
import json
import sqlite3

import pytest

from backend.db import DDL as LEGACY_DDL
from backend.db import connect, get_db, init_db
from backend.models import JobLight, today_iso, url_to_b64


def _make_app():
    from fastapi import FastAPI

    from backend.routers import queueapi

    app = FastAPI()
    app.include_router(queueapi.router, prefix="/api")
    return app


def _client_for(db_path):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    app = _make_app()

    def _override():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override
    return TestClient(app)


@pytest.fixture
def api(tmp_path):
    db_path = tmp_path / "queue_test.db"
    conn = connect(db_path)
    init_db(conn)
    try:
        yield _client_for(db_path), conn
    finally:
        conn.close()


def insert_job(conn, url, *, tier=4, odds="Strong match / Lower bar",
               odds_score=90, company="Acme", title="Engineer",
               posted=None, first_seen=None, present=1, full_desc="a description"):
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier, odds, odds_score, is_new, title, "
        "company, posted, first_seen, remote, present, full_desc) "
        "VALUES (?,?,?,?,?,0,?,?,?,?,0,?,?)",
        (url, url, tier, odds, odds_score, title, company, posted,
         first_seen or today_iso(), present, full_desc),
    )
    conn.commit()


def test_happy_path_shape_and_ranking(api):
    client, conn = api
    insert_job(conn, "https://x.example/1", odds_score=95, company="c1")
    insert_job(conn, "https://x.example/2", odds_score=90, company="c2")
    insert_job(conn, "https://x.example/gone", present=0)

    res = client.get("/api/queue/today")
    assert res.status_code == 200
    body = res.json()
    # top-level contract, pinned. `snapshot_id` was added by 5.5a's snapshot-
    # on-serve. It is NULLABLE in the contract (capture failed, or a pre-
    # migration-21 database), but it is NOT null here: these legacy-inserted
    # jobs resolve to no posting_id, so the day's snapshot captures with
    # EMPTY ITEMS -- which is still a real captured snapshot with a real id.
    # See test_snapshot_on_serve_* below for the full contract.
    assert set(body.keys()) == {
        "generated_for", "cap", "snapshot_id", "queue", "excluded", "excluded_counts",
        "considered",
    }
    assert body["generated_for"] == today_iso()
    assert body["cap"] == 10  # config.DEFAULT_DAILY_QUEUE_SIZE, no setting stored
    assert body["considered"] == 2  # present=0 never reaches the ranker
    # a snapshot is still captured even though these jobs have no job_state
    # row to resolve a posting_id from -- an empty-items snapshot is valid.
    assert isinstance(body["snapshot_id"], str) and body["snapshot_id"]
    assert [e["job"]["url"] for e in body["queue"]] == [
        "https://x.example/1", "https://x.example/2",
    ]
    # entry contract, pinned
    entry = body["queue"][0]
    assert set(entry.keys()) == {"job", "rank", "lane", "lane_rank", "evidence"}
    assert entry["rank"] == 1
    assert set(entry["evidence"].keys()) == {
        "lane", "lane_rank", "match_band", "competition_band", "odds_score",
        "tier", "freshness", "uncertainty", "why",
    }
    # the job payload is a full JobLight
    assert set(JobLight.model_fields.keys()) <= set(entry["job"].keys())


def test_cap_from_app_settings_and_query_override(api):
    client, conn = api
    for i in range(5):
        insert_job(conn, f"https://x.example/{i}", odds_score=90 - i,
                   company=f"c{i}")
    conn.execute(
        "INSERT INTO app_settings (key, value) VALUES ('daily_queue_size', '3')"
    )
    conn.commit()

    body = client.get("/api/queue/today").json()
    assert body["cap"] == 3
    assert len(body["queue"]) == 3
    assert body["excluded_counts"] == {"beyond-cap": 2}

    body = client.get("/api/queue/today?cap=1").json()
    assert body["cap"] == 1
    assert len(body["queue"]) == 1

    # cap=0 is legal: a finished day still gets its exclusion accounting
    body = client.get("/api/queue/today?cap=0").json()
    assert body["queue"] == []
    assert body["excluded_counts"] == {"beyond-cap": 5}

    assert client.get("/api/queue/today?cap=101").status_code == 422


def test_state_and_staleness_reach_exclusion_accounting(api):
    client, conn = api
    insert_job(conn, "https://x.example/applied", company="c1")
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) "
        "VALUES (?,?,?,?)",
        ("https://x.example/applied", "https://x.example/applied", "Applied",
         "2026-01-01T00:00:00"),
    )
    insert_job(conn, "https://x.example/stale", company="c2",
               posted="2020-01-01", first_seen="2020-01-01")
    insert_job(conn, "https://x.example/live", company="c3")
    conn.commit()

    body = client.get("/api/queue/today").json()
    assert [e["job"]["url"] for e in body["queue"]] == ["https://x.example/live"]
    assert body["excluded_counts"] == {"not-new": 1, "stale-posting": 1}
    (row,) = body["excluded"]
    assert row["url_b64"] == url_to_b64("https://x.example/stale")
    assert row["reason"] == "stale-posting"
    assert set(row.keys()) == {"url_b64", "title", "company", "reason", "detail"}


def test_canonical_flag_503_on_legacy_only_db(tmp_path, monkeypatch):
    from backend import config

    db_path = tmp_path / "legacy_only.db"
    conn = connect(db_path)
    conn.executescript(LEGACY_DDL)
    conn.commit()
    conn.close()

    client = _client_for(db_path)
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")
    res = client.get("/api/queue/today")
    assert res.status_code == 503


def test_canonical_flag_dispatches_to_canonical_reads(api, monkeypatch):
    """flag=canonical + canonical schema: the queue is built from
    canonical_reads.list_jobs, not the legacy jobs table -- a legacy-only row
    must not appear."""
    from backend import config

    client, conn = api
    insert_job(conn, "https://x.example/legacy-only")
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")
    res = client.get("/api/queue/today")
    assert res.status_code == 200
    body = res.json()
    assert body["queue"] == []
    assert body["considered"] == 0


# --------------------------------------------------------------------------- #
# Task 5.5a: snapshot-on-serve
# --------------------------------------------------------------------------- #
def insert_posting(conn, posting_id):
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, today_iso(), today_iso()),
    )


def link_job_state_posting(conn, seen_key, posting_id, *, url=None):
    """Insert a `postings` row plus a `job_state` row bridging `seen_key` to
    it (status='New' so the queue's own eligibility check still admits the
    job) -- the FALLBACK bridge `_capture_today_snapshot` reads when the
    alias table cannot name the url (see `link_alias_posting` for the primary
    one)."""
    insert_posting(conn, posting_id)
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at, posting_id) "
        "VALUES (?,?,'New',?,?)",
        (seen_key, url or seen_key, today_iso(), posting_id),
    )
    conn.commit()


def link_alias_posting(conn, url, posting_id, *, namespace="greenhouse"):
    """Insert a `postings` row plus the ACTIVE `posting_aliases` row carrying
    `url` -- the PRIMARY bridge `_capture_today_snapshot` resolves through
    (B1), and the realistic shape of the corpus: every claim writes an alias,
    while `job_state.posting_id` is only ever set by migration 19's one-shot
    backfill."""
    insert_posting(conn, posting_id)
    conn.execute(
        "INSERT INTO posting_aliases (alias_id, posting_id, alias_kind, namespace, value, "
        "url, valid_from) VALUES (?,?,'source',?,?,?,?)",
        (f"alias-{posting_id}", posting_id, namespace, f"v-{posting_id}", url, today_iso()),
    )
    conn.commit()


def test_snapshot_on_serve_captures_once_per_day_and_echoes(api):
    client, conn = api
    insert_job(conn, "https://x.example/1", odds_score=95, company="c1")
    link_job_state_posting(conn, "https://x.example/1", "p1")

    body1 = client.get("/api/queue/today").json()
    sid1 = body1["snapshot_id"]
    assert isinstance(sid1, str) and sid1

    item = conn.execute(
        "SELECT rank, posting_id FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (sid1,),
    ).fetchone()
    assert item["posting_id"] == "p1"
    assert item["rank"] == 1
    snap = conn.execute(
        "SELECT surface, queue_size FROM recommendation_snapshots WHERE snapshot_id=?",
        (sid1,),
    ).fetchone()
    assert snap["surface"] == "today"
    assert snap["queue_size"] == 1

    # a second same-day request (a shrunken remaining-contract cap, exactly
    # as the client would send later in the day) echoes the SAME snapshot_id
    # and captures nothing new.
    body2 = client.get("/api/queue/today?cap=1").json()
    assert body2["snapshot_id"] == sid1

    n_snapshots = conn.execute(
        "SELECT COUNT(*) AS n FROM recommendation_snapshots WHERE surface='today'"
    ).fetchone()["n"]
    assert n_snapshots == 1


def test_snapshot_capture_failure_serves_anyway_with_null_snapshot_id(api, monkeypatch):
    client, conn = api
    insert_job(conn, "https://x.example/1", odds_score=95, company="c1")
    link_job_state_posting(conn, "https://x.example/1", "p1")

    from backend.routers import queueapi

    def _boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(queueapi.outcomes, "capture_snapshot", _boom)
    res = client.get("/api/queue/today")
    assert res.status_code == 200
    body = res.json()
    assert body["snapshot_id"] is None
    assert len(body["queue"]) == 1  # queue serving is unaffected by the failure
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM recommendation_snapshots"
    ).fetchone()["n"] == 0


# --------------------------------------------------------------------------- #
# B1: posting_id resolution -- posting_aliases.url is primary, job_state is
# the fallback.
# --------------------------------------------------------------------------- #
def test_snapshot_resolves_posting_id_via_posting_aliases_url(api):
    """B1: the job has NO job_state row at all -- the shape the Today queue
    is made of by construction (unacted jobs). It still resolves, through the
    active alias row for its url. Before this fix capture found a posting for
    18 of 1095 real jobs and the whole attribution thread downstream was
    dead."""
    client, conn = api
    insert_job(conn, "https://x.example/alias-only", odds_score=95)
    link_alias_posting(conn, "https://x.example/alias-only", "p_alias")

    body = client.get("/api/queue/today").json()
    assert len(body["queue"]) == 1
    item = conn.execute(
        "SELECT posting_id, rank FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchone()
    assert item["posting_id"] == "p_alias"
    assert item["rank"] == 1


def test_snapshot_alias_bridge_wins_over_job_state_bridge(api):
    """Ordering pin: alias first, job_state second. `outcomes.record_outcome_
    event` resolves a url in this same order, so the two ends of the seam
    always name the same posting."""
    client, conn = api
    insert_job(conn, "https://x.example/both", odds_score=95)
    link_job_state_posting(conn, "https://x.example/both", "p_state",
                           url="https://x.example/both")
    link_alias_posting(conn, "https://x.example/both", "p_alias")

    body = client.get("/api/queue/today").json()
    item = conn.execute(
        "SELECT posting_id FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchone()
    assert item["posting_id"] == "p_alias"


def test_snapshot_retired_alias_falls_back_to_job_state(api):
    """A retired alias (`valid_to` set) names a url the posting no longer
    answers to, so it does not bridge -- the job_state fallback still does."""
    client, conn = api
    insert_job(conn, "https://x.example/retired", odds_score=95)
    link_job_state_posting(conn, "https://x.example/retired", "p_state",
                           url="https://x.example/retired")
    link_alias_posting(conn, "https://x.example/retired", "p_alias")
    conn.execute("UPDATE posting_aliases SET valid_to=? WHERE posting_id='p_alias'",
                 (today_iso(),))
    conn.commit()

    body = client.get("/api/queue/today").json()
    item = conn.execute(
        "SELECT posting_id FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchone()
    assert item["posting_id"] == "p_state"


def test_snapshot_two_urls_aliasing_one_posting_keeps_the_better_rank(api):
    """Two queue entries resolving to the SAME canonical posting: keep the
    better-ranked one and drop the other. `capture_snapshot` rejects a
    duplicate posting_id for the whole batch, so without the dedupe a single
    aliased pair would cost the day its entire snapshot."""
    client, conn = api
    insert_job(conn, "https://x.example/a", odds_score=95)
    insert_job(conn, "https://x.example/b", odds_score=90)
    insert_posting(conn, "p_one")
    for i, url in enumerate(("https://x.example/a", "https://x.example/b")):
        conn.execute(
            "INSERT INTO posting_aliases (alias_id, posting_id, alias_kind, namespace, "
            "value, url, valid_from) VALUES (?, 'p_one', 'source', 'greenhouse', ?, ?, ?)",
            (f"alias-{i}", f"v{i}", url, today_iso()),
        )
    conn.commit()

    body = client.get("/api/queue/today").json()
    assert isinstance(body["snapshot_id"], str) and body["snapshot_id"]
    rows = conn.execute(
        "SELECT posting_id, rank FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchall()
    assert [(r["posting_id"], r["rank"]) for r in rows] == [("p_one", 1)]
    snap = conn.execute(
        "SELECT queue_size FROM recommendation_snapshots WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchone()
    assert snap["queue_size"] == 2  # both entries were still SHOWN


# --------------------------------------------------------------------------- #
# B3: the snapshot records the DAY'S queue, not this request's slice of it.
# --------------------------------------------------------------------------- #
def _five_aliased_jobs(conn):
    for i in range(5):
        insert_job(conn, f"https://x.example/{i}", odds_score=95 - i, company=f"c{i}")
        link_alias_posting(conn, f"https://x.example/{i}", f"p{i}")
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('daily_queue_size', '5')")
    conn.commit()


def test_daily_queue_size_zero_captures_no_snapshot(api):
    """Seam L3: `daily_queue_size=0` means the user asked for no queue -- the
    day records NO snapshot (snapshot_id null, zero rows) rather than a
    fabricated max(1, ...) queue nobody saw."""
    client, conn = api
    for i in range(3):
        insert_job(conn, f"https://x.example/z{i}", odds_score=90 - i, company=f"z{i}")
        link_alias_posting(conn, f"https://x.example/z{i}", f"pz{i}")
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('daily_queue_size', '0')")
    conn.commit()

    body = client.get("/api/queue/today?cap=0").json()
    assert body["queue"] == []
    assert body["snapshot_id"] is None
    n_snaps = conn.execute("SELECT COUNT(*) AS n FROM recommendation_snapshots").fetchone()["n"]
    assert n_snaps == 0


def test_snapshot_captures_full_configured_queue_not_the_request_cap(api):
    """B3: a shrunken remaining-contract cap (the client's normal late-in-the-
    day request) serves 1 row but must not pin the day's ranking-quality
    denominator to 1. The snapshot is the configured queue: 5 items,
    queue_size 5."""
    client, conn = api
    _five_aliased_jobs(conn)

    body = client.get("/api/queue/today?cap=1").json()
    assert len(body["queue"]) == 1          # serving still honors the cap
    snap = conn.execute(
        "SELECT queue_size FROM recommendation_snapshots WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchone()
    assert snap["queue_size"] == 5
    ranks = [
        r["rank"] for r in conn.execute(
            "SELECT rank FROM recommendation_snapshot_items WHERE snapshot_id=? ORDER BY rank",
            (body["snapshot_id"],),
        )
    ]
    assert ranks == [1, 2, 3, 4, 5]


def test_cap_zero_request_still_gets_and_defines_nothing_of_the_day_snapshot(api):
    """B3: a finished day (`cap=0`) still gets the day's `snapshot_id` back,
    and the snapshot it gets is the full configured queue -- never an empty
    one minted from a zero-slot request."""
    client, conn = api
    _five_aliased_jobs(conn)

    body = client.get("/api/queue/today?cap=0").json()
    assert body["queue"] == []
    assert isinstance(body["snapshot_id"], str) and body["snapshot_id"]
    snap = conn.execute(
        "SELECT queue_size FROM recommendation_snapshots WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchone()
    assert snap["queue_size"] == 5
    n_items = conn.execute(
        "SELECT COUNT(*) AS n FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (body["snapshot_id"],),
    ).fetchone()["n"]
    assert n_items == 5


# --------------------------------------------------------------------------- #
# B4: check + capture are one BEGIN IMMEDIATE, so a concurrent first-of-day
# pair cannot double-capture.
# --------------------------------------------------------------------------- #
def test_capture_holds_the_write_lock_across_check_and_capture(tmp_path, monkeypatch):
    """The check ("is there a snapshot for today?") and the write must be one
    transaction. Probed by asking a SECOND connection for the write lock at
    the moment capture begins -- with the transaction in place it is refused,
    which is exactly what makes the racing request block and then see the
    winner's row instead of writing a second snapshot for the same day."""
    db_path = tmp_path / "lock_test.db"
    conn = connect(db_path)
    init_db(conn)
    insert_job(conn, "https://x.example/1", odds_score=95)
    link_alias_posting(conn, "https://x.example/1", "p1")

    client = _client_for(db_path)
    from backend.routers import queueapi

    original = queueapi.outcomes.capture_snapshot
    observed = {}

    def _probing_capture(c, **kwargs):
        # timeout=0: fail immediately rather than waiting out the default
        # 5-second busy timeout.
        probe = sqlite3.connect(db_path, timeout=0)
        try:
            probe.execute("BEGIN IMMEDIATE")
            observed["write_lock_held_by_capture"] = False
            probe.rollback()
        except sqlite3.OperationalError as exc:
            observed["write_lock_held_by_capture"] = "locked" in str(exc).lower()
        finally:
            probe.close()
        return original(c, **kwargs)

    monkeypatch.setattr(queueapi.outcomes, "capture_snapshot", _probing_capture)
    try:
        body = client.get("/api/queue/today").json()
        assert isinstance(body["snapshot_id"], str) and body["snapshot_id"]
        assert observed["write_lock_held_by_capture"] is True
    finally:
        conn.close()


def test_capture_releases_the_lock_when_the_day_is_already_captured(api):
    """The early-return path (day already captured) writes nothing and must
    not leave the write lock held -- the next request on this database has to
    be able to write."""
    client, conn = api
    insert_job(conn, "https://x.example/1", odds_score=95)
    link_alias_posting(conn, "https://x.example/1", "p1")

    first = client.get("/api/queue/today").json()
    second = client.get("/api/queue/today").json()
    assert second["snapshot_id"] == first["snapshot_id"]
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM recommendation_snapshots WHERE surface='today'"
    ).fetchone()["n"] == 1
    # an unrelated write still succeeds afterwards
    conn.execute("INSERT INTO app_settings (key, value) VALUES ('probe', '1')")
    conn.commit()


def test_snapshot_day_and_captured_at_come_from_one_timestamp(api, monkeypatch):
    """B10: the day key the existence check uses is the first ten characters
    of the SAME `captured_at` the row is written with -- never a separately
    read clock, which on a midnight crossing would file the row under one day
    and then look for it under another, capturing twice.

    Pinned by making capture's own clock disagree with the calendar: the
    snapshot lands on 2020-01-01, and the second request must still find it.
    Reading the day from `today_iso()` instead would look for the REAL
    today's snapshot, find none, and capture again."""
    client, conn = api
    insert_job(conn, "https://x.example/1", odds_score=95)
    link_alias_posting(conn, "https://x.example/1", "p1")

    from backend.routers import queueapi

    monkeypatch.setattr(queueapi, "now_iso", lambda: "2020-01-01T09:00:00")

    first = client.get("/api/queue/today").json()
    captured_at = conn.execute(
        "SELECT captured_at FROM recommendation_snapshots WHERE snapshot_id=?",
        (first["snapshot_id"],),
    ).fetchone()["captured_at"]
    assert captured_at == "2020-01-01T09:00:00"

    second = client.get("/api/queue/today").json()
    assert second["snapshot_id"] == first["snapshot_id"]
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM recommendation_snapshots WHERE surface='today'"
    ).fetchone()["n"] == 1


def test_snapshot_on_serve_orthogonal_to_reads_flag(api, monkeypatch):
    """Read-flag orthogonality (5.5 contract): snapshot-on-serve runs the same
    way whether the jobs list came from legacy or canonical reads -- it reads
    off the already-built queue entries, not off `config.READS_SOURCE`."""
    from backend import config

    client, conn = api
    insert_job(conn, "https://x.example/1", odds_score=95, company="c1")
    link_job_state_posting(conn, "https://x.example/1", "p1")
    monkeypatch.setattr(config, "READS_SOURCE", "canonical")

    body = client.get("/api/queue/today").json()
    # canonical dispatch sees no canonical postings graph rows for this job
    # (only inserted via the legacy `jobs` table), so the queue is empty --
    # but the endpoint still serves 200 and still captures an (empty) snapshot.
    assert body["queue"] == []
    assert isinstance(body["snapshot_id"], str) and body["snapshot_id"]


# --------------------------------------------------------------------------- #
# Task 5.5a: GET /api/ranking/metrics -- router shape (module math lives in
# tests/test_ranking_metrics.py; this pins the HTTP wiring only)
# --------------------------------------------------------------------------- #
def test_ranking_metrics_router_shape_on_empty_db(api):
    client, conn = api
    res = client.get("/api/ranking/metrics")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {
        "generated_at", "min_sample", "ghost_days",
        "top10_application_rate", "time_to_application", "response_rate",
        "stale_rate", "ghost_rate", "queue_completion", "source_yield",
    }
    assert body["min_sample"] == 5
    assert body["ghost_days"] == 21
    assert isinstance(body["generated_at"], str) and body["generated_at"]
    assert set(body["top10_application_rate"].keys()) == {
        "n_served_top10", "n_applied", "rate", "low_sample",
    }
    assert set(body["time_to_application"].keys()) == {
        "n_served", "n_applied", "median_days", "low_sample",
    }
    assert set(body["response_rate"].keys()) == {
        "n_applied", "n_responded", "rate", "low_sample",
    }
    assert set(body["stale_rate"].keys()) == {
        "n_served", "n_stale_never_engaged", "rate", "low_sample",
    }
    assert set(body["ghost_rate"].keys()) == {
        "n_applied_total", "n_applied_eligible", "n_ghosted", "rate", "low_sample",
        "ghost_days",
    }
    assert set(body["queue_completion"].keys()) == {
        "by_day", "n_days", "median_rate", "low_sample",
    }
    assert body["queue_completion"]["by_day"] == []
    assert set(body["source_yield"].keys()) == {"by_source", "by_source_category"}
    # empty DB: every cell is honestly zero/None, low_sample everywhere
    assert body["top10_application_rate"] == {
        "n_served_top10": 0, "n_applied": 0, "rate": None, "low_sample": True,
    }
    json.dumps(body)  # must be serializable


def test_ranking_metrics_query_params_and_validation(api):
    client, conn = api
    body = client.get("/api/ranking/metrics?min_sample=2&ghost_days=7").json()
    assert body["min_sample"] == 2
    assert body["ghost_days"] == 7

    assert client.get("/api/ranking/metrics?min_sample=0").status_code == 422
    assert client.get("/api/ranking/metrics?ghost_days=0").status_code == 422


def test_ranking_metrics_pre_migration_21_schema_is_503(tmp_path):
    """B8: every table this endpoint reads arrived in migration 21. A legacy
    database answers "not migrated" as 503 -- the same shape
    `routers/runsapi.py` gives a missing canonical run schema -- not a raw
    500 from an unguarded `no such table`."""
    db_path = tmp_path / "legacy_only.db"
    conn = connect(db_path)
    conn.executescript(LEGACY_DDL)
    conn.commit()
    conn.close()

    res = _client_for(db_path).get("/api/ranking/metrics")
    assert res.status_code == 503
    assert res.json() == {"detail": "canonical tables not migrated"}


def test_ranking_metrics_other_operational_errors_still_surface(api, monkeypatch):
    """B8's guard is narrow on purpose: only "no such table" is the database
    saying "not migrated". Any other OperationalError is a bug and must not
    be dressed up as 503."""
    from backend.routers import queueapi

    client, _conn = api

    def _boom(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(queueapi, "compute_ranking_metrics", _boom)
    with pytest.raises(sqlite3.OperationalError):
        client.get("/api/ranking/metrics")


# --------------------------------------------------------------------------- #
# B9: the REAL cross-agent payload, replayed end to end.
#
# GET /api/queue/today -> POST /api/outcomes/events with the returned
# snapshot_id and NO rank (what the frontend sends after F1). No test on
# either side of the 5.5 wave replayed this, which is how the seam shipped
# dead.
# --------------------------------------------------------------------------- #
def _seam_client(db_path):
    """A client whose app mounts BOTH routers -- the queue that hands out a
    snapshot_id and the outcomes endpoint that has to accept it."""
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import outcomesapi, queueapi

    app = FastAPI()
    app.include_router(queueapi.router, prefix="/api")
    app.include_router(outcomesapi.router, prefix="/api")

    def _override():
        c = connect(db_path)
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override
    return TestClient(app)


@pytest.fixture
def seam(tmp_path):
    db_path = tmp_path / "seam_test.db"
    conn = connect(db_path)
    init_db(conn)
    try:
        yield _seam_client(db_path), conn
    finally:
        conn.close()


def test_seam_queue_snapshot_then_open_derives_rank(seam):
    """The payload the client actually sends: url_b64 + snapshot_id, no rank.
    201, with the rank derived server-side from the day's snapshot."""
    client, conn = seam
    insert_job(conn, "https://x.example/first", odds_score=95, company="c1")
    insert_job(conn, "https://x.example/second", odds_score=90, company="c2")
    link_alias_posting(conn, "https://x.example/first", "p_first")
    link_alias_posting(conn, "https://x.example/second", "p_second")

    queue = client.get("/api/queue/today").json()
    snapshot_id = queue["snapshot_id"]
    assert isinstance(snapshot_id, str) and snapshot_id
    second_entry = queue["queue"][1]
    assert second_entry["rank"] == 2

    res = client.post(
        "/api/outcomes/events",
        json={
            "kind": "opened",
            "url_b64": second_entry["job"]["url_b64"],
            "snapshot_id": snapshot_id,
        },
    )
    assert res.status_code == 201
    body = res.json()
    assert body["snapshot_id"] == snapshot_id
    assert body["rank"] == 2          # derived, never sent
    assert body["posting_id"] == "p_second"

    row = conn.execute(
        "SELECT snapshot_id, rank, posting_id FROM outcome_events "
        "WHERE outcome_event_id=?", (body["outcome_event_id"],),
    ).fetchone()
    assert (row["snapshot_id"], row["rank"], row["posting_id"]) == (
        snapshot_id, 2, "p_second",
    )


def test_seam_open_of_a_posting_the_snapshot_never_held_degrades(seam):
    """The degraded path: a real open of something the day's snapshot does
    not contain. 201, stored unattributed (snapshot_id and rank NULL) -- the
    fact is kept, never 422'd away to protect a nullable column."""
    client, conn = seam
    insert_job(conn, "https://x.example/served", odds_score=95, company="c1")
    link_alias_posting(conn, "https://x.example/served", "p_served")
    # present=0 -> never reaches the ranker, so never in the snapshot
    insert_job(conn, "https://x.example/unserved", odds_score=99, company="c2", present=0)
    link_alias_posting(conn, "https://x.example/unserved", "p_unserved")

    snapshot_id = client.get("/api/queue/today").json()["snapshot_id"]
    res = client.post(
        "/api/outcomes/events",
        json={
            "kind": "opened",
            "url_b64": url_to_b64("https://x.example/unserved"),
            "snapshot_id": snapshot_id,
        },
    )
    assert res.status_code == 201
    body = res.json()
    assert body["snapshot_id"] is None
    assert body["rank"] is None
    assert body["posting_id"] == "p_unserved"   # the open itself is still identified
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM outcome_events"
    ).fetchone()["n"] == 1


def test_seam_open_feeds_ranking_metrics(seam):
    """The whole point of the seam: the derived attribution has to show up in
    /api/ranking/metrics. One served posting, one open of it -> source_yield
    counts the open, queue completion counts the day."""
    client, conn = seam
    insert_job(conn, "https://x.example/1", odds_score=95, company="c1")
    link_alias_posting(conn, "https://x.example/1", "p1")

    queue = client.get("/api/queue/today").json()
    client.post(
        "/api/outcomes/events",
        json={
            "kind": "opened",
            "url_b64": queue["queue"][0]["job"]["url_b64"],
            "snapshot_id": queue["snapshot_id"],
        },
    )

    metrics = client.get("/api/ranking/metrics").json()
    (cell,) = metrics["source_yield"]["by_source"]
    assert cell["n_recommended"] == 1
    assert cell["n_opened"] == 1
    (day,) = metrics["queue_completion"]["by_day"]
    assert day["n_completed"] == 1
    assert day["rate"] == 1.0


def test_ranking_metrics_non_empty_cell_shapes_are_pinned(seam):
    """B9: QueueCompletionDay and SourceYieldCell shape-pinned on NON-EMPTY
    fixtures -- the empty-DB shape test above cannot see a single by_day or
    by_source key, which is exactly where the frontend types bind."""
    client, conn = seam
    insert_job(conn, "https://x.example/1", odds_score=95, company="c1")
    link_alias_posting(conn, "https://x.example/1", "p1")
    client.get("/api/queue/today")

    metrics = client.get("/api/ranking/metrics").json()
    (day,) = metrics["queue_completion"]["by_day"]
    assert set(day.keys()) == {"day", "queue_size", "n_served", "n_completed", "rate"}
    assert day["day"] == today_iso()
    assert metrics["queue_completion"]["n_days"] == 1

    (cell,) = metrics["source_yield"]["by_source"]
    assert set(cell.keys()) == {
        "key", "n_recommended", "n_opened", "n_applied", "n_responded",
        "open_rate", "application_rate", "response_rate", "low_sample",
    }
    (cat_cell,) = metrics["source_yield"]["by_source_category"]
    assert set(cat_cell.keys()) == set(cell.keys())
    json.dumps(metrics)
