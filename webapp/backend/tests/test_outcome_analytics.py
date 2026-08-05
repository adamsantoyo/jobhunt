"""Phase 5, W-5.3: `outcome_analytics.py` unit + HTTP coverage.

Every database is built under tmp_path via `test_source_scheduler_fakes.
make_connect` (fresh -> full canonical schema through `db.init_db`). Nothing
here touches webapp/app.db (repo-root conftest.py fences JOBHUNT_DB).

`state_events`/`job_state`/`jobs`/`outcome_events` rows are built directly
with SQL (there is no writer module for the legacy tables this task owns);
`postings`/`posting_versions`/`score_versions`/snapshots are built with the
same local insert helpers `test_outcomes.py` uses, plus `outcomes.
capture_snapshot` itself for the snapshot/item rows -- reusing the already-
tested W-5.2 writer rather than hand-rolling snapshot rows keeps this file
honest about what a real capture actually denormalizes.
"""
import json
import uuid

import pytest

from backend import outcomes
from backend.outcome_analytics import outcome_analytics
from backend.tests.test_source_scheduler_fakes import make_connect

AT = "2026-08-01T12:00:00"


# --------------------------------------------------------------------------- #
# Fixtures / local insert helpers (mirrors test_outcomes.py)
# --------------------------------------------------------------------------- #
@pytest.fixture
def conn(tmp_path):
    c = make_connect(tmp_path)()
    try:
        yield c
    finally:
        c.close()


class _FakeFamilies:
    def __init__(self, keywords):
        self.keywords = keywords


class _FakeProfile:
    def __init__(self, keywords, content_hash="oa-fake-profile-hash"):
        self.families = _FakeFamilies(keywords)
        self.content_hash = content_hash


def use_fake_profile(conn, monkeypatch, keywords=None, content_hash="oa-fake-profile-hash"):
    profile = _FakeProfile(keywords or {}, content_hash=content_hash)
    monkeypatch.setattr(outcomes, "_load_profile", lambda: profile)
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions (profile_version_id, content_hash, "
        "profile_json, created_at) VALUES (?,?,?,?)",
        (content_hash, content_hash, "{}", AT),
    )
    conn.commit()
    return profile


def insert_posting(conn, posting_id, at=AT):
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, at, at),
    )


def insert_version(conn, posting_id, version_id=None, *, observed_at=AT, title="Support Engineer",
                    company="Acme Robotics", source="greenhouse",
                    odds="Weak match / High competition", tier=1, odds_score=10):
    version_id = version_id or str(uuid.uuid4())
    conn.execute(
        "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, "
        "version_hash, observed_at, title, company, source, odds, tier, odds_score, "
        "payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (version_id, posting_id, "source", version_id, observed_at, title, company, source,
         odds, tier, odds_score, "{}"),
    )
    return version_id


def ensure_profile_row(conn, profile_version_id, content_hash=None, at=AT):
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions (profile_version_id, content_hash, "
        "profile_json, created_at) VALUES (?,?,?,?)",
        (profile_version_id, content_hash or profile_version_id, "{}", at),
    )


def insert_score(conn, *, posting_version_id, posting_id, tier, odds, odds_score=50,
                  scorer_hash="scorer-1", profile_version_id="oa-fake-profile-hash",
                  superseded_at=None, created_at=AT, features_json=None):
    ensure_profile_row(conn, profile_version_id)
    score_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, created_at, "
        "superseded_at, features_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (score_id, posting_id, posting_version_id, profile_version_id, score_id, scorer_hash,
         tier, odds, odds_score, created_at, superseded_at,
         json.dumps(features_json) if features_json is not None else None),
    )
    return score_id


def insert_job_state(conn, seen_key, *, url=None, posting_id=None, status="Applied",
                      applied_date=None, updated_at=AT):
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, applied_date, updated_at, posting_id) "
        "VALUES (?,?,?,?,?,?)",
        (seen_key, url, status, applied_date, updated_at, posting_id),
    )


def insert_status_event(conn, seen_key, old_value, new_value, at, *, url=None, posting_id=None,
                         source="patch"):
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source, "
        "posting_id) VALUES (?,?, 'status', ?,?,?,?,?)",
        (seen_key, url, old_value, new_value, at, source, posting_id),
    )


def insert_jobs_row(conn, url, seen_key, *, tier=1, odds=None, source=None):
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier, odds, source) VALUES (?,?,?,?,?)",
        (url, seen_key, tier, odds, source),
    )


def insert_opened_event(conn, *, posting_id=None, seen_key=None, url=None, at=AT):
    conn.execute(
        "INSERT INTO outcome_events (outcome_event_id, kind, at, posting_id, seen_key, url) "
        "VALUES (?, 'opened', ?, ?, ?, ?)",
        (str(uuid.uuid4()), at, posting_id, seen_key, url),
    )


# --------------------------------------------------------------------------- #
# empty DB
# --------------------------------------------------------------------------- #
def test_empty_db_zeroed_shape(conn):
    conn.commit()
    result = outcome_analytics(conn, min_sample=5)

    assert result["min_sample"] == 5
    ao = result["application_outcomes"]
    assert ao["n_applied_total"] == 0
    for key in ("by_source", "by_source_category", "by_match_band", "by_competition_band",
                "by_role_family"):
        assert ao[key] == []

    ro = result["recommendation_outcomes"]
    assert ro["n_snapshots"] == 0
    assert ro["n_recommended_total"] == 0
    for key in ("by_rank", "by_match_band", "by_competition_band", "by_role_family",
                "by_source_category"):
        assert ro[key] == []
    assert ro["by_feature"] == {"score_row": [], "hireability": []}

    json.dumps(result)  # must not raise


# --------------------------------------------------------------------------- #
# application funnel counting
# --------------------------------------------------------------------------- #
def test_application_funnel_counts_and_response_definition(conn):
    # sk1: applied, responded (Phone screen), reached interview and offer too.
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00")
    insert_status_event(conn, "sk1", "Applied", "Phone screen", "2026-08-02T09:00:00")
    insert_status_event(conn, "sk1", "Phone screen", "Interview", "2026-08-03T09:00:00")
    insert_status_event(conn, "sk1", "Interview", "Offer", "2026-08-04T09:00:00")

    # sk2: applied then passed -- must NOT count as responded.
    insert_status_event(conn, "sk2", None, "Applied", "2026-08-01T09:00:00")
    insert_status_event(conn, "sk2", "Applied", "Passed", "2026-08-05T09:00:00")

    conn.commit()
    result = outcome_analytics(conn, min_sample=1)
    ao = result["application_outcomes"]
    assert ao["n_applied_total"] == 2

    by_key = {c["key"]: c for c in ao["by_source"]}
    # neither identity resolves a posting -> both land in "unknown"
    assert set(by_key) == {"unknown"}
    cell = by_key["unknown"]
    assert cell["n_applied"] == 2
    assert cell["n_responded"] == 1
    assert cell["n_phone_screen"] == 1
    assert cell["n_interview"] == 1
    assert cell["n_offer"] == 1
    assert cell["response_rate"] == pytest.approx(0.5)
    assert cell["median_days_to_response"] == pytest.approx(1.0)


def test_two_grain_timestamps_parse(conn):
    # sk1's Applied event is a bare backfilled date; its response is full isoformat.
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01")
    insert_status_event(conn, "sk1", "Applied", "Interview", "2026-08-03T09:00:00")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    cell = result["application_outcomes"]["by_source"][0]
    assert cell["n_applied"] == 1
    assert cell["median_days_to_response"] == pytest.approx(2.375)  # 2026-08-01T00:00 -> 08-03T09:00


def test_reapplication_counts_once_per_identity(conn):
    insert_status_event(conn, "sk1", None, "Applied", "2026-07-01T09:00:00")
    insert_status_event(conn, "sk1", "Applied", "Rejected", "2026-07-05T09:00:00")
    insert_status_event(conn, "sk1", "Rejected", "Applied", "2026-08-01T09:00:00")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    assert result["application_outcomes"]["n_applied_total"] == 1


# --------------------------------------------------------------------------- #
# identity dedupe
# --------------------------------------------------------------------------- #
def test_identity_dedupe_posting_id_vs_seen_key_only(conn):
    insert_posting(conn, "p1")
    conn.commit()

    # Two different seen_keys, both event rows carrying the SAME posting_id --
    # one identity.
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00", posting_id="p1")
    insert_status_event(conn, "sk2", None, "Applied", "2026-08-02T09:00:00", posting_id="p1")
    # A wholly separate seen_key-only identity (no posting_id anywhere).
    insert_status_event(conn, "sk3", None, "Applied", "2026-08-01T09:00:00")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    assert result["application_outcomes"]["n_applied_total"] == 2


# --------------------------------------------------------------------------- #
# dimension attribution
# --------------------------------------------------------------------------- #
def test_dimension_attribution_current_score_band(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1", source="greenhouse:acme",
                         odds="Weak match / High competition", tier=1)
    insert_score(conn, posting_version_id=v1, posting_id="p1", tier=3,
                 odds="Strong match / Standard")
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00", posting_id="p1")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    ao = result["application_outcomes"]
    match_keys = {c["key"] for c in ao["by_match_band"]}
    comp_keys = {c["key"] for c in ao["by_competition_band"]}
    assert "Strong match" in match_keys  # current score wins over the version's own odds
    assert "Standard" in comp_keys
    source_keys = {c["key"] for c in ao["by_source"]}
    assert "greenhouse:acme" in source_keys


def test_dimension_attribution_legacy_import_score_reachable(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1", tier=1, odds="Weak match / High competition")
    ensure_profile_row(conn, "legacy-import")
    score_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, created_at, "
        "superseded_at) VALUES (?,NULL,?,?,?,'legacy-import',?,?,?,?,NULL)",
        (score_id, v1, "legacy-import", score_id, 4, "Target", None, AT),
    )
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00", posting_id="p1")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    ao = result["application_outcomes"]
    # legacy odds "Target" has no " / " -> unsplittable -> lands in "unknown"
    assert {c["key"] for c in ao["by_match_band"]} == {"unknown"}
    assert ao["by_match_band"][0]["n_applied"] == 1


def test_dimension_attribution_url_fallback_to_legacy_jobs(conn):
    insert_jobs_row(conn, "https://x/1", "sk1", odds="Strong match / Lower bar", source="dice:x")
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00", url="https://x/1")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    ao = result["application_outcomes"]
    assert {c["key"] for c in ao["by_match_band"]} == {"Strong match"}
    assert {c["key"] for c in ao["by_competition_band"]} == {"Lower bar"}
    assert {c["key"] for c in ao["by_source"]} == {"dice:x"}
    # jobs-table fallback carries no title -> role_family cannot be resolved
    assert {c["key"] for c in ao["by_role_family"]} == {"unknown"}


def test_unresolvable_identity_lands_in_unknown_last(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1", source="greenhouse:acme", odds="Strong match / Standard", tier=3)
    insert_score(conn, posting_version_id=v1, posting_id="p1", tier=3, odds="Strong match / Standard")
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00", posting_id="p1")
    insert_status_event(conn, "sk1", "Applied", "Interview", "2026-08-03T09:00:00", posting_id="p1")

    # Three fully unresolvable identities (no posting_id, no url) -> "unknown", n=3
    for i in range(3):
        insert_status_event(conn, f"sk-unk-{i}", None, "Applied", "2026-08-01T09:00:00")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    by_source = result["application_outcomes"]["by_source"]
    assert by_source[-1]["key"] == "unknown"
    assert by_source[-1]["n_applied"] == 3
    # known cell (n=1) still sorts before "unknown" (n=3) -- unknown always last
    assert by_source[0]["key"] == "greenhouse:acme"


def test_role_family_and_source_category_attribution(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch, keywords={"support": ("support",)})
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1", title="Support Engineer", source="greenhouse:acme",
                         odds="Strong match / Standard", tier=3)
    insert_score(conn, posting_version_id=v1, posting_id="p1", tier=3, odds="Strong match / Standard")
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00", posting_id="p1")

    insert_posting(conn, "p2")
    v2 = insert_version(conn, "p2", title="Generic Title", source=None,
                         odds="Weak match / High competition", tier=1)
    insert_score(conn, posting_version_id=v2, posting_id="p2", tier=1,
                 odds="Weak match / High competition")
    insert_status_event(conn, "sk2", None, "Applied", "2026-08-01T09:00:00", posting_id="p2")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    ao = result["application_outcomes"]
    role_keys = {c["key"]: c["n_applied"] for c in ao["by_role_family"]}
    assert role_keys.get("support") == 1
    assert role_keys.get("unknown") == 1
    cat_keys = {c["key"] for c in ao["by_source_category"]}
    assert "unknown" in cat_keys  # p2's NULL source has no category


# --------------------------------------------------------------------------- #
# recommendation outcomes
# --------------------------------------------------------------------------- #
def test_recommended_distinct_across_overlapping_snapshots(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    for pid in ("p1", "p2", "p3"):
        insert_posting(conn, pid)
        insert_version(conn, pid)
    conn.commit()

    outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
        at="2026-08-01T10:00:00",
    )
    outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p2", "rank": 1}, {"posting_id": "p3", "rank": 2}],
        at="2026-08-02T10:00:00",
    )

    result = outcome_analytics(conn, min_sample=1)
    ro = result["recommendation_outcomes"]
    assert ro["n_snapshots"] == 2
    assert ro["n_recommended_total"] == 3


def test_opened_matching_posting_id_and_url_bridge(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    insert_posting(conn, "p2")
    insert_version(conn, "p2")
    conn.commit()
    outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
        at="2026-08-01T10:00:00",
    )

    # direct posting_id match
    insert_opened_event(conn, posting_id="p1")
    # url-only event, posting_id NULL on the event row itself, bridged via job_state
    insert_job_state(conn, "sk2", url="https://x/2", posting_id="p2", status="New")
    insert_opened_event(conn, seen_key="sk2", url="https://x/2")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    opened = {c["key"]: c["n_opened"] for c in result["recommendation_outcomes"]["by_rank"]}
    # both p1 (rank 1) and p2 (rank 2) show 1 opened
    assert opened["1"] == 1
    assert opened["2"] == 1


def test_applied_after_first_snapshot_attribution(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    insert_posting(conn, "p2")
    insert_version(conn, "p2")
    conn.commit()

    # p1 recommended at t1, applied at t2 (AFTER) -> counts.
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-03T10:00:00", posting_id="p1")

    # p2 was applied to at t0, recommended LATER at t1 -> a pre-existing apply must
    # not count as caused by the recommendation.
    insert_status_event(conn, "sk2", None, "Applied", "2026-07-01T10:00:00", posting_id="p2")
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p2", "rank": 2}], at="2026-08-01T10:00:00",
    )
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    by_rank = {c["key"]: c for c in result["recommendation_outcomes"]["by_rank"]}
    assert by_rank["1"]["n_applied"] == 1
    assert by_rank["1"]["median_days_to_apply"] == pytest.approx(2.0)
    assert by_rank["2"]["n_applied"] == 0


def test_h1_bare_date_applied_same_day_as_snapshot_counts(conn, monkeypatch):
    # H1(a): a backfilled Applied row is bare 'YYYY-MM-DD' while the snapshot
    # is always full-grain. A same-DAY apply must still count -- comparing
    # full-timestamp `delta > 0` against a date parsed as local midnight
    # would silently treat it as "before" the snapshot and drop it.
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()

    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T09:00:00",
    )
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01", posting_id="p1")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    by_rank = {c["key"]: c for c in result["recommendation_outcomes"]["by_rank"]}
    assert by_rank["1"]["n_applied"] == 1
    assert by_rank["1"]["median_days_to_apply"] == pytest.approx(0.0)


def test_h1_full_grain_exact_timestamp_tie_counts(conn, monkeypatch):
    # H1(b): an apply at the EXACT same instant as the snapshot's captured_at
    # is a tie, not "before" -- `>=`, not strict `>`.
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()

    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T10:00:00", posting_id="p1")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    by_rank = {c["key"]: c for c in result["recommendation_outcomes"]["by_rank"]}
    assert by_rank["1"]["n_applied"] == 1
    assert by_rank["1"]["median_days_to_apply"] == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# M3: open_rate is recommendation-attributed, not lifetime
# --------------------------------------------------------------------------- #
def test_open_before_first_snapshot_does_not_count(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()

    # Opened BEFORE the posting was ever recommended -- must not count as
    # caused by the recommendation.
    insert_opened_event(conn, posting_id="p1", at="2026-07-01T10:00:00")
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    by_rank = {c["key"]: c for c in result["recommendation_outcomes"]["by_rank"]}
    assert by_rank["1"]["n_opened"] == 0


def test_open_after_first_snapshot_counts(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()

    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )
    insert_opened_event(conn, posting_id="p1", at="2026-08-02T10:00:00")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    by_rank = {c["key"]: c for c in result["recommendation_outcomes"]["by_rank"]}
    assert by_rank["1"]["n_opened"] == 1


# --------------------------------------------------------------------------- #
# L6: opened-event url bridge mirrors the writer (jobs.seen_key first, then
# job_state), not job_state alone
# --------------------------------------------------------------------------- #
def test_open_url_known_only_to_jobs_table_is_bridged(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )
    # The event's url is known only to the `jobs` cache (not directly on the
    # outcome_events row, and job_state carries no matching url of its own);
    # jobs.seen_key bridges to a job_state row that DOES carry posting_id.
    insert_jobs_row(conn, "https://x/1", "sk1", source="dice:x")
    insert_job_state(conn, "sk1", posting_id="p1", status="New")
    insert_opened_event(conn, url="https://x/1", at="2026-08-02T10:00:00")
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    by_rank = {c["key"]: c for c in result["recommendation_outcomes"]["by_rank"]}
    assert by_rank["1"]["n_opened"] == 1


# --------------------------------------------------------------------------- #
# M1: application_outcomes and recommendation_outcomes must attribute the
# SAME seen_key's apply to the SAME posting
# --------------------------------------------------------------------------- #
def test_seen_key_posting_id_resolution_shared_across_both_families(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "pA")
    insert_version(conn, "pA", source="greenhouse:acme")
    insert_posting(conn, "pB")
    insert_version(conn, "pB", source="dice:zz")
    conn.commit()

    # sk1's FIRST Applied row carries NO posting_id; a LATER row on the same
    # seen_key carries pA explicitly. job_state (the weaker bridge) says pB.
    # Per-stream resolution (posting_id-first) must pick pA for the WHOLE
    # stream, not pB, and not disagree between the two endpoint families.
    insert_job_state(conn, "sk1", posting_id="pB", status="Applied")
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00")
    insert_status_event(
        conn, "sk1", "Applied", "Phone screen", "2026-08-02T09:00:00", posting_id="pA",
    )
    conn.commit()
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "pA", "rank": 1}], at="2026-07-31T09:00:00",
    )
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    ao_sources = {c["key"] for c in result["application_outcomes"]["by_source"]}
    assert ao_sources == {"greenhouse:acme"}  # pA's source, not pB's

    by_rank = {c["key"]: c for c in result["recommendation_outcomes"]["by_rank"]}
    assert by_rank["1"]["n_applied"] == 1  # pA's recommended posting sees the apply


# --------------------------------------------------------------------------- #
# M2: by_feature denominators must not silently drop unscored postings
# --------------------------------------------------------------------------- #
def test_by_feature_unknown_cell_for_unscored_recommended_postings(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1")
    insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=3, odds="Strong match / Standard",
        features_json={"score_row": {"raw_score": 12}, "hireability": {"skills_strong": 3}},
    )
    insert_posting(conn, "p2")
    insert_version(conn, "p2")  # no score at all -> unresolvable score attribution
    conn.commit()

    outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
        at="2026-08-01T10:00:00",
    )

    result = outcome_analytics(conn, min_sample=1)
    by_feature = result["recommendation_outcomes"]["by_feature"]
    score_row = {c["key"]: c for c in by_feature["score_row"]}
    assert score_row["raw_score"]["n_recommended"] == 1
    assert "unknown" in score_row
    assert score_row["unknown"]["n_recommended"] == 1
    assert by_feature["score_row"][-1]["key"] == "unknown"  # unknown always sorts last

    hireability = {c["key"]: c for c in by_feature["hireability"]}
    assert hireability["skills_strong"]["n_recommended"] == 1
    assert hireability["unknown"]["n_recommended"] == 1


# --------------------------------------------------------------------------- #
# M5: denormalized band-label seam (splittable vs legacy-unsplittable odds,
# NULL source_category) -- exercised through a REAL capture_snapshot, not
# hand-rolled item rows
# --------------------------------------------------------------------------- #
def test_recommendation_band_and_source_category_unknown_buckets(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(
        conn, "p1", source="greenhouse:acme", odds="Strong match / Standard", tier=3,
    )
    insert_posting(conn, "p2")
    insert_version(conn, "p2", source=None, odds="Likely", tier=2)  # legacy, unsplittable
    conn.commit()

    outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
        at="2026-08-01T10:00:00",
    )

    result = outcome_analytics(conn, min_sample=1)
    ro = result["recommendation_outcomes"]

    match_keys = {c["key"] for c in ro["by_match_band"]}
    comp_keys = {c["key"] for c in ro["by_competition_band"]}
    assert "Strong match" in match_keys
    assert "unknown" in match_keys
    assert ro["by_match_band"][-1]["key"] == "unknown"
    assert "Standard" in comp_keys
    assert "unknown" in comp_keys
    assert ro["by_competition_band"][-1]["key"] == "unknown"

    cat_keys = {c["key"]: c["n_recommended"] for c in ro["by_source_category"]}
    assert "unknown" in cat_keys  # p2's NULL source has no category
    assert cat_keys["unknown"] == 1


def test_by_rank_uses_latest_snapshot_attribution(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()

    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 3}], at="2026-08-01T10:00:00",
    )
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-02T10:00:00",
    )
    conn.commit()

    result = outcome_analytics(conn, min_sample=1)
    keys = {c["key"] for c in result["recommendation_outcomes"]["by_rank"]}
    assert keys == {"1"}  # the later snapshot's rank wins, not the first


def test_by_feature_presence_from_features_json(conn, monkeypatch):
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1")
    insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=3, odds="Strong match / Standard",
        features_json={
            "score_row": {"raw_score": 12, "function_match": 5, "degree_gated": -1},
            "hireability": {"skills_strong": 3, "junior": 0},
        },
    )
    conn.commit()
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )

    result = outcome_analytics(conn, min_sample=1)
    by_feature = result["recommendation_outcomes"]["by_feature"]
    score_row_keys = {c["key"] for c in by_feature["score_row"]}
    hireability_keys = {c["key"] for c in by_feature["hireability"]}
    assert {"raw_score", "function_match", "degree_gated"} <= score_row_keys
    assert {"skills_strong", "junior"} <= hireability_keys
    # a feature absent from every recommended posting's score is not emitted
    assert "cap_stale" not in score_row_keys
    for c in by_feature["score_row"]:
        assert c["n_recommended"] > 0


# --------------------------------------------------------------------------- #
# low_sample flag
# --------------------------------------------------------------------------- #
def test_low_sample_boundary(conn):
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00")
    insert_status_event(conn, "sk2", None, "Applied", "2026-08-01T09:00:00")
    conn.commit()

    result = outcome_analytics(conn, min_sample=2)
    cell = result["application_outcomes"]["by_source"][0]
    assert cell["n_applied"] == 2
    assert cell["low_sample"] is False  # n == min_sample -> not low

    result3 = outcome_analytics(conn, min_sample=3)
    cell3 = result3["application_outcomes"]["by_source"][0]
    assert cell3["low_sample"] is True  # n < min_sample -> low


def test_min_sample_zero_never_flags_low(conn):
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00")
    conn.commit()
    result = outcome_analytics(conn, min_sample=0)
    assert result["application_outcomes"]["by_source"][0]["low_sample"] is False


# --------------------------------------------------------------------------- #
# rates None on zero denominators / json-dumpable
# --------------------------------------------------------------------------- #
def test_rate_zero_when_numerator_zero_and_json_dumpable(conn, monkeypatch):
    # L3: despite the name this test used to have, `_rcell`/`_cell`'s
    # `else None` denominator guards are dead code -- a cell is only ever
    # built from a group with >=1 row (see `_group`), so `n_applied`/
    # `n_recommended` is never 0 for an existing cell. What this test
    # actually exercises is a RATE with a zero NUMERATOR (n_applied=0 out of
    # a real n_recommended=1), which legitimately reports 0.0, not None.
    use_fake_profile(conn, monkeypatch)
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at=AT,
    )
    result = outcome_analytics(conn, min_sample=5)
    rcell = result["recommendation_outcomes"]["by_rank"][0]
    assert rcell["n_applied"] == 0
    assert rcell["application_rate"] == 0.0
    assert rcell["median_days_to_apply"] is None
    assert result["application_outcomes"]["by_source"] == []
    json.dumps(result)


# --------------------------------------------------------------------------- #
# router
# --------------------------------------------------------------------------- #
@pytest.fixture
def api(tmp_path):
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.db import connect
    from backend.db import get_db
    from backend.routers import outcomesapi

    db_path = tmp_path / "outcome_analytics_api_test.db"
    from backend.db import init_db
    c = connect(db_path)
    init_db(c)

    app = FastAPI()
    app.include_router(outcomesapi.router, prefix="/api")

    def _override():
        conn = connect(db_path)
        try:
            yield conn
        finally:
            conn.close()

    app.dependency_overrides[get_db] = _override
    try:
        yield TestClient(app), c
    finally:
        app.dependency_overrides.pop(get_db, None)
        c.close()


def test_router_happy_path(api):
    client, conn = api
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00")
    conn.commit()

    resp = client.get("/api/outcomes/analytics")
    assert resp.status_code == 200
    body = resp.json()
    assert body["min_sample"] == 5
    assert body["application_outcomes"]["n_applied_total"] == 1


def test_router_min_sample_query_param(api):
    client, conn = api
    resp = client.get("/api/outcomes/analytics?min_sample=10")
    assert resp.status_code == 200
    assert resp.json()["min_sample"] == 10


@pytest.mark.parametrize("min_sample", [-1, 1001])
def test_router_min_sample_validation_422(api, min_sample):
    client, _conn = api
    resp = client.get(f"/api/outcomes/analytics?min_sample={min_sample}")
    assert resp.status_code == 422


def test_router_response_contract_keys_pinned(api, monkeypatch):
    # M6: an empty DB never MINTS a cell (a cell only exists for a group with
    # >=1 row -- see `_group`), so running this on an empty DB never pinned
    # a cell's own key set at all, only the outer envelope. Seed one applied
    # identity and one recommended posting so a real application cell and a
    # real recommendation cell both exist to pin.
    client, conn = api
    use_fake_profile(conn, monkeypatch)
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00")
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()
    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at=AT,
    )
    conn.commit()

    resp = client.get("/api/outcomes/analytics")
    body = resp.json()

    assert set(body.keys()) == {"min_sample", "application_outcomes", "recommendation_outcomes"}
    ao_keys = {
        "n_applied_total", "by_source", "by_source_category", "by_match_band",
        "by_competition_band", "by_role_family",
    }
    assert set(body["application_outcomes"].keys()) == ao_keys
    ro_keys = {
        "n_snapshots", "n_recommended_total", "by_rank", "by_match_band",
        "by_competition_band", "by_role_family", "by_source_category", "by_feature",
    }
    assert set(body["recommendation_outcomes"].keys()) == ro_keys
    assert set(body["recommendation_outcomes"]["by_feature"].keys()) == {"score_row", "hireability"}

    application_cell = body["application_outcomes"]["by_source"][0]
    assert set(application_cell.keys()) == {
        "key", "n_applied", "n_responded", "n_phone_screen", "n_interview", "n_offer",
        "response_rate", "interview_rate", "offer_rate", "median_days_to_response", "low_sample",
    }
    recommendation_cell = body["recommendation_outcomes"]["by_rank"][0]
    assert set(recommendation_cell.keys()) == {
        "key", "n_recommended", "n_opened", "n_applied", "open_rate", "application_rate",
        "median_days_to_apply", "low_sample",
    }
