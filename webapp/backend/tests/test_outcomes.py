"""Phase 5, W-5.2: `outcomes.py` unit coverage -- enrichment precedence, the
legacy-import score fallback, odds splitting, the rubric.py:635 role-family
mirror, recommendation idempotency, input validation, and outcome-event url
resolution.

Every database is built under tmp_path via `test_source_scheduler_fakes.
make_connect` (fresh -> full canonical schema through `db.init_db`). Nothing
here touches webapp/app.db (repo-root conftest.py fences JOBHUNT_DB).
"""
import uuid

import pytest

from backend import outcomes
from backend.sources import registry
from backend.tests.test_source_scheduler_fakes import make_connect

AT = "2026-08-01T12:00:00"


# --------------------------------------------------------------------------- #
# Fixtures / local insert helpers
# --------------------------------------------------------------------------- #
@pytest.fixture
def conn(tmp_path):
    c = make_connect(tmp_path)()
    try:
        yield c
    finally:
        c.close()


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
                  scorer_hash="scorer-1", profile_version_id="pv1", superseded_at=None,
                  created_at=AT):
    ensure_profile_row(conn, profile_version_id)
    score_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, created_at, "
        "superseded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (score_id, posting_id, posting_version_id, profile_version_id, score_id, scorer_hash,
         tier, odds, odds_score, created_at, superseded_at),
    )
    return score_id


class _FakeFamilies:
    def __init__(self, keywords):
        self.keywords = keywords


class _FakeProfile:
    def __init__(self, keywords, content_hash="fake-profile-hash"):
        self.families = _FakeFamilies(keywords)
        self.content_hash = content_hash


# --------------------------------------------------------------------------- #
# Enrichment precedence: explicit > current score > latest version
# --------------------------------------------------------------------------- #
def test_enrichment_prefers_explicit_over_score_over_version(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(
        conn, "p1", title="Version Title", source="greenhouse",
        odds="Weak match / High competition", tier=1, odds_score=10,
    )
    insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=3,
        odds="Strong match / Standard", odds_score=90,
    )
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1, "odds_score": 55}],
    )
    item = result["items"][0]
    # explicit odds_score wins over both score and version
    assert item["odds_score"] == 55
    # tier/odds have no explicit override -> current score wins over the version
    assert item["tier"] == 3
    assert item["odds"] == "Strong match / Standard"
    assert item["posting_version_id"] == v1


def test_enrichment_falls_back_to_version_when_no_current_score(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(
        conn, "p1", title="Version Title", source="greenhouse",
        odds="Weak match / High competition", tier=1, odds_score=10,
    )
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    item = result["items"][0]
    assert item["tier"] == 1
    assert item["odds"] == "Weak match / High competition"
    assert item["odds_score"] == 10
    assert item["posting_version_id"] == v1
    assert item["source"] == "greenhouse"


def test_enrichment_ignores_superseded_scores(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1", tier=1, odds="Weak match / High competition")
    insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=5, odds="Strong match / Standard",
        superseded_at="2026-08-02T00:00:00",
    )
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    item = result["items"][0]
    # superseded score must not win -- falls through to the version's own tier/odds
    assert item["tier"] == 1
    assert item["odds"] == "Weak match / High competition"


# --------------------------------------------------------------------------- #
# Legacy-import score: NULL posting_id, reachable only via posting_versions join
# --------------------------------------------------------------------------- #
def test_legacy_import_score_reachable_via_posting_version_join(conn):
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
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    item = result["items"][0]
    assert item["score_version_id"] == score_id
    assert item["tier"] == 4
    assert item["odds"] == "Target"
    # legacy odds value has no " / " separator -> unsplittable
    assert item["match_label"] is None
    assert item["competition_label"] is None


# --------------------------------------------------------------------------- #
# Odds splitting
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "odds,expected",
    [
        ("Strong match / Standard", ("Strong match", "Standard")),
        ("Weak match / High competition", ("Weak match", "High competition")),
        ("Level stretch / Lower bar", ("Level stretch", "Lower bar")),
        ("Target", (None, None)),
        ("Likely", (None, None)),
        ("Reach", (None, None)),
        (None, (None, None)),
        ("", (None, None)),
    ],
)
def test_split_odds(odds, expected):
    assert outcomes._split_odds(odds) == expected


# --------------------------------------------------------------------------- #
# role_family mirror (rubric.py:635-638)
# --------------------------------------------------------------------------- #
def test_role_family_title_hit():
    profile = _FakeProfile({"support": ("support", "helpdesk")})
    assert outcomes._role_family("Support Engineer", None, profile) == "support"


def test_role_family_description_prefix_hit():
    profile = _FakeProfile({"support": ("support", "helpdesk")})
    description = "helpdesk specialist " + "x" * 2000  # keyword well within first 1500 chars
    assert outcomes._role_family("Generic Title", description, profile) == "support"


def test_role_family_description_hit_requires_first_1500_chars():
    profile = _FakeProfile({"support": ("support",)})
    # keyword appears only after the 1500-char cutoff -> no match
    description = ("x" * 1600) + " support"
    assert outcomes._role_family("Generic Title", description, profile) is None


def test_role_family_no_match_returns_none():
    profile = _FakeProfile({"support": ("support", "helpdesk")})
    assert outcomes._role_family("Generic Title", "nothing relevant here", profile) is None


def test_role_family_none_profile_returns_none():
    assert outcomes._role_family("Support Engineer", "support", None) is None


def test_role_family_derived_during_capture(conn, monkeypatch):
    monkeypatch.setattr(outcomes, "_load_profile", lambda: _FakeProfile({"support": ("support",)}))
    insert_posting(conn, "p1")
    insert_version(conn, "p1", title="Support Engineer")
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    assert result["items"][0]["role_family"] == "support"


# --------------------------------------------------------------------------- #
# recommendations idempotency
# --------------------------------------------------------------------------- #
def test_recommendation_idempotent_across_captures(conn, monkeypatch):
    fake_profile = _FakeProfile({}, content_hash="ch-1")
    monkeypatch.setattr(outcomes, "_load_profile", lambda: fake_profile)
    ensure_profile_row(conn, "pv-fixed", content_hash="ch-1")
    insert_posting(conn, "p1")
    insert_version(conn, "p1", tier=2, odds="Strong match / Standard")
    conn.commit()

    first = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )
    second = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-02T10:00:00",
    )

    rec_id_1 = first["items"][0]["recommendation_id"]
    rec_id_2 = second["items"][0]["recommendation_id"]
    assert rec_id_1 is not None
    assert rec_id_1 == rec_id_2
    count = conn.execute("SELECT COUNT(*) AS c FROM recommendations").fetchone()["c"]
    assert count == 1


def test_recommendation_not_ensured_without_profile_version(conn):
    # No profile.json / no matching profile_versions row -> profile_version_id is
    # None, so no `recommendations` row can legally be created (NOT NULL column).
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    assert result["items"][0]["recommendation_id"] is None
    assert conn.execute("SELECT COUNT(*) AS c FROM recommendations").fetchone()["c"] == 0


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def test_missing_posting_id_raises(conn):
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(conn, surface="sweep", items=[{"rank": 1}])


def test_zero_rank_raises(conn):
    insert_posting(conn, "p1")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(conn, surface="sweep", items=[{"posting_id": "p1", "rank": 0}])


def test_duplicate_rank_raises(conn):
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(
            conn, surface="sweep",
            items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 1}],
        )


def test_duplicate_posting_id_raises(conn):
    insert_posting(conn, "p1")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(
            conn, surface="sweep",
            items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p1", "rank": 2}],
        )


def test_unknown_posting_id_raises(conn):
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(conn, surface="sweep", items=[{"posting_id": "nope", "rank": 1}])


def test_empty_items_is_valid(conn):
    result = outcomes.capture_snapshot(conn, surface="sweep", items=[])
    assert result["queue_size"] == 0
    assert result["items"] == []
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshot_items"
    ).fetchone()["c"] == 0


# --------------------------------------------------------------------------- #
# outcome events: url resolution paths + validation
# --------------------------------------------------------------------------- #
def test_outcome_event_resolves_via_jobs_and_job_state(conn):
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/1', 'sk1', 1)"
    )
    insert_posting(conn, "p1")
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at, posting_id) "
        "VALUES ('sk1', 'https://x/1', 'New', ?, 'p1')", (AT,),
    )
    conn.commit()

    event = outcomes.record_outcome_event(conn, kind="opened", url="https://x/1")
    assert event["seen_key"] == "sk1"
    assert event["posting_id"] == "p1"
    assert event["url"] == "https://x/1"


def test_outcome_event_resolves_via_job_state_fallback(conn):
    insert_posting(conn, "p2")
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at, posting_id) "
        "VALUES ('sk2', 'https://x/2', 'New', ?, 'p2')", (AT,),
    )
    conn.commit()

    event = outcomes.record_outcome_event(conn, kind="opened", url="https://x/2")
    assert event["seen_key"] == "sk2"
    assert event["posting_id"] == "p2"


def insert_alias(conn, posting_id, url, *, alias_id=None, namespace="greenhouse",
                  value=None, valid_from=AT):
    alias_id = alias_id or str(uuid.uuid4())
    conn.execute(
        "INSERT INTO posting_aliases (alias_id, posting_id, alias_kind, namespace, value, "
        "url, valid_from) VALUES (?,?,?,?,?,?,?)",
        (alias_id, posting_id, "source", namespace, value or alias_id, url, valid_from),
    )
    return alias_id


def test_outcome_event_resolves_via_posting_aliases_url(conn):
    """B1's bridge on the read side: `job_state.posting_id` is only ever set
    by migration 19's one-shot backfill, so an ingested-since job has none --
    the active `posting_aliases` row for the url is what actually names the
    posting, and it must be what an open resolves through too. Without this,
    capture (which resolves via the alias join) and open capture name
    different things and the 5.5 attribution seam stays dead."""
    insert_posting(conn, "p_alias")
    insert_alias(conn, "p_alias", "https://x/alias-only")
    conn.execute(
        "INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/alias-only', 'sk-a', 1)"
    )
    # a job_state row with NO posting_id -- the realistic shape post-ingest
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at) "
        "VALUES ('sk-a', 'https://x/alias-only', 'New', ?)", (AT,),
    )
    conn.commit()

    event = outcomes.record_outcome_event(conn, kind="opened", url="https://x/alias-only")
    assert event["posting_id"] == "p_alias"
    assert event["seen_key"] == "sk-a"


def test_outcome_event_alias_bridge_wins_over_job_state(conn):
    """Ordering pin: alias first, job_state second -- the SAME order
    `routers/queueapi._posting_ids_for` uses at capture time. If the two
    disagree, both ends of the seam must still pick the same posting."""
    insert_posting(conn, "p_alias")
    insert_posting(conn, "p_state")
    insert_alias(conn, "p_alias", "https://x/both")
    conn.execute("INSERT INTO jobs (url, seen_key, tier) VALUES ('https://x/both', 'sk-b', 1)")
    conn.execute(
        "INSERT INTO job_state (seen_key, url, status, updated_at, posting_id) "
        "VALUES ('sk-b', 'https://x/both', 'New', ?, 'p_state')", (AT,),
    )
    conn.commit()

    event = outcomes.record_outcome_event(conn, kind="opened", url="https://x/both")
    assert event["posting_id"] == "p_alias"


def test_outcome_event_retired_alias_does_not_resolve(conn):
    """Only ACTIVE aliases (`valid_to IS NULL`) bridge -- a retired one names
    a url this posting no longer answers to."""
    insert_posting(conn, "p_retired")
    alias_id = insert_alias(conn, "p_retired", "https://x/retired")
    conn.execute("UPDATE posting_aliases SET valid_to=? WHERE alias_id=?", (AT, alias_id))
    conn.commit()

    event = outcomes.record_outcome_event(conn, kind="opened", url="https://x/retired")
    assert event["posting_id"] is None
    assert event["url"] == "https://x/retired"


def test_outcome_event_unknown_url_stores_url_only(conn):
    event = outcomes.record_outcome_event(conn, kind="opened", url="https://nowhere/never-seen")
    assert event["seen_key"] is None
    assert event["posting_id"] is None
    assert event["url"] == "https://nowhere/never-seen"
    row = conn.execute(
        "SELECT * FROM outcome_events WHERE outcome_event_id=?", (event["outcome_event_id"],)
    ).fetchone()
    assert row["url"] == "https://nowhere/never-seen"


def test_outcome_event_explicit_posting_id(conn):
    insert_posting(conn, "p3")
    conn.commit()
    event = outcomes.record_outcome_event(conn, kind="opened", posting_id="p3")
    assert event["posting_id"] == "p3"
    assert event["seen_key"] is None
    assert event["url"] is None


def test_outcome_event_unknown_posting_id_raises(conn):
    with pytest.raises(ValueError):
        outcomes.record_outcome_event(conn, kind="opened", posting_id="nope")


def test_outcome_event_unknown_kind_raises(conn):
    with pytest.raises(ValueError):
        outcomes.record_outcome_event(conn, kind="clicked", url="https://x/1")


def test_outcome_event_requires_identifier(conn):
    with pytest.raises(ValueError):
        outcomes.record_outcome_event(conn, kind="opened")


def test_outcome_event_unknown_snapshot_id_raises(conn):
    insert_posting(conn, "p1")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.record_outcome_event(
            conn, kind="opened", posting_id="p1", snapshot_id="does-not-exist",
        )


def test_outcome_event_valid_snapshot_id_is_stored(conn):
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()
    snapshot = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    event = outcomes.record_outcome_event(
        conn, kind="opened", posting_id="p1", snapshot_id=snapshot["snapshot_id"], rank=1,
    )
    assert event["snapshot_id"] == snapshot["snapshot_id"]
    assert event["rank"] == 1


# --------------------------------------------------------------------------- #
# B2 (5.5 fix): rank is SERVER-DERIVED. The client sends snapshot_id only.
# --------------------------------------------------------------------------- #
def test_outcome_event_rank_derived_from_snapshot_and_posting(conn):
    """snapshot_id given, no rank: the rank comes from the item this snapshot
    holds for this posting -- the payload the real client sends."""
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    insert_version(conn, "p1")
    insert_version(conn, "p2")
    conn.commit()
    snapshot = outcomes.capture_snapshot(
        conn, surface="today",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
    )

    event = outcomes.record_outcome_event(
        conn, kind="opened", posting_id="p2", snapshot_id=snapshot["snapshot_id"],
    )
    assert event["snapshot_id"] == snapshot["snapshot_id"]
    assert event["rank"] == 2
    row = conn.execute(
        "SELECT snapshot_id, rank FROM outcome_events WHERE outcome_event_id=?",
        (event["outcome_event_id"],),
    ).fetchone()
    assert row["snapshot_id"] == snapshot["snapshot_id"]
    assert row["rank"] == 2


def test_outcome_event_derived_rank_survives_a_renumbering_serve(conn):
    """The seam failure this fix exists for: `build_queue` renumbers ranks on
    every serve, so the rank the client saw is not the rank the day's stored
    snapshot recorded. Deriving server-side reports the SNAPSHOT's rank, and
    reports it without the client ever sending one."""
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    insert_version(conn, "p1")
    insert_version(conn, "p2")
    conn.commit()
    # the day's snapshot put p1 second
    snapshot = outcomes.capture_snapshot(
        conn, surface="today",
        items=[{"posting_id": "p2", "rank": 1}, {"posting_id": "p1", "rank": 2}],
    )
    event = outcomes.record_outcome_event(
        conn, kind="opened", posting_id="p1", snapshot_id=snapshot["snapshot_id"],
    )
    assert event["rank"] == 2  # the snapshot's rank, not a client-side 1


def test_outcome_event_unmatched_posting_degrades_to_unattributed(conn):
    """No item in that snapshot matches this posting: the open is still
    recorded, unattributed (snapshot_id and rank NULL) -- never a ValueError.
    An open is a real fact even when the ranking side channel cannot place
    it."""
    insert_posting(conn, "p1")
    insert_posting(conn, "p_not_served")
    insert_version(conn, "p1")
    conn.commit()
    snapshot = outcomes.capture_snapshot(
        conn, surface="today", items=[{"posting_id": "p1", "rank": 1}],
    )

    event = outcomes.record_outcome_event(
        conn, kind="opened", posting_id="p_not_served",
        snapshot_id=snapshot["snapshot_id"],
    )
    assert event["snapshot_id"] is None
    assert event["rank"] is None
    assert event["posting_id"] == "p_not_served"
    row = conn.execute(
        "SELECT snapshot_id, rank, posting_id FROM outcome_events WHERE outcome_event_id=?",
        (event["outcome_event_id"],),
    ).fetchone()
    assert row["snapshot_id"] is None and row["rank"] is None
    assert row["posting_id"] == "p_not_served"


def test_outcome_event_unresolvable_url_with_snapshot_degrades(conn):
    """Same degradation when the event resolves to no posting_id at all (a
    url the graph has never heard of): unattributed, still recorded."""
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()
    snapshot = outcomes.capture_snapshot(
        conn, surface="today", items=[{"posting_id": "p1", "rank": 1}],
    )

    event = outcomes.record_outcome_event(
        conn, kind="opened", url="https://nowhere/never-seen",
        snapshot_id=snapshot["snapshot_id"],
    )
    assert event["snapshot_id"] is None
    assert event["rank"] is None
    assert event["url"] == "https://nowhere/never-seen"


def test_outcome_event_unknown_snapshot_id_still_raises_without_rank(conn):
    """The degradation is for an unmatched ITEM, not for a snapshot_id that
    names nothing -- that stays a clean ValueError (a caller bug)."""
    insert_posting(conn, "p1")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.record_outcome_event(
            conn, kind="opened", posting_id="p1", snapshot_id="does-not-exist",
        )


def test_outcome_event_rank_without_snapshot_id_is_stored_null(conn):
    """A rank that names no snapshot points at nothing: ignored, stored NULL,
    never left dangling for a later reader to pair with the wrong snapshot."""
    insert_posting(conn, "p1")
    conn.commit()
    event = outcomes.record_outcome_event(conn, kind="opened", posting_id="p1", rank=7)
    assert event["rank"] is None
    assert event["snapshot_id"] is None
    row = conn.execute(
        "SELECT rank FROM outcome_events WHERE outcome_event_id=?",
        (event["outcome_event_id"],),
    ).fetchone()
    assert row["rank"] is None


def test_outcome_event_explicit_bad_pair_still_raises(conn):
    """Rule 3 unchanged: an EXPLICIT (snapshot_id, rank) claim must be true."""
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()
    snapshot = outcomes.capture_snapshot(
        conn, surface="today", items=[{"posting_id": "p1", "rank": 1}],
    )
    with pytest.raises(ValueError):
        outcomes.record_outcome_event(
            conn, kind="opened", posting_id="p1", snapshot_id=snapshot["snapshot_id"], rank=99,
        )


# --------------------------------------------------------------------------- #
# Append-only invariants
# --------------------------------------------------------------------------- #
def test_capture_never_mutates_a_prior_snapshots_items(conn, monkeypatch):
    fake_profile = _FakeProfile({}, content_hash="ch-2")
    monkeypatch.setattr(outcomes, "_load_profile", lambda: fake_profile)
    ensure_profile_row(conn, "pv-2", content_hash="ch-2")
    insert_posting(conn, "p1")
    insert_version(conn, "p1", tier=2, odds="Strong match / Standard")
    conn.commit()

    first = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )
    before = dict(conn.execute(
        "SELECT * FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (first["snapshot_id"],),
    ).fetchone())

    outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-02T10:00:00",
    )

    after = dict(conn.execute(
        "SELECT * FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (first["snapshot_id"],),
    ).fetchone())
    assert before == after


# --------------------------------------------------------------------------- #
# F1: _source_category needs the adapter registry populated (import-time side
# effect), and an unregistered namespace records None, not graph's aggregator-
# default (that default exists for canonical-version RANKING; analytics must
# not fabricate a category for a namespace the registry never heard of).
# --------------------------------------------------------------------------- #
def test_source_category_registered_namespace_returns_its_category():
    assert registry.is_registered("greenhouse")
    assert outcomes._source_category("greenhouse:acme") == "direct"


def test_source_category_unregistered_namespace_returns_none():
    assert not registry.is_registered("totally-fake-namespace")
    assert outcomes._source_category("totally-fake-namespace:acme") is None


def test_source_category_legacy_string_returns_none():
    # A legacy CSV-import source string ("jobspy-indeed") has no ':' namespace
    # shape and is not a registered source_key -- None, not a fabricated
    # aggregator default.
    assert outcomes._source_category("jobspy-indeed") is None


def test_source_category_none_and_empty_return_none():
    assert outcomes._source_category(None) is None
    assert outcomes._source_category("") is None


def test_source_category_derived_during_capture(conn):
    insert_posting(conn, "p1")
    insert_version(conn, "p1", source="greenhouse:acme")
    conn.commit()
    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    assert result["items"][0]["source_category"] == "direct"


# --------------------------------------------------------------------------- #
# F2: caller-supplied posting_version_id / score_version_id are validated up
# front (ValueError, no partial write); any failure during the write rolls
# back so nothing partial survives.
# --------------------------------------------------------------------------- #
def test_ghost_posting_version_id_raises_with_no_partial_write(conn):
    insert_posting(conn, "p1")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(
            conn, surface="sweep",
            items=[{"posting_id": "p1", "rank": 1, "posting_version_id": "ghost-pv"}],
        )
    assert conn.in_transaction is False
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshots"
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshot_items"
    ).fetchone()["c"] == 0


def test_posting_version_id_belonging_to_a_different_posting_raises(conn):
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    v2 = insert_version(conn, "p2")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(
            conn, surface="sweep",
            items=[{"posting_id": "p1", "rank": 1, "posting_version_id": v2}],
        )


def test_ghost_score_version_id_raises_with_no_partial_write(conn):
    insert_posting(conn, "p1")
    conn.commit()
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(
            conn, surface="sweep",
            items=[{"posting_id": "p1", "rank": 1, "score_version_id": "ghost-sv"}],
        )
    assert conn.in_transaction is False
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshots"
    ).fetchone()["c"] == 0


def test_capture_snapshot_rolls_back_on_mid_write_failure(conn, monkeypatch):
    """Mutation-verifies the try/except/rollback wrapping itself (F2b), not
    just the up-front validation (F2a): item 1's header+item+recommendation
    rows are genuinely written before item 2's `_ensure_recommendation` call
    raises, so this only passes if the rollback actually undoes prior writes
    on THIS same call rather than merely never having written them."""
    fake_profile = _FakeProfile({}, content_hash="ch-rollback")
    monkeypatch.setattr(outcomes, "_load_profile", lambda: fake_profile)
    ensure_profile_row(conn, "pv-rollback", content_hash="ch-rollback")
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    insert_version(conn, "p1")
    insert_version(conn, "p2")
    conn.commit()

    calls = {"n": 0}
    original_ensure = outcomes._ensure_recommendation

    def _flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return original_ensure(*args, **kwargs)

    monkeypatch.setattr(outcomes, "_ensure_recommendation", _flaky)

    with pytest.raises(RuntimeError):
        outcomes.capture_snapshot(
            conn, surface="sweep",
            items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
        )
    assert conn.in_transaction is False
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshots"
    ).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM recommendation_snapshot_items"
    ).fetchone()["c"] == 0
    assert conn.execute("SELECT COUNT(*) AS c FROM recommendations").fetchone()["c"] == 0


# --------------------------------------------------------------------------- #
# F3: posting_version_id / score_version_id must describe the SAME version.
# --------------------------------------------------------------------------- #
def test_unscored_newer_version_does_not_win_over_the_scored_current_version(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(
        conn, "p1", version_id="v1", observed_at="2026-08-01T00:00:00",
        tier=1, odds="Weak match / High competition",
    )
    v2 = insert_version(
        conn, "p1", version_id="v2", observed_at="2026-08-02T00:00:00",
        tier=1, odds="Weak match / High competition",
    )
    score_id = insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=3, odds="Strong match / Standard",
    )
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}],
    )
    item = result["items"][0]
    # v2 is the LATEST version, but v1 is the one that's scored -- the item
    # must pair v1's score with v1's posting_version_id, not v2.
    assert item["posting_version_id"] == v1
    assert item["posting_version_id"] != v2
    assert item["score_version_id"] == score_id
    assert item["tier"] == 3


def test_explicit_posting_version_id_restricts_score_lookup_to_that_version(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1", version_id="v1", tier=1, odds="Weak match / High competition")
    v2 = insert_version(conn, "p1", version_id="v2", tier=1, odds="Weak match / High competition")
    # A score exists only against v2; the caller explicitly names v1.
    insert_score(conn, posting_version_id=v2, posting_id="p1", tier=5, odds="Strong match / Standard")
    conn.commit()

    result = outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1, "posting_version_id": v1}],
    )
    item = result["items"][0]
    assert item["posting_version_id"] == v1
    # v2's score must NOT leak onto a v1 claim -- no score exists for v1, so
    # this falls through to v1's own tier/odds, not v2's score.
    assert item["score_version_id"] is None
    assert item["tier"] == 1
    assert item["odds"] == "Weak match / High competition"


# --------------------------------------------------------------------------- #
# F4: the recommendations idempotency key includes version identity -- a new
# score mints a NEW recommendations row rather than freezing a stale claim.
# --------------------------------------------------------------------------- #
def test_new_score_mints_a_new_recommendation_row(conn, monkeypatch):
    fake_profile = _FakeProfile({}, content_hash="ch-f4")
    monkeypatch.setattr(outcomes, "_load_profile", lambda: fake_profile)
    ensure_profile_row(conn, "pv-f4", content_hash="ch-f4")
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1", tier=1)
    score1 = insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=2, odds="Strong match / Standard",
        profile_version_id="pv-f4",
    )
    conn.commit()

    first = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-01T10:00:00",
    )

    # Supersede score1 with score2 -- a rescore, not a re-version.
    conn.execute(
        "UPDATE score_versions SET superseded_at=? WHERE score_version_id=?", (AT, score1),
    )
    score2 = insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=4, odds="Strong match / Standard",
        profile_version_id="pv-f4", created_at="2026-08-02T00:00:00",
    )
    conn.commit()

    second = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], at="2026-08-02T10:00:00",
    )

    rec1 = first["items"][0]["recommendation_id"]
    rec2 = second["items"][0]["recommendation_id"]
    assert rec1 is not None and rec2 is not None
    assert rec1 != rec2

    rows = {
        r["recommendation_id"]: dict(r)
        for r in conn.execute("SELECT * FROM recommendations").fetchall()
    }
    assert len(rows) == 2
    assert rows[rec1]["score_version_id"] == score1
    assert rows[rec2]["score_version_id"] == score2
    assert rows[rec1]["idempotency_key"] != rows[rec2]["idempotency_key"]


# --------------------------------------------------------------------------- #
# F5: role_family mirrors rubric.py's backslash strip on the description.
# --------------------------------------------------------------------------- #
def test_role_family_backslash_split_keyword_matches_after_strip():
    profile = _FakeProfile({"support": ("helpdesk",)})
    # "hel\\pdesk" strips to "helpdesk" before matching, exactly like
    # rubric.py's `d = (desc or "").lower().replace("\\", "")`.
    description = "hel\\pdesk specialist"
    assert outcomes._role_family("Generic Title", description, profile) == "support"


# --------------------------------------------------------------------------- #
# F11-lite: `_current_score` is a UNION ALL of two index-usable branches now,
# not `posting_id=? OR posting_version_id IN (...)`. No behavior change --
# these pin the exact cases the rewrite must keep answering identically.
# --------------------------------------------------------------------------- #
def test_current_score_union_rewrite_posting_id_branch(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1")
    score_id = insert_score(conn, posting_version_id=v1, posting_id="p1", tier=3, odds="Strong match / Standard")
    conn.commit()
    row = outcomes._current_score(conn, "p1")
    assert row["score_version_id"] == score_id


def test_current_score_union_rewrite_legacy_join_branch(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1")
    ensure_profile_row(conn, "legacy-import")
    score_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, created_at, "
        "superseded_at) VALUES (?,NULL,?,?,?,'legacy-import',?,?,?,?,NULL)",
        (score_id, v1, "legacy-import", score_id, 4, "Target", None, AT),
    )
    conn.commit()
    row = outcomes._current_score(conn, "p1")
    assert row["score_version_id"] == score_id


def test_current_score_explicit_posting_version_id_restricts_lookup(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1")
    v2 = insert_version(conn, "p1")
    insert_score(conn, posting_version_id=v1, posting_id="p1", tier=1, odds="Target")
    score2 = insert_score(conn, posting_version_id=v2, posting_id="p1", tier=2, odds="Target")
    conn.commit()
    row = outcomes._current_score(conn, "p1", posting_version_id=v2)
    assert row["score_version_id"] == score2


# --------------------------------------------------------------------------- #
# F12: header `scorer_hash` is derived from items' winning score rows.
# --------------------------------------------------------------------------- #
def test_scorer_hash_derived_when_all_items_share_one(conn):
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    v1 = insert_version(conn, "p1")
    v2 = insert_version(conn, "p2")
    insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=2, odds="Strong match / Standard",
        scorer_hash="scorer-x",
    )
    insert_score(
        conn, posting_version_id=v2, posting_id="p2", tier=2, odds="Strong match / Standard",
        scorer_hash="scorer-x",
    )
    conn.commit()
    result = outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
    )
    assert result["scorer_hash"] == "scorer-x"
    row = conn.execute(
        "SELECT scorer_hash FROM recommendation_snapshots WHERE snapshot_id=?",
        (result["snapshot_id"],),
    ).fetchone()
    assert row["scorer_hash"] == "scorer-x"


def test_scorer_hash_none_when_mixed(conn):
    insert_posting(conn, "p1")
    insert_posting(conn, "p2")
    v1 = insert_version(conn, "p1")
    v2 = insert_version(conn, "p2")
    insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=2, odds="Strong match / Standard",
        scorer_hash="scorer-x",
    )
    insert_score(
        conn, posting_version_id=v2, posting_id="p2", tier=2, odds="Strong match / Standard",
        scorer_hash="scorer-y",
    )
    conn.commit()
    result = outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1}, {"posting_id": "p2", "rank": 2}],
    )
    assert result["scorer_hash"] is None


def test_scorer_hash_none_when_no_scores(conn):
    insert_posting(conn, "p1")
    insert_version(conn, "p1")
    conn.commit()
    result = outcomes.capture_snapshot(conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}])
    assert result["scorer_hash"] is None


def test_scorer_hash_explicit_wins_over_derivation(conn):
    insert_posting(conn, "p1")
    v1 = insert_version(conn, "p1")
    insert_score(
        conn, posting_version_id=v1, posting_id="p1", tier=2, odds="Strong match / Standard",
        scorer_hash="scorer-x",
    )
    conn.commit()
    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], scorer_hash="explicit-hash",
    )
    assert result["scorer_hash"] == "explicit-hash"


# --------------------------------------------------------------------------- #
# F13: explicit queue_size, defaulting to len(items); negative rejected.
# --------------------------------------------------------------------------- #
def test_queue_size_defaults_to_len_items(conn):
    insert_posting(conn, "p1")
    conn.commit()
    result = outcomes.capture_snapshot(conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}])
    assert result["queue_size"] == 1


def test_queue_size_explicit_larger_than_items_is_legal(conn):
    insert_posting(conn, "p1")
    conn.commit()
    result = outcomes.capture_snapshot(
        conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}], queue_size=300,
    )
    assert result["queue_size"] == 300
    row = conn.execute(
        "SELECT queue_size FROM recommendation_snapshots WHERE snapshot_id=?",
        (result["snapshot_id"],),
    ).fetchone()
    assert row["queue_size"] == 300


def test_queue_size_negative_raises(conn):
    with pytest.raises(ValueError):
        outcomes.capture_snapshot(conn, surface="sweep", items=[], queue_size=-1)


# --------------------------------------------------------------------------- #
# F14: idempotency_key dedupes outcome_events; no key means no dedupe.
# --------------------------------------------------------------------------- #
def test_idempotency_key_dedupes_outcome_events(conn):
    insert_posting(conn, "p1")
    conn.commit()
    first = outcomes.record_outcome_event(
        conn, kind="opened", posting_id="p1", idempotency_key="k1",
    )
    second = outcomes.record_outcome_event(
        conn, kind="opened", posting_id="p1", idempotency_key="k1",
    )
    assert first["outcome_event_id"] == second["outcome_event_id"]
    assert conn.execute("SELECT COUNT(*) AS c FROM outcome_events").fetchone()["c"] == 1


def test_no_idempotency_key_never_dedupes(conn):
    insert_posting(conn, "p1")
    conn.commit()
    first = outcomes.record_outcome_event(conn, kind="opened", posting_id="p1")
    second = outcomes.record_outcome_event(conn, kind="opened", posting_id="p1")
    assert first["outcome_event_id"] != second["outcome_event_id"]
    assert conn.execute("SELECT COUNT(*) AS c FROM outcome_events").fetchone()["c"] == 2


# --------------------------------------------------------------------------- #
# F15: (snapshot_id, rank) must reference an actual snapshot item.
# --------------------------------------------------------------------------- #
def test_outcome_event_rank_against_empty_snapshot_raises(conn):
    insert_posting(conn, "p1")
    conn.commit()
    snapshot = outcomes.capture_snapshot(conn, surface="sweep", items=[])
    with pytest.raises(ValueError):
        outcomes.record_outcome_event(
            conn, kind="opened", posting_id="p1", snapshot_id=snapshot["snapshot_id"], rank=99,
        )


# --------------------------------------------------------------------------- #
# F16: recommendation_snapshot_items carries title/company.
# --------------------------------------------------------------------------- #
def test_snapshot_item_title_and_company_populated_from_version(conn):
    insert_posting(conn, "p1")
    insert_version(conn, "p1", title="Support Engineer", company="Acme Robotics")
    conn.commit()
    result = outcomes.capture_snapshot(conn, surface="sweep", items=[{"posting_id": "p1", "rank": 1}])
    assert result["items"][0]["title"] == "Support Engineer"
    assert result["items"][0]["company"] == "Acme Robotics"
    row = conn.execute(
        "SELECT title, company FROM recommendation_snapshot_items WHERE snapshot_id=?",
        (result["snapshot_id"],),
    ).fetchone()
    assert row["title"] == "Support Engineer"
    assert row["company"] == "Acme Robotics"


def test_snapshot_item_title_and_company_explicit_override(conn):
    insert_posting(conn, "p1")
    insert_version(conn, "p1", title="Support Engineer", company="Acme Robotics")
    conn.commit()
    result = outcomes.capture_snapshot(
        conn, surface="sweep",
        items=[{"posting_id": "p1", "rank": 1, "title": "Custom Title", "company": "Custom Co"}],
    )
    assert result["items"][0]["title"] == "Custom Title"
    assert result["items"][0]["company"] == "Custom Co"
