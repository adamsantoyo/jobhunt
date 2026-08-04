"""Phase 3.3: the normalization bridge and aggregator -> direct local resolution.

Four rules, each with a test that fails when it is reverted:

  THE BRIDGE IS NON-DESTRUCTIVE. A legacy URL alias gains a normalized twin; the
    legacy alias, the posting, and the state stay exactly where they were. This is
    the Phase 4 cutover unblocker -- without it the first canonical scrape of a URL
    the legacy corpus already knows mints a NEW posting, and the user's status and
    notes stay on the old one.
  AMBIGUITY IS ARCHIVED, NEVER RESOLVED. Two legacy postings behind one normalized
    URL bridges NOTHING and writes evidence naming both. Same rule for local
    matching: two plausible direct candidates is `canonical-match-ambiguous`, not a
    coin flip. A wrong merge moves a user's notes onto someone else's job.
  A MATCH REDIRECTS, IT DOES NOT DELETE. `posting_redirects` loser -> survivor,
    survivor chosen by identity precedence (rank-0 requisition alias, then older
    first_seen_at, then posting_id). The loser keeps every alias and version.
  ONE MATCH, ONE INVALIDATION. Exactly one `score_invalidations` row, for the
    survivor. The corpus is not touched.

Every database is created under `tmp_path` by `make_connect`. Nothing here can
reach webapp/app.db.
"""
import json

import pytest

from backend.sources import resolver, runstore
from backend.sources.contract import SourceCategory
from backend.tests.test_source_scheduler_fakes import make_connect

AT = "2026-08-04T12:00:00+00:00"


def category_map(**mapping):
    """A `category_of` that reads a plain dict, defaulting to AGGREGATOR.

    Injected rather than taken from the adapter registry: these tests are about
    the resolution RULES, and a fixture that had to register sixteen real adapters
    to say "this namespace is a board" would be testing the registry.
    """
    table = {k.replace("__", ":"): v for k, v in mapping.items()}

    def _category(namespace: str) -> SourceCategory:
        return table.get(namespace, SourceCategory.AGGREGATOR)

    return _category


@pytest.fixture
def conn(tmp_path):
    connect = make_connect(tmp_path)
    c = connect()
    try:
        yield c
    finally:
        c.close()


def run(conn, run_uid="run-1", *, requested_at=AT, status="succeeded"):
    conn.execute(
        "INSERT OR IGNORE INTO pipeline_runs (run_uid, kind, status, requested_at) "
        "VALUES (?,'daily',?,?)",
        (run_uid, status, requested_at),
    )
    return run_uid


def posting(conn, posting_id, *, first_seen=AT):
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, first_seen, first_seen),
    )
    return posting_id


def alias(conn, posting_id, *, kind, namespace, value, confidence=1.0, at=AT):
    alias_id = runstore.new_uid()
    conn.execute(
        "INSERT INTO posting_aliases (alias_id, posting_id, alias_kind, namespace, value, "
        "url, req_id, provenance_json, confidence, valid_from, valid_to) "
        "VALUES (?,?,?,?,?,?,NULL,NULL,?,?,NULL)",
        (alias_id, posting_id, kind, namespace, value, value, confidence, at),
    )
    return alias_id


def aliases_of(conn, posting_id):
    return {
        (r["alias_kind"], r["namespace"], r["value"])
        for r in conn.execute(
            "SELECT alias_kind, namespace, value FROM posting_aliases "
            "WHERE posting_id=? AND valid_to IS NULL",
            (posting_id,),
        )
    }


def evidence_kinds(conn):
    return [r["evidence_kind"] for r in conn.execute(
        "SELECT evidence_kind FROM identity_evidence ORDER BY evidence_kind"
    )]


def invalidations(conn):
    return [
        (r["posting_id"], r["reason"], r["consumed_at"])
        for r in conn.execute(
            "SELECT posting_id, reason, consumed_at FROM score_invalidations "
            "ORDER BY posting_id, reason"
        )
    ]


def redirects(conn):
    return {
        (r["from_posting_id"], r["to_posting_id"]): r["reason"]
        for r in conn.execute(
            "SELECT from_posting_id, to_posting_id, reason FROM posting_redirects"
        )
    }


# --------------------------------------------------------------------------- #
# Pure normalizers
# --------------------------------------------------------------------------- #
def test_company_key_folds_legal_forms_but_not_meaningful_tokens():
    """`scraper.canon_company` is deliberately NOT imported: it belongs to the legacy
    CSV pipeline, and coupling canonical identity to it would let a legacy tweak
    silently re-partition the canonical corpus.
    """
    assert resolver.company_key("Acme Robotics, Inc.") == resolver.company_key("acme robotics")
    assert resolver.company_key("The Acme Group LLC") == "acme"
    # Dropping a MEANINGFUL token is how two employers get merged.
    assert resolver.company_key("Acme") != resolver.company_key("Acme Robotics")


def test_title_similarity_rejects_a_level_difference():
    """A level difference is a different requisition, not a fuzzy match."""
    assert resolver.title_jaccard("Support Engineer", "Support Engineer") == 1.0
    assert resolver.title_jaccard(
        "Support Engineer", "Senior Support Engineer"
    ) < resolver.MIN_TITLE_JACCARD
    assert resolver.title_jaccard(
        "Technical Support Engineer II", "Technical Support Engineer, II"
    ) >= resolver.MIN_TITLE_JACCARD


def test_location_compatibility_is_permissive_but_not_blind():
    """One of six conditions, so it may be permissive; two-letter tokens are excluded
    because "CA" would make every California posting compatible with every other."""
    assert resolver.locations_compatible("San Francisco, CA", "San Francisco Bay Area")
    assert resolver.locations_compatible("", "San Francisco, CA")
    assert resolver.locations_compatible("San Francisco, CA", None)
    assert not resolver.locations_compatible("San Francisco, CA", "Austin, TX")
    assert not resolver.locations_compatible("San Jose, CA", "Los Angeles, CA"), (
        "a two-letter state token must not be the thing that makes two cities compatible"
    )


# --------------------------------------------------------------------------- #
# The bridge
# --------------------------------------------------------------------------- #
def test_the_bridge_adds_a_normalized_alias_beside_the_legacy_one(conn):
    """THE PHASE 4 UNBLOCKER.

    Migration 11 wrote `namespace='legacy-url'` with the RAW url; every canonical
    write since uses `namespace='url'` with `normalize_url`. The two never meet, so
    a scrape of a URL the legacy corpus knows resolves to a brand-new posting and
    the user's job_state follows nothing. The bridge makes the next scrape land on
    the legacy posting inside `write_records`, with no special case anywhere.
    """
    posting(conn, "legacy-1")
    alias(conn, "legacy-1", kind="url", namespace="legacy-url",
          value="https://Boards.example.com/acme/jobs/42/?utm_source=newsletter#apply")

    report = resolver.bridge_legacy_url_aliases(conn, run_uid="run-1", at=AT)

    assert report["bridged"] == 1
    assert report["ambiguous"] == 0 and report["canonical_matches"] == 0
    assert ("url", "url", "https://boards.example.com/acme/jobs/42") in aliases_of(conn, "legacy-1")
    # Non-destructive: the legacy alias is untouched and still active.
    assert any(ns == "legacy-url" for _, ns, _ in aliases_of(conn, "legacy-1"))
    row = conn.execute(
        "SELECT confidence, provenance_json FROM posting_aliases WHERE namespace='url'"
    ).fetchone()
    assert row["confidence"] == resolver.BRIDGE_ALIAS_CONFIDENCE
    assert json.loads(row["provenance_json"])["bridge"] == "legacy-url"
    assert invalidations(conn) == [], "a plain bridge invalidates no score"


def test_the_bridge_is_idempotent(conn):
    """It runs every pass; after the first time it must cost nothing."""
    posting(conn, "legacy-1")
    alias(conn, "legacy-1", kind="url", namespace="legacy-url",
          value="https://boards.example.com/acme/jobs/42")

    first = resolver.bridge_legacy_url_aliases(conn, run_uid="run-1", at=AT)
    second = resolver.bridge_legacy_url_aliases(conn, run_uid="run-2", at=AT)

    assert first["bridged"] == 1
    assert second["bridged"] == 0 and second["already_bridged"] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM posting_aliases WHERE namespace='url'"
    ).fetchone()[0] == 1


def test_the_canonical_match_half_of_the_bridge_is_idempotent_too(conn):
    """The OTHER branch, which ran every day forever.

    `test_the_bridge_is_idempotent` covers the branch that inserts an alias, and
    that one settles because the alias lookup deciding the branch is the same one
    guarding the insert. The canonical-match branch had no such guard: the redirect
    is `INSERT OR IGNORE` on the loser's primary key so the TABLE settled, but the
    return value was dropped, and `emit_invalidation`'s id mixes in `run_uid` -- so
    a collision merged on Monday queued the survivor for rescoring again on Tuesday,
    Wednesday, and every day after, and `canonical_matches` reported a fresh
    duplicate the cutover had already dealt with. Three runs, not two: the second
    run is where a per-run id first differs, and the third is what proves the
    counter is not merely off by one.
    """
    normalized = "https://boards.example.com/acme/jobs/42"
    for n in (1, 2, 3):
        run(conn, f"run-{n}")
    posting(conn, "legacy-old", first_seen="2025-01-01T00:00:00+00:00")
    posting(conn, "canonical-new", first_seen="2026-08-01T00:00:00+00:00")
    alias(conn, "legacy-old", kind="url", namespace="legacy-url", value=normalized)
    alias(conn, "canonical-new", kind="url", namespace="url", value=normalized, confidence=0.5)
    alias(conn, "canonical-new", kind="source_req", namespace="greenhouse:acme", value="42")

    reports = [
        resolver.bridge_legacy_url_aliases(conn, run_uid=f"run-{n}", at=AT) for n in (1, 2, 3)
    ]

    assert reports[0]["canonical_matches"] == 1
    assert reports[0]["invalidated"] == ("canonical-new",)
    assert reports[0]["already_matched"] == 0
    for later in reports[1:]:
        assert later["canonical_matches"] == 0, "a settled merge is not a new duplicate"
        assert later["invalidated"] == (), "the survivor must not re-enter the work list"
        assert later["already_matched"] == 1

    assert redirects(conn) == {("legacy-old", "canonical-new"): "normalization-bridge"}
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_evidence WHERE evidence_kind='canonical-match'"
    ).fetchone()[0] == 1
    assert invalidations(conn) == [("canonical-new", "canonical-match", None)]
    assert conn.execute("SELECT COUNT(*) FROM score_invalidations").fetchone()[0] == 1


def test_an_ambiguous_normalization_bridges_nothing_and_is_archived(conn):
    """Two raw URLs differing only in a tracking parameter, recorded against two
    different legacy lineages. Bridging either one asserts an identity the evidence
    does not support, so NOTHING is bridged and both postings are named.
    """
    posting(conn, "legacy-a")
    posting(conn, "legacy-b")
    alias(conn, "legacy-a", kind="url", namespace="legacy-url",
          value="https://boards.example.com/acme/jobs/42?utm_campaign=a")
    alias(conn, "legacy-b", kind="url", namespace="legacy-url",
          value="https://boards.example.com/acme/jobs/42?gclid=b")

    report = resolver.bridge_legacy_url_aliases(conn, run_uid="run-1", at=AT)

    assert report["ambiguous"] == 1 and report["bridged"] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM posting_aliases WHERE namespace='url'"
    ).fetchone()[0] == 0
    archived = conn.execute(
        "SELECT posting_id, evidence_json FROM identity_evidence "
        "WHERE evidence_kind='normalization-bridge-ambiguous' ORDER BY posting_id"
    ).fetchall()
    assert [r["posting_id"] for r in archived] == ["legacy-a", "legacy-b"]
    assert json.loads(archived[0]["evidence_json"])["candidate_posting_ids"] == [
        "legacy-a", "legacy-b"
    ]
    assert invalidations(conn) == []


def test_a_bridge_collision_is_a_canonical_match_with_survivor_precedence(conn):
    """Both corpora know this URL and disagree about which posting it is -- exactly
    the duplicate the cutover would otherwise ship.

    Survivor precedence is the system's identity precedence, not a fresh rule: a
    posting carrying a rank-0 `source_req` alias is claimed by a source's own
    requisition id, which outranks a posting evidenced only by a URL.
    """
    normalized = "https://boards.example.com/acme/jobs/42"
    run(conn)
    posting(conn, "legacy-old", first_seen="2025-01-01T00:00:00+00:00")
    posting(conn, "canonical-new", first_seen="2026-08-01T00:00:00+00:00")
    alias(conn, "legacy-old", kind="url", namespace="legacy-url", value=normalized)
    alias(conn, "canonical-new", kind="url", namespace="url", value=normalized, confidence=0.5)
    alias(conn, "canonical-new", kind="source_req", namespace="greenhouse:acme", value="42")

    report = resolver.bridge_legacy_url_aliases(conn, run_uid="run-1", at=AT)

    assert report["canonical_matches"] == 1 and report["bridged"] == 0
    # The requisition-bearing posting wins despite being much younger.
    assert redirects(conn) == {("legacy-old", "canonical-new"): "normalization-bridge"}
    assert "canonical-match" in evidence_kinds(conn)
    assert invalidations(conn) == [("canonical-new", "canonical-match", None)]
    # Nothing deleted, nothing repointed: the loser keeps its aliases as evidence.
    assert ("url", "legacy-url", normalized) in aliases_of(conn, "legacy-old")


def test_without_a_requisition_the_older_posting_survives(conn):
    """Precedence rule 2: the older posting is the one a user's status and notes are
    most likely already attached to, and preserving those is the point of the bridge.
    """
    normalized = "https://boards.example.com/acme/jobs/42"
    run(conn)
    posting(conn, "zzz-older", first_seen="2025-01-01T00:00:00+00:00")
    posting(conn, "aaa-newer", first_seen="2026-08-01T00:00:00+00:00")
    alias(conn, "zzz-older", kind="url", namespace="legacy-url", value=normalized)
    alias(conn, "aaa-newer", kind="url", namespace="url", value=normalized, confidence=0.5)

    resolver.bridge_legacy_url_aliases(conn, run_uid="run-1", at=AT)

    assert redirects(conn) == {("aaa-newer", "zzz-older"): "normalization-bridge"}, (
        "posting_id sort order must not beat first_seen_at"
    )
    assert invalidations(conn) == [("zzz-older", "canonical-match", None)]


# --------------------------------------------------------------------------- #
# Aggregator -> direct local resolution
# --------------------------------------------------------------------------- #
def _index(*observations):
    return resolver.build_direct_index(observations)


def direct(posting_id, *, company="Acme Robotics", title="Support Engineer",
           location="San Francisco, CA", namespace="greenhouse:acme"):
    return resolver.DirectObservation(
        posting_id=posting_id, namespace=namespace, company=company,
        title=title, location=location,
    )


def test_a_true_match_redirects_and_invalidates_exactly_one_posting(conn):
    """The whole point: after this, the survivor's canonical version is the BOARD's
    dated, described record rather than the aggregator's undated one -- which is what
    makes the ghost-listing caps stop firing and the tier actually move.
    """
    run(conn)
    posting(conn, "board-1")
    posting(conn, "agg-1")
    alias(conn, "board-1", kind="source_req", namespace="greenhouse:acme", value="42")

    report = resolver.resolve_aggregators(
        conn, run_uid="run-1",
        subjects=[("agg-1", "Acme Robotics, Inc.", "Support Engineer",
                   "San Francisco Bay Area")],
        index=_index(direct("board-1")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT),
        at=AT,
    )

    assert report["matched"] == (("agg-1", "board-1"),)
    assert report["ambiguous"] == 0
    assert redirects(conn) == {("agg-1", "board-1"): "aggregator-local-resolution"}
    assert invalidations(conn) == [("board-1", "canonical-match", None)], (
        "one match invalidates ONE posting -- never a corpus sweep"
    )
    assert conn.execute("SELECT COUNT(*) FROM score_invalidations").fetchone()[0] == 1


@pytest.mark.parametrize("company,title,location", [
    ("Acme Robotics", "Senior Support Engineer", "San Francisco, CA"),  # level differs
    ("Acme Analytics", "Support Engineer", "San Francisco, CA"),        # employer differs
    ("Acme Robotics", "Support Engineer", "Austin, TX"),                # location conflicts
])
def test_a_near_miss_resolves_nothing(conn, company, title, location):
    """Every condition must hold. Any one of them alone matches wildly: one employer
    posts forty roles, one title recurs across thirty employers.
    """
    posting(conn, "board-1")
    posting(conn, "agg-1")

    report = resolver.resolve_aggregators(
        conn, run_uid="run-1", subjects=[("agg-1", company, title, location)],
        index=_index(direct("board-1")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT), at=AT,
    )

    assert report["matched"] == () and report["ambiguous"] == 0
    assert redirects(conn) == {}
    assert invalidations(conn) == []


def test_two_plausible_candidates_are_archived_rather_than_guessed(conn):
    """One company posting the same title on two boards. Picking either is a coin
    flip whose cost is a user's notes on the wrong job, so the evidence names BOTH
    and nothing is merged.
    """
    # The run exists, so a merge WOULD succeed here: this test has to fail on the
    # guard being gone, not on an incidental foreign key.
    run(conn)
    posting(conn, "board-1")
    posting(conn, "board-2")
    posting(conn, "agg-1")

    report = resolver.resolve_aggregators(
        conn, run_uid="run-1",
        subjects=[("agg-1", "Acme Robotics", "Support Engineer", "San Francisco, CA")],
        index=_index(direct("board-1"), direct("board-2", namespace="lever:acme")),
        category_of=category_map(
            greenhouse__acme=SourceCategory.DIRECT, lever__acme=SourceCategory.DIRECT
        ),
        at=AT,
    )

    assert report["matched"] == () and report["ambiguous"] == 1
    assert redirects(conn) == {}
    assert invalidations(conn) == []
    archived = conn.execute(
        "SELECT posting_id, evidence_json FROM identity_evidence "
        "WHERE evidence_kind='canonical-match-ambiguous'"
    ).fetchone()
    assert archived["posting_id"] == "agg-1"
    assert json.loads(archived["evidence_json"])["candidate_posting_ids"] == [
        "board-1", "board-2"
    ], "the evidence must name EVERY candidate, not the one that happened to sort first"


def test_a_subject_already_claimed_by_a_direct_requisition_is_left_alone(conn):
    """A rank-0 requisition alias in a DIRECT namespace is the authoritative claim.
    Nothing here may redirect a posting a board has claimed by requisition.
    """
    posting(conn, "board-1")
    posting(conn, "agg-1")
    alias(conn, "agg-1", kind="source_req", namespace="greenhouse:acme", value="99")

    report = resolver.resolve_aggregators(
        conn, run_uid="run-1",
        subjects=[("agg-1", "Acme Robotics", "Support Engineer", "San Francisco, CA")],
        index=_index(direct("board-1")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT), at=AT,
    )

    assert report["matched"] == ()
    assert redirects(conn) == {}


def test_a_posting_never_resolves_to_itself(conn):
    posting(conn, "p-1")
    report = resolver.resolve_aggregators(
        conn, run_uid="run-1",
        subjects=[("p-1", "Acme Robotics", "Support Engineer", "San Francisco, CA")],
        index=_index(direct("p-1")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT), at=AT,
    )
    assert report["matched"] == () and redirects(conn) == {}


def test_resolution_is_idempotent(conn):
    """The daily run re-resolves what it resolved yesterday; the second time must
    cost one ignored insert and no second invalidation."""
    run(conn)
    posting(conn, "board-1")
    posting(conn, "agg-1")
    subjects = [("agg-1", "Acme Robotics", "Support Engineer", "San Francisco, CA")]
    kwargs = dict(
        index=_index(direct("board-1")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT), at=AT,
    )

    resolver.resolve_aggregators(conn, run_uid="run-1", subjects=subjects, **kwargs)
    resolver.resolve_aggregators(conn, run_uid="run-1", subjects=subjects, **kwargs)

    assert len(redirects(conn)) == 1
    assert conn.execute("SELECT COUNT(*) FROM score_invalidations").fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM identity_evidence WHERE evidence_kind='canonical-match'"
    ).fetchone()[0] == 1


def test_the_direct_index_only_contains_direct_inventory(conn):
    """An aggregator cannot resolve INTO another aggregator: that is two mirrors of
    an unknown original, and merging them asserts an identity neither claimed."""
    runstore.create_pipeline_run(conn, run_uid="run-1", kind="daily", requested_at=AT)
    runstore.create_source_run(
        conn, source_run_id="sr-1", run_uid="run-1", source="greenhouse:acme", attempt=1
    )
    posting(conn, "board-1")
    posting(conn, "agg-1")
    for posting_id, namespace in (("board-1", "greenhouse:acme"), ("agg-1", "jobspy:indeed")):
        version_id = runstore.posting_version_id_for(posting_id, f"sha256:{posting_id}")
        conn.execute(
            "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, "
            "source_run_id, version_hash, observed_at, title, company, location, source, "
            "payload_json) VALUES (?,?,'source',?,?,?,?,?,?,?,'{}')",
            (version_id, posting_id, "sr-1", f"sha256:{posting_id}", AT,
             "Support Engineer", "Acme Robotics", "San Francisco, CA", namespace),
        )
        conn.execute(
            "INSERT INTO run_postings (run_uid, posting_id, posting_version_id, source_run_id, "
            "present, first_seen_in_run, recorded_at, membership_kind) "
            "VALUES ('run-1',?,?,'sr-1',1,1,?, 'snapshot')",
            (posting_id, version_id, AT),
        )

    observations = resolver.direct_observations(
        conn, run_uid="run-1",
        category_of=category_map(
            greenhouse__acme=SourceCategory.DIRECT, jobspy__indeed=SourceCategory.AGGREGATOR
        ),
    )
    assert [o.posting_id for o in observations] == ["board-1"]


def test_an_unregistered_namespace_is_never_treated_as_direct_inventory(conn):
    """`graph.registry_category` degrades an unknown source key to AGGREGATOR, which
    is conservative twice over: it cannot outrank a real board in canonical
    selection, and it can never become something aggregators are merged INTO.
    """
    from backend.sources import graph

    assert graph.registry_category("not-a-real-source:x") is SourceCategory.AGGREGATOR
    assert graph.registry_category("") is SourceCategory.AGGREGATOR
    assert graph.registry_category("") not in resolver.DIRECT_CATEGORIES


# --------------------------------------------------------------------------- #
# Invalidations
# --------------------------------------------------------------------------- #
def test_invalidations_are_consumed_only_by_the_ids_a_pass_actually_covered(conn):
    """Consumption is a completion step, and a partial pass must not clear work it
    did not do."""
    run(conn, "run-1")
    run(conn, "run-2")
    posting(conn, "p-1")
    posting(conn, "p-2")
    resolver.emit_invalidation(conn, posting_id="p-1", reason="canonical-match",
                               run_uid="run-1", at=AT)
    resolver.emit_invalidation(conn, posting_id="p-2", reason="canonical-match",
                               run_uid="run-1", at=AT)

    assert resolver.open_invalidations(conn) == ["p-1", "p-2"]
    assert resolver.consume_invalidations(
        conn, run_uid="run-1", at=AT, posting_ids=["p-1"]
    ) == 1
    assert resolver.open_invalidations(conn) == ["p-2"]

    assert resolver.consume_invalidations(conn, run_uid="run-2", at=AT) == 1
    assert resolver.open_invalidations(conn) == []
    # Consumed rows stay as evidence.
    assert conn.execute("SELECT COUNT(*) FROM score_invalidations").fetchone()[0] == 2


def test_open_invalidations_are_cursor_paginated(conn):
    """Same contract as `runstore.dirty_posting_ids`: `limit` alone re-returns the
    same first N, because this is a query and nothing records consumption."""
    run(conn)
    for n in range(5):
        posting(conn, f"p-{n}")
        resolver.emit_invalidation(conn, posting_id=f"p-{n}", reason="canonical-match",
                                   run_uid="run-1", at=AT)
    assert resolver.open_invalidations(conn, limit=2) == ["p-0", "p-1"]
    assert resolver.open_invalidations(conn, limit=2) == ["p-0", "p-1"]
    assert resolver.open_invalidations(conn, limit=2, after="p-1") == ["p-2", "p-3"]
