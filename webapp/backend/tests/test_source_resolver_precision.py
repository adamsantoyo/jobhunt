"""Phase 3.7: resolver precision fixtures -- the edges `test_source_resolver.py`
does not already cover.

`test_source_resolver.py` already proves: (a) a clean aggregator-to-direct match
redirects and invalidates exactly one posting
(`test_a_true_match_redirects_and_invalidates_exactly_one_posting`); (b) two
plausible direct survivors archive as an ambiguity and guess nothing
(`test_two_plausible_candidates_are_archived_rather_than_guessed`); (f) the
legacy-url bridge's three outcomes -- a normalized match bridges
(`test_the_bridge_adds_a_normalized_alias_beside_the_legacy_one`), an ambiguous
collision archives (`test_an_ambiguous_normalization_bridges_nothing_and_is_archived`),
and a settled collision re-run mints zero new evidence
(`test_the_canonical_match_half_of_the_bridge_is_idempotent_too`, added in
762e76d). This file does not repeat any of those.

What is missing is three CHARACTERIZATION tests: they pin what the resolver
ACTUALLY does today at three precision edges, not what an ideal resolver would
do. Each is a deliberate wave-2 deferred issue, named in its own docstring, so
that a future threshold or algorithm decision has to touch this file and
consciously flip the pinned expectation rather than silently start failing a
test nobody remembers writing.
"""
from __future__ import annotations

import pytest

from backend.sources import resolver
from backend.sources.contract import SourceCategory
from backend.tests.test_source_scheduler_fakes import make_connect

AT = "2026-08-04T12:00:00+00:00"


def category_map(**mapping):
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
    alias_id = resolver.runstore.new_uid()
    conn.execute(
        "INSERT INTO posting_aliases (alias_id, posting_id, alias_kind, namespace, value, "
        "url, req_id, provenance_json, confidence, valid_from, valid_to) "
        "VALUES (?,?,?,?,?,?,NULL,NULL,?,?,NULL)",
        (alias_id, posting_id, kind, namespace, value, value, confidence, at),
    )
    return alias_id


def redirects(conn):
    return {
        (r["from_posting_id"], r["to_posting_id"]): r["reason"]
        for r in conn.execute(
            "SELECT from_posting_id, to_posting_id, reason FROM posting_redirects"
        )
    }


def direct(posting_id, *, company="Acme Robotics", title="Support Engineer",
           location="San Francisco, CA", namespace="greenhouse:acme"):
    return resolver.DirectObservation(
        posting_id=posting_id, namespace=namespace, company=company,
        title=title, location=location,
    )


def _index(*observations):
    return resolver.build_direct_index(observations)


# --------------------------------------------------------------------------- #
# (c) Title Jaccard exactly at 0.75 admits a seniority variant
# --------------------------------------------------------------------------- #
def test_title_jaccard_at_exactly_the_floor_admits_a_seniority_variant():
    """CHARACTERIZATION -- pins CURRENT behavior, not intended behavior.

    TRACKED WAVE-2 DEFERRED ISSUE: `MIN_TITLE_JACCARD = 0.75` is meant to reject
    a level difference ("Support Engineer" vs "Senior Support Engineer" scores
    0.67 and correctly does NOT match -- see
    `test_title_symmetry_rejects_a_level_difference` in test_source_resolver.py).
    But on a longer, THREE-token base title, adding a single seniority word only
    grows the token union by one against a three-token intersection, landing
    EXACTLY on the floor: "Technical Support Engineer" (3 tokens) vs "Staff
    Technical Support Engineer" (4 tokens) intersects on all 3 base tokens over a
    4-token union: 3/4 = 0.75, `>= MIN_TITLE_JACCARD` -- so it MATCHES, and a
    Staff-level requisition merges into an IC subject's identity.

    This is the same class of bug the 2-token case correctly rejects; the floor
    was tuned against a 2-token example and does not generalize to 3-token
    titles. Resolving it (a length-aware floor, or a level-token diff check
    independent of Jaccard) is a threshold decision for a human, not something a
    test should silently paper over -- so this test pins the CURRENT match and
    will need to be updated (not silenced) the day that decision is made.
    """
    assert resolver.title_jaccard(
        "Technical Support Engineer", "Staff Technical Support Engineer"
    ) == pytest.approx(0.75)
    assert resolver.title_jaccard(
        "Technical Support Engineer", "Staff Technical Support Engineer"
    ) >= resolver.MIN_TITLE_JACCARD


def test_local_resolution_merges_across_the_jaccard_floor_seniority_gap(conn):
    """The same characterization, through `resolve_local`'s public surface: an
    aggregator's plain "Technical Support Engineer" mirror resolves onto a
    board's "Staff Technical Support Engineer" posting -- a real level
    difference -- because the title floor treats them as the same requisition.
    """
    run(conn)
    posting(conn, "board-staff")
    posting(conn, "agg-1")

    report = resolver.resolve_aggregators(
        conn, run_uid="run-1",
        subjects=[("agg-1", "Acme Robotics", "Technical Support Engineer", "San Francisco, CA")],
        index=_index(direct("board-staff", title="Staff Technical Support Engineer")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT),
        at=AT,
    )
    assert report["matched"] == (("agg-1", "board-staff"),), (
        "pinning CURRENT behavior -- see this test's module docstring for the "
        "tracked wave-2 threshold issue this exposes"
    )


# --------------------------------------------------------------------------- #
# (d) locations_compatible degenerates on sub-3-char-token-only locations
# --------------------------------------------------------------------------- #
def test_locations_compatible_degenerates_when_every_token_is_too_short():
    """CHARACTERIZATION -- pins CURRENT behavior, not intended behavior.

    TRACKED WAVE-2 DEFERRED ISSUE: `MIN_LOCATION_TOKEN = 3` exists so a bare
    two-letter state code ("CA") cannot single-handedly make every California
    posting location-compatible with every other. But the filter is UNCONDITIONAL:
    when a location string's tokens are ALL shorter than 3 characters, EVERY
    token is dropped and `location_tokens` returns the empty set -- which
    `locations_compatible` treats identically to "no location given" (permissive
    by design, since one side is frequently blank). "NY" and "LA" are each a
    single 2-character token; both degenerate to the empty set, and two
    DEMONSTRABLY DIFFERENT abbreviated locations therefore read as compatible.

    The floor conflates two different situations -- "this location string
    carries no useful token" and "this location string carries only SHORT
    tokens" -- and only the first one is the permissive case the design comment
    intends. Fixing it (falling back to the raw short token when nothing else
    survives the filter, say) is a resolver-precision decision for a human, so
    this test pins the CURRENT permissive answer.
    """
    assert resolver.location_tokens("NY") == frozenset()
    assert resolver.location_tokens("LA") == frozenset()
    assert resolver.locations_compatible("NY", "LA"), (
        "pinning CURRENT behavior -- see this test's module docstring for the "
        "tracked wave-2 threshold issue this exposes"
    )


def test_local_resolution_merges_across_conflicting_short_token_locations(conn):
    """The same characterization, through `resolve_local`'s public surface: an
    aggregator subject located "NY" and a direct posting located "LA" pass the
    location leg of the conjunction purely because both degenerate to the empty
    token set -- not because anything about them is actually compatible.
    """
    run(conn)
    posting(conn, "board-la")
    posting(conn, "agg-1")

    report = resolver.resolve_aggregators(
        conn, run_uid="run-1",
        subjects=[("agg-1", "Acme Robotics", "Support Engineer", "NY")],
        index=_index(direct("board-la", location="LA")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT),
        at=AT,
    )
    assert report["matched"] == (("agg-1", "board-la"),), (
        "pinning CURRENT behavior -- see this test's module docstring for the "
        "tracked wave-2 threshold issue this exposes"
    )


# --------------------------------------------------------------------------- #
# (e) A redirect merge is one hop: A -> B -> C under-merges
# --------------------------------------------------------------------------- #
def test_a_redirect_chain_is_not_walked_and_under_merges(conn):
    """CHARACTERIZATION -- pins CURRENT behavior, not intended behavior.

    TRACKED WAVE-2 DEFERRED ISSUE: `record_redirect` writes a single literal
    edge, `from_posting_id -> to_posting_id`, and nothing in this module ever
    walks a chain of them. When posting A resolves into B today (aggregator ->
    direct local resolution) and B is LATER found to be the same posting as C
    (a normalization-bridge canonical match, say B's legacy-url and C's url
    alias collide, and C wins survivor precedence), the result is TWO one-hop
    edges -- `A -> B` and `B -> C` -- not the transitive `A -> C` a "these are
    all the same posting" model would produce. A consumer that resolves A with a
    single `posting_redirects` lookup (exactly what `graph._redirected_away`
    and `graph._state_maps` do -- both look up ONE level, by primary key or by
    direct incoming edge) still lands on B, a posting that itself no longer
    speaks for anything. A's observations therefore never reach C's canonical
    version selection: this posting is UNDER-MERGED.

    Walking the chain (or rewriting A's edge when B redirects further) is a
    correctness decision that touches every redirect consumer, not something to
    silently fix inside one test -- so this test pins the CURRENT two-edge,
    no-transitive-closure result.
    """
    run(conn, "run-1")
    run(conn, "run-2")
    posting(conn, "posting-a")
    posting(conn, "posting-b", first_seen="2025-01-01T00:00:00+00:00")
    posting(conn, "posting-c", first_seen="2026-08-01T00:00:00+00:00")

    # Hop 1: A (aggregator) resolves locally onto B (a direct board posting).
    first = resolver.resolve_aggregators(
        conn, run_uid="run-1",
        subjects=[("posting-a", "Acme Robotics", "Support Engineer", "San Francisco, CA")],
        index=_index(direct("posting-b")),
        category_of=category_map(greenhouse__acme=SourceCategory.DIRECT),
        at=AT,
    )
    assert first["matched"] == (("posting-a", "posting-b"),)

    # Hop 2: B is LATER found to be the same posting as C, via the legacy-url
    # bridge. C wins survivor precedence (a rank-0 source_req alias), so the
    # merge direction is B -> C -- a SECOND hop starting where the first ended.
    normalized = "https://boards.example.com/acme/jobs/99"
    alias(conn, "posting-b", kind="url", namespace="legacy-url", value=normalized)
    alias(conn, "posting-c", kind="url", namespace="url", value=normalized, confidence=0.5)
    alias(conn, "posting-c", kind="source_req", namespace="greenhouse:acme", value="99")
    resolver.bridge_legacy_url_aliases(conn, run_uid="run-2", at=AT)

    edges = redirects(conn)
    assert edges == {
        ("posting-a", "posting-b"): "aggregator-local-resolution",
        ("posting-b", "posting-c"): "normalization-bridge",
    }, "pinning CURRENT behavior -- two one-hop edges, not a transitive A -> C"

    # The under-merge, stated as the fact that matters: a single-hop lookup of A
    # (what every consumer in this codebase actually does) resolves to B, not to
    # A's true final identity C.
    single_hop = conn.execute(
        "SELECT to_posting_id FROM posting_redirects WHERE from_posting_id=?",
        ("posting-a",),
    ).fetchone()["to_posting_id"]
    assert single_hop == "posting-b", (
        "pinning CURRENT behavior -- A's redirect still names B, not C, so A's "
        "observations never reach C's canonical version selection"
    )
