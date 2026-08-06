"""Task 5.5a: `ranking_metrics.py` -- ranking-quality metrics over the served
"today" queue.

Written FIRST per the dispatch instruction ("author the shape tests first for
GET /api/ranking/metrics ... binding on the frontend implementer running in
parallel") -- these module-level tests pin the metric MATH; the router-level
shape pin for GET /api/ranking/metrics lives in test_queue_api.py (that file
owns HTTP wiring for everything in routers/queueapi.py).

Every database is built under tmp_path via `test_source_scheduler_fakes.
make_connect` (fresh -> full canonical schema through `db.init_db`), mirroring
test_outcome_analytics.py / test_calibration.py. Nothing here touches
webapp/app.db (repo-root conftest.py fences JOBHUNT_DB).

"today" surface snapshots are built with `outcomes.capture_snapshot` itself
(the already-tested W-5.2 writer), never hand-rolled rows -- so these tests
exercise the SAME denormalization a real snapshot-on-serve capture produces.
`state_events` / `outcome_events` rows (no writer module owned by this task)
are built directly with SQL, mirroring test_outcome_analytics.py's local
insert helpers.
"""
import json
import uuid

import pytest

from backend import outcomes
from backend.ranking_metrics import ranking_metrics
from backend.tests.test_source_scheduler_fakes import make_connect

AT = "2026-08-01T09:00:00"
#: A reference moment far enough after every fixture timestamp below that
#: "on/after first serve" / maturity-window math has real headroom to test
#: both sides of, without every date literal needing hand-tuning against it.
NOW = "2026-09-01T00:00:00"


@pytest.fixture
def conn(tmp_path):
    c = make_connect(tmp_path)()
    try:
        yield c
    finally:
        c.close()


# --------------------------------------------------------------------------- #
# insert helpers (mirrors test_outcome_analytics.py's local helpers)
# --------------------------------------------------------------------------- #
def insert_posting(conn, posting_id, at=AT):
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, at, at),
    )


def insert_version(conn, posting_id, *, observed_at=AT, source="greenhouse:acme",
                    posted=None, first_seen=None):
    version_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, "
        "version_hash, observed_at, title, company, source, posted, first_seen, tier, "
        "odds, odds_score, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (version_id, posting_id, "source", version_id, observed_at, "Support Engineer",
         "Acme", source, posted, first_seen, 4, "Strong match / Lower bar", 90, "{}"),
    )
    return version_id


def insert_status_event(conn, seen_key, old_value, new_value, at, *, posting_id=None,
                         url=None, source="patch"):
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source, "
        "posting_id) VALUES (?,?, 'status', ?,?,?,?,?)",
        (seen_key, url, old_value, new_value, at, source, posting_id),
    )


def insert_opened_event(conn, *, posting_id, at=AT):
    conn.execute(
        "INSERT INTO outcome_events (outcome_event_id, kind, at, posting_id) "
        "VALUES (?, 'opened', ?, ?)",
        (str(uuid.uuid4()), at, posting_id),
    )


def serve_today(conn, items, *, at=AT, queue_size=None):
    """One surface='today' snapshot via the real writer (`outcomes.
    capture_snapshot`). `items`: [(posting_id, rank), ...].

    `queue_size` defaults to len(items) but may be given LARGER, which is the
    real partial-view case `capture_snapshot` documents (the queue showed N
    entries; only some resolved to a posting_id worth recording)."""
    return outcomes.capture_snapshot(
        conn, surface="today",
        items=[{"posting_id": pid, "rank": rank} for pid, rank in items],
        at=at, queue_size=len(items) if queue_size is None else queue_size,
    )


def apply_to(conn, posting_id, at, *, seen_key=None, responded_at=None, response_stage="Phone screen"):
    """One applied identity resolving to `posting_id` (explicit
    state_events.posting_id, no job_state row needed -- `_seen_key_posting_
    ids` honors an explicit per-row posting_id directly)."""
    seen_key = seen_key or f"sk-{posting_id}"
    insert_status_event(conn, seen_key, "New", "Applied", at, posting_id=posting_id)
    if responded_at is not None:
        insert_status_event(
            conn, seen_key, "Applied", response_stage, responded_at, posting_id=posting_id,
        )
    return seen_key


def full_posting(conn, posting_id, *, source="greenhouse:acme", posted=None, first_seen=None):
    insert_posting(conn, posting_id)
    insert_version(conn, posting_id, source=source, posted=posted, first_seen=first_seen)


# --------------------------------------------------------------------------- #
# empty DB
# --------------------------------------------------------------------------- #
def test_empty_db_zeroed_shape(conn):
    conn.commit()
    report = ranking_metrics(conn, now=NOW)
    assert report["generated_at"] == NOW
    assert report["min_sample"] == 5
    assert report["ghost_days"] == 21
    assert report["top10_application_rate"] == {
        "n_served_top10": 0, "n_applied": 0, "rate": None, "low_sample": True,
    }
    assert report["time_to_application"] == {
        "n_served": 0, "n_applied": 0, "median_days": None, "low_sample": True,
    }
    assert report["response_rate"] == {
        "n_applied": 0, "n_responded": 0, "rate": None, "low_sample": True,
    }
    assert report["stale_rate"] == {
        "n_served": 0, "n_stale_never_engaged": 0, "rate": None, "low_sample": True,
    }
    assert report["ghost_rate"] == {
        "n_applied_total": 0, "n_applied_eligible": 0, "n_ghosted": 0, "rate": None,
        "low_sample": True, "ghost_days": 21,
    }
    assert report["queue_completion"] == {
        "by_day": [], "n_days": 0, "median_rate": None, "low_sample": True,
    }
    assert report["source_yield"] == {"by_source": [], "by_source_category": []}
    json.dumps(report)  # must be serializable


def test_determinism_two_runs_identical(conn):
    full_posting(conn, "p1")
    serve_today(conn, [("p1", 1)])
    apply_to(conn, "p1", "2026-08-02T10:00:00")
    conn.commit()
    r1 = ranking_metrics(conn, now=NOW)
    r2 = ranking_metrics(conn, now=NOW)
    assert r1 == r2
    assert json.dumps(r1, sort_keys=True) == json.dumps(r2, sort_keys=True)


# --------------------------------------------------------------------------- #
# top10_application_rate
# --------------------------------------------------------------------------- #
def test_top10_application_rate_math(conn):
    for pid in ("p1", "p2", "p3"):
        full_posting(conn, pid)
    serve_today(conn, [("p1", 1), ("p2", 2), ("p3", 3)], at=AT)
    apply_to(conn, "p1", "2026-08-02T10:00:00")  # after serve
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["top10_application_rate"]
    assert cell["n_served_top10"] == 3
    assert cell["n_applied"] == 1
    assert cell["rate"] == pytest.approx(1 / 3)


def test_top10_application_rate_excludes_beyond_rank_10(conn):
    for pid in ("p1", "p11"):
        full_posting(conn, pid)
    serve_today(conn, [("p1", 1), ("p11", 11)], at=AT)
    apply_to(conn, "p1", "2026-08-02T10:00:00")
    apply_to(conn, "p11", "2026-08-02T10:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["top10_application_rate"]
    # only p1 (rank<=10) counts toward the population at all
    assert cell["n_served_top10"] == 1
    assert cell["n_applied"] == 1
    assert cell["rate"] == 1.0


def test_top10_application_rate_apply_before_serve_does_not_count(conn):
    full_posting(conn, "p1")
    apply_to(conn, "p1", "2026-07-01T10:00:00")  # before the serve
    serve_today(conn, [("p1", 1)], at=AT)
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["top10_application_rate"]
    assert cell["n_served_top10"] == 1
    assert cell["n_applied"] == 0
    assert cell["rate"] == 0.0


def test_top10_application_rate_exact_timestamp_tie_counts(conn):
    """H1-style tie rule reused from outcome_analytics: an apply at exactly
    the serve's own timestamp counts as caused by it, not excluded."""
    full_posting(conn, "p1")
    serve_today(conn, [("p1", 1)], at=AT)
    apply_to(conn, "p1", AT)
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["top10_application_rate"]
    assert cell["n_applied"] == 1


# --------------------------------------------------------------------------- #
# time_to_application
# --------------------------------------------------------------------------- #
def test_time_to_application_median(conn):
    for pid in ("p1", "p2", "p3"):
        full_posting(conn, pid)
    serve_today(conn, [("p1", 1), ("p2", 2), ("p3", 3)], at="2026-08-01T00:00:00")
    apply_to(conn, "p1", "2026-08-03T00:00:00")  # 2 days
    apply_to(conn, "p2", "2026-08-05T00:00:00")  # 4 days
    # p3 never applied -- adds to n_served, not n_applied
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["time_to_application"]
    assert cell["n_served"] == 3
    assert cell["n_applied"] == 2
    assert cell["median_days"] == pytest.approx(3.0)


def test_time_to_application_beyond_rank_10_still_counted(conn):
    """Unlike top10_application_rate, this family is over ALL served ranks."""
    full_posting(conn, "p11")
    serve_today(conn, [("p11", 11)], at="2026-08-01T00:00:00")
    apply_to(conn, "p11", "2026-08-02T00:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["time_to_application"]
    assert cell["n_served"] == 1
    assert cell["n_applied"] == 1
    assert cell["median_days"] == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# response_rate (population-wide -- recorded interpretation, see module docstring)
# --------------------------------------------------------------------------- #
def test_response_rate_math(conn):
    full_posting(conn, "p1")
    full_posting(conn, "p2")
    apply_to(conn, "p1", "2026-08-01T00:00:00", responded_at="2026-08-05T00:00:00")
    apply_to(conn, "p2", "2026-08-01T00:00:00")  # no response
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["response_rate"]
    assert cell["n_applied"] == 2
    assert cell["n_responded"] == 1
    assert cell["rate"] == 0.5


def test_response_rate_counts_applications_never_served_today(conn):
    """Recorded interpretation: response_rate is population-wide, NOT scoped
    to postings served via a "today" snapshot -- an application to a posting
    this module never saw served still counts."""
    full_posting(conn, "p_unserved")
    apply_to(conn, "p_unserved", "2026-08-01T00:00:00", responded_at="2026-08-03T00:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["response_rate"]
    assert cell["n_applied"] == 1
    assert cell["n_responded"] == 1


def test_response_rate_passed_is_not_a_response(conn):
    """`_RESPONSE_STAGES` reused from outcome_analytics excludes Passed --
    the applicant giving up is not the company responding."""
    full_posting(conn, "p1")
    apply_to(conn, "p1", "2026-08-01T00:00:00", responded_at="2026-08-05T00:00:00",
             response_stage="Passed")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["response_rate"]
    assert cell["n_applied"] == 1
    assert cell["n_responded"] == 0


# --------------------------------------------------------------------------- #
# stale_rate
# --------------------------------------------------------------------------- #
def test_stale_rate_never_engaged_and_aged_past_policy_counts(conn):
    # posted 100 days before NOW -- QueuePolicy().stale_days == 45
    full_posting(conn, "p_stale", posted="2026-05-24")
    full_posting(conn, "p_fresh", posted="2026-08-25")  # ~7 days before NOW
    serve_today(conn, [("p_stale", 1), ("p_fresh", 2)], at=AT)
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["stale_rate"]
    assert cell["n_served"] == 2
    assert cell["n_stale_never_engaged"] == 1
    assert cell["rate"] == 0.5


def test_stale_rate_excludes_opened_postings(conn):
    full_posting(conn, "p_stale_opened", posted="2026-05-01")
    serve_today(conn, [("p_stale_opened", 1)], at=AT)
    insert_opened_event(conn, posting_id="p_stale_opened", at="2026-08-02T00:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["stale_rate"]
    assert cell["n_served"] == 1
    assert cell["n_stale_never_engaged"] == 0  # opened -> not "never engaged"


def test_stale_rate_excludes_applied_postings(conn):
    full_posting(conn, "p_stale_applied", posted="2026-05-01")
    serve_today(conn, [("p_stale_applied", 1)], at=AT)
    apply_to(conn, "p_stale_applied", "2026-08-02T00:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["stale_rate"]
    assert cell["n_stale_never_engaged"] == 0


def test_stale_rate_ages_against_the_injected_now_not_the_clock(conn):
    """B7 (mutation survivor M2): `stale_rate` ages postings against the
    INJECTED `now`, never `date.today()`. One posting, two reference moments,
    two different answers -- so a version of `_stale_rate` that read the wall
    clock (or ignored `now` entirely) cannot satisfy both assertions no
    matter what today's real date happens to be."""
    full_posting(conn, "p1", posted="2026-05-24")  # QueuePolicy().stale_days == 45
    serve_today(conn, [("p1", 1)], at=AT)
    conn.commit()

    # 2026-09-01 - 2026-05-24 == 99 days -> stale
    late = ranking_metrics(conn, now="2026-09-01T00:00:00")["stale_rate"]
    assert late["n_stale_never_engaged"] == 1
    assert late["rate"] == 1.0

    # 2026-06-01 - 2026-05-24 == 8 days -> not stale at that moment
    early = ranking_metrics(conn, now="2026-06-01T00:00:00")["stale_rate"]
    assert early["n_served"] == 1
    assert early["n_stale_never_engaged"] == 0
    assert early["rate"] == 0.0


def test_stale_rate_first_seen_fallback_when_posted_missing(conn):
    """Mirrors ranking.freshness_of's own basis fallback."""
    full_posting(conn, "p_old_first_seen", posted=None, first_seen="2026-05-01")
    serve_today(conn, [("p_old_first_seen", 1)], at=AT)
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["stale_rate"]
    assert cell["n_stale_never_engaged"] == 1


# --------------------------------------------------------------------------- #
# ghost_rate
# --------------------------------------------------------------------------- #
def test_ghost_rate_maturity_and_ghosting(conn):
    full_posting(conn, "p_ghost")     # applied 31 days before NOW, no response
    full_posting(conn, "p_responded")  # applied 31 days before NOW, WITH response
    full_posting(conn, "p_too_recent")  # applied 5 days before NOW -- immature
    apply_to(conn, "p_ghost", "2026-08-01T00:00:00")
    apply_to(conn, "p_responded", "2026-08-01T00:00:00", responded_at="2026-08-05T00:00:00")
    apply_to(conn, "p_too_recent", "2026-08-27T00:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW, ghost_days=21)["ghost_rate"]
    assert cell["n_applied_total"] == 3
    assert cell["n_applied_eligible"] == 2  # p_ghost + p_responded (>= 21d old)
    assert cell["n_ghosted"] == 1
    assert cell["rate"] == 0.5
    assert cell["ghost_days"] == 21


def test_ghost_rate_query_overridable_window(conn):
    full_posting(conn, "p1")
    apply_to(conn, "p1", "2026-08-01T00:00:00")  # 31 days before NOW
    conn.commit()

    # default 21d: eligible + ghosted
    default_cell = ranking_metrics(conn, now=NOW)["ghost_rate"]
    assert default_cell["n_applied_eligible"] == 1
    assert default_cell["n_ghosted"] == 1

    # 60d window: not old enough to judge yet
    wide_cell = ranking_metrics(conn, now=NOW, ghost_days=60)["ghost_rate"]
    assert wide_cell["n_applied_eligible"] == 0
    assert wide_cell["n_ghosted"] == 0
    assert wide_cell["rate"] is None


def test_ghost_rate_exact_boundary_is_eligible(conn):
    """`>=` at the boundary, not strictly `>` -- exactly `ghost_days` old
    counts as mature."""
    full_posting(conn, "p1")
    apply_to(conn, "p1", "2026-08-11T00:00:00")  # exactly 21 days before NOW
    conn.commit()

    cell = ranking_metrics(conn, now=NOW, ghost_days=21)["ghost_rate"]
    assert cell["n_applied_eligible"] == 1


# --------------------------------------------------------------------------- #
# queue_completion
# --------------------------------------------------------------------------- #
def test_queue_completion_day_bucketing_and_median(conn):
    for pid in ("d1a", "d1b", "d2a"):
        full_posting(conn, pid)
    serve_today(conn, [("d1a", 1), ("d1b", 2)], at="2026-08-01T09:00:00")
    serve_today(conn, [("d2a", 1)], at="2026-08-02T09:00:00")
    # d1a opened SAME day as its serve
    insert_opened_event(conn, posting_id="d1a", at="2026-08-01T15:00:00")
    # d2a gets a state_event (not necessarily Applied) same day
    insert_status_event(conn, "sk-d2a", "New", "Snoozed", "2026-08-02T15:00:00",
                        posting_id="d2a")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["queue_completion"]
    by_day = {row["day"]: row for row in cell["by_day"]}
    assert by_day["2026-08-01"] == {
        "day": "2026-08-01", "queue_size": 2, "n_served": 2, "n_completed": 1,
        "rate": 0.5,
    }
    assert by_day["2026-08-02"] == {
        "day": "2026-08-02", "queue_size": 1, "n_served": 1, "n_completed": 1,
        "rate": 1.0,
    }
    assert cell["n_days"] == 2
    assert cell["median_rate"] == pytest.approx(0.75)


def test_queue_completion_denominator_is_queue_size_not_n_served(conn):
    """B7 (mutation survivor M1): the denominator is the day's QUEUE_SIZE --
    what the visitor was shown -- not `n_served`, the count of snapshot items
    that happened to resolve to a posting_id. Pinned with the two deliberately
    different (queue_size 4, two resolved items, one acted on): the correct
    rate is 1/4, and the n_served denominator would report 1/2."""
    for pid in ("q1", "q2"):
        full_posting(conn, pid)
    serve_today(conn, [("q1", 1), ("q2", 2)], at="2026-08-01T09:00:00", queue_size=4)
    insert_opened_event(conn, posting_id="q1", at="2026-08-01T15:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["queue_completion"]
    (row,) = cell["by_day"]
    assert row["queue_size"] == 4
    assert row["n_served"] == 2
    assert row["n_completed"] == 1
    assert row["rate"] == pytest.approx(0.25)
    assert cell["median_rate"] == pytest.approx(0.25)


def test_queue_completion_day_with_no_resolved_items_is_null_not_zero(conn):
    """B7: a day whose snapshot resolved NO items (queue shown, nothing
    identifiable) is unmeasured, not a 0% day -- `rate` is null, and it stays
    out of `median_rate` while still counting toward `n_days`."""
    full_posting(conn, "p1")
    serve_today(conn, [], at="2026-08-01T09:00:00", queue_size=3)
    serve_today(conn, [("p1", 1)], at="2026-08-02T09:00:00")
    insert_opened_event(conn, posting_id="p1", at="2026-08-02T15:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["queue_completion"]
    by_day = {row["day"]: row for row in cell["by_day"]}
    assert by_day["2026-08-01"]["queue_size"] == 3
    assert by_day["2026-08-01"]["n_served"] == 0
    assert by_day["2026-08-01"]["rate"] is None
    assert by_day["2026-08-02"]["rate"] == 1.0
    assert cell["n_days"] == 2               # the null-rate day is still a day
    assert cell["median_rate"] == 1.0        # ...but not a data point


def test_queue_completion_median_over_non_null_days_only(conn):
    """B7: three days, one unmeasured -- the median is over the two real
    rates (1.0 and 0.0 -> 0.5), never over a null coerced to zero."""
    for pid in ("m1", "m2"):
        full_posting(conn, pid)
    serve_today(conn, [], at="2026-08-01T09:00:00", queue_size=2)
    serve_today(conn, [("m1", 1)], at="2026-08-02T09:00:00")
    insert_opened_event(conn, posting_id="m1", at="2026-08-02T15:00:00")
    serve_today(conn, [("m2", 1)], at="2026-08-03T09:00:00")  # never acted on
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["queue_completion"]
    assert [row["rate"] for row in cell["by_day"]] == [None, 1.0, 0.0]
    assert cell["n_days"] == 3
    assert cell["median_rate"] == pytest.approx(0.5)


def test_queue_completion_action_on_a_different_day_not_counted(conn):
    full_posting(conn, "p1")
    serve_today(conn, [("p1", 1)], at="2026-08-01T09:00:00")
    # opened the NEXT day, not the serve day
    insert_opened_event(conn, posting_id="p1", at="2026-08-02T09:00:00")
    conn.commit()

    cell = ranking_metrics(conn, now=NOW)["queue_completion"]
    (row,) = cell["by_day"]
    assert row["n_completed"] == 0
    assert row["rate"] == 0.0


# --------------------------------------------------------------------------- #
# source_yield
# --------------------------------------------------------------------------- #
def test_source_yield_by_source_and_category_funnel(conn):
    full_posting(conn, "p1", source="greenhouse:acme")
    full_posting(conn, "p2", source="greenhouse:acme")
    serve_today(conn, [("p1", 1), ("p2", 2)], at="2026-08-01T00:00:00")
    insert_opened_event(conn, posting_id="p1", at="2026-08-02T00:00:00")
    apply_to(conn, "p1", "2026-08-03T00:00:00", responded_at="2026-08-05T00:00:00")
    conn.commit()

    yields = ranking_metrics(conn, now=NOW)["source_yield"]
    (cell,) = yields["by_source"]
    assert cell["key"] == "greenhouse:acme"
    assert cell["n_recommended"] == 2
    assert cell["n_opened"] == 1
    assert cell["n_applied"] == 1
    assert cell["n_responded"] == 1
    assert cell["open_rate"] == 0.5
    assert cell["application_rate"] == 0.5
    assert cell["response_rate"] == 1.0

    (cat_cell,) = yields["by_source_category"]
    assert cat_cell["n_recommended"] == 2


def test_source_yield_unknown_source_bucket_never_dropped(conn):
    # a posting with no source attribution at all -- capture_snapshot leaves
    # `source` NULL when the posting_versions row has none.
    insert_posting(conn, "p_no_source")
    version_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, "
        "version_hash, observed_at, payload_json) VALUES (?,?,?,?,?,?)",
        (version_id, "p_no_source", "source", version_id, AT, "{}"),
    )
    serve_today(conn, [("p_no_source", 1)], at=AT)
    conn.commit()

    yields = ranking_metrics(conn, now=NOW)["source_yield"]
    (cell,) = yields["by_source"]
    assert cell["key"] == "unknown"
    assert cell["n_recommended"] == 1


def test_source_yield_open_before_first_serve_not_attributed(conn):
    full_posting(conn, "p1")
    insert_opened_event(conn, posting_id="p1", at="2026-07-01T00:00:00")  # before serve
    serve_today(conn, [("p1", 1)], at=AT)
    conn.commit()

    (cell,) = ranking_metrics(conn, now=NOW)["source_yield"]["by_source"]
    assert cell["n_opened"] == 0


def test_source_yield_same_day_open_before_serve_not_attributed(conn):
    """B5: the open and the serve are on the SAME local day, the open two
    hours EARLIER. Attribution compares full timestamps (the 5.3 rule), so
    this open was not caused by the recommendation and must not count.

    Before the fix, open timestamps were truncated to 'YYYY-MM-DD' on the way
    in, which `_at_on_or_after` reads as a bare backfilled date and compares
    at DATE grain -- making this open indistinguishable from one at 23:59 and
    counting it. `queue_completion` is unaffected either way: that family is
    day-grain BY DEFINITION ("did the visitor act on the day's queue that
    day"), so it still counts this open, and the two answers below are the
    point of keeping the grains separate."""
    full_posting(conn, "p1")
    insert_opened_event(conn, posting_id="p1", at="2026-08-01T07:00:00")
    serve_today(conn, [("p1", 1)], at="2026-08-01T09:00:00")
    conn.commit()

    report = ranking_metrics(conn, now=NOW)
    (cell,) = report["source_yield"]["by_source"]
    assert cell["n_opened"] == 0
    (day_row,) = report["queue_completion"]["by_day"]
    assert day_row["n_completed"] == 1


def test_source_yield_same_day_open_after_serve_is_attributed(conn):
    """The other side of B5's boundary: same day, two hours LATER, counts."""
    full_posting(conn, "p1")
    serve_today(conn, [("p1", 1)], at="2026-08-01T09:00:00")
    insert_opened_event(conn, posting_id="p1", at="2026-08-01T11:00:00")
    conn.commit()

    (cell,) = ranking_metrics(conn, now=NOW)["source_yield"]["by_source"]
    assert cell["n_opened"] == 1


# --------------------------------------------------------------------------- #
# B6: the population-wide families share `outcome_analytics.
# _application_identities` -- one definition of applied/responded, and a
# denominator that really is population-wide.
# --------------------------------------------------------------------------- #
def test_population_families_count_identities_with_no_posting_id(conn):
    """B6: an applied identity that resolves to NO posting_id (state-tracked
    by seen_key only -- no explicit `state_events.posting_id`, no `job_state`
    bridge) is a real application. `response_rate` and `ghost_rate` count it;
    the posting-keyed families cannot see it and must not pretend to."""
    full_posting(conn, "p_served")
    serve_today(conn, [("p_served", 1)], at=AT)
    apply_to(conn, "p_served", "2026-08-02T00:00:00", responded_at="2026-08-04T00:00:00")
    # seen_key-only identity: no posting_id column, no job_state row.
    insert_status_event(conn, "sk-orphan", "New", "Applied", "2026-08-02T00:00:00")
    conn.commit()

    report = ranking_metrics(conn, now=NOW)
    assert report["response_rate"]["n_applied"] == 2   # both identities
    assert report["response_rate"]["n_responded"] == 1
    assert report["response_rate"]["rate"] == 0.5
    assert report["ghost_rate"]["n_applied_total"] == 2
    assert report["ghost_rate"]["n_applied_eligible"] == 2
    assert report["ghost_rate"]["n_ghosted"] == 1     # the orphan, never answered
    # posting-keyed families see only the resolvable one
    assert report["top10_application_rate"]["n_served_top10"] == 1
    assert report["top10_application_rate"]["n_applied"] == 1


def test_applied_facts_come_from_the_shared_identity_computation(conn):
    """B6: `ranking_metrics` no longer keeps its own copy of "what counts as
    applied"; it re-keys `outcome_analytics._application_identities` by
    posting. Pinned by patching that ONE function and watching every
    posting-keyed family follow it."""
    from backend import ranking_metrics as rm

    full_posting(conn, "p1")
    serve_today(conn, [("p1", 1)], at=AT)
    apply_to(conn, "p1", "2026-08-02T00:00:00")
    conn.commit()
    assert ranking_metrics(conn, now=NOW)["top10_application_rate"]["n_applied"] == 1

    original = rm._application_identities
    try:
        rm._application_identities = lambda conn, profile: []
        report = ranking_metrics(conn, now=NOW)
    finally:
        rm._application_identities = original
    assert report["top10_application_rate"]["n_applied"] == 0
    assert report["response_rate"]["n_applied"] == 0


# --------------------------------------------------------------------------- #
# min_sample
# --------------------------------------------------------------------------- #
def test_min_sample_flips_low_sample_flag(conn):
    full_posting(conn, "p1")
    serve_today(conn, [("p1", 1)], at=AT)
    apply_to(conn, "p1", "2026-08-02T00:00:00")
    conn.commit()

    assert ranking_metrics(conn, min_sample=1, now=NOW)["top10_application_rate"]["low_sample"] is False
    assert ranking_metrics(conn, min_sample=2, now=NOW)["top10_application_rate"]["low_sample"] is True
