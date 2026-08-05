"""Task 5.1: the pure Today-queue core (`backend.ranking.build_queue`).

Covers the deterministic contract (permutation invariance), the compare.ts-
mirrored product semantics (eligibility, bank/aim interleave), and everything
5.1 adds: freshness buckets + stale exclusion, company diversification,
uncertainty flags + cap, and the every-job-accounted exclusion invariant.
Pure module -- no DB, no app, `today` always passed explicitly.
"""
import random
from dataclasses import asdict

from backend.models import JobLight, JobState, url_to_b64
from backend.ranking import (
    QueuePolicy,
    build_queue,
    freshness_of,
    parse_odds,
)

TODAY = "2026-03-15"


def make_state(status="New", hidden=False, snoozed_until=None) -> JobState:
    return JobState(
        status=status,
        notes="",
        starred=False,
        hidden=hidden,
        contact="",
        snoozed_until=snoozed_until,
        updated_at="2026-03-01T00:00:00",
    )


def make_job(
    url,
    *,
    tier=4,
    odds="Strong match / Lower bar",
    odds_score=90,
    company="Acme",
    title="Engineer",
    posted=None,
    first_seen="2026-03-10",
    has_desc=True,
    state=None,
    odds_why="fits the profile",
    why=None,
) -> JobLight:
    return JobLight(
        url=url,
        url_b64=url_to_b64(url),
        seen_key=url,
        tier=tier,
        odds=odds,
        odds_score=odds_score,
        odds_why=odds_why,
        is_new=False,
        title=title,
        company=company,
        location=None,
        salary=None,
        salary_min=None,
        salary_max=None,
        posted=posted,
        first_seen=first_seen,
        remote=False,
        source=None,
        also_seen_on=None,
        req_id=None,
        why=why,
        flags=None,
        desc_snippet=None,
        has_desc=has_desc,
        state=state,
    )


def serialize(result):
    """Comparable full snapshot: entries (url + rank + lane + evidence) and the
    complete exclusion accounting."""
    return (
        [(e.job.url, e.rank, e.lane, e.lane_rank, e.evidence) for e in result.entries],
        [asdict(x) for x in result.excluded],
        result.excluded_counts,
        result.considered,
    )


def accounted(result):
    return len(result.entries) + sum(result.excluded_counts.values())


# --- determinism --------------------------------------------------------------


def test_output_invariant_under_input_permutation():
    jobs = [
        make_job(f"https://x.example/{i}", tier=3 + i % 3,
                 odds=["Strong match / Lower bar", "Moderate match / Standard",
                       "Weak match / High competition", None][i % 4],
                 odds_score=[90, 70, 40, None][i % 4],
                 company=f"co{i % 5}",
                 posted=["2026-03-12", "2026-02-20", "2026-01-01", None][i % 4],
                 has_desc=(i % 3 != 0))
        for i in range(16)
    ]
    baseline = serialize(build_queue(jobs, cap=5, today=TODAY))
    rng = random.Random(0)
    for _ in range(5):
        shuffled = jobs[:]
        rng.shuffle(shuffled)
        assert serialize(build_queue(shuffled, cap=5, today=TODAY)) == baseline


# --- eligibility reasons (compare.ts isActionable/isQueueEligible parity) -----


def test_each_ineligibility_reason_counted_once():
    jobs = [
        make_job("https://x.example/applied", state=make_state(status="Applied")),
        make_job("https://x.example/hidden", state=make_state(hidden=True)),
        make_job("https://x.example/snoozed",
                 state=make_state(snoozed_until="2026-04-01")),
        make_job("https://x.example/lowtier", tier=2),
        make_job("https://x.example/ok"),
    ]
    result = build_queue(jobs, cap=10, today=TODAY)
    assert [e.job.url for e in result.entries] == ["https://x.example/ok"]
    assert result.excluded_counts == {
        "not-new": 1, "hidden": 1, "snoozed": 1, "tier-below-cutoff": 1,
    }
    assert accounted(result) == result.considered == 5
    # obvious cuts get counts only, never detailed rows
    assert result.excluded == []


def test_snooze_expiring_today_is_actionable():
    job = make_job("https://x.example/1", state=make_state(snoozed_until=TODAY))
    result = build_queue([job], cap=5, today=TODAY)
    assert len(result.entries) == 1


def test_status_new_with_state_row_is_actionable():
    job = make_job("https://x.example/1", state=make_state(status="New"))
    assert len(build_queue([job], cap=5, today=TODAY).entries) == 1


# --- lanes: bank-first interleave, compare.ts semantics -----------------------


def test_bank_aim_interleave_order_and_lanes():
    # bank order (competition first): winnable, standard, reach
    # aim order (tier first):         reach (t5), standard (t4); winnable is t3 -> bank only
    winnable = make_job("https://x.example/win", tier=3,
                        odds="Strong match / Lower bar", company="c1")
    standard = make_job("https://x.example/std", tier=4,
                        odds="Moderate match / Standard", company="c2")
    reach = make_job("https://x.example/reach", tier=5,
                     odds="Strong match / High competition", company="c3")
    result = build_queue([standard, reach, winnable], cap=10, today=TODAY)
    assert [(e.job.url, e.lane) for e in result.entries] == [
        ("https://x.example/win", "bank"),   # bank[0]
        ("https://x.example/reach", "aim"),  # aim[0]
        ("https://x.example/std", "bank"),   # bank[1]; aim[1]=std dedupes
    ]
    assert accounted(result) == 3


def test_competition_outranks_match_in_bank_lane():
    lower_bar = make_job("https://x.example/a", tier=3,
                         odds="Weak match / Lower bar", odds_score=40, company="c1")
    strong_high = make_job("https://x.example/b", tier=3,
                           odds="Strong match / High competition", odds_score=95,
                           company="c2")
    result = build_queue([strong_high, lower_bar], cap=2, today=TODAY)
    assert [e.job.url for e in result.entries] == [
        "https://x.example/a", "https://x.example/b",
    ]


def test_unparseable_legacy_odds_rank_last_and_flag_unscored():
    legacy = make_job("https://x.example/legacy", odds="Target", odds_score=80,
                      company="c1")
    banded = make_job("https://x.example/banded",
                      odds="Weak match / High competition", odds_score=10,
                      company="c2")
    result = build_queue([legacy, banded], cap=2, today=TODAY)
    assert [e.job.url for e in result.entries] == [
        "https://x.example/banded", "https://x.example/legacy",
    ]
    assert "unscored" in result.entries[1].evidence["uncertainty"]


# --- freshness ----------------------------------------------------------------


def test_stale_posting_excluded_with_detail():
    stale = make_job("https://x.example/old", posted="2026-01-01")  # 73d
    result = build_queue([stale], cap=5, today=TODAY)
    assert result.entries == []
    assert result.excluded_counts == {"stale-posting": 1}
    (row,) = result.excluded
    assert row.reason == "stale-posting"
    assert row.detail == "age:73d basis:posted"


def test_freshness_demotes_within_band_not_across():
    fresh = make_job("https://x.example/fresh", posted="2026-03-10", company="c1")
    aging = make_job("https://x.example/aging", posted="2026-02-01", company="c2")
    # same band: fresh first
    result = build_queue([aging, fresh], cap=2, today=TODAY)
    assert [e.job.url for e in result.entries] == [
        "https://x.example/fresh", "https://x.example/aging",
    ]
    # band still dominates: aging Strong/Lower beats fresh Moderate/Standard in bank
    aging_strong = make_job("https://x.example/as", posted="2026-02-01",
                            odds="Strong match / Lower bar", company="c3")
    fresh_moderate = make_job("https://x.example/fm", posted="2026-03-10",
                              odds="Moderate match / Standard", company="c4")
    result = build_queue([fresh_moderate, aging_strong], cap=2, today=TODAY)
    assert [e.job.url for e in result.entries] == [
        "https://x.example/as", "https://x.example/fm",
    ]


def test_unknown_age_ranks_with_aging_and_reports_honestly():
    unknown = make_job("https://x.example/unk", posted=None, first_seen=None,
                       company="c1")
    fresh = make_job("https://x.example/fresh", posted="2026-03-12", company="c2")
    result = build_queue([unknown, fresh], cap=2, today=TODAY)
    assert [e.job.url for e in result.entries] == [
        "https://x.example/fresh", "https://x.example/unk",
    ]
    assert result.entries[1].evidence["freshness"] == {
        "bucket": "unknown", "age_days": None, "basis": None,
    }


def test_posted_falls_back_to_first_seen():
    job = make_job("https://x.example/1", posted="last week",  # unparseable
                   first_seen="2026-03-13")
    policy = QueuePolicy()
    from datetime import date
    fresh = freshness_of(job, date.fromisoformat(TODAY), policy)
    assert (fresh.bucket, fresh.age_days, fresh.basis) == ("fresh", 2, "first_seen")


def test_future_posted_clamps_to_fresh():
    job = make_job("https://x.example/1", posted="2026-04-01")
    from datetime import date
    fresh = freshness_of(job, date.fromisoformat(TODAY), QueuePolicy())
    assert (fresh.bucket, fresh.age_days) == ("fresh", 0)


# --- diversification ----------------------------------------------------------


def test_company_cap_enforced_case_insensitively_with_detail():
    jobs = [
        make_job("https://x.example/1", company="Anthropic", odds_score=95),
        make_job("https://x.example/2", company="anthropic", odds_score=90),
        make_job("https://x.example/3", company="ANTHROPIC", odds_score=85),
        make_job("https://x.example/4", company="Other", odds_score=10),
    ]
    result = build_queue(jobs, cap=10, today=TODAY)
    companies = [e.job.company for e in result.entries]
    assert len([c for c in companies if c and c.lower() == "anthropic"]) == 2
    assert "https://x.example/4" in [e.job.url for e in result.entries]
    assert result.excluded_counts["company-cap"] == 1
    capped = [x for x in result.excluded if x.reason == "company-cap"]
    assert len(capped) == 1
    assert accounted(result) == 4


def test_missing_company_is_not_one_company():
    jobs = [
        make_job(f"https://x.example/{i}", company=None, odds_score=90 - i)
        for i in range(4)
    ]
    result = build_queue(jobs, cap=10, today=TODAY)
    assert len(result.entries) == 4  # no cap across unknown companies


# --- uncertainty --------------------------------------------------------------


def test_uncertainty_flags_on_entries():
    nodesc = make_job("https://x.example/nd", has_desc=False, company="c1")
    unscored = make_job("https://x.example/us", odds=None, odds_score=None,
                        company="c2")
    both = make_job("https://x.example/both", odds=None, odds_score=None,
                    has_desc=False, company="c3")
    result = build_queue([nodesc], cap=5, today=TODAY)
    assert result.entries[0].evidence["uncertainty"] == ["no-description"]
    result = build_queue([unscored], cap=5, today=TODAY)
    assert result.entries[0].evidence["uncertainty"] == ["unscored"]
    result = build_queue([both], cap=5, today=TODAY)
    assert result.entries[0].evidence["uncertainty"] == ["unscored", "no-description"]


def test_uncertain_cap_bounds_unscored_entries():
    # cap 10 -> uncertain_cap = 2
    uncertain = [
        make_job(f"https://x.example/u{i}", odds=None, odds_score=None,
                 company=f"cu{i}")
        for i in range(3)
    ]
    scored = [
        make_job(f"https://x.example/s{i}", odds_score=90 - i, company=f"cs{i}")
        for i in range(3)
    ]
    result = build_queue(uncertain + scored, cap=10, today=TODAY)
    flagged = [e for e in result.entries if e.evidence["uncertainty"]]
    assert len(flagged) == 2
    assert result.excluded_counts["uncertainty-cap"] == 1
    (row,) = [x for x in result.excluded if x.reason == "uncertainty-cap"]
    assert row.detail == "unscored"
    assert accounted(result) == 6


def test_certain_jobs_never_blocked_by_uncertainty_cap():
    jobs = [
        make_job(f"https://x.example/u{i}", odds=None, odds_score=None,
                 company=f"cu{i}")
        for i in range(5)
    ] + [make_job("https://x.example/sure", company="cs")]
    result = build_queue(jobs, cap=10, today=TODAY)
    assert "https://x.example/sure" in [e.job.url for e in result.entries]


# --- cap + full accounting ----------------------------------------------------


def test_beyond_cap_details_are_the_next_up_list():
    jobs = [
        make_job(f"https://x.example/{i}", odds_score=90 - i, company=f"c{i}")
        for i in range(8)
    ]
    result = build_queue(jobs, cap=3, today=TODAY)
    assert len(result.entries) == 3
    assert result.excluded_counts["beyond-cap"] == 5
    nxt = [x for x in result.excluded if x.reason == "beyond-cap"]
    assert len(nxt) == 3  # detailed rows bounded at cap
    assert [x.detail for x in nxt] == ["next up"] * 3
    # next-up rows are the jobs ranked immediately past the cut
    assert [x.url_b64 for x in nxt] == [
        url_to_b64(f"https://x.example/{i}") for i in (3, 4, 5)
    ]
    assert accounted(result) == 8


def test_cap_zero_still_accounts_everything():
    jobs = [make_job(f"https://x.example/{i}", company=f"c{i}") for i in range(3)]
    result = build_queue(jobs, cap=0, today=TODAY)
    assert result.entries == []
    assert result.excluded_counts == {"beyond-cap": 3}
    assert [x for x in result.excluded if x.reason == "beyond-cap"] == []
    assert accounted(result) == 3


def test_every_job_lands_in_exactly_one_bin_mixed_corpus():
    jobs = [
        make_job("https://x.example/applied", state=make_state(status="Applied")),
        make_job("https://x.example/stale", posted="2025-12-01", company="c1"),
        make_job("https://x.example/a", company="Dup", odds_score=95),
        make_job("https://x.example/b", company="Dup", odds_score=90),
        make_job("https://x.example/c", company="Dup", odds_score=85),
        make_job("https://x.example/d", company="c2", odds_score=80),
        make_job("https://x.example/e", company="c3", odds_score=75),
    ]
    result = build_queue(jobs, cap=3, today=TODAY)
    assert accounted(result) == result.considered == 7
    assert len(result.entries) == 3


# --- odds parsing -------------------------------------------------------------


def test_parse_odds_halves_and_fallbacks():
    assert parse_odds("Strong match / Lower bar") == ("Strong match", "Lower bar")
    assert parse_odds("Target") == (None, None)
    assert parse_odds(None) == (None, None)
    assert parse_odds("") == (None, None)
