"""Task 5.1: the Today queue as a deterministic, explainable top-N ranking.

This module is the PURE CORE of the server-side Today queue. It takes a list of
`JobLight` rows (however the caller assembled them -- legacy or canonical reads),
an explicit `today` date string, and a policy of constants, and returns the top-N
queue plus a complete accounting of everything it did NOT queue and why. It reads
no clock, opens no connection, and draws no randomness: for a given (jobs, cap,
policy, today) the output is byte-identical regardless of input order, because
every sort key ends in `url_b64` (a total order over any real input -- URLs are
the legacy primary key).

Product semantics are PRESERVED from the client-side composer this replaces
(`frontend/src/lib/compare.ts` `composeQueue`), not re-invented:

- Eligibility: tier >= min_tier and "actionable" (no state or status "New", not
  hidden, not snoozed past today) -- the same rules as `isQueueEligible` /
  `isActionable`.
- Two lanes, interleaved bank-first and deduped:
  - bank ("bank a win"): competition rank ascending FIRST (Lower bar most
    winnable), then match quality, mirroring `bankCmp` -- this lane's point is
    "clear the lowest bar", so competition outranks match by design.
  - aim ("aim high"): tier 4/5 only, tier descending then combined odds rank,
    mirroring `aimCmp`.

What 5.1 ADDS on top of those semantics:

- Freshness: posting age (from `posted`, falling back to `first_seen`) buckets
  into fresh / aging / stale. Stale postings are EXCLUDED (reason
  `stale-posting`) -- applying into a listing that has been up for weeks is the
  ghost-application failure mode the queue exists to avoid. Within a lane,
  freshness demotes AFTER the band ranks and BEFORE odds_score: a fresh posting
  beats an aging one in the same band, but an aging Strong match still beats a
  fresh Moderate. Unknown age (no parseable date at all) ranks WITH aging --
  penalized past fresh, but not invented into either extreme -- and is reported
  honestly as bucket "unknown" in the evidence.
- Diversification: at most `company_cap` queue slots per company (case-folded;
  jobs with no company are each their own key -- a missing name is not a
  company). Overflow is recorded as `company-cap`, so ten Anthropic postings
  cannot crowd out the rest of the day.
- Uncertainty: a job whose odds are missing/unparseable or whose score is null
  is flagged `unscored`; one with no fetched description is flagged
  `no-description` (rule zero caps its tier, so its very tier is uncertain).
  Flagged jobs stay rankable -- hiding them would silently starve probes -- but
  hold at most `uncertain_cap` slots (default cap//5, min 1), overflow recorded
  as `uncertainty-cap`.
- Exclusion accounting: EVERY input job is either queued or counted under
  exactly one reason (the first failing check, checked in a fixed order), so
  `len(entries) + sum(excluded_counts.values()) == considered` always holds.
  Detailed rows (url/title/company/reason) are kept only for the SURPRISING
  cuts -- stale-posting, company-cap, uncertainty-cap, and the first cap-worth
  of beyond-cap (the "next up" list) -- never for the obvious ones (a job you
  already applied to is not a surprise), keeping the payload bounded on a ~34k
  corpus.

The impure boundary (clock, DB, flag dispatch) lives in
`routers/queueapi.py`; keeping it out of this module is the same split
`graph.decide_mode` and `enrichment.prefilter_posting` use.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from .models import JobLight

__all__ = [
    "QueuePolicy",
    "QueueEntry",
    "Exclusion",
    "QueueResult",
    "build_queue",
    "parse_odds",
    "freshness_of",
]

# --- odds bands (mirrors frontend/src/lib/format.ts; keep in sync by test) ----

#: Match-quality rank: lower is better. Unknown/legacy values rank after every
#: known band (see `_match_rank`), never throw.
MATCH_RANK = {
    "Strong match": 0,
    "Moderate match": 1,
    "Weak match": 2,
    "Level stretch": 3,
}
_MATCH_UNKNOWN = 5

#: Competition rank: lower is more winnable.
COMPETITION_RANK = {
    "Lower bar": 0,
    "Standard": 1,
    "High competition": 2,
}
_COMPETITION_UNKNOWN = 3

#: Combined odds rank for the aim lane when either half is unparseable -- after
#: every real match/competition combination (format.ts uses the same sentinel).
_ODDS_UNKNOWN = 1000


def parse_odds(odds: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Split "Strong match / Lower bar" into its halves.

    A legacy single-word value (Likely/Target/Reach -- a scorer older than the
    band change) has no " / " separator, so both halves come back None; callers
    treat that as unknown, never throw. Mirrors format.ts `parseOdds`.
    """
    if not odds:
        return None, None
    idx = odds.find(" / ")
    if idx == -1:
        return None, None
    return odds[:idx], odds[idx + 3 :]


def _match_rank(match: Optional[str]) -> int:
    if match is None:
        return _MATCH_UNKNOWN
    return MATCH_RANK.get(match, _MATCH_UNKNOWN)


def _competition_rank(competition: Optional[str]) -> int:
    if competition is None:
        return _COMPETITION_UNKNOWN
    return COMPETITION_RANK.get(competition, _COMPETITION_UNKNOWN)


def _odds_rank(match: Optional[str], competition: Optional[str]) -> int:
    if match is None or competition is None:
        return _ODDS_UNKNOWN
    return _match_rank(match) * 10 + _competition_rank(competition)


# --- freshness ----------------------------------------------------------------

#: Sort rank per bucket; "unknown" deliberately shares aging's rank (penalized
#: past fresh, not invented into stale).
_BUCKET_RANK = {"fresh": 0, "aging": 1, "unknown": 1, "stale": 2}


@dataclass(frozen=True)
class Freshness:
    bucket: str  # fresh | aging | unknown | stale
    age_days: Optional[int]
    basis: Optional[str]  # "posted" | "first_seen" | None


def _parse_date(value: Optional[str]) -> Optional[date]:
    """First 10 chars as an ISO date, else None. `posted` is free text from
    adapters/CSV, so anything unparseable simply doesn't count as a date."""
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def freshness_of(job: JobLight, today: date, policy: "QueuePolicy") -> Freshness:
    """Age from `posted`, falling back to `first_seen`; a future-dated posting
    clamps to age 0 (fresh) rather than going negative."""
    for basis in ("posted", "first_seen"):
        parsed = _parse_date(getattr(job, basis))
        if parsed is not None:
            age = max(0, (today - parsed).days)
            if age <= policy.fresh_days:
                bucket = "fresh"
            elif age <= policy.stale_days:
                bucket = "aging"
            else:
                bucket = "stale"
            return Freshness(bucket=bucket, age_days=age, basis=basis)
    return Freshness(bucket="unknown", age_days=None, basis=None)


# --- policy and result types --------------------------------------------------


@dataclass(frozen=True)
class QueuePolicy:
    """The queue's constants. Deliberately NOT read from config at import time --
    the caller passes one in, so tests and future per-user tuning need no
    monkeypatching."""

    min_tier: int = 3
    company_cap: int = 2
    fresh_days: int = 14
    stale_days: int = 45
    #: None derives max(1, cap // 5) at build time.
    uncertain_cap: Optional[int] = None

    def effective_uncertain_cap(self, cap: int) -> int:
        if self.uncertain_cap is not None:
            return self.uncertain_cap
        return max(1, cap // 5)


@dataclass(frozen=True)
class QueueEntry:
    job: JobLight
    rank: int  # 1-based position in the final queue
    lane: str  # "bank" | "aim"
    lane_rank: int  # 0-based position within the lane it was drawn from
    evidence: dict


@dataclass(frozen=True)
class Exclusion:
    url_b64: str
    title: Optional[str]
    company: Optional[str]
    reason: str
    detail: Optional[str]


@dataclass(frozen=True)
class QueueResult:
    entries: list[QueueEntry]
    #: Detailed rows for the surprising cuts only (stale-posting, company-cap,
    #: uncertainty-cap, first cap-worth of beyond-cap), in decision order.
    excluded: list[Exclusion]
    #: reason -> count over EVERY non-queued input job (sorted by reason for
    #: stable serialization). len(entries) + sum(values) == considered.
    excluded_counts: dict[str, int]
    considered: int


# --- eligibility --------------------------------------------------------------


def _ineligibility(job: JobLight, today_iso: str, policy: QueuePolicy) -> Optional[tuple[str, Optional[str]]]:
    """First failing eligibility check as (reason, detail), or None if eligible.

    Check order is fixed (it decides which single reason a job is counted
    under): pipeline state first -- a job you're already working is the least
    surprising absence -- then hidden/snoozed, then tier, with freshness
    handled separately by the caller (its exclusions get detailed rows).
    Mirrors format.ts `isActionable`: snoozed_until == today is NOT snoozed.
    """
    s = job.state
    if s is not None and s.status != "New":
        return "not-new", f"status:{s.status}"
    if s is not None and s.hidden:
        return "hidden", None
    if s is not None and s.snoozed_until and s.snoozed_until > today_iso:
        return "snoozed", f"until:{s.snoozed_until}"
    if job.tier < policy.min_tier:
        return "tier-below-cutoff", f"tier:{job.tier}"
    return None


def _uncertainty_flags(job: JobLight) -> list[str]:
    flags = []
    match, competition = parse_odds(job.odds)
    if match is None or competition is None or job.odds_score is None:
        flags.append("unscored")
    if not job.has_desc:
        flags.append("no-description")
    return flags


def _company_key(job: JobLight) -> str:
    """Case-folded company for the diversity cap; a job with no company name is
    its own key (url_b64) -- "unknown company" is not one company."""
    name = (job.company or "").strip().casefold()
    return name if name else f"\x00url:{job.url_b64}"


def _excluded_row(job: JobLight, reason: str, detail: Optional[str]) -> Exclusion:
    return Exclusion(
        url_b64=job.url_b64,
        title=job.title,
        company=job.company,
        reason=reason,
        detail=detail,
    )


# --- the queue ----------------------------------------------------------------


def build_queue(
    jobs: list[JobLight],
    *,
    cap: int,
    today: str,
    policy: QueuePolicy = QueuePolicy(),
) -> QueueResult:
    """Compose the deterministic top-`cap` Today queue over `jobs`.

    `today` is an ISO date string (the impure caller computes it once); output
    is invariant under any permutation of `jobs`.
    """
    today_date = date.fromisoformat(today)
    uncertain_cap = policy.effective_uncertain_cap(cap)

    counts: dict[str, int] = {}
    detailed: list[Exclusion] = []

    def count(reason: str) -> None:
        counts[reason] = counts.get(reason, 0) + 1

    # -- eligibility + freshness gate (each job lands in exactly one bin) ------
    eligible: list[tuple[JobLight, Freshness]] = []
    for job in sorted(jobs, key=lambda j: j.url_b64):  # stable accounting order
        failure = _ineligibility(job, today, policy)
        if failure is not None:
            reason, _detail = failure
            count(reason)
            continue
        fresh = freshness_of(job, today_date, policy)
        if fresh.bucket == "stale":
            count("stale-posting")
            detailed.append(
                _excluded_row(job, "stale-posting", f"age:{fresh.age_days}d basis:{fresh.basis}")
            )
            continue
        eligible.append((job, fresh))

    # -- lanes (product semantics from compare.ts, freshness added) ------------
    def score_keys(job: JobLight) -> tuple[int, int]:
        # (missing, -score): null odds_score sorts after any real score.
        if job.odds_score is None:
            return (1, 0)
        return (0, -job.odds_score)

    def bank_key(item: tuple[JobLight, Freshness]):
        job, fresh = item
        match, competition = parse_odds(job.odds)
        return (
            _competition_rank(competition),
            _match_rank(match),
            _BUCKET_RANK[fresh.bucket],
            *score_keys(job),
            -job.tier,
            job.url_b64,
        )

    def aim_key(item: tuple[JobLight, Freshness]):
        job, fresh = item
        match, competition = parse_odds(job.odds)
        return (
            -job.tier,
            _odds_rank(match, competition),
            _BUCKET_RANK[fresh.bucket],
            *score_keys(job),
            job.url_b64,
        )

    bank = sorted(eligible, key=bank_key)
    aim = sorted((item for item in eligible if item[0].tier >= 4), key=aim_key)

    # -- interleave bank-first with dedupe + caps ------------------------------
    entries: list[QueueEntry] = []
    decided: set[str] = set()  # url_b64 of every queued OR cap-excluded job
    company_counts: dict[str, int] = {}
    uncertain_used = 0

    def try_push(item: tuple[JobLight, Freshness], lane: str, lane_rank: int) -> None:
        nonlocal uncertain_used
        job, fresh = item
        if job.url_b64 in decided:
            return  # already queued via the other lane, or already cap-excluded
        ckey = _company_key(job)
        if company_counts.get(ckey, 0) >= policy.company_cap:
            decided.add(job.url_b64)
            count("company-cap")
            detailed.append(
                _excluded_row(job, "company-cap", f"company already holds {policy.company_cap} slots")
            )
            return
        flags = _uncertainty_flags(job)
        if flags and uncertain_used >= uncertain_cap:
            decided.add(job.url_b64)
            count("uncertainty-cap")
            detailed.append(
                _excluded_row(job, "uncertainty-cap", "+".join(flags))
            )
            return
        decided.add(job.url_b64)
        company_counts[ckey] = company_counts.get(ckey, 0) + 1
        if flags:
            uncertain_used += 1
        match, competition = parse_odds(job.odds)
        entries.append(
            QueueEntry(
                job=job,
                rank=len(entries) + 1,
                lane=lane,
                lane_rank=lane_rank,
                evidence={
                    "lane": lane,
                    "lane_rank": lane_rank,
                    "match_band": match,
                    "competition_band": competition,
                    "odds_score": job.odds_score,
                    "tier": job.tier,
                    "freshness": {
                        "bucket": fresh.bucket,
                        "age_days": fresh.age_days,
                        "basis": fresh.basis,
                    },
                    "uncertainty": flags,
                    "why": job.odds_why or job.why,
                },
            )
        )

    for i in range(max(len(bank), len(aim))):
        if len(entries) >= cap:
            break
        if i < len(bank):
            try_push(bank[i], "bank", i)
        if len(entries) >= cap:
            break
        if i < len(aim):
            try_push(aim[i], "aim", i)

    # -- beyond-cap accounting (bank order covers ALL eligible jobs) -----------
    overflow_detailed = 0
    for item in bank:
        job, _fresh = item
        if job.url_b64 in decided:
            continue
        count("beyond-cap")
        if overflow_detailed < cap:  # the "next up" list, bounded
            detailed.append(_excluded_row(job, "beyond-cap", "next up"))
            overflow_detailed += 1

    return QueueResult(
        entries=entries,
        excluded=detailed,
        excluded_counts=dict(sorted(counts.items())),
        considered=len(jobs),
    )
