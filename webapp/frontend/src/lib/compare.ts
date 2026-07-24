// The ONE comparator set. Kills reimplementations across Kanban/Companies/Matrix
// (tierOddsScoreCmp) and Today (bankCmp/aimCmp, queue composition).
import type { JobLight } from "../api/types";
import { isActionable, oddsRank } from "./format";

// odds_score may be null; push nulls to the bottom of a desc sort.
function score(j: JobLight): number {
  return j.odds_score ?? Number.NEGATIVE_INFINITY;
}

/** Default board/list ordering: tier desc, odds rank asc, odds_score desc. */
export function tierOddsScoreCmp(a: JobLight, b: JobLight): number {
  if (a.tier !== b.tier) return b.tier - a.tier;
  const or = oddsRank(a.odds) - oddsRank(b.odds);
  if (or !== 0) return or;
  return score(b) - score(a);
}

/** "Bank a win" — winnable first: odds rank, then odds_score desc, then tier desc. */
export function bankCmp(a: JobLight, b: JobLight): number {
  const o = oddsRank(a.odds) - oddsRank(b.odds);
  if (o !== 0) return o;
  const s = score(b) - score(a);
  if (s !== 0) return s;
  return b.tier - a.tier;
}

/** "Aim high" — fit first: tier desc, then odds rank, then odds_score desc. */
export function aimCmp(a: JobLight, b: JobLight): number {
  const t = b.tier - a.tier;
  if (t !== 0) return t;
  const o = oddsRank(a.odds) - oddsRank(b.odds);
  if (o !== 0) return o;
  return score(b) - score(a);
}

/** Eligible for the do-today queue: strong fit and currently actionable. */
export function isQueueEligible(job: JobLight): boolean {
  return job.tier >= 3 && isActionable(job);
}

/**
 * Deterministic, pure queue composition: bank (odds-first) interleaved with aim
 * (tier-4/5 fit-first), deduped by url_b64, capped at `cap`.
 */
export function composeQueue(jobs: JobLight[], cap: number): JobLight[] {
  const eligible = jobs.filter(isQueueEligible);
  const bank = [...eligible].sort(bankCmp);
  const aim = eligible.filter((j) => j.tier === 4 || j.tier === 5).sort(aimCmp);

  const out: JobLight[] = [];
  const seen = new Set<string>();
  const push = (j: JobLight) => {
    if (out.length >= cap) return;
    if (seen.has(j.url_b64)) return;
    seen.add(j.url_b64);
    out.push(j);
  };

  const n = Math.max(bank.length, aim.length);
  for (let i = 0; i < n && out.length < cap; i++) {
    if (i < bank.length) push(bank[i]);
    if (out.length >= cap) break;
    if (i < aim.length) push(aim[i]);
  }
  return out;
}
