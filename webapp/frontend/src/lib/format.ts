import type { JobLight } from "../api/types";

/** Local-time ISO date, YYYY-MM-DD. */
export function todayISO(): string {
  return dateToISO(new Date());
}

/** Convert a Date to a local YYYY-MM-DD string. */
export function dateToISO(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/** ISO date `days` from today (local). */
export function isoPlusDays(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return dateToISO(d);
}

function shortMoney(n: number): string {
  if (n >= 1000) {
    const k = n / 1000;
    const rounded = Math.round(k * 10) / 10;
    return `$${Number.isInteger(rounded) ? rounded.toFixed(0) : rounded.toFixed(1)}k`;
  }
  return `$${n}`;
}

/**
 * Human salary string. Prefers the raw display string from the pipeline; falls
 * back to synthesizing one from salary_min / salary_max.
 */
export function fmtSalary(job: Pick<JobLight, "salary" | "salary_min" | "salary_max">): string {
  // Prefer the parsed annualized numbers: the raw salary text is scraper output
  // ("55000.0-65000.0 yearly", "USD 130,000.00 - 180,000.00 per year", ...).
  const lo = job.salary_min;
  const hi = job.salary_max;
  if (lo != null && hi != null) return lo === hi ? shortMoney(lo) : `${shortMoney(lo)}-${shortMoney(hi)}`;
  if (lo != null) return `${shortMoney(lo)}+`;
  if (hi != null) return `up to ${shortMoney(hi)}`;
  if (job.salary && job.salary.trim()) return job.salary.trim();
  return "";
}

/** Short, stable date rendering. Passes through non-ISO strings unchanged. */
export function fmtDate(d: string | null | undefined): string {
  if (!d) return "";
  const s = String(d).trim();
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (!m) return s;
  return `${m[1]}-${m[2]}-${m[3]}`;
}

/** Only http(s) URLs are safe to open or link — scraped data can carry any scheme. */
export function isHttpUrl(url: string | null | undefined): boolean {
  return !!url && (url.startsWith("http://") || url.startsWith("https://"));
}

/** Split the comma+space-joined flags string into a clean list. */
export function flagsList(flags: string | null | undefined): string[] {
  if (!flags) return [];
  return flags
    .split(",")
    .map((f) => f.trim())
    .filter(Boolean);
}

/**
 * The scorer stores `odds` as a combined "<match label> / <competition label>"
 * string (e.g. "Strong match / High competition") -- honest match/competition
 * labels, replacing a Likely/Target/Reach prediction the tool had no outcome
 * data to calibrate. `parseOdds` splits it back into its two halves.
 *
 * A legacy single-word value (Likely/Target/Reach, written by a scorer older
 * than this change and still possible in job_history/jobs until the next
 * sweep) has no " / " separator, so both halves come back null -- callers
 * must treat that as "unknown", never throw.
 */
export interface ParsedOdds {
  match: string | null;
  competition: string | null;
}

export function parseOdds(odds: string | null | undefined): ParsedOdds {
  if (!odds) return { match: null, competition: null };
  const idx = odds.indexOf(" / ");
  if (idx === -1) return { match: null, competition: null };
  return { match: odds.slice(0, idx), competition: odds.slice(idx + 3) };
}

const MATCH_RANK: Record<string, number> = {
  "Strong match": 0,
  "Moderate match": 1,
  "Weak match": 2,
  "Level stretch": 3,
  Unscored: 4,
};

const COMPETITION_RANK: Record<string, number> = {
  "Lower bar": 0,
  Standard: 1,
  "High competition": 2,
};

/**
 * Sortable odds rank: match quality first (Strong match best), then
 * competition (Lower bar ranks better than High competition). A legacy
 * single-word value or anything else unparseable ranks after every known
 * match/competition combination -- never throws.
 */
export function oddsRank(odds: string | null | undefined): number {
  const { match, competition } = parseOdds(odds);
  if (match == null || competition == null) return 1000;
  const m = MATCH_RANK[match] ?? MATCH_RANK.Unscored + 1;
  const c = COMPETITION_RANK[competition] ?? COMPETITION_RANK["High competition"] + 1;
  return m * 10 + c;
}

/**
 * A job is actionable/untriaged iff it has no state (or status "New"), is not
 * hidden, and is not currently snoozed.
 */
export function isActionable(job: JobLight): boolean {
  const s = job.state;
  const today = todayISO();
  const statusOk = !s || s.status === "New";
  const notHidden = !s || !s.hidden;
  const notSnoozed = !s || !s.snoozed_until || s.snoozed_until <= today;
  return statusOk && notHidden && notSnoozed;
}
