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

/** Split the comma+space-joined flags string into a clean list. */
export function flagsList(flags: string | null | undefined): string[] {
  if (!flags) return [];
  return flags
    .split(",")
    .map((f) => f.trim())
    .filter(Boolean);
}

/** Sortable odds rank: Likely (best) < Target < Reach < unknown. */
export function oddsRank(odds: string | null | undefined): number {
  switch (odds) {
    case "Likely":
      return 0;
    case "Target":
      return 1;
    case "Reach":
      return 2;
    default:
      return 3;
  }
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
