// Tier / odds / status / flag badges. Colors follow the contract & tracker xlsx
// semantics and are defined in index.css via data attributes.

import { parseOdds } from "../lib/format";

export function TierBadge({ tier }: { tier: number }) {
  return (
    <span className="badge tier" data-tier={tier} title={`Tier ${tier}`}>
      T{tier}
    </span>
  );
}

/** A job's stored odds string: "<match label> / <competition label>". Colored
 * by the competition half; a legacy single-word value (Likely/Target/Reach)
 * still renders as plain text, unstyled, instead of crashing. */
export function OddsBadge({ odds }: { odds: string | null | undefined }) {
  if (!odds) return null;
  const { competition } = parseOdds(odds);
  return (
    <span className="badge odds" data-competition={competition ?? "unknown"} title={`Match: ${odds}`}>
      {odds}
    </span>
  );
}

/** A bare competition-axis label ("High competition" / "Standard" / "Lower
 * bar"), for the matrix/heatmap headers that key on that axis alone. */
export function CompetitionBadge({ competition }: { competition: string }) {
  return (
    <span className="badge odds" data-competition={competition} title={`Competition: ${competition}`}>
      {competition}
    </span>
  );
}

export function StatusBadge({ status }: { status: string | null | undefined }) {
  const s = status || "New";
  return (
    <span className="badge status" data-status={s} title={`Status: ${s}`}>
      {s}
    </span>
  );
}

/** Maps a flag string to its semantic color class. */
export function flagKind(flag: string): string {
  const f = flag.toLowerCase();
  if (f.includes("reposted")) return "amber";
  if (f.includes("unresolved-aggregator")) return "orange";
  if (f.includes("salary-from-desc") || f.includes("salary_from_desc")) return "green";
  if (f.includes("stale") || f.includes("ghost")) return "red";
  return "muted";
}

export function FlagBadge({ flag }: { flag: string }) {
  return (
    <span className="badge flag" data-kind={flagKind(flag)} title={flag}>
      {flag}
    </span>
  );
}
