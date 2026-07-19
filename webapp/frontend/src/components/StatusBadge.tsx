// Tier / odds / status / flag badges. Colors follow the contract & tracker xlsx
// semantics and are defined in index.css via data attributes.

export function TierBadge({ tier }: { tier: number }) {
  return (
    <span className="badge tier" data-tier={tier} title={`Tier ${tier}`}>
      T{tier}
    </span>
  );
}

export function OddsBadge({ odds }: { odds: string | null | undefined }) {
  if (!odds) return null;
  return (
    <span className="badge odds" data-odds={odds} title={`Odds: ${odds}`}>
      {odds}
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
