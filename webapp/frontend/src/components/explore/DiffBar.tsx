import { useMemo } from "react";
import { useAnalytics } from "../../store/queries";
import type { ChangesResponse } from "../../api/types";

export type DiffKind = "new" | "reposted" | "tier" | "gone";

const CHIPS: Array<{ key: DiffKind | null; label: string }> = [
  { key: null, label: "All" },
  { key: "new", label: "New this run" },
  { key: "reposted", label: "Reposted" },
  { key: "tier", label: "Tier changed" },
  { key: "gone", label: "Disappeared" },
];

function countFor(key: DiffKind | null, changes: ChangesResponse | undefined): number | null {
  if (!changes || key === null) return null;
  switch (key) {
    case "new":
      return changes.new.length;
    case "reposted":
      return changes.reposted.length;
    case "tier":
      return changes.tier_changed.length;
    case "gone":
      return changes.disappeared.length;
  }
}

// Diff chip row + baseline picker. Chip counts and the baseline<->current
// caption come from `changes` (the caller owns the useChanges(since) call so
// the same response also drives row filtering). Baseline option list mirrors
// the old Changes.tsx: run dates from useAnalytics().new_per_run, excluding
// whichever run is "current".
export function DiffBar({
  diff,
  since,
  changes,
  changesLoading,
  onSelectDiff,
  onSelectSince,
}: {
  diff: DiffKind | null;
  since: string | undefined;
  changes: ChangesResponse | undefined;
  changesLoading: boolean;
  onSelectDiff: (d: DiffKind | null) => void;
  onSelectSince: (v: string) => void;
}) {
  const { data: analytics } = useAnalytics();

  const runDates = useMemo(
    () => (analytics?.new_per_run ?? []).map((r) => r.run_date).sort(),
    [analytics],
  );

  const currentRun = changes?.current ?? (runDates.length ? runDates[runDates.length - 1] : null);
  const baselineOptions = useMemo(
    () => runDates.filter((d) => d !== currentRun),
    [runDates, currentRun],
  );

  return (
    <div className="explore-diffbar">
      <div className="explore-diffbar-chips">
        {CHIPS.map((c) => {
          const n = countFor(c.key, changes);
          const active = diff === c.key;
          return (
            <button
              key={c.label}
              type="button"
              className="chip-toggle"
              data-on={active ? "1" : "0"}
              onClick={() => onSelectDiff(c.key)}
            >
              {c.label}
              {n !== null && <span className="muted-sm"> ({n})</span>}
            </button>
          );
        })}
      </div>
      <div className="explore-diffbar-baseline">
        <span className="field-label">Compare</span>
        <select
          className="input filter-select"
          value={since ?? ""}
          onChange={(e) => onSelectSince(e.target.value)}
        >
          <option value="">Previous run (auto)</option>
          {baselineOptions.map((d) => (
            <option key={d} value={d}>
              {d}
            </option>
          ))}
        </select>
        <span className="muted-sm">
          {changes?.baseline ?? "…"} <span aria-hidden>→</span> {currentRun ?? "…"}
          {changesLoading ? " (loading…)" : ""}
        </span>
      </div>
    </div>
  );
}
