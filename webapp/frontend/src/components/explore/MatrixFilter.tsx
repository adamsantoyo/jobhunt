import { useState } from "react";
import { OddsBadge, TierBadge } from "../StatusBadge";
import type { TableFilters } from "../FilterBar";
import type { JobLight } from "../../api/types";

// Fit x Odds heat map, doubling as a filter widget. Cell counts come from
// `facetJobs` (the caller has already applied every filter EXCEPT tiers/odds,
// so a cell shows "how many jobs would match if you picked this tier+odds").
// Clicking a cell / header mutates the SAME filters object FilterBar reads, so
// the two stay in sync for free (one state object, per the design contract).

const TIERS = [5, 4, 3, 2, 1] as const;
const ODDS = ["Likely", "Target", "Reach"] as const;

// Corner labels per contract: tier5xLikely = apply today, tier5xReach = aspirational.
function cornerLabel(tier: number, odds: string): string | undefined {
  if (tier === 5 && odds === "Likely") return "apply today";
  if (tier === 5 && odds === "Reach") return "aspirational";
  return undefined;
}

function countBuckets(jobs: JobLight[]): Map<string, number> {
  const m = new Map<string, number>();
  for (const t of TIERS) for (const o of ODDS) m.set(`${t}|${o}`, 0);
  for (const j of jobs) {
    if (!j.odds || !(ODDS as readonly string[]).includes(j.odds)) continue;
    const key = `${j.tier}|${j.odds}`;
    m.set(key, (m.get(key) ?? 0) + 1);
  }
  return m;
}

export function MatrixFilter({
  facetJobs,
  filters,
  onChange,
}: {
  facetJobs: JobLight[];
  filters: TableFilters;
  onChange: (next: TableFilters) => void;
}) {
  const [expanded, setExpanded] = useState(true);
  const counts = countBuckets(facetJobs);
  const maxCount = Math.max(1, ...counts.values());

  const isTierActive = (t: number) => filters.tiers.length === 1 && filters.tiers[0] === t;
  const isOddsActive = (o: string) => filters.odds.length === 1 && filters.odds[0] === o;
  const isCellActive = (t: number, o: string) => isTierActive(t) && isOddsActive(o);

  const onTierHeaderClick = (t: number) =>
    onChange({ ...filters, tiers: isTierActive(t) ? [] : [t] });
  const onOddsHeaderClick = (o: string) =>
    onChange({ ...filters, odds: isOddsActive(o) ? [] : [o] });
  const onCellClick = (t: number, o: string) =>
    onChange(
      isCellActive(t, o)
        ? { ...filters, tiers: [], odds: [] }
        : { ...filters, tiers: [t], odds: [o] },
    );

  const cellBg = (count: number) => {
    if (count === 0) return "var(--bg-1)";
    const pct = 10 + Math.round((count / maxCount) * 55);
    return `color-mix(in srgb, var(--accent) ${pct}%, var(--bg-1))`;
  };

  return (
    <div className="explore-matrix">
      <div className="explore-matrix-toggle-row">
        <button type="button" className="btn btn-sm" onClick={() => setExpanded((e) => !e)}>
          Matrix {expanded ? "▾" : "▸"}
        </button>
        {!expanded && (
          <span className="muted-sm">Fit (tier) x Odds heat map. Click to expand.</span>
        )}
      </div>

      {expanded && (
        <div className="explore-matrix-grid">
          <div />
          {ODDS.map((o) => (
            <button
              key={`h-${o}`}
              type="button"
              className="explore-matrix-colhead"
              data-active={isOddsActive(o) ? "1" : "0"}
              onClick={() => onOddsHeaderClick(o)}
              title={`Toggle odds = ${o}`}
            >
              <OddsBadge odds={o} />
            </button>
          ))}

          {TIERS.map((t) => (
            <FilterRow
              key={t}
              tier={t}
              isTierActive={isTierActive(t)}
              onTierHeaderClick={onTierHeaderClick}
              isCellActive={isCellActive}
              onCellClick={onCellClick}
              counts={counts}
              cellBg={cellBg}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function FilterRow({
  tier,
  isTierActive,
  onTierHeaderClick,
  isCellActive,
  onCellClick,
  counts,
  cellBg,
}: {
  tier: number;
  isTierActive: boolean;
  onTierHeaderClick: (t: number) => void;
  isCellActive: (t: number, o: string) => boolean;
  onCellClick: (t: number, o: string) => void;
  counts: Map<string, number>;
  cellBg: (count: number) => string;
}) {
  return (
    <>
      <button
        type="button"
        className="explore-matrix-rowhead"
        data-active={isTierActive ? "1" : "0"}
        onClick={() => onTierHeaderClick(tier)}
        title={`Toggle tier = ${tier}`}
      >
        <TierBadge tier={tier} />
      </button>
      {ODDS.map((o) => {
        const count = counts.get(`${tier}|${o}`) ?? 0;
        const label = cornerLabel(tier, o);
        return (
          <button
            key={`${tier}-${o}`}
            type="button"
            className="explore-matrix-cell"
            data-active={isCellActive(tier, o) ? "1" : "0"}
            style={{ background: cellBg(count) }}
            onClick={() => onCellClick(tier, o)}
            title={`Tier ${tier} x ${o}: ${count} job${count === 1 ? "" : "s"}`}
          >
            <span className="explore-matrix-cell-count">{count}</span>
            {label && <span className="explore-matrix-cell-corner">{label}</span>}
          </button>
        );
      })}
    </>
  );
}
