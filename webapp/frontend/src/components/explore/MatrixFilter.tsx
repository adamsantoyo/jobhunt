import { useState } from "react";
import { CompetitionBadge, TierBadge } from "../StatusBadge";
import type { TableFilters } from "../FilterBar";
import type { JobLight } from "../../api/types";
import { parseOdds } from "../../lib/format";

// Fit x Competition heat map, doubling as a filter widget. Cell counts come
// from `facetJobs` (the caller has already applied every filter EXCEPT
// tiers/competition, so a cell shows "how many jobs would match if you picked
// this tier+competition"). Clicking a cell / header mutates the SAME filters
// object FilterBar reads, so the two stay in sync for free (one state object,
// per the design contract). The match axis is a separate FilterBar chip
// group, not a matrix column -- 5 match labels x 3 competition labels would
// make a 15-cell grid unreadable.

const TIERS = [5, 4, 3, 2, 1] as const;
const COMPETITION = ["High competition", "Standard", "Lower bar"] as const;

// Corner labels per contract: tier5xLower-bar = strongest cell (apply today),
// tier5xHigh-competition = aspirational.
function cornerLabel(tier: number, competition: string): string | undefined {
  if (tier === 5 && competition === "Lower bar") return "apply today";
  if (tier === 5 && competition === "High competition") return "aspirational";
  return undefined;
}

function countBuckets(jobs: JobLight[]): Map<string, number> {
  const m = new Map<string, number>();
  for (const t of TIERS) for (const c of COMPETITION) m.set(`${t}|${c}`, 0);
  for (const j of jobs) {
    const competition = parseOdds(j.odds).competition;
    if (!competition || !(COMPETITION as readonly string[]).includes(competition)) continue;
    const key = `${j.tier}|${competition}`;
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
  const isCompetitionActive = (c: string) =>
    filters.competition.length === 1 && filters.competition[0] === c;
  const isCellActive = (t: number, c: string) => isTierActive(t) && isCompetitionActive(c);

  const onTierHeaderClick = (t: number) =>
    onChange({ ...filters, tiers: isTierActive(t) ? [] : [t] });
  const onCompetitionHeaderClick = (c: string) =>
    onChange({ ...filters, competition: isCompetitionActive(c) ? [] : [c] });
  const onCellClick = (t: number, c: string) =>
    onChange(
      isCellActive(t, c)
        ? { ...filters, tiers: [], competition: [] }
        : { ...filters, tiers: [t], competition: [c] },
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
          <span className="muted-sm">Fit (tier) x competition heat map. Click to expand.</span>
        )}
      </div>

      {expanded && (
        <div className="explore-matrix-grid">
          <div />
          {COMPETITION.map((c) => (
            <button
              key={`h-${c}`}
              type="button"
              className="explore-matrix-colhead"
              data-active={isCompetitionActive(c) ? "1" : "0"}
              onClick={() => onCompetitionHeaderClick(c)}
              title={`Toggle competition = ${c}`}
            >
              <CompetitionBadge competition={c} />
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
  isCellActive: (t: number, c: string) => boolean;
  onCellClick: (t: number, c: string) => void;
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
      {COMPETITION.map((c) => {
        const count = counts.get(`${tier}|${c}`) ?? 0;
        const label = cornerLabel(tier, c);
        return (
          <button
            key={`${tier}-${c}`}
            type="button"
            className="explore-matrix-cell"
            data-active={isCellActive(tier, c) ? "1" : "0"}
            style={{ background: cellBg(count) }}
            onClick={() => onCellClick(tier, c)}
            title={`Tier ${tier} x ${c}: ${count} job${count === 1 ? "" : "s"}`}
          >
            <span className="explore-matrix-cell-count">{count}</span>
            {label && <span className="explore-matrix-cell-corner">{label}</span>}
          </button>
        );
      })}
    </>
  );
}
