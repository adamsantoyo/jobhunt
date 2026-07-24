// Tier x Odds count heatmap. A small counts matrix reads best as a grid, not a
// recharts plot: one sequential blue ramp carries magnitude (surface -> accent),
// and the count sits in every cell so identity is never color-alone. Clicking a
// cell is out of scope here (the Matrix view owns navigation); this is read-only.

const ODDS = ["Likely", "Target", "Reach"] as const;
const TIERS = [5, 4, 3, 2, 1] as const;

export function HeatMatrix({ matrix }: { matrix: Record<string, Record<string, number>> }) {
  let max = 0;
  for (const t of TIERS) {
    for (const o of ODDS) {
      const v = matrix?.[String(t)]?.[o] ?? 0;
      if (v > max) max = v;
    }
  }

  const cell = (count: number) => {
    // sequential magnitude: alpha from ~0.06 (near zero) to 0.85 (max)
    const frac = max > 0 ? count / max : 0;
    const alpha = count === 0 ? 0 : 0.08 + frac * 0.77;
    const strong = frac > 0.55;
    return {
      background: count === 0 ? "var(--bg-2)" : `rgba(74, 158, 255, ${alpha.toFixed(3)})`,
      color: count === 0 ? "var(--fg-faint)" : strong ? "#eaf3ff" : "var(--fg-dim)",
    };
  };

  return (
    <div className="an-heat" role="table" aria-label="Tier by odds counts">
      <div className="an-heat-row an-heat-head" role="row">
        <span className="an-heat-corner" role="columnheader" />
        {ODDS.map((o) => (
          <span key={o} className="an-heat-colh" role="columnheader">
            {o}
          </span>
        ))}
      </div>
      {TIERS.map((t) => (
        <div key={t} className="an-heat-row" role="row">
          <span className="an-heat-rowh" role="rowheader">
            T{t}
          </span>
          {ODDS.map((o) => {
            const count = matrix?.[String(t)]?.[o] ?? 0;
            const style = cell(count);
            return (
              <span
                key={o}
                className="an-heat-cell"
                role="cell"
                style={style}
                title={`Tier ${t} · ${o}: ${count}`}
              >
                {count}
              </span>
            );
          })}
        </div>
      ))}
    </div>
  );
}
