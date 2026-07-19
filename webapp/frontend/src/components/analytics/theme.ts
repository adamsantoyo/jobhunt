// Chart palette + helpers for the /analytics dashboard, scoped to this area.
// Colors are the app's fixed design tokens (index.css) expressed as concrete hex
// so recharts can set them as SVG fills. The two-series legend pair (blue kept /
// green with_desc) was validated for CVD separation + contrast on the dark chart
// surface; identity is never color-alone (legend + direct labels back every chart).

export const VIZ = {
  // categorical / semantic marks
  blue: "#4a9eff", // series 1 (kept, primary magnitude)
  green: "#3fb950", // series 2 (with_desc / good)
  amber: "#d9a441",
  orange: "#e0873d",
  red: "#f0553f",
  // chart chrome (map to index.css tokens)
  surface: "#141920", // --bg-1 (chart surface)
  grid: "#232b35", // --border-soft (hairline gridline)
  axis: "#2a333f", // --border (baseline / axis)
  ink: "#e6edf3", // --fg (primary ink)
  dim: "#aeb9c5", // --fg-dim (secondary ink)
  mute: "#7d8896", // --fg-mute (axis labels)
  bandFill: "rgba(74, 158, 255, 0.12)",
  bandStroke: "rgba(74, 158, 255, 0.5)",
} as const;

// Ordinal tier ramp: tier 5 strong green -> tier 1 muted. Mirrors the tier badge
// semantics in index.css (5 = strong, 4 = light green, 3 neutral, 2/1 muted).
export const TIER_COLORS: Record<number, string> = {
  5: "#3fb950",
  4: "#56a663",
  3: "#6b7684",
  2: "#4c5661",
  1: "#3c454f",
};

export const ODDS_COLORS: Record<string, string> = {
  Likely: VIZ.green,
  Target: VIZ.amber,
  Reach: VIZ.red,
};

/** Compact dollar label, e.g. 100000 -> "$100k". */
export function kfmt(n: number): string {
  if (!Number.isFinite(n)) return "";
  return `$${Math.round(n / 1000)}k`;
}

/** Compact integer, e.g. 1284 -> "1,284". */
export function nfmt(n: number): string {
  return n.toLocaleString("en-US");
}

// Shared axis tick style for recharts.
export const TICK = { fill: VIZ.mute, fontSize: 11 } as const;

// Scoped stylesheet for the analytics dashboard (injected once by the page so we
// never touch the shared index.css). All values reference existing CSS tokens.
export const ANALYTICS_CSS = `
.an-page { padding: 16px; display: flex; flex-direction: column; gap: 16px; }
.an-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }
.an-stat {
  background: var(--bg-1); border: 1px solid var(--border-soft);
  border-radius: var(--radius); padding: 12px 14px; min-width: 0;
}
.an-stat-value { font-size: 26px; font-weight: 600; line-height: 1.1; }
.an-stat-label {
  font-size: 11px; color: var(--fg-mute); text-transform: uppercase;
  letter-spacing: 0.04em; margin-top: 5px;
}
.an-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }
.an-card {
  background: var(--bg-1); border: 1px solid var(--border-soft);
  border-radius: var(--radius); display: flex; flex-direction: column; min-width: 0;
}
.an-card-head {
  padding: 10px 14px 2px; display: flex; justify-content: space-between;
  align-items: baseline; gap: 10px;
}
.an-card-title { font-size: 13px; margin: 0; }
.an-card-sub { font-size: 11px; color: var(--fg-mute); white-space: nowrap; }
.an-card-body { padding: 6px 8px 10px; min-width: 0; }
.an-card-body.an-scroll { overflow-y: auto; }
.an-tip {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius-sm); padding: 6px 9px; font-size: 11px;
  box-shadow: 0 6px 18px rgba(0, 0, 0, 0.45); pointer-events: none;
}
.an-tip-title { color: var(--fg); font-weight: 600; margin-bottom: 3px; }
.an-tip-row { color: var(--fg-dim); display: flex; gap: 14px; justify-content: space-between; }
.an-tip-row + .an-tip-row { margin-top: 1px; }
.an-tip-key { display: inline-flex; align-items: center; gap: 5px; }
.an-tip-val { color: var(--fg); font-variant-numeric: tabular-nums; }
.an-swatch { width: 8px; height: 8px; border-radius: 2px; display: inline-block; flex: 0 0 auto; }
.an-empty { color: var(--fg-mute); padding: 26px; text-align: center; font-size: 12px; }
.an-heat { display: flex; flex-direction: column; gap: 2px; padding: 4px 6px 2px; }
.an-heat-row { display: grid; grid-template-columns: 40px repeat(3, 1fr); gap: 2px; }
.an-heat-head { margin-bottom: 2px; }
.an-heat-corner { }
.an-heat-colh, .an-heat-rowh {
  font-size: 11px; color: var(--fg-mute); display: flex; align-items: center;
}
.an-heat-colh { justify-content: center; }
.an-heat-rowh { justify-content: flex-end; padding-right: 6px; font-weight: 600; }
.an-heat-cell {
  display: flex; align-items: center; justify-content: center;
  height: 34px; border-radius: var(--radius-sm); font-size: 13px; font-weight: 600;
  font-variant-numeric: tabular-nums;
}
`;
