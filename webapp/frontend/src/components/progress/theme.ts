// Chart palette + helpers for the /progress dashboard, scoped to this area.
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

// Competition axis (Phase 3.5): jobs.odds now stores a combined "<match> /
// <competition>" string; charts key colors off the competition half only.
// Lower bar (easiest) = green, Standard = amber, High competition = red.
export const COMPETITION_COLORS: Record<string, string> = {
  "Lower bar": VIZ.green,
  Standard: VIZ.amber,
  "High competition": VIZ.red,
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
