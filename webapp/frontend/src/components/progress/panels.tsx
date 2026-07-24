import type { ReactNode } from "react";

// Top-of-dashboard stat tile: big value in a semantic accent, quiet label below.
// Value uses proportional figures (per the dataviz stat-tile contract).
export function StatCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: ReactNode;
  accent?: string;
}) {
  return (
    <div className="an-stat">
      <div className="an-stat-value" style={accent ? { color: accent } : undefined}>
        {value}
      </div>
      <div className="an-stat-label">{label}</div>
    </div>
  );
}

// Titled chart container. `height` fixes the plot area so ResponsiveContainer can
// measure; `scroll` lets a tall categorical chart (source coverage) scroll in place.
export function ChartCard({
  title,
  subtitle,
  height = 220,
  scroll = false,
  children,
}: {
  title: string;
  subtitle?: string;
  height?: number;
  scroll?: boolean;
  children: ReactNode;
}) {
  return (
    <section className="an-card">
      <header className="an-card-head">
        <h2 className="an-card-title">{title}</h2>
        {subtitle && <span className="an-card-sub">{subtitle}</span>}
      </header>
      <div className={scroll ? "an-card-body an-scroll" : "an-card-body"} style={{ height }}>
        {children}
      </div>
    </section>
  );
}
