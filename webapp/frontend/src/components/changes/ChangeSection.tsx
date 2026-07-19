import type { ReactNode } from "react";

// Titled, collapsible-looking section wrapper for a Changes list. Shows a count
// pill and an empty-state line when there is nothing to show.
export function ChangeSection({
  title,
  count,
  accent,
  children,
}: {
  title: string;
  count: number;
  accent?: string;
  children: ReactNode;
}) {
  return (
    <section
      style={{
        border: "1px solid var(--border)",
        borderRadius: "var(--radius)",
        background: "var(--bg-1)",
        overflow: "hidden",
      }}
    >
      <header
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "8px 12px",
          borderBottom: "1px solid var(--border-soft)",
        }}
      >
        <span style={{ fontWeight: 600, color: accent ?? "var(--fg)" }}>{title}</span>
        <span className="chip" style={{ fontVariantNumeric: "tabular-nums" }}>
          {count}
        </span>
      </header>
      <div style={{ padding: count > 0 ? "6px 8px 8px" : "10px 12px" }}>
        {count > 0 ? (
          children
        ) : (
          <span className="muted-sm">None</span>
        )}
      </div>
    </section>
  );
}
