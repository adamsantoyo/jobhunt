// Custom recharts tooltip using the app's text tokens (never the series color for
// text — the swatch beside each row carries identity). Passed via <Tooltip
// content={(p) => <ChartTooltip {...p} .../>} so we control label + value formatting.

// `value` matches recharts' ValueType (a number/string or an array of them) so
// this component can be passed straight to <Tooltip content={...}>.
interface TipEntry {
  name?: string | number;
  value?: number | string | Array<number | string>;
  color?: string;
  fill?: string;
  dataKey?: string | number;
  payload?: Record<string, unknown>;
}

export function ChartTooltip({
  active,
  payload,
  label,
  title,
  fmtValue,
}: {
  active?: boolean;
  payload?: TipEntry[];
  label?: string | number;
  // Optional override for the heading (else uses `label`).
  title?: (label: string | number | undefined, p: TipEntry[]) => string;
  fmtValue?: (v: number, e: TipEntry) => string;
}) {
  if (!active || !payload || payload.length === 0) return null;
  const heading = title ? title(label, payload) : String(label ?? "");
  return (
    <div className="an-tip">
      {heading && <div className="an-tip-title">{heading}</div>}
      {payload.map((e, i) => {
        const swatch = e.color || e.fill || "var(--fg-mute)";
        const raw = typeof e.value === "number" ? e.value : Number(e.value);
        const val =
          fmtValue && Number.isFinite(raw)
            ? fmtValue(raw, e)
            : String(e.value ?? "");
        return (
          <div className="an-tip-row" key={i}>
            <span className="an-tip-key">
              <span className="an-swatch" style={{ background: swatch }} />
              {e.name}
            </span>
            <span className="an-tip-val">{val}</span>
          </div>
        );
      })}
    </div>
  );
}
