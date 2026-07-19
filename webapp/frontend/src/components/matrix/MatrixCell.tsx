import { useState, type CSSProperties } from "react";
import { JobChip } from "../JobChip";
import type { JobLight } from "../../api/types";

// One tier x odds cell: a count header, an optional corner label, and a capped
// list of JobChips (click -> drawer). "+N more" reveals the rest.

const CAP = 18;

const cellStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 4,
  padding: 6,
  minWidth: 0,
  background: "var(--bg-1)",
  border: "1px solid var(--border-soft)",
  borderRadius: "var(--radius-sm)",
};
const headStyle: CSSProperties = {
  display: "flex",
  alignItems: "baseline",
  justifyContent: "space-between",
  gap: 6,
};
const countStyle: CSSProperties = {
  fontVariantNumeric: "tabular-nums",
  fontWeight: 600,
  color: "var(--fg-dim)",
};
const cornerStyle: CSSProperties = {
  fontSize: 10,
  textTransform: "uppercase",
  letterSpacing: "0.04em",
  color: "var(--fg-mute)",
};
const listStyle: CSSProperties = { display: "flex", flexDirection: "column", gap: 4 };
const emptyStyle: CSSProperties = { color: "var(--fg-faint)", fontSize: 12, padding: "2px 0" };

export function MatrixCell({
  jobs,
  onOpen,
  cornerLabel,
}: {
  jobs: JobLight[];
  onOpen: (job: JobLight) => void;
  cornerLabel?: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const shown = expanded ? jobs : jobs.slice(0, CAP);
  const overflow = jobs.length - shown.length;

  return (
    <div style={cellStyle}>
      <div style={headStyle}>
        <span style={countStyle}>{jobs.length}</span>
        {cornerLabel && <span style={cornerStyle}>{cornerLabel}</span>}
      </div>
      {jobs.length === 0 ? (
        <span style={emptyStyle}>—</span>
      ) : (
        <div style={listStyle}>
          {shown.map((job) => (
            <JobChip key={job.url_b64} job={job} onOpen={onOpen} showBadges={false} />
          ))}
          {overflow > 0 && (
            <button type="button" className="btn btn-sm btn-link" onClick={() => setExpanded(true)}>
              +{overflow} more
            </button>
          )}
          {expanded && jobs.length > CAP && (
            <button type="button" className="btn btn-sm btn-link" onClick={() => setExpanded(false)}>
              show less
            </button>
          )}
        </div>
      )}
    </div>
  );
}
