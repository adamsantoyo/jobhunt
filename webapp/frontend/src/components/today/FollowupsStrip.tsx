import { useState, type CSSProperties } from "react";
import { useFollowups } from "../../store/queries";
import { fmtDate } from "../../lib/format";
import { statusOf } from "../../lib/statuses";
import { StatusBadge } from "../StatusBadge";
import type { JobLight } from "../../api/types";

// Overdue-then-upcoming follow-up rows. Hidden entirely when empty; capped to
// CAP visible rows with a "+N more" expander.
const CAP = 6;

const wrapStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius)",
  overflow: "hidden",
  background: "var(--bg-1)",
};
const rowStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  padding: "6px 10px",
  borderBottom: "1px solid var(--border-soft)",
  cursor: "pointer",
  background: "transparent",
  border: "none",
  color: "var(--fg)",
  font: "inherit",
  textAlign: "left",
  width: "100%",
};
const groupHeadStyle: CSSProperties = {
  padding: "4px 10px 2px",
  fontSize: 10.5,
  textTransform: "uppercase",
  letterSpacing: "0.04em",
  color: "var(--fg-mute)",
  background: "var(--bg-2)",
};
const titleStyle: CSSProperties = {
  flex: 1,
  minWidth: 0,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  color: "var(--fg-dim)",
};

function Row({
  job,
  onOpen,
  accent,
}: {
  job: JobLight;
  onOpen: (job: JobLight) => void;
  accent?: string;
}) {
  const status = statusOf(job);
  const due = job.state?.follow_up_date;
  return (
    <button type="button" style={rowStyle} onClick={() => onOpen(job)}>
      <span style={{ fontWeight: 600 }}>{job.company ?? "—"}</span>
      <span style={{ color: "var(--fg-faint)" }}>·</span>
      <span style={titleStyle}>{job.title ?? "—"}</span>
      <StatusBadge status={status} />
      <span className="muted-sm" style={accent ? { color: accent } : undefined}>
        due {fmtDate(due)}
      </span>
    </button>
  );
}

export function FollowupsStrip({ onOpen }: { onOpen: (job: JobLight) => void }) {
  const { data } = useFollowups();
  const [expandedAll, setExpandedAll] = useState(false);

  const overdue = data?.overdue ?? [];
  const upcoming = data?.upcoming ?? [];
  if (overdue.length === 0 && upcoming.length === 0) return null;

  const combined: Array<{ job: JobLight; group: "overdue" | "upcoming" }> = [
    ...overdue.map((job) => ({ job, group: "overdue" as const })),
    ...upcoming.map((job) => ({ job, group: "upcoming" as const })),
  ];
  const visible = expandedAll ? combined : combined.slice(0, CAP);
  const hiddenCount = combined.length - visible.length;

  let lastGroup: "overdue" | "upcoming" | null = null;

  return (
    <div style={wrapStyle}>
      {visible.map(({ job, group }) => {
        const showHead = group !== lastGroup;
        lastGroup = group;
        return (
          <div key={job.url_b64}>
            {showHead && <div style={groupHeadStyle}>{group === "overdue" ? "Overdue" : "Upcoming"}</div>}
            <Row job={job} onOpen={onOpen} accent={group === "overdue" ? "var(--red)" : undefined} />
          </div>
        );
      })}
      {hiddenCount > 0 && (
        <button
          type="button"
          className="btn btn-sm"
          style={{ margin: 6, alignSelf: "flex-start" }}
          onClick={() => setExpandedAll(true)}
        >
          +{hiddenCount} more
        </button>
      )}
    </div>
  );
}
