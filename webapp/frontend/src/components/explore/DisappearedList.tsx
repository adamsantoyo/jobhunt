import type { CSSProperties } from "react";
import { TierBadge } from "../StatusBadge";
import type { DisappearedJob } from "../../api/types";

// Read-only rows for diff=gone. Disappeared jobs are not present in /api/jobs
// (they dropped off the tracker), so there is no drawer to open and no
// selection/bulk affordance for them.

const rowStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  width: "100%",
  background: "var(--bg-2)",
  border: "1px solid var(--border-soft)",
  borderRadius: "var(--radius-sm)",
  padding: "5px 8px",
  fontSize: 12,
  opacity: 0.75,
};

function DisappearedRow({ job }: { job: DisappearedJob }) {
  return (
    <div style={rowStyle}>
      <span style={{ flex: "0 0 auto" }}>
        <TierBadge tier={job.tier} />
      </span>
      <span
        style={{
          fontWeight: 600,
          flex: "0 0 auto",
          maxWidth: 200,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        title={job.company ?? ""}
      >
        {job.company ?? "—"}
      </span>
      <span
        style={{
          minWidth: 0,
          flex: 1,
          color: "var(--fg-dim)",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        title={job.title ?? ""}
      >
        {job.title ?? "—"}
      </span>
      <span
        className="muted-sm"
        style={{
          flex: "0 0 auto",
          maxWidth: 160,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        title={job.location ?? ""}
      >
        {job.location ?? ""}
      </span>
      <span className="muted-sm" style={{ flex: "0 0 auto" }}>
        last seen {job.last_seen}
      </span>
    </div>
  );
}

export function DisappearedList({ jobs }: { jobs: DisappearedJob[] }) {
  return (
    <div className="explore-disappeared">
      {jobs.map((j) => (
        <DisappearedRow key={j.url_b64} job={j} />
      ))}
      {jobs.length === 0 && (
        <p className="muted" style={{ padding: 12 }}>
          Nothing disappeared for this comparison.
        </p>
      )}
    </div>
  );
}
