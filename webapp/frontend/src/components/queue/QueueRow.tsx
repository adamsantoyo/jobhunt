import type { CSSProperties } from "react";
import { useQuickAction } from "../../store/queries";
import { fmtSalary } from "../../lib/format";
import { OddsBadge, TierBadge } from "../StatusBadge";
import type { JobLight } from "../../api/types";

// One row of the Apply Queue. Owns its own quick-action mutation so acting on a
// row optimistically drops it out of the actionable set (lanes recompute).
// Opening the drawer is delegated to the page via onOpen (?job=<url_b64>).

const rowStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  padding: "6px 8px",
  borderBottom: "1px solid var(--border-soft)",
};
const mainStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  flex: 1,
  minWidth: 0,
  background: "transparent",
  border: "none",
  color: "var(--fg)",
  font: "inherit",
  textAlign: "left",
  cursor: "pointer",
  padding: 0,
};
const badgesStyle: CSSProperties = { display: "flex", gap: 3, flex: "0 0 auto" };
const textCol: CSSProperties = { display: "flex", flexDirection: "column", minWidth: 0, gap: 1 };
const line1: CSSProperties = {
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
};
const line2: CSSProperties = {
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  color: "var(--fg-mute)",
  fontSize: 11,
};
const actionsStyle: CSSProperties = { display: "flex", gap: 5, flex: "0 0 auto" };

export function QueueRow({
  job,
  onOpen,
}: {
  job: JobLight;
  onOpen: (job: JobLight) => void;
}) {
  const quick = useQuickAction();
  const pending = quick.isPending;
  const salary = fmtSalary(job);

  return (
    <div style={rowStyle}>
      <button type="button" style={mainStyle} onClick={() => onOpen(job)} title="Open details">
        <span style={badgesStyle}>
          <TierBadge tier={job.tier} />
          <OddsBadge odds={job.odds} />
        </span>
        <span style={textCol}>
          <span style={line1}>
            <span style={{ fontWeight: 600 }}>{job.company ?? "—"}</span>
            <span style={{ color: "var(--fg-faint)" }}> · </span>
            <span style={{ color: "var(--fg-dim)" }}>{job.title ?? "—"}</span>
          </span>
          <span style={line2}>
            {job.remote && <span className="tag-remote">R</span>}
            {job.location || "location n/a"}
            {salary ? ` · ${salary}` : ""}
          </span>
        </span>
      </button>
      <div style={actionsStyle}>
        <button
          type="button"
          className="btn btn-sm btn-primary"
          disabled={pending}
          onClick={() => quick.mutate({ urlB64: job.url_b64, body: { action: "applied" } })}
          title="Mark applied"
        >
          Applied
        </button>
        <button
          type="button"
          className="btn btn-sm"
          disabled={pending}
          onClick={() => quick.mutate({ urlB64: job.url_b64, body: { action: "snooze", days: 3 } })}
          title="Snooze 3 days"
        >
          Snooze
        </button>
        <button
          type="button"
          className="btn btn-sm"
          disabled={pending}
          onClick={() => quick.mutate({ urlB64: job.url_b64, body: { action: "pass" } })}
          title="Pass"
        >
          Pass
        </button>
      </div>
    </div>
  );
}
