import { useState } from "react";
import { useFollowups } from "../../store/queries";
import { fmtDate } from "../../lib/format";
import { statusOf } from "../../lib/statuses";
import { StatusBadge } from "../StatusBadge";
import { FOLLOWUPS_CAP } from "../../lib/ui";
import type { JobLight } from "../../api/types";

// Overdue-then-upcoming follow-up rows. Hidden entirely when empty; capped to
// FOLLOWUPS_CAP visible rows with a "+N more" expander.

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
    <button type="button" className="td-followups-row" onClick={() => onOpen(job)}>
      <span className="td-strong">{job.company ?? "—"}</span>
      <span className="td-sep">·</span>
      <span className="td-followups-title">{job.title ?? "—"}</span>
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
  const visible = expandedAll ? combined : combined.slice(0, FOLLOWUPS_CAP);
  const hiddenCount = combined.length - visible.length;

  let lastGroup: "overdue" | "upcoming" | null = null;

  return (
    <div className="td-followups-wrap">
      {visible.map(({ job, group }) => {
        const showHead = group !== lastGroup;
        lastGroup = group;
        return (
          <div key={job.url_b64}>
            {showHead && <div className="td-followups-group-head">{group === "overdue" ? "Overdue" : "Upcoming"}</div>}
            <Row job={job} onOpen={onOpen} accent={group === "overdue" ? "var(--red)" : undefined} />
          </div>
        );
      })}
      {hiddenCount > 0 && (
        <button
          type="button"
          className="btn btn-sm td-followups-more"
          onClick={() => setExpandedAll(true)}
        >
          +{hiddenCount} more
        </button>
      )}
    </div>
  );
}
