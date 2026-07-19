import type { CSSProperties } from "react";
import { QueueRow } from "./QueueRow";
import type { JobLight } from "../../api/types";

// One lane of the two-lane Apply Queue (Bank a win / Aim high).

const laneStyle: CSSProperties = {
  flex: "1 1 380px",
  minWidth: 0,
  display: "flex",
  flexDirection: "column",
  background: "var(--bg-1)",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius)",
  overflow: "hidden",
};
const headStyle: CSSProperties = {
  display: "flex",
  alignItems: "baseline",
  justifyContent: "space-between",
  gap: 8,
  padding: "10px 12px",
  borderBottom: "1px solid var(--border)",
  background: "var(--bg-2)",
};
const titleStyle: CSSProperties = { display: "flex", alignItems: "baseline", gap: 8 };
const emptyStyle: CSSProperties = {
  padding: "20px 12px",
  color: "var(--fg-mute)",
  fontSize: 12,
};

export function ApplyLane({
  title,
  subtitle,
  accent,
  jobs,
  onOpen,
}: {
  title: string;
  subtitle: string;
  accent: string;
  jobs: JobLight[];
  onOpen: (job: JobLight) => void;
}) {
  return (
    <section style={laneStyle}>
      <header style={headStyle}>
        <div style={titleStyle}>
          <span style={{ width: 8, height: 8, borderRadius: 8, background: accent }} />
          <h2>{title}</h2>
          <span className="muted-sm">{subtitle}</span>
        </div>
        <span className="chip">{jobs.length}</span>
      </header>
      {jobs.length === 0 ? (
        <div style={emptyStyle}>Nothing to act on here right now.</div>
      ) : (
        <div>
          {jobs.map((job) => (
            <QueueRow key={job.url_b64} job={job} onOpen={onOpen} />
          ))}
        </div>
      )}
    </section>
  );
}
