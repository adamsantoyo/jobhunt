import { TierBadge } from "../StatusBadge";
import type { DisappearedJob } from "../../api/types";

// Read-only rows for diff=gone. Disappeared jobs are not present in /api/jobs
// (they dropped off the tracker), so there is no drawer to open and no
// selection/bulk affordance for them.

function DisappearedRow({ job }: { job: DisappearedJob }) {
  return (
    <div className="ex-gone-row">
      <span className="ex-flex-none">
        <TierBadge tier={job.tier} />
      </span>
      <span
        className="ex-gone-company"
        title={job.company ?? ""}
      >
        {job.company ?? "—"}
      </span>
      <span
        className="ex-gone-title"
        title={job.title ?? ""}
      >
        {job.title ?? "—"}
      </span>
      <span
        className="muted-sm ex-gone-location"
        title={job.location ?? ""}
      >
        {job.location ?? ""}
      </span>
      <span className="muted-sm ex-flex-none">
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
        <p className="muted ex-empty-note">
          Nothing disappeared for this comparison.
        </p>
      )}
    </div>
  );
}
