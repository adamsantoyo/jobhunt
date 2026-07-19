import type { JobLight } from "../api/types";
import { OddsBadge, TierBadge } from "./StatusBadge";

// Compact clickable chip: "company · title" (truncated) with tier/odds badges.
// Used by Matrix cells and other dense lists; onOpen receives the job so the
// caller can open the drawer (?job=<url_b64>).
export function JobChip({
  job,
  onOpen,
  showBadges = true,
}: {
  job: JobLight;
  onOpen: (job: JobLight) => void;
  showBadges?: boolean;
}) {
  return (
    <button
      type="button"
      className="job-chip"
      onClick={() => onOpen(job)}
      title={`${job.company ?? ""} — ${job.title ?? ""}`}
    >
      {showBadges && (
        <span className="job-chip-badges">
          <TierBadge tier={job.tier} />
          <OddsBadge odds={job.odds} />
        </span>
      )}
      <span className="job-chip-text">
        <span className="job-chip-company">{job.company ?? "—"}</span>
        <span className="job-chip-sep"> · </span>
        <span className="job-chip-title">{job.title ?? "—"}</span>
      </span>
      {job.state?.starred && <span className="job-chip-star">★</span>}
    </button>
  );
}
