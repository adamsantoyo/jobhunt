// The ONE status vocabulary. Backend mirror: webapp/backend/config.py STATUSES /
// ACTIVE_STATUSES. Do not fork copies of these lists into pages/components.
import type { JobLight } from "../api/types";

/** Full status set, in pipeline order (matches backend config.STATUSES). */
export const DEFAULT_STATUSES = [
  "New",
  "Interested",
  "Applied",
  "Phone screen",
  "Interview",
  "Offer",
  "Rejected",
  "Passed",
];

/** Kanban board columns — no New, no terminal rails. */
export const PIPELINE_STATUSES = ["Interested", "Applied", "Phone screen", "Interview", "Offer"];

/** Collapsed side rails on the Kanban board. */
export const RAIL_STATUSES = ["Rejected", "Passed"];

/** Statuses considered "in flight" for follow-up / ghosted affordances. */
export const ACTIVE_STATUSES = ["Applied", "Phone screen", "Interview"];

/** A job's effective status: untriaged jobs have no state row yet. */
export function statusOf(job: JobLight): string {
  return job.state?.status ?? "New";
}

/** True when `status` is Applied or further along the given column order. */
export function isAppliedPlus(status: string, order: string[]): boolean {
  const ai = order.indexOf("Applied");
  const si = order.indexOf(status);
  return ai >= 0 && si >= ai;
}
