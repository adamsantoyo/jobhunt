import { useDraggable } from "@dnd-kit/core";
import type { JobLight } from "../../api/types";
import { OddsBadge, TierBadge } from "../StatusBadge";
import { fmtDate, todayISO } from "../../lib/format";
import { isAppliedPlus } from "../../lib/statuses";

// Statuses on which a past-due follow-up should shout at you.
const ACTIVE_FOLLOWUP = new Set(["Applied", "Phone screen", "Interview"]);
// Days in a stage before the Applied chip tints amber / a ghosted affordance appears.
const STALE_DAYS = 14;

function isOverdue(job: JobLight, status: string): boolean {
  const f = job.state?.follow_up_date;
  return !!f && ACTIVE_FOLLOWUP.has(status) && f < todayISO();
}

/** Whole days elapsed since `status_since`; null (hidden) when there's no event yet.
 * Parsed field-by-field into a local Date: backfilled events carry a bare
 * 'YYYY-MM-DD', which `new Date(...)` would anchor at UTC midnight while the
 * backend (and every other date in this app) treats it as local midnight. */
function daysInStage(statusSince: string | null | undefined): number | null {
  if (!statusSince) return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?/.exec(statusSince);
  if (!m) return null;
  const then = new Date(
    Number(m[1]), Number(m[2]) - 1, Number(m[3]),
    Number(m[4] ?? 0), Number(m[5] ?? 0), Number(m[6] ?? 0),
  ).getTime();
  return Math.max(0, Math.floor((Date.now() - then) / 86_400_000));
}

/** In the Applied stage, unmoved, for 14+ days — likely ghosted. Anchored on the
 * current stint (status_since), not applied_date: the backend preserves the
 * original applied_date, so a role bounced back to Applied today would otherwise
 * read as both "0d in stage" and "ghosted?". */
function isGhosted(status: string, stageDays: number | null): boolean {
  return status === "Applied" && stageDays !== null && stageDays >= STALE_DAYS;
}

/** Inner content of a card — shared by the column list and the drag overlay. */
function CardInner({
  job,
  status,
  order,
  onOpen,
}: {
  job: JobLight;
  status: string;
  order: string[];
  onOpen?: (job: JobLight) => void;
}) {
  const appliedPlus = isAppliedPlus(status, order);
  const applied = job.state?.applied_date;
  const followUp = job.state?.follow_up_date;
  const overdue = isOverdue(job, status);
  const stageDays = daysInStage(job.state?.status_since);
  const ghosted = isGhosted(status, stageDays);

  return (
    <>
      <div className="kb-card-badges">
        <TierBadge tier={job.tier} />
        <OddsBadge odds={job.odds} />
        {overdue && (
          <span className="badge overdue" title="Follow-up overdue">
            overdue
          </span>
        )}
        {stageDays !== null && (
          <span
            className="badge stage"
            data-stale={status === "Applied" && stageDays >= STALE_DAYS ? "1" : undefined}
            title={`${stageDays}d in ${status}`}
          >
            {stageDays}d
          </span>
        )}
        {ghosted && (
          <span
            className="badge ghosted"
            title="No response in 14+ days — nudge or move on"
            onClick={(e) => {
              e.stopPropagation();
              onOpen?.(job);
            }}
          >
            ghosted?
          </span>
        )}
        {job.state?.starred && <span className="kb-card-star">★</span>}
      </div>
      <div className="kb-card-title">{job.title ?? "—"}</div>
      <div className="kb-card-company">{job.company ?? "—"}</div>
      {job.location && <div className="kb-card-loc">{job.location}</div>}
      {appliedPlus && (applied || followUp) && (
        <div className="kb-card-dates">
          {applied && (
            <span>
              <span className="kb-date-label">applied</span>
              {fmtDate(applied)}
            </span>
          )}
          {followUp && (
            <span>
              <span className="kb-date-label">follow-up</span>
              {fmtDate(followUp)}
            </span>
          )}
        </div>
      )}
    </>
  );
}

/** Static card used inside the DragOverlay while dragging. */
export function CardOverlay({
  job,
  status,
  order,
}: {
  job: JobLight;
  status: string;
  order: string[];
}) {
  return (
    <div className="kb-card" data-overlay="1">
      <CardInner job={job} status={status} order={order} />
    </div>
  );
}

/** Draggable card used inside columns. */
export function DraggableCard({
  job,
  status,
  order,
  onOpen,
}: {
  job: JobLight;
  status: string;
  order: string[];
  onOpen: (job: JobLight) => void;
}) {
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
    id: job.url_b64,
    data: { status },
  });

  return (
    <div
      ref={setNodeRef}
      className="kb-card"
      data-dragging={isDragging ? "1" : undefined}
      {...attributes}
      {...listeners}
      role="button"
      tabIndex={0}
      title={`${job.company ?? ""} — ${job.title ?? ""}`}
      onClick={() => onOpen(job)}
      onKeyDown={(e) => {
        // Enter opens the drawer; Space is reserved by dnd-kit to lift the card.
        if (e.key === "Enter") {
          e.preventDefault();
          onOpen(job);
        }
      }}
    >
      <CardInner job={job} status={status} order={order} onOpen={onOpen} />
    </div>
  );
}
