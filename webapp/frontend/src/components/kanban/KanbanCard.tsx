import { useDraggable } from "@dnd-kit/core";
import type { JobLight } from "../../api/types";
import { OddsBadge, TierBadge } from "../StatusBadge";
import { fmtDate, todayISO } from "../../lib/format";

// Statuses on which a past-due follow-up should shout at you.
const ACTIVE_FOLLOWUP = new Set(["Applied", "Phone screen", "Interview"]);

/** True when this card's status is Applied or further along the pipeline. */
export function isAppliedPlus(status: string, order: string[]): boolean {
  const ai = order.indexOf("Applied");
  const si = order.indexOf(status);
  return ai >= 0 && si >= ai;
}

function isOverdue(job: JobLight, status: string): boolean {
  const f = job.state?.follow_up_date;
  return !!f && ACTIVE_FOLLOWUP.has(status) && f < todayISO();
}

/** Inner content of a card — shared by the column list and the drag overlay. */
function CardInner({
  job,
  status,
  order,
}: {
  job: JobLight;
  status: string;
  order: string[];
}) {
  const appliedPlus = isAppliedPlus(status, order);
  const applied = job.state?.applied_date;
  const followUp = job.state?.follow_up_date;
  const overdue = isOverdue(job, status);

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
      <CardInner job={job} status={status} order={order} />
    </div>
  );
}
