import { useState } from "react";
import { useDroppable } from "@dnd-kit/core";
import type { JobLight } from "../../api/types";
import { DraggableCard } from "./KanbanCard";

const NEW_CAP = 50;

export function KanbanColumn({
  status,
  jobs,
  order,
  onOpen,
}: {
  status: string;
  jobs: JobLight[];
  order: string[];
  onOpen: (job: JobLight) => void;
}) {
  const { setNodeRef, isOver } = useDroppable({ id: status });
  const [showAll, setShowAll] = useState(false);

  // "New" is a backlog: render a cap with a show-more affordance.
  const capped = status === "New" && !showAll;
  const visible = capped ? jobs.slice(0, NEW_CAP) : jobs;
  const hiddenCount = jobs.length - visible.length;

  return (
    <section className="kb-col" data-over={isOver ? "1" : undefined} aria-label={status}>
      <header className="kb-col-head">
        <span className="kb-col-title">{status}</span>
        <span className="kb-col-count">{jobs.length}</span>
      </header>
      <div ref={setNodeRef} className="kb-col-body">
        {visible.map((job) => (
          <DraggableCard
            key={job.url_b64}
            job={job}
            status={status}
            order={order}
            onOpen={onOpen}
          />
        ))}
        {jobs.length === 0 && <div className="kb-empty">—</div>}
        {capped && hiddenCount > 0 && (
          <button
            type="button"
            className="btn btn-sm kb-more"
            onClick={() => setShowAll(true)}
          >
            Show {hiddenCount} more
          </button>
        )}
        {status === "New" && showAll && jobs.length > NEW_CAP && (
          <button
            type="button"
            className="btn btn-sm kb-more"
            onClick={() => setShowAll(false)}
          >
            Show less
          </button>
        )}
      </div>
    </section>
  );
}
