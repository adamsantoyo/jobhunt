import { useDroppable } from "@dnd-kit/core";
import type { JobLight } from "../../api/types";
import { DraggableCard } from "./KanbanCard";

export function KanbanColumn({
  status,
  jobs,
  order,
  onOpen,
  onCollapse,
}: {
  status: string;
  jobs: JobLight[];
  order: string[];
  onOpen: (job: JobLight) => void;
  /** Present only for an expanded rail column — renders a collapse control. */
  onCollapse?: () => void;
}) {
  const { setNodeRef, isOver } = useDroppable({ id: status });

  return (
    <section className="kb-col" data-over={isOver ? "1" : undefined} aria-label={status}>
      <header className="kb-col-head">
        {onCollapse && (
          <button
            type="button"
            className="kb-col-collapse"
            title={`Collapse ${status} back into a rail`}
            onClick={onCollapse}
          >
            ›
          </button>
        )}
        <span className="kb-col-title">{status}</span>
        <span className="kb-col-count">{jobs.length}</span>
      </header>
      <div ref={setNodeRef} className="kb-col-body">
        {jobs.map((job) => (
          <DraggableCard
            key={job.url_b64}
            job={job}
            status={status}
            order={order}
            onOpen={onOpen}
          />
        ))}
        {jobs.length === 0 && <div className="kb-empty">—</div>}
      </div>
    </section>
  );
}
