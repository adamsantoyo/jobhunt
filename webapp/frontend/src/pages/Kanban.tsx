import { useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  DndContext,
  DragOverlay,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragStartEvent,
} from "@dnd-kit/core";
import { useJobs, usePatchState } from "../store/queries";
import { tierOddsScoreCmp } from "../lib/compare";
import { PIPELINE_STATUSES, RAIL_STATUSES, statusOf } from "../lib/statuses";
import type { JobLight } from "../api/types";
import { KanbanColumn } from "../components/kanban/KanbanColumn";
import { KanbanRail } from "../components/kanban/KanbanRail";
import { CardOverlay } from "../components/kanban/KanbanCard";
import "../components/kanban/kanban.css";

// Full column order for drop-target validation and the "applied or further"
// check on cards — pipeline columns left-to-right, then the terminal rails.
const BOARD_ORDER = [...PIPELINE_STATUSES, ...RAIL_STATUSES];

export default function Kanban() {
  const { data, isLoading, isError } = useJobs();
  const patchState = usePatchState();
  const [, setParams] = useSearchParams();

  const [activeId, setActiveId] = useState<string | null>(null);
  // Guards the click-to-open handler from firing after a real drag.
  const draggingRef = useRef(false);
  // Rails start collapsed; expanding one turns it into a normal droppable column.
  const [expandedRails, setExpandedRails] = useState<Set<string>>(new Set());

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor),
  );

  // Group present, non-hidden jobs by their pipeline status. Status-New jobs
  // (and anything outside the board's statuses) don't render here at all.
  const byStatus = useMemo(() => {
    const groups: Record<string, JobLight[]> = {};
    for (const s of BOARD_ORDER) groups[s] = [];
    for (const job of data?.jobs ?? []) {
      if (job.state?.hidden) continue; // hidden jobs stay off the board
      const s = statusOf(job);
      if (!groups[s]) continue;
      groups[s].push(job);
    }
    for (const s of Object.keys(groups)) groups[s].sort(tierOddsScoreCmp);
    return groups;
  }, [data]);

  const pipelineEmpty = PIPELINE_STATUSES.every((s) => (byStatus[s]?.length ?? 0) === 0);

  const activeJob = useMemo(
    () => (activeId ? (data?.jobs ?? []).find((j) => j.url_b64 === activeId) ?? null : null),
    [activeId, data],
  );

  const openJob = (job: JobLight) => {
    if (draggingRef.current) return; // suppress the click fired right after a drag
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set("job", job.url_b64);
        return next;
      },
      { replace: false },
    );
  };

  const toggleRail = (status: string) => {
    setExpandedRails((prev) => {
      const next = new Set(prev);
      if (next.has(status)) next.delete(status);
      else next.add(status);
      return next;
    });
  };

  const onDragStart = (e: DragStartEvent) => {
    draggingRef.current = true;
    setActiveId(String(e.active.id));
  };

  const finishDrag = () => {
    setActiveId(null);
    // Reset after the click event (which fires post-dragend) has been swallowed.
    setTimeout(() => {
      draggingRef.current = false;
    }, 0);
  };

  const onDragEnd = (e: DragEndEvent) => {
    const { active, over } = e;
    finishDrag();
    if (!over) return;
    const from = active.data.current?.status as string | undefined;
    const to = String(over.id);
    if (!to || to === from) return;
    if (!BOARD_ORDER.includes(to)) return;
    patchState.mutate({ urlB64: String(active.id), patch: { status: to } });
  };

  return (
    <div className="kb-page">
      <div className="kb-toolbar">
        <h1>Pipeline</h1>
        <span className="kb-hint">
          {isLoading ? "loading…" : "drag a card to change its status"}
        </span>
      </div>

      {isError && <div className="page-error">Failed to load jobs.</div>}

      <DndContext
        sensors={sensors}
        onDragStart={onDragStart}
        onDragEnd={onDragEnd}
        onDragCancel={finishDrag}
      >
        <div className="kb-board">
          {/* Columns stay mounted even when empty: they must remain drop targets
              so a Rejected/Passed card can be dragged back into the pipeline. */}
          <div className="kb-columns">
            {PIPELINE_STATUSES.map((status) => (
              <KanbanColumn
                key={status}
                status={status}
                jobs={byStatus[status] ?? []}
                order={BOARD_ORDER}
                onOpen={openJob}
              />
            ))}
            {pipelineEmpty && (
              <div className="kb-columns-hint">Nothing in flight yet — start from Today.</div>
            )}
          </div>
          <div className="kb-rails">
            {RAIL_STATUSES.map((status) =>
              expandedRails.has(status) ? (
                <KanbanColumn
                  key={status}
                  status={status}
                  jobs={byStatus[status] ?? []}
                  order={BOARD_ORDER}
                  onOpen={openJob}
                  onCollapse={() => toggleRail(status)}
                />
              ) : (
                <KanbanRail
                  key={status}
                  status={status}
                  count={(byStatus[status] ?? []).length}
                  onExpand={() => toggleRail(status)}
                />
              ),
            )}
          </div>
        </div>
        <DragOverlay>
          {activeJob ? (
            <CardOverlay job={activeJob} status={statusOf(activeJob)} order={BOARD_ORDER} />
          ) : null}
        </DragOverlay>
      </DndContext>
    </div>
  );
}
