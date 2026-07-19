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
import { useConfig, useJobs, usePatchState } from "../store/queries";
import { oddsRank } from "../lib/format";
import type { JobLight } from "../api/types";
import { KanbanColumn } from "../components/kanban/KanbanColumn";
import { CardOverlay } from "../components/kanban/KanbanCard";
import "../components/kanban/kanban.css";

// Fixed column order (matches config STATUSES). New is the untriaged backlog.
const DEFAULT_ORDER = [
  "New",
  "Interested",
  "Applied",
  "Phone screen",
  "Interview",
  "Offer",
  "Rejected",
  "Passed",
];

function statusOf(job: JobLight): string {
  return job.state?.status ?? "New";
}

// Within a column, strongest fits first: tier desc, odds rank asc, score desc.
function cardSort(a: JobLight, b: JobLight): number {
  if (a.tier !== b.tier) return b.tier - a.tier;
  const or = oddsRank(a.odds) - oddsRank(b.odds);
  if (or !== 0) return or;
  return (b.odds_score ?? -Infinity) - (a.odds_score ?? -Infinity);
}

export default function Kanban() {
  const { data, isLoading, isError } = useJobs();
  const { data: config } = useConfig();
  const patchState = usePatchState();
  const [, setParams] = useSearchParams();

  const [activeId, setActiveId] = useState<string | null>(null);
  // Guards the click-to-open handler from firing after a real drag.
  const draggingRef = useRef(false);

  const order = config?.statuses ?? DEFAULT_ORDER;

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor),
  );

  // Group present, non-hidden jobs by their pipeline status.
  const byStatus = useMemo(() => {
    const groups: Record<string, JobLight[]> = {};
    for (const s of order) groups[s] = [];
    for (const job of data?.jobs ?? []) {
      if (job.state?.hidden) continue; // hidden jobs stay off the board
      const s = statusOf(job);
      (groups[s] ??= []).push(job);
    }
    for (const s of Object.keys(groups)) groups[s].sort(cardSort);
    return groups;
  }, [data, order]);

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
    if (!order.includes(to)) return;
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
          {order.map((status) => (
            <KanbanColumn
              key={status}
              status={status}
              jobs={byStatus[status] ?? []}
              order={order}
              onOpen={openJob}
            />
          ))}
        </div>
        <DragOverlay>
          {activeJob ? (
            <CardOverlay job={activeJob} status={statusOf(activeJob)} order={order} />
          ) : null}
        </DragOverlay>
      </DndContext>
    </div>
  );
}
