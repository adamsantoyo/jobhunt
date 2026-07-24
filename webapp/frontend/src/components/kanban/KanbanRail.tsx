import { useDroppable } from "@dnd-kit/core";

/**
 * Collapsed rail for a terminal status (Rejected/Passed): a narrow strip
 * showing the status name + count. Still a valid drop target while collapsed
 * — dropping a card here reassigns its status same as an expanded column.
 */
export function KanbanRail({
  status,
  count,
  onExpand,
}: {
  status: string;
  count: number;
  onExpand: () => void;
}) {
  const { setNodeRef, isOver } = useDroppable({ id: status });

  return (
    <button
      type="button"
      ref={setNodeRef}
      className="kb-rail"
      data-over={isOver ? "1" : undefined}
      onClick={onExpand}
      title={`${status} (${count}) — click to expand, or drop a card here`}
    >
      <span className="kb-rail-count">{count}</span>
      <span className="kb-rail-label">{status}</span>
    </button>
  );
}
