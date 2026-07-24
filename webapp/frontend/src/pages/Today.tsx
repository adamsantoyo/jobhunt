import { useMemo, useRef, type CSSProperties } from "react";
import { useSearchParams } from "react-router-dom";
import { useActivity, useConfig, useJobs } from "../store/queries";
import { composeQueue } from "../lib/compare";
import { PaceHeader } from "../components/today/PaceHeader";
import { FollowupsStrip } from "../components/today/FollowupsStrip";
import { TodayCard } from "../components/today/TodayCard";
import type { JobLight } from "../api/types";

// The execution loop: "what do I do right now?" — pace, then follow-ups that
// need a nudge, then a finishable do-today queue. Done-today comes ONLY from
// /api/activity (no more client-side actedToday guessing from state fields).

function progressFillStyle(pct: number): CSSProperties {
  return { width: `${pct}%` };
}

export default function Today() {
  const { data: jobsResp, isLoading, isError } = useJobs();
  const { data: config } = useConfig();
  const { data: activity } = useActivity();
  const [, setParams] = useSearchParams();

  const jobs = useMemo(() => jobsResp?.jobs ?? [], [jobsResp]);
  const cap = config?.daily_queue_size ?? 10;
  const done = activity?.today.done ?? 0;
  // The queue is the REMAINING daily contract: work already done today shrinks it,
  // so with more eligible jobs than the cap it still finishes at `cap` actions
  // instead of refilling forever.
  const queue = useMemo(
    () => composeQueue(jobs, Math.max(0, cap - done)),
    [jobs, cap, done],
  );
  // Acting on the last card empties the queue via the optimistic jobs cache
  // before /api/activity refetches, so `done` can lag at 0 for a beat. Remember
  // that this session had a queue, so that beat reads as "queue clear", not as
  // the opposite "nothing eligible" message.
  const sawQueueRef = useRef(false);
  if (queue.length > 0) sawQueueRef.current = true;
  const cappedDone = Math.min(done, cap);
  const pct = cap > 0 ? Math.round((cappedDone / cap) * 100) : 0;

  const openJob = (job: JobLight) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set("job", job.url_b64);
        return next;
      },
      { replace: false },
    );

  return (
    <div className="td-page">
      <div>
        <h1>Today</h1>
        <p className="muted-sm td-subtitle">
          What to act on right now — one honest bridge from interested to submitted.
        </p>
      </div>

      <PaceHeader />
      <FollowupsStrip onOpen={openJob} />

      {isError && <div className="page-error">Failed to load jobs.</div>}
      {isLoading && <p className="muted">Loading…</p>}

      {!isLoading && !isError && (
        <section>
          <div className="td-queue-head">
            <h2>Today's queue</h2>
            <div className="td-progress-wrap">
              <span className="td-progress-fill" style={progressFillStyle(pct)} />
            </div>
            <span className="muted-sm">
              {cappedDone}/{cap}
            </span>
          </div>

          {queue.length === 0 ? (
            <div className="td-empty">
              {done > 0
                ? `Queue clear — ${done} submitted today ✓`
                : sawQueueRef.current
                  ? "Queue clear ✓"
                  : "Nothing eligible — lower the bar in Explore or run a sweep."}
            </div>
          ) : (
            <div className="td-list">
              {queue.map((job) => (
                <TodayCard key={job.url_b64} job={job} onOpen={openJob} />
              ))}
            </div>
          )}
        </section>
      )}
    </div>
  );
}
