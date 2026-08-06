import { useMemo, useRef, type CSSProperties } from "react";
import { useSearchParams } from "react-router-dom";
import { useActivity, useConfig, useQueueToday } from "../store/queries";
import { PaceHeader } from "../components/today/PaceHeader";
import { FollowupsStrip } from "../components/today/FollowupsStrip";
import { TodayCard } from "../components/today/TodayCard";
import type { JobLight } from "../api/types";

// The execution loop: "what do I do right now?" — pace, then follow-ups that
// need a nudge, then a finishable do-today queue. Done-today comes ONLY from
// /api/activity (no more client-side actedToday guessing from state fields).
// The queue itself (5.5) comes from GET /api/queue/today -- the server-side
// ranker (backend/ranking.py) replaces the old client-side `composeQueue`;
// this page's job is just to compute the remaining-contract cap and render.

function progressFillStyle(pct: number): CSSProperties {
  return { width: `${pct}%` };
}

export default function Today() {
  const { data: config, isSuccess: configReady, isError: configError } = useConfig();
  const { data: activity, isSuccess: activityReady, isError: activityError } = useActivity();
  const [, setParams] = useSearchParams();

  const cap = config?.daily_queue_size ?? 10;
  const done = activity?.today.done ?? 0;
  // The queue is the REMAINING daily contract: work already done today shrinks
  // it, so with more eligible jobs than the cap it still finishes at `cap`
  // actions instead of refilling forever. This is the same number
  // `composeQueue` used to receive as its `cap` argument -- subtracting
  // done-today stays a client concern (per the 5.1 contract) because
  // done-today comes from /api/activity, which the client already holds.
  // Clamped to 100 client-side (5.5 fix F2) -- the server-recomputed daily
  // snapshot caps the SERVED queue there too (5.5 contract B3), so a cap
  // above it would just fetch and discard.
  const remainingCap = Math.min(100, Math.max(0, cap - done));
  // Real config/activity must resolve before the request fires -- otherwise
  // the very first render fires the query against the `?? 10`/`?? 0`
  // fallback guesses, and the settle onto real data changes the cap-keyed
  // query key and (pre-F2) collapsed the section back to Loading. Once
  // done>=cap, remainingCap is 0 and the request is skipped entirely: that
  // state renders the existing queue-clear copy below without a round-trip.
  const queueReady = configReady && activityReady;
  const { data: queueResp, isLoading: queueLoading, isError: queueError } = useQueueToday(remainingCap, {
    enabled: queueReady && remainingCap > 0,
  });
  // A config/activity failure must surface as the error banner, not gate the
  // queue request forever behind a permanent spinner.
  const isError = configError || activityError || queueError;
  const isLoading = !isError && (!queueReady || (remainingCap > 0 && queueLoading));
  const queue = useMemo(() => queueResp?.queue ?? [], [queueResp]);
  const snapshotId = queueResp?.snapshot_id ?? null;

  // Acting on the last card empties the queue via the optimistic queue cache
  // (store/queries.ts's `removeFromQueueCaches`) before /api/activity
  // refetches, so `done` can lag at 0 for a beat. Remember that this session
  // had a queue, so that beat reads as "queue clear", not as the opposite
  // "nothing eligible" message.
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

      {isError && <div className="page-error">Failed to load the queue.</div>}
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
              {queue.map((entry) => (
                <TodayCard
                  key={entry.job.url_b64}
                  job={entry.job}
                  onOpen={openJob}
                  snapshotId={snapshotId}
                />
              ))}
            </div>
          )}
        </section>
      )}
    </div>
  );
}
