import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { api } from "../api/client";
import type { SweepEvent } from "../api/types";

interface SweepUiState {
  active: boolean;
  kind: string | null;
  step: string | null;
  done: number;
  total: number;
  message: string | null;
  lastLine: string | null;
  error: string | null;
}

const INITIAL: SweepUiState = {
  active: false,
  kind: null,
  step: null,
  done: 0,
  total: 0,
  message: null,
  lastLine: null,
  error: null,
};

// Alt-tabbing through a stack of windows must not churn socket setup per toggle.
const VISIBILITY_DEBOUNCE_MS = 300;
// Floor between a `bye` and the reopen it triggers: a server that recycles us
// immediately must not spin this into an open/close loop.
const MIN_STREAM_MS = 1000;
// EventSource fires onerror once per failed reconnect. One is routine (a recycle, a
// transient drop); a run of them means the backend is actually gone.
const FAILURES_BEFORE_INACTIVE = 3;

/**
 * Progress strip driven by an EventSource on /api/sweep/progress. Renders only
 * while a sweep is active. On every run completion it learns about, it invalidates
 * all queries so the freshly ingested data shows up everywhere.
 *
 * The stream is opened only while the tab is visible and closed as soon as it is
 * hidden. Browsers cap HTTP/1.1 connections at ~6 per origin, and an SSE stream
 * holds its socket open indefinitely, so a handful of background tabs parked on
 * the app used to exhaust the whole pool for that origin -- every later request
 * (page loads included) then stalled without ever reaching the server. The server
 * recycles long-lived streams for the same reason.
 *
 * Nothing is lost to any of that, and it takes no replay protocol: every stream
 * opens with a `sync` frame carrying live runner state plus a per-process count of
 * completed runs. Comparing that count against the last one we saw is how a tab
 * that was hidden for an entire sweep learns the sweep ended -- and why a fresh
 * page load, which has no previous count, is never shown a finished run's strip.
 */
export function SweepProgress() {
  const qc = useQueryClient();
  const [state, setState] = useState<SweepUiState>(INITIAL);

  useEffect(() => {
    let closed = false;
    let es: EventSource | null = null;
    let failures = 0;
    let openedAt = 0;
    let visTimer: number | undefined;
    let reopenTimer: number | undefined;
    // Last (process, completed-run count) this tab has seen. Effect-local on
    // purpose: a real page reload starts with no baseline, so its first frame can
    // never look like a completion -- it just fetched everything anyway.
    let seen: { boot: string; finished: number } | null = null;

    /** Fold a frame's run counters in. True when it reveals a run that finished
     *  since we last looked (hidden tab, reconnect gap, deliberate recycle). */
    const observe = (e: SweepEvent): boolean => {
      if (!e.boot || e.finished == null) return false;
      const prev = seen;
      seen = { boot: e.boot, finished: e.finished };
      // No baseline, or a restarted server: counters are not comparable across
      // processes, so adopt quietly rather than invent a completion.
      if (!prev || prev.boot !== e.boot) return false;
      return e.finished > prev.finished;
    };

    const closeStream = () => {
      window.clearTimeout(reopenTimer);
      const src = es;
      if (!src) return;
      es = null;
      src.close();
    };

    const openStream = () => {
      if (closed || es || document.visibilityState !== "visible") return;
      const src = new EventSource("/api/sweep/progress");
      es = src;
      openedAt = Date.now();

      src.onopen = () => {
        failures = 0;
      };

      src.onmessage = (evt) => {
        // A handler left over from a replaced socket must not write state.
        if (closed || es !== src) return;
        let data: SweepEvent;
        try {
          data = JSON.parse(evt.data) as SweepEvent;
        } catch {
          return;
        }
        if (data.type === "bye") {
          // Deliberate recycle (lifetime cap, or our queue backed up). Reopen at
          // once so there is no gap and no onerror -- with a floor, so a server
          // that byes us instantly cannot spin us.
          const wait = Math.max(0, MIN_STREAM_MS - (Date.now() - openedAt));
          closeStream();
          reopenTimer = window.setTimeout(openStream, wait);
          return;
        }
        const missed = observe(data);
        setState((prev) => reduce(prev, data, missed));
        // Exactly one refetch per run completion we learn about, whether the `done`
        // arrived live or a sync frame told us we missed it.
        if (missed) qc.invalidateQueries();
      };

      src.onerror = () => {
        if (closed || es !== src) return;
        // EventSource retries on its own and onopen resets this, so a recycle or a
        // single hiccup never blinks a running sweep's strip out; only a run of
        // failed reconnects means the backend is really gone.
        if (++failures >= FAILURES_BEFORE_INACTIVE) {
          setState((prev) => (prev.active ? { ...prev, active: false } : prev));
        }
      };
    };

    // Hold a socket only while the tab is actually being looked at.
    const apply = () => {
      if (document.visibilityState === "visible") openStream();
      else closeStream();
    };
    const onVisibility = () => {
      window.clearTimeout(visTimer);
      visTimer = window.setTimeout(apply, VISIBILITY_DEBOUNCE_MS);
    };

    apply(); // first paint is immediate; only later toggles are debounced
    document.addEventListener("visibilitychange", onVisibility);

    return () => {
      closed = true;
      window.clearTimeout(visTimer);
      document.removeEventListener("visibilitychange", onVisibility);
      closeStream();
    };
  }, [qc]);

  if (!state.active && !state.error) return null;

  const pct = state.total > 0 ? Math.min(100, Math.round((state.done / state.total) * 100)) : 0;

  return (
    <div className="sweep-strip" data-error={state.error ? "1" : "0"}>
      <span className="sweep-kind">{state.kind === "full" ? "Full sweep" : "Quick refresh"}</span>
      {state.error ? (
        <span className="sweep-step sweep-error">{state.error}</span>
      ) : (
        <>
          <span className="sweep-step">{state.step ?? state.message ?? "working…"}</span>
          {state.total > 0 && (
            <span className="sweep-count">
              {state.done}/{state.total}
            </span>
          )}
          <span className="sweep-bar">
            <span className="sweep-bar-fill" style={{ width: `${pct}%` }} />
          </span>
        </>
      )}
      {state.lastLine && <span className="sweep-line" title={state.lastLine}>{state.lastLine}</span>}
      <button
        type="button"
        className="btn btn-sm sweep-cancel"
        onClick={() => {
          if (state.error) {
            setState(INITIAL);
          } else {
            void api.sweepCancel().catch(() => undefined);
          }
        }}
      >
        {state.error ? "Dismiss" : "Cancel"}
      </button>
    </div>
  );
}

/** `missed` is true when this frame revealed a run that completed while we were not
 *  listening; only then may a sync frame surface that run's failure. */
function reduce(prev: SweepUiState, e: SweepEvent, missed = false): SweepUiState {
  switch (e.type) {
    case "sync":
      // Per-subscriber catch-up built on attach, so it is a hard SET, not a merge:
      // it rebases us after a hidden tab, a recycle, or a reconnect gap.
      if (e.running) {
        return {
          ...INITIAL,
          active: true,
          kind: e.kind ?? null,
          step: e.step ?? null,
          done: e.done ?? 0,
          total: e.total ?? 0,
          lastLine: e.line ?? null,
        };
      }
      // Idle. Surface a failure we missed while away; otherwise drop a strip left
      // running by a sweep that ended behind our back. An error the user has not
      // dismissed yet (active:false) is left alone.
      if (missed && e.last_error) return { ...INITIAL, kind: e.kind ?? null, error: e.last_error };
      return prev.active ? INITIAL : prev;
    case "start":
      return { ...INITIAL, active: true, kind: e.kind ?? null };
    case "step":
      return {
        ...prev,
        active: true,
        kind: e.kind ?? prev.kind,
        step: e.step ?? prev.step,
        done: e.done ?? prev.done,
        total: e.total ?? prev.total,
      };
    case "skipped":
      return { ...prev, active: true, lastLine: `skipped: ${e.step ?? ""}` };
    case "log":
      return { ...prev, lastLine: e.line ?? prev.lastLine };
    case "ingested":
      return { ...prev, message: e.message ?? "ingested" };
    case "done":
      return { ...INITIAL };
    case "error":
      return { ...prev, active: false, error: e.message ?? "sweep failed" };
    default:
      return prev;
  }
}
