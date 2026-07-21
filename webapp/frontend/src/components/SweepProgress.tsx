import { useEffect, useRef, useState } from "react";
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

/**
 * Progress strip driven by an EventSource on /api/sweep/progress. Renders only
 * while a sweep is active. On `done`/`error` it invalidates all queries so the
 * freshly ingested data shows up everywhere.
 *
 * The stream is opened only while the tab is visible and closed as soon as it is
 * hidden. Browsers cap HTTP/1.1 connections at ~6 per origin, and an SSE stream
 * holds its socket open indefinitely, so a handful of background tabs parked on
 * the app used to exhaust the whole pool for that origin -- every later request
 * (page loads included) then stalled without ever reaching the server. Nothing is
 * lost by disconnecting: the endpoint replays a snapshot of the active run on
 * subscribe, so a re-shown tab catches straight back up.
 */
export function SweepProgress() {
  const qc = useQueryClient();
  const [state, setState] = useState<SweepUiState>(INITIAL);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    let closed = false;

    const openStream = () => {
      if (closed || esRef.current) return;
      const es = new EventSource("/api/sweep/progress");
      esRef.current = es;

      es.onmessage = (evt) => {
        if (closed) return;
        let data: SweepEvent;
        try {
          data = JSON.parse(evt.data) as SweepEvent;
        } catch {
          return;
        }
        setState((prev) => reduce(prev, data));
        if (data.type === "done" || data.type === "error") {
          // New data landed (or the run ended); refresh everything.
          qc.invalidateQueries();
        }
      };

      es.onerror = () => {
        // Endpoint unavailable (e.g. backend not up yet). EventSource auto-retries;
        // just stop showing an active strip.
        if (closed) return;
        setState((prev) => (prev.active ? { ...prev, active: false } : prev));
      };
    };

    const closeStream = () => {
      const es = esRef.current;
      if (!es) return;
      esRef.current = null;
      es.close();
    };

    // Hold a socket only while the tab is actually being looked at.
    const syncToVisibility = () => {
      if (document.visibilityState === "visible") openStream();
      else closeStream();
    };

    syncToVisibility();
    document.addEventListener("visibilitychange", syncToVisibility);

    return () => {
      closed = true;
      document.removeEventListener("visibilitychange", syncToVisibility);
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

function reduce(prev: SweepUiState, e: SweepEvent): SweepUiState {
  switch (e.type) {
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
