import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useCancelRun, useRunDetail } from "../store/queries";
import type { RunDetail, RunEventFrame, RunSettledPayload, SourceRunRow } from "../api/types";

// Live strip for one canonical run (Phase 4.3), replacing the legacy sweep
// strip in canonical mode. Subscribes to GET /api/runs/{run_uid}/events and
// folds the persisted event vocabulary from runservice.py / scheduler.py /
// graph.py / scoring.py into per-source rows, two stage rows, and a settled
// summary. Every payload field rendered here is scheduler/adapter-derived
// text (decision 10, phase4-spec.md) -- it is placed only in JSX text
// children or `title` attributes, never through dangerouslySetInnerHTML or
// string concatenation into markup.

// -- event vocabulary (runservice.py / scheduler.py / graph.py / scoring.py) -- //
const EVENT_ENRICHMENT_STARTED = "stage.enrichment.started";
const EVENT_ENRICHMENT_FINISHED = "stage.enrichment.finished";
const EVENT_ENRICHMENT_FAILED = "stage.enrichment.failed";
const EVENT_ENRICHMENT_CANCELLED = "stage.enrichment.cancelled";
const EVENT_SCORING_STARTED = "stage.scoring.started";
const EVENT_SCORING_FINISHED = "stage.scoring.finished";
const EVENT_SCORING_FAILED = "stage.scoring.failed";
const EVENT_SCORING_CANCELLED = "stage.scoring.cancelled";
const EVENT_STAGES_SKIPPED = "service.stages.skipped";
const EVENT_RUN_SETTLED = "service.run.settled";
const EVENT_SCORE_BATCH = "score.batch_scored";
const RUN_FETCH_DONE = /^run\.(succeeded|partial|failed|cancelled)$/;
const SOURCE_TERMINAL = /^source\.(succeeded|failed|timeout|cancelled)$/;

type SourceChip = "running" | "succeeded" | "failed" | "timeout" | "cancelled" | "retry";
type StageStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled" | "skipped";

interface SourceRow {
  source: string;
  attempt: number;
  chip: SourceChip;
  deadlineSeconds: number | null;
  startedAtMs: number | null;
  finishedAtMs: number | null;
  fetched: number | null;
  errorMessage: string | null;
  note: string | null;
}

interface StageState {
  status: StageStatus;
  detail?: string;
}

/** One entry of the fetch report's `skipped_not_due` list (scheduler.py's
 * `_run_report`): a target the DAILY-dueness preflight skipped before it ever
 * attempted a fetch, so it never gets a `source.started` event or a row in
 * `state.sources` -- this is the only evidence of it, and it arrives all at
 * once with the `run.{status}` event rather than incrementally. */
interface SkippedNotDueEntry {
  source: string;
  label: string | null;
  ageSeconds: number | null;
}

interface FetchPhase {
  status: string;
  kept: number | null;
  created: number | null;
  succeeded: number | null;
  failed: number | null;
  cancelled: number | null;
  skipped: number | null;
  skippedNotDue: SkippedNotDueEntry[];
}

interface RunPanelState {
  runKind: string | null;
  runStartedAtMs: number | null;
  plannedTargets: number | null;
  sources: Record<string, SourceRow>;
  sourceOrder: string[];
  enrichment: StageState;
  scoring: StageState;
  scoringBatches: number;
  scoringScoredSoFar: number;
  fetchPhase: FetchPhase | null;
  settled: RunSettledPayload | null;
}

function initialState(): RunPanelState {
  return {
    runKind: null,
    runStartedAtMs: null,
    plannedTargets: null,
    sources: {},
    sourceOrder: [],
    enrichment: { status: "pending" },
    scoring: { status: "pending" },
    scoringBatches: 0,
    scoringScoredSoFar: 0,
    fetchPhase: null,
    settled: null,
  };
}

function num(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}
function str(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}
function errorText(err: unknown): string | null {
  if (err == null) return null;
  if (typeof err === "string") return err;
  if (typeof err === "object") {
    const rec = err as Record<string, unknown>;
    const msg = str(rec.message);
    const type = str(rec.type);
    if (msg && type) return `${type}: ${msg}`;
    if (msg) return msg;
  }
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}
/** `run.{status}`'s `skipped_not_due` payload field: an array of
 * `{source, label, last_success_at, age_seconds, refresh_interval_seconds}`
 * dicts (scheduler.py's `_run_report`). Only `source`/`label`/`age_seconds`
 * are rendered; anything malformed is dropped rather than thrown on. */
function parseSkippedNotDue(v: unknown): SkippedNotDueEntry[] {
  if (!Array.isArray(v)) return [];
  const out: SkippedNotDueEntry[] = [];
  for (const item of v) {
    if (item == null || typeof item !== "object") continue;
    const rec = item as Record<string, unknown>;
    const source = str(rec.source);
    if (!source) continue;
    out.push({ source, label: str(rec.label), ageSeconds: num(rec.age_seconds) });
  }
  return out;
}
function enrichmentDetail(p: Record<string, unknown>): string {
  const written = num(p.rows_written) ?? 0;
  const fetched = num(p.fetched) ?? 0;
  const failed = num(p.failed) ?? 0;
  const considered = num(p.considered) ?? 0;
  return `${written} written, ${fetched} fetched, ${failed} failed of ${considered} considered`;
}
function scoringDetail(p: Record<string, unknown>): string {
  const scored = num(p.scored) ?? 0;
  const reused = num(p.reused) ?? 0;
  const blocked = num(p.blocked) ?? 0;
  const mode = str(p.mode) ?? "";
  return `${scored} scored, ${reused} reused, ${blocked} blocked${mode ? ` (${mode} pass)` : ""}`;
}

function foldEvent(state: RunPanelState, frame: RunEventFrame): RunPanelState {
  const p: Record<string, unknown> = frame.payload ?? {};
  const atMs = Date.parse(frame.at);

  if (frame.event_type === "run.started" || frame.event_type === "run.resumed") {
    return {
      ...state,
      runKind: str(p.kind),
      plannedTargets: num(p.planned_targets),
      runStartedAtMs: Number.isFinite(atMs) ? atMs : state.runStartedAtMs,
    };
  }

  if (frame.event_type === "source.started") {
    const source = str(p.source) ?? frame.source_run_id ?? "unknown";
    const row: SourceRow = {
      source,
      attempt: num(p.attempt) ?? 1,
      chip: "running",
      deadlineSeconds: num(p.deadline_seconds),
      startedAtMs: Number.isFinite(atMs) ? atMs : null,
      finishedAtMs: null,
      fetched: null,
      errorMessage: null,
      note: null,
    };
    const known = source in state.sources;
    return {
      ...state,
      sources: { ...state.sources, [source]: row },
      sourceOrder: known ? state.sourceOrder : [...state.sourceOrder, source],
    };
  }

  const terminal = SOURCE_TERMINAL.exec(frame.event_type);
  if (terminal) {
    const status = terminal[1] as "succeeded" | "failed" | "timeout" | "cancelled";
    const source = str(p.source) ?? frame.source_run_id ?? "unknown";
    const prev = state.sources[source];
    const errObj =
      p.error && typeof p.error === "object" ? (p.error as Record<string, unknown>) : null;
    const disposition = errObj ? str(errObj.disposition) : null;
    // A failed attempt with a TRANSIENT disposition is a candidate for the
    // scheduler's one retry (scheduler.py's `_run_target` loop); a timeout
    // never retries even though it also classifies TRANSIENT (the loop
    // breaks on `status == "timeout"` unconditionally). Until either the
    // next attempt's `source.started` arrives or the run settles, "retry" is
    // the honest label for that window -- it can still turn out the budget
    // ran out and nothing retried, which the settled/run-detail read then
    // corrects.
    const chip: SourceChip = status === "failed" && disposition === "transient" ? "retry" : status;
    const row: SourceRow = {
      source,
      attempt: prev?.attempt ?? 1,
      chip,
      deadlineSeconds: prev?.deadlineSeconds ?? null,
      startedAtMs: prev?.startedAtMs ?? null,
      finishedAtMs: Number.isFinite(atMs) ? atMs : null,
      fetched: num(p.fetched),
      errorMessage: errObj ? str(errObj.message) : null,
      note: null,
    };
    const known = source in state.sources;
    return {
      ...state,
      sources: { ...state.sources, [source]: row },
      sourceOrder: known ? state.sourceOrder : [...state.sourceOrder, source],
    };
  }

  if (frame.event_type === "source.retry_skipped") {
    const source = str(p.source);
    if (!source || !(source in state.sources)) return state;
    const reason = str(p.reason) ?? "budget exhausted";
    return {
      ...state,
      sources: {
        ...state.sources,
        [source]: { ...state.sources[source], note: `retry skipped: ${reason}` },
      },
    };
  }

  switch (frame.event_type) {
    case EVENT_ENRICHMENT_STARTED:
      return { ...state, enrichment: { status: "running" } };
    case EVENT_ENRICHMENT_FINISHED:
      return { ...state, enrichment: { status: "succeeded", detail: enrichmentDetail(p) } };
    case EVENT_ENRICHMENT_FAILED:
      return {
        ...state,
        enrichment: { status: "failed", detail: errorText(p.error) ?? undefined },
      };
    case EVENT_ENRICHMENT_CANCELLED:
      return { ...state, enrichment: { status: "cancelled", detail: str(p.reason) ?? undefined } };
    case EVENT_SCORING_STARTED:
      return {
        ...state,
        scoring: { status: "running" },
        scoringBatches: 0,
        scoringScoredSoFar: 0,
      };
    case EVENT_SCORE_BATCH:
      return {
        ...state,
        scoringBatches: state.scoringBatches + 1,
        scoringScoredSoFar: state.scoringScoredSoFar + (num(p.scored) ?? 0),
      };
    case EVENT_SCORING_FINISHED:
      return { ...state, scoring: { status: "succeeded", detail: scoringDetail(p) } };
    case EVENT_SCORING_FAILED:
      return { ...state, scoring: { status: "failed", detail: errorText(p.error) ?? undefined } };
    case EVENT_SCORING_CANCELLED:
      return { ...state, scoring: { status: "cancelled", detail: str(p.reason) ?? undefined } };
    case EVENT_STAGES_SKIPPED: {
      const reason = str(p.reason) ?? "skipped";
      return {
        ...state,
        enrichment: { status: "skipped", detail: reason },
        scoring: { status: "skipped", detail: reason },
      };
    }
    case EVENT_RUN_SETTLED:
      return { ...state, settled: p as unknown as RunSettledPayload };
    default: {
      const fetchDone = RUN_FETCH_DONE.exec(frame.event_type);
      if (fetchDone) {
        return {
          ...state,
          fetchPhase: {
            status: fetchDone[1],
            kept: num(p.accepted),
            created: num(p.created),
            succeeded: num(p.succeeded),
            failed: num(p.failed),
            cancelled: num(p.cancelled),
            skipped: num(p.skipped),
            skippedNotDue: parseSkippedNotDue(p.skipped_not_due),
          },
        };
      }
      return state;
    }
  }
}

// -- SSE subscription: visibility + reconnect discipline -- //
//
// This does NOT mirror SweepProgress's error handling, on purpose. SweepProgress's
// /api/sweep/progress never answers anything but 200 -- every failure it sees is a
// network blip or a deliberate server-side `bye` recycle, both of which EventSource
// retries on its own, so counting onerror calls toward a threshold and otherwise
// getting out of the way is enough.
//
// GET /api/runs/{run_uid}/events is different: routers/runsapi.py's handler can
// answer 503 (no canonical schema), 404 (run not found -- e.g. reconnecting after
// the row genuinely never existed) or 400 (bad cursor) BEFORE the stream even
// opens. Per the EventSource spec (and MDN's "Reconnection timeout" section), a
// non-200 response parks `readyState` at CLOSED permanently: exactly ONE `onerror`
// fires and the browser never retries on its own. The old version of this hook
// counted onerror calls toward `FAILURES_BEFORE_STALE` assuming they would keep
// arriving the way SweepProgress's do -- against a CLOSED socket only one ever
// does, so the counter could never reach its own threshold and the panel sat on
// "running" forever with a dead stream and no live data, indistinguishable from a
// genuinely healthy long-running fetch.
//
// The fix: read `readyState` in onerror. CLOSED means the browser has already
// given up, so WE own the reopen -- with backoff, resuming via `?after=` (a
// browser-driven reconnect instead resends `Last-Event-ID`, which the server
// prefers; `?after=` is only the fallback for reopens the header never sees).
// CONNECTING means the browser is already retrying on its own; nothing to do but
// report it honestly. Either way `failures` is one shared counter, so a run of
// failures past MAX_RECONNECT_ATTEMPTS gives up VISIBLY (a "stale" badge) rather
// than looping (manually) or waiting (on the browser) forever -- RunPanel's own
// polling of `useRunDetail` is what carries the run home from there.
const VISIBILITY_DEBOUNCE_MS = 300;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 20_000;

function reconnectDelayMs(attempt: number): number {
  return Math.min(RECONNECT_BASE_MS * 2 ** (attempt - 1), RECONNECT_MAX_MS);
}

type ConnectionState = "connecting" | "live" | "reconnecting" | "stale";

function useRunEvents(runUid: string, onSettled: () => void) {
  const [state, setState] = useState<RunPanelState>(initialState);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const onSettledRef = useRef(onSettled);
  onSettledRef.current = onSettled;
  // Lets an outside caller (RunPanel's detail-fallback effect, fix 3a) settle
  // this hook's state and permanently stop the stream even though the settle
  // did not arrive as a `service.run.settled` frame. Assigned fresh inside
  // the effect below on every `runUid` change.
  const settleFromOutsideRef = useRef<(payload: RunSettledPayload) => void>(() => {});

  useEffect(() => {
    let closed = false;
    let settledDone = false;
    let es: EventSource | null = null;
    let failures = 0;
    // The cursor for reopens: OUR OWN deliberate ones (backoff after a
    // permanent failure, tab hidden -> visible) and the browser's own
    // auto-retry both need it, though a browser-driven reconnect usually
    // resends `Last-Event-ID` instead, which the server prefers over this.
    let lastSequence: number | null = null;
    let visTimer: number | undefined;
    let reconnectTimer: number | undefined;

    const closeStream = () => {
      window.clearTimeout(reconnectTimer);
      const src = es;
      if (!src) return;
      es = null;
      src.close();
    };

    const settle = (payload: RunSettledPayload) => {
      if (settledDone) return;
      settledDone = true;
      closeStream();
      setState((prev) => ({ ...prev, settled: payload }));
      onSettledRef.current();
    };
    settleFromOutsideRef.current = settle;

    const openStream = () => {
      if (closed || es || settledDone || document.visibilityState !== "visible") return;
      const base = `/api/runs/${encodeURIComponent(runUid)}/events`;
      const url = lastSequence != null ? `${base}?after=${lastSequence}` : base;
      const src = new EventSource(url);
      es = src;
      setConnection((prev) => (prev === "live" ? prev : "connecting"));

      src.onopen = () => {
        failures = 0;
        setConnection("live");
      };

      src.onmessage = (evt) => {
        if (closed || es !== src) return;
        let frame: RunEventFrame;
        try {
          frame = JSON.parse(evt.data) as RunEventFrame;
        } catch {
          return;
        }
        lastSequence = frame.sequence;
        failures = 0;
        setState((prev) => foldEvent(prev, frame));
        if (frame.event_type === EVENT_RUN_SETTLED) {
          // The server closes the stream right after this frame too, but a
          // client-side close is what stops EventSource's own auto-reconnect
          // from looping against a run that will never emit anything else
          // (the endpoint answers a bare empty 200 for a cursor already past
          // the last row, which EventSource reads as "connection closed,
          // retry" -- see event_stream()'s docstring in runsapi.py).
          settle(frame.payload as unknown as RunSettledPayload);
        }
      };

      src.onerror = () => {
        if (closed || es !== src || settledDone) return;
        failures += 1;
        if (failures > MAX_RECONNECT_ATTEMPTS) {
          // Give up VISIBLY. `es` may still technically be open (a CONNECTING
          // socket the browser keeps retrying) -- close it ourselves so we
          // are not still silently listening after telling the user we quit.
          es = null;
          src.close();
          setConnection("stale");
          return;
        }
        if (src.readyState === EventSource.CLOSED) {
          // Permanent-failure semantics (see the comment above this hook):
          // the browser will not retry a CLOSED socket, so WE schedule the
          // reopen, with backoff, resuming past whatever we last saw.
          es = null;
          setConnection("reconnecting");
          const delay = reconnectDelayMs(failures);
          reconnectTimer = window.setTimeout(() => {
            es = null;
            openStream();
          }, delay);
        } else {
          // CONNECTING: the browser is already retrying on its own schedule.
          // Report it honestly without racing it with a second EventSource;
          // the shared `failures` counter above still bounds how long this
          // can go on before the give-up branch takes over.
          setConnection("reconnecting");
        }
      };
    };

    const apply = () => {
      if (document.visibilityState === "visible") openStream();
      else closeStream();
    };
    const onVisibility = () => {
      window.clearTimeout(visTimer);
      visTimer = window.setTimeout(apply, VISIBILITY_DEBOUNCE_MS);
    };

    apply();
    document.addEventListener("visibilitychange", onVisibility);

    return () => {
      closed = true;
      window.clearTimeout(visTimer);
      document.removeEventListener("visibilitychange", onVisibility);
      closeStream();
    };
  }, [runUid]);

  const forceSettle = useCallback((payload: RunSettledPayload) => {
    settleFromOutsideRef.current(payload);
  }, []);

  return { state, connection, forceSettle };
}

function fmtElapsed(ms: number): string {
  const totalSec = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function kindLabel(kind: string): string {
  switch (kind) {
    case "daily":
      return "Daily refresh";
    case "full-direct":
      return "Full refresh";
    case "aggregators":
      return "Aggregators";
    default:
      return kind;
  }
}
function outcomeLabel(outcome: string): string {
  switch (outcome) {
    case "degraded":
      return "degraded (post-fetch stage failed)";
    default:
      return outcome;
  }
}
function sourceChipLabel(chip: SourceChip): string {
  return chip === "retry" ? "retrying" : chip;
}

/** Fix 4: surfaces failed/cancelled/skipped counts from the `run.{status}`
 * payload so a run where every target was dueness-skipped (or several
 * failed) does not render as an unexplained "0 kept, 0 new" -- the same
 * numbers `_run_report` in scheduler.py already computes, just not dropped
 * on the floor the way the old reducer's `RUN_FETCH_DONE` branch did. */
function fetchSummaryText(phase: FetchPhase): string {
  const parts = [`${phase.succeeded ?? 0} succeeded`];
  if (phase.failed) parts.push(`${phase.failed} failed`);
  if (phase.cancelled) parts.push(`${phase.cancelled} cancelled`);
  if (phase.skipped) {
    const fresh = phase.skippedNotDue.length;
    parts.push(`${phase.skipped} skipped${fresh > 0 ? ` (${fresh} fresh)` : ""}`);
  }
  return `fetch: ${parts.join(", ")}`;
}

/** Fix 3a's fallback authority: `RunDetail.settled` when it exists (the same
 * persisted `service.run.settled` payload the stream would have delivered),
 * else -- for the rare case a process crashed before that event was ever
 * appended -- a minimal payload synthesized from the run row itself. Either
 * way `detail.terminal` (`pipeline_runs.status not in {"running"}`) is what
 * licenses using this at all: a still-running run has neither. */
function settledFromDetail(detail: RunDetail): RunSettledPayload {
  if (detail.settled) return detail.settled;
  return {
    run_uid: detail.run_uid,
    kind: detail.kind,
    fetch_status: detail.status,
    fetch_error: detail.error,
    stages: {},
    stage_failures: [],
    stages_cancelled: [],
    outcome: detail.status,
  };
}

export function RunPanel({ runUid, onDismiss }: { runUid: string; onDismiss: () => void }) {
  const qc = useQueryClient();
  const handleSettled = useCallback(() => {
    // Mirrors SweepProgress's observe() -> invalidateQueries(): a settled
    // canonical run means everything data-derived may have changed.
    qc.invalidateQueries();
  }, [qc]);
  const { state, connection, forceSettle } = useRunEvents(runUid, handleSettled);

  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    if (state.settled) return;
    const id = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [state.settled]);

  // Attempt-scoped counts (fetched/accepted/changed/item_count) live in
  // `source_runs` columns, not in the SSE payloads -- runservice.py keeps
  // that table out of `run_events`. Polled while the run is live so the
  // per-source rows below can show them; one more read after settle picks
  // up the final numbers.
  const detail = useRunDetail(runUid, { refetchInterval: state.settled ? false : 3000 });
  const latestBySource = useMemo(() => {
    const map = new Map<string, SourceRunRow>();
    for (const row of detail.data?.source_runs ?? []) {
      const existing = map.get(row.source);
      if (!existing || row.attempt >= existing.attempt) map.set(row.source, row);
    }
    return map;
  }, [detail.data]);

  // Fix 3a: dead-stream honesty. The SSE stream is one way this panel learns
  // a run is done, but not the only one -- `useRunDetail`'s own poll is
  // authoritative regardless of whether the stream ever delivered
  // `service.run.settled` (it may not have: a permanently-failed EventSource
  // that gave up, see `useRunEvents`'s "stale" state, or a page opened after
  // the fact). Once the run row itself says terminal, settle from there and
  // let `forceSettle` stop the stream -- `state.settled` flipping non-null
  // also turns off THIS query's own `refetchInterval` above on the next render.
  useEffect(() => {
    if (state.settled || !detail.data) return;
    if (detail.data.terminal) forceSettle(settledFromDetail(detail.data));
  }, [detail.data, state.settled, forceSettle]);

  const cancel = useCancelRun();
  const [cancelError, setCancelError] = useState<string | null>(null);

  const kind = state.runKind ?? detail.data?.kind ?? null;
  const outcome = state.settled?.outcome ?? null;
  const headChip =
    outcome ?? (connection === "stale" ? "stale" : connection === "reconnecting" ? "reconnecting" : "running");
  const headLabel = outcome
    ? outcomeLabel(outcome)
    : connection === "stale"
      ? "stream lost (still polling)"
      : connection === "reconnecting"
        ? "reconnecting..."
        : "running";
  const keptCount = detail.data?.kept_count ?? state.fetchPhase?.kept ?? null;
  const newCount = detail.data?.new_count ?? state.fetchPhase?.created ?? null;
  const showStages = kind !== "aggregators";

  // Fix 5: order active rows first so the strip's capped viewport (see
  // .run-panel-sources's max-height in index.css) shows what is actually
  // live on a run wide enough to scroll, rather than whichever sources
  // happened to start first and already finished. Stable within each group
  // (insertion/arrival order), so a row does not jump around as it runs.
  const orderedSources = useMemo(() => {
    const running: string[] = [];
    const terminal: string[] = [];
    for (const source of state.sourceOrder) {
      const chip = state.sources[source]?.chip;
      (chip === "running" || chip === "retry" ? running : terminal).push(source);
    }
    return [...running, ...terminal];
  }, [state.sourceOrder, state.sources]);

  // Fix 4: `skipped_not_due` targets never get a `source.started` event (the
  // scheduler skips them before creating a task), so they have no row in
  // `state.sources` at all -- without this they would just be absent, which
  // is exactly the "empty success" a dueness-skipped daily run must not
  // render as. Filtered against `state.sources` in case a target manages to
  // both start AND show up here (should not happen, but a stray double-count
  // is worse than a redundant filter).
  const skippedNotDueRows = useMemo(
    () => (state.fetchPhase?.skippedNotDue ?? []).filter((row) => !(row.source in state.sources)),
    [state.fetchPhase, state.sources],
  );

  return (
    <div className="run-panel" data-settled={state.settled ? "1" : undefined}>
      <div className="run-panel-head">
        <span className="run-chip" data-chip={headChip}>
          {headLabel}
        </span>
        <span className="run-panel-kind">{kind ? kindLabel(kind) : "run"}</span>
        {state.runStartedAtMs != null && (
          <span className="run-panel-elapsed">{fmtElapsed(nowMs - state.runStartedAtMs)}</span>
        )}
        {state.plannedTargets != null && (
          <span className="run-panel-planned">{state.plannedTargets} sources planned</span>
        )}
        <div className="run-panel-actions">
          {!state.settled && (
            <button
              type="button"
              className="btn btn-sm"
              disabled={cancel.isPending}
              onClick={() => {
                setCancelError(null);
                cancel.mutate(runUid, {
                  onError: (e) => setCancelError(e instanceof Error ? e.message : String(e)),
                });
              }}
            >
              Cancel
            </button>
          )}
          {state.settled && (
            <button type="button" className="btn btn-sm" onClick={onDismiss}>
              Dismiss
            </button>
          )}
        </div>
      </div>

      {cancelError && <div className="run-panel-note run-panel-note-error">{cancelError}</div>}

      <div className="run-panel-sources">
        {orderedSources.length === 0 && skippedNotDueRows.length === 0 && (
          <span className="run-panel-note">waiting for sources to start...</span>
        )}
        {orderedSources.map((source) => (
          <SourceRowView
            key={source}
            row={state.sources[source]}
            overlay={latestBySource.get(source)}
            nowMs={nowMs}
          />
        ))}
        {skippedNotDueRows.map((row) => (
          <SkippedNotDueRowView key={`skip:${row.source}`} row={row} />
        ))}
      </div>

      {state.fetchPhase && (
        <div className="run-panel-fetch-summary">
          <span className="run-panel-note">{fetchSummaryText(state.fetchPhase)}</span>
        </div>
      )}

      {showStages && (
        <div className="run-panel-stages">
          <StageRowView label="Enrichment" stage={state.enrichment} />
          <StageRowView
            label="Scoring"
            stage={state.scoring}
            extra={
              state.scoringBatches > 0
                ? `${state.scoringScoredSoFar} scored across ${state.scoringBatches} batch${
                    state.scoringBatches === 1 ? "" : "es"
                  } so far`
                : undefined
            }
          />
        </div>
      )}

      {state.settled && (
        <div className="run-panel-settled">
          <span className="run-panel-counts">
            {keptCount ?? 0} kept &middot; {newCount ?? 0} new
          </span>
          {state.settled.stage_failures.length > 0 && (
            <span className="run-panel-note run-panel-note-error">
              stage failures: {state.settled.stage_failures.join(", ")}
            </span>
          )}
          {state.settled.stages_cancelled.length > 0 && (
            <span className="run-panel-note">
              stages cancelled: {state.settled.stages_cancelled.join(", ")}
            </span>
          )}
          {state.settled.fetch_error != null && (
            <span className="run-panel-note run-panel-note-error">
              {errorText(state.settled.fetch_error)}
            </span>
          )}
        </div>
      )}
    </div>
  );
}

/** Fix 4: a `skipped_not_due` target -- never attempted, so none of
 * `SourceRowView`'s attempt/elapsed/fetched fields apply to it. */
function SkippedNotDueRowView({ row }: { row: SkippedNotDueEntry }) {
  return (
    <div className="run-source-row" data-skipped="1">
      <span className="run-source-name" title={row.label ?? row.source}>
        {row.source}
      </span>
      <span className="run-chip run-chip-sm" data-chip="skipped">
        skipped (fresh)
      </span>
      {row.ageSeconds != null && (
        <span className="run-source-note">last success {Math.round(row.ageSeconds)}s ago</span>
      )}
    </div>
  );
}

function SourceRowView({
  row,
  overlay,
  nowMs,
}: {
  row: SourceRow;
  overlay: SourceRunRow | undefined;
  nowMs: number;
}) {
  const elapsedMs = row.startedAtMs != null ? (row.finishedAtMs ?? nowMs) - row.startedAtMs : null;
  const fetched = overlay?.fetched_count ?? row.fetched;
  const accepted = overlay?.accepted_count ?? null;
  const changed = overlay?.changed_count ?? null;
  return (
    <div className="run-source-row">
      <span className="run-source-name" title={row.source}>
        {row.source}
      </span>
      {row.attempt > 1 && <span className="run-source-attempt">attempt {row.attempt}</span>}
      <span className="run-chip run-chip-sm" data-chip={row.chip}>
        {sourceChipLabel(row.chip)}
      </span>
      <span className="run-source-counts">
        {fetched != null && `${fetched} fetched`}
        {accepted != null && ` · ${accepted} accepted`}
        {changed != null && changed > 0 && ` · ${changed} changed`}
      </span>
      {elapsedMs != null && <span className="run-source-elapsed">{fmtElapsed(elapsedMs)}</span>}
      {row.deadlineSeconds != null && (
        <span className="run-source-deadline">/ {row.deadlineSeconds}s budget</span>
      )}
      {row.errorMessage && (
        <span className="run-source-error" title={row.errorMessage}>
          {row.errorMessage}
        </span>
      )}
      {row.note && <span className="run-source-note">{row.note}</span>}
    </div>
  );
}

function StageRowView({
  label,
  stage,
  extra,
}: {
  label: string;
  stage: StageState;
  extra?: string;
}) {
  return (
    <div className="run-stage-row">
      <span className="run-stage-label">{label}</span>
      <span className="run-chip run-chip-sm" data-chip={stage.status}>
        {stage.status}
      </span>
      {stage.detail && <span className="run-stage-detail">{stage.detail}</span>}
      {extra && <span className="run-stage-detail">{extra}</span>}
    </div>
  );
}
