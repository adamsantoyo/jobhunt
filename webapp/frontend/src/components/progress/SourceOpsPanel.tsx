import { useState } from "react";
import { ApiError } from "../../api/client";
import { useRetrySource, useRunsCapability, useSourceOps } from "../../store/queries";
import type { SourceOpsEntry } from "../../api/types";
import { fmtDate } from "../../lib/format";
import { StatCard } from "./panels";

// Sources tab (Phase 4.4 frontend), consuming GET /api/sources/ops per the
// pinned contract (plans/phase4-spec.md wave-2 decision 8). `last_error` is
// job-source/adapter-derived text -- rendered as a text node only, same rule
// as RunPanel's event payloads (decision 10).

const ERROR_TRUNCATE_CHARS = 90;

function fmtAge(seconds: number | null): string {
  if (seconds == null) return "never";
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  return `${Math.round(seconds / 86400)}d ago`;
}
function fmtDuration(v: number | null): string {
  return v == null ? "-" : `${v.toFixed(1)}s`;
}
/** Rows are integers on the wire, but `median_rows` is a `statistics.median`
 * over an even-sized window and can land on a `.5` -- round it for display. */
function fmtRows(v: number | null): string {
  return v == null ? "-" : String(Math.round(v));
}
/** `last_error` is a plain string per contract (SourceOpsEntry.last_error),
 * but this coerces defensively: a future contract slip that puts an object
 * back on the wire must degrade to text, never throw during render and blank
 * the whole SPA (there is no ErrorBoundary anywhere in this app). */
function lastErrorText(v: unknown): string {
  if (v == null) return "";
  if (typeof v === "string") return v;
  return String(v);
}

export function SourceOpsPanel() {
  const capability = useRunsCapability();
  // While the capability probe itself is still in flight, `capability.data`
  // is undefined and `mode` would default to "legacy" -- showing the legacy
  // explanation copy for a database that may well turn out to be canonical a
  // moment later. Gate on the probe's own pending state first so a loading
  // probe reads as "loading", never as a false "legacy".
  if (capability.isPending) return <p className="muted">Loading sources...</p>;

  const mode = capability.data === "canonical" ? "canonical" : "legacy";
  return mode === "canonical" ? <SourceOpsTable /> : <LegacySourceOpsNotice />;
}

function LegacySourceOpsNotice() {
  return (
    <div className="an-empty">
      Source operations needs the canonical run engine, which is not active on this
      database yet. This panel lights up automatically once it is.
    </div>
  );
}

function SourceOpsTable() {
  const ops = useSourceOps();
  const retry = useRetrySource();
  const [banner, setBanner] = useState<{ tone: "error" | "ok"; text: string } | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  if (ops.isLoading) return <p className="muted">Loading sources...</p>;
  if (ops.isError || !ops.data) {
    return <div className="page-error">Failed to load source operations.</div>;
  }

  const sources = ops.data.sources;
  const staleCount = sources.filter((s) => s.stale).length;
  const circuitOpenCount = sources.filter((s) => s.circuit_open).length;
  const anomalyCount = sources.filter((s) => s.row_anomaly.flag).length;

  const toggle = (name: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const doRetry = (source: string) => {
    setBanner(null);
    retry.mutate(source, {
      onSuccess: () => setBanner({ tone: "ok", text: `Retry started for ${source}.` }),
      onError: (e) => {
        if (e instanceof ApiError && e.status === 409) {
          setBanner({ tone: "error", text: e.message });
        } else if (e instanceof ApiError && e.status === 503) {
          setBanner({
            tone: "error",
            text: "Canonical run engine is not available on this database.",
          });
        } else if (e instanceof ApiError && e.status === 404) {
          setBanner({ tone: "error", text: `Unknown source: ${source}` });
        } else {
          setBanner({ tone: "error", text: e instanceof Error ? e.message : String(e) });
        }
      },
    });
  };

  return (
    <>
      <div className="an-stats">
        <StatCard label="Sources" value={sources.length} />
        <StatCard
          label="Stale"
          value={staleCount}
          accent={staleCount > 0 ? "var(--amber)" : undefined}
        />
        <StatCard
          label="Circuit open"
          value={circuitOpenCount}
          accent={circuitOpenCount > 0 ? "var(--red)" : undefined}
        />
        <StatCard
          label="Row anomalies"
          value={anomalyCount}
          accent={anomalyCount > 0 ? "var(--amber)" : undefined}
        />
      </div>

      {banner && (
        <div
          className="app-banner src-ops-banner"
          data-tone={banner.tone}
          role={banner.tone === "error" ? "alert" : "status"}
        >
          <span className="app-banner-text">{banner.text}</span>
          <button type="button" className="btn btn-sm" onClick={() => setBanner(null)}>
            Dismiss
          </button>
        </div>
      )}

      <div className="src-ops-table">
        <div className="src-ops-row src-ops-head">
          <span>Source</span>
          <span>Category</span>
          <span>Freshness</span>
          <span>Failures</span>
          <span>p50 / p95</span>
          <span>Rows (last / median)</span>
          <span>Flags</span>
          <span>Last error</span>
          <span />
        </div>
        {sources.map((s) => (
          <SourceOpsRow
            key={s.source}
            entry={s}
            expanded={expanded.has(s.source)}
            onToggle={() => toggle(s.source)}
            onRetry={() => doRetry(s.source)}
            retrying={retry.isPending && retry.variables === s.source}
          />
        ))}
        {sources.length === 0 && (
          <div className="an-empty">No source instances recorded yet.</div>
        )}
      </div>
      <div className="src-ops-generated" title={ops.data.generated_at}>
        as of {fmtDate(ops.data.generated_at)} ({fmtAge(generatedAgeSeconds(ops.data.generated_at))})
      </div>
    </>
  );
}

function generatedAgeSeconds(generatedAt: string): number | null {
  const ms = Date.parse(generatedAt);
  return Number.isFinite(ms) ? Math.max(0, (Date.now() - ms) / 1000) : null;
}

function SourceOpsRow({
  entry,
  expanded,
  onToggle,
  onRetry,
  retrying,
}: {
  entry: SourceOpsEntry;
  expanded: boolean;
  onToggle: () => void;
  onRetry: () => void;
  retrying: boolean;
}) {
  const errText = lastErrorText(entry.last_error);
  const isLong = errText.length > ERROR_TRUNCATE_CHARS;
  const shown = isLong && !expanded ? `${errText.slice(0, ERROR_TRUNCATE_CHARS)}...` : errText;
  return (
    <div
      className="src-ops-row"
      data-stale={entry.stale || undefined}
      data-circuit={entry.circuit_open || undefined}
    >
      <span className="src-ops-name" title={entry.source}>
        <span className="src-ops-name-text">{entry.source}</span>
        {entry.licenses_absence && (
          <span
            className="badge flag"
            data-kind="muted"
            title="This source's completeness licenses marking missing postings absent"
          >
            licenses absence
          </span>
        )}
      </span>
      <span className="src-ops-category">{entry.category ?? "-"}</span>
      <span
        className="src-ops-freshness"
        data-stale={entry.stale || undefined}
        title={entry.last_success_at ?? "no successful run recorded"}
      >
        {fmtAge(entry.age_seconds)}
      </span>
      <span className="src-ops-failures" data-bad={entry.circuit_open || undefined}>
        {entry.consecutive_failures}
      </span>
      <span className="src-ops-durations">
        {fmtDuration(entry.p50_duration_seconds)} / {fmtDuration(entry.p95_duration_seconds)}
      </span>
      <span className="src-ops-rows" data-anomaly={entry.row_anomaly.flag || undefined}>
        {entry.last_rows ?? "-"} / {fmtRows(entry.median_rows)}
        {entry.row_anomaly.flag && entry.row_anomaly.ratio != null && (
          <span className="src-ops-anomaly-note">{entry.row_anomaly.ratio.toFixed(2)}x</span>
        )}
      </span>
      <span className="src-ops-chips">
        {entry.circuit_open && (
          <span className="badge flag" data-kind="red">
            circuit open
          </span>
        )}
        {entry.stale === true && (
          <span className="badge flag" data-kind="amber">
            stale
          </span>
        )}
        {entry.stale === null && (
          <span className="badge flag" data-kind="muted" title="No run recorded for this source yet">
            never run
          </span>
        )}
      </span>
      <span
        className="src-ops-error"
        data-expandable={isLong || undefined}
        onClick={isLong ? onToggle : undefined}
        title={expanded || !isLong ? undefined : errText}
      >
        {shown || "-"}
      </span>
      <span className="src-ops-actions">
        <button type="button" className="btn btn-sm" disabled={retrying} onClick={onRetry}>
          {retrying ? "Retrying..." : "Retry"}
        </button>
      </span>
    </div>
  );
}
