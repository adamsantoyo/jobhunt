import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "../api/client";
import {
  qk,
  useCreateRun,
  useFreshness,
  useReview,
  useRuns,
  useRunsCapability,
} from "../store/queries";
import { fmtDate } from "../lib/format";
import { SOURCE_CHIP_CAP } from "../lib/ui";
import type { RunKind } from "../api/types";
import { JobDetailDrawer } from "./JobDetailDrawer";
import { SweepProgress } from "./SweepProgress";
import { RunPanel } from "./RunPanel";
import { SettingsDialog } from "./SettingsDialog";
import { ErrorBoundary } from "./ErrorBoundary";

const NAV: Array<{ to: string; label: string }> = [
  { to: "/today", label: "Today" },
  { to: "/kanban", label: "Pipeline" },
  { to: "/explore", label: "Explore" },
  { to: "/progress", label: "Progress" },
];

export function AppShell() {
  const qc = useQueryClient();
  const location = useLocation();
  const { data: freshness } = useFreshness();
  const { data: review } = useReview();
  const [settingsOpen, setSettingsOpen] = useState(false);

  const reviewCount = review?.length ?? 0;
  const running = freshness?.sweep.running ?? false;

  // Capability probe (spec decision 7): 200 on GET /api/runs means the
  // database has the canonical run schema and the run controls below drive
  // POST /api/runs + SSE instead of the legacy sweep endpoints. Anything
  // else -- still loading, a network error, an unexpected status -- resolves
  // to "legacy", which is exactly today's behavior, so the fallback is never
  // a broken or half-canonical UI.
  const capability = useRunsCapability();
  const canonical = capability.data === "canonical";
  // True once the probe has settled either way (success or error) -- as
  // opposed to `capability.isPending`, still true on its very first fetch.
  // Nothing canonical- or legacy-specific renders before this: mounting
  // SweepProgress (it opens a legacy SSE socket) speculatively during the
  // probe, only to unmount it a moment later once canonical resolves, is
  // exactly the open/close churn the Chrome 6-per-origin SSE cap punishes.
  const probeSettled = !capability.isPending;

  // Runs this tab is currently displaying a strip for. Seeded from any run
  // this process is mid-executing (covers a page reload during a run, and a
  // retry started from the Sources tab, which has no direct handle to this
  // component -- it only invalidates the `runs` query this hook shares).
  const [watchedRuns, setWatchedRuns] = useState<string[]>([]);
  const runsList = useRuns(5, { enabled: canonical });
  useEffect(() => {
    if (!canonical) return;
    const active = (runsList.data ?? []).filter((r) => r.active).map((r) => r.run_uid);
    if (active.length === 0) return;
    setWatchedRuns((prev) => {
      const missing = active.filter((uid) => !prev.includes(uid));
      return missing.length === 0 ? prev : [...prev, ...missing];
    });
  }, [canonical, runsList.data]);
  const createRun = useCreateRun();

  // Lane conflicts (runservice.py's `_EXCLUSION_GROUPS`): "daily" and
  // "full-direct" share the direct-inventory writer lane, so either one
  // active blocks starting the other; "aggregators" is independent and only
  // conflicts with itself. Mirrors the legacy path's own affordance, which
  // disables both its buttons off `freshness.sweep.running`.
  const activeRuns = (runsList.data ?? []).filter((r) => r.active);
  const directLaneBusy = activeRuns.some((r) => r.kind === "daily" || r.kind === "full-direct");
  const aggregatorsLaneBusy = activeRuns.some((r) => r.kind === "aggregators");

  // Sweep-control outcomes render as a full-width banner under the topbar, never
  // as a native dialog: Chrome can suppress window.confirm/alert for the page,
  // which once made "Full sweep" a silent no-op.
  const [banner, setBanner] = useState<{ tone: "error" | "ok"; text: string } | null>(null);
  const [confirmFullOpen, setConfirmFullOpen] = useState(false);

  const surfaceError = (e: unknown) => {
    if (e instanceof ApiError && e.status === 409) {
      setBanner({
        tone: "error",
        text: "A sweep is already running. See the progress strip below, or cancel it there first.",
      });
    } else {
      setBanner({ tone: "error", text: e instanceof Error ? e.message : String(e) });
    }
  };

  // Success notices clear themselves; errors stay until dismissed.
  useEffect(() => {
    if (banner?.tone !== "ok") return;
    const t = window.setTimeout(() => setBanner(null), 10_000);
    return () => window.clearTimeout(t);
  }, [banner]);

  const quick = useMutation({
    mutationFn: api.refreshQuick,
    onMutate: () => setBanner(null),
    onError: surfaceError,
    onSettled: () => qc.invalidateQueries({ queryKey: qk.freshness }),
  });
  const full = useMutation({
    mutationFn: api.sweepFull,
    onMutate: () => setBanner(null),
    onSuccess: () =>
      setBanner({
        tone: "ok",
        text: "Full sweep started (36 steps, 20-45 min). Progress appears in the strip below.",
      }),
    onError: surfaceError,
    onSettled: () => qc.invalidateQueries({ queryKey: qk.freshness }),
  });

  // Canonical mode's own error handling. A 409 (runservice.py's
  // `_check_conflicts`) carries a message built for logs, not users -- it
  // names the conflicting run by UUID. The lane-busy disabling above should
  // catch this before the request even fires, so a 409 reaching here means a
  // race (another tab, another process); the banner just needs to say a run
  // is already going, not spell out which one.
  const surfaceCanonicalError = (e: unknown) => {
    if (e instanceof ApiError && e.status === 409) {
      setBanner({
        tone: "error",
        text: "A run is already active in that lane. See the panel below, or wait for it to finish.",
      });
    } else if (e instanceof ApiError) {
      setBanner({ tone: "error", text: e.message });
    } else {
      setBanner({ tone: "error", text: e instanceof Error ? e.message : String(e) });
    }
  };

  const startCanonical = (kind: RunKind) => {
    setBanner(null);
    createRun.mutate(kind, {
      onSuccess: (data) => {
        setWatchedRuns((prev) => (prev.includes(data.run_uid) ? prev : [...prev, data.run_uid]));
        if (kind === "full-direct") {
          setBanner({
            tone: "ok",
            text: "Full refresh started. Sources run concurrently; the panel below tracks progress.",
          });
        }
      },
      onError: surfaceCanonicalError,
    });
  };

  return (
    <div className="app-shell">
      <nav className="sidebar">
        <div className="sidebar-brand">
          <span className="brand-mark">◆</span> JobHunt
        </div>
        <ul className="nav-list">
          {NAV.map((item) => (
            <li key={item.to}>
              <NavLink
                to={item.to}
                className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
              >
                {item.label}
              </NavLink>
            </li>
          ))}
        </ul>
        {reviewCount > 0 && (
          <NavLink
            to="/review"
            className={({ isActive }) => `sidebar-review nav-link${isActive ? " active" : ""}`}
            title="States that need manual reconciliation review"
          >
            ⚠ {reviewCount} need review
          </NavLink>
        )}
      </nav>

      <div className="main">
        <header className="topbar">
          <div className="topbar-left">
            <span className="run-date">
              Run {freshness?.latest_run ? fmtDate(freshness.latest_run) : "-"}
            </span>
            {freshness?.kept != null && <span className="run-kept">{freshness.kept} kept</span>}
            {freshness?.new_this_run != null && freshness.new_this_run > 0 && (
              <span className="chip chip-new">{freshness.new_this_run} new</span>
            )}
            <SourceChips
              sources={freshness?.sources ?? []}
              zero={freshness?.zero_row_sources ?? []}
              stale={freshness?.stale_refresh_sources ?? []}
            />
          </div>

          <div className="topbar-right">
            {/* Nothing sweep/run-related renders until the capability probe
                resolves -- a brief empty slot here beats mounting the legacy
                SweepProgress SSE socket speculatively and tearing it down a
                moment later once canonical resolves. */}
            {probeSettled &&
              (canonical ? (
                <>
                  <button
                    type="button"
                    className="btn"
                    disabled={createRun.isPending || directLaneBusy}
                    title={directLaneBusy ? "A daily or full refresh is already running" : undefined}
                    onClick={() => startCanonical("daily")}
                  >
                    Daily refresh
                  </button>
                  <button
                    type="button"
                    className="btn"
                    disabled={createRun.isPending || directLaneBusy}
                    title={directLaneBusy ? "A daily or full refresh is already running" : undefined}
                    onClick={() => setConfirmFullOpen(true)}
                  >
                    Full refresh
                  </button>
                  <button
                    type="button"
                    className="btn"
                    disabled={createRun.isPending || aggregatorsLaneBusy}
                    title={aggregatorsLaneBusy ? "Aggregators are already running" : undefined}
                    onClick={() => startCanonical("aggregators")}
                  >
                    Aggregators
                  </button>
                </>
              ) : (
                <>
                  <button
                    type="button"
                    className="btn"
                    disabled={running || quick.isPending}
                    onClick={() => quick.mutate()}
                  >
                    Quick refresh
                  </button>
                  <button
                    type="button"
                    className="btn"
                    disabled={running || full.isPending}
                    onClick={() => setConfirmFullOpen(true)}
                  >
                    Full sweep
                  </button>
                </>
              ))}
            <button
              type="button"
              className="btn btn-icon"
              aria-label="Settings"
              onClick={() => setSettingsOpen(true)}
            >
              ⚙
            </button>
          </div>
        </header>

        {banner && (
          <div
            className="app-banner"
            data-tone={banner.tone}
            role={banner.tone === "error" ? "alert" : "status"}
          >
            <span className="app-banner-text">{banner.text}</span>
            <button type="button" className="btn btn-sm" onClick={() => setBanner(null)}>
              Dismiss
            </button>
          </div>
        )}

        {/* Own boundary, separate from the routed Outlet's: a throw in here
            (RunPanel renders untrusted SSE payload data) should lose just
            the strip, not the whole SPA, and the rest of the shell -- nav,
            topbar, the routed page below -- should keep working. Not keyed
            by route; the strip is route-independent. */}
        <ErrorBoundary
          title="The run panel hit an error."
          hint="The rest of the app still works."
        >
          {probeSettled &&
            (canonical ? (
              watchedRuns.length > 0 && (
                <div className="run-panel-stack">
                  {watchedRuns.map((uid) => (
                    <RunPanel
                      key={uid}
                      runUid={uid}
                      onDismiss={() =>
                        setWatchedRuns((prev) => prev.filter((existing) => existing !== uid))
                      }
                    />
                  ))}
                </div>
              )
            ) : (
              <SweepProgress />
            ))}
        </ErrorBoundary>

        <main className="content">
          <ErrorBoundary key={location.pathname + location.search}>
            <Outlet />
          </ErrorBoundary>
        </main>
      </div>

      {confirmFullOpen && (
        <div className="modal-overlay" onClick={() => setConfirmFullOpen(false)}>
          <div
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-label={canonical ? "Confirm full refresh" : "Confirm full sweep"}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="modal-head">
              <h2>{canonical ? "Run a full refresh?" : "Run a full sweep?"}</h2>
              <button
                type="button"
                className="btn btn-icon"
                onClick={() => setConfirmFullOpen(false)}
                aria-label="Close"
              >
                ✕
              </button>
            </div>
            <div className="modal-body">
              {canonical ? (
                <p className="confirm-text">
                  This re-scrapes every direct source. Sources run concurrently, so it
                  typically takes a few minutes rather than tens of minutes. Daily refresh only
                  re-checks known jobs, targets under a minute, and is usually what you want
                  during the day.
                </p>
              ) : (
                <p className="confirm-text">
                  This re-scrapes every source (36 steps) and can take 20-45 minutes. Quick
                  refresh only re-checks known jobs and is usually what you want during the day.
                </p>
              )}
            </div>
            <div className="modal-foot">
              <button type="button" className="btn" onClick={() => setConfirmFullOpen(false)}>
                Cancel
              </button>
              <button
                type="button"
                className="btn btn-primary"
                onClick={() => {
                  setConfirmFullOpen(false);
                  if (canonical) startCanonical("full-direct");
                  else full.mutate();
                }}
              >
                {canonical ? "Start full refresh" : "Start full sweep"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Drawer is mounted ONCE for the whole app, driven by ?job=<url_b64>. */}
      <JobDetailDrawer />
      <SettingsDialog open={settingsOpen} onClose={() => setSettingsOpen(false)} />
    </div>
  );
}

function SourceChips({
  sources,
  zero,
  stale,
}: {
  sources: Array<{ name: string; rows: number; refreshed: boolean | null }>;
  zero: string[];
  stale: string[];
}) {
  const [expandedAll, setExpandedAll] = useState(false);
  const zeroSet = new Set(zero);
  const staleSet = new Set(stale);
  if (sources.length === 0 && zero.length === 0 && stale.length === 0) return null;

  // Classify each chip by kind and sort: problem chips first (stable sort)
  const classified = sources.map((s) => {
    let kind = "ok";
    if (zeroSet.has(s.name) || s.rows === 0) kind = "zero";
    else if (staleSet.has(s.name)) kind = "stale";
    else if (s.refreshed === false) kind = "stale";
    return { ...s, kind };
  });

  // Stable sort: problem chips (kind !== "ok") before ok chips
  const sorted = classified.sort((a, b) => {
    const aIsProblem = a.kind !== "ok" ? 0 : 1;
    const bIsProblem = b.kind !== "ok" ? 0 : 1;
    return aIsProblem - bIsProblem;
  });

  const visible = expandedAll ? sorted : sorted.slice(0, SOURCE_CHIP_CAP);
  const hiddenCount = sorted.length - visible.length;

  return (
    <div className="source-chips" data-expanded={expandedAll || undefined}>
      {visible.map((s) => (
        <span
          key={s.name}
          className="src-chip"
          data-kind={s.kind}
          title={`${s.name}: ${s.rows} rows${s.refreshed === false ? " (not refreshed)" : ""}`}
        >
          {s.name} {s.rows}
        </span>
      ))}
      {sorted.length > SOURCE_CHIP_CAP && (
        <button
          type="button"
          className="src-chip"
          data-kind="toggle"
          onClick={() => setExpandedAll(!expandedAll)}
          title={`${hiddenCount} more sources`}
        >
          {expandedAll ? "show less" : `+${hiddenCount} more`}
        </button>
      )}
    </div>
  );
}
