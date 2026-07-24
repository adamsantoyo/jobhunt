import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api/client";
import { qk, useFreshness, useReview } from "../store/queries";
import { fmtDate } from "../lib/format";
import { JobDetailDrawer } from "./JobDetailDrawer";
import { SweepProgress } from "./SweepProgress";
import { SettingsDialog } from "./SettingsDialog";

const NAV: Array<{ to: string; label: string }> = [
  { to: "/today", label: "Today" },
  { to: "/kanban", label: "Pipeline" },
  { to: "/explore", label: "Explore" },
  { to: "/analytics", label: "Analytics" },
];

export function AppShell() {
  const qc = useQueryClient();
  const { data: freshness } = useFreshness();
  const { data: review } = useReview();
  const [settingsOpen, setSettingsOpen] = useState(false);

  const reviewCount = review?.length ?? 0;
  const running = freshness?.sweep.running ?? false;

  const [actionError, setActionError] = useState<string | null>(null);
  const surfaceError = (e: unknown) =>
    setActionError(e instanceof Error ? e.message : String(e));

  const quick = useMutation({
    mutationFn: api.refreshQuick,
    onMutate: () => setActionError(null),
    onError: surfaceError,
    onSettled: () => qc.invalidateQueries({ queryKey: qk.freshness }),
  });
  const full = useMutation({
    mutationFn: api.sweepFull,
    onMutate: () => setActionError(null),
    onError: surfaceError,
    onSettled: () => qc.invalidateQueries({ queryKey: qk.freshness }),
  });

  const startFull = () => {
    if (window.confirm("Run a FULL sweep? This re-scrapes everything and can take 20-45 minutes.")) {
      full.mutate();
    }
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
              Run {freshness?.latest_run ? fmtDate(freshness.latest_run) : "—"}
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
            {actionError && (
              <span
                className="sweep-error"
                role="alert"
                style={{ cursor: "pointer", fontSize: 12 }}
                title="Click to dismiss"
                onClick={() => setActionError(null)}
              >
                {actionError}
              </span>
            )}
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
              onClick={startFull}
            >
              Full sweep
            </button>
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

        <SweepProgress />

        <main className="content">
          <Outlet />
        </main>
      </div>

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
  const zeroSet = new Set(zero);
  const staleSet = new Set(stale);
  if (sources.length === 0 && zero.length === 0 && stale.length === 0) return null;

  return (
    <div className="source-chips">
      {sources.slice(0, 14).map((s) => {
        let kind = "ok";
        if (zeroSet.has(s.name) || s.rows === 0) kind = "zero";
        else if (staleSet.has(s.name)) kind = "stale";
        else if (s.refreshed === false) kind = "stale";
        return (
          <span
            key={s.name}
            className="src-chip"
            data-kind={kind}
            title={`${s.name}: ${s.rows} rows${s.refreshed === false ? " (not refreshed)" : ""}`}
          >
            {s.name} {s.rows}
          </span>
        );
      })}
    </div>
  );
}
