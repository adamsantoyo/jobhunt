import { useEffect, useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "../api/client";
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

  // Sweep-control outcomes render as a full-width banner under the topbar, never
  // as a native dialog: Chrome can suppress window.confirm/alert for the page,
  // which once made "Full sweep" a silent no-op.
  const [banner, setBanner] = useState<{ tone: "error" | "ok"; text: string } | null>(null);
  const [confirmFullOpen, setConfirmFullOpen] = useState(false);

  const surfaceError = (e: unknown) => {
    if (e instanceof ApiError && e.status === 409) {
      setBanner({
        tone: "error",
        text: "A sweep is already running — see the progress strip below, or cancel it there first.",
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

        <SweepProgress />

        <main className="content">
          <Outlet />
        </main>
      </div>

      {confirmFullOpen && (
        <div className="modal-overlay" onClick={() => setConfirmFullOpen(false)}>
          <div
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-label="Confirm full sweep"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="modal-head">
              <h2>Run a full sweep?</h2>
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
              <p className="confirm-text">
                This re-scrapes every source (36 steps) and can take 20-45 minutes. Quick
                refresh only re-checks known jobs and is usually what you want during the day.
              </p>
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
                  full.mutate();
                }}
              >
                Start full sweep
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
