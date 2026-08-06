import { useState, type CSSProperties, type KeyboardEvent } from "react";
import { useConfig, useJobDetail, usePatchState, useQuickAction } from "../../store/queries";
import { api } from "../../api/client";
import { fmtSalary, flagsList, isHttpUrl } from "../../lib/format";
import { highlightText } from "../../lib/highlight";
import { FlagBadge, OddsBadge, TierBadge } from "../StatusBadge";
import { Menu, MenuItem } from "./Menu";
import type { JobFull, JobLight } from "../../api/types";

// One row of the do-today queue. Collapsed row shows the fit/odds badges plus
// company/title/meta; clicking the row body toggles an inline JD expansion
// (never the drawer — the drawer is reserved for the ⤢ button). Actions are
// always visible: Apply flips the card into a submit-confirmation state,
// Shortlist/Snooze/Pass act immediately (or via a small reason/duration menu).

const APPLIED_VIA_OPTIONS: Array<{ value: string; label: string }> = [
  { value: "site", label: "Site" },
  { value: "linkedin", label: "LinkedIn" },
  { value: "email", label: "Email" },
  { value: "referral", label: "Referral" },
  { value: "other", label: "Other" },
];

const PASS_REASONS: Array<{ value: string; label: string }> = [
  { value: "comp", label: "Comp" },
  { value: "location", label: "Location" },
  { value: "seniority", label: "Seniority" },
  { value: "stack", label: "Stack" },
  { value: "other", label: "Other" },
];

const SNOOZE_OPTIONS: Array<{ days: number; label: string }> = [
  { days: 1, label: "1 day" },
  { days: 3, label: "3 days" },
  { days: 7, label: "1 week" },
];

/** Days between an ISO date string and today (local-naive), or null if unset/unparsable. */
function daysAgo(dateStr: string | null | undefined): number | null {
  if (!dateStr) return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(dateStr));
  if (!m) return null;
  const then = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  return Math.floor((today.getTime() - then.getTime()) / 86_400_000);
}

function agoLabel(dateStr: string | null | undefined): string {
  const d = daysAgo(dateStr);
  if (d === null) return "";
  return d <= 0 ? "today" : `${d}d ago`;
}

const skeletonBarStyle = (w: string): CSSProperties => ({ width: w });

export function TodayCard({
  job,
  onOpen,
  snapshotId = null,
}: {
  job: JobLight;
  onOpen: (job: JobLight) => void;
  /** Today snapshot this card was served from (5.5 open-event capture).
   * No `rank` prop: F1 -- rank never leaves the client, the server derives
   * it from the (snapshot_id, posting) match. */
  snapshotId?: string | null;
}) {
  const [expanded, setExpanded] = useState(false);
  const [hasExpandedOnce, setHasExpandedOnce] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [appliedVia, setAppliedVia] = useState("site");
  const [snoozeOpen, setSnoozeOpen] = useState(false);
  const [passOpen, setPassOpen] = useState(false);

  const patchState = usePatchState();
  const quick = useQuickAction();
  const { data: config } = useConfig();
  const detail = useJobDetail(hasExpandedOnce ? job.url_b64 : null);

  const pending = patchState.isPending || quick.isPending;
  const salary = fmtSalary(job);
  const flags = flagsList(job.flags);
  const posted = job.posted ?? job.first_seen;

  const toggleExpand = () => {
    if (!expanded) setHasExpandedOnce(true);
    setExpanded(!expanded);
  };

  const startApply = () => {
    // Scraped URLs can carry any scheme; only open real web links. The confirm
    // state still shows either way so an application made elsewhere can be logged.
    if (isHttpUrl(job.url)) {
      window.open(job.url, "_blank", "noopener");
      // Fire-and-forget: never awaited, never blocks/delays the open above.
      api.captureOpened(job.url_b64, { snapshotId });
    }
    setAppliedVia("site");
    setConfirming(true);
  };
  const submitApplied = () => {
    quick.mutate({ urlB64: job.url_b64, body: { action: "applied", applied_via: appliedVia } });
    setConfirming(false);
  };
  const cancelConfirm = () => setConfirming(false);
  // Escape only: Enter is left to the natively-focused button (Submitted ✓ holds
  // autoFocus), so Enter on "Not yet" or a via-chip activates THAT control instead
  // of submitting from anywhere in the container.
  const onConfirmKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.key === "Escape") {
      e.preventDefault();
      cancelConfirm();
    }
  };

  const doShortlist = () => patchState.mutate({ urlB64: job.url_b64, patch: { status: "Interested" } });
  const doSnooze = (days: number) => {
    quick.mutate({ urlB64: job.url_b64, body: { action: "snooze", days } });
    setSnoozeOpen(false);
  };
  const doPass = (reason?: string) => {
    quick.mutate({ urlB64: job.url_b64, body: reason ? { action: "pass", reason } : { action: "pass" } });
    setPassOpen(false);
  };

  return (
    <div className="td-card">
      <div className="td-card-row">
        <button
          type="button"
          className="td-card-row-main"
          onClick={toggleExpand}
          title={expanded ? "Collapse" : "Expand"}
        >
          <span className="td-card-badges">
            <TierBadge tier={job.tier} />
            <OddsBadge odds={job.odds} />
          </span>
          <span className="td-card-textcol">
            <span className="td-card-line1">
              <span className="td-strong">{job.company ?? "—"}</span>
              <span className="td-sep"> · </span>
              <span className="td-title-dim">{job.title ?? "—"}</span>
            </span>
            <span className="td-card-line2">
              {job.remote && <span className="tag-remote">R</span>}
              {job.location || "location n/a"}
              {salary ? ` · ${salary}` : ""}
              {job.source ? ` · ${job.source}` : ""}
              {posted ? ` · ${agoLabel(posted)}` : ""}
            </span>
          </span>
        </button>
        <button
          type="button"
          className="btn btn-icon"
          onClick={() => onOpen(job)}
          title="Open full details"
          aria-label="Open full details"
        >
          ⤢
        </button>
      </div>

      {expanded && (
        <div className="td-card-expand">
          {(job.why || job.odds_why) && (
            <div className="drawer-why">
              {job.why && (
                <p>
                  <span className="why-label">Fit</span> {job.why}
                </p>
              )}
              {job.odds_why && (
                <p>
                  <span className="why-label">Match</span> {job.odds_why}
                </p>
              )}
            </div>
          )}
          {flags.length > 0 && (
            <div className="drawer-flags">
              {flags.map((f) => (
                <FlagBadge key={f} flag={f} />
              ))}
            </div>
          )}
          <JdBody detail={detail} />
        </div>
      )}

      {confirming ? (
        <div className="td-card-confirm" onKeyDown={onConfirmKeyDown}>
          <span className="muted-sm">Did you submit?</span>
          <div className="td-seg-row">
            {APPLIED_VIA_OPTIONS.map((o) => (
              <button
                key={o.value}
                type="button"
                className="chip-toggle"
                data-on={appliedVia === o.value ? "1" : "0"}
                onClick={() => setAppliedVia(o.value)}
              >
                {o.label}
              </button>
            ))}
          </div>
          <div className="td-card-actions">
            <button
              type="button"
              className="btn btn-sm btn-primary"
              autoFocus
              disabled={pending}
              onClick={submitApplied}
            >
              Submitted ✓
            </button>
            <button type="button" className="btn btn-sm" disabled={pending} onClick={cancelConfirm}>
              Not yet
            </button>
          </div>
        </div>
      ) : (
        <div className="td-card-actions">
          <button type="button" className="btn btn-sm btn-primary" disabled={pending} onClick={startApply}>
            Apply →
          </button>
          <button type="button" className="btn btn-sm" disabled={pending} onClick={doShortlist}>
            Shortlist
          </button>
          <div className="td-menu-wrap">
            <button
              type="button"
              className="btn btn-sm"
              disabled={pending}
              title={`Default ${config?.snooze_default_days ?? 3}d`}
              onClick={() => setSnoozeOpen((v) => !v)}
            >
              Snooze ▾
            </button>
            {snoozeOpen && (
              <Menu onClose={() => setSnoozeOpen(false)}>
                {SNOOZE_OPTIONS.map((o) => (
                  <MenuItem key={o.days} onClick={() => doSnooze(o.days)}>
                    {o.label}
                  </MenuItem>
                ))}
              </Menu>
            )}
          </div>
          <div className="td-menu-wrap">
            <div className="td-pass-group">
              <button type="button" className="btn btn-sm" disabled={pending} onClick={() => doPass()}>
                Pass
              </button>
              <button
                type="button"
                className="btn btn-sm btn-icon"
                disabled={pending}
                title="Pass with reason"
                aria-label="Pass with reason"
                onClick={() => setPassOpen((v) => !v)}
              >
                ▾
              </button>
            </div>
            {passOpen && (
              <Menu onClose={() => setPassOpen(false)}>
                {PASS_REASONS.map((r) => (
                  <MenuItem key={r.value} onClick={() => doPass(r.value)}>
                    {r.label}
                  </MenuItem>
                ))}
              </Menu>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function JdBody({ detail }: { detail: ReturnType<typeof useJobDetail> }) {
  if (detail.isLoading) {
    return (
      <div className="td-skeleton">
        <div className="td-skeleton-bar" style={skeletonBarStyle("92%")} />
        <div className="td-skeleton-bar" style={skeletonBarStyle("78%")} />
        <div className="td-skeleton-bar" style={skeletonBarStyle("85%")} />
      </div>
    );
  }
  if (detail.isError || !detail.data) {
    return <div className="muted-sm">Failed to load description.</div>;
  }
  const full: JobFull = detail.data;
  const bodyText = (full.full_desc && full.full_desc.trim()) || full.desc_snippet || "";
  return (
    <div className="jd-text td-jd-body">
      {highlightText(bodyText, full.skill_hits ?? [])}
    </div>
  );
}
