import { useState, type CSSProperties, type KeyboardEvent } from "react";
import { useConfig, useJobDetail, usePatchState, useQuickAction } from "../../store/queries";
import { fmtSalary, flagsList } from "../../lib/format";
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

const cardStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  background: "var(--bg-1)",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius)",
  padding: "8px 10px",
  gap: 4,
};
const rowStyle: CSSProperties = { display: "flex", alignItems: "center", gap: 8 };
const rowMainStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  flex: 1,
  minWidth: 0,
  background: "transparent",
  border: "none",
  color: "var(--fg)",
  font: "inherit",
  textAlign: "left",
  cursor: "pointer",
  padding: 0,
};
const badgesStyle: CSSProperties = { display: "flex", gap: 3, flex: "0 0 auto" };
const textCol: CSSProperties = { display: "flex", flexDirection: "column", minWidth: 0, gap: 1 };
const line1: CSSProperties = { overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" };
const line2: CSSProperties = {
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  color: "var(--fg-mute)",
  fontSize: 11,
};
const expandStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 8,
  marginTop: 4,
  paddingTop: 8,
  borderTop: "1px solid var(--border-soft)",
};
const actionsStyle: CSSProperties = { display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center", marginTop: 4 };
const menuWrapStyle: CSSProperties = { position: "relative", display: "inline-flex" };
const passGroupStyle: CSSProperties = { display: "flex", gap: 1 };
const confirmStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 6,
  marginTop: 4,
  padding: 8,
  background: "var(--bg-2)",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius-sm)",
};
const segRowStyle: CSSProperties = { display: "flex", gap: 4, flexWrap: "wrap" };
const skeletonStyle: CSSProperties = { display: "flex", flexDirection: "column", gap: 6 };
const skeletonBarStyle = (w: string): CSSProperties => ({
  height: 10,
  width: w,
  background: "var(--bg-3)",
  borderRadius: 3,
});

export function TodayCard({ job, onOpen }: { job: JobLight; onOpen: (job: JobLight) => void }) {
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
    window.open(job.url, "_blank", "noopener");
    setAppliedVia("site");
    setConfirming(true);
  };
  const submitApplied = () => {
    quick.mutate({ urlB64: job.url_b64, body: { action: "applied", applied_via: appliedVia } });
    setConfirming(false);
  };
  const cancelConfirm = () => setConfirming(false);
  const onConfirmKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      submitApplied();
    } else if (e.key === "Escape") {
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
    <div style={cardStyle}>
      <div style={rowStyle}>
        <button
          type="button"
          style={rowMainStyle}
          onClick={toggleExpand}
          title={expanded ? "Collapse" : "Expand"}
        >
          <span style={badgesStyle}>
            <TierBadge tier={job.tier} />
            <OddsBadge odds={job.odds} />
          </span>
          <span style={textCol}>
            <span style={line1}>
              <span style={{ fontWeight: 600 }}>{job.company ?? "—"}</span>
              <span style={{ color: "var(--fg-faint)" }}> · </span>
              <span style={{ color: "var(--fg-dim)" }}>{job.title ?? "—"}</span>
            </span>
            <span style={line2}>
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
        <div style={expandStyle}>
          {(job.why || job.odds_why) && (
            <div className="drawer-why">
              {job.why && (
                <p>
                  <span className="why-label">Fit</span> {job.why}
                </p>
              )}
              {job.odds_why && (
                <p>
                  <span className="why-label">Odds</span> {job.odds_why}
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
        <div style={confirmStyle} onKeyDown={onConfirmKeyDown}>
          <span className="muted-sm">Did you submit?</span>
          <div style={segRowStyle}>
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
          <div style={actionsStyle}>
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
        <div style={actionsStyle}>
          <button type="button" className="btn btn-sm btn-primary" disabled={pending} onClick={startApply}>
            Apply →
          </button>
          <button type="button" className="btn btn-sm" disabled={pending} onClick={doShortlist}>
            Shortlist
          </button>
          <div style={menuWrapStyle}>
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
          <div style={menuWrapStyle}>
            <div style={passGroupStyle}>
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
      <div style={skeletonStyle}>
        <div style={skeletonBarStyle("92%")} />
        <div style={skeletonBarStyle("78%")} />
        <div style={skeletonBarStyle("85%")} />
      </div>
    );
  }
  if (detail.isError || !detail.data) {
    return <div className="muted-sm">Failed to load description.</div>;
  }
  const full: JobFull = detail.data;
  const bodyText = (full.full_desc && full.full_desc.trim()) || full.desc_snippet || "";
  return (
    <div className="jd-text" style={{ maxHeight: 360, overflow: "auto" }}>
      {highlightText(bodyText, full.skill_hits ?? [])}
    </div>
  );
}
