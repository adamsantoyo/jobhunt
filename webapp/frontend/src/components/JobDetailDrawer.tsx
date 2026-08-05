import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useConfig, useJobDetail, usePatchState, useQuickAction } from "../store/queries";
import { fmtDate, fmtSalary, flagsList, isHttpUrl } from "../lib/format";
import { highlightText } from "../lib/highlight";
import { DEFAULT_STATUSES } from "../lib/statuses";
import type { JobFull, JobState, StatePatch } from "../api/types";
import { FlagBadge, OddsBadge, TierBadge } from "./StatusBadge";

/**
 * Job detail drawer, mounted ONCE in AppShell. Opened by any view by setting the
 * `?job=<url_b64>` search param. Renders the full JD as PLAIN TEXT with skill
 * <mark> highlights, why / odds-why / flags / salary, a safe apply link, and a
 * full state editor plus quick actions.
 */
export function JobDetailDrawer() {
  const [params, setParams] = useSearchParams();
  const urlB64 = params.get("job");
  const { data: job, isLoading, isError } = useJobDetail(urlB64);

  const close = () => {
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.delete("job");
        return next;
      },
      { replace: true },
    );
  };

  // Close on Escape while open.
  useEffect(() => {
    if (!urlB64) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlB64]);

  if (!urlB64) return null;

  return (
    <div className="drawer-overlay" onClick={close}>
      <aside
        className="drawer"
        role="dialog"
        aria-modal="true"
        aria-label="Job details"
        onClick={(e) => e.stopPropagation()}
      >
        {isLoading && <div className="drawer-loading">Loading…</div>}
        {isError && <div className="drawer-loading">Failed to load job.</div>}
        {job && <DrawerContent job={job} onClose={close} />}
      </aside>
    </div>
  );
}

function DrawerContent({ job, onClose }: { job: JobFull; onClose: () => void }) {
  const { data: config } = useConfig();
  const patchState = usePatchState();
  const quick = useQuickAction();

  const statuses = config?.statuses ?? DEFAULT_STATUSES;
  const skills = useMemo(() => {
    const set = new Set<string>();
    for (const s of config?.skills ?? []) set.add(s);
    for (const s of job.skill_hits ?? []) set.add(s);
    return Array.from(set);
  }, [config?.skills, job.skill_hits]);

  const bodyText = (job.full_desc && job.full_desc.trim()) || job.desc_snippet || "";
  const flags = flagsList(job.flags);
  const starred = job.state?.starred ?? false;

  const doPatch = (patch: StatePatch) => patchState.mutate({ urlB64: job.url_b64, patch });

  return (
    <>
      <header className="drawer-head">
        <div className="drawer-head-main">
          <div className="drawer-badges">
            <TierBadge tier={job.tier} />
            <OddsBadge odds={job.odds} />
            {job.odds_score != null && <span className="muted-sm">score {job.odds_score}</span>}
            {job.is_new && <span className="badge new">NEW</span>}
            {job.remote && <span className="badge remote">Remote</span>}
          </div>
          <h2 className="drawer-title">{job.title ?? "—"}</h2>
          <div className="drawer-sub">
            <span className="drawer-company">{job.company ?? "—"}</span>
            {job.location && <span className="drawer-loc"> · {job.location}</span>}
          </div>
          <div className="drawer-meta">
            {fmtSalary(job) && <span className="drawer-salary">{fmtSalary(job)}</span>}
            {job.source && <span className="muted-sm">{job.source}</span>}
            {job.posted && <span className="muted-sm">posted {fmtDate(job.posted)}</span>}
            {job.first_seen && <span className="muted-sm">first seen {fmtDate(job.first_seen)}</span>}
            {job.req_id && <span className="muted-sm">req {job.req_id}</span>}
          </div>
        </div>
        <button type="button" className="btn btn-icon" onClick={onClose} aria-label="Close">
          ✕
        </button>
      </header>

      {/* quick actions */}
      <div className="drawer-actions">
        <button
          type="button"
          className="btn btn-primary"
          onClick={() => quick.mutate({ urlB64: job.url_b64, body: { action: "applied" } })}
        >
          Applied
        </button>
        <button
          type="button"
          className="btn"
          onClick={() => quick.mutate({ urlB64: job.url_b64, body: { action: "snooze", days: 3 } })}
        >
          Snooze 3d
        </button>
        <button
          type="button"
          className="btn"
          onClick={() => quick.mutate({ urlB64: job.url_b64, body: { action: "pass" } })}
        >
          Pass
        </button>
        <button
          type="button"
          className="btn"
          onClick={() =>
            quick.mutate({ urlB64: job.url_b64, body: { action: starred ? "unstar" : "star" } })
          }
        >
          {starred ? "★ Unstar" : "☆ Star"}
        </button>
        {isHttpUrl(job.url) ? (
          <a className="btn btn-link" href={job.url} target="_blank" rel="noopener noreferrer">
            Open posting ↗
          </a>
        ) : (
          <span className="muted-sm">no apply link</span>
        )}
      </div>

      {job.state?.needs_review && (
        <div className="drawer-review">
          ⚠ Needs review{job.state.review_reason ? `: ${job.state.review_reason}` : ""}
        </div>
      )}

      {/* why / odds-why */}
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

      {/* state editor */}
      <StateEditor job={job} statuses={statuses} onPatch={doPatch} />

      {/* JD body: plain text with skill highlights, never raw HTML */}
      <section className="drawer-jd">
        <div className="drawer-jd-head">
          Description {job.full_desc ? "" : "(snippet)"}
        </div>
        <div className="jd-text">{highlightText(bodyText, skills)}</div>
      </section>
    </>
  );
}

function StateEditor({
  job,
  statuses,
  onPatch,
}: {
  job: JobFull;
  statuses: string[];
  onPatch: (patch: StatePatch) => void;
}) {
  const state: JobState | null = job.state;
  const [notes, setNotes] = useState(state?.notes ?? "");
  const [contact, setContact] = useState(state?.contact ?? "");

  // Re-sync text fields when switching to a different job.
  useEffect(() => {
    setNotes(state?.notes ?? "");
    setContact(state?.contact ?? "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [job.url_b64]);

  return (
    <section className="drawer-state">
      <div className="field-row">
        <label className="field">
          <span className="field-label">Status</span>
          <select
            className="input"
            value={state?.status ?? "New"}
            onChange={(e) => onPatch({ status: e.target.value })}
          >
            {statuses.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          <span className="field-label">Follow-up</span>
          <input
            className="input"
            type="date"
            value={state?.follow_up_date ?? ""}
            onChange={(e) => onPatch({ follow_up_date: e.target.value || null })}
          />
        </label>
        <label className="field">
          <span className="field-label">Applied</span>
          <input
            className="input"
            type="date"
            value={state?.applied_date ?? ""}
            onChange={(e) => onPatch({ applied_date: e.target.value || null })}
          />
        </label>
      </div>

      <label className="field">
        <span className="field-label">Contact</span>
        <input
          className="input"
          type="text"
          value={contact}
          onChange={(e) => setContact(e.target.value)}
          onBlur={() => {
            if (contact !== (state?.contact ?? "")) onPatch({ contact });
          }}
          placeholder="recruiter / referral"
        />
      </label>

      <label className="field">
        <span className="field-label">Notes</span>
        <textarea
          className="input"
          rows={4}
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
          onBlur={() => {
            if (notes !== (state?.notes ?? "")) onPatch({ notes });
          }}
        />
      </label>

      <div className="field-inline">
        <label className="check">
          <input
            type="checkbox"
            checked={state?.hidden ?? false}
            onChange={(e) => onPatch({ hidden: e.target.checked })}
          />
          Hidden
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={state?.starred ?? false}
            onChange={(e) => onPatch({ starred: e.target.checked })}
          />
          Starred
        </label>
        {state?.snoozed_until && (
          <span className="muted-sm">snoozed until {fmtDate(state.snoozed_until)}</span>
        )}
        {state?.updated_at && (
          <span className="muted-sm">updated {fmtDate(state.updated_at)}</span>
        )}
      </div>
    </section>
  );
}
