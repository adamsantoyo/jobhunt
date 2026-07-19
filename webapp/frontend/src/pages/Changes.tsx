import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useAnalytics, useChanges } from "../store/queries";
import { fmtSalary } from "../lib/format";
import { OddsBadge, TierBadge } from "../components/StatusBadge";
import { ChangeSection } from "../components/changes/ChangeSection";
import type { DisappearedJob, JobLight, TierChange } from "../api/types";

const rowStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  width: "100%",
  textAlign: "left",
  background: "var(--bg-2)",
  border: "1px solid var(--border-soft)",
  borderRadius: "var(--radius-sm)",
  color: "var(--fg)",
  padding: "5px 8px",
  font: "inherit",
  fontSize: 12,
};

function JobRow({
  job,
  onOpen,
  extra,
}: {
  job: JobLight;
  onOpen: (job: JobLight) => void;
  extra?: React.ReactNode;
}) {
  return (
    <button type="button" onClick={() => onOpen(job)} style={{ ...rowStyle, cursor: "pointer" }}>
      <span style={{ display: "flex", gap: 3, flex: "0 0 auto" }}>
        <TierBadge tier={job.tier} />
        <OddsBadge odds={job.odds} />
      </span>
      <span style={{ fontWeight: 600, flex: "0 0 auto", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={job.company ?? ""}>
        {job.company ?? "—"}
      </span>
      <span
        style={{ minWidth: 0, flex: 1, color: "var(--fg-dim)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
        title={job.title ?? ""}
      >
        {job.title ?? "—"}
      </span>
      {extra}
      <span className="muted-sm" style={{ flex: "0 0 auto", minWidth: 70, textAlign: "right" }}>
        {fmtSalary(job)}
      </span>
    </button>
  );
}

function DisappearedRow({ job }: { job: DisappearedJob }) {
  // Disappeared jobs are no longer present, so there is no drawer link for them.
  return (
    <div style={{ ...rowStyle, opacity: 0.75 }}>
      <span style={{ flex: "0 0 auto" }}>
        <TierBadge tier={job.tier} />
      </span>
      <span style={{ fontWeight: 600, flex: "0 0 auto", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={job.company ?? ""}>
        {job.company ?? "—"}
      </span>
      <span
        style={{ minWidth: 0, flex: 1, color: "var(--fg-dim)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
        title={job.title ?? ""}
      >
        {job.title ?? "—"}
      </span>
      <span
        className="muted-sm"
        style={{ flex: "0 0 auto", maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
        title={job.location ?? ""}
      >
        {job.location ?? ""}
      </span>
      <span className="muted-sm" style={{ flex: "0 0 auto" }}>
        last seen {job.last_seen}
      </span>
    </div>
  );
}

function TierArrow({ from, to }: { from: number; to: number }) {
  const up = to > from;
  return (
    <span
      style={{
        flex: "0 0 auto",
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
        color: up ? "var(--green)" : "var(--red)",
        fontWeight: 600,
        fontVariantNumeric: "tabular-nums",
      }}
    >
      T{from} <span aria-hidden>→</span> T{to}
    </span>
  );
}

export default function Changes() {
  const [params, setParams] = useSearchParams();
  const since = params.get("since") ?? undefined;

  const { data, isLoading, isError } = useChanges(since);
  const { data: analytics } = useAnalytics();

  // Candidate baseline runs come from the per-run ledger; the latest run is the
  // "current" side of the diff and can't also be the baseline.
  const runDates = useMemo(() => {
    const all = (analytics?.new_per_run ?? []).map((r) => r.run_date).sort();
    return all;
  }, [analytics]);

  const currentRun = data?.current ?? (runDates.length ? runDates[runDates.length - 1] : null);
  const baselineOptions = useMemo(
    () => runDates.filter((d) => d !== currentRun),
    [runDates, currentRun],
  );

  const openJob = (job: JobLight) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set("job", job.url_b64);
        return next;
      },
      { replace: false },
    );

  const onSelectSince = (value: string) => {
    setParams((prev) => {
      const next = new URLSearchParams(prev);
      if (value) next.set("since", value);
      else next.delete("since");
      return next;
    });
  };

  const newJobs = data?.new ?? [];
  const reposted = data?.reposted ?? [];
  const tierChanged: TierChange[] = data?.tier_changed ?? [];
  const disappeared = data?.disappeared ?? [];

  return (
    <div style={{ padding: 16, display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 12, flexWrap: "wrap" }}>
        <h1 style={{ margin: 0 }}>Changes</h1>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span className="field-label">Compare</span>
          <select
            className="input filter-select"
            value={since ?? ""}
            onChange={(e) => onSelectSince(e.target.value)}
          >
            <option value="">Previous run (auto)</option>
            {baselineOptions.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </div>
        <span className="muted">
          {data?.baseline ?? "…"} <span aria-hidden>→</span> {currentRun ?? "…"}
          {isLoading ? " (loading…)" : ""}
        </span>
      </div>

      {isError && <div className="page-error">Failed to load changes.</div>}

      <ChangeSection title="New" count={newJobs.length} accent="var(--green)">
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {newJobs.map((j) => (
            <JobRow key={j.url_b64} job={j} onOpen={openJob} />
          ))}
        </div>
      </ChangeSection>

      <ChangeSection title="Reposted" count={reposted.length} accent="var(--amber)">
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {reposted.map((j) => (
            <JobRow key={j.url_b64} job={j} onOpen={openJob} />
          ))}
        </div>
      </ChangeSection>

      <ChangeSection title="Tier changed" count={tierChanged.length} accent="var(--accent)">
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {tierChanged.map((tc) => (
            <JobRow
              key={tc.job.url_b64}
              job={tc.job}
              onOpen={openJob}
              extra={<TierArrow from={tc.from} to={tc.to} />}
            />
          ))}
        </div>
      </ChangeSection>

      <ChangeSection title="Disappeared" count={disappeared.length} accent="var(--red)">
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {disappeared.map((j) => (
            <DisappearedRow key={j.url_b64} job={j} />
          ))}
        </div>
      </ChangeSection>
    </div>
  );
}
