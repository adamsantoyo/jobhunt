import { useMemo, useState } from "react";
import { fmtSalary, isActionable, oddsRank } from "../../lib/format";
import { statusOf } from "../../lib/statuses";
import { OddsBadge, StatusBadge, TierBadge } from "../StatusBadge";
import { CompanyEditor } from "../companies/CompanyEditor";
import type { CompanyState, JobLight } from "../../api/types";

interface Group {
  company: string;
  jobs: JobLight[];
  openCount: number;
  maxTier: number;
  bestOdds: string | null;
}

// Ported as-is from the retired Companies.tsx: key = trimmed company (or
// "(unknown)"), per-group maxTier / bestOdds (oddsRank) / openCount
// (isActionable); roles sorted tier desc -> oddsRank -> title; groups sorted
// maxTier desc -> openCount desc -> name.
function buildGroups(jobs: JobLight[]): Group[] {
  const map = new Map<string, JobLight[]>();
  for (const j of jobs) {
    const key = (j.company ?? "").trim() || "(unknown)";
    const arr = map.get(key);
    if (arr) arr.push(j);
    else map.set(key, [j]);
  }

  const groups: Group[] = [];
  for (const [company, list] of map) {
    let maxTier = 0;
    let bestRank = 99;
    let bestOdds: string | null = null;
    let openCount = 0;
    for (const j of list) {
      if (j.tier > maxTier) maxTier = j.tier;
      const r = oddsRank(j.odds);
      if (r < bestRank) {
        bestRank = r;
        bestOdds = j.odds ?? null;
      }
      if (isActionable(j)) openCount += 1;
    }
    list.sort(
      (a, b) =>
        b.tier - a.tier ||
        oddsRank(a.odds) - oddsRank(b.odds) ||
        (a.title ?? "").localeCompare(b.title ?? ""),
    );
    groups.push({ company, jobs: list, openCount, maxTier, bestOdds });
  }

  groups.sort(
    (a, b) =>
      b.maxTier - a.maxTier ||
      b.openCount - a.openCount ||
      a.company.localeCompare(b.company),
  );
  return groups;
}

export function CompanyGroups({
  jobs,
  companyStates,
  onOpen,
}: {
  jobs: JobLight[];
  companyStates: CompanyState[] | undefined;
  onOpen: (job: JobLight) => void;
}) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const groups = useMemo(() => buildGroups(jobs), [jobs]);

  const stateByCompany = useMemo(() => {
    const m = new Map<string, CompanyState>();
    for (const c of companyStates ?? []) m.set(c.company, c);
    return m;
  }, [companyStates]);

  const toggle = (company: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(company)) next.delete(company);
      else next.add(company);
      return next;
    });

  return (
    <div className="explore-groups">
      {groups.map((g) => {
        const isOpen = expanded.has(g.company);
        return (
          <div
            key={g.company}
            style={{
              border: "1px solid var(--border)",
              borderRadius: "var(--radius)",
              background: "var(--bg-1)",
              overflow: "hidden",
            }}
          >
            <button
              type="button"
              onClick={() => toggle(g.company)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                width: "100%",
                textAlign: "left",
                background: "transparent",
                border: "none",
                color: "var(--fg)",
                cursor: "pointer",
                padding: "9px 12px",
                font: "inherit",
              }}
            >
              <span className="muted" style={{ width: 12, flex: "0 0 auto", fontSize: 10 }}>
                {isOpen ? "▼" : "▶"}
              </span>
              <span style={{ display: "flex", gap: 4, flex: "0 0 auto" }}>
                <TierBadge tier={g.maxTier} />
                <OddsBadge odds={g.bestOdds} />
              </span>
              <span
                style={{
                  fontWeight: 600,
                  minWidth: 0,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {g.company}
              </span>
              <span className="muted-sm" style={{ marginLeft: "auto", flex: "0 0 auto" }}>
                {g.openCount} open · {g.jobs.length} role{g.jobs.length === 1 ? "" : "s"}
              </span>
            </button>

            {isOpen && (
              <div
                style={{
                  display: "flex",
                  flexDirection: "column",
                  gap: 12,
                  padding: "4px 12px 12px",
                  borderTop: "1px solid var(--border-soft)",
                }}
              >
                <div style={{ display: "flex", flexDirection: "column", gap: 3, marginTop: 8 }}>
                  {g.jobs.map((job) => (
                    <button
                      key={job.url_b64}
                      type="button"
                      onClick={() => onOpen(job)}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 8,
                        width: "100%",
                        textAlign: "left",
                        background: "var(--bg-2)",
                        border: "1px solid var(--border-soft)",
                        borderRadius: "var(--radius-sm)",
                        color: "var(--fg)",
                        cursor: "pointer",
                        padding: "5px 8px",
                        font: "inherit",
                        fontSize: 12,
                      }}
                    >
                      <span style={{ display: "flex", gap: 3, flex: "0 0 auto" }}>
                        <TierBadge tier={job.tier} />
                        <OddsBadge odds={job.odds} />
                      </span>
                      {job.state?.starred && (
                        <span style={{ color: "var(--amber)", flex: "0 0 auto" }}>★</span>
                      )}
                      <span
                        style={{
                          minWidth: 0,
                          flex: 1,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                        title={job.title ?? ""}
                      >
                        {job.is_new && <span className="dot-new" title="new this run" />}
                        {job.title ?? "—"}
                      </span>
                      <span
                        className="muted"
                        style={{
                          flex: "0 0 auto",
                          maxWidth: 180,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                        title={job.location ?? ""}
                      >
                        {job.remote && <span className="tag-remote">R</span>}
                        {job.location ?? ""}
                      </span>
                      <span className="muted-sm" style={{ flex: "0 0 auto", minWidth: 70, textAlign: "right" }}>
                        {fmtSalary(job)}
                      </span>
                      <span style={{ flex: "0 0 auto" }}>
                        <StatusBadge status={statusOf(job)} />
                      </span>
                    </button>
                  ))}
                </div>

                <CompanyEditor company={g.company} state={stateByCompany.get(g.company)} />
              </div>
            )}
          </div>
        );
      })}
      {groups.length === 0 && (
        <p className="muted" style={{ padding: 12 }}>
          No companies match.
        </p>
      )}
    </div>
  );
}
