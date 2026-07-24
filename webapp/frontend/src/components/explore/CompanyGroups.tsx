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
          <div key={g.company} className="ex-group-card">
            <button
              type="button"
              onClick={() => toggle(g.company)}
              className="ex-group-toggle"
            >
              <span className="muted ex-group-caret">
                {isOpen ? "▼" : "▶"}
              </span>
              <span className="ex-group-badges">
                <TierBadge tier={g.maxTier} />
                <OddsBadge odds={g.bestOdds} />
              </span>
              <span className="ex-group-name">
                {g.company}
              </span>
              <span className="muted-sm ex-group-meta">
                {g.openCount} open · {g.jobs.length} role{g.jobs.length === 1 ? "" : "s"}
              </span>
            </button>

            {isOpen && (
              <div className="ex-group-body">
                <div className="ex-role-list">
                  {g.jobs.map((job) => (
                    <button
                      key={job.url_b64}
                      type="button"
                      onClick={() => onOpen(job)}
                      className="ex-role-row"
                    >
                      <span className="ex-role-badges">
                        <TierBadge tier={job.tier} />
                        <OddsBadge odds={job.odds} />
                      </span>
                      {job.state?.starred && (
                        <span className="ex-role-star">★</span>
                      )}
                      <span
                        className="ex-role-title"
                        title={job.title ?? ""}
                      >
                        {job.is_new && <span className="dot-new" title="new this run" />}
                        {job.title ?? "—"}
                      </span>
                      <span
                        className="muted ex-role-location"
                        title={job.location ?? ""}
                      >
                        {job.remote && <span className="tag-remote">R</span>}
                        {job.location ?? ""}
                      </span>
                      <span className="muted-sm ex-role-salary">
                        {fmtSalary(job)}
                      </span>
                      <span className="ex-flex-none">
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
        <p className="muted ex-empty-note">
          No companies match.
        </p>
      )}
    </div>
  );
}
