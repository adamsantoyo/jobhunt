import { useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useConfig, useJobs, usePatchState } from "../store/queries";
import { fmtDate, fmtSalary, oddsRank } from "../lib/format";
import { OddsBadge, StatusBadge, TierBadge } from "../components/StatusBadge";
import { EMPTY_FILTERS, FilterBar, type TableFilters } from "../components/FilterBar";
import { DEFAULT_STATUSES } from "../lib/statuses";
import type { JobLight } from "../api/types";

type SortKey = "tier" | "odds" | "company" | "salary_min" | "posted" | "first_seen";
type SortDir = "asc" | "desc";

// grid-template-columns shared by header + rows (see .tbl-grid in index.css).
const GRID = "32px 30px 46px 74px 1.3fr 2fr 1.2fr 100px 110px 92px 92px 96px 1.6fr";

function statusOf(job: JobLight): string {
  return job.state?.status ?? "New";
}

function cmp(a: JobLight, b: JobLight, key: SortKey): number {
  switch (key) {
    case "tier":
      return a.tier - b.tier;
    case "odds":
      return oddsRank(a.odds) - oddsRank(b.odds);
    case "company":
      return (a.company ?? "").localeCompare(b.company ?? "");
    case "salary_min": {
      const av = a.salary_min ?? a.salary_max ?? -1;
      const bv = b.salary_min ?? b.salary_max ?? -1;
      return av - bv;
    }
    case "posted":
      return (a.posted ?? "").localeCompare(b.posted ?? "");
    case "first_seen":
      return (a.first_seen ?? "").localeCompare(b.first_seen ?? "");
    default:
      return 0;
  }
}

function matches(job: JobLight, f: TableFilters): boolean {
  if (!f.includeHidden && job.state?.hidden) return false;
  if (f.tiers.length && !f.tiers.includes(job.tier)) return false;
  if (f.odds.length && !(job.odds && f.odds.includes(job.odds))) return false;
  if (f.sources.length && !(job.source && f.sources.includes(job.source))) return false;
  if (f.statuses.length && !f.statuses.includes(statusOf(job))) return false;
  if (f.remote !== null && job.remote !== f.remote) return false;
  if (f.hasDesc !== null && job.has_desc !== f.hasDesc) return false;
  if (f.minSalary != null) {
    const top = job.salary_max ?? job.salary_min ?? 0;
    if (top < f.minSalary) return false;
  }
  if (f.flagsContains.trim()) {
    const needle = f.flagsContains.trim().toLowerCase();
    if (!(job.flags ?? "").toLowerCase().includes(needle)) return false;
  }
  if (f.search.trim()) {
    const q = f.search.trim().toLowerCase();
    const hay = `${job.title ?? ""} ${job.company ?? ""} ${job.location ?? ""}`.toLowerCase();
    if (!hay.includes(q)) return false;
  }
  return true;
}

export default function TableView() {
  const { data, isLoading, isError } = useJobs();
  const { data: config } = useConfig();
  const patchState = usePatchState();
  const [, setParams] = useSearchParams();

  const [filters, setFilters] = useState<TableFilters>(EMPTY_FILTERS);
  const [sortKey, setSortKey] = useState<SortKey>("tier");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const jobs = data?.jobs ?? [];

  const sourceOptions = useMemo(() => {
    const s = new Set<string>();
    for (const j of jobs) if (j.source) s.add(j.source);
    return Array.from(s).sort();
  }, [jobs]);

  const statusOptions = config?.statuses ?? DEFAULT_STATUSES;

  const rows = useMemo(() => {
    const filtered = jobs.filter((j) => matches(j, filters));
    const dir = sortDir === "asc" ? 1 : -1;
    filtered.sort((a, b) => {
      const primary = cmp(a, b, sortKey) * dir;
      if (primary !== 0) return primary;
      // stable secondary: tier desc then company asc
      const t = b.tier - a.tier;
      if (t !== 0) return t;
      return (a.company ?? "").localeCompare(b.company ?? "");
    });
    return filtered;
  }, [jobs, filters, sortKey, sortDir]);

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 34,
    overscan: 12,
  });

  const openJob = (job: JobLight) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set("job", job.url_b64);
        return next;
      },
      { replace: false },
    );

  const onSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "company" || key === "posted" || key === "first_seen" ? "asc" : "desc");
    }
  };

  const toggleSelect = (urlB64: string) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(urlB64)) next.delete(urlB64);
      else next.add(urlB64);
      return next;
    });

  const allVisibleSelected = rows.length > 0 && rows.every((r) => selected.has(r.url_b64));
  const toggleSelectAll = () =>
    setSelected((prev) => {
      if (allVisibleSelected) {
        const next = new Set(prev);
        for (const r of rows) next.delete(r.url_b64);
        return next;
      }
      const next = new Set(prev);
      for (const r of rows) next.add(r.url_b64);
      return next;
    });

  const bulk = (patch: { starred?: boolean; hidden?: boolean }) => {
    for (const urlB64 of selected) patchState.mutate({ urlB64, patch });
    setSelected(new Set());
  };

  const SortHead = ({ label, k }: { label: string; k: SortKey }) => (
    <button type="button" className="th-sort" onClick={() => onSort(k)}>
      {label}
      {sortKey === k && <span className="sort-caret">{sortDir === "asc" ? "▲" : "▼"}</span>}
    </button>
  );

  return (
    <div className="table-view">
      <FilterBar
        filters={filters}
        onChange={setFilters}
        sourceOptions={sourceOptions}
        statusOptions={statusOptions}
      />

      <div className="table-toolbar">
        <span className="muted">
          {rows.length} of {jobs.length} jobs
          {isLoading ? " (loading…)" : ""}
        </span>
        {selected.size > 0 && (
          <div className="bulk-bar">
            <span>{selected.size} selected</span>
            <button type="button" className="btn btn-sm" onClick={() => bulk({ starred: true })}>
              Star
            </button>
            <button type="button" className="btn btn-sm" onClick={() => bulk({ starred: false })}>
              Unstar
            </button>
            <button type="button" className="btn btn-sm" onClick={() => bulk({ hidden: true })}>
              Hide
            </button>
            <button type="button" className="btn btn-sm" onClick={() => bulk({ hidden: false })}>
              Unhide
            </button>
            <button type="button" className="btn btn-sm" onClick={() => setSelected(new Set())}>
              Clear
            </button>
          </div>
        )}
      </div>

      {isError && <div className="page-error">Failed to load jobs.</div>}

      <div className="tbl">
        <div className="tbl-head tbl-grid" style={{ gridTemplateColumns: GRID }}>
          <div className="th th-check">
            <input
              type="checkbox"
              checked={allVisibleSelected}
              onChange={toggleSelectAll}
              aria-label="Select all visible"
            />
          </div>
          <div className="th">★</div>
          <div className="th">
            <SortHead label="Tier" k="tier" />
          </div>
          <div className="th">
            <SortHead label="Odds" k="odds" />
          </div>
          <div className="th">
            <SortHead label="Company" k="company" />
          </div>
          <div className="th">Title</div>
          <div className="th">Location</div>
          <div className="th">
            <SortHead label="Salary" k="salary_min" />
          </div>
          <div className="th">Source</div>
          <div className="th">
            <SortHead label="Posted" k="posted" />
          </div>
          <div className="th">
            <SortHead label="Seen" k="first_seen" />
          </div>
          <div className="th">Status</div>
          <div className="th">Flags</div>
        </div>

        <div ref={parentRef} className="tbl-body">
          <div className="tbl-inner" style={{ height: `${virtualizer.getTotalSize()}px` }}>
            {virtualizer.getVirtualItems().map((vi) => {
              const job = rows[vi.index];
              const isSel = selected.has(job.url_b64);
              return (
                <div
                  key={job.url_b64}
                  className="tbl-row tbl-grid"
                  data-selected={isSel ? "1" : "0"}
                  data-hidden={job.state?.hidden ? "1" : "0"}
                  style={{
                    gridTemplateColumns: GRID,
                    transform: `translateY(${vi.start}px)`,
                  }}
                  onClick={() => openJob(job)}
                >
                  <div className="td td-check" onClick={(e) => e.stopPropagation()}>
                    <input
                      type="checkbox"
                      checked={isSel}
                      onChange={() => toggleSelect(job.url_b64)}
                      aria-label="Select row"
                    />
                  </div>
                  <div
                    className="td td-star"
                    onClick={(e) => {
                      e.stopPropagation();
                      patchState.mutate({
                        urlB64: job.url_b64,
                        patch: { starred: !job.state?.starred },
                      });
                    }}
                    title={job.state?.starred ? "Unstar" : "Star"}
                  >
                    {job.state?.starred ? "★" : "☆"}
                  </div>
                  <div className="td">
                    <TierBadge tier={job.tier} />
                  </div>
                  <div className="td">
                    <OddsBadge odds={job.odds} />
                  </div>
                  <div className="td td-ellipsis" title={job.company ?? ""}>
                    {job.company ?? "—"}
                  </div>
                  <div className="td td-ellipsis" title={job.title ?? ""}>
                    {job.is_new && <span className="dot-new" title="new this run" />}
                    {job.title ?? "—"}
                  </div>
                  <div className="td td-ellipsis" title={job.location ?? ""}>
                    {job.remote && <span className="tag-remote">R</span>}
                    {job.location ?? "—"}
                  </div>
                  <div className="td td-ellipsis" title={fmtSalary(job)}>
                    {fmtSalary(job) || "—"}
                  </div>
                  <div className="td td-ellipsis" title={job.source ?? ""}>
                    {job.source ?? "—"}
                  </div>
                  <div className="td td-num">{fmtDate(job.posted)}</div>
                  <div className="td td-num">{fmtDate(job.first_seen)}</div>
                  <div className="td">
                    <StatusBadge status={statusOf(job)} />
                  </div>
                  <div className="td td-ellipsis" title={job.flags ?? ""}>
                    {job.flags || ""}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
