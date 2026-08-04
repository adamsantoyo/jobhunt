import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useChanges, useCompanies, useConfig, useJobs, usePatchState } from "../store/queries";
import { fmtDate, fmtSalary, oddsRank, parseOdds } from "../lib/format";
import { OddsBadge, StatusBadge, TierBadge } from "../components/StatusBadge";
import { EMPTY_FILTERS, FilterBar, type TableFilters } from "../components/FilterBar";
import { DEFAULT_STATUSES, statusOf } from "../lib/statuses";
import { MatrixFilter } from "../components/explore/MatrixFilter";
import { DiffBar, type DiffKind } from "../components/explore/DiffBar";
import { CompanyGroups } from "../components/explore/CompanyGroups";
import { DisappearedList } from "../components/explore/DisappearedList";
import type { JobLight } from "../api/types";

// Matrix + Table + Companies + Changes, collapsed into one view built on the
// Table engine (see plans/phase2-spec.md "Design contract for /explore").
// Flat-mode sort/filter/row anatomy below is ported as-is from the retired
// TableView.tsx.

type SortKey = "tier" | "odds" | "company" | "salary_min" | "posted" | "first_seen";
type SortDir = "asc" | "desc";

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
  if (f.match.length || f.competition.length) {
    const { match, competition } = parseOdds(job.odds);
    if (f.match.length && !(match && f.match.includes(match))) return false;
    if (f.competition.length && !(competition && f.competition.includes(competition))) return false;
  }
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

function TierArrow({ from, to }: { from: number; to: number }) {
  const up = to > from;
  return (
    <span className="ex-tier-arrow" style={{ color: up ? "var(--green)" : "var(--red)" }}>
      T{from} <span aria-hidden>→</span> T{to}
    </span>
  );
}

export default function Explore() {
  const { data, isLoading, isError } = useJobs();
  const { data: config } = useConfig();
  const { data: companyStates } = useCompanies();
  const patchState = usePatchState();
  const [searchParams, setSearchParams] = useSearchParams();

  const [filters, setFilters] = useState<TableFilters>(EMPTY_FILTERS);
  const [sortKey, setSortKey] = useState<SortKey>("tier");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const group = searchParams.get("group") === "company";
  const diff = (searchParams.get("diff") as DiffKind | null) ?? null;
  const since = searchParams.get("since") ?? undefined;

  const { data: changes, isLoading: changesLoading } = useChanges(since);

  const jobs = data?.jobs ?? [];

  const sourceOptions = useMemo(() => {
    const s = new Set<string>();
    for (const j of jobs) if (j.source) s.add(j.source);
    return Array.from(s).sort();
  }, [jobs]);

  const statusOptions = config?.statuses ?? DEFAULT_STATUSES;

  // Diff sets: url_b64 membership for new/reposted, and a from->to map for
  // tier changes. `gone` is not a row-level filter over /api/jobs -- those
  // jobs aren't in the jobs list at all, so it swaps the whole body instead.
  const diffSets = useMemo(() => {
    const newSet = new Set((changes?.new ?? []).map((j) => j.url_b64));
    const repostedSet = new Set((changes?.reposted ?? []).map((j) => j.url_b64));
    const tierMap = new Map((changes?.tier_changed ?? []).map((tc) => [tc.job.url_b64, tc]));
    return { newSet, repostedSet, tierMap };
  }, [changes]);

  const diffMatches = (job: JobLight): boolean => {
    if (!diff || diff === "gone") return true;
    if (diff === "new") return diffSets.newSet.has(job.url_b64);
    if (diff === "reposted") return diffSets.repostedSet.has(job.url_b64);
    return diffSets.tierMap.has(job.url_b64);
  };

  // Facet base for MatrixFilter: every current filter EXCEPT tiers/competition
  // (the matrix's own two axes -- match is a separate FilterBar chip group and
  // stays applied), still intersected with the active diff chip (a diff chip
  // is just another filter, per the design contract's diff-filtering
  // semantics).
  const facetJobs = useMemo(() => {
    const f: TableFilters = { ...filters, tiers: [], competition: [] };
    return jobs.filter((j) => matches(j, f) && diffMatches(j));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobs, filters, diff, diffSets]);

  const filteredRows = useMemo(() => {
    return jobs.filter((j) => matches(j, filters) && diffMatches(j));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobs, filters, diff, diffSets]);

  const rows = useMemo(() => {
    const sorted = [...filteredRows];
    const dir = sortDir === "asc" ? 1 : -1;
    sorted.sort((a, b) => {
      const primary = cmp(a, b, sortKey) * dir;
      if (primary !== 0) return primary;
      // stable secondary: tier desc then company asc
      const t = b.tier - a.tier;
      if (t !== 0) return t;
      return (a.company ?? "").localeCompare(b.company ?? "");
    });
    return sorted;
  }, [filteredRows, sortKey, sortDir]);

  // Bulk selection only exists in flat mode; entering group mode clears it.
  useEffect(() => {
    if (group) setSelected(new Set());
  }, [group]);

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 34,
    overscan: 12,
  });

  const openJob = (job: JobLight) =>
    setSearchParams(
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

  const setGroup = (on: boolean) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (on) next.set("group", "company");
      else next.delete("group");
      return next;
    });

  const setDiff = (d: DiffKind | null) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (d) next.set("diff", d);
      else next.delete("diff");
      return next;
    });

  const setSince = (v: string) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (v) next.set("since", v);
      else next.delete("since");
      return next;
    });

  const SortHead = ({ label, k }: { label: string; k: SortKey }) => (
    <button type="button" className="th-sort" onClick={() => onSort(k)}>
      {label}
      {sortKey === k && <span className="sort-caret">{sortDir === "asc" ? "▲" : "▼"}</span>}
    </button>
  );

  const disappeared = changes?.disappeared ?? [];
  // A diff chip other than "gone" filters /api/jobs rows by a changes.* set;
  // while that set hasn't loaded yet, show a loading state instead of an
  // (incorrectly) empty table.
  const changesPending = changesLoading && !changes;
  const diffPending = !!diff && diff !== "gone" && changesPending;

  let body: ReactNode;
  if (diff === "gone") {
    body = changesPending ? (
      <p className="muted ex-loading-note">Loading…</p>
    ) : (
      <DisappearedList jobs={disappeared} />
    );
  } else if (diffPending) {
    body = (
      <p className="muted ex-loading-note">Loading…</p>
    );
  } else if (group) {
    body = <CompanyGroups jobs={filteredRows} companyStates={companyStates} onOpen={openJob} />;
  } else {
    body = (
      <div className="tbl">
        <div className="tbl-head tbl-grid ex-grid-cols">
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
              const tierChange = diff === "tier" ? diffSets.tierMap.get(job.url_b64) : undefined;
              return (
                <div
                  key={job.url_b64}
                  className="tbl-row tbl-grid ex-grid-cols"
                  data-selected={isSel ? "1" : "0"}
                  data-hidden={job.state?.hidden ? "1" : "0"}
                  style={{ transform: `translateY(${vi.start}px)` }}
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
                    {tierChange && <TierArrow from={tierChange.from} to={tierChange.to} />}
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
    );
  }

  return (
    <div className="explore-page">
      <MatrixFilter facetJobs={facetJobs} filters={filters} onChange={setFilters} />

      <DiffBar
        diff={diff}
        since={since}
        changes={changes}
        changesLoading={changesLoading}
        onSelectDiff={setDiff}
        onSelectSince={setSince}
      />

      <FilterBar
        filters={filters}
        onChange={setFilters}
        sourceOptions={sourceOptions}
        statusOptions={statusOptions}
      />

      <div className="table-toolbar">
        <span className="muted">
          {diff === "gone" ? `${disappeared.length} disappeared` : `${rows.length} of ${jobs.length} jobs`}
          {isLoading || diffPending || (diff === "gone" && changesPending) ? " (loading…)" : ""}
        </span>

        <div className="explore-toolbar-group">
          <span className="filter-label">Group</span>
          <button
            type="button"
            className="chip-toggle"
            data-on={!group ? "1" : "0"}
            onClick={() => setGroup(false)}
          >
            none
          </button>
          <button
            type="button"
            className="chip-toggle"
            data-on={group ? "1" : "0"}
            onClick={() => setGroup(true)}
          >
            company
          </button>
        </div>

        {!group && diff !== "gone" && selected.size > 0 && (
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

      {body}
    </div>
  );
}
