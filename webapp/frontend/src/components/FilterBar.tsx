// Filter controls for the Table view. The filter shape is exported so the page
// owns the state and passes it down; FilterBar is a controlled component.

export interface TableFilters {
  tiers: number[];
  odds: string[];
  sources: string[];
  statuses: string[];
  remote: boolean | null; // null = any
  minSalary: number | null;
  flagsContains: string;
  hasDesc: boolean | null; // null = any
  search: string;
  includeHidden: boolean;
}

export const EMPTY_FILTERS: TableFilters = {
  tiers: [],
  odds: [],
  sources: [],
  statuses: [],
  remote: null,
  minSalary: null,
  flagsContains: "",
  hasDesc: null,
  search: "",
  includeHidden: false,
};

const ALL_TIERS = [5, 4, 3, 2, 1];
const ALL_ODDS = ["Likely", "Target", "Reach"];

function toggle<T>(arr: T[], value: T): T[] {
  return arr.includes(value) ? arr.filter((v) => v !== value) : [...arr, value];
}

export function FilterBar({
  filters,
  onChange,
  sourceOptions,
  statusOptions,
}: {
  filters: TableFilters;
  onChange: (next: TableFilters) => void;
  sourceOptions: string[];
  statusOptions: string[];
}) {
  const set = (patch: Partial<TableFilters>) => onChange({ ...filters, ...patch });

  return (
    <div className="filter-bar">
      <div className="filter-group">
        <input
          className="input filter-search"
          type="search"
          placeholder="Search title / company / location…"
          value={filters.search}
          onChange={(e) => set({ search: e.target.value })}
        />
      </div>

      <div className="filter-group">
        <span className="filter-label">Tier</span>
        {ALL_TIERS.map((t) => (
          <button
            key={t}
            type="button"
            className="chip-toggle"
            data-on={filters.tiers.includes(t) ? "1" : "0"}
            onClick={() => set({ tiers: toggle(filters.tiers, t) })}
          >
            {t}
          </button>
        ))}
      </div>

      <div className="filter-group">
        <span className="filter-label">Odds</span>
        {ALL_ODDS.map((o) => (
          <button
            key={o}
            type="button"
            className="chip-toggle"
            data-on={filters.odds.includes(o) ? "1" : "0"}
            onClick={() => set({ odds: toggle(filters.odds, o) })}
          >
            {o}
          </button>
        ))}
      </div>

      <div className="filter-group">
        <span className="filter-label">Source</span>
        <select
          className="input filter-select"
          value=""
          onChange={(e) => {
            if (e.target.value) set({ sources: toggle(filters.sources, e.target.value) });
          }}
        >
          <option value="">
            {filters.sources.length ? `${filters.sources.length} selected` : "any"}
          </option>
          {sourceOptions.map((s) => (
            <option key={s} value={s}>
              {filters.sources.includes(s) ? "✓ " : ""}
              {s}
            </option>
          ))}
        </select>
        {filters.sources.length > 0 && (
          <button type="button" className="chip-toggle" onClick={() => set({ sources: [] })}>
            clear
          </button>
        )}
      </div>

      <div className="filter-group">
        <span className="filter-label">Status</span>
        <select
          className="input filter-select"
          value=""
          onChange={(e) => {
            if (e.target.value) set({ statuses: toggle(filters.statuses, e.target.value) });
          }}
        >
          <option value="">
            {filters.statuses.length ? `${filters.statuses.length} selected` : "any"}
          </option>
          {statusOptions.map((s) => (
            <option key={s} value={s}>
              {filters.statuses.includes(s) ? "✓ " : ""}
              {s}
            </option>
          ))}
        </select>
        {filters.statuses.length > 0 && (
          <button type="button" className="chip-toggle" onClick={() => set({ statuses: [] })}>
            clear
          </button>
        )}
      </div>

      <div className="filter-group">
        <span className="filter-label">Remote</span>
        <select
          className="input filter-select"
          value={filters.remote === null ? "" : filters.remote ? "yes" : "no"}
          onChange={(e) =>
            set({ remote: e.target.value === "" ? null : e.target.value === "yes" })
          }
        >
          <option value="">any</option>
          <option value="yes">yes</option>
          <option value="no">no</option>
        </select>
      </div>

      <div className="filter-group">
        <span className="filter-label">Min $</span>
        <input
          className="input filter-num"
          type="number"
          placeholder="0"
          value={filters.minSalary ?? ""}
          onChange={(e) =>
            set({ minSalary: e.target.value ? parseInt(e.target.value, 10) : null })
          }
        />
      </div>

      <div className="filter-group">
        <span className="filter-label">Flags</span>
        <input
          className="input filter-num"
          type="text"
          placeholder="contains…"
          value={filters.flagsContains}
          onChange={(e) => set({ flagsContains: e.target.value })}
        />
      </div>

      <div className="filter-group">
        <span className="filter-label">Desc</span>
        <select
          className="input filter-select"
          value={filters.hasDesc === null ? "" : filters.hasDesc ? "yes" : "no"}
          onChange={(e) =>
            set({ hasDesc: e.target.value === "" ? null : e.target.value === "yes" })
          }
        >
          <option value="">any</option>
          <option value="yes">has desc</option>
          <option value="no">snippet only</option>
        </select>
      </div>

      <div className="filter-group">
        <label className="check">
          <input
            type="checkbox"
            checked={filters.includeHidden}
            onChange={(e) => set({ includeHidden: e.target.checked })}
          />
          Show hidden
        </label>
      </div>

      <div className="filter-group">
        <button type="button" className="btn btn-sm" onClick={() => onChange({ ...EMPTY_FILTERS })}>
          Reset
        </button>
      </div>
    </div>
  );
}
