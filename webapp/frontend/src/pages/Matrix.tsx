import { useMemo, type CSSProperties } from "react";
import { useSearchParams } from "react-router-dom";
import { useJobs } from "../store/queries";
import { MatrixCell } from "../components/matrix/MatrixCell";
import { OddsBadge, TierBadge } from "../components/StatusBadge";
import type { JobLight } from "../api/types";

const TIERS = [5, 4, 3, 2, 1] as const;
const ODDS = ["Likely", "Target", "Reach"] as const;

// Corner labels per contract: tier5xLikely = apply today, tier5xReach = aspirational.
function cornerLabel(tier: number, odds: string): string | undefined {
  if (tier === 5 && odds === "Likely") return "apply today";
  if (tier === 5 && odds === "Reach") return "aspirational";
  return undefined;
}

// Within a cell: starred first, then odds_score desc, then company.
function cellSort(a: JobLight, b: JobLight): number {
  const sa = a.state?.starred ? 1 : 0;
  const sb = b.state?.starred ? 1 : 0;
  if (sa !== sb) return sb - sa;
  const os = (b.odds_score ?? Number.NEGATIVE_INFINITY) - (a.odds_score ?? Number.NEGATIVE_INFINITY);
  if (os !== 0) return os;
  return (a.company ?? "").localeCompare(b.company ?? "");
}

const pageStyle: CSSProperties = { padding: "16px 18px", display: "flex", flexDirection: "column", gap: 12 };
const gridStyle: CSSProperties = {
  display: "grid",
  gridTemplateColumns: "58px repeat(3, minmax(0, 1fr))",
  gap: 8,
  alignItems: "stretch",
};
const colHeadStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  padding: "4px 0",
};
const rowHeadStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
};

export default function Matrix() {
  const { data, isLoading, isError } = useJobs();
  const [, setParams] = useSearchParams();

  const jobs = useMemo(() => data?.jobs ?? [], [data]);

  // Group non-hidden jobs by "tier|odds"; sort each bucket once.
  const buckets = useMemo(() => {
    const map = new Map<string, JobLight[]>();
    for (const j of jobs) {
      if (j.state?.hidden) continue;
      if (!j.odds || !(ODDS as readonly string[]).includes(j.odds)) continue;
      const key = `${j.tier}|${j.odds}`;
      const arr = map.get(key);
      if (arr) arr.push(j);
      else map.set(key, [j]);
    }
    for (const arr of map.values()) arr.sort(cellSort);
    return map;
  }, [jobs]);

  const openJob = (job: JobLight) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set("job", job.url_b64);
        return next;
      },
      { replace: false },
    );

  return (
    <div style={pageStyle}>
      <div>
        <h1>Fit × Odds Matrix</h1>
        <p className="muted-sm" style={{ margin: 0 }}>
          Tier (fit) down, odds across. Top-left is where you apply today; bottom-right is aspirational.
        </p>
      </div>

      {isError && <div className="page-error">Failed to load jobs.</div>}
      {isLoading && <p className="muted">Loading…</p>}

      {!isLoading && !isError && (
        <div style={gridStyle}>
          {/* header row: empty corner + odds column labels */}
          <div />
          {ODDS.map((odds) => (
            <div key={`h-${odds}`} style={colHeadStyle}>
              <OddsBadge odds={odds} />
            </div>
          ))}

          {/* tier rows */}
          {TIERS.map((tier) => (
            <MatrixRow
              key={tier}
              tier={tier}
              rowHeadStyle={rowHeadStyle}
              buckets={buckets}
              onOpen={openJob}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function MatrixRow({
  tier,
  rowHeadStyle,
  buckets,
  onOpen,
}: {
  tier: number;
  rowHeadStyle: CSSProperties;
  buckets: Map<string, JobLight[]>;
  onOpen: (job: JobLight) => void;
}) {
  return (
    <>
      <div style={rowHeadStyle}>
        <TierBadge tier={tier} />
      </div>
      {ODDS.map((odds) => (
        <MatrixCell
          key={`${tier}-${odds}`}
          jobs={buckets.get(`${tier}|${odds}`) ?? []}
          onOpen={onOpen}
          cornerLabel={cornerLabel(tier, odds)}
        />
      ))}
    </>
  );
}
