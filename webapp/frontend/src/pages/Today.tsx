import { useMemo, type CSSProperties } from "react";
import { useSearchParams } from "react-router-dom";
import { useJobs } from "../store/queries";
import { isActionable, oddsRank, todayISO } from "../lib/format";
import { ApplyLane } from "../components/queue/ApplyLane";
import type { JobLight } from "../api/types";

const LANE_CAP = 15;

// odds_score may be null; push nulls to the bottom of a desc sort.
function score(j: JobLight): number {
  return j.odds_score ?? Number.NEGATIVE_INFINITY;
}

// "Bank a win" — winnable first: odds rank (Likely<Target<Reach), then odds_score desc, then tier desc.
function bankCmp(a: JobLight, b: JobLight): number {
  const o = oddsRank(a.odds) - oddsRank(b.odds);
  if (o !== 0) return o;
  const s = score(b) - score(a);
  if (s !== 0) return s;
  return b.tier - a.tier;
}

// "Aim high" — fit first: tier desc (5 then 4), then odds rank, then odds_score desc.
function aimCmp(a: JobLight, b: JobLight): number {
  const t = b.tier - a.tier;
  if (t !== 0) return t;
  const o = oddsRank(a.odds) - oddsRank(b.odds);
  if (o !== 0) return o;
  return score(b) - score(a);
}

// A job counts as "done today" if it was applied / passed / snoozed today.
function actedToday(j: JobLight, today: string): boolean {
  const s = j.state;
  if (!s) return false;
  if (s.applied_date === today) return true;
  const upd = s.updated_at ? s.updated_at.slice(0, 10) : "";
  if (upd !== today) return false;
  if (s.status === "Passed") return true;
  if (s.snoozed_until && s.snoozed_until > today) return true;
  return false;
}

const pageStyle: CSSProperties = { padding: "16px 18px", display: "flex", flexDirection: "column", gap: 14 };
const headStyle: CSSProperties = { display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 12, flexWrap: "wrap" };
const countsStyle: CSSProperties = { display: "flex", gap: 16, alignItems: "baseline" };
const lanesStyle: CSSProperties = { display: "flex", gap: 14, flexWrap: "wrap", alignItems: "flex-start" };

function Stat({ label, value, tone }: { label: string; value: number; tone?: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column" }}>
      <span style={{ fontSize: 20, fontWeight: 700, color: tone ?? "var(--fg)", fontVariantNumeric: "tabular-nums" }}>
        {value}
      </span>
      <span className="muted-sm">{label}</span>
    </div>
  );
}

export default function Today() {
  const { data, isLoading, isError } = useJobs();
  const [, setParams] = useSearchParams();

  const jobs = useMemo(() => data?.jobs ?? [], [data]);
  const today = todayISO();

  const { bank, aim, actionableCount, snoozedCount, doneCount } = useMemo(() => {
    const actionable = jobs.filter(isActionable);
    const bankLane = [...actionable].sort(bankCmp).slice(0, LANE_CAP);
    const bankSet = new Set(bankLane.map((j) => j.url_b64));
    const aimLane = actionable
      .filter((j) => (j.tier === 5 || j.tier === 4) && !bankSet.has(j.url_b64))
      .sort(aimCmp)
      .slice(0, LANE_CAP);

    const snoozed = jobs.filter(
      (j) => !!j.state?.snoozed_until && j.state.snoozed_until > today && !j.state.hidden,
    ).length;
    const done = jobs.filter((j) => actedToday(j, today)).length;

    return {
      bank: bankLane,
      aim: aimLane,
      actionableCount: actionable.length,
      snoozedCount: snoozed,
      doneCount: done,
    };
  }, [jobs, today]);

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
      <div style={headStyle}>
        <div>
          <h1>Apply Queue</h1>
          <p className="muted-sm" style={{ margin: 0 }}>
            The short right set to act on today — one click to Applied, Snooze, or Pass.
          </p>
        </div>
        <div style={countsStyle}>
          <Stat label="actionable" value={actionableCount} tone="var(--accent)" />
          <Stat label="snoozed" value={snoozedCount} tone="var(--amber)" />
          <Stat label="done today" value={doneCount} tone="var(--green)" />
        </div>
      </div>

      {isError && <div className="page-error">Failed to load jobs.</div>}
      {isLoading && <p className="muted">Loading…</p>}

      {!isLoading && !isError && (
        <div style={lanesStyle}>
          <ApplyLane
            title="Bank a win"
            subtitle="odds-first — the winnable roles"
            accent="var(--green)"
            jobs={bank}
            onOpen={openJob}
          />
          <ApplyLane
            title="Aim high"
            subtitle="fit-first — tier 5 & 4 reaches"
            accent="var(--accent)"
            jobs={aim}
            onOpen={openJob}
          />
        </div>
      )}
    </div>
  );
}
