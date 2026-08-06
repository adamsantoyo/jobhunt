import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  Legend,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useAnalytics, useActivity, useConfig, useFunnel, useRankingMetrics } from "../store/queries";
import { ApiError } from "../api/client";
import { ChartCard, StatCard } from "../components/progress/panels";
import { ChartTooltip } from "../components/progress/ChartTooltip";
import { HeatMatrix } from "../components/progress/HeatMatrix";
import { SourceOpsPanel } from "../components/progress/SourceOpsPanel";
import { COMPETITION_COLORS, kfmt, nfmt, TICK, TIER_COLORS, VIZ } from "../components/progress/theme";
import { fmtDate } from "../lib/format";
import type { FunnelTotals, StageConversion, TimeInStage, SourceYieldCell } from "../api/types";

// Progress = four lenses on "how is this search going": My funnel (this
// applicant's own event-log-derived pipeline, computed client-side from
// useFunnel/useActivity/useConfig), Market (the existing scrape-wide charts,
// ported unchanged from the old /analytics dashboard), Sources (Phase 4.4's
// canonical-run source health panel, empty-stated until cutover), and
// Quality (Phase 5.5's ranking-quality panel over GET /api/ranking/metrics).

type ProgressTab = "funnel" | "market" | "sources" | "quality";

const STATUS_ORDER = [
  "New",
  "Interested",
  "Applied",
  "Phone screen",
  "Interview",
  "Offer",
  "Rejected",
  "Passed",
];
// /api/analytics buckets `odds` by the competition axis only (Phase 3.5's
// combined "<match> / <competition>" label, collapsed server-side by
// analytics._competition_of) -- 3 columns, not the 15-cell match x
// competition cross product.
const COMPETITION_ORDER = ["High competition", "Standard", "Lower bar"];

const gridProps = { stroke: VIZ.grid, strokeWidth: 1 } as const;
const axisLine = { stroke: VIZ.axis } as const;

/** Rounded whole-percent string, or the honest placeholder when unknown. */
function pctOrDash(rate: number | null): string {
  return rate == null ? "-" : `${Math.round(rate * 100)}%`;
}

function rateAccent(rate: number | null): string {
  if (rate == null) return VIZ.mute;
  if (rate >= 0.25) return VIZ.green;
  if (rate >= 0.1) return VIZ.amber;
  return VIZ.red;
}

/** Weeks remaining to `deadline` (ISO date), ceil, floored at 0. Local-naive,
 * matching the whole-day semantics the backend's week math uses elsewhere. */
function weeksRemaining(deadline: string | undefined): number {
  if (!deadline) return 0;
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(deadline);
  if (!m) return 0;
  const dl = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  // Round before ceil: a DST hour inside the span must not push an exact
  // N-week gap up to N+1.
  const diffDays = Math.round((dl.getTime() - today.getTime()) / 86_400_000);
  return Math.max(0, Math.ceil(diffDays / 7));
}

export default function Progress() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get("tab");
  const tab: ProgressTab =
    tabParam === "market"
      ? "market"
      : tabParam === "sources"
        ? "sources"
        : tabParam === "quality"
          ? "quality"
          : "funnel";

  const setTab = (next: ProgressTab) => {
    setSearchParams(
      (prev) => {
        const params = new URLSearchParams(prev);
        params.set("tab", next);
        return params;
      },
      { replace: true },
    );
  };

  return (
    <div className="an-page">
      <h1>Progress</h1>
      <div className="an-tabs">
        <button
          type="button"
          className="chip-toggle"
          data-on={tab === "funnel" ? "1" : "0"}
          onClick={() => setTab("funnel")}
        >
          My funnel
        </button>
        <button
          type="button"
          className="chip-toggle"
          data-on={tab === "market" ? "1" : "0"}
          onClick={() => setTab("market")}
        >
          Market
        </button>
        <button
          type="button"
          className="chip-toggle"
          data-on={tab === "sources" ? "1" : "0"}
          onClick={() => setTab("sources")}
        >
          Sources
        </button>
        <button
          type="button"
          className="chip-toggle"
          data-on={tab === "quality" ? "1" : "0"}
          onClick={() => setTab("quality")}
        >
          Quality
        </button>
      </div>

      {tab === "funnel" ? (
        <FunnelTab />
      ) : tab === "market" ? (
        <MarketTab />
      ) : tab === "sources" ? (
        <SourceOpsPanel />
      ) : (
        <QualityTab />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab 1 — My funnel
// ---------------------------------------------------------------------------

function FunnelTab() {
  const { data: funnel, isLoading, isError } = useFunnel();
  const { data: activity } = useActivity();
  const { data: config } = useConfig();
  const { data: analytics } = useAnalytics();

  if (isLoading) return <p className="muted">Loading...</p>;
  if (isError || !funnel) return <div className="page-error">Failed to load funnel.</div>;

  const totals: FunnelTotals = funnel.totals;
  const target = config?.weekly_app_target ?? 0;
  const weekApps = activity?.apps_this_week ?? 0;
  const streak = activity?.streak_days ?? 0;
  const overdue = analytics?.followups?.overdue ?? 0;
  const ghosted = funnel.ghosted.applied_no_response_14d;

  const weekAccent =
    target > 0
      ? weekApps >= target
        ? VIZ.green
        : weekApps >= target / 2
          ? VIZ.amber
          : VIZ.red
      : VIZ.mute;

  return (
    <>
      <div className="an-stats">
        <StatCard
          label="Response rate"
          value={pctOrDash(funnel.response_rate)}
          accent={rateAccent(funnel.response_rate)}
        />
        <StatCard label="Applied" value={nfmt(totals.applied)} />
        <StatCard label="This week" value={`${nfmt(weekApps)} / ${nfmt(target)}`} accent={weekAccent} />
        <StatCard label="Streak" value={`${streak}d`} />
        <StatCard
          label="Ghosted"
          value={nfmt(ghosted)}
          accent={ghosted > 0 ? VIZ.red : VIZ.mute}
        />
        <StatCard
          label="Overdue follow-ups"
          value={nfmt(overdue)}
          accent={overdue > 0 ? VIZ.red : VIZ.mute}
        />
      </div>

      <div className="an-grid">
        <AppsPerWeekChart appsPerWeek={funnel.apps_per_week} target={target} />
        <StageConversionPanel totalApplied={totals.applied} rows={funnel.stage_conversion} />
        <TimeInStagePanel rows={funnel.time_in_stage} />
        <PaceModelPanel totals={totals} target={target} deadline={config?.deadline} />
      </div>
    </>
  );
}

function AppsPerWeekChart({
  appsPerWeek,
  target,
}: {
  appsPerWeek: Array<{ week_start: string; count: number }>;
  target: number;
}) {
  return (
    <ChartCard title="Apps per week" subtitle={target > 0 ? `target ${target}/week` : "applications logged"}>
      {appsPerWeek.length === 0 ? (
        <div className="an-empty">
          No applications logged yet. Apps land here as you apply from Today.
        </div>
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={appsPerWeek} margin={{ top: 12, right: 20, bottom: 4, left: 0 }}>
            <CartesianGrid {...gridProps} vertical={false} />
            <XAxis
              dataKey="week_start"
              tickFormatter={(d: string) => (d?.length >= 10 ? d.slice(5) : d)}
              tick={TICK}
              axisLine={axisLine}
              tickLine={false}
              minTickGap={18}
            />
            <YAxis tick={TICK} axisLine={axisLine} tickLine={false} allowDecimals={false} />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
              content={(p) => (
                <ChartTooltip {...p} title={(l) => String(l)} fmtValue={(v) => nfmt(v)} />
              )}
            />
            {target > 0 && (
              <ReferenceLine
                y={target}
                stroke={VIZ.amber}
                strokeDasharray="4 4"
                label={{ value: "target", position: "right", fill: VIZ.mute, fontSize: 10 }}
              />
            )}
            <Bar isAnimationActive={false} dataKey="count" name="Applications" fill={VIZ.blue} radius={[4, 4, 0, 0]} maxBarSize={32}>
              <LabelList dataKey="count" position="top" fill={VIZ.dim} fontSize={11} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </ChartCard>
  );
}

function StageConversionPanel({
  totalApplied,
  rows,
}: {
  totalApplied: number;
  rows: StageConversion[];
}) {
  return (
    <ChartCard title="Stage conversion" subtitle="advancing through your pipeline" scroll>
      <div className="an-stagelist">
        <div className="an-stage-row an-stage-mouth" title="Applied (funnel mouth)">
          <span className="an-stage-label">Applied</span>
          <div className="an-stage-bar">
            <span className="an-stage-fill" style={{ width: "100%" }} />
          </div>
          <span className="an-stage-count">{nfmt(totalApplied)}</span>
        </div>
        {rows.map((r) => {
          const widthPct = r.rate != null ? Math.round(r.rate * 100) : 0;
          return (
            <div className="an-stage-row" key={`${r.from}->${r.to}`} title={`${r.from} -> ${r.to}`}>
              <span className="an-stage-label">
                {r.from} {"->"} {r.to}
              </span>
              <div className="an-stage-bar">
                <span className="an-stage-fill" style={{ width: `${widthPct}%` }} />
              </div>
              <span className="an-stage-count">
                {nfmt(r.advanced)}/{nfmt(r.entered)} · {pctOrDash(r.rate)}
              </span>
            </div>
          );
        })}
      </div>
    </ChartCard>
  );
}

function TimeInStagePanel({ rows }: { rows: TimeInStage[] }) {
  return (
    <ChartCard title="Time in stage" subtitle="median days per status" scroll>
      <div className="an-timelist">
        {rows.map((r) => (
          <div className="an-time-row" key={r.status}>
            <span className="an-time-label">{r.status}</span>
            <span className="an-time-value">
              {r.median_days == null ? "no data yet" : `${r.median_days.toFixed(1)}d (n=${r.n})`}
            </span>
          </div>
        ))}
      </div>
    </ChartCard>
  );
}

function PaceModelPanel({
  totals,
  target,
  deadline,
}: {
  totals: FunnelTotals;
  target: number;
  deadline: string | undefined;
}) {
  const weeks = weeksRemaining(deadline);
  const projectedApps = totals.applied + weeks * target;

  const rateToPhone = totals.applied > 0 ? totals.phone_screen / totals.applied : 0;
  const rateToInterview =
    totals.phone_screen > 0 ? rateToPhone * (totals.interview / totals.phone_screen) : 0;
  const rateToOffer = totals.interview > 0 ? rateToInterview * (totals.offer / totals.interview) : 0;
  const projectedInterviews = Math.round(projectedApps * rateToInterview);
  const projectedOffers = Math.round(projectedApps * rateToOffer);
  const unlocked = totals.applied >= 10;

  return (
    <ChartCard title="Pace model" subtitle="projection to your deadline" scroll>
      <div className="an-pace">
        <div className="an-pace-metric">{nfmt(projectedApps)} applications</div>
        <div className="an-pace-line">
          at {nfmt(target)} apps/week through {deadline ? fmtDate(deadline) : "no deadline set"}
        </div>
        {unlocked ? (
          <div className="an-pace-line">
            projected {nfmt(projectedInterviews)} interviews, {nfmt(projectedOffers)} offers,
            based on your observed conversion rates
          </div>
        ) : (
          <div className="an-pace-note">Projections unlock after 10 applications.</div>
        )}
      </div>
    </ChartCard>
  );
}

// ---------------------------------------------------------------------------
// Tab 2 — Market (ported unchanged from the old /analytics dashboard)
// ---------------------------------------------------------------------------

function MarketTab() {
  const { data, isLoading, isError } = useAnalytics();

  const funnel = useMemo(
    () =>
      STATUS_ORDER.map((status) => ({ status, count: data?.funnel?.[status] ?? 0 })),
    [data],
  );

  const tiers = useMemo(
    () => [5, 4, 3, 2, 1].map((t) => ({ tier: t, count: data?.tiers?.[String(t)] ?? 0 })),
    [data],
  );

  const odds = useMemo(
    () => COMPETITION_ORDER.map((c) => ({ odds: c, count: data?.odds?.[c] ?? 0 })),
    [data],
  );

  const sources = useMemo(() => {
    const rows = (data?.by_source ?? []).map((s) => ({
      source: s.source,
      kept: s.kept,
      with_desc: s.with_desc,
      pct: s.kept > 0 ? Math.round((s.with_desc / s.kept) * 100) : 0,
    }));
    rows.sort((a, b) => b.kept - a.kept);
    return rows;
  }, [data]);

  const runs = useMemo(
    () =>
      (data?.new_per_run ?? []).map((r) => ({
        run_date: r.run_date,
        new_this_run: r.new_this_run,
        kept: r.kept,
      })),
    [data],
  );

  const comp = useMemo(() => {
    const buckets = (data?.comp?.buckets ?? []).map((b) => ({
      label: kfmt(b.lo),
      lo: b.lo,
      hi: b.hi,
      count: b.count,
    }));
    const band = data?.comp?.band ?? null;
    let bandStart: string | null = null;
    let bandEnd: string | null = null;
    if (band && buckets.length) {
      const inBand = buckets.filter((b) => b.hi > band[0] && b.lo < band[1]);
      if (inBand.length) {
        bandStart = inBand[0].label;
        bandEnd = inBand[inBand.length - 1].label;
      }
    }
    return { buckets, band, bandStart, bandEnd };
  }, [data]);

  const cards = useMemo(() => {
    const last = runs.length ? runs[runs.length - 1] : null;
    const keptFromTiers = tiers.reduce((a, t) => a + t.count, 0);
    return {
      kept: last?.kept ?? keptFromTiers,
      newThisRun: last?.new_this_run ?? 0,
      tier5: data?.tiers?.["5"] ?? 0,
      overdue: data?.followups?.overdue ?? 0,
    };
  }, [runs, tiers, data]);

  const sourceHeight = Math.max(180, sources.length * 30 + 48);

  if (isLoading) return <p className="muted">Loading...</p>;
  if (isError || !data) return <div className="page-error">Failed to load market data.</div>;

  return (
    <>
      <div className="an-stats">
        <StatCard label="Kept this run" value={nfmt(cards.kept)} accent={VIZ.ink} />
        <StatCard label="New this run" value={nfmt(cards.newThisRun)} accent={VIZ.green} />
        <StatCard label="Tier 5 roles" value={nfmt(cards.tier5)} accent={VIZ.green} />
        <StatCard
          label="Overdue follow-ups"
          value={nfmt(cards.overdue)}
          accent={cards.overdue > 0 ? VIZ.red : VIZ.mute}
        />
      </div>

      <div className="an-grid">
        {/* Status funnel — horizontal bars in pipeline order (single series) */}
        <ChartCard title="Status funnel" subtitle="jobs by pipeline status">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              layout="vertical"
              data={funnel}
              margin={{ top: 4, right: 34, bottom: 4, left: 8 }}
            >
              <CartesianGrid {...gridProps} horizontal={false} />
              <XAxis type="number" tick={TICK} axisLine={axisLine} tickLine={false} allowDecimals={false} />
              <YAxis
                type="category"
                dataKey="status"
                width={92}
                tick={TICK}
                axisLine={axisLine}
                tickLine={false}
              />
              <Tooltip
                cursor={{ fill: "rgba(255,255,255,0.04)" }}
                content={(p) => <ChartTooltip {...p} fmtValue={(v) => nfmt(v)} />}
              />
              <Bar isAnimationActive={false} dataKey="count" name="Jobs" fill={VIZ.blue} radius={[0, 4, 4, 0]} maxBarSize={22}>
                <LabelList dataKey="count" position="right" fill={VIZ.dim} fontSize={11} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Tier distribution — columns, ordinal tier ramp */}
        <ChartCard title="Tier distribution" subtitle="kept roles by tier">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={tiers} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
              <CartesianGrid {...gridProps} vertical={false} />
              <XAxis
                dataKey="tier"
                tickFormatter={(t) => `T${t}`}
                tick={TICK}
                axisLine={axisLine}
                tickLine={false}
              />
              <YAxis tick={TICK} axisLine={axisLine} tickLine={false} allowDecimals={false} />
              <Tooltip
                cursor={{ fill: "rgba(255,255,255,0.04)" }}
                content={(p) => (
                  <ChartTooltip {...p} title={(l) => `Tier ${l}`} fmtValue={(v) => nfmt(v)} />
                )}
              />
              <Bar isAnimationActive={false} dataKey="count" name="Roles" radius={[4, 4, 0, 0]} maxBarSize={40}>
                {tiers.map((t) => (
                  <Cell key={t.tier} fill={TIER_COLORS[t.tier]} />
                ))}
                <LabelList dataKey="count" position="top" fill={VIZ.dim} fontSize={11} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Competition distribution: columns, competition semantic colors */}
        <ChartCard title="Competition distribution" subtitle="High competition / Standard / Lower bar">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={odds} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
              <CartesianGrid {...gridProps} vertical={false} />
              <XAxis dataKey="odds" tick={TICK} axisLine={axisLine} tickLine={false} />
              <YAxis tick={TICK} axisLine={axisLine} tickLine={false} allowDecimals={false} />
              <Tooltip
                cursor={{ fill: "rgba(255,255,255,0.04)" }}
                content={(p) => <ChartTooltip {...p} fmtValue={(v) => nfmt(v)} />}
              />
              <Bar isAnimationActive={false} dataKey="count" name="Roles" radius={[4, 4, 0, 0]} maxBarSize={56}>
                {odds.map((o) => (
                  <Cell key={o.odds} fill={COMPETITION_COLORS[o.odds] ?? VIZ.mute} />
                ))}
                <LabelList dataKey="count" position="top" fill={VIZ.dim} fontSize={11} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Tier x Competition heat counts */}
        <ChartCard title="Tier × competition" subtitle="kept roles by cell" height={220}>
          <HeatMatrix matrix={data.matrix ?? {}} />
        </ChartCard>

        {/* New per run — single-series trend line */}
        <ChartCard title="New roles per run" subtitle="new_this_run over time">
          {runs.length === 0 ? (
            <div className="an-empty">No run history yet.</div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={runs} margin={{ top: 12, right: 20, bottom: 4, left: 0 }}>
                <CartesianGrid {...gridProps} vertical={false} />
                <XAxis
                  dataKey="run_date"
                  tickFormatter={(d: string) => (d?.length >= 10 ? d.slice(5) : d)}
                  tick={TICK}
                  axisLine={axisLine}
                  tickLine={false}
                  minTickGap={18}
                />
                <YAxis tick={TICK} axisLine={axisLine} tickLine={false} allowDecimals={false} />
                <Tooltip
                  content={(p) => (
                    <ChartTooltip {...p} title={(l) => String(l)} fmtValue={(v) => nfmt(v)} />
                  )}
                />
                <Line
                  isAnimationActive={false}
                  type="monotone"
                  dataKey="new_this_run"
                  name="New roles"
                  stroke={VIZ.green}
                  strokeWidth={2}
                  dot={{ r: 3, fill: VIZ.green, stroke: VIZ.surface, strokeWidth: 2 }}
                  activeDot={{ r: 5, fill: VIZ.green, stroke: VIZ.surface, strokeWidth: 2 }}
                >
                  <LabelList
                    dataKey="new_this_run"
                    position="top"
                    fill={VIZ.dim}
                    fontSize={10}
                  />
                </Line>
              </LineChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

        {/* Comp histogram — salary midpoints, $10k buckets, comp band overlay */}
        <ChartCard
          title="Compensation spread"
          subtitle={
            comp.band ? `target ${kfmt(comp.band[0])}–${kfmt(comp.band[1])}` : "salary midpoints"
          }
        >
          {comp.buckets.length === 0 ? (
            <div className="an-empty">No salary data.</div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={comp.buckets} margin={{ top: 12, right: 12, bottom: 4, left: 0 }}>
                <CartesianGrid {...gridProps} vertical={false} />
                {comp.band && comp.bandStart && comp.bandEnd && (
                  <ReferenceArea
                    x1={comp.bandStart}
                    x2={comp.bandEnd}
                    fill={VIZ.bandFill}
                    stroke={VIZ.bandStroke}
                    strokeDasharray="3 3"
                    ifOverflow="extendDomain"
                    label={{ value: "target", position: "insideTop", fill: VIZ.mute, fontSize: 10 }}
                  />
                )}
                <XAxis
                  dataKey="label"
                  tick={TICK}
                  axisLine={axisLine}
                  tickLine={false}
                  minTickGap={8}
                />
                <YAxis tick={TICK} axisLine={axisLine} tickLine={false} allowDecimals={false} />
                <Tooltip
                  cursor={{ fill: "rgba(255,255,255,0.04)" }}
                  content={(p) => (
                    <ChartTooltip
                      {...p}
                      title={(_l, pl) => {
                        const b = pl[0]?.payload as { lo?: number; hi?: number } | undefined;
                        return b?.lo != null && b?.hi != null
                          ? `${kfmt(b.lo)}–${kfmt(b.hi)}`
                          : String(_l);
                      }}
                      fmtValue={(v) => `${nfmt(v)} roles`}
                    />
                  )}
                />
                <Bar isAnimationActive={false} dataKey="count" name="Roles" fill={VIZ.blue} radius={[4, 4, 0, 0]} maxBarSize={38} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </ChartCard>

        {/* Source coverage — grouped horizontal bars: kept vs with_desc (2 series) */}
        <ChartCard
          title="Source coverage"
          subtitle="kept vs with description"
          height={sourceHeight}
          scroll
        >
          {sources.length === 0 ? (
            <div className="an-empty">No source data.</div>
          ) : (
            <ResponsiveContainer width="100%" height={sourceHeight - 16}>
              <BarChart
                layout="vertical"
                data={sources}
                barGap={2}
                margin={{ top: 4, right: 40, bottom: 4, left: 8 }}
              >
                <CartesianGrid {...gridProps} horizontal={false} />
                <XAxis
                  type="number"
                  tick={TICK}
                  axisLine={axisLine}
                  tickLine={false}
                  allowDecimals={false}
                />
                <YAxis
                  type="category"
                  dataKey="source"
                  width={120}
                  tick={TICK}
                  axisLine={axisLine}
                  tickLine={false}
                />
                <Tooltip
                  cursor={{ fill: "rgba(255,255,255,0.04)" }}
                  content={(p) => (
                    <ChartTooltip
                      {...p}
                      title={(l, pl) => {
                        const row = pl[0]?.payload as { pct?: number } | undefined;
                        return row?.pct != null ? `${l} · ${row.pct}% with desc` : String(l);
                      }}
                      fmtValue={(v) => nfmt(v)}
                    />
                  )}
                />
                <Legend
                  wrapperStyle={{ fontSize: 11, color: VIZ.dim, paddingTop: 4 }}
                  iconType="square"
                  iconSize={9}
                />
                <Bar isAnimationActive={false} dataKey="kept" name="Kept" fill={VIZ.blue} radius={[0, 3, 3, 0]} maxBarSize={11} />
                <Bar
                  dataKey="with_desc"
                  name="With desc"
                  fill={VIZ.green}
                  radius={[0, 3, 3, 0]}
                  maxBarSize={11}
                />
              </BarChart>
            </ResponsiveContainer>
          )}
        </ChartCard>
      </div>
    </>
  );
}

// ---------------------------------------------------------------------------
// Tab 4 — Quality (Phase 5.5: ranking-quality metrics)
// ---------------------------------------------------------------------------
// Renders GET /api/ranking/metrics: how well the server-side ranker
// (backend/ranking.py, served via GET /api/queue/today) is actually doing,
// not just what it recommended. Every cell carries its own explicit
// denominator and a `low_sample` badge (n below the endpoint's min_sample)
// rather than a bare rate that quietly lies when n is tiny -- same
// convention as outcome_analytics.py's cells, which this endpoint reuses.
//
// Funnel-vs-analytics label note (5.3 watch item, phase5-progress.md): /api/
// funnel and this endpoint deliberately disagree on stage totals -- funnel.py
// counts a posting as "responded" once it has EVER reached a response stage
// (a reached-count, order-independent), while this endpoint's response_rate
// is a per-application rate scoped to what the ranker actually served.
// Different denominators, different questions -- the banner below and each
// stat's own subtitle say so explicitly rather than let two similar-looking
// percentages silently disagree.

function LowSampleBadge({ show, title }: { show: boolean; title: string }) {
  if (!show) return null;
  return (
    <span className="badge flag rk-low-sample" data-kind="amber" title={title}>
      low n
    </span>
  );
}

/** min_sample is a per-response value (GET /api/ranking/metrics's own
 * `min_sample` query param/default), not a fixed constant -- the badge's
 * tooltip states the actual threshold in effect rather than the bare word
 * "min_sample" (5.5 fix F6). */
function lowSampleTitle(minSample: number): string {
  return `Sample below min_sample=${minSample} -- rate may be noisy`;
}

function RkCellRow({
  label,
  numerator,
  denominator,
  lowSample,
  lowSampleTitle: title,
}: {
  label: string;
  numerator: number;
  denominator: number;
  lowSample: boolean;
  lowSampleTitle: string;
}) {
  return (
    <div className="rk-cell-row">
      <span className="rk-cell-label">{label}</span>
      <span className="rk-cell-value">
        {nfmt(numerator)} / {nfmt(denominator)}
        <LowSampleBadge show={lowSample} title={title} />
      </span>
    </div>
  );
}

function SourceYieldTable({ rows, minSample }: { rows: SourceYieldCell[]; minSample: number }) {
  if (rows.length === 0) {
    return <div className="an-empty">No recommendation history yet.</div>;
  }
  const title = lowSampleTitle(minSample);
  return (
    <div className="rk-table-wrap">
      <table className="rk-source-table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Rec'd</th>
            <th>Opened</th>
            <th>Applied</th>
            <th>Responded</th>
            <th>Open %</th>
            <th>Apply %</th>
            <th>Resp %</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.key}>
              <td className="rk-source-key">
                {r.key}
                <LowSampleBadge show={r.low_sample} title={title} />
              </td>
              <td>{nfmt(r.n_recommended)}</td>
              <td>{nfmt(r.n_opened)}</td>
              <td>{nfmt(r.n_applied)}</td>
              <td>{nfmt(r.n_responded)}</td>
              <td>{pctOrDash(r.open_rate)}</td>
              <td>{pctOrDash(r.application_rate)}</td>
              <td>{pctOrDash(r.response_rate)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function QualityTab() {
  const { data: metrics, isLoading, isError, error } = useRankingMetrics();

  if (isLoading) return <p className="muted">Loading...</p>;
  if (isError) {
    // GET /api/ranking/metrics answers 503 on a pre-canonical-schema database
    // (same precedent as GET /api/runs, see useRunsCapability) -- that is an
    // expected "not migrated yet" state, not a failure worth an error banner.
    if (error instanceof ApiError && error.status === 503) {
      return (
        <div className="an-empty">
          Ranking-quality metrics need the canonical schema -- not migrated on this
          database yet.
        </div>
      );
    }
    return <div className="page-error">Failed to load ranking metrics.</div>;
  }
  if (!metrics) return <div className="page-error">Failed to load ranking metrics.</div>;

  const t10 = metrics.top10_application_rate;
  const resp = metrics.response_rate;
  const stale = metrics.stale_rate;
  const ghost = metrics.ghost_rate;
  const tta = metrics.time_to_application;
  const completion = metrics.queue_completion;
  const lsTitle = lowSampleTitle(metrics.min_sample);

  return (
    <>
      <div className="rk-note">
        Generated {fmtDate(metrics.generated_at)}. Every rate below carries its own
        explicit denominator (the "Denominators" panel(s)) and a "low n" badge when its
        sample is under min_sample={metrics.min_sample} -- read the badge before trusting
        a lone percentage.
      </div>

      <h3 className="rk-group-head">Served-queue quality</h3>
      <div className="rk-note">
        Scoped to postings the ranker actually served: open/apply attributed back to the
        Today queue that recommended them -- a different denominator than "Stage
        conversion" on the My funnel tab, which counts postings that ever reached a
        pipeline stage regardless of serve order. The two are expected to disagree; that
        is not a bug in either panel.
      </div>

      <div className="an-stats">
        <StatCard
          label="Top-10 application rate"
          value={
            <>
              {pctOrDash(t10.rate)} <LowSampleBadge show={t10.low_sample} title={lsTitle} />
            </>
          }
          accent={rateAccent(t10.rate)}
        />
        <StatCard
          label="Stale-in-queue rate"
          value={
            <>
              {pctOrDash(stale.rate)} <LowSampleBadge show={stale.low_sample} title={lsTitle} />
            </>
          }
          accent={stale.rate != null && stale.rate > 0.25 ? VIZ.red : VIZ.mute}
        />
        <StatCard
          label="Median days to apply"
          value={
            <>
              {tta.median_days == null ? "-" : `${tta.median_days.toFixed(1)}d`}{" "}
              <LowSampleBadge show={tta.low_sample} title={lsTitle} />
            </>
          }
        />
      </div>

      <div className="an-grid">
        <ChartCard title="Denominators" subtitle="what each served-queue rate above is out of" scroll>
          <div className="rk-cell-list">
            <RkCellRow
              label="Top-10 served -> applied"
              numerator={t10.n_applied}
              denominator={t10.n_served_top10}
              lowSample={t10.low_sample}
              lowSampleTitle={lsTitle}
            />
            <RkCellRow
              label="Served -> stale, never engaged"
              numerator={stale.n_stale_never_engaged}
              denominator={stale.n_served}
              lowSample={stale.low_sample}
              lowSampleTitle={lsTitle}
            />
            <div className="rk-cell-row">
              <span className="rk-cell-label">Time-to-application sample</span>
              <span className="rk-cell-value">
                n={nfmt(tta.n_applied)} of {nfmt(tta.n_served)} served
                <LowSampleBadge show={tta.low_sample} title={lsTitle} />
              </span>
            </div>
          </div>
        </ChartCard>

        <ChartCard title="Queue completion" subtitle="served jobs acted on, same local day" scroll>
          {completion.by_day.length === 0 ? (
            <div className="an-empty">No served-day history yet.</div>
          ) : (
            <div className="an-timelist">
              <div className="an-time-row">
                <span className="an-time-label">Median (all days)</span>
                <span className="an-time-value">
                  {pctOrDash(completion.median_rate)}{" "}
                  <LowSampleBadge show={completion.low_sample} title={lsTitle} />
                </span>
              </div>
              {completion.by_day.map((d) => (
                <div className="an-time-row" key={d.day}>
                  <span className="an-time-label">{d.day}</span>
                  <span className="an-time-value">
                    {/* Fraction and percentage share the SAME denominator
                        (queue_size, 5.5 fix F5) -- n_served is context, not
                        what either number above is computed out of. */}
                    {nfmt(d.n_completed)}/{nfmt(d.queue_size)} · {pctOrDash(d.rate)}{" "}
                    <span className="muted-sm">({nfmt(d.n_served)} served)</span>
                  </span>
                </div>
              ))}
            </div>
          )}
        </ChartCard>

        <ChartCard title="Source yield" subtitle="recommended -> opened -> applied -> responded" scroll>
          <SourceYieldTable rows={metrics.source_yield.by_source} minSample={metrics.min_sample} />
        </ChartCard>

        <ChartCard
          title="Source yield by category"
          subtitle="recommended -> opened -> applied -> responded"
          scroll
        >
          <SourceYieldTable
            rows={metrics.source_yield.by_source_category}
            minSample={metrics.min_sample}
          />
        </ChartCard>
      </div>

      <h3 className="rk-group-head">All applications</h3>
      <div className="rk-note">
        Population-wide, NOT scoped to the served queue: every logged application counts
        here, including ones the ranker never served or whose posting can no longer be
        resolved (those bucket "unknown" for band/source facts). This is a different
        question from the served-queue section above, not a stricter or looser version of
        the same one.
      </div>

      <div className="an-stats">
        <StatCard
          label="Response rate (per application)"
          value={
            <>
              {pctOrDash(resp.rate)} <LowSampleBadge show={resp.low_sample} title={lsTitle} />
            </>
          }
          accent={rateAccent(resp.rate)}
        />
        <StatCard
          label={`Ghost rate (no response ≥${ghost.ghost_days}d, mature applications)`}
          value={
            <>
              {pctOrDash(ghost.rate)} <LowSampleBadge show={ghost.low_sample} title={lsTitle} />
            </>
          }
          accent={ghost.rate != null && ghost.rate > 0.25 ? VIZ.red : VIZ.mute}
        />
      </div>

      <div className="an-grid">
        <ChartCard title="Denominators" subtitle="what each all-applications rate above is out of" scroll>
          <div className="rk-cell-list">
            <RkCellRow
              label="Applied -> responded"
              numerator={resp.n_responded}
              denominator={resp.n_applied}
              lowSample={resp.low_sample}
              lowSampleTitle={lsTitle}
            />
            <RkCellRow
              label="Eligible applications -> ghosted"
              numerator={ghost.n_ghosted}
              denominator={ghost.n_applied_eligible}
              lowSample={ghost.low_sample}
              lowSampleTitle={lsTitle}
            />
            <div className="rk-cell-row">
              <span className="rk-cell-label">Applied total (incl. below-eligibility-window)</span>
              <span className="rk-cell-value">n={nfmt(ghost.n_applied_total)}</span>
            </div>
          </div>
        </ChartCard>
      </div>

      <div className="rk-note">
        This ghost rate is not the My funnel tab's "Ghosted" stat: that one counts
        applications sitting ≥14 days in a non-terminal status (status-sitting), computed
        the moment they cross that age regardless of any reply. This one counts
        applications with no response signal for ≥{ghost.ghost_days}d after applying
        (response-silence), only once they are old enough to judge. Different concepts by
        design -- both are worth watching.
      </div>
    </>
  );
}
