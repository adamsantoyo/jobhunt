import { useMemo } from "react";
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
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useAnalytics } from "../store/queries";
import { ChartCard, StatCard } from "../components/analytics/panels";
import { ChartTooltip } from "../components/analytics/ChartTooltip";
import { HeatMatrix } from "../components/analytics/HeatMatrix";
import {
  ANALYTICS_CSS,
  kfmt,
  nfmt,
  ODDS_COLORS,
  TICK,
  TIER_COLORS,
  VIZ,
} from "../components/analytics/theme";

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
const ODDS_ORDER = ["Likely", "Target", "Reach"];

const gridProps = { stroke: VIZ.grid, strokeWidth: 1 } as const;
const axisLine = { stroke: VIZ.axis } as const;

export default function Analytics() {
  const { data, isLoading, isError } = useAnalytics();

  // ---- derive series (all client-side from the single analytics payload) ----
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
    () => ODDS_ORDER.map((o) => ({ odds: o, count: data?.odds?.[o] ?? 0 })),
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

  // ---- top cards ----
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

  if (isLoading) {
    return (
      <div className="an-page">
        <style>{ANALYTICS_CSS}</style>
        <h1>Analytics</h1>
        <p className="muted">Loading…</p>
      </div>
    );
  }
  if (isError || !data) {
    return (
      <div className="an-page">
        <style>{ANALYTICS_CSS}</style>
        <h1>Analytics</h1>
        <div className="page-error">Failed to load analytics.</div>
      </div>
    );
  }

  return (
    <div className="an-page">
      <style>{ANALYTICS_CSS}</style>
      <h1>Analytics</h1>

      {/* Top stat tiles */}
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
              <Bar dataKey="count" name="Jobs" fill={VIZ.blue} radius={[0, 4, 4, 0]} maxBarSize={22}>
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
              <Bar dataKey="count" name="Roles" radius={[4, 4, 0, 0]} maxBarSize={40}>
                {tiers.map((t) => (
                  <Cell key={t.tier} fill={TIER_COLORS[t.tier]} />
                ))}
                <LabelList dataKey="count" position="top" fill={VIZ.dim} fontSize={11} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Odds distribution — columns, odds semantic colors */}
        <ChartCard title="Odds distribution" subtitle="Likely / Target / Reach">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={odds} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
              <CartesianGrid {...gridProps} vertical={false} />
              <XAxis dataKey="odds" tick={TICK} axisLine={axisLine} tickLine={false} />
              <YAxis tick={TICK} axisLine={axisLine} tickLine={false} allowDecimals={false} />
              <Tooltip
                cursor={{ fill: "rgba(255,255,255,0.04)" }}
                content={(p) => <ChartTooltip {...p} fmtValue={(v) => nfmt(v)} />}
              />
              <Bar dataKey="count" name="Roles" radius={[4, 4, 0, 0]} maxBarSize={56}>
                {odds.map((o) => (
                  <Cell key={o.odds} fill={ODDS_COLORS[o.odds] ?? VIZ.mute} />
                ))}
                <LabelList dataKey="count" position="top" fill={VIZ.dim} fontSize={11} />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Tier x Odds heat counts */}
        <ChartCard title="Tier × odds" subtitle="kept roles by cell" height={220}>
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
                <Bar dataKey="count" name="Roles" fill={VIZ.blue} radius={[4, 4, 0, 0]} maxBarSize={38} />
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
                <Bar dataKey="kept" name="Kept" fill={VIZ.blue} radius={[0, 3, 3, 0]} maxBarSize={11} />
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
    </div>
  );
}
