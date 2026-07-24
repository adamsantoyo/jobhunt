import { useActivity, useConfig, useJobs } from "../../store/queries";
import { isQueueEligible } from "../../lib/compare";
import { todayISO } from "../../lib/format";

// The compact daily pace row: pace vs target, streak, done-today, weeks to
// deadline, and the two queue-composition stats (actionable / snoozed), both
// tier>=3-scoped per the plan.

function Stat({ label, value, tone }: { label: string; value: string | number; tone?: string }) {
  return (
    <div className="td-stat-col">
      <span
        className="td-stat-value"
        style={{ color: tone ?? "var(--fg)" }}
      >
        {value}
      </span>
      <span className="muted-sm">{label}</span>
    </div>
  );
}

/** Weeks remaining to `deadline` (ISO date), ceil, floored at 0. Local-naive. */
function weeksToDeadline(deadline: string | undefined): number {
  if (!deadline) return 0;
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(deadline);
  if (!m) return 0;
  const dl = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const diffDays = (dl.getTime() - today.getTime()) / 86_400_000;
  return Math.max(0, Math.ceil(diffDays / 7));
}

export function PaceHeader() {
  const { data: activity } = useActivity();
  const { data: config } = useConfig();
  const { data: jobsResp } = useJobs();
  const jobs = jobsResp?.jobs ?? [];
  const today = todayISO();

  const actionable = jobs.filter(isQueueEligible).length;
  const snoozed = jobs.filter(
    (j) => j.tier >= 3 && !!j.state?.snoozed_until && j.state.snoozed_until > today && !j.state?.hidden,
  ).length;

  const target = config?.weekly_app_target ?? 0;
  const appsWeek = activity?.apps_this_week ?? 0;
  const streak = activity?.streak_days ?? 0;
  const done = activity?.today.done ?? 0;
  const weeks = weeksToDeadline(config?.deadline);

  return (
    <div className="td-pace-row">
      <Stat label="apps this week" value={`${appsWeek}/${target}`} tone="var(--accent)" />
      <Stat label="streak" value={`${streak}d`} tone="var(--amber)" />
      <Stat label="done today" value={done} tone="var(--green)" />
      <Stat label="to deadline" value={`${weeks}w`} />
      <Stat label="actionable" value={actionable} tone="var(--accent)" />
      <Stat label="snoozed" value={snoozed} tone="var(--amber)" />
    </div>
  );
}
