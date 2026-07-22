// DTO types mirroring backend models.py. Field names are the contract; do not rename.

export interface JobState {
  status: string;
  notes: string;
  follow_up_date: string | null;
  applied_date: string | null;
  starred: boolean;
  hidden: boolean;
  contact: string;
  snoozed_until: string | null;
  applied_via: string | null;
  needs_review: boolean;
  review_reason: string | null;
  updated_at: string;
}

export interface JobLight {
  url: string;
  url_b64: string;
  seen_key: string;
  tier: number;
  odds: string | null;
  odds_score: number | null;
  odds_why: string | null;
  is_new: boolean;
  title: string | null;
  company: string | null;
  location: string | null;
  salary: string | null;
  salary_min: number | null;
  salary_max: number | null;
  posted: string | null;
  first_seen: string | null;
  remote: boolean;
  source: string | null;
  also_seen_on: string | null;
  req_id: string | null;
  why: string | null;
  flags: string | null;
  desc_snippet: string | null;
  has_desc: boolean;
  state: JobState | null;
}

export interface JobFull extends JobLight {
  full_desc: string | null;
  skill_hits: string[];
}

export interface JobsResponse {
  run_date: string | null;
  jobs: JobLight[];
}

export interface CompanyState {
  company: string;
  contact: string;
  notes: string;
  updated_at: string;
}

export interface AnalyticsResponse {
  funnel: Record<string, number>;
  tiers: Record<string, number>;
  odds: Record<string, number>;
  matrix: Record<string, Record<string, number>>;
  by_source: Array<{ source: string; kept: number; with_desc: number }>;
  new_per_run: Array<{ run_date: string; kept: number; new_this_run: number }>;
  comp: { buckets: Array<{ lo: number; hi: number; count: number }>; band: [number, number] };
  followups: { overdue: number; upcoming: number };
}

export interface FunnelTotals {
  applied: number;
  responded: number;
  phone_screen: number;
  interview: number;
  offer: number;
  rejected: number;
}

export interface StageConversion {
  from: string;
  to: string;
  entered: number;
  advanced: number;
  rate: number | null;
}

export interface TimeInStage {
  status: string;
  median_days: number | null;
  n: number;
}

export interface AppsPerWeek {
  week_start: string;
  count: number;
}

export interface FunnelResponse {
  totals: FunnelTotals;
  response_rate: number | null;
  stage_conversion: StageConversion[];
  time_in_stage: TimeInStage[];
  apps_per_week: AppsPerWeek[];
  ghosted: { applied_no_response_14d: number };
}

export interface FollowupsResponse {
  overdue: JobLight[];
  upcoming: JobLight[];
}

export interface TierChange {
  job: JobLight;
  from: number;
  to: number;
}

export interface DisappearedJob {
  url: string;
  url_b64: string;
  title: string | null;
  company: string | null;
  location: string | null;
  tier: number;
  last_seen: string;
}

export interface ChangesResponse {
  baseline: string | null;
  current: string | null;
  new: JobLight[];
  reposted: JobLight[];
  tier_changed: TierChange[];
  disappeared: DisappearedJob[];
}

export interface FreshnessSource {
  name: string;
  rows: number;
  refreshed: boolean | null;
  at: string | null;
}

export interface SweepStatus {
  running: boolean;
  kind: string | null;
  step: string | null;
  done: number;
  total: number;
}

export interface FreshnessResponse {
  latest_run: string | null;
  ingested_at: string | null;
  kept: number | null;
  new_this_run: number | null;
  sources: FreshnessSource[];
  zero_row_sources: string[];
  stale_refresh_sources: string[];
  sweep: SweepStatus;
}

export interface AppConfig {
  skills: string[];
  comp_band: [number, number];
  statuses: string[];
  daily_queue_size: number;
  weekly_app_target: number;
  deadline: string;
  snooze_default_days: number;
}

export interface IngestReport {
  rows: number;
  new: number;
  healed: number;
  needs_review: number;
  descs_joined: number;
  runs_backfilled: number;
}

// ---- request bodies ----

export interface StatePatch {
  status?: string;
  notes?: string;
  follow_up_date?: string | null;
  applied_date?: string | null;
  starred?: boolean;
  hidden?: boolean;
  contact?: string;
  snoozed_until?: string | null;
  applied_via?: string | null;
  /** Write-only: durably acknowledge a review item (never echoed back on JobState). */
  review_dismissed?: boolean;
}

export interface ReviewItem {
  job: JobLight;
  candidates: JobLight[];
}

export interface ReconcileBody {
  from_url_b64: string;
  to_url_b64: string;
}

export type QuickAction = "applied" | "snooze" | "pass" | "star" | "unstar";

export interface QuickActionBody {
  action: QuickAction;
  days?: number;
  /** 'applied' only: how the application was submitted. */
  applied_via?: string;
  /** 'pass' only: one-tap pass reason (comp/location/seniority/stack/...). */
  reason?: string;
}

export interface CompanyPatch {
  contact?: string;
  notes?: string;
}

export interface ConfigPatch {
  skills?: string[];
  comp_band?: [number, number];
  daily_queue_size?: number;
  weekly_app_target?: number;
  deadline?: string;
  snooze_default_days?: number;
}

// SSE event shape emitted by GET /api/sweep/progress
// SSE event shape emitted by GET /api/sweep/progress.
// `sync` and `bye` are transport-level: `sync` is the per-subscriber catch-up frame
// every stream opens with, `bye` announces a deliberate recycle. kind/step/line are
// nullable because `sync` reports live runner state, which is None before the first
// step resolves.
export interface SweepEvent {
  type: "start" | "step" | "log" | "skipped" | "ingested" | "done" | "error" | "sync" | "bye";
  kind?: string | null;
  step?: string | null;
  done?: number;
  total?: number;
  line?: string | null;
  message?: string;
  /** Per-process nonce: run counters from different processes are not comparable. */
  boot?: string;
  /** Runs completed since `boot`. An increase means a run ended while we were away. */
  finished?: number;
  /** sync only. */
  running?: boolean;
  /** sync only: how the last completed run failed, when it failed. */
  last_error?: string | null;
  /** bye only: why the server is recycling this stream. */
  reason?: string;
}
