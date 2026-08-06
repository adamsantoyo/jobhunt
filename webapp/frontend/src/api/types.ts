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
  /** ISO timestamp of the latest field='status' state_event; null if none. */
  status_since: string | null;
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

export interface ActivityResponse {
  today: { applied: number; passed: number; snoozed: number; done: number };
  apps_this_week: number;
  streak_days: number;
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

// ---------------------------------------------------------------------------
// Phase 4.3 -- canonical runs
// ---------------------------------------------------------------------------
// Mirrors webapp/backend/routers/runsapi.py + runservice.py. Only wired once
// the capability probe (GET /api/runs) answers 200; a legacy (pre-Phase-4)
// database answers 503 and the app keeps using SweepEvent/SweepStatus below
// instead (see useRunsCapability in store/queries.ts).

/** Kinds `RunService.start_run` executes this wave (SUPPORTED_KINDS in
 * runservice.py). `llm-review` and `manual-import` are real `RunKind`
 * values but answer 501 until wave 3, so they are not offered here. */
export type RunKind = "daily" | "full-direct" | "aggregators";

export interface RunStartResponse {
  run_uid: string;
  kind: string;
  status: "running";
}

export interface RunSummary {
  run_uid: string;
  kind: string;
  status: string;
  trigger: string | null;
  requested_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  kept_count: number | null;
  new_count: number | null;
  error: unknown;
  /** True only while THIS server process is still executing the run
   * (RunService.is_active); false for a run recorded by another process or
   * before/after this one's lifetime, even if `status` still says
   * "running" (see `interrupted` recovery in runservice.py). */
  active: boolean;
}

export interface SourceRunRow {
  source_run_id: string;
  source: string;
  step: string;
  attempt: number;
  status: string;
  requested_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  deadline_at: string | null;
  item_count: number | null;
  fetched_count: number | null;
  accepted_count: number | null;
  changed_count: number | null;
  inventory_scope: string | null;
  error: unknown;
}

export interface StageReport {
  phase: string;
  at: string;
  sequence: number;
  report?: unknown;
  error?: unknown;
}

export interface RunSettledStageEntry {
  status: string;
  reason?: string;
  report?: unknown;
  error?: unknown;
}

/** Payload of the terminal `service.run.settled` event / `RunDetail.settled`. */
export interface RunSettledPayload {
  run_uid: string;
  kind: string;
  fetch_status: string;
  fetch_error: unknown;
  stages: Record<string, RunSettledStageEntry>;
  stage_failures: string[];
  stages_cancelled: string[];
  outcome: "succeeded" | "partial" | "degraded" | "cancelled" | "failed" | string;
}

export interface RunDetail {
  run_uid: string;
  kind: string;
  status: string;
  trigger: string | null;
  requested_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  kept_count: number | null;
  new_count: number | null;
  config_hash: string | null;
  code_hash: string | null;
  scorer_hash: string | null;
  profile_version_id: string | null;
  report: unknown;
  error: unknown;
  active: boolean;
  source_runs: SourceRunRow[];
  stages: Record<string, StageReport>;
  settled: RunSettledPayload | null;
  terminal: boolean;
  change_summary: unknown;
}

/** One parsed frame from GET /api/runs/{run_uid}/events. `payload` is
 * scheduler/service-derived and can carry adapter error text pulled from a
 * job source -- render every field as a text node only, never through
 * dangerouslySetInnerHTML or string-to-JSX interpolation (phase4-spec.md
 * decision 10). */
export interface RunEventFrame {
  sequence: number;
  event_type: string;
  at: string;
  source_run_id: string | null;
  payload: Record<string, unknown> | null;
}

// ---------------------------------------------------------------------------
// Phase 4.4 -- source operations panel
// ---------------------------------------------------------------------------
// Pinned contract: plans/phase4-spec.md, wave-2 decision 8. W-B implements
// GET /api/sources/ops and POST /api/sources/{source}/retry to this shape.

export interface SourceRowAnomaly {
  flag: boolean;
  ratio: number | null;
}

export interface SourceOpsEntry {
  source: string;
  /** Null when the source has no configured plan AND no adapter to fall back
   * on (unregistered): there is nothing to categorize it by. */
  category: string | null;
  last_success_at: string | null;
  last_failure_at: string | null;
  age_seconds: number | null;
  /** Null when the source has never run (no freshness row yet): "stale" is
   * meaningless without a baseline to be stale relative to. */
  stale: boolean | null;
  consecutive_failures: number;
  p50_duration_seconds: number | null;
  p95_duration_seconds: number | null;
  last_rows: number | null;
  median_rows: number | null;
  row_anomaly: SourceRowAnomaly;
  circuit_open: boolean;
  /** Pre-formatted "Type: message" text from the last failed attempt, or null.
   * Always a string on the wire -- never render this without going through a
   * defensive coercion first (SourceOpsPanel's `lastErrorText`), in case a
   * future backend change slips an object back in. */
  last_error: string | null;
  licenses_absence: boolean;
}

export interface SourceOpsResponse {
  sources: SourceOpsEntry[];
  generated_at: string;
}

export interface RetryResponse {
  run_uid: string;
}

// ---------------------------------------------------------------------------
// Phase 5.1/5.5 -- server-side Today queue (GET /api/queue/today)
// ---------------------------------------------------------------------------
// Shape pinned in backend/tests/test_queue_api.py::test_happy_path_shape_and_ranking
// (5.1) plus the 5.5 contract addition (plans/phase5-progress.md, "5.5
// contract"): a nullable top-level `snapshot_id`, echoed on every same-day
// response once the backend's snapshot-on-serve wave lands, null when
// capture failed or was skipped. `evidence`/freshness/uncertainty field
// names mirror backend/ranking.py's `QueueEntry.evidence` dict literally.

export interface QueueFreshness {
  bucket: string;
  age_days: number | null;
  basis: string | null;
}

export interface QueueEvidence {
  lane: string;
  lane_rank: number;
  match_band: string | null;
  competition_band: string | null;
  odds_score: number | null;
  tier: number;
  freshness: QueueFreshness;
  /** Flags like "unscored" / "no-description"; empty array when none. */
  uncertainty: string[];
  why: string | null;
}

export interface QueueEntry {
  job: JobLight;
  rank: number;
  lane: string;
  lane_rank: number;
  evidence: QueueEvidence;
}

export interface QueueExcludedRow {
  url_b64: string;
  title: string | null;
  company: string | null;
  reason: string;
  detail: string | null;
}

export interface QueueTodayResponse {
  generated_for: string;
  cap: number;
  queue: QueueEntry[];
  excluded: QueueExcludedRow[];
  excluded_counts: Record<string, number>;
  considered: number;
  /** 5.5 addition: today's captured "today"-surface snapshot id, echoed on
   * every same-day response; null when capture failed or was skipped. Lets
   * the client attribute open events back to the served queue. */
  snapshot_id: string | null;
}

// ---------------------------------------------------------------------------
// Phase 5.2/5.5 -- outcome event capture (POST /api/outcomes/events)
// ---------------------------------------------------------------------------
// Mirrors backend/routers/outcomesapi.py's `OutcomeEventIn`. Only the
// "opened" kind is emitted by this wave's two capture sites (TodayCard's
// window.open, JobDetailDrawer's external link); the field is typed as
// `string` (not a literal union) because the backend kind whitelist is
// extensible and this DTO does not own that vocabulary.
//
// No `rank` field: the 5.5 fix adjudication (F1) moved rank derivation to
// the server (outcomesapi.record_outcome_event derives rank from the
// matching (snapshot_id, posting) item) -- the client never had an
// authoritative rank to send in the first place, since build_queue
// renumbers ranks per serve.
export interface OutcomeEventBody {
  kind: string;
  url_b64?: string;
  posting_id?: string;
  snapshot_id?: string | null;
  payload?: Record<string, unknown> | null;
  idempotency_key?: string | null;
}

// ---------------------------------------------------------------------------
// Phase 5.5 -- ranking-quality metrics (GET /api/ranking/metrics)
// ---------------------------------------------------------------------------
// Mirrors backend/ranking_metrics.py::ranking_metrics's literal return dict
// (verified read-only against the landed module + routers/queueapi.py's
// `get_ranking_metrics`, which passes it straight through) -- not a guess
// from the contract prose, the actual field names each cell builder returns.

export interface Top10ApplicationRate {
  n_served_top10: number;
  n_applied: number;
  rate: number | null;
  low_sample: boolean;
}

export interface TimeToApplication {
  n_served: number;
  n_applied: number;
  median_days: number | null;
  low_sample: boolean;
}

export interface ResponseRateCell {
  n_applied: number;
  n_responded: number;
  rate: number | null;
  low_sample: boolean;
}

export interface StaleRateCell {
  n_served: number;
  n_stale_never_engaged: number;
  rate: number | null;
  low_sample: boolean;
}

export interface GhostRateCell {
  n_applied_total: number;
  n_applied_eligible: number;
  n_ghosted: number;
  rate: number | null;
  low_sample: boolean;
  ghost_days: number;
}

export interface QueueCompletionDay {
  day: string;
  queue_size: number;
  n_served: number;
  n_completed: number;
  rate: number | null;
}

export interface QueueCompletion {
  by_day: QueueCompletionDay[];
  n_days: number;
  median_rate: number | null;
  low_sample: boolean;
}

export interface SourceYieldCell {
  key: string;
  n_recommended: number;
  n_opened: number;
  n_applied: number;
  n_responded: number;
  open_rate: number | null;
  application_rate: number | null;
  response_rate: number | null;
  low_sample: boolean;
}

export interface RankingMetricsResponse {
  generated_at: string;
  min_sample: number;
  ghost_days: number;
  top10_application_rate: Top10ApplicationRate;
  time_to_application: TimeToApplication;
  response_rate: ResponseRateCell;
  stale_rate: StaleRateCell;
  ghost_rate: GhostRateCell;
  queue_completion: QueueCompletion;
  source_yield: { by_source: SourceYieldCell[]; by_source_category: SourceYieldCell[] };
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
