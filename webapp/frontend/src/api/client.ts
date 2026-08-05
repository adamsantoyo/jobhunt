// Typed fetch wrapper. Adds the X-App CSRF header on EVERY request and JSON-parses
// error bodies. No component should call fetch directly; use these functions.
import type {
  JobsResponse,
  JobFull,
  JobState,
  CompanyState,
  AnalyticsResponse,
  ChangesResponse,
  FreshnessResponse,
  AppConfig,
  IngestReport,
  StatePatch,
  QuickActionBody,
  CompanyPatch,
  ConfigPatch,
  ReviewItem,
  ReconcileBody,
  FunnelResponse,
  FollowupsResponse,
  ActivityResponse,
  RunKind,
  RunStartResponse,
  RunSummary,
  RunDetail,
  SourceOpsResponse,
  RetryResponse,
} from "./types";

const APP_HEADER = "X-App";
const APP_VALUE = "jobhunt";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = "ApiError";
  }
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    [APP_HEADER]: APP_VALUE,
    ...((options.headers as Record<string, string> | undefined) ?? {}),
  };
  if (options.body != null) headers["Content-Type"] = "application/json";

  const res = await fetch(path, { ...options, headers });

  if (!res.ok) {
    let detail: unknown = res.statusText;
    try {
      const data = await res.json();
      detail = (data && typeof data === "object" && "detail" in data) ? data.detail : data;
    } catch {
      /* non-JSON error body; keep statusText */
    }
    const message = typeof detail === "string" ? detail : JSON.stringify(detail);
    throw new ApiError(res.status, message);
  }

  if (res.status === 204) return undefined as T;
  const ct = res.headers.get("content-type") ?? "";
  if (!ct.includes("application/json")) return undefined as T;
  return (await res.json()) as T;
}

function jsonBody(body: unknown): RequestInit {
  return { body: JSON.stringify(body) };
}

export const api = {
  // jobs
  getJobs: () => request<JobsResponse>("/api/jobs"),
  getJob: (urlB64: string) => request<JobFull>(`/api/jobs/${urlB64}`),
  patchState: (urlB64: string, patch: StatePatch) =>
    request<JobState>(`/api/jobs/${urlB64}/state`, { method: "PATCH", ...jsonBody(patch) }),
  quickAction: (urlB64: string, body: QuickActionBody) =>
    request<JobState>(`/api/jobs/${urlB64}/quick`, { method: "POST", ...jsonBody(body) }),

  // companies
  getCompanies: () => request<CompanyState[]>("/api/companies"),
  patchCompany: (company: string, patch: CompanyPatch) =>
    request<CompanyState>(`/api/companies/${encodeURIComponent(company)}`, {
      method: "PATCH",
      ...jsonBody(patch),
    }),

  // review / analytics / changes / freshness
  getReview: () => request<ReviewItem[]>("/api/review"),
  reconcile: (body: ReconcileBody) =>
    request<JobState>("/api/review/reconcile", { method: "POST", ...jsonBody(body) }),
  getAnalytics: () => request<AnalyticsResponse>("/api/analytics"),
  getChanges: (since?: string) =>
    request<ChangesResponse>(`/api/changes${since ? `?since=${encodeURIComponent(since)}` : ""}`),
  getFreshness: () => request<FreshnessResponse>("/api/freshness"),
  getFunnel: () => request<FunnelResponse>("/api/funnel"),
  getFollowups: () => request<FollowupsResponse>("/api/followups"),
  getActivity: () => request<ActivityResponse>("/api/activity"),

  // config
  getConfig: () => request<AppConfig>("/api/config"),
  patchConfig: (patch: ConfigPatch) =>
    request<AppConfig>("/api/config", { method: "PATCH", ...jsonBody(patch) }),

  // sweep control
  refreshQuick: () =>
    request<{ started: boolean; kind: string }>("/api/refresh/quick", { method: "POST" }),
  sweepFull: () =>
    request<{ started: boolean; kind: string }>("/api/sweep/full", { method: "POST" }),
  sweepCancel: () => request<unknown>("/api/sweep/cancel", { method: "POST" }),
  ingest: () => request<IngestReport>("/api/ingest", { method: "POST" }),

  // canonical runs (Phase 4.3). GET /api/runs answers 503 on a legacy
  // database -- see useRunsCapability, which is the only caller that treats
  // that status as expected rather than an error. The event stream itself is
  // opened directly with `new EventSource(...)` in RunPanel, not through
  // `request()`: EventSource cannot set the X-App header, same as the
  // existing /api/sweep/progress stream.
  getRuns: (limit?: number) =>
    request<RunSummary[]>(`/api/runs${limit != null ? `?limit=${limit}` : ""}`),
  getRunDetail: (runUid: string) =>
    request<RunDetail>(`/api/runs/${encodeURIComponent(runUid)}`),
  createRun: (kind: RunKind) =>
    request<RunStartResponse>("/api/runs", { method: "POST", ...jsonBody({ kind }) }),
  cancelRun: (runUid: string) =>
    request<{ run_uid: string; cancelling: boolean }>(
      `/api/runs/${encodeURIComponent(runUid)}/cancel`,
      { method: "POST" },
    ),

  // source operations panel (Phase 4.4). Pinned contract: phase4-spec.md
  // wave-2 decision 8.
  getSourceOps: () => request<SourceOpsResponse>("/api/sources/ops"),
  retrySource: (source: string) =>
    request<RetryResponse>(`/api/sources/${encodeURIComponent(source)}/retry`, {
      method: "POST",
    }),
};
