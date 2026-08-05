// TanStack Query hooks. This is the pinned public data surface every view builds
// against. Mutations optimistically patch the single jobs cache and invalidate on
// settle so all views stay consistent from one /api/jobs fetch.
import {
  useQuery,
  useMutation,
  useQueryClient,
  type QueryClient,
} from "@tanstack/react-query";
import { api, ApiError } from "../api/client";
import { dateToISO, isoPlusDays, todayISO } from "../lib/format";
import type {
  JobsResponse,
  JobFull,
  JobState,
  StatePatch,
  QuickActionBody,
  CompanyPatch,
  ConfigPatch,
  ReconcileBody,
  RunKind,
} from "../api/types";

export const qk = {
  jobs: ["jobs"] as const,
  job: (b64: string) => ["job", b64] as const,
  companies: ["companies"] as const,
  analytics: ["analytics"] as const,
  changes: (since?: string) => ["changes", since ?? null] as const,
  freshness: ["freshness"] as const,
  config: ["config"] as const,
  review: ["review"] as const,
  funnel: ["funnel"] as const,
  followups: ["followups"] as const,
  activity: ["activity"] as const,
  // canonical runs (Phase 4.3) + source operations (Phase 4.4)
  runsCapability: ["runsCapability"] as const,
  runsAll: ["runs"] as const,
  runs: (limit: number) => ["runs", limit] as const,
  runDetail: (runUid: string) => ["runDetail", runUid] as const,
  sourceOps: ["sourceOps"] as const,
};

// Local-naive ISO timestamp matching backend models.now_iso() (datetime.now().isoformat()).
function nowIso(): string {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${dateToISO(d)}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

const EMPTY_STATE: JobState = {
  status: "New",
  notes: "",
  follow_up_date: null,
  applied_date: null,
  starred: false,
  hidden: false,
  contact: "",
  snoozed_until: null,
  applied_via: null,
  needs_review: false,
  review_reason: null,
  updated_at: "",
  status_since: null,
};

function mergePatch(existing: JobState | null, patch: StatePatch): JobState {
  const base = existing ?? EMPTY_STATE;
  // review_dismissed is write-only (not a JobState field) -- never leak it into
  // the cached state object.
  const rest = { ...patch };
  delete rest.review_dismissed;
  // Any user edit clears needs_review (matches the backend PATCH contract).
  // Optimistic status_since: server truth replaces this onSuccess.
  const statusSince = patch.status !== undefined ? nowIso() : base.status_since;
  return { ...base, ...rest, needs_review: false, status_since: statusSince };
}

function applyQuick(existing: JobState | null, body: QuickActionBody): JobState {
  const base = existing ?? EMPTY_STATE;
  switch (body.action) {
    case "applied":
      return {
        ...base,
        status: "Applied",
        applied_date: base.applied_date ?? todayISO(),
        status_since: nowIso(),
      };
    case "snooze":
      return { ...base, snoozed_until: isoPlusDays(body.days ?? 3) };
    case "pass":
      return { ...base, status: "Passed", status_since: nowIso() };
    case "star":
      return { ...base, starred: true };
    case "unstar":
      return { ...base, starred: false };
    default:
      return base;
  }
}

function patchJobsCache(
  qc: QueryClient,
  urlB64: string,
  updater: (state: JobState | null) => JobState,
) {
  qc.setQueryData<JobsResponse>(qk.jobs, (prev) => {
    if (!prev) return prev;
    return {
      ...prev,
      jobs: prev.jobs.map((j) =>
        j.url_b64 === urlB64 ? { ...j, state: updater(j.state) } : j,
      ),
    };
  });
  qc.setQueryData<JobFull>(qk.job(urlB64), (prev) =>
    prev ? { ...prev, state: updater(prev.state) } : prev,
  );
}

function invalidateStateDerived(qc: QueryClient) {
  qc.invalidateQueries({ queryKey: qk.jobs });
  qc.invalidateQueries({ queryKey: qk.review });
  qc.invalidateQueries({ queryKey: qk.analytics });
  qc.invalidateQueries({ queryKey: qk.followups });
  qc.invalidateQueries({ queryKey: qk.activity });
  qc.invalidateQueries({ queryKey: qk.funnel });
}

export function useReconcile() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: ReconcileBody) => api.reconcile(body),
    onSettled: (_state, _err, body) => {
      invalidateStateDerived(qc);
      // State moved between two url-keyed detail caches -- refresh both, or the
      // drawer would show stale state for either job.
      qc.invalidateQueries({ queryKey: qk.job(body.from_url_b64) });
      qc.invalidateQueries({ queryKey: qk.job(body.to_url_b64) });
    },
  });
}

// ---- queries ----

export function useJobs() {
  return useQuery({ queryKey: qk.jobs, queryFn: api.getJobs, staleTime: 60_000 });
}

export function useJobDetail(urlB64: string | null | undefined) {
  return useQuery({
    queryKey: qk.job(urlB64 ?? ""),
    queryFn: () => api.getJob(urlB64 as string),
    enabled: !!urlB64,
  });
}

export function useCompanies() {
  return useQuery({ queryKey: qk.companies, queryFn: api.getCompanies, staleTime: 60_000 });
}

export function useAnalytics() {
  return useQuery({ queryKey: qk.analytics, queryFn: api.getAnalytics, staleTime: 60_000 });
}

export function useChanges(since?: string) {
  return useQuery({
    queryKey: qk.changes(since),
    queryFn: () => api.getChanges(since),
    staleTime: 60_000,
  });
}

export function useFreshness() {
  return useQuery({
    queryKey: qk.freshness,
    queryFn: api.getFreshness,
    staleTime: 15_000,
    refetchInterval: 30_000,
  });
}

export function useConfig() {
  return useQuery({ queryKey: qk.config, queryFn: api.getConfig, staleTime: 300_000 });
}

export function useReview() {
  return useQuery({ queryKey: qk.review, queryFn: api.getReview, staleTime: 60_000 });
}

export function useFunnel() {
  return useQuery({ queryKey: qk.funnel, queryFn: api.getFunnel, staleTime: 60_000 });
}

export function useFollowups() {
  return useQuery({ queryKey: qk.followups, queryFn: api.getFollowups, staleTime: 60_000 });
}

export function useActivity() {
  return useQuery({ queryKey: qk.activity, queryFn: api.getActivity, staleTime: 30_000 });
}

// ---- mutations ----

export function usePatchState() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ urlB64, patch }: { urlB64: string; patch: StatePatch }) =>
      api.patchState(urlB64, patch),
    onMutate: async ({ urlB64, patch }) => {
      await qc.cancelQueries({ queryKey: qk.jobs });
      const prevJobs = qc.getQueryData<JobsResponse>(qk.jobs);
      const prevJob = qc.getQueryData<JobFull>(qk.job(urlB64));
      patchJobsCache(qc, urlB64, (s) => mergePatch(s, patch));
      return { prevJobs, prevJob, urlB64 };
    },
    onError: (_err, _vars, ctx) => {
      if (!ctx) return;
      if (ctx.prevJobs) qc.setQueryData(qk.jobs, ctx.prevJobs);
      if (ctx.prevJob) qc.setQueryData(qk.job(ctx.urlB64), ctx.prevJob);
    },
    onSuccess: (state, { urlB64 }) => {
      // Replace optimistic guess with the authoritative server state.
      patchJobsCache(qc, urlB64, () => state);
    },
    onSettled: () => invalidateStateDerived(qc),
  });
}

export function useQuickAction() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ urlB64, body }: { urlB64: string; body: QuickActionBody }) =>
      api.quickAction(urlB64, body),
    onMutate: async ({ urlB64, body }) => {
      await qc.cancelQueries({ queryKey: qk.jobs });
      const prevJobs = qc.getQueryData<JobsResponse>(qk.jobs);
      const prevJob = qc.getQueryData<JobFull>(qk.job(urlB64));
      patchJobsCache(qc, urlB64, (s) => applyQuick(s, body));
      return { prevJobs, prevJob, urlB64 };
    },
    onError: (_err, _vars, ctx) => {
      if (!ctx) return;
      if (ctx.prevJobs) qc.setQueryData(qk.jobs, ctx.prevJobs);
      if (ctx.prevJob) qc.setQueryData(qk.job(ctx.urlB64), ctx.prevJob);
    },
    onSuccess: (state, { urlB64 }) => {
      patchJobsCache(qc, urlB64, () => state);
    },
    onSettled: () => invalidateStateDerived(qc),
  });
}

export function usePatchCompany() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ company, patch }: { company: string; patch: CompanyPatch }) =>
      api.patchCompany(company, patch),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: qk.companies });
    },
  });
}

export function usePatchConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (patch: ConfigPatch) => api.patchConfig(patch),
    onSuccess: (config) => {
      qc.setQueryData(qk.config, config);
    },
    onSettled: () => {
      qc.invalidateQueries({ queryKey: qk.config });
      // skill_hits in job details depend on the skills list.
      qc.invalidateQueries({ queryKey: qk.jobs });
    },
  });
}

// ---------------------------------------------------------------------------
// Canonical runs (Phase 4.3) + source operations (Phase 4.4)
// ---------------------------------------------------------------------------

export type RunsCapability = "legacy" | "canonical";

/**
 * Probes GET /api/runs (spec decision 7): 200 means the database has the
 * canonical run schema and the new run UI drives it; 503 means a pre-Phase-4
 * database, and every canonical control stays hidden in favor of the
 * existing legacy sweep UI.
 *
 * A confirmed "canonical" result is cached indefinitely and never refetched
 * on window focus: once a database has the canonical schema it never loses
 * it again this process's lifetime, so there is nothing to re-check.
 *
 * A "legacy" result is NOT final the same way: a server restart runs forward
 * migrations on an existing database (see phase4-spec.md's standing watch
 * items), and a restart does NOT reload this SPA tab -- the tab is a
 * long-lived page in the browser, a restart is a separate backend process,
 * and nothing forces a reload between them. A tab left open across exactly
 * that migration would otherwise be stuck offering the legacy sweep UI
 * forever with no way to notice the upgrade short of a manual reload. While
 * in "legacy", this gently re-probes every 60s (and treats the result as
 * stale after the same interval) so such a tab picks up the migration on its
 * own; a confirmed "canonical" result needs none of that.
 *
 * Any state other than a confirmed "canonical" (loading, network error, a
 * non-503 error) must be treated by callers as legacy: the legacy path is
 * the safe default the app already behaves like today.
 */
const LEGACY_RECHECK_MS = 60_000;

export function useRunsCapability() {
  return useQuery({
    queryKey: qk.runsCapability,
    queryFn: async (): Promise<RunsCapability> => {
      try {
        await api.getRuns(1);
        return "canonical";
      } catch (e) {
        if (e instanceof ApiError && e.status === 503) return "legacy";
        throw e;
      }
    },
    staleTime: (query) => (query.state.data === "canonical" ? Infinity : LEGACY_RECHECK_MS),
    refetchInterval: (query) => (query.state.data === "canonical" ? false : LEGACY_RECHECK_MS),
    refetchOnWindowFocus: false,
  });
}

export function useRuns(limit = 20, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.runs(limit),
    queryFn: () => api.getRuns(limit),
    enabled: options.enabled ?? true,
    staleTime: 5_000,
  });
}

export function useRunDetail(
  runUid: string | null | undefined,
  options: { refetchInterval?: number | false } = {},
) {
  return useQuery({
    queryKey: qk.runDetail(runUid ?? ""),
    queryFn: () => api.getRunDetail(runUid as string),
    enabled: !!runUid,
    refetchInterval: options.refetchInterval ?? false,
  });
}

export function useCreateRun() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (kind: RunKind) => api.createRun(kind),
    onSettled: () => qc.invalidateQueries({ queryKey: qk.runsAll }),
  });
}

export function useCancelRun() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (runUid: string) => api.cancelRun(runUid),
    onSettled: (_data, _err, runUid) => {
      qc.invalidateQueries({ queryKey: qk.runsAll });
      qc.invalidateQueries({ queryKey: qk.runDetail(runUid) });
    },
  });
}

export function useSourceOps(options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.sourceOps,
    queryFn: api.getSourceOps,
    enabled: options.enabled ?? true,
    staleTime: 15_000,
  });
}

export function useRetrySource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (source: string) => api.retrySource(source),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: qk.sourceOps });
      // A retry starts a run; the mounted AppShell's `useRuns` observer
      // picks it up and mounts a RunPanel for it without any prop plumbing
      // between this hook's caller (the Sources tab) and AppShell.
      qc.invalidateQueries({ queryKey: qk.runsAll });
    },
  });
}
