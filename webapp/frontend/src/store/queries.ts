// TanStack Query hooks. This is the pinned public data surface every view builds
// against. Mutations optimistically patch the single jobs cache and invalidate on
// settle so all views stay consistent from one /api/jobs fetch.
import {
  useQuery,
  useMutation,
  useQueryClient,
  type QueryClient,
} from "@tanstack/react-query";
import { api } from "../api/client";
import { isoPlusDays, todayISO } from "../lib/format";
import type {
  JobsResponse,
  JobFull,
  JobState,
  StatePatch,
  QuickActionBody,
  CompanyPatch,
  ConfigPatch,
  ReconcileBody,
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
};

const EMPTY_STATE: JobState = {
  status: "New",
  notes: "",
  follow_up_date: null,
  applied_date: null,
  starred: false,
  hidden: false,
  contact: "",
  snoozed_until: null,
  needs_review: false,
  review_reason: null,
  updated_at: "",
};

function mergePatch(existing: JobState | null, patch: StatePatch): JobState {
  const base = existing ?? EMPTY_STATE;
  // review_dismissed is write-only (not a JobState field) — never leak it into
  // the cached state object.
  const rest = { ...patch };
  delete rest.review_dismissed;
  // Any user edit clears needs_review (matches the backend PATCH contract).
  return { ...base, ...rest, needs_review: false };
}

function applyQuick(existing: JobState | null, body: QuickActionBody): JobState {
  const base = existing ?? EMPTY_STATE;
  switch (body.action) {
    case "applied":
      return { ...base, status: "Applied", applied_date: base.applied_date ?? todayISO() };
    case "snooze":
      return { ...base, snoozed_until: isoPlusDays(body.days ?? 3) };
    case "pass":
      return { ...base, status: "Passed" };
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
}

export function useReconcile() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: ReconcileBody) => api.reconcile(body),
    onSettled: (_state, _err, body) => {
      invalidateStateDerived(qc);
      // State moved between two url-keyed detail caches — refresh both, or the
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
