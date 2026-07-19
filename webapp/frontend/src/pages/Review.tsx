import { useSearchParams } from "react-router-dom";
import { useReview, usePatchState } from "../store/queries";
import { StatusBadge, TierBadge } from "../components/StatusBadge";
import { fmtDate } from "../lib/format";
import type { JobLight } from "../api/types";

/**
 * Manual-reconciliation queue: job_state rows whose URL vanished and whose
 * seen-key match was ambiguous, so ingest refused to guess. The user resolves a
 * row by editing it (any state edit clears the flag) or dismissing it as-is.
 */
export default function Review() {
  const { data: rows, isLoading, isError } = useReview();

  if (isLoading) return <div className="muted">Loading…</div>;
  if (isError) return <div className="page-error">Failed to load the review list.</div>;

  const items = rows ?? [];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0 }}>Needs review</h1>
        <p className="muted" style={{ marginTop: 4, maxWidth: 720 }}>
          These saved statuses lost their job URL in a sweep and matched several possible
          successors, so nothing was migrated automatically. Open one to re-anchor or edit it,
          or dismiss to keep the state as-is; either clears it from this list.
        </p>
      </div>
      {items.length === 0 ? (
        <div className="muted">Nothing needs review. Ambiguous URL rewrites will land here.</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {items.map((job) => (
            <ReviewRow key={job.url_b64} job={job} />
          ))}
        </div>
      )}
    </div>
  );
}

function ReviewRow({ job }: { job: JobLight }) {
  const [, setSearchParams] = useSearchParams();
  const patch = usePatchState();
  const st = job.state;

  const dismiss = () => {
    // Re-asserting the current status counts as a user edit, which clears the flag.
    patch.mutate({ urlB64: job.url_b64, patch: { status: st?.status ?? "New" } });
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 12,
        padding: "10px 14px",
        background: "var(--bg-1)",
        border: "1px solid var(--border)",
        borderRadius: 8,
      }}
    >
      <TierBadge tier={job.tier} />
      <StatusBadge status={st?.status} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontWeight: 600 }}>
          {job.company || "(unknown)"} · {job.title || job.seen_key}
        </div>
        <div className="muted" style={{ fontSize: 12, whiteSpace: "normal" }}>
          {st?.review_reason || "ambiguous url rewrite"}
          {st?.updated_at ? ` — flagged ${fmtDate(st.updated_at)}` : ""}
        </div>
      </div>
      <button
        type="button"
        className="btn btn-sm"
        onClick={() => setSearchParams((p) => ({ ...Object.fromEntries(p), job: job.url_b64 }))}
      >
        Open
      </button>
      <button type="button" className="btn btn-sm" disabled={patch.isPending} onClick={dismiss}>
        Dismiss
      </button>
    </div>
  );
}
