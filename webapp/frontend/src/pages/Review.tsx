import { useSearchParams } from "react-router-dom";
import { useReview, usePatchState, useReconcile } from "../store/queries";
import { StatusBadge, TierBadge, OddsBadge } from "../components/StatusBadge";
import { fmtDate, fmtSalary } from "../lib/format";
import { NOTES_PREVIEW_CHARS } from "../lib/ui";
import type { ReviewItem, JobLight } from "../api/types";

/**
 * Manual-reconciliation queue: job_state rows whose URL vanished (or was recycled
 * for a different role) and whose seen-key match was ambiguous, so ingest refused
 * to guess. Resolution options, all durable across future ingests:
 *  - Attach the saved state to one of the live candidate jobs (reconcile), or
 *  - Dismiss: keep the state parked as-is and stop flagging it.
 */
export default function Review() {
  const { data: items, isLoading, isError } = useReview();

  if (isLoading) return <div className="muted">Loading…</div>;
  if (isError) return <div className="page-error">Failed to load the review list.</div>;

  const list = items ?? [];

  return (
    <div>
      <div className="rv-header">
        <h1 className="rv-title">Needs review</h1>
        <p className="muted rv-subtitle">
          These saved statuses lost their job URL in a sweep and matched several possible
          successors, so nothing was migrated automatically. Attach the state to the right
          role below, or dismiss to keep it parked; both stick across future refreshes.
        </p>
      </div>
      {list.length === 0 ? (
        <div className="muted">Nothing needs review. Ambiguous URL rewrites will land here.</div>
      ) : (
        <div className="rv-list">
          {list.map((item) => (
            <ReviewCard key={item.job.url_b64} item={item} />
          ))}
        </div>
      )}
    </div>
  );
}

function ReviewCard({ item }: { item: ReviewItem }) {
  const patch = usePatchState();
  const st = item.job.state;

  const dismiss = () => {
    patch.mutate({ urlB64: item.job.url_b64, patch: { review_dismissed: true } });
  };

  return (
    <div className="rv-card">
      <div className="rv-card-head">
        <StatusBadge status={st?.status} />
        <div className="rv-card-main">
          <div className="rv-strong">
            {item.job.company || "(unknown)"} · {item.job.title || item.job.seen_key}
          </div>
          <div className="muted rv-reason">
            {st?.review_reason || "ambiguous url rewrite"}
            {st?.updated_at ? ` — flagged ${fmtDate(st.updated_at)}` : ""}
            {st?.notes ? ` — notes: ${st.notes.slice(0, NOTES_PREVIEW_CHARS)}` : ""}
          </div>
        </div>
        <button type="button" className="btn btn-sm" disabled={patch.isPending} onClick={dismiss}>
          Dismiss
        </button>
      </div>

      {item.candidates.length > 0 && (
        <div className="rv-candidates">
          <div className="muted rv-candidates-label">
            Possible successors — attach the saved state to the right one
          </div>
          {item.candidates.map((cand) => (
            <CandidateRow key={cand.url_b64} fromB64={item.job.url_b64} cand={cand} />
          ))}
        </div>
      )}
    </div>
  );
}

function CandidateRow({ fromB64, cand }: { fromB64: string; cand: JobLight }) {
  const [, setSearchParams] = useSearchParams();
  const reconcile = useReconcile();
  const taken = cand.state != null;

  return (
    <div className="rv-candidate-row">
      <TierBadge tier={cand.tier} />
      <OddsBadge odds={cand.odds} />
      <div className="rv-candidate-title">
        {cand.title} <span className="muted">· {cand.location || "location n/a"}</span>
        {fmtSalary(cand) && <span className="muted"> · {fmtSalary(cand)}</span>}
        {taken && <span className="muted"> · already has its own state</span>}
      </div>
      <button
        type="button"
        className="btn btn-sm"
        onClick={() => setSearchParams((p) => ({ ...Object.fromEntries(p), job: cand.url_b64 }))}
      >
        Open
      </button>
      <button
        type="button"
        className="btn btn-sm"
        disabled={taken || reconcile.isPending}
        title={taken ? "This job already has state of its own" : "Move the saved state onto this job"}
        onClick={() => reconcile.mutate({ from_url_b64: fromB64, to_url_b64: cand.url_b64 })}
      >
        Attach
      </button>
    </div>
  );
}
