import { useEffect, useState } from "react";
import { usePatchCompany } from "../../store/queries";
import { fmtDate } from "../../lib/format";
import type { CompanyState } from "../../api/types";

// Per-company contact + notes editor (PATCH /api/companies/{company}). Controlled
// local drafts seeded from the company_state row; saves only the changed fields.
export function CompanyEditor({
  company,
  state,
}: {
  company: string;
  state: CompanyState | undefined;
}) {
  const patchCompany = usePatchCompany();
  const [contact, setContact] = useState(state?.contact ?? "");
  const [notes, setNotes] = useState(state?.notes ?? "");

  // Re-seed drafts if the server row changes (e.g. after invalidation) while not editing.
  useEffect(() => {
    setContact(state?.contact ?? "");
    setNotes(state?.notes ?? "");
  }, [state?.contact, state?.notes]);

  const dirty = contact !== (state?.contact ?? "") || notes !== (state?.notes ?? "");

  const save = () => {
    if (!dirty) return;
    patchCompany.mutate({ company, patch: { contact, notes } });
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 8,
        background: "var(--bg-1)",
        border: "1px solid var(--border-soft)",
        borderRadius: "var(--radius)",
        padding: 10,
      }}
    >
      <div className="field">
        <label className="field-label">Contact / referral</label>
        <input
          className="input"
          value={contact}
          placeholder="name, email, or how you know someone here"
          onChange={(e) => setContact(e.target.value)}
        />
      </div>
      <div className="field">
        <label className="field-label">Notes</label>
        <textarea
          className="input"
          rows={3}
          value={notes}
          placeholder="research, referral status, anything company-wide"
          onChange={(e) => setNotes(e.target.value)}
        />
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <button
          type="button"
          className="btn btn-sm btn-primary"
          disabled={!dirty || patchCompany.isPending}
          onClick={save}
        >
          {patchCompany.isPending ? "Saving…" : "Save"}
        </button>
        {state?.updated_at && (
          <span className="muted-sm">updated {fmtDate(state.updated_at)}</span>
        )}
        {patchCompany.isError && (
          <span className="muted-sm" style={{ color: "var(--red)" }}>
            save failed
          </span>
        )}
      </div>
    </div>
  );
}
