import { useEffect, useState } from "react";
import { useConfig, usePatchConfig } from "../store/queries";

// Settings modal: edit the skills list (used for JD highlighting + skill_hits)
// and the comp band [lo, hi] (overlaid on the analytics comp histogram).
export function SettingsDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { data: config } = useConfig();
  const patch = usePatchConfig();

  const [skillsText, setSkillsText] = useState("");
  const [lo, setLo] = useState("");
  const [hi, setHi] = useState("");

  // Reload local editor state whenever the dialog opens or config changes.
  useEffect(() => {
    if (!open || !config) return;
    setSkillsText(config.skills.join("\n"));
    setLo(String(config.comp_band[0]));
    setHi(String(config.comp_band[1]));
  }, [open, config]);

  if (!open) return null;

  const save = () => {
    const skills = skillsText
      .split(/[\n,]/)
      .map((s) => s.trim())
      .filter(Boolean);
    const loN = parseInt(lo, 10);
    const hiN = parseInt(hi, 10);
    const band: [number, number] | undefined =
      Number.isFinite(loN) && Number.isFinite(hiN) ? [loN, hiN] : undefined;
    patch.mutate(
      { skills, ...(band ? { comp_band: band } : {}) },
      { onSuccess: () => onClose() },
    );
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-label="Settings"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-head">
          <h2>Settings</h2>
          <button type="button" className="btn btn-icon" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>

        <div className="modal-body">
          <label className="field">
            <span className="field-label">Skills (one per line — highlighted in job descriptions)</span>
            <textarea
              className="input"
              rows={10}
              value={skillsText}
              onChange={(e) => setSkillsText(e.target.value)}
              placeholder="support engineer&#10;linux&#10;networking"
            />
          </label>

          <div className="field-row">
            <label className="field">
              <span className="field-label">Comp band low ($)</span>
              <input
                className="input"
                type="number"
                value={lo}
                onChange={(e) => setLo(e.target.value)}
              />
            </label>
            <label className="field">
              <span className="field-label">Comp band high ($)</span>
              <input
                className="input"
                type="number"
                value={hi}
                onChange={(e) => setHi(e.target.value)}
              />
            </label>
          </div>
        </div>

        <div className="modal-foot">
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button type="button" className="btn btn-primary" onClick={save} disabled={patch.isPending}>
            {patch.isPending ? "Saving…" : "Save"}
          </button>
        </div>
      </div>
    </div>
  );
}
