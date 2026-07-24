import { useEffect, useState } from "react";
import { useConfig, usePatchConfig } from "../store/queries";
import type { ConfigPatch } from "../api/types";

// Settings modal: edit the skills list (used for JD highlighting + skill_hits),
// the comp band [lo, hi] (overlaid on the analytics comp histogram), and goal
// configuration knobs (daily queue size, weekly app target, deadline, snooze days).
export function SettingsDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { data: config } = useConfig();
  const patch = usePatchConfig();

  // Goals section
  const [dailyQueueSize, setDailyQueueSize] = useState("");
  const [weeklyAppTarget, setWeeklyAppTarget] = useState("");
  const [deadline, setDeadline] = useState("");
  const [snoozeDefaultDays, setSnoozeDefaultDays] = useState("");

  // Display section
  const [skillsText, setSkillsText] = useState("");
  const [lo, setLo] = useState("");
  const [hi, setHi] = useState("");

  // Reload local editor state whenever the dialog opens or config changes.
  useEffect(() => {
    if (!open || !config) return;
    // Goals
    setDailyQueueSize(String(config.daily_queue_size));
    setWeeklyAppTarget(String(config.weekly_app_target));
    setDeadline(config.deadline);
    setSnoozeDefaultDays(String(config.snooze_default_days));
    // Display
    setSkillsText(config.skills.join("\n"));
    setLo(String(config.comp_band[0]));
    setHi(String(config.comp_band[1]));
  }, [open, config]);

  if (!open) return null;

  const save = () => {
    const patchPayload: ConfigPatch = {};

    // Goals section: only include valid finite integers
    const dailyQueueN = parseInt(dailyQueueSize, 10);
    if (Number.isFinite(dailyQueueN)) {
      patchPayload.daily_queue_size = dailyQueueN;
    }

    const weeklyAppN = parseInt(weeklyAppTarget, 10);
    if (Number.isFinite(weeklyAppN)) {
      patchPayload.weekly_app_target = weeklyAppN;
    }

    // Deadline: only include if not empty
    if (deadline.trim()) {
      patchPayload.deadline = deadline;
    }

    const snoozeN = parseInt(snoozeDefaultDays, 10);
    if (Number.isFinite(snoozeN)) {
      patchPayload.snooze_default_days = snoozeN;
    }

    // Display section
    // Always sent: an empty list is a valid edit (clearing all skills).
    patchPayload.skills = skillsText
      .split(/[\n,]/)
      .map((s) => s.trim())
      .filter(Boolean);

    const loN = parseInt(lo, 10);
    const hiN = parseInt(hi, 10);
    if (Number.isFinite(loN) && Number.isFinite(hiN)) {
      patchPayload.comp_band = [loN, hiN];
    }

    patch.mutate(patchPayload, { onSuccess: () => onClose() });
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
          {/* Goals section */}
          <div className="settings-section">
            <h3>Goals</h3>
            <div className="field-row">
              <label className="field">
                <span className="field-label">Daily queue size</span>
                <input
                  className="input"
                  type="number"
                  min="1"
                  max="50"
                  value={dailyQueueSize}
                  onChange={(e) => setDailyQueueSize(e.target.value)}
                />
              </label>
              <label className="field">
                <span className="field-label">Weekly application target</span>
                <input
                  className="input"
                  type="number"
                  min="1"
                  value={weeklyAppTarget}
                  onChange={(e) => setWeeklyAppTarget(e.target.value)}
                />
              </label>
            </div>

            <div className="field-row">
              <label className="field">
                <span className="field-label">Deadline</span>
                <input
                  className="input"
                  type="date"
                  value={deadline}
                  onChange={(e) => setDeadline(e.target.value)}
                />
              </label>
              <label className="field">
                <span className="field-label">Default snooze (days)</span>
                <input
                  className="input"
                  type="number"
                  min="1"
                  value={snoozeDefaultDays}
                  onChange={(e) => setSnoozeDefaultDays(e.target.value)}
                />
              </label>
            </div>
          </div>

          {/* Display section */}
          <div className="settings-section">
            <h3>Display</h3>
            <p className="settings-caption">These affect highlighting and charts only. They do not change scoring or the pipeline.</p>

            <label className="field">
              <span className="field-label">Skills (one per line, highlighted in job descriptions)</span>
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
