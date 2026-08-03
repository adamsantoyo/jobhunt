import json
from types import SimpleNamespace

import pytest

import sweep


def test_next_appends_attempt_history_to_legacy_step(monkeypatch, tmp_path):
    state_path = tmp_path / "sweep_state.json"
    state_path.write_text(json.dumps({
        "example": {"status": "failed", "secs": 7, "rc": 3, "legacy": "kept"},
    }))
    monkeypatch.setattr(sweep, "STATE", str(state_path))
    monkeypatch.setattr(sweep, "STEPS", [("example", ["example.py"])])
    monkeypatch.setattr(sweep.sys, "argv", ["sweep.py", "--next"])
    monkeypatch.setattr(
        sweep.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="ok\n", stderr="", returncode=0),
    )

    sweep.main()

    record = json.loads(state_path.read_text())["example"]
    assert record["status"] == "done"
    assert record["legacy"] == "kept"
    assert len(record["attempts"]) == 1
    assert record["attempts"][0]["attempt"] == 1
    assert record["attempts"][0]["rc"] == 0
    assert record["attempts"][0]["timed_out"] is False


def test_interrupted_atomic_save_preserves_last_complete_state(monkeypatch, tmp_path):
    state_path = tmp_path / "sweep_state.json"
    original = {"example": {"status": "done", "rc": 0}}
    state_path.write_text(json.dumps(original))
    monkeypatch.setattr(sweep, "STATE", str(state_path))

    def interrupt_replace(source, destination):
        raise OSError("simulated interrupted replacement")

    monkeypatch.setattr(sweep.os, "replace", interrupt_replace)

    with pytest.raises(OSError, match="interrupted replacement"):
        sweep.save({"example": {"status": "failed", "rc": 1}})

    assert json.loads(state_path.read_text()) == original