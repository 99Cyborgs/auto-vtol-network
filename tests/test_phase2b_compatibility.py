from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from avn.governance.sweep import run_adaptive_sweep


ROOT = Path(__file__).resolve().parents[1]


def test_legacy_sweep_shim_is_retired_with_guidance() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_phase2b_sweep.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Historical sweep shim retired." in result.stderr
    assert str((ROOT / "archive" / "legacy_runtime" / "configs" / "trust_and_comms_compound.toml").resolve()) in result.stderr
    assert "python -m avn adaptive-sweep" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr


def test_example_adaptive_sweep_manifest_runs_with_temp_output_root(tmp_path: Path) -> None:
    manifest_path = ROOT / "configs" / "example_adaptive_sweep_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["output_root"] = str(tmp_path / "outputs")
    temp_manifest = tmp_path / "example_adaptive_sweep_manifest.json"
    temp_manifest.write_text(json.dumps(payload), encoding="utf-8")

    sweep, paths = run_adaptive_sweep(temp_manifest)

    assert sweep.sweep_id == "baseline-queue-threshold-sweep"
    assert sweep.axis_path == "alert_thresholds.queue_pressure"
    assert paths["adaptive_sweep"].exists()
    assert paths["validation_report"].exists()
