from __future__ import annotations

import json
from pathlib import Path

from avn.governance.sweep import run_adaptive_sweep
from avn.governance.validation import validate_run_directory
from avn.sim.runner import run_scenario


def test_run_emits_canonical_governed_artifacts(tmp_path: Path) -> None:
    result = run_scenario("weather_closure", output_root=tmp_path)

    assert result.replay_path.exists()
    assert result.summary_path.exists()
    assert result.threshold_ledger_path.exists()
    assert result.promotion_decisions_path.exists()
    assert result.validation_report_path.exists()
    assert result.artifact_manifest_path.exists()

    threshold_ledger = json.loads(result.threshold_ledger_path.read_text(encoding="utf-8"))
    promotion_decisions = json.loads(result.promotion_decisions_path.read_text(encoding="utf-8"))
    validation_report = json.loads(result.validation_report_path.read_text(encoding="utf-8"))
    manifest = json.loads(result.artifact_manifest_path.read_text(encoding="utf-8"))

    assert threshold_ledger["scenario_id"] == "weather_closure"
    assert threshold_ledger["summary"]["threshold_count"] >= 1
    assert promotion_decisions["scenario_id"] == "weather_closure"
    assert validation_report["status"] == "passed"
    assert manifest["artifact_family"] == "avn_run"
    assert {artifact["artifact_type"] for artifact in manifest["artifacts"]} == {
        "promotion_decisions",
        "replay",
        "summary",
        "threshold_ledger",
        "validation_report",
    }

    validated = validate_run_directory(result.output_dir)
    assert validated.status == "passed"


def test_adaptive_sweep_runs_on_canonical_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "adaptive_sweep_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "sweep_id": "queue-threshold-sweep",
                "scenario": "baseline_flow",
                "output_root": str(tmp_path / "outputs"),
                "metric_key": "max_queue_length",
                "axis": {
                    "path": "alert_thresholds.queue_pressure",
                    "values": [0, 10],
                },
                "max_iterations": 3,
            }
        ),
        encoding="utf-8",
    )

    sweep, paths = run_adaptive_sweep(manifest_path)

    assert sweep.sweep_id == "queue-threshold-sweep"
    assert sweep.axis_path == "alert_thresholds.queue_pressure"
    assert len(sweep.points) >= 2
    assert {point.release_status for point in sweep.points} == {"allow", "blocked"}
    assert paths["adaptive_sweep"].exists()
    assert paths["validation_report"].exists()
    assert paths["artifact_manifest"].exists()
