from __future__ import annotations

import json

import avn.__main__ as cli
from avn.__main__ import main
from avn.sim.batch import run_scenario_batch


def test_run_scenario_batch_writes_aggregate_outputs(tmp_path) -> None:
    result = run_scenario_batch(
        ["baseline_flow", "weather_closure"],
        repeat=1,
        output_root=tmp_path,
        batch_id="batch-smoke",
    )

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert result.output_dir == tmp_path / "batch-smoke"
    assert summary["batch_id"] == "batch-smoke"
    assert summary["run_count"] == 2
    assert [record["scenario_id"] for record in summary["runs"]] == ["baseline_flow", "weather_closure"]
    assert summary["scenario_statistics"]["baseline_flow"]["run_count"] == 1
    assert "max_queue_length" in summary["suite_statistics"]["metrics"]
    assert manifest["artifact_family"] == "avn_batch_run"
    assert len(manifest["runs"]) == 2
    assert all((tmp_path / "batch-smoke" / "runs").exists() for _ in manifest["runs"])


def test_batch_run_rejects_invalid_repeat(tmp_path) -> None:
    try:
        run_scenario_batch(["baseline_flow"], repeat=0, output_root=tmp_path, batch_id="invalid")
    except ValueError as exc:
        assert "repeat must be >= 1" in str(exc)
    else:
        raise AssertionError("Expected ValueError for repeat=0")


def test_batch_cli_passes_none_for_default_scenario_suite(monkeypatch, tmp_path) -> None:
    captured = {}

    class DummyResult:
        batch_id = "demo-batch"
        output_dir = tmp_path / "batch"
        summary_path = tmp_path / "batch" / "batch_summary.json"
        manifest_path = tmp_path / "batch" / "batch_manifest.json"
        summary = {"suite_statistics": {"run_count": 8}}

    def fake_run_scenario_batch(scenarios, *, repeat, output_root, batch_id):
        captured["scenarios"] = scenarios
        captured["repeat"] = repeat
        captured["output_root"] = output_root
        captured["batch_id"] = batch_id
        return DummyResult()

    monkeypatch.setattr(cli, "run_scenario_batch", fake_run_scenario_batch)

    assert main(["batch-run", "--repeat", "2", "--output-root", str(tmp_path)]) == 0
    assert captured["scenarios"] is None
    assert captured["repeat"] == 2
    assert captured["output_root"] == tmp_path
    assert captured["batch_id"] is None


def test_adaptive_sweep_cli_surfaces_existing_output_dir(monkeypatch, tmp_path) -> None:
    def fake_run_adaptive_sweep(_manifest):
        raise FileExistsError(183, "exists", str(tmp_path / "existing-sweep"))

    monkeypatch.setattr(cli, "run_adaptive_sweep", fake_run_adaptive_sweep)

    try:
        main(["adaptive-sweep", str(tmp_path / "manifest.json")])
    except SystemExit as exc:
        assert "already exists" in str(exc)
        assert "existing-sweep" in str(exc)
    else:
        raise AssertionError("Expected SystemExit for an existing adaptive sweep output directory.")
