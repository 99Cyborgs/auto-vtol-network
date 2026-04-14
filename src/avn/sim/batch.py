from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from avn.sim.runner import run_loaded_scenario
from avn.sim.scenario_loader import list_scenarios, load_scenario


@dataclass(slots=True)
class BatchRunResult:
    batch_id: str
    output_dir: Path
    summary_path: Path
    manifest_path: Path
    summary: dict[str, Any]
    manifest: dict[str, Any]


def _stat_block(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    numeric_keys = sorted(
        {
            key
            for summary in summaries
            for key, value in summary.items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
    )
    metrics = {
        key: {
            "min": min(float(summary[key]) for summary in summaries),
            "max": max(float(summary[key]) for summary in summaries),
            "mean": round(mean(float(summary[key]) for summary in summaries), 3),
        }
        for key in numeric_keys
    }
    alert_totals: dict[str, int] = {}
    for summary in summaries:
        for code, count in summary.get("alerts_by_code", {}).items():
            alert_totals[code] = alert_totals.get(code, 0) + int(count)
    return {
        "run_count": len(summaries),
        "metrics": metrics,
        "alerts_by_code_total": alert_totals,
    }


def run_scenario_batch(
    scenarios: list[str] | None = None,
    *,
    repeat: int = 1,
    output_root: str | Path | None = None,
    batch_id: str | None = None,
) -> BatchRunResult:
    if repeat < 1:
        raise ValueError("repeat must be >= 1")

    scenario_identifiers = list(scenarios) if scenarios else list_scenarios()
    if not scenario_identifiers:
        raise ValueError("No scenarios were provided for batch execution.")

    root = Path(output_root).resolve() if output_root else Path("outputs/avn_batch").resolve()
    resolved_batch_id = batch_id or datetime.now().strftime("batch_%Y%m%d_%H%M%S")
    output_dir = root / resolved_batch_id
    output_dir.mkdir(parents=True, exist_ok=False)
    runs_root = output_dir / "runs"
    runs_root.mkdir(parents=True, exist_ok=False)

    run_records: list[dict[str, Any]] = []
    summaries_by_scenario: dict[str, list[dict[str, Any]]] = {}

    for scenario_identifier in scenario_identifiers:
        for run_index in range(1, repeat + 1):
            scenario = load_scenario(scenario_identifier)
            output_name = f"{scenario.scenario_id}_r{run_index:02d}"
            result = run_loaded_scenario(scenario, output_root=runs_root, output_name=output_name)
            summary = dict(result.replay.summary)
            summaries_by_scenario.setdefault(result.scenario_id, []).append(summary)
            run_records.append(
                {
                    "scenario_id": result.scenario_id,
                    "scenario_source": str(scenario_identifier),
                    "run_index": run_index,
                    "run_dir": str(result.output_dir),
                    "summary_path": str(result.summary_path),
                    "replay_path": str(result.replay_path),
                    "artifact_manifest_path": str(result.artifact_manifest_path),
                    "summary": summary,
                }
            )

    scenario_statistics = {
        scenario_id: _stat_block(summaries)
        for scenario_id, summaries in sorted(summaries_by_scenario.items())
    }
    suite_statistics = _stat_block([record["summary"] for record in run_records])

    summary_payload = {
        "batch_id": resolved_batch_id,
        "contract_version": 1,
        "repeat": repeat,
        "scenario_sources": [str(item) for item in scenario_identifiers],
        "run_count": len(run_records),
        "runs": run_records,
        "scenario_statistics": scenario_statistics,
        "suite_statistics": suite_statistics,
    }
    summary_path = output_dir / "batch_summary.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    manifest_payload = {
        "batch_id": resolved_batch_id,
        "artifact_family": "avn_batch_run",
        "summary_path": str(summary_path),
        "runs": [
            {
                "scenario_id": record["scenario_id"],
                "run_index": record["run_index"],
                "run_dir": record["run_dir"],
                "artifact_manifest_path": record["artifact_manifest_path"],
            }
            for record in run_records
        ],
    }
    manifest_path = output_dir / "batch_manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")

    return BatchRunResult(
        batch_id=resolved_batch_id,
        output_dir=output_dir,
        summary_path=summary_path,
        manifest_path=manifest_path,
        summary=summary_payload,
        manifest=manifest_payload,
    )
