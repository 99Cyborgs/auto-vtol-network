from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from avn.core.state import ReplayBundle, ScenarioDefinition
from avn.governance.artifacts import write_run_artifacts
from avn.governance.thresholds import build_promotion_decisions, build_threshold_ledger
from avn.governance.validation import build_run_validation_report
from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


@dataclass(slots=True)
class RunResult:
    scenario_id: str
    output_dir: Path
    replay_path: Path
    summary_path: Path
    threshold_ledger_path: Path
    promotion_decisions_path: Path
    validation_report_path: Path
    artifact_manifest_path: Path
    replay: ReplayBundle


def run_scenario(identifier: str | Path, *, output_root: str | Path | None = None) -> RunResult:
    scenario = load_scenario(identifier)
    return run_loaded_scenario(scenario, output_root=output_root)


def run_loaded_scenario(
    scenario: ScenarioDefinition,
    *,
    output_root: str | Path | None = None,
    output_name: str | None = None,
) -> RunResult:
    replay = SimulationEngine(scenario).run()
    root = Path(output_root).resolve() if output_root else scenario.output_root
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = output_name or f"{scenario.scenario_id}_{timestamp}"
    output_dir = root / run_name
    output_dir.mkdir(parents=True, exist_ok=False)
    threshold_ledger = build_threshold_ledger(replay, scenario)
    promotion_decisions = build_promotion_decisions(threshold_ledger)
    validation_report = build_run_validation_report(
        replay=replay.to_dict(),
        summary=replay.summary,
        threshold_ledger=threshold_ledger.to_dict(),
        promotion_decisions=promotion_decisions.to_dict(),
    )
    paths = write_run_artifacts(
        output_dir,
        replay=replay,
        summary=replay.summary,
        threshold_ledger=threshold_ledger.to_dict(),
        promotion_decisions=promotion_decisions.to_dict(),
        validation_report=validation_report,
    )
    return RunResult(
        scenario_id=scenario.scenario_id,
        output_dir=output_dir,
        replay_path=paths["replay"],
        summary_path=paths["summary"],
        threshold_ledger_path=paths["threshold_ledger"],
        promotion_decisions_path=paths["promotion_decisions"],
        validation_report_path=paths["validation_report"],
        artifact_manifest_path=paths["artifact_manifest"],
        replay=replay,
    )
