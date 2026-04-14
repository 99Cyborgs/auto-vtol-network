from __future__ import annotations

import copy
import json
from pathlib import Path

from avn.governance.artifacts import write_sweep_artifacts
from avn.governance.models import AdaptiveSweepManifest, AdaptiveSweepResult, SweepAxis, SweepPointResult
from avn.governance.thresholds import build_promotion_decisions, build_threshold_ledger
from avn.governance.validation import build_run_validation_report
from avn.sim.runner import run_loaded_scenario
from avn.sim.scenario_loader import load_scenario
from avn.core.policies import get_policy_profile


def load_sweep_manifest(path: str | Path) -> AdaptiveSweepManifest:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    axis = payload["axis"]
    return AdaptiveSweepManifest(
        sweep_id=str(payload["sweep_id"]),
        scenario=str(payload["scenario"]),
        output_root=str(payload.get("output_root", "outputs/avn")),
        metric_key=str(payload["metric_key"]),
        axis=SweepAxis(path=str(axis["path"]), values=tuple(axis["values"])),
        max_iterations=int(payload.get("max_iterations", 6)),
        initial_samples=int(payload.get("initial_samples", 3)),
    )


def _apply_override(scenario, path: str, value) -> None:
    parts = path.split(".")
    if len(parts) < 2:
        raise ValueError(f"Unsupported override path: {path}")
    if parts[0] == "alert_thresholds":
        scenario.alert_thresholds[parts[1]] = value
        return
    collection_name, item_id, field_name = parts[0], parts[1], parts[2]
    collection = getattr(scenario, collection_name)
    id_field = {
        "nodes": "node_id",
        "corridors": "corridor_id",
        "vehicles": "vehicle_id",
        "disturbances": "disturbance_id",
    }.get(collection_name)
    if id_field is None:
        raise ValueError(f"Unsupported override collection: {collection_name}")
    for item in collection:
        if getattr(item, id_field) == item_id:
            setattr(item, field_name, value)
            return
    raise ValueError(f"Could not resolve override target {path}")


def _refine_numeric_values(values: list[float | int]) -> list[float]:
    ordered = sorted(float(value) for value in values)
    refined: list[float] = []
    for lower, upper in zip(ordered, ordered[1:], strict=False):
        midpoint = round((lower + upper) / 2.0, 6)
        if midpoint not in ordered:
            refined.append(midpoint)
    return refined


def run_adaptive_sweep(manifest_path: str | Path) -> tuple[AdaptiveSweepResult, dict[str, Path]]:
    manifest = load_sweep_manifest(manifest_path)
    base_scenario = load_scenario(manifest.scenario)
    output_root = Path(manifest.output_root).resolve()
    output_dir = output_root / f"{manifest.sweep_id}"
    output_dir.mkdir(parents=True, exist_ok=False)

    pending = list(manifest.axis.values)
    executed_values: set[float | int | str | bool] = set()
    point_results: list[SweepPointResult] = []
    stopping_reason = "max_iterations_reached"
    last_ledger = None
    last_promotion = None

    for _iteration in range(manifest.max_iterations):
        iteration_values = [value for value in pending if value not in executed_values]
        if not iteration_values:
            stopping_reason = "no_new_points"
            break
        for value in iteration_values:
            scenario = copy.deepcopy(base_scenario)
            _apply_override(scenario, manifest.axis.path, value)
            result = run_loaded_scenario(
                scenario,
                output_root=output_dir / "runs",
                output_name=f"{manifest.axis.path.replace('.', '_')}_{str(value).replace('.', '_')}",
            )
            ledger = build_threshold_ledger(result.replay, scenario)
            promotion = build_promotion_decisions(ledger)
            metric_evaluation = next(
                evaluation
                for evaluation in ledger.evaluations
                if evaluation.metric_key == manifest.metric_key
            )
            observed_metric = float(result.replay.summary[manifest.metric_key])
            point_results.append(
                SweepPointResult(
                    point_id=f"{manifest.sweep_id}:{value}",
                    axis_value=value,
                    run_dir=str(result.output_dir),
                    release_status="allow" if metric_evaluation.status == "passed" else "blocked",
                    observed_metric=observed_metric,
                    threshold_target=metric_evaluation.target_value,
                )
            )
            executed_values.add(value)
            last_ledger = ledger
            last_promotion = promotion

        numeric_values = [value for value in executed_values if isinstance(value, (int, float)) and not isinstance(value, bool)]
        release_statuses = {point.release_status for point in point_results}
        if len(release_statuses) <= 1 or len(numeric_values) < 2:
            stopping_reason = "uniform_release_status"
            break
        pending = _refine_numeric_values(numeric_values)
        if not pending:
            stopping_reason = "axis_fully_refined"
            break
    else:
        stopping_reason = "max_iterations_reached"

    if last_ledger is None or last_promotion is None:
        raise RuntimeError("Adaptive sweep produced no results.")

    sweep = AdaptiveSweepResult(
        sweep_id=manifest.sweep_id,
        contract_version=1,
        scenario_id=base_scenario.scenario_id,
        metric_key=manifest.metric_key,
        axis_path=manifest.axis.path,
        stopping_reason=stopping_reason,
        points=sorted(point_results, key=lambda point: str(point.axis_value)),
        thresholds=last_ledger.evaluations,
        promotion=last_promotion,
    )
    validation = build_run_validation_report(
        replay={
            "scenario_id": base_scenario.scenario_id,
            "policy": {
                "policy_id": get_policy_profile(base_scenario.policy_id).policy_id,
                "label": get_policy_profile(base_scenario.policy_id).label,
                "description": get_policy_profile(base_scenario.policy_id).description,
            },
            "summary": {"sweep_id": manifest.sweep_id},
            "steps": [{"nodes": [], "corridors": [], "vehicles": [], "metrics": {}, "alerts": [], "events": []}],
            "event_log": [],
        },
        summary={"scenario_id": base_scenario.scenario_id, "completed_vehicles": 0, "max_queue_length": 0, "max_corridor_load_ratio": 0},
        threshold_ledger={"scenario_id": base_scenario.scenario_id, "evaluations": [item.to_dict() for item in last_ledger.evaluations], "summary": last_ledger.summary},
        promotion_decisions=last_promotion.to_dict(),
    )
    paths = write_sweep_artifacts(output_dir, sweep, validation)
    return sweep, paths
