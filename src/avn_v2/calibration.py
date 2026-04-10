from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from .artifacts import V2_CONTRACT_VERSION
from .config import load_reference_bundle, load_scenario_config
from .fitting import (
    candidate_rank_key,
    inspect_bundle_coverage,
    score_event_expectations,
    score_metric_targets,
    score_series_targets,
    summarize_fit,
)
from .models import FitParameter, ReferenceBundle, ScenarioConfig
from .validation import build_bundle_validation_report, ensure_bundle_validation_passes
from .reporting import utc_timestamp


SEARCH_EPSILON = 0.01
MAX_PAIRWISE_PASSES = 3


def _apply_overrides(config: ScenarioConfig, overrides: dict[str, int | float]) -> ScenarioConfig:
    cloned = copy.deepcopy(config)
    for dotted_key, value in overrides.items():
        target: object = cloned
        parts = dotted_key.split(".")
        for part in parts[:-1]:
            target = getattr(target, part)
        object.__setattr__(target, parts[-1], value)
    return cloned


def _disable_calibration(config: ScenarioConfig) -> ScenarioConfig:
    cloned = copy.deepcopy(config)
    object.__setattr__(cloned.calibration, "enabled", False)
    object.__setattr__(cloned.calibration, "bundle", None)
    return cloned


def _get_nested_value(config: ScenarioConfig, dotted_key: str) -> int | float:
    value: object = config
    for part in dotted_key.split("."):
        value = getattr(value, part)
    return value  # type: ignore[return-value]


def _find_value_index(parameter: FitParameter, current: int | float) -> int:
    values = list(parameter.values)
    if current in values:
        return values.index(current)
    return min(range(len(values)), key=lambda idx: abs(float(values[idx]) - float(current)))


def _neighbor_values(parameter: FitParameter, current: int | float) -> tuple[int | float, ...]:
    values = list(parameter.values)
    index = _find_value_index(parameter, current)
    neighbor_indexes = {index}
    if index > 0:
        neighbor_indexes.add(index - 1)
    if index < len(values) - 1:
        neighbor_indexes.add(index + 1)
    return tuple(values[idx] for idx in sorted(neighbor_indexes))


def _parameter_sensitivity_threshold(bundle: ReferenceBundle) -> float:
    return round(max(0.0, bundle.confidence_policy.max_sensitivity_delta), 6)


def _derive_confidence_score(
    bundle: ReferenceBundle,
    fit_summary: dict[str, Any],
    coverage_summary: dict[str, Any],
    sensitivity_summary: dict[str, Any],
) -> tuple[float, dict[str, Any], str, list[str]]:
    policy = bundle.confidence_policy
    flags = list(coverage_summary.get("bundle_strength_flags", []))
    if sensitivity_summary["brittle"]:
        flags.append("sensitive_local_minimum")
    fit_margin_component = max(
        0.0,
        min(1.0, 1.0 - (float(fit_summary["total_score"]) / max(bundle.gate.max_total_score, 0.000001))),
    )
    coverage_component = max(
        0.0,
        min(1.0, float(coverage_summary.get("coverage_completeness", 0.0)) / max(policy.min_coverage_completeness, 0.000001)),
    )
    if float(sensitivity_summary.get("max_neighbor_score_delta", 0.0)) <= policy.max_sensitivity_delta:
        sensitivity_component = 1.0
    else:
        sensitivity_component = max(
            0.0,
            1.0 - (
                (float(sensitivity_summary.get("max_neighbor_score_delta", 0.0)) - policy.max_sensitivity_delta)
                / max(policy.max_sensitivity_delta, 0.000001)
            ),
        )
    if int(fit_summary["failed_objective_count"]) <= policy.max_failed_objectives:
        failure_component = 1.0
    else:
        failure_component = max(
            0.0,
            1.0 - (
                (int(fit_summary["failed_objective_count"]) - policy.max_failed_objectives)
                / max(1.0, float(policy.max_failed_objectives + 1))
            ),
        )
    confidence_components = {
        "fit_margin": round(fit_margin_component, 6),
        "coverage_completeness": round(coverage_component, 6),
        "sensitivity_stability": round(sensitivity_component, 6),
        "objective_failures": round(failure_component, 6),
    }
    confidence_score = round(sum(confidence_components.values()) / len(confidence_components), 6)
    if "evidence_insufficient" in fit_summary.get("failure_reasons", ()) or not coverage_summary.get("sufficient", False):
        tier = "low"
    elif confidence_score >= policy.high_confidence_score:
        tier = "high"
    elif confidence_score >= policy.medium_confidence_score:
        tier = "medium"
    else:
        tier = "low"
    return confidence_score, confidence_components, tier, flags


def _evaluate_candidate(
    bundle: ReferenceBundle,
    base_config: ScenarioConfig,
    *,
    output_root: Path,
    overrides: dict[str, int | float],
    candidate_id: str,
    evaluated_candidates: list[dict[str, Any]],
    evaluated_keys: dict[tuple[tuple[str, int | float], ...], dict[str, Any]],
    coverage_summary: dict[str, Any],
    coverage_ok: bool,
) -> dict[str, Any]:
    from .engine import run_scenario

    signature = tuple(sorted(overrides.items()))
    existing = evaluated_keys.get(signature)
    if existing is not None:
        return existing
    candidate_config = _apply_overrides(base_config, overrides)
    result = run_scenario(candidate_config, output_root=output_root / candidate_id)
    backtest_trace = json.loads(result.backtest_trace_path.read_text(encoding="utf-8"))
    metric_objectives, metric_ok = score_metric_targets(bundle, result.summary)
    event_objectives, event_ok = score_event_expectations(bundle, backtest_trace)
    series_objectives, series_ok = score_series_targets(bundle, backtest_trace)
    objectives = metric_objectives + event_objectives + series_objectives
    fit_summary = summarize_fit(
        bundle,
        objectives,
        bundle.gate,
        metric_ok=metric_ok,
        event_ok=event_ok,
        series_ok=series_ok,
        coverage_ok=coverage_ok,
        coverage_summary=coverage_summary,
    )
    candidate = {
        "candidate_id": candidate_id,
        "overrides": dict(sorted(overrides.items())),
        "fit_summary": fit_summary,
        "objectives": objectives,
        "run_summary_path": str(result.run_summary_path),
        "backtest_trace_path": str(result.backtest_trace_path),
    }
    evaluated_candidates.append(candidate)
    evaluated_keys[signature] = candidate
    return candidate


def _ranked_best(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(candidates, key=candidate_rank_key)[0]


def _score_delta(previous: dict[str, Any], current: dict[str, Any]) -> float:
    return round(float(previous["fit_summary"]["total_score"]) - float(current["fit_summary"]["total_score"]), 6)


def _measure_sensitivity(
    bundle: ReferenceBundle,
    *,
    selected_candidate: dict[str, Any],
    base_config: ScenarioConfig,
    output_root: Path,
    evaluated_candidates: list[dict[str, Any]],
    evaluated_keys: dict[tuple[tuple[str, int | float], ...], dict[str, Any]],
    coverage_summary: dict[str, Any],
    coverage_ok: bool,
    start_index: int,
) -> tuple[dict[str, Any], int]:
    baseline_score = float(selected_candidate["fit_summary"]["total_score"])
    threshold = _parameter_sensitivity_threshold(bundle)
    score_deltas = {}
    sensitive_parameters = []
    candidate_index = start_index
    for parameter in bundle.fit_parameters:
        current_value = selected_candidate["overrides"][parameter.scenario_key]
        deltas = []
        for value in _neighbor_values(parameter, current_value):
            if value == current_value:
                continue
            candidate = _evaluate_candidate(
                bundle,
                base_config,
                output_root=output_root,
                overrides={**selected_candidate["overrides"], parameter.scenario_key: value},
                candidate_id=f"candidate_{candidate_index:03d}",
                evaluated_candidates=evaluated_candidates,
                evaluated_keys=evaluated_keys,
                coverage_summary=coverage_summary,
                coverage_ok=coverage_ok,
            )
            candidate_index += 1
            deltas.append(
                {
                    "value": value,
                    "score_delta": round(float(candidate["fit_summary"]["total_score"]) - baseline_score, 6),
                    "promotable": candidate["fit_summary"]["promotable"],
                }
            )
        if deltas:
            score_deltas[parameter.scenario_key] = deltas
            if any(item["score_delta"] > threshold or not item["promotable"] for item in deltas):
                sensitive_parameters.append(parameter.scenario_key)
    max_delta = max(
        (item["score_delta"] for deltas in score_deltas.values() for item in deltas),
        default=0.0,
    )
    return (
        {
            "baseline_score": round(baseline_score, 6),
            "neighbor_count": sum(len(deltas) for deltas in score_deltas.values()),
            "max_neighbor_score_delta": round(max_delta, 6),
            "sensitivity_threshold": threshold,
            "brittle": bool(sensitive_parameters),
            "sensitive_parameters": sensitive_parameters,
            "score_deltas_by_parameter": score_deltas,
        },
        candidate_index,
    )


def build_calibration_report(
    bundle: ReferenceBundle,
    *,
    report_id: str,
    scenario_name: str,
    selected_candidate: dict[str, Any],
    evaluated_candidates: list[dict[str, Any]],
    coverage_summary: dict[str, Any],
    search_metadata: dict[str, Any],
    sensitivity_summary: dict[str, Any],
    bundle_validation_report: dict[str, Any],
) -> dict[str, Any]:
    fit_summary = selected_candidate["fit_summary"]
    confidence_score, confidence_components, confidence_tier, bundle_strength_flags = _derive_confidence_score(
        bundle,
        fit_summary,
        coverage_summary,
        sensitivity_summary,
    )
    return {
        "id": f"{report_id}:calibration_report",
        "artifact_type": "calibration_report",
        "contract_version": V2_CONTRACT_VERSION,
        "reference_bundle_contract_version": bundle.contract_version,
        "generated_at": utc_timestamp(),
        "bundle_id": bundle.bundle_id,
        "bundle_version": bundle.version,
        "scenario_name": scenario_name,
        "selected_parameters": selected_candidate["overrides"],
        "fit_quality_summary": fit_summary,
        "objectives": selected_candidate["objectives"],
        "candidate_count": len(evaluated_candidates),
        "candidate_budget": search_metadata["candidate_budget"],
        "search_strategy": search_metadata["search_strategy"],
        "search_passes": search_metadata["search_passes"],
        "converged": search_metadata["converged"],
        "improvement_history": search_metadata["improvement_history"],
        "top_candidates": evaluated_candidates[:5],
        "promotable": fit_summary["promotable"],
        "failure_reasons": fit_summary["failure_reasons"],
        "evidence_coverage_summary": coverage_summary,
        "confidence_score": confidence_score,
        "confidence_components": confidence_components,
        "confidence_policy": {
            "high_confidence_score": bundle.confidence_policy.high_confidence_score,
            "medium_confidence_score": bundle.confidence_policy.medium_confidence_score,
            "max_sensitivity_delta": bundle.confidence_policy.max_sensitivity_delta,
            "max_failed_objectives": bundle.confidence_policy.max_failed_objectives,
            "min_coverage_completeness": bundle.confidence_policy.min_coverage_completeness,
        },
        "confidence_tier": confidence_tier,
        "sensitivity_summary": sensitivity_summary,
        "bundle_strength_flags": bundle_strength_flags,
        "bundle_validation_id": bundle_validation_report["id"],
        "provenance": bundle.provenance,
    }


def fit_bundle_to_config(
    bundle: ReferenceBundle,
    config: ScenarioConfig,
    *,
    output_root: Path,
    report_id: str | None = None,
    bundle_validation_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_config = _disable_calibration(config)
    coverage_summary, coverage_ok = inspect_bundle_coverage(bundle)
    evaluated_candidates: list[dict[str, Any]] = []
    evaluated_keys: dict[tuple[tuple[str, int | float], ...], dict[str, Any]] = {}

    current_overrides = {
        parameter.scenario_key: _get_nested_value(base_config, parameter.scenario_key)
        for parameter in bundle.fit_parameters
    }
    candidate_index = 0
    best_candidate = _evaluate_candidate(
        bundle,
        base_config,
        output_root=output_root,
        overrides=current_overrides,
        candidate_id=f"candidate_{candidate_index:03d}",
        evaluated_candidates=evaluated_candidates,
        evaluated_keys=evaluated_keys,
        coverage_summary=coverage_summary,
        coverage_ok=coverage_ok,
    )
    candidate_index += 1

    # Start with one deterministic coordinate sweep from scenario defaults.
    for parameter in bundle.fit_parameters:
        parameter_best = best_candidate
        parameter_best_overrides = dict(current_overrides)
        for value in parameter.values:
            candidate = _evaluate_candidate(
                bundle,
                base_config,
                output_root=output_root,
                overrides={**current_overrides, parameter.scenario_key: value},
                candidate_id=f"candidate_{candidate_index:03d}",
                evaluated_candidates=evaluated_candidates,
                evaluated_keys=evaluated_keys,
                coverage_summary=coverage_summary,
                coverage_ok=coverage_ok,
            )
            candidate_index += 1
            if candidate_rank_key(candidate) < candidate_rank_key(parameter_best):
                parameter_best = candidate
                parameter_best_overrides = dict(parameter_best["overrides"])
        current_overrides = parameter_best_overrides
        best_candidate = parameter_best

    improvement_history = [
        {
            "phase": "coordinate_sweep",
            "pass_index": 0,
            "best_score": best_candidate["fit_summary"]["total_score"],
            "improvement": None,
        }
    ]

    converged = False
    pairwise_passes = 0
    for pass_index in range(1, MAX_PAIRWISE_PASSES + 1):
        previous_best = best_candidate
        current_overrides = dict(best_candidate["overrides"])
        for first_index, first_parameter in enumerate(bundle.fit_parameters):
            for second_parameter in bundle.fit_parameters[first_index + 1 :]:
                first_values = _neighbor_values(first_parameter, current_overrides[first_parameter.scenario_key])
                second_values = _neighbor_values(second_parameter, current_overrides[second_parameter.scenario_key])
                local_best = best_candidate
                for first_value in first_values:
                    for second_value in second_values:
                        candidate_overrides = {
                            **current_overrides,
                            first_parameter.scenario_key: first_value,
                            second_parameter.scenario_key: second_value,
                        }
                        candidate = _evaluate_candidate(
                            bundle,
                            base_config,
                            output_root=output_root,
                            overrides=candidate_overrides,
                            candidate_id=f"candidate_{candidate_index:03d}",
                            evaluated_candidates=evaluated_candidates,
                            evaluated_keys=evaluated_keys,
                            coverage_summary=coverage_summary,
                            coverage_ok=coverage_ok,
                        )
                        candidate_index += 1
                        if candidate_rank_key(candidate) < candidate_rank_key(local_best):
                            local_best = candidate
                if candidate_rank_key(local_best) < candidate_rank_key(best_candidate):
                    best_candidate = local_best
                    current_overrides = dict(best_candidate["overrides"])
        pairwise_passes += 1
        improvement = _score_delta(previous_best, best_candidate)
        improvement_history.append(
            {
                "phase": "pairwise_refinement",
                "pass_index": pass_index,
                "best_score": best_candidate["fit_summary"]["total_score"],
                "improvement": improvement,
            }
        )
        if improvement <= SEARCH_EPSILON:
            converged = True
            break

    sensitivity_summary, candidate_index = _measure_sensitivity(
        bundle,
        selected_candidate=best_candidate,
        base_config=base_config,
        output_root=output_root,
        evaluated_candidates=evaluated_candidates,
        evaluated_keys=evaluated_keys,
        coverage_summary=coverage_summary,
        coverage_ok=coverage_ok,
        start_index=candidate_index,
    )
    evaluated_candidates.sort(key=candidate_rank_key)
    return build_calibration_report(
        bundle,
        report_id=report_id or f"{config.outputs.artifact_prefix}_{config.scenario_name}_calibration",
        scenario_name=config.scenario_name,
        selected_candidate=evaluated_candidates[0],
        evaluated_candidates=evaluated_candidates,
        coverage_summary=coverage_summary,
        search_metadata={
            "search_strategy": "deterministic_coordinate_plus_pairwise_refinement",
            "search_passes": {
                "coordinate_sweep": 1,
                "pairwise_refinement": pairwise_passes,
            },
            "converged": converged,
            "improvement_history": improvement_history,
            "candidate_budget": {
                "evaluated": len(evaluated_candidates),
                "max_pairwise_passes": MAX_PAIRWISE_PASSES,
            },
        },
        sensitivity_summary=sensitivity_summary,
        bundle_validation_report=bundle_validation_report
        or build_bundle_validation_report(bundle, report_id=report_id or bundle.bundle_id),
    )


def calibrate_bundle(bundle_path: str) -> dict[str, Any]:
    bundle = load_reference_bundle(bundle_path)
    bundle_validation_report = ensure_bundle_validation_passes(bundle, report_id=bundle.bundle_id)
    scenario = load_scenario_config(bundle.scenario_path)
    calibration_report = fit_bundle_to_config(
        bundle,
        scenario,
        output_root=scenario.outputs.root / "calibration_runs",
        bundle_validation_report=bundle_validation_report,
    )
    return {
        "bundle_validation": bundle_validation_report,
        "calibration_report": calibration_report,
    }
