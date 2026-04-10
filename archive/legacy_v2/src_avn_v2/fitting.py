from __future__ import annotations

from collections import defaultdict
from itertools import product
from typing import Any

from .models import CalibrationGate, FitParameter, ReferenceBundle, ReferenceSeriesTarget


def parameter_grid(parameters: tuple[FitParameter, ...]) -> list[dict[str, int | float]]:
    if not parameters:
        return [{}]
    combinations = []
    for values in product(*(parameter.values for parameter in parameters)):
        combinations.append(
            {
                parameter.scenario_key: value
                for parameter, value in zip(parameters, values, strict=True)
            }
        )
    return combinations


def _normalized_residual(delta: float, tolerance: float) -> float:
    if tolerance <= 0.0:
        return abs(delta)
    return abs(delta) / tolerance


def inspect_bundle_coverage(bundle: ReferenceBundle) -> tuple[dict[str, Any], bool]:
    scope_counts = defaultdict(int)
    for target in bundle.series_targets:
        scope_counts[target.scope] += 1
    coverage = {
        "required": {
            "metric_targets": bundle.coverage_requirements.min_metric_targets,
            "event_expectations": bundle.coverage_requirements.min_event_expectations,
            "series_targets": bundle.coverage_requirements.min_series_targets,
            "required_scopes": list(bundle.coverage_requirements.required_scopes),
        },
        "observed": {
            "metric_targets": len(bundle.metric_targets),
            "event_expectations": len(bundle.event_expectations),
            "series_targets": len(bundle.series_targets),
            "scopes": dict(sorted(scope_counts.items())),
        },
    }
    flags: list[str] = []
    if len(bundle.metric_targets) < bundle.coverage_requirements.min_metric_targets:
        flags.append("insufficient_metric_targets")
    if len(bundle.event_expectations) < bundle.coverage_requirements.min_event_expectations:
        flags.append("insufficient_event_expectations")
    if len(bundle.series_targets) < bundle.coverage_requirements.min_series_targets:
        flags.append("insufficient_series_targets")
    for scope in bundle.coverage_requirements.required_scopes:
        if scope_counts.get(scope, 0) <= 0:
            flags.append(f"missing_required_scope:{scope}")
    total_requirements = 3 + len(bundle.coverage_requirements.required_scopes)
    satisfied = 0
    if len(bundle.metric_targets) >= bundle.coverage_requirements.min_metric_targets:
        satisfied += 1
    if len(bundle.event_expectations) >= bundle.coverage_requirements.min_event_expectations:
        satisfied += 1
    if len(bundle.series_targets) >= bundle.coverage_requirements.min_series_targets:
        satisfied += 1
    for scope in bundle.coverage_requirements.required_scopes:
        if scope_counts.get(scope, 0) > 0:
            satisfied += 1
    coverage["coverage_completeness"] = round(satisfied / max(total_requirements, 1), 6)
    coverage["bundle_strength_flags"] = flags
    coverage["sufficient"] = not flags
    return coverage, not flags


def score_metric_targets(bundle: ReferenceBundle, summary: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    results = []
    all_within = True
    for target in bundle.metric_targets:
        simulated_value = float(summary.get(target.metric_key, 0.0))
        delta = simulated_value - target.reference_value
        within = abs(delta) <= target.tolerance
        all_within = all_within and within
        results.append(
            {
                "objective_id": f"metric:{target.metric_key}",
                "objective_type": "metric",
                "objective_group": target.objective_group,
                "metric_key": target.metric_key,
                "reference_value": target.reference_value,
                "simulated_value": simulated_value,
                "delta": round(delta, 6),
                "tolerance": target.tolerance,
                "weight": target.weight,
                "weighted_residual": round(float(target.weight) * _normalized_residual(delta, target.tolerance), 6),
                "normalized_residual": round(_normalized_residual(delta, target.tolerance), 6),
                "within_tolerance": within,
            }
        )
    return results, all_within


def score_event_expectations(bundle: ReferenceBundle, backtest_trace: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    summary_lookup = {
        item["event_type"]: item
        for item in backtest_trace.get("event_summary", [])
    }
    results = []
    all_within = True
    for expectation in bundle.event_expectations:
        observed = summary_lookup.get(expectation.event_type, {"count": 0, "first_minute": None})
        observed_count = int(observed.get("count", 0))
        count_delta = observed_count - expectation.expected_count
        count_within = abs(count_delta) <= expectation.count_tolerance
        timing_within = True
        timing_delta = None
        if expectation.first_minute is not None:
            observed_first = observed.get("first_minute")
            if observed_first is None:
                timing_within = False
            else:
                timing_delta = float(observed_first) - expectation.first_minute
                timing_within = abs(timing_delta) <= float(expectation.timing_tolerance or 0.0)
        within = count_within and timing_within
        all_within = all_within and within
        normalized_count = _normalized_residual(float(count_delta), float(max(1, expectation.count_tolerance)))
        normalized_timing = 0.0
        if expectation.first_minute is not None:
            normalized_timing = 1.0 if timing_delta is None else _normalized_residual(
                timing_delta,
                float(expectation.timing_tolerance or 1.0),
            )
        normalized = max(normalized_count, normalized_timing)
        results.append(
            {
                "objective_id": expectation.expectation_id,
                "objective_type": "event_expectation",
                "objective_group": expectation.objective_group,
                "event_type": expectation.event_type,
                "expected_count": expectation.expected_count,
                "observed_count": observed_count,
                "count_tolerance": expectation.count_tolerance,
                "first_minute": expectation.first_minute,
                "observed_first_minute": observed.get("first_minute"),
                "timing_tolerance": expectation.timing_tolerance,
                "weight": expectation.weight,
                "weighted_residual": round(float(expectation.weight) * normalized, 6),
                "normalized_residual": round(normalized, 6),
                "within_tolerance": within,
            }
        )
    return results, all_within


def _matching_series(trace: list[dict[str, Any]], target: ReferenceSeriesTarget) -> list[dict[str, Any]]:
    matches = [
        entry
        for entry in trace
        if entry["scope"] == target.scope
        and entry["entity_id"] == target.entity_id
        and entry["metric_key"] == target.metric_key
    ]
    if target.minute is not None:
        return [entry for entry in matches if int(entry["minute"]) == target.minute]
    if target.minute_start is None or target.minute_end is None:
        return matches
    return [
        entry
        for entry in matches
        if target.minute_start <= int(entry["minute"]) <= target.minute_end
    ]


def _aggregate_series(matches: list[dict[str, Any]], aggregation: str) -> float | None:
    if not matches:
        return None
    ordered = sorted(matches, key=lambda item: int(item["minute"]))
    values = [float(item["value"]) for item in ordered]
    if aggregation == "avg":
        return round(sum(values) / len(values), 6)
    if aggregation == "max":
        return round(max(values), 6)
    if aggregation == "min":
        return round(min(values), 6)
    if aggregation == "last":
        return round(values[-1], 6)
    return round(values[0], 6)


def score_series_targets(bundle: ReferenceBundle, backtest_trace: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    trace = backtest_trace.get("series_trace", [])
    results = []
    all_within = True
    for target in bundle.series_targets:
        matches = _matching_series(trace, target)
        simulated_value = _aggregate_series(matches, target.aggregation)
        if simulated_value is None:
            within = False
            normalized = 1.0
            delta = None
        else:
            delta = simulated_value - target.reference_value
            within = abs(delta) <= target.tolerance
            normalized = _normalized_residual(delta, target.tolerance)
        all_within = all_within and within
        results.append(
            {
                "objective_id": target.target_id,
                "objective_type": "series_target",
                "objective_group": target.objective_group,
                "scope": target.scope,
                "entity_id": target.entity_id,
                "metric_key": target.metric_key,
                "minute": target.minute,
                "minute_start": target.minute_start,
                "minute_end": target.minute_end,
                "aggregation": target.aggregation,
                "reference_value": target.reference_value,
                "simulated_value": simulated_value,
                "delta": None if delta is None else round(delta, 6),
                "tolerance": target.tolerance,
                "weight": target.weight,
                "weighted_residual": round(float(target.weight) * normalized, 6),
                "normalized_residual": round(normalized, 6),
                "within_tolerance": within,
            }
        )
    return results, all_within


def summarize_fit(
    bundle: ReferenceBundle,
    objectives: list[dict[str, Any]],
    gate: CalibrationGate,
    *,
    metric_ok: bool,
    event_ok: bool,
    series_ok: bool,
    coverage_ok: bool,
    coverage_summary: dict[str, Any],
) -> dict[str, Any]:
    objective_groups = defaultdict(list)
    for item in objectives:
        objective_groups[str(item["objective_group"])].append(item)
    configured_group_weights = bundle.objective_group_weights or {
        "metric": 1.0,
        "event": 1.0,
        "series": 1.0,
    }
    group_scores = {}
    weighted_total = 0.0
    total_group_weight = 0.0
    for group_name, group_items in sorted(objective_groups.items()):
        objective_weight = sum(float(item["weight"]) for item in group_items) or 1.0
        group_score = sum(float(item["weight"]) * float(item["normalized_residual"]) for item in group_items) / objective_weight
        group_weight = float(configured_group_weights.get(group_name, 1.0))
        group_scores[group_name] = {
            "objective_count": len(group_items),
            "group_weight": group_weight,
            "group_score": round(group_score, 6),
        }
        weighted_total += group_score * group_weight
        total_group_weight += group_weight
    total_score = weighted_total / max(total_group_weight, 1.0)
    failed_objectives = sum(1 for item in objectives if not item["within_tolerance"])
    worst_weighted_residual = max((float(item["weighted_residual"]) for item in objectives), default=0.0)
    promotable = total_score <= gate.max_total_score
    if gate.require_metric_match:
        promotable = promotable and metric_ok
    if gate.require_event_match:
        promotable = promotable and event_ok
    if gate.require_series_match:
        promotable = promotable and series_ok
    if gate.require_evidence_coverage:
        promotable = promotable and coverage_ok
    reasons = []
    if gate.require_metric_match and not metric_ok:
        reasons.append("metric_targets_out_of_tolerance")
    if gate.require_event_match and not event_ok:
        reasons.append("event_expectations_out_of_tolerance")
    if gate.require_series_match and not series_ok:
        reasons.append("series_targets_out_of_tolerance")
    if gate.require_evidence_coverage and not coverage_ok:
        reasons.append("evidence_insufficient")
    reasons.extend(flag for flag in coverage_summary.get("bundle_strength_flags", []) if flag not in reasons)
    if total_score > gate.max_total_score:
        reasons.append("fit_score_exceeds_gate")
    return {
        "total_score": round(total_score, 6),
        "metric_match": metric_ok,
        "event_match": event_ok,
        "series_match": series_ok,
        "evidence_coverage_ok": coverage_ok,
        "coverage_completeness": float(coverage_summary.get("coverage_completeness", 0.0)),
        "failed_objective_count": failed_objectives,
        "worst_weighted_residual": round(worst_weighted_residual, 6),
        "group_scores": group_scores,
        "promotable": promotable,
        "failure_reasons": reasons,
    }


def candidate_rank_key(candidate: dict[str, Any]) -> tuple[float, int, float, str]:
    fit_summary = candidate["fit_summary"]
    return (
        float(fit_summary["total_score"]),
        int(fit_summary["failed_objective_count"]),
        float(fit_summary["worst_weighted_residual"]),
        str(candidate["candidate_id"]),
    )
