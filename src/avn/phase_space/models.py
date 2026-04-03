from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum
from hashlib import sha256
import json
import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from avn.sweep_analysis import TrancheSliceResult


PHASE_METRIC_KEYS = (
    "time_to_first_failure",
    "safe_region_exit_time",
    "degraded_mode_dwell_time",
    "peak_corridor_load_ratio",
    "peak_node_utilization_ratio",
    "peak_queue_ratio",
    "stale_state_exposure_minutes",
    "trusted_active_fraction",
    "unsafe_admission_count",
    "no_admissible_landing_events",
    "contingency_saturation_duration",
    "reachable_landing_option_mean",
    "rho_c",
    "lambda_c",
    "gamma_c",
    "w_c",
    "rho_proxy",
    "lambda_proxy",
    "gamma_proxy",
    "chi_proxy",
)


class ThresholdEvidenceStatus(StrEnum):
    PROXY = "PROXY"
    BOUNDED_ESTIMATE = "BOUNDED_ESTIMATE"
    CROSS_RUN_STABLE = "CROSS_RUN_STABLE"
    VALIDATED = "VALIDATED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class AdmissibilityState(StrEnum):
    ADMISSIBLE_CANDIDATE = "ADMISSIBLE_CANDIDATE"
    INADMISSIBLE_CANDIDATE = "INADMISSIBLE_CANDIDATE"
    UNRESOLVED = "UNRESOLVED"


def _coerce_numeric_param(value: object) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    return None


def _coerce_numeric_metric(value: object, *, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    return default


def _extract_parameter_space(resolved_params: dict[str, object]) -> tuple[dict[str, float], dict[str, object]]:
    parameters: dict[str, float] = {}
    context: dict[str, object] = {}
    for key, value in sorted(resolved_params.items()):
        numeric_value = _coerce_numeric_param(value)
        if numeric_value is None:
            context[key] = value
            continue
        parameters[key] = numeric_value
    return parameters, context


def _stable_hash(payload: object) -> str:
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _compute_gamma_proxy(result: TrancheSliceResult) -> float:
    physics_summary = result.physics_summary
    if isinstance(physics_summary.get("gamma_e_min"), (int, float)):
        return float(physics_summary["gamma_e_min"])
    resolved = result.resolved_params
    base_reliability = _coerce_numeric_metric(resolved.get("disturbance.comms_reliability"), default=1.0)
    reliability_multiplier = _coerce_numeric_metric(
        resolved.get("modifiers.comms_reliability_multiplier"),
        default=1.0,
    )
    drop_probability = _coerce_numeric_metric(resolved.get("disturbance.message_drop_probability"), default=0.0)
    latency_minutes = _coerce_numeric_metric(resolved.get("disturbance.comms_latency_minutes"), default=0.0)
    stale_after_minutes = _coerce_numeric_metric(resolved.get("disturbance.stale_after_minutes"), default=12.0)

    freshness_factor = 1.0 / (1.0 + (latency_minutes / max(stale_after_minutes, 1.0)))
    return max(
        0.0,
        min(
            1.0,
            base_reliability * reliability_multiplier * (1.0 - min(max(drop_probability, 0.0), 0.99)) * freshness_factor,
        ),
    )


def _compute_chi_proxy(result: TrancheSliceResult) -> float:
    physics_summary = result.physics_summary
    if isinstance(physics_summary.get("chi_e_peak"), (int, float)):
        return float(physics_summary["chi_e_peak"])
    compromised_ratio = _coerce_numeric_metric(
        result.resolved_params.get("trust.compromised_participant_ratio"),
        default=0.0,
    )
    trust_degradation = max(0.0, 1.0 - result.trust_metrics_snapshot.trusted_active_fraction)
    unsafe_pressure = min(1.0, result.trust_metrics_snapshot.unsafe_admission_count / 10.0)
    return max(compromised_ratio, trust_degradation, unsafe_pressure)


def slice_replay_hash(result: TrancheSliceResult) -> str:
    return _stable_hash(
        {
            "slice_id": result.slice_id,
            "tranche_name": result.tranche_name,
            "seed": result.seed,
            "resolved_params": result.resolved_params,
            "first_dominant_failure_mechanism": result.first_dominant_failure_mechanism,
            "time_to_first_failure": result.time_to_first_failure,
            "safe_region_exit_time": result.safe_region_exit_time,
            "safe_region_exit_cause": result.safe_region_exit_cause,
            "degraded_mode_dwell_time": result.degraded_mode_dwell_time,
            "trust_metrics_snapshot": result.trust_metrics_snapshot.to_dict(),
            "comms_metrics_snapshot": result.comms_metrics_snapshot.to_dict(),
            "contingency_metrics_snapshot": result.contingency_metrics_snapshot.to_dict(),
            "throughput_metrics_snapshot": result.throughput_metrics_snapshot.to_dict(),
        }
    )


def admissibility_state_from_result(
    result: TrancheSliceResult,
) -> tuple[AdmissibilityState, tuple[str, ...]]:
    reasons: list[str] = []
    if result.safe_region_exit_time is None and not result.safe_region_exit_cause:
        if (
            result.trust_metrics_snapshot.unsafe_admission_count == 0
            and result.contingency_metrics_snapshot.no_admissible_landing_events == 0
        ):
            return (
                AdmissibilityState.ADMISSIBLE_CANDIDATE,
                ("no_recorded_safe_region_exit", "no_recorded_admission_or_landing_violation"),
            )

    if result.safe_region_exit_time is not None:
        reasons.append("safe_region_exit_recorded")
    if result.safe_region_exit_cause:
        reasons.append(f"exit_cause:{result.safe_region_exit_cause}")
    if result.trust_metrics_snapshot.unsafe_admission_count > 0:
        reasons.append("unsafe_admission_recorded")
    if result.contingency_metrics_snapshot.no_admissible_landing_events > 0:
        reasons.append("no_admissible_landing_recorded")
    if result.contingency_metrics_snapshot.contingency_saturation_duration > 0.0:
        reasons.append("contingency_saturation_recorded")
    if (
        result.comms_metrics_snapshot.stale_state_exposure_minutes > 0.0
        and result.first_dominant_failure_mechanism == "stale_information_instability"
    ):
        reasons.append("stale_state_instability_recorded")

    if reasons:
        return AdmissibilityState.INADMISSIBLE_CANDIDATE, tuple(reasons)
    return AdmissibilityState.UNRESOLVED, ("insufficient_safety_signal",)


@dataclass(slots=True)
class PhasePoint:
    parameters: dict[str, float]
    mechanism: str
    metrics: dict[str, Any]
    slice_id: str
    replay_hash: str
    admissibility_state: AdmissibilityState
    admissibility_reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "parameters": dict(self.parameters),
            "mechanism": self.mechanism,
            "metrics": dict(self.metrics),
            "slice_id": self.slice_id,
            "replay_hash": self.replay_hash,
            "admissibility_state": self.admissibility_state.value,
            "admissibility_reasons": list(self.admissibility_reasons),
        }


@dataclass(slots=True)
class PhaseRegion:
    bounds: dict[str, tuple[float, float]]
    dominant_mechanism: str
    entropy: float
    sample_density: float
    transition_axis: str | None = None
    fixed_context: dict[str, object] = field(default_factory=dict)
    mechanism_counts: dict[str, int] = field(default_factory=dict)
    local_disagreement: float = 0.0
    local_gradient: float = 0.0
    representative_slice_ids: tuple[str, ...] = ()
    estimated_threshold: float | None = None
    axis_total_span: float | None = None
    bracket_width: float = 0.0
    normalized_bracket_width: float = 0.0
    support_count: int = 0
    left_support_count: int = 0
    right_support_count: int = 0
    refined_depth: int = 0
    neighbor_agreement: float = 0.0
    phase_consistency: float = 0.0
    replay_hash: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "bounds": {axis: [lower, upper] for axis, (lower, upper) in self.bounds.items()},
            "dominant_mechanism": self.dominant_mechanism,
            "entropy": self.entropy,
            "sample_density": self.sample_density,
            "bracket_width": self.bracket_width,
            "normalized_bracket_width": self.normalized_bracket_width,
            "support_count": self.support_count,
            "left_support_count": self.left_support_count,
            "right_support_count": self.right_support_count,
            "refined_depth": self.refined_depth,
            "neighbor_agreement": self.neighbor_agreement,
            "phase_consistency": self.phase_consistency,
        }
        if self.transition_axis is not None:
            payload["transition_axis"] = self.transition_axis
        if self.fixed_context:
            payload["fixed_context"] = dict(self.fixed_context)
        if self.mechanism_counts:
            payload["mechanism_counts"] = dict(self.mechanism_counts)
        if self.local_disagreement:
            payload["local_disagreement"] = self.local_disagreement
        if self.local_gradient:
            payload["local_gradient"] = self.local_gradient
        if self.representative_slice_ids:
            payload["representative_slice_ids"] = list(self.representative_slice_ids)
        if self.estimated_threshold is not None:
            payload["estimated_threshold"] = self.estimated_threshold
        if self.axis_total_span is not None:
            payload["axis_total_span"] = self.axis_total_span
        if self.replay_hash is not None:
            payload["replay_hash"] = self.replay_hash
        return payload


def phase_point_from_slice_result(result: TrancheSliceResult) -> PhasePoint:
    parameters, context = _extract_parameter_space(result.resolved_params)
    admissibility_state, admissibility_reasons = admissibility_state_from_result(result)
    replay_hash = slice_replay_hash(result)
    phase_detection = result.phase_detection if isinstance(result.phase_detection, dict) else {}
    physics_summary = result.physics_summary if isinstance(result.physics_summary, dict) else {}

    def _phase_value(name: str, key: str) -> float | None:
        record = phase_detection.get(name, {})
        if not isinstance(record, dict):
            return None
        details = record.get("details", {})
        if isinstance(details, dict) and isinstance(details.get(key), (int, float)):
            return float(details[key])
        if isinstance(record.get("threshold_value"), (int, float)):
            return float(record["threshold_value"])
        return None

    metrics: dict[str, Any] = {
        "time_to_first_failure": result.time_to_first_failure,
        "safe_region_exit_time": result.safe_region_exit_time,
        "degraded_mode_dwell_time": result.degraded_mode_dwell_time,
        "peak_corridor_load_ratio": result.throughput_metrics_snapshot.peak_corridor_load_ratio,
        "peak_node_utilization_ratio": result.throughput_metrics_snapshot.peak_node_utilization_ratio,
        "peak_queue_ratio": result.throughput_metrics_snapshot.peak_queue_ratio,
        "stale_state_exposure_minutes": result.comms_metrics_snapshot.stale_state_exposure_minutes,
        "trusted_active_fraction": result.trust_metrics_snapshot.trusted_active_fraction,
        "unsafe_admission_count": result.trust_metrics_snapshot.unsafe_admission_count,
        "no_admissible_landing_events": result.contingency_metrics_snapshot.no_admissible_landing_events,
        "contingency_saturation_duration": result.contingency_metrics_snapshot.contingency_saturation_duration,
        "reachable_landing_option_mean": result.contingency_metrics_snapshot.reachable_landing_option_mean,
        "rho_c": _phase_value("flow_breakdown", "rho_c"),
        "lambda_c": _phase_value("queue_divergence", "lambda_c"),
        "gamma_c": _phase_value("comms_failure", "gamma_c"),
        "w_c": _phase_value("weather_collapse", "w_c"),
        "flow_breakdown_detected": bool(phase_detection.get("flow_breakdown", {}).get("detected", False)),
        "flow_breakdown_confidence": _coerce_numeric_metric(
            phase_detection.get("flow_breakdown", {}).get("confidence"),
            default=0.0,
        ),
        "queue_divergence_detected": bool(phase_detection.get("queue_divergence", {}).get("detected", False)),
        "queue_divergence_confidence": _coerce_numeric_metric(
            phase_detection.get("queue_divergence", {}).get("confidence"),
            default=0.0,
        ),
        "comms_failure_detected": bool(phase_detection.get("comms_failure", {}).get("detected", False)),
        "comms_failure_confidence": _coerce_numeric_metric(
            phase_detection.get("comms_failure", {}).get("confidence"),
            default=0.0,
        ),
        "weather_collapse_detected": bool(phase_detection.get("weather_collapse", {}).get("detected", False)),
        "weather_collapse_confidence": _coerce_numeric_metric(
            phase_detection.get("weather_collapse", {}).get("confidence"),
            default=0.0,
        ),
        "rho_proxy": _coerce_numeric_metric(
            result.resolved_params.get("modifiers.demand_multiplier"),
            default=_coerce_numeric_metric(physics_summary.get("rho_e_peak"), default=result.throughput_metrics_snapshot.peak_corridor_load_ratio),
        ),
        "lambda_proxy": _coerce_numeric_metric(
            physics_summary.get("lambda_e_peak"),
            default=max(
                result.throughput_metrics_snapshot.peak_node_utilization_ratio,
                result.throughput_metrics_snapshot.peak_queue_ratio,
            ),
        ),
        "gamma_proxy": _compute_gamma_proxy(result),
        "chi_proxy": _compute_chi_proxy(result),
        "seed": result.seed,
        "tranche_name": result.tranche_name,
        "safe_region_exit_cause": result.safe_region_exit_cause,
        "replay_hash": replay_hash,
        "admissibility_state": admissibility_state.value,
        "admissibility_reasons": list(admissibility_reasons),
        "confidence_score": result.confidence_score,
        "seed_count": result.seed_count,
    }
    if context:
        metrics["context"] = context
    return PhasePoint(
        parameters=parameters,
        mechanism=result.dominant_failure_mode or result.first_dominant_failure_mechanism,
        metrics=metrics,
        slice_id=result.slice_id,
        replay_hash=replay_hash,
        admissibility_state=admissibility_state,
        admissibility_reasons=admissibility_reasons,
    )


def phase_points_from_slice_results(results: list[TrancheSliceResult]) -> list[PhasePoint]:
    return [phase_point_from_slice_result(result) for result in results]


def phase_map_payload(tranche_name: str, points: list[PhasePoint]) -> dict[str, object]:
    mechanism_counts = Counter(point.mechanism for point in points)
    axes = sorted(
        axis
        for axis in {axis for point in points for axis in point.parameters}
        if len({point.parameters[axis] for point in points if axis in point.parameters}) > 1
    )
    bounds = {
        axis: {
            "min": min(point.parameters[axis] for point in points if axis in point.parameters),
            "max": max(point.parameters[axis] for point in points if axis in point.parameters),
        }
        for axis in axes
    }
    return {
        "tranche_name": tranche_name,
        "point_count": len(points),
        "axes": axes,
        "mechanism_counts": dict(sorted(mechanism_counts.items())),
        "mechanism_proportions": {
            mechanism: count / len(points)
            for mechanism, count in sorted(mechanism_counts.items())
        }
        if points
        else {},
        "bounds": bounds,
        "points": [point.to_dict() for point in points],
    }


def phase_context(point: PhasePoint) -> dict[str, object]:
    context = point.metrics.get("context", {})
    if isinstance(context, dict):
        return dict(context)
    return {}
