from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import math
from statistics import fmean
import statistics
from typing import Any, Sequence

from avn.phase_space.models import (
    AdmissibilityState,
    PhasePoint,
    PhaseRegion,
    PromotionGovernanceOutcome,
    ThresholdEvidenceStatus,
    ThresholdEvidenceType,
)


@dataclass(frozen=True, slots=True)
class ThresholdSpec:
    name: str
    symbol: str
    proxy_key: str
    fallback_proxy_key: str | None
    target_mechanism: str
    preferred_axis: str | None
    direction: str
    event_key: str
    detection_method: str


THRESHOLD_SPECS = (
    ThresholdSpec(
        name="rho_c",
        symbol="rho",
        proxy_key="rho_c",
        fallback_proxy_key="rho_proxy",
        target_mechanism="CORRIDOR_CONGESTION",
        preferred_axis="modifiers.demand_multiplier",
        direction="increasing",
        event_key="flow_breakdown",
        detection_method="phase_detection.flow_breakdown",
    ),
    ThresholdSpec(
        name="lambda_c",
        symbol="lambda",
        proxy_key="lambda_c",
        fallback_proxy_key="lambda_proxy",
        target_mechanism="NODE_SATURATION",
        preferred_axis="modifiers.demand_multiplier",
        direction="increasing",
        event_key="queue_divergence",
        detection_method="phase_detection.queue_divergence",
    ),
    ThresholdSpec(
        name="gamma_c",
        symbol="gamma",
        proxy_key="gamma_c",
        fallback_proxy_key="gamma_proxy",
        target_mechanism="COMMS_FAILURE",
        preferred_axis="disturbance.comms_reliability",
        direction="decreasing",
        event_key="comms_failure",
        detection_method="phase_detection.comms_failure",
    ),
    ThresholdSpec(
        name="w_c",
        symbol="w",
        proxy_key="w_c",
        fallback_proxy_key=None,
        target_mechanism="WEATHER_COLLAPSE",
        preferred_axis="disturbance.weather_severity",
        direction="increasing",
        event_key="weather_collapse",
        detection_method="phase_detection.weather_collapse",
    ),
)


MIN_TOTAL_SUPPORT = 3
MIN_SIDE_SUPPORT = 1
MIN_NEIGHBOR_AGREEMENT = 0.60
MIN_PHASE_CONSISTENCY = 0.50
MIN_SIGNAL_MONOTONICITY = 0.75
MIN_SUPPORT_DENSITY = 0.50
VALIDATED_RELATIVE_SPREAD = 0.10
STABLE_RELATIVE_SPREAD = 0.35
MAX_LEAKAGE = 0.25
LEAKAGE_THRESHOLD = 0.35
ENTROPY_MIN = 0.90
MAX_LOCAL_VARIANCE = 200.0
MIN_GLOBAL_CONFIDENCE = 0.65
ADAPTIVE_SUPPORT_WEIGHT = 0.60
ADAPTIVE_UNCERTAINTY_WEIGHT = 0.40

NORMALIZATION_ORIGIN_FIXED_CONSTANT = "FIXED_CONSTANT"
NORMALIZATION_ORIGIN_TRANCHE_ENVELOPE = "TRANCHE_ADMISSIBILITY_ENVELOPE"
NORMALIZATION_ORIGIN_CROSS_TRANCHE_ENVELOPE = "CROSS_TRANCHE_ENVELOPE"
NORMALIZATION_ORIGIN_GOVERNED_FALLBACK = "GOVERNED_FALLBACK"
NORMALIZATION_ORIGIN_FALLBACK_SPARSE_SUPPORT = "FALLBACK_SPARSE_SUPPORT"
NORMALIZATION_ORIGIN_UNKNOWN = "UNKNOWN"

PROMOTION_PROMOTED = "PROMOTED"
PROMOTION_LOW_CONFIDENCE = "BLOCKED_BY_LOW_CONFIDENCE"
PROMOTION_EVENT_ORDERING = "BLOCKED_BY_EVENT_ORDERING"
PROMOTION_AXIS_LEAKAGE = "BLOCKED_BY_AXIS_LEAKAGE"
PROMOTION_MECHANISM_LEAKAGE = "BLOCKED_BY_MECHANISM_LEAKAGE"
PROMOTION_HIGH_VARIANCE = "BLOCKED_BY_HIGH_VARIANCE"
PROMOTION_NONMONOTONICITY = "BLOCKED_BY_NONMONOTONICITY"
PROMOTION_CONTRADICTION = "BLOCKED_BY_CONTRADICTION"
PROMOTION_NORMALIZATION_FALLBACK = "BLOCKED_BY_NORMALIZATION_FALLBACK_POLICY"

BLOCKER_INSUFFICIENT_ADMISSIBLE_SUPPORT = "INSUFFICIENT_ADMISSIBLE_SUPPORT"
BLOCKER_NUISANCE_DOMINANCE = "LEAKAGE_DOMINATED_BY_SINGLE_NUISANCE"

CONTRADICTION_LOCAL_INCONSISTENCY = "LOCAL_INCONSISTENCY"
CONTRADICTION_CROSS_TRANCHE_CONFLICT = "CROSS_TRANCHE_CONFLICT"
CONTRADICTION_ENVELOPE_VIOLATION = "ENVELOPE_VIOLATION"
CONTRADICTION_NON_MONOTONIC_THRESHOLD = "NON_MONOTONIC_THRESHOLD"
CONTRADICTION_NUISANCE_DOMINANCE = "NuisanceDominance"

CONTRADICTION_SEVERITY_BLOCKING = "BLOCKING"
CONTRADICTION_SEVERITY_WARNING = "WARNING"

EPISTEMIC_NOTE = (
    "Threshold statuses express computational support from recorded tranche slices and deterministic replay. "
    "They do not claim physical certainty outside the observed sweep envelope."
)
ADMISSIBILITY_NOTE = (
    "Admissibility overlays are derived from recorded tranche metrics only and separate safety-relevant candidate regions "
    "from phase-transition discovery without introducing a new solver."
)

EVENT_CHAIN_KEYS = (
    "admissibility_degradation_time",
    "phase_transition_time",
    "safe_region_exit_time",
    "collapse_time",
)

NUISANCE_AXIS_FACTORS = (
    ("congestion", "congestion_and_spillback_pressure"),
    ("trust", "trust_degradation"),
    ("comms", "communications_degradation"),
    ("navigation", "navigation_degradation"),
    ("weather", "weather_severity"),
    ("contingency", "contingency_saturation_pressure"),
)

LEGACY_NUISANCE_SOURCE_NAMES = {
    "congestion": "congestion_and_spillback_pressure",
    "trust": "trust_degradation",
    "comms": "communications_degradation",
    "navigation": "navigation_degradation",
    "weather": "weather_severity",
    "contingency": "contingency_saturation_pressure",
}


def _stable_hash(payload: object) -> str:
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _sort_proxy(point: PhasePoint, spec: ThresholdSpec) -> float:
    value = _metric_value(point, spec)
    if not isinstance(value, (int, float)):
        return float("-inf") if spec.direction == "increasing" else float("inf")
    return float(value)


def _metric_value(point: PhasePoint, spec: ThresholdSpec) -> float | None:
    value = point.metrics.get(spec.proxy_key)
    if isinstance(value, (int, float)):
        return float(value)
    if spec.fallback_proxy_key is None:
        return None
    fallback = point.metrics.get(spec.fallback_proxy_key)
    if isinstance(fallback, (int, float)):
        return float(fallback)
    return None


def _event_detected(point: PhasePoint, spec: ThresholdSpec) -> bool:
    detected_key = f"{spec.event_key}_detected"
    return bool(point.metrics.get(detected_key, False))


def _event_confidence(point: PhasePoint, spec: ThresholdSpec) -> float:
    confidence_key = f"{spec.event_key}_confidence"
    value = point.metrics.get(confidence_key)
    if not isinstance(value, (int, float)):
        return 0.0
    return float(value)


def _numeric_variance(values: Sequence[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return statistics.pvariance(values)


def validate_event_order_consistency(events_per_seed: Sequence[dict[str, Any]]) -> dict[str, object]:
    orderings: set[tuple[str, ...]] = set()
    invalid_reasons: list[str] = []

    for index, event_chain in enumerate(events_per_seed):
        numeric_events = [
            (key, float(event_chain[key]))
            for key in EVENT_CHAIN_KEYS
            if isinstance(event_chain.get(key), (int, float))
        ]
        if len(numeric_events) < 3:
            invalid_reasons.append(f"seed_{index}:insufficient_events")
            continue
        ordered_names = tuple(name for name, _value in sorted(numeric_events, key=lambda item: item[1]))
        orderings.add(ordered_names)
        ordered_values = [value for _name, value in sorted(numeric_events, key=lambda item: item[1])]
        if any(math.isclose(left, right) for left, right in zip(ordered_values, ordered_values[1:])):
            invalid_reasons.append(f"seed_{index}:collapsed_timesteps")
        if any(left > right for left, right in zip(ordered_values, ordered_values[1:])):
            invalid_reasons.append(f"seed_{index}:reversed_order")

    ordering_valid = bool(events_per_seed) and not invalid_reasons and len(orderings) == 1
    if len(orderings) > 1:
        invalid_reasons.append("cross_seed_ordering_mismatch")
    return {
        "ordering_valid": ordering_valid,
        "orderings": [list(ordering) for ordering in sorted(orderings)],
        "invalid_reasons": invalid_reasons,
        "seed_count": len(events_per_seed),
    }


def _event_chain_from_point(point: PhasePoint) -> dict[str, float | None]:
    safe_exit = (
        float(point.metrics["safe_region_exit_time"])
        if isinstance(point.metrics.get("safe_region_exit_time"), (int, float))
        else None
    )
    collapse = (
        float(point.metrics["collapse_time"])
        if isinstance(point.metrics.get("collapse_time"), (int, float))
        else float(point.metrics["time_to_first_failure"])
        if isinstance(point.metrics.get("time_to_first_failure"), (int, float))
        else safe_exit
    )
    inferred_phase_transition = (
        float(point.metrics["phase_transition_time"])
        if isinstance(point.metrics.get("phase_transition_time"), (int, float))
        else (safe_exit - 1.0 if safe_exit is not None else None)
    )
    inferred_admissibility = (
        float(point.metrics["admissibility_exit_time"])
        if isinstance(point.metrics.get("admissibility_exit_time"), (int, float))
        else (inferred_phase_transition - 1.0 if inferred_phase_transition is not None else None)
    )
    return {
        "admissibility_degradation_time": inferred_admissibility,
        "phase_transition_time": inferred_phase_transition,
        "safe_region_exit_time": safe_exit,
        "collapse_time": collapse,
    }


def _infer_axis_isolation(points: Sequence[PhasePoint], dominant_axis: str | None = None) -> dict[str, object]:
    axes = sorted({axis for point in points for axis in point.parameters})
    if dominant_axis is None:
        varying_axes = [
            axis
            for axis in axes
            if len({point.parameters[axis] for point in points if axis in point.parameters}) > 1
        ]
        dominant_axis = varying_axes[0] if varying_axes else (axes[0] if axes else None)
    target_values = [
        float(point.parameters[dominant_axis])
        for point in points
        if dominant_axis is not None and dominant_axis in point.parameters
    ]
    target_variance = _numeric_variance(target_values)
    leakage_numerator = sum(
        _numeric_variance([float(point.parameters[axis]) for point in points if axis in point.parameters])
        for axis in axes
        if axis != dominant_axis
    )
    if dominant_axis is None:
        leakage_score = math.inf if leakage_numerator > 0.0 else 0.0
    else:
        leakage_score = leakage_numerator / max(target_variance, 1e-9)
    axis_purity_score = 1.0 / (1.0 + max(leakage_score, 0.0)) if math.isfinite(leakage_score) else 0.0
    return {
        "dominant_axis": dominant_axis,
        "axis_purity_score": axis_purity_score,
        "leakage_score": leakage_score,
        "is_isolated": leakage_score <= MAX_LEAKAGE,
    }


def _signal_monotonicity(points: Sequence[PhasePoint], spec: ThresholdSpec) -> float:
    ordered = _ordered_points(points, spec)
    if len(ordered) < 2:
        return 0.0
    flags = [1.0 if _event_detected(point, spec) else 0.0 for point in ordered]
    consistent_steps = sum(
        1
        for left, right in zip(flags, flags[1:])
        if (right >= left if spec.direction == "increasing" else right <= left)
    )
    return consistent_steps / max(len(flags) - 1, 1)


def _fixed_normalization_basis(spec: ThresholdSpec) -> dict[str, object]:
    basis_values = {
        "rho_c": ("admissibility_margin", "rho_safe", 1.10),
        "lambda_c": ("capacity_ratio", "mu", 1.0),
        "gamma_c": ("admissibility_margin", "gamma_min", 0.80),
        "w_c": ("weather_collapse_boundary", "w_critical", 0.40),
    }
    kind, label, value = basis_values[spec.name]
    return {
        "kind": kind,
        "label": label,
        "value": value,
        "origin": NORMALIZATION_ORIGIN_FIXED_CONSTANT,
        "confidence": 1.0,
    }


def _fallback_normalization_basis(spec: ThresholdSpec) -> dict[str, object]:
    basis = _fixed_normalization_basis(spec)
    return {
        **basis,
        "origin": NORMALIZATION_ORIGIN_GOVERNED_FALLBACK,
        "confidence": 0.35,
    }


def _unknown_normalization_basis(spec: ThresholdSpec) -> dict[str, object]:
    basis = _fixed_normalization_basis(spec)
    return {
        **basis,
        "value": None,
        "origin": NORMALIZATION_ORIGIN_UNKNOWN,
        "confidence": 0.0,
    }


def _normalization_basis_value(basis: dict[str, object]) -> float | None:
    value = basis.get("value")
    if not isinstance(value, (int, float)):
        return None
    return float(value)


def _normalize_with_basis(estimate: float | None, basis: dict[str, object]) -> float | None:
    if estimate is None:
        return None
    basis_value = _normalization_basis_value(basis)
    if basis_value is None or math.isclose(basis_value, 0.0):
        return None
    return float(estimate) / basis_value


def _nuisance_factor_value(point: PhasePoint, factor_name: str) -> float | None:
    if factor_name == "congestion_and_spillback_pressure":
        values = [
            point.metrics.get("congestion_spillback_pressure"),
            point.metrics.get("peak_corridor_load_ratio"),
            point.metrics.get("peak_node_utilization_ratio"),
            point.metrics.get("peak_queue_ratio"),
        ]
        numeric_values = [float(value) for value in values if isinstance(value, (int, float))]
        return max(numeric_values) if numeric_values else None
    if factor_name == "trust_degradation":
        value = point.metrics.get("trust_degradation")
        if isinstance(value, (int, float)):
            return float(value)
        trusted = point.metrics.get("trusted_active_fraction")
        return max(0.0, 1.0 - float(trusted)) if isinstance(trusted, (int, float)) else None
    if factor_name == "communications_degradation":
        value = point.metrics.get("communications_degradation")
        if isinstance(value, (int, float)):
            return float(value)
        reliability = point.metrics.get("comms_reliability_min")
        return max(0.0, 1.0 - float(reliability)) if isinstance(reliability, (int, float)) else None
    if factor_name == "navigation_degradation":
        value = point.metrics.get("navigation_degradation")
        if isinstance(value, (int, float)):
            return float(value)
        nav = point.metrics.get("alpha_nav_min")
        return max(0.0, 1.0 - float(nav)) if isinstance(nav, (int, float)) else None
    if factor_name == "weather_severity":
        value = point.metrics.get("weather_severity_peak")
        return float(value) if isinstance(value, (int, float)) else None
    if factor_name == "contingency_saturation_pressure":
        value = point.metrics.get("contingency_saturation_pressure")
        if isinstance(value, (int, float)):
            return float(value)
        margin = point.metrics.get("contingency_margin_min")
        if isinstance(margin, (int, float)):
            return max(0.0, -float(margin))
    return None


def _candidate_axis(spec: ThresholdSpec, points: Sequence[PhasePoint]) -> str | None:
    if spec.preferred_axis is not None and any(spec.preferred_axis in point.parameters for point in points):
        return spec.preferred_axis
    varying_axes = [
        axis
        for axis in sorted({axis for point in points for axis in point.parameters})
        if len({point.parameters[axis] for point in points if axis in point.parameters}) > 1
    ]
    return varying_axes[0] if len(varying_axes) == 1 else None


def _parameter_axis_span(points: Sequence[PhasePoint], axis: str | None) -> float:
    if axis is None:
        return 0.0
    axis_values = [float(point.parameters[axis]) for point in points if axis in point.parameters]
    if len(axis_values) < 2:
        return 0.0
    return max(axis_values) - min(axis_values)


def _derive_tranche_admissibility_basis(
    spec: ThresholdSpec,
    points: Sequence[PhasePoint],
    *,
    support_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    admissible_values = [
        float(value)
        for point in points
        if point.admissibility_state == AdmissibilityState.ADMISSIBLE_CANDIDATE
        and (value := _metric_value(point, spec)) is not None
    ]
    numeric_values = [
        float(value)
        for point in points
        if (value := _metric_value(point, spec)) is not None
    ]
    if admissible_values:
        envelope_value = (
            max(admissible_values)
            if spec.direction == "increasing"
            else min(admissible_values)
        )
        confidence = min(1.0, len(admissible_values) / max(len(numeric_values), 1))
        return {
            "kind": "admissibility_envelope",
            "label": f"{spec.name}_tranche_admissibility_envelope",
            "value": envelope_value,
            "origin": NORMALIZATION_ORIGIN_TRANCHE_ENVELOPE,
            "confidence": confidence,
        }
    if numeric_values:
        return {
            **_fallback_normalization_basis(spec),
            "origin": NORMALIZATION_ORIGIN_FALLBACK_SPARSE_SUPPORT,
        }
    return _unknown_normalization_basis(spec)


def _derive_cross_tranche_basis(
    spec: ThresholdSpec,
    local_records: Sequence[tuple[str, dict[str, object]]],
) -> dict[str, object]:
    envelope_values = [
        float(record["normalization_basis_value"])
        for _tranche_name, record in local_records
        if record.get("normalization_basis_origin") == NORMALIZATION_ORIGIN_TRANCHE_ENVELOPE
        and isinstance(record.get("normalization_basis_value"), (int, float))
    ]
    confidences = [
        float(record["normalization_basis_confidence"])
        for _tranche_name, record in local_records
        if record.get("normalization_basis_origin") == NORMALIZATION_ORIGIN_TRANCHE_ENVELOPE
        and isinstance(record.get("normalization_basis_confidence"), (int, float))
    ]
    if envelope_values:
        return {
            "kind": "cross_tranche_admissibility_envelope",
            "label": f"{spec.name}_cross_tranche_admissibility_envelope",
            "value": fmean(envelope_values),
            "origin": NORMALIZATION_ORIGIN_CROSS_TRANCHE_ENVELOPE,
            "confidence": min(1.0, fmean(confidences) if confidences else 0.5),
        }
    if local_records:
        return _fallback_normalization_basis(spec)
    return _unknown_normalization_basis(spec)


def _normalized_threshold_value(
    spec: ThresholdSpec,
    estimate: float | None,
    basis: dict[str, object] | None = None,
) -> tuple[float | None, dict[str, object]]:
    resolved_basis = basis if basis is not None else _fallback_normalization_basis(spec)
    return _normalize_with_basis(estimate, resolved_basis), resolved_basis


def _normalized_bound(spec: ThresholdSpec, record: dict[str, object], key: str) -> float | None:
    value = record.get(key)
    if not isinstance(value, (int, float)):
        return None
    basis_value = record.get("normalization_basis_value")
    if not isinstance(basis_value, (int, float)):
        basis = record.get("normalization_basis")
        if not isinstance(basis, dict):
            basis = _fallback_normalization_basis(spec)
        basis_value = _normalization_basis_value(basis)
    if basis_value is None or math.isclose(float(basis_value), 0.0):
        return None
    return float(value) / float(basis_value)


def _ordered_points(points: Sequence[PhasePoint], spec: ThresholdSpec) -> list[PhasePoint]:
    ordering_axis = _candidate_axis(spec, points)
    if ordering_axis is not None:
        return sorted(
            [point for point in points if ordering_axis in point.parameters],
            key=lambda point: (point.parameters[ordering_axis], point.slice_id),
            reverse=spec.direction == "decreasing",
        )
    return sorted(
        [point for point in points if _metric_value(point, spec) is not None],
        key=lambda point: (_sort_proxy(point, spec), point.slice_id),
        reverse=spec.direction == "decreasing",
    )


def _attach_transition_region_provenance(
    candidate: dict[str, object],
    transition_regions: Sequence[PhaseRegion],
) -> dict[str, object]:
    source_axis = candidate.get("source_axis")
    estimate = candidate.get("estimate")
    if not isinstance(source_axis, str) or not isinstance(estimate, (int, float)):
        return candidate

    matching_regions = [
        region
        for region in transition_regions
        if region.transition_axis == source_axis
        and source_axis in region.bounds
        and float(region.bounds[source_axis][0]) <= float(estimate) <= float(region.bounds[source_axis][1])
    ]
    if not matching_regions:
        return candidate
    region = sorted(
        matching_regions,
        key=lambda item: (
            abs(float(item.bounds[source_axis][1]) - float(item.bounds[source_axis][0])),
            item.replay_hash or "",
        ),
    )[0]
    return {
        **candidate,
        "source_replay_hash": region.replay_hash,
        "fixed_context": dict(region.fixed_context),
    }


def _phase_event_candidate(spec: ThresholdSpec, points: Sequence[PhasePoint]) -> dict[str, object] | None:
    ordered = _ordered_points(points, spec)
    if len(ordered) < 2:
        return None
    source_axis = _candidate_axis(spec, ordered) or spec.proxy_key

    candidates: list[dict[str, object]] = []
    for index, (left, right) in enumerate(zip(ordered, ordered[1:])):
        left_detected = _event_detected(left, spec)
        right_detected = _event_detected(right, spec)
        if left_detected == right_detected:
            continue

        detected_point = right if right_detected else left
        detected_value = _metric_value(detected_point, spec)
        if detected_value is None:
            continue

        lower_value = _metric_value(left, spec)
        upper_value = _metric_value(right, spec)
        if lower_value is None or upper_value is None:
            continue
        threshold_value = detected_value
        support_points = ordered[max(0, index - 1) : min(len(ordered), index + 3)]
        support_metrics = {
            "bracket_width": abs(float(upper_value) - float(lower_value)),
            "normalized_bracket_width": abs(float(upper_value) - float(lower_value)),
            "axis_total_span": None,
            "refined_depth": 0,
            "neighbor_agreement": 1.0 if left_detected != right_detected else 0.0,
            "phase_consistency": 1.0 if detected_point.mechanism == spec.target_mechanism else 0.5,
            "signal_monotonicity": 1.0,
            "left_support_count": 1,
            "right_support_count": 1,
            "detection_confidence": _event_confidence(detected_point, spec),
        }
        candidates.append(
            {
                "estimate": threshold_value,
                "lower_bound": min(float(lower_value), float(upper_value)),
                "upper_bound": max(float(lower_value), float(upper_value)),
                "supporting_slice_ids": [point.slice_id for point in support_points],
                "supporting_slice_hashes": [point.replay_hash for point in support_points],
                "support_count": len(support_points),
                "source": spec.detection_method,
                "source_axis": source_axis,
                "source_replay_hash": None,
                "fixed_context": {},
                "support_metrics": support_metrics,
                "replay_hash": _stable_hash(
                    {
                        "threshold_name": spec.name,
                        "candidate_source": spec.detection_method,
                        "supporting_slice_hashes": [point.replay_hash for point in support_points],
                        "estimate": threshold_value,
                    }
                ),
            }
        )

    if candidates:
        return sorted(
            candidates,
            key=lambda candidate: (
                -float(candidate["support_metrics"]["detection_confidence"]),
                float(candidate["support_metrics"]["bracket_width"]),
                -int(candidate["support_count"]),
            ),
        )[0]

    detected_points = [
        point
        for point in ordered
        if _event_detected(point, spec) and _metric_value(point, spec) is not None
    ]
    if not detected_points:
        return None
    anchor = sorted(
        detected_points,
        key=lambda point: (-_event_confidence(point, spec), point.slice_id),
    )[0]
    value = _metric_value(anchor, spec)
    if value is None:
        return None
    return {
        "estimate": value,
        "lower_bound": value,
        "upper_bound": value,
        "supporting_slice_ids": [anchor.slice_id],
        "supporting_slice_hashes": [anchor.replay_hash],
        "support_count": 1,
        "source": spec.detection_method,
        "source_axis": source_axis,
        "source_replay_hash": None,
        "fixed_context": {},
        "support_metrics": {
            "bracket_width": 0.0,
            "normalized_bracket_width": 0.0,
            "axis_total_span": None,
            "refined_depth": 0,
            "neighbor_agreement": 0.0,
            "phase_consistency": 1.0 if anchor.mechanism == spec.target_mechanism else 0.5,
            "signal_monotonicity": 1.0,
            "left_support_count": 0,
            "right_support_count": 1,
            "detection_confidence": _event_confidence(anchor, spec),
        },
        "replay_hash": _stable_hash(
            {
                "threshold_name": spec.name,
                "candidate_source": spec.detection_method,
                "supporting_slice_hashes": [anchor.replay_hash],
                "estimate": value,
            }
        ),
    }


def _proxy_crossing_candidate(spec: ThresholdSpec, points: Sequence[PhasePoint]) -> dict[str, object] | None:
    ordered = _ordered_points(points, spec)
    if len(ordered) < 2:
        return None
    source_axis = _candidate_axis(spec, ordered) or spec.proxy_key
    for index, (left, right) in enumerate(zip(ordered, ordered[1:])):
        if left.mechanism == right.mechanism:
            continue
        left_value = _metric_value(left, spec)
        right_value = _metric_value(right, spec)
        if left_value is None or right_value is None:
            continue
        support_points = ordered[max(0, index - 1) : min(len(ordered), index + 3)]
        detection_confidence = 0.60 if len(support_points) >= 3 else 0.25
        return {
            "estimate": (float(left_value) + float(right_value)) / 2.0,
            "lower_bound": min(float(left_value), float(right_value)),
            "upper_bound": max(float(left_value), float(right_value)),
            "supporting_slice_ids": [point.slice_id for point in support_points],
            "supporting_slice_hashes": [point.replay_hash for point in support_points],
            "support_count": len(support_points),
            "source": "proxy_crossing",
            "source_axis": source_axis,
            "source_replay_hash": None,
            "fixed_context": {},
            "support_metrics": {
                "bracket_width": abs(float(right_value) - float(left_value)),
                "normalized_bracket_width": abs(float(right_value) - float(left_value)),
                "axis_total_span": None,
                "refined_depth": 0,
                "neighbor_agreement": 0.5,
                "phase_consistency": 0.5,
                "signal_monotonicity": 1.0,
                "left_support_count": 1,
                "right_support_count": 1,
                "detection_confidence": detection_confidence,
            },
            "replay_hash": _stable_hash(
                {
                    "threshold_name": spec.name,
                    "candidate_source": "proxy_crossing",
                    "supporting_slice_hashes": [point.replay_hash for point in support_points],
                }
            ),
        }
    return None


def _best_candidate(
    spec: ThresholdSpec,
    points: Sequence[PhasePoint],
    transition_regions: Sequence[PhaseRegion],
) -> dict[str, object] | None:
    candidate = _phase_event_candidate(spec, points)
    if candidate is None:
        candidate = _proxy_crossing_candidate(spec, points)
    if candidate is None:
        return None
    return _attach_transition_region_provenance(candidate, transition_regions)


def _meets_local_boundary_support(candidate: dict[str, object]) -> bool:
    support_metrics = candidate["support_metrics"]
    return (
        int(candidate["support_count"]) >= 2
        and float(support_metrics["detection_confidence"]) >= 0.55
        and float(support_metrics["phase_consistency"]) >= MIN_PHASE_CONSISTENCY
    )


def _replay_agreement(
    spec: ThresholdSpec,
    candidate: dict[str, object] | None,
    replay_points: Sequence[PhasePoint] | None,
    replay_transition_regions: Sequence[PhaseRegion] | None,
) -> bool:
    if candidate is None:
        return False
    if replay_points is None or replay_transition_regions is None:
        return True
    replay_candidate = _best_candidate(spec, replay_points, replay_transition_regions)
    if replay_candidate is None:
        return False
    return candidate["replay_hash"] == replay_candidate["replay_hash"]


def _threshold_id(spec: ThresholdSpec, *, scope: str, tranche_name: str | None = None) -> str:
    if tranche_name:
        return f"{scope}:{tranche_name}:{spec.name}"
    return f"{scope}:{spec.name}"


def _candidate_neighborhood(
    spec: ThresholdSpec,
    points: Sequence[PhasePoint],
    candidate: dict[str, object],
) -> list[PhasePoint]:
    support_ids = set(candidate.get("supporting_slice_ids", []))
    ordered = [point for point in _ordered_points(points, spec) if _metric_value(point, spec) is not None]
    if not ordered:
        return []

    estimate = float(candidate["estimate"])
    lower = float(candidate["lower_bound"])
    upper = float(candidate["upper_bound"])
    values = [float(_metric_value(point, spec)) for point in ordered if _metric_value(point, spec) is not None]
    total_span = max(values) - min(values) if values else 0.0
    window = max(abs(upper - lower), total_span * 0.15, 1e-6)

    neighborhood = [
        point
        for point in ordered
        if point.slice_id in support_ids or abs(float(_metric_value(point, spec)) - estimate) <= window
    ]
    if len(neighborhood) >= 3:
        return neighborhood
    ranked = sorted(
        ordered,
        key=lambda point: (
            abs(float(_metric_value(point, spec)) - estimate),
            point.slice_id,
        ),
    )
    return ranked[: min(5, len(ranked))]


def _dominant_mechanism(points: Sequence[PhasePoint]) -> str | None:
    mechanisms = [point.mechanism for point in points if point.mechanism]
    if not mechanisms:
        return None
    return sorted(
        statistics.multimode(mechanisms),
    )[0]


def _admissibility_support_audit(
    spec: ThresholdSpec,
    points: Sequence[PhasePoint],
    candidate: dict[str, object] | None,
) -> dict[str, object]:
    if candidate is None:
        return {
            "admissibility_support_density": 0.0,
            "admissibility_support_span": 0.0,
            "admissibility_support_confidence": 0.0,
            "support_axis": None,
            "neighborhood_slice_ids": [],
            "admissible_slice_ids": [],
            "blocking": False,
        }

    neighborhood = _candidate_neighborhood(spec, points, candidate)
    support_axis = _candidate_axis(spec, neighborhood or points)
    admissible_points = [
        point
        for point in neighborhood
        if point.admissibility_state == AdmissibilityState.ADMISSIBLE_CANDIDATE
    ]
    density = len(admissible_points) / len(neighborhood) if neighborhood else 0.0
    confidence = density * min(1.0, len(admissible_points) / max(MIN_TOTAL_SUPPORT, 1))
    return {
        "admissibility_support_density": density,
        "admissibility_support_span": _parameter_axis_span(admissible_points, support_axis),
        "admissibility_support_confidence": confidence,
        "support_axis": support_axis,
        "neighborhood_slice_ids": [point.slice_id for point in neighborhood],
        "admissible_slice_ids": [point.slice_id for point in admissible_points],
        "blocking": bool(neighborhood) and density < MIN_SUPPORT_DENSITY,
    }


def _response_signal(point: PhasePoint, spec: ThresholdSpec) -> float:
    event_response = 1.0 if _event_detected(point, spec) else 0.0
    target_alignment = 1.0 if point.mechanism == spec.target_mechanism else 0.0
    return (0.7 * event_response) + (0.3 * target_alignment)


def _max_nuisance_score(nuisance_vector: dict[str, float]) -> float:
    return max((float(value) for value in nuisance_vector.values()), default=0.0)


def _nuisance_entropy(nuisance_vector: dict[str, float]) -> float:
    total = sum(float(value) for value in nuisance_vector.values())
    if total <= 1e-9:
        return 0.0
    probabilities = [float(value) / total for value in nuisance_vector.values() if float(value) > 0.0]
    return -sum(probability * math.log(probability) for probability in probabilities)


def _nuisance_audit(
    spec: ThresholdSpec,
    points: Sequence[PhasePoint],
    candidate: dict[str, object] | None,
) -> dict[str, object]:
    nuisance_vector = {axis: 0.0 for axis, _factor_name in NUISANCE_AXIS_FACTORS}
    if candidate is None:
        return {
            "mechanism_leakage_score": 0.0,
            "mechanism_leakage_sources": [],
            "nuisance_vector": nuisance_vector,
            "nuisance_dominant_axis": None,
            "nuisance_entropy": 0.0,
            "nuisance_sensitivity_summary": {},
            "leakage_blocking_reason": None,
            "blocking": False,
        }

    neighborhood = _candidate_neighborhood(spec, points, candidate)
    summary: dict[str, object] = {
        "region_slice_ids": [point.slice_id for point in neighborhood],
        "factor_assessments": {},
        "assessment_status": "ok" if len(neighborhood) >= 4 else "insufficient_neighborhood_support",
    }
    if len(neighborhood) < 4:
        return {
            "mechanism_leakage_score": 0.0,
            "mechanism_leakage_sources": [],
            "nuisance_vector": nuisance_vector,
            "nuisance_dominant_axis": None,
            "nuisance_entropy": 0.0,
            "nuisance_sensitivity_summary": summary,
            "leakage_blocking_reason": None,
            "blocking": False,
        }

    for nuisance_axis, factor_name in NUISANCE_AXIS_FACTORS:
        factor_values = [
            (point, value)
            for point in neighborhood
            if (value := _nuisance_factor_value(point, factor_name)) is not None
        ]
        if len(factor_values) < 3:
            summary["factor_assessments"][nuisance_axis] = {
                "present": False,
                "sample_count": len(factor_values),
            }
            continue

        ordered = sorted(factor_values, key=lambda item: (item[1], item[0].slice_id))
        midpoint = max(1, len(ordered) // 2)
        lower_points = [point for point, _value in ordered[:midpoint]]
        upper_points = [point for point, _value in ordered[midpoint:]]
        lower_values = [value for _point, value in ordered[:midpoint]]
        upper_values = [value for _point, value in ordered[midpoint:]]
        delta_nuisance = fmean(upper_values) - fmean(lower_values)
        if abs(delta_nuisance) <= 1e-9:
            summary["factor_assessments"][nuisance_axis] = {
                "present": False,
                "sample_count": len(ordered),
                "assessment_status": "constant_under_bounded_variation",
            }
            continue

        lower_signal = fmean([_response_signal(point, spec) for point in lower_points])
        upper_signal = fmean([_response_signal(point, spec) for point in upper_points])
        sensitivity = abs(upper_signal - lower_signal) / max(abs(delta_nuisance), 1e-9)
        normalized_sensitivity = min(1.0, sensitivity)
        nuisance_vector[nuisance_axis] = normalized_sensitivity
        summary["factor_assessments"][nuisance_axis] = {
            "present": True,
            "sample_count": len(ordered),
            "value_range": [min(lower_values + upper_values), max(lower_values + upper_values)],
            "delta_nuisance": delta_nuisance,
            "lower_signal": lower_signal,
            "upper_signal": upper_signal,
            "sensitivity": sensitivity,
            "normalized_sensitivity": normalized_sensitivity,
        }

    dominant_axis = None
    max_score = _max_nuisance_score(nuisance_vector)
    if max_score > 0.0:
        dominant_axis = sorted(
            nuisance_vector.items(),
            key=lambda item: (-float(item[1]), item[0]),
        )[0][0]
    entropy = _nuisance_entropy(nuisance_vector)
    blocking = max_score > LEAKAGE_THRESHOLD and entropy < ENTROPY_MIN
    mechanism_leakage_sources = sorted(
        LEGACY_NUISANCE_SOURCE_NAMES[axis]
        for axis, score in nuisance_vector.items()
        if score > 0.0 and (axis == dominant_axis or score >= LEAKAGE_THRESHOLD)
    )
    return {
        "mechanism_leakage_score": max_score,
        "mechanism_leakage_sources": mechanism_leakage_sources,
        "nuisance_vector": nuisance_vector,
        "nuisance_dominant_axis": dominant_axis,
        "nuisance_entropy": entropy,
        "nuisance_sensitivity_summary": summary,
        "leakage_blocking_reason": BLOCKER_NUISANCE_DOMINANCE if blocking else None,
        "blocking": blocking,
    }


def map_contradictions_to_outcome(
    contradictions: Sequence[dict[str, object]],
) -> PromotionGovernanceOutcome:
    contradiction_types = {
        str(item.get("contradiction_type"))
        for item in contradictions
        if isinstance(item, dict) and item.get("contradiction_type") is not None
    }
    if (
        CONTRADICTION_CROSS_TRANCHE_CONFLICT in contradiction_types
        or CONTRADICTION_ENVELOPE_VIOLATION in contradiction_types
    ):
        return PromotionGovernanceOutcome.GLOBAL_BLOCK
    if (
        CONTRADICTION_NUISANCE_DOMINANCE in contradiction_types
        or CONTRADICTION_LOCAL_INCONSISTENCY in contradiction_types
        or CONTRADICTION_NON_MONOTONIC_THRESHOLD in contradiction_types
    ):
        return PromotionGovernanceOutcome.LOCAL_BLOCK
    return PromotionGovernanceOutcome.ALLOW


def _normalization_origin_rank(origin: str | None) -> int:
    ranking = {
        NORMALIZATION_ORIGIN_CROSS_TRANCHE_ENVELOPE: 0,
        NORMALIZATION_ORIGIN_TRANCHE_ENVELOPE: 1,
        NORMALIZATION_ORIGIN_FIXED_CONSTANT: 2,
        NORMALIZATION_ORIGIN_GOVERNED_FALLBACK: 3,
        NORMALIZATION_ORIGIN_FALLBACK_SPARSE_SUPPORT: 4,
        NORMALIZATION_ORIGIN_UNKNOWN: 5,
    }
    return ranking.get(str(origin), 99)


def _normalization_regressed(previous_origin: str | None, current_origin: str | None) -> bool:
    return _normalization_origin_rank(current_origin) > _normalization_origin_rank(previous_origin)


def _monotonicity_audit(
    previous_record: dict[str, object] | None,
    *,
    current_support_confidence: float,
    current_normalization_origin: str,
    current_nuisance_vector: dict[str, float],
) -> tuple[bool, str | None]:
    if not isinstance(previous_record, dict):
        return False, None

    previous_confidence = previous_record.get("admissibility_support_confidence")
    if isinstance(previous_confidence, (int, float)) and current_support_confidence < float(previous_confidence) - 1e-9:
        return True, "ADMISSIBILITY_CONFIDENCE_DECREASED"

    previous_origin = previous_record.get("normalization_basis_origin")
    if isinstance(previous_origin, str) and _normalization_regressed(previous_origin, current_normalization_origin):
        return True, "NORMALIZATION_BASIS_REGRESSED"

    previous_vector = previous_record.get("nuisance_vector")
    previous_max = (
        _max_nuisance_score({str(axis): float(value) for axis, value in previous_vector.items()})
        if isinstance(previous_vector, dict)
        else 0.0
    )
    current_max = _max_nuisance_score(current_nuisance_vector)
    if current_max > previous_max + 1e-9:
        return True, "NUISANCE_LEAKAGE_INCREASED"
    return False, None


def adaptive_region_priority_terms(
    region: PhaseRegion,
    threshold_payload: dict[str, object],
) -> dict[str, float]:
    thresholds = threshold_payload.get("thresholds", {})
    if not isinstance(thresholds, dict):
        return {"support_density": 0.0, "support_confidence": 0.0}

    matching_records = []
    for record in thresholds.values():
        if not isinstance(record, dict):
            continue
        derivation_basis = record.get("derivation_basis")
        if not isinstance(derivation_basis, dict):
            continue
        if region.replay_hash is not None and derivation_basis.get("source_replay_hash") == region.replay_hash:
            matching_records.append(record)
            continue
        if region.transition_axis is not None and derivation_basis.get("source_axis") == region.transition_axis:
            matching_records.append(record)
    if not matching_records:
        return {"support_density": 0.0, "support_confidence": 0.0}
    return {
        "support_density": max(
            float(record.get("admissibility_support_density", 0.0))
            for record in matching_records
        ),
        "support_confidence": min(
            float(record.get("admissibility_support_confidence", 0.0))
            for record in matching_records
        ),
    }


def _threshold_record(
    tranche_name: str,
    spec: ThresholdSpec,
    points: Sequence[PhasePoint],
    candidate: dict[str, object] | None,
    *,
    replay_agreement: bool,
    axis_isolation: dict[str, object],
    tranche_event_consistency: dict[str, object],
    signal_monotonicity: float,
    previous_record: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    support_audit = _admissibility_support_audit(spec, points, candidate)
    nuisance_audit = _nuisance_audit(spec, points, candidate)
    normalization_basis = _derive_tranche_admissibility_basis(
        spec,
        points,
        support_audit=support_audit,
    )
    monotonicity_violation, monotonicity_block_reason = _monotonicity_audit(
        previous_record,
        current_support_confidence=float(support_audit["admissibility_support_confidence"]),
        current_normalization_origin=str(normalization_basis["origin"]),
        current_nuisance_vector=dict(nuisance_audit["nuisance_vector"]),
    )
    base_record = {
        "threshold_id": _threshold_id(spec, scope="local_tranche", tranche_name=tranche_name),
        "symbol": spec.symbol,
        "legacy_name": spec.name,
        "proxy": spec.proxy_key,
        "target_mechanism": spec.target_mechanism,
        "preferred_axis": spec.preferred_axis,
        "direction": spec.direction,
        "detection_method": spec.detection_method,
        "estimate_scope": "local_tranche_estimate",
        "tranche_scope": {
            "tranche_name": tranche_name,
            "scope": "local_tranche",
        },
        "epistemic_note": EPISTEMIC_NOTE,
        "axis_purity_score": axis_isolation["axis_purity_score"],
        "leakage_score": axis_isolation["leakage_score"],
        "is_isolated": axis_isolation["is_isolated"],
        "event_consistency": tranche_event_consistency,
        "normalization_basis": dict(normalization_basis),
        "normalization_basis_origin": normalization_basis["origin"],
        "normalization_basis_value": normalization_basis["value"],
        "normalization_basis_confidence": normalization_basis["confidence"],
        "admissibility_support_density": support_audit["admissibility_support_density"],
        "admissibility_support_span": support_audit["admissibility_support_span"],
        "admissibility_support_confidence": support_audit["admissibility_support_confidence"],
        "nuisance_vector": dict(nuisance_audit["nuisance_vector"]),
        "nuisance_dominant_axis": nuisance_audit["nuisance_dominant_axis"],
        "nuisance_entropy": nuisance_audit["nuisance_entropy"],
        "monotonicity_violation": monotonicity_violation,
        "monotonicity_block_reason": monotonicity_block_reason,
    }
    if candidate is None:
        governance_outcome = PromotionGovernanceOutcome.ALLOW
        threshold_record = {
            **base_record,
            "estimate": None,
            "normalized_threshold_value": None,
            "evidence_type": ThresholdEvidenceType.PROXY_ONLY.value,
            "confidence": 0.0,
            "supporting_event_count": 0,
            "seed_count": 0,
            "variance": None,
            "status": ThresholdEvidenceStatus.INSUFFICIENT_DATA.value,
            "status_reason": "No replayable tranche evidence candidate was found.",
            "support_count": 0,
            "derivation_basis": None,
            "support_metrics": None,
            "mechanism_leakage_score": nuisance_audit["mechanism_leakage_score"],
            "mechanism_leakage_sources": list(nuisance_audit["mechanism_leakage_sources"]),
            "nuisance_sensitivity_summary": nuisance_audit["nuisance_sensitivity_summary"],
            "leakage_blocking_reason": nuisance_audit["leakage_blocking_reason"],
            "replay_hash_provenance": {
                "threshold_replay_hash": None,
                "supporting_slice_hashes": [],
                "replay_agreement": False,
            },
            "threshold_promotion_decision": PROMOTION_LOW_CONFIDENCE,
            "promotion_blockers": [PROMOTION_LOW_CONFIDENCE],
            "promotion_governance_outcome": governance_outcome.value,
            "contradictions": [],
            "promotion_state": {
                "promoted": False,
                "decision": "insufficient_data",
                "threshold_promotion_decision": PROMOTION_LOW_CONFIDENCE,
                "promotion_governance_outcome": governance_outcome.value,
            },
        }
        ledger_entry = {
            "threshold": spec.name,
            "symbol": spec.symbol,
            "status": ThresholdEvidenceStatus.INSUFFICIENT_DATA.value,
            "estimate_scope": "local_tranche_estimate",
            "decision": "insufficient_data",
            "threshold_promotion_decision": PROMOTION_LOW_CONFIDENCE,
            "normalization_basis_origin": normalization_basis["origin"],
            "promotion_blockers": [PROMOTION_LOW_CONFIDENCE],
            "promotion_governance_outcome": governance_outcome.value,
        }
        promotion_decision = {
            "threshold": spec.name,
            "symbol": spec.symbol,
            "scope": "local_tranche",
            "accepted": False,
            "decision": "insufficient_data",
            "threshold_promotion_decision": PROMOTION_LOW_CONFIDENCE,
            "reason": "No evidence candidate available.",
            "promotion_blockers": [PROMOTION_LOW_CONFIDENCE],
            "promotion_governance_outcome": governance_outcome.value,
        }
        return threshold_record, ledger_entry, promotion_decision

    promotable = _meets_local_boundary_support(candidate) and replay_agreement
    supporting_points = [
        point for point in points if point.slice_id in set(candidate["supporting_slice_ids"])
    ]
    supporting_event_count = sum(1 for point in supporting_points if _event_detected(point, spec))
    metric_values = [
        float(value)
        for point in supporting_points
        if (value := _metric_value(point, spec)) is not None
    ]
    estimate_variance = _numeric_variance(metric_values)
    event_consistency = validate_event_order_consistency([_event_chain_from_point(point) for point in supporting_points])
    normalized_value, normalization_basis = _normalized_threshold_value(
        spec,
        float(candidate["estimate"]),
        basis=normalization_basis,
    )
    monotonicity_violation, monotonicity_block_reason = _monotonicity_audit(
        previous_record,
        current_support_confidence=float(support_audit["admissibility_support_confidence"]),
        current_normalization_origin=str(normalization_basis["origin"]),
        current_nuisance_vector=dict(nuisance_audit["nuisance_vector"]),
    )
    seed_count = max(
        (int(point.metrics.get("seed_count", 1)) for point in supporting_points),
        default=0,
    )
    normalized_lower = _normalize_with_basis(float(candidate["lower_bound"]), normalization_basis)
    normalized_upper = _normalize_with_basis(float(candidate["upper_bound"]), normalization_basis)
    support_metrics = dict(candidate["support_metrics"])
    support_metrics["normalized_bracket_width"] = (
        None
        if normalized_lower is None or normalized_upper is None
        else abs(normalized_upper - normalized_lower)
    )
    phase_candidate = candidate["source"] == spec.detection_method
    if (
        phase_candidate
        and supporting_event_count >= 2
        and event_consistency["ordering_valid"]
        and estimate_variance <= MAX_LOCAL_VARIANCE
    ):
        evidence_type = ThresholdEvidenceType.PHASE_DERIVED
    elif phase_candidate:
        evidence_type = ThresholdEvidenceType.MIXED
    else:
        evidence_type = ThresholdEvidenceType.PROXY_ONLY
    confidence = min(
        1.0,
        float(candidate["support_metrics"]["detection_confidence"])
        * (1.0 if replay_agreement else 0.7)
        * (1.0 if event_consistency["ordering_valid"] else 0.6)
        * max(float(support_audit["admissibility_support_confidence"]), 0.2)
        * max(signal_monotonicity, 0.2)
        * max(float(axis_isolation["axis_purity_score"]), 0.2),
    )
    contradictions: list[dict[str, object]] = []
    if not event_consistency["ordering_valid"]:
        contradictions.append(
            _contradiction_record(
                spec,
                CONTRADICTION_LOCAL_INCONSISTENCY,
                [tranche_name],
                severity=CONTRADICTION_SEVERITY_BLOCKING,
                message="Multi-seed event ordering is inconsistent within the tranche support neighborhood.",
                threshold_ids_involved=[_threshold_id(spec, scope="local_tranche", tranche_name=tranche_name)],
                invalid_reasons=list(event_consistency.get("invalid_reasons", [])),
            )
        )
    if nuisance_audit["blocking"]:
        contradictions.append(
            _contradiction_record(
                spec,
                CONTRADICTION_NUISANCE_DOMINANCE,
                [tranche_name],
                severity=CONTRADICTION_SEVERITY_BLOCKING,
                message="Threshold support is dominated by a single nuisance axis.",
                threshold_ids_involved=[_threshold_id(spec, scope="local_tranche", tranche_name=tranche_name)],
                nuisance_dominant_axis=nuisance_audit["nuisance_dominant_axis"],
                nuisance_entropy=nuisance_audit["nuisance_entropy"],
                nuisance_vector=dict(nuisance_audit["nuisance_vector"]),
            )
        )
    if signal_monotonicity < MIN_SIGNAL_MONOTONICITY or monotonicity_violation:
        contradictions.append(
            _contradiction_record(
                spec,
                CONTRADICTION_NON_MONOTONIC_THRESHOLD,
                [tranche_name],
                severity=CONTRADICTION_SEVERITY_BLOCKING,
                message="Threshold promotion violates monotonicity invariants.",
                threshold_ids_involved=[_threshold_id(spec, scope="local_tranche", tranche_name=tranche_name)],
                signal_monotonicity=signal_monotonicity,
                monotonicity_violation=monotonicity_violation,
                monotonicity_block_reason=monotonicity_block_reason,
            )
        )
    governance_outcome = map_contradictions_to_outcome(contradictions)
    promotion_blockers: list[str] = []
    promotable = (
        promotable
        and evidence_type == ThresholdEvidenceType.PHASE_DERIVED
        and bool(axis_isolation["is_isolated"])
        and event_consistency["ordering_valid"]
        and signal_monotonicity >= MIN_SIGNAL_MONOTONICITY
        and estimate_variance <= MAX_LOCAL_VARIANCE
        and float(support_audit["admissibility_support_density"]) >= MIN_SUPPORT_DENSITY
        and not nuisance_audit["blocking"]
        and not monotonicity_violation
        and governance_outcome == PromotionGovernanceOutcome.ALLOW
    )
    if promotable:
        status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
        decision = "promoted_to_tranche_boundary"
        reason = "Phase-derived transition evidence satisfied replay, admissibility, nuisance, and monotonicity governance."
        threshold_promotion_decision = PROMOTION_PROMOTED
    else:
        status = ThresholdEvidenceStatus.PROXY
        if evidence_type == ThresholdEvidenceType.PROXY_ONLY:
            decision = "retained_as_proxy_only"
            reason = "Only proxy-level evidence is available."
            threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
            promotion_blockers.append(PROMOTION_LOW_CONFIDENCE)
        elif not _meets_local_boundary_support(candidate):
            decision = "retained_as_proxy_weak_support"
            reason = "Transition evidence did not satisfy minimum adjacent support or consistency requirements."
            threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
            promotion_blockers.append(PROMOTION_LOW_CONFIDENCE)
        elif not replay_agreement:
            decision = "retained_as_proxy_replay_mismatch"
            reason = "Transition evidence failed deterministic replay agreement."
            threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
            promotion_blockers.append(PROMOTION_LOW_CONFIDENCE)
        elif not event_consistency["ordering_valid"]:
            decision = "retained_as_mixed_inconsistent_event_order"
            reason = "Multi-seed event ordering was inconsistent or collapsed in time."
            threshold_promotion_decision = PROMOTION_EVENT_ORDERING
            promotion_blockers.append(PROMOTION_EVENT_ORDERING)
        elif not bool(axis_isolation["is_isolated"]):
            decision = "retained_as_mixed_axis_leakage"
            reason = "Tranche axis leakage exceeded the isolation threshold."
            threshold_promotion_decision = PROMOTION_AXIS_LEAKAGE
            promotion_blockers.append(PROMOTION_AXIS_LEAKAGE)
        elif nuisance_audit["blocking"]:
            decision = "retained_as_mixed_mechanism_leakage"
            reason = BLOCKER_NUISANCE_DOMINANCE
            threshold_promotion_decision = PROMOTION_MECHANISM_LEAKAGE
            promotion_blockers.append(PROMOTION_MECHANISM_LEAKAGE)
            promotion_blockers.append(BLOCKER_NUISANCE_DOMINANCE)
        elif monotonicity_violation or signal_monotonicity < MIN_SIGNAL_MONOTONICITY:
            decision = "retained_as_mixed_non_monotonic"
            reason = (
                "Threshold behavior was not monotonic along the tranche sweep axis."
                if not monotonicity_violation
                else f"Monotonicity invariant failed: {monotonicity_block_reason}."
            )
            threshold_promotion_decision = PROMOTION_NONMONOTONICITY
            promotion_blockers.append(PROMOTION_NONMONOTONICITY)
        elif estimate_variance > MAX_LOCAL_VARIANCE:
            decision = "retained_as_mixed_high_variance"
            reason = "Threshold support variance exceeded the local promotion bound."
            threshold_promotion_decision = PROMOTION_HIGH_VARIANCE
            promotion_blockers.append(PROMOTION_HIGH_VARIANCE)
        elif float(support_audit["admissibility_support_density"]) < MIN_SUPPORT_DENSITY:
            decision = "retained_as_sparse_admissibility_support"
            reason = "Admissibility support density near the threshold is below the promotion minimum."
            threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
            promotion_blockers.append(BLOCKER_INSUFFICIENT_ADMISSIBLE_SUPPORT)
        else:
            decision = "retained_as_mixed_threshold"
            reason = "Threshold has partial phase support but failed tranche promotion governance."
            threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
            promotion_blockers.append(PROMOTION_LOW_CONFIDENCE)
        if (
            float(support_audit["admissibility_support_density"]) < MIN_SUPPORT_DENSITY
            and BLOCKER_INSUFFICIENT_ADMISSIBLE_SUPPORT not in promotion_blockers
        ):
            promotion_blockers.append(BLOCKER_INSUFFICIENT_ADMISSIBLE_SUPPORT)

    threshold_record = {
        **base_record,
        "estimate": candidate["estimate"],
        "normalized_threshold_value": normalized_value,
        "normalization_basis": dict(normalization_basis),
        "normalization_basis_origin": normalization_basis["origin"],
        "normalization_basis_value": normalization_basis["value"],
        "normalization_basis_confidence": normalization_basis["confidence"],
        "evidence_type": evidence_type.value,
        "confidence": confidence,
        "supporting_event_count": supporting_event_count,
        "seed_count": seed_count,
        "variance": estimate_variance,
        "lower_bound": candidate["lower_bound"],
        "upper_bound": candidate["upper_bound"],
        "status": status.value,
        "status_reason": reason,
        "support_count": candidate["support_count"],
        "derivation_basis": {
            "source": candidate["source"],
            "source_axis": candidate["source_axis"],
            "source_replay_hash": candidate["source_replay_hash"],
            "fixed_context": dict(candidate["fixed_context"]),
            "supporting_slice_ids": list(candidate["supporting_slice_ids"]),
        },
        "support_metrics": support_metrics,
        "mechanism_leakage_score": nuisance_audit["mechanism_leakage_score"],
        "mechanism_leakage_sources": list(nuisance_audit["mechanism_leakage_sources"]),
        "nuisance_sensitivity_summary": nuisance_audit["nuisance_sensitivity_summary"],
        "leakage_blocking_reason": nuisance_audit["leakage_blocking_reason"],
        "replay_hash_provenance": {
            "threshold_replay_hash": candidate["replay_hash"],
            "supporting_slice_hashes": list(candidate["supporting_slice_hashes"]),
            "replay_agreement": replay_agreement,
        },
        "confidence_metric": candidate["support_metrics"]["detection_confidence"],
        "event_consistency": event_consistency,
        "threshold_promotion_decision": threshold_promotion_decision,
        "promotion_blockers": promotion_blockers,
        "promotion_governance_outcome": governance_outcome.value,
        "contradictions": contradictions,
        "promotion_state": {
            "promoted": promotable,
            "decision": decision,
            "threshold_promotion_decision": threshold_promotion_decision,
            "promotion_governance_outcome": governance_outcome.value,
            "local_requirements_met": evidence_type == ThresholdEvidenceType.PHASE_DERIVED and event_consistency["ordering_valid"] and estimate_variance <= MAX_LOCAL_VARIANCE,
            "tranche_requirements_met": promotable,
        },
    }
    ledger_entry = {
        "threshold": spec.name,
        "symbol": spec.symbol,
        "threshold_id": _threshold_id(spec, scope="local_tranche", tranche_name=tranche_name),
        "status": status.value,
        "evidence_type": evidence_type.value,
        "estimate_scope": "local_tranche_estimate",
        "estimate": candidate["estimate"],
        "normalized_threshold_value": normalized_value,
        "normalization_basis_origin": normalization_basis["origin"],
        "normalization_basis_value": normalization_basis["value"],
        "normalization_basis_confidence": normalization_basis["confidence"],
        "lower_bound": candidate["lower_bound"],
        "upper_bound": candidate["upper_bound"],
        "support_count": candidate["support_count"],
        "supporting_event_count": supporting_event_count,
        "seed_count": seed_count,
        "variance": estimate_variance,
        "decision": decision,
        "threshold_promotion_decision": threshold_promotion_decision,
        "promotion_blockers": promotion_blockers,
        "promotion_governance_outcome": governance_outcome.value,
        "replay_hash": candidate["replay_hash"],
        "supporting_slice_ids": list(candidate["supporting_slice_ids"]),
        "support_metrics": support_metrics,
        "detection_method": spec.detection_method,
        "confidence_metric": confidence,
        "mechanism_leakage_score": nuisance_audit["mechanism_leakage_score"],
        "mechanism_leakage_sources": list(nuisance_audit["mechanism_leakage_sources"]),
        "admissibility_support_density": support_audit["admissibility_support_density"],
        "admissibility_support_confidence": support_audit["admissibility_support_confidence"],
        "nuisance_dominant_axis": nuisance_audit["nuisance_dominant_axis"],
        "nuisance_entropy": nuisance_audit["nuisance_entropy"],
        "monotonicity_violation": monotonicity_violation,
        "monotonicity_block_reason": monotonicity_block_reason,
    }
    promotion_decision = {
        "threshold": spec.name,
        "symbol": spec.symbol,
        "threshold_id": _threshold_id(spec, scope="local_tranche", tranche_name=tranche_name),
        "scope": "local_tranche",
        "accepted": promotable,
        "decision": decision,
        "threshold_promotion_decision": threshold_promotion_decision,
        "reason": reason,
        "support_count": candidate["support_count"],
        "replay_agreement": replay_agreement,
        "supporting_slice_ids": list(candidate["supporting_slice_ids"]),
        "confidence_metric": confidence,
        "evidence_type": evidence_type.value,
        "ordering_valid": event_consistency["ordering_valid"],
        "is_isolated": bool(axis_isolation["is_isolated"]),
        "signal_monotonicity": signal_monotonicity,
        "normalization_basis_origin": normalization_basis["origin"],
        "promotion_blockers": promotion_blockers,
        "mechanism_leakage_score": nuisance_audit["mechanism_leakage_score"],
        "mechanism_leakage_sources": list(nuisance_audit["mechanism_leakage_sources"]),
        "promotion_governance_outcome": governance_outcome.value,
        "admissibility_support_density": support_audit["admissibility_support_density"],
        "admissibility_support_confidence": support_audit["admissibility_support_confidence"],
        "nuisance_vector": dict(nuisance_audit["nuisance_vector"]),
        "nuisance_dominant_axis": nuisance_audit["nuisance_dominant_axis"],
        "nuisance_entropy": nuisance_audit["nuisance_entropy"],
        "monotonicity_violation": monotonicity_violation,
        "monotonicity_block_reason": monotonicity_block_reason,
        "contradictions": contradictions,
    }
    return threshold_record, ledger_entry, promotion_decision


def build_threshold_estimates(
    tranche_name: str,
    points: Sequence[PhasePoint],
    transition_regions: Sequence[PhaseRegion],
    *,
    replay_points: Sequence[PhasePoint] | None = None,
    replay_transition_regions: Sequence[PhaseRegion] | None = None,
    dominant_axis: str | None = None,
    previous_thresholds: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    thresholds: dict[str, object] = {}
    threshold_ledger: list[dict[str, object]] = []
    promotion_decisions: list[dict[str, object]] = []
    axis_isolation = _infer_axis_isolation(points, dominant_axis=dominant_axis)
    tranche_event_consistency = validate_event_order_consistency([_event_chain_from_point(point) for point in points])

    for spec in THRESHOLD_SPECS:
        candidate = _best_candidate(spec, points, transition_regions)
        replay_ok = _replay_agreement(spec, candidate, replay_points, replay_transition_regions)
        threshold_record, ledger_entry, promotion_decision = _threshold_record(
            tranche_name,
            spec,
            points,
            candidate,
            replay_agreement=replay_ok,
            axis_isolation=axis_isolation,
            tranche_event_consistency=tranche_event_consistency,
            signal_monotonicity=_signal_monotonicity(points, spec),
            previous_record=(previous_thresholds or {}).get(spec.name),
        )
        thresholds[spec.name] = threshold_record
        threshold_ledger.append(ledger_entry)
        promotion_decisions.append(promotion_decision)

    return {
        "analysis_contract_version": 2,
        "tranche_name": tranche_name,
        "scope": "local_tranche",
        "epistemic_note": EPISTEMIC_NOTE,
        "event_consistency": tranche_event_consistency,
        "axis_isolation": axis_isolation,
        "thresholds": thresholds,
        "threshold_ledger": threshold_ledger,
        "promotion_decisions": promotion_decisions,
    }


def _aggregate_span(values: Sequence[float]) -> tuple[float, float, float]:
    mean_value = fmean(values)
    return mean_value, min(values), max(values)


def _status_rank(status: str) -> int:
    ranking = {
        ThresholdEvidenceStatus.VALIDATED.value: 0,
        ThresholdEvidenceStatus.CROSS_RUN_STABLE.value: 1,
        ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value: 2,
        ThresholdEvidenceStatus.PROXY.value: 3,
        ThresholdEvidenceStatus.INSUFFICIENT_DATA.value: 4,
    }
    return ranking.get(status, 99)


def _contradiction_record(
    spec: ThresholdSpec,
    contradiction_type: str,
    affected_tranches: Sequence[str],
    *,
    severity: str,
    message: str,
    threshold_ids_involved: Sequence[str],
    **details: object,
) -> dict[str, object]:
    return {
        "threshold": spec.name,
        "symbol": spec.symbol,
        "contradiction_type": contradiction_type,
        "contradiction_severity": severity,
        "threshold_ids_involved": list(threshold_ids_involved),
        "affected_tranches": list(affected_tranches),
        "blocking": severity == CONTRADICTION_SEVERITY_BLOCKING,
        "message": message,
        **details,
    }


def _cross_tranche_record(
    spec: ThresholdSpec,
    local_records: list[tuple[str, dict[str, object]]],
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    numeric_records = [(name, record) for name, record in local_records if isinstance(record.get("estimate"), (int, float))]
    promotable_records = [(name, record) for name, record in numeric_records if bool(record.get("promotion_state", {}).get("promoted"))]
    findings: list[dict[str, object]] = []
    contradictions: list[dict[str, object]] = []

    normalization_basis = _derive_cross_tranche_basis(spec, promotable_records or numeric_records)
    global_threshold_id = _threshold_id(spec, scope="global")
    if not numeric_records:
        governance_outcome = PromotionGovernanceOutcome.ALLOW
        record = {
            "threshold_id": global_threshold_id,
            "symbol": spec.symbol,
            "legacy_name": spec.name,
            "proxy": spec.proxy_key,
            "target_mechanism": spec.target_mechanism,
            "estimate": None,
            "normalized_threshold_value": None,
            "normalization_basis": dict(normalization_basis),
            "normalization_basis_origin": normalization_basis["origin"],
            "normalization_basis_value": normalization_basis["value"],
            "normalization_basis_confidence": normalization_basis["confidence"],
            "status": ThresholdEvidenceStatus.INSUFFICIENT_DATA.value,
            "estimate_scope": "global_candidate",
            "epistemic_note": EPISTEMIC_NOTE,
            "supporting_tranches": [],
            "promotion_blockers": [PROMOTION_LOW_CONFIDENCE],
            "threshold_promotion_decision": PROMOTION_LOW_CONFIDENCE,
            "replay_hash_provenance": {"threshold_replay_hash": None, "supporting_threshold_hashes": []},
            "promotion_governance_outcome": governance_outcome.value,
            "promotion_state": {
                "promoted": False,
                "decision": "insufficient_data",
                "threshold_promotion_decision": PROMOTION_LOW_CONFIDENCE,
                "promotion_governance_outcome": governance_outcome.value,
            },
            "contradictions": [],
        }
        decision = {
            "threshold": spec.name,
            "symbol": spec.symbol,
            "threshold_id": global_threshold_id,
            "scope": "cross_tranche",
            "accepted": False,
            "decision": "insufficient_data",
            "threshold_promotion_decision": PROMOTION_LOW_CONFIDENCE,
            "reason": "No tranche produced a numeric threshold estimate.",
            "supporting_tranches": [],
            "promotion_blockers": [PROMOTION_LOW_CONFIDENCE],
            "promotion_governance_outcome": governance_outcome.value,
        }
        return record, findings, contradictions, decision

    estimates = [float(record["estimate"]) for _name, record in numeric_records]
    mean_estimate, min_estimate, max_estimate = _aggregate_span(estimates)
    normalized_estimates = [_normalize_with_basis(float(record["estimate"]), normalization_basis) for _name, record in numeric_records]
    normalized_estimates = [value for value in normalized_estimates if value is not None]
    if normalized_estimates:
        mean_normalized, min_normalized, max_normalized = _aggregate_span(normalized_estimates)
    else:
        mean_normalized = None
        min_normalized = None
        max_normalized = None
    relative_spread = (max_estimate - min_estimate) / max(abs(mean_estimate), 1.0)
    normalized_relative_spread = (
        (max_normalized - min_normalized) / max(abs(mean_normalized), 1.0)
        if isinstance(mean_normalized, (int, float)) and isinstance(min_normalized, (int, float)) and isinstance(max_normalized, (int, float))
        else relative_spread
    )
    threshold_hashes = [str(record["replay_hash_provenance"]["threshold_replay_hash"]) for _name, record in promotable_records if record.get("replay_hash_provenance", {}).get("threshold_replay_hash") is not None]
    confidence_values = [float(record.get("confidence", 0.0)) for _name, record in numeric_records if isinstance(record.get("confidence"), (int, float))]
    confidence = min(1.0, fmean(confidence_values) if confidence_values else 0.0)

    for tranche_name, record in numeric_records:
        lower = record.get("lower_bound")
        upper = record.get("upper_bound")
        if isinstance(lower, (int, float)) and isinstance(upper, (int, float)) and lower > upper:
            contradictions.append(
                _contradiction_record(
                    spec,
                    CONTRADICTION_CROSS_TRANCHE_CONFLICT,
                    [tranche_name],
                    severity=CONTRADICTION_SEVERITY_BLOCKING,
                    message=f"{tranche_name} reported lower_bound greater than upper_bound.",
                    threshold_ids_involved=[str(record.get("threshold_id", _threshold_id(spec, scope="local_tranche", tranche_name=tranche_name)))],
                    lower_bound=lower,
                    upper_bound=upper,
                )
            )

    if relative_spread > STABLE_RELATIVE_SPREAD:
        findings.append({"threshold": spec.name, "symbol": spec.symbol, "kind": "instability", "severity": "warning", "message": "Cross-tranche spread exceeds the stability tolerance for global promotion.", "supporting_tranches": [name for name, _record in numeric_records], "relative_spread": relative_spread, "normalized_relative_spread": normalized_relative_spread})
    if len(promotable_records) >= 2 and (relative_spread > STABLE_RELATIVE_SPREAD or normalized_relative_spread > STABLE_RELATIVE_SPREAD):
        contradictions.append(
            _contradiction_record(
                spec,
                CONTRADICTION_CROSS_TRANCHE_CONFLICT,
                [name for name, _record in promotable_records],
                severity=CONTRADICTION_SEVERITY_BLOCKING,
                message="Promotable tranche estimates exhibit excessive spread.",
                threshold_ids_involved=[str(record.get("threshold_id", _threshold_id(spec, scope="local_tranche", tranche_name=name))) for name, record in promotable_records],
                relative_spread=relative_spread,
                normalized_relative_spread=normalized_relative_spread,
            )
        )

    promotable_evidence = all(record.get("evidence_type") == ThresholdEvidenceType.PHASE_DERIVED.value for _name, record in promotable_records)
    if promotable_records and not promotable_evidence:
        contradictions.append(
            _contradiction_record(
                spec,
                CONTRADICTION_CROSS_TRANCHE_CONFLICT,
                [name for name, _record in promotable_records],
                severity=CONTRADICTION_SEVERITY_BLOCKING,
                message="Promotable tranche candidates mix incompatible phase classes.",
                threshold_ids_involved=[str(record.get("threshold_id", _threshold_id(spec, scope="local_tranche", tranche_name=name))) for name, record in promotable_records],
                evidence_types=[record.get("evidence_type") for _name, record in promotable_records],
            )
        )

    basis_origins = {str(record.get("normalization_basis_origin", NORMALIZATION_ORIGIN_UNKNOWN)) for _name, record in promotable_records}
    basis_values = [float(record["normalization_basis_value"]) for _name, record in promotable_records if isinstance(record.get("normalization_basis_value"), (int, float))]
    basis_spread = ((max(basis_values) - min(basis_values)) / max(abs(fmean(basis_values)), 1.0)) if len(basis_values) >= 2 else 0.0
    if promotable_records and (
        NORMALIZATION_ORIGIN_GOVERNED_FALLBACK in basis_origins
        or NORMALIZATION_ORIGIN_FALLBACK_SPARSE_SUPPORT in basis_origins
        or NORMALIZATION_ORIGIN_UNKNOWN in basis_origins
        or basis_spread > STABLE_RELATIVE_SPREAD
    ):
        contradictions.append(
            _contradiction_record(
                spec,
                CONTRADICTION_ENVELOPE_VIOLATION,
                [name for name, _record in promotable_records],
                severity=CONTRADICTION_SEVERITY_BLOCKING,
                message="Normalization provenance conflicts across promotable tranche candidates.",
                threshold_ids_involved=[str(record.get("threshold_id", _threshold_id(spec, scope="local_tranche", tranche_name=name))) for name, record in promotable_records],
                normalization_basis_origins=sorted(basis_origins),
                normalization_basis_values=basis_values,
            )
        )

    if any(value < 0.0 for value in estimates) and any(value > 0.0 for value in estimates):
        contradictions.append(
            _contradiction_record(
                spec,
                CONTRADICTION_CROSS_TRANCHE_CONFLICT,
                [name for name, _record in numeric_records],
                severity=CONTRADICTION_SEVERITY_BLOCKING,
                message="Cross-tranche estimates cross sign and cannot support a single promoted threshold.",
                threshold_ids_involved=[str(record.get("threshold_id", _threshold_id(spec, scope="local_tranche", tranche_name=name))) for name, record in numeric_records],
                estimates=estimates,
            )
        )

    status = ThresholdEvidenceStatus.PROXY
    decision = "retained_as_global_candidate"
    reason = "Only proxy-level or single-tranche evidence is available."
    threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
    promotion_blockers: list[str] = [PROMOTION_LOW_CONFIDENCE]
    promoted = False
    lower_bound = min_estimate
    upper_bound = max_estimate
    evidence_type = ThresholdEvidenceType.PROXY_ONLY
    governance_outcome = map_contradictions_to_outcome(contradictions)
    if len(promotable_records) >= 2:
        promotable_lowers = [value for _name, record in promotable_records if (value := _normalized_bound(spec, record, "lower_bound")) is not None]
        promotable_uppers = [value for _name, record in promotable_records if (value := _normalized_bound(spec, record, "upper_bound")) is not None]
        intersection_lower = max(promotable_lowers, default=min_estimate)
        intersection_upper = min(promotable_uppers, default=max_estimate)
        if intersection_lower > intersection_upper:
            contradictions.append(
                _contradiction_record(
                    spec,
                    CONTRADICTION_CROSS_TRANCHE_CONFLICT,
                    [name for name, _record in promotable_records],
                    severity=CONTRADICTION_SEVERITY_BLOCKING,
                    message="Promotable tranche bounds do not intersect.",
                    threshold_ids_involved=[str(record.get("threshold_id", _threshold_id(spec, scope="local_tranche", tranche_name=name))) for name, record in promotable_records],
                    intersection_lower=intersection_lower,
                    intersection_upper=intersection_upper,
                )
            )
            findings.append({"threshold": spec.name, "symbol": spec.symbol, "kind": "contradiction", "severity": "error", "contradiction_type": CONTRADICTION_CROSS_TRANCHE_CONFLICT, "message": "Promotable tranche bounds do not intersect, so no single global threshold can be promoted.", "supporting_tranches": [name for name, _record in promotable_records], "intersection_lower": intersection_lower, "intersection_upper": intersection_upper})
            governance_outcome = map_contradictions_to_outcome(contradictions)
            status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
            decision = "retained_as_contradictory_global_candidate"
            reason = "Promotable tranche bounds do not intersect."
            threshold_promotion_decision = PROMOTION_CONTRADICTION
            promotion_blockers = [PROMOTION_CONTRADICTION]
            evidence_type = ThresholdEvidenceType.MIXED
        else:
            basis_value = _normalization_basis_value(normalization_basis)
            lower_bound = intersection_lower * basis_value if basis_value is not None else min_estimate
            upper_bound = intersection_upper * basis_value if basis_value is not None else max_estimate
            evidence_type = ThresholdEvidenceType.PHASE_DERIVED if promotable_evidence else ThresholdEvidenceType.MIXED
            governance_outcome = map_contradictions_to_outcome(contradictions)
            if governance_outcome != PromotionGovernanceOutcome.ALLOW:
                status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
                decision = "retained_as_contradictory_global_candidate"
                reason = "Blocking contradiction classes prevent global promotion."
                threshold_promotion_decision = PROMOTION_CONTRADICTION
                promotion_blockers = [PROMOTION_CONTRADICTION]
            elif normalization_basis["origin"] in {
                NORMALIZATION_ORIGIN_GOVERNED_FALLBACK,
                NORMALIZATION_ORIGIN_FALLBACK_SPARSE_SUPPORT,
                NORMALIZATION_ORIGIN_UNKNOWN,
            }:
                status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
                decision = "retained_as_fallback_normalized_global_candidate"
                reason = "Global promotion is blocked because normalization fell back to governed constants."
                threshold_promotion_decision = PROMOTION_NORMALIZATION_FALLBACK
                promotion_blockers = [PROMOTION_NORMALIZATION_FALLBACK]
            elif confidence < MIN_GLOBAL_CONFIDENCE:
                status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
                decision = "retained_as_low_confidence_global_candidate"
                reason = "Cross-tranche confidence is below the global promotion threshold."
                threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
                promotion_blockers = [PROMOTION_LOW_CONFIDENCE]
            elif relative_spread > STABLE_RELATIVE_SPREAD or normalized_relative_spread > STABLE_RELATIVE_SPREAD:
                status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
                decision = "retained_as_unstable_global_candidate"
                reason = "Promotable tranche estimates exist, but their spread is too large for stable global promotion."
                threshold_promotion_decision = PROMOTION_HIGH_VARIANCE
                promotion_blockers = [PROMOTION_HIGH_VARIANCE]
            else:
                promoted = True
                threshold_promotion_decision = PROMOTION_PROMOTED
                promotion_blockers = []
                if len(promotable_records) >= 3 and relative_spread <= VALIDATED_RELATIVE_SPREAD:
                    status = ThresholdEvidenceStatus.VALIDATED
                    decision = "promoted_to_validated_global_threshold"
                    reason = "Multiple tranche-bounded estimates agreed tightly enough to satisfy the validated cross-run criterion."
                else:
                    status = ThresholdEvidenceStatus.CROSS_RUN_STABLE
                    decision = "promoted_to_cross_run_stable_threshold"
                    reason = "Multiple tranche-bounded estimates agreed under deterministic replay and cross-tranche spread controls."
    elif promotable_records:
        best_tranche, best_record = sorted(promotable_records, key=lambda item: (_status_rank(str(item[1].get("status"))), float(item[1].get("support_metrics", {}).get("normalized_bracket_width", 1.0) or 1.0), item[0]))[0]
        status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
        decision = "retained_as_single_tranche_global_candidate"
        reason = "Only one tranche produced promotable bounded evidence, so the estimate is not cross-run stable."
        threshold_promotion_decision = PROMOTION_LOW_CONFIDENCE
        promotion_blockers = [PROMOTION_LOW_CONFIDENCE]
        lower_bound = float(best_record["lower_bound"])
        upper_bound = float(best_record["upper_bound"])
        evidence_type = ThresholdEvidenceType.MIXED

    supporting_tranches = [name for name, _record in numeric_records]
    record = {
        "threshold_id": global_threshold_id,
        "symbol": spec.symbol,
        "legacy_name": spec.name,
        "proxy": spec.proxy_key,
        "target_mechanism": spec.target_mechanism,
        "estimate": mean_estimate,
        "normalized_threshold_value": mean_normalized,
        "normalization_basis": dict(normalization_basis),
        "normalization_basis_origin": normalization_basis["origin"],
        "normalization_basis_value": normalization_basis["value"],
        "normalization_basis_confidence": normalization_basis["confidence"],
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "status": status.value,
        "evidence_type": evidence_type.value,
        "confidence": confidence,
        "estimate_scope": "global_promoted_estimate" if promoted else "global_candidate",
        "epistemic_note": EPISTEMIC_NOTE,
        "supporting_tranches": supporting_tranches,
        "support_count": len(numeric_records),
        "supporting_local_statuses": {name: record["status"] for name, record in numeric_records},
        "promotion_blockers": promotion_blockers,
        "threshold_promotion_decision": threshold_promotion_decision,
        "replay_hash_provenance": {"threshold_replay_hash": _stable_hash({"threshold_name": spec.name, "supporting_threshold_hashes": threshold_hashes, "supporting_tranches": supporting_tranches, "lower_bound": lower_bound, "upper_bound": upper_bound, "threshold_promotion_decision": threshold_promotion_decision}), "supporting_threshold_hashes": threshold_hashes},
        "promotion_governance_outcome": governance_outcome.value,
        "promotion_state": {
            "promoted": promoted,
            "decision": decision,
            "threshold_promotion_decision": threshold_promotion_decision,
            "promotion_governance_outcome": governance_outcome.value,
        },
        "contradictions": contradictions,
    }
    promotion_decision = {
        "threshold": spec.name,
        "symbol": spec.symbol,
        "threshold_id": global_threshold_id,
        "scope": "cross_tranche",
        "accepted": promoted,
        "decision": decision,
        "threshold_promotion_decision": threshold_promotion_decision,
        "reason": reason,
        "supporting_tranches": supporting_tranches,
        "evidence_type": evidence_type.value,
        "confidence": confidence,
        "contradiction_count": len(contradictions),
        "promotion_blockers": promotion_blockers,
        "normalization_basis_origin": normalization_basis["origin"],
        "promotion_governance_outcome": governance_outcome.value,
    }
    return record, findings, contradictions, promotion_decision


def build_cross_tranche_thresholds(tranche_thresholds: dict[str, dict[str, object]]) -> dict[str, object]:
    aggregate: dict[str, dict[str, object]] = {}
    global_thresholds: dict[str, object] = {}
    threshold_ledger: list[dict[str, object]] = []
    promotion_decisions: list[dict[str, object]] = []
    consistency_findings: list[dict[str, object]] = []
    contradictions: list[dict[str, object]] = []

    for spec in THRESHOLD_SPECS:
        local_records = []
        for tranche_name, payload in sorted(tranche_thresholds.items()):
            thresholds = payload.get("thresholds", {})
            if not isinstance(thresholds, dict):
                continue
            record = thresholds.get(spec.name)
            if isinstance(record, dict):
                local_records.append((tranche_name, record))

        global_record, findings, spec_contradictions, promotion_decision = _cross_tranche_record(spec, local_records)
        global_thresholds[spec.name] = global_record
        threshold_ledger.append(
            {
                "threshold": spec.name,
                "symbol": spec.symbol,
                "threshold_id": global_record["threshold_id"],
                "estimate_scope": global_record["estimate_scope"],
                "status": global_record["status"],
                "evidence_type": global_record.get("evidence_type"),
                "estimate": global_record["estimate"],
                "normalized_threshold_value": global_record.get("normalized_threshold_value"),
                "normalization_basis_origin": global_record.get("normalization_basis_origin"),
                "normalization_basis_value": global_record.get("normalization_basis_value"),
                "normalization_basis_confidence": global_record.get("normalization_basis_confidence"),
                "lower_bound": global_record.get("lower_bound"),
                "upper_bound": global_record.get("upper_bound"),
                "supporting_tranches": global_record["supporting_tranches"],
                "promotion_decision": global_record["promotion_state"]["decision"],
                "threshold_promotion_decision": global_record.get("threshold_promotion_decision"),
                "promotion_blockers": global_record.get("promotion_blockers", []),
                "promotion_governance_outcome": global_record.get("promotion_governance_outcome"),
            }
        )
        promotion_decisions.append(promotion_decision)
        consistency_findings.extend(findings)
        contradictions.extend(spec_contradictions)

        numeric_values = [
            (tranche_name, float(record["estimate"]))
            for tranche_name, record in local_records
            if isinstance(record.get("estimate"), (int, float))
        ]
        if numeric_values:
            aggregate[spec.name] = {
                "mean_estimate": fmean(estimate for _tranche_name, estimate in numeric_values),
                "min_estimate": min(estimate for _tranche_name, estimate in numeric_values),
                "max_estimate": max(estimate for _tranche_name, estimate in numeric_values),
                "supporting_tranches": [tranche_name for tranche_name, _estimate in numeric_values],
            }

    return {
        "analysis_contract_version": 2,
        "scope": "cross_tranche",
        "epistemic_note": EPISTEMIC_NOTE,
        "tranches": tranche_thresholds,
        "global_thresholds": global_thresholds,
        "aggregate": aggregate,
        "threshold_ledger": threshold_ledger,
        "promotion_decisions": promotion_decisions,
        "consistency_findings": consistency_findings,
        "contradictions": contradictions,
    }


def build_admissibility_overlay(tranche_name: str, points: Sequence[PhasePoint]) -> dict[str, object]:
    axes = sorted({axis for point in points for axis in point.parameters})
    admissible_regions: list[dict[str, object]] = []
    inadmissible_regions: list[dict[str, object]] = []
    unresolved_regions: list[dict[str, object]] = []
    seen_signatures: set[tuple[object, ...]] = set()

    for axis in axes:
        grouped: dict[tuple[object, ...], list[PhasePoint]] = {}
        for point in points:
            if axis not in point.parameters:
                continue
            fixed_parameters = tuple(
                sorted(
                    (key, point.parameters[key])
                    for key in axes
                    if key != axis and key in point.parameters
                )
            )
            fixed_context = point.metrics.get("context", {})
            if not isinstance(fixed_context, dict):
                fixed_context = {}
            signature = (
                axis,
                fixed_parameters,
                tuple(sorted(fixed_context.items())),
            )
            grouped.setdefault(signature, []).append(point)

        for signature, grouped_points in sorted(grouped.items(), key=str):
            ordered = sorted(grouped_points, key=lambda item: (item.parameters[axis], item.slice_id))
            if not ordered:
                continue

            start = 0
            while start < len(ordered):
                state = ordered[start].admissibility_state
                end = start + 1
                while end < len(ordered) and ordered[end].admissibility_state == state:
                    end += 1
                segment = ordered[start:end]
                lower = min(point.parameters[axis] for point in segment)
                upper = max(point.parameters[axis] for point in segment)
                fixed_context = dict(signature[2])
                region_signature = (
                    axis,
                    state.value,
                    lower,
                    upper,
                    signature[1],
                    signature[2],
                )
                if region_signature not in seen_signatures:
                    seen_signatures.add(region_signature)
                    supporting_slice_hashes = [point.replay_hash for point in segment]
                    region_payload = {
                        "state": state.value,
                        "axis": axis,
                        "bounds": {axis: [lower, upper]},
                        "fixed_parameters": {name: [value, value] for name, value in signature[1]},
                        "fixed_context": fixed_context,
                        "support_count": len(segment),
                        "supporting_slice_ids": [point.slice_id for point in segment],
                        "reasons": sorted({reason for point in segment for reason in point.admissibility_reasons}),
                        "replay_hash": _stable_hash(
                            {
                                "state": state.value,
                                "axis": axis,
                                "supporting_slice_hashes": supporting_slice_hashes,
                                "fixed_parameters": signature[1],
                                "fixed_context": signature[2],
                            }
                        ),
                    }
                    if state == AdmissibilityState.ADMISSIBLE_CANDIDATE:
                        admissible_regions.append(region_payload)
                    elif state == AdmissibilityState.INADMISSIBLE_CANDIDATE:
                        inadmissible_regions.append(region_payload)
                    else:
                        unresolved_regions.append(region_payload)
                start = end

    if not unresolved_regions and (admissible_regions or inadmissible_regions):
        unresolved_regions.append(
            {
                "state": AdmissibilityState.UNRESOLVED.value,
                "axis": None,
                "bounds": {},
                "fixed_parameters": {},
                "fixed_context": {},
                "support_count": 0,
                "supporting_slice_ids": [],
                "reasons": ["no_intermediate_admissibility_region_observed"],
                "replay_hash": _stable_hash(
                    {
                        "tranche_name": tranche_name,
                        "reason": "no_intermediate_admissibility_region_observed",
                    }
                ),
            }
        )

    return {
        "analysis_contract_version": 2,
        "tranche_name": tranche_name,
        "epistemic_note": ADMISSIBILITY_NOTE,
        "admissible_region_candidates": admissible_regions,
        "inadmissible_region_candidates": inadmissible_regions,
        "unresolved_regions": unresolved_regions,
        "point_labels": [
            {
                "slice_id": point.slice_id,
                "state": point.admissibility_state.value,
                "reasons": list(point.admissibility_reasons),
                "replay_hash": point.replay_hash,
            }
            for point in sorted(points, key=lambda item: item.slice_id)
        ],
    }
