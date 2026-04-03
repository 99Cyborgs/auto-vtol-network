from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import math
from statistics import fmean
from typing import Sequence

from avn.phase_space.models import AdmissibilityState, PhasePoint, PhaseRegion, ThresholdEvidenceStatus


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
VALIDATED_RELATIVE_SPREAD = 0.10
STABLE_RELATIVE_SPREAD = 0.35

EPISTEMIC_NOTE = (
    "Threshold statuses express computational support from recorded tranche slices and deterministic replay. "
    "They do not claim physical certainty outside the observed sweep envelope."
)
ADMISSIBILITY_NOTE = (
    "Admissibility overlays are derived from recorded tranche metrics only and separate safety-relevant candidate regions "
    "from phase-transition discovery without introducing a new solver."
)


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


def _ordered_points(points: Sequence[PhasePoint], spec: ThresholdSpec) -> list[PhasePoint]:
    if spec.preferred_axis is not None and any(spec.preferred_axis in point.parameters for point in points):
        return sorted(
            [point for point in points if spec.preferred_axis in point.parameters],
            key=lambda point: (point.parameters[spec.preferred_axis], point.slice_id),
            reverse=spec.direction == "decreasing",
        )
    return sorted(
        [point for point in points if _metric_value(point, spec) is not None],
        key=lambda point: (_sort_proxy(point, spec), point.slice_id),
        reverse=spec.direction == "decreasing",
    )


def _phase_event_candidate(spec: ThresholdSpec, points: Sequence[PhasePoint]) -> dict[str, object] | None:
    ordered = _ordered_points(points, spec)
    if len(ordered) < 2:
        return None

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
                "source_axis": spec.preferred_axis or spec.proxy_key,
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
        "source_axis": spec.preferred_axis or spec.proxy_key,
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
            "source_axis": spec.preferred_axis or spec.proxy_key,
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
    del transition_regions
    candidate = _phase_event_candidate(spec, points)
    if candidate is None:
        candidate = _proxy_crossing_candidate(spec, points)
    return candidate


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


def _threshold_record(
    tranche_name: str,
    spec: ThresholdSpec,
    candidate: dict[str, object] | None,
    *,
    replay_agreement: bool,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    base_record = {
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
    }
    if candidate is None:
        threshold_record = {
            **base_record,
            "estimate": None,
            "status": ThresholdEvidenceStatus.INSUFFICIENT_DATA.value,
            "status_reason": "No replayable tranche evidence candidate was found.",
            "support_count": 0,
            "derivation_basis": None,
            "support_metrics": None,
            "replay_hash_provenance": {
                "threshold_replay_hash": None,
                "supporting_slice_hashes": [],
                "replay_agreement": False,
            },
            "promotion_state": {
                "promoted": False,
                "decision": "insufficient_data",
            },
        }
        ledger_entry = {
            "threshold": spec.name,
            "symbol": spec.symbol,
            "status": ThresholdEvidenceStatus.INSUFFICIENT_DATA.value,
            "estimate_scope": "local_tranche_estimate",
            "decision": "insufficient_data",
        }
        promotion_decision = {
            "threshold": spec.name,
            "symbol": spec.symbol,
            "scope": "local_tranche",
            "accepted": False,
            "decision": "insufficient_data",
            "reason": "No evidence candidate available.",
        }
        return threshold_record, ledger_entry, promotion_decision

    promotable = _meets_local_boundary_support(candidate) and replay_agreement
    if promotable:
        status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
        decision = "promoted_to_tranche_boundary"
        reason = "Transition evidence met adjacent support requirements and matched deterministic replay."
    else:
        status = ThresholdEvidenceStatus.PROXY
        if not _meets_local_boundary_support(candidate):
            decision = "retained_as_proxy_weak_support"
            reason = "Transition evidence did not satisfy minimum adjacent support or consistency requirements."
        elif not replay_agreement:
            decision = "retained_as_proxy_replay_mismatch"
            reason = "Transition evidence failed deterministic replay agreement."
        else:
            decision = "retained_as_proxy"
            reason = "Only proxy-level evidence is available."

    threshold_record = {
        **base_record,
        "estimate": candidate["estimate"],
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
        "support_metrics": dict(candidate["support_metrics"]),
        "replay_hash_provenance": {
            "threshold_replay_hash": candidate["replay_hash"],
            "supporting_slice_hashes": list(candidate["supporting_slice_hashes"]),
            "replay_agreement": replay_agreement,
        },
        "confidence_metric": candidate["support_metrics"]["detection_confidence"],
        "promotion_state": {
            "promoted": promotable,
            "decision": decision,
        },
    }
    ledger_entry = {
        "threshold": spec.name,
        "symbol": spec.symbol,
        "status": status.value,
        "estimate_scope": "local_tranche_estimate",
        "estimate": candidate["estimate"],
        "lower_bound": candidate["lower_bound"],
        "upper_bound": candidate["upper_bound"],
        "support_count": candidate["support_count"],
        "decision": decision,
        "replay_hash": candidate["replay_hash"],
        "supporting_slice_ids": list(candidate["supporting_slice_ids"]),
        "support_metrics": dict(candidate["support_metrics"]),
        "detection_method": spec.detection_method,
        "confidence_metric": candidate["support_metrics"]["detection_confidence"],
    }
    promotion_decision = {
        "threshold": spec.name,
        "symbol": spec.symbol,
        "scope": "local_tranche",
        "accepted": promotable,
        "decision": decision,
        "reason": reason,
        "support_count": candidate["support_count"],
        "replay_agreement": replay_agreement,
        "supporting_slice_ids": list(candidate["supporting_slice_ids"]),
        "confidence_metric": candidate["support_metrics"]["detection_confidence"],
    }
    return threshold_record, ledger_entry, promotion_decision


def build_threshold_estimates(
    tranche_name: str,
    points: Sequence[PhasePoint],
    transition_regions: Sequence[PhaseRegion],
    *,
    replay_points: Sequence[PhasePoint] | None = None,
    replay_transition_regions: Sequence[PhaseRegion] | None = None,
) -> dict[str, object]:
    thresholds: dict[str, object] = {}
    threshold_ledger: list[dict[str, object]] = []
    promotion_decisions: list[dict[str, object]] = []

    for spec in THRESHOLD_SPECS:
        candidate = _best_candidate(spec, points, transition_regions)
        replay_ok = _replay_agreement(spec, candidate, replay_points, replay_transition_regions)
        threshold_record, ledger_entry, promotion_decision = _threshold_record(
            tranche_name,
            spec,
            candidate,
            replay_agreement=replay_ok,
        )
        thresholds[spec.name] = threshold_record
        threshold_ledger.append(ledger_entry)
        promotion_decisions.append(promotion_decision)

    return {
        "analysis_contract_version": 2,
        "tranche_name": tranche_name,
        "scope": "local_tranche",
        "epistemic_note": EPISTEMIC_NOTE,
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


def _cross_tranche_record(
    spec: ThresholdSpec,
    local_records: list[tuple[str, dict[str, object]]],
) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    numeric_records = [
        (tranche_name, record)
        for tranche_name, record in local_records
        if isinstance(record.get("estimate"), (int, float))
    ]
    promotable_records = [
        (tranche_name, record)
        for tranche_name, record in numeric_records
        if bool(record.get("promotion_state", {}).get("promoted"))
    ]
    findings: list[dict[str, object]] = []

    for tranche_name, record in numeric_records:
        lower_bound = record.get("lower_bound")
        upper_bound = record.get("upper_bound")
        if isinstance(lower_bound, (int, float)) and isinstance(upper_bound, (int, float)) and lower_bound > upper_bound:
            findings.append(
                {
                    "threshold": spec.name,
                    "symbol": spec.symbol,
                    "kind": "impossible_ordering",
                    "severity": "error",
                    "message": f"{tranche_name} reported lower_bound greater than upper_bound.",
                    "supporting_tranches": [tranche_name],
                }
            )

    if not numeric_records:
        record = {
            "symbol": spec.symbol,
            "legacy_name": spec.name,
            "proxy": spec.proxy_key,
            "target_mechanism": spec.target_mechanism,
            "estimate": None,
            "status": ThresholdEvidenceStatus.INSUFFICIENT_DATA.value,
            "estimate_scope": "global_candidate",
            "epistemic_note": EPISTEMIC_NOTE,
            "supporting_tranches": [],
            "replay_hash_provenance": {
                "threshold_replay_hash": None,
                "supporting_threshold_hashes": [],
            },
            "promotion_state": {
                "promoted": False,
                "decision": "insufficient_data",
            },
        }
        decision = {
            "threshold": spec.name,
            "symbol": spec.symbol,
            "scope": "cross_tranche",
            "accepted": False,
            "decision": "insufficient_data",
            "reason": "No tranche produced a numeric threshold estimate.",
            "supporting_tranches": [],
        }
        return record, findings, decision

    estimates = [float(record["estimate"]) for _tranche_name, record in numeric_records]
    mean_estimate, min_estimate, max_estimate = _aggregate_span(estimates)
    relative_spread = (max_estimate - min_estimate) / max(abs(mean_estimate), 1.0)
    threshold_hashes = [
        str(record["replay_hash_provenance"]["threshold_replay_hash"])
        for _tranche_name, record in promotable_records
        if record.get("replay_hash_provenance", {}).get("threshold_replay_hash") is not None
    ]

    if relative_spread > STABLE_RELATIVE_SPREAD:
        findings.append(
            {
                "threshold": spec.name,
                "symbol": spec.symbol,
                "kind": "instability",
                "severity": "warning",
                "message": "Cross-tranche spread exceeds the stability tolerance for global promotion.",
                "supporting_tranches": [tranche_name for tranche_name, _record in numeric_records],
                "relative_spread": relative_spread,
            }
        )

    status = ThresholdEvidenceStatus.PROXY
    decision = "retained_as_global_candidate"
    reason = "Only proxy-level or single-tranche evidence is available."
    promoted = False
    lower_bound = min_estimate
    upper_bound = max_estimate

    if len(promotable_records) >= 2:
        promotable_lowers = [
            float(record["lower_bound"])
            for _tranche_name, record in promotable_records
            if isinstance(record.get("lower_bound"), (int, float))
        ]
        promotable_uppers = [
            float(record["upper_bound"])
            for _tranche_name, record in promotable_records
            if isinstance(record.get("upper_bound"), (int, float))
        ]
        intersection_lower = max(promotable_lowers, default=min_estimate)
        intersection_upper = min(promotable_uppers, default=max_estimate)
        if intersection_lower > intersection_upper:
            status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
            decision = "retained_as_contradictory_global_candidate"
            reason = "Promotable tranche estimates exist, but their bounds contradict one another and cannot be promoted globally."
            findings.append(
                {
                    "threshold": spec.name,
                    "symbol": spec.symbol,
                    "kind": "contradiction",
                    "severity": "error",
                    "message": "Promotable tranche bounds do not intersect, so no single global threshold can be promoted.",
                    "supporting_tranches": [tranche_name for tranche_name, _record in promotable_records],
                    "intersection_lower": intersection_lower,
                    "intersection_upper": intersection_upper,
                }
            )
        else:
            lower_bound = intersection_lower
            upper_bound = intersection_upper
            promoted = relative_spread <= STABLE_RELATIVE_SPREAD
            if promoted:
                if len(promotable_records) >= 3 and relative_spread <= VALIDATED_RELATIVE_SPREAD:
                    status = ThresholdEvidenceStatus.VALIDATED
                    decision = "promoted_to_validated_global_threshold"
                    reason = "Multiple tranche-bounded estimates agreed tightly enough to satisfy the validated cross-run criterion."
                else:
                    status = ThresholdEvidenceStatus.CROSS_RUN_STABLE
                    decision = "promoted_to_cross_run_stable_threshold"
                    reason = "Multiple tranche-bounded estimates agreed under deterministic replay and cross-tranche spread controls."
            else:
                status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
                decision = "retained_as_unstable_global_candidate"
                reason = "Promotable tranche estimates exist, but their spread is too large for stable global promotion."
    elif promotable_records:
        tranche_name, tranche_record = sorted(
            promotable_records,
            key=lambda item: (
                _status_rank(str(item[1].get("status"))),
                float(item[1].get("support_metrics", {}).get("normalized_bracket_width", 1.0)),
                item[0],
            ),
        )[0]
        status = ThresholdEvidenceStatus.BOUNDED_ESTIMATE
        decision = "retained_as_single_tranche_global_candidate"
        reason = "Only one tranche produced promotable bounded evidence, so the estimate is not cross-run stable."
        lower_bound = float(tranche_record["lower_bound"])
        upper_bound = float(tranche_record["upper_bound"])

    supporting_tranches = [tranche_name for tranche_name, _record in numeric_records]
    record = {
        "symbol": spec.symbol,
        "legacy_name": spec.name,
        "proxy": spec.proxy_key,
        "target_mechanism": spec.target_mechanism,
        "estimate": mean_estimate,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "status": status.value,
        "estimate_scope": "global_promoted_estimate" if promoted else "global_candidate",
        "epistemic_note": EPISTEMIC_NOTE,
        "supporting_tranches": supporting_tranches,
        "support_count": len(numeric_records),
        "supporting_local_statuses": {
            tranche_name: record["status"]
            for tranche_name, record in numeric_records
        },
        "replay_hash_provenance": {
            "threshold_replay_hash": _stable_hash(
                {
                    "threshold_name": spec.name,
                    "supporting_threshold_hashes": threshold_hashes,
                    "supporting_tranches": supporting_tranches,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                }
            ),
            "supporting_threshold_hashes": threshold_hashes,
        },
        "promotion_state": {
            "promoted": promoted,
            "decision": decision,
        },
    }
    promotion_decision = {
        "threshold": spec.name,
        "symbol": spec.symbol,
        "scope": "cross_tranche",
        "accepted": promoted,
        "decision": decision,
        "reason": reason,
        "supporting_tranches": supporting_tranches,
    }
    return record, findings, promotion_decision


def build_cross_tranche_thresholds(tranche_thresholds: dict[str, dict[str, object]]) -> dict[str, object]:
    aggregate: dict[str, dict[str, object]] = {}
    global_thresholds: dict[str, object] = {}
    threshold_ledger: list[dict[str, object]] = []
    promotion_decisions: list[dict[str, object]] = []
    consistency_findings: list[dict[str, object]] = []

    for spec in THRESHOLD_SPECS:
        local_records = []
        for tranche_name, payload in sorted(tranche_thresholds.items()):
            thresholds = payload.get("thresholds", {})
            if not isinstance(thresholds, dict):
                continue
            record = thresholds.get(spec.name)
            if isinstance(record, dict):
                local_records.append((tranche_name, record))

        global_record, findings, promotion_decision = _cross_tranche_record(spec, local_records)
        global_thresholds[spec.name] = global_record
        threshold_ledger.append(
            {
                "threshold": spec.name,
                "symbol": spec.symbol,
                "estimate_scope": global_record["estimate_scope"],
                "status": global_record["status"],
                "estimate": global_record["estimate"],
                "lower_bound": global_record.get("lower_bound"),
                "upper_bound": global_record.get("upper_bound"),
                "supporting_tranches": global_record["supporting_tranches"],
                "promotion_decision": global_record["promotion_state"]["decision"],
            }
        )
        promotion_decisions.append(promotion_decision)
        consistency_findings.extend(findings)

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
