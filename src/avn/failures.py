from __future__ import annotations

from dataclasses import dataclass


CORRIDOR_CONGESTION = "CORRIDOR_CONGESTION"
NODE_SATURATION = "NODE_SATURATION"
REROUTE_CASCADE = "REROUTE_CASCADE"
WEATHER_COLLAPSE = "WEATHER_COLLAPSE"
COMMS_FAILURE = "COMMS_FAILURE"
TRUST_FAILURE = "TRUST_FAILURE"

FAILURE_MODES = (
    CORRIDOR_CONGESTION,
    NODE_SATURATION,
    REROUTE_CASCADE,
    WEATHER_COLLAPSE,
    COMMS_FAILURE,
    TRUST_FAILURE,
)

FAILURE_MODE_TO_LEGACY = {
    CORRIDOR_CONGESTION: "corridor_capacity_exceeded",
    NODE_SATURATION: "node_service_collapse",
    REROUTE_CASCADE: "stale_information_instability",
    WEATHER_COLLAPSE: "coupled_failure_indeterminate",
    COMMS_FAILURE: "stale_information_instability",
    TRUST_FAILURE: "trust_breakdown",
}


@dataclass(slots=True, frozen=True)
class FailureClassification:
    dominant_failure_mode: str
    legacy_mechanism: str
    confidence: float
    scores: dict[str, float]

    def to_dict(self) -> dict[str, object]:
        return {
            "dominant_failure_mode": self.dominant_failure_mode,
            "legacy_mechanism": self.legacy_mechanism,
            "confidence": self.confidence,
            "scores": dict(self.scores),
        }


def classify_failure_mode(summary: dict[str, object]) -> FailureClassification:
    phase_detection = summary.get("phase_detection", {})
    if not isinstance(phase_detection, dict):
        phase_detection = {}
    legacy_mechanism = str(summary.get("first_dominant_failure_mechanism", ""))

    def _phase_confidence(name: str) -> float:
        record = phase_detection.get(name, {})
        if isinstance(record, dict) and bool(record.get("detected")):
            return float(record.get("confidence", 0.0))
        return 0.0

    weather_score = (
        3.0 * _phase_confidence("weather_collapse")
        + float(summary.get("weather_severity_peak", 0.0))
        + max(0.0, 1.0 - float(summary.get("alpha_e_min", summary.get("physics_summary", {}).get("alpha_e_min", 1.0))))
    )
    comms_score = (
        2.0 * _phase_confidence("comms_failure")
        + float(summary.get("reservation_invalidations", 0))
        + float(summary.get("lost_link_fallback_activations", 0))
    )
    reroute_score = (
        float(summary.get("delayed_reroute_count", 0))
        + float(summary.get("lost_link_fallback_activations", 0))
        + _phase_confidence("admissibility_exit") * 0.5
    )
    trust_score = (
        2.5 * (1.0 if legacy_mechanism == "trust_breakdown" else 0.0)
        + float(summary.get("unsafe_admission_count", 0))
        + float(summary.get("quarantine_count", 0))
        + float(summary.get("revocation_count", 0))
        + float(summary.get("chi_e_peak", summary.get("physics_summary", {}).get("chi_e_peak", 0.0)))
        + _phase_confidence("admissibility_exit") * 0.25
    )
    node_score = (
        _phase_confidence("queue_divergence")
        + float(summary.get("peak_queue_ratio", 0.0))
        + float(summary.get("peak_node_utilization_ratio", 0.0))
    )
    corridor_score = (
        1.5 * _phase_confidence("flow_breakdown")
        + float(summary.get("peak_corridor_load_ratio", 0.0))
        + max(0.0, 1.0 - float(summary.get("mean_corridor_speed", 0.0)) / 100.0)
    )

    scores = {
        CORRIDOR_CONGESTION: corridor_score,
        NODE_SATURATION: node_score,
        REROUTE_CASCADE: reroute_score,
        WEATHER_COLLAPSE: weather_score,
        COMMS_FAILURE: comms_score,
        TRUST_FAILURE: trust_score,
    }
    dominant_mode = sorted(scores.items(), key=lambda item: (-item[1], item[0]))[0][0]
    total_score = sum(scores.values())
    confidence = scores[dominant_mode] / total_score if total_score > 0.0 else 0.0
    return FailureClassification(
        dominant_failure_mode=dominant_mode,
        legacy_mechanism=FAILURE_MODE_TO_LEGACY[dominant_mode],
        confidence=confidence,
        scores=scores,
    )


def classify_legacy_failure(first_violation_cause: str | None, summary: dict[str, object]) -> str:
    cause_to_classification = {
        "corridor_load_ratio": "corridor_capacity_exceeded",
        "node_utilization_ratio": "node_service_collapse",
        "queue_ratio": "node_service_collapse",
        "stale_state_exposure": "stale_information_instability",
        "trusted_participant_fraction": "trust_breakdown",
        "unsafe_admissions": "trust_breakdown",
        "reachable_landing_options": "contingency_unreachable",
        "contingency_saturation_duration": "contingency_unreachable",
        "operator_intervention_rate": "coupled_failure_indeterminate",
    }
    no_admissible = int(summary.get("no_admissible_landing_events", 0))
    unsafe_admissions = int(summary.get("unsafe_admission_count", 0))
    revocations = int(summary.get("revocation_count", 0))
    quarantines = int(summary.get("quarantine_count", 0))
    stale_exposure = float(summary.get("stale_state_exposure_minutes", 0.0))

    if first_violation_cause and first_violation_cause not in {
        "corridor_load_ratio",
        "node_utilization_ratio",
        "queue_ratio",
    }:
        return cause_to_classification.get(first_violation_cause, "coupled_failure_indeterminate")

    if no_admissible > 0:
        return "contingency_unreachable"
    if unsafe_admissions > 0 or revocations > 0 or quarantines > 0:
        return "trust_breakdown"
    if stale_exposure > 0.0:
        return "stale_information_instability"

    if first_violation_cause:
        return cause_to_classification.get(first_violation_cause, "coupled_failure_indeterminate")
    return "coupled_failure_indeterminate"
