from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean
from typing import Sequence

from avn.physics.admissibility import AdmissibilityResult
from avn.physics.disturbance_model import DisturbanceResponse
from avn.physics.state_mapping import PhysicsStateSample


@dataclass(slots=True, frozen=True)
class PhaseDetectionRecord:
    detected: bool
    threshold_value: float | None
    time_minute: int | None
    detection_method: str
    confidence: float
    details: dict[str, float | int | str | list[str]]

    def to_dict(self) -> dict[str, object]:
        return {
            "detected": self.detected,
            "threshold_value": self.threshold_value,
            "time_minute": self.time_minute,
            "detection_method": self.detection_method,
            "confidence": self.confidence,
            "details": dict(self.details),
        }


def _not_detected(method: str) -> PhaseDetectionRecord:
    return PhaseDetectionRecord(
        detected=False,
        threshold_value=None,
        time_minute=None,
        detection_method=method,
        confidence=0.0,
        details={},
    )


def _flow_breakdown(
    samples: Sequence[PhysicsStateSample],
    responses: Sequence[DisturbanceResponse],
) -> PhaseDetectionRecord:
    positive_support = 0
    for index in range(1, len(samples)):
        delta_rho = samples[index].rho_e - samples[index - 1].rho_e
        if abs(delta_rho) < 1e-9:
            continue
        slope = (samples[index].q_e - samples[index - 1].q_e) / delta_rho
        if slope > 0.0:
            positive_support += 1
            continue
        if delta_rho > 0.0 and positive_support > 0:
            confidence = min(1.0, 0.45 + 0.15 * positive_support + min(0.4, abs(slope) / 5.0))
            return PhaseDetectionRecord(
                detected=True,
                threshold_value=samples[index].rho_e,
                time_minute=samples[index].time_minute,
                detection_method="dq_drho_nonpositive",
                confidence=confidence,
                details={
                    "rho_c": samples[index].rho_e,
                    "q_c": samples[index].q_e,
                    "alpha_e": responses[index].alpha_e,
                    "slope": slope,
                },
            )
    return _not_detected("dq_drho_nonpositive")


def _queue_divergence(samples: Sequence[PhysicsStateSample]) -> PhaseDetectionRecord:
    window = 3
    if len(samples) < window:
        return _not_detected("queue_growth_regression")

    for end_index in range(window - 1, len(samples)):
        segment = samples[end_index - window + 1 : end_index + 1]
        deltas = [
            right.queue_e - left.queue_e
            for left, right in zip(segment, segment[1:])
        ]
        strictly_rising = all(delta > 0.0 for delta in deltas)
        slope = fmean(deltas) if deltas else 0.0
        if not strictly_rising or slope <= 0.0 or segment[-1].lambda_e <= 1.0:
            continue
        confidence = min(1.0, 0.4 + 0.2 * window + min(0.2, slope))
        return PhaseDetectionRecord(
            detected=True,
            threshold_value=segment[-1].lambda_e,
            time_minute=segment[-1].time_minute,
            detection_method="queue_growth_regression",
            confidence=confidence,
            details={
                "lambda_c": segment[-1].lambda_e,
                "queue_e": segment[-1].queue_e,
                "mean_queue_slope": slope,
            },
        )
    return _not_detected("queue_growth_regression")


def _admissibility_exit(admissibility_results: Sequence[AdmissibilityResult]) -> PhaseDetectionRecord:
    for index, result in enumerate(admissibility_results):
        if result.inside_A:
            continue
        confidence = min(1.0, 0.55 + 0.1 * len(result.violated_constraints))
        return PhaseDetectionRecord(
            detected=True,
            threshold_value=None,
            time_minute=index,
            detection_method="admissibility_boundary_crossing",
            confidence=confidence,
            details={
                "violated_constraints": list(result.violated_constraints),
            },
        )
    return _not_detected("admissibility_boundary_crossing")


def _contingency_saturation(
    samples: Sequence[PhysicsStateSample],
    admissibility_results: Sequence[AdmissibilityResult],
) -> PhaseDetectionRecord:
    for sample, admissibility in zip(samples, admissibility_results):
        contingency_margin = sample.kappa_i - sample.demand_diverts
        if contingency_margin >= 0.0 and "contingency_margin" not in admissibility.violated_constraints:
            continue
        confidence = min(1.0, 0.6 + min(0.4, abs(contingency_margin) / 2.0))
        return PhaseDetectionRecord(
            detected=True,
            threshold_value=contingency_margin,
            time_minute=sample.time_minute,
            detection_method="contingency_margin_crossing",
            confidence=confidence,
            details={
                "contingency_margin": contingency_margin,
                "kappa_i": sample.kappa_i,
                "demand_diverts": sample.demand_diverts,
                "violated_constraints": list(admissibility.violated_constraints),
            },
        )
    return _not_detected("contingency_margin_crossing")


def _comms_failure(
    samples: Sequence[PhysicsStateSample],
    admissibility_results: Sequence[AdmissibilityResult],
) -> PhaseDetectionRecord:
    for sample, admissibility in zip(samples, admissibility_results):
        if sample.gamma_e > 0.0 and ("gamma" in admissibility.violated_constraints or sample.gamma_e <= 0.80):
            confidence = min(1.0, 0.6 + (0.80 - min(sample.gamma_e, 0.80)))
            return PhaseDetectionRecord(
                detected=True,
                threshold_value=sample.gamma_e,
                time_minute=sample.time_minute,
                detection_method="gamma_boundary_crossing",
                confidence=confidence,
                details={
                    "gamma_c": sample.gamma_e,
                    "violated_constraints": list(admissibility.violated_constraints),
                },
            )
    return _not_detected("gamma_boundary_crossing")


def _weather_collapse(
    samples: Sequence[PhysicsStateSample],
    responses: Sequence[DisturbanceResponse],
    admissibility_results: Sequence[AdmissibilityResult],
) -> PhaseDetectionRecord:
    for sample, response, admissibility in zip(samples, responses, admissibility_results):
        weather_capacity_collapse = sample.w_e >= 0.4 and response.alpha_e <= 0.7
        if not weather_capacity_collapse:
            continue
        if admissibility.inside_A and response.c_e > 0.0:
            continue
        confidence = min(1.0, 0.5 + sample.w_e * 0.4 + (0.7 - min(response.alpha_e, 0.7)) * 0.3)
        return PhaseDetectionRecord(
            detected=True,
            threshold_value=sample.w_e,
            time_minute=sample.time_minute,
            detection_method="weather_capacity_collapse",
            confidence=confidence,
            details={
                "w_c": sample.w_e,
                "alpha_e": response.alpha_e,
                "violated_constraints": list(admissibility.violated_constraints),
            },
        )
    return _not_detected("weather_capacity_collapse")


def detect_phase_events(
    samples: Sequence[PhysicsStateSample],
    responses: Sequence[DisturbanceResponse],
    admissibility_results: Sequence[AdmissibilityResult],
) -> dict[str, PhaseDetectionRecord]:
    if not (len(samples) == len(responses) == len(admissibility_results)):
        raise ValueError("Phase detection inputs must have identical lengths")

    flow_breakdown = _flow_breakdown(samples, responses)
    queue_divergence = _queue_divergence(samples)
    admissibility_exit = _admissibility_exit(admissibility_results)
    contingency_saturation = _contingency_saturation(samples, admissibility_results)
    comms_failure = _comms_failure(samples, admissibility_results)
    weather_collapse = _weather_collapse(samples, responses, admissibility_results)
    return {
        "flow_breakdown": flow_breakdown,
        "queue_divergence": queue_divergence,
        "admissibility_exit": admissibility_exit,
        "contingency_saturation": contingency_saturation,
        "comms_failure": comms_failure,
        "weather_collapse": weather_collapse,
    }
