from __future__ import annotations

from math import floor

from avn.core.models import DisturbanceState


def disturbance_modified_speed(free_flow_speed: float, disturbance: DisturbanceState) -> float:
    weather_penalty = 1.0 - 0.45 * disturbance.weather_severity
    comms_penalty = 0.85 + 0.15 * disturbance.comms_reliability
    return max(0.25 * free_flow_speed, free_flow_speed * weather_penalty * comms_penalty)


def separation_inflation(required_separation: float, disturbance: DisturbanceState) -> float:
    multiplier = 1.0 + 0.6 * disturbance.weather_severity + 0.4 * (1.0 - disturbance.comms_reliability)
    return required_separation * multiplier


def effective_capacity_reduction(
    base_capacity: float,
    free_flow_speed: float,
    modified_speed: float,
    required_separation: float,
    inflated_separation: float,
    disturbance: DisturbanceState,
) -> float:
    speed_ratio = modified_speed / free_flow_speed if free_flow_speed else 0.0
    separation_ratio = required_separation / inflated_separation if inflated_separation else 0.0
    comms_factor = 0.8 + 0.2 * disturbance.comms_reliability
    return max(0.25, base_capacity * speed_ratio * separation_ratio * comms_factor)


def _operational_factor(operational_state: str) -> float:
    if operational_state == "normal":
        return 1.0
    if operational_state == "contingency":
        return 0.75
    if operational_state == "constrained":
        return 0.5
    if operational_state == "closed":
        return 0.0
    return 0.9


def step_node_queue(
    queue_length: int,
    service_rate: float,
    time_step_minutes: float,
    *,
    service_credit: float = 0.0,
    operational_state: str = "normal",
) -> tuple[int, int, float]:
    if queue_length < 0:
        raise ValueError("queue_length must be non-negative")
    if service_rate < 0:
        raise ValueError("service_rate must be non-negative")

    service_credit += service_rate * _operational_factor(operational_state) * (time_step_minutes / 60.0)
    served = min(queue_length, floor(service_credit))
    remaining = queue_length - served
    service_credit -= served
    return served, remaining, service_credit


def approximate_reserve_energy_drain(
    distance_km: float,
    time_step_minutes: float,
    weather_severity: float,
    *,
    mission_class: str = "passenger",
    status: str = "enroute",
    reserve_multiplier: float = 1.0,
) -> float:
    mission_factor = {
        "passenger": 1.0,
        "cargo": 1.15,
        "medevac": 1.25,
        "public_service": 1.10,
    }.get(mission_class, 1.05)

    if status == "enroute":
        base_drain = distance_km * 0.32 + time_step_minutes * 0.04
        weather_factor = 1.0 + 0.5 * weather_severity
    else:
        base_drain = time_step_minutes * 0.02
        weather_factor = 1.0 + 0.2 * weather_severity

    return base_drain * mission_factor * weather_factor * reserve_multiplier
