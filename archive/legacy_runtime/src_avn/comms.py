from __future__ import annotations

import random

from avn.core.models import DisturbanceState, VehicleState


def update_information_age(
    vehicle: VehicleState,
    disturbance: DisturbanceState,
    time_step_minutes: int,
    rng: random.Random,
) -> bool:
    dropped = rng.random() < max(0.0, min(1.0, disturbance.message_drop_probability))
    if dropped:
        vehicle.information_age_minutes += time_step_minutes
    else:
        vehicle.information_age_minutes = disturbance.comms_latency_minutes
    return dropped


def is_state_stale(vehicle: VehicleState, disturbance: DisturbanceState) -> bool:
    return vehicle.information_age_minutes > disturbance.stale_after_minutes


def should_activate_lost_link(vehicle: VehicleState, disturbance: DisturbanceState) -> bool:
    return vehicle.information_age_minutes >= disturbance.low_bandwidth_threshold_minutes


def stale_exposure_increment(vehicle: VehicleState, disturbance: DisturbanceState, time_step_minutes: int) -> float:
    if is_state_stale(vehicle, disturbance):
        vehicle.stale_state_exposure_minutes += time_step_minutes
        return float(time_step_minutes)
    return 0.0
