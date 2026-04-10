from __future__ import annotations

from avn.core.models import DisturbanceState, VehicleState


MISSION_FACTORS = {
    "cargo": 1.15,
    "medevac": 1.25,
    "public_service": 1.10,
    "passenger": 1.0,
}


def reserve_required_for_distance(
    distance_km: float,
    mission_class: str,
    weather_severity: float,
    reserve_multiplier: float,
    min_margin: float,
) -> float:
    mission_factor = MISSION_FACTORS.get(mission_class, 1.05)
    travel_drain = distance_km * 0.32 * mission_factor * (1.0 + 0.5 * weather_severity) * reserve_multiplier
    return travel_drain + min_margin


def compute_reachable_landing_options(
    network,
    vehicle: VehicleState,
    disturbance: DisturbanceState,
    reserve_multiplier: float,
) -> list[dict[str, float | str]]:
    options: list[dict[str, float | str]] = []
    for node in network.contingency_nodes():
        if node.state.operational_state == "closed":
            continue
        if node.state.contingency_occupied >= node.state.contingency_landing_slots:
            continue
        distance_km = network.distance_to_node(vehicle, node.state.node_id)
        if distance_km is None:
            continue
        reserve_required = reserve_required_for_distance(
            distance_km,
            vehicle.mission_class,
            disturbance.weather_severity,
            reserve_multiplier,
            vehicle.min_contingency_margin,
        )
        margin = vehicle.reserve_energy - reserve_required
        if margin >= 0.0:
            options.append(
                {
                    "node_id": node.state.node_id,
                    "distance_km": distance_km,
                    "reserve_margin": margin,
                }
            )
    options.sort(key=lambda option: (float(option["distance_km"]), -float(option["reserve_margin"])))
    return options
