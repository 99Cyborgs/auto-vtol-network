from __future__ import annotations

from avn.core.state import CorridorRuntime, NodeCondition, NodeRuntime, VehicleRuntime


def build_metrics(
    nodes: dict[str, NodeRuntime],
    node_conditions: dict[str, NodeCondition],
    corridors: dict[str, CorridorRuntime],
    vehicles: dict[str, VehicleRuntime],
) -> dict[str, float | int]:
    queue_lengths = [len(node.queue) for node in nodes.values()]
    corridor_load_ratios = []
    for corridor in corridors.values():
        nominal_slots = max(
            1.0,
            corridor.last_effective_capacity
            * max(corridor.definition.length_km / max(corridor.last_speed_kmh, 1.0), 0.1),
        )
        corridor_load_ratios.append(len(corridor.occupants) / nominal_slots)
    completed = sum(1 for vehicle in vehicles.values() if vehicle.status == "completed")
    active = sum(1 for vehicle in vehicles.values() if vehicle.status not in {"scheduled", "completed"})
    holding = sum(1 for vehicle in vehicles.values() if vehicle.status == "holding")
    reroutes = sum(vehicle.reroute_count for vehicle in vehicles.values())
    weather_peak = max((condition.weather_severity for condition in node_conditions.values()), default=0.0)
    weather_peak = max(weather_peak, max((corridor.last_weather_severity for corridor in corridors.values()), default=0.0))
    return {
        "completed_vehicles": completed,
        "active_vehicles": active,
        "holding_vehicles": holding,
        "max_queue_length": max(queue_lengths, default=0),
        "avg_queue_length": round(sum(queue_lengths) / max(len(queue_lengths), 1), 3),
        "max_corridor_load_ratio": round(max(corridor_load_ratios, default=0.0), 3),
        "reroute_count": reroutes,
        "weather_severity": round(weather_peak, 3),
    }
