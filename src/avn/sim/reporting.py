from __future__ import annotations

from avn.core.state import CorridorSnapshot, NodeCondition, NodeSnapshot, StepSnapshot, VehicleRuntime, VehicleSnapshot
from avn.sim.runtime import SimulationRuntime


def build_node_snapshots(
    state: SimulationRuntime,
    node_conditions: dict[str, NodeCondition],
    available_by_node: dict[str, int],
) -> list[NodeSnapshot]:
    snapshots: list[NodeSnapshot] = []
    for node_id, runtime in state.nodes.items():
        runtime.occupancy = len(runtime.stand_occupants)
        condition = node_conditions.get(node_id, NodeCondition())
        snapshots.append(
            NodeSnapshot(
                node_id=node_id,
                label=runtime.definition.label,
                node_type=runtime.definition.node_type,
                x=runtime.definition.x,
                y=runtime.definition.y,
                queue_length=len(runtime.queue),
                occupancy=runtime.occupancy,
                service_rate_per_hour=runtime.definition.service_rate_per_hour * condition.service_multiplier,
                available_departures=available_by_node.get(node_id, 0),
                weather_severity=condition.weather_severity,
                status=condition.status,
            )
        )
    return snapshots


def build_corridor_snapshots(state: SimulationRuntime) -> list[CorridorSnapshot]:
    snapshots: list[CorridorSnapshot] = []
    for corridor in state.corridors.values():
        occupancy_limit = max(
            1.0,
            corridor.last_effective_capacity
            * max(corridor.definition.length_km / max(corridor.last_speed_kmh, 1.0), 0.1),
        )
        snapshots.append(
            CorridorSnapshot(
                corridor_id=corridor.definition.corridor_id,
                origin=corridor.definition.origin,
                destination=corridor.definition.destination,
                length_km=corridor.definition.length_km,
                load=len(corridor.occupants),
                load_ratio=round(len(corridor.occupants) / occupancy_limit, 3),
                effective_capacity_per_hour=round(corridor.last_effective_capacity, 3),
                speed_kmh=round(corridor.last_speed_kmh, 3),
                weather_severity=round(corridor.last_weather_severity, 3),
                status=corridor.last_status,
            )
        )
    return snapshots


def build_vehicle_snapshots(state: SimulationRuntime) -> list[VehicleSnapshot]:
    snapshots: list[VehicleSnapshot] = []
    for vehicle in state.vehicles.values():
        x, y = build_vehicle_position(state, vehicle)
        snapshots.append(
            VehicleSnapshot(
                vehicle_id=vehicle.vehicle_id,
                mission_class=vehicle.mission_class,
                status=vehicle.status,
                current_node=vehicle.current_node if vehicle.status != "enroute" else None,
                active_corridor=vehicle.active_corridor,
                route=list(vehicle.route),
                route_cursor=vehicle.route_cursor,
                progress_km=round(vehicle.progress_km, 3),
                reserve_energy=round(vehicle.reserve_energy, 3),
                reroute_count=vehicle.reroute_count,
                x=round(x, 3),
                y=round(y, 3),
                contingency_target=vehicle.contingency_target,
            )
        )
    return snapshots


def build_vehicle_position(state: SimulationRuntime, vehicle: VehicleRuntime) -> tuple[float, float]:
    if vehicle.active_corridor is None:
        node = state.nodes[vehicle.current_node].definition
        return node.x, node.y
    corridor = state.corridors[vehicle.active_corridor].definition
    origin = state.nodes[corridor.origin].definition
    destination = state.nodes[corridor.destination].definition
    progress = vehicle.progress_km / max(corridor.length_km, 1.0)
    x = origin.x + (destination.x - origin.x) * progress
    y = origin.y + (destination.y - origin.y) * progress
    return x, y


def build_summary(state: SimulationRuntime, steps: list[StepSnapshot]) -> dict[str, object]:
    min_reserve_energy = min((vehicle.reserve_energy for vehicle in state.vehicles.values()), default=0.0)
    max_weather_severity = max((step.metrics["weather_severity"] for step in steps), default=0.0)
    return {
        "scenario_id": state.scenario.scenario_id,
        "completed_vehicles": steps[-1].metrics["completed_vehicles"],
        "active_vehicles": steps[-1].metrics["active_vehicles"],
        "holding_vehicles": steps[-1].metrics["holding_vehicles"],
        "max_queue_length": max(step.metrics["max_queue_length"] for step in steps),
        "max_corridor_load_ratio": max(step.metrics["max_corridor_load_ratio"] for step in steps),
        "min_reserve_energy": round(min_reserve_energy, 3),
        "max_weather_severity": round(max_weather_severity, 3),
        "reroute_count": sum(1 for event in state.event_log if event.get("event_type") == "vehicle_rerouted"),
        "alerts_by_code": alert_counts(steps),
    }


def alert_counts(steps: list[StepSnapshot]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for step in steps:
        for alert in step.alerts:
            counts[alert.code] = counts.get(alert.code, 0) + 1
    return counts
