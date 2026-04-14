from __future__ import annotations

from avn.core.routing import compute_contingency_target, compute_route
from avn.core.state import CorridorCondition, NodeCondition, VehicleRuntime
from avn.core.vehicles import sort_vehicle_queue
from avn.sim.runtime import SimulationRuntime


def release_vehicles(
    state: SimulationRuntime,
    time_minute: int,
    step_events: list[dict[str, object]],
) -> None:
    for vehicle in state.vehicles.values():
        if vehicle.status != "scheduled" or vehicle.release_minute > time_minute:
            continue
        if not node_has_available_stand(state, vehicle.origin):
            step_events.append(
                {
                    "time_minute": time_minute,
                    "event_type": "vehicle_release_delayed",
                    "vehicle_id": vehicle.vehicle_id,
                    "node_id": vehicle.origin,
                }
            )
            continue
        step_events.append(
            {
                "time_minute": time_minute,
                "event_type": "vehicle_released",
                "vehicle_id": vehicle.vehicle_id,
                "origin": vehicle.origin,
                "destination": vehicle.destination,
            }
        )
        admit_vehicle_to_node(state, vehicle, vehicle.origin, time_minute, step_events)


def advance_enroute_vehicles(
    state: SimulationRuntime,
    time_minute: int,
    step_minutes: int,
    step_events: list[dict[str, object]],
) -> None:
    for vehicle in state.vehicles.values():
        if vehicle.status != "enroute" or vehicle.active_corridor is None:
            continue
        corridor = state.corridors[vehicle.active_corridor]
        speed = corridor.last_speed_kmh or corridor.definition.base_speed_kmh
        travel_distance = speed * (step_minutes / 60.0)
        vehicle.progress_km += travel_distance
        if vehicle.progress_km < corridor.definition.length_km:
            vehicle.reserve_energy = max(0.0, vehicle.reserve_energy - travel_distance * vehicle.cruise_energy_per_km)
            continue

        corridor.occupants = [item for item in corridor.occupants if item != vehicle.vehicle_id]
        vehicle.reserve_energy = max(
            0.0,
            vehicle.reserve_energy - corridor.definition.length_km * vehicle.cruise_energy_per_km,
        )
        vehicle.progress_km = 0.0
        vehicle.route_cursor += 1
        arrival_node = corridor.definition.destination
        state.nodes[arrival_node].reserved_arrivals = max(0, state.nodes[arrival_node].reserved_arrivals - 1)
        vehicle.current_node = arrival_node
        vehicle.active_corridor = None
        if arrival_node == vehicle.destination:
            vehicle.status = "completed"
            vehicle.completed_minute = time_minute
            vehicle.turnaround_complete_minute = None
            step_events.append(
                {
                    "time_minute": time_minute,
                    "event_type": "vehicle_completed",
                    "vehicle_id": vehicle.vehicle_id,
                    "destination": arrival_node,
                }
            )
            continue

        step_events.append(
            {
                "time_minute": time_minute,
                "event_type": "vehicle_arrived",
                "vehicle_id": vehicle.vehicle_id,
                "node_id": arrival_node,
            }
        )
        admit_vehicle_to_node(state, vehicle, arrival_node, time_minute, step_events)


def advance_turnarounds(
    state: SimulationRuntime,
    time_minute: int,
    step_events: list[dict[str, object]],
) -> None:
    for vehicle in state.vehicles.values():
        if vehicle.status != "turnaround" or vehicle.turnaround_complete_minute is None:
            continue
        if vehicle.turnaround_complete_minute > time_minute:
            continue
        vehicle.status = "queued"
        vehicle.turnaround_complete_minute = None
        if vehicle.current_node is not None and vehicle.vehicle_id not in state.nodes[vehicle.current_node].queue:
            state.nodes[vehicle.current_node].queue.append(vehicle.vehicle_id)
            step_events.append(
                {
                    "time_minute": time_minute,
                    "event_type": "vehicle_turnaround_complete",
                    "vehicle_id": vehicle.vehicle_id,
                    "node_id": vehicle.current_node,
                }
            )


def dispatch_queued_vehicles(
    state: SimulationRuntime,
    time_minute: int,
    available_by_node: dict[str, int],
    node_conditions: dict[str, NodeCondition],
    corridor_conditions: dict[str, CorridorCondition],
    step_events: list[dict[str, object]],
) -> None:
    for node_id, node in state.nodes.items():
        if available_by_node[node_id] <= 0 or not node.queue:
            continue

        ordered_queue = sort_vehicle_queue(node.queue, state.vehicles)
        remaining_queue: list[str] = []
        actual_dispatches = 0
        for vehicle_id in ordered_queue:
            vehicle = state.vehicles[vehicle_id]
            if actual_dispatches >= available_by_node[node_id]:
                remaining_queue.append(vehicle_id)
                continue

            route = compute_route(
                state.graph,
                state.corridors,
                state.nodes,
                node_conditions,
                corridor_conditions,
                node_id,
                vehicle.destination,
                state.policy,
            )
            if route is None:
                contingency = compute_contingency_target(state.graph, state.nodes, node_conditions, node_id)
                if contingency and contingency != vehicle.destination:
                    route = compute_route(
                        state.graph,
                        state.corridors,
                        state.nodes,
                        node_conditions,
                        corridor_conditions,
                        node_id,
                        contingency,
                        state.policy,
                    )
                    if route is not None:
                        vehicle.contingency_target = contingency
                        vehicle.destination = contingency
                        step_events.append(
                            {
                                "time_minute": time_minute,
                                "event_type": "vehicle_contingency_divert",
                                "vehicle_id": vehicle.vehicle_id,
                                "contingency_target": contingency,
                            }
                        )

            if route is None or len(route) < 2:
                vehicle.status = "holding"
                remaining_queue.append(vehicle_id)
                step_events.append(
                    {
                        "time_minute": time_minute,
                        "event_type": "vehicle_holding",
                        "vehicle_id": vehicle.vehicle_id,
                        "node_id": node_id,
                    }
                )
                continue

            if vehicle.route and vehicle.route[vehicle.route_cursor:] != route:
                vehicle.reroute_count += 1
                vehicle.last_reroute_minute = time_minute
                step_events.append(
                    {
                        "time_minute": time_minute,
                        "event_type": "vehicle_rerouted",
                        "vehicle_id": vehicle.vehicle_id,
                        "new_route": route,
                    }
                )

            edge = state.graph.get_edge_data(route[0], route[1])
            if edge is None:
                remaining_queue.append(vehicle_id)
                continue
            next_node_id = route[1]
            if not node_has_available_stand(state, next_node_id):
                vehicle.status = "holding"
                remaining_queue.append(vehicle_id)
                step_events.append(
                    {
                        "time_minute": time_minute,
                        "event_type": "vehicle_stand_hold",
                        "vehicle_id": vehicle.vehicle_id,
                        "node_id": node_id,
                        "blocked_node_id": next_node_id,
                    }
                )
                continue
            corridor = state.corridors[edge["corridor_id"]]
            corridor_condition = corridor_conditions.get(corridor.definition.corridor_id, CorridorCondition())
            if policy_blocks_dispatch(state, corridor_condition):
                vehicle.status = "holding"
                remaining_queue.append(vehicle_id)
                step_events.append(
                    {
                        "time_minute": time_minute,
                        "event_type": "vehicle_policy_hold",
                        "vehicle_id": vehicle.vehicle_id,
                        "policy_id": state.policy.policy_id,
                        "corridor_id": corridor.definition.corridor_id,
                        "weather_severity": corridor_condition.weather_severity,
                    }
                )
                continue
            if corridor.departure_credit < 1.0 or corridor.last_status == "closed" or corridor.last_speed_kmh <= 0.0:
                remaining_queue.append(vehicle_id)
                continue

            release_node_stand(state, node_id, vehicle.vehicle_id)
            corridor.departure_credit -= 1.0
            corridor.departures_this_step += 1
            corridor.occupants.append(vehicle.vehicle_id)
            state.nodes[next_node_id].reserved_arrivals += 1
            vehicle.status = "enroute"
            vehicle.route = route
            vehicle.route_cursor = 0
            vehicle.active_corridor = corridor.definition.corridor_id
            vehicle.progress_km = 0.0
            vehicle.turnaround_complete_minute = None
            actual_dispatches += 1
            step_events.append(
                {
                    "time_minute": time_minute,
                    "event_type": "vehicle_dispatched",
                    "vehicle_id": vehicle.vehicle_id,
                    "corridor_id": corridor.definition.corridor_id,
                    "route": route,
                }
            )

        node.queue = remaining_queue
        node.departures_this_step = actual_dispatches
        node.service_credit = max(0.0, node.service_credit - actual_dispatches)


def policy_blocks_dispatch(state: SimulationRuntime, corridor_condition: CorridorCondition) -> bool:
    limit = state.policy.dispatch_weather_limit
    return limit is not None and corridor_condition.weather_severity > limit


def node_has_available_stand(state: SimulationRuntime, node_id: str) -> bool:
    runtime = state.nodes[node_id]
    capacity = runtime.definition.stand_capacity
    if capacity is None:
        return True
    return len(runtime.stand_occupants) + runtime.reserved_arrivals < capacity


def release_node_stand(state: SimulationRuntime, node_id: str, vehicle_id: str) -> None:
    state.nodes[node_id].stand_occupants.discard(vehicle_id)


def admit_vehicle_to_node(
    state: SimulationRuntime,
    vehicle: VehicleRuntime,
    node_id: str,
    time_minute: int,
    step_events: list[dict[str, object]],
) -> None:
    runtime = state.nodes[node_id]
    runtime.stand_occupants.add(vehicle.vehicle_id)
    vehicle.current_node = node_id
    turnaround_minutes = runtime.definition.turnaround_minutes
    if turnaround_minutes > 0:
        vehicle.status = "turnaround"
        vehicle.turnaround_complete_minute = time_minute + turnaround_minutes
        step_events.append(
            {
                "time_minute": time_minute,
                "event_type": "vehicle_turnaround_started",
                "vehicle_id": vehicle.vehicle_id,
                "node_id": node_id,
                "ready_minute": vehicle.turnaround_complete_minute,
            }
        )
        return
    vehicle.status = "queued"
    vehicle.turnaround_complete_minute = None
    if vehicle.vehicle_id not in runtime.queue:
        runtime.queue.append(vehicle.vehicle_id)
