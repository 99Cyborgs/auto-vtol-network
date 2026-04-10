from __future__ import annotations

from dataclasses import asdict

from avn.core.alerts import build_alerts
from avn.core.corridors import update_corridor_runtime
from avn.core.graph import build_graph
from avn.core.metrics import build_metrics
from avn.core.nodes import accrue_service_credit
from avn.core.routing import compute_contingency_target, compute_route
from avn.core.state import (
    CorridorCondition,
    CorridorRuntime,
    CorridorSnapshot,
    NodeCondition,
    NodeRuntime,
    NodeSnapshot,
    ReplayBundle,
    ScenarioDefinition,
    StepSnapshot,
    VehicleRuntime,
    VehicleSnapshot,
)
from avn.core.vehicles import sort_vehicle_queue
from avn.sim.injectors import scenario_conditions


class Simulator:
    def __init__(self, scenario: ScenarioDefinition) -> None:
        self.scenario = scenario
        self.graph = build_graph(scenario.nodes, scenario.corridors)
        self.nodes = {node.node_id: NodeRuntime(definition=node) for node in scenario.nodes}
        self.corridors = {
            corridor.corridor_id: CorridorRuntime(definition=corridor) for corridor in scenario.corridors
        }
        self.vehicles = {
            vehicle.vehicle_id: VehicleRuntime.from_definition(vehicle) for vehicle in scenario.vehicles
        }
        self.event_log: list[dict[str, object]] = []

    def run(self) -> ReplayBundle:
        steps: list[StepSnapshot] = []
        step_minutes = self.scenario.time_step_minutes
        for time_minute in range(0, self.scenario.duration_minutes + step_minutes, step_minutes):
            node_conditions, corridor_conditions, disturbance_events = scenario_conditions(self.scenario, time_minute)
            step_events: list[dict[str, object]] = list(disturbance_events)
            self._release_vehicles(time_minute, step_events)
            self._advance_enroute_vehicles(time_minute, step_minutes, step_events)

            available_by_node: dict[str, int] = {}
            for node_id, runtime in self.nodes.items():
                available_by_node[node_id] = accrue_service_credit(
                    runtime,
                    node_conditions.get(node_id, NodeCondition()),
                    step_minutes,
                )

            for corridor_id, runtime in self.corridors.items():
                update_corridor_runtime(
                    runtime,
                    corridor_conditions.get(corridor_id, CorridorCondition()),
                    step_minutes,
                )

            self._dispatch_queued_vehicles(
                time_minute,
                available_by_node,
                node_conditions,
                corridor_conditions,
                step_events,
            )

            alerts = build_alerts(
                time_minute,
                self.nodes,
                node_conditions,
                self.corridors,
                self.vehicles,
                self.scenario.alert_thresholds,
            )
            metrics = build_metrics(self.nodes, node_conditions, self.corridors, self.vehicles)
            step = StepSnapshot(
                time_minute=time_minute,
                nodes=self._node_snapshots(node_conditions, available_by_node),
                corridors=self._corridor_snapshots(),
                vehicles=self._vehicle_snapshots(),
                metrics=metrics,
                alerts=alerts,
                events=step_events,
            )
            steps.append(step)
            self.event_log.extend(step_events)
            self.event_log.extend(asdict(alert) for alert in alerts)

        return ReplayBundle(
            scenario_id=self.scenario.scenario_id,
            name=self.scenario.name,
            description=self.scenario.description,
            seed=self.scenario.seed,
            duration_minutes=self.scenario.duration_minutes,
            time_step_minutes=self.scenario.time_step_minutes,
            summary=self._build_summary(steps),
            steps=steps,
            event_log=self.event_log,
        )

    def _release_vehicles(self, time_minute: int, step_events: list[dict[str, object]]) -> None:
        for vehicle in self.vehicles.values():
            if vehicle.status != "scheduled" or vehicle.release_minute > time_minute:
                continue
            vehicle.status = "queued"
            self.nodes[vehicle.origin].queue.append(vehicle.vehicle_id)
            step_events.append(
                {
                    "time_minute": time_minute,
                    "event_type": "vehicle_released",
                    "vehicle_id": vehicle.vehicle_id,
                    "origin": vehicle.origin,
                    "destination": vehicle.destination,
                }
            )

    def _advance_enroute_vehicles(
        self,
        time_minute: int,
        step_minutes: int,
        step_events: list[dict[str, object]],
    ) -> None:
        for vehicle in self.vehicles.values():
            if vehicle.status != "enroute" or vehicle.active_corridor is None:
                continue
            corridor = self.corridors[vehicle.active_corridor]
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
            vehicle.current_node = arrival_node
            vehicle.active_corridor = None
            if arrival_node == vehicle.destination:
                vehicle.status = "completed"
                vehicle.completed_minute = time_minute
                step_events.append(
                    {
                        "time_minute": time_minute,
                        "event_type": "vehicle_completed",
                        "vehicle_id": vehicle.vehicle_id,
                        "destination": arrival_node,
                    }
                )
            else:
                vehicle.status = "queued"
                self.nodes[arrival_node].queue.append(vehicle.vehicle_id)
                step_events.append(
                    {
                        "time_minute": time_minute,
                        "event_type": "vehicle_arrived",
                        "vehicle_id": vehicle.vehicle_id,
                        "node_id": arrival_node,
                    }
                )

    def _dispatch_queued_vehicles(
        self,
        time_minute: int,
        available_by_node: dict[str, int],
        node_conditions: dict[str, NodeCondition],
        corridor_conditions: dict[str, CorridorCondition],
        step_events: list[dict[str, object]],
    ) -> None:
        for node_id, node in self.nodes.items():
            if available_by_node[node_id] <= 0 or not node.queue:
                continue

            ordered_queue = sort_vehicle_queue(node.queue, self.vehicles)
            remaining_queue: list[str] = []
            actual_dispatches = 0
            for vehicle_id in ordered_queue:
                vehicle = self.vehicles[vehicle_id]
                if actual_dispatches >= available_by_node[node_id]:
                    remaining_queue.append(vehicle_id)
                    continue

                route = compute_route(
                    self.graph,
                    self.corridors,
                    self.nodes,
                    node_conditions,
                    corridor_conditions,
                    node_id,
                    vehicle.destination,
                )
                if route is None:
                    contingency = compute_contingency_target(self.graph, self.nodes, node_conditions, node_id)
                    if contingency and contingency != vehicle.destination:
                        route = compute_route(
                            self.graph,
                            self.corridors,
                            self.nodes,
                            node_conditions,
                            corridor_conditions,
                            node_id,
                            contingency,
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

                if vehicle.route and vehicle.route[vehicle.route_cursor :] != route:
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

                edge = self.graph.get_edge_data(route[0], route[1])
                if edge is None:
                    remaining_queue.append(vehicle_id)
                    continue
                corridor = self.corridors[edge["corridor_id"]]
                if corridor.departure_credit < 1.0 or corridor.last_status == "closed" or corridor.last_speed_kmh <= 0.0:
                    remaining_queue.append(vehicle_id)
                    continue

                corridor.departure_credit -= 1.0
                corridor.departures_this_step += 1
                corridor.occupants.append(vehicle.vehicle_id)
                vehicle.status = "enroute"
                vehicle.route = route
                vehicle.route_cursor = 0
                vehicle.active_corridor = corridor.definition.corridor_id
                vehicle.progress_km = 0.0
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

    def _node_snapshots(
        self,
        node_conditions: dict[str, NodeCondition],
        available_by_node: dict[str, int],
    ) -> list[NodeSnapshot]:
        snapshots: list[NodeSnapshot] = []
        for node_id, runtime in self.nodes.items():
            runtime.occupancy = len(runtime.queue) + sum(
                1
                for vehicle in self.vehicles.values()
                if vehicle.current_node == node_id and vehicle.status in {"queued", "holding"}
            )
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

    def _corridor_snapshots(self) -> list[CorridorSnapshot]:
        snapshots: list[CorridorSnapshot] = []
        for corridor in self.corridors.values():
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

    def _vehicle_snapshots(self) -> list[VehicleSnapshot]:
        snapshots: list[VehicleSnapshot] = []
        for vehicle in self.vehicles.values():
            x, y = self._vehicle_position(vehicle)
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

    def _vehicle_position(self, vehicle: VehicleRuntime) -> tuple[float, float]:
        if vehicle.active_corridor is None:
            node = self.nodes[vehicle.current_node].definition
            return node.x, node.y
        corridor = self.corridors[vehicle.active_corridor].definition
        origin = self.nodes[corridor.origin].definition
        destination = self.nodes[corridor.destination].definition
        progress = vehicle.progress_km / max(corridor.length_km, 1.0)
        x = origin.x + (destination.x - origin.x) * progress
        y = origin.y + (destination.y - origin.y) * progress
        return x, y

    def _build_summary(self, steps: list[StepSnapshot]) -> dict[str, object]:
        min_reserve_energy = min((vehicle.reserve_energy for vehicle in self.vehicles.values()), default=0.0)
        max_weather_severity = max((step.metrics["weather_severity"] for step in steps), default=0.0)
        return {
            "scenario_id": self.scenario.scenario_id,
            "completed_vehicles": steps[-1].metrics["completed_vehicles"],
            "active_vehicles": steps[-1].metrics["active_vehicles"],
            "holding_vehicles": steps[-1].metrics["holding_vehicles"],
            "max_queue_length": max(step.metrics["max_queue_length"] for step in steps),
            "max_corridor_load_ratio": max(step.metrics["max_corridor_load_ratio"] for step in steps),
            "min_reserve_energy": round(min_reserve_energy, 3),
            "max_weather_severity": round(max_weather_severity, 3),
            "reroute_count": sum(
                1 for event in self.event_log if event.get("event_type") == "vehicle_rerouted"
            ),
            "alerts_by_code": self._alert_counts(steps),
        }

    @staticmethod
    def _alert_counts(steps: list[StepSnapshot]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for step in steps:
            for alert in step.alerts:
                counts[alert.code] = counts.get(alert.code, 0) + 1
        return counts
