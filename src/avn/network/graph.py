from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx

from avn.core.models import CorridorConfig, CorridorState, DisturbanceState, NodeConfig, ScenarioModifiers
from avn.nodes.vertiports import VertiportNode, build_node
from avn.physics.dynamics import (
    disturbance_modified_speed,
    effective_capacity_reduction,
    separation_inflation,
)


@dataclass
class Corridor:
    state: CorridorState
    departure_credit: float = 0.0
    vehicles_in_corridor: set[str] = field(default_factory=set)
    departures_this_step: int = 0
    capacity_multiplier: float = 1.0
    separation_multiplier: float = 1.0

    @classmethod
    def from_config(cls, config: CorridorConfig, modifiers: ScenarioModifiers) -> "Corridor":
        state = CorridorState(
            corridor_id=config.corridor_id,
            origin=config.origin,
            destination=config.destination,
            length=config.length,
            free_flow_speed=config.free_flow_speed,
            base_capacity=config.base_capacity * modifiers.corridor_capacity_multiplier,
            density=0.0,
            flow=0.0,
            required_separation=config.required_separation * modifiers.separation_multiplier,
            effective_capacity=config.base_capacity * modifiers.corridor_capacity_multiplier,
            modified_speed=config.free_flow_speed,
            inflated_separation=config.required_separation * modifiers.separation_multiplier,
        )
        return cls(
            state=state,
            capacity_multiplier=modifiers.corridor_capacity_multiplier,
            separation_multiplier=modifiers.separation_multiplier,
        )

    def start_step(
        self,
        disturbance: DisturbanceState,
        time_step_minutes: float,
        *,
        accrue_capacity: bool = True,
    ) -> None:
        if self.state.is_closed:
            self.state.modified_speed = 0.0
            self.state.effective_capacity = 0.0
            self.state.degraded_mode = True
            self.departures_this_step = 0
            return

        self.state.modified_speed = disturbance_modified_speed(self.state.free_flow_speed, disturbance)
        self.state.inflated_separation = separation_inflation(self.state.required_separation, disturbance)
        self.state.effective_capacity = effective_capacity_reduction(
            self.state.base_capacity,
            self.state.free_flow_speed,
            self.state.modified_speed,
            self.state.required_separation,
            self.state.inflated_separation,
            disturbance,
        )
        self.state.degraded_mode = (
            disturbance.weather_severity >= 0.60
            or disturbance.comms_reliability <= 0.85
            or disturbance.message_drop_probability >= 0.15
        )
        if accrue_capacity:
            self.departure_credit += self.state.effective_capacity * (time_step_minutes / 60.0)
        self.departures_this_step = 0

    def can_accept_departure(self) -> bool:
        return not self.state.is_closed and self.departure_credit >= 1.0

    def record_departure(self, vehicle_id: str) -> None:
        self.departure_credit = max(0.0, self.departure_credit - 1.0)
        self.departures_this_step += 1
        self.vehicles_in_corridor.add(vehicle_id)

    def record_exit(self, vehicle_id: str) -> None:
        self.vehicles_in_corridor.discard(vehicle_id)

    def finalize_step(self, time_step_minutes: float) -> None:
        step_hours = time_step_minutes / 60.0
        self.state.density = len(self.vehicles_in_corridor) / max(self.state.length, 1.0)
        self.state.flow = self.departures_this_step / step_hours if step_hours else 0.0


class VTOLNetwork:
    def __init__(self) -> None:
        self.graph = nx.DiGraph()
        self.nodes: dict[str, VertiportNode] = {}
        self.corridors: dict[str, Corridor] = {}

    @classmethod
    def from_config(
        cls,
        nodes: list[NodeConfig],
        corridors: list[CorridorConfig],
        modifiers: ScenarioModifiers,
    ) -> "VTOLNetwork":
        network = cls()
        for node_config in nodes:
            node = build_node(node_config)
            node.state.base_service_rate *= modifiers.node_service_multiplier
            node.state.service_rate = node.state.base_service_rate
            node.state.base_contingency_landing_slots = int(
                round(node.state.base_contingency_landing_slots * modifiers.contingency_capacity_multiplier)
            )
            node.state.contingency_landing_slots = node.state.base_contingency_landing_slots
            network.nodes[node.state.node_id] = node
            network.graph.add_node(node.state.node_id, node=node)

        for corridor_config in corridors:
            corridor = Corridor.from_config(corridor_config, modifiers)
            network.corridors[corridor.state.corridor_id] = corridor
            network.graph.add_edge(
                corridor.state.origin,
                corridor.state.destination,
                corridor=corridor,
                weight=corridor.state.length,
            )
        return network

    def get_node(self, node_id: str) -> VertiportNode:
        return self.nodes[node_id]

    def get_corridor(self, corridor_id: str) -> Corridor:
        return self.corridors[corridor_id]

    def contingency_nodes(self) -> list[VertiportNode]:
        return [node for node in self.nodes.values() if node.state.contingency_landing_slots > 0]

    def corridor_between(self, origin: str, destination: str) -> Corridor | None:
        edge = self.graph.get_edge_data(origin, destination)
        if edge is None:
            return None
        return edge["corridor"]

    def shortest_path(self, origin: str, destination: str, *, avoid_closed: bool = True) -> list[str] | None:
        graph = self._routing_graph(avoid_closed=avoid_closed)
        try:
            return nx.shortest_path(graph, origin, destination, weight="weight")
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def shortest_path_distance(self, origin: str, destination: str, *, avoid_closed: bool = True) -> float | None:
        graph = self._routing_graph(avoid_closed=avoid_closed)
        try:
            return float(nx.shortest_path_length(graph, origin, destination, weight="weight"))
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def distance_to_node(self, vehicle, node_id: str) -> float | None:
        if vehicle.current_location in self.nodes:
            return self.shortest_path_distance(vehicle.current_location, node_id)

        if vehicle.active_corridor_id is None:
            return None

        corridor = self.get_corridor(vehicle.active_corridor_id)
        remaining_distance = max(0.0, corridor.state.length - vehicle.progress_km)
        onward_distance = self.shortest_path_distance(corridor.state.destination, node_id)
        if onward_distance is None:
            return None
        return remaining_distance + onward_distance

    def set_node_closed(self, node_id: str, *, closed: bool) -> None:
        node = self.nodes[node_id]
        if closed:
            node.state.operational_state = "closed"
            node.state.service_rate = 0.0
        else:
            node.state.operational_state = "normal"
            node.state.service_rate = node.state.base_service_rate

    def set_corridor_closed(self, corridor_id: str, *, closed: bool) -> None:
        self.corridors[corridor_id].state.is_closed = closed

    def prepare_step(
        self,
        disturbance: DisturbanceState,
        time_step_minutes: float,
        *,
        accrue_capacity: bool = True,
    ) -> None:
        for corridor in self.corridors.values():
            corridor.start_step(disturbance, time_step_minutes, accrue_capacity=accrue_capacity)

    def finalize_step(self, time_step_minutes: float) -> None:
        for corridor in self.corridors.values():
            corridor.finalize_step(time_step_minutes)

    def _routing_graph(self, *, avoid_closed: bool) -> nx.DiGraph:
        if not avoid_closed:
            return self.graph

        graph = nx.DiGraph()
        for node_id, node in self.nodes.items():
            if node.state.operational_state == "closed":
                continue
            graph.add_node(node_id)

        for corridor in self.corridors.values():
            if corridor.state.is_closed:
                continue
            if corridor.state.origin not in graph or corridor.state.destination not in graph:
                continue
            graph.add_edge(
                corridor.state.origin,
                corridor.state.destination,
                corridor=corridor,
                weight=corridor.state.length,
            )
        return graph
