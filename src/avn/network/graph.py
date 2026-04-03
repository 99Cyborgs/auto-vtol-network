from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx

from avn.core.models import CorridorConfig, CorridorState, DisturbanceState, NodeConfig
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

    @classmethod
    def from_config(cls, config: CorridorConfig) -> "Corridor":
        state = CorridorState(
            corridor_id=config.corridor_id,
            origin=config.origin,
            destination=config.destination,
            length=config.length,
            free_flow_speed=config.free_flow_speed,
            base_capacity=config.base_capacity,
            density=0.0,
            flow=0.0,
            required_separation=config.required_separation,
            effective_capacity=config.base_capacity,
            modified_speed=config.free_flow_speed,
            inflated_separation=config.required_separation,
        )
        return cls(state=state)

    def start_step(
        self,
        disturbance: DisturbanceState,
        time_step_minutes: float,
        *,
        accrue_capacity: bool = True,
    ) -> None:
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
        if accrue_capacity:
            self.departure_credit += self.state.effective_capacity * (time_step_minutes / 60.0)
        self.departures_this_step = 0

    def can_accept_departure(self) -> bool:
        return self.departure_credit >= 1.0

    def record_departure(self, vehicle_id: str) -> None:
        self.departure_credit -= 1.0
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
    def from_config(cls, nodes: list[NodeConfig], corridors: list[CorridorConfig]) -> "VTOLNetwork":
        network = cls()
        for node_config in nodes:
            node = build_node(node_config)
            network.nodes[node.state.node_id] = node
            network.graph.add_node(node.state.node_id, node=node)

        for corridor_config in corridors:
            corridor = Corridor.from_config(corridor_config)
            network.corridors[corridor.state.corridor_id] = corridor
            network.graph.add_edge(
                corridor.state.origin,
                corridor.state.destination,
                corridor=corridor,
            )
        return network

    def get_node(self, node_id: str) -> VertiportNode:
        return self.nodes[node_id]

    def get_corridor(self, corridor_id: str) -> Corridor:
        return self.corridors[corridor_id]

    def corridor_between(self, origin: str, destination: str) -> Corridor | None:
        edge = self.graph.get_edge_data(origin, destination)
        if edge is None:
            return None
        return edge["corridor"]

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
