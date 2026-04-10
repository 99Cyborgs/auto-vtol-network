from __future__ import annotations

from dataclasses import dataclass, replace
from math import floor

from avn.core.models import VehicleConfig, VehicleState


@dataclass
class Vehicle:
    state: VehicleState

    @classmethod
    def from_config(cls, config: VehicleConfig) -> "Vehicle":
        return cls(
            state=VehicleState(
                id=config.vehicle_id,
                mission_class=config.mission_class,
                route=list(config.route),
                current_location=config.route[0],
                reserve_energy=config.reserve_energy,
                status=config.status,
                conformance_ok=True,
                supplier_id=config.supplier_id,
                trust_state=config.trust_state,
                min_contingency_margin=config.min_contingency_margin,
                initial_planned_hops=max(0, len(config.route) - 1),
            )
        )

    def next_node(self) -> str | None:
        next_index = self.state.route_index + 1
        if next_index >= len(self.state.route):
            return None
        return self.state.route[next_index]

    def final_destination(self) -> str:
        return self.state.route[-1]

    def dispatch_to(self, corridor_id: str) -> None:
        self.state.status = "enroute"
        self.state.current_location = corridor_id
        self.state.active_corridor_id = corridor_id
        self.state.progress_km = 0.0

    def advance(self, distance_km: float) -> None:
        self.state.progress_km += distance_km

    def arrive(self, node_id: str) -> None:
        self.state.route_index += 1
        self.state.current_location = node_id
        self.state.active_corridor_id = None
        self.state.progress_km = 0.0
        self.state.status = "completed" if self.next_node() is None else "queued"

    def set_route(self, route: list[str]) -> None:
        self.state.route = route
        self.state.route_index = 0
        self.state.current_location = route[0]


def _expanded_configs(configs: list[VehicleConfig], demand_multiplier: float) -> list[VehicleConfig]:
    if demand_multiplier <= 1.0:
        return list(configs)

    whole_copies = max(1, floor(demand_multiplier))
    remainder = max(0.0, demand_multiplier - whole_copies)
    expanded: list[VehicleConfig] = []

    for copy_index in range(whole_copies):
        for config in configs:
            suffix = "" if copy_index == 0 else f"_D{copy_index + 1}"
            expanded.append(replace(config, vehicle_id=f"{config.vehicle_id}{suffix}"))

    extra_count = int(round(len(configs) * remainder))
    for index, config in enumerate(configs[:extra_count]):
        expanded.append(replace(config, vehicle_id=f"{config.vehicle_id}_X{index + 1}"))

    return expanded


def build_fleet(configs: list[VehicleConfig], demand_multiplier: float = 1.0) -> list[Vehicle]:
    return [Vehicle.from_config(config) for config in _expanded_configs(configs, demand_multiplier)]
