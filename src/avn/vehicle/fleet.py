from __future__ import annotations

from dataclasses import dataclass

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
                route=config.route,
                current_location=config.route[0],
                reserve_energy=config.reserve_energy,
                status=config.status,
                conformance_ok=True,
            )
        )

    def next_node(self) -> str | None:
        next_index = self.state.route_index + 1
        if next_index >= len(self.state.route):
            return None
        return self.state.route[next_index]

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


def build_fleet(configs: list[VehicleConfig]) -> list[Vehicle]:
    return [Vehicle.from_config(config) for config in configs]

