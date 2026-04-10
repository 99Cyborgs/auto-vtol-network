from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


PRIORITY_ORDER = {
    "ems": 0,
    "emergency": 0,
    "inspection": 1,
    "cargo": 2,
    "passenger": 3,
    "routine": 4,
}


@dataclass(slots=True)
class NodeDefinition:
    node_id: str
    label: str
    node_type: str
    x: float
    y: float
    service_rate_per_hour: float
    queue_alert_threshold: int
    emergency_capacity: int = 0


@dataclass(slots=True)
class CorridorDefinition:
    corridor_id: str
    origin: str
    destination: str
    length_km: float
    base_speed_kmh: float
    base_capacity_per_hour: float
    weather_exposure: float = 1.0


@dataclass(slots=True)
class VehicleDefinition:
    vehicle_id: str
    origin: str
    destination: str
    release_minute: int
    mission_class: str
    reserve_energy: float
    cruise_energy_per_km: float = 0.6


@dataclass(slots=True)
class DisturbanceDefinition:
    disturbance_id: str
    kind: str
    target_id: str
    start_minute: int
    end_minute: int
    weather_severity: float = 0.0
    status: str = "nominal"
    capacity_multiplier: float = 1.0
    service_multiplier: float = 1.0
    note: str = ""


@dataclass(slots=True)
class ScenarioDefinition:
    scenario_id: str
    name: str
    description: str
    seed: int
    duration_minutes: int
    time_step_minutes: int
    recommended: bool
    nodes: list[NodeDefinition]
    corridors: list[CorridorDefinition]
    vehicles: list[VehicleDefinition]
    disturbances: list[DisturbanceDefinition]
    output_root: Path
    alert_thresholds: dict[str, float | int] = field(default_factory=dict)


@dataclass(slots=True)
class NodeCondition:
    weather_severity: float = 0.0
    service_multiplier: float = 1.0
    status: str = "normal"
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CorridorCondition:
    weather_severity: float = 0.0
    capacity_multiplier: float = 1.0
    status: str = "open"
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class VehicleRuntime:
    vehicle_id: str
    mission_class: str
    priority_rank: int
    origin: str
    destination: str
    current_node: str
    release_minute: int
    reserve_energy: float
    cruise_energy_per_km: float
    status: str = "scheduled"
    route: list[str] = field(default_factory=list)
    route_cursor: int = 0
    active_corridor: str | None = None
    progress_km: float = 0.0
    reroute_count: int = 0
    contingency_target: str | None = None
    last_reroute_minute: int | None = None
    completed_minute: int | None = None

    @classmethod
    def from_definition(cls, definition: VehicleDefinition) -> "VehicleRuntime":
        return cls(
            vehicle_id=definition.vehicle_id,
            mission_class=definition.mission_class,
            priority_rank=PRIORITY_ORDER.get(definition.mission_class, 9),
            origin=definition.origin,
            destination=definition.destination,
            current_node=definition.origin,
            release_minute=definition.release_minute,
            reserve_energy=definition.reserve_energy,
            cruise_energy_per_km=definition.cruise_energy_per_km,
        )


@dataclass(slots=True)
class NodeRuntime:
    definition: NodeDefinition
    queue: list[str] = field(default_factory=list)
    service_credit: float = 0.0
    departures_this_step: int = 0
    occupancy: int = 0


@dataclass(slots=True)
class CorridorRuntime:
    definition: CorridorDefinition
    departure_credit: float = 0.0
    occupants: list[str] = field(default_factory=list)
    departures_this_step: int = 0
    last_effective_capacity: float = 0.0
    last_speed_kmh: float = 0.0
    last_status: str = "open"
    last_weather_severity: float = 0.0


@dataclass(slots=True)
class AlertRecord:
    time_minute: int
    severity: str
    code: str
    message: str
    entity_id: str | None = None


@dataclass(slots=True)
class NodeSnapshot:
    node_id: str
    label: str
    node_type: str
    x: float
    y: float
    queue_length: int
    occupancy: int
    service_rate_per_hour: float
    available_departures: int
    weather_severity: float
    status: str


@dataclass(slots=True)
class CorridorSnapshot:
    corridor_id: str
    origin: str
    destination: str
    length_km: float
    load: int
    load_ratio: float
    effective_capacity_per_hour: float
    speed_kmh: float
    weather_severity: float
    status: str


@dataclass(slots=True)
class VehicleSnapshot:
    vehicle_id: str
    mission_class: str
    status: str
    current_node: str | None
    active_corridor: str | None
    route: list[str]
    route_cursor: int
    progress_km: float
    reserve_energy: float
    reroute_count: int
    x: float
    y: float
    contingency_target: str | None = None


@dataclass(slots=True)
class StepSnapshot:
    time_minute: int
    nodes: list[NodeSnapshot]
    corridors: list[CorridorSnapshot]
    vehicles: list[VehicleSnapshot]
    metrics: dict[str, float | int]
    alerts: list[AlertRecord]
    events: list[dict[str, Any]]


@dataclass(slots=True)
class ReplayBundle:
    scenario_id: str
    name: str
    description: str
    seed: int
    duration_minutes: int
    time_step_minutes: int
    summary: dict[str, Any]
    steps: list[StepSnapshot]
    event_log: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
