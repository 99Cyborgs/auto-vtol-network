from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class NodeConfig:
    node_id: str
    node_type: str
    service_rate: float
    contingency_capacity: int
    occupancy: int = 0
    operational_state: str = "normal"


@dataclass(slots=True)
class CorridorConfig:
    corridor_id: str
    origin: str
    destination: str
    length: float
    free_flow_speed: float
    base_capacity: float
    required_separation: float


@dataclass(slots=True)
class VehicleConfig:
    vehicle_id: str
    mission_class: str
    route: list[str]
    reserve_energy: float
    status: str = "queued"


@dataclass(slots=True)
class DisturbanceScheduleEntry:
    start_minute: int
    weather_severity: float
    comms_reliability: float


@dataclass(slots=True)
class NodeState:
    node_id: str
    node_type: str
    queue_length: int
    service_rate: float
    contingency_capacity: int
    occupancy: int
    operational_state: str


@dataclass(slots=True)
class CorridorState:
    corridor_id: str
    origin: str
    destination: str
    length: float
    free_flow_speed: float
    base_capacity: float
    density: float
    flow: float
    required_separation: float
    effective_capacity: float
    modified_speed: float
    inflated_separation: float


@dataclass(slots=True)
class VehicleState:
    id: str
    mission_class: str
    route: list[str]
    current_location: str
    reserve_energy: float
    status: str
    conformance_ok: bool
    route_index: int = 0
    progress_km: float = 0.0
    active_corridor_id: str | None = None


@dataclass(slots=True)
class DisturbanceState:
    weather_severity: float
    comms_reliability: float


@dataclass(slots=True)
class SimulationConfig:
    scenario_name: str
    description: str
    time_step_minutes: int
    duration_minutes: int
    output_root: Path
    nodes: list[NodeConfig] = field(default_factory=list)
    corridors: list[CorridorConfig] = field(default_factory=list)
    vehicles: list[VehicleConfig] = field(default_factory=list)
    disturbance_base: DisturbanceState = field(
        default_factory=lambda: DisturbanceState(weather_severity=0.0, comms_reliability=1.0)
    )
    disturbance_schedule: list[DisturbanceScheduleEntry] = field(default_factory=list)


@dataclass(slots=True)
class MetricsSnapshot:
    time_minute: int
    completed_vehicles: int
    active_vehicles: int
    avg_queue_length: float
    total_corridor_flow: float
    mean_corridor_speed: float
    mean_effective_capacity: float
    mean_reserve_energy: float
    weather_severity: float
    comms_reliability: float

