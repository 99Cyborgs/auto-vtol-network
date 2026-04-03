from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


TRUST_STATES = ("trusted", "degraded", "quarantined", "revoked", "unknown")


@dataclass(slots=True)
class NodeConfig:
    node_id: str
    node_type: str
    service_rate: float
    contingency_capacity: int
    occupancy: int = 0
    operational_state: str = "normal"
    supplier_id: str = "network_infra"
    trust_state: str = "trusted"
    contingency_landing_slots: int = 0
    contingency_turnaround_minutes: int = 120
    landing_priority: int = 0
    accepts_degraded_mode: bool = True


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
    supplier_id: str = "fleet_ops"
    trust_state: str = "trusted"
    intent_update_interval_minutes: int = 5
    min_contingency_margin: float = 10.0


@dataclass(slots=True)
class SupplierConfig:
    supplier_id: str
    trust_state: str = "trusted"
    supplier_type: str = "identity"


@dataclass(slots=True)
class DisturbanceScheduleEntry:
    start_minute: int
    weather_severity: float
    comms_reliability: float
    comms_latency_minutes: float = 1.0
    message_drop_probability: float = 0.0
    stale_after_minutes: float = 10.0
    reroute_delay_minutes: float = 0.0
    low_bandwidth_threshold_minutes: float = 15.0
    node_service_multiplier: float = 1.0


@dataclass(slots=True)
class TrustEventConfig:
    start_minute: int
    target_type: str
    target_id: str
    trigger: str
    resulting_state: str
    propagation_delay_minutes: int = 0
    note: str = ""


@dataclass(slots=True)
class InfrastructureEventConfig:
    start_minute: int
    end_minute: int
    target_type: str
    target_id: str
    state: str = "closed"
    service_multiplier: float = 1.0
    contingency_slots_delta: int = 0


@dataclass(slots=True)
class VehicleInjectionConfig:
    start_minute: int
    vehicle_id: str
    mission_class: str
    route: list[str]
    reserve_energy: float
    supplier_id: str = "unverified"
    trust_state: str = "unknown"
    status: str = "queued"
    note: str = "adversary_injection"


@dataclass(slots=True)
class ScenarioModifiers:
    demand_multiplier: float = 1.0
    weather_multiplier: float = 1.0
    corridor_capacity_multiplier: float = 1.0
    node_service_multiplier: float = 1.0
    separation_multiplier: float = 1.0
    reserve_consumption_multiplier: float = 1.0
    comms_reliability_multiplier: float = 1.0
    latency_multiplier: float = 1.0
    drop_probability_multiplier: float = 1.0
    contingency_capacity_multiplier: float = 1.0
    closure_probability: float = 0.0


@dataclass(slots=True)
class SafeRegionConfig:
    max_corridor_load_ratio: float = 1.25
    max_node_utilization_ratio: float = 1.10
    max_queue_ratio: float = 1.10
    max_stale_state_exposure_minutes: float = 60.0
    min_trusted_participant_fraction: float = 0.0
    max_unsafe_admissions: int = 9999
    min_reachable_landing_options: float = 0.0
    max_contingency_saturation_duration: float = 1_000_000.0
    max_operator_interventions_per_hour: float = 1_000_000.0


@dataclass(slots=True)
class PhysicsModelConfig:
    a_w: float = 0.9
    a_gamma: float = 1.1
    a_eta: float = 0.4
    a_chi: float = 0.8
    minimum_alpha: float = 0.2


@dataclass(slots=True)
class AdmissibilityConfig:
    rho_safe: float = 1.10
    queue_safe: float = 1.0
    gamma_min: float = 0.80
    chi_max: float = 1.0
    reserve_min: float = 0.0


@dataclass(slots=True)
class OperatorConfig:
    max_interventions_per_hour: float = 6.0
    override_latency_minutes: float = 5.0


@dataclass(slots=True)
class NodeState:
    node_id: str
    node_type: str
    queue_length: int
    service_rate: float
    base_service_rate: float
    contingency_capacity: int
    occupancy: int
    operational_state: str
    supplier_id: str
    trust_state: str
    base_contingency_landing_slots: int
    contingency_landing_slots: int
    contingency_turnaround_minutes: int
    landing_priority: int
    contingency_occupied: int = 0
    accepts_degraded_mode: bool = True


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
    is_closed: bool = False
    degraded_mode: bool = False


@dataclass(slots=True)
class VehicleState:
    id: str
    mission_class: str
    route: list[str]
    current_location: str
    reserve_energy: float
    status: str
    conformance_ok: bool
    supplier_id: str
    trust_state: str
    route_index: int = 0
    progress_km: float = 0.0
    active_corridor_id: str | None = None
    information_age_minutes: float = 0.0
    stale_state_exposure_minutes: float = 0.0
    fallback_mode: str = "normal"
    unsafe_admissions: int = 0
    quarantine_reason: str = ""
    delayed_until_minute: int = 0
    reroute_count: int = 0
    divert_attempt_count: int = 0
    divert_success_count: int = 0
    no_admissible_landing: bool = False
    lost_link_activations: int = 0
    operator_interventions: int = 0
    reachable_landing_options: int = 0
    min_contingency_margin: float = 10.0
    initial_planned_hops: int = 0


@dataclass(slots=True)
class SupplierState:
    supplier_id: str
    trust_state: str
    supplier_type: str
    last_event_trigger: str = "nominal"


@dataclass(slots=True)
class DisturbanceState:
    weather_severity: float
    comms_reliability: float
    comms_latency_minutes: float = 1.0
    message_drop_probability: float = 0.0
    stale_after_minutes: float = 10.0
    reroute_delay_minutes: float = 0.0
    low_bandwidth_threshold_minutes: float = 15.0
    node_service_multiplier: float = 1.0


@dataclass(slots=True)
class SimulationConfig:
    scenario_name: str
    description: str
    time_step_minutes: int
    duration_minutes: int
    output_root: Path
    seed: int = 0
    nodes: list[NodeConfig] = field(default_factory=list)
    corridors: list[CorridorConfig] = field(default_factory=list)
    vehicles: list[VehicleConfig] = field(default_factory=list)
    suppliers: list[SupplierConfig] = field(default_factory=list)
    disturbance_base: DisturbanceState = field(
        default_factory=lambda: DisturbanceState(weather_severity=0.0, comms_reliability=1.0)
    )
    disturbance_schedule: list[DisturbanceScheduleEntry] = field(default_factory=list)
    trust_events: list[TrustEventConfig] = field(default_factory=list)
    infrastructure_events: list[InfrastructureEventConfig] = field(default_factory=list)
    vehicle_injections: list[VehicleInjectionConfig] = field(default_factory=list)
    modifiers: ScenarioModifiers = field(default_factory=ScenarioModifiers)
    safe_region: SafeRegionConfig = field(default_factory=SafeRegionConfig)
    physics_model: PhysicsModelConfig = field(default_factory=PhysicsModelConfig)
    admissibility: AdmissibilityConfig = field(default_factory=AdmissibilityConfig)
    operator: OperatorConfig = field(default_factory=OperatorConfig)


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
    corridor_load_ratio: float
    node_utilization_ratio: float
    queue_ratio: float
    incomplete_missions: int
    divert_attempt_count: int
    divert_success_rate: float
    reserve_margin_mean: float
    reserve_margin_min: float
    unsafe_admission_count: int
    quarantine_count: int
    revocation_count: int
    trusted_active_fraction: float
    information_age_mean: float
    information_age_max: float
    stale_state_exposure_minutes: float
    delayed_reroute_count: int
    lost_link_fallback_activations: int
    reservation_invalidations: int
    reachable_landing_option_mean: float
    no_admissible_landing_events: int
    contingency_node_utilization: float
    contingency_saturation_duration: float
    safe_region_violation_count: int
    safe_region_primary_cause: str
    operator_intervention_count: int
    degraded_mode_dwell_time: float
    trust_induced_throughput_loss: float
    rho_e: float = 0.0
    q_e: float = 0.0
    lambda_e: float = 0.0
    c_e: float = 0.0
    s_e: float = 0.0
    alpha_e: float = 1.0
    gamma_e: float = 1.0
    eta_e: float = 1.0
    chi_e: float = 0.0
    admissibility_status: str = "inside_A"
