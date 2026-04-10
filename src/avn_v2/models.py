from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


def as_payload(model: Any) -> Any:
    if hasattr(model, "to_dict"):
        return model.to_dict()
    if isinstance(model, list):
        return [as_payload(item) for item in model]
    if isinstance(model, tuple):
        return [as_payload(item) for item in model]
    if isinstance(model, dict):
        return {str(key): as_payload(value) for key, value in sorted(model.items(), key=lambda item: str(item[0]))}
    return model


@dataclass(slots=True, frozen=True)
class NodeRecord:
    node_id: str
    node_type: str
    turnaround_minutes: int
    service_rate_per_hour: float
    queue_capacity: int
    contingency_slots: int
    trust_state: str = "trusted"


@dataclass(slots=True, frozen=True)
class CorridorRecord:
    corridor_id: str
    origin: str
    destination: str
    length_km: float
    travel_minutes: float
    capacity_per_hour: float
    energy_cost: float
    reservation_window_minutes: int = 15


@dataclass(slots=True, frozen=True)
class VehicleRecord:
    vehicle_id: str
    home_node: str
    vehicle_class: str
    energy_capacity: float
    reserve_energy: float
    trust_state: str = "trusted"
    operator_required: bool = False


@dataclass(slots=True, frozen=True)
class DemandRequestRecord:
    request_id: str
    release_minute: int
    origin: str
    destination: str
    priority: str
    required_vehicle_class: str
    max_delay_minutes: int


@dataclass(slots=True, frozen=True)
class DisruptionEvent:
    event_id: str
    start_minute: int
    end_minute: int
    target_type: str
    target_id: str
    effect_type: str
    value: float | str
    note: str = ""

    def active_at(self, minute: int) -> bool:
        return self.start_minute <= minute < self.end_minute


@dataclass(slots=True, frozen=True)
class DispatchPolicy:
    max_wait_minutes: int
    max_reroutes: int
    degraded_dispatch_enabled: bool
    operator_delay_minutes: int


@dataclass(slots=True, frozen=True)
class ReservationPolicy:
    lookahead_minutes: int
    reservation_horizon_hops: int
    conflict_tolerance: int


@dataclass(slots=True, frozen=True)
class ContingencyPolicy:
    min_energy_reserve: float
    reroute_buffer_minutes: int
    diversion_limit: int


@dataclass(slots=True, frozen=True)
class CalibrationConfig:
    bundle: Path | None = None
    enabled: bool = False
    travel_time_multiplier: float = 1.0
    energy_cost_multiplier: float = 1.0
    service_rate_multiplier: float = 1.0
    reservation_capacity_multiplier: float = 1.0


@dataclass(slots=True, frozen=True)
class OutputConfig:
    root: Path
    artifact_prefix: str = "avn_v2"


@dataclass(slots=True, frozen=True)
class ScenarioConfig:
    scenario_name: str
    description: str
    duration_minutes: int
    time_step_minutes: int
    nodes: tuple[NodeRecord, ...]
    corridors: tuple[CorridorRecord, ...]
    vehicles: tuple[VehicleRecord, ...]
    demand_requests: tuple[DemandRequestRecord, ...]
    disruptions: tuple[DisruptionEvent, ...]
    dispatch_policy: DispatchPolicy
    reservation_policy: ReservationPolicy
    contingency_policy: ContingencyPolicy
    calibration: CalibrationConfig
    outputs: OutputConfig
    scenario_path: Path


@dataclass(slots=True, frozen=True)
class ExperimentAxis:
    name: str
    values: tuple[int | float | str | bool, ...]


@dataclass(slots=True, frozen=True)
class GovernanceWaiver:
    category: str
    justification_id: str


@dataclass(slots=True, frozen=True)
class GovernancePolicy:
    minimum_confidence_tier: str = "medium"
    fatal_blocker_categories: tuple[str, ...] = ("operational_breach", "fit_failure", "evidence_insufficiency")
    advisory_blocker_categories: tuple[str, ...] = ()
    waivable_categories: tuple[str, ...] = ()
    waivers: tuple[GovernanceWaiver, ...] = ()


@dataclass(slots=True, frozen=True)
class ExperimentManifest:
    experiment_name: str
    base_scenario: Path
    output_root: Path
    adaptive_refinement: bool
    axes: tuple[ExperimentAxis, ...]
    promoted_metrics: tuple[str, ...]
    calibration_bundle: Path | None = None
    ingested_bundle_source: Path | None = None
    calibration_gate: str = "waived"
    use_calibrated_parameters: bool = True
    governance_policy: GovernancePolicy = GovernancePolicy()


@dataclass(slots=True, frozen=True)
class ExternalRawInput:
    name: str
    path: Path
    format: str


@dataclass(slots=True, frozen=True)
class ExternalFieldMapping:
    fields: dict[str, str]
    optional_fields: tuple[str, ...] = ()
    defaults: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExternalNormalization:
    metric_unit_mappings: dict[str, dict[str, Any]] = field(default_factory=dict)
    event_type_mapping: dict[str, str] = field(default_factory=dict)
    scope_mapping: dict[str, str] = field(default_factory=dict)
    time_multiplier: float = 1.0
    time_rounding: str = "nearest"
    missing_value_policy: str = "reject"


@dataclass(slots=True, frozen=True)
class ExternalSourceManifest:
    source_id: str
    source_type: str
    version: str
    scenario_path: Path
    ingestion_mode: str
    bundle_family: str
    raw_inputs: dict[str, ExternalRawInput]
    field_mapping: dict[str, ExternalFieldMapping]
    normalization: ExternalNormalization
    fit_space_overrides: dict[str, Any]
    coverage_requirements: CoverageRequirements
    confidence_policy: ConfidencePolicy
    quality_checks: QualityChecks
    provenance_defaults: dict[str, Any]
    output_root: Path | None
    manifest_path: Path


@dataclass(slots=True, frozen=True)
class ReferenceMetricTarget:
    metric_key: str
    reference_value: float
    tolerance: float
    unit: str
    weight: float = 1.0
    objective_group: str = "metric"


@dataclass(slots=True, frozen=True)
class ReferenceEventExpectation:
    expectation_id: str
    event_type: str
    expected_count: int
    count_tolerance: int
    first_minute: float | None = None
    timing_tolerance: float | None = None
    weight: float = 1.0
    objective_group: str = "event"


@dataclass(slots=True, frozen=True)
class ReferenceSeriesTarget:
    target_id: str
    scope: str
    entity_id: str
    metric_key: str
    reference_value: float
    tolerance: float
    weight: float = 1.0
    minute: int | None = None
    minute_start: int | None = None
    minute_end: int | None = None
    aggregation: str = "point"
    objective_group: str = "series"


@dataclass(slots=True, frozen=True)
class FitParameter:
    name: str
    scenario_key: str
    values: tuple[int | float, ...]
    description: str = ""


@dataclass(slots=True, frozen=True)
class CoverageRequirements:
    min_metric_targets: int = 0
    min_event_expectations: int = 0
    min_series_targets: int = 0
    required_scopes: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class ConfidencePolicy:
    high_confidence_score: float = 0.85
    medium_confidence_score: float = 0.6
    max_sensitivity_delta: float = 0.3
    max_failed_objectives: int = 0
    min_coverage_completeness: float = 1.0


@dataclass(slots=True, frozen=True)
class QualityChecks:
    require_reference_sources: bool = True
    require_bundle_hashes: bool = False
    require_monotonic_windows: bool = True
    require_scope_coverage: bool = True
    required_metric_columns: tuple[str, ...] = ("metric_key", "reference_value", "tolerance")
    required_event_columns: tuple[str, ...] = ("expectation_id", "event_type", "expected_count", "count_tolerance")
    required_series_columns: tuple[str, ...] = ("target_id", "scope", "entity_id", "metric_key", "reference_value", "tolerance")


@dataclass(slots=True, frozen=True)
class CalibrationGate:
    max_total_score: float
    require_metric_match: bool = True
    require_event_match: bool = True
    require_series_match: bool = False
    require_calibrated_parameters: bool = True
    require_evidence_coverage: bool = True


@dataclass(slots=True, frozen=True)
class ReferenceBundle:
    contract_version: int
    bundle_id: str
    name: str
    version: str
    scenario_path: Path
    metric_targets: tuple[ReferenceMetricTarget, ...]
    event_expectations: tuple[ReferenceEventExpectation, ...]
    series_targets: tuple[ReferenceSeriesTarget, ...]
    fit_parameters: tuple[FitParameter, ...]
    coverage_requirements: CoverageRequirements
    confidence_policy: ConfidencePolicy
    quality_checks: QualityChecks
    objective_group_weights: dict[str, float]
    gate: CalibrationGate
    reference_sources: tuple[str, ...]
    provenance: dict[str, Any]
    source_files: dict[str, Path]
    bundle_path: Path


@dataclass(slots=True)
class Reservation:
    reservation_id: str
    corridor_id: str
    vehicle_id: str
    request_id: str
    start_minute: int
    end_minute: int

    def overlaps(self, start_minute: int, end_minute: int) -> bool:
        return self.start_minute < end_minute and start_minute < self.end_minute


@dataclass(slots=True)
class RuntimeVehicle:
    vehicle_id: str
    vehicle_class: str
    home_node: str
    current_node: str
    energy_capacity: float
    energy_remaining: float
    reserve_energy: float
    trust_state: str
    operator_required: bool
    status: str = "idle"
    available_minute: int = 0
    assigned_request_id: str | None = None
    remaining_path: list[str] = field(default_factory=list)
    next_corridor_id: str | None = None
    next_arrival_minute: int | None = None
    reroute_count: int = 0
    reserved_corridors: list[str] = field(default_factory=list)
    route_history: list[str] = field(default_factory=list)
    contingency_node: str | None = None


@dataclass(slots=True)
class RuntimeRequest:
    request_id: str
    release_minute: int
    origin: str
    destination: str
    priority: str
    required_vehicle_class: str
    max_delay_minutes: int
    status: str = "pending"
    assigned_vehicle_id: str | None = None
    dispatch_minute: int | None = None
    completion_minute: int | None = None
    reroute_count: int = 0
    diversion_node: str | None = None
    failure_reason: str | None = None


@dataclass(slots=True)
class RuntimeNode:
    record: NodeRecord
    dispatch_credit: float = 0.0
    queue_length: int = 0
    active_contingency_occupancy: int = 0


@dataclass(slots=True)
class RuntimeCorridor:
    record: CorridorRecord
    reservations: list[Reservation] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ArtifactRecord:
    artifact_id: str
    artifact_type: str
    contract_version: int
    path: str
    sha256: str


@dataclass(slots=True)
class ScenarioRunResult:
    output_dir: Path
    events_path: Path
    backtest_trace_path: Path
    manifest_path: Path
    run_summary_path: Path
    threshold_ledger_path: Path
    hazard_ledger_path: Path
    promotion_decisions_path: Path
    contradictions_path: Path
    report_bundle_path: Path
    calibration_report_path: Path | None
    bundle_validation_path: Path | None
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return as_payload(asdict(self))
