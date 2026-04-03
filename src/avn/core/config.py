from __future__ import annotations

import csv
import tomllib
from pathlib import Path

from avn.core.models import (
    AdmissibilityConfig,
    CorridorConfig,
    DisturbanceScheduleEntry,
    DisturbanceState,
    InfrastructureEventConfig,
    NodeConfig,
    OperatorConfig,
    PhysicsModelConfig,
    SafeRegionConfig,
    ScenarioModifiers,
    SimulationConfig,
    SupplierConfig,
    TrustEventConfig,
    VehicleConfig,
    VehicleInjectionConfig,
)


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _as_bool(value: str | bool | None, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _default_contingency_slots(node_type: str, contingency_capacity: int) -> int:
    lowered = node_type.strip().lower()
    if lowered == "emergency":
        return max(1, contingency_capacity // 2)
    if lowered == "hub":
        return 1
    return 0


def _load_nodes(path: Path) -> list[NodeConfig]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            NodeConfig(
                node_id=row["node_id"],
                node_type=row["node_type"],
                service_rate=float(row["service_rate"]),
                contingency_capacity=int(row["contingency_capacity"]),
                occupancy=int(row.get("occupancy", 0) or 0),
                operational_state=row.get("operational_state", "normal") or "normal",
                supplier_id=row.get("supplier_id", "network_infra") or "network_infra",
                trust_state=row.get("trust_state", "trusted") or "trusted",
                contingency_landing_slots=int(
                    row.get(
                        "contingency_landing_slots",
                        _default_contingency_slots(row["node_type"], int(row["contingency_capacity"])),
                    )
                    or _default_contingency_slots(row["node_type"], int(row["contingency_capacity"]))
                ),
                contingency_turnaround_minutes=int(row.get("contingency_turnaround_minutes", 120) or 120),
                landing_priority=int(row.get("landing_priority", 0) or 0),
                accepts_degraded_mode=_as_bool(row.get("accepts_degraded_mode"), True),
            )
            for row in reader
        ]


def _load_corridors(path: Path) -> list[CorridorConfig]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            CorridorConfig(
                corridor_id=row["corridor_id"],
                origin=row["origin"],
                destination=row["destination"],
                length=float(row["length"]),
                free_flow_speed=float(row["free_flow_speed"]),
                base_capacity=float(row["base_capacity"]),
                required_separation=float(row["required_separation"]),
            )
            for row in reader
        ]


def _load_vehicles(path: Path) -> list[VehicleConfig]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            VehicleConfig(
                vehicle_id=row["vehicle_id"],
                mission_class=row["mission_class"],
                route=[segment.strip() for segment in row["route"].split("|") if segment.strip()],
                reserve_energy=float(row["reserve_energy"]),
                status=row.get("status", "queued") or "queued",
                supplier_id=row.get("supplier_id", "fleet_ops") or "fleet_ops",
                trust_state=row.get("trust_state", "trusted") or "trusted",
                intent_update_interval_minutes=int(row.get("intent_update_interval_minutes", 5) or 5),
                min_contingency_margin=float(row.get("min_contingency_margin", 10.0) or 10.0),
            )
            for row in reader
        ]


def _load_suppliers(raw_suppliers: list[dict[str, object]], nodes: list[NodeConfig], vehicles: list[VehicleConfig]) -> list[SupplierConfig]:
    suppliers: dict[str, SupplierConfig] = {}
    for supplier in raw_suppliers:
        supplier_id = str(supplier["supplier_id"])
        suppliers[supplier_id] = SupplierConfig(
            supplier_id=supplier_id,
            trust_state=str(supplier.get("trust_state", "trusted")),
            supplier_type=str(supplier.get("supplier_type", "identity")),
        )

    for node in nodes:
        suppliers.setdefault(node.supplier_id, SupplierConfig(supplier_id=node.supplier_id))
    for vehicle in vehicles:
        suppliers.setdefault(vehicle.supplier_id, SupplierConfig(supplier_id=vehicle.supplier_id))
    return list(suppliers.values())


def _load_disturbance_state(raw_state: dict[str, object]) -> DisturbanceState:
    return DisturbanceState(
        weather_severity=float(raw_state.get("weather_severity", 0.0)),
        comms_reliability=float(raw_state.get("comms_reliability", 1.0)),
        comms_latency_minutes=float(raw_state.get("comms_latency_minutes", 1.0)),
        message_drop_probability=float(raw_state.get("message_drop_probability", 0.0)),
        stale_after_minutes=float(raw_state.get("stale_after_minutes", 10.0)),
        reroute_delay_minutes=float(raw_state.get("reroute_delay_minutes", 0.0)),
        low_bandwidth_threshold_minutes=float(raw_state.get("low_bandwidth_threshold_minutes", 15.0)),
        node_service_multiplier=float(raw_state.get("node_service_multiplier", 1.0)),
    )


def _load_disturbance_schedule(entries: list[dict[str, object]]) -> list[DisturbanceScheduleEntry]:
    schedule = [
        DisturbanceScheduleEntry(
            start_minute=int(entry["start_minute"]),
            weather_severity=float(entry.get("weather_severity", 0.0)),
            comms_reliability=float(entry.get("comms_reliability", 1.0)),
            comms_latency_minutes=float(entry.get("comms_latency_minutes", 1.0)),
            message_drop_probability=float(entry.get("message_drop_probability", 0.0)),
            stale_after_minutes=float(entry.get("stale_after_minutes", 10.0)),
            reroute_delay_minutes=float(entry.get("reroute_delay_minutes", 0.0)),
            low_bandwidth_threshold_minutes=float(entry.get("low_bandwidth_threshold_minutes", 15.0)),
            node_service_multiplier=float(entry.get("node_service_multiplier", 1.0)),
        )
        for entry in entries
    ]
    return sorted(schedule, key=lambda entry: entry.start_minute)


def _load_trust_events(entries: list[dict[str, object]]) -> list[TrustEventConfig]:
    events = [
        TrustEventConfig(
            start_minute=int(entry["start_minute"]),
            target_type=str(entry["target_type"]),
            target_id=str(entry["target_id"]),
            trigger=str(entry["trigger"]),
            resulting_state=str(entry["resulting_state"]),
            propagation_delay_minutes=int(entry.get("propagation_delay_minutes", 0)),
            note=str(entry.get("note", "")),
        )
        for entry in entries
    ]
    return sorted(events, key=lambda entry: entry.start_minute)


def _load_infrastructure_events(entries: list[dict[str, object]]) -> list[InfrastructureEventConfig]:
    events = [
        InfrastructureEventConfig(
            start_minute=int(entry["start_minute"]),
            end_minute=int(entry["end_minute"]),
            target_type=str(entry["target_type"]),
            target_id=str(entry["target_id"]),
            state=str(entry.get("state", "closed")),
            service_multiplier=float(entry.get("service_multiplier", 1.0)),
            contingency_slots_delta=int(entry.get("contingency_slots_delta", 0)),
        )
        for entry in entries
    ]
    return sorted(events, key=lambda entry: entry.start_minute)


def _load_vehicle_injections(entries: list[dict[str, object]]) -> list[VehicleInjectionConfig]:
    injections = [
        VehicleInjectionConfig(
            start_minute=int(entry["start_minute"]),
            vehicle_id=str(entry["vehicle_id"]),
            mission_class=str(entry["mission_class"]),
            route=[segment.strip() for segment in str(entry["route"]).split("|") if segment.strip()],
            reserve_energy=float(entry["reserve_energy"]),
            supplier_id=str(entry.get("supplier_id", "unverified")),
            trust_state=str(entry.get("trust_state", "unknown")),
            status=str(entry.get("status", "queued")),
            note=str(entry.get("note", "adversary_injection")),
        )
        for entry in entries
    ]
    return sorted(injections, key=lambda entry: entry.start_minute)


def load_simulation_config(config_path: str | Path) -> SimulationConfig:
    path = Path(config_path).resolve()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    base_dir = path.parent
    data_sources = raw["data_sources"]
    nodes_path = _resolve_path(base_dir, data_sources["nodes"])
    corridors_path = _resolve_path(base_dir, data_sources["corridors"])
    vehicles_path = _resolve_path(base_dir, data_sources["vehicles"])
    output_root = _resolve_path(base_dir, str(raw.get("output_root", "../outputs")))

    nodes = _load_nodes(nodes_path)
    corridors = _load_corridors(corridors_path)
    vehicles = _load_vehicles(vehicles_path)

    disturbances_raw = raw.get("disturbances", {})
    disturbance_base = _load_disturbance_state(disturbances_raw.get("base", {}))
    disturbance_schedule = _load_disturbance_schedule(disturbances_raw.get("schedule", []))

    trust_raw = raw.get("trust", {})
    infrastructure_raw = raw.get("infrastructure", {})
    modifiers_raw = raw.get("modifiers", {})
    safe_region_raw = raw.get("safe_region", {})
    physics_raw = raw.get("physics_model", {})
    admissibility_raw = raw.get("admissibility", {})
    operator_raw = raw.get("operator", {})

    safe_region = SafeRegionConfig(
        max_corridor_load_ratio=float(safe_region_raw.get("max_corridor_load_ratio", 1.25)),
        max_node_utilization_ratio=float(safe_region_raw.get("max_node_utilization_ratio", 1.10)),
        max_queue_ratio=float(safe_region_raw.get("max_queue_ratio", 1.10)),
        max_stale_state_exposure_minutes=float(safe_region_raw.get("max_stale_state_exposure_minutes", 60.0)),
        min_trusted_participant_fraction=float(safe_region_raw.get("min_trusted_participant_fraction", 0.0)),
        max_unsafe_admissions=int(safe_region_raw.get("max_unsafe_admissions", 9999)),
        min_reachable_landing_options=float(safe_region_raw.get("min_reachable_landing_options", 0.0)),
        max_contingency_saturation_duration=float(
            safe_region_raw.get("max_contingency_saturation_duration", 1_000_000.0)
        ),
        max_operator_interventions_per_hour=float(
            safe_region_raw.get("max_operator_interventions_per_hour", 1_000_000.0)
        ),
    )

    return SimulationConfig(
        scenario_name=str(raw["scenario_name"]),
        description=str(raw["description"]),
        time_step_minutes=int(raw["time_step_minutes"]),
        duration_minutes=int(raw["duration_minutes"]),
        output_root=output_root,
        seed=int(raw.get("seed", 0)),
        nodes=nodes,
        corridors=corridors,
        vehicles=vehicles,
        suppliers=_load_suppliers(raw.get("suppliers", []), nodes, vehicles),
        disturbance_base=disturbance_base,
        disturbance_schedule=disturbance_schedule,
        trust_events=_load_trust_events(trust_raw.get("events", [])),
        infrastructure_events=_load_infrastructure_events(infrastructure_raw.get("events", [])),
        vehicle_injections=_load_vehicle_injections(raw.get("vehicle_injections", [])),
        modifiers=ScenarioModifiers(
            demand_multiplier=float(modifiers_raw.get("demand_multiplier", 1.0)),
            weather_multiplier=float(modifiers_raw.get("weather_multiplier", 1.0)),
            corridor_capacity_multiplier=float(modifiers_raw.get("corridor_capacity_multiplier", 1.0)),
            node_service_multiplier=float(modifiers_raw.get("node_service_multiplier", 1.0)),
            separation_multiplier=float(modifiers_raw.get("separation_multiplier", 1.0)),
            reserve_consumption_multiplier=float(modifiers_raw.get("reserve_consumption_multiplier", 1.0)),
            comms_reliability_multiplier=float(modifiers_raw.get("comms_reliability_multiplier", 1.0)),
            latency_multiplier=float(modifiers_raw.get("latency_multiplier", 1.0)),
            drop_probability_multiplier=float(modifiers_raw.get("drop_probability_multiplier", 1.0)),
            contingency_capacity_multiplier=float(modifiers_raw.get("contingency_capacity_multiplier", 1.0)),
            closure_probability=float(modifiers_raw.get("closure_probability", 0.0)),
        ),
        safe_region=safe_region,
        physics_model=PhysicsModelConfig(
            a_w=float(physics_raw.get("a_w", 0.9)),
            a_gamma=float(physics_raw.get("a_gamma", 1.1)),
            a_eta=float(physics_raw.get("a_eta", 0.4)),
            a_chi=float(physics_raw.get("a_chi", 0.8)),
            minimum_alpha=float(physics_raw.get("minimum_alpha", 0.2)),
        ),
        admissibility=AdmissibilityConfig(
            rho_safe=float(admissibility_raw.get("rho_safe", safe_region.max_corridor_load_ratio)),
            queue_safe=float(admissibility_raw.get("queue_safe", safe_region.max_queue_ratio)),
            gamma_min=float(admissibility_raw.get("gamma_min", 0.80)),
            chi_max=float(
                admissibility_raw.get(
                    "chi_max",
                    max(0.0, 1.0 - safe_region.min_trusted_participant_fraction),
                )
            ),
            reserve_min=float(admissibility_raw.get("reserve_min", 0.0)),
        ),
        operator=OperatorConfig(
            max_interventions_per_hour=float(operator_raw.get("max_interventions_per_hour", 6.0)),
            override_latency_minutes=float(operator_raw.get("override_latency_minutes", 5.0)),
        ),
    )
