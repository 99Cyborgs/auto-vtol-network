from __future__ import annotations

import csv
import json
import tomllib
from pathlib import Path

from .models import (
    CalibrationConfig,
    CalibrationGate,
    ConfidencePolicy,
    CoverageRequirements,
    ContingencyPolicy,
    CorridorRecord,
    DemandRequestRecord,
    DispatchPolicy,
    DisruptionEvent,
    ExperimentAxis,
    ExperimentManifest,
    ExternalFieldMapping,
    ExternalNormalization,
    ExternalRawInput,
    ExternalSourceManifest,
    FitParameter,
    GovernancePolicy,
    GovernanceWaiver,
    NodeRecord,
    OutputConfig,
    QualityChecks,
    ReferenceBundle,
    ReferenceEventExpectation,
    ReferenceMetricTarget,
    ReferenceSeriesTarget,
    ReservationPolicy,
    ScenarioConfig,
    VehicleRecord,
)


def _resolve(base_dir: Path, value: str | None) -> Path | None:
    if value is None:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _as_number(value: str) -> int | float:
    numeric = float(value)
    if numeric.is_integer():
        return int(numeric)
    return numeric


def _as_scope_values(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    return tuple(str(item) for item in raw)


def _as_string_tuple(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    return tuple(str(item) for item in raw)


def _load_nodes(path: Path) -> tuple[NodeRecord, ...]:
    return tuple(
        NodeRecord(
            node_id=row["node_id"],
            node_type=row["node_type"],
            turnaround_minutes=int(row["turnaround_minutes"]),
            service_rate_per_hour=float(row["service_rate_per_hour"]),
            queue_capacity=int(row["queue_capacity"]),
            contingency_slots=int(row["contingency_slots"]),
            trust_state=row.get("trust_state", "trusted") or "trusted",
        )
        for row in _load_csv(path)
    )


def _load_corridors(path: Path) -> tuple[CorridorRecord, ...]:
    return tuple(
        CorridorRecord(
            corridor_id=row["corridor_id"],
            origin=row["origin"],
            destination=row["destination"],
            length_km=float(row["length_km"]),
            travel_minutes=float(row["travel_minutes"]),
            capacity_per_hour=float(row["capacity_per_hour"]),
            energy_cost=float(row["energy_cost"]),
            reservation_window_minutes=int(row.get("reservation_window_minutes", 15) or 15),
        )
        for row in _load_csv(path)
    )


def _load_vehicles(path: Path) -> tuple[VehicleRecord, ...]:
    return tuple(
        VehicleRecord(
            vehicle_id=row["vehicle_id"],
            home_node=row["home_node"],
            vehicle_class=row["vehicle_class"],
            energy_capacity=float(row["energy_capacity"]),
            reserve_energy=float(row["reserve_energy"]),
            trust_state=row.get("trust_state", "trusted") or "trusted",
            operator_required=str(row.get("operator_required", "false")).strip().lower() in {"1", "true", "yes"},
        )
        for row in _load_csv(path)
    )


def _load_demand(path: Path) -> tuple[DemandRequestRecord, ...]:
    return tuple(
        DemandRequestRecord(
            request_id=row["request_id"],
            release_minute=int(row["release_minute"]),
            origin=row["origin"],
            destination=row["destination"],
            priority=row["priority"],
            required_vehicle_class=row["required_vehicle_class"],
            max_delay_minutes=int(row["max_delay_minutes"]),
        )
        for row in _load_csv(path)
    )


def _load_disruptions(path: Path | None) -> tuple[DisruptionEvent, ...]:
    if path is None or not path.exists():
        return ()
    return tuple(
        DisruptionEvent(
            event_id=row["event_id"],
            start_minute=int(row["start_minute"]),
            end_minute=int(row["end_minute"]),
            target_type=row["target_type"],
            target_id=row["target_id"],
            effect_type=row["effect_type"],
            value=(float(row["value"]) if row["value"].replace(".", "", 1).replace("-", "", 1).isdigit() else row["value"]),
            note=row.get("note", "") or "",
        )
        for row in _load_csv(path)
    )


def load_scenario_config(config_path: str | Path) -> ScenarioConfig:
    path = Path(config_path).resolve()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    base_dir = path.parent

    network = raw["network"]
    fleet = raw["fleet"]
    demand = raw["demand"]
    dispatch_policy = raw["dispatch_policy"]
    reservation_policy = raw["reservation_policy"]
    contingency_policy = raw["contingency_policy"]
    outputs = raw["outputs"]
    calibration = raw.get("calibration", {})
    disruptions = raw.get("disruptions", {})

    return ScenarioConfig(
        scenario_name=str(raw["scenario_name"]),
        description=str(raw["description"]),
        duration_minutes=int(raw.get("duration_minutes", 180)),
        time_step_minutes=int(raw.get("time_step_minutes", 1)),
        nodes=_load_nodes(_resolve(base_dir, network["nodes"])),
        corridors=_load_corridors(_resolve(base_dir, network["corridors"])),
        vehicles=_load_vehicles(_resolve(base_dir, fleet["vehicles"])),
        demand_requests=_load_demand(_resolve(base_dir, demand["requests"])),
        disruptions=_load_disruptions(_resolve(base_dir, disruptions.get("events"))),
        dispatch_policy=DispatchPolicy(
            max_wait_minutes=int(dispatch_policy.get("max_wait_minutes", 20)),
            max_reroutes=int(dispatch_policy.get("max_reroutes", 2)),
            degraded_dispatch_enabled=bool(dispatch_policy.get("degraded_dispatch_enabled", False)),
            operator_delay_minutes=int(dispatch_policy.get("operator_delay_minutes", 3)),
        ),
        reservation_policy=ReservationPolicy(
            lookahead_minutes=int(reservation_policy.get("lookahead_minutes", 20)),
            reservation_horizon_hops=int(reservation_policy.get("reservation_horizon_hops", 2)),
            conflict_tolerance=int(reservation_policy.get("conflict_tolerance", 0)),
        ),
        contingency_policy=ContingencyPolicy(
            min_energy_reserve=float(contingency_policy.get("min_energy_reserve", 15.0)),
            reroute_buffer_minutes=int(contingency_policy.get("reroute_buffer_minutes", 5)),
            diversion_limit=int(contingency_policy.get("diversion_limit", 1)),
        ),
        calibration=CalibrationConfig(
            bundle=_resolve(base_dir, calibration.get("bundle")),
            enabled=bool(calibration.get("enabled", False)),
            travel_time_multiplier=float(calibration.get("travel_time_multiplier", 1.0)),
            energy_cost_multiplier=float(calibration.get("energy_cost_multiplier", 1.0)),
            service_rate_multiplier=float(calibration.get("service_rate_multiplier", 1.0)),
            reservation_capacity_multiplier=float(calibration.get("reservation_capacity_multiplier", 1.0)),
        ),
        outputs=OutputConfig(
            root=_resolve(base_dir, outputs.get("root", "../outputs/v2")) or path.parent.resolve(),
            artifact_prefix=str(outputs.get("artifact_prefix", "avn_v2")),
        ),
        scenario_path=path,
    )


def load_experiment_manifest(manifest_path: str | Path) -> ExperimentManifest:
    path = Path(manifest_path).resolve()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    base_dir = path.parent
    axes = tuple(
        ExperimentAxis(
            name=str(axis["name"]),
            values=tuple(axis["values"]),
        )
        for axis in raw.get("axes", [])
    )
    governance_policy = raw.get("governance_policy", {})
    waivers = tuple(
        GovernanceWaiver(
            category=str(item["category"]),
            justification_id=str(item["justification_id"]),
        )
        for item in governance_policy.get("waivers", [])
    )
    return ExperimentManifest(
        experiment_name=str(raw["experiment_name"]),
        base_scenario=_resolve(base_dir, raw["base_scenario"]) or path,
        output_root=_resolve(base_dir, raw.get("output_root", "../outputs/v2")) or path.parent.resolve(),
        adaptive_refinement=bool(raw.get("adaptive_refinement", True)),
        axes=axes,
        promoted_metrics=tuple(str(metric) for metric in raw.get("promoted_metrics", ("completed_requests", "avg_delay_minutes"))),
        calibration_bundle=_resolve(base_dir, raw.get("calibration_bundle")),
        ingested_bundle_source=_resolve(base_dir, raw.get("ingested_bundle_source")),
        calibration_gate=str(raw.get("calibration_gate", "waived")),
        use_calibrated_parameters=bool(raw.get("use_calibrated_parameters", True)),
        governance_policy=GovernancePolicy(
            minimum_confidence_tier=str(governance_policy.get("minimum_confidence_tier", "medium")),
            fatal_blocker_categories=_as_string_tuple(
                governance_policy.get(
                    "fatal_blocker_categories",
                    ("operational_breach", "fit_failure", "evidence_insufficiency"),
                )
            ),
            advisory_blocker_categories=_as_string_tuple(governance_policy.get("advisory_blocker_categories")),
            waivable_categories=_as_string_tuple(governance_policy.get("waivable_categories")),
            waivers=waivers,
        ),
    )


def load_external_source_manifest(manifest_path: str | Path) -> ExternalSourceManifest:
    path = Path(manifest_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    base_dir = path.parent
    raw_inputs = {
        str(name): ExternalRawInput(
            name=str(name),
            path=_resolve(base_dir, str(spec["path"])) or path,
            format=str(spec.get("format", "csv")),
        )
        for name, spec in payload.get("raw_inputs", {}).items()
    }
    field_mapping = {
        str(name): ExternalFieldMapping(
            fields={str(key): str(value) for key, value in spec.get("fields", {}).items()},
            optional_fields=_as_string_tuple(spec.get("optional_fields")),
            defaults={str(key): value for key, value in spec.get("defaults", {}).items()},
        )
        for name, spec in payload.get("field_mapping", {}).items()
    }
    normalization = payload.get("normalization", {})
    coverage = payload.get("coverage_requirements", {})
    confidence = payload.get("confidence_policy", {})
    quality = payload.get("quality_checks", {})
    return ExternalSourceManifest(
        source_id=str(payload["source_id"]),
        source_type=str(payload["source_type"]),
        version=str(payload["version"]),
        scenario_path=_resolve(base_dir, payload["scenario"]) or path,
        ingestion_mode=str(payload.get("ingestion_mode", "copy")),
        bundle_family=str(payload.get("bundle_family", "baseline")),
        raw_inputs=raw_inputs,
        field_mapping=field_mapping,
        normalization=ExternalNormalization(
            metric_unit_mappings={
                str(key): {str(inner_key): inner_value for inner_key, inner_value in value.items()}
                for key, value in normalization.get("metric_unit_mappings", {}).items()
            },
            event_type_mapping={str(key): str(value) for key, value in normalization.get("event_type_mapping", {}).items()},
            scope_mapping={str(key): str(value) for key, value in normalization.get("scope_mapping", {}).items()},
            time_multiplier=float(normalization.get("time_multiplier", 1.0)),
            time_rounding=str(normalization.get("time_rounding", "nearest")),
            missing_value_policy=str(normalization.get("missing_value_policy", "reject")),
        ),
        fit_space_overrides={str(key): value for key, value in payload.get("fit_space_overrides", {}).items()},
        coverage_requirements=CoverageRequirements(
            min_metric_targets=int(coverage.get("min_metric_targets", 0)),
            min_event_expectations=int(coverage.get("min_event_expectations", 0)),
            min_series_targets=int(coverage.get("min_series_targets", 0)),
            required_scopes=_as_scope_values(coverage.get("required_scopes")),
        ),
        confidence_policy=ConfidencePolicy(
            high_confidence_score=float(confidence.get("high_confidence_score", 0.85)),
            medium_confidence_score=float(confidence.get("medium_confidence_score", 0.6)),
            max_sensitivity_delta=float(confidence.get("max_sensitivity_delta", 0.3)),
            max_failed_objectives=int(confidence.get("max_failed_objectives", 0)),
            min_coverage_completeness=float(confidence.get("min_coverage_completeness", 1.0)),
        ),
        quality_checks=QualityChecks(
            require_reference_sources=bool(quality.get("require_reference_sources", True)),
            require_bundle_hashes=bool(quality.get("require_bundle_hashes", False)),
            require_monotonic_windows=bool(quality.get("require_monotonic_windows", True)),
            require_scope_coverage=bool(quality.get("require_scope_coverage", True)),
            required_metric_columns=_as_string_tuple(quality.get("required_metric_columns", ("metric_key", "reference_value", "tolerance"))),
            required_event_columns=_as_string_tuple(quality.get("required_event_columns", ("expectation_id", "event_type", "expected_count", "count_tolerance"))),
            required_series_columns=_as_string_tuple(quality.get("required_series_columns", ("target_id", "scope", "entity_id", "metric_key", "reference_value", "tolerance"))),
        ),
        provenance_defaults={str(key): value for key, value in payload.get("provenance_defaults", {}).items()},
        output_root=_resolve(base_dir, payload.get("output_root")),
        manifest_path=path,
    )


def load_reference_bundle(bundle_path: str | Path) -> ReferenceBundle:
    path = Path(bundle_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    base_dir = path.parent
    backtest = payload["backtest"]
    fit_space = payload["fit_space"]
    metrics_path = _resolve(base_dir, backtest["metric_targets"]) or path
    with metrics_path.open("r", encoding="utf-8", newline="") as handle:
        metric_targets = tuple(
            ReferenceMetricTarget(
                metric_key=row["metric_key"],
                reference_value=float(row["reference_value"]),
                tolerance=float(row["tolerance"]),
                unit=row.get("unit", ""),
                weight=float(row.get("weight", 1.0)),
                objective_group=row.get("objective_group", "metric") or "metric",
            )
            for row in csv.DictReader(handle)
        )
    event_expectations: tuple[ReferenceEventExpectation, ...] = ()
    event_path = _resolve(base_dir, backtest.get("event_expectations"))
    if event_path is not None and event_path.exists():
        with event_path.open("r", encoding="utf-8", newline="") as handle:
            event_expectations = tuple(
                ReferenceEventExpectation(
                    expectation_id=row["expectation_id"],
                    event_type=row["event_type"],
                    expected_count=int(row["expected_count"]),
                    count_tolerance=int(row["count_tolerance"]),
                    first_minute=None if not row.get("first_minute") else float(row["first_minute"]),
                    timing_tolerance=None if not row.get("timing_tolerance") else float(row["timing_tolerance"]),
                    weight=float(row.get("weight", 1.0)),
                    objective_group=row.get("objective_group", "event") or "event",
                )
                for row in csv.DictReader(handle)
            )
    series_targets: tuple[ReferenceSeriesTarget, ...] = ()
    series_path = _resolve(base_dir, backtest.get("series_targets"))
    if series_path is not None and series_path.exists():
        with series_path.open("r", encoding="utf-8", newline="") as handle:
            series_targets = tuple(
                ReferenceSeriesTarget(
                    target_id=row["target_id"],
                    scope=row["scope"],
                    entity_id=row["entity_id"],
                    metric_key=row["metric_key"],
                    reference_value=float(row["reference_value"]),
                    tolerance=float(row["tolerance"]),
                    weight=float(row.get("weight", 1.0)),
                    minute=None if not row.get("minute") else int(row["minute"]),
                    minute_start=None if not row.get("minute_start") else int(row["minute_start"]),
                    minute_end=None if not row.get("minute_end") else int(row["minute_end"]),
                    aggregation=row.get("aggregation", "point") or "point",
                    objective_group=row.get("objective_group", "series") or "series",
                )
                for row in csv.DictReader(handle)
            )
    fit_parameters = []
    for parameter in fit_space.get("parameters", []):
        if "values" in parameter:
            values = tuple(_as_number(str(value)) for value in parameter["values"])
        else:
            min_value = float(parameter["min_value"])
            max_value = float(parameter["max_value"])
            step = float(parameter["step"])
            values_list: list[int | float] = []
            current = min_value
            while current <= max_value + (step / 10.0):
                if bool(parameter.get("integer", False)):
                    values_list.append(int(round(current)))
                else:
                    values_list.append(round(current, 6))
                current += step
            values = tuple(dict.fromkeys(values_list))
        fit_parameters.append(
            FitParameter(
                name=str(parameter["name"]),
                scenario_key=str(parameter["scenario_key"]),
                values=values,
                description=str(parameter.get("description", "")),
            )
        )
    gates = payload.get("gates", {})
    coverage = payload.get("coverage_requirements", {})
    confidence = payload.get("confidence_policy", {})
    quality = payload.get("quality_checks", {})
    source_files = {"metric_targets": metrics_path}
    if event_path is not None:
        source_files["event_expectations"] = event_path
    if series_path is not None:
        source_files["series_targets"] = series_path
    return ReferenceBundle(
        contract_version=int(payload.get("contract_version", 4)),
        bundle_id=str(payload["bundle_id"]),
        name=str(payload["name"]),
        version=str(payload["version"]),
        scenario_path=_resolve(base_dir, payload["scenario"]) or path,
        metric_targets=metric_targets,
        event_expectations=event_expectations,
        series_targets=series_targets,
        fit_parameters=tuple(fit_parameters),
        coverage_requirements=CoverageRequirements(
            min_metric_targets=int(coverage.get("min_metric_targets", 0)),
            min_event_expectations=int(coverage.get("min_event_expectations", 0)),
            min_series_targets=int(coverage.get("min_series_targets", 0)),
            required_scopes=_as_scope_values(coverage.get("required_scopes")),
        ),
        confidence_policy=ConfidencePolicy(
            high_confidence_score=float(confidence.get("high_confidence_score", 0.85)),
            medium_confidence_score=float(confidence.get("medium_confidence_score", 0.6)),
            max_sensitivity_delta=float(confidence.get("max_sensitivity_delta", 0.3)),
            max_failed_objectives=int(confidence.get("max_failed_objectives", 0)),
            min_coverage_completeness=float(confidence.get("min_coverage_completeness", 1.0)),
        ),
        quality_checks=QualityChecks(
            require_reference_sources=bool(quality.get("require_reference_sources", True)),
            require_bundle_hashes=bool(quality.get("require_bundle_hashes", False)),
            require_monotonic_windows=bool(quality.get("require_monotonic_windows", True)),
            require_scope_coverage=bool(quality.get("require_scope_coverage", True)),
            required_metric_columns=_as_string_tuple(quality.get("required_metric_columns", ("metric_key", "reference_value", "tolerance"))),
            required_event_columns=_as_string_tuple(quality.get("required_event_columns", ("expectation_id", "event_type", "expected_count", "count_tolerance"))),
            required_series_columns=_as_string_tuple(quality.get("required_series_columns", ("target_id", "scope", "entity_id", "metric_key", "reference_value", "tolerance"))),
        ),
        objective_group_weights={
            str(key): float(value)
            for key, value in payload.get("objective_group_weights", {}).items()
        },
        gate=CalibrationGate(
            max_total_score=float(gates.get("max_total_score", 1.0)),
            require_metric_match=bool(gates.get("require_metric_match", True)),
            require_event_match=bool(gates.get("require_event_match", True)),
            require_series_match=bool(gates.get("require_series_match", False)),
            require_calibrated_parameters=bool(gates.get("require_calibrated_parameters", True)),
            require_evidence_coverage=bool(gates.get("require_evidence_coverage", True)),
        ),
        reference_sources=_as_string_tuple(payload.get("reference_sources")),
        provenance={str(key): value for key, value in payload.get("provenance", {}).items()},
        source_files=source_files,
        bundle_path=path,
    )
