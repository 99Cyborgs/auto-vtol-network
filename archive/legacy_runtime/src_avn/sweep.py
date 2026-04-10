from __future__ import annotations

import argparse
import copy
import csv
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import math
from pathlib import Path
import statistics

from avn.core.config import load_simulation_config
from avn.core.models import TrustEventConfig
from avn.sweep_adaptive import adaptive_sweep
from avn.sweep_analysis import (
    ArtifactPaths,
    TrancheSliceResult,
    build_slice_result,
    CommsMetricsSnapshot,
    ContingencyMetricsSnapshot,
    ThroughputMetricsSnapshot,
    TrustMetricsSnapshot,
    load_slice_results_payload,
    write_aggregate_csv,
    write_admissibility_overlay_json,
    write_convergence_report_json,
    write_contradictions_json,
    write_cross_tranche_outputs,
    write_cross_tranche_promotion_decisions_json,
    write_cross_tranche_threshold_ledger_json,
    write_cross_tranche_thresholds_json,
    write_global_phase_map_json,
    write_phase_map_json,
    write_phase_boundaries_json,
    write_promotion_decisions_json,
    write_slice_results_json,
    write_threshold_ledger_json,
    write_threshold_estimates_json,
    write_transition_regions_json,
)
from avn.sweep_tranches import (
    BUILT_IN_TRANCHES,
    SeedPolicy,
    SweepAxis,
    TrancheDefinition,
    generate_tranche_slices,
    get_tranche,
)
from avn.simulation.engine import SimulationEngine


@dataclass(slots=True)
class SweepResult:
    output_dir: Path
    aggregate_csv_path: Path
    summary_json_path: Path
    rows: list[dict[str, float | int | str]]


@dataclass(slots=True)
class TrancheRunResult:
    tranche_name: str
    output_dir: Path
    aggregate_csv_path: Path
    slice_results_json_path: Path
    phase_boundaries_json_path: Path
    slice_results: list[TrancheSliceResult]
    phase_map_json_path: Path | None = None
    transition_regions_json_path: Path | None = None
    threshold_estimates_json_path: Path | None = None
    threshold_ledger_json_path: Path | None = None
    promotion_decisions_json_path: Path | None = None
    admissibility_overlay_json_path: Path | None = None
    convergence_report_json_path: Path | None = None
    adaptive_metadata: dict[str, object] | None = None


@dataclass(slots=True)
class GlobalAnalysisResult:
    output_dir: Path
    mechanism_dominance_matrix_path: Path
    tranche_comparison_path: Path
    cross_tranche_summary_path: Path
    tranche_outputs: dict[str, Path]
    global_phase_map_path: Path | None = None
    cross_tranche_thresholds_path: Path | None = None
    contradictions_path: Path | None = None
    cross_tranche_threshold_ledger_path: Path | None = None
    cross_tranche_promotion_decisions_path: Path | None = None


def run_phase2b_sweep(config_path: str | Path, *, output_root: Path | None = None) -> SweepResult:
    base_config = load_simulation_config(config_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    root = output_root if output_root is not None else base_config.output_root
    output_dir = (root / "sweeps" / f"phase2b_sweep_{timestamp}").resolve()
    output_dir.mkdir(parents=True, exist_ok=False)

    sweep_points = [
        {
            "label": "baseline",
            "demand_multiplier": 1.0,
            "weather_multiplier": 1.0,
            "corridor_capacity_multiplier": 1.0,
            "node_service_multiplier": 1.0,
            "separation_multiplier": 1.0,
            "reserve_consumption_multiplier": 1.0,
            "closure_probability": 0.0,
            "comms_reliability_multiplier": 1.0,
        },
        {
            "label": "stale_bias",
            "demand_multiplier": 1.25,
            "weather_multiplier": 1.0,
            "corridor_capacity_multiplier": 1.0,
            "node_service_multiplier": 1.0,
            "separation_multiplier": 1.0,
            "reserve_consumption_multiplier": 1.0,
            "closure_probability": 0.0,
            "comms_reliability_multiplier": 0.78,
        },
        {
            "label": "capacity_bias",
            "demand_multiplier": 1.5,
            "weather_multiplier": 1.1,
            "corridor_capacity_multiplier": 0.85,
            "node_service_multiplier": 0.9,
            "separation_multiplier": 1.15,
            "reserve_consumption_multiplier": 1.05,
            "closure_probability": 0.0,
            "comms_reliability_multiplier": 0.92,
        },
        {
            "label": "contingency_bias",
            "demand_multiplier": 1.45,
            "weather_multiplier": 1.2,
            "corridor_capacity_multiplier": 0.95,
            "node_service_multiplier": 0.9,
            "separation_multiplier": 1.1,
            "reserve_consumption_multiplier": 1.25,
            "closure_probability": 0.35,
            "comms_reliability_multiplier": 0.9,
        },
        {
            "label": "compound",
            "demand_multiplier": 1.7,
            "weather_multiplier": 1.2,
            "corridor_capacity_multiplier": 0.85,
            "node_service_multiplier": 0.8,
            "separation_multiplier": 1.2,
            "reserve_consumption_multiplier": 1.3,
            "closure_probability": 0.35,
            "comms_reliability_multiplier": 0.75,
        },
    ]

    rows: list[dict[str, float | int | str]] = []
    for index, point in enumerate(sweep_points):
        config = copy.deepcopy(base_config)
        config.output_root = output_dir / "runs"
        config.seed = base_config.seed + index
        config.modifiers.demand_multiplier = float(point["demand_multiplier"])
        config.modifiers.weather_multiplier = float(point["weather_multiplier"])
        config.modifiers.corridor_capacity_multiplier = float(point["corridor_capacity_multiplier"])
        config.modifiers.node_service_multiplier = float(point["node_service_multiplier"])
        config.modifiers.separation_multiplier = float(point["separation_multiplier"])
        config.modifiers.reserve_consumption_multiplier = float(point["reserve_consumption_multiplier"])
        config.modifiers.closure_probability = float(point["closure_probability"])
        config.modifiers.comms_reliability_multiplier = float(point["comms_reliability_multiplier"])

        result = SimulationEngine(config).run()
        row = {
            "label": point["label"],
            "scenario_name": result.scenario_name,
            "seed": config.seed,
            "demand_multiplier": config.modifiers.demand_multiplier,
            "weather_multiplier": config.modifiers.weather_multiplier,
            "corridor_capacity_multiplier": config.modifiers.corridor_capacity_multiplier,
            "node_service_multiplier": config.modifiers.node_service_multiplier,
            "separation_multiplier": config.modifiers.separation_multiplier,
            "reserve_consumption_multiplier": config.modifiers.reserve_consumption_multiplier,
            "closure_probability": config.modifiers.closure_probability,
            "comms_reliability_multiplier": config.modifiers.comms_reliability_multiplier,
            "completed_vehicles": result.summary["completed_vehicles"],
            "incomplete_vehicles": result.summary["incomplete_vehicles"],
            "unsafe_admission_count": result.summary["unsafe_admission_count"],
            "stale_state_exposure_minutes": result.summary["stale_state_exposure_minutes"],
            "no_admissible_landing_events": result.summary["no_admissible_landing_events"],
            "peak_corridor_load_ratio": result.summary["peak_corridor_load_ratio"],
            "peak_queue_ratio": result.summary["peak_queue_ratio"],
            "contingency_saturation_duration": result.summary["contingency_saturation_duration"],
            "first_safe_region_exit_cause": result.summary["first_safe_region_exit_cause"],
            "first_dominant_failure_mechanism": result.summary["first_dominant_failure_mechanism"],
        }
        rows.append(row)

    aggregate_csv_path = output_dir / "aggregate.csv"
    with aggregate_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    summary_json_path = output_dir / "summary.json"
    with summary_json_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "base_config": str(Path(config_path).resolve()),
                "run_count": len(rows),
                "aggregate_csv": str(aggregate_csv_path),
                "rows": rows,
            },
            handle,
            indent=2,
        )

    return SweepResult(
        output_dir=output_dir,
        aggregate_csv_path=aggregate_csv_path,
        summary_json_path=summary_json_path,
        rows=rows,
    )


def _timestamp_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def _apply_disturbance_field(config, field: str, value: float | int) -> None:
    setattr(config.disturbance_base, field, value)
    for entry in config.disturbance_schedule:
        setattr(entry, field, value)


def _apply_emergency_pad_density(config, density: float) -> None:
    emergency_nodes = sorted(
        (node for node in config.nodes if node.node_type.lower() == "emergency"),
        key=lambda node: node.node_id,
    )
    if not emergency_nodes:
        return

    if density <= 0.0:
        keep_count = 0
    else:
        keep_count = max(1, min(len(emergency_nodes), math.ceil(len(emergency_nodes) * density)))
    keep_ids = {node.node_id for node in emergency_nodes[:keep_count]}

    for node in emergency_nodes:
        if node.node_id in keep_ids:
            continue
        node.service_rate = 0.0
        node.contingency_capacity = 0
        node.contingency_landing_slots = 0
        node.operational_state = "closed"


def _apply_contingency_slot_multiplier(config, multiplier: float) -> None:
    for node in config.nodes:
        original_slots = node.contingency_landing_slots
        if original_slots <= 0:
            continue
        scaled = int(round(original_slots * multiplier))
        if multiplier > 0.0:
            scaled = max(1, scaled)
        node.contingency_landing_slots = scaled


def _apply_contingency_node_impairment(config, severity: float) -> None:
    for node in config.nodes:
        if node.contingency_landing_slots <= 0 and node.node_type.lower() != "emergency":
            continue
        node.service_rate *= severity


def _apply_trust_compromise_events(
    config,
    *,
    compromised_ratio: float,
    revocation_delay_minutes: int,
    compromise_state: str,
) -> None:
    if compromised_ratio <= 0.0:
        return

    vehicle_configs = sorted(config.vehicles, key=lambda vehicle: vehicle.vehicle_id)
    target_count = max(1, min(len(vehicle_configs), math.ceil(len(vehicle_configs) * compromised_ratio)))
    target_vehicles = vehicle_configs[:target_count]
    compromise_minute = 20
    revoke_minute = compromise_minute + max(0, revocation_delay_minutes)

    for vehicle in target_vehicles:
        config.trust_events.append(
            TrustEventConfig(
                start_minute=compromise_minute,
                target_type="vehicle",
                target_id=vehicle.vehicle_id,
                trigger="tranche_compromise",
                resulting_state=compromise_state,
            )
        )
        config.trust_events.append(
            TrustEventConfig(
                start_minute=revoke_minute,
                target_type="vehicle",
                target_id=vehicle.vehicle_id,
                trigger="tranche_revocation",
                resulting_state="revoked",
            )
        )


def _apply_coupled_load_stress(config) -> None:
    config.modifiers.demand_multiplier *= 1.35
    config.modifiers.corridor_capacity_multiplier *= 0.90
    config.modifiers.node_service_multiplier *= 0.90


def _apply_coupled_comms_stress(config) -> None:
    _apply_disturbance_field(config, "comms_reliability", min(config.disturbance_base.comms_reliability, 0.72))
    _apply_disturbance_field(config, "comms_latency_minutes", max(config.disturbance_base.comms_latency_minutes, 6.0))
    _apply_disturbance_field(
        config,
        "message_drop_probability",
        max(config.disturbance_base.message_drop_probability, 0.18),
    )
    _apply_disturbance_field(config, "stale_after_minutes", min(config.disturbance_base.stale_after_minutes, 8.0))
    _apply_disturbance_field(config, "reroute_delay_minutes", max(config.disturbance_base.reroute_delay_minutes, 10.0))
    _apply_disturbance_field(
        config,
        "low_bandwidth_threshold_minutes",
        min(config.disturbance_base.low_bandwidth_threshold_minutes, 14.0),
    )


def _apply_coupled_trust_stress(config) -> None:
    _apply_trust_compromise_events(
        config,
        compromised_ratio=0.20,
        revocation_delay_minutes=30,
        compromise_state="degraded",
    )


def _apply_coupled_contingency_stress(config) -> None:
    _apply_emergency_pad_density(config, 0.67)
    _apply_contingency_slot_multiplier(config, 0.60)
    _apply_contingency_node_impairment(config, 0.70)
    for vehicle in config.vehicles:
        vehicle.min_contingency_margin = 18.0
    config.modifiers.reserve_consumption_multiplier *= 1.15


def _apply_coupled_weather_stress(config) -> None:
    config.modifiers.weather_multiplier *= 1.30
    config.modifiers.corridor_capacity_multiplier *= 0.90
    config.modifiers.closure_probability = max(config.modifiers.closure_probability, 0.15)


def _apply_coupled_compound_level(config, level: float) -> None:
    if level <= 0.0:
        return
    config.modifiers.demand_multiplier *= 1.0 + 0.30 * level
    config.modifiers.weather_multiplier *= 1.0 + 0.25 * level
    config.modifiers.corridor_capacity_multiplier *= max(0.55, 1.0 - 0.20 * level)
    config.modifiers.node_service_multiplier *= max(0.60, 1.0 - 0.15 * level)
    config.modifiers.reserve_consumption_multiplier *= 1.0 + 0.10 * level
    _apply_disturbance_field(config, "comms_reliability", max(0.55, 0.96 - 0.22 * level))
    _apply_disturbance_field(
        config,
        "message_drop_probability",
        min(0.35, max(config.disturbance_base.message_drop_probability, 0.04 + 0.18 * level)),
    )
    _apply_disturbance_field(
        config,
        "comms_latency_minutes",
        max(config.disturbance_base.comms_latency_minutes, 2.0 + 6.0 * level),
    )
    _apply_trust_compromise_events(
        config,
        compromised_ratio=min(0.5, 0.15 * level),
        revocation_delay_minutes=max(5, int(round(30 * (1.0 - 0.5 * level)))),
        compromise_state="degraded",
    )
    _apply_emergency_pad_density(config, max(0.25, 1.0 - 0.5 * level))


def _apply_resolved_params(config, tranche: TrancheDefinition, resolved_params: dict[str, object]) -> None:
    for key, value in resolved_params.items():
        if key == "scenario.clear_trust_events" and value:
            config.trust_events = []
        elif key == "scenario.clear_infrastructure_events" and value:
            config.infrastructure_events = []
        elif key == "scenario.clear_vehicle_injections" and value:
            config.vehicle_injections = []

    for key, value in resolved_params.items():
        if not key.startswith("modifiers."):
            continue
        field_name = key.split(".", 1)[1]
        setattr(config.modifiers, field_name, value)

    for key, value in resolved_params.items():
        if not key.startswith("disturbance."):
            continue
        field_name = key.split(".", 1)[1]
        _apply_disturbance_field(config, field_name, value)

    for key, value in resolved_params.items():
        if not key.startswith("vehicles."):
            continue
        field_name = key.split(".", 1)[1]
        for vehicle in config.vehicles:
            setattr(vehicle, field_name, value)

    for key, value in resolved_params.items():
        if not key.startswith("safe_region."):
            continue
        field_name = key.split(".", 1)[1]
        setattr(config.safe_region, field_name, value)

    for key, value in resolved_params.items():
        if key == "contingency.emergency_pad_density":
            _apply_emergency_pad_density(config, float(value))
        elif key == "contingency.slot_capacity_multiplier":
            _apply_contingency_slot_multiplier(config, float(value))
        elif key == "contingency.node_impairment_severity":
            _apply_contingency_node_impairment(config, float(value))

    trust_ratio = float(resolved_params.get("trust.compromised_participant_ratio", 0.0))
    trust_delay = int(resolved_params.get("trust.revocation_delay_minutes", 0))
    trust_state = str(resolved_params.get("trust.compromise_state", "degraded"))
    _apply_trust_compromise_events(
        config,
        compromised_ratio=trust_ratio,
        revocation_delay_minutes=trust_delay,
        compromise_state=trust_state,
    )

    for key, value in resolved_params.items():
        if not key.startswith("coupled.") or not bool(value):
            continue
        if key == "coupled.load_stress_enabled":
            _apply_coupled_load_stress(config)
        elif key == "coupled.comms_stress_enabled":
            _apply_coupled_comms_stress(config)
        elif key == "coupled.trust_stress_enabled":
            _apply_coupled_trust_stress(config)
        elif key == "coupled.contingency_stress_enabled":
            _apply_coupled_contingency_stress(config)
        elif key == "coupled.weather_stress_enabled":
            _apply_coupled_weather_stress(config)
        elif key == "coupled.compound_level":
            _apply_coupled_compound_level(config, float(value))

    config.trust_events = sorted(config.trust_events, key=lambda event: event.start_minute)
    config.infrastructure_events = sorted(config.infrastructure_events, key=lambda event: event.start_minute)


def _resolve_output_root(tranche: TrancheDefinition, output_root: Path | None) -> Path:
    if output_root is not None:
        return output_root.resolve()
    base_config = load_simulation_config(tranche.base_config_path)
    return base_config.output_root.resolve()


def _tranche_from_payload(payload: dict[str, object]) -> TrancheDefinition:
    tranche_payload = payload["tranche"]
    if not isinstance(tranche_payload, dict):
        raise ValueError("slice_results.json is missing a tranche definition payload")
    return TrancheDefinition(
        tranche_name=str(tranche_payload["tranche_name"]),
        description=str(tranche_payload["description"]),
        base_config_path=Path(str(tranche_payload["base_config_path"])),
        fixed_params=copy.deepcopy(tranche_payload["fixed_params"]),
        sweep_axes=tuple(
            SweepAxis(
                name=str(axis_payload["name"]),
                values=tuple(axis_payload["values"]),
            )
            for axis_payload in tranche_payload["sweep_axes"]
        ),
        expected_metrics=tuple(tranche_payload["expected_metrics"]),
        expected_failure_modes=tuple(tranche_payload["expected_failure_modes"]),
        seed_policy=SeedPolicy(
            base_seed=int(tranche_payload["seed_policy"]["base_seed"]),
            strategy=str(tranche_payload["seed_policy"]["strategy"]),
            replicates=int(tranche_payload["seed_policy"].get("replicates", 3)),
        ),
        dominant_axis=str(tranche_payload.get("dominant_axis", tranche_payload["sweep_axes"][0]["name"])),
        minimum_slice_count=int(tranche_payload.get("minimum_slice_count", 1)),
    )


def _replicate_seed(base_seed: int, replicate_index: int) -> int:
    modulus = 2_147_483_647
    return (base_seed + (replicate_index + 1) * 9973) % modulus


def _majority_value(values: list[object]) -> object:
    serialized = [json.dumps(value, sort_keys=True) for value in values]
    winning_payload = sorted(Counter(serialized).items(), key=lambda item: (-item[1], item[0]))[0][0]
    return json.loads(winning_payload)


def _mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _variance(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return statistics.pvariance(values)


def _aggregate_phase_detection(run_summaries: list[dict[str, object]]) -> dict[str, object]:
    event_names = sorted(
        {
            event_name
            for summary in run_summaries
            for event_name in (
                summary.get("phase_detection", {}).keys()
                if isinstance(summary.get("phase_detection"), dict)
                else []
            )
        }
    )
    aggregate: dict[str, object] = {}
    for event_name in event_names:
        records = [
            summary["phase_detection"][event_name]
            for summary in run_summaries
            if isinstance(summary.get("phase_detection"), dict)
            and isinstance(summary["phase_detection"].get(event_name), dict)
        ]
        if not records:
            continue
        detected_records = [record for record in records if bool(record.get("detected"))]
        threshold_values = [
            float(record["threshold_value"])
            for record in detected_records
            if isinstance(record.get("threshold_value"), (int, float))
        ]
        times = [
            float(record["time_minute"])
            for record in detected_records
            if isinstance(record.get("time_minute"), (int, float))
        ]
        confidences = [
            float(record.get("confidence", 0.0))
            for record in records
            if isinstance(record.get("confidence"), (int, float))
        ]
        representative = detected_records[0] if detected_records else records[0]
        aggregate[event_name] = {
            "detected": bool(detected_records),
            "threshold_value": _mean(threshold_values) if threshold_values else None,
            "time_minute": _mean(times) if times else None,
            "detection_method": representative.get("detection_method", ""),
            "confidence": _mean(confidences),
            "details": copy.deepcopy(representative.get("details", {})),
            "seed_support": len(detected_records),
        }
    return aggregate


def _aggregate_summary_dicts(run_summaries: list[dict[str, object]], key: str) -> dict[str, object]:
    values = [summary.get(key, {}) for summary in run_summaries if isinstance(summary.get(key), dict)]
    if not values:
        return {}
    keys = sorted({inner_key for value in values for inner_key in value})
    aggregate: dict[str, object] = {}
    for inner_key in keys:
        numeric_values = [
            float(value[inner_key])
            for value in values
            if isinstance(value.get(inner_key), (int, float))
        ]
        if numeric_values:
            aggregate[inner_key] = _mean(numeric_values)
        else:
            aggregate[inner_key] = _majority_value([value.get(inner_key) for value in values])
    return aggregate


def _confidence_score(
    *,
    run_summaries: list[dict[str, object]],
    dominant_failure_mode: str,
    phase_detection: dict[str, object],
    variance: dict[str, float],
) -> float:
    mode_consensus = sum(
        1 for summary in run_summaries if summary.get("dominant_failure_mode") == dominant_failure_mode
    ) / max(len(run_summaries), 1)
    event_confidences = [
        float(record.get("confidence", 0.0))
        for record in phase_detection.values()
        if isinstance(record, dict)
    ]
    variance_penalty = 1.0 / (1.0 + sum(variance.values()))
    return max(
        0.0,
        min(
            1.0,
            0.45 * mode_consensus
            + 0.35 * (_mean(event_confidences) if event_confidences else 0.0)
            + 0.20 * variance_penalty,
        ),
    )


def _aggregate_tranche_slice_result(
    tranche: TrancheDefinition,
    slice_definition,
    simulation_results: list,
) -> TrancheSliceResult:
    run_summaries = [result.summary for result in simulation_results]
    dominant_failure_mode = str(
        _majority_value([summary.get("dominant_failure_mode", "CORRIDOR_CONGESTION") for summary in run_summaries])
    )
    legacy_mechanism = str(
        _majority_value([summary["first_dominant_failure_mechanism"] for summary in run_summaries])
    )
    exit_times = [
        float(summary["first_safe_region_exit_time"])
        for summary in run_summaries
        if summary.get("first_safe_region_exit_time") not in {None, ""}
    ]
    variance = {
        "time_to_first_failure": _variance(exit_times),
        "peak_corridor_load_ratio": _variance(
            [float(summary["peak_corridor_load_ratio"]) for summary in run_summaries]
        ),
        "peak_queue_ratio": _variance(
            [float(summary["peak_queue_ratio"]) for summary in run_summaries]
        ),
        "dominant_failure_mode_confidence": _variance(
            [float(summary.get("dominant_failure_mode_confidence", 0.0)) for summary in run_summaries]
        ),
    }
    phase_detection = _aggregate_phase_detection(run_summaries)
    physics_summary = _aggregate_summary_dicts(run_summaries, "physics_summary")
    admissibility_summary = _aggregate_summary_dicts(run_summaries, "admissibility_summary")
    event_chains = [
        copy.deepcopy(summary.get("event_chain", {}))
        for summary in run_summaries
        if isinstance(summary.get("event_chain"), dict)
    ]
    confidence_score = _confidence_score(
        run_summaries=run_summaries,
        dominant_failure_mode=dominant_failure_mode,
        phase_detection=phase_detection,
        variance=variance,
    )
    final_trust_distributions = [
        summary.get("trust_state_distribution_over_time", [{}])[-1]
        for summary in run_summaries
        if isinstance(summary.get("trust_state_distribution_over_time"), list)
        and summary["trust_state_distribution_over_time"]
    ]
    trusted_active_fraction = 1.0
    if final_trust_distributions:
        trusted_active_fractions: list[float] = []
        for distribution in final_trust_distributions:
            active_total = sum(
                int(distribution.get(key, 0))
                for key in ("trusted", "degraded", "unknown", "quarantined", "revoked")
            )
            trusted_active_fractions.append(
                int(distribution.get("trusted", 0)) / active_total if active_total else 1.0
            )
        trusted_active_fraction = _mean(trusted_active_fractions)

    representative = simulation_results[0]
    return TrancheSliceResult(
        slice_id=slice_definition.slice_id,
        tranche_name=tranche.tranche_name,
        seed=slice_definition.seed,
        resolved_params=copy.deepcopy(slice_definition.resolved_params),
        first_dominant_failure_mechanism=legacy_mechanism,
        dominant_failure_mode=dominant_failure_mode,
        time_to_first_failure=(_mean(exit_times) if exit_times else None),
        safe_region_exit_time=(_mean(exit_times) if exit_times else None),
        safe_region_exit_cause=str(
            _majority_value([summary.get("first_safe_region_exit_cause", "") for summary in run_summaries])
        ),
        degraded_mode_dwell_time=_mean([float(summary["degraded_mode_dwell_time"]) for summary in run_summaries]),
        trust_metrics_snapshot=TrustMetricsSnapshot(
            unsafe_admission_count=int(round(_mean([float(summary["unsafe_admission_count"]) for summary in run_summaries]))),
            quarantine_count=int(round(_mean([float(summary["quarantine_count"]) for summary in run_summaries]))),
            revocation_count=int(round(_mean([float(summary["revocation_count"]) for summary in run_summaries]))),
            trusted_active_fraction=trusted_active_fraction,
            operator_intervention_count=int(
                round(_mean([float(summary["operator_intervention_count"]) for summary in run_summaries]))
            ),
            trust_induced_throughput_loss=_mean(
                [float(summary["trust_induced_throughput_loss"]) for summary in run_summaries]
            ),
        ),
        comms_metrics_snapshot=CommsMetricsSnapshot(
            information_age_mean=_mean([float(summary["information_age_mean"]) for summary in run_summaries]),
            information_age_max=_mean([float(summary["information_age_max"]) for summary in run_summaries]),
            stale_state_exposure_minutes=_mean(
                [float(summary["stale_state_exposure_minutes"]) for summary in run_summaries]
            ),
            delayed_reroute_count=int(round(_mean([float(summary["delayed_reroute_count"]) for summary in run_summaries]))),
            lost_link_fallback_activations=int(
                round(_mean([float(summary["lost_link_fallback_activations"]) for summary in run_summaries]))
            ),
            reservation_invalidations=int(
                round(_mean([float(summary["reservation_invalidations"]) for summary in run_summaries]))
            ),
        ),
        contingency_metrics_snapshot=ContingencyMetricsSnapshot(
            reachable_landing_option_mean=_mean(
                [float(summary["reachable_landing_option_mean"]) for summary in run_summaries]
            ),
            no_admissible_landing_events=int(
                round(_mean([float(summary["no_admissible_landing_events"]) for summary in run_summaries]))
            ),
            contingency_node_utilization=_mean(
                [float(summary["contingency_node_utilization"]) for summary in run_summaries]
            ),
            contingency_saturation_duration=_mean(
                [float(summary["contingency_saturation_duration"]) for summary in run_summaries]
            ),
            reserve_margin_mean=_mean([float(summary["reserve_margin_mean"]) for summary in run_summaries]),
            reserve_margin_min=_mean([float(summary["reserve_margin_min"]) for summary in run_summaries]),
        ),
        throughput_metrics_snapshot=ThroughputMetricsSnapshot(
            completed_vehicles=int(round(_mean([float(summary["completed_vehicles"]) for summary in run_summaries]))),
            incomplete_vehicles=int(round(_mean([float(summary["incomplete_vehicles"]) for summary in run_summaries]))),
            avg_queue_length=_mean([float(summary["avg_queue_length"]) for summary in run_summaries]),
            peak_avg_queue_length=_mean([float(summary["peak_avg_queue_length"]) for summary in run_summaries]),
            peak_corridor_load_ratio=_mean(
                [float(summary["peak_corridor_load_ratio"]) for summary in run_summaries]
            ),
            peak_node_utilization_ratio=_mean(
                [float(summary["peak_node_utilization_ratio"]) for summary in run_summaries]
            ),
            peak_queue_ratio=_mean([float(summary["peak_queue_ratio"]) for summary in run_summaries]),
            mean_corridor_speed=_mean([float(summary["mean_corridor_speed"]) for summary in run_summaries]),
        ),
        artifact_paths=ArtifactPaths(
            output_dir=representative.output_dir,
            metrics_path=representative.metrics_path,
            event_log_path=representative.event_log_path,
            run_summary_path=representative.run_summary_path,
            threshold_summary_path=representative.threshold_summary_path,
            plot_paths=tuple(representative.plot_paths),
        ),
        phase_detection=phase_detection,
        physics_summary=physics_summary,
        admissibility_summary=admissibility_summary,
        mean_metrics={
            "seed_values": [summary.get("seed") for summary in run_summaries],
            "dominant_failure_mode": dominant_failure_mode,
            "legacy_failure_mechanism": legacy_mechanism,
            "phase_detection": phase_detection,
            "event_chain": _majority_value(event_chains) if event_chains else {},
            "seed_event_chains": event_chains,
            "physics_summary": physics_summary,
            "admissibility_summary": admissibility_summary,
        },
        variance=variance,
        confidence_score=confidence_score,
        seed_count=len(simulation_results),
    )


def execute_tranche_slice(
    tranche: TrancheDefinition,
    slice_definition,
    *,
    output_dir: Path,
) -> TrancheSliceResult:
    simulation_results = []
    for replicate_index in range(tranche.seed_policy.replicates):
        config = load_simulation_config(slice_definition.base_config_path)
        _apply_resolved_params(config, tranche, slice_definition.resolved_params)
        config.output_root = output_dir / "runs"
        config.seed = _replicate_seed(slice_definition.seed, replicate_index)
        config.scenario_name = f"{slice_definition.slice_id}_seed{replicate_index + 1:02d}"
        config.description = (
            f"{tranche.description} [{slice_definition.slice_id}] "
            f"(replicate {replicate_index + 1}/{tranche.seed_policy.replicates})"
        )
        simulation_results.append(SimulationEngine(config).run())
    return _aggregate_tranche_slice_result(tranche, slice_definition, simulation_results)


def _write_tranche_outputs(
    tranche: TrancheDefinition,
    output_dir: Path,
    slice_results: list[TrancheSliceResult],
    *,
    adaptive_metadata: dict[str, object] | None = None,
) -> TrancheRunResult:
    aggregate_csv_path = write_aggregate_csv(output_dir, tranche.tranche_name, slice_results)
    slice_results_json_path = write_slice_results_json(
        output_dir,
        tranche,
        slice_results,
        adaptive_payload=adaptive_metadata,
    )
    phase_boundaries_json_path = write_phase_boundaries_json(output_dir, tranche.tranche_name, slice_results)
    phase_map_json_path = write_phase_map_json(output_dir, tranche.tranche_name, slice_results)
    transition_regions_json_path = write_transition_regions_json(output_dir, tranche.tranche_name, slice_results)
    threshold_estimates_json_path = write_threshold_estimates_json(
        output_dir,
        tranche.tranche_name,
        slice_results,
        adaptive_payload=adaptive_metadata,
    )
    threshold_ledger_json_path = write_threshold_ledger_json(
        output_dir,
        tranche.tranche_name,
        slice_results,
        adaptive_payload=adaptive_metadata,
    )
    promotion_decisions_json_path = write_promotion_decisions_json(
        output_dir,
        tranche.tranche_name,
        slice_results,
        adaptive_payload=adaptive_metadata,
    )
    admissibility_overlay_json_path = write_admissibility_overlay_json(output_dir, tranche.tranche_name, slice_results)
    convergence_report_json_path = write_convergence_report_json(
        output_dir,
        tranche.tranche_name,
        slice_results,
        adaptive_payload=adaptive_metadata,
    )
    return TrancheRunResult(
        tranche_name=tranche.tranche_name,
        output_dir=output_dir,
        aggregate_csv_path=aggregate_csv_path,
        slice_results_json_path=slice_results_json_path,
        phase_boundaries_json_path=phase_boundaries_json_path,
        slice_results=slice_results,
        phase_map_json_path=phase_map_json_path,
        transition_regions_json_path=transition_regions_json_path,
        threshold_estimates_json_path=threshold_estimates_json_path,
        threshold_ledger_json_path=threshold_ledger_json_path,
        promotion_decisions_json_path=promotion_decisions_json_path,
        admissibility_overlay_json_path=admissibility_overlay_json_path,
        convergence_report_json_path=convergence_report_json_path,
        adaptive_metadata=adaptive_metadata,
    )


def _write_global_outputs(
    output_dir: Path,
    tranche_results: dict[str, list[TrancheSliceResult]],
    *,
    tranche_outputs: dict[str, Path],
) -> GlobalAnalysisResult:
    matrix_path, comparison_path, summary_path = write_cross_tranche_outputs(output_dir, tranche_results)
    global_phase_map_path = write_global_phase_map_json(output_dir, tranche_results)
    cross_tranche_thresholds_path = write_cross_tranche_thresholds_json(output_dir, tranche_results)
    contradictions_path = write_contradictions_json(output_dir, tranche_results)
    cross_tranche_threshold_ledger_path = write_cross_tranche_threshold_ledger_json(output_dir, tranche_results)
    cross_tranche_promotion_decisions_path = write_cross_tranche_promotion_decisions_json(
        output_dir,
        tranche_results,
    )
    return GlobalAnalysisResult(
        output_dir=output_dir,
        mechanism_dominance_matrix_path=matrix_path,
        tranche_comparison_path=comparison_path,
        cross_tranche_summary_path=summary_path,
        tranche_outputs=tranche_outputs,
        global_phase_map_path=global_phase_map_path,
        cross_tranche_thresholds_path=cross_tranche_thresholds_path,
        contradictions_path=contradictions_path,
        cross_tranche_threshold_ledger_path=cross_tranche_threshold_ledger_path,
        cross_tranche_promotion_decisions_path=cross_tranche_promotion_decisions_path,
    )


def run_tranche(
    tranche_name: str,
    *,
    output_root: Path | None = None,
    max_slices: int | None = None,
    run_id: str | None = None,
    adaptive: bool = False,
    max_iterations: int = 8,
    convergence_threshold: float = 0.3,
) -> TrancheRunResult:
    tranche = get_tranche(tranche_name)
    if max_slices is not None and max_slices < tranche.minimum_slice_count:
        raise ValueError(
            f"Tranche '{tranche_name}' requires at least {tranche.minimum_slice_count} slices; "
            f"requested {max_slices}"
        )
    root = _resolve_output_root(tranche, output_root)
    batch_id = run_id or _timestamp_id()
    output_dir = (root / "sweeps" / f"tranche_{tranche_name}_{batch_id}").resolve()
    output_dir.mkdir(parents=True, exist_ok=False)

    adaptive_metadata: dict[str, object] | None = None
    if adaptive:
        adaptive_run = adaptive_sweep(
            tranche,
            execute_slice=lambda active_tranche, slice_definition: execute_tranche_slice(
                active_tranche,
                slice_definition,
                output_dir=output_dir,
            ),
            max_iterations=max_iterations,
            convergence_threshold=convergence_threshold,
            max_slices=max_slices,
        )
        slice_results = adaptive_run.slice_results
        adaptive_metadata = adaptive_run.adaptive_payload
    else:
        slice_definitions = generate_tranche_slices(tranche, max_slices=max_slices)
        slice_results = [
            execute_tranche_slice(tranche, slice_definition, output_dir=output_dir)
            for slice_definition in slice_definitions
        ]

    return _write_tranche_outputs(
        tranche,
        output_dir,
        slice_results,
        adaptive_metadata=adaptive_metadata,
    )


def run_all_tranches(
    *,
    output_root: Path | None = None,
    max_slices: int | None = None,
    adaptive: bool = False,
    max_iterations: int = 8,
    convergence_threshold: float = 0.3,
) -> tuple[list[TrancheRunResult], GlobalAnalysisResult]:
    run_id = _timestamp_id()
    tranche_names = sorted(BUILT_IN_TRANCHES)
    root = _resolve_output_root(get_tranche(tranche_names[0]), output_root)

    tranche_runs = [
        run_tranche(
            name,
            output_root=root,
            max_slices=max_slices,
            run_id=run_id,
            adaptive=adaptive,
            max_iterations=max_iterations,
            convergence_threshold=convergence_threshold,
        )
        for name in tranche_names
    ]
    global_output_dir = (root / "sweeps" / f"global_{run_id}").resolve()
    global_output_dir.mkdir(parents=True, exist_ok=False)
    tranche_results = {run.tranche_name: run.slice_results for run in tranche_runs}
    global_result = _write_global_outputs(
        global_output_dir,
        tranche_results,
        tranche_outputs={run.tranche_name: run.output_dir for run in tranche_runs},
    )
    manifest_path = global_output_dir / "batch_manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "run_id": run_id,
                "global_output_dir": str(global_output_dir),
                "adaptive": adaptive,
                "max_iterations": max_iterations if adaptive else None,
                "convergence_threshold": convergence_threshold if adaptive else None,
                "tranche_outputs": {
                    run.tranche_name: str(run.output_dir)
                    for run in tranche_runs
                },
            },
            handle,
            indent=2,
        )

    return tranche_runs, global_result


def analyze_only(path: str | Path) -> TrancheRunResult | GlobalAnalysisResult:
    target = Path(path).resolve()
    if target.is_file() and target.name == "slice_results.json":
        payload_path = target
    elif target.is_dir() and (target / "slice_results.json").exists():
        payload_path = target / "slice_results.json"
    else:
        payload_path = None

    if payload_path is not None:
        payload, results = load_slice_results_payload(payload_path)
        tranche = _tranche_from_payload(payload)
        output_dir = payload_path.parent
        analysis = _write_tranche_outputs(
            tranche,
            output_dir,
            results,
            adaptive_metadata=payload.get("adaptive") if isinstance(payload.get("adaptive"), dict) else None,
        )
        analysis.slice_results_json_path = payload_path
        return analysis

    manifest_path = target if target.is_file() and target.name == "batch_manifest.json" else target / "batch_manifest.json"
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        tranche_outputs = {
            tranche_name: Path(path_str).resolve()
            for tranche_name, path_str in manifest["tranche_outputs"].items()
        }
        tranche_results: dict[str, list[TrancheSliceResult]] = {}
        for tranche_name, tranche_dir in tranche_outputs.items():
            payload, results = load_slice_results_payload(tranche_dir / "slice_results.json")
            tranche = _tranche_from_payload(payload)
            _write_tranche_outputs(
                tranche,
                tranche_dir,
                results,
                adaptive_metadata=payload.get("adaptive") if isinstance(payload.get("adaptive"), dict) else None,
            )
            tranche_results[tranche_name] = results
        global_output_dir = manifest_path.parent if manifest_path.is_file() else target
        return _write_global_outputs(
            global_output_dir,
            tranche_results,
            tranche_outputs=tranche_outputs,
        )

    raise FileNotFoundError(
        f"Expected a tranche directory with slice_results.json or a global directory with batch_manifest.json at {target}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run AVN legacy sweeps or Phase 2C tranche exploration.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--list-tranches", action="store_true", help="List built-in Phase 2C tranches.")
    group.add_argument("--tranche", choices=sorted(BUILT_IN_TRANCHES), help="Run a built-in tranche.")
    group.add_argument("--all", action="store_true", dest="run_all", help="Run all built-in tranches and global analysis.")
    group.add_argument("--analyze-only", type=Path, help="Recompute tranche or global analysis from existing artifacts.")
    parser.add_argument("--output-root", type=Path, help="Override the default outputs root.")
    parser.add_argument("--max-slices", type=int, help="Optional deterministic cap on slice execution count.")
    parser.add_argument("--adaptive", action="store_true", help="Enable the Adaptive Sweep Engine.")
    parser.add_argument("--max-iterations", type=int, default=8, help="Maximum adaptive refinement iterations.")
    parser.add_argument(
        "--convergence-threshold",
        type=float,
        default=0.3,
        help="Convergence threshold for adaptive entropy and boundary shift.",
    )
    parser.add_argument(
        "legacy_config",
        nargs="?",
        type=Path,
        help="Optional legacy Phase 2B sweep config path for compatibility mode.",
    )
    args = parser.parse_args(argv)

    if args.max_slices is not None and args.max_slices <= 0:
        parser.error("--max-slices must be a positive integer")
    if args.max_iterations <= 0:
        parser.error("--max-iterations must be a positive integer")
    if args.convergence_threshold <= 0.0:
        parser.error("--convergence-threshold must be positive")

    if args.list_tranches:
        for name in sorted(BUILT_IN_TRANCHES):
            tranche = BUILT_IN_TRANCHES[name]
            print(f"{name}: {tranche.slice_count} slices")
            print(f"  {tranche.description}")
        return 0

    if args.tranche:
        result = run_tranche(
            args.tranche,
            output_root=args.output_root,
            max_slices=args.max_slices,
            adaptive=args.adaptive,
            max_iterations=args.max_iterations,
            convergence_threshold=args.convergence_threshold,
        )
        print(f"Tranche: {result.tranche_name}")
        print(f"Output directory: {result.output_dir}")
        print(f"Aggregate CSV: {result.aggregate_csv_path}")
        print(f"Slice results JSON: {result.slice_results_json_path}")
        print(f"Phase boundaries JSON: {result.phase_boundaries_json_path}")
        if result.phase_map_json_path is not None:
            print(f"Phase map JSON: {result.phase_map_json_path}")
        if result.transition_regions_json_path is not None:
            print(f"Transition regions JSON: {result.transition_regions_json_path}")
        if result.threshold_estimates_json_path is not None:
            print(f"Threshold estimates JSON: {result.threshold_estimates_json_path}")
        if result.threshold_ledger_json_path is not None:
            print(f"Threshold ledger JSON: {result.threshold_ledger_json_path}")
        if result.promotion_decisions_json_path is not None:
            print(f"Promotion decisions JSON: {result.promotion_decisions_json_path}")
        if result.admissibility_overlay_json_path is not None:
            print(f"Admissibility overlay JSON: {result.admissibility_overlay_json_path}")
        if result.convergence_report_json_path is not None:
            print(f"Convergence report JSON: {result.convergence_report_json_path}")
        print(f"Executed slices: {len(result.slice_results)}")
        if args.adaptive and result.adaptive_metadata is not None:
            print(f"Adaptive iterations: {len(result.adaptive_metadata.get('iterations', []))}")
            print(f"Adaptive stopping reason: {result.adaptive_metadata.get('stopping_reason', 'unknown')}")
        return 0

    if args.run_all:
        tranche_runs, global_result = run_all_tranches(
            output_root=args.output_root,
            max_slices=args.max_slices,
            adaptive=args.adaptive,
            max_iterations=args.max_iterations,
            convergence_threshold=args.convergence_threshold,
        )
        print(f"Executed tranches: {', '.join(run.tranche_name for run in tranche_runs)}")
        print(f"Global output directory: {global_result.output_dir}")
        print(f"Mechanism dominance matrix: {global_result.mechanism_dominance_matrix_path}")
        print(f"Tranche comparison JSON: {global_result.tranche_comparison_path}")
        print(f"Cross-tranche summary JSON: {global_result.cross_tranche_summary_path}")
        if global_result.global_phase_map_path is not None:
            print(f"Global phase map JSON: {global_result.global_phase_map_path}")
        if global_result.cross_tranche_thresholds_path is not None:
            print(f"Cross-tranche thresholds JSON: {global_result.cross_tranche_thresholds_path}")
        if global_result.contradictions_path is not None:
            print(f"Contradictions JSON: {global_result.contradictions_path}")
        if global_result.cross_tranche_threshold_ledger_path is not None:
            print(f"Cross-tranche threshold ledger JSON: {global_result.cross_tranche_threshold_ledger_path}")
        if global_result.cross_tranche_promotion_decisions_path is not None:
            print(
                "Cross-tranche promotion decisions JSON: "
                f"{global_result.cross_tranche_promotion_decisions_path}"
            )
        return 0

    if args.analyze_only:
        result = analyze_only(args.analyze_only)
        if isinstance(result, TrancheRunResult):
            print(f"Re-analyzed tranche: {result.tranche_name}")
            print(f"Output directory: {result.output_dir}")
            print(f"Aggregate CSV: {result.aggregate_csv_path}")
            print(f"Phase boundaries JSON: {result.phase_boundaries_json_path}")
            if result.phase_map_json_path is not None:
                print(f"Phase map JSON: {result.phase_map_json_path}")
            if result.transition_regions_json_path is not None:
                print(f"Transition regions JSON: {result.transition_regions_json_path}")
            if result.threshold_estimates_json_path is not None:
                print(f"Threshold estimates JSON: {result.threshold_estimates_json_path}")
            if result.threshold_ledger_json_path is not None:
                print(f"Threshold ledger JSON: {result.threshold_ledger_json_path}")
            if result.promotion_decisions_json_path is not None:
                print(f"Promotion decisions JSON: {result.promotion_decisions_json_path}")
            if result.admissibility_overlay_json_path is not None:
                print(f"Admissibility overlay JSON: {result.admissibility_overlay_json_path}")
            if result.convergence_report_json_path is not None:
                print(f"Convergence report JSON: {result.convergence_report_json_path}")
        else:
            print(f"Re-analyzed global outputs: {result.output_dir}")
            print(f"Mechanism dominance matrix: {result.mechanism_dominance_matrix_path}")
            print(f"Tranche comparison JSON: {result.tranche_comparison_path}")
            print(f"Cross-tranche summary JSON: {result.cross_tranche_summary_path}")
            if result.global_phase_map_path is not None:
                print(f"Global phase map JSON: {result.global_phase_map_path}")
            if result.cross_tranche_thresholds_path is not None:
                print(f"Cross-tranche thresholds JSON: {result.cross_tranche_thresholds_path}")
            if result.contradictions_path is not None:
                print(f"Contradictions JSON: {result.contradictions_path}")
            if result.cross_tranche_threshold_ledger_path is not None:
                print(f"Cross-tranche threshold ledger JSON: {result.cross_tranche_threshold_ledger_path}")
            if result.cross_tranche_promotion_decisions_path is not None:
                print(
                    "Cross-tranche promotion decisions JSON: "
                    f"{result.cross_tranche_promotion_decisions_path}"
                )
        return 0

    if args.legacy_config:
        result = run_phase2b_sweep(args.legacy_config, output_root=args.output_root)
        print(f"Legacy sweep directory: {result.output_dir}")
        print(f"Aggregate CSV: {result.aggregate_csv_path}")
        print(f"Summary JSON: {result.summary_json_path}")
        print("Labels: " + ", ".join(str(row["label"]) for row in result.rows))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
