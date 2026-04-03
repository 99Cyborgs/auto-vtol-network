from __future__ import annotations

import copy
import csv
import json
from hashlib import sha256
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from avn.phase_space import (
    build_admissibility_overlay,
    build_convergence_report,
    build_cross_tranche_thresholds,
    build_threshold_estimates,
    detect_transition_regions,
    phase_map_payload,
    phase_points_from_slice_results,
)
from avn.sweep_tranches import ParamValue, TrancheDefinition, TrancheSlice

if TYPE_CHECKING:
    from avn.simulation.engine import SimulationResult


FAILURE_MECHANISMS = (
    "CORRIDOR_CONGESTION",
    "NODE_SATURATION",
    "REROUTE_CASCADE",
    "WEATHER_COLLAPSE",
    "COMMS_FAILURE",
    "TRUST_FAILURE",
)

LEGACY_TO_FAILURE_MODE = {
    "corridor_capacity_exceeded": "CORRIDOR_CONGESTION",
    "node_service_collapse": "NODE_SATURATION",
    "stale_information_instability": "COMMS_FAILURE",
    "trust_breakdown": "TRUST_FAILURE",
    "contingency_unreachable": "NODE_SATURATION",
    "coupled_failure_indeterminate": "REROUTE_CASCADE",
}


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return statistics.fmean(values)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def _sort_scalar(value: object) -> tuple[int, object]:
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        return (1, float(value))
    return (2, str(value))


def _stable_hash(payload: object) -> str:
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _slice_mode(result: TrancheSliceResult) -> str:
    if result.dominant_failure_mode:
        return result.dominant_failure_mode
    return LEGACY_TO_FAILURE_MODE.get(
        result.first_dominant_failure_mechanism,
        result.first_dominant_failure_mechanism,
    )


def _compute_tranche_slice_replay_hash(result: TrancheSliceResult) -> str:
    return _stable_hash(
        {
            "slice_id": result.slice_id,
            "tranche_name": result.tranche_name,
            "seed": result.seed,
            "resolved_params": result.resolved_params,
            "first_dominant_failure_mechanism": result.first_dominant_failure_mechanism,
            "dominant_failure_mode": result.dominant_failure_mode,
            "time_to_first_failure": result.time_to_first_failure,
            "safe_region_exit_time": result.safe_region_exit_time,
            "safe_region_exit_cause": result.safe_region_exit_cause,
            "degraded_mode_dwell_time": result.degraded_mode_dwell_time,
            "trust_metrics_snapshot": result.trust_metrics_snapshot.to_dict(),
            "comms_metrics_snapshot": result.comms_metrics_snapshot.to_dict(),
            "contingency_metrics_snapshot": result.contingency_metrics_snapshot.to_dict(),
            "throughput_metrics_snapshot": result.throughput_metrics_snapshot.to_dict(),
            "phase_detection": result.phase_detection,
            "physics_summary": result.physics_summary,
            "admissibility_summary": result.admissibility_summary,
            "variance": result.variance,
            "confidence_score": result.confidence_score,
            "seed_count": result.seed_count,
        }
    )


@dataclass(slots=True)
class TrustMetricsSnapshot:
    unsafe_admission_count: int
    quarantine_count: int
    revocation_count: int
    trusted_active_fraction: float
    operator_intervention_count: int
    trust_induced_throughput_loss: float

    def to_dict(self) -> dict[str, float | int]:
        return {
            "unsafe_admission_count": self.unsafe_admission_count,
            "quarantine_count": self.quarantine_count,
            "revocation_count": self.revocation_count,
            "trusted_active_fraction": self.trusted_active_fraction,
            "operator_intervention_count": self.operator_intervention_count,
            "trust_induced_throughput_loss": self.trust_induced_throughput_loss,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrustMetricsSnapshot:
        return cls(
            unsafe_admission_count=int(payload["unsafe_admission_count"]),
            quarantine_count=int(payload["quarantine_count"]),
            revocation_count=int(payload["revocation_count"]),
            trusted_active_fraction=float(payload["trusted_active_fraction"]),
            operator_intervention_count=int(payload["operator_intervention_count"]),
            trust_induced_throughput_loss=float(payload["trust_induced_throughput_loss"]),
        )


@dataclass(slots=True)
class CommsMetricsSnapshot:
    information_age_mean: float
    information_age_max: float
    stale_state_exposure_minutes: float
    delayed_reroute_count: int
    lost_link_fallback_activations: int
    reservation_invalidations: int

    def to_dict(self) -> dict[str, float | int]:
        return {
            "information_age_mean": self.information_age_mean,
            "information_age_max": self.information_age_max,
            "stale_state_exposure_minutes": self.stale_state_exposure_minutes,
            "delayed_reroute_count": self.delayed_reroute_count,
            "lost_link_fallback_activations": self.lost_link_fallback_activations,
            "reservation_invalidations": self.reservation_invalidations,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CommsMetricsSnapshot:
        return cls(
            information_age_mean=float(payload["information_age_mean"]),
            information_age_max=float(payload["information_age_max"]),
            stale_state_exposure_minutes=float(payload["stale_state_exposure_minutes"]),
            delayed_reroute_count=int(payload["delayed_reroute_count"]),
            lost_link_fallback_activations=int(payload["lost_link_fallback_activations"]),
            reservation_invalidations=int(payload["reservation_invalidations"]),
        )


@dataclass(slots=True)
class ContingencyMetricsSnapshot:
    reachable_landing_option_mean: float
    no_admissible_landing_events: int
    contingency_node_utilization: float
    contingency_saturation_duration: float
    reserve_margin_mean: float
    reserve_margin_min: float

    def to_dict(self) -> dict[str, float | int]:
        return {
            "reachable_landing_option_mean": self.reachable_landing_option_mean,
            "no_admissible_landing_events": self.no_admissible_landing_events,
            "contingency_node_utilization": self.contingency_node_utilization,
            "contingency_saturation_duration": self.contingency_saturation_duration,
            "reserve_margin_mean": self.reserve_margin_mean,
            "reserve_margin_min": self.reserve_margin_min,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ContingencyMetricsSnapshot:
        return cls(
            reachable_landing_option_mean=float(payload["reachable_landing_option_mean"]),
            no_admissible_landing_events=int(payload["no_admissible_landing_events"]),
            contingency_node_utilization=float(payload["contingency_node_utilization"]),
            contingency_saturation_duration=float(payload["contingency_saturation_duration"]),
            reserve_margin_mean=float(payload["reserve_margin_mean"]),
            reserve_margin_min=float(payload["reserve_margin_min"]),
        )


@dataclass(slots=True)
class ThroughputMetricsSnapshot:
    completed_vehicles: int
    incomplete_vehicles: int
    avg_queue_length: float
    peak_avg_queue_length: float
    peak_corridor_load_ratio: float
    peak_node_utilization_ratio: float
    peak_queue_ratio: float
    mean_corridor_speed: float

    def to_dict(self) -> dict[str, float | int]:
        return {
            "completed_vehicles": self.completed_vehicles,
            "incomplete_vehicles": self.incomplete_vehicles,
            "avg_queue_length": self.avg_queue_length,
            "peak_avg_queue_length": self.peak_avg_queue_length,
            "peak_corridor_load_ratio": self.peak_corridor_load_ratio,
            "peak_node_utilization_ratio": self.peak_node_utilization_ratio,
            "peak_queue_ratio": self.peak_queue_ratio,
            "mean_corridor_speed": self.mean_corridor_speed,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ThroughputMetricsSnapshot:
        return cls(
            completed_vehicles=int(payload["completed_vehicles"]),
            incomplete_vehicles=int(payload["incomplete_vehicles"]),
            avg_queue_length=float(payload["avg_queue_length"]),
            peak_avg_queue_length=float(payload["peak_avg_queue_length"]),
            peak_corridor_load_ratio=float(payload["peak_corridor_load_ratio"]),
            peak_node_utilization_ratio=float(payload["peak_node_utilization_ratio"]),
            peak_queue_ratio=float(payload["peak_queue_ratio"]),
            mean_corridor_speed=float(payload["mean_corridor_speed"]),
        )


@dataclass(slots=True)
class ArtifactPaths:
    output_dir: Path
    metrics_path: Path
    event_log_path: Path
    run_summary_path: Path
    threshold_summary_path: Path
    plot_paths: tuple[Path, ...]

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir).resolve()
        self.metrics_path = Path(self.metrics_path).resolve()
        self.event_log_path = Path(self.event_log_path).resolve()
        self.run_summary_path = Path(self.run_summary_path).resolve()
        self.threshold_summary_path = Path(self.threshold_summary_path).resolve()
        self.plot_paths = tuple(Path(path).resolve() for path in self.plot_paths)

    def to_dict(self) -> dict[str, object]:
        return {
            "output_dir": str(self.output_dir),
            "metrics_path": str(self.metrics_path),
            "event_log_path": str(self.event_log_path),
            "run_summary_path": str(self.run_summary_path),
            "threshold_summary_path": str(self.threshold_summary_path),
            "plot_paths": [str(path) for path in self.plot_paths],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ArtifactPaths:
        return cls(
            output_dir=Path(str(payload["output_dir"])),
            metrics_path=Path(str(payload["metrics_path"])),
            event_log_path=Path(str(payload["event_log_path"])),
            run_summary_path=Path(str(payload["run_summary_path"])),
            threshold_summary_path=Path(str(payload["threshold_summary_path"])),
            plot_paths=tuple(Path(str(path)) for path in payload.get("plot_paths", [])),
        )


@dataclass(slots=True)
class TrancheSliceResult:
    slice_id: str
    tranche_name: str
    seed: int
    resolved_params: dict[str, ParamValue]
    first_dominant_failure_mechanism: str
    time_to_first_failure: float | None
    safe_region_exit_time: float | None
    safe_region_exit_cause: str
    degraded_mode_dwell_time: float
    trust_metrics_snapshot: TrustMetricsSnapshot
    comms_metrics_snapshot: CommsMetricsSnapshot
    contingency_metrics_snapshot: ContingencyMetricsSnapshot
    throughput_metrics_snapshot: ThroughputMetricsSnapshot
    artifact_paths: ArtifactPaths
    dominant_failure_mode: str = ""
    phase_detection: dict[str, Any] = None
    physics_summary: dict[str, Any] = None
    admissibility_summary: dict[str, Any] = None
    mean_metrics: dict[str, Any] = None
    variance: dict[str, float] = None
    confidence_score: float = 0.0
    seed_count: int = 1
    replay_hash: str | None = None

    def __post_init__(self) -> None:
        if self.phase_detection is None:
            self.phase_detection = {}
        if self.physics_summary is None:
            self.physics_summary = {}
        if self.admissibility_summary is None:
            self.admissibility_summary = {}
        if self.mean_metrics is None:
            self.mean_metrics = {}
        if self.variance is None:
            self.variance = {}
        if self.replay_hash is None:
            self.replay_hash = _compute_tranche_slice_replay_hash(self)

    def to_dict(self) -> dict[str, object]:
        return {
            "slice_id": self.slice_id,
            "tranche_name": self.tranche_name,
            "seed": self.seed,
            "resolved_params": copy.deepcopy(self.resolved_params),
            "first_dominant_failure_mechanism": self.first_dominant_failure_mechanism,
            "dominant_failure_mode": self.dominant_failure_mode,
            "time_to_first_failure": self.time_to_first_failure,
            "safe_region_exit_time": self.safe_region_exit_time,
            "safe_region_exit_cause": self.safe_region_exit_cause,
            "degraded_mode_dwell_time": self.degraded_mode_dwell_time,
            "trust_metrics_snapshot": self.trust_metrics_snapshot.to_dict(),
            "comms_metrics_snapshot": self.comms_metrics_snapshot.to_dict(),
            "contingency_metrics_snapshot": self.contingency_metrics_snapshot.to_dict(),
            "throughput_metrics_snapshot": self.throughput_metrics_snapshot.to_dict(),
            "artifact_paths": self.artifact_paths.to_dict(),
            "phase_detection": copy.deepcopy(self.phase_detection),
            "physics_summary": copy.deepcopy(self.physics_summary),
            "admissibility_summary": copy.deepcopy(self.admissibility_summary),
            "mean_metrics": copy.deepcopy(self.mean_metrics),
            "variance": dict(self.variance),
            "confidence_score": self.confidence_score,
            "seed_count": self.seed_count,
            "replay_hash": self.replay_hash,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TrancheSliceResult:
        return cls(
            slice_id=str(payload["slice_id"]),
            tranche_name=str(payload["tranche_name"]),
            seed=int(payload["seed"]),
            resolved_params=copy.deepcopy(payload["resolved_params"]),
            first_dominant_failure_mechanism=str(payload["first_dominant_failure_mechanism"]),
            dominant_failure_mode=str(payload.get("dominant_failure_mode", "")),
            time_to_first_failure=(
                None if payload.get("time_to_first_failure") is None else float(payload["time_to_first_failure"])
            ),
            safe_region_exit_time=(
                None if payload.get("safe_region_exit_time") is None else float(payload["safe_region_exit_time"])
            ),
            safe_region_exit_cause=str(payload.get("safe_region_exit_cause", "")),
            degraded_mode_dwell_time=float(payload["degraded_mode_dwell_time"]),
            trust_metrics_snapshot=TrustMetricsSnapshot.from_dict(payload["trust_metrics_snapshot"]),
            comms_metrics_snapshot=CommsMetricsSnapshot.from_dict(payload["comms_metrics_snapshot"]),
            contingency_metrics_snapshot=ContingencyMetricsSnapshot.from_dict(payload["contingency_metrics_snapshot"]),
            throughput_metrics_snapshot=ThroughputMetricsSnapshot.from_dict(payload["throughput_metrics_snapshot"]),
            artifact_paths=ArtifactPaths.from_dict(payload["artifact_paths"]),
            phase_detection=copy.deepcopy(payload.get("phase_detection", {})),
            physics_summary=copy.deepcopy(payload.get("physics_summary", {})),
            admissibility_summary=copy.deepcopy(payload.get("admissibility_summary", {})),
            mean_metrics=copy.deepcopy(payload.get("mean_metrics", {})),
            variance={str(key): float(value) for key, value in payload.get("variance", {}).items()},
            confidence_score=float(payload.get("confidence_score", 0.0)),
            seed_count=int(payload.get("seed_count", 1)),
            replay_hash=(str(payload["replay_hash"]) if payload.get("replay_hash") is not None else None),
        )


def build_slice_result(slice_definition: TrancheSlice, simulation_result: SimulationResult) -> TrancheSliceResult:
    summary = simulation_result.summary
    trust_distribution = summary.get("trust_state_distribution_over_time", [])
    last_trust_distribution = trust_distribution[-1] if trust_distribution else {}
    active_total = sum(
        int(last_trust_distribution.get(key, 0))
        for key in ("trusted", "degraded", "unknown", "quarantined", "revoked")
    )
    trusted_active_fraction = (
        int(last_trust_distribution.get("trusted", 0)) / active_total if active_total else 1.0
    )
    exit_time = summary.get("first_safe_region_exit_time")
    normalized_exit_time = None if exit_time in {None, ""} else float(exit_time)

    return TrancheSliceResult(
        slice_id=slice_definition.slice_id,
        tranche_name=slice_definition.tranche_name,
        seed=slice_definition.seed,
        resolved_params=copy.deepcopy(slice_definition.resolved_params),
        first_dominant_failure_mechanism=str(summary["first_dominant_failure_mechanism"]),
        dominant_failure_mode=str(summary.get("dominant_failure_mode", "CORRIDOR_CONGESTION")),
        time_to_first_failure=normalized_exit_time,
        safe_region_exit_time=normalized_exit_time,
        safe_region_exit_cause=str(summary.get("first_safe_region_exit_cause", "")),
        degraded_mode_dwell_time=float(summary["degraded_mode_dwell_time"]),
        trust_metrics_snapshot=TrustMetricsSnapshot(
            unsafe_admission_count=int(summary["unsafe_admission_count"]),
            quarantine_count=int(summary["quarantine_count"]),
            revocation_count=int(summary["revocation_count"]),
            trusted_active_fraction=trusted_active_fraction,
            operator_intervention_count=int(summary["operator_intervention_count"]),
            trust_induced_throughput_loss=float(summary["trust_induced_throughput_loss"]),
        ),
        comms_metrics_snapshot=CommsMetricsSnapshot(
            information_age_mean=float(summary["information_age_mean"]),
            information_age_max=float(summary["information_age_max"]),
            stale_state_exposure_minutes=float(summary["stale_state_exposure_minutes"]),
            delayed_reroute_count=int(summary["delayed_reroute_count"]),
            lost_link_fallback_activations=int(summary["lost_link_fallback_activations"]),
            reservation_invalidations=int(summary["reservation_invalidations"]),
        ),
        contingency_metrics_snapshot=ContingencyMetricsSnapshot(
            reachable_landing_option_mean=float(summary["reachable_landing_option_mean"]),
            no_admissible_landing_events=int(summary["no_admissible_landing_events"]),
            contingency_node_utilization=float(summary["contingency_node_utilization"]),
            contingency_saturation_duration=float(summary["contingency_saturation_duration"]),
            reserve_margin_mean=float(summary["reserve_margin_mean"]),
            reserve_margin_min=float(summary["reserve_margin_min"]),
        ),
        throughput_metrics_snapshot=ThroughputMetricsSnapshot(
            completed_vehicles=int(summary["completed_vehicles"]),
            incomplete_vehicles=int(summary["incomplete_vehicles"]),
            avg_queue_length=float(summary["avg_queue_length"]),
            peak_avg_queue_length=float(summary["peak_avg_queue_length"]),
            peak_corridor_load_ratio=float(summary["peak_corridor_load_ratio"]),
            peak_node_utilization_ratio=float(summary["peak_node_utilization_ratio"]),
            peak_queue_ratio=float(summary["peak_queue_ratio"]),
            mean_corridor_speed=float(summary["mean_corridor_speed"]),
        ),
        artifact_paths=ArtifactPaths(
            output_dir=simulation_result.output_dir,
            metrics_path=simulation_result.metrics_path,
            event_log_path=simulation_result.event_log_path,
            run_summary_path=simulation_result.run_summary_path,
            threshold_summary_path=simulation_result.threshold_summary_path,
            plot_paths=tuple(simulation_result.plot_paths),
        ),
        phase_detection=copy.deepcopy(summary.get("phase_detection", {})),
        physics_summary=copy.deepcopy(summary.get("physics_summary", {})),
        admissibility_summary=copy.deepcopy(summary.get("admissibility_summary", {})),
        mean_metrics=copy.deepcopy(summary),
        variance={},
        confidence_score=float(summary.get("dominant_failure_mode_confidence", 0.0)),
        seed_count=1,
    )


def build_slice_results_payload(
    tranche: TrancheDefinition,
    results: list[TrancheSliceResult],
    *,
    output_dir: Path,
    adaptive_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "analysis_contract_version": 2,
        "tranche": tranche.to_dict(),
        "output_dir": str(output_dir.resolve()),
        "slice_count": len(results),
        "results": [result.to_dict() for result in results],
    }
    if adaptive_payload is not None:
        payload["adaptive"] = copy.deepcopy(adaptive_payload)
    return payload


def load_slice_results_payload(path: str | Path) -> tuple[dict[str, object], list[TrancheSliceResult]]:
    payload_path = Path(path)
    with payload_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload, [TrancheSliceResult.from_dict(item) for item in payload["results"]]


def write_slice_results_json(
    output_dir: Path,
    tranche: TrancheDefinition,
    results: list[TrancheSliceResult],
    *,
    adaptive_payload: dict[str, object] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "slice_results.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            build_slice_results_payload(
                tranche,
                results,
                output_dir=output_dir,
                adaptive_payload=adaptive_payload,
            ),
            handle,
            indent=2,
        )
    return path


def write_aggregate_csv(output_dir: Path, tranche_name: str, results: list[TrancheSliceResult]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "aggregate.csv"
    fieldnames = [
        "tranche_name",
        "mechanism",
        "dominance_count",
        "dominance_proportion",
        "mean_time_to_first_failure",
        "median_time_to_first_failure",
        "mean_safe_region_exit_time",
        "median_safe_region_exit_time",
        "mean_degraded_mode_dwell_time",
    ]
    rows: list[dict[str, object]] = []

    for mechanism in ("__all__", *FAILURE_MECHANISMS):
        if mechanism == "__all__":
            subset = results
        else:
            subset = [result for result in results if _slice_mode(result) == mechanism]
            if not subset:
                continue
        failure_times = [float(result.time_to_first_failure) for result in subset if result.time_to_first_failure is not None]
        exit_times = [float(result.safe_region_exit_time) for result in subset if result.safe_region_exit_time is not None]
        rows.append(
            {
                "tranche_name": tranche_name,
                "mechanism": mechanism,
                "dominance_count": len(subset),
                "dominance_proportion": len(subset) / len(results) if results else 0.0,
                "mean_time_to_first_failure": _mean(failure_times),
                "median_time_to_first_failure": _median(failure_times),
                "mean_safe_region_exit_time": _mean(exit_times),
                "median_safe_region_exit_time": _median(exit_times),
                "mean_degraded_mode_dwell_time": _mean([result.degraded_mode_dwell_time for result in subset]),
            }
        )

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def analyze_phase_boundaries(tranche_name: str, results: list[TrancheSliceResult]) -> dict[str, object]:
    if not results:
        raise ValueError("At least one tranche slice result is required")

    mechanism_counts = Counter(_slice_mode(result) for result in results)
    safe_region_exit_distribution = Counter(result.safe_region_exit_cause or "no_exit" for result in results)
    failure_times = [float(result.time_to_first_failure) for result in results if result.time_to_first_failure is not None]
    varying_axes = [
        key
        for key in results[0].resolved_params
        if len({json.dumps(result.resolved_params[key], sort_keys=True) for result in results}) > 1
    ]

    dominant_failure_regions: list[dict[str, object]] = []
    for mechanism in FAILURE_MECHANISMS:
        subset = [result for result in results if _slice_mode(result) == mechanism]
        if not subset:
            continue
        region_axes: dict[str, object] = {}
        for axis in varying_axes:
            values = [result.resolved_params[axis] for result in subset]
            if all(isinstance(value, (bool, int, float)) for value in values):
                region_axes[axis] = {
                    "min": min(float(value) for value in values),
                    "max": max(float(value) for value in values),
                    "unique_values": sorted(set(values), key=_sort_scalar),
                }
            else:
                region_axes[axis] = {"unique_values": sorted({str(value) for value in values})}
        dominant_failure_regions.append(
            {
                "mechanism": mechanism,
                "slice_count": len(subset),
                "proportion": len(subset) / len(results),
                "parameter_region": region_axes,
                "representative_slice_ids": [result.slice_id for result in subset[:5]],
            }
        )

    parameter_sensitivity: list[dict[str, object]] = []
    for axis in varying_axes:
        grouped: dict[str, list[TrancheSliceResult]] = defaultdict(list)
        raw_values: dict[str, ParamValue] = {}
        for result in results:
            serialized = json.dumps(result.resolved_params[axis], sort_keys=True)
            grouped[serialized].append(result)
            raw_values[serialized] = result.resolved_params[axis]

        summaries: list[dict[str, object]] = []
        for serialized in sorted(grouped, key=lambda item: _sort_scalar(raw_values[item])):
            group = grouped[serialized]
            counts = Counter(_slice_mode(item) for item in group)
            dominant_mechanism = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
            times = [float(item.time_to_first_failure) for item in group if item.time_to_first_failure is not None]
            summaries.append(
                {
                    "axis_value": raw_values[serialized],
                    "slice_count": len(group),
                    "dominant_mechanism": dominant_mechanism,
                    "mechanism_counts": dict(sorted(counts.items())),
                    "mechanism_proportions": {
                        mechanism: count / len(group)
                        for mechanism, count in sorted(counts.items())
                    },
                    "mean_time_to_first_failure": _mean(times),
                    "median_time_to_first_failure": _median(times),
                }
            )
        parameter_sensitivity.append({"axis": axis, "summaries": summaries})

    dominant_failure_switches: list[dict[str, object]] = []
    monotonic_threshold_regions: list[dict[str, object]] = []
    for axis in varying_axes:
        other_axes = [key for key in varying_axes if key != axis]
        grouped_sequences: dict[str, list[TrancheSliceResult]] = defaultdict(list)
        grouped_context: dict[str, dict[str, ParamValue]] = {}

        for result in results:
            context = {key: result.resolved_params[key] for key in other_axes}
            context_key = json.dumps(context, sort_keys=True)
            grouped_sequences[context_key].append(result)
            grouped_context[context_key] = context

        for context_key, group in sorted(grouped_sequences.items()):
            ordered_group = sorted(group, key=lambda item: _sort_scalar(item.resolved_params[axis]))
            if len(ordered_group) < 2:
                continue

            mechanism_sequence = [_slice_mode(item) for item in ordered_group]
            value_sequence = [item.resolved_params[axis] for item in ordered_group]
            switch_count = 0
            for previous, current in zip(ordered_group, ordered_group[1:]):
                if _slice_mode(previous) == _slice_mode(current):
                    continue
                switch_count += 1
                previous_value = previous.resolved_params[axis]
                current_value = current.resolved_params[axis]
                estimated_threshold: float | None = None
                if isinstance(previous_value, (bool, int, float)) and isinstance(current_value, (bool, int, float)):
                    estimated_threshold = (float(previous_value) + float(current_value)) / 2.0
                dominant_failure_switches.append(
                    {
                        "axis": axis,
                        "fixed_context": grouped_context[context_key],
                        "from_value": previous_value,
                        "to_value": current_value,
                        "from_mechanism": _slice_mode(previous),
                        "to_mechanism": _slice_mode(current),
                        "estimated_threshold": estimated_threshold,
                    }
                )

            if switch_count == 1 and len(set(mechanism_sequence)) == 2:
                transition_index = next(
                    index
                    for index in range(1, len(mechanism_sequence))
                    if mechanism_sequence[index] != mechanism_sequence[index - 1]
                )
                monotonic_threshold_regions.append(
                    {
                        "axis": axis,
                        "fixed_context": grouped_context[context_key],
                        "from_mechanism": mechanism_sequence[transition_index - 1],
                        "to_mechanism": mechanism_sequence[transition_index],
                        "last_before_value": value_sequence[transition_index - 1],
                        "first_after_value": value_sequence[transition_index],
                    }
                )

    return {
        "tranche_name": tranche_name,
        "slice_count": len(results),
        "failure_mechanism_counts": dict(sorted(mechanism_counts.items())),
        "failure_mechanism_proportions": {
            mechanism: mechanism_counts[mechanism] / len(results)
            for mechanism in sorted(mechanism_counts)
        },
        "mean_time_to_first_failure": _mean(failure_times),
        "median_time_to_first_failure": _median(failure_times),
        "safe_region_exit_distribution": dict(sorted(safe_region_exit_distribution.items())),
        "parameter_sensitivity": parameter_sensitivity,
        "dominant_failure_regions": dominant_failure_regions,
        "dominant_failure_switches": dominant_failure_switches,
        "monotonic_threshold_regions": monotonic_threshold_regions,
    }


def write_phase_boundaries_json(output_dir: Path, tranche_name: str, results: list[TrancheSliceResult]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "phase_boundaries.json"
    payload = analyze_phase_boundaries(tranche_name, results)
    threshold_payload = build_phase_space_outputs(tranche_name, results)["threshold_estimates"]
    promoted_boundaries, rejected_candidates = _governed_transition_summary(threshold_payload)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                **payload,
                "analysis_contract_version": 2,
                "governed_transition_boundaries": promoted_boundaries,
                "rejected_transition_candidates": rejected_candidates,
            },
            handle,
            indent=2,
        )
    return path


def _phase_points_by_iteration(
    results: list[TrancheSliceResult],
    adaptive_payload: dict[str, object] | None,
) -> tuple[list[list[object]], list[int], list[int], bool, int | None, float]:
    sorted_results = sorted(results, key=lambda item: item.slice_id)
    point_lookup = {
        point.slice_id: point
        for point in phase_points_from_slice_results(sorted_results)
    }

    default_threshold = 0.3
    if adaptive_payload is None:
        return (
            [list(point_lookup.values())],
            [len(point_lookup)],
            [len(point_lookup)],
            False,
            None,
            default_threshold,
        )

    records = adaptive_payload.get("iterations", [])
    if not isinstance(records, list) or not records:
        return (
            [list(point_lookup.values())],
            [len(point_lookup)],
            [len(point_lookup)],
            bool(adaptive_payload.get("enabled")),
            None,
            float(adaptive_payload.get("convergence_threshold", default_threshold)),
        )

    cumulative_ids: list[str] = []
    iteration_points: list[list[object]] = []
    cumulative_counts: list[int] = []
    new_counts: list[int] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        executed_ids = [
            slice_id
            for slice_id in record.get("executed_slice_ids", [])
            if isinstance(slice_id, str) and slice_id in point_lookup
        ]
        cumulative_ids.extend(executed_ids)
        unique_ids = list(dict.fromkeys(cumulative_ids))
        iteration_points.append([point_lookup[slice_id] for slice_id in sorted(unique_ids)])
        cumulative_counts.append(len(unique_ids))
        new_counts.append(len(executed_ids))

    if not iteration_points:
        iteration_points = [list(point_lookup.values())]
        cumulative_counts = [len(point_lookup)]
        new_counts = [len(point_lookup)]

    return (
        iteration_points,
        cumulative_counts,
        new_counts,
        bool(adaptive_payload.get("enabled")),
        int(adaptive_payload["max_iterations"]) if isinstance(adaptive_payload.get("max_iterations"), int) else None,
        float(adaptive_payload.get("convergence_threshold", default_threshold)),
    )


def _round_trip_phase_points(results: list[TrancheSliceResult]) -> tuple[list[object], list[object]]:
    replay_results = [
        TrancheSliceResult.from_dict(result.to_dict())
        for result in sorted(results, key=lambda item: item.slice_id)
    ]
    replay_points = phase_points_from_slice_results(replay_results)
    replay_regions = detect_transition_regions(replay_points)
    return replay_points, replay_regions


def _threshold_history_by_iteration(
    tranche_name: str,
    iteration_points: list[list[object]],
    iteration_regions: list[list[object]],
) -> list[dict[str, object]]:
    history: list[dict[str, object]] = []
    for index, (points, regions) in enumerate(zip(iteration_points, iteration_regions, strict=True)):
        iteration_thresholds = build_threshold_estimates(
            tranche_name,
            points,
            regions,
            replay_points=points,
            replay_transition_regions=regions,
        )
        history.append(
            {
                "iteration": index,
                "slice_count": len(points),
                "threshold_statuses": {
                    threshold_name: {
                        "status": threshold_payload["status"],
                        "estimate": threshold_payload.get("estimate"),
                        "promotion_decision": threshold_payload["promotion_state"]["decision"],
                    }
                    for threshold_name, threshold_payload in iteration_thresholds["thresholds"].items()
                },
                "promotion_decisions": iteration_thresholds["promotion_decisions"],
            }
        )
    return history


def _governed_transition_summary(threshold_payload: dict[str, object]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    thresholds = threshold_payload.get("thresholds", {})
    if not isinstance(thresholds, dict):
        return [], []

    promoted_boundaries: list[dict[str, object]] = []
    rejected_candidates: list[dict[str, object]] = []
    for threshold_name, threshold_record in sorted(thresholds.items()):
        if not isinstance(threshold_record, dict):
            continue
        derivation_basis = threshold_record.get("derivation_basis")
        if not isinstance(derivation_basis, dict):
            derivation_basis = {}
        summary = {
            "threshold": threshold_name,
            "symbol": threshold_record.get("symbol"),
            "status": threshold_record.get("status"),
            "estimate": threshold_record.get("estimate"),
            "lower_bound": threshold_record.get("lower_bound"),
            "upper_bound": threshold_record.get("upper_bound"),
            "source": derivation_basis.get("source"),
            "source_axis": derivation_basis.get("source_axis"),
            "support_count": threshold_record.get("support_count"),
            "promotion_decision": threshold_record.get("promotion_state", {}).get("decision"),
        }
        if bool(threshold_record.get("promotion_state", {}).get("promoted")):
            promoted_boundaries.append(summary)
        else:
            rejected_candidates.append(summary)
    return promoted_boundaries, rejected_candidates


def build_phase_space_outputs(
    tranche_name: str,
    results: list[TrancheSliceResult],
    *,
    adaptive_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    phase_points = phase_points_from_slice_results(sorted(results, key=lambda item: item.slice_id))
    transition_regions = detect_transition_regions(phase_points)
    replay_points, replay_regions = _round_trip_phase_points(results)
    thresholds = build_threshold_estimates(
        tranche_name,
        phase_points,
        transition_regions,
        replay_points=replay_points,
        replay_transition_regions=replay_regions,
    )
    admissibility_overlay = build_admissibility_overlay(tranche_name, phase_points)
    iteration_points, cumulative_counts, new_counts, adaptive_enabled, max_iterations, convergence_threshold = (
        _phase_points_by_iteration(results, adaptive_payload)
    )
    iteration_regions = [detect_transition_regions(points) for points in iteration_points]
    thresholds["promotion_history"] = _threshold_history_by_iteration(
        tranche_name,
        iteration_points,
        iteration_regions,
    )
    convergence_report = build_convergence_report(
        iteration_regions,
        convergence_threshold=convergence_threshold,
        iteration_slice_counts=cumulative_counts,
        new_slice_counts=new_counts,
        adaptive_enabled=adaptive_enabled,
        max_iterations=max_iterations,
    )
    if adaptive_payload is not None and isinstance(adaptive_payload.get("stopping_reason"), str):
        convergence_report["stopping_reason"] = adaptive_payload["stopping_reason"]
    return {
        "phase_map": phase_map_payload(tranche_name, phase_points),
        "transition_regions": {
            "analysis_contract_version": 2,
            "tranche_name": tranche_name,
            "region_count": len(transition_regions),
            "regions": [region.to_dict() for region in transition_regions],
        },
        "threshold_estimates": thresholds,
        "threshold_ledger": {
            "analysis_contract_version": 2,
            "tranche_name": tranche_name,
            "scope": "local_tranche",
            "epistemic_note": thresholds["epistemic_note"],
            "entries": thresholds["threshold_ledger"],
            "promotion_history": thresholds["promotion_history"],
        },
        "promotion_decisions": {
            "analysis_contract_version": 2,
            "tranche_name": tranche_name,
            "scope": "local_tranche",
            "decisions": thresholds["promotion_decisions"],
        },
        "admissibility_overlay": admissibility_overlay,
        "convergence_report": convergence_report,
    }


def write_phase_map_json(output_dir: Path, tranche_name: str, results: list[TrancheSliceResult]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "phase_map.json"
    payload = build_phase_space_outputs(tranche_name, results)["phase_map"]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_transition_regions_json(
    output_dir: Path,
    tranche_name: str,
    results: list[TrancheSliceResult],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "transition_regions.json"
    payload = build_phase_space_outputs(tranche_name, results)["transition_regions"]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_threshold_estimates_json(
    output_dir: Path,
    tranche_name: str,
    results: list[TrancheSliceResult],
    *,
    adaptive_payload: dict[str, object] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "threshold_estimates.json"
    payload = build_phase_space_outputs(
        tranche_name,
        results,
        adaptive_payload=adaptive_payload,
    )["threshold_estimates"]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_threshold_ledger_json(
    output_dir: Path,
    tranche_name: str,
    results: list[TrancheSliceResult],
    *,
    adaptive_payload: dict[str, object] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "threshold_ledger.json"
    payload = build_phase_space_outputs(
        tranche_name,
        results,
        adaptive_payload=adaptive_payload,
    )["threshold_ledger"]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_promotion_decisions_json(
    output_dir: Path,
    tranche_name: str,
    results: list[TrancheSliceResult],
    *,
    adaptive_payload: dict[str, object] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "promotion_decisions.json"
    payload = build_phase_space_outputs(
        tranche_name,
        results,
        adaptive_payload=adaptive_payload,
    )["promotion_decisions"]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_admissibility_overlay_json(
    output_dir: Path,
    tranche_name: str,
    results: list[TrancheSliceResult],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "admissibility_overlay.json"
    payload = build_phase_space_outputs(tranche_name, results)["admissibility_overlay"]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_convergence_report_json(
    output_dir: Path,
    tranche_name: str,
    results: list[TrancheSliceResult],
    *,
    adaptive_payload: dict[str, object] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "convergence_report.json"
    payload = build_phase_space_outputs(
        tranche_name,
        results,
        adaptive_payload=adaptive_payload,
    )["convergence_report"]
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def _mechanism_rankings(results: list[TrancheSliceResult]) -> list[str]:
    counts = Counter(_slice_mode(result) for result in results)
    return [
        mechanism
        for mechanism, _count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _summarize_tranche(results: list[TrancheSliceResult]) -> dict[str, object]:
    counts = Counter(_slice_mode(result) for result in results)
    dominant_mechanism = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
    failure_times = [float(result.time_to_first_failure) for result in results if result.time_to_first_failure is not None]
    return {
        "slice_count": len(results),
        "dominant_mechanism": dominant_mechanism,
        "mechanism_counts": dict(sorted(counts.items())),
        "mechanism_proportions": {
            mechanism: counts[mechanism] / len(results)
            for mechanism in sorted(counts)
        },
        "mean_time_to_first_failure": _mean(failure_times),
        "median_time_to_first_failure": _median(failure_times),
        "mechanism_ranking": _mechanism_rankings(results),
    }


def write_cross_tranche_outputs(
    output_dir: Path,
    tranche_results: dict[str, list[TrancheSliceResult]],
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    matrix_path = output_dir / "mechanism_dominance_matrix.csv"
    comparison_path = output_dir / "tranche_comparison.json"
    summary_path = output_dir / "cross_tranche_summary.json"

    summaries = {
        tranche_name: _summarize_tranche(results)
        for tranche_name, results in sorted(tranche_results.items())
    }

    with matrix_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = ["tranche_name", *FAILURE_MECHANISMS, "dominant_mechanism", "mean_time_to_first_failure"]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for tranche_name, summary in summaries.items():
            row = {
                "tranche_name": tranche_name,
                "dominant_mechanism": summary["dominant_mechanism"],
                "mean_time_to_first_failure": summary["mean_time_to_first_failure"],
            }
            proportions = summary["mechanism_proportions"]
            for mechanism in FAILURE_MECHANISMS:
                row[mechanism] = proportions.get(mechanism, 0.0)
            writer.writerow(row)

    with comparison_path.open("w", encoding="utf-8") as handle:
        json.dump({"tranches": summaries}, handle, indent=2)

    fastest_by_mechanism: dict[str, dict[str, object]] = {}
    for mechanism in FAILURE_MECHANISMS:
        candidates: list[tuple[str, float]] = []
        for tranche_name, results in tranche_results.items():
            mechanism_times = [
                float(result.time_to_first_failure)
                for result in results
                if _slice_mode(result) == mechanism and result.time_to_first_failure is not None
            ]
            if mechanism_times:
                candidates.append((tranche_name, statistics.fmean(mechanism_times)))
        if candidates:
            tranche_name, mean_time = sorted(candidates, key=lambda item: (item[1], item[0]))[0]
            fastest_by_mechanism[mechanism] = {
                "tranche_name": tranche_name,
                "mean_time_to_first_failure": mean_time,
            }

    mixed_stress_summary: dict[str, object] = {}
    if "coupled" in summaries:
        isolated_primary_counts = Counter(
            summary["dominant_mechanism"]
            for tranche_name, summary in summaries.items()
            if tranche_name != "coupled"
        )
        isolated_ordering = [
            mechanism
            for mechanism, _count in sorted(isolated_primary_counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        coupled_ordering = list(summaries["coupled"]["mechanism_ranking"])
        mixed_stress_summary = {
            "coupled_primary_mechanism": summaries["coupled"]["dominant_mechanism"],
            "coupled_mechanism_ranking": coupled_ordering,
            "isolated_primary_mechanisms": {
                tranche_name: summary["dominant_mechanism"]
                for tranche_name, summary in summaries.items()
                if tranche_name != "coupled"
            },
            "isolated_primary_ordering": isolated_ordering,
            "ordering_shift_detected": coupled_ordering[:3] != isolated_ordering[:3],
        }

    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "fastest_tranche_by_mechanism": fastest_by_mechanism,
                "mixed_stress_summary": mixed_stress_summary,
            },
            handle,
            indent=2,
        )

    return matrix_path, comparison_path, summary_path


def write_global_phase_map_json(
    output_dir: Path,
    tranche_results: dict[str, list[TrancheSliceResult]],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "global_phase_map.json"
    payload = {
        "tranches": {
            tranche_name: build_phase_space_outputs(tranche_name, results)["phase_map"]
            for tranche_name, results in sorted(tranche_results.items())
        }
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_cross_tranche_thresholds_json(
    output_dir: Path,
    tranche_results: dict[str, list[TrancheSliceResult]],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "cross_tranche_thresholds.json"
    payload = build_cross_tranche_thresholds(
        {
            tranche_name: build_phase_space_outputs(tranche_name, results)["threshold_estimates"]
            for tranche_name, results in sorted(tranche_results.items())
        }
    )
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_cross_tranche_threshold_ledger_json(
    output_dir: Path,
    tranche_results: dict[str, list[TrancheSliceResult]],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "cross_tranche_threshold_ledger.json"
    payload = build_cross_tranche_thresholds(
        {
            tranche_name: build_phase_space_outputs(tranche_name, results)["threshold_estimates"]
            for tranche_name, results in sorted(tranche_results.items())
        }
    )
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "analysis_contract_version": payload["analysis_contract_version"],
                "scope": payload["scope"],
                "epistemic_note": payload["epistemic_note"],
                "entries": payload["threshold_ledger"],
                "consistency_findings": payload["consistency_findings"],
            },
            handle,
            indent=2,
        )
    return path


def write_cross_tranche_promotion_decisions_json(
    output_dir: Path,
    tranche_results: dict[str, list[TrancheSliceResult]],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "cross_tranche_promotion_decisions.json"
    payload = build_cross_tranche_thresholds(
        {
            tranche_name: build_phase_space_outputs(tranche_name, results)["threshold_estimates"]
            for tranche_name, results in sorted(tranche_results.items())
        }
    )
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "analysis_contract_version": payload["analysis_contract_version"],
                "scope": payload["scope"],
                "decisions": payload["promotion_decisions"],
                "consistency_findings": payload["consistency_findings"],
            },
            handle,
            indent=2,
        )
    return path
