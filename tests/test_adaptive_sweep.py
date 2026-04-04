from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from avn.phase_space.convergence import build_convergence_report
from avn.phase_space.models import (
    PhaseRegion,
    PromotionGovernanceOutcome,
    ThresholdEvidenceStatus,
    ThresholdEvidenceType,
    phase_points_from_slice_results,
)
from avn.phase_space.thresholds import (
    build_cross_tranche_thresholds,
    build_threshold_estimates,
    map_contradictions_to_outcome,
    normalize_governed_artifact_payload,
    normalize_threshold_payload,
)
from avn.phase_space.transitions import detect_transition_regions
from avn.sweep import TrancheRunResult, analyze_only, main
from avn.sweep_adaptive import _refinement_parameter_sets, adaptive_sweep
from avn.sweep_analysis import (
    ArtifactPaths,
    CommsMetricsSnapshot,
    ContingencyMetricsSnapshot,
    ThroughputMetricsSnapshot,
    TrancheSliceResult,
    TrustMetricsSnapshot,
    build_phase_space_outputs,
    write_contradictions_json,
    write_cross_tranche_promotion_decisions_json,
    write_cross_tranche_threshold_ledger_json,
    write_cross_tranche_thresholds_json,
    write_promotion_decisions_json,
    write_slice_results_json,
    write_threshold_estimates_json,
    write_threshold_ledger_json,
)
from avn.sweep_tranches import SeedPolicy, SweepAxis, TrancheDefinition, TrancheSlice


def _result_for_slice(
    tmp_path: Path,
    *,
    tranche_name: str,
    slice_definition: TrancheSlice,
    mechanism: str,
    force_admissible: bool = False,
    safe_region_exit_time: float | None | str = "default",
    safe_region_exit_cause: str | None = None,
    trusted_active_fraction: float | None = None,
    comms_reliability_min: float | None = None,
    alpha_nav_min: float | None = None,
    weather_severity_peak: float | None = None,
    contingency_margin_min: float | None = None,
) -> TrancheSliceResult:
    output_dir = tmp_path / slice_definition.slice_id
    output_dir.mkdir(parents=True, exist_ok=True)
    axis_value = float(slice_definition.resolved_params.get("alpha", slice_definition.resolved_params.get("modifiers.demand_multiplier", 0.0)))
    exit_time = (
        max(5.0, 100.0 - (axis_value * 40.0))
        if safe_region_exit_time == "default"
        else safe_region_exit_time
    )
    phase_transition_time = max(1.0, exit_time - 2.0)
    admissibility_time = max(0.0, phase_transition_time - 2.0)
    phase_detection = {
        "flow_breakdown": {
            "detected": mechanism == "corridor_capacity_exceeded",
            "threshold_value": axis_value if mechanism == "corridor_capacity_exceeded" else None,
            "time_minute": phase_transition_time if mechanism == "corridor_capacity_exceeded" else None,
            "detection_method": "synthetic_flow_breakdown",
            "confidence": 0.8 if mechanism == "corridor_capacity_exceeded" else 0.0,
            "details": {"rho_c": axis_value} if mechanism == "corridor_capacity_exceeded" else {},
        },
        "queue_divergence": {
            "detected": mechanism == "node_service_collapse",
            "threshold_value": axis_value if mechanism == "node_service_collapse" else None,
            "time_minute": phase_transition_time if mechanism == "node_service_collapse" else None,
            "detection_method": "synthetic_queue_divergence",
            "confidence": 0.85 if mechanism == "node_service_collapse" else 0.0,
            "details": {"lambda_c": axis_value} if mechanism == "node_service_collapse" else {},
        },
        "comms_failure": {"detected": False, "threshold_value": None, "time_minute": None, "detection_method": "none", "confidence": 0.0, "details": {}},
        "weather_collapse": {"detected": False, "threshold_value": None, "time_minute": None, "detection_method": "none", "confidence": 0.0, "details": {}},
        "admissibility_exit": {
            "detected": not force_admissible,
            "threshold_value": None,
            "time_minute": None if force_admissible else admissibility_time,
            "detection_method": "synthetic_admissibility_exit",
            "confidence": 0.0 if force_admissible else 0.8,
            "details": {},
        },
        "contingency_saturation": {"detected": False, "threshold_value": None, "time_minute": None, "detection_method": "none", "confidence": 0.0, "details": {}},
    }
    return TrancheSliceResult(
        slice_id=slice_definition.slice_id,
        tranche_name=tranche_name,
        seed=slice_definition.seed,
        resolved_params=dict(slice_definition.resolved_params),
        first_dominant_failure_mechanism=mechanism,
        time_to_first_failure=exit_time,
        safe_region_exit_time=(
            None if force_admissible else exit_time
        ),
        safe_region_exit_cause=(
            ""
            if force_admissible
            else (safe_region_exit_cause or ("corridor_load_ratio" if mechanism == "corridor_capacity_exceeded" else "queue_ratio"))
        ),
        degraded_mode_dwell_time=axis_value * 10.0,
        trust_metrics_snapshot=TrustMetricsSnapshot(
            unsafe_admission_count=0 if mechanism != "trust_breakdown" else 2,
            quarantine_count=0,
            revocation_count=0,
            trusted_active_fraction=(
                trusted_active_fraction
                if trusted_active_fraction is not None
                else 1.0
            ),
            operator_intervention_count=0,
            trust_induced_throughput_loss=axis_value / 10.0,
        ),
        comms_metrics_snapshot=CommsMetricsSnapshot(
            information_age_mean=2.0 + axis_value,
            information_age_max=4.0 + axis_value,
            stale_state_exposure_minutes=0.0 if mechanism != "stale_information_instability" else 10.0,
            delayed_reroute_count=0,
            lost_link_fallback_activations=0,
            reservation_invalidations=0,
        ),
        contingency_metrics_snapshot=ContingencyMetricsSnapshot(
            reachable_landing_option_mean=max(0.0, 2.0 - axis_value),
            no_admissible_landing_events=0,
            contingency_node_utilization=min(1.0, axis_value),
            contingency_saturation_duration=0.0,
            reserve_margin_mean=14.0 - axis_value,
            reserve_margin_min=10.0 - axis_value,
        ),
        throughput_metrics_snapshot=ThroughputMetricsSnapshot(
            completed_vehicles=max(1, 10 - int(round(axis_value * 2))),
            incomplete_vehicles=int(round(axis_value * 2)),
            avg_queue_length=axis_value,
            peak_avg_queue_length=axis_value + 0.5,
            peak_corridor_load_ratio=0.8 + (axis_value * 0.4),
            peak_node_utilization_ratio=0.6 + (axis_value * 0.5),
            peak_queue_ratio=0.2 + (axis_value * 0.4),
            mean_corridor_speed=105.0 - (axis_value * 5.0),
        ),
        artifact_paths=ArtifactPaths(
            output_dir=output_dir,
            metrics_path=output_dir / "metrics.csv",
            event_log_path=output_dir / "events.json",
            run_summary_path=output_dir / "run_summary.json",
            threshold_summary_path=output_dir / "threshold_summary.json",
            plot_paths=(),
        ),
        phase_detection=phase_detection,
        physics_summary={
            "alpha_nav_min": 1.0 if alpha_nav_min is None else alpha_nav_min,
            "alpha_comms_min": 1.0 if comms_reliability_min is None else comms_reliability_min,
            "alpha_weather_min": 1.0,
            "alpha_trust_min": 1.0,
            "alpha_e_min": 1.0,
            "contingency_margin_min": 0.0 if contingency_margin_min is None else contingency_margin_min,
        },
        admissibility_summary={"inside_fraction": 1.0 if force_admissible else 0.0},
        mean_metrics={
            "event_chain": {
                "admissibility_degradation_time": admissibility_time,
                "phase_transition_time": phase_transition_time,
                "safe_region_exit_time": exit_time,
                "collapse_time": exit_time + 2.0,
            },
            "weather_severity_peak": 0.0 if weather_severity_peak is None else weather_severity_peak,
            "comms_reliability_min": 1.0 if comms_reliability_min is None else comms_reliability_min,
            "reserve_margin_min": 10.0 - axis_value,
        },
    )


def _synthetic_tranche(tmp_path: Path) -> TrancheDefinition:
    config_path = tmp_path / "synthetic.toml"
    config_path.write_text("synthetic = true\n", encoding="utf-8")
    return TrancheDefinition(
        tranche_name="synthetic",
        description="Synthetic tranche for adaptive sweep tests.",
        base_config_path=config_path,
        fixed_params={},
        sweep_axes=(SweepAxis("alpha", (0.0, 1.0)),),
        expected_metrics=("first_dominant_failure_mechanism",),
        expected_failure_modes=("corridor_capacity_exceeded", "node_service_collapse"),
        seed_policy=SeedPolicy(base_seed=901),
    )


def _canonical_round_trip(payload: dict[str, object]) -> dict[str, object]:
    normalized = normalize_governed_artifact_payload(payload)
    renormalized = normalize_governed_artifact_payload(
        json.loads(json.dumps(normalized, sort_keys=True))
    )
    assert renormalized == normalized
    return normalized


def _governed_round_trip_results(tmp_path: Path, tranche_name: str) -> list[TrancheSliceResult]:
    slice_definitions = [
        TrancheSlice(
            slice_id=f"{tranche_name}_a",
            tranche_name=tranche_name,
            seed=1,
            resolved_params={"modifiers.demand_multiplier": 1.0},
            base_config_path=tmp_path / "synthetic.toml",
        ),
        TrancheSlice(
            slice_id=f"{tranche_name}_b",
            tranche_name=tranche_name,
            seed=2,
            resolved_params={"modifiers.demand_multiplier": 1.4},
            base_config_path=tmp_path / "synthetic.toml",
        ),
        TrancheSlice(
            slice_id=f"{tranche_name}_c",
            tranche_name=tranche_name,
            seed=3,
            resolved_params={"modifiers.demand_multiplier": 1.8},
            base_config_path=tmp_path / "synthetic.toml",
        ),
        TrancheSlice(
            slice_id=f"{tranche_name}_d",
            tranche_name=tranche_name,
            seed=4,
            resolved_params={"modifiers.demand_multiplier": 2.0},
            base_config_path=tmp_path / "synthetic.toml",
        ),
    ]
    return [
        _result_for_slice(tmp_path, tranche_name=tranche_name, slice_definition=slice_definitions[0], mechanism="corridor_capacity_exceeded", force_admissible=True),
        _result_for_slice(tmp_path, tranche_name=tranche_name, slice_definition=slice_definitions[1], mechanism="corridor_capacity_exceeded", force_admissible=True),
        _result_for_slice(tmp_path, tranche_name=tranche_name, slice_definition=slice_definitions[2], mechanism="node_service_collapse", force_admissible=True),
        _result_for_slice(tmp_path, tranche_name=tranche_name, slice_definition=slice_definitions[3], mechanism="node_service_collapse", force_admissible=True),
    ]


def _base_governed_threshold_payload() -> dict[str, object]:
    return {
        "thresholds": {
            "rho_c": {
                "threshold_id": "local_tranche:synthetic:rho_c",
                "symbol": "rho",
                "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                "estimate": 1.1,
                "lower_bound": 1.0,
                "upper_bound": 1.2,
                "support_density": 0.8,
                "support_span": 0.2,
                "support_confidence": 0.7,
                "nuisance_vector": {
                    "congestion": 0.0,
                    "trust": 0.4,
                    "comms": 0.0,
                    "navigation": 0.0,
                    "weather": 0.0,
                    "contingency": 0.0,
                },
                "dominant_axis": "trust",
                "entropy": 0.0,
                "monotonicity_violation": False,
                "monotonicity_block_reason": None,
                "contradictions": [],
                "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                "promotion_state": {
                    "promoted": False,
                    "decision": "retained_as_proxy_only",
                    "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                },
            }
        }
    }


def test_detect_transition_regions_on_synthetic_data(tmp_path: Path) -> None:
    tranche_name = "load"
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_a",
                tranche_name=tranche_name,
                seed=1,
                resolved_params={"modifiers.demand_multiplier": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_b",
                tranche_name=tranche_name,
                seed=2,
                resolved_params={"modifiers.demand_multiplier": 1.4},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_c",
                tranche_name=tranche_name,
                seed=3,
                resolved_params={"modifiers.demand_multiplier": 1.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
        ),
    ]

    regions = detect_transition_regions(phase_points_from_slice_results(results))

    assert len(regions) == 1
    assert regions[0].transition_axis == "modifiers.demand_multiplier"
    assert regions[0].bounds["modifiers.demand_multiplier"] == (1.4, 1.8)
    assert regions[0].estimated_threshold == 1.6


def test_adaptive_sweep_refines_deterministically(tmp_path: Path) -> None:
    tranche = _synthetic_tranche(tmp_path)

    def execute_slice(_tranche: TrancheDefinition, slice_definition: TrancheSlice) -> TrancheSliceResult:
        alpha = float(slice_definition.resolved_params["alpha"])
        mechanism = "corridor_capacity_exceeded" if alpha < 0.5 else "node_service_collapse"
        return _result_for_slice(
            tmp_path,
            tranche_name=tranche.tranche_name,
            slice_definition=slice_definition,
            mechanism=mechanism,
        )

    run_a = adaptive_sweep(
        tranche,
        execute_slice=execute_slice,
        max_iterations=3,
        convergence_threshold=0.2,
        max_slices=6,
    )
    run_b = adaptive_sweep(
        tranche,
        execute_slice=execute_slice,
        max_iterations=3,
        convergence_threshold=0.2,
        max_slices=6,
    )

    assert [result.slice_id for result in run_a.slice_results] == [result.slice_id for result in run_b.slice_results]
    assert run_a.adaptive_payload == run_b.adaptive_payload
    assert len(run_a.adaptive_payload["iterations"]) >= 2
    assert len(run_a.slice_results) > 3


def test_adaptive_refinement_bounds_dense_ambiguous_regions(tmp_path: Path) -> None:
    tranche = _synthetic_tranche(tmp_path)
    dense_region = PhaseRegion(
        bounds={"alpha": (0.2, 0.4)},
        dominant_mechanism="NODE_SATURATION",
        entropy=0.2,
        sample_density=1.0,
        transition_axis="alpha",
        local_disagreement=0.2,
        local_gradient=0.2,
        replay_hash="dense",
    )
    sparse_region = PhaseRegion(
        bounds={"alpha": (0.7, 0.9)},
        dominant_mechanism="NODE_SATURATION",
        entropy=0.3,
        sample_density=0.5,
        transition_axis="alpha",
        local_disagreement=0.3,
        local_gradient=0.3,
        replay_hash="sparse",
    )
    threshold_payload = {
        "thresholds": {
            "rho_c": {
                "threshold_id": "local_tranche:synthetic:rho_c",
                "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                "estimate": 0.3,
                "lower_bound": 0.2,
                "upper_bound": 0.4,
                "support_density": 1.0,
                "support_span": 0.2,
                "support_confidence": 0.0,
                "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                "dominant_axis": None,
                "entropy": 0.0,
                "monotonicity_violation": False,
                "monotonicity_block_reason": None,
                "contradictions": [],
                "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                "promotion_state": {
                    "promoted": False,
                    "decision": "retained_as_proxy_only",
                    "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                },
                "derivation_basis": {"source_axis": "alpha", "source_replay_hash": "dense"},
            },
            "lambda_c": {
                "threshold_id": "local_tranche:synthetic:lambda_c",
                "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                "estimate": 0.8,
                "lower_bound": 0.7,
                "upper_bound": 0.9,
                "support_density": 0.6,
                "support_span": 0.2,
                "support_confidence": 1.0,
                "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                "dominant_axis": None,
                "entropy": 0.0,
                "monotonicity_violation": False,
                "monotonicity_block_reason": None,
                "contradictions": [],
                "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                "promotion_state": {
                    "promoted": True,
                    "decision": "promoted_to_tranche_boundary",
                    "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                },
                "derivation_basis": {"source_axis": "alpha", "source_replay_hash": "sparse"},
            },
        }
    }

    selected = _refinement_parameter_sets(
        tranche,
        [dense_region, sparse_region],
        threshold_payload=threshold_payload,
        executed_slice_ids=set(),
        limit=1,
    )

    assert selected == [{"alpha": 0.8}]


def test_convergence_stopping_condition() -> None:
    iteration_regions = [
        [
            PhaseRegion(
                bounds={"alpha": (0.0, 1.0)},
                dominant_mechanism="corridor_capacity_exceeded",
                entropy=0.6,
                sample_density=2.0,
                transition_axis="alpha",
                estimated_threshold=0.5,
                axis_total_span=1.0,
            )
        ],
        [
            PhaseRegion(
                bounds={"alpha": (0.4, 0.6)},
                dominant_mechanism="corridor_capacity_exceeded",
                entropy=0.05,
                sample_density=8.0,
                transition_axis="alpha",
                estimated_threshold=0.52,
                axis_total_span=1.0,
            )
        ],
    ]

    report = build_convergence_report(
        iteration_regions,
        convergence_threshold=0.1,
        iteration_slice_counts=[3, 4],
        new_slice_counts=[3, 1],
        adaptive_enabled=True,
        max_iterations=5,
    )

    assert report["converged"] is True
    assert report["iterations"][-1]["converged"] is True
    assert report["iterations"][-1]["boundary_shift"] == 0.02


def test_analyze_only_rebuilds_adaptive_artifacts(tmp_path: Path) -> None:
    tranche = _synthetic_tranche(tmp_path)
    slice_definitions = [
        TrancheSlice(
            slice_id="synthetic_a",
            tranche_name=tranche.tranche_name,
            seed=1,
            resolved_params={"alpha": 0.0},
            base_config_path=tranche.base_config_path,
        ),
        TrancheSlice(
            slice_id="synthetic_b",
            tranche_name=tranche.tranche_name,
            seed=2,
            resolved_params={"alpha": 0.5},
            base_config_path=tranche.base_config_path,
        ),
        TrancheSlice(
            slice_id="synthetic_c",
            tranche_name=tranche.tranche_name,
            seed=3,
            resolved_params={"alpha": 1.0},
            base_config_path=tranche.base_config_path,
        ),
    ]
    results = [
        _result_for_slice(tmp_path, tranche_name=tranche.tranche_name, slice_definition=slice_definitions[0], mechanism="corridor_capacity_exceeded"),
        _result_for_slice(tmp_path, tranche_name=tranche.tranche_name, slice_definition=slice_definitions[1], mechanism="node_service_collapse"),
        _result_for_slice(tmp_path, tranche_name=tranche.tranche_name, slice_definition=slice_definitions[2], mechanism="node_service_collapse"),
    ]

    output_dir = tmp_path / "adaptive_analysis"
    output_dir.mkdir()
    write_slice_results_json(
        output_dir,
        tranche,
        results,
        adaptive_payload={
            "enabled": True,
            "max_iterations": 2,
            "convergence_threshold": 0.2,
            "stopping_reason": "converged",
            "iterations": [
                {"iteration": 0, "executed_slice_ids": ["synthetic_a", "synthetic_b"]},
                {"iteration": 1, "executed_slice_ids": ["synthetic_c"]},
            ],
        },
    )

    analysis = analyze_only(output_dir)

    assert analysis.phase_map_json_path is not None and analysis.phase_map_json_path.exists()
    assert analysis.transition_regions_json_path is not None and analysis.transition_regions_json_path.exists()
    assert analysis.threshold_estimates_json_path is not None and analysis.threshold_estimates_json_path.exists()
    assert analysis.convergence_report_json_path is not None and analysis.convergence_report_json_path.exists()


def test_analyze_only_reconstructs_threshold_status_and_promotion_history(tmp_path: Path) -> None:
    tranche = _synthetic_tranche(tmp_path)
    slice_definitions = [
        TrancheSlice(
            slice_id="synthetic_a",
            tranche_name=tranche.tranche_name,
            seed=1,
            resolved_params={"alpha": 0.0},
            base_config_path=tranche.base_config_path,
        ),
        TrancheSlice(
            slice_id="synthetic_b",
            tranche_name=tranche.tranche_name,
            seed=2,
            resolved_params={"alpha": 0.5},
            base_config_path=tranche.base_config_path,
        ),
        TrancheSlice(
            slice_id="synthetic_c",
            tranche_name=tranche.tranche_name,
            seed=3,
            resolved_params={"alpha": 1.0},
            base_config_path=tranche.base_config_path,
        ),
    ]
    results = [
        _result_for_slice(tmp_path, tranche_name=tranche.tranche_name, slice_definition=slice_definitions[0], mechanism="corridor_capacity_exceeded"),
        _result_for_slice(tmp_path, tranche_name=tranche.tranche_name, slice_definition=slice_definitions[1], mechanism="node_service_collapse"),
        _result_for_slice(tmp_path, tranche_name=tranche.tranche_name, slice_definition=slice_definitions[2], mechanism="node_service_collapse"),
    ]

    output_dir = tmp_path / "adaptive_replay"
    output_dir.mkdir()
    adaptive_payload = {
        "enabled": True,
        "max_iterations": 2,
        "convergence_threshold": 0.2,
        "stopping_reason": "converged",
        "iterations": [
            {"iteration": 0, "executed_slice_ids": ["synthetic_a", "synthetic_b"]},
            {"iteration": 1, "executed_slice_ids": ["synthetic_c"]},
        ],
    }
    write_slice_results_json(output_dir, tranche, results, adaptive_payload=adaptive_payload)
    write_threshold_estimates_json(
        output_dir,
        tranche.tranche_name,
        results,
        adaptive_payload=adaptive_payload,
    )
    write_threshold_ledger_json(output_dir, tranche.tranche_name, results, adaptive_payload=adaptive_payload)
    write_promotion_decisions_json(output_dir, tranche.tranche_name, results, adaptive_payload=adaptive_payload)

    initial_thresholds = json.loads((output_dir / "threshold_estimates.json").read_text(encoding="utf-8"))
    initial_ledger = json.loads((output_dir / "threshold_ledger.json").read_text(encoding="utf-8"))
    initial_decisions = json.loads((output_dir / "promotion_decisions.json").read_text(encoding="utf-8"))

    analyze_only(output_dir)

    replay_thresholds = json.loads((output_dir / "threshold_estimates.json").read_text(encoding="utf-8"))
    replay_ledger = json.loads((output_dir / "threshold_ledger.json").read_text(encoding="utf-8"))
    replay_decisions = json.loads((output_dir / "promotion_decisions.json").read_text(encoding="utf-8"))

    assert replay_thresholds == initial_thresholds
    assert replay_ledger == initial_ledger
    assert replay_decisions == initial_decisions
    assert replay_thresholds["thresholds"]["lambda_c"]["status"] == ThresholdEvidenceStatus.PROXY.value
    assert replay_thresholds["thresholds"]["lambda_c"]["evidence_type"] == ThresholdEvidenceType.PHASE_DERIVED.value
    assert replay_thresholds["thresholds"]["lambda_c"]["threshold_promotion_decision"] == (
        initial_thresholds["thresholds"]["lambda_c"]["threshold_promotion_decision"]
    )
    assert replay_thresholds["thresholds"]["lambda_c"]["promotion_blockers"] == [
        "INSUFFICIENT_ADMISSIBLE_SUPPORT"
    ]
    assert replay_ledger["promotion_history"][-1]["threshold_statuses"]["lambda_c"]["promotion_decision"] == (
        "retained_as_sparse_admissibility_support"
    )


def test_threshold_promotion_refuses_weak_transition_support(tmp_path: Path) -> None:
    tranche_name = "load"
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_a",
                tranche_name=tranche_name,
                seed=1,
                resolved_params={"modifiers.demand_multiplier": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_b",
                tranche_name=tranche_name,
                seed=2,
                resolved_params={"modifiers.demand_multiplier": 1.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
        ),
    ]

    phase_points = phase_points_from_slice_results(results)
    thresholds = build_threshold_estimates(
        tranche_name,
        phase_points,
        detect_transition_regions(phase_points),
        replay_points=phase_points,
        replay_transition_regions=detect_transition_regions(phase_points),
    )

    assert thresholds["thresholds"]["rho_c"]["status"] == ThresholdEvidenceStatus.PROXY.value
    assert thresholds["thresholds"]["rho_c"]["promotion_state"]["promoted"] is False
    assert thresholds["promotion_decisions"][0]["decision"] == "retained_as_mixed_non_monotonic"
    assert thresholds["promotion_decisions"][0]["promotion_blockers"] == [
        "BLOCKED_BY_NONMONOTONICITY",
        "INSUFFICIENT_ADMISSIBLE_SUPPORT",
    ]
    assert thresholds["promotion_decisions"][0]["threshold_promotion_decision"] == "BLOCKED_BY_NONMONOTONICITY"


def test_normalization_basis_prefers_tranche_admissibility_envelope(tmp_path: Path) -> None:
    tranche_name = "load"
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_a",
                tranche_name=tranche_name,
                seed=1,
                resolved_params={"modifiers.demand_multiplier": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_b",
                tranche_name=tranche_name,
                seed=2,
                resolved_params={"modifiers.demand_multiplier": 1.4},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_c",
                tranche_name=tranche_name,
                seed=3,
                resolved_params={"modifiers.demand_multiplier": 1.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
        ),
    ]

    points = phase_points_from_slice_results(results)
    payload = build_threshold_estimates(
        tranche_name,
        points,
        detect_transition_regions(points),
        replay_points=points,
        replay_transition_regions=detect_transition_regions(points),
    )

    rho = payload["thresholds"]["rho_c"]
    assert rho["normalization_basis_origin"] == "TRANCHE_ADMISSIBILITY_ENVELOPE"
    assert rho["normalization_basis_value"] == 1.0
    assert rho["normalization_basis_confidence"] > 0.0


def test_normalization_basis_emits_explicit_fallback_provenance(tmp_path: Path) -> None:
    tranche_name = "load"
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_a",
                tranche_name=tranche_name,
                seed=1,
                resolved_params={"modifiers.demand_multiplier": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche_name,
            slice_definition=TrancheSlice(
                slice_id="load_b",
                tranche_name=tranche_name,
                seed=2,
                resolved_params={"modifiers.demand_multiplier": 1.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
        ),
    ]

    points = phase_points_from_slice_results(results)
    payload = build_threshold_estimates(
        tranche_name,
        points,
        detect_transition_regions(points),
        replay_points=points,
        replay_transition_regions=detect_transition_regions(points),
    )

    rho = payload["thresholds"]["rho_c"]
    assert rho["normalization_basis_origin"] == "FALLBACK_SPARSE_SUPPORT"
    assert rho["normalization_basis_value"] is not None
    assert rho["normalization_basis_confidence"] < 1.0


def test_sparse_admissibility_support_blocks_promotion(tmp_path: Path) -> None:
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name="load",
            slice_definition=TrancheSlice(
                slice_id="a",
                tranche_name="load",
                seed=1,
                resolved_params={"modifiers.demand_multiplier": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="load",
            slice_definition=TrancheSlice(
                slice_id="b",
                tranche_name="load",
                seed=2,
                resolved_params={"modifiers.demand_multiplier": 1.2},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="load",
            slice_definition=TrancheSlice(
                slice_id="c",
                tranche_name="load",
                seed=3,
                resolved_params={"modifiers.demand_multiplier": 1.4},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
        ),
    ]
    points = phase_points_from_slice_results(results)
    payload = build_threshold_estimates(
        "load",
        points,
        detect_transition_regions(points),
        replay_points=points,
        replay_transition_regions=detect_transition_regions(points),
    )

    rho = payload["thresholds"]["rho_c"]
    assert rho["promotion_state"]["promoted"] is False
    assert rho["admissibility_support_density"] < 0.5
    assert "INSUFFICIENT_ADMISSIBLE_SUPPORT" in rho["promotion_blockers"]
    assert rho["normalization_basis_origin"] == "TRANCHE_ADMISSIBILITY_ENVELOPE"


def test_mechanism_leakage_blocks_threshold_promotion(tmp_path: Path) -> None:
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="a",
                tranche_name="synthetic",
                seed=1,
                resolved_params={"alpha": 0.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            trusted_active_fraction=0.95,
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="b",
                tranche_name="synthetic",
                seed=2,
                resolved_params={"alpha": 0.9},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            trusted_active_fraction=0.90,
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="c",
                tranche_name="synthetic",
                seed=3,
                resolved_params={"alpha": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            trusted_active_fraction=0.20,
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="d",
                tranche_name="synthetic",
                seed=4,
                resolved_params={"alpha": 1.1},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            trusted_active_fraction=0.10,
            force_admissible=True,
        ),
    ]
    for result in results:
        result.throughput_metrics_snapshot.peak_corridor_load_ratio = 1.0
        result.throughput_metrics_snapshot.peak_node_utilization_ratio = 1.0
        result.throughput_metrics_snapshot.peak_queue_ratio = 1.0
        result.contingency_metrics_snapshot.reachable_landing_option_mean = 1.0
        result.contingency_metrics_snapshot.reserve_margin_min = 10.0
        result.physics_summary["contingency_margin_min"] = 0.0
        result.mean_metrics["reserve_margin_min"] = 10.0
    points = phase_points_from_slice_results(results)
    payload = build_threshold_estimates(
        "synthetic",
        points,
        detect_transition_regions(points),
        replay_points=points,
        replay_transition_regions=detect_transition_regions(points),
        dominant_axis="alpha",
    )

    lambda_record = payload["thresholds"]["lambda_c"]
    assert lambda_record["promotion_state"]["promoted"] is False
    assert lambda_record["threshold_promotion_decision"] == "BLOCKED_BY_MECHANISM_LEAKAGE"
    assert "trust_degradation" in lambda_record["mechanism_leakage_sources"]
    assert lambda_record["mechanism_leakage_score"] > 0.35
    assert lambda_record["nuisance_dominant_axis"] == "trust"
    assert lambda_record["promotion_governance_outcome"] == PromotionGovernanceOutcome.LOCAL_BLOCK.value


def test_high_entropy_nuisance_passes(tmp_path: Path) -> None:
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="a",
                tranche_name="synthetic",
                seed=1,
                resolved_params={"alpha": 0.6},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            trusted_active_fraction=0.98,
            comms_reliability_min=0.98,
            alpha_nav_min=0.98,
            weather_severity_peak=0.05,
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="b",
                tranche_name="synthetic",
                seed=2,
                resolved_params={"alpha": 0.7},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            trusted_active_fraction=0.92,
            comms_reliability_min=0.92,
            alpha_nav_min=0.92,
            weather_severity_peak=0.10,
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="c",
                tranche_name="synthetic",
                seed=3,
                resolved_params={"alpha": 0.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            trusted_active_fraction=0.85,
            comms_reliability_min=0.85,
            alpha_nav_min=0.85,
            weather_severity_peak=0.18,
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="d",
                tranche_name="synthetic",
                seed=4,
                resolved_params={"alpha": 0.9},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            trusted_active_fraction=0.75,
            comms_reliability_min=0.75,
            alpha_nav_min=0.75,
            weather_severity_peak=0.28,
            force_admissible=True,
        ),
    ]
    points = phase_points_from_slice_results(results)
    payload = build_threshold_estimates(
        "synthetic",
        points,
        detect_transition_regions(points),
        replay_points=points,
        replay_transition_regions=detect_transition_regions(points),
        dominant_axis="alpha",
    )

    lambda_record = payload["thresholds"]["lambda_c"]
    assert lambda_record["nuisance_entropy"] > 0.9
    assert lambda_record["threshold_promotion_decision"] != "BLOCKED_BY_MECHANISM_LEAKAGE"
    assert lambda_record["promotion_governance_outcome"] == PromotionGovernanceOutcome.ALLOW.value
    assert lambda_record["promotion_state"]["promoted"] is True


def test_contradiction_mapping_is_deterministic() -> None:
    assert map_contradictions_to_outcome(
        [{"contradiction_type": "CROSS_TRANCHE_CONFLICT"}]
    ) == PromotionGovernanceOutcome.GLOBAL_BLOCK
    assert map_contradictions_to_outcome(
        [{"contradiction_type": "ENVELOPE_VIOLATION"}]
    ) == PromotionGovernanceOutcome.GLOBAL_BLOCK
    assert map_contradictions_to_outcome(
        [{"contradiction_type": "NuisanceDominance"}]
    ) == PromotionGovernanceOutcome.LOCAL_BLOCK
    assert map_contradictions_to_outcome(
        [{"contradiction_type": "NON_MONOTONIC_THRESHOLD"}]
    ) == PromotionGovernanceOutcome.LOCAL_BLOCK
    assert map_contradictions_to_outcome([]) == PromotionGovernanceOutcome.ALLOW


def test_legacy_contradiction_strings_normalize_to_deterministic_outcome() -> None:
    normalized = normalize_threshold_payload(
        {
            "thresholds": {
                "rho_c": {
                    "threshold_id": "local_tranche:legacy:rho_c",
                    "symbol": "rho",
                    "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                    "estimate": 1.1,
                    "lower_bound": 1.0,
                    "upper_bound": 1.2,
                    "support_density": 0.8,
                    "support_span": 0.2,
                    "support_confidence": 0.7,
                    "nuisance_vector": {
                        "congestion": 0.0,
                        "trust": 0.6,
                        "comms": 0.0,
                        "navigation": 0.0,
                        "weather": 0.0,
                        "contingency": 0.0,
                    },
                    "dominant_axis": "trust",
                    "entropy": 0.0,
                    "monotonicity_violation": False,
                    "monotonicity_block_reason": None,
                    "contradictions": [{"contradiction_type": "non-monotonicity"}],
                    "promotion_state": {
                        "promoted": False,
                        "decision": "retained_as_mixed_non_monotonic",
                        "promotion_governance_outcome": PromotionGovernanceOutcome.LOCAL_BLOCK.value,
                    },
                }
            }
        }
    )

    rho_record = normalized["thresholds"]["rho_c"]
    assert rho_record["contradictions"][0]["contradiction_type"] == "NON_MONOTONIC_THRESHOLD"
    assert rho_record["promotion_governance_outcome"] == PromotionGovernanceOutcome.LOCAL_BLOCK.value


def test_missing_governance_fields_fail_closed() -> None:
    with pytest.raises(ValueError, match="nuisance_vector"):
        normalize_threshold_payload(
            {
                "thresholds": {
                    "rho_c": {
                        "threshold_id": "local_tranche:bad:rho_c",
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "estimate": 1.0,
                        "lower_bound": 0.9,
                        "upper_bound": 1.1,
                        "support_density": 0.6,
                        "support_span": 0.2,
                        "support_confidence": 0.6,
                        "entropy": 0.5,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_state": {
                            "promoted": False,
                            "decision": "retained",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                    }
                }
            }
        )


def test_conflicting_support_aliases_fail_closed() -> None:
    payload = _base_governed_threshold_payload()
    payload["thresholds"]["rho_c"]["admissibility_support_density"] = 0.4

    with pytest.raises(ValueError, match="support_density"):
        normalize_threshold_payload(payload)


def test_conflicting_nuisance_aliases_fail_closed() -> None:
    payload = _base_governed_threshold_payload()
    payload["thresholds"]["rho_c"]["nuisance_dominant_axis"] = "weather"

    with pytest.raises(ValueError, match="dominant_axis"):
        normalize_threshold_payload(payload)


def test_entropy_alias_mismatch_fails_closed() -> None:
    payload = _base_governed_threshold_payload()
    payload["thresholds"]["rho_c"]["nuisance_entropy"] = 0.5

    with pytest.raises(ValueError, match="entropy"):
        normalize_threshold_payload(payload)


def test_contradiction_outcome_mismatch_fails_closed() -> None:
    payload = _base_governed_threshold_payload()
    payload["thresholds"]["rho_c"]["contradictions"] = [
        {"contradiction_type": "NON_MONOTONIC_THRESHOLD"}
    ]
    payload["thresholds"]["rho_c"]["promotion_governance_outcome"] = PromotionGovernanceOutcome.ALLOW.value
    payload["thresholds"]["rho_c"]["promotion_state"]["promotion_governance_outcome"] = (
        PromotionGovernanceOutcome.ALLOW.value
    )

    with pytest.raises(ValueError, match="promotion_governance_outcome"):
        normalize_threshold_payload(payload)


def test_derived_leakage_tampering_fails_closed() -> None:
    payload = _base_governed_threshold_payload()
    payload["thresholds"]["rho_c"]["mechanism_leakage_score"] = 0.9
    payload["thresholds"]["rho_c"]["mechanism_leakage_sources"] = ["weather_severity"]

    with pytest.raises(ValueError, match="mechanism_leakage"):
        normalize_threshold_payload(payload)


def test_monotonicity_violation_blocks_promotion(tmp_path: Path) -> None:
    current_results = [
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="c_a",
                tranche_name="synthetic",
                seed=11,
                resolved_params={"alpha": 0.6},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="c_b",
                tranche_name="synthetic",
                seed=12,
                resolved_params={"alpha": 0.7},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="c_c",
                tranche_name="synthetic",
                seed=13,
                resolved_params={"alpha": 0.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="c_d",
                tranche_name="synthetic",
                seed=14,
                resolved_params={"alpha": 0.9},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
    ]
    for result in current_results:
        result.throughput_metrics_snapshot.peak_corridor_load_ratio = 1.0
        result.throughput_metrics_snapshot.peak_node_utilization_ratio = 1.0
        result.throughput_metrics_snapshot.peak_queue_ratio = 1.0
        result.contingency_metrics_snapshot.reachable_landing_option_mean = 1.0
        result.contingency_metrics_snapshot.reserve_margin_min = 10.0
        result.physics_summary["contingency_margin_min"] = 0.0
        result.mean_metrics["reserve_margin_min"] = 10.0
    current_points = phase_points_from_slice_results(current_results)
    current_payload = build_threshold_estimates(
        "synthetic",
        current_points,
        detect_transition_regions(current_points),
        replay_points=current_points,
        replay_transition_regions=detect_transition_regions(current_points),
        dominant_axis="alpha",
        previous_thresholds={
            "lambda_c": {
                "admissibility_support_confidence": 1.1,
                "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                "nuisance_vector": {
                    "congestion": 0.0,
                    "trust": 0.0,
                    "comms": 0.0,
                    "navigation": 0.0,
                    "weather": 0.0,
                    "contingency": 0.0,
                },
            }
        },
    )

    lambda_record = current_payload["thresholds"]["lambda_c"]
    assert lambda_record["monotonicity_violation"] is True
    assert lambda_record["monotonicity_block_reason"] == "ADMISSIBILITY_CONFIDENCE_DECREASED"
    assert lambda_record["threshold_promotion_decision"] == "BLOCKED_BY_NONMONOTONICITY"
    assert lambda_record["promotion_governance_outcome"] == PromotionGovernanceOutcome.LOCAL_BLOCK.value


def test_replay_corruption_is_detected_and_rejected(tmp_path: Path) -> None:
    tranche = _synthetic_tranche(tmp_path)
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name=tranche.tranche_name,
            slice_definition=TrancheSlice(
                slice_id="a",
                tranche_name=tranche.tranche_name,
                seed=1,
                resolved_params={"alpha": 0.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche.tranche_name,
            slice_definition=TrancheSlice(
                slice_id="b",
                tranche_name=tranche.tranche_name,
                seed=2,
                resolved_params={"alpha": 0.5},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche.tranche_name,
            slice_definition=TrancheSlice(
                slice_id="c",
                tranche_name=tranche.tranche_name,
                seed=3,
                resolved_params={"alpha": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
    ]

    with pytest.raises(ValueError, match="ambiguous|incomplete|repeat"):
        build_phase_space_outputs(
            tranche.tranche_name,
            results,
            adaptive_payload={
                "enabled": True,
                "max_iterations": 3,
                "convergence_threshold": 0.2,
                "iterations": [
                    {"iteration": 0, "executed_slice_ids": ["a", "b"]},
                    {"iteration": 2, "executed_slice_ids": ["b"]},
                ],
            },
        )


def test_contradiction_class_specific_blocking() -> None:
    payload = build_cross_tranche_thresholds(
        {
            "load": {
                "thresholds": {
                    "rho_c": {
                        "threshold_id": "local_tranche:load:rho_c",
                        "estimate": 1.10,
                        "normalized_threshold_value": 1.0,
                        "lower_bound": 1.20,
                        "upper_bound": 1.10,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.8,
                        "support_density": 0.8,
                        "support_span": 0.1,
                        "support_confidence": 0.8,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-load"},
                        "support_metrics": {"normalized_bracket_width": 0.1},
                        "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                        "normalization_basis_value": 1.1,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
            "weather": {
                "thresholds": {
                    "rho_c": {
                        "threshold_id": "local_tranche:weather:rho_c",
                        "estimate": 1.12,
                        "normalized_threshold_value": 1.018,
                        "lower_bound": 1.05,
                        "upper_bound": 1.18,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.82,
                        "support_density": 0.8,
                        "support_span": 0.13,
                        "support_confidence": 0.82,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-weather"},
                        "support_metrics": {"normalized_bracket_width": 0.08},
                        "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                        "normalization_basis_value": 1.1,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
        }
    )

    rho_global = payload["global_thresholds"]["rho_c"]
    contradiction_types = {item["contradiction_type"] for item in payload["contradictions"]}

    assert rho_global["promotion_state"]["promoted"] is False
    assert rho_global["threshold_promotion_decision"] == "BLOCKED_BY_CONTRADICTION"
    assert "CROSS_TRANCHE_CONFLICT" in contradiction_types
    assert rho_global["promotion_governance_outcome"] == PromotionGovernanceOutcome.GLOBAL_BLOCK.value


def test_cross_tranche_inconsistency_flagging() -> None:
    payload = build_cross_tranche_thresholds(
        {
            "load": {
                "thresholds": {
                    "rho_c": {
                        "estimate": 1.10,
                        "lower_bound": 1.00,
                        "upper_bound": 1.20,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.8,
                        "support_density": 0.8,
                        "support_span": 0.2,
                        "support_confidence": 0.8,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-load"},
                        "support_metrics": {"normalized_bracket_width": 0.1},
                        "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                        "normalization_basis_value": 1.1,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
            "weather": {
                "thresholds": {
                    "rho_c": {
                        "estimate": 1.90,
                        "lower_bound": 1.80,
                        "upper_bound": 2.00,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.82,
                        "support_density": 0.8,
                        "support_span": 0.2,
                        "support_confidence": 0.82,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-weather"},
                        "support_metrics": {"normalized_bracket_width": 0.1},
                        "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                        "normalization_basis_value": 1.1,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
        }
    )

    rho_global = payload["global_thresholds"]["rho_c"]
    finding_kinds = {finding["kind"] for finding in payload["consistency_findings"]}

    assert rho_global["promotion_state"]["promoted"] is False
    assert rho_global["status"] == ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value
    assert rho_global["threshold_promotion_decision"] == "BLOCKED_BY_CONTRADICTION"
    assert "contradiction" in finding_kinds
    assert "instability" in finding_kinds
    assert payload["contradictions"]


def test_cross_tranche_multiple_contradictions_collapse_to_single_governance_outcome() -> None:
    payload = build_cross_tranche_thresholds(
        {
            "load": {
                "thresholds": {
                    "rho_c": {
                        "threshold_id": "local_tranche:load:rho_c",
                        "estimate": 1.10,
                        "normalized_threshold_value": 1.0,
                        "lower_bound": 1.05,
                        "upper_bound": 1.15,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.8,
                        "support_density": 0.7,
                        "support_span": 0.1,
                        "support_confidence": 0.7,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-load"},
                        "support_metrics": {"normalized_bracket_width": 0.05},
                        "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                        "normalization_basis_value": 1.1,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
            "weather": {
                "thresholds": {
                    "rho_c": {
                        "threshold_id": "local_tranche:weather:rho_c",
                        "estimate": -1.12,
                        "normalized_threshold_value": -1.018,
                        "lower_bound": 1.30,
                        "upper_bound": 1.20,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.82,
                        "support_density": 0.7,
                        "support_span": 0.1,
                        "support_confidence": 0.7,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-weather"},
                        "support_metrics": {"normalized_bracket_width": 0.05},
                        "normalization_basis_origin": "GOVERNED_FALLBACK",
                        "normalization_basis_value": 0.3,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
        }
    )

    contradiction_types = {item["contradiction_type"] for item in payload["contradictions"]}
    rho_global = payload["global_thresholds"]["rho_c"]
    assert "CROSS_TRANCHE_CONFLICT" in contradiction_types
    assert "ENVELOPE_VIOLATION" in contradiction_types
    assert rho_global["promotion_governance_outcome"] == PromotionGovernanceOutcome.GLOBAL_BLOCK.value
    assert rho_global["promotion_state"]["promotion_governance_outcome"] == PromotionGovernanceOutcome.GLOBAL_BLOCK.value


def test_axis_isolation_blocks_non_isolated_threshold_promotion(tmp_path: Path) -> None:
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="a",
                tranche_name="synthetic",
                seed=1,
                resolved_params={"alpha": 0.0, "beta": 0.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="b",
                tranche_name="synthetic",
                seed=2,
                resolved_params={"alpha": 0.5, "beta": 0.8},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="c",
                tranche_name="synthetic",
                seed=3,
                resolved_params={"alpha": 1.0, "beta": 1.6},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
    ]
    points = phase_points_from_slice_results(results)
    payload = build_threshold_estimates(
        "synthetic",
        points,
        detect_transition_regions(points),
        replay_points=points,
        replay_transition_regions=detect_transition_regions(points),
        dominant_axis="alpha",
    )

    assert payload["axis_isolation"]["is_isolated"] is False
    assert payload["thresholds"]["lambda_c"]["promotion_state"]["promoted"] is False
    assert payload["thresholds"]["lambda_c"]["promotion_state"]["decision"] == "retained_as_mixed_axis_leakage"


def test_cross_tranche_global_promotion_requires_phase_derived_consensus() -> None:
    payload = build_cross_tranche_thresholds(
        {
            "load": {
                "thresholds": {
                    "rho_c": {
                        "estimate": 1.10,
                        "normalized_threshold_value": 1.0,
                        "lower_bound": 1.00,
                        "upper_bound": 1.20,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.8,
                        "support_density": 0.8,
                        "support_span": 0.2,
                        "support_confidence": 0.8,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-load"},
                        "support_metrics": {"normalized_bracket_width": 0.1},
                        "is_isolated": True,
                        "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                        "normalization_basis_value": 1.1,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
            "weather": {
                "thresholds": {
                    "rho_c": {
                        "estimate": 1.12,
                        "normalized_threshold_value": 1.018181818,
                        "lower_bound": 1.05,
                        "upper_bound": 1.18,
                        "status": ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value,
                        "evidence_type": ThresholdEvidenceType.PHASE_DERIVED.value,
                        "confidence": 0.82,
                        "support_density": 0.8,
                        "support_span": 0.13,
                        "support_confidence": 0.82,
                        "nuisance_vector": {"congestion": 0.0, "trust": 0.0, "comms": 0.0, "navigation": 0.0, "weather": 0.0, "contingency": 0.0},
                        "dominant_axis": None,
                        "entropy": 0.0,
                        "monotonicity_violation": False,
                        "monotonicity_block_reason": None,
                        "contradictions": [],
                        "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        "promotion_state": {
                            "promoted": True,
                            "decision": "promoted_to_tranche_boundary",
                            "promotion_governance_outcome": PromotionGovernanceOutcome.ALLOW.value,
                        },
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-weather"},
                        "support_metrics": {"normalized_bracket_width": 0.08},
                        "is_isolated": True,
                        "normalization_basis_origin": "TRANCHE_ADMISSIBILITY_ENVELOPE",
                        "normalization_basis_value": 1.1,
                        "normalization_basis_confidence": 0.8,
                    }
                }
            },
        }
    )

    rho_global = payload["global_thresholds"]["rho_c"]
    assert rho_global["promotion_state"]["promoted"] is True
    assert rho_global["evidence_type"] == ThresholdEvidenceType.PHASE_DERIVED.value
    assert rho_global["normalized_threshold_value"] is not None
    assert rho_global["threshold_promotion_decision"] == "PROMOTED"


def test_canonical_threshold_normalization_derives_compatibility_fields() -> None:
    normalized = normalize_threshold_payload(_base_governed_threshold_payload())

    rho_record = normalized["thresholds"]["rho_c"]
    assert rho_record["admissibility_support_density"] == rho_record["support_density"]
    assert rho_record["admissibility_support_span"] == rho_record["support_span"]
    assert rho_record["admissibility_support_confidence"] == rho_record["support_confidence"]
    assert rho_record["nuisance_dominant_axis"] == rho_record["dominant_axis"] == "trust"
    assert rho_record["nuisance_entropy"] == rho_record["entropy"]
    assert rho_record["mechanism_leakage_sources"] == ["trust_degradation"]
    assert rho_record["mechanism_leakage_score"] == pytest.approx(0.4)


def test_governed_artifact_round_trips_preserve_canonical_shape(tmp_path: Path) -> None:
    local_results = _governed_round_trip_results(tmp_path / "local_results", "synthetic")
    local_output = tmp_path / "local_artifacts"
    local_output.mkdir(parents=True, exist_ok=True)

    local_paths = [
        write_threshold_estimates_json(local_output, "synthetic", local_results),
        write_threshold_ledger_json(local_output, "synthetic", local_results),
        write_promotion_decisions_json(local_output, "synthetic", local_results),
    ]

    local_payloads = {
        path.name: _canonical_round_trip(json.loads(path.read_text(encoding="utf-8")))
        for path in local_paths
    }
    assert local_payloads["threshold_estimates.json"]["thresholds"]["rho_c"]["mechanism_leakage_sources"] is not None
    assert local_payloads["threshold_ledger.json"]["promotion_history"]
    assert local_payloads["promotion_decisions.json"]["decisions"][0]["mechanism_leakage_score"] >= 0.0

    cross_results = {
        "load": _governed_round_trip_results(tmp_path / "load_results", "load"),
        "weather": _governed_round_trip_results(tmp_path / "weather_results", "weather"),
    }
    cross_output = tmp_path / "cross_artifacts"
    cross_output.mkdir(parents=True, exist_ok=True)
    cross_paths = [
        write_cross_tranche_thresholds_json(cross_output, cross_results),
        write_contradictions_json(cross_output, cross_results),
        write_cross_tranche_threshold_ledger_json(cross_output, cross_results),
        write_cross_tranche_promotion_decisions_json(cross_output, cross_results),
    ]

    cross_payloads = {
        path.name: _canonical_round_trip(json.loads(path.read_text(encoding="utf-8")))
        for path in cross_paths
    }
    assert cross_payloads["cross_tranche_thresholds.json"]["global_thresholds"]["rho_c"][
        "mechanism_leakage_sources"
    ] is not None
    assert cross_payloads["contradictions.json"]["global_thresholds"]["rho_c"][
        "promotion_governance_outcome"
    ] in {
        PromotionGovernanceOutcome.ALLOW.value,
        PromotionGovernanceOutcome.LOCAL_BLOCK.value,
        PromotionGovernanceOutcome.GLOBAL_BLOCK.value,
    }
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["entries"][0]["mechanism_leakage_score"] >= 0.0
    assert cross_payloads["cross_tranche_promotion_decisions.json"]["decisions"][0][
        "mechanism_leakage_sources"
    ] is not None


def test_src_does_not_branch_on_contradiction_taxonomy_outside_thresholds() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    thresholds_path = src_root / "avn" / "phase_space" / "thresholds.py"
    taxonomy_literals = {
        "LOCAL_INCONSISTENCY",
        "CROSS_TRANCHE_CONFLICT",
        "ENVELOPE_VIOLATION",
        "NON_MONOTONIC_THRESHOLD",
        "NuisanceDominance",
        "nuisance_dominance",
        "non_monotonicity",
    }
    violations: list[str] = []

    for path in sorted(src_root.rglob("*.py")):
        if path == thresholds_path or "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare) and any(
                isinstance(op, (ast.Eq, ast.NotEq, ast.In, ast.NotIn)) for op in node.ops
            ):
                subtree = list(ast.walk(node))
                branches_on_contradiction = any(
                    isinstance(item, ast.Name) and item.id in {"contradiction_type", "contradiction_types"}
                    for item in subtree
                ) or any(
                    isinstance(item, ast.Constant) and item.value == "contradiction_type"
                    for item in subtree
                )
                references_taxonomy = any(
                    (
                        isinstance(item, ast.Constant)
                        and isinstance(item.value, str)
                        and item.value in taxonomy_literals
                    )
                    or (
                        isinstance(item, ast.Name)
                        and item.id.startswith("CONTRADICTION_")
                    )
                    for item in subtree
                )
                if branches_on_contradiction and references_taxonomy:
                    violations.append(f"{path}:{node.lineno}")
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                value = node.value if isinstance(node, ast.AnnAssign) else node.value
                targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
                if value is None:
                    continue
                names = [
                    target.id
                    for target in targets
                    if isinstance(target, ast.Name)
                ]
                if not any("contradiction" in name for name in names):
                    continue
                values = list(ast.walk(value))
                if any(
                    (
                        isinstance(item, ast.Constant)
                        and isinstance(item.value, str)
                        and item.value in taxonomy_literals
                    )
                    or (
                        isinstance(item, ast.Name)
                        and item.id.startswith("CONTRADICTION_")
                    )
                    for item in values
                ):
                    violations.append(f"{path}:{node.lineno}")

    assert not violations, f"Contradiction taxonomy drift detected outside thresholds.py: {violations}"


def test_cli_passes_adaptive_flags(monkeypatch, tmp_path: Path, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_run_tranche(tranche_name: str, **kwargs) -> TrancheRunResult:
        captured["tranche_name"] = tranche_name
        captured.update(kwargs)
        output_dir = tmp_path / "cli"
        output_dir.mkdir()
        return TrancheRunResult(
            tranche_name=tranche_name,
            output_dir=output_dir,
            aggregate_csv_path=output_dir / "aggregate.csv",
            slice_results_json_path=output_dir / "slice_results.json",
            phase_boundaries_json_path=output_dir / "phase_boundaries.json",
            slice_results=[],
            phase_map_json_path=output_dir / "phase_map.json",
            transition_regions_json_path=output_dir / "transition_regions.json",
            threshold_estimates_json_path=output_dir / "threshold_estimates.json",
            convergence_report_json_path=output_dir / "convergence_report.json",
            adaptive_metadata={"iterations": [], "stopping_reason": "converged"},
        )

    monkeypatch.setattr("avn.sweep.run_tranche", fake_run_tranche)

    exit_code = main(
        [
            "--tranche",
            "load",
            "--adaptive",
            "--max-iterations",
            "3",
            "--convergence-threshold",
            "0.25",
        ]
    )
    capsys.readouterr()

    assert exit_code == 0
    assert captured["tranche_name"] == "load"
    assert captured["adaptive"] is True
    assert captured["max_iterations"] == 3
    assert captured["convergence_threshold"] == 0.25
