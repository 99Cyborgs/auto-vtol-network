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
    write_phase_map_json,
    write_promotion_decisions_json,
    write_slice_results_json,
    write_threshold_estimates_json,
    write_threshold_ledger_json,
    _admissibility_overlay_summary,
    _cross_tranche_summary_summary,
    _contradictions_summary,
    _consistency_findings_summary,
    _convergence_report_summary,
    _global_phase_map_summary,
    _phase_boundaries_summary,
    _phase_map_summary,
    _promotion_record_summary,
    _slice_results_summary,
    _threshold_catalog_summary,
    _transition_regions_summary,
    _tranche_comparison_summary,
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


def test_build_phase_space_outputs_attach_phase_and_runtime_summaries(tmp_path: Path) -> None:
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="summary_a",
                tranche_name="synthetic",
                seed=1,
                resolved_params={"alpha": 0.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="corridor_capacity_exceeded",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="summary_b",
                tranche_name="synthetic",
                seed=2,
                resolved_params={"alpha": 0.5},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name="synthetic",
            slice_definition=TrancheSlice(
                slice_id="summary_c",
                tranche_name="synthetic",
                seed=3,
                resolved_params={"alpha": 1.0},
                base_config_path=tmp_path / "synthetic.toml",
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
    ]

    payload = build_phase_space_outputs(
        "synthetic",
        results,
        adaptive_payload={
            "enabled": True,
            "max_iterations": 2,
            "convergence_threshold": 0.2,
            "stopping_reason": "converged",
            "iterations": [
                {"iteration": 0, "executed_slice_ids": ["summary_a", "summary_b"]},
                {"iteration": 1, "executed_slice_ids": ["summary_c"]},
            ],
        },
    )

    phase_summary = payload["phase_map"]["summary"]
    overlay_summary = payload["admissibility_overlay"]["summary"]
    convergence_summary = payload["convergence_report"]["summary"]

    assert payload["phase_map"]["artifact_type"] == "phase_map"
    assert payload["phase_map"]["analysis_contract_version"] == 2
    assert payload["phase_map"]["scope"] == "local_tranche"
    assert payload["phase_map"]["tranche_name"] == "synthetic"
    assert payload["transition_regions"]["artifact_type"] == "transition_regions"
    assert payload["transition_regions"]["scope"] == "local_tranche"
    assert payload["admissibility_overlay"]["artifact_type"] == "admissibility_overlay"
    assert payload["admissibility_overlay"]["analysis_contract_version"] == 2
    assert payload["admissibility_overlay"]["scope"] == "local_tranche"
    assert payload["convergence_report"]["artifact_type"] == "convergence_report"
    assert payload["convergence_report"]["analysis_contract_version"] == 2
    assert payload["convergence_report"]["tranche_name"] == "synthetic"
    assert payload["convergence_report"]["scope"] == "local_tranche"
    assert phase_summary["point_count"] == payload["phase_map"]["point_count"]
    assert phase_summary["axis_count"] == len(payload["phase_map"]["axes"])
    assert phase_summary["dominant_mechanism"] == "NODE_SATURATION"
    assert sum(phase_summary["admissibility_state_counts"].values()) == payload["phase_map"]["point_count"]
    assert overlay_summary["point_label_count"] == len(payload["admissibility_overlay"]["point_labels"])
    assert sum(overlay_summary["state_region_counts"].values()) == (
        len(payload["admissibility_overlay"]["admissible_region_candidates"])
        + len(payload["admissibility_overlay"]["inadmissible_region_candidates"])
        + len(payload["admissibility_overlay"]["unresolved_regions"])
    )
    assert convergence_summary["iteration_count"] == len(payload["convergence_report"]["iterations"])
    assert convergence_summary["stopping_reason"] == "converged"
    assert convergence_summary["final_iteration"]["cumulative_slice_count"] == 3


def test_slice_results_payload_includes_summary(tmp_path: Path) -> None:
    tranche = _synthetic_tranche(tmp_path)
    results = [
        _result_for_slice(
            tmp_path,
            tranche_name=tranche.tranche_name,
            slice_definition=TrancheSlice(
                slice_id="slice_summary_a",
                tranche_name=tranche.tranche_name,
                seed=1,
                resolved_params={"alpha": 0.0},
                base_config_path=tranche.base_config_path,
            ),
            mechanism="corridor_capacity_exceeded",
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche.tranche_name,
            slice_definition=TrancheSlice(
                slice_id="slice_summary_b",
                tranche_name=tranche.tranche_name,
                seed=2,
                resolved_params={"alpha": 0.5},
                base_config_path=tranche.base_config_path,
            ),
            mechanism="node_service_collapse",
            force_admissible=True,
        ),
        _result_for_slice(
            tmp_path,
            tranche_name=tranche.tranche_name,
            slice_definition=TrancheSlice(
                slice_id="slice_summary_c",
                tranche_name=tranche.tranche_name,
                seed=3,
                resolved_params={"alpha": 1.0},
                base_config_path=tranche.base_config_path,
            ),
            mechanism="node_service_collapse",
        ),
    ]
    output_dir = tmp_path / "slice_summary_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    write_slice_results_json(
        output_dir,
        tranche,
        results,
        adaptive_payload={
            "enabled": True,
            "iterations": [
                {"iteration": 0, "executed_slice_ids": ["slice_summary_a", "slice_summary_b"]},
                {"iteration": 1, "executed_slice_ids": ["slice_summary_c"]},
            ],
        },
    )

    payload = json.loads((output_dir / "slice_results.json").read_text(encoding="utf-8"))
    summary = payload["summary"]

    assert payload["artifact_type"] == "slice_results"
    assert payload["analysis_contract_version"] == 2
    assert payload["scope"] == "local_tranche"
    assert payload["tranche_name"] == tranche.tranche_name
    assert summary["slice_count"] == len(payload["results"])
    assert summary["dominant_mechanism"] == "NODE_SATURATION"
    assert summary["dominant_mechanism_counts"] == {
        "CORRIDOR_CONGESTION": 1,
        "NODE_SATURATION": 2,
    }
    assert summary["safe_region_exit_distribution"] == {
        "corridor_load_ratio": 1,
        "no_exit": 1,
        "queue_ratio": 1,
    }
    assert summary["replay_hash_coverage"]["with_replay_hash_count"] == len(payload["results"])
    assert summary["replay_hash_coverage"]["missing_replay_hash_count"] == 0
    assert summary["replay_hash_coverage"]["unique_replay_hash_count"] == len(payload["results"])
    assert summary["adaptive_enabled"] is True
    assert summary["adaptive_iteration_count"] == 2
    assert summary["threshold_count"] == len(payload["thresholds"])
    assert summary["promoted_threshold_count"] == sum(
        1
        for record in payload["thresholds"].values()
        if record["promotion_state"]["promoted"]
    )
    assert summary["contradiction_threshold_count"] == sum(
        1
        for record in payload["thresholds"].values()
        if record["contradictions"]
    )


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
        write_phase_map_json(local_output, "synthetic", local_results),
        write_threshold_estimates_json(local_output, "synthetic", local_results),
        write_threshold_ledger_json(local_output, "synthetic", local_results),
        write_promotion_decisions_json(local_output, "synthetic", local_results),
    ]

    local_payloads = {
        path.name: _canonical_round_trip(json.loads(path.read_text(encoding="utf-8")))
        for path in local_paths
    }
    assert local_payloads["phase_map.json"]["artifact_type"] == "phase_map"
    assert local_payloads["phase_map.json"]["analysis_contract_version"] == 2
    assert local_payloads["phase_map.json"]["scope"] == "local_tranche"
    assert local_payloads["phase_map.json"]["tranche_name"] == "synthetic"
    assert local_payloads["threshold_estimates.json"]["artifact_type"] == "threshold_estimates"
    assert local_payloads["threshold_estimates.json"]["thresholds"]["rho_c"]["mechanism_leakage_sources"] is not None
    assert local_payloads["threshold_estimates.json"]["summary"]["threshold_count"] == len(
        local_payloads["threshold_estimates.json"]["thresholds"]
    )
    assert (
        local_payloads["threshold_estimates.json"]["summary"]["promoted_count"]
        + local_payloads["threshold_estimates.json"]["summary"]["blocked_count"]
        + local_payloads["threshold_estimates.json"]["summary"]["unknown_promotion_count"]
        == len(local_payloads["threshold_estimates.json"]["thresholds"])
    )
    assert local_payloads["threshold_estimates.json"]["summary"]["contradiction_threshold_count"] == len(
        local_payloads["threshold_estimates.json"]["summary"]["contradiction_thresholds"]
    )
    assert local_payloads["threshold_ledger.json"]["artifact_type"] == "threshold_ledger"
    assert local_payloads["threshold_ledger.json"]["promotion_history"]
    assert local_payloads["promotion_decisions.json"]["artifact_type"] == "promotion_decisions"
    assert local_payloads["promotion_decisions.json"]["decisions"][0]["mechanism_leakage_score"] >= 0.0
    assert local_payloads["threshold_ledger.json"]["summary"]["record_count"] == len(
        local_payloads["threshold_ledger.json"]["entries"]
    )
    assert (
        local_payloads["threshold_ledger.json"]["summary"]["promoted_count"]
        + local_payloads["threshold_ledger.json"]["summary"]["blocked_count"]
        == len(local_payloads["threshold_ledger.json"]["entries"])
    )
    assert local_payloads["threshold_ledger.json"]["summary"]["blocker_counts"] == (
        local_payloads["threshold_ledger.json"]["summary"]["promotion_blocker_counts"]
    )
    assert local_payloads["threshold_ledger.json"]["summary"]["contradiction_thresholds"] == (
        local_payloads["threshold_ledger.json"]["summary"]["thresholds_with_contradictions"]
    )
    assert local_payloads["promotion_decisions.json"]["summary"]["record_count"] == len(
        local_payloads["promotion_decisions.json"]["decisions"]
    )
    assert (
        local_payloads["promotion_decisions.json"]["summary"]["acceptance_counts"].get("accepted", 0)
        + local_payloads["promotion_decisions.json"]["summary"]["acceptance_counts"].get("rejected", 0)
        == len(local_payloads["promotion_decisions.json"]["decisions"])
    )
    assert local_payloads["promotion_decisions.json"]["summary"]["accepted_count"] == (
        local_payloads["promotion_decisions.json"]["summary"]["acceptance_counts"].get("accepted", 0)
    )
    assert local_payloads["promotion_decisions.json"]["summary"]["accepted_count"] == len(
        local_payloads["promotion_decisions.json"]["summary"]["accepted_thresholds"]
    )

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
    assert cross_payloads["cross_tranche_thresholds.json"]["artifact_type"] == "cross_tranche_thresholds"
    assert cross_payloads["cross_tranche_thresholds.json"]["global_thresholds"]["rho_c"][
        "mechanism_leakage_sources"
    ] is not None
    assert cross_payloads["cross_tranche_thresholds.json"]["summary"]["threshold_count"] == len(
        cross_payloads["cross_tranche_thresholds.json"]["global_thresholds"]
    )
    assert cross_payloads["cross_tranche_thresholds.json"]["promotion_summary"]["record_count"] == len(
        cross_payloads["cross_tranche_thresholds.json"]["promotion_decisions"]
    )
    assert cross_payloads["cross_tranche_thresholds.json"]["consistency_summary"]["finding_count"] == len(
        cross_payloads["cross_tranche_thresholds.json"]["consistency_findings"]
    )
    assert cross_payloads["cross_tranche_thresholds.json"]["contradiction_summary"]["contradiction_count"] == len(
        cross_payloads["cross_tranche_thresholds.json"]["contradictions"]
    )
    assert cross_payloads["contradictions.json"]["artifact_type"] == "contradictions"
    assert cross_payloads["contradictions.json"]["global_thresholds"]["rho_c"][
        "promotion_governance_outcome"
    ] in {
        PromotionGovernanceOutcome.ALLOW.value,
        PromotionGovernanceOutcome.LOCAL_BLOCK.value,
        PromotionGovernanceOutcome.GLOBAL_BLOCK.value,
    }
    assert cross_payloads["contradictions.json"]["summary"]["contradiction_count"] == len(
        cross_payloads["contradictions.json"]["contradictions"]
    )
    assert cross_payloads["contradictions.json"]["threshold_summary"]["threshold_count"] == len(
        cross_payloads["contradictions.json"]["global_thresholds"]
    )
    assert cross_payloads["contradictions.json"]["promotion_summary"]["record_count"] == len(
        cross_payloads["contradictions.json"]["promotion_decisions"]
    )
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["artifact_type"] == "cross_tranche_threshold_ledger"
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["entries"][0]["mechanism_leakage_score"] >= 0.0
    assert (
        cross_payloads["cross_tranche_promotion_decisions.json"]["artifact_type"]
        == "cross_tranche_promotion_decisions"
    )
    assert cross_payloads["cross_tranche_promotion_decisions.json"]["decisions"][0][
        "mechanism_leakage_sources"
    ] is not None
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["summary"]["record_count"] == len(
        cross_payloads["cross_tranche_threshold_ledger.json"]["entries"]
    )
    assert cross_payloads["cross_tranche_promotion_decisions.json"]["summary"]["record_count"] == len(
        cross_payloads["cross_tranche_promotion_decisions.json"]["decisions"]
    )
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["consistency_summary"]["finding_count"] == len(
        cross_payloads["cross_tranche_threshold_ledger.json"]["consistency_findings"]
    )
    assert cross_payloads["cross_tranche_promotion_decisions.json"]["consistency_summary"]["finding_count"] == len(
        cross_payloads["cross_tranche_promotion_decisions.json"]["consistency_findings"]
    )
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["summary"]["blocker_counts"] == (
        cross_payloads["cross_tranche_threshold_ledger.json"]["summary"]["promotion_blocker_counts"]
    )
    assert cross_payloads["cross_tranche_promotion_decisions.json"]["summary"]["accepted_count"] == len(
        cross_payloads["cross_tranche_promotion_decisions.json"]["summary"]["accepted_thresholds"]
    )
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["consistency_summary"]["finding_kind_counts"] == (
        cross_payloads["cross_tranche_threshold_ledger.json"]["consistency_summary"]["kind_counts"]
    )
    assert cross_payloads["cross_tranche_threshold_ledger.json"]["consistency_summary"]["affected_threshold_count"] == len(
        cross_payloads["cross_tranche_threshold_ledger.json"]["consistency_summary"]["affected_thresholds"]
    )


def test_promotion_record_summary_handles_sparse_records_with_stable_aliases() -> None:
    summary = _promotion_record_summary(
        [
            {
                "threshold": "rho_c",
                "threshold_promotion_decision": "PROMOTED",
                "promotion_blockers": [],
                "promotion_governance_outcome": "ALLOW",
                "status": "VALIDATED",
                "evidence_type": "PHASE_DERIVED",
                "accepted": True,
                "contradictions": [],
            },
            {
                "threshold": "lambda_c",
                "threshold_promotion_decision": "BLOCKED_BY_NONMONOTONICITY",
                "promotion_blockers": [
                    "BLOCKED_BY_NONMONOTONICITY",
                    "INSUFFICIENT_ADMISSIBLE_SUPPORT",
                ],
                "promotion_governance_outcome": "GLOBAL_BLOCK",
                "status": "PROXY",
                "evidence_type": "BOUNDED_ESTIMATE",
                "accepted": False,
                "contradictions": [{"contradiction_type": "NON_MONOTONIC_THRESHOLD"}],
            },
            {
                "threshold": "tau_c",
                "promotion_blockers": [],
                "contradictions": [],
            },
            {
                "promotion_blockers": [],
            },
        ]
    )

    assert summary["record_count"] == 4
    assert summary["threshold_record_count"] == 3
    assert summary["missing_threshold_record_count"] == 1
    assert summary["promoted_count"] == 1
    assert summary["blocked_count"] == 1
    assert summary["unknown_promotion_count"] == 1
    assert summary["promoted_thresholds"] == ["rho_c"]
    assert summary["blocked_thresholds"] == ["lambda_c"]
    assert summary["unknown_promotion_thresholds"] == ["tau_c"]
    assert summary["accepted_count"] == 1
    assert summary["rejected_count"] == 1
    assert summary["accepted_thresholds"] == ["rho_c"]
    assert summary["rejected_thresholds"] == ["lambda_c"]
    assert summary["blocker_counts"] == {
        "BLOCKED_BY_NONMONOTONICITY": 1,
        "INSUFFICIENT_ADMISSIBLE_SUPPORT": 1,
    }
    assert summary["blocker_counts"] == summary["promotion_blocker_counts"]
    assert summary["governance_outcome_counts"] == {
        "ALLOW": 1,
        "GLOBAL_BLOCK": 1,
    }
    assert summary["status_counts"] == {
        "PROXY": 1,
        "VALIDATED": 1,
    }
    assert summary["evidence_type_counts"] == {
        "BOUNDED_ESTIMATE": 1,
        "PHASE_DERIVED": 1,
    }
    assert summary["contradiction_thresholds"] == ["lambda_c"]
    assert summary["contradiction_thresholds"] == summary["thresholds_with_contradictions"]
    assert summary["acceptance_counts"] == {
        "accepted": 1,
        "rejected": 1,
    }

    empty_summary = _promotion_record_summary([])
    assert empty_summary == {
        "record_count": 0,
        "threshold_record_count": 0,
        "missing_threshold_record_count": 0,
        "promoted_count": 0,
        "blocked_count": 0,
        "unknown_promotion_count": 0,
        "promoted_thresholds": [],
        "blocked_thresholds": [],
        "unknown_promotion_thresholds": [],
        "accepted_count": 0,
        "rejected_count": 0,
        "accepted_thresholds": [],
        "rejected_thresholds": [],
        "contradiction_thresholds": [],
        "thresholds_with_contradictions": [],
        "blocker_counts": {},
        "decision_counts": {},
        "threshold_promotion_decision_counts": {},
        "promotion_blocker_counts": {},
        "governance_outcome_counts": {},
        "status_counts": {},
        "evidence_type_counts": {},
        "acceptance_counts": {},
    }


def test_threshold_catalog_summary_handles_sparse_records_with_stable_aliases() -> None:
    summary = _threshold_catalog_summary(
        {
            "rho_c": {
                "estimate": 1.12,
                "status": "VALIDATED",
                "evidence_type": "PHASE_DERIVED",
                "promotion_governance_outcome": "ALLOW",
                "threshold_promotion_decision": "PROMOTED",
                "promotion_state": {"decision": "promoted_to_tranche_boundary"},
                "contradictions": [],
                "is_isolated": True,
                "normalized_threshold_value": 1.0,
                "monotonicity_violation": False,
            },
            "lambda_c": {
                "estimate": 0.84,
                "status": "PROXY",
                "evidence_type": "BOUNDED_ESTIMATE",
                "promotion_governance_outcome": "LOCAL_BLOCK",
                "threshold_promotion_decision": "BLOCKED_BY_NONMONOTONICITY",
                "promotion_state": {"decision": "retained_as_non_monotonic_proxy"},
                "contradictions": [{"contradiction_type": "NON_MONOTONIC_THRESHOLD"}],
                "is_isolated": False,
                "normalized_threshold_value": None,
                "monotonicity_violation": True,
            },
            "tau_c": {
                "status": "INSUFFICIENT_DATA",
                "evidence_type": "PROXY_ONLY",
                "promotion_state": {},
                "contradictions": [],
                "is_isolated": True,
                "normalized_threshold_value": None,
                "monotonicity_violation": False,
            },
        }
    )

    assert summary["threshold_count"] == 3
    assert summary["estimated_threshold_count"] == 2
    assert summary["missing_estimate_count"] == 1
    assert summary["thresholds_with_estimates"] == ["lambda_c", "rho_c"]
    assert summary["thresholds_without_estimates"] == ["tau_c"]
    assert summary["promoted_count"] == 1
    assert summary["blocked_count"] == 1
    assert summary["unknown_promotion_count"] == 1
    assert summary["promoted_thresholds"] == ["rho_c"]
    assert summary["blocked_thresholds"] == ["lambda_c"]
    assert summary["unknown_promotion_thresholds"] == ["tau_c"]
    assert summary["contradiction_threshold_count"] == 1
    assert summary["contradiction_thresholds"] == ["lambda_c"]
    assert summary["contradiction_thresholds"] == summary["thresholds_with_contradictions"]
    assert summary["isolated_threshold_count"] == 2
    assert summary["isolated_thresholds"] == ["rho_c", "tau_c"]
    assert summary["normalized_threshold_count"] == 1
    assert summary["normalized_thresholds"] == ["rho_c"]
    assert summary["monotonicity_violation_count"] == 1
    assert summary["monotonicity_violation_thresholds"] == ["lambda_c"]
    assert summary["status_counts"] == {
        "INSUFFICIENT_DATA": 1,
        "PROXY": 1,
        "VALIDATED": 1,
    }
    assert summary["evidence_type_counts"] == {
        "BOUNDED_ESTIMATE": 1,
        "PHASE_DERIVED": 1,
        "PROXY_ONLY": 1,
    }
    assert summary["promotion_governance_outcome_counts"] == {
        "ALLOW": 1,
        "LOCAL_BLOCK": 1,
    }
    assert summary["threshold_promotion_decision_counts"] == {
        "BLOCKED_BY_NONMONOTONICITY": 1,
        "PROMOTED": 1,
    }
    assert summary["decision_counts"] == {
        "promoted_to_tranche_boundary": 1,
        "retained_as_non_monotonic_proxy": 1,
    }

    empty_summary = _threshold_catalog_summary({})
    assert empty_summary == {
        "threshold_count": 0,
        "estimated_threshold_count": 0,
        "missing_estimate_count": 0,
        "thresholds_with_estimates": [],
        "thresholds_without_estimates": [],
        "promoted_count": 0,
        "blocked_count": 0,
        "unknown_promotion_count": 0,
        "promoted_thresholds": [],
        "blocked_thresholds": [],
        "unknown_promotion_thresholds": [],
        "contradiction_threshold_count": 0,
        "contradiction_thresholds": [],
        "thresholds_with_contradictions": [],
        "isolated_threshold_count": 0,
        "isolated_thresholds": [],
        "normalized_threshold_count": 0,
        "normalized_thresholds": [],
        "monotonicity_violation_count": 0,
        "monotonicity_violation_thresholds": [],
        "status_counts": {},
        "evidence_type_counts": {},
        "promotion_governance_outcome_counts": {},
        "threshold_promotion_decision_counts": {},
        "decision_counts": {},
    }


def test_phase_map_summary_handles_presence_and_absence() -> None:
    summary = _phase_map_summary(
        {
            "point_count": 3,
            "axes": ["alpha", "beta"],
            "mechanism_counts": {
                "COMMS_FAILURE": 1,
                "NODE_SATURATION": 2,
            },
            "points": [
                {"admissibility_state": "ADMISSIBLE"},
                {"admissibility_state": "INADMISSIBLE"},
                {"admissibility_state": "ADMISSIBLE"},
            ],
        }
    )

    assert summary == {
        "point_count": 3,
        "axis_count": 2,
        "axes": ["alpha", "beta"],
        "mechanism_count": 2,
        "dominant_mechanism": "NODE_SATURATION",
        "mechanism_ranking": ["NODE_SATURATION", "COMMS_FAILURE"],
        "admissibility_state_counts": {
            "ADMISSIBLE": 2,
            "INADMISSIBLE": 1,
        },
    }

    empty_summary = _phase_map_summary({})
    assert empty_summary == {
        "point_count": 0,
        "axis_count": 0,
        "axes": [],
        "mechanism_count": 0,
        "dominant_mechanism": None,
        "mechanism_ranking": [],
        "admissibility_state_counts": {},
    }


def test_transition_regions_summary_handles_presence_and_absence() -> None:
    summary = _transition_regions_summary(
        {
            "regions": [
                {
                    "transition_axis": "alpha",
                    "dominant_mechanism": "corridor_capacity_exceeded",
                    "entropy": 0.4,
                    "local_gradient": 0.2,
                    "support_count": 3,
                    "estimated_threshold": 0.5,
                },
                {
                    "transition_axis": "alpha",
                    "dominant_mechanism": "node_service_collapse",
                    "entropy": 0.1,
                    "local_gradient": 0.4,
                    "support_count": 2,
                },
                {
                    "transition_axis": "beta",
                    "dominant_mechanism": "node_service_collapse",
                    "entropy": 0.2,
                    "support_count": 5,
                    "estimated_threshold": 0.8,
                },
            ]
        }
    )

    assert summary == {
        "region_count": 3,
        "axis_counts": {
            "alpha": 2,
            "beta": 1,
        },
        "axes": ["alpha", "beta"],
        "dominant_mechanism_counts": {
            "CORRIDOR_CONGESTION": 1,
            "NODE_SATURATION": 2,
        },
        "dominant_mechanism_ordering": ["NODE_SATURATION", "CORRIDOR_CONGESTION"],
        "max_entropy": 0.4,
        "mean_entropy": pytest.approx((0.4 + 0.1 + 0.2) / 3.0),
        "max_local_gradient": 0.4,
        "max_support_count": 5,
        "threshold_estimate_count": 2,
    }

    empty_summary = _transition_regions_summary({})
    assert empty_summary == {
        "region_count": 0,
        "axis_counts": {},
        "axes": [],
        "dominant_mechanism_counts": {},
        "dominant_mechanism_ordering": [],
        "max_entropy": 0.0,
        "mean_entropy": 0.0,
        "max_local_gradient": 0.0,
        "max_support_count": 0,
        "threshold_estimate_count": 0,
    }


def test_admissibility_overlay_summary_handles_placeholder_and_reason_counts() -> None:
    summary = _admissibility_overlay_summary(
        {
            "admissible_region_candidates": [
                {"axis": "alpha", "support_count": 2, "reasons": ["ok"]},
            ],
            "inadmissible_region_candidates": [
                {"axis": "alpha", "support_count": 1, "reasons": ["queue_limit"]},
            ],
            "unresolved_regions": [
                {
                    "axis": None,
                    "support_count": 0,
                    "reasons": ["no_intermediate_admissibility_region_observed"],
                }
            ],
            "point_labels": [
                {"state": "ADMISSIBLE", "reasons": ["ok"]},
                {"state": "INADMISSIBLE", "reasons": ["queue_limit"]},
                {"state": "ADMISSIBLE", "reasons": ["ok"]},
            ],
        }
    )

    assert summary["region_count"] == 3
    assert summary["state_region_counts"] == {
        "ADMISSIBLE": 1,
        "INADMISSIBLE": 1,
        "UNRESOLVED": 1,
    }
    assert summary["state_support_counts"] == {
        "ADMISSIBLE": 2,
        "INADMISSIBLE": 1,
        "UNRESOLVED": 0,
    }
    assert summary["point_label_count"] == 3
    assert summary["point_label_state_counts"] == {
        "ADMISSIBLE": 2,
        "INADMISSIBLE": 1,
    }
    assert summary["axis_count"] == 1
    assert summary["axes"] == ["alpha"]
    assert summary["reason_counts"] == {
        "no_intermediate_admissibility_region_observed": 1,
        "ok": 3,
        "queue_limit": 2,
    }
    assert summary["placeholder_unresolved_region_count"] == 1

    empty_summary = _admissibility_overlay_summary({})
    assert empty_summary == {
        "region_count": 0,
        "state_region_counts": {},
        "state_support_counts": {},
        "point_label_count": 0,
        "point_label_state_counts": {},
        "axis_count": 0,
        "axes": [],
        "reason_counts": {},
        "placeholder_unresolved_region_count": 0,
    }


def test_consistency_findings_summary_handles_presence_and_absence() -> None:
    summary = _consistency_findings_summary(
        [
            {"threshold": "rho_c", "kind": "ORDERING_CONFLICT"},
            {"threshold": "lambda_c", "kind": "ORDERING_CONFLICT"},
            {"threshold": "rho_c", "kind": "STATUS_DIVERGENCE"},
            {"kind": "STATUS_DIVERGENCE"},
            {"threshold": "tau_c"},
            {},
        ]
    )

    assert summary["finding_count"] == 6
    assert summary["kind_counts"] == {
        "ORDERING_CONFLICT": 2,
        "STATUS_DIVERGENCE": 2,
    }
    assert summary["finding_kind_counts"] == summary["kind_counts"]
    assert summary["affected_thresholds"] == ["lambda_c", "rho_c", "tau_c"]
    assert summary["affected_threshold_count"] == 3
    assert summary["thresholds_with_findings"] == summary["affected_thresholds"]
    assert summary["findings_without_threshold_count"] == 2
    assert summary["findings_without_kind_count"] == 2

    empty_summary = _consistency_findings_summary([])
    assert empty_summary == {
        "finding_count": 0,
        "kind_counts": {},
        "finding_kind_counts": {},
        "affected_thresholds": [],
        "affected_threshold_count": 0,
        "thresholds_with_findings": [],
        "findings_without_threshold_count": 0,
        "findings_without_kind_count": 0,
    }


def test_convergence_and_global_phase_map_summaries_handle_presence_and_absence() -> None:
    convergence_summary = _convergence_report_summary(
        {
            "converged": True,
            "stopping_reason": "converged",
            "iterations": [
                {
                    "iteration": 0,
                    "cumulative_slice_count": 2,
                    "transition_region_count": 1,
                    "boundary_shift": 1.0,
                    "classification_stability": 0.0,
                    "converged": False,
                },
                {
                    "iteration": 1,
                    "cumulative_slice_count": 3,
                    "transition_region_count": 1,
                    "boundary_shift": 0.05,
                    "classification_stability": 1.0,
                    "converged": True,
                },
            ],
        }
    )

    assert convergence_summary == {
        "iteration_count": 2,
        "converged": True,
        "stopping_reason": "converged",
        "converged_iteration": 1,
        "final_iteration": {
            "iteration": 1,
            "cumulative_slice_count": 3,
            "transition_region_count": 1,
            "boundary_shift": 0.05,
            "classification_stability": 1.0,
            "converged": True,
        },
    }

    empty_convergence_summary = _convergence_report_summary({})
    assert empty_convergence_summary == {
        "iteration_count": 0,
        "converged": False,
        "stopping_reason": None,
        "converged_iteration": None,
        "final_iteration": None,
    }

    global_summary = _global_phase_map_summary(
        {
            "comms": {
                "point_count": 2,
                "axes": ["gamma"],
                "summary": {"dominant_mechanism": "COMMS_FAILURE"},
            },
            "load": {
                "point_count": 3,
                "axes": ["alpha"],
                "summary": {"dominant_mechanism": "NODE_SATURATION"},
            },
            "trust": {
                "point_count": 1,
                "axes": ["alpha", "trust"],
                "summary": {"dominant_mechanism": "NODE_SATURATION"},
            },
        }
    )

    assert global_summary == {
        "tranche_count": 3,
        "total_point_count": 6,
        "axes": ["alpha", "gamma", "trust"],
        "axis_count": 3,
        "tranche_point_counts": {
            "comms": 2,
            "load": 3,
            "trust": 1,
        },
        "dominant_mechanism_counts": {
            "COMMS_FAILURE": 1,
            "NODE_SATURATION": 2,
        },
        "dominant_mechanism_ordering": ["NODE_SATURATION", "COMMS_FAILURE"],
        "per_tranche_dominant_mechanisms": {
            "comms": "COMMS_FAILURE",
            "load": "NODE_SATURATION",
            "trust": "NODE_SATURATION",
        },
    }

    empty_global_summary = _global_phase_map_summary({})
    assert empty_global_summary == {
        "tranche_count": 0,
        "total_point_count": 0,
        "axes": [],
        "axis_count": 0,
        "tranche_point_counts": {},
        "dominant_mechanism_counts": {},
        "dominant_mechanism_ordering": [],
        "per_tranche_dominant_mechanisms": {},
    }


def test_phase_boundaries_and_tranche_comparison_summaries_handle_presence_and_absence() -> None:
    phase_boundaries_summary = _phase_boundaries_summary(
        {
            "slice_count": 4,
            "failure_mechanism_counts": {
                "corridor_capacity_exceeded": 1,
                "node_service_collapse": 3,
            },
            "safe_region_exit_distribution": {
                "corridor_load_ratio": 1,
                "queue_ratio": 3,
            },
            "parameter_sensitivity": [{"axis": "alpha"}, {"axis": "beta"}],
            "dominant_failure_regions": [{}, {}],
            "dominant_failure_switches": [{}, {}, {}],
            "monotonic_threshold_regions": [{}],
            "governed_transition_boundaries": [{}, {}],
            "rejected_transition_candidates": [{}],
        }
    )

    assert phase_boundaries_summary == {
        "slice_count": 4,
        "dominant_mechanism": "NODE_SATURATION",
        "dominant_mechanism_counts": {
            "CORRIDOR_CONGESTION": 1,
            "NODE_SATURATION": 3,
        },
        "parameter_sensitivity_axis_count": 2,
        "dominant_failure_region_count": 2,
        "switch_count": 3,
        "monotonic_threshold_region_count": 1,
        "governed_transition_boundary_count": 2,
        "rejected_transition_candidate_count": 1,
        "safe_region_exit_count": 4,
    }

    empty_phase_boundaries_summary = _phase_boundaries_summary({})
    assert empty_phase_boundaries_summary == {
        "slice_count": 0,
        "dominant_mechanism": None,
        "dominant_mechanism_counts": {},
        "parameter_sensitivity_axis_count": 0,
        "dominant_failure_region_count": 0,
        "switch_count": 0,
        "monotonic_threshold_region_count": 0,
        "governed_transition_boundary_count": 0,
        "rejected_transition_candidate_count": 0,
        "safe_region_exit_count": 0,
    }

    tranche_comparison_summary = _tranche_comparison_summary(
        {
            "comms": {
                "slice_count": 2,
                "dominant_mechanism": "COMMS_FAILURE",
                "mean_time_to_first_failure": 55.0,
            },
            "load": {
                "slice_count": 3,
                "dominant_mechanism": "corridor_capacity_exceeded",
                "mean_time_to_first_failure": 70.0,
            },
            "trust": {
                "slice_count": 1,
                "dominant_mechanism": "TRUST_FAILURE",
                "mean_time_to_first_failure": 60.0,
            },
        }
    )

    assert tranche_comparison_summary == {
        "tranche_count": 3,
        "total_slice_count": 6,
        "dominant_mechanism_counts": {
            "COMMS_FAILURE": 1,
            "CORRIDOR_CONGESTION": 1,
            "TRUST_FAILURE": 1,
        },
        "dominant_mechanism_ordering": [
            "COMMS_FAILURE",
            "CORRIDOR_CONGESTION",
            "TRUST_FAILURE",
        ],
        "per_tranche_dominant_mechanisms": {
            "comms": "COMMS_FAILURE",
            "load": "CORRIDOR_CONGESTION",
            "trust": "TRUST_FAILURE",
        },
        "fastest_tranche": {
            "tranche_name": "comms",
            "mean_time_to_first_failure": 55.0,
        },
    }

    empty_tranche_comparison_summary = _tranche_comparison_summary({})
    assert empty_tranche_comparison_summary == {
        "tranche_count": 0,
        "total_slice_count": 0,
        "dominant_mechanism_counts": {},
        "dominant_mechanism_ordering": [],
        "per_tranche_dominant_mechanisms": {},
        "fastest_tranche": None,
    }


def test_slice_results_summary_handles_presence_and_absence() -> None:
    summary = _slice_results_summary(
        [
            {
                "dominant_failure_mode": "CORRIDOR_CONGESTION",
                "safe_region_exit_cause": "corridor_load_ratio",
                "time_to_first_failure": 80.0,
                "safe_region_exit_time": 80.0,
                "degraded_mode_dwell_time": 10.0,
                "replay_hash": "hash-a",
            },
            {
                "dominant_failure_mode": "node_service_collapse",
                "safe_region_exit_cause": "",
                "time_to_first_failure": 60.0,
                "safe_region_exit_time": None,
                "degraded_mode_dwell_time": 12.0,
                "replay_hash": "hash-b",
            },
            {
                "first_dominant_failure_mechanism": "stale_information_instability",
                "safe_region_exit_cause": "stale_state_exposure",
                "time_to_first_failure": 55.0,
                "safe_region_exit_time": 55.0,
                "degraded_mode_dwell_time": 14.0,
            },
        ],
        {
            "rho_c": {
                "promotion_state": {"promoted": True},
                "contradictions": [],
            },
            "lambda_c": {
                "promotion_state": {"promoted": False},
                "contradictions": [{"contradiction_type": "NON_MONOTONIC_THRESHOLD"}],
            },
        },
        {
            "enabled": True,
            "iterations": [{"iteration": 0}, {"iteration": 1}],
        },
    )

    assert summary["slice_count"] == 3
    assert summary["dominant_mechanism"] == "COMMS_FAILURE"
    assert summary["dominant_mechanism_counts"] == {
        "COMMS_FAILURE": 1,
        "CORRIDOR_CONGESTION": 1,
        "NODE_SATURATION": 1,
    }
    assert summary["safe_region_exit_distribution"] == {
        "corridor_load_ratio": 1,
        "no_exit": 1,
        "stale_state_exposure": 1,
    }
    assert summary["mean_time_to_first_failure"] == pytest.approx((80.0 + 60.0 + 55.0) / 3.0)
    assert summary["median_time_to_first_failure"] == 60.0
    assert summary["mean_safe_region_exit_time"] == pytest.approx((80.0 + 55.0) / 2.0)
    assert summary["median_safe_region_exit_time"] == 67.5
    assert summary["mean_degraded_mode_dwell_time"] == 12.0
    assert summary["replay_hash_coverage"] == {
        "with_replay_hash_count": 2,
        "missing_replay_hash_count": 1,
        "unique_replay_hash_count": 2,
    }
    assert summary["adaptive_enabled"] is True
    assert summary["adaptive_iteration_count"] == 2
    assert summary["threshold_count"] == 2
    assert summary["promoted_threshold_count"] == 1
    assert summary["contradiction_threshold_count"] == 1

    empty_summary = _slice_results_summary([], {}, None)
    assert empty_summary == {
        "slice_count": 0,
        "dominant_mechanism": None,
        "dominant_mechanism_counts": {},
        "safe_region_exit_distribution": {},
        "mean_time_to_first_failure": None,
        "median_time_to_first_failure": None,
        "mean_safe_region_exit_time": None,
        "median_safe_region_exit_time": None,
        "mean_degraded_mode_dwell_time": None,
        "replay_hash_coverage": {
            "with_replay_hash_count": 0,
            "missing_replay_hash_count": 0,
            "unique_replay_hash_count": 0,
        },
        "adaptive_enabled": False,
        "adaptive_iteration_count": 0,
        "threshold_count": 0,
        "promoted_threshold_count": 0,
        "contradiction_threshold_count": 0,
    }


def test_cross_tranche_summary_summary_handles_presence_and_absence() -> None:
    summary = _cross_tranche_summary_summary(
        {
            "COMMS_FAILURE": {"tranche_name": "comms", "mean_time_to_first_failure": 55.0},
            "NODE_SATURATION": {"tranche_name": "load", "mean_time_to_first_failure": 60.0},
            "corridor_capacity_exceeded": {"tranche_name": "load", "mean_time_to_first_failure": 65.0},
        },
        {
            "coupled_primary_mechanism": "stale_information_instability",
            "ordering_shift_detected": True,
            "ordering_shift_magnitude": 2,
            "emergent_mechanisms": ["REROUTE_CASCADE"],
            "suppressed_mechanisms": ["node_service_collapse", "TRUST_FAILURE"],
        },
    )

    assert summary == {
        "fastest_mechanism_count": 3,
        "fastest_tranche_counts": {
            "comms": 1,
            "load": 2,
        },
        "fastest_tranche_ordering": ["load", "comms"],
        "mechanisms_with_fastest_tranche": {
            "comms": ["COMMS_FAILURE"],
            "load": ["CORRIDOR_CONGESTION", "NODE_SATURATION"],
        },
        "coupled_primary_mechanism": "COMMS_FAILURE",
        "ordering_shift_detected": True,
        "ordering_shift_magnitude": 2,
        "emergent_mechanism_count": 1,
        "emergent_mechanisms": ["REROUTE_CASCADE"],
        "suppressed_mechanism_count": 2,
        "suppressed_mechanisms": ["NODE_SATURATION", "TRUST_FAILURE"],
    }

    empty_summary = _cross_tranche_summary_summary({}, {})
    assert empty_summary == {
        "fastest_mechanism_count": 0,
        "fastest_tranche_counts": {},
        "fastest_tranche_ordering": [],
        "mechanisms_with_fastest_tranche": {},
        "coupled_primary_mechanism": None,
        "ordering_shift_detected": False,
        "ordering_shift_magnitude": 0,
        "emergent_mechanism_count": 0,
        "emergent_mechanisms": [],
        "suppressed_mechanism_count": 0,
        "suppressed_mechanisms": [],
    }


def test_contradictions_summary_handles_presence_and_absence() -> None:
    summary = _contradictions_summary(
        [
            {
                "threshold": "rho_c",
                "contradiction_type": "CROSS_TRANCHE_CONFLICT",
                "contradiction_severity": "BLOCKING",
                "affected_tranches": ["load", "weather"],
                "blocking": True,
            },
            {
                "threshold": "lambda_c",
                "contradiction_type": "NON_MONOTONIC_THRESHOLD",
                "contradiction_severity": "BLOCKING",
                "affected_tranches": ["load"],
                "blocking": True,
            },
            {
                "threshold": "rho_c",
                "contradiction_type": "ENVELOPE_VIOLATION",
                "contradiction_severity": "WARNING",
                "affected_tranches": ["coupled"],
                "blocking": False,
            },
            {
                "contradiction_severity": "WARNING",
                "affected_tranches": [],
            },
            {},
        ]
    )

    assert summary["contradiction_count"] == 5
    assert summary["contradiction_type_counts"] == {
        "CROSS_TRANCHE_CONFLICT": 1,
        "ENVELOPE_VIOLATION": 1,
        "NON_MONOTONIC_THRESHOLD": 1,
    }
    assert summary["severity_counts"] == {
        "BLOCKING": 2,
        "WARNING": 2,
    }
    assert summary["affected_thresholds"] == ["lambda_c", "rho_c"]
    assert summary["affected_threshold_count"] == 2
    assert summary["affected_tranches"] == ["coupled", "load", "weather"]
    assert summary["affected_tranche_count"] == 3
    assert summary["blocking_count"] == 2
    assert summary["non_blocking_count"] == 1
    assert summary["contradictions_without_threshold_count"] == 2
    assert summary["contradictions_without_type_count"] == 2

    empty_summary = _contradictions_summary([])
    assert empty_summary == {
        "contradiction_count": 0,
        "contradiction_type_counts": {},
        "severity_counts": {},
        "affected_thresholds": [],
        "affected_threshold_count": 0,
        "affected_tranches": [],
        "affected_tranche_count": 0,
        "blocking_count": 0,
        "non_blocking_count": 0,
        "contradictions_without_threshold_count": 0,
        "contradictions_without_type_count": 0,
    }


def test_cross_tranche_consistency_summary_remains_zeroed_when_findings_absent(tmp_path: Path) -> None:
    output_dir = tmp_path / "cross_tranche_no_findings"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = write_cross_tranche_threshold_ledger_json(
        output_dir,
        {
            "load": _governed_round_trip_results(tmp_path / "single_tranche_results", "load"),
        },
    )

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["consistency_findings"] == []
    assert payload["consistency_summary"] == {
        "finding_count": 0,
        "kind_counts": {},
        "finding_kind_counts": {},
        "affected_thresholds": [],
        "affected_threshold_count": 0,
        "thresholds_with_findings": [],
        "findings_without_threshold_count": 0,
        "findings_without_kind_count": 0,
    }


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
