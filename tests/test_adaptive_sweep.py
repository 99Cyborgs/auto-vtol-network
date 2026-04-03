from __future__ import annotations

import json
from pathlib import Path

from avn.phase_space.convergence import build_convergence_report
from avn.phase_space.models import PhaseRegion, ThresholdEvidenceStatus, phase_points_from_slice_results
from avn.phase_space.thresholds import build_cross_tranche_thresholds, build_threshold_estimates
from avn.phase_space.transitions import detect_transition_regions
from avn.sweep import TrancheRunResult, analyze_only, main
from avn.sweep_adaptive import adaptive_sweep
from avn.sweep_analysis import (
    ArtifactPaths,
    CommsMetricsSnapshot,
    ContingencyMetricsSnapshot,
    ThroughputMetricsSnapshot,
    TrancheSliceResult,
    TrustMetricsSnapshot,
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
) -> TrancheSliceResult:
    output_dir = tmp_path / slice_definition.slice_id
    output_dir.mkdir(parents=True, exist_ok=True)
    axis_value = float(slice_definition.resolved_params.get("alpha", slice_definition.resolved_params.get("modifiers.demand_multiplier", 0.0)))
    return TrancheSliceResult(
        slice_id=slice_definition.slice_id,
        tranche_name=tranche_name,
        seed=slice_definition.seed,
        resolved_params=dict(slice_definition.resolved_params),
        first_dominant_failure_mechanism=mechanism,
        time_to_first_failure=max(5.0, 100.0 - (axis_value * 40.0)),
        safe_region_exit_time=max(5.0, 100.0 - (axis_value * 40.0)),
        safe_region_exit_cause="corridor_load_ratio" if mechanism == "corridor_capacity_exceeded" else "queue_ratio",
        degraded_mode_dwell_time=axis_value * 10.0,
        trust_metrics_snapshot=TrustMetricsSnapshot(
            unsafe_admission_count=0 if mechanism != "trust_breakdown" else 2,
            quarantine_count=0,
            revocation_count=0,
            trusted_active_fraction=max(0.0, 1.0 - axis_value),
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
    assert replay_thresholds["thresholds"]["lambda_c"]["status"] == ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value
    assert replay_ledger["promotion_history"][-1]["threshold_statuses"]["lambda_c"]["promotion_decision"] == (
        "promoted_to_tranche_boundary"
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
    assert thresholds["promotion_decisions"][0]["decision"] == "retained_as_proxy_weak_support"


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
                        "promotion_state": {"promoted": True, "decision": "promoted_to_tranche_boundary"},
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-load"},
                        "support_metrics": {"normalized_bracket_width": 0.1},
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
                        "promotion_state": {"promoted": True, "decision": "promoted_to_tranche_boundary"},
                        "replay_hash_provenance": {"threshold_replay_hash": "hash-weather"},
                        "support_metrics": {"normalized_bracket_width": 0.1},
                    }
                }
            },
        }
    )

    rho_global = payload["global_thresholds"]["rho_c"]
    finding_kinds = {finding["kind"] for finding in payload["consistency_findings"]}

    assert rho_global["promotion_state"]["promoted"] is False
    assert rho_global["status"] == ThresholdEvidenceStatus.BOUNDED_ESTIMATE.value
    assert "contradiction" in finding_kinds
    assert "instability" in finding_kinds


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
