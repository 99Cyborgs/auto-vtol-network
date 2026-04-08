from __future__ import annotations

import json
from pathlib import Path

import pytest

from avn.sweep import TrancheRunResult, analyze_only, execute_tranche_slice, main, run_tranche
from avn.sweep_analysis import (
    ArtifactPaths,
    CommsMetricsSnapshot,
    ContingencyMetricsSnapshot,
    ThroughputMetricsSnapshot,
    TrancheSliceResult,
    TrustMetricsSnapshot,
    analyze_phase_boundaries,
    write_aggregate_csv,
    write_cross_tranche_outputs,
    write_global_phase_map_json,
    write_phase_boundaries_json,
    write_transition_regions_json,
)
from avn.sweep_tranches import SeedPolicy, SweepAxis, TrancheDefinition, generate_tranche_slices, get_built_in_tranches


def _fixture_slice_result(
    tmp_path: Path,
    *,
    tranche_name: str,
    slice_id: str,
    mechanism: str,
    axis_value: float,
    exit_time: float,
    exit_cause: str,
) -> TrancheSliceResult:
    output_dir = tmp_path / slice_id
    output_dir.mkdir(parents=True, exist_ok=True)
    return TrancheSliceResult(
        slice_id=slice_id,
        tranche_name=tranche_name,
        seed=11,
        resolved_params={"modifiers.demand_multiplier": axis_value, "modifiers.node_service_multiplier": 1.0},
        first_dominant_failure_mechanism=mechanism,
        time_to_first_failure=exit_time,
        safe_region_exit_time=exit_time,
        safe_region_exit_cause=exit_cause,
        degraded_mode_dwell_time=15.0,
        trust_metrics_snapshot=TrustMetricsSnapshot(
            unsafe_admission_count=0,
            quarantine_count=0,
            revocation_count=0,
            trusted_active_fraction=1.0,
            operator_intervention_count=0,
            trust_induced_throughput_loss=0.0,
        ),
        comms_metrics_snapshot=CommsMetricsSnapshot(
            information_age_mean=2.0,
            information_age_max=4.0,
            stale_state_exposure_minutes=0.0,
            delayed_reroute_count=0,
            lost_link_fallback_activations=0,
            reservation_invalidations=0,
        ),
        contingency_metrics_snapshot=ContingencyMetricsSnapshot(
            reachable_landing_option_mean=2.0,
            no_admissible_landing_events=0,
            contingency_node_utilization=0.2,
            contingency_saturation_duration=0.0,
            reserve_margin_mean=14.0,
            reserve_margin_min=10.0,
        ),
        throughput_metrics_snapshot=ThroughputMetricsSnapshot(
            completed_vehicles=10,
            incomplete_vehicles=1,
            avg_queue_length=0.5,
            peak_avg_queue_length=1.0,
            peak_corridor_load_ratio=0.9,
            peak_node_utilization_ratio=0.8,
            peak_queue_ratio=0.4,
            mean_corridor_speed=105.0,
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


def test_tranche_registry_contains_expected_names() -> None:
    registry = get_built_in_tranches()
    assert set(registry) == {"load", "comms", "trust", "contingency", "weather", "coupled"}


def test_slice_generation_count_and_stability() -> None:
    tranche = TrancheDefinition(
        tranche_name="test",
        description="Synthetic tranche for deterministic slice generation coverage.",
        base_config_path=Path("configs/nominal.toml"),
        fixed_params={"modifiers.weather_multiplier": 1.0},
        sweep_axes=(SweepAxis("alpha", (1, 2, 3)),),
        expected_metrics=("first_dominant_failure_mechanism",),
        expected_failure_modes=("coupled_failure_indeterminate",),
        seed_policy=SeedPolicy(base_seed=101),
        dominant_axis="alpha",
    )

    slices_a = generate_tranche_slices(tranche)
    slices_b = generate_tranche_slices(tranche)

    assert len(slices_a) == 3
    assert [item.slice_id for item in slices_a] == [item.slice_id for item in slices_b]
    assert [item.seed for item in slices_a] == [item.seed for item in slices_b]
    assert slices_a[0].resolved_params["alpha"] == 1


def test_same_slice_executes_deterministically(tmp_path: Path) -> None:
    tranche = get_built_in_tranches()["load"]
    slice_definition = generate_tranche_slices(tranche, max_slices=3)[0]

    result_a = execute_tranche_slice(tranche, slice_definition, output_dir=tmp_path / "run_a")
    result_b = execute_tranche_slice(tranche, slice_definition, output_dir=tmp_path / "run_b")

    assert result_a.seed == result_b.seed
    assert result_a.first_dominant_failure_mechanism == result_b.first_dominant_failure_mechanism
    assert result_a.time_to_first_failure == result_b.time_to_first_failure
    assert result_a.safe_region_exit_cause == result_b.safe_region_exit_cause
    assert result_a.trust_metrics_snapshot.to_dict() == result_b.trust_metrics_snapshot.to_dict()
    assert result_a.comms_metrics_snapshot.to_dict() == result_b.comms_metrics_snapshot.to_dict()
    assert result_a.contingency_metrics_snapshot.to_dict() == result_b.contingency_metrics_snapshot.to_dict()
    assert result_a.throughput_metrics_snapshot.to_dict() == result_b.throughput_metrics_snapshot.to_dict()
    assert result_a.artifact_paths.run_summary_path.exists()
    assert result_b.artifact_paths.run_summary_path.exists()


def test_aggregation_and_boundary_analysis_from_fixtures(tmp_path: Path) -> None:
    load_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_a",
            mechanism="corridor_capacity_exceeded",
            axis_value=1.0,
            exit_time=80.0,
            exit_cause="corridor_load_ratio",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_b",
            mechanism="corridor_capacity_exceeded",
            axis_value=1.4,
            exit_time=70.0,
            exit_cause="corridor_load_ratio",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_c",
            mechanism="node_service_collapse",
            axis_value=1.8,
            exit_time=55.0,
            exit_cause="queue_ratio",
        ),
    ]

    aggregate_csv_path = write_aggregate_csv(tmp_path / "load_analysis", "load", load_results)
    phase_payload = analyze_phase_boundaries("load", load_results)
    phase_json_path = write_phase_boundaries_json(tmp_path / "load_analysis", "load", load_results)
    transition_regions_path = write_transition_regions_json(tmp_path / "load_analysis", "load", load_results)

    comms_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="comms",
            slice_id="comms_a",
            mechanism="stale_information_instability",
            axis_value=1.0,
            exit_time=60.0,
            exit_cause="stale_state_exposure",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="comms",
            slice_id="comms_b",
            mechanism="stale_information_instability",
            axis_value=1.8,
            exit_time=50.0,
            exit_cause="stale_state_exposure",
        ),
    ]
    matrix_path, comparison_path, summary_path = write_cross_tranche_outputs(
        tmp_path / "global_analysis",
        {"load": load_results, "comms": comms_results},
    )

    assert aggregate_csv_path.exists()
    assert phase_json_path.exists()
    assert transition_regions_path.exists()
    assert phase_payload["failure_mechanism_counts"]["CORRIDOR_CONGESTION"] == 2
    assert phase_payload["failure_mechanism_counts"]["NODE_SATURATION"] == 1
    assert any(item["axis"] == "modifiers.demand_multiplier" for item in phase_payload["parameter_sensitivity"])
    assert any(
        item["from_mechanism"] == "CORRIDOR_CONGESTION"
        and item["to_mechanism"] == "NODE_SATURATION"
        for item in phase_payload["dominant_failure_switches"]
    )
    assert matrix_path.exists()
    assert comparison_path.exists()
    assert summary_path.exists()

    phase_json_payload = json.loads(phase_json_path.read_text(encoding="utf-8"))
    transition_regions_payload = json.loads(transition_regions_path.read_text(encoding="utf-8"))
    comparison_payload = json.loads(comparison_path.read_text(encoding="utf-8"))

    assert phase_json_payload["artifact_type"] == "phase_boundaries"
    assert phase_json_payload["analysis_contract_version"] == 2
    assert phase_json_payload["scope"] == "local_tranche"
    assert transition_regions_payload["artifact_type"] == "transition_regions"
    assert transition_regions_payload["analysis_contract_version"] == 2
    assert transition_regions_payload["scope"] == "local_tranche"
    assert comparison_payload["artifact_type"] == "tranche_comparison"
    assert comparison_payload["analysis_contract_version"] == 2
    assert comparison_payload["scope"] == "cross_tranche"
    assert phase_json_payload["summary"]["slice_count"] == 3
    assert phase_json_payload["summary"]["dominant_mechanism"] == "CORRIDOR_CONGESTION"
    assert phase_json_payload["summary"]["switch_count"] == len(phase_json_payload["dominant_failure_switches"])
    assert phase_json_payload["summary"]["governed_transition_boundary_count"] == len(
        phase_json_payload["governed_transition_boundaries"]
    )
    assert transition_regions_payload["summary"]["region_count"] == transition_regions_payload["region_count"]
    assert transition_regions_payload["summary"]["axes"] == ["modifiers.demand_multiplier"]
    assert transition_regions_payload["summary"]["threshold_estimate_count"] == len(
        [
            region
            for region in transition_regions_payload["regions"]
            if "estimated_threshold" in region
        ]
    )
    assert comparison_payload["summary"]["tranche_count"] == 2
    assert comparison_payload["summary"]["total_slice_count"] == 5
    assert comparison_payload["summary"]["fastest_tranche"]["tranche_name"] == "comms"
    assert comparison_payload["summary"]["per_tranche_dominant_mechanisms"] == {
        "comms": "COMMS_FAILURE",
        "load": "CORRIDOR_CONGESTION",
    }


def test_cross_tranche_summary_quantifies_coupled_ordering_shifts(tmp_path: Path) -> None:
    load_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_shift_a",
            mechanism="corridor_capacity_exceeded",
            axis_value=1.0,
            exit_time=80.0,
            exit_cause="corridor_load_ratio",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_shift_b",
            mechanism="corridor_capacity_exceeded",
            axis_value=1.2,
            exit_time=74.0,
            exit_cause="corridor_load_ratio",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_shift_c",
            mechanism="node_service_collapse",
            axis_value=1.5,
            exit_time=62.0,
            exit_cause="queue_ratio",
        ),
    ]
    comms_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="comms",
            slice_id="comms_shift_a",
            mechanism="stale_information_instability",
            axis_value=0.9,
            exit_time=64.0,
            exit_cause="stale_state_exposure",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="comms",
            slice_id="comms_shift_b",
            mechanism="stale_information_instability",
            axis_value=1.1,
            exit_time=58.0,
            exit_cause="stale_state_exposure",
        ),
    ]
    trust_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="trust",
            slice_id="trust_shift_a",
            mechanism="trust_breakdown",
            axis_value=0.8,
            exit_time=76.0,
            exit_cause="unsafe_admission_count",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="trust",
            slice_id="trust_shift_b",
            mechanism="trust_breakdown",
            axis_value=1.0,
            exit_time=71.0,
            exit_cause="unsafe_admission_count",
        ),
    ]
    coupled_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="coupled",
            slice_id="coupled_shift_a",
            mechanism="stale_information_instability",
            axis_value=1.0,
            exit_time=57.0,
            exit_cause="stale_state_exposure",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="coupled",
            slice_id="coupled_shift_b",
            mechanism="stale_information_instability",
            axis_value=1.2,
            exit_time=53.0,
            exit_cause="stale_state_exposure",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="coupled",
            slice_id="coupled_shift_c",
            mechanism="corridor_capacity_exceeded",
            axis_value=1.4,
            exit_time=49.0,
            exit_cause="corridor_load_ratio",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="coupled",
            slice_id="coupled_shift_d",
            mechanism="coupled_failure_indeterminate",
            axis_value=1.6,
            exit_time=45.0,
            exit_cause="reroute_delay",
        ),
    ]

    _, _, summary_path = write_cross_tranche_outputs(
        tmp_path / "global_analysis_shifted",
        {
            "load": load_results,
            "comms": comms_results,
            "trust": trust_results,
            "coupled": coupled_results,
        },
    )

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    mixed = summary_payload["mixed_stress_summary"]
    shift_records = {
        entry["mechanism"]: entry
        for entry in mixed["mechanism_shift_table"]
    }

    assert summary_payload["artifact_type"] == "cross_tranche_summary"
    assert summary_payload["analysis_contract_version"] == 2
    assert summary_payload["scope"] == "cross_tranche"
    assert mixed["coupled_primary_mechanism"] == "COMMS_FAILURE"
    assert mixed["isolated_primary_mechanism_counts"] == {
        "COMMS_FAILURE": 1,
        "CORRIDOR_CONGESTION": 1,
        "TRUST_FAILURE": 1,
    }
    assert mixed["isolated_mean_ordering"][:3] == [
        "COMMS_FAILURE",
        "TRUST_FAILURE",
        "CORRIDOR_CONGESTION",
    ]
    assert mixed["ordering_shift_detected"] is True
    assert mixed["ordering_shift_magnitude"] == 1
    assert mixed["emergent_mechanisms"] == ["REROUTE_CASCADE"]
    assert mixed["suppressed_mechanisms"] == ["NODE_SATURATION", "TRUST_FAILURE"]
    assert mixed["isolated_mean_mechanism_proportions"]["TRUST_FAILURE"] == pytest.approx(1.0 / 3.0)
    assert shift_records["CORRIDOR_CONGESTION"]["coupled_rank"] == 2
    assert shift_records["CORRIDOR_CONGESTION"]["isolated_mean_rank"] == 3
    assert shift_records["CORRIDOR_CONGESTION"]["rank_shift"] == 1
    assert shift_records["REROUTE_CASCADE"]["isolated_mean_rank"] is None
    assert shift_records["REROUTE_CASCADE"]["share_delta"] == pytest.approx(0.25)
    assert summary_payload["summary"] == {
        "fastest_mechanism_count": 5,
        "fastest_tranche_counts": {
            "coupled": 3,
            "load": 1,
            "trust": 1,
        },
        "fastest_tranche_ordering": ["coupled", "load", "trust"],
        "mechanisms_with_fastest_tranche": {
            "coupled": ["COMMS_FAILURE", "CORRIDOR_CONGESTION", "REROUTE_CASCADE"],
            "load": ["NODE_SATURATION"],
            "trust": ["TRUST_FAILURE"],
        },
        "coupled_primary_mechanism": "COMMS_FAILURE",
        "ordering_shift_detected": True,
        "ordering_shift_magnitude": 1,
        "emergent_mechanism_count": 1,
        "emergent_mechanisms": ["REROUTE_CASCADE"],
        "suppressed_mechanism_count": 2,
        "suppressed_mechanisms": ["NODE_SATURATION", "TRUST_FAILURE"],
    }


def test_global_phase_map_includes_aggregate_summary(tmp_path: Path) -> None:
    load_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_global_a",
            mechanism="corridor_capacity_exceeded",
            axis_value=1.0,
            exit_time=80.0,
            exit_cause="corridor_load_ratio",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="load",
            slice_id="load_global_b",
            mechanism="node_service_collapse",
            axis_value=1.6,
            exit_time=60.0,
            exit_cause="queue_ratio",
        ),
    ]
    comms_results = [
        _fixture_slice_result(
            tmp_path,
            tranche_name="comms",
            slice_id="comms_global_a",
            mechanism="stale_information_instability",
            axis_value=0.9,
            exit_time=58.0,
            exit_cause="stale_state_exposure",
        ),
        _fixture_slice_result(
            tmp_path,
            tranche_name="comms",
            slice_id="comms_global_b",
            mechanism="stale_information_instability",
            axis_value=1.1,
            exit_time=55.0,
            exit_cause="stale_state_exposure",
        ),
    ]

    global_phase_map_path = write_global_phase_map_json(
        tmp_path / "global_phase_map",
        {
            "load": load_results,
            "comms": comms_results,
        },
    )

    payload = json.loads(global_phase_map_path.read_text(encoding="utf-8"))
    summary = payload["summary"]

    assert payload["artifact_type"] == "global_phase_map"
    assert payload["analysis_contract_version"] == 2
    assert payload["scope"] == "cross_tranche"
    assert summary["tranche_count"] == 2
    assert summary["total_point_count"] == 4
    assert summary["axis_count"] == 1
    assert summary["axes"] == ["modifiers.demand_multiplier"]
    assert summary["tranche_point_counts"] == {
        "comms": 2,
        "load": 2,
    }
    assert summary["dominant_mechanism_counts"] == {
        "COMMS_FAILURE": 1,
        "CORRIDOR_CONGESTION": 1,
    }
    assert summary["dominant_mechanism_ordering"] == [
        "COMMS_FAILURE",
        "CORRIDOR_CONGESTION",
    ]
    assert summary["per_tranche_dominant_mechanisms"] == {
        "comms": "COMMS_FAILURE",
        "load": "CORRIDOR_CONGESTION",
    }
    assert payload["tranches"]["load"]["summary"]["point_count"] == 2
    assert payload["tranches"]["comms"]["summary"]["dominant_mechanism"] == "COMMS_FAILURE"


def test_run_tranche_and_analyze_only_round_trip(tmp_path: Path) -> None:
    result = run_tranche("load", output_root=tmp_path, max_slices=3)
    analysis = analyze_only(result.output_dir)

    assert result.aggregate_csv_path.exists()
    assert result.slice_results_json_path.exists()
    assert result.phase_boundaries_json_path.exists()
    assert result.phase_map_json_path is not None and result.phase_map_json_path.exists()
    assert result.transition_regions_json_path is not None and result.transition_regions_json_path.exists()
    assert result.threshold_estimates_json_path is not None and result.threshold_estimates_json_path.exists()
    assert result.threshold_ledger_json_path is not None and result.threshold_ledger_json_path.exists()
    assert result.promotion_decisions_json_path is not None and result.promotion_decisions_json_path.exists()
    assert result.admissibility_overlay_json_path is not None and result.admissibility_overlay_json_path.exists()
    assert result.convergence_report_json_path is not None and result.convergence_report_json_path.exists()
    assert len(result.slice_results) == 3
    assert analysis.aggregate_csv_path == result.aggregate_csv_path
    assert analysis.phase_boundaries_json_path == result.phase_boundaries_json_path
    assert analysis.phase_map_json_path == result.phase_map_json_path
    assert analysis.transition_regions_json_path == result.transition_regions_json_path
    assert analysis.threshold_estimates_json_path == result.threshold_estimates_json_path
    assert analysis.threshold_ledger_json_path == result.threshold_ledger_json_path
    assert analysis.promotion_decisions_json_path == result.promotion_decisions_json_path
    assert analysis.admissibility_overlay_json_path == result.admissibility_overlay_json_path
    assert analysis.convergence_report_json_path == result.convergence_report_json_path


def test_cli_list_tranches(capsys) -> None:
    exit_code = main(["--list-tranches"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "load: 5 slices" in captured.out
    assert "coupled: 4 slices" in captured.out


def test_cli_static_invocation_remains_additive(monkeypatch, tmp_path: Path, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_run_tranche(tranche_name: str, **kwargs) -> TrancheRunResult:
        captured["tranche_name"] = tranche_name
        captured.update(kwargs)
        output_dir = tmp_path / "cli_static"
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
            threshold_ledger_json_path=output_dir / "threshold_ledger.json",
            promotion_decisions_json_path=output_dir / "promotion_decisions.json",
            admissibility_overlay_json_path=output_dir / "admissibility_overlay.json",
            convergence_report_json_path=output_dir / "convergence_report.json",
        )

    monkeypatch.setattr("avn.sweep.run_tranche", fake_run_tranche)

    exit_code = main(["--tranche", "load"])
    captured_output = capsys.readouterr().out

    assert exit_code == 0
    assert captured["tranche_name"] == "load"
    assert captured["adaptive"] is False
    assert "Threshold ledger JSON:" in captured_output
    assert "Promotion decisions JSON:" in captured_output
    assert "Admissibility overlay JSON:" in captured_output
