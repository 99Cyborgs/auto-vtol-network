from __future__ import annotations

from pathlib import Path

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
    write_phase_boundaries_json,
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
