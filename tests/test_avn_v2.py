from __future__ import annotations

import json
from pathlib import Path

import pytest

from avn_v2.__main__ import main as avn_v2_main
from avn_v2.config import load_external_source_manifest, load_scenario_config
from avn_v2.engine import run_scenario
from avn_v2.validation import BundleValidationError
from skills.auto_vtol_network.harness import load_v2_request, run_skill_pack


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_SCENARIO_PATH = REPO_ROOT / "configs" / "v2" / "nominal.toml"
BASE_EXPERIMENT_PATH = REPO_ROOT / "configs" / "v2" / "dispatch_conflict_experiment.toml"
BASE_BUNDLE_PATH = REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "bundle.json"


def _temp_scenario(tmp_path: Path) -> Path:
    payload = BASE_SCENARIO_PATH.read_text(encoding="utf-8")
    replacements = {
        "../../data/v2/bundles/reference_baseline/nodes.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "nodes.csv").resolve()).replace("\\", "/"),
        "../../data/v2/bundles/reference_baseline/corridors.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "corridors.csv").resolve()).replace("\\", "/"),
        "../../data/v2/bundles/reference_baseline/vehicles.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "vehicles.csv").resolve()).replace("\\", "/"),
        "../../data/v2/bundles/reference_baseline/demand_requests.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "demand_requests.csv").resolve()).replace("\\", "/"),
        "../../data/v2/bundles/reference_baseline/disruptions.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "disruptions.csv").resolve()).replace("\\", "/"),
        "../../data/v2/bundles/reference_baseline/bundle.json": str((tmp_path / "bundle.json").resolve()).replace("\\", "/"),
        "../../outputs/v2": str((tmp_path / "outputs").resolve()).replace("\\", "/"),
    }
    for old, new in replacements.items():
        payload = payload.replace(old, new)
    path = tmp_path / "nominal.toml"
    path.write_text(payload, encoding="utf-8")
    bundle_payload = BASE_BUNDLE_PATH.read_text(encoding="utf-8")
    bundle_replacements = {
        "../../../configs/v2/nominal.toml": str(path.resolve()).replace("\\", "/"),
        "reference_metrics.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "reference_metrics.csv").resolve()).replace("\\", "/"),
        "event_expectations.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "event_expectations.csv").resolve()).replace("\\", "/"),
        "series_targets.csv": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "series_targets.csv").resolve()).replace("\\", "/"),
    }
    for old, new in bundle_replacements.items():
        bundle_payload = bundle_payload.replace(old, new)
    (tmp_path / "bundle.json").write_text(bundle_payload, encoding="utf-8")
    return path


def _temp_experiment(tmp_path: Path, scenario_path: Path) -> Path:
    payload = BASE_EXPERIMENT_PATH.read_text(encoding="utf-8")
    payload = payload.replace("nominal.toml", scenario_path.name)
    payload = payload.replace("../../data/v2/bundles/reference_baseline/bundle.json", str((tmp_path / "bundle.json").resolve()).replace("\\", "/"))
    payload = payload.replace("../../outputs/v2/experiments", str((tmp_path / "experiments").resolve()).replace("\\", "/"))
    path = tmp_path / "experiment.toml"
    path.write_text(payload, encoding="utf-8")
    return path


def _write_experiment_manifest(
    tmp_path: Path,
    scenario_path: Path,
    *,
    bundle_path: Path,
    governance_policy: str = "",
) -> Path:
    payload = f"""experiment_name = "policy_experiment"
base_scenario = "{scenario_path.resolve().as_posix()}"
output_root = "{(tmp_path / 'experiments').resolve().as_posix()}"
adaptive_refinement = true
calibration_bundle = "{bundle_path.resolve().as_posix()}"
calibration_gate = "required"
use_calibrated_parameters = true
promoted_metrics = ["completed_requests", "avg_delay_minutes", "diversion_count"]

[[axes]]
name = "dispatch_policy.max_wait_minutes"
values = [12, 18]

[[axes]]
name = "reservation_policy.reservation_horizon_hops"
values = [1, 2]
"""
    if governance_policy:
        payload = payload + "\n" + governance_policy.strip() + "\n"
    path = tmp_path / "policy_experiment.toml"
    path.write_text(payload, encoding="utf-8")
    return path


def _write_custom_scenario(
    tmp_path: Path,
    *,
    nodes_csv: str,
    corridors_csv: str,
    vehicles_csv: str,
    demand_csv: str,
    disruptions_csv: str,
    enable_calibration: bool = False,
) -> Path:
    (tmp_path / "nodes.csv").write_text(nodes_csv, encoding="utf-8")
    (tmp_path / "corridors.csv").write_text(corridors_csv, encoding="utf-8")
    (tmp_path / "vehicles.csv").write_text(vehicles_csv, encoding="utf-8")
    (tmp_path / "demand.csv").write_text(demand_csv, encoding="utf-8")
    (tmp_path / "disruptions.csv").write_text(disruptions_csv, encoding="utf-8")
    scenario_path = tmp_path / "scenario.toml"
    scenario_path.write_text(
        f"""scenario_name = "custom_v2"
description = "custom v2 scenario"
duration_minutes = 40
time_step_minutes = 1

[network]
nodes = "{(tmp_path / 'nodes.csv').resolve().as_posix()}"
corridors = "{(tmp_path / 'corridors.csv').resolve().as_posix()}"

[fleet]
vehicles = "{(tmp_path / 'vehicles.csv').resolve().as_posix()}"

[demand]
requests = "{(tmp_path / 'demand.csv').resolve().as_posix()}"

[dispatch_policy]
max_wait_minutes = 12
max_reroutes = 2
degraded_dispatch_enabled = false
operator_delay_minutes = 1

[reservation_policy]
lookahead_minutes = 15
reservation_horizon_hops = 2
conflict_tolerance = 0

[contingency_policy]
min_energy_reserve = 12.0
reroute_buffer_minutes = 2
diversion_limit = 1

[disruptions]
events = "{(tmp_path / 'disruptions.csv').resolve().as_posix()}"

[calibration]
enabled = {str(enable_calibration).lower()}

[outputs]
root = "{(tmp_path / 'outputs').resolve().as_posix()}"
artifact_prefix = "avn_v2"
""",
        encoding="utf-8",
    )
    return scenario_path


def _write_external_source_manifest(
    tmp_path: Path,
    scenario_path: Path,
    *,
    source_type: str = "csv_directory",
    invalid_scope: bool = False,
    missing_metric_mapping: bool = False,
) -> Path:
    raw_root = tmp_path / "external_raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    (raw_root / "metrics.csv").write_text(
        """name,value,tol,unit,weight,group
completed_requests,3,2,count,1.2,metric
avg_delay_minutes,180,180,seconds,1.0,metric
diversion_count,1,1,count,0.9,metric
peak_queue_length,2,2,count,0.7,metric
""",
        encoding="utf-8",
    )
    (raw_root / "events.csv").write_text(
        """expectation,event,count,count_tol,first_seen,timing_tol,weight,group
exp-request-dispatched,dispatch,4,1,0,10,1.0,event
exp-request-completed,complete,3,1,10,20,1.0,event
exp-request-diverted,divert,1,1,0,40,0.7,event
""",
        encoding="utf-8",
    )
    series_scope_row = "mystery" if invalid_scope else "hub"
    (raw_root / "series.csv").write_text(
        f"""target,scope_name,entity,metric,value,tol,weight,minute_point,minute_from,minute_to,aggregation,group
series-network-pending-window,system,all,pending_requests,2,2,0.6,,4,6,max,series
series-node-queue-hub-a,{series_scope_row},HUB_A,queue_length,2,2,0.6,5,,,point,series
series-corridor-reservations,route,B_C,reservation_count,1,1,0.5,,8,10,max,series
""",
        encoding="utf-8",
    )
    metrics_fields = {
        "metric_key": "name",
        "reference_value": "value",
        "unit": "unit",
        "weight": "weight",
        "objective_group": "group",
    }
    if not missing_metric_mapping:
        metrics_fields["tolerance"] = "tol"
    manifest_path = tmp_path / "external_source_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "source_id": "vendor-baseline",
                "source_type": source_type,
                "version": "2026-04-09",
                "scenario": str(scenario_path.resolve()).replace("\\", "/"),
                "ingestion_mode": "copy",
                "bundle_family": "baseline",
                "output_root": str((tmp_path / "external").resolve()).replace("\\", "/"),
                "raw_inputs": {
                    "metrics": {"path": str((raw_root / "metrics.csv").resolve()).replace("\\", "/"), "format": "csv"},
                    "event_expectations": {"path": str((raw_root / "events.csv").resolve()).replace("\\", "/"), "format": "csv"},
                    "series_targets": {"path": str((raw_root / "series.csv").resolve()).replace("\\", "/"), "format": "csv"},
                },
                "field_mapping": {
                    "metrics": {
                        "fields": metrics_fields,
                        "optional_fields": [],
                        "defaults": {},
                    },
                    "event_expectations": {
                        "fields": {
                            "expectation_id": "expectation",
                            "event_type": "event",
                            "expected_count": "count",
                            "count_tolerance": "count_tol",
                            "first_minute": "first_seen",
                            "timing_tolerance": "timing_tol",
                            "weight": "weight",
                            "objective_group": "group",
                        }
                    },
                    "series_targets": {
                        "fields": {
                            "target_id": "target",
                            "scope": "scope_name",
                            "entity_id": "entity",
                            "metric_key": "metric",
                            "reference_value": "value",
                            "tolerance": "tol",
                            "weight": "weight",
                            "minute": "minute_point",
                            "minute_start": "minute_from",
                            "minute_end": "minute_to",
                            "aggregation": "aggregation",
                            "objective_group": "group",
                        }
                    },
                },
                "normalization": {
                    "metric_unit_mappings": {
                        "seconds": {"target_unit": "minutes", "multiplier": 0.016666667}
                    },
                    "event_type_mapping": {
                        "dispatch": "request_dispatched",
                        "complete": "request_completed",
                        "divert": "request_diverted",
                    },
                    "scope_mapping": {
                        "system": "network",
                        "hub": "node",
                        "route": "corridor",
                    },
                    "time_multiplier": 1.0,
                    "time_rounding": "nearest",
                    "missing_value_policy": "reject",
                },
                "fit_space_overrides": {
                    "parameters": [
                        {
                            "name": "operator_delay_minutes",
                            "scenario_key": "dispatch_policy.operator_delay_minutes",
                            "values": [1, 2, 3],
                            "description": "Dispatch operator delay for ingestion-backed calibration.",
                        }
                    ]
                },
                "coverage_requirements": {
                    "min_metric_targets": 4,
                    "min_event_expectations": 2,
                    "min_series_targets": 3,
                    "required_scopes": ["network", "node", "corridor"],
                },
                "confidence_policy": {
                    "high_confidence_score": 0.85,
                    "medium_confidence_score": 0.6,
                    "max_sensitivity_delta": 0.3,
                    "max_failed_objectives": 0,
                    "min_coverage_completeness": 1.0,
                },
                "quality_checks": {
                    "require_reference_sources": True,
                    "require_bundle_hashes": True,
                    "require_monotonic_windows": True,
                    "require_scope_coverage": True,
                },
                "provenance_defaults": {
                    "maintainer": "repo",
                    "curation_note": "External ingestion fixture for v2 bundle generation.",
                    "generated_at": "2026-04-09T00:00:00Z",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifest_path


def test_v2_run_emits_contract_artifacts(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    result = run_scenario(load_scenario_config(scenario_path))

    assert result.run_summary_path.exists()
    assert result.threshold_ledger_path.exists()
    assert result.hazard_ledger_path.exists()
    assert result.promotion_decisions_path.exists()
    assert result.report_bundle_path.exists()

    summary = json.loads(result.run_summary_path.read_text(encoding="utf-8"))
    assert summary["artifact_type"] == "run_summary"
    assert summary["contract_version"] == 4
    assert summary["scenario_name"] == "nominal_v2"
    assert summary["completed_requests"] >= 1
    backtest_trace = json.loads(result.backtest_trace_path.read_text(encoding="utf-8"))
    assert backtest_trace["artifact_type"] == "backtest_trace"
    assert backtest_trace["contract_version"] == 4


def test_v2_cli_run_report_and_calibrate(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    bundle_target = tmp_path / "bundle.json"

    assert avn_v2_main(["run", str(scenario_path)]) == 0
    run_dirs = list((tmp_path / "outputs").glob("avn_v2_nominal_v2_*"))
    assert run_dirs
    run_dir = run_dirs[0]

    assert avn_v2_main(["report", str(run_dir)]) == 0
    assert (run_dir / "report_view.v2.json").exists()

    assert avn_v2_main(["calibrate", str(bundle_target)]) == 0
    assert (bundle_target.parent / f"{bundle_target.stem}.calibration_report.v2.json").exists()
    assert (bundle_target.parent / f"{bundle_target.stem}.bundle_validation.v2.json").exists()


def test_v2_external_source_manifest_loads(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    manifest_path = _write_external_source_manifest(tmp_path, scenario_path)
    manifest = load_external_source_manifest(manifest_path)

    assert manifest.source_id == "vendor-baseline"
    assert manifest.source_type == "csv_directory"
    assert manifest.output_root == (tmp_path / "external").resolve()
    assert set(manifest.raw_inputs) == {"metrics", "event_expectations", "series_targets"}


def test_v2_ingest_generates_bundle_and_supports_calibration(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    manifest_path = _write_external_source_manifest(tmp_path, scenario_path)

    assert avn_v2_main(["ingest", str(manifest_path)]) == 0
    report_path = tmp_path / "external" / "bundles" / "baseline-vendor-baseline-2026-04-09" / "ingestion_report.v2.json"
    bundle_path = report_path.parent / "bundle.json"
    reference_metrics_path = report_path.parent / "reference_metrics.csv"

    assert report_path.exists()
    assert bundle_path.exists()
    assert reference_metrics_path.exists()

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["artifact_type"] == "ingestion_report"
    assert report["usable_bundle"] is True
    assert report["bundle_validation_status"] == "passed"
    metrics_rows = reference_metrics_path.read_text(encoding="utf-8")
    assert "avg_delay_minutes,3.00000006,3.00000006,minutes,1.0,metric" in metrics_rows

    assert avn_v2_main(["calibrate", str(bundle_path)]) == 0
    assert (bundle_path.parent / "bundle.calibration_report.v2.json").exists()
    assert (bundle_path.parent / "bundle.bundle_validation.v2.json").exists()


def test_v2_experiment_runs_with_adaptive_slice(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    experiment_path = _temp_experiment(tmp_path, scenario_path)

    assert avn_v2_main(["experiment", str(experiment_path)]) == 0
    summary_files = list((tmp_path / "experiments").rglob("experiment_summary.v2.json"))
    assert summary_files
    payload = json.loads(summary_files[0].read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "experiment_summary"
    assert payload["contract_version"] == 4
    assert len(payload["slices"]) >= 4
    assert any(item.get("adaptive") for item in payload["slices"])
    assert payload["calibration_gate"] == "required"
    assert "fit_quality_summary" in payload
    assert "calibration_confidence" in payload
    assert "evidence_weak_regions" in payload
    assert "policy_result" in payload
    assert "fatal_blockers" in payload


def test_v2_experiment_can_use_ingested_bundle_source_alias(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    manifest_path = _write_external_source_manifest(tmp_path, scenario_path)
    assert avn_v2_main(["ingest", str(manifest_path)]) == 0

    experiment_path = tmp_path / "ingested_experiment.toml"
    experiment_path.write_text(
        f"""experiment_name = "ingested_bundle_experiment"
base_scenario = "{scenario_path.resolve().as_posix()}"
output_root = "{(tmp_path / 'ingested_experiments').resolve().as_posix()}"
adaptive_refinement = true
ingested_bundle_source = "{manifest_path.resolve().as_posix()}"
calibration_gate = "required"
use_calibrated_parameters = true
promoted_metrics = ["completed_requests", "avg_delay_minutes"]

[[axes]]
name = "dispatch_policy.max_wait_minutes"
values = [12, 18]
""",
        encoding="utf-8",
    )

    assert avn_v2_main(["experiment", str(experiment_path)]) == 0
    payload = json.loads(next((tmp_path / "ingested_experiments").rglob("experiment_summary.v2.json")).read_text(encoding="utf-8"))
    assert payload["calibration_bundle_id"] == "baseline-vendor-baseline-2026-04-09"
    assert Path(payload["calibration_bundle_path"]).name == "bundle.json"


def test_skill_pack_can_ingest_v2_bundle(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    result = run_scenario(load_scenario_config(scenario_path))

    request = load_v2_request(result.output_dir)
    receipt = run_skill_pack(request, output_dir=tmp_path / "skill_pack")

    assert (tmp_path / "skill_pack" / "run_receipt.json").exists()
    assert receipt.status == "completed"
    thresholds = json.loads((tmp_path / "skill_pack" / "threshold_ledger.json").read_text(encoding="utf-8"))
    assert thresholds["type"] == "threshold_ledger"


def test_v2_calibration_report_uses_fit_and_score_schema(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    bundle_target = tmp_path / "bundle.json"
    assert avn_v2_main(["calibrate", str(bundle_target)]) == 0
    report = json.loads((bundle_target.parent / f"{bundle_target.stem}.calibration_report.v2.json").read_text(encoding="utf-8"))
    validation = json.loads((bundle_target.parent / f"{bundle_target.stem}.bundle_validation.v2.json").read_text(encoding="utf-8"))

    assert report["artifact_type"] == "calibration_report"
    assert report["contract_version"] == 4
    assert "selected_parameters" in report
    assert "fit_quality_summary" in report
    assert "top_candidates" in report
    assert report["candidate_count"] >= 1
    assert report["search_strategy"] == "deterministic_coordinate_plus_pairwise_refinement"
    assert "search_passes" in report
    assert "improvement_history" in report
    assert "evidence_coverage_summary" in report
    assert "confidence_tier" in report
    assert "confidence_score" in report
    assert "confidence_components" in report
    assert "bundle_validation_id" in report
    assert validation["artifact_type"] == "bundle_validation"
    assert validation["status"] == "passed"


def test_v2_repositions_nearest_idle_vehicle(tmp_path: Path) -> None:
    scenario_path = _write_custom_scenario(
        tmp_path,
        nodes_csv="""node_id,node_type,turnaround_minutes,service_rate_per_hour,queue_capacity,contingency_slots,trust_state
HUB_A,hub,6,60,4,1,trusted
HUB_B,hub,6,60,4,1,trusted
PAD_C,contingency_pad,8,40,2,1,trusted
""",
        corridors_csv="""corridor_id,origin,destination,length_km,travel_minutes,capacity_per_hour,energy_cost,reservation_window_minutes
A_B,HUB_A,HUB_B,18,8,8,8,15
B_C,HUB_B,PAD_C,12,6,8,6,15
A_C,HUB_A,PAD_C,20,10,6,10,15
""",
        vehicles_csv="""vehicle_id,home_node,vehicle_class,energy_capacity,reserve_energy,trust_state,operator_required
V_A1,HUB_A,standard,100,12,trusted,false
""",
        demand_csv="""request_id,release_minute,origin,destination,priority,required_vehicle_class,max_delay_minutes
REQ_100,0,HUB_B,PAD_C,cargo,standard,12
""",
        disruptions_csv="event_id,start_minute,end_minute,target_type,target_id,effect_type,value,note\n",
    )
    result = run_scenario(load_scenario_config(scenario_path))
    summary = json.loads(result.run_summary_path.read_text(encoding="utf-8"))

    assert summary["reposition_count"] >= 1
    assert summary["completed_requests"] == 1


def test_v2_queue_overflow_creates_distinct_failure_chain(tmp_path: Path) -> None:
    scenario_path = _write_custom_scenario(
        tmp_path,
        nodes_csv="""node_id,node_type,turnaround_minutes,service_rate_per_hour,queue_capacity,contingency_slots,trust_state
HUB_A,hub,8,30,1,1,trusted
PAD_C,contingency_pad,8,40,2,1,trusted
""",
        corridors_csv="""corridor_id,origin,destination,length_km,travel_minutes,capacity_per_hour,energy_cost,reservation_window_minutes
A_C,HUB_A,PAD_C,20,10,4,10,15
""",
        vehicles_csv="""vehicle_id,home_node,vehicle_class,energy_capacity,reserve_energy,trust_state,operator_required
V_A1,HUB_A,standard,100,12,trusted,false
""",
        demand_csv="""request_id,release_minute,origin,destination,priority,required_vehicle_class,max_delay_minutes
REQ_200,0,HUB_A,PAD_C,cargo,standard,12
REQ_201,0,HUB_A,PAD_C,routine,standard,12
REQ_202,0,HUB_A,PAD_C,routine,standard,12
""",
        disruptions_csv="event_id,start_minute,end_minute,target_type,target_id,effect_type,value,note\n",
    )
    result = run_scenario(load_scenario_config(scenario_path))
    summary = json.loads(result.run_summary_path.read_text(encoding="utf-8"))
    threshold_ledger = json.loads(result.threshold_ledger_path.read_text(encoding="utf-8"))

    assert summary["queue_overflow_count"] >= 1
    assert summary["dominant_failure_chain"] == "dispatch_queue_collapse"
    assert any(item["id"] == "queue.pressure" for item in threshold_ledger["thresholds"])


def test_v2_sparse_bundle_reports_evidence_insufficient(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    sparse_bundle = tmp_path / "sparse_bundle.json"
    sparse_metrics = tmp_path / "sparse_metrics.csv"
    sparse_metrics.write_text(
        "metric_key,reference_value,tolerance,unit,weight,objective_group\ncompleted_requests,3,2,count,1.0,metric\n",
        encoding="utf-8",
    )
    sparse_bundle.write_text(
        json.dumps(
            {
                "contract_version": 4,
                "reference_sources": ["repo://test/sparse_metrics.csv"],
                "bundle_id": "sparse-bundle",
                "name": "Sparse Bundle",
                "version": "3.0.0",
                "scenario": str(scenario_path.resolve()).replace("\\", "/"),
                "backtest": {"metric_targets": str(sparse_metrics.resolve()).replace("\\", "/")},
                "coverage_requirements": {
                    "min_metric_targets": 2,
                    "min_event_expectations": 1,
                    "min_series_targets": 1,
                    "required_scopes": [],
                },
                "objective_group_weights": {"metric": 1.0, "event": 1.0, "series": 1.0},
                "confidence_policy": {
                    "high_confidence_score": 0.85,
                    "medium_confidence_score": 0.6,
                    "max_sensitivity_delta": 0.3,
                    "max_failed_objectives": 0,
                    "min_coverage_completeness": 1.0,
                },
                "quality_checks": {
                    "require_reference_sources": True,
                    "require_bundle_hashes": False,
                    "require_monotonic_windows": True,
                    "require_scope_coverage": False,
                },
                "fit_space": {"parameters": []},
                "gates": {
                    "max_total_score": 2.0,
                    "require_metric_match": True,
                    "require_event_match": False,
                    "require_series_match": False,
                    "require_calibrated_parameters": True,
                    "require_evidence_coverage": True,
                },
                "provenance": {"source": "test", "maintainer": "repo", "curation_note": "sparse bundle", "generated_at": "2026-04-09T00:00:00Z"},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    assert avn_v2_main(["calibrate", str(sparse_bundle)]) == 0
    report = json.loads((tmp_path / "sparse_bundle.calibration_report.v2.json").read_text(encoding="utf-8"))

    assert report["promotable"] is False
    assert "evidence_insufficient" in report["failure_reasons"]
    assert "low" == report["confidence_tier"]
    assert "insufficient_metric_targets" in report["bundle_strength_flags"]


def test_v2_ingest_rejects_missing_required_mapping(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    manifest_path = _write_external_source_manifest(tmp_path, scenario_path, missing_metric_mapping=True)

    assert avn_v2_main(["ingest", str(manifest_path)]) == 1
    report = json.loads(
        (tmp_path / "external" / "bundles" / "baseline-vendor-baseline-2026-04-09" / "ingestion_report.v2.json").read_text(encoding="utf-8")
    )
    assert report["usable_bundle"] is False
    assert "missing_required_mapping:metrics:tolerance" in report["failure_reasons"]


def test_v2_ingest_rejects_unsupported_source_type(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    manifest_path = _write_external_source_manifest(tmp_path, scenario_path, source_type="live_api")

    assert avn_v2_main(["ingest", str(manifest_path)]) == 1
    report = json.loads(
        (tmp_path / "external" / "bundles" / "baseline-vendor-baseline-2026-04-09" / "ingestion_report.v2.json").read_text(encoding="utf-8")
    )
    assert report["usable_bundle"] is False
    assert "unsupported_source_type:live_api" in report["failure_reasons"]


def test_v2_ingest_rejects_unknown_scope_during_normalization(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    manifest_path = _write_external_source_manifest(tmp_path, scenario_path, invalid_scope=True)

    assert avn_v2_main(["ingest", str(manifest_path)]) == 1
    report = json.loads(
        (tmp_path / "external" / "bundles" / "baseline-vendor-baseline-2026-04-09" / "ingestion_report.v2.json").read_text(encoding="utf-8")
    )
    assert report["usable_bundle"] is False
    assert report["normalization_summary"]["series_targets"]["rejected_rows"] >= 1
    assert report["bundle_validation_status"] == "failed"


def test_v2_missing_provenance_fails_bundle_validation(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    invalid_bundle = tmp_path / "invalid_bundle.json"
    invalid_bundle.write_text(
        json.dumps(
            {
                "contract_version": 4,
                "bundle_id": "invalid-bundle",
                "name": "Invalid Bundle",
                "version": "4.0.0",
                "scenario": str(scenario_path.resolve()).replace("\\", "/"),
                "backtest": {"metric_targets": str((REPO_ROOT / "data" / "v2" / "bundles" / "reference_baseline" / "reference_metrics.csv").resolve()).replace("\\", "/")},
                "coverage_requirements": {"min_metric_targets": 1, "min_event_expectations": 0, "min_series_targets": 0, "required_scopes": []},
                "confidence_policy": {
                    "high_confidence_score": 0.85,
                    "medium_confidence_score": 0.6,
                    "max_sensitivity_delta": 0.3,
                    "max_failed_objectives": 0,
                    "min_coverage_completeness": 1.0,
                },
                "quality_checks": {
                    "require_reference_sources": True,
                    "require_bundle_hashes": False,
                    "require_monotonic_windows": True,
                    "require_scope_coverage": True,
                },
                "objective_group_weights": {"metric": 1.0},
                "fit_space": {"parameters": []},
                "gates": {
                    "max_total_score": 2.0,
                    "require_metric_match": True,
                    "require_event_match": False,
                    "require_series_match": False,
                    "require_calibrated_parameters": True,
                    "require_evidence_coverage": True,
                },
                "reference_sources": [],
                "provenance": {"source": "test"},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(BundleValidationError):
        avn_v2_main(["calibrate", str(invalid_bundle)])


def test_v2_report_and_adapter_preserve_confidence_and_grouped_blockers(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    result = run_scenario(load_scenario_config(scenario_path))

    report_view_path = Path(result.output_dir) / "report_view.v2.json"
    assert avn_v2_main(["report", str(result.output_dir)]) == 0
    report_view = json.loads(report_view_path.read_text(encoding="utf-8"))
    promotion = json.loads(result.promotion_decisions_path.read_text(encoding="utf-8"))
    request = load_v2_request(result.output_dir)

    assert "calibration_confidence" in report_view["summary"]
    assert "policy_result" in report_view["summary"]
    assert "decision_axes" in promotion
    assert "promotion_blockers" in promotion
    assert any(item.startswith("calibration_confidence=") for item in request.decision_context)
    assert any(item.startswith("policy_result=") for item in request.decision_context)


def test_v2_advisory_governance_policy_does_not_block_experiment(tmp_path: Path) -> None:
    scenario_path = _temp_scenario(tmp_path)
    sparse_bundle = tmp_path / "advisory_bundle.json"
    sparse_metrics = tmp_path / "advisory_metrics.csv"
    sparse_metrics.write_text(
        "metric_key,reference_value,tolerance,unit,weight,objective_group\ncompleted_requests,3,2,count,1.0,metric\navg_delay_minutes,3,3,minutes,1.0,metric\n",
        encoding="utf-8",
    )
    sparse_bundle.write_text(
        json.dumps(
            {
                "contract_version": 4,
                "bundle_id": "advisory-bundle",
                "name": "Advisory Bundle",
                "version": "4.0.0",
                "scenario": str(scenario_path.resolve()).replace("\\", "/"),
                "backtest": {"metric_targets": str(sparse_metrics.resolve()).replace("\\", "/")},
                "coverage_requirements": {"min_metric_targets": 2, "min_event_expectations": 1, "min_series_targets": 1, "required_scopes": ["node"]},
                "confidence_policy": {
                    "high_confidence_score": 0.85,
                    "medium_confidence_score": 0.6,
                    "max_sensitivity_delta": 0.3,
                    "max_failed_objectives": 0,
                    "min_coverage_completeness": 1.0,
                },
                "quality_checks": {
                    "require_reference_sources": True,
                    "require_bundle_hashes": False,
                    "require_monotonic_windows": True,
                    "require_scope_coverage": True,
                },
                "objective_group_weights": {"metric": 1.0, "event": 1.0, "series": 1.0},
                "fit_space": {"parameters": []},
                "gates": {
                    "max_total_score": 2.0,
                    "require_metric_match": True,
                    "require_event_match": False,
                    "require_series_match": False,
                    "require_calibrated_parameters": True,
                    "require_evidence_coverage": True,
                },
                "reference_sources": ["repo://test/advisory_metrics.csv"],
                "provenance": {"source": "test", "maintainer": "repo", "curation_note": "advisory bundle", "generated_at": "2026-04-09T00:00:00Z"},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    experiment_path = _write_experiment_manifest(
        tmp_path,
        scenario_path,
        bundle_path=sparse_bundle,
        governance_policy="""
[governance_policy]
minimum_confidence_tier = "low"
fatal_blocker_categories = ["operational_breach"]
advisory_blocker_categories = ["fit_failure", "evidence_insufficiency"]
waivable_categories = []
""",
    )

    assert avn_v2_main(["experiment", str(experiment_path)]) == 0
    payload = json.loads(next((tmp_path / "experiments").rglob("experiment_summary.v2.json")).read_text(encoding="utf-8"))
    assert payload["policy_result"]["policy_eligible"] is True
    assert payload["advisory_blockers"]
