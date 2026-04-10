from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifacts import V2_CONTRACT_VERSION, manifest_entry, write_json
from .models import GovernancePolicy
from .policy import contradiction_policy_metadata, default_governance_policy, evaluate_governance_policy


def utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_v2_artifact_bundle(
    *,
    output_dir: Path,
    scenario_name: str,
    run_id: str,
    summary: dict[str, Any],
    backtest_trace_path: Path,
    threshold_ledger: dict[str, Any],
    hazard_ledger: dict[str, Any],
    promotion_decisions: dict[str, Any],
    contradictions: dict[str, Any],
    calibration_report: dict[str, Any] | None = None,
    bundle_validation_report: dict[str, Any] | None = None,
) -> dict[str, Path | None]:
    run_summary_path = write_json(output_dir / "run_summary.v2.json", summary)
    threshold_ledger_path = write_json(output_dir / "threshold_ledger.v2.json", threshold_ledger)
    hazard_ledger_path = write_json(output_dir / "hazard_ledger.v2.json", hazard_ledger)
    promotion_decisions_path = write_json(output_dir / "promotion_decisions.v2.json", promotion_decisions)
    contradictions_path = write_json(output_dir / "contradictions.v2.json", contradictions)
    calibration_report_path = None
    if calibration_report is not None:
        calibration_report_path = write_json(output_dir / "calibration_report.v2.json", calibration_report)
    bundle_validation_path = None
    if bundle_validation_report is not None:
        bundle_validation_path = write_json(output_dir / "bundle_validation.v2.json", bundle_validation_report)

    manifest_entries = [
        manifest_entry(
            artifact_id=f"{run_id}:run_summary",
            artifact_type="run_summary",
            path=run_summary_path,
            contract_version=V2_CONTRACT_VERSION,
        ),
        manifest_entry(
            artifact_id=f"{run_id}:backtest_trace",
            artifact_type="backtest_trace",
            path=backtest_trace_path,
            contract_version=V2_CONTRACT_VERSION,
        ),
        manifest_entry(
            artifact_id=f"{run_id}:threshold_ledger",
            artifact_type="threshold_ledger",
            path=threshold_ledger_path,
            contract_version=V2_CONTRACT_VERSION,
        ),
        manifest_entry(
            artifact_id=f"{run_id}:hazard_ledger",
            artifact_type="hazard_ledger",
            path=hazard_ledger_path,
            contract_version=V2_CONTRACT_VERSION,
        ),
        manifest_entry(
            artifact_id=f"{run_id}:promotion_decisions",
            artifact_type="promotion_decisions",
            path=promotion_decisions_path,
            contract_version=V2_CONTRACT_VERSION,
        ),
        manifest_entry(
            artifact_id=f"{run_id}:contradictions",
            artifact_type="contradictions",
            path=contradictions_path,
            contract_version=V2_CONTRACT_VERSION,
        ),
    ]
    if calibration_report_path is not None:
        manifest_entries.append(
            manifest_entry(
                artifact_id=f"{run_id}:calibration_report",
                artifact_type="calibration_report",
                path=calibration_report_path,
                contract_version=V2_CONTRACT_VERSION,
            )
        )
    if bundle_validation_path is not None:
        manifest_entries.append(
            manifest_entry(
                artifact_id=f"{run_id}:bundle_validation",
                artifact_type="bundle_validation",
                path=bundle_validation_path,
                contract_version=V2_CONTRACT_VERSION,
            )
        )

    manifest_path = write_json(
        output_dir / "artifact_manifest.v2.json",
        {
            "id": f"{run_id}:artifact_manifest",
            "artifact_type": "artifact_manifest",
            "contract_version": V2_CONTRACT_VERSION,
            "scenario_name": scenario_name,
            "generated_at": utc_timestamp(),
            "entries": manifest_entries,
        },
    )
    report_bundle_path = write_json(
        output_dir / "report_bundle.v2.json",
        {
            "id": f"{run_id}:report_bundle",
            "artifact_type": "report_bundle",
            "contract_version": V2_CONTRACT_VERSION,
            "scenario_name": scenario_name,
            "generated_at": utc_timestamp(),
            "summary": {
                "dominant_failure_chain": summary["dominant_failure_chain"],
                "completed_requests": summary["completed_requests"],
                "cancelled_requests": summary["cancelled_requests"],
                "diversion_count": summary["diversion_count"],
                "queue_overflow_count": summary["queue_overflow_count"],
                "reposition_count": summary["reposition_count"],
                "avg_delay_minutes": summary["avg_delay_minutes"],
                "fit_quality_summary": None if calibration_report is None else calibration_report.get("fit_quality_summary"),
                "confidence_score": None if calibration_report is None else calibration_report.get("confidence_score"),
                "calibration_confidence": None if calibration_report is None else calibration_report.get("confidence_tier"),
                "calibration_status": "present" if calibration_report is not None else "not_requested",
                "bundle_validation_status": None if bundle_validation_report is None else bundle_validation_report.get("status"),
            },
            "artifacts": manifest_entries,
        },
    )
    return {
        "manifest_path": manifest_path,
        "run_summary_path": run_summary_path,
        "threshold_ledger_path": threshold_ledger_path,
        "hazard_ledger_path": hazard_ledger_path,
        "promotion_decisions_path": promotion_decisions_path,
        "contradictions_path": contradictions_path,
        "report_bundle_path": report_bundle_path,
        "calibration_report_path": calibration_report_path,
        "bundle_validation_path": bundle_validation_path,
    }


def build_run_governance_artifacts(
    *,
    run_id: str,
    scenario_name: str,
    summary: dict[str, Any],
    calibration_report: dict[str, Any] | None,
    bundle_validation_report: dict[str, Any] | None = None,
    governance_policy: GovernancePolicy | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    active_policy = governance_policy or default_governance_policy()
    delay_breach = round(
        summary["dispatch_policy"]["max_wait_minutes"] + summary["dispatch_policy"]["operator_delay_minutes"],
        2,
    )
    fit_quality_summary = None if calibration_report is None else calibration_report.get("fit_quality_summary", {})
    selected_parameters = None if calibration_report is None else calibration_report.get("selected_parameters", {})
    evidence_coverage_summary = None if calibration_report is None else calibration_report.get("evidence_coverage_summary", {})
    confidence_tier = "missing" if calibration_report is None else str(calibration_report.get("confidence_tier", "low"))
    confidence_score = None if calibration_report is None else calibration_report.get("confidence_score")
    bundle_strength_flags = [] if calibration_report is None else list(calibration_report.get("bundle_strength_flags", ()))
    unresolved_contradictions = [] if calibration_report is None else list(calibration_report.get("failure_reasons", ()))
    grouped_blockers = {
        "fit_failure": [],
        "evidence_insufficiency": [],
        "operational_breach": [],
    }
    for reason in unresolved_contradictions:
        if reason == "evidence_insufficient" or reason.startswith("insufficient_") or reason.startswith("missing_required_scope:"):
            grouped_blockers["evidence_insufficiency"].append(reason)
        else:
            grouped_blockers["fit_failure"].append(reason)
    if summary["cancelled_requests"] > 0:
        grouped_blockers["operational_breach"].append("service_commitment_breach")
    if summary["queue_overflow_count"] > 0:
        grouped_blockers["operational_breach"].append("queue_pressure_breach")
    if summary["diversion_count"] > summary["contingency_policy"]["diversion_limit"]:
        grouped_blockers["operational_breach"].append("diversion_limit_breach")
    if bundle_validation_report is not None and bundle_validation_report.get("status") != "passed":
        grouped_blockers["evidence_insufficiency"].extend(bundle_validation_report.get("failure_reasons", ()))
        unresolved_contradictions.extend(bundle_validation_report.get("failure_reasons", ()))
    grouped_blockers = {
        key: list(dict.fromkeys(values))
        for key, values in grouped_blockers.items()
    }
    unresolved_contradictions = list(dict.fromkeys(unresolved_contradictions))
    operational_readiness = not grouped_blockers["operational_breach"]
    policy_result = evaluate_governance_policy(grouped_blockers, confidence_tier=confidence_tier, policy=active_policy)
    promotion_eligibility = (
        operational_readiness
        and calibration_report is not None
        and calibration_report.get("promotable", False)
        and policy_result["policy_eligible"]
    )
    threshold_ledger = {
        "id": f"{run_id}:threshold_ledger",
        "artifact_type": "threshold_ledger",
        "contract_version": V2_CONTRACT_VERSION,
        "scenario_name": scenario_name,
        "thresholds": [
            {
                "id": "dispatch.delay",
                "metric_key": "avg_delay_minutes",
                "warning_value": round(max(summary["avg_delay_minutes"] * 0.75, 1.0), 2),
                "breach_value": delay_breach,
                "status": "breached" if summary["avg_delay_minutes"] > delay_breach else "within_bounds",
            },
            {
                "id": "dispatch.diversions",
                "metric_key": "diversion_count",
                "warning_value": 1,
                "breach_value": max(1, summary["contingency_policy"]["diversion_limit"]),
                "status": (
                    "breached"
                    if summary["diversion_count"] > summary["contingency_policy"]["diversion_limit"]
                    else "within_bounds"
                ),
            },
            {
                "id": "reservation.conflicts",
                "metric_key": "reservation_conflicts",
                "warning_value": 1,
                "breach_value": max(2, summary["reservation_conflicts"]),
                "status": "breached" if summary["reservation_conflicts"] > 2 else "watch",
            },
            {
                "id": "queue.pressure",
                "metric_key": "peak_queue_length",
                "warning_value": 1,
                "breach_value": max(1, summary["peak_queue_length"]),
                "status": "breached" if summary["queue_overflow_count"] > 0 else "watch",
            },
        ],
        "calibration_bundle_id": None if calibration_report is None else calibration_report.get("bundle_id"),
        "calibration_report_id": None if calibration_report is None else calibration_report.get("id"),
        "fit_quality_summary": fit_quality_summary,
        "confidence_score": confidence_score,
        "calibration_confidence": confidence_tier,
        "evidence_coverage_summary": evidence_coverage_summary,
        "selected_parameters": selected_parameters,
    }
    hazard_ledger = {
        "id": f"{run_id}:hazard_ledger",
        "artifact_type": "hazard_ledger",
        "contract_version": V2_CONTRACT_VERSION,
        "scenario_name": scenario_name,
        "hazards": [
            {
                "id": "hazard.dispatch_conflict",
                "title": "Reservation conflict backlog",
                "severity": "high" if summary["reservation_conflicts"] > 0 else "low",
                "mechanism": "dispatch_reroute",
                "evidence": {
                    "reservation_conflicts": summary["reservation_conflicts"],
                    "reroute_count": summary["reroute_count"],
                },
                "mitigation": "Increase reservation capacity or add alternate routing capacity.",
            },
            {
                "id": "hazard.contingency_pressure",
                "title": "Contingency pressure under route failure",
                "severity": "high" if summary["diversion_count"] > 0 else "medium",
                "mechanism": "contingency_management",
                "evidence": {
                    "diversion_count": summary["diversion_count"],
                    "contingency_activations": summary["contingency_activations"],
                },
                "mitigation": "Increase contingency slots or improve reroute policy before promotion.",
            },
            {
                "id": "hazard.queue_pressure",
                "title": "Origin queue overflow",
                "severity": "high" if summary["queue_overflow_count"] > 0 else "low",
                "mechanism": "dispatch_queue",
                "evidence": {
                    "queue_overflow_count": summary["queue_overflow_count"],
                    "peak_queue_length": summary["peak_queue_length"],
                    "reposition_count": summary["reposition_count"],
                },
                "mitigation": "Increase origin service rate, add local vehicles, or reduce dispatch wait budget.",
            },
        ],
        "calibration_basis": {
            "bundle_id": None if calibration_report is None else calibration_report.get("bundle_id"),
            "fit_report_id": None if calibration_report is None else calibration_report.get("id"),
            "fit_quality_summary": fit_quality_summary,
            "confidence_score": confidence_score,
            "confidence_tier": confidence_tier,
            "bundle_strength_flags": bundle_strength_flags,
        },
    }
    contradictions = {
        "id": f"{run_id}:contradictions",
        "artifact_type": "contradictions",
        "contract_version": V2_CONTRACT_VERSION,
        "scenario_name": scenario_name,
        "findings": [
            {
                "id": "contradiction.calibration_required",
                "status": "open" if calibration_report is None or not calibration_report.get("promotable", False) else "resolved",
                "category": "evidence_insufficiency" if calibration_report is None else "fit_failure",
                **contradiction_policy_metadata("evidence_insufficiency" if calibration_report is None else "fit_failure", policy=active_policy),
                "note": "Promotion requires a cited calibration bundle, selected parameters, and a passing fit report.",
            },
            {
                "id": "contradiction.service_commitment",
                "status": "open" if summary["cancelled_requests"] > 0 or summary["queue_overflow_count"] > 0 else "resolved",
                "category": "operational_breach",
                **contradiction_policy_metadata("operational_breach", policy=active_policy),
                "note": "Cancelled demand or queue overflow indicates service commitment breach under the current policy.",
            },
        ]
        + [
            {
                "id": f"contradiction.fit:{reason}",
                "status": "open",
                "category": (
                    "evidence_insufficiency"
                    if reason == "evidence_insufficient" or reason.startswith("insufficient_") or reason.startswith("missing_required_scope:") or reason.startswith("missing_")
                    else "fit_failure"
                ),
                **contradiction_policy_metadata(
                    "evidence_insufficiency"
                    if reason == "evidence_insufficient" or reason.startswith("insufficient_") or reason.startswith("missing_required_scope:") or reason.startswith("missing_")
                    else "fit_failure",
                    policy=active_policy,
                ),
                "note": f"Calibration gate failed due to {reason}.",
            }
            for reason in unresolved_contradictions
        ],
    }
    promotion_decisions = {
        "id": f"{run_id}:promotion_decisions",
        "artifact_type": "promotion_decisions",
        "contract_version": V2_CONTRACT_VERSION,
        "scenario_name": scenario_name,
        "calibration_basis": {
            "bundle_id": None if calibration_report is None else calibration_report.get("bundle_id"),
            "bundle_version": None if calibration_report is None else calibration_report.get("bundle_version"),
            "fit_report_id": None if calibration_report is None else calibration_report.get("id"),
            "fit_quality_summary": fit_quality_summary,
            "confidence_score": confidence_score,
            "confidence_tier": confidence_tier,
            "evidence_coverage_summary": evidence_coverage_summary,
            "bundle_strength_flags": bundle_strength_flags,
            "bundle_validation_id": None if bundle_validation_report is None else bundle_validation_report.get("id"),
            "unresolved_contradictions": unresolved_contradictions,
        },
        "decision_axes": {
            "operational_readiness": {
                "decision": "ready" if operational_readiness else "hold",
                "blockers": grouped_blockers["operational_breach"],
            },
            "calibration_confidence": {
                "decision": confidence_tier,
                "blockers": grouped_blockers["evidence_insufficiency"],
            },
            "promotion_eligibility": {
                "decision": "promote" if promotion_eligibility else "hold",
                "blockers": grouped_blockers["fit_failure"] + grouped_blockers["evidence_insufficiency"] + grouped_blockers["operational_breach"],
            },
        },
        "promotion_blockers": grouped_blockers,
        "policy_result": policy_result,
        "waiver_status": policy_result["waiver_status"],
        "decisions": [
            {
                "id": "promotion.operational_baseline",
                "decision": "promote" if promotion_eligibility else "hold",
                "operational_readiness": "ready" if operational_readiness else "hold",
                "calibration_confidence": confidence_tier,
                "policy_effect": "eligible" if policy_result["policy_eligible"] else "blocked",
                "rationale": "Promotion requires bounded operations, sufficient calibration evidence, a valid bundle package, and a policy-permitted blocker set.",
                "blockers": grouped_blockers["fit_failure"] + grouped_blockers["evidence_insufficiency"] + grouped_blockers["operational_breach"],
            }
        ],
    }
    return threshold_ledger, hazard_ledger, promotion_decisions, contradictions


def build_report_from_directory(path: Path) -> dict[str, Any]:
    report_bundle_path = path if path.name.endswith("report_bundle.v2.json") else path / "report_bundle.v2.json"
    if report_bundle_path.exists():
        payload = json.loads(report_bundle_path.read_text(encoding="utf-8"))
        root = report_bundle_path.parent
        promotion_path = root / "promotion_decisions.v2.json"
        bundle_validation_path = root / "bundle_validation.v2.json"
        promotion_payload = json.loads(promotion_path.read_text(encoding="utf-8")) if promotion_path.exists() else {}
        bundle_validation_payload = json.loads(bundle_validation_path.read_text(encoding="utf-8")) if bundle_validation_path.exists() else {}
        return {
            "id": payload["id"],
            "artifact_type": "report_view",
            "contract_version": payload["contract_version"],
            "scenario_name": payload["scenario_name"],
            "generated_at": utc_timestamp(),
            "summary": {
                **payload["summary"],
                "policy_result": promotion_payload.get("policy_result"),
                "bundle_validation_status": bundle_validation_payload.get("status"),
            },
            "artifact_count": len(payload["artifacts"]),
        }
    experiment_summary_path = path if path.name.endswith("experiment_summary.v2.json") else path / "experiment_summary.v2.json"
    payload = json.loads(experiment_summary_path.read_text(encoding="utf-8"))
    return {
        "id": payload["id"],
        "artifact_type": "report_view",
        "contract_version": payload["contract_version"],
        "scenario_name": payload.get("scenario_name", payload["experiment_name"]),
        "generated_at": utc_timestamp(),
        "summary": {
            "best_slice": payload["best_slice"],
            "policy_result": payload.get("policy_result"),
            "fatal_blockers": payload.get("fatal_blockers", []),
            "advisory_blockers": payload.get("advisory_blockers", []),
            "slice_count": len(payload["slices"]),
            "adaptive_refinement": payload["adaptive_refinement"],
            "calibration_gate": payload.get("calibration_gate"),
            "calibration_bundle": payload.get("calibration_bundle_id"),
            "calibration_confidence": payload.get("calibration_confidence"),
            "evidence_weak_regions": payload.get("evidence_weak_regions", []),
            "promotion_blockers": payload.get("promotion_blockers", {}),
            "fit_quality_summary": payload.get("fit_quality_summary"),
        },
        "artifact_count": len(payload["slices"]),
    }
