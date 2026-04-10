from __future__ import annotations

import copy
from itertools import product
from pathlib import Path
from typing import Any

from .artifacts import V2_CONTRACT_VERSION, write_json
from .calibration import fit_bundle_to_config
from .config import load_experiment_manifest, load_scenario_config
from .engine import _apply_override, run_scenario
from .config import load_reference_bundle
from .ingest import resolve_ingested_bundle_source
from .policy import evaluate_governance_policy
from .reporting import utc_timestamp
from .validation import BundleValidationError, build_bundle_validation_report


def _coerce_midpoint(value_a: Any, value_b: Any) -> Any:
    if isinstance(value_a, bool) or isinstance(value_b, bool):
        return value_a
    if isinstance(value_a, (int, float)) and isinstance(value_b, (int, float)):
        midpoint = (float(value_a) + float(value_b)) / 2.0
        if isinstance(value_a, int) and isinstance(value_b, int):
            return int(round(midpoint))
        return round(midpoint, 4)
    return value_a


def _group_calibration_blockers(calibration_report: dict[str, Any] | None) -> dict[str, list[str]]:
    grouped = {
        "fit_failure": [],
        "evidence_insufficiency": [],
        "operational_breach": [],
    }
    if calibration_report is None:
        grouped["evidence_insufficiency"].append("calibration_missing")
        return grouped
    for reason in calibration_report.get("failure_reasons", ()):
        if reason == "evidence_insufficient" or str(reason).startswith("insufficient_") or str(reason).startswith("missing_required_scope:"):
            grouped["evidence_insufficiency"].append(str(reason))
        else:
            grouped["fit_failure"].append(str(reason))
    return grouped


def run_experiment(manifest_path: str | Path) -> dict[str, Any]:
    manifest = load_experiment_manifest(manifest_path)
    base = load_scenario_config(manifest.base_scenario)
    output_dir = manifest.output_root / f"{manifest.experiment_name}_{utc_timestamp().replace(':', '').replace('-', '')}"
    output_dir.mkdir(parents=True, exist_ok=False)
    calibration_report = None
    calibration_bundle = None
    bundle_validation_report = None
    promotion_blockers = {
        "fit_failure": [],
        "evidence_insufficiency": [],
        "operational_breach": [],
    }
    calibrated_base = base
    baseline_summary = None
    calibration_bundle_path = manifest.calibration_bundle
    if calibration_bundle_path is None and manifest.ingested_bundle_source is not None:
        calibration_bundle_path = resolve_ingested_bundle_source(manifest.ingested_bundle_source)
    if calibration_bundle_path is not None:
        calibration_bundle = load_reference_bundle(calibration_bundle_path)
        bundle_validation_report = build_bundle_validation_report(
            calibration_bundle,
            report_id=f"{manifest.experiment_name}:bundle",
        )
        if bundle_validation_report["status"] != "passed":
            promotion_blockers["evidence_insufficiency"].extend(bundle_validation_report["failure_reasons"])
            prefit_policy = evaluate_governance_policy(
                promotion_blockers,
                confidence_tier="missing",
                policy=manifest.governance_policy,
            )
            if not prefit_policy["policy_eligible"]:
                raise BundleValidationError(
                    f"Bundle validation failed for experiment {manifest.experiment_name}: {', '.join(bundle_validation_report['failure_reasons'])}"
                )
        calibration_report = fit_bundle_to_config(
            calibration_bundle,
            base,
            output_root=output_dir / "calibration_fit",
            report_id=f"{manifest.experiment_name}:base",
            bundle_validation_report=bundle_validation_report,
        )
        if manifest.calibration_gate == "required" and not calibration_report["promotable"]:
            grouped_from_calibration = _group_calibration_blockers(calibration_report)
            for category, blocker_ids in grouped_from_calibration.items():
                promotion_blockers[category].extend(blocker_ids)
        calibrated_base = copy.deepcopy(base)
        object.__setattr__(calibrated_base.calibration, "enabled", False)
        object.__setattr__(calibrated_base.calibration, "bundle", None)
        if manifest.use_calibrated_parameters:
            for scenario_key, value in calibration_report["selected_parameters"].items():
                calibrated_base = _apply_override(calibrated_base, scenario_key, value)

    slices = []
    axes = manifest.axes
    for values in product(*(axis.values for axis in axes)):
        config = calibrated_base
        overrides = {}
        for axis, value in zip(axes, values, strict=True):
            config = _apply_override(config, axis.name, value)
            overrides[axis.name] = value
        result = run_scenario(config, output_root=output_dir / "runs")
        baseline_summary = baseline_summary or result.summary
        drift = {}
        if calibration_report is not None:
            for metric in manifest.promoted_metrics:
                baseline_value = float(baseline_summary.get(metric, 0.0))
                current_value = float(result.summary.get(metric, 0.0))
                drift[metric] = round(current_value - baseline_value, 6)
        slices.append(
            {
                "slice_id": result.summary["run_id"],
                "overrides": overrides,
                "summary": {
                    "completed_requests": result.summary["completed_requests"],
                    "cancelled_requests": result.summary["cancelled_requests"],
                    "avg_delay_minutes": result.summary["avg_delay_minutes"],
                    "diversion_count": result.summary["diversion_count"],
                    "queue_overflow_count": result.summary["queue_overflow_count"],
                    "dominant_failure_chain": result.summary["dominant_failure_chain"],
                },
                "calibration_drift": drift,
                "report_bundle_path": str(result.report_bundle_path),
            }
        )

    if manifest.adaptive_refinement and axes:
        first_axis = axes[0]
        if len(first_axis.values) >= 2:
            midpoint_value = _coerce_midpoint(first_axis.values[0], first_axis.values[-1])
            config = _apply_override(calibrated_base, first_axis.name, midpoint_value)
            result = run_scenario(config, output_root=output_dir / "adaptive_runs")
            drift = {}
            if calibration_report is not None and baseline_summary is not None:
                for metric in manifest.promoted_metrics:
                    baseline_value = float(baseline_summary.get(metric, 0.0))
                    current_value = float(result.summary.get(metric, 0.0))
                    drift[metric] = round(current_value - baseline_value, 6)
            slices.append(
                {
                    "slice_id": result.summary["run_id"],
                    "overrides": {first_axis.name: midpoint_value},
                    "summary": {
                        "completed_requests": result.summary["completed_requests"],
                        "cancelled_requests": result.summary["cancelled_requests"],
                        "avg_delay_minutes": result.summary["avg_delay_minutes"],
                        "diversion_count": result.summary["diversion_count"],
                        "queue_overflow_count": result.summary["queue_overflow_count"],
                        "dominant_failure_chain": result.summary["dominant_failure_chain"],
                    },
                    "calibration_drift": drift,
                    "report_bundle_path": str(result.report_bundle_path),
                    "adaptive": True,
                }
            )

    best_slice = sorted(
        slices,
        key=lambda item: (
            item["summary"]["diversion_count"],
            item["summary"]["avg_delay_minutes"],
            -item["summary"]["completed_requests"],
        ),
    )[0]
    sensitivity_threshold = (
        0.0
        if calibration_report is None
        else float(calibration_report.get("sensitivity_summary", {}).get("sensitivity_threshold", 0.0))
    )
    unstable_regions = [
        {
            "slice_id": item["slice_id"],
            "reason": "failure_chain_shift" if item["summary"]["dominant_failure_chain"] != best_slice["summary"]["dominant_failure_chain"] else "calibration_drift",
            "calibration_drift": item.get("calibration_drift", {}),
        }
        for item in slices
        if item["summary"]["dominant_failure_chain"] != best_slice["summary"]["dominant_failure_chain"]
        or any(abs(float(value)) > 2.0 for value in item.get("calibration_drift", {}).values())
    ]
    evidence_weak_regions = [
        {
            "slice_id": item["slice_id"],
            "reason": "weak_calibration_evidence",
            "calibration_drift": item.get("calibration_drift", {}),
        }
        for item in slices
        if calibration_report is not None
        and (
            calibration_report.get("confidence_tier") == "low"
            or any(abs(float(value)) > sensitivity_threshold for value in item.get("calibration_drift", {}).values())
        )
    ]
    best_fit_slice = sorted(
        slices,
        key=lambda item: (
            sum(abs(float(value)) for value in item.get("calibration_drift", {}).values()),
            item["summary"]["avg_delay_minutes"],
            item["slice_id"],
        ),
    )[0]
    if any(item["summary"]["cancelled_requests"] > 0 or item["summary"]["queue_overflow_count"] > 0 for item in slices):
        promotion_blockers["operational_breach"].append("slice_operational_breach")
    promotion_blockers = {
        key: list(dict.fromkeys(values))
        for key, values in promotion_blockers.items()
    }
    policy_result = evaluate_governance_policy(
        promotion_blockers,
        confidence_tier="missing" if calibration_report is None else str(calibration_report.get("confidence_tier", "low")),
        policy=manifest.governance_policy,
    )
    policy_eligible_slices = [
        {
            "slice_id": item["slice_id"],
            "eligible": item["summary"]["cancelled_requests"] == 0 and item["summary"]["queue_overflow_count"] == 0,
        }
        for item in slices
    ]
    payload = {
        "id": f"{manifest.experiment_name}:experiment_summary",
        "artifact_type": "experiment_summary",
        "contract_version": V2_CONTRACT_VERSION,
        "generated_at": utc_timestamp(),
        "experiment_name": manifest.experiment_name,
        "base_scenario": str(manifest.base_scenario.resolve()),
        "adaptive_refinement": manifest.adaptive_refinement,
        "calibration_gate": manifest.calibration_gate,
        "calibration_bundle_path": None if calibration_bundle_path is None else str(calibration_bundle_path.resolve()),
        "calibration_bundle_id": None if calibration_bundle is None else calibration_bundle.bundle_id,
        "bundle_validation_id": None if bundle_validation_report is None else bundle_validation_report["id"],
        "calibration_report_id": None if calibration_report is None else calibration_report["id"],
        "fit_quality_summary": None if calibration_report is None else calibration_report["fit_quality_summary"],
        "calibration_confidence": None
        if calibration_report is None
        else {
            "confidence_score": calibration_report.get("confidence_score"),
            "confidence_tier": calibration_report.get("confidence_tier"),
            "bundle_strength_flags": calibration_report.get("bundle_strength_flags", []),
            "evidence_coverage_summary": calibration_report.get("evidence_coverage_summary", {}),
            "sensitivity_summary": calibration_report.get("sensitivity_summary", {}),
        },
        "policy_result": policy_result,
        "fatal_blockers": policy_result["fatal_blockers"],
        "advisory_blockers": policy_result["advisory_blockers"],
        "waived_blockers": policy_result["waived_blockers"],
        "promotion_blockers": promotion_blockers,
        "promoted_metrics": list(manifest.promoted_metrics),
        "best_slice": best_slice,
        "best_fit_slice": best_fit_slice,
        "unstable_regions": unstable_regions,
        "evidence_weak_regions": evidence_weak_regions,
        "policy_eligible_slices": policy_eligible_slices,
        "calibration_drift_summary": {
            metric: {
                "min": min(float(item.get("calibration_drift", {}).get(metric, 0.0)) for item in slices),
                "max": max(float(item.get("calibration_drift", {}).get(metric, 0.0)) for item in slices),
            }
            for metric in manifest.promoted_metrics
        },
        "slices": slices,
    }
    if bundle_validation_report is not None:
        write_json(output_dir / "bundle_validation.v2.json", bundle_validation_report)
    summary_path = write_json(output_dir / "experiment_summary.v2.json", payload)
    return {"output_dir": str(output_dir.resolve()), "summary_path": str(summary_path.resolve()), "payload": payload}
