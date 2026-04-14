from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from avn.governance.artifacts import ARTIFACT_CONTRACT_VERSION, payload_sha256
from avn.governance.models import ValidationCheck, ValidationReport


REQUIRED_RUN_ARTIFACTS = {
    "replay",
    "summary",
    "threshold_ledger",
    "promotion_decisions",
    "validation_report",
}
REQUIRED_BATCH_SUMMARY_KEYS = {
    "batch_id",
    "contract_version",
    "repeat",
    "scenario_sources",
    "run_count",
    "runs",
    "scenario_statistics",
    "suite_statistics",
}


def _failed_report(*, report_id: str, category: str, check_id: str, detail: str, summary: dict[str, Any]) -> ValidationReport:
    return ValidationReport(
        report_id=report_id,
        contract_version=ARTIFACT_CONTRACT_VERSION,
        status="failed",
        checks=[
            ValidationCheck(
                check_id=check_id,
                status="failed",
                detail=detail,
                category=category,
            )
        ],
        summary=summary,
    )


def _required_keys(payload: dict[str, Any], keys: set[str], *, check_id: str, checks: list[ValidationCheck]) -> bool:
    missing = sorted(keys - set(payload))
    passed = not missing
    checks.append(
        ValidationCheck(
            check_id=check_id,
            status="passed" if passed else "failed",
            detail="Required keys are present." if passed else f"Missing keys: {missing}",
        )
    )
    return passed


def build_run_validation_report(
    *,
    replay: dict[str, Any],
    summary: dict[str, Any],
    threshold_ledger: dict[str, Any],
    promotion_decisions: dict[str, Any],
) -> ValidationReport:
    checks: list[ValidationCheck] = []
    _required_keys(replay, {"scenario_id", "policy", "summary", "steps", "event_log"}, check_id="replay.schema", checks=checks)
    _required_keys(
        summary,
        {"scenario_id", "completed_vehicles", "max_queue_length", "max_corridor_load_ratio"},
        check_id="summary.schema",
        checks=checks,
    )
    _required_keys(
        threshold_ledger,
        {"scenario_id", "evaluations", "summary"},
        check_id="threshold_ledger.schema",
        checks=checks,
    )
    _required_keys(
        promotion_decisions,
        {"scenario_id", "release_status", "decisions", "summary"},
        check_id="promotion_decisions.schema",
        checks=checks,
    )
    release_status_matches = promotion_decisions.get("release_status") == threshold_ledger.get("summary", {}).get("release_status")
    checks.append(
        ValidationCheck(
            check_id="governance.release_status",
            status="passed" if release_status_matches else "failed",
            detail="Promotion decisions match threshold summary release status."
            if release_status_matches
            else "Promotion decisions do not match threshold ledger release status.",
            category="consistency",
        )
    )
    status = "passed" if all(check.status == "passed" for check in checks) else "failed"
    return ValidationReport(
        report_id=f"{summary.get('scenario_id', 'unknown')}:validation_report",
        contract_version=ARTIFACT_CONTRACT_VERSION,
        status=status,
        checks=checks,
        summary={
            "check_count": len(checks),
            "failure_count": sum(1 for check in checks if check.status != "passed"),
        },
    )


def validate_run_directory(path: str | Path) -> ValidationReport:
    output_dir = Path(path)
    manifest_path = output_dir / "artifact_manifest.json"
    if not manifest_path.exists():
        return _failed_report(
            report_id=f"{output_dir.name or 'unknown'}:validate_run_directory",
            category="manifest",
            check_id="manifest.exists",
            detail=f"Missing artifact manifest: {manifest_path}",
            summary={"artifact_family": None, "failure_count": 1},
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return _failed_report(
            report_id=f"{output_dir.name or 'unknown'}:validate_run_directory",
            category="manifest",
            check_id="manifest.json",
            detail=f"Artifact manifest is not valid JSON: {manifest_path} ({exc.msg})",
            summary={"artifact_family": None, "failure_count": 1},
        )
    checks: list[ValidationCheck] = []
    artifacts = manifest.get("artifacts", [])
    artifact_types = {item["artifact_type"] for item in artifacts}
    missing_artifacts = sorted(REQUIRED_RUN_ARTIFACTS - artifact_types)
    checks.append(
        ValidationCheck(
            check_id="manifest.required_artifacts",
            status="passed" if not missing_artifacts else "failed",
            detail="All required run artifacts are declared."
            if not missing_artifacts
            else f"Missing required artifacts: {missing_artifacts}",
            category="manifest",
        )
    )
    for artifact in artifacts:
        artifact_path = Path(artifact["path"])
        exists = artifact_path.exists()
        checks.append(
            ValidationCheck(
                check_id=f"artifact.exists.{artifact['artifact_type']}",
                status="passed" if exists else "failed",
                detail=f"{artifact['artifact_type']} exists at {artifact_path}" if exists else f"Missing artifact: {artifact_path}",
                category="manifest",
            )
        )
        if not exists:
            continue
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        actual_hash = payload_sha256(payload)
        checks.append(
            ValidationCheck(
                check_id=f"artifact.hash.{artifact['artifact_type']}",
                status="passed" if actual_hash == artifact["sha256"] else "failed",
                detail="Artifact hash matches manifest." if actual_hash == artifact["sha256"] else "Artifact hash mismatch.",
                category="manifest",
            )
        )

    status = "passed" if all(check.status == "passed" for check in checks) else "failed"
    return ValidationReport(
        report_id=f"{manifest.get('manifest_id', 'unknown')}:validate_run_directory",
        contract_version=ARTIFACT_CONTRACT_VERSION,
        status=status,
        checks=checks,
        summary={
            "artifact_family": manifest.get("artifact_family"),
            "failure_count": sum(1 for check in checks if check.status != "passed"),
        },
    )


def validate_batch_directory(path: str | Path) -> ValidationReport:
    output_dir = Path(path)
    manifest_path = output_dir / "batch_manifest.json"
    summary_path = output_dir / "batch_summary.json"
    if not manifest_path.exists():
        return _failed_report(
            report_id=f"{output_dir.name or 'unknown'}:validate_batch_directory",
            category="manifest",
            check_id="batch.manifest.exists",
            detail=f"Missing batch manifest: {manifest_path}",
            summary={"artifact_family": None, "failure_count": 1, "run_count": 0},
        )
    if not summary_path.exists():
        return _failed_report(
            report_id=f"{output_dir.name or 'unknown'}:validate_batch_directory",
            category="manifest",
            check_id="batch.summary.exists",
            detail=f"Missing batch summary: {summary_path}",
            summary={"artifact_family": None, "failure_count": 1, "run_count": 0},
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return _failed_report(
            report_id=f"{output_dir.name or 'unknown'}:validate_batch_directory",
            category="manifest",
            check_id="batch.manifest.json",
            detail=f"Batch manifest is not valid JSON: {manifest_path} ({exc.msg})",
            summary={"artifact_family": None, "failure_count": 1, "run_count": 0},
        )
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return _failed_report(
            report_id=f"{output_dir.name or 'unknown'}:validate_batch_directory",
            category="manifest",
            check_id="batch.summary.json",
            detail=f"Batch summary is not valid JSON: {summary_path} ({exc.msg})",
            summary={"artifact_family": manifest.get("artifact_family"), "failure_count": 1, "run_count": 0},
        )

    checks: list[ValidationCheck] = []
    checks.append(
        ValidationCheck(
            check_id="batch.artifact_family",
            status="passed" if manifest.get("artifact_family") == "avn_batch_run" else "failed",
            detail="Batch artifact family is avn_batch_run."
            if manifest.get("artifact_family") == "avn_batch_run"
            else f"Unexpected batch artifact family: {manifest.get('artifact_family')}",
            category="manifest",
        )
    )
    _required_keys(summary, REQUIRED_BATCH_SUMMARY_KEYS, check_id="batch.summary.schema", checks=checks)

    summary_path_matches = Path(manifest.get("summary_path", "")) == summary_path
    checks.append(
        ValidationCheck(
            check_id="batch.summary.path",
            status="passed" if summary_path_matches else "failed",
            detail="Batch manifest points at the local batch_summary.json."
            if summary_path_matches
            else f"Batch manifest summary_path does not match {summary_path}",
            category="manifest",
        )
    )

    run_records = manifest.get("runs", [])
    summary_runs = summary.get("runs", [])
    run_count_matches = summary.get("run_count") == len(run_records) == len(summary_runs)
    checks.append(
        ValidationCheck(
            check_id="batch.run_count",
            status="passed" if run_count_matches else "failed",
            detail="Batch run counts match across summary and manifest."
            if run_count_matches
            else "Batch run counts differ between summary and manifest.",
            category="consistency",
        )
    )

    for run_record in run_records:
        run_dir = Path(run_record["run_dir"])
        exists = run_dir.exists()
        run_id = f"{run_record['scenario_id']}.r{run_record['run_index']}"
        checks.append(
            ValidationCheck(
                check_id=f"batch.run.exists.{run_id}",
                status="passed" if exists else "failed",
                detail=f"Run directory exists at {run_dir}" if exists else f"Missing run directory: {run_dir}",
                category="manifest",
            )
        )
        manifest_exists = Path(run_record["artifact_manifest_path"]).exists()
        checks.append(
            ValidationCheck(
                check_id=f"batch.run.manifest.{run_id}",
                status="passed" if manifest_exists else "failed",
                detail="Run artifact manifest exists." if manifest_exists else f"Missing run manifest: {run_record['artifact_manifest_path']}",
                category="manifest",
            )
        )
        if not exists or not manifest_exists:
            continue
        nested_report = validate_run_directory(run_dir)
        checks.append(
            ValidationCheck(
                check_id=f"batch.run.validation.{run_id}",
                status="passed" if nested_report.status == "passed" else "failed",
                detail="Nested run validation passed."
                if nested_report.status == "passed"
                else f"Nested run validation failed: {nested_report.summary}",
                category="consistency",
            )
        )

    status = "passed" if all(check.status == "passed" for check in checks) else "failed"
    return ValidationReport(
        report_id=f"{manifest.get('batch_id', 'unknown')}:validate_batch_directory",
        contract_version=ARTIFACT_CONTRACT_VERSION,
        status=status,
        checks=checks,
        summary={
            "artifact_family": manifest.get("artifact_family"),
            "failure_count": sum(1 for check in checks if check.status != "passed"),
            "run_count": len(run_records),
        },
    )
