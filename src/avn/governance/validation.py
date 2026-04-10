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
    _required_keys(replay, {"scenario_id", "summary", "steps", "event_log"}, check_id="replay.schema", checks=checks)
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
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
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
