from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass, fields
from hashlib import sha256
from pathlib import Path
from typing import Any

from avn.core.state import ReplayBundle
from avn.governance.models import AdaptiveSweepResult, ArtifactManifest, ArtifactRecord, ValidationReport


ARTIFACT_CONTRACT_VERSION = 1


def _normalize(value: Any) -> Any:
    if is_dataclass(value):
        return {field.name: _normalize(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, dict):
        return {str(key): _normalize(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, tuple):
        return [_normalize(item) for item in value]
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    return value


def payload_sha256(payload: dict[str, Any]) -> str:
    canonical = json.dumps(_normalize(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(canonical).hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(_normalize(payload), handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def write_replay(path: Path, replay: ReplayBundle) -> Path:
    return write_json(path, replay.to_dict())


def _manifest_record(*, artifact_id: str, artifact_type: str, path: Path) -> ArtifactRecord:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ArtifactRecord(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        contract_version=ARTIFACT_CONTRACT_VERSION,
        path=str(path.resolve()),
        sha256=payload_sha256(payload),
    )


def write_run_artifacts(
    output_dir: Path,
    *,
    replay: ReplayBundle,
    summary: dict[str, Any],
    threshold_ledger: dict[str, Any],
    promotion_decisions: dict[str, Any],
    validation_report: ValidationReport,
) -> dict[str, Path]:
    replay_path = write_replay(output_dir / "replay.json", replay)
    summary_path = write_json(output_dir / "summary.json", summary)
    threshold_path = write_json(output_dir / "threshold_ledger.json", threshold_ledger)
    promotion_path = write_json(output_dir / "promotion_decisions.json", promotion_decisions)
    validation_path = write_json(output_dir / "validation_report.json", validation_report.to_dict())
    manifest = ArtifactManifest(
        manifest_id=f"{replay.scenario_id}:manifest",
        contract_version=ARTIFACT_CONTRACT_VERSION,
        artifact_family="avn_run",
        artifacts=[
            _manifest_record(artifact_id=f"{replay.scenario_id}:replay", artifact_type="replay", path=replay_path),
            _manifest_record(artifact_id=f"{replay.scenario_id}:summary", artifact_type="summary", path=summary_path),
            _manifest_record(
                artifact_id=f"{replay.scenario_id}:threshold_ledger",
                artifact_type="threshold_ledger",
                path=threshold_path,
            ),
            _manifest_record(
                artifact_id=f"{replay.scenario_id}:promotion_decisions",
                artifact_type="promotion_decisions",
                path=promotion_path,
            ),
            _manifest_record(
                artifact_id=f"{replay.scenario_id}:validation_report",
                artifact_type="validation_report",
                path=validation_path,
            ),
        ],
        metadata={"scenario_id": replay.scenario_id},
    )
    manifest_path = write_json(output_dir / "artifact_manifest.json", manifest.to_dict())
    return {
        "replay": replay_path,
        "summary": summary_path,
        "threshold_ledger": threshold_path,
        "promotion_decisions": promotion_path,
        "validation_report": validation_path,
        "artifact_manifest": manifest_path,
    }


def write_sweep_artifacts(output_dir: Path, sweep: AdaptiveSweepResult, validation_report: ValidationReport) -> dict[str, Path]:
    sweep_path = write_json(output_dir / "adaptive_sweep.json", sweep.to_dict())
    validation_path = write_json(output_dir / "validation_report.json", validation_report.to_dict())
    manifest = ArtifactManifest(
        manifest_id=f"{sweep.sweep_id}:manifest",
        contract_version=ARTIFACT_CONTRACT_VERSION,
        artifact_family="avn_adaptive_sweep",
        artifacts=[
            _manifest_record(artifact_id=f"{sweep.sweep_id}:adaptive_sweep", artifact_type="adaptive_sweep", path=sweep_path),
            _manifest_record(
                artifact_id=f"{sweep.sweep_id}:validation_report",
                artifact_type="validation_report",
                path=validation_path,
            ),
        ],
        metadata={"scenario_id": sweep.scenario_id, "metric_key": sweep.metric_key, "axis_path": sweep.axis_path},
    )
    manifest_path = write_json(output_dir / "artifact_manifest.json", manifest.to_dict())
    return {
        "adaptive_sweep": sweep_path,
        "validation_report": validation_path,
        "artifact_manifest": manifest_path,
    }
