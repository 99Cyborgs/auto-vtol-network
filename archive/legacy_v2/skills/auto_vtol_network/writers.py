from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, Sequence

from .contracts import ArtifactManifestEntry, GovernedMetadata
from .enums import ArtifactType, EngineType


class ArtifactWriteError(RuntimeError):
    """Raised when additive or canonical writer rules are violated."""


def canonicalize_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {str(key): canonicalize_payload(val) for key, val in sorted(payload.items(), key=lambda item: str(item[0]))}
    if isinstance(payload, list):
        normalized = [canonicalize_payload(item) for item in payload]
        if all(isinstance(item, dict) and "id" in item for item in normalized):
            return sorted(normalized, key=lambda item: str(item["id"]))
        return normalized
    return payload


def payload_sha256(payload: Mapping[str, Any]) -> str:
    return sha256(
        json.dumps(canonicalize_payload(dict(payload)), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _contains_item(sequence: Sequence[Any], candidate: Any) -> bool:
    candidate_text = json.dumps(candidate, sort_keys=True, separators=(",", ":"))
    return any(json.dumps(item, sort_keys=True, separators=(",", ":")) == candidate_text for item in sequence)


def _ensure_additive(existing: Any, updated: Any, *, path: Path, scope: str = "$") -> None:
    if isinstance(existing, dict) and isinstance(updated, dict):
        missing_keys = sorted(set(existing) - set(updated))
        if missing_keys:
            raise ArtifactWriteError(f"{path} would drop keys at {scope}: {', '.join(missing_keys)}")
        for key in existing:
            _ensure_additive(existing[key], updated[key], path=path, scope=f"{scope}.{key}")
        return

    if isinstance(existing, list) and isinstance(updated, list):
        if all(isinstance(item, dict) and "id" in item for item in existing):
            existing_map = {str(item["id"]): item for item in existing}
            updated_map = {str(item["id"]): item for item in updated if isinstance(item, dict) and "id" in item}
            missing_ids = sorted(set(existing_map) - set(updated_map))
            if missing_ids:
                raise ArtifactWriteError(f"{path} would drop ids at {scope}: {', '.join(missing_ids)}")
            for record_id, existing_item in existing_map.items():
                _ensure_additive(existing_item, updated_map[record_id], path=path, scope=f"{scope}[{record_id}]")
            return
        missing_items = [item for item in existing if not _contains_item(updated, item)]
        if missing_items:
            raise ArtifactWriteError(f"{path} would drop list content at {scope}.")


def _artifact_payload(artifact: GovernedMetadata | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(artifact, Mapping):
        return canonicalize_payload(dict(artifact))
    return canonicalize_payload(artifact.to_dict())


def write_artifact(path: Path, artifact: GovernedMetadata | Mapping[str, Any]) -> Path:
    payload = _artifact_payload(artifact)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            existing = canonicalize_payload(json.load(handle))
        _ensure_additive(existing, payload, path=path)
        if existing == payload:
            return path
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def record_count(payload: Mapping[str, Any]) -> int:
    for key in ("records", "state_variables", "thresholds", "scenarios", "hazards", "stages", "decisions", "entries", "artifacts"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
    return 1


def build_manifest_entry(
    artifact: GovernedMetadata,
    *,
    artifact_type: ArtifactType,
    path: Path,
) -> ArtifactManifestEntry:
    payload = _artifact_payload(artifact)
    return ArtifactManifestEntry(
        id=f"manifest:{artifact.id}",
        type="artifact_manifest_entry",
        timestamp=artifact.timestamp,
        provenance=artifact.provenance,
        assumptions=artifact.assumptions,
        evidence_refs=artifact.evidence_refs,
        uncertainties=artifact.uncertainties,
        engine_tags=tuple(dict.fromkeys(artifact.engine_tags + (EngineType.DECISION_PROMOTION_SUMMARY,))),
        artifact_id=artifact.id,
        artifact_type=artifact_type,
        path=str(path.resolve()),
        sha256=payload_sha256(payload),
        record_count=record_count(payload),
    )
