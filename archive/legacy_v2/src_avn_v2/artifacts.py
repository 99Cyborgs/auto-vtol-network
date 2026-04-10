from __future__ import annotations

import json
from dataclasses import is_dataclass, fields
from hashlib import sha256
from pathlib import Path
from typing import Any


V2_CONTRACT_VERSION = 4


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
        normalized = [_normalize(item) for item in value]
        if all(isinstance(item, dict) and "id" in item for item in normalized):
            return sorted(normalized, key=lambda item: str(item["id"]))
        return normalized
    return value


def payload_sha256(payload: dict[str, Any]) -> str:
    canonical = json.dumps(_normalize(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(canonical).hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize(payload)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def manifest_entry(*, artifact_id: str, artifact_type: str, path: Path, contract_version: int) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "id": f"manifest:{artifact_id}",
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "contract_version": contract_version,
        "path": str(path.resolve()),
        "sha256": payload_sha256(payload),
    }
