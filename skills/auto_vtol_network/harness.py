from __future__ import annotations

import json
from pathlib import Path

from .contracts import ArtifactManifestArtifact, GovernedMetadata, RunReceipt, SkillPackRequest
from .enums import ArtifactType, EngineType
from .registry import ENGINE_REGISTRY, resolve_engine_sequence
from .v2_adapter import build_request_from_v2_bundle
from .validators import validate_artifact, validate_request
from .writers import build_manifest_entry, write_artifact


def load_request(path: Path) -> SkillPackRequest:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return SkillPackRequest.from_dict(payload)


def load_v2_request(path: Path) -> SkillPackRequest:
    return build_request_from_v2_bundle(path)


def _build_manifest_artifact(
    request: SkillPackRequest,
    entries,
) -> ArtifactManifestArtifact:
    return ArtifactManifestArtifact(
        id=f"{request.run_id}:{ArtifactType.ARTIFACT_MANIFEST.value}",
        type=ArtifactType.ARTIFACT_MANIFEST,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.DECISION_PROMOTION_SUMMARY,),
        entries=tuple(entries),
    )


def _build_run_receipt(
    request: SkillPackRequest,
    *,
    selected_engines: tuple[EngineType, ...],
    entries,
    output_dir: Path,
) -> RunReceipt:
    return RunReceipt(
        id=f"{request.run_id}:{ArtifactType.RUN_RECEIPT.value}",
        type=ArtifactType.RUN_RECEIPT,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.DECISION_PROMOTION_SUMMARY,),
        run_id=request.run_id,
        status="completed",
        selected_engines=selected_engines,
        artifacts=tuple(entries),
        output_dir=str(output_dir.resolve()),
    )


def run_skill_pack(
    request: SkillPackRequest,
    *,
    output_dir: Path,
    selected_engines: tuple[EngineType, ...] | None = None,
) -> RunReceipt:
    validate_request(request)
    ordered_engines = resolve_engine_sequence(selected_engines or request.selected_engines)
    output_dir.mkdir(parents=True, exist_ok=True)

    artifacts: dict[ArtifactType, GovernedMetadata] = {}
    manifest_entries = []

    for engine in ordered_engines:
        registration = ENGINE_REGISTRY[engine]
        artifact = registration.builder(request, artifacts)
        validate_artifact(artifact)
        artifact_path = write_artifact(output_dir / registration.filename, artifact)
        manifest_entries.append(
            build_manifest_entry(
                artifact,
                artifact_type=registration.artifact_type,
                path=artifact_path,
            )
        )
        artifacts[registration.artifact_type] = artifact

    manifest = _build_manifest_artifact(request, manifest_entries)
    validate_artifact(manifest)
    write_artifact(output_dir / "artifact_manifest.json", manifest)

    receipt = _build_run_receipt(
        request,
        selected_engines=ordered_engines,
        entries=manifest_entries,
        output_dir=output_dir,
    )
    validate_artifact(receipt)
    write_artifact(output_dir / "run_receipt.json", receipt)
    return receipt
