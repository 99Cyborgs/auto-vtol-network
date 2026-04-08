from __future__ import annotations

from typing import Mapping

from ..contracts import DeploymentStageProfileArtifact, GovernedMetadata, SkillPackRequest
from ..enums import ArtifactType, EngineType, ReadinessState
from ..validators import readiness_rank, stage_order_key


def build_deployment_stage_profile(
    request: SkillPackRequest,
    _: Mapping[ArtifactType, GovernedMetadata],
) -> DeploymentStageProfileArtifact:
    stages = tuple(sorted(request.deployment_stages, key=lambda record: stage_order_key(record.stage)))
    blockers = tuple(sorted(request.blockers, key=lambda record: record.id))
    highest_stage = ""
    highest_state = ReadinessState.BLOCKED
    for stage in stages:
        if readiness_rank(stage.readiness_state) >= readiness_rank(highest_state):
            highest_stage = stage.stage.value
            highest_state = stage.readiness_state
    return DeploymentStageProfileArtifact(
        id=f"{request.run_id}:{ArtifactType.DEPLOYMENT_STAGE_PROFILE.value}",
        type=ArtifactType.DEPLOYMENT_STAGE_PROFILE,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.DEPLOYMENT_READINESS_MAPPING,),
        stages=stages,
        blockers=blockers,
        highest_ready_stage=highest_stage,
        highest_readiness_state=highest_state,
    )
