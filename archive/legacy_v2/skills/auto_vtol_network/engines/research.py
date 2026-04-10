from __future__ import annotations

from typing import Mapping

from ..contracts import GovernedMetadata, ResearchIntakeArtifact, SkillPackRequest
from ..enums import ArtifactType, EngineType
from .common import unique_strings


def build_research_intake(
    request: SkillPackRequest,
    _: Mapping[ArtifactType, GovernedMetadata],
) -> ResearchIntakeArtifact:
    records = tuple(sorted(request.research_records, key=lambda record: record.id))
    constraint_summary = unique_strings(
        constraint for record in records for constraint in record.constraints
    )
    architecture_driver_summary = unique_strings(
        driver for record in records for driver in record.architecture_drivers
    )
    open_question_count = sum(len(record.open_questions) for record in records)
    return ResearchIntakeArtifact(
        id=f"{request.run_id}:{ArtifactType.RESEARCH_INTAKE.value}",
        type=ArtifactType.RESEARCH_INTAKE,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.RESEARCH_INTAKE,),
        records=records,
        constraint_summary=constraint_summary,
        architecture_driver_summary=architecture_driver_summary,
        source_count=len(records),
        open_question_count=open_question_count,
    )
