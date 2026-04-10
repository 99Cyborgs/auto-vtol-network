from __future__ import annotations

from typing import Mapping

from ..contracts import GovernedMetadata, HazardLedgerArtifact, SkillPackRequest
from ..enums import ArtifactType, BlockerSeverity, EngineType
from .common import unique_strings


def build_hazard_ledger(
    request: SkillPackRequest,
    _: Mapping[ArtifactType, GovernedMetadata],
) -> HazardLedgerArtifact:
    hazards = tuple(sorted(request.hazards, key=lambda record: record.id))
    high_severity_hazard_ids = tuple(
        record.id
        for record in hazards
        if record.severity in {BlockerSeverity.HIGH, BlockerSeverity.CRITICAL}
    )
    linked_scenario_ids = unique_strings(
        scenario_id
        for record in hazards
        for scenario_id in record.linked_scenario_ids
    )
    return HazardLedgerArtifact(
        id=f"{request.run_id}:{ArtifactType.HAZARD_LEDGER.value}",
        type=ArtifactType.HAZARD_LEDGER,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.SAFETY_HAZARD_LEDGER,),
        hazards=hazards,
        high_severity_hazard_ids=high_severity_hazard_ids,
        linked_scenario_ids=linked_scenario_ids,
    )
