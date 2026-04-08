from __future__ import annotations

from typing import Mapping

from ..contracts import (
    ArchitectureSummaryArtifact,
    GovernedMetadata,
    SkillPackRequest,
    ThresholdLedgerArtifact,
)
from ..enums import ArtifactType, EngineType
from .common import default_state_variables, unique_strings


def build_threshold_ledger(
    request: SkillPackRequest,
    artifacts: Mapping[ArtifactType, GovernedMetadata],
) -> ThresholdLedgerArtifact:
    architecture = artifacts.get(ArtifactType.ARCHITECTURE_SUMMARY)
    if isinstance(architecture, ArchitectureSummaryArtifact):
        state_variables = architecture.state_variables
    else:
        state_variables = request.state_variables or default_state_variables(request)
    thresholds = tuple(sorted(request.thresholds, key=lambda record: record.id))
    tracked_metric_keys = unique_strings(record.metric_key for record in thresholds)
    failure_regime_focus = unique_strings(
        indicator
        for record in state_variables
        for indicator in record.failure_regime_indicators
    )
    return ThresholdLedgerArtifact(
        id=f"{request.run_id}:{ArtifactType.THRESHOLD_LEDGER.value}",
        type=ArtifactType.THRESHOLD_LEDGER,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.PHYSICS_THRESHOLD_TRACKING,),
        state_variables=tuple(sorted(state_variables, key=lambda record: record.id)),
        thresholds=thresholds,
        tracked_metric_keys=tracked_metric_keys,
        failure_regime_focus=failure_regime_focus,
    )
