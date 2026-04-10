from __future__ import annotations

from typing import Mapping

from ..contracts import GovernedMetadata, ScenarioCatalogArtifact, SkillPackRequest
from ..enums import ArtifactType, EngineType
from .common import unique_strings


def build_scenario_catalog(
    request: SkillPackRequest,
    _: Mapping[ArtifactType, GovernedMetadata],
) -> ScenarioCatalogArtifact:
    scenarios = tuple(sorted(request.scenarios, key=lambda record: record.id))
    covered_threshold_ids = unique_strings(
        threshold_id
        for record in scenarios
        for threshold_id in record.linked_threshold_ids
    )
    covered_hazard_ids = unique_strings(
        hazard_id
        for record in scenarios
        for hazard_id in record.linked_hazard_ids
    )
    covered_metric_keys = unique_strings(
        metric.metric_key
        for record in scenarios
        for metric in record.metric_specs
    )
    return ScenarioCatalogArtifact(
        id=f"{request.run_id}:{ArtifactType.SCENARIO_CATALOG.value}",
        type=ArtifactType.SCENARIO_CATALOG,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.SCENARIO_STRESS_PLANNING,),
        scenarios=scenarios,
        covered_threshold_ids=covered_threshold_ids,
        covered_hazard_ids=covered_hazard_ids,
        covered_metric_keys=covered_metric_keys,
    )
