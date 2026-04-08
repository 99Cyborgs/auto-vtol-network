from __future__ import annotations

from typing import Mapping

from ..contracts import ArchitectureSummaryArtifact, GovernedMetadata, SkillPackRequest
from ..enums import ArtifactType, EngineType
from .common import default_state_variables, unique_strings


def build_architecture_summary(
    request: SkillPackRequest,
    artifacts: Mapping[ArtifactType, GovernedMetadata],
) -> ArchitectureSummaryArtifact:
    research_artifact = artifacts.get(ArtifactType.RESEARCH_INTAKE)
    constraint_links = ()
    research_driver_summary = ()
    if research_artifact is not None and hasattr(research_artifact, "records"):
        constraint_links = tuple(record.id for record in research_artifact.records)
        research_driver_summary = unique_strings(
            driver
            for record in research_artifact.records
            for driver in getattr(record, "architecture_drivers", ())
        )
    state_variables = request.state_variables or default_state_variables(request)
    return ArchitectureSummaryArtifact(
        id=f"{request.run_id}:{ArtifactType.ARCHITECTURE_SUMMARY.value}",
        type=ArtifactType.ARCHITECTURE_SUMMARY,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.ARCHITECTURE_ARTIFACT_GENERATION,),
        network_name=request.network_name,
        topology_model=request.topology_model,
        service_priority=request.service_priority,
        node_roles=request.node_roles,
        corridor_policies=request.corridor_policies,
        governance_controls=request.governance_controls,
        governance_first=request.governance_first,
        simulation_scope=request.simulation_scope,
        free_flight_architecture=request.free_flight_architecture,
        passenger_autonomy_assumed=request.passenger_autonomy_assumed,
        state_variables=tuple(sorted(state_variables, key=lambda record: record.id)),
        constraint_links=constraint_links,
        research_driver_summary=research_driver_summary,
    )
