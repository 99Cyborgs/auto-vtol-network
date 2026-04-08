from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping

from .contracts import GovernedMetadata, SkillPackRequest
from .engines import (
    build_architecture_summary,
    build_deployment_stage_profile,
    build_hazard_ledger,
    build_promotion_decisions,
    build_research_intake,
    build_scenario_catalog,
    build_threshold_ledger,
)
from .enums import ArtifactType, EngineType


@dataclass(slots=True, frozen=True)
class EngineRegistration:
    engine: EngineType
    artifact_type: ArtifactType
    filename: str
    builder: Callable[[SkillPackRequest, Mapping[ArtifactType, GovernedMetadata]], GovernedMetadata]


ENGINE_SEQUENCE: tuple[EngineType, ...] = (
    EngineType.RESEARCH_INTAKE,
    EngineType.ARCHITECTURE_ARTIFACT_GENERATION,
    EngineType.PHYSICS_THRESHOLD_TRACKING,
    EngineType.SCENARIO_STRESS_PLANNING,
    EngineType.SAFETY_HAZARD_LEDGER,
    EngineType.DEPLOYMENT_READINESS_MAPPING,
    EngineType.DECISION_PROMOTION_SUMMARY,
)


ENGINE_REGISTRY: dict[EngineType, EngineRegistration] = {
    EngineType.RESEARCH_INTAKE: EngineRegistration(
        engine=EngineType.RESEARCH_INTAKE,
        artifact_type=ArtifactType.RESEARCH_INTAKE,
        filename="research_intake.json",
        builder=build_research_intake,
    ),
    EngineType.ARCHITECTURE_ARTIFACT_GENERATION: EngineRegistration(
        engine=EngineType.ARCHITECTURE_ARTIFACT_GENERATION,
        artifact_type=ArtifactType.ARCHITECTURE_SUMMARY,
        filename="architecture_summary.json",
        builder=build_architecture_summary,
    ),
    EngineType.PHYSICS_THRESHOLD_TRACKING: EngineRegistration(
        engine=EngineType.PHYSICS_THRESHOLD_TRACKING,
        artifact_type=ArtifactType.THRESHOLD_LEDGER,
        filename="threshold_ledger.json",
        builder=build_threshold_ledger,
    ),
    EngineType.SCENARIO_STRESS_PLANNING: EngineRegistration(
        engine=EngineType.SCENARIO_STRESS_PLANNING,
        artifact_type=ArtifactType.SCENARIO_CATALOG,
        filename="scenario_catalog.json",
        builder=build_scenario_catalog,
    ),
    EngineType.SAFETY_HAZARD_LEDGER: EngineRegistration(
        engine=EngineType.SAFETY_HAZARD_LEDGER,
        artifact_type=ArtifactType.HAZARD_LEDGER,
        filename="hazard_ledger.json",
        builder=build_hazard_ledger,
    ),
    EngineType.DEPLOYMENT_READINESS_MAPPING: EngineRegistration(
        engine=EngineType.DEPLOYMENT_READINESS_MAPPING,
        artifact_type=ArtifactType.DEPLOYMENT_STAGE_PROFILE,
        filename="deployment_stage_profile.json",
        builder=build_deployment_stage_profile,
    ),
    EngineType.DECISION_PROMOTION_SUMMARY: EngineRegistration(
        engine=EngineType.DECISION_PROMOTION_SUMMARY,
        artifact_type=ArtifactType.PROMOTION_DECISIONS,
        filename="promotion_decisions.json",
        builder=build_promotion_decisions,
    ),
}


def resolve_engine_sequence(selected: tuple[EngineType, ...] | None) -> tuple[EngineType, ...]:
    requested = ENGINE_SEQUENCE if not selected else tuple(dict.fromkeys(selected))
    return tuple(engine for engine in ENGINE_SEQUENCE if engine in requested)
