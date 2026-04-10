from __future__ import annotations

from enum import StrEnum


class EngineType(StrEnum):
    RESEARCH_INTAKE = "research_intake"
    ARCHITECTURE_ARTIFACT_GENERATION = "architecture_artifact_generation"
    PHYSICS_THRESHOLD_TRACKING = "physics_threshold_tracking"
    SCENARIO_STRESS_PLANNING = "scenario_stress_planning"
    SAFETY_HAZARD_LEDGER = "safety_hazard_ledger"
    DEPLOYMENT_READINESS_MAPPING = "deployment_readiness_mapping"
    DECISION_PROMOTION_SUMMARY = "decision_promotion_summary"


class ArtifactType(StrEnum):
    RESEARCH_INTAKE = "research_intake"
    ARCHITECTURE_SUMMARY = "architecture_summary"
    THRESHOLD_LEDGER = "threshold_ledger"
    SCENARIO_CATALOG = "scenario_catalog"
    HAZARD_LEDGER = "hazard_ledger"
    DEPLOYMENT_STAGE_PROFILE = "deployment_stage_profile"
    PROMOTION_DECISIONS = "promotion_decisions"
    ARTIFACT_MANIFEST = "artifact_manifest"
    RUN_RECEIPT = "run_receipt"


class ReadinessState(StrEnum):
    BLOCKED = "blocked"
    LIMITED = "limited"
    SIMULATION_READY = "simulation_ready"
    PROMOTABLE = "promotable"


class BlockerSeverity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DeploymentStage(StrEnum):
    RESEARCH_BASELINE = "research_baseline"
    ARCHITECTURE_BASELINE = "architecture_baseline"
    THRESHOLD_TRACKING = "threshold_tracking"
    INTEGRATED_SIMULATION = "integrated_simulation"
    SAFETY_REVIEW = "safety_review"
    GOVERNED_PILOT_PLANNING = "governed_pilot_planning"


class Comparator(StrEnum):
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="
