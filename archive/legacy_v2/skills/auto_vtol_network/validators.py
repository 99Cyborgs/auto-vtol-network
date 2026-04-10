from __future__ import annotations

from collections.abc import Iterable

from .contracts import (
    ArchitectureSummaryArtifact,
    ArtifactManifestArtifact,
    BlockerRecord,
    DeploymentStageProfileArtifact,
    DeploymentStageRecord,
    GovernedMetadata,
    HazardLedgerArtifact,
    HazardRecord,
    PromotionDecision,
    PromotionDecisionsArtifact,
    ResearchIntakeArtifact,
    ResearchIntakeRecord,
    RunReceipt,
    ScenarioCatalogArtifact,
    ScenarioDefinition,
    SkillPackRequest,
    StateVariableDefinition,
    ThresholdLedgerArtifact,
    ThresholdRecord,
)
from .enums import ArtifactType, BlockerSeverity, DeploymentStage, ReadinessState


class SkillPackValidationError(ValueError):
    """Raised when request or artifact validation fails."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SkillPackValidationError(message)


def _validate_common_metadata(model: GovernedMetadata) -> None:
    _require(bool(model.id), "Governed records and artifacts require a non-empty id.")
    _require(bool(model.timestamp), f"{model.id} is missing a timestamp.")
    _require(bool(model.provenance.generated_by), f"{model.id} is missing provenance.generated_by.")
    _require(bool(model.provenance.run_id), f"{model.id} is missing provenance.run_id.")
    _require(bool(model.provenance.sources), f"{model.id} is missing provenance.sources.")
    _require(bool(model.provenance.baselines), f"{model.id} is missing provenance.baselines.")
    _require(bool(model.engine_tags), f"{model.id} is missing engine tags.")


def _validate_record_collection(records: Iterable[GovernedMetadata]) -> None:
    seen: set[str] = set()
    for record in records:
        _validate_common_metadata(record)
        _require(record.id not in seen, f"Duplicate record id detected: {record.id}")
        seen.add(record.id)


def _validate_research_record(record: ResearchIntakeRecord) -> None:
    _validate_common_metadata(record)
    _require(bool(record.title), f"{record.id} is missing a title.")
    _require(bool(record.summary), f"{record.id} is missing a summary.")
    _require(bool(record.portfolio_ref), f"{record.id} is missing a portfolio reference.")


def _validate_state_variable(record: StateVariableDefinition) -> None:
    _validate_common_metadata(record)
    _require(bool(record.metric_key), f"{record.id} is missing a metric_key.")
    _require(bool(record.symbol), f"{record.id} is missing a symbol.")
    _require(bool(record.safe_operating_guidance), f"{record.id} is missing safe operating guidance.")


def _validate_threshold(record: ThresholdRecord, state_variables: dict[str, StateVariableDefinition]) -> None:
    _validate_common_metadata(record)
    _require(bool(record.metric_key), f"{record.id} is missing a metric linkage.")
    _require(record.variable_id in state_variables, f"{record.id} references unknown state variable {record.variable_id}.")
    linked_state = state_variables[record.variable_id]
    _require(
        linked_state.metric_key == record.metric_key,
        f"{record.id} metric linkage {record.metric_key} does not match {linked_state.id}:{linked_state.metric_key}.",
    )
    _require(bool(record.rationale), f"{record.id} is missing a rationale.")


def _validate_scenario(record: ScenarioDefinition, threshold_ids: set[str], hazard_ids: set[str]) -> None:
    _validate_common_metadata(record)
    _require(bool(record.success_criteria), f"{record.id} is missing success criteria.")
    _require(bool(record.failure_injections), f"{record.id} is missing failure injections.")
    for threshold_id in record.linked_threshold_ids:
        _require(threshold_id in threshold_ids, f"{record.id} references unknown threshold {threshold_id}.")
    for hazard_id in record.linked_hazard_ids:
        _require(hazard_id in hazard_ids, f"{record.id} references unknown hazard {hazard_id}.")


def _validate_hazard(record: HazardRecord, threshold_ids: set[str], scenario_ids: set[str]) -> None:
    _validate_common_metadata(record)
    _require(bool(record.mitigations), f"{record.id} is missing mitigation steps.")
    for threshold_id in record.linked_threshold_ids:
        _require(threshold_id in threshold_ids, f"{record.id} references unknown threshold {threshold_id}.")
    for scenario_id in record.linked_scenario_ids:
        _require(scenario_id in scenario_ids, f"{record.id} references unknown scenario {scenario_id}.")


def _validate_blocker(record: BlockerRecord) -> None:
    _validate_common_metadata(record)
    _require(bool(record.title), f"{record.id} is missing a blocker title.")
    _require(bool(record.description), f"{record.id} is missing a blocker description.")
    _require(bool(record.resolution_path), f"{record.id} is missing a resolution path.")
    _require(bool(record.affected_surfaces), f"{record.id} is missing affected surfaces.")


def _validate_stage(record: DeploymentStageRecord, blocker_ids: set[str]) -> None:
    _validate_common_metadata(record)
    _require(bool(record.blockers), f"{record.id} is missing blockers.")
    _require(bool(record.entry_criteria), f"{record.id} is missing entry criteria.")
    _require(bool(record.exit_criteria), f"{record.id} is missing exit criteria.")
    for blocker in record.blockers:
        _require(blocker.id in blocker_ids, f"{record.id} references unknown blocker {blocker.id}.")


def _validate_decision(record: PromotionDecision, blocker_ids: set[str]) -> None:
    _validate_common_metadata(record)
    _require(bool(record.subject_id), f"{record.id} is missing a subject_id.")
    _require(bool(record.decision), f"{record.id} is missing a decision.")
    _require(bool(record.rationale), f"{record.id} is missing rationale.")
    for blocker_id in record.blocker_ids:
        _require(blocker_id in blocker_ids, f"{record.id} references unknown blocker {blocker_id}.")


def validate_request(request: SkillPackRequest) -> None:
    _require(bool(request.run_id), "Request is missing run_id.")
    _require(bool(request.timestamp), "Request is missing timestamp.")
    _require(request.topology_model == "corridor_node", "Skill pack only supports corridor and node topology.")
    _require(request.governance_first, "Skill pack requires governance_first=true.")
    _require(
        request.simulation_scope == "thresholds_and_failure_regimes",
        "Simulation scope must remain thresholds_and_failure_regimes.",
    )
    _require(not request.free_flight_architecture, "Free flight architecture is out of scope.")
    _require(not request.passenger_autonomy_assumed, "Passenger autonomy assumptions are not allowed.")
    _require(
        set(request.service_priority[:2]) == {"cargo", "public_service"},
        "Service priority must place cargo and public_service first.",
    )

    for record in request.research_records:
        _validate_research_record(record)

    state_variables = {record.id: record for record in request.state_variables}
    for record in request.state_variables:
        _validate_state_variable(record)

    threshold_ids = {record.id for record in request.thresholds}
    for record in request.thresholds:
        _validate_threshold(record, state_variables)

    hazard_ids = {record.id for record in request.hazards}
    scenario_ids = {record.id for record in request.scenarios}
    blocker_ids = {record.id for record in request.blockers}

    for blocker in request.blockers:
        _validate_blocker(blocker)

    for scenario in request.scenarios:
        _validate_scenario(scenario, threshold_ids, hazard_ids)

    for hazard in request.hazards:
        _validate_hazard(hazard, threshold_ids, scenario_ids)

    for stage in request.deployment_stages:
        _validate_stage(stage, blocker_ids)


def validate_artifact(artifact: object) -> None:
    if isinstance(artifact, ResearchIntakeArtifact):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.RESEARCH_INTAKE, "Research intake artifact has an invalid type.")
        _validate_record_collection(artifact.records)
        for record in artifact.records:
            _validate_research_record(record)
        _require(artifact.source_count == len(artifact.records), "Research intake source_count is inconsistent.")
        return

    if isinstance(artifact, ArchitectureSummaryArtifact):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.ARCHITECTURE_SUMMARY, "Architecture artifact has an invalid type.")
        _require(artifact.topology_model == "corridor_node", "Architecture artifact broke corridor-node constraint.")
        _require(not artifact.free_flight_architecture, "Architecture artifact broke free-flight constraint.")
        _require(not artifact.passenger_autonomy_assumed, "Architecture artifact broke passenger-autonomy constraint.")
        for record in artifact.state_variables:
            _validate_state_variable(record)
        return

    if isinstance(artifact, ThresholdLedgerArtifact):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.THRESHOLD_LEDGER, "Threshold artifact has an invalid type.")
        state_variables = {record.id: record for record in artifact.state_variables}
        for record in artifact.state_variables:
            _validate_state_variable(record)
        for record in artifact.thresholds:
            _validate_threshold(record, state_variables)
        return

    if isinstance(artifact, ScenarioCatalogArtifact):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.SCENARIO_CATALOG, "Scenario artifact has an invalid type.")
        threshold_ids = set(artifact.covered_threshold_ids)
        hazard_ids = set(artifact.covered_hazard_ids)
        for record in artifact.scenarios:
            _validate_scenario(record, threshold_ids, hazard_ids)
        return

    if isinstance(artifact, HazardLedgerArtifact):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.HAZARD_LEDGER, "Hazard artifact has an invalid type.")
        threshold_ids = {threshold_id for hazard in artifact.hazards for threshold_id in hazard.linked_threshold_ids}
        scenario_ids = set(artifact.linked_scenario_ids)
        for record in artifact.hazards:
            _validate_hazard(record, threshold_ids, scenario_ids)
        return

    if isinstance(artifact, DeploymentStageProfileArtifact):
        _validate_common_metadata(artifact)
        _require(
            artifact.type is ArtifactType.DEPLOYMENT_STAGE_PROFILE,
            "Deployment stage profile artifact has an invalid type.",
        )
        blocker_ids = {record.id for record in artifact.blockers}
        _require(bool(blocker_ids), "Deployment stage profile must include blockers.")
        for blocker in artifact.blockers:
            _validate_blocker(blocker)
        for stage in artifact.stages:
            _validate_stage(stage, blocker_ids)
        return

    if isinstance(artifact, PromotionDecisionsArtifact):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.PROMOTION_DECISIONS, "Promotion artifact has an invalid type.")
        blocker_ids = {record.id for record in artifact.blockers}
        for blocker in artifact.blockers:
            _validate_blocker(blocker)
        for decision in artifact.decisions:
            _validate_decision(decision, blocker_ids)
        return

    if isinstance(artifact, ArtifactManifestArtifact):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.ARTIFACT_MANIFEST, "Artifact manifest has an invalid type.")
        for entry in artifact.entries:
            _validate_common_metadata(entry)
            _require(bool(entry.sha256), f"{entry.id} is missing a payload hash.")
            _require(entry.record_count >= 0, f"{entry.id} has an invalid record_count.")
        return

    if isinstance(artifact, RunReceipt):
        _validate_common_metadata(artifact)
        _require(artifact.type is ArtifactType.RUN_RECEIPT, "Run receipt has an invalid type.")
        _require(bool(artifact.run_id), "Run receipt is missing run_id.")
        _require(bool(artifact.selected_engines), "Run receipt is missing selected engines.")
        _require(bool(artifact.output_dir), "Run receipt is missing output_dir.")
        for entry in artifact.artifacts:
            _validate_common_metadata(entry)
        return

    raise SkillPackValidationError(f"Unsupported artifact type: {type(artifact)!r}")


def stage_order_key(stage: DeploymentStage) -> int:
    ordering = {
        DeploymentStage.RESEARCH_BASELINE: 0,
        DeploymentStage.ARCHITECTURE_BASELINE: 1,
        DeploymentStage.THRESHOLD_TRACKING: 2,
        DeploymentStage.INTEGRATED_SIMULATION: 3,
        DeploymentStage.SAFETY_REVIEW: 4,
        DeploymentStage.GOVERNED_PILOT_PLANNING: 5,
    }
    return ordering[stage]


def blocker_severity_rank(severity: BlockerSeverity) -> int:
    ordering = {
        BlockerSeverity.LOW: 0,
        BlockerSeverity.MEDIUM: 1,
        BlockerSeverity.HIGH: 2,
        BlockerSeverity.CRITICAL: 3,
    }
    return ordering[severity]


def readiness_rank(state: ReadinessState) -> int:
    ordering = {
        ReadinessState.BLOCKED: 0,
        ReadinessState.LIMITED: 1,
        ReadinessState.SIMULATION_READY: 2,
        ReadinessState.PROMOTABLE: 3,
    }
    return ordering[state]
