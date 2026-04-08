from __future__ import annotations

from typing import Mapping

from ..contracts import (
    DeploymentStageProfileArtifact,
    GovernedMetadata,
    HazardLedgerArtifact,
    PromotionDecision,
    PromotionDecisionsArtifact,
    ScenarioCatalogArtifact,
    SkillPackRequest,
    ThresholdLedgerArtifact,
)
from ..enums import ArtifactType, BlockerSeverity, DeploymentStage, EngineType, ReadinessState
from ..validators import blocker_severity_rank, stage_order_key
from .common import unique_strings


def build_promotion_decisions(
    request: SkillPackRequest,
    artifacts: Mapping[ArtifactType, GovernedMetadata],
) -> PromotionDecisionsArtifact:
    deployment = artifacts.get(ArtifactType.DEPLOYMENT_STAGE_PROFILE)
    scenario = artifacts.get(ArtifactType.SCENARIO_CATALOG)
    threshold = artifacts.get(ArtifactType.THRESHOLD_LEDGER)
    hazard = artifacts.get(ArtifactType.HAZARD_LEDGER)

    stages = request.deployment_stages if not isinstance(deployment, DeploymentStageProfileArtifact) else deployment.stages
    blockers = request.blockers if not isinstance(deployment, DeploymentStageProfileArtifact) else deployment.blockers

    threshold_ids = set()
    if isinstance(threshold, ThresholdLedgerArtifact):
        threshold_ids = {record.id for record in threshold.thresholds}

    scenario_threshold_ids = set()
    if isinstance(scenario, ScenarioCatalogArtifact):
        scenario_threshold_ids = set(scenario.covered_threshold_ids)

    hazard_threshold_ids = set()
    if isinstance(hazard, HazardLedgerArtifact):
        hazard_threshold_ids = {
            threshold_id
            for record in hazard.hazards
            for threshold_id in record.linked_threshold_ids
        }

    uncovered_thresholds = tuple(sorted(threshold_ids - (scenario_threshold_ids | hazard_threshold_ids)))
    decisions = []
    for stage in sorted(stages, key=lambda record: stage_order_key(record.stage)):
        blocker_ids = tuple(sorted({blocker.id for blocker in stage.blockers}))
        max_severity = max(
            (blocker.severity for blocker in stage.blockers),
            default=BlockerSeverity.LOW,
            key=blocker_severity_rank,
        )

        rationale = [
            f"Stage {stage.stage.value} is assessed as {stage.readiness_state.value}.",
            f"Maximum blocker severity is {max_severity.value}.",
        ]
        required_actions = list(request.decision_context)
        approved = stage.readiness_state in {ReadinessState.SIMULATION_READY, ReadinessState.PROMOTABLE}

        if blocker_severity_rank(max_severity) >= blocker_severity_rank(BlockerSeverity.HIGH):
            approved = False
            required_actions.append("Resolve high-severity blockers before promotion.")

        if uncovered_thresholds:
            approved = False
            rationale.append(f"Threshold coverage is incomplete for {', '.join(uncovered_thresholds)}.")
            required_actions.append("Cover all tracked thresholds with scenarios or hazards.")

        if stage.stage is DeploymentStage.GOVERNED_PILOT_PLANNING and blocker_ids:
            approved = False
            rationale.append("Governed pilot planning remains blocked until active blockers are retired.")
            required_actions.append("Retire all active blockers before governed pilot planning.")

        decision_text = "promote" if approved else "hold"
        if approved:
            rationale.append("Promotion is limited to governed simulation and planning scope.")
        else:
            rationale.append("Governance constraints require additional evidence before promotion.")

        decisions.append(
            PromotionDecision(
                id=f"decision:{stage.id}",
                type="promotion_decision",
                timestamp=request.timestamp,
                provenance=request.provenance,
                assumptions=request.assumptions,
                evidence_refs=request.evidence_refs,
                uncertainties=request.uncertainties,
                engine_tags=(EngineType.DECISION_PROMOTION_SUMMARY,),
                subject_id=stage.id,
                subject_type="deployment_stage",
                target_stage=stage.stage,
                approved=approved,
                decision=decision_text,
                readiness_state=stage.readiness_state,
                blocker_ids=blocker_ids,
                rationale=tuple(rationale),
                required_actions=unique_strings(required_actions),
            )
        )

    approved_stage_ids = tuple(decision.subject_id for decision in decisions if decision.approved)
    held_stage_ids = tuple(decision.subject_id for decision in decisions if not decision.approved)
    return PromotionDecisionsArtifact(
        id=f"{request.run_id}:{ArtifactType.PROMOTION_DECISIONS.value}",
        type=ArtifactType.PROMOTION_DECISIONS,
        timestamp=request.timestamp,
        provenance=request.provenance,
        assumptions=request.assumptions,
        evidence_refs=request.evidence_refs,
        uncertainties=request.uncertainties,
        engine_tags=(EngineType.DECISION_PROMOTION_SUMMARY,),
        decisions=tuple(decisions),
        blockers=tuple(sorted(blockers, key=lambda record: record.id)),
        approved_stage_ids=approved_stage_ids,
        held_stage_ids=held_stage_ids,
    )
