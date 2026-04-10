from __future__ import annotations

from avn.core.state import ReplayBundle, ScenarioDefinition
from avn.governance.models import PromotionDecision, PromotionDecisionSet, ThresholdEvaluation, ThresholdLedger


THRESHOLD_TO_METRIC = {
    "queue_pressure": "max_queue_length",
    "corridor_load_ratio": "max_corridor_load_ratio",
    "low_reserve": "min_reserve_energy",
}


def build_threshold_ledger(replay: ReplayBundle, scenario: ScenarioDefinition) -> ThresholdLedger:
    evaluations: list[ThresholdEvaluation] = []
    for threshold_key, target in sorted(scenario.alert_thresholds.items()):
        metric_key = THRESHOLD_TO_METRIC.get(threshold_key)
        if metric_key is None or metric_key not in replay.summary:
            continue
        observed = float(replay.summary[metric_key])
        target_value = float(target)
        status = "passed" if observed <= target_value else "failed"
        evaluations.append(
            ThresholdEvaluation(
                threshold_id=f"{scenario.scenario_id}:{threshold_key}",
                metric_key=metric_key,
                comparator="<=",
                target_value=target_value,
                observed_value=observed,
                status=status,
                evidence={
                    "threshold_key": threshold_key,
                    "alert_count": replay.summary.get("alerts_by_code", {}),
                },
            )
        )

    passed = sum(1 for evaluation in evaluations if evaluation.status == "passed")
    failed = len(evaluations) - passed
    return ThresholdLedger(
        ledger_id=f"{scenario.scenario_id}:threshold_ledger",
        contract_version=1,
        scenario_id=scenario.scenario_id,
        evaluations=evaluations,
        summary={
            "threshold_count": len(evaluations),
            "passed_count": passed,
            "failed_count": failed,
            "release_status": "allow" if failed == 0 else "blocked",
        },
    )


def build_promotion_decisions(ledger: ThresholdLedger) -> PromotionDecisionSet:
    decisions = [
        PromotionDecision(
            decision_id=f"{evaluation.threshold_id}:promotion",
            threshold_id=evaluation.threshold_id,
            status="allow" if evaluation.status == "passed" else "blocked",
            rationale=(
                f"{evaluation.metric_key} observed {evaluation.observed_value:.3f} {evaluation.comparator} "
                f"target {evaluation.target_value:.3f}"
            ),
        )
        for evaluation in ledger.evaluations
    ]
    blocked = sum(1 for decision in decisions if decision.status == "blocked")
    release_status = "allow" if blocked == 0 else "blocked"
    return PromotionDecisionSet(
        artifact_id=f"{ledger.scenario_id}:promotion_decisions",
        contract_version=1,
        scenario_id=ledger.scenario_id,
        release_status=release_status,
        decisions=decisions,
        summary={
            "decision_count": len(decisions),
            "blocked_count": blocked,
            "allowed_count": len(decisions) - blocked,
        },
    )
