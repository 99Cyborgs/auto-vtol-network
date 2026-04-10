from __future__ import annotations

from typing import Any

from .models import GovernancePolicy


CONFIDENCE_ORDER = {
    "missing": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


def default_governance_policy() -> GovernancePolicy:
    return GovernancePolicy()


def evaluate_governance_policy(
    grouped_blockers: dict[str, list[str]],
    *,
    confidence_tier: str,
    policy: GovernancePolicy | None,
) -> dict[str, Any]:
    active_policy = policy or default_governance_policy()
    effective_blockers = {
        key: list(dict.fromkeys(values))
        for key, values in grouped_blockers.items()
    }
    if CONFIDENCE_ORDER.get(confidence_tier, 0) < CONFIDENCE_ORDER.get(active_policy.minimum_confidence_tier, 0):
        effective_blockers.setdefault("evidence_insufficiency", [])
        effective_blockers["evidence_insufficiency"].append("confidence_below_policy_minimum")
        effective_blockers["evidence_insufficiency"] = list(dict.fromkeys(effective_blockers["evidence_insufficiency"]))

    waiver_records = []
    waived_blockers = []
    remaining_blockers = {key: list(values) for key, values in effective_blockers.items()}
    for waiver in active_policy.waivers:
        blockers = remaining_blockers.get(waiver.category, [])
        if not blockers:
            continue
        if waiver.category not in active_policy.waivable_categories:
            continue
        waived_blockers.extend(
            {
                "category": waiver.category,
                "blocker_id": blocker_id,
                "justification_id": waiver.justification_id,
            }
            for blocker_id in blockers
        )
        waiver_records.append(
            {
                "category": waiver.category,
                "justification_id": waiver.justification_id,
                "applied": True,
            }
        )
        remaining_blockers[waiver.category] = []

    fatal_blockers = []
    advisory_blockers = []
    for category, blocker_ids in remaining_blockers.items():
        if category in active_policy.advisory_blocker_categories and category not in active_policy.fatal_blocker_categories:
            advisory_blockers.extend({"category": category, "blocker_id": blocker_id} for blocker_id in blocker_ids)
            continue
        fatal_blockers.extend({"category": category, "blocker_id": blocker_id} for blocker_id in blocker_ids)

    policy_eligible = not fatal_blockers
    return {
        "policy": {
            "minimum_confidence_tier": active_policy.minimum_confidence_tier,
            "fatal_blocker_categories": list(active_policy.fatal_blocker_categories),
            "advisory_blocker_categories": list(active_policy.advisory_blocker_categories),
            "waivable_categories": list(active_policy.waivable_categories),
        },
        "observed_confidence_tier": confidence_tier,
        "policy_eligible": policy_eligible,
        "fatal_blockers": fatal_blockers,
        "advisory_blockers": advisory_blockers,
        "waived_blockers": waived_blockers,
        "waiver_status": {
            "applied_waivers": waiver_records,
            "waived_categories": sorted({item["category"] for item in waived_blockers}),
        },
    }


def contradiction_policy_metadata(category: str, *, policy: GovernancePolicy | None) -> dict[str, Any]:
    active_policy = policy or default_governance_policy()
    if category in active_policy.advisory_blocker_categories and category not in active_policy.fatal_blocker_categories:
        severity = "warning"
        policy_effect = "advisory"
    else:
        severity = "error"
        policy_effect = "fatal"
    return {
        "severity": severity,
        "policy_effect": policy_effect,
        "waivable": category in active_policy.waivable_categories,
    }
