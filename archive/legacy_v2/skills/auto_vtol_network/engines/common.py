from __future__ import annotations

from typing import Iterable

from ..contracts import SkillPackRequest, StateVariableDefinition
from ..enums import BlockerSeverity, EngineType


def unique_strings(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        ordered.append(value)
        seen.add(value)
    return tuple(ordered)


def default_state_variables(request: SkillPackRequest) -> tuple[StateVariableDefinition, ...]:
    payloads = (
        {
            "id": "state-rho-e",
            "name": "Corridor Density",
            "symbol": "rho_e",
            "metric_key": "rho_e",
            "unit": "density",
            "description": "Aggregate corridor density across the governed network.",
            "safe_operating_guidance": "Keep rho_e below congestion onset identified by threshold tracking.",
            "failure_regime_indicators": ["congestion", "safe_region_exit"],
        },
        {
            "id": "state-lambda-e",
            "name": "Flow to Capacity Ratio",
            "symbol": "lambda_e",
            "metric_key": "lambda_e",
            "unit": "ratio",
            "description": "Effective flow divided by available corridor capacity.",
            "safe_operating_guidance": "Keep lambda_e within bounded tranche support before queue spillback.",
            "failure_regime_indicators": ["capacity_saturation", "queue_growth"],
        },
        {
            "id": "state-gamma-e",
            "name": "Communications Effectiveness",
            "symbol": "gamma_e",
            "metric_key": "gamma_e",
            "unit": "fraction",
            "description": "Effective communication freshness and reliability proxy.",
            "safe_operating_guidance": "Maintain gamma_e above degraded-mode trigger thresholds.",
            "failure_regime_indicators": ["lost_link", "stale_state"],
        },
        {
            "id": "state-chi-e",
            "name": "Trust Degradation Burden",
            "symbol": "chi_e",
            "metric_key": "chi_e",
            "unit": "fraction",
            "description": "Weighted trust degradation burden over active vehicles.",
            "safe_operating_guidance": "Keep chi_e bounded below trust quarantine escalation thresholds.",
            "failure_regime_indicators": ["trust_collapse", "supplier_quarantine"],
        },
        {
            "id": "state-queue-e",
            "name": "Contingency Queue Ratio",
            "symbol": "queue_e",
            "metric_key": "queue_e",
            "unit": "ratio",
            "description": "Queue pressure against contingency handling capacity.",
            "safe_operating_guidance": "Keep queue_e below sustained contingency saturation limits.",
            "failure_regime_indicators": ["landing_queue_overflow", "service_backlog"],
        },
        {
            "id": "state-reserve-e",
            "name": "Reserve Margin",
            "symbol": "reserve_e",
            "metric_key": "reserve_e",
            "unit": "energy_margin",
            "description": "Minimum reserve margin available to active vehicles.",
            "safe_operating_guidance": "Keep reserve_e above diversion and contingency minima.",
            "failure_regime_indicators": ["reserve_shortfall", "forced_diversion"],
        },
        {
            "id": "state-kappa-i",
            "name": "Available Contingency Supply",
            "symbol": "kappa_i",
            "metric_key": "kappa_i",
            "unit": "slots",
            "description": "Total remaining contingency landing supply across nodes.",
            "safe_operating_guidance": "Prevent kappa_i collapse in cargo and public service corridors.",
            "failure_regime_indicators": ["contingency_exhaustion"],
        },
        {
            "id": "state-demand-diverts",
            "name": "Demand Diverts",
            "symbol": "demand_diverts",
            "metric_key": "demand_diverts",
            "unit": "vehicles",
            "description": "Vehicles requiring diversion or holding due to degraded admissibility.",
            "safe_operating_guidance": "Bound diverted demand before corridor spillover destabilizes node service.",
            "failure_regime_indicators": ["reroute_pressure", "holding_pattern_growth"],
        },
    )
    return tuple(
        StateVariableDefinition.from_dict(
            payload,
            timestamp=request.timestamp,
            provenance=request.provenance,
            assumptions=request.assumptions,
            evidence_refs=request.evidence_refs,
            uncertainties=request.uncertainties,
            engine_tag=EngineType.ARCHITECTURE_ARTIFACT_GENERATION,
        )
        for payload in payloads
    )


def max_blocker_severity(stage_blockers) -> BlockerSeverity:
    return max((blocker.severity for blocker in stage_blockers), default=BlockerSeverity.LOW, key=_severity_rank)


def _severity_rank(severity: BlockerSeverity) -> int:
    ordering = {
        BlockerSeverity.LOW: 0,
        BlockerSeverity.MEDIUM: 1,
        BlockerSeverity.HIGH: 2,
        BlockerSeverity.CRITICAL: 3,
    }
    return ordering[severity]
