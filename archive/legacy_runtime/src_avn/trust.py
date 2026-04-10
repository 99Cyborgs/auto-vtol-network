from __future__ import annotations

from avn.core.models import TRUST_STATES


TRUST_PRIORITY = {
    "trusted": 4,
    "degraded": 3,
    "unknown": 2,
    "quarantined": 1,
    "revoked": 0,
}


def normalize_trust_state(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in TRUST_STATES:
        return "unknown"
    return normalized


def trust_service_priority(trust_state: str) -> int:
    return TRUST_PRIORITY.get(normalize_trust_state(trust_state), 0)


def can_file_intent(trust_state: str) -> bool:
    state = normalize_trust_state(trust_state)
    return state in {"trusted", "degraded", "unknown"}


def can_participate_in_cooperative_flow(trust_state: str) -> bool:
    state = normalize_trust_state(trust_state)
    return state in {"trusted", "degraded"}


def can_receive_reservation(trust_state: str, *, degraded_mode: bool) -> bool:
    state = normalize_trust_state(trust_state)
    if state == "trusted":
        return True
    if state == "degraded":
        return True
    if state == "unknown":
        return not degraded_mode
    return False


def requires_operator_override(trust_state: str, *, degraded_mode: bool) -> bool:
    state = normalize_trust_state(trust_state)
    return state in {"degraded", "unknown"} or degraded_mode


def is_quarantined(trust_state: str) -> bool:
    return normalize_trust_state(trust_state) in {"quarantined", "revoked"}
