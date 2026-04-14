from __future__ import annotations

from dataclasses import dataclass


DEFAULT_POLICY_ID = "balanced"


@dataclass(frozen=True, slots=True)
class PolicyProfile:
    policy_id: str
    label: str
    description: str
    weather_penalty_weight: float
    queue_penalty_weight: float
    occupancy_penalty_weight: float
    dispatch_weather_limit: float | None = None


POLICY_PROFILES: dict[str, PolicyProfile] = {
    "balanced": PolicyProfile(
        policy_id="balanced",
        label="Balanced Flow",
        description="Balances travel time against congestion and moderate disruption penalties.",
        weather_penalty_weight=20.0,
        queue_penalty_weight=3.0,
        occupancy_penalty_weight=1.0,
    ),
    "throughput_max": PolicyProfile(
        policy_id="throughput_max",
        label="Throughput Max",
        description="Prefers routes that shed queue pressure and corridor crowding, even if they are longer.",
        weather_penalty_weight=14.0,
        queue_penalty_weight=5.0,
        occupancy_penalty_weight=2.0,
    ),
    "disruption_avoidant": PolicyProfile(
        policy_id="disruption_avoidant",
        label="Disruption Avoidant",
        description="Avoids degraded weather corridors earlier and holds rather than dispatching into higher-risk conditions.",
        weather_penalty_weight=34.0,
        queue_penalty_weight=2.0,
        occupancy_penalty_weight=0.75,
        dispatch_weather_limit=0.45,
    ),
}


def get_policy_profile(policy_id: str | None) -> PolicyProfile:
    resolved = policy_id or DEFAULT_POLICY_ID
    try:
        return POLICY_PROFILES[resolved]
    except KeyError as exc:
        valid = ", ".join(sorted(POLICY_PROFILES))
        raise ValueError(f"Unknown policy_id '{resolved}'. Expected one of: {valid}.") from exc
