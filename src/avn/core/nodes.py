from __future__ import annotations

from avn.core.state import NodeCondition, NodeRuntime


def accrue_service_credit(node: NodeRuntime, condition: NodeCondition, time_step_minutes: int) -> int:
    base_rate = node.definition.service_rate_per_hour
    multiplier = condition.service_multiplier
    if condition.status == "closed":
        node.service_credit = 0.0
        node.departures_this_step = 0
        return 0
    if condition.status == "degraded":
        multiplier *= 0.7
    node.service_credit += base_rate * multiplier * (time_step_minutes / 60.0)
    available = int(node.service_credit)
    node.departures_this_step = 0
    return available
