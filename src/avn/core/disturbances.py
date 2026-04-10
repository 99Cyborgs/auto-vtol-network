from __future__ import annotations

from avn.core.state import CorridorCondition, DisturbanceDefinition, NodeCondition


def build_conditions(
    disturbances: list[DisturbanceDefinition],
    time_minute: int,
) -> tuple[dict[str, NodeCondition], dict[str, CorridorCondition], list[dict[str, str | int | float]]]:
    node_conditions: dict[str, NodeCondition] = {}
    corridor_conditions: dict[str, CorridorCondition] = {}
    active_events: list[dict[str, str | int | float]] = []

    for disturbance in disturbances:
        if not (disturbance.start_minute <= time_minute < disturbance.end_minute):
            continue
        active_events.append(
            {
                "time_minute": time_minute,
                "event_type": f"{disturbance.kind}_disturbance",
                "target_id": disturbance.target_id,
                "status": disturbance.status,
                "weather_severity": disturbance.weather_severity,
                "note": disturbance.note,
            }
        )
        if disturbance.kind == "node":
            condition = node_conditions.setdefault(disturbance.target_id, NodeCondition())
            condition.weather_severity = max(condition.weather_severity, disturbance.weather_severity)
            condition.service_multiplier *= disturbance.service_multiplier
            if disturbance.status != "nominal":
                condition.status = disturbance.status
            if disturbance.note:
                condition.notes.append(disturbance.note)
        elif disturbance.kind == "corridor":
            condition = corridor_conditions.setdefault(disturbance.target_id, CorridorCondition())
            condition.weather_severity = max(condition.weather_severity, disturbance.weather_severity)
            condition.capacity_multiplier *= disturbance.capacity_multiplier
            if disturbance.status != "nominal":
                condition.status = disturbance.status
            if disturbance.note:
                condition.notes.append(disturbance.note)

    return node_conditions, corridor_conditions, active_events
