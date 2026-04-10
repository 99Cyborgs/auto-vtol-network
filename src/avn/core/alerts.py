from __future__ import annotations

from avn.core.state import AlertRecord, CorridorRuntime, NodeCondition, NodeRuntime, VehicleRuntime


def build_alerts(
    time_minute: int,
    nodes: dict[str, NodeRuntime],
    node_conditions: dict[str, NodeCondition],
    corridors: dict[str, CorridorRuntime],
    vehicles: dict[str, VehicleRuntime],
    thresholds: dict[str, float | int],
) -> list[AlertRecord]:
    alerts: list[AlertRecord] = []
    queue_alert = int(thresholds.get("queue_pressure", 4))
    corridor_alert = float(thresholds.get("corridor_load_ratio", 0.9))
    reserve_alert = float(thresholds.get("low_reserve", 18.0))

    for node_id, runtime in nodes.items():
        if len(runtime.queue) >= max(queue_alert, runtime.definition.queue_alert_threshold):
            alerts.append(
                AlertRecord(
                    time_minute=time_minute,
                    severity="warning",
                    code="node_queue_pressure",
                    message=f"{runtime.definition.label} queue pressure is {len(runtime.queue)} vehicles.",
                    entity_id=node_id,
                )
            )
        condition = node_conditions.get(node_id)
        if condition and condition.status in {"degraded", "closed"}:
            alerts.append(
                AlertRecord(
                    time_minute=time_minute,
                    severity="critical" if condition.status == "closed" else "warning",
                    code="node_degraded",
                    message=f"{runtime.definition.label} is {condition.status}.",
                    entity_id=node_id,
                )
            )

    for corridor in corridors.values():
        if corridor.last_effective_capacity <= 0.0 and corridor.last_status == "closed":
            alerts.append(
                AlertRecord(
                    time_minute=time_minute,
                    severity="critical",
                    code="corridor_closed",
                    message=f"{corridor.definition.corridor_id} is closed.",
                    entity_id=corridor.definition.corridor_id,
                )
            )
            continue
        nominal = max(
            1.0,
            corridor.last_effective_capacity
            * max(corridor.definition.length_km / max(corridor.last_speed_kmh, 1.0), 0.1),
        )
        load_ratio = len(corridor.occupants) / nominal
        if load_ratio >= corridor_alert:
            alerts.append(
                AlertRecord(
                    time_minute=time_minute,
                    severity="warning",
                    code="corridor_load",
                    message=f"{corridor.definition.corridor_id} load ratio reached {load_ratio:.2f}.",
                    entity_id=corridor.definition.corridor_id,
                )
            )
        if corridor.last_weather_severity >= 0.5:
            alerts.append(
                AlertRecord(
                    time_minute=time_minute,
                    severity="warning",
                    code="weather_degraded",
                    message=f"{corridor.definition.corridor_id} weather severity is {corridor.last_weather_severity:.2f}.",
                    entity_id=corridor.definition.corridor_id,
                )
            )

    for vehicle in vehicles.values():
        if vehicle.reserve_energy <= reserve_alert and vehicle.status not in {"completed", "scheduled"}:
            alerts.append(
                AlertRecord(
                    time_minute=time_minute,
                    severity="warning",
                    code="low_reserve",
                    message=f"{vehicle.vehicle_id} reserve dropped to {vehicle.reserve_energy:.1f}.",
                    entity_id=vehicle.vehicle_id,
                )
            )
        if vehicle.status == "holding":
            alerts.append(
                AlertRecord(
                    time_minute=time_minute,
                    severity="critical",
                    code="contingency_hold",
                    message=f"{vehicle.vehicle_id} is holding for a contingency route.",
                    entity_id=vehicle.vehicle_id,
                )
            )
    return alerts
