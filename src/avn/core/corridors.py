from __future__ import annotations

from avn.core.state import CorridorCondition, CorridorRuntime
from avn.core.weather import corridor_capacity_factor, corridor_speed_factor


def update_corridor_runtime(
    corridor: CorridorRuntime,
    condition: CorridorCondition,
    time_step_minutes: int,
) -> tuple[int, float]:
    corridor.last_weather_severity = condition.weather_severity
    corridor.last_status = condition.status
    if condition.status == "closed":
        corridor.departure_credit = 0.0
        corridor.last_effective_capacity = 0.0
        corridor.last_speed_kmh = 0.0
        corridor.departures_this_step = 0
        return 0, 0.0
    capacity = (
        corridor.definition.base_capacity_per_hour
        * condition.capacity_multiplier
        * corridor_capacity_factor(condition.weather_severity * corridor.definition.weather_exposure)
    )
    speed = (
        corridor.definition.base_speed_kmh
        * corridor_speed_factor(condition.weather_severity * corridor.definition.weather_exposure)
    )
    corridor.departure_credit += capacity * (time_step_minutes / 60.0)
    corridor.last_effective_capacity = capacity
    corridor.last_speed_kmh = speed
    corridor.departures_this_step = 0
    return int(corridor.departure_credit), speed
