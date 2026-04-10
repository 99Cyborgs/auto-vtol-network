from __future__ import annotations


def corridor_capacity_factor(weather_severity: float) -> float:
    if weather_severity >= 0.95:
        return 0.0
    if weather_severity >= 0.75:
        return 0.35
    if weather_severity >= 0.5:
        return 0.6
    if weather_severity >= 0.25:
        return 0.82
    return 1.0


def corridor_speed_factor(weather_severity: float) -> float:
    if weather_severity >= 0.95:
        return 0.0
    if weather_severity >= 0.75:
        return 0.5
    if weather_severity >= 0.5:
        return 0.7
    if weather_severity >= 0.25:
        return 0.88
    return 1.0
