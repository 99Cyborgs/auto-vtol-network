from __future__ import annotations

from avn.core.disturbances import build_conditions
from avn.core.state import ScenarioDefinition


def scenario_conditions(scenario: ScenarioDefinition, time_minute: int):
    return build_conditions(scenario.disturbances, time_minute)
