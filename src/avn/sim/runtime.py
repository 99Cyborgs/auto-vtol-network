from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx

from avn.core.policies import PolicyProfile
from avn.core.state import CorridorRuntime, NodeRuntime, ScenarioDefinition, VehicleRuntime


@dataclass(slots=True)
class SimulationRuntime:
    scenario: ScenarioDefinition
    policy: PolicyProfile
    graph: nx.DiGraph
    nodes: dict[str, NodeRuntime]
    corridors: dict[str, CorridorRuntime]
    vehicles: dict[str, VehicleRuntime]
    event_log: list[dict[str, object]] = field(default_factory=list)
