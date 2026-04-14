import json

import pytest

from avn.core.graph import build_graph
from avn.core.policies import DEFAULT_POLICY_ID, get_policy_profile
from avn.core.routing import compute_route
from avn.core.state import (
    CorridorCondition,
    CorridorDefinition,
    CorridorRuntime,
    NodeDefinition,
    NodeRuntime,
)
from avn.sim.scenario_loader import load_scenario


def _build_test_network(
    *,
    direct_lengths_km: tuple[float, float],
    alternate_lengths_km: tuple[float, float],
) -> tuple[dict[str, NodeRuntime], dict[str, CorridorRuntime], object]:
    nodes = [
        NodeDefinition("ORIGIN", "Origin", "hub", 0, 0, 12, 2, 1),
        NodeDefinition("DIRECT", "Direct", "vertiport", 50, 0, 12, 2, 0),
        NodeDefinition("SAFE", "Safe", "vertiport", 50, 50, 12, 2, 0),
        NodeDefinition("DEST", "Dest", "hub", 100, 0, 12, 2, 1),
    ]
    corridors = [
        CorridorDefinition("O_D", "ORIGIN", "DIRECT", direct_lengths_km[0], 120, 12, 1.0),
        CorridorDefinition("D_X", "DIRECT", "DEST", direct_lengths_km[1], 120, 12, 1.0),
        CorridorDefinition("O_S", "ORIGIN", "SAFE", alternate_lengths_km[0], 120, 12, 1.0),
        CorridorDefinition("S_X", "SAFE", "DEST", alternate_lengths_km[1], 120, 12, 1.0),
    ]
    graph = build_graph(nodes, corridors)
    node_runtimes = {node.node_id: NodeRuntime(definition=node) for node in nodes}
    corridor_runtimes = {corridor.corridor_id: CorridorRuntime(definition=corridor) for corridor in corridors}
    return node_runtimes, corridor_runtimes, graph


def test_disruption_avoidant_prefers_clearer_route_than_balanced() -> None:
    nodes, corridors, graph = _build_test_network(
        direct_lengths_km=(12.0, 12.0),
        alternate_lengths_km=(15.0, 15.0),
    )
    corridor_conditions = {"O_D": CorridorCondition(weather_severity=0.1, status="degraded")}

    balanced_route = compute_route(
        graph,
        corridors,
        nodes,
        {},
        corridor_conditions,
        "ORIGIN",
        "DEST",
        get_policy_profile("balanced"),
    )
    avoidant_route = compute_route(
        graph,
        corridors,
        nodes,
        {},
        corridor_conditions,
        "ORIGIN",
        "DEST",
        get_policy_profile("disruption_avoidant"),
    )

    assert balanced_route == ["ORIGIN", "DIRECT", "DEST"]
    assert avoidant_route == ["ORIGIN", "SAFE", "DEST"]


def test_throughput_max_avoids_loaded_corridor_more_aggressively_than_balanced() -> None:
    nodes, corridors, graph = _build_test_network(
        direct_lengths_km=(10.0, 10.0),
        alternate_lengths_km=(13.0, 13.0),
    )
    corridors["O_D"].occupants = ["V1", "V2"]

    balanced_route = compute_route(
        graph,
        corridors,
        nodes,
        {},
        {},
        "ORIGIN",
        "DEST",
        get_policy_profile("balanced"),
    )
    throughput_route = compute_route(
        graph,
        corridors,
        nodes,
        {},
        {},
        "ORIGIN",
        "DEST",
        get_policy_profile("throughput_max"),
    )

    assert balanced_route == ["ORIGIN", "DIRECT", "DEST"]
    assert throughput_route == ["ORIGIN", "SAFE", "DEST"]


def test_loader_defaults_policy_to_balanced(tmp_path) -> None:
    scenario_path = tmp_path / "policy_default.json"
    scenario_path.write_text(
        json.dumps(
            {
                "scenario_id": "policy_default",
                "name": "Policy Default",
                "description": "Scenario without an explicit policy id.",
                "seed": 1,
                "duration_minutes": 5,
                "time_step_minutes": 5,
                "recommended": False,
                "output_root": str(tmp_path / "outputs"),
                "nodes": [
                    {
                        "node_id": "A",
                        "label": "A",
                        "node_type": "hub",
                        "x": 0,
                        "y": 0,
                        "service_rate_per_hour": 10,
                        "queue_alert_threshold": 1,
                        "emergency_capacity": 1,
                    }
                ],
                "corridors": [],
                "vehicles": [],
                "disturbances": [],
            }
        ),
        encoding="utf-8",
    )

    scenario = load_scenario(scenario_path)

    assert scenario.policy_id == DEFAULT_POLICY_ID


def test_loader_rejects_unknown_policy(tmp_path) -> None:
    scenario_path = tmp_path / "policy_invalid.json"
    scenario_path.write_text(
        json.dumps(
            {
                "scenario_id": "policy_invalid",
                "name": "Policy Invalid",
                "description": "Scenario with an invalid policy id.",
                "seed": 1,
                "duration_minutes": 5,
                "time_step_minutes": 5,
                "policy_id": "not_real",
                "recommended": False,
                "output_root": str(tmp_path / "outputs"),
                "nodes": [],
                "corridors": [],
                "vehicles": [],
                "disturbances": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unknown policy_id"):
        load_scenario(scenario_path)


def test_route_avoids_stand_saturated_transit_node() -> None:
    nodes, corridors, graph = _build_test_network(
        direct_lengths_km=(10.0, 10.0),
        alternate_lengths_km=(12.0, 12.0),
    )
    nodes["DIRECT"].definition = NodeDefinition("DIRECT", "Direct", "vertiport", 50, 0, 12, 2, 0, stand_capacity=1)
    nodes["DIRECT"].stand_occupants.add("BLOCKER")

    route = compute_route(
        graph,
        corridors,
        nodes,
        {},
        {},
        "ORIGIN",
        "DEST",
        get_policy_profile("balanced"),
    )

    assert route == ["ORIGIN", "SAFE", "DEST"]
