from __future__ import annotations

import networkx as nx

from avn.core.state import CorridorCondition, CorridorRuntime, NodeCondition, NodeRuntime


def compute_route(
    graph: nx.DiGraph,
    corridors: dict[str, CorridorRuntime],
    nodes: dict[str, NodeRuntime],
    node_conditions: dict[str, NodeCondition],
    corridor_conditions: dict[str, CorridorCondition],
    origin: str,
    destination: str,
) -> list[str] | None:
    route_graph = nx.DiGraph()
    for node_id, node_data in graph.nodes(data=True):
        if node_conditions.get(node_id, NodeCondition()).status == "closed":
            continue
        route_graph.add_node(node_id, **node_data)

    for source, target, data in graph.edges(data=True):
        corridor_id = data["corridor_id"]
        condition = corridor_conditions.get(corridor_id, CorridorCondition())
        corridor = corridors[corridor_id]
        if condition.status == "closed":
            continue
        if source not in route_graph or target not in route_graph:
            continue
        node_penalty = max(0.0, len(nodes[target].queue) - nodes[target].definition.queue_alert_threshold) * 3.0
        weather_penalty = condition.weather_severity * 20.0
        route_graph.add_edge(
            source,
            target,
            weight=float(data["travel_minutes"]) + weather_penalty + node_penalty + len(corridor.occupants),
        )
    try:
        return nx.shortest_path(route_graph, origin, destination, weight="weight")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return None


def compute_contingency_target(
    graph: nx.DiGraph,
    nodes: dict[str, NodeRuntime],
    node_conditions: dict[str, NodeCondition],
    origin: str,
) -> str | None:
    best: tuple[float, str] | None = None
    for node_id, runtime in nodes.items():
        if runtime.definition.node_type not in {"emergency_pad", "hub"}:
            continue
        if runtime.definition.emergency_capacity <= 0:
            continue
        if node_conditions.get(node_id, NodeCondition()).status == "closed":
            continue
        try:
            distance = nx.shortest_path_length(graph, origin, node_id, weight="travel_minutes")
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            continue
        candidate = (float(distance), node_id)
        if best is None or candidate < best:
            best = candidate
    return best[1] if best else None
