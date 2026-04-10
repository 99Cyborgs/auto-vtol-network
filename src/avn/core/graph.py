from __future__ import annotations

import networkx as nx

from avn.core.state import CorridorDefinition, NodeDefinition


def build_graph(nodes: list[NodeDefinition], corridors: list[CorridorDefinition]) -> nx.DiGraph:
    graph = nx.DiGraph()
    for node in nodes:
        graph.add_node(node.node_id, node=node)
    for corridor in corridors:
        graph.add_edge(
            corridor.origin,
            corridor.destination,
            corridor_id=corridor.corridor_id,
            length_km=corridor.length_km,
            travel_minutes=(corridor.length_km / max(corridor.base_speed_kmh, 1.0)) * 60.0,
        )
    return graph
