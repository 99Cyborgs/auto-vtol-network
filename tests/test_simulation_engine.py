from __future__ import annotations

from pathlib import Path

from avn.core.state import (
    CorridorDefinition,
    DisturbanceDefinition,
    NodeDefinition,
    ScenarioDefinition,
    VehicleDefinition,
    VehicleRuntime,
)
from avn.sim.engine import SimulationEngine
from avn.sim.reporting import build_summary
from avn.sim.runtime import SimulationRuntime


def _scenario(
    *,
    scenario_id: str,
    nodes: list[NodeDefinition],
    corridors: list[CorridorDefinition],
    vehicles: list[VehicleDefinition],
    disturbances: list[DisturbanceDefinition] | None = None,
    policy_id: str = "balanced",
    duration_minutes: int = 10,
    time_step_minutes: int = 5,
) -> ScenarioDefinition:
    return ScenarioDefinition(
        scenario_id=scenario_id,
        name=scenario_id.replace("_", " ").title(),
        description=f"Test scenario for {scenario_id}.",
        seed=1,
        duration_minutes=duration_minutes,
        time_step_minutes=time_step_minutes,
        recommended=False,
        nodes=nodes,
        corridors=corridors,
        vehicles=vehicles,
        disturbances=disturbances or [],
        output_root=Path("outputs/tests"),
        policy_id=policy_id,
    )


def _queue_vehicle(engine: SimulationEngine, node_id: str) -> tuple[SimulationRuntime, VehicleRuntime]:
    state = engine.initial_state()
    vehicle = next(iter(state.vehicles.values()))
    vehicle.status = "queued"
    vehicle.current_node = node_id
    state.nodes[node_id].queue.append(vehicle.vehicle_id)
    state.nodes[node_id].stand_occupants.add(vehicle.vehicle_id)
    return state, vehicle


def test_step_emits_release_delay_when_origin_stand_is_full() -> None:
    scenario = _scenario(
        scenario_id="release_delay",
        nodes=[
            NodeDefinition("A", "A", "hub", 0, 0, 60, 1, stand_capacity=1),
            NodeDefinition("B", "B", "hub", 10, 0, 60, 1),
        ],
        corridors=[],
        vehicles=[VehicleDefinition("V1", "A", "B", 0, "passenger", 50.0)],
    )
    engine = SimulationEngine(scenario)
    state = engine.initial_state()
    state.nodes["A"].stand_occupants.add("BLOCKER")

    step = engine.step(state, 0)

    assert any(event["event_type"] == "vehicle_release_delayed" for event in step.events)
    assert state.vehicles["V1"].status == "scheduled"
    assert not state.nodes["A"].queue


def test_step_advances_enroute_vehicle_to_completion() -> None:
    scenario = _scenario(
        scenario_id="enroute_completion",
        nodes=[
            NodeDefinition("A", "A", "hub", 0, 0, 60, 1),
            NodeDefinition("B", "B", "hub", 10, 0, 60, 1),
        ],
        corridors=[CorridorDefinition("A_B", "A", "B", 10.0, 120.0, 60.0)],
        vehicles=[VehicleDefinition("V1", "A", "B", 0, "passenger", 50.0)],
    )
    engine = SimulationEngine(scenario)
    state = engine.initial_state()
    vehicle = state.vehicles["V1"]
    vehicle.status = "enroute"
    vehicle.route = ["A", "B"]
    vehicle.active_corridor = "A_B"
    state.corridors["A_B"].occupants.append(vehicle.vehicle_id)
    state.nodes["B"].reserved_arrivals = 1

    step = engine.step(state, 5)

    assert any(event["event_type"] == "vehicle_completed" for event in step.events)
    assert vehicle.status == "completed"
    assert vehicle.current_node == "B"
    assert state.nodes["B"].reserved_arrivals == 0


def test_step_advances_turnaround_vehicle_back_to_queue() -> None:
    scenario = _scenario(
        scenario_id="turnaround_complete",
        nodes=[NodeDefinition("A", "A", "hub", 0, 0, 0, 1, turnaround_minutes=5)],
        corridors=[],
        vehicles=[VehicleDefinition("V1", "A", "A", 0, "passenger", 50.0)],
    )
    engine = SimulationEngine(scenario)
    state = engine.initial_state()
    vehicle = state.vehicles["V1"]
    vehicle.status = "turnaround"
    vehicle.current_node = "A"
    vehicle.turnaround_complete_minute = 5
    state.nodes["A"].stand_occupants.add(vehicle.vehicle_id)

    step = engine.step(state, 5)

    assert any(event["event_type"] == "vehicle_turnaround_complete" for event in step.events)
    assert vehicle.status == "queued"
    assert state.nodes["A"].queue == ["V1"]


def test_step_emits_stand_hold_when_destination_has_no_free_stands() -> None:
    scenario = _scenario(
        scenario_id="stand_hold",
        nodes=[
            NodeDefinition("A", "A", "hub", 0, 0, 60, 1),
            NodeDefinition("B", "B", "hub", 10, 0, 60, 1, stand_capacity=1),
        ],
        corridors=[CorridorDefinition("A_B", "A", "B", 10.0, 120.0, 60.0)],
        vehicles=[VehicleDefinition("V1", "A", "B", 0, "passenger", 50.0)],
    )
    engine = SimulationEngine(scenario)
    state, vehicle = _queue_vehicle(engine, "A")
    state.nodes["B"].stand_occupants.add("BLOCKER")

    step = engine.step(state, 0)

    assert any(event["event_type"] == "vehicle_stand_hold" for event in step.events)
    assert vehicle.status == "holding"
    assert state.nodes["A"].queue == ["V1"]


def test_step_emits_policy_hold_for_disruption_avoidant_dispatch_limit() -> None:
    scenario = _scenario(
        scenario_id="policy_hold",
        policy_id="disruption_avoidant",
        nodes=[
            NodeDefinition("A", "A", "hub", 0, 0, 60, 1),
            NodeDefinition("B", "B", "hub", 10, 0, 60, 1),
        ],
        corridors=[CorridorDefinition("A_B", "A", "B", 10.0, 120.0, 60.0)],
        vehicles=[VehicleDefinition("V1", "A", "B", 0, "passenger", 50.0)],
        disturbances=[DisturbanceDefinition("WX", "corridor", "A_B", 0, 5, weather_severity=0.8, status="degraded")],
    )
    engine = SimulationEngine(scenario)
    state, vehicle = _queue_vehicle(engine, "A")

    step = engine.step(state, 0)

    assert any(event["event_type"] == "vehicle_policy_hold" for event in step.events)
    assert vehicle.status == "holding"


def test_step_emits_reroute_event_when_route_changes() -> None:
    scenario = _scenario(
        scenario_id="reroute",
        nodes=[
            NodeDefinition("ORIGIN", "Origin", "hub", 0, 0, 60, 1),
            NodeDefinition("DIRECT", "Direct", "vertiport", 10, 0, 60, 1),
            NodeDefinition("SAFE", "Safe", "vertiport", 10, 10, 60, 1),
            NodeDefinition("DEST", "Dest", "hub", 20, 0, 60, 1),
        ],
        corridors=[
            CorridorDefinition("O_D", "ORIGIN", "DIRECT", 10.0, 120.0, 60.0),
            CorridorDefinition("D_X", "DIRECT", "DEST", 10.0, 120.0, 60.0),
            CorridorDefinition("O_S", "ORIGIN", "SAFE", 12.0, 120.0, 60.0),
            CorridorDefinition("S_X", "SAFE", "DEST", 12.0, 120.0, 60.0),
        ],
        vehicles=[VehicleDefinition("V1", "ORIGIN", "DEST", 0, "passenger", 50.0)],
        disturbances=[DisturbanceDefinition("CLOSE_DIRECT", "corridor", "O_D", 0, 5, status="closed")],
    )
    engine = SimulationEngine(scenario)
    state, vehicle = _queue_vehicle(engine, "ORIGIN")
    vehicle.route = ["ORIGIN", "DIRECT", "DEST"]

    step = engine.step(state, 0)

    reroute_event = next(event for event in step.events if event["event_type"] == "vehicle_rerouted")
    assert reroute_event["new_route"] == ["ORIGIN", "SAFE", "DEST"]
    assert vehicle.route == ["ORIGIN", "SAFE", "DEST"]
    assert vehicle.active_corridor == "O_S"


def test_step_emits_contingency_divert_when_destination_is_unreachable() -> None:
    scenario = _scenario(
        scenario_id="contingency_divert",
        nodes=[
            NodeDefinition("ORIGIN", "Origin", "hub", 0, 0, 60, 1),
            NodeDefinition("DEST", "Dest", "hub", 20, 0, 60, 1),
            NodeDefinition("PAD", "Pad", "emergency_pad", 10, 10, 60, 1, emergency_capacity=1),
        ],
        corridors=[CorridorDefinition("O_P", "ORIGIN", "PAD", 10.0, 120.0, 60.0)],
        vehicles=[VehicleDefinition("V1", "ORIGIN", "DEST", 0, "passenger", 50.0)],
    )
    engine = SimulationEngine(scenario)
    state, vehicle = _queue_vehicle(engine, "ORIGIN")

    step = engine.step(state, 0)

    divert_event = next(event for event in step.events if event["event_type"] == "vehicle_contingency_divert")
    dispatch_event = next(event for event in step.events if event["event_type"] == "vehicle_dispatched")
    assert divert_event["contingency_target"] == "PAD"
    assert vehicle.destination == "PAD"
    assert dispatch_event["route"] == ["ORIGIN", "PAD"]


def test_reporting_helpers_build_snapshots_and_summary() -> None:
    scenario = _scenario(
        scenario_id="reporting",
        nodes=[
            NodeDefinition("A", "A", "hub", 0, 0, 60, 1),
            NodeDefinition("B", "B", "hub", 10, 0, 60, 1),
        ],
        corridors=[CorridorDefinition("A_B", "A", "B", 10.0, 120.0, 60.0)],
        vehicles=[VehicleDefinition("V1", "A", "B", 99, "passenger", 50.0)],
    )
    engine = SimulationEngine(scenario)
    state = engine.initial_state()

    step = engine.step(state, 0)
    summary = build_summary(state, [step])

    assert step.nodes and step.corridors and step.vehicles
    assert step.vehicles[0].status == "scheduled"
    assert summary["scenario_id"] == "reporting"
    assert summary["active_vehicles"] == 0
    assert summary["alerts_by_code"] == {}
