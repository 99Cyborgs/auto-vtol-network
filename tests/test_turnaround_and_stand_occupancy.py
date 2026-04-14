from __future__ import annotations

import json

from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def test_intermediate_node_turnaround_delays_next_dispatch(tmp_path) -> None:
    scenario_path = tmp_path / "turnaround_delay.json"
    scenario_path.write_text(
        json.dumps(
            {
                "scenario_id": "turnaround_delay",
                "name": "Turnaround Delay",
                "description": "Intermediate turnaround should delay onward dispatch.",
                "seed": 1,
                "duration_minutes": 20,
                "time_step_minutes": 5,
                "nodes": [
                    {
                        "node_id": "A",
                        "label": "A",
                        "node_type": "hub",
                        "x": 0,
                        "y": 0,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                        "turnaround_minutes": 0,
                    },
                    {
                        "node_id": "B",
                        "label": "B",
                        "node_type": "hub",
                        "x": 50,
                        "y": 0,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                        "turnaround_minutes": 10,
                        "stand_capacity": 1,
                    },
                    {
                        "node_id": "C",
                        "label": "C",
                        "node_type": "hub",
                        "x": 100,
                        "y": 0,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                    },
                ],
                "corridors": [
                    {
                        "corridor_id": "A_B",
                        "origin": "A",
                        "destination": "B",
                        "length_km": 10,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                    {
                        "corridor_id": "B_C",
                        "origin": "B",
                        "destination": "C",
                        "length_km": 10,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                ],
                "vehicles": [
                    {
                        "vehicle_id": "V1",
                        "origin": "A",
                        "destination": "C",
                        "release_minute": 0,
                        "mission_class": "passenger",
                        "reserve_energy": 50,
                    }
                ],
                "disturbances": [],
            }
        ),
        encoding="utf-8",
    )

    replay = SimulationEngine(load_scenario(scenario_path)).run()
    dispatches = [event for event in replay.event_log if event.get("event_type") == "vehicle_dispatched"]
    turnaround_events = [event for event in replay.event_log if event.get("event_type", "").startswith("vehicle_turnaround")]

    assert [event["corridor_id"] for event in dispatches] == ["A_B", "B_C"]
    assert dispatches[1]["time_minute"] == 15
    assert [event["event_type"] for event in turnaround_events] == [
        "vehicle_turnaround_started",
        "vehicle_turnaround_complete",
    ]


def test_destination_stand_capacity_blocks_second_dispatch_until_space_frees(tmp_path) -> None:
    scenario_path = tmp_path / "stand_capacity_hold.json"
    scenario_path.write_text(
        json.dumps(
            {
                "scenario_id": "stand_capacity_hold",
                "name": "Stand Capacity Hold",
                "description": "Destination stand reservations should delay the second dispatch.",
                "seed": 1,
                "duration_minutes": 15,
                "time_step_minutes": 5,
                "nodes": [
                    {
                        "node_id": "A",
                        "label": "A",
                        "node_type": "hub",
                        "x": 0,
                        "y": 0,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                    },
                    {
                        "node_id": "B",
                        "label": "B",
                        "node_type": "hub",
                        "x": 0,
                        "y": 50,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                    },
                    {
                        "node_id": "MID",
                        "label": "Mid",
                        "node_type": "hub",
                        "x": 50,
                        "y": 25,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                        "stand_capacity": 1,
                    },
                ],
                "corridors": [
                    {
                        "corridor_id": "A_MID",
                        "origin": "A",
                        "destination": "MID",
                        "length_km": 10,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                    {
                        "corridor_id": "B_MID",
                        "origin": "B",
                        "destination": "MID",
                        "length_km": 10,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                ],
                "vehicles": [
                    {
                        "vehicle_id": "V1",
                        "origin": "A",
                        "destination": "MID",
                        "release_minute": 0,
                        "mission_class": "passenger",
                        "reserve_energy": 50,
                    },
                    {
                        "vehicle_id": "V2",
                        "origin": "B",
                        "destination": "MID",
                        "release_minute": 0,
                        "mission_class": "passenger",
                        "reserve_energy": 50,
                    },
                ],
                "disturbances": [],
            }
        ),
        encoding="utf-8",
    )

    replay = SimulationEngine(load_scenario(scenario_path)).run()
    dispatches = [event for event in replay.event_log if event.get("event_type") == "vehicle_dispatched"]
    stand_holds = [event for event in replay.event_log if event.get("event_type") == "vehicle_stand_hold"]

    assert [(event["vehicle_id"], event["time_minute"]) for event in dispatches] == [("V1", 0), ("V2", 5)]
    assert stand_holds
    assert stand_holds[0]["vehicle_id"] == "V2"
    assert stand_holds[0]["blocked_node_id"] == "MID"


def test_dispatch_avoids_stand_saturated_transit_node_when_alternate_exists(tmp_path) -> None:
    scenario_path = tmp_path / "alternate_when_transit_full.json"
    scenario_path.write_text(
        json.dumps(
            {
                "scenario_id": "alternate_when_transit_full",
                "name": "Alternate Around Full Transit",
                "description": "Dispatch should reroute around a transit node with no free stands.",
                "seed": 1,
                "duration_minutes": 15,
                "time_step_minutes": 5,
                "nodes": [
                    {
                        "node_id": "ORIGIN",
                        "label": "Origin",
                        "node_type": "hub",
                        "x": 0,
                        "y": 0,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                    },
                    {
                        "node_id": "DIRECT",
                        "label": "Direct",
                        "node_type": "vertiport",
                        "x": 50,
                        "y": 0,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                        "stand_capacity": 1,
                        "turnaround_minutes": 10,
                    },
                    {
                        "node_id": "SAFE",
                        "label": "Safe",
                        "node_type": "vertiport",
                        "x": 50,
                        "y": 50,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                    },
                    {
                        "node_id": "DEST",
                        "label": "Dest",
                        "node_type": "hub",
                        "x": 100,
                        "y": 0,
                        "service_rate_per_hour": 60,
                        "queue_alert_threshold": 1,
                    },
                ],
                "corridors": [
                    {
                        "corridor_id": "O_D",
                        "origin": "ORIGIN",
                        "destination": "DIRECT",
                        "length_km": 10,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                    {
                        "corridor_id": "D_X",
                        "origin": "DIRECT",
                        "destination": "DEST",
                        "length_km": 10,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                    {
                        "corridor_id": "O_S",
                        "origin": "ORIGIN",
                        "destination": "SAFE",
                        "length_km": 12,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                    {
                        "corridor_id": "S_X",
                        "origin": "SAFE",
                        "destination": "DEST",
                        "length_km": 12,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                    {
                        "corridor_id": "BLOCK_DIRECT",
                        "origin": "SAFE",
                        "destination": "DIRECT",
                        "length_km": 5,
                        "base_speed_kmh": 120,
                        "base_capacity_per_hour": 60,
                    },
                ],
                "vehicles": [
                    {
                        "vehicle_id": "BLOCKER",
                        "origin": "DIRECT",
                        "destination": "DEST",
                        "release_minute": 0,
                        "mission_class": "passenger",
                        "reserve_energy": 50,
                    },
                    {
                        "vehicle_id": "V1",
                        "origin": "ORIGIN",
                        "destination": "DEST",
                        "release_minute": 0,
                        "mission_class": "passenger",
                        "reserve_energy": 50,
                    },
                ],
                "disturbances": [],
            }
        ),
        encoding="utf-8",
    )

    replay = SimulationEngine(load_scenario(scenario_path)).run()
    dispatches = [event for event in replay.event_log if event.get("event_type") == "vehicle_dispatched"]
    v1_dispatch = next(event for event in dispatches if event["vehicle_id"] == "V1")
    v1_reroutes = [
        event for event in replay.event_log
        if event.get("event_type") == "vehicle_rerouted" and event.get("vehicle_id") == "V1"
    ]
    v1_stand_holds = [
        event for event in replay.event_log
        if event.get("event_type") == "vehicle_stand_hold" and event.get("vehicle_id") == "V1"
    ]

    assert v1_dispatch["corridor_id"] == "O_S"
    assert v1_dispatch["route"] == ["ORIGIN", "SAFE", "DEST"]
    assert not v1_reroutes
    assert not v1_stand_holds
