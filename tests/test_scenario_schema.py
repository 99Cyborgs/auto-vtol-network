import json

import pytest

from avn.sim import list_scenarios, load_scenario, scenario_to_payload, validate_scenario_payload


def test_builtin_scenarios_load_under_canonical_schema() -> None:
    for scenario_id in list_scenarios():
        scenario = load_scenario(scenario_id)
        payload = validate_scenario_payload(scenario_to_payload(scenario), source=scenario_id)
        assert payload["scenario_id"] == scenario_id


def test_validate_scenario_payload_rejects_missing_top_level_field() -> None:
    with pytest.raises(ValueError, match="missing required top-level fields"):
        validate_scenario_payload(
            {
                "name": "Broken",
                "description": "missing scenario id",
                "duration_minutes": 5,
                "time_step_minutes": 5,
                "nodes": [],
                "corridors": [],
                "vehicles": [],
            },
            source="broken.json",
        )


def test_validate_scenario_payload_rejects_malformed_node_entry() -> None:
    with pytest.raises(ValueError, match="nodes\\[0\\] is missing required fields"):
        validate_scenario_payload(
            {
                "scenario_id": "bad-node",
                "name": "Bad Node",
                "description": "node is missing queue threshold",
                "duration_minutes": 5,
                "time_step_minutes": 5,
                "nodes": [
                    {
                        "node_id": "A",
                        "label": "A",
                        "node_type": "hub",
                        "x": 0,
                        "y": 0,
                        "service_rate_per_hour": 10,
                    }
                ],
                "corridors": [],
                "vehicles": [],
            },
            source="bad-node.json",
        )


def test_validate_scenario_payload_rejects_unknown_references() -> None:
    with pytest.raises(ValueError, match="references unknown nodes"):
        validate_scenario_payload(
            {
                "scenario_id": "bad-ref",
                "name": "Bad Ref",
                "description": "corridor points at a missing node",
                "duration_minutes": 5,
                "time_step_minutes": 5,
                "nodes": [
                    {
                        "node_id": "A",
                        "label": "A",
                        "node_type": "hub",
                        "x": 0,
                        "y": 0,
                        "service_rate_per_hour": 10,
                        "queue_alert_threshold": 1,
                    }
                ],
                "corridors": [
                    {
                        "corridor_id": "A_B",
                        "origin": "A",
                        "destination": "B",
                        "length_km": 10,
                        "base_speed_kmh": 100,
                        "base_capacity_per_hour": 10,
                    }
                ],
                "vehicles": [],
            },
            source="bad-ref.json",
        )


def test_validate_scenario_payload_rejects_unknown_disturbance_target() -> None:
    with pytest.raises(ValueError, match="unknown corridor target"):
        validate_scenario_payload(
            {
                "scenario_id": "bad-disturbance",
                "name": "Bad Disturbance",
                "description": "disturbance points at a missing corridor",
                "duration_minutes": 5,
                "time_step_minutes": 5,
                "nodes": [
                    {
                        "node_id": "A",
                        "label": "A",
                        "node_type": "hub",
                        "x": 0,
                        "y": 0,
                        "service_rate_per_hour": 10,
                        "queue_alert_threshold": 1,
                    }
                ],
                "corridors": [],
                "vehicles": [],
                "disturbances": [
                    {
                        "disturbance_id": "missing",
                        "kind": "corridor",
                        "target_id": "A_B",
                        "start_minute": 0,
                        "end_minute": 5,
                    }
                ],
            },
            source="bad-disturbance.json",
        )


def test_scenario_to_payload_can_be_written_and_reloaded(tmp_path) -> None:
    source = load_scenario("baseline_flow")
    payload = scenario_to_payload(source)
    scenario_path = tmp_path / "round_trip.json"
    scenario_path.write_text(json.dumps(payload), encoding="utf-8")

    round_trip = load_scenario(scenario_path)

    assert round_trip.scenario_id == source.scenario_id
    assert round_trip.policy_id == source.policy_id
    assert len(round_trip.nodes) == len(source.nodes)
    assert len(round_trip.corridors) == len(source.corridors)
    assert len(round_trip.vehicles) == len(source.vehicles)
