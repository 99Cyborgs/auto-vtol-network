from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def test_weather_closure_shifts_flow_to_southern_branch() -> None:
    replay = SimulationEngine(load_scenario("weather_closure")).run()
    southern_reroutes = [
        event
        for event in replay.event_log
        if event.get("event_type") == "vehicle_rerouted" and "MID_SOUTH" in event.get("new_route", [])
    ]

    assert southern_reroutes
