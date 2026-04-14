from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def test_weather_closure_produces_reroutes() -> None:
    replay = SimulationEngine(load_scenario("weather_closure")).run()
    reroutes = [event for event in replay.event_log if event.get("event_type") == "vehicle_rerouted"]

    assert reroutes
    assert replay.summary["reroute_count"] >= 1
