from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def test_alerts_cover_queue_weather_and_closure_conditions() -> None:
    replay = SimulationEngine(load_scenario("weather_closure")).run()
    codes = {event["code"] for event in replay.event_log if event.get("code")}

    assert "weather_degraded" in codes
    assert "corridor_closed" in codes
