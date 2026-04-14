from avn.core.weather import corridor_capacity_factor, corridor_speed_factor
from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def test_weather_thresholds_reduce_capacity_and_speed() -> None:
    assert corridor_capacity_factor(0.0) == 1.0
    assert corridor_capacity_factor(0.8) < corridor_capacity_factor(0.4)
    assert corridor_speed_factor(0.8) < corridor_speed_factor(0.4)


def test_weather_closure_creates_closed_corridor_frames() -> None:
    replay = SimulationEngine(load_scenario("weather_closure")).run()
    north_edge_frames = [
        corridor
        for step in replay.steps
        for corridor in step.corridors
        if corridor.corridor_id == "N_E"
    ]

    assert any(corridor.status == "closed" for corridor in north_edge_frames)
