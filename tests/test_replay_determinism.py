from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def test_replay_is_deterministic_for_same_scenario() -> None:
    scenario = load_scenario("baseline_flow")
    replay_a = SimulationEngine(scenario).run().to_dict()
    replay_b = SimulationEngine(load_scenario("baseline_flow")).run().to_dict()

    assert replay_a == replay_b
