from avn.sim.event_loop import Simulator
from avn.sim.scenario_loader import load_scenario


def test_node_saturation_exposes_queue_pressure() -> None:
    replay = Simulator(load_scenario("node_saturation")).run()

    assert replay.summary["max_queue_length"] >= 2
    assert replay.summary["alerts_by_code"].get("node_queue_pressure", 0) >= 1
