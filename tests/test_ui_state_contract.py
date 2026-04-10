from avn.sim.event_loop import Simulator
from avn.sim.scenario_loader import load_scenario
from avn.ui.serializers import serialize_replay


def test_dashboard_serializer_uses_canonical_replay_contract() -> None:
    replay = Simulator(load_scenario("baseline_flow")).run()
    payload = serialize_replay(replay)

    assert {"scenario_id", "name", "summary", "steps", "event_log"} <= set(payload)
    assert {"nodes", "corridors", "vehicles", "metrics", "alerts", "events"} <= set(payload["steps"][0])
