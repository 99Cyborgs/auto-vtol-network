from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def test_ems_vehicle_receives_preferential_dispatch() -> None:
    replay = SimulationEngine(load_scenario("priority_mission")).run()
    dispatches = [
        event for event in replay.event_log if event.get("event_type") == "vehicle_dispatched"
    ]
    ems_index = next(index for index, event in enumerate(dispatches) if event["vehicle_id"] == "EMS_01")
    routine_index = next(index for index, event in enumerate(dispatches) if event["vehicle_id"] == "ROUTINE_03")

    assert ems_index < routine_index
