from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario


def _dispatch_counts(scenario_id: str) -> tuple[dict[str, int], dict[str, object]]:
    replay = SimulationEngine(load_scenario(scenario_id)).run()
    counts: dict[str, int] = {}
    for event in replay.event_log:
        if event.get("event_type") != "vehicle_dispatched":
            continue
        corridor_id = str(event["corridor_id"])
        counts[corridor_id] = counts.get(corridor_id, 0) + 1
    return counts, replay.summary


def test_incident_diversion_policy_variants_shift_branch_usage() -> None:
    balanced_counts, balanced_summary = _dispatch_counts("incident_diversion_balanced")
    avoidant_counts, avoidant_summary = _dispatch_counts("incident_diversion_avoidant")

    assert balanced_counts.get("N_E", 0) > avoidant_counts.get("N_E", 0)
    assert avoidant_counts.get("S_E", 0) > balanced_counts.get("S_E", 0)
    assert balanced_summary["min_reserve_energy"] > 0.0
    assert avoidant_summary["min_reserve_energy"] > 0.0


def test_metro_surge_throughput_mode_reduces_peak_corridor_load() -> None:
    _balanced_counts, balanced_summary = _dispatch_counts("metro_surge_balanced")
    _throughput_counts, throughput_summary = _dispatch_counts("metro_surge_throughput_max")

    assert throughput_summary["max_corridor_load_ratio"] < balanced_summary["max_corridor_load_ratio"]
    assert throughput_summary["min_reserve_energy"] > 0.0
