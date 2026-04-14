import json
import threading
from urllib.error import HTTPError
from urllib.request import urlopen

from avn.demo_assets import load_demo_replay_payloads
from avn.sim.runner import run_scenario
from avn.sim.engine import SimulationEngine
from avn.sim.scenario_loader import load_scenario
from avn.ui.api import run_dashboard
from avn.ui.serializers import serialize_replay


def test_dashboard_serializer_uses_canonical_replay_contract() -> None:
    replay = SimulationEngine(load_scenario("baseline_flow")).run()
    payload = serialize_replay(replay)

    assert {"scenario_id", "name", "policy", "summary", "steps", "event_log"} <= set(payload)
    assert payload["policy"]["policy_id"] == "balanced"
    assert {"nodes", "corridors", "vehicles", "metrics", "alerts", "events"} <= set(payload["steps"][0])


def test_dashboard_serves_saved_replay_bundle(tmp_path) -> None:
    result = run_scenario("baseline_flow", output_root=tmp_path)
    expected_payload = json.loads(result.replay_path.read_text(encoding="utf-8"))
    server = run_dashboard(replay_path=result.replay_path, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        scenarios = json.loads(urlopen(f"{base_url}/api/scenarios").read().decode("utf-8"))
        replay = json.loads(
            urlopen(f"{base_url}/api/replay?scenario={expected_payload['scenario_id']}").read().decode("utf-8")
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert scenarios == [{"scenario_id": expected_payload["scenario_id"], "name": expected_payload["name"]}]
    assert replay == expected_payload


def test_dashboard_respects_curated_scenario_order() -> None:
    ordered_ids = ["incident_diversion_balanced", "incident_diversion_avoidant", "metro_surge_balanced"]
    server = run_dashboard(scenarios=ordered_ids, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        scenarios = json.loads(urlopen(f"{base_url}/api/scenarios").read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert [item["scenario_id"] for item in scenarios] == ordered_ids


def test_dashboard_serves_packaged_demo_replays_in_curated_order() -> None:
    demo_payloads = load_demo_replay_payloads()
    ordered_ids = [payload["scenario_id"] for payload in demo_payloads]
    server = run_dashboard(replay_payloads=demo_payloads, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        scenarios = json.loads(urlopen(f"{base_url}/api/scenarios").read().decode("utf-8"))
        replay = json.loads(urlopen(f"{base_url}/api/replay").read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert [item["scenario_id"] for item in scenarios] == ordered_ids
    assert replay["scenario_id"] == ordered_ids[0]


def test_dashboard_rejects_unknown_scenario_requests(tmp_path) -> None:
    result = run_scenario("baseline_flow", output_root=tmp_path)
    server = run_dashboard(replay_path=result.replay_path, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            urlopen(f"{base_url}/api/replay?scenario=not-a-scenario")
        except HTTPError as exc:
            assert exc.code == 404
        else:
            raise AssertionError("Expected 404 for an unknown scenario id.")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_dashboard_requires_at_least_one_source() -> None:
    try:
        run_dashboard(scenarios=[], host="127.0.0.1", port=0)
    except ValueError as exc:
        assert "at least one scenario or replay payload" in str(exc)
    else:
        raise AssertionError("Expected ValueError for an empty dashboard source list.")
