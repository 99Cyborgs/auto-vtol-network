from avn.demo_assets import DEMO_SCENARIO_IDS, load_demo_replay_payloads
import scripts.refresh_demo_replays as refresh_demo_replays


def test_packaged_demo_replays_load_in_curated_order() -> None:
    payloads = load_demo_replay_payloads()

    assert [payload["scenario_id"] for payload in payloads] == DEMO_SCENARIO_IDS
    assert all("policy" in payload for payload in payloads)
    assert all(payload["steps"] for payload in payloads)


def test_refresh_demo_replays_uses_temporary_output_root(monkeypatch, tmp_path) -> None:
    calls = []

    class DummyResult:
        def __init__(self, scenario_id: str) -> None:
            self.replay_path = tmp_path / f"{scenario_id}.json"
            self.replay_path.write_text(
                f'{{"scenario_id": "{scenario_id}", "policy": {{}}, "steps": [{{}}]}}',
                encoding="utf-8",
            )

    def fake_run_scenario(scenario_id, *, output_root=None):
        calls.append((scenario_id, output_root))
        return DummyResult(scenario_id)

    monkeypatch.setattr(refresh_demo_replays, "OUTPUT_DIR", tmp_path / "demo_assets")
    monkeypatch.setattr(refresh_demo_replays, "run_scenario", fake_run_scenario)

    assert refresh_demo_replays.main() == 0
    assert [scenario_id for scenario_id, _output_root in calls] == DEMO_SCENARIO_IDS
    assert all(output_root is not None for _scenario_id, output_root in calls)
