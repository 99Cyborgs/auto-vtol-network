import json

import pytest

import avn.__main__ as cli
from avn.__main__ import main
from avn.demo_assets import DEMO_SCENARIO_IDS
from avn.ui.api import _load_dashboard_replay, run_dashboard


def test_dashboard_cli_rejects_replay_and_scenario_together() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["dashboard", "--scenario", "baseline_flow", "--replay", "outputs/avn/example/replay.json"])

    assert excinfo.value.code == 2


def test_dashboard_replay_mode_rejects_incomplete_payload(tmp_path) -> None:
    replay_path = tmp_path / "replay.json"
    replay_path.write_text(json.dumps({"scenario_id": "broken", "name": "Broken Replay", "steps": []}), encoding="utf-8")

    with pytest.raises(ValueError, match="missing required top-level fields|non-empty steps list"):
        run_dashboard(replay_path=replay_path, host="127.0.0.1", port=0)


def test_dashboard_replay_mode_rejects_invalid_json(tmp_path) -> None:
    replay_path = tmp_path / "replay.json"
    replay_path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(ValueError, match="not valid JSON"):
        run_dashboard(replay_path=replay_path, host="127.0.0.1", port=0)


def test_dashboard_replay_mode_backfills_missing_policy_metadata(tmp_path) -> None:
    replay_path = tmp_path / "replay.json"
    replay_path.write_text(
        json.dumps(
            {
                "scenario_id": "legacy",
                "name": "Legacy Replay",
                "description": "Replay emitted before policy metadata was added.",
                "summary": {"scenario_id": "legacy"},
                "steps": [
                    {
                        "nodes": [],
                        "corridors": [],
                        "vehicles": [],
                        "metrics": {},
                        "alerts": [],
                        "events": [],
                    }
                ],
                "event_log": [],
            }
        ),
        encoding="utf-8",
    )

    payload = _load_dashboard_replay(replay_path)

    assert payload["policy"]["policy_id"] == "balanced"


def test_demo_cli_uses_curated_demo_scenarios(monkeypatch) -> None:
    captured = {}

    class DummyServer:
        def serve_forever(self) -> None:
            return

        def server_close(self) -> None:
            return

    def fake_run_dashboard(**kwargs):
        captured.update(kwargs)
        return DummyServer()

    monkeypatch.setattr(cli, "run_dashboard", fake_run_dashboard)

    assert main(["demo", "--host", "127.0.0.1", "--port", "0"]) == 0
    assert captured["scenario"] is None
    assert captured["replay_path"] is None
    assert captured["scenarios"] is None
    assert captured["replay_payloads"] is not None
    assert [payload["scenario_id"] for payload in captured["replay_payloads"]] == DEMO_SCENARIO_IDS
