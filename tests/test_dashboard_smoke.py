import json
from pathlib import Path

import scripts.dashboard_smoke as dashboard_smoke


def test_override_replay_bundle_writes_saved_identity(tmp_path: Path) -> None:
    source = tmp_path / "replay.json"
    source.write_text(
        json.dumps(
            {
                "scenario_id": "weather_closure",
                "name": "Original Replay",
                "description": "Original description",
                "policy": {"policy_id": "balanced", "label": "Balanced Flow", "description": "Demo"},
                "steps": [{}],
                "summary": {"scenario_id": "weather_closure"},
                "event_log": [],
            }
        ),
        encoding="utf-8",
    )

    target, payload = dashboard_smoke._override_replay_bundle(source, tmp_path)

    assert target.exists()
    assert payload["name"] == dashboard_smoke.SMOKE_REPLAY_NAME
    assert payload["description"] == dashboard_smoke.SMOKE_REPLAY_DESCRIPTION
    assert payload["scenario_id"] == "weather_closure"
    saved = json.loads(target.read_text(encoding="utf-8"))
    assert saved["name"] == dashboard_smoke.SMOKE_REPLAY_NAME
    assert saved["description"] == dashboard_smoke.SMOKE_REPLAY_DESCRIPTION
