from __future__ import annotations

import json
from importlib import resources
from pathlib import Path

from avn.core.state import (
    CorridorDefinition,
    DisturbanceDefinition,
    NodeDefinition,
    ScenarioDefinition,
    VehicleDefinition,
)


def _resource_base():
    return resources.files("avn.scenarios")


def list_scenarios() -> list[str]:
    return sorted(path.name[:-5] for path in _resource_base().iterdir() if path.name.endswith(".json"))


def load_scenario(identifier: str | Path) -> ScenarioDefinition:
    path = Path(identifier)
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        resource = _resource_base().joinpath(f"{identifier}.json")
        payload = json.loads(resource.read_text(encoding="utf-8"))
    return ScenarioDefinition(
        scenario_id=payload["scenario_id"],
        name=payload["name"],
        description=payload["description"],
        seed=int(payload.get("seed", 0)),
        duration_minutes=int(payload["duration_minutes"]),
        time_step_minutes=int(payload["time_step_minutes"]),
        recommended=bool(payload.get("recommended", False)),
        output_root=Path(payload.get("output_root", "outputs/avn")).resolve(),
        alert_thresholds=payload.get("alert_thresholds", {}),
        nodes=[NodeDefinition(**item) for item in payload["nodes"]],
        corridors=[CorridorDefinition(**item) for item in payload["corridors"]],
        vehicles=[VehicleDefinition(**item) for item in payload["vehicles"]],
        disturbances=[DisturbanceDefinition(**item) for item in payload.get("disturbances", [])],
    )
