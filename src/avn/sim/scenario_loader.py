from __future__ import annotations

import json
from dataclasses import MISSING, asdict, fields
from importlib import resources
from pathlib import Path
from typing import Any

from avn.core.policies import DEFAULT_POLICY_ID, get_policy_profile
from avn.core.state import (
    CorridorDefinition,
    DisturbanceDefinition,
    NodeDefinition,
    ScenarioDefinition,
    VehicleDefinition,
)


_DISTURBANCE_KINDS = {"node", "corridor"}
_TOP_LEVEL_REQUIRED_FIELDS = {
    "scenario_id",
    "name",
    "description",
    "duration_minutes",
    "time_step_minutes",
    "nodes",
    "corridors",
    "vehicles",
}


def _required_dataclass_fields(cls: type) -> set[str]:
    return {field.name for field in fields(cls) if field.default is MISSING and field.default_factory is MISSING}


_SECTION_SCHEMAS: dict[str, tuple[str, set[str]]] = {
    "nodes": ("node_id", _required_dataclass_fields(NodeDefinition)),
    "corridors": ("corridor_id", _required_dataclass_fields(CorridorDefinition)),
    "vehicles": ("vehicle_id", _required_dataclass_fields(VehicleDefinition)),
    "disturbances": ("disturbance_id", _required_dataclass_fields(DisturbanceDefinition)),
}


def _resource_base():
    return resources.files("avn.scenarios")


def list_scenarios() -> list[str]:
    return sorted(path.name[:-5] for path in _resource_base().iterdir() if path.name.endswith(".json"))


def _require_mapping(payload: object, *, source: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError(f"Scenario payload must be a JSON object: {source}")
    return payload


def _require_integer(value: object, *, field_name: str, source: str, minimum: int | None = None) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer in {source}.") from exc
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{field_name} must be >= {minimum} in {source}.")
    return parsed


def _validate_collection(payload: dict[str, Any], *, section: str, source: str) -> None:
    items = payload.get(section, [])
    if not isinstance(items, list):
        raise ValueError(f"{section} must be a list in {source}.")

    id_field, required_fields = _SECTION_SCHEMAS[section]
    seen_ids: set[str] = set()
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"{section}[{index}] must be an object in {source}.")

        missing = sorted(required_fields - set(item))
        if missing:
            raise ValueError(f"{section}[{index}] is missing required fields in {source}: {', '.join(missing)}")

        entity_id = str(item[id_field])
        if entity_id in seen_ids:
            raise ValueError(f"{section} contains duplicate {id_field} values in {source}: {entity_id}")
        seen_ids.add(entity_id)


def validate_scenario_payload(payload: object, *, source: str = "<scenario>") -> dict[str, Any]:
    scenario = _require_mapping(payload, source=source)

    missing = sorted(_TOP_LEVEL_REQUIRED_FIELDS - set(scenario))
    if missing:
        raise ValueError(f"Scenario payload is missing required top-level fields in {source}: {', '.join(missing)}")

    for field_name in ("scenario_id", "name", "description"):
        if not isinstance(scenario[field_name], str) or not scenario[field_name].strip():
            raise ValueError(f"{field_name} must be a non-empty string in {source}.")

    _require_integer(scenario["duration_minutes"], field_name="duration_minutes", source=source, minimum=1)
    _require_integer(scenario["time_step_minutes"], field_name="time_step_minutes", source=source, minimum=1)
    if "seed" in scenario:
        _require_integer(scenario["seed"], field_name="seed", source=source)

    if "recommended" in scenario and not isinstance(scenario["recommended"], bool):
        raise ValueError(f"recommended must be a boolean in {source}.")
    if "output_root" in scenario and not isinstance(scenario["output_root"], str):
        raise ValueError(f"output_root must be a string path in {source}.")
    if "policy_id" in scenario and not isinstance(scenario["policy_id"], str):
        raise ValueError(f"policy_id must be a string in {source}.")
    if "alert_thresholds" in scenario and not isinstance(scenario["alert_thresholds"], dict):
        raise ValueError(f"alert_thresholds must be an object in {source}.")

    for section in _SECTION_SCHEMAS:
        _validate_collection(scenario, section=section, source=source)

    node_ids = {str(node["node_id"]) for node in scenario["nodes"]}
    corridor_ids = {str(corridor["corridor_id"]) for corridor in scenario["corridors"]}

    for index, corridor in enumerate(scenario["corridors"]):
        if str(corridor["origin"]) not in node_ids or str(corridor["destination"]) not in node_ids:
            raise ValueError(
                f"corridors[{index}] references unknown nodes in {source}: "
                f"{corridor['origin']} -> {corridor['destination']}"
            )

    for index, vehicle in enumerate(scenario["vehicles"]):
        if str(vehicle["origin"]) not in node_ids or str(vehicle["destination"]) not in node_ids:
            raise ValueError(
                f"vehicles[{index}] references unknown nodes in {source}: "
                f"{vehicle['origin']} -> {vehicle['destination']}"
            )

    for index, disturbance in enumerate(scenario.get("disturbances", [])):
        kind = str(disturbance["kind"])
        if kind not in _DISTURBANCE_KINDS:
            raise ValueError(
                f"disturbances[{index}] has unsupported kind in {source}: {kind}. "
                f"Expected one of {sorted(_DISTURBANCE_KINDS)}."
            )
        target_pool = node_ids if kind == "node" else corridor_ids
        if str(disturbance["target_id"]) not in target_pool:
            raise ValueError(
                f"disturbances[{index}] references unknown {kind} target in {source}: {disturbance['target_id']}"
            )

    policy_id = str(scenario.get("policy_id", DEFAULT_POLICY_ID))
    get_policy_profile(policy_id)
    return scenario


def scenario_to_payload(scenario: ScenarioDefinition) -> dict[str, Any]:
    return {
        "scenario_id": scenario.scenario_id,
        "name": scenario.name,
        "description": scenario.description,
        "seed": scenario.seed,
        "duration_minutes": scenario.duration_minutes,
        "time_step_minutes": scenario.time_step_minutes,
        "recommended": scenario.recommended,
        "policy_id": scenario.policy_id,
        "output_root": str(scenario.output_root),
        "alert_thresholds": dict(scenario.alert_thresholds),
        "nodes": [asdict(node) for node in scenario.nodes],
        "corridors": [asdict(corridor) for corridor in scenario.corridors],
        "vehicles": [asdict(vehicle) for vehicle in scenario.vehicles],
        "disturbances": [asdict(disturbance) for disturbance in scenario.disturbances],
    }


def load_scenario(identifier: str | Path) -> ScenarioDefinition:
    path = Path(identifier)
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        source = str(path)
    else:
        resource = _resource_base().joinpath(f"{identifier}.json")
        payload = json.loads(resource.read_text(encoding="utf-8"))
        source = f"built-in scenario {identifier}"
    payload = validate_scenario_payload(payload, source=source)
    policy_id = str(payload.get("policy_id", DEFAULT_POLICY_ID))
    return ScenarioDefinition(
        scenario_id=payload["scenario_id"],
        name=payload["name"],
        description=payload["description"],
        seed=int(payload.get("seed", 0)),
        duration_minutes=int(payload["duration_minutes"]),
        time_step_minutes=int(payload["time_step_minutes"]),
        recommended=bool(payload.get("recommended", False)),
        output_root=Path(payload.get("output_root", "outputs/avn")).resolve(),
        policy_id=policy_id,
        alert_thresholds=payload.get("alert_thresholds", {}),
        nodes=[NodeDefinition(**item) for item in payload["nodes"]],
        corridors=[CorridorDefinition(**item) for item in payload["corridors"]],
        vehicles=[VehicleDefinition(**item) for item in payload["vehicles"]],
        disturbances=[DisturbanceDefinition(**item) for item in payload.get("disturbances", [])],
    )
