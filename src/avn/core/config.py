from __future__ import annotations

import csv
import tomllib
from pathlib import Path

from avn.core.models import (
    CorridorConfig,
    DisturbanceScheduleEntry,
    DisturbanceState,
    NodeConfig,
    SimulationConfig,
    VehicleConfig,
)


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _load_nodes(path: Path) -> list[NodeConfig]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            NodeConfig(
                node_id=row["node_id"],
                node_type=row["node_type"],
                service_rate=float(row["service_rate"]),
                contingency_capacity=int(row["contingency_capacity"]),
                occupancy=int(row.get("occupancy", 0) or 0),
                operational_state=row.get("operational_state", "normal") or "normal",
            )
            for row in reader
        ]


def _load_corridors(path: Path) -> list[CorridorConfig]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            CorridorConfig(
                corridor_id=row["corridor_id"],
                origin=row["origin"],
                destination=row["destination"],
                length=float(row["length"]),
                free_flow_speed=float(row["free_flow_speed"]),
                base_capacity=float(row["base_capacity"]),
                required_separation=float(row["required_separation"]),
            )
            for row in reader
        ]


def _load_vehicles(path: Path) -> list[VehicleConfig]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            VehicleConfig(
                vehicle_id=row["vehicle_id"],
                mission_class=row["mission_class"],
                route=[segment.strip() for segment in row["route"].split("|") if segment.strip()],
                reserve_energy=float(row["reserve_energy"]),
                status=row.get("status", "queued") or "queued",
            )
            for row in reader
        ]


def load_simulation_config(config_path: str | Path) -> SimulationConfig:
    path = Path(config_path).resolve()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    base_dir = path.parent
    data_sources = raw["data_sources"]
    nodes_path = _resolve_path(base_dir, data_sources["nodes"])
    corridors_path = _resolve_path(base_dir, data_sources["corridors"])
    vehicles_path = _resolve_path(base_dir, data_sources["vehicles"])
    output_root = _resolve_path(base_dir, raw["output_root"])

    disturbance_base_raw = raw["disturbances"]["base"]
    disturbance_base = DisturbanceState(
        weather_severity=float(disturbance_base_raw["weather_severity"]),
        comms_reliability=float(disturbance_base_raw["comms_reliability"]),
    )
    disturbance_schedule = [
        DisturbanceScheduleEntry(
            start_minute=int(entry["start_minute"]),
            weather_severity=float(entry["weather_severity"]),
            comms_reliability=float(entry["comms_reliability"]),
        )
        for entry in raw["disturbances"].get("schedule", [])
    ]

    return SimulationConfig(
        scenario_name=raw["scenario_name"],
        description=raw["description"],
        time_step_minutes=int(raw["time_step_minutes"]),
        duration_minutes=int(raw["duration_minutes"]),
        output_root=output_root,
        nodes=_load_nodes(nodes_path),
        corridors=_load_corridors(corridors_path),
        vehicles=_load_vehicles(vehicles_path),
        disturbance_base=disturbance_base,
        disturbance_schedule=disturbance_schedule,
    )

