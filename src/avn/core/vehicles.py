from __future__ import annotations

from avn.core.state import VehicleRuntime


def sort_vehicle_queue(queue: list[str], vehicles: dict[str, VehicleRuntime]) -> list[str]:
    return sorted(
        queue,
        key=lambda vehicle_id: (
            vehicles[vehicle_id].priority_rank,
            vehicles[vehicle_id].release_minute,
            vehicles[vehicle_id].vehicle_id,
        ),
    )
