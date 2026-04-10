from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import networkx as nx

from .artifacts import V2_CONTRACT_VERSION, write_json
from .calibration import fit_bundle_to_config
from .config import load_reference_bundle
from .models import (
    Reservation,
    RuntimeCorridor,
    RuntimeNode,
    RuntimeRequest,
    RuntimeVehicle,
    ScenarioConfig,
    ScenarioRunResult,
)
from .validation import ensure_bundle_validation_passes
from .reporting import build_run_governance_artifacts, utc_timestamp, write_v2_artifact_bundle


PRIORITY_ORDER = {
    "emergency": 0,
    "public_service": 1,
    "cargo": 2,
    "routine": 3,
}


def _timestamp_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")


def _apply_override(config: ScenarioConfig, dotted_key: str, value: object) -> ScenarioConfig:
    import copy

    cloned = copy.deepcopy(config)
    target: object = cloned
    parts = dotted_key.split(".")
    for part in parts[:-1]:
        target = getattr(target, part)
    object.__setattr__(target, parts[-1], value)
    return cloned


class OperationalSimulator:
    def __init__(self, config: ScenarioConfig) -> None:
        self.config = config
        self.graph = nx.DiGraph()
        self.nodes = {record.node_id: RuntimeNode(record=record) for record in config.nodes}
        self.corridors = {record.corridor_id: RuntimeCorridor(record=record) for record in config.corridors}
        for record in config.corridors:
            self.graph.add_edge(record.origin, record.destination, corridor_id=record.corridor_id, weight=record.travel_minutes)
        self.vehicles = {
            record.vehicle_id: RuntimeVehicle(
                vehicle_id=record.vehicle_id,
                vehicle_class=record.vehicle_class,
                home_node=record.home_node,
                current_node=record.home_node,
                energy_capacity=record.energy_capacity,
                energy_remaining=record.energy_capacity,
                reserve_energy=record.reserve_energy,
                trust_state=record.trust_state,
                operator_required=record.operator_required,
            )
            for record in config.vehicles
        }
        self.requests = {
            record.request_id: RuntimeRequest(
                request_id=record.request_id,
                release_minute=record.release_minute,
                origin=record.origin,
                destination=record.destination,
                priority=record.priority,
                required_vehicle_class=record.required_vehicle_class,
                max_delay_minutes=record.max_delay_minutes,
            )
            for record in config.demand_requests
        }
        self.events: list[dict[str, Any]] = []
        self.metrics_history: list[dict[str, Any]] = []
        self.series_trace: list[dict[str, Any]] = []
        self.counters: Counter[str] = Counter()
        self.peak_queue_length = 0

    def _log(self, minute: int, event_type: str, **payload: Any) -> None:
        self.events.append({"minute": minute, "event_type": event_type, **payload})

    def _active_events(self, minute: int, *, target_type: str | None = None, target_id: str | None = None) -> list[Any]:
        matches = []
        for event in self.config.disruptions:
            if not event.active_at(minute):
                continue
            if target_type is not None and event.target_type not in {target_type, "global"}:
                continue
            if target_id is not None and event.target_type != "global" and event.target_id != target_id:
                continue
            matches.append(event)
        return matches

    def _node_open(self, minute: int, node_id: str) -> bool:
        return not any(event.effect_type == "close" for event in self._active_events(minute, target_type="node", target_id=node_id))

    def _corridor_open(self, minute: int, corridor_id: str) -> bool:
        return not any(event.effect_type == "close" for event in self._active_events(minute, target_type="corridor", target_id=corridor_id))

    def _travel_multiplier(self, minute: int, corridor_id: str) -> float:
        multiplier = self.config.calibration.travel_time_multiplier
        for event in self._active_events(minute, target_type="corridor", target_id=corridor_id):
            if event.effect_type == "travel_time_multiplier":
                multiplier *= float(event.value)
        for event in self._active_events(minute, target_type="global"):
            if event.effect_type == "travel_time_multiplier":
                multiplier *= float(event.value)
        return max(0.5, multiplier)

    def _energy_multiplier(self, minute: int, corridor_id: str, vehicle_id: str) -> float:
        multiplier = self.config.calibration.energy_cost_multiplier
        for event in self._active_events(minute, target_type="corridor", target_id=corridor_id):
            if event.effect_type == "energy_multiplier":
                multiplier *= float(event.value)
        for event in self._active_events(minute, target_type="vehicle", target_id=vehicle_id):
            if event.effect_type == "energy_multiplier":
                multiplier *= float(event.value)
        return max(0.5, multiplier)

    def _operator_delay(self, minute: int, vehicle: RuntimeVehicle) -> int:
        delay = self.config.dispatch_policy.operator_delay_minutes if vehicle.operator_required else 0
        for event in self._active_events(minute, target_type="vehicle", target_id=vehicle.vehicle_id):
            if event.effect_type == "operator_delay_minutes":
                delay += int(float(event.value))
        for event in self._active_events(minute, target_type="global"):
            if event.effect_type == "operator_delay_minutes":
                delay += int(float(event.value))
        return delay

    def _reservation_capacity(self, minute: int, corridor_id: str) -> int:
        corridor = self.corridors[corridor_id].record
        base_capacity = max(
            1,
            int(
                round(
                    corridor.capacity_per_hour
                    * corridor.reservation_window_minutes
                    / 60.0
                    * self.config.calibration.reservation_capacity_multiplier
                )
            ),
        )
        multiplier = 1.0
        for event in self._active_events(minute, target_type="corridor", target_id=corridor_id):
            if event.effect_type == "reservation_capacity_multiplier":
                multiplier *= float(event.value)
        for event in self._active_events(minute, target_type="global"):
            if event.effect_type == "reservation_capacity_multiplier":
                multiplier *= float(event.value)
        return max(1, int(round(base_capacity * multiplier)))

    def _node_service_multiplier(self, minute: int, node_id: str) -> float:
        multiplier = self.config.calibration.service_rate_multiplier
        for event in self._active_events(minute, target_type="node", target_id=node_id):
            if event.effect_type == "service_rate_multiplier":
                multiplier *= float(event.value)
            if event.effect_type == "dispatch_pause":
                multiplier *= 0.0
        for event in self._active_events(minute, target_type="global"):
            if event.effect_type == "service_rate_multiplier":
                multiplier *= float(event.value)
        return max(0.0, multiplier)

    def _effective_turnaround_minutes(self, minute: int, node_id: str) -> int:
        base_turnaround = self.nodes[node_id].record.turnaround_minutes
        multiplier = self._node_service_multiplier(minute, node_id)
        if multiplier <= 0.0:
            return base_turnaround * 4
        return max(1, int(round(base_turnaround / multiplier)))

    def _admission_allowed(self, minute: int, vehicle: RuntimeVehicle, request: RuntimeRequest) -> bool:
        if vehicle.trust_state not in {"trusted", "verified"}:
            self.counters["trust_denials"] += 1
            self._log(minute, "trust_denial", vehicle_id=vehicle.vehicle_id, request_id=request.request_id)
            return False
        if not self._node_open(minute, request.origin):
            return False
        if not self.config.dispatch_policy.degraded_dispatch_enabled:
            degraded = any(
                event.effect_type == "dispatch_pause"
                for event in self._active_events(minute, target_type="node", target_id=request.origin)
            )
            if degraded:
                self.counters["dispatch_pauses"] += 1
                return False
        return True

    def _find_path(self, minute: int, origin: str, destination: str, *, avoid_corridors: set[str] | None = None) -> list[str] | None:
        graph = nx.DiGraph()
        avoid = avoid_corridors or set()
        for node_id in self.nodes:
            if self._node_open(minute, node_id):
                graph.add_node(node_id)
        for corridor in self.corridors.values():
            if corridor.record.corridor_id in avoid or not self._corridor_open(minute, corridor.record.corridor_id):
                continue
            if corridor.record.origin not in graph or corridor.record.destination not in graph:
                continue
            graph.add_edge(
                corridor.record.origin,
                corridor.record.destination,
                corridor_id=corridor.record.corridor_id,
                weight=corridor.record.travel_minutes * self._travel_multiplier(minute, corridor.record.corridor_id),
            )
        try:
            return nx.shortest_path(graph, origin, destination, weight="weight")
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def _path_corridor_ids(self, path: list[str]) -> list[str]:
        return [self.graph[origin][destination]["corridor_id"] for origin, destination in zip(path[:-1], path[1:], strict=True)]

    def _project_reservations(
        self,
        *,
        minute: int,
        vehicle: RuntimeVehicle,
        request: RuntimeRequest,
        path: list[str],
    ) -> list[Reservation] | None:
        start_minute = minute + self._operator_delay(minute, vehicle)
        reservations: list[Reservation] = []
        for corridor_id in self._path_corridor_ids(path)[: self.config.reservation_policy.reservation_horizon_hops]:
            corridor = self.corridors[corridor_id].record
            travel_minutes = int(round(corridor.travel_minutes * self._travel_multiplier(start_minute, corridor_id)))
            end_minute = start_minute + max(travel_minutes, corridor.reservation_window_minutes)
            overlapping = sum(
                1 for existing in self.corridors[corridor_id].reservations if existing.overlaps(start_minute, end_minute)
            )
            if overlapping > self.config.reservation_policy.conflict_tolerance and overlapping >= self._reservation_capacity(start_minute, corridor_id):
                self.counters["reservation_conflicts"] += 1
                self._log(minute, "reservation_conflict", corridor_id=corridor_id, request_id=request.request_id)
                return None
            reservations.append(
                Reservation(
                    reservation_id=f"{request.request_id}:{corridor_id}:{start_minute}",
                    corridor_id=corridor_id,
                    vehicle_id=vehicle.vehicle_id,
                    request_id=request.request_id,
                    start_minute=start_minute,
                    end_minute=end_minute,
                )
            )
            start_minute += travel_minutes
        return reservations

    def _reserve_path(self, *, minute: int, vehicle: RuntimeVehicle, request: RuntimeRequest, path: list[str]) -> bool:
        reservations = self._project_reservations(minute=minute, vehicle=vehicle, request=request, path=path)
        if reservations is None:
            return False
        for reservation in reservations:
            self.corridors[reservation.corridor_id].reservations.append(reservation)
        vehicle.reserved_corridors = [reservation.corridor_id for reservation in reservations]
        return True

    def _release_used_reservation(self, vehicle: RuntimeVehicle, corridor_id: str) -> None:
        corridor = self.corridors[corridor_id]
        corridor.reservations = [
            reservation
            for reservation in corridor.reservations
            if not (reservation.vehicle_id == vehicle.vehicle_id and reservation.corridor_id == corridor_id)
        ]
        vehicle.reserved_corridors = [reserved for reserved in vehicle.reserved_corridors if reserved != corridor_id]

    def _dispatch_candidates(self, request: RuntimeRequest, minute: int) -> list[tuple[RuntimeVehicle, list[str], int]]:
        candidates: list[tuple[RuntimeVehicle, list[str], int]] = []
        for vehicle in self.vehicles.values():
            if vehicle.status != "idle" or vehicle.available_minute > minute:
                continue
            if vehicle.vehicle_class != request.required_vehicle_class:
                continue
            if not self._admission_allowed(minute, vehicle, request):
                continue
            reposition_path = [vehicle.current_node]
            reposition_minutes = 0
            if vehicle.current_node != request.origin:
                reposition_path = self._find_path(minute, vehicle.current_node, request.origin)
                if reposition_path is None:
                    continue
                reposition_minutes = int(
                    round(
                        sum(
                            self.corridors[corridor_id].record.travel_minutes
                            * self._travel_multiplier(minute, corridor_id)
                            for corridor_id in self._path_corridor_ids(reposition_path)
                        )
                    )
                )
            service_path = self._find_path(minute, request.origin, request.destination)
            if service_path is None:
                continue
            full_path = reposition_path + service_path[1:]
            candidates.append((vehicle, full_path, reposition_minutes))
        return sorted(candidates, key=lambda item: (item[2], item[0].operator_required, item[0].vehicle_id))

    def _dispatch_request(self, minute: int, request: RuntimeRequest) -> bool:
        for vehicle, path, reposition_minutes in self._dispatch_candidates(request, minute):
            avoid_corridors: set[str] = set()
            while True:
                candidate_path = path
                if avoid_corridors:
                    reposition_path = [vehicle.current_node]
                    if vehicle.current_node != request.origin:
                        reposition_path = self._find_path(
                            minute,
                            vehicle.current_node,
                            request.origin,
                            avoid_corridors=avoid_corridors,
                        )
                        if reposition_path is None:
                            break
                    service_path = self._find_path(
                        minute,
                        request.origin,
                        request.destination,
                        avoid_corridors=avoid_corridors,
                    )
                    if service_path is None:
                        break
                    candidate_path = reposition_path + service_path[1:]
                path = candidate_path
                if path is None:
                    break
                corridor_ids = self._path_corridor_ids(path)
                total_energy = sum(
                    self.corridors[corridor_id].record.energy_cost * self._energy_multiplier(minute, corridor_id, vehicle.vehicle_id)
                    for corridor_id in corridor_ids
                )
                if vehicle.energy_remaining - total_energy < max(vehicle.reserve_energy, self.config.contingency_policy.min_energy_reserve):
                    self.counters["energy_denials"] += 1
                    self._log(minute, "energy_denial", vehicle_id=vehicle.vehicle_id, request_id=request.request_id)
                    break
                if not self._reserve_path(minute=minute, vehicle=vehicle, request=request, path=path):
                    if not corridor_ids:
                        break
                    avoid_corridors.add(corridor_ids[0])
                    continue
                operator_delay = self._operator_delay(minute, vehicle)
                if operator_delay > 0:
                    self.counters["operator_overrides"] += 1
                vehicle.status = "planned_departure"
                vehicle.assigned_request_id = request.request_id
                vehicle.remaining_path = path[1:]
                vehicle.route_history.extend(path)
                vehicle.available_minute = minute + operator_delay
                request.status = "dispatched"
                request.assigned_vehicle_id = vehicle.vehicle_id
                request.dispatch_minute = minute
                self.counters["dispatched_requests"] += 1
                if reposition_minutes > 0:
                    self.counters["reposition_count"] += 1
                    self._log(
                        minute,
                        "vehicle_reposition_planned",
                        request_id=request.request_id,
                        vehicle_id=vehicle.vehicle_id,
                        reposition_minutes=reposition_minutes,
                        start_node=vehicle.current_node,
                        request_origin=request.origin,
                    )
                self._log(minute, "request_dispatched", request_id=request.request_id, vehicle_id=vehicle.vehicle_id, path=path)
                return True
        return False

    def _nearest_contingency_node(self, minute: int, origin: str) -> str | None:
        candidates = []
        for node in self.nodes.values():
            if node.record.contingency_slots <= node.active_contingency_occupancy or node.record.contingency_slots <= 0:
                continue
            path = self._find_path(minute, origin, node.record.node_id)
            if path is not None:
                candidates.append((len(path), node.record.node_id))
        if not candidates:
            return None
        candidates.sort()
        return candidates[0][1]

    def _divert_request(self, minute: int, request: RuntimeRequest, *, reason: str) -> None:
        node_id = self._nearest_contingency_node(minute, request.origin)
        if node_id is None:
            request.status = "cancelled"
            request.failure_reason = reason
            self.counters["cancelled_requests"] += 1
            self._log(minute, "request_cancelled", request_id=request.request_id, reason=reason)
            return
        self.nodes[node_id].active_contingency_occupancy += 1
        request.status = "diverted"
        request.diversion_node = node_id
        request.failure_reason = reason
        request.completion_minute = minute
        self.counters["diversion_count"] += 1
        self.counters["contingency_activations"] += 1
        self._log(minute, "request_diverted", request_id=request.request_id, node_id=node_id, reason=reason)

    def _plan_next_leg(self, minute: int, vehicle: RuntimeVehicle, request: RuntimeRequest) -> None:
        if vehicle.current_node == request.destination:
            vehicle.status = "servicing"
            vehicle.available_minute = minute + self._effective_turnaround_minutes(minute, vehicle.current_node)
            request.status = "completed"
            request.completion_minute = minute
            self.counters["completed_requests"] += 1
            self._log(minute, "request_completed", request_id=request.request_id, vehicle_id=vehicle.vehicle_id)
            return
        path = self._find_path(minute, vehicle.current_node, request.destination)
        if path is None:
            if request.reroute_count >= self.config.dispatch_policy.max_reroutes:
                self._divert_request(minute, request, reason="reroute_exhausted")
                vehicle.status = "idle"
                vehicle.assigned_request_id = None
                return
            request.reroute_count += 1
            vehicle.reroute_count += 1
            self.counters["reroute_count"] += 1
            self._log(minute, "reroute_attempt", request_id=request.request_id, vehicle_id=vehicle.vehicle_id)
            return
        if not self._reserve_path(minute=minute, vehicle=vehicle, request=request, path=path):
            if request.reroute_count >= self.config.dispatch_policy.max_reroutes:
                self._divert_request(minute, request, reason="reservation_failure")
                vehicle.status = "idle"
                vehicle.assigned_request_id = None
                return
            request.reroute_count += 1
            vehicle.reroute_count += 1
            self.counters["reroute_count"] += 1
            self._log(minute, "reroute_due_to_reservation", request_id=request.request_id, vehicle_id=vehicle.vehicle_id)
            return
        vehicle.remaining_path = path[1:]
        vehicle.status = "planned_departure"
        vehicle.available_minute = minute + self.config.contingency_policy.reroute_buffer_minutes

    def _advance_vehicle(self, minute: int, vehicle: RuntimeVehicle) -> None:
        request = None if vehicle.assigned_request_id is None else self.requests[vehicle.assigned_request_id]
        if vehicle.status == "servicing" and vehicle.available_minute <= minute:
            vehicle.status = "idle"
            vehicle.assigned_request_id = None
            vehicle.remaining_path = []
            vehicle.next_corridor_id = None
            vehicle.next_arrival_minute = None
            vehicle.route_history = []
            return
        if vehicle.status == "planned_departure" and vehicle.available_minute <= minute and vehicle.remaining_path:
            next_node = vehicle.remaining_path[0]
            corridor_id = self.graph[vehicle.current_node][next_node]["corridor_id"]
            corridor = self.corridors[corridor_id].record
            vehicle.next_corridor_id = corridor_id
            vehicle.status = "enroute"
            vehicle.next_arrival_minute = minute + int(round(corridor.travel_minutes * self._travel_multiplier(minute, corridor_id)))
            self._log(minute, "vehicle_departed", vehicle_id=vehicle.vehicle_id, corridor_id=corridor_id)
            return
        if vehicle.status == "enroute" and vehicle.next_arrival_minute is not None and vehicle.next_arrival_minute <= minute:
            next_node = vehicle.remaining_path.pop(0)
            corridor_id = vehicle.next_corridor_id or ""
            corridor = self.corridors[corridor_id].record
            vehicle.current_node = next_node
            vehicle.energy_remaining -= corridor.energy_cost * self._energy_multiplier(minute, corridor_id, vehicle.vehicle_id)
            self._release_used_reservation(vehicle, corridor_id)
            vehicle.next_corridor_id = None
            vehicle.next_arrival_minute = None
            self._log(minute, "vehicle_arrived", vehicle_id=vehicle.vehicle_id, node_id=next_node)
            if request is not None:
                self._plan_next_leg(minute, vehicle, request)

    def _record_metrics(self, minute: int) -> None:
        pending_count = sum(1 for request in self.requests.values() if request.status == "pending")
        self.peak_queue_length = max(self.peak_queue_length, pending_count)
        self.metrics_history.append(
            {
                "minute": minute,
                "pending_requests": pending_count,
                "avg_queue": round(pending_count / max(1, len(self.nodes)), 4),
                "diversion_count": self.counters["diversion_count"],
            }
        )
        for node in self.nodes.values():
            self.series_trace.append(
                {
                    "scope": "node",
                    "entity_id": node.record.node_id,
                    "minute": minute,
                    "metric_key": "queue_length",
                    "value": node.queue_length,
                }
            )
            self.series_trace.append(
                {
                    "scope": "node",
                    "entity_id": node.record.node_id,
                    "minute": minute,
                    "metric_key": "dispatch_credit",
                    "value": round(node.dispatch_credit, 6),
                }
            )
        for corridor in self.corridors.values():
            self.series_trace.append(
                {
                    "scope": "corridor",
                    "entity_id": corridor.record.corridor_id,
                    "minute": minute,
                    "metric_key": "reservation_count",
                    "value": len(corridor.reservations),
                }
            )
        self.series_trace.append(
            {
                "scope": "network",
                "entity_id": "all",
                "minute": minute,
                "metric_key": "pending_requests",
                "value": pending_count,
            }
        )

    def _refresh_queue_lengths(self, minute: int) -> None:
        for node in self.nodes.values():
            node.queue_length = 0
        for request in self.requests.values():
            if request.status == "pending" and request.release_minute <= minute:
                self.nodes[request.origin].queue_length += 1

    def _enforce_queue_caps(self, minute: int) -> None:
        for node_id, node in self.nodes.items():
            overflow = max(0, node.queue_length - node.record.queue_capacity)
            if overflow <= 0:
                continue
            pending_requests = sorted(
                [
                    request
                    for request in self.requests.values()
                    if request.status == "pending" and request.release_minute <= minute and request.origin == node_id
                ],
                key=lambda item: (
                    PRIORITY_ORDER.get(item.priority, 99),
                    item.release_minute,
                    item.request_id,
                ),
            )
            for request in reversed(pending_requests[-overflow:]):
                self.counters["queue_overflow_count"] += 1
                self._log(minute, "queue_overflow", node_id=node_id, request_id=request.request_id)
                self._divert_request(minute, request, reason="queue_overflow")

    def _classify_failure_chain(self) -> tuple[str, dict[str, int]]:
        counts = Counter(event["event_type"] for event in self.events)
        if counts["queue_overflow"] > 0:
            return "dispatch_queue_collapse", dict(counts)
        if counts["reservation_conflict"] > 0 or counts["reroute_due_to_reservation"] > 0:
            return "dispatch_reservation_breakdown", dict(counts)
        if counts["request_diverted"] > 0:
            return "contingency_saturation", dict(counts)
        if counts["trust_denial"] > 0:
            return "trust_admission_breakdown", dict(counts)
        if counts["energy_denial"] > 0:
            return "energy_reachability_breakdown", dict(counts)
        return "bounded_operational_flow", dict(counts)

    def _event_summary(self) -> list[dict[str, Any]]:
        grouped: dict[str, list[int]] = {}
        for event in self.events:
            grouped.setdefault(event["event_type"], []).append(int(event["minute"]))
        return [
            {
                "event_type": event_type,
                "count": len(minutes),
                "first_minute": min(minutes),
                "last_minute": max(minutes),
            }
            for event_type, minutes in sorted(grouped.items())
        ]

    def run(self, *, output_root: Path | None = None) -> ScenarioRunResult:
        run_id = f"{self.config.outputs.artifact_prefix}_{self.config.scenario_name}_{_timestamp_id()}"
        output_dir = (output_root or self.config.outputs.root) / run_id
        output_dir.mkdir(parents=True, exist_ok=False)
        self._log(0, "scenario_start", scenario_name=self.config.scenario_name)

        for minute in range(0, self.config.duration_minutes + 1, self.config.time_step_minutes):
            for node in self.nodes.values():
                if self._node_open(minute, node.record.node_id):
                    node.dispatch_credit += (
                        node.record.service_rate_per_hour / 60.0 * self._node_service_multiplier(minute, node.record.node_id)
                    )
            for vehicle in self.vehicles.values():
                self._advance_vehicle(minute, vehicle)
            self._refresh_queue_lengths(minute)
            self._enforce_queue_caps(minute)
            for request in sorted(
                self.requests.values(),
                key=lambda item: (item.release_minute, PRIORITY_ORDER.get(item.priority, 99), item.request_id),
            ):
                if request.release_minute > minute or request.status != "pending":
                    continue
                if minute - request.release_minute > min(request.max_delay_minutes, self.config.dispatch_policy.max_wait_minutes):
                    self._divert_request(minute, request, reason="wait_budget_exceeded")
                    continue
                if self.nodes[request.origin].dispatch_credit < 1.0:
                    continue
                if self._dispatch_request(minute, request):
                    self.nodes[request.origin].dispatch_credit = max(0.0, self.nodes[request.origin].dispatch_credit - 1.0)
            self._record_metrics(minute)

        events_path = write_json(output_dir / "event_log.v2.json", {"contract_version": V2_CONTRACT_VERSION, "events": self.events})
        backtest_trace_path = write_json(
            output_dir / "backtest_trace.v2.json",
            {
                "id": f"{run_id}:backtest_trace",
                "artifact_type": "backtest_trace",
                "contract_version": V2_CONTRACT_VERSION,
                "generated_at": utc_timestamp(),
                "scenario_name": self.config.scenario_name,
                "metrics_history": self.metrics_history,
                "event_summary": self._event_summary(),
                "series_trace": self.series_trace,
            },
        )
        completed_requests = sum(1 for request in self.requests.values() if request.status == "completed")
        cancelled_requests = sum(1 for request in self.requests.values() if request.status == "cancelled")
        completed_with_timestamps = [request for request in self.requests.values() if request.completion_minute is not None]
        mean_completion = round(
            sum(request.completion_minute or request.release_minute for request in completed_with_timestamps)
            / max(1, len(completed_with_timestamps)),
            4,
        )
        avg_delay = round(
            sum(max(0, (request.dispatch_minute or request.release_minute) - request.release_minute) for request in self.requests.values())
            / max(1, len(self.requests)),
            4,
        )
        dominant_failure_chain, classification_evidence = self._classify_failure_chain()
        summary = {
            "id": f"{run_id}:run_summary",
            "artifact_type": "run_summary",
            "contract_version": V2_CONTRACT_VERSION,
            "generated_at": utc_timestamp(),
            "run_id": run_id,
            "scenario_name": self.config.scenario_name,
            "scenario_path": str(self.config.scenario_path.resolve()),
            "duration_minutes": self.config.duration_minutes,
            "completed_requests": completed_requests,
            "cancelled_requests": cancelled_requests,
            "diversion_count": self.counters["diversion_count"],
            "contingency_activations": self.counters["contingency_activations"],
            "reservation_conflicts": self.counters["reservation_conflicts"],
            "reroute_count": self.counters["reroute_count"],
            "reposition_count": self.counters["reposition_count"],
            "queue_overflow_count": self.counters["queue_overflow_count"],
            "peak_queue_length": self.peak_queue_length,
            "operator_overrides": self.counters["operator_overrides"],
            "trust_denials": self.counters["trust_denials"],
            "avg_delay_minutes": avg_delay,
            "mean_completion_minute": mean_completion,
            "dispatch_policy": {
                "max_wait_minutes": self.config.dispatch_policy.max_wait_minutes,
                "operator_delay_minutes": self.config.dispatch_policy.operator_delay_minutes,
                "max_reroutes": self.config.dispatch_policy.max_reroutes,
            },
            "reservation_policy": {
                "lookahead_minutes": self.config.reservation_policy.lookahead_minutes,
                "reservation_horizon_hops": self.config.reservation_policy.reservation_horizon_hops,
                "conflict_tolerance": self.config.reservation_policy.conflict_tolerance,
            },
            "contingency_policy": {
                "min_energy_reserve": self.config.contingency_policy.min_energy_reserve,
                "diversion_limit": self.config.contingency_policy.diversion_limit,
            },
            "dominant_failure_chain": dominant_failure_chain,
            "classification_evidence": classification_evidence,
            "request_outcomes": {
                request.request_id: {
                    "status": request.status,
                    "assigned_vehicle_id": request.assigned_vehicle_id,
                    "dispatch_minute": request.dispatch_minute,
                    "completion_minute": request.completion_minute,
                    "diversion_node": request.diversion_node,
                    "failure_reason": request.failure_reason,
                }
                for request in sorted(self.requests.values(), key=lambda item: item.request_id)
            },
        }

        calibration_report = None
        bundle_validation_report = None
        if self.config.calibration.enabled and self.config.calibration.bundle is not None:
            bundle = load_reference_bundle(self.config.calibration.bundle)
            bundle_validation_report = ensure_bundle_validation_passes(bundle, report_id=run_id)
            calibration_report = fit_bundle_to_config(
                bundle,
                self.config,
                output_root=output_dir / "calibration_fit",
                report_id=run_id,
                bundle_validation_report=bundle_validation_report,
            )

        threshold_ledger, hazard_ledger, promotion_decisions, contradictions = build_run_governance_artifacts(
            run_id=run_id,
            scenario_name=self.config.scenario_name,
            summary=summary,
            calibration_report=calibration_report,
            bundle_validation_report=bundle_validation_report,
        )
        artifact_paths = write_v2_artifact_bundle(
            output_dir=output_dir,
            scenario_name=self.config.scenario_name,
            run_id=run_id,
            summary=summary,
            backtest_trace_path=backtest_trace_path,
            threshold_ledger=threshold_ledger,
            hazard_ledger=hazard_ledger,
            promotion_decisions=promotion_decisions,
            contradictions=contradictions,
            calibration_report=calibration_report,
            bundle_validation_report=bundle_validation_report,
        )
        return ScenarioRunResult(
            output_dir=output_dir.resolve(),
            events_path=events_path.resolve(),
            backtest_trace_path=backtest_trace_path.resolve(),
            manifest_path=artifact_paths["manifest_path"].resolve(),
            run_summary_path=artifact_paths["run_summary_path"].resolve(),
            threshold_ledger_path=artifact_paths["threshold_ledger_path"].resolve(),
            hazard_ledger_path=artifact_paths["hazard_ledger_path"].resolve(),
            promotion_decisions_path=artifact_paths["promotion_decisions_path"].resolve(),
            contradictions_path=artifact_paths["contradictions_path"].resolve(),
            report_bundle_path=artifact_paths["report_bundle_path"].resolve(),
            calibration_report_path=None if artifact_paths["calibration_report_path"] is None else artifact_paths["calibration_report_path"].resolve(),
            bundle_validation_path=None if artifact_paths["bundle_validation_path"] is None else artifact_paths["bundle_validation_path"].resolve(),
            summary=summary,
        )


def run_scenario(config: ScenarioConfig, *, output_root: Path | None = None) -> ScenarioRunResult:
    return OperationalSimulator(config).run(output_root=output_root)
