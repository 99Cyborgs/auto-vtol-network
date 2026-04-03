from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import random

from avn.comms import is_state_stale, should_activate_lost_link, stale_exposure_increment, update_information_age
from avn.contingency import compute_reachable_landing_options
from avn.core.config import load_simulation_config
from avn.core.models import (
    AdmissibilityConfig,
    DisturbanceState,
    InfrastructureEventConfig,
    MetricsSnapshot,
    PhysicsModelConfig,
    SimulationConfig,
    SupplierState,
    VehicleInjectionConfig,
)
from avn.disturbances.timeline import DisturbanceTimeline
from avn.failure_classification import classify_failure
from avn.failures import classify_failure_mode
from avn.metrics.recorder import MetricsRecorder
from avn.network.graph import VTOLNetwork
from avn.physics.admissibility import AdmissibilityResult, evaluate_admissibility
from avn.physics.disturbance_model import DisturbanceResponse, compute_disturbance_response
from avn.physics.dynamics import approximate_reserve_energy_drain
from avn.physics.phase_detection import detect_phase_events
from avn.physics.state_mapping import PhysicsStateSample, map_engine_state
from avn.trust import (
    can_file_intent,
    can_receive_reservation,
    is_quarantined,
    normalize_trust_state,
    requires_operator_override,
    trust_service_priority,
)
from avn.vehicle.fleet import Vehicle, build_fleet
from avn.viz.plots import generate_plots


@dataclass(slots=True)
class SimulationResult:
    scenario_name: str
    output_dir: Path
    metrics_path: Path
    event_log_path: Path
    run_summary_path: Path
    threshold_summary_path: Path
    plot_paths: list[Path]
    summary: dict[str, float | int | str]


class SimulationEngine:
    def __init__(self, config: SimulationConfig) -> None:
        self.config = config
        self.network = VTOLNetwork.from_config(config.nodes, config.corridors, config.modifiers)
        self.vehicles: list[Vehicle] = build_fleet(config.vehicles, config.modifiers.demand_multiplier)
        self.timeline = DisturbanceTimeline(config.disturbance_base, config.disturbance_schedule)
        self.recorder = MetricsRecorder()
        self.rng = random.Random(config.seed)
        self.physics_model: PhysicsModelConfig = config.physics_model
        self.admissibility_config: AdmissibilityConfig = config.admissibility
        self.suppliers = {
            supplier.supplier_id: SupplierState(
                supplier_id=supplier.supplier_id,
                trust_state=supplier.trust_state,
                supplier_type=supplier.supplier_type,
            )
            for supplier in config.suppliers
        }

        self._applied_trust_events: set[int] = set()
        self._applied_injections: set[int] = set()
        self._pending_supplier_propagations: list[dict[str, int | str]] = []
        self._node_effects: dict[str, dict[str, float | bool | int]] = {}
        self._corridor_effects: dict[str, dict[str, bool]] = {}

        self.unsafe_admission_count = 0
        self.quarantine_event_count = 0
        self.revocation_event_count = 0
        self.delayed_reroute_count = 0
        self.lost_link_fallback_activations = 0
        self.reservation_invalidations = 0
        self.no_admissible_landing_events = 0
        self.operator_intervention_count = 0
        self.degraded_mode_dwell_time = 0.0
        self.divert_attempt_count = 0
        self.divert_success_count = 0
        self.trust_denied_dispatches = 0
        self.total_dispatch_attempts = 0
        self.false_quarantine_count = 0
        self.revocation_propagation_delays: list[int] = []

        self.safe_region_first_violation_time: int | None = None
        self.safe_region_first_violation_cause: str | None = None
        self.safe_region_cumulative_durations: dict[str, float] = defaultdict(float)

        self.trust_distribution_over_time: list[dict[str, int | float]] = []
        self.information_age_distribution_over_time: list[dict[str, int | float]] = []
        self.landing_option_distribution_over_time: list[dict[str, int | float]] = []
        self.physics_time_series: list[PhysicsStateSample] = []
        self.disturbance_responses: list[DisturbanceResponse] = []
        self.admissibility_time_series: list[AdmissibilityResult] = []

        self.last_metrics: dict[str, float | int | str] = {}
        self._randomize_closures()

    def run(self) -> SimulationResult:
        current_minute = 0
        time_step = self.config.time_step_minutes
        output_dir = self._create_output_dir()

        self.recorder.record_event(
            0,
            "scenario_start",
            scenario=self.config.scenario_name,
            seed=self.config.seed,
        )

        initial_disturbance = self._disturbance_state_at(0)
        self._apply_timed_events(0)
        self.network.prepare_step(initial_disturbance, time_step, accrue_capacity=False)
        self._refresh_node_states(initial_disturbance)
        self._update_vehicle_comms_and_trust(0, initial_disturbance, time_step)
        self._update_landing_reachability(0, initial_disturbance)
        self.network.finalize_step(time_step)
        self._update_physics_control_layer(0, initial_disturbance)
        self._record_metrics(0, initial_disturbance)
        self._evaluate_safe_region(0, 0)

        while current_minute < self.config.duration_minutes:
            disturbance = self._disturbance_state_at(current_minute)
            self._apply_timed_events(current_minute)
            self.network.prepare_step(disturbance, time_step)
            self._refresh_node_states(disturbance)
            self._update_vehicle_comms_and_trust(current_minute, disturbance, time_step)
            self._dispatch_vehicles(current_minute, disturbance)
            self._move_vehicles(current_minute + time_step, disturbance)
            self._apply_idle_energy_drain(disturbance)
            self._refresh_node_states(disturbance)
            self._update_landing_reachability(current_minute, disturbance)
            self.network.finalize_step(time_step)
            current_minute += time_step
            current_disturbance = self._disturbance_state_at(current_minute)
            self._update_physics_control_layer(current_minute, current_disturbance)
            self._record_metrics(current_minute, current_disturbance)
            self._evaluate_safe_region(current_minute, time_step)

        self.recorder.record_event(
            current_minute,
            "scenario_complete",
            completed_vehicles=sum(1 for vehicle in self.vehicles if vehicle.state.status == "completed"),
            no_admissible_landing_events=self.no_admissible_landing_events,
        )

        metrics_path = self.recorder.write_metrics_csv(output_dir)
        event_log_path = self.recorder.write_event_log(output_dir)
        summary = self._build_summary()
        threshold_summary = self._build_threshold_summary(summary)
        run_summary_path = self.recorder.write_json(output_dir, "run_summary.json", summary)
        threshold_summary_path = self.recorder.write_json(output_dir, "threshold_summary.json", threshold_summary)
        plot_paths = generate_plots(self.recorder.snapshots, output_dir)

        return SimulationResult(
            scenario_name=self.config.scenario_name,
            output_dir=output_dir,
            metrics_path=metrics_path,
            event_log_path=event_log_path,
            run_summary_path=run_summary_path,
            threshold_summary_path=threshold_summary_path,
            plot_paths=plot_paths,
            summary=summary,
        )

    def _create_output_dir(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        output_dir = self.config.output_root / f"{self.config.scenario_name}_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=False)
        return output_dir

    def _randomize_closures(self) -> None:
        probability = self.config.modifiers.closure_probability
        if probability <= 0.0:
            return

        for node in self.network.contingency_nodes():
            if self.rng.random() < probability:
                self.config.infrastructure_events.append(
                    InfrastructureEventConfig(
                        start_minute=0,
                        end_minute=self.config.duration_minutes,
                        target_type="node",
                        target_id=node.state.node_id,
                        state="closed",
                    )
                )

    def _disturbance_state_at(self, minute: int) -> DisturbanceState:
        base = self.timeline.state_at(minute)
        return DisturbanceState(
            weather_severity=min(1.0, base.weather_severity * self.config.modifiers.weather_multiplier),
            comms_reliability=max(
                0.0,
                min(1.0, base.comms_reliability * self.config.modifiers.comms_reliability_multiplier),
            ),
            comms_latency_minutes=max(0.0, base.comms_latency_minutes * self.config.modifiers.latency_multiplier),
            message_drop_probability=max(
                0.0,
                min(1.0, base.message_drop_probability * self.config.modifiers.drop_probability_multiplier),
            ),
            stale_after_minutes=base.stale_after_minutes,
            reroute_delay_minutes=base.reroute_delay_minutes,
            low_bandwidth_threshold_minutes=base.low_bandwidth_threshold_minutes,
            node_service_multiplier=max(0.0, base.node_service_multiplier),
        )

    def _apply_timed_events(self, current_minute: int) -> None:
        self._apply_infrastructure_events(current_minute)
        self._apply_trust_events(current_minute)
        self._apply_vehicle_injections(current_minute)

    def _apply_infrastructure_events(self, current_minute: int) -> None:
        node_effects: dict[str, dict[str, float | bool | int]] = defaultdict(
            lambda: {"closed": False, "service_multiplier": 1.0, "slots_delta": 0}
        )
        corridor_effects: dict[str, dict[str, bool]] = defaultdict(lambda: {"closed": False})

        for event in self.config.infrastructure_events:
            if not (event.start_minute <= current_minute < event.end_minute):
                continue
            if event.target_type == "node":
                effect = node_effects[event.target_id]
                effect["closed"] = bool(effect["closed"]) or event.state == "closed"
                effect["service_multiplier"] = float(effect["service_multiplier"]) * event.service_multiplier
                effect["slots_delta"] = int(effect["slots_delta"]) + event.contingency_slots_delta
            elif event.target_type == "corridor":
                effect = corridor_effects[event.target_id]
                effect["closed"] = bool(effect["closed"]) or event.state == "closed"

        self._node_effects = node_effects
        self._corridor_effects = corridor_effects
        for corridor_id in self.network.corridors:
            closed = bool(corridor_effects.get(corridor_id, {}).get("closed", False))
            self.network.set_corridor_closed(corridor_id, closed=closed)

    def _apply_trust_events(self, current_minute: int) -> None:
        for index, event in enumerate(self.config.trust_events):
            if index in self._applied_trust_events or event.start_minute != current_minute:
                continue
            self._applied_trust_events.add(index)
            self._apply_trust_transition(
                current_minute,
                event.target_type,
                event.target_id,
                event.resulting_state,
                event.trigger,
                event.propagation_delay_minutes,
            )

        remaining: list[dict[str, int | str]] = []
        for pending in self._pending_supplier_propagations:
            if int(pending["apply_minute"]) > current_minute:
                remaining.append(pending)
                continue
            self._apply_direct_trust_state(
                current_minute,
                str(pending["target_type"]),
                str(pending["target_id"]),
                str(pending["resulting_state"]),
                str(pending["trigger"]),
            )
            self.revocation_propagation_delays.append(int(pending["delay"]))
        self._pending_supplier_propagations = remaining

    def _apply_trust_transition(
        self,
        current_minute: int,
        target_type: str,
        target_id: str,
        resulting_state: str,
        trigger: str,
        propagation_delay_minutes: int,
    ) -> None:
        if target_type == "supplier":
            supplier = self.suppliers[target_id]
            supplier.trust_state = normalize_trust_state(resulting_state)
            supplier.last_event_trigger = trigger
            self.recorder.record_event(
                current_minute,
                "supplier_trust_transition",
                supplier_id=target_id,
                trust_state=supplier.trust_state,
                trigger=trigger,
            )
            for vehicle in self.vehicles:
                if vehicle.state.supplier_id != target_id:
                    continue
                self._pending_supplier_propagations.append(
                    {
                        "apply_minute": current_minute + propagation_delay_minutes,
                        "target_type": "vehicle",
                        "target_id": vehicle.state.id,
                        "resulting_state": supplier.trust_state,
                        "trigger": trigger,
                        "delay": propagation_delay_minutes,
                    }
                )
            for node in self.network.nodes.values():
                if node.state.supplier_id != target_id:
                    continue
                self._pending_supplier_propagations.append(
                    {
                        "apply_minute": current_minute + propagation_delay_minutes,
                        "target_type": "node",
                        "target_id": node.state.node_id,
                        "resulting_state": supplier.trust_state,
                        "trigger": trigger,
                        "delay": propagation_delay_minutes,
                    }
                )
            return

        self._apply_direct_trust_state(current_minute, target_type, target_id, resulting_state, trigger)

    def _apply_direct_trust_state(
        self,
        current_minute: int,
        target_type: str,
        target_id: str,
        resulting_state: str,
        trigger: str,
    ) -> None:
        resulting_state = normalize_trust_state(resulting_state)
        if target_type == "vehicle":
            vehicle = self._vehicle_by_id(target_id)
            if vehicle is None:
                return
            previous = vehicle.state.trust_state
            vehicle.state.trust_state = resulting_state
            if resulting_state == "quarantined" and previous != "quarantined":
                self.quarantine_event_count += 1
                vehicle.state.status = "holding"
                vehicle.state.quarantine_reason = trigger
            if resulting_state == "revoked" and previous != "revoked":
                self.revocation_event_count += 1
                vehicle.state.status = "holding"
                vehicle.state.quarantine_reason = trigger
            if trigger == "operator_override":
                self._record_operator_intervention(current_minute, vehicle.state.id, trigger)
            self.recorder.record_event(
                current_minute,
                "vehicle_trust_transition",
                vehicle_id=vehicle.state.id,
                from_state=previous,
                to_state=resulting_state,
                trigger=trigger,
            )
        elif target_type == "node":
            node = self.network.get_node(target_id)
            previous = node.state.trust_state
            node.state.trust_state = resulting_state
            self.recorder.record_event(
                current_minute,
                "node_trust_transition",
                node_id=node.state.node_id,
                from_state=previous,
                to_state=resulting_state,
                trigger=trigger,
            )

    def _apply_vehicle_injections(self, current_minute: int) -> None:
        for index, injection in enumerate(self.config.vehicle_injections):
            if index in self._applied_injections or injection.start_minute != current_minute:
                continue
            self._applied_injections.add(index)
            self.vehicles.append(Vehicle.from_config(self._vehicle_config_from_injection(injection)))
            self.recorder.record_event(
                current_minute,
                "vehicle_injected",
                vehicle_id=injection.vehicle_id,
                route=injection.route,
                supplier_id=injection.supplier_id,
                trust_state=injection.trust_state,
                note=injection.note,
            )

    def _vehicle_config_from_injection(self, injection: VehicleInjectionConfig):
        from avn.core.models import VehicleConfig

        return VehicleConfig(
            vehicle_id=injection.vehicle_id,
            mission_class=injection.mission_class,
            route=injection.route,
            reserve_energy=injection.reserve_energy,
            status=injection.status,
            supplier_id=injection.supplier_id,
            trust_state=injection.trust_state,
        )

    def _refresh_node_states(self, disturbance: DisturbanceState) -> None:
        occupancy_by_node: dict[str, int] = defaultdict(int)
        queue_by_node: dict[str, int] = defaultdict(int)

        for vehicle in self.vehicles:
            if vehicle.state.status in {"queued", "holding", "completed"} and vehicle.state.current_location in self.network.nodes:
                occupancy_by_node[vehicle.state.current_location] += 1
            if vehicle.state.status == "queued" and vehicle.state.current_location in self.network.nodes:
                queue_by_node[vehicle.state.current_location] += 1

        for node_id, node in self.network.nodes.items():
            node_effect = self._node_effects.get(node_id, {})
            service_multiplier = disturbance.node_service_multiplier * float(node_effect.get("service_multiplier", 1.0))
            if node.state.trust_state == "degraded":
                service_multiplier *= 0.85
            elif node.state.trust_state == "unknown":
                service_multiplier *= 0.65
            elif node.state.trust_state in {"quarantined", "revoked"}:
                service_multiplier = 0.0
            node.refresh_state(
                occupancy=occupancy_by_node.get(node.state.node_id, 0),
                queue_length=queue_by_node.get(node.state.node_id, 0),
                service_multiplier=service_multiplier,
                forced_closed=bool(node_effect.get("closed", False)),
                contingency_slots_delta=int(node_effect.get("slots_delta", 0)),
            )

    def _update_vehicle_comms_and_trust(
        self,
        current_minute: int,
        disturbance: DisturbanceState,
        time_step_minutes: int,
    ) -> None:
        for vehicle in self.vehicles:
            if vehicle.state.status == "completed":
                continue
            if not can_file_intent(vehicle.state.trust_state):
                vehicle.state.information_age_minutes += time_step_minutes
            else:
                dropped = update_information_age(vehicle.state, disturbance, time_step_minutes, self.rng)
                if dropped:
                    self.recorder.record_event(
                        current_minute,
                        "message_drop",
                        vehicle_id=vehicle.state.id,
                        information_age_minutes=round(vehicle.state.information_age_minutes, 2),
                    )

            stale_increment = stale_exposure_increment(vehicle.state, disturbance, time_step_minutes)
            if stale_increment > 0:
                self.recorder.record_event(
                    current_minute,
                    "stale_state_exposure",
                    vehicle_id=vehicle.state.id,
                    information_age_minutes=round(vehicle.state.information_age_minutes, 2),
                )

            if should_activate_lost_link(vehicle.state, disturbance):
                if vehicle.state.fallback_mode != "lost_link":
                    vehicle.state.fallback_mode = "lost_link"
                    vehicle.state.lost_link_activations += 1
                    self.lost_link_fallback_activations += 1
                    self._record_operator_intervention(current_minute, vehicle.state.id, "lost_link_fallback")
                    self.recorder.record_event(
                        current_minute,
                        "lost_link_fallback",
                        vehicle_id=vehicle.state.id,
                        information_age_minutes=round(vehicle.state.information_age_minutes, 2),
                    )
            elif vehicle.state.fallback_mode == "lost_link":
                vehicle.state.fallback_mode = "normal"
                self.recorder.record_event(current_minute, "lost_link_recovered", vehicle_id=vehicle.state.id)

            stale_threshold = disturbance.stale_after_minutes
            if vehicle.state.information_age_minutes > stale_threshold * 2.5 and vehicle.state.trust_state in {"trusted", "degraded"}:
                self._apply_direct_trust_state(
                    current_minute,
                    "vehicle",
                    vehicle.state.id,
                    "unknown",
                    "stale_identity_evidence",
                )
            elif vehicle.state.information_age_minutes > stale_threshold * 1.5 and vehicle.state.trust_state == "trusted":
                self._apply_direct_trust_state(
                    current_minute,
                    "vehicle",
                    vehicle.state.id,
                    "degraded",
                    "stale_identity_evidence",
                )

    def _dispatch_vehicles(self, time_minute: int, disturbance: DisturbanceState) -> None:
        queued_by_node: dict[str, list[Vehicle]] = defaultdict(list)
        for vehicle in self.vehicles:
            if vehicle.state.status == "queued" and vehicle.state.current_location in self.network.nodes:
                queued_by_node[vehicle.state.current_location].append(vehicle)

        for node_id, queued_vehicles in queued_by_node.items():
            queued_vehicles.sort(key=lambda vehicle: (-trust_service_priority(vehicle.state.trust_state), vehicle.state.id))
            node = self.network.get_node(node_id)
            planned_slots, credit_after = node.plan_dispatches(self.config.time_step_minutes)

            actual_dispatches = 0
            for vehicle in queued_vehicles:
                if actual_dispatches >= planned_slots:
                    break
                if time_minute < vehicle.state.delayed_until_minute:
                    continue
                next_node = vehicle.next_node()
                if next_node is None:
                    vehicle.state.status = "completed"
                    continue

                corridor = self.network.corridor_between(node_id, next_node)
                if corridor is None or corridor.state.is_closed:
                    if self._attempt_reroute_or_divert(vehicle, time_minute, disturbance, "route_blocked"):
                        continue
                    vehicle.state.status = "holding"
                    vehicle.state.conformance_ok = False
                    self.recorder.record_event(
                        time_minute,
                        "route_blocked",
                        vehicle_id=vehicle.state.id,
                        location=node_id,
                        requested_next_node=next_node,
                    )
                    continue

                self.total_dispatch_attempts += 1
                if is_quarantined(vehicle.state.trust_state) or not can_file_intent(vehicle.state.trust_state):
                    self.trust_denied_dispatches += 1
                    vehicle.state.status = "holding"
                    continue

                if is_state_stale(vehicle.state, disturbance):
                    self.reservation_invalidations += 1
                    vehicle.state.delayed_until_minute = time_minute + int(round(max(1.0, disturbance.reroute_delay_minutes)))
                    self.delayed_reroute_count += 1
                    self.recorder.record_event(
                        time_minute,
                        "reservation_invalidated",
                        vehicle_id=vehicle.state.id,
                        corridor_id=corridor.state.corridor_id,
                        information_age_minutes=round(vehicle.state.information_age_minutes, 2),
                    )
                    continue

                if vehicle.state.fallback_mode == "lost_link":
                    self.reservation_invalidations += 1
                    continue

                if not can_receive_reservation(vehicle.state.trust_state, degraded_mode=corridor.state.degraded_mode):
                    self.trust_denied_dispatches += 1
                    if vehicle.state.trust_state == "unknown":
                        self._apply_direct_trust_state(time_minute, "vehicle", vehicle.state.id, "quarantined", "spoof_detection")
                    continue

                if not corridor.can_accept_departure():
                    continue

                if vehicle.state.trust_state == "unknown":
                    self.unsafe_admission_count += 1
                    vehicle.state.unsafe_admissions += 1
                    self._record_operator_intervention(time_minute, vehicle.state.id, "unsafe_admission")
                    self.recorder.record_event(
                        time_minute,
                        "unsafe_admission",
                        vehicle_id=vehicle.state.id,
                        corridor_id=corridor.state.corridor_id,
                    )
                elif requires_operator_override(vehicle.state.trust_state, degraded_mode=corridor.state.degraded_mode):
                    self._record_operator_intervention(time_minute, vehicle.state.id, "manual_override")

                corridor.record_departure(vehicle.state.id)
                vehicle.dispatch_to(corridor.state.corridor_id)
                actual_dispatches += 1
                self.recorder.record_event(
                    time_minute,
                    "dispatch",
                    vehicle_id=vehicle.state.id,
                    origin=node_id,
                    destination=next_node,
                    corridor_id=corridor.state.corridor_id,
                    trust_state=vehicle.state.trust_state,
                )

            node.commit_dispatches(planned_slots, actual_dispatches, credit_after)

    def _attempt_reroute_or_divert(
        self,
        vehicle: Vehicle,
        current_minute: int,
        disturbance: DisturbanceState,
        reason: str,
    ) -> bool:
        current_node = vehicle.state.current_location
        final_destination = vehicle.state.route[-1]
        alternate_path = self.network.shortest_path(current_node, final_destination)
        if alternate_path is not None and len(alternate_path) > 1 and alternate_path != vehicle.state.route[vehicle.state.route_index :]:
            vehicle.state.route = alternate_path
            vehicle.state.route_index = 0
            vehicle.state.reroute_count += 1
            vehicle.state.delayed_until_minute = current_minute + int(round(disturbance.reroute_delay_minutes))
            self.delayed_reroute_count += 1
            self._record_operator_intervention(current_minute, vehicle.state.id, "reroute_update")
            self.recorder.record_event(
                current_minute,
                "reroute_planned",
                vehicle_id=vehicle.state.id,
                reason=reason,
                route=alternate_path,
            )
            return True

        return self._attempt_diversion(vehicle, current_minute, disturbance, reason)

    def _attempt_diversion(
        self,
        vehicle: Vehicle,
        current_minute: int,
        disturbance: DisturbanceState,
        reason: str,
    ) -> bool:
        self.divert_attempt_count += 1
        vehicle.state.divert_attempt_count += 1
        options = compute_reachable_landing_options(
            self.network,
            vehicle.state,
            disturbance,
            self.config.modifiers.reserve_consumption_multiplier,
        )
        vehicle.state.reachable_landing_options = len(options)
        for option in options:
            target_node = self.network.get_node(str(option["node_id"]))
            if not target_node.reserve_contingency_slot():
                continue
            path = self.network.shortest_path(vehicle.state.current_location, str(option["node_id"]))
            if path is None or len(path) < 2:
                continue
            vehicle.state.route = path
            vehicle.state.route_index = 0
            vehicle.state.reroute_count += 1
            vehicle.state.divert_success_count += 1
            vehicle.state.delayed_until_minute = current_minute + int(round(disturbance.reroute_delay_minutes))
            self.divert_success_count += 1
            self._record_operator_intervention(current_minute, vehicle.state.id, "contingency_divert")
            self.recorder.record_event(
                current_minute,
                "divert_planned",
                vehicle_id=vehicle.state.id,
                target_node=str(option["node_id"]),
                reserve_margin=round(float(option["reserve_margin"]), 2),
                reason=reason,
            )
            return True

        self._mark_no_admissible_landing(vehicle, current_minute, reason)
        return False

    def _move_vehicles(self, event_time_minute: int, disturbance: DisturbanceState) -> None:
        time_step_hours = self.config.time_step_minutes / 60.0
        for vehicle in self.vehicles:
            if vehicle.state.status != "enroute" or vehicle.state.active_corridor_id is None:
                continue

            corridor = self.network.get_corridor(vehicle.state.active_corridor_id)
            speed_multiplier = 0.75 if vehicle.state.fallback_mode == "lost_link" else 1.0
            distance_km = corridor.state.modified_speed * speed_multiplier * time_step_hours
            vehicle.advance(distance_km)
            vehicle.state.reserve_energy -= approximate_reserve_energy_drain(
                distance_km,
                self.config.time_step_minutes,
                disturbance.weather_severity,
                mission_class=vehicle.state.mission_class,
                status="enroute",
                reserve_multiplier=self.config.modifiers.reserve_consumption_multiplier,
            )

            if vehicle.state.reserve_energy <= 0.0:
                vehicle.state.conformance_ok = False

            if vehicle.state.progress_km >= corridor.state.length:
                destination = corridor.state.destination
                corridor.record_exit(vehicle.state.id)
                vehicle.arrive(destination)
                self.recorder.record_event(
                    event_time_minute,
                    "arrival",
                    vehicle_id=vehicle.state.id,
                    destination=destination,
                    corridor_id=corridor.state.corridor_id,
                    reserve_energy=round(vehicle.state.reserve_energy, 3),
                )

                if vehicle.state.status == "completed":
                    event_type = "contingency_landing" if self.network.get_node(destination).state.contingency_landing_slots > 0 else "mission_complete"
                    self.recorder.record_event(
                        event_time_minute,
                        event_type,
                        vehicle_id=vehicle.state.id,
                        destination=destination,
                    )

    def _apply_idle_energy_drain(self, disturbance: DisturbanceState) -> None:
        for vehicle in self.vehicles:
            if vehicle.state.status not in {"queued", "holding"}:
                continue
            vehicle.state.reserve_energy -= approximate_reserve_energy_drain(
                0.0,
                self.config.time_step_minutes,
                disturbance.weather_severity,
                mission_class=vehicle.state.mission_class,
                status=vehicle.state.status,
                reserve_multiplier=self.config.modifiers.reserve_consumption_multiplier,
            )
            if vehicle.state.reserve_energy <= 0.0:
                vehicle.state.conformance_ok = False

    def _update_landing_reachability(self, current_minute: int, disturbance: DisturbanceState) -> None:
        for vehicle in self.vehicles:
            if vehicle.state.status == "completed":
                continue
            options = compute_reachable_landing_options(
                self.network,
                vehicle.state,
                disturbance,
                self.config.modifiers.reserve_consumption_multiplier,
            )
            previous = vehicle.state.reachable_landing_options
            vehicle.state.reachable_landing_options = len(options)
            if vehicle.state.reachable_landing_options == 0 and previous > 0 and vehicle.state.reserve_energy <= vehicle.state.min_contingency_margin + 12.0:
                self._mark_no_admissible_landing(vehicle, current_minute, "contingency_unreachable")

    def _mark_no_admissible_landing(self, vehicle: Vehicle, current_minute: int, reason: str) -> None:
        if vehicle.state.no_admissible_landing:
            return
        vehicle.state.no_admissible_landing = True
        self.no_admissible_landing_events += 1
        self._record_operator_intervention(current_minute, vehicle.state.id, "no_admissible_landing")
        self.recorder.record_event(
            current_minute,
            "no_admissible_landing",
            vehicle_id=vehicle.state.id,
            reason=reason,
            reserve_energy=round(vehicle.state.reserve_energy, 2),
        )

    def _record_operator_intervention(self, current_minute: int, vehicle_id: str, reason: str) -> None:
        self.operator_intervention_count += 1
        vehicle = self._vehicle_by_id(vehicle_id)
        if vehicle is not None:
            vehicle.state.operator_interventions += 1
        self.recorder.record_event(
            current_minute,
            "operator_intervention",
            vehicle_id=vehicle_id,
            reason=reason,
        )

    def _update_physics_control_layer(self, time_minute: int, disturbance: DisturbanceState) -> None:
        sample = map_engine_state(
            self.network,
            self.vehicles,
            disturbance,
            time_minute=time_minute,
        )
        response = compute_disturbance_response(sample, self.physics_model)
        admissibility = evaluate_admissibility(sample, response, self.admissibility_config)
        self.physics_time_series.append(sample)
        self.disturbance_responses.append(response)
        self.admissibility_time_series.append(admissibility)
        if not admissibility.inside_A:
            self.recorder.record_event(
                time_minute,
                "admissibility_exit_candidate",
                violated_constraints=list(admissibility.violated_constraints),
                margins={key: round(value, 6) for key, value in admissibility.margins.items()},
            )

    def _record_metrics(self, time_minute: int, disturbance: DisturbanceState) -> None:
        node_states = [node.state for node in self.network.nodes.values()]
        corridor_objects = list(self.network.corridors.values())
        corridor_states = [corridor.state for corridor in corridor_objects]
        active_vehicle_states = [vehicle.state for vehicle in self.vehicles if vehicle.state.status != "completed"]
        completed_vehicles = sum(1 for vehicle in self.vehicles if vehicle.state.status == "completed")
        active_vehicles = len(active_vehicle_states)

        avg_queue_length = sum(node.queue_length for node in node_states) / len(node_states) if node_states else 0.0
        total_corridor_flow = sum(corridor.flow for corridor in corridor_states)
        mean_corridor_speed = (
            sum(corridor.modified_speed for corridor in corridor_states) / len(corridor_states)
            if corridor_states
            else 0.0
        )
        mean_effective_capacity = (
            sum(corridor.effective_capacity for corridor in corridor_states) / len(corridor_states)
            if corridor_states
            else 0.0
        )
        mean_reserve_energy = (
            sum(vehicle.state.reserve_energy for vehicle in self.vehicles) / len(self.vehicles) if self.vehicles else 0.0
        )

        corridor_load_ratio = max(
            (
                len(corridor.vehicles_in_corridor)
                / max(
                    1.0,
                    corridor.state.effective_capacity
                    * (corridor.state.length / max(corridor.state.modified_speed, 1.0)),
                )
                for corridor in corridor_objects
            ),
            default=0.0,
        )
        node_utilization_ratio = max(
            (node.occupancy / max(node.contingency_capacity, 1) for node in node_states),
            default=0.0,
        )
        queue_ratio = max(
            (node.queue_length / max(node.contingency_capacity, 1) for node in node_states),
            default=0.0,
        )

        reserve_margins = [vehicle.state.reserve_energy - vehicle.state.min_contingency_margin for vehicle in self.vehicles]
        reserve_margin_mean = sum(reserve_margins) / len(reserve_margins) if reserve_margins else 0.0
        reserve_margin_min = min(reserve_margins) if reserve_margins else 0.0

        trusted_active_fraction = (
            sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "trusted") / active_vehicles
            if active_vehicles
            else 1.0
        )
        information_age_mean = (
            sum(vehicle.information_age_minutes for vehicle in active_vehicle_states) / active_vehicles
            if active_vehicles
            else 0.0
        )
        information_age_max = max((vehicle.information_age_minutes for vehicle in active_vehicle_states), default=0.0)
        stale_state_exposure_minutes = sum(vehicle.state.stale_state_exposure_minutes for vehicle in self.vehicles)
        reachable_landing_option_mean = (
            sum(vehicle.reachable_landing_options for vehicle in active_vehicle_states) / active_vehicles
            if active_vehicles
            else self.config.safe_region.min_reachable_landing_options
        )
        contingency_node_utilization = max(
            (
                node.contingency_occupied / max(node.contingency_landing_slots, 1)
                for node in node_states
                if node.contingency_landing_slots > 0
            ),
            default=0.0,
        )
        if any(
            node.contingency_landing_slots > 0 and node.contingency_occupied >= node.contingency_landing_slots
            for node in node_states
        ):
            self.safe_region_cumulative_durations["contingency_saturation_duration"] += self.config.time_step_minutes

        if any(corridor.degraded_mode for corridor in corridor_states):
            self.degraded_mode_dwell_time += self.config.time_step_minutes

        operator_intervention_rate = self.operator_intervention_count / max(time_minute / 60.0, 1 / 60.0)
        trust_induced_throughput_loss = self.trust_denied_dispatches / max(self.total_dispatch_attempts, 1)

        safe_region_causes = self._safe_region_causes(
            corridor_load_ratio,
            node_utilization_ratio,
            queue_ratio,
            stale_state_exposure_minutes,
            trusted_active_fraction,
            reachable_landing_option_mean,
            operator_intervention_rate,
        )
        latest_physics = self.physics_time_series[-1] if self.physics_time_series else None
        latest_response = self.disturbance_responses[-1] if self.disturbance_responses else None
        latest_admissibility = self.admissibility_time_series[-1] if self.admissibility_time_series else None

        self.last_metrics = {
            "corridor_load_ratio": corridor_load_ratio,
            "node_utilization_ratio": node_utilization_ratio,
            "queue_ratio": queue_ratio,
            "stale_state_exposure_minutes": stale_state_exposure_minutes,
            "trusted_active_fraction": trusted_active_fraction,
            "reachable_landing_option_mean": reachable_landing_option_mean,
            "operator_intervention_rate": operator_intervention_rate,
            "rho_e": latest_physics.rho_e if latest_physics is not None else 0.0,
            "q_e": latest_physics.q_e if latest_physics is not None else 0.0,
            "lambda_e": latest_physics.lambda_e if latest_physics is not None else 0.0,
            "alpha_e": latest_response.alpha_e if latest_response is not None else 1.0,
            "c_e": latest_response.c_e if latest_response is not None else mean_effective_capacity,
            "s_e": latest_response.s_e if latest_response is not None else 0.0,
            "gamma_e": latest_physics.gamma_e if latest_physics is not None else disturbance.comms_reliability,
            "eta_e": latest_physics.eta_e if latest_physics is not None else 1.0,
            "chi_e": latest_physics.chi_e if latest_physics is not None else 0.0,
            "admissibility_status": latest_admissibility.status if latest_admissibility is not None else "inside_A",
        }

        self.trust_distribution_over_time.append(
            {
                "time_minute": time_minute,
                "trusted": sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "trusted"),
                "degraded": sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "degraded"),
                "unknown": sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "unknown"),
                "quarantined": sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "quarantined"),
                "revoked": sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "revoked"),
            }
        )
        self.information_age_distribution_over_time.append(
            {
                "time_minute": time_minute,
                "mean_information_age": information_age_mean,
                "max_information_age": information_age_max,
                "stale_vehicle_count": sum(
                    1
                    for vehicle in active_vehicle_states
                    if vehicle.information_age_minutes > disturbance.stale_after_minutes
                ),
            }
        )
        self.landing_option_distribution_over_time.append(
            {
                "time_minute": time_minute,
                "mean_reachable_options": reachable_landing_option_mean,
                "min_reachable_options": min((vehicle.reachable_landing_options for vehicle in active_vehicle_states), default=0),
                "max_reachable_options": max((vehicle.reachable_landing_options for vehicle in active_vehicle_states), default=0),
            }
        )

        self.recorder.record_snapshot(
            MetricsSnapshot(
                time_minute=time_minute,
                completed_vehicles=completed_vehicles,
                active_vehicles=active_vehicles,
                avg_queue_length=avg_queue_length,
                total_corridor_flow=total_corridor_flow,
                mean_corridor_speed=mean_corridor_speed,
                mean_effective_capacity=mean_effective_capacity,
                mean_reserve_energy=mean_reserve_energy,
                weather_severity=disturbance.weather_severity,
                comms_reliability=disturbance.comms_reliability,
                corridor_load_ratio=corridor_load_ratio,
                node_utilization_ratio=node_utilization_ratio,
                queue_ratio=queue_ratio,
                incomplete_missions=active_vehicles,
                divert_attempt_count=self.divert_attempt_count,
                divert_success_rate=self.divert_success_count / max(self.divert_attempt_count, 1),
                reserve_margin_mean=reserve_margin_mean,
                reserve_margin_min=reserve_margin_min,
                unsafe_admission_count=self.unsafe_admission_count,
                quarantine_count=sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "quarantined"),
                revocation_count=sum(1 for vehicle in active_vehicle_states if vehicle.trust_state == "revoked"),
                trusted_active_fraction=trusted_active_fraction,
                information_age_mean=information_age_mean,
                information_age_max=information_age_max,
                stale_state_exposure_minutes=stale_state_exposure_minutes,
                delayed_reroute_count=self.delayed_reroute_count,
                lost_link_fallback_activations=self.lost_link_fallback_activations,
                reservation_invalidations=self.reservation_invalidations,
                reachable_landing_option_mean=reachable_landing_option_mean,
                no_admissible_landing_events=self.no_admissible_landing_events,
                contingency_node_utilization=contingency_node_utilization,
                contingency_saturation_duration=self.safe_region_cumulative_durations.get(
                    "contingency_saturation_duration", 0.0
                ),
                safe_region_violation_count=len(safe_region_causes),
                safe_region_primary_cause=";".join(sorted(safe_region_causes)) if safe_region_causes else "",
                operator_intervention_count=self.operator_intervention_count,
                degraded_mode_dwell_time=self.degraded_mode_dwell_time,
                trust_induced_throughput_loss=trust_induced_throughput_loss,
                rho_e=latest_physics.rho_e if latest_physics is not None else 0.0,
                q_e=latest_physics.q_e if latest_physics is not None else 0.0,
                lambda_e=latest_physics.lambda_e if latest_physics is not None else 0.0,
                c_e=latest_response.c_e if latest_response is not None else mean_effective_capacity,
                s_e=latest_response.s_e if latest_response is not None else 0.0,
                alpha_e=latest_response.alpha_e if latest_response is not None else 1.0,
                gamma_e=latest_physics.gamma_e if latest_physics is not None else disturbance.comms_reliability,
                eta_e=latest_physics.eta_e if latest_physics is not None else 1.0,
                chi_e=latest_physics.chi_e if latest_physics is not None else 0.0,
                admissibility_status=latest_admissibility.status if latest_admissibility is not None else "inside_A",
            )
        )

    def _safe_region_causes(
        self,
        corridor_load_ratio: float,
        node_utilization_ratio: float,
        queue_ratio: float,
        stale_state_exposure_minutes: float,
        trusted_active_fraction: float,
        reachable_landing_option_mean: float,
        operator_intervention_rate: float,
    ) -> list[str]:
        causes: list[str] = []
        if corridor_load_ratio > self.config.safe_region.max_corridor_load_ratio:
            causes.append("corridor_load_ratio")
        if node_utilization_ratio > self.config.safe_region.max_node_utilization_ratio:
            causes.append("node_utilization_ratio")
        if queue_ratio > self.config.safe_region.max_queue_ratio:
            causes.append("queue_ratio")
        if stale_state_exposure_minutes > self.config.safe_region.max_stale_state_exposure_minutes:
            causes.append("stale_state_exposure")
        if trusted_active_fraction < self.config.safe_region.min_trusted_participant_fraction:
            causes.append("trusted_participant_fraction")
        if self.unsafe_admission_count > self.config.safe_region.max_unsafe_admissions:
            causes.append("unsafe_admissions")
        if reachable_landing_option_mean < self.config.safe_region.min_reachable_landing_options:
            causes.append("reachable_landing_options")
        if self.safe_region_cumulative_durations.get("contingency_saturation_duration", 0.0) > self.config.safe_region.max_contingency_saturation_duration:
            causes.append("contingency_saturation_duration")
        if operator_intervention_rate > self.config.safe_region.max_operator_interventions_per_hour:
            causes.append("operator_intervention_rate")
        return causes

    def _evaluate_safe_region(self, current_minute: int, time_increment: int) -> None:
        causes = self._safe_region_causes(
            float(self.last_metrics.get("corridor_load_ratio", 0.0)),
            float(self.last_metrics.get("node_utilization_ratio", 0.0)),
            float(self.last_metrics.get("queue_ratio", 0.0)),
            float(self.last_metrics.get("stale_state_exposure_minutes", 0.0)),
            float(self.last_metrics.get("trusted_active_fraction", 1.0)),
            float(self.last_metrics.get("reachable_landing_option_mean", 0.0)),
            float(self.last_metrics.get("operator_intervention_rate", 0.0)),
        )
        if causes and self.safe_region_first_violation_time is None:
            self.safe_region_first_violation_time = current_minute
            self.safe_region_first_violation_cause = causes[0]
            self.recorder.record_event(
                current_minute,
                "safe_region_exit",
                primary_cause=causes[0],
                causes=causes,
            )
        for cause in causes:
            self.safe_region_cumulative_durations[cause] += time_increment

    def _build_summary(self) -> dict[str, object]:
        final_snapshot = self.recorder.snapshots[-1]
        phase_detection = {
            name: record.to_dict()
            for name, record in detect_phase_events(
                self.physics_time_series,
                self.disturbance_responses,
                self.admissibility_time_series,
            ).items()
        }
        admissibility_inside_count = sum(1 for result in self.admissibility_time_series if result.inside_A)
        admissibility_summary = {
            "inside_fraction": (
                admissibility_inside_count / len(self.admissibility_time_series)
                if self.admissibility_time_series
                else 1.0
            ),
            "first_exit_time": next(
                (
                    sample.time_minute
                    for sample, result in zip(self.physics_time_series, self.admissibility_time_series)
                    if not result.inside_A
                ),
                None,
            ),
            "violation_counts": dict(
                Counter(
                    constraint
                    for result in self.admissibility_time_series
                    for constraint in result.violated_constraints
                )
            ),
        }
        physics_summary = {
            "rho_e_peak": max((sample.rho_e for sample in self.physics_time_series), default=0.0),
            "q_e_peak": max((sample.q_e for sample in self.physics_time_series), default=0.0),
            "lambda_e_peak": max((sample.lambda_e for sample in self.physics_time_series), default=0.0),
            "gamma_e_min": min((sample.gamma_e for sample in self.physics_time_series), default=1.0),
            "w_e_peak": max((sample.w_e for sample in self.physics_time_series), default=0.0),
            "chi_e_peak": max((sample.chi_e for sample in self.physics_time_series), default=0.0),
            "c_e_min": min((response.c_e for response in self.disturbance_responses), default=0.0),
            "s_e_max": max((response.s_e for response in self.disturbance_responses), default=0.0),
            "alpha_e_min": min((response.alpha_e for response in self.disturbance_responses), default=1.0),
        }
        legacy_classification = classify_failure(
            self.safe_region_first_violation_cause,
            {
                "no_admissible_landing_events": self.no_admissible_landing_events,
                "unsafe_admission_count": self.unsafe_admission_count,
                "quarantine_count": self.quarantine_event_count,
                "revocation_count": self.revocation_event_count,
                "stale_state_exposure_minutes": final_snapshot.stale_state_exposure_minutes,
                "peak_corridor_load_ratio": max(snapshot.corridor_load_ratio for snapshot in self.recorder.snapshots),
                "peak_queue_ratio": max(snapshot.queue_ratio for snapshot in self.recorder.snapshots),
            },
        )
        classification = classify_failure_mode(
            {
                "phase_detection": phase_detection,
                "first_dominant_failure_mechanism": legacy_classification,
                "unsafe_admission_count": self.unsafe_admission_count,
                "quarantine_count": self.quarantine_event_count,
                "revocation_count": self.revocation_event_count,
                "delayed_reroute_count": self.delayed_reroute_count,
                "lost_link_fallback_activations": self.lost_link_fallback_activations,
                "reservation_invalidations": self.reservation_invalidations,
                "peak_corridor_load_ratio": max(snapshot.corridor_load_ratio for snapshot in self.recorder.snapshots),
                "peak_node_utilization_ratio": max(
                    snapshot.node_utilization_ratio for snapshot in self.recorder.snapshots
                ),
                "peak_queue_ratio": max(snapshot.queue_ratio for snapshot in self.recorder.snapshots),
                "mean_corridor_speed": sum(
                    snapshot.mean_corridor_speed for snapshot in self.recorder.snapshots
                ) / len(self.recorder.snapshots),
                "weather_severity_peak": max(
                    snapshot.weather_severity for snapshot in self.recorder.snapshots
                ),
                "physics_summary": physics_summary,
                "alpha_e_min": physics_summary["alpha_e_min"],
                "chi_e_peak": physics_summary["chi_e_peak"],
            }
        )

        return {
            "scenario_name": self.config.scenario_name,
            "description": self.config.description,
            "seed": self.config.seed,
            "completed_vehicles": final_snapshot.completed_vehicles,
            "incomplete_vehicles": final_snapshot.incomplete_missions,
            "avg_queue_length": final_snapshot.avg_queue_length,
            "peak_avg_queue_length": max(snapshot.avg_queue_length for snapshot in self.recorder.snapshots),
            "mean_corridor_speed": sum(snapshot.mean_corridor_speed for snapshot in self.recorder.snapshots) / len(self.recorder.snapshots),
            "mean_reserve_energy": final_snapshot.mean_reserve_energy,
            "peak_corridor_load_ratio": max(snapshot.corridor_load_ratio for snapshot in self.recorder.snapshots),
            "peak_node_utilization_ratio": max(snapshot.node_utilization_ratio for snapshot in self.recorder.snapshots),
            "peak_queue_ratio": max(snapshot.queue_ratio for snapshot in self.recorder.snapshots),
            "unsafe_admission_count": self.unsafe_admission_count,
            "quarantine_count": self.quarantine_event_count,
            "false_quarantine_count": self.false_quarantine_count,
            "revocation_count": self.revocation_event_count,
            "revocation_propagation_delay_minutes": (
                sum(self.revocation_propagation_delays) / len(self.revocation_propagation_delays)
                if self.revocation_propagation_delays
                else 0.0
            ),
            "stale_state_exposure_minutes": final_snapshot.stale_state_exposure_minutes,
            "information_age_mean": final_snapshot.information_age_mean,
            "information_age_max": final_snapshot.information_age_max,
            "delayed_reroute_count": self.delayed_reroute_count,
            "lost_link_fallback_activations": self.lost_link_fallback_activations,
            "reservation_invalidations": self.reservation_invalidations,
            "reachable_landing_option_mean": final_snapshot.reachable_landing_option_mean,
            "no_admissible_landing_events": self.no_admissible_landing_events,
            "contingency_node_utilization": final_snapshot.contingency_node_utilization,
            "contingency_saturation_duration": self.safe_region_cumulative_durations.get(
                "contingency_saturation_duration", 0.0
            ),
            "reserve_margin_mean": final_snapshot.reserve_margin_mean,
            "reserve_margin_min": final_snapshot.reserve_margin_min,
            "operator_intervention_count": self.operator_intervention_count,
            "degraded_mode_dwell_time": self.degraded_mode_dwell_time,
            "trust_induced_throughput_loss": final_snapshot.trust_induced_throughput_loss,
            "first_safe_region_exit_time": self.safe_region_first_violation_time,
            "first_safe_region_exit_cause": self.safe_region_first_violation_cause or "",
            "first_dominant_failure_mechanism": legacy_classification,
            "dominant_failure_mode": classification.dominant_failure_mode,
            "dominant_failure_mode_confidence": classification.confidence,
            "failure_mode_scores": classification.scores,
            "phase_detection": phase_detection,
            "physics_summary": physics_summary,
            "admissibility_summary": admissibility_summary,
            "alpha_e": final_snapshot.alpha_e,
            "c_e": final_snapshot.c_e,
            "s_e": final_snapshot.s_e,
            "admissibility_status": final_snapshot.admissibility_status,
            "weather_severity_peak": max(snapshot.weather_severity for snapshot in self.recorder.snapshots),
            "comms_reliability_min": min(snapshot.comms_reliability for snapshot in self.recorder.snapshots),
            "trust_state_distribution_over_time": self.trust_distribution_over_time,
            "information_age_distribution_over_time": self.information_age_distribution_over_time,
            "reachable_landing_option_distribution_over_time": self.landing_option_distribution_over_time,
            "safe_region_cumulative_duration_by_cause": dict(self.safe_region_cumulative_durations),
        }

    def _build_threshold_summary(self, summary: dict[str, object]) -> dict[str, object]:
        return {
            "scenario_name": self.config.scenario_name,
            "safe_region_rules": asdict(self.config.safe_region),
            "first_violation_timestamp": self.safe_region_first_violation_time,
            "first_violation_cause": self.safe_region_first_violation_cause,
            "cumulative_duration_by_cause": dict(self.safe_region_cumulative_durations),
            "failure_classification": summary["first_dominant_failure_mechanism"],
            "dominant_failure_mode": summary["dominant_failure_mode"],
            "phase_detection": summary["phase_detection"],
            "physics_summary": summary["physics_summary"],
            "admissibility_summary": summary["admissibility_summary"],
        }

    def _vehicle_by_id(self, vehicle_id: str) -> Vehicle | None:
        for vehicle in self.vehicles:
            if vehicle.state.id == vehicle_id:
                return vehicle
        return None


def run_from_config(config_path: str | Path) -> SimulationResult:
    config = load_simulation_config(config_path)
    engine = SimulationEngine(config)
    return engine.run()
