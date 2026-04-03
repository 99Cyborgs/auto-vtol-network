from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from avn.core.config import load_simulation_config
from avn.core.models import DisturbanceState, MetricsSnapshot, SimulationConfig
from avn.disturbances.timeline import DisturbanceTimeline
from avn.metrics.recorder import MetricsRecorder
from avn.network.graph import VTOLNetwork
from avn.physics.dynamics import approximate_reserve_energy_drain
from avn.vehicle.fleet import Vehicle, build_fleet
from avn.viz.plots import generate_plots


@dataclass(slots=True)
class SimulationResult:
    scenario_name: str
    output_dir: Path
    metrics_path: Path
    event_log_path: Path
    plot_paths: list[Path]
    summary: dict[str, float | int]


class SimulationEngine:
    def __init__(self, config: SimulationConfig) -> None:
        self.config = config
        self.network = VTOLNetwork.from_config(config.nodes, config.corridors)
        self.vehicles: list[Vehicle] = build_fleet(config.vehicles)
        self.timeline = DisturbanceTimeline(config.disturbance_base, config.disturbance_schedule)
        self.recorder = MetricsRecorder()

    def run(self) -> SimulationResult:
        current_minute = 0
        time_step = self.config.time_step_minutes
        output_dir = self._create_output_dir()

        initial_disturbance = self.timeline.state_at(0)
        self.network.prepare_step(initial_disturbance, time_step, accrue_capacity=False)
        self._refresh_node_states()
        self.network.finalize_step(time_step)
        self.recorder.record_event(0, "scenario_start", scenario=self.config.scenario_name)
        self._record_metrics(0, initial_disturbance)

        while current_minute < self.config.duration_minutes:
            disturbance = self.timeline.state_at(current_minute)
            self.network.prepare_step(disturbance, time_step)
            self._refresh_node_states()
            self._dispatch_vehicles(current_minute)
            self._move_vehicles(current_minute + time_step, disturbance)
            self._apply_idle_energy_drain(disturbance)
            self._refresh_node_states()
            self.network.finalize_step(time_step)
            current_minute += time_step
            self._record_metrics(current_minute, self.timeline.state_at(current_minute))

        self.recorder.record_event(
            current_minute,
            "scenario_complete",
            completed_vehicles=sum(1 for vehicle in self.vehicles if vehicle.state.status == "completed"),
        )
        metrics_path = self.recorder.write_metrics_csv(output_dir)
        event_log_path = self.recorder.write_event_log(output_dir)
        plot_paths = generate_plots(self.recorder.snapshots, output_dir)
        summary = self._build_summary()
        return SimulationResult(
            scenario_name=self.config.scenario_name,
            output_dir=output_dir,
            metrics_path=metrics_path,
            event_log_path=event_log_path,
            plot_paths=plot_paths,
            summary=summary,
        )

    def _create_output_dir(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = self.config.output_root / f"{self.config.scenario_name}_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=False)
        return output_dir

    def _refresh_node_states(self) -> None:
        occupancy_by_node: dict[str, int] = defaultdict(int)
        queue_by_node: dict[str, int] = defaultdict(int)

        for vehicle in self.vehicles:
            if vehicle.state.status in {"queued", "holding", "completed"}:
                occupancy_by_node[vehicle.state.current_location] += 1
            if vehicle.state.status == "queued":
                queue_by_node[vehicle.state.current_location] += 1

        for node in self.network.nodes.values():
            node.refresh_state(
                occupancy=occupancy_by_node.get(node.state.node_id, 0),
                queue_length=queue_by_node.get(node.state.node_id, 0),
            )

    def _dispatch_vehicles(self, time_minute: int) -> None:
        queued_by_node: dict[str, list[Vehicle]] = defaultdict(list)
        for vehicle in self.vehicles:
            if vehicle.state.status == "queued":
                queued_by_node[vehicle.state.current_location].append(vehicle)

        for node_id, queued_vehicles in queued_by_node.items():
            queued_vehicles.sort(key=lambda vehicle: vehicle.state.id)
            node = self.network.get_node(node_id)
            planned_slots, credit_after = node.plan_dispatches(self.config.time_step_minutes)

            actual_dispatches = 0
            for vehicle in queued_vehicles:
                if actual_dispatches >= planned_slots:
                    break

                next_node = vehicle.next_node()
                if next_node is None:
                    continue

                corridor = self.network.corridor_between(node_id, next_node)
                if corridor is None:
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

                if not corridor.can_accept_departure():
                    continue

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
                )

            node.commit_dispatches(planned_slots, actual_dispatches, credit_after)

    def _move_vehicles(self, event_time_minute: int, disturbance: DisturbanceState) -> None:
        time_step_hours = self.config.time_step_minutes / 60.0
        for vehicle in self.vehicles:
            if vehicle.state.status != "enroute" or vehicle.state.active_corridor_id is None:
                continue

            corridor = self.network.get_corridor(vehicle.state.active_corridor_id)
            distance_km = corridor.state.modified_speed * time_step_hours
            vehicle.advance(distance_km)
            vehicle.state.reserve_energy -= approximate_reserve_energy_drain(
                distance_km,
                self.config.time_step_minutes,
                disturbance.weather_severity,
                mission_class=vehicle.state.mission_class,
                status="enroute",
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
                    self.recorder.record_event(
                        event_time_minute,
                        "mission_complete",
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
            )
            if vehicle.state.reserve_energy <= 0.0:
                vehicle.state.conformance_ok = False

    def _record_metrics(self, time_minute: int, disturbance: DisturbanceState) -> None:
        node_states = [node.state for node in self.network.nodes.values()]
        corridor_states = [corridor.state for corridor in self.network.corridors.values()]
        active_vehicles = sum(1 for vehicle in self.vehicles if vehicle.state.status != "completed")
        completed_vehicles = sum(1 for vehicle in self.vehicles if vehicle.state.status == "completed")
        avg_queue_length = (
            sum(node.queue_length for node in node_states) / len(node_states) if node_states else 0.0
        )
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
        mean_reserve_energy = sum(vehicle.state.reserve_energy for vehicle in self.vehicles) / len(self.vehicles)

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
            )
        )

    def _build_summary(self) -> dict[str, float | int]:
        final_snapshot = self.recorder.snapshots[-1]
        queue_values = [snapshot.avg_queue_length for snapshot in self.recorder.snapshots]
        speed_values = [snapshot.mean_corridor_speed for snapshot in self.recorder.snapshots]
        return {
            "completed_vehicles": final_snapshot.completed_vehicles,
            "incomplete_vehicles": final_snapshot.active_vehicles,
            "avg_queue_length": final_snapshot.avg_queue_length,
            "peak_avg_queue_length": max(queue_values),
            "mean_corridor_speed": sum(speed_values) / len(speed_values),
            "mean_reserve_energy": final_snapshot.mean_reserve_energy,
        }


def run_from_config(config_path: str | Path) -> SimulationResult:
    config = load_simulation_config(config_path)
    engine = SimulationEngine(config)
    return engine.run()
