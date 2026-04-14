from __future__ import annotations

from dataclasses import asdict

from avn.core.alerts import build_alerts
from avn.core.corridors import update_corridor_runtime
from avn.core.graph import build_graph
from avn.core.metrics import build_metrics
from avn.core.nodes import accrue_service_credit
from avn.core.policies import get_policy_profile
from avn.core.state import (
    CorridorCondition,
    CorridorRuntime,
    NodeCondition,
    NodeRuntime,
    PolicySnapshot,
    ReplayBundle,
    ScenarioDefinition,
    StepSnapshot,
    VehicleRuntime,
)
from avn.sim.injectors import scenario_conditions
from avn.sim.reporting import build_corridor_snapshots, build_node_snapshots, build_summary, build_vehicle_snapshots
from avn.sim.runtime import SimulationRuntime
from avn.sim.transitions import advance_enroute_vehicles, advance_turnarounds, dispatch_queued_vehicles, release_vehicles


class SimulationEngine:
    def __init__(self, scenario: ScenarioDefinition) -> None:
        self.scenario = scenario

    def initial_state(self) -> SimulationRuntime:
        policy = get_policy_profile(self.scenario.policy_id)
        return SimulationRuntime(
            scenario=self.scenario,
            policy=policy,
            graph=build_graph(self.scenario.nodes, self.scenario.corridors),
            nodes={node.node_id: NodeRuntime(definition=node) for node in self.scenario.nodes},
            corridors={
                corridor.corridor_id: CorridorRuntime(definition=corridor) for corridor in self.scenario.corridors
            },
            vehicles={
                vehicle.vehicle_id: VehicleRuntime.from_definition(vehicle) for vehicle in self.scenario.vehicles
            },
        )

    def step(self, state: SimulationRuntime, time_minute: int) -> StepSnapshot:
        node_conditions, corridor_conditions, disturbance_events = scenario_conditions(state.scenario, time_minute)
        step_events: list[dict[str, object]] = list(disturbance_events)
        step_minutes = state.scenario.time_step_minutes

        release_vehicles(state, time_minute, step_events)
        advance_enroute_vehicles(state, time_minute, step_minutes, step_events)
        advance_turnarounds(state, time_minute, step_events)

        available_by_node: dict[str, int] = {}
        for node_id, runtime in state.nodes.items():
            available_by_node[node_id] = accrue_service_credit(
                runtime,
                node_conditions.get(node_id, NodeCondition()),
                step_minutes,
            )

        for corridor_id, runtime in state.corridors.items():
            update_corridor_runtime(
                runtime,
                corridor_conditions.get(corridor_id, CorridorCondition()),
                step_minutes,
            )

        dispatch_queued_vehicles(
            state,
            time_minute,
            available_by_node,
            node_conditions,
            corridor_conditions,
            step_events,
        )

        alerts = build_alerts(
            time_minute,
            state.nodes,
            node_conditions,
            state.corridors,
            state.vehicles,
            state.scenario.alert_thresholds,
        )
        metrics = build_metrics(state.nodes, node_conditions, state.corridors, state.vehicles)
        step = StepSnapshot(
            time_minute=time_minute,
            nodes=build_node_snapshots(state, node_conditions, available_by_node),
            corridors=build_corridor_snapshots(state),
            vehicles=build_vehicle_snapshots(state),
            metrics=metrics,
            alerts=alerts,
            events=step_events,
        )
        state.event_log.extend(step_events)
        state.event_log.extend(asdict(alert) for alert in alerts)
        return step

    def run(self) -> ReplayBundle:
        state = self.initial_state()
        steps: list[StepSnapshot] = []
        step_minutes = self.scenario.time_step_minutes
        for time_minute in range(0, self.scenario.duration_minutes + step_minutes, step_minutes):
            steps.append(self.step(state, time_minute))

        return ReplayBundle(
            scenario_id=state.scenario.scenario_id,
            name=state.scenario.name,
            description=state.scenario.description,
            policy=PolicySnapshot(
                policy_id=state.policy.policy_id,
                label=state.policy.label,
                description=state.policy.description,
            ),
            seed=state.scenario.seed,
            duration_minutes=state.scenario.duration_minutes,
            time_step_minutes=state.scenario.time_step_minutes,
            summary=build_summary(state, steps),
            steps=steps,
            event_log=state.event_log,
        )
