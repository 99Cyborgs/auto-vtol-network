from __future__ import annotations

from dataclasses import dataclass

from avn.core.models import NodeConfig, NodeState
from avn.physics.dynamics import step_node_queue


@dataclass
class VertiportNode:
    state: NodeState
    service_credit: float = 0.0

    def refresh_state(self, occupancy: int, queue_length: int) -> None:
        self.state.occupancy = occupancy
        self.state.queue_length = queue_length

        if self.state.service_rate <= 0:
            self.state.operational_state = "closed"
        elif occupancy >= self.state.contingency_capacity:
            self.state.operational_state = "constrained"
        elif occupancy >= max(1, int(self.state.contingency_capacity * 0.8)):
            self.state.operational_state = "contingency"
        else:
            self.state.operational_state = "normal"

    def plan_dispatches(self, time_step_minutes: float) -> tuple[int, float]:
        slots, _remaining_queue, credit_after = step_node_queue(
            self.state.queue_length,
            self.state.service_rate,
            time_step_minutes,
            service_credit=self.service_credit,
            operational_state=self.state.operational_state,
        )
        return slots, credit_after

    def commit_dispatches(self, planned_slots: int, actual_dispatches: int, credit_after: float) -> None:
        unused_slots = max(0, planned_slots - actual_dispatches)
        self.service_credit = max(0.0, credit_after + unused_slots)
        self.state.queue_length = max(0, self.state.queue_length - actual_dispatches)


class MicroVertiport(VertiportNode):
    pass


class HubVertiport(VertiportNode):
    pass


class EmergencyPad(VertiportNode):
    pass


def build_node(config: NodeConfig) -> VertiportNode:
    state = NodeState(
        node_id=config.node_id,
        node_type=config.node_type,
        queue_length=0,
        service_rate=config.service_rate,
        contingency_capacity=config.contingency_capacity,
        occupancy=config.occupancy,
        operational_state=config.operational_state,
    )

    node_type = config.node_type.lower()
    if node_type == "micro":
        return MicroVertiport(state=state)
    if node_type == "hub":
        return HubVertiport(state=state)
    if node_type == "emergency":
        return EmergencyPad(state=state)
    raise ValueError(f"Unsupported node type: {config.node_type}")

