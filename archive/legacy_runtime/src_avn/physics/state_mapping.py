from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from avn.core.models import DisturbanceState

if TYPE_CHECKING:
    from avn.network.graph import VTOLNetwork
    from avn.vehicle.fleet import Vehicle


@dataclass(slots=True, frozen=True)
class PhysicsStateSample:
    time_minute: int
    rho_e: float
    q_e: float
    lambda_e: float
    c_e: float
    w_e: float
    gamma_e: float
    eta_e: float
    chi_e: float
    queue_e: float
    reserve_e: float
    base_separation: float
    active_vehicle_count: int
    kappa_i: float = 0.0
    r_e: float = 0.0
    demand_diverts: float = 0.0

    def to_dict(self) -> dict[str, float | int]:
        return {
            "time_minute": self.time_minute,
            "rho_e": self.rho_e,
            "q_e": self.q_e,
            "lambda_e": self.lambda_e,
            "c_e": self.c_e,
            "w_e": self.w_e,
            "gamma_e": self.gamma_e,
            "eta_e": self.eta_e,
            "chi_e": self.chi_e,
            "queue_e": self.queue_e,
            "reserve_e": self.reserve_e,
            "base_separation": self.base_separation,
            "active_vehicle_count": self.active_vehicle_count,
            "kappa_i": self.kappa_i,
            "r_e": self.r_e,
            "demand_diverts": self.demand_diverts,
        }


def _gamma_effective(disturbance: DisturbanceState) -> float:
    freshness_factor = 1.0 / (
        1.0 + (disturbance.comms_latency_minutes / max(disturbance.stale_after_minutes, 1.0))
    )
    return max(
        0.0,
        min(
            1.0,
            disturbance.comms_reliability
            * (1.0 - min(max(disturbance.message_drop_probability, 0.0), 0.99))
            * freshness_factor,
        ),
    )


def _trust_degradation(active_vehicles: list["Vehicle"]) -> float:
    if not active_vehicles:
        return 0.0

    weights = {
        "trusted": 0.0,
        "degraded": 0.35,
        "unknown": 0.55,
        "quarantined": 0.85,
        "revoked": 1.0,
    }
    return sum(weights.get(vehicle.state.trust_state, 0.5) for vehicle in active_vehicles) / len(active_vehicles)


def map_engine_state(
    network: "VTOLNetwork",
    vehicles: list["Vehicle"],
    disturbance: DisturbanceState,
    *,
    time_minute: int,
) -> PhysicsStateSample:
    corridor_states = [corridor.state for corridor in network.corridors.values()]
    node_states = [node.state for node in network.nodes.values()]
    active_vehicles = [vehicle for vehicle in vehicles if vehicle.state.status != "completed"]

    total_density = sum(corridor.density for corridor in corridor_states)
    total_flow = sum(corridor.flow for corridor in corridor_states)
    mean_capacity = (
        sum(corridor.effective_capacity for corridor in corridor_states) / len(corridor_states)
        if corridor_states
        else 0.0
    )
    queue_ratio = max(
        (node.queue_length / max(node.contingency_capacity, 1) for node in node_states),
        default=0.0,
    )
    reserve_margin_min = min(
        (vehicle.state.reserve_energy - vehicle.state.min_contingency_margin for vehicle in vehicles),
        default=0.0,
    )
    contingency_supply = sum(
        max(0, node.contingency_landing_slots - node.contingency_occupied)
        for node in node_states
        if node.contingency_landing_slots > 0
    )
    demand_diverts = sum(
        1
        for vehicle in active_vehicles
        if vehicle.state.reachable_landing_options <= 0 or vehicle.state.status == "holding"
    )
    mean_required_separation = (
        sum(corridor.required_separation for corridor in corridor_states) / len(corridor_states)
        if corridor_states
        else 0.0
    )
    lambda_e = total_flow / max(mean_capacity, 1e-9) if mean_capacity > 0.0 else 0.0

    return PhysicsStateSample(
        time_minute=time_minute,
        rho_e=total_density,
        q_e=total_flow,
        lambda_e=lambda_e,
        c_e=mean_capacity,
        w_e=disturbance.weather_severity,
        gamma_e=_gamma_effective(disturbance),
        eta_e=1.0,
        chi_e=_trust_degradation(active_vehicles),
        queue_e=queue_ratio,
        reserve_e=reserve_margin_min,
        base_separation=mean_required_separation,
        active_vehicle_count=len(active_vehicles),
        kappa_i=float(contingency_supply),
        r_e=reserve_margin_min,
        demand_diverts=float(demand_diverts),
    )
