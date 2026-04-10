from __future__ import annotations

from dataclasses import dataclass
from math import exp

from avn.core.models import PhysicsModelConfig
from avn.physics.state_mapping import PhysicsStateSample


@dataclass(slots=True, frozen=True)
class DisturbanceResponse:
    alpha_e: float
    c_e: float
    s_e: float
    alpha_weather: float = 1.0
    alpha_comms: float = 1.0
    alpha_nav: float = 1.0
    alpha_trust: float = 1.0

    def to_dict(self) -> dict[str, float]:
        return {
            "alpha_e": self.alpha_e,
            "c_e": self.c_e,
            "s_e": self.s_e,
            "alpha_weather": self.alpha_weather,
            "alpha_comms": self.alpha_comms,
            "alpha_nav": self.alpha_nav,
            "alpha_trust": self.alpha_trust,
        }


def compute_disturbance_response(
    sample: PhysicsStateSample,
    physics_model: PhysicsModelConfig,
) -> DisturbanceResponse:
    alpha_weather = exp(-physics_model.a_w * sample.w_e)
    alpha_comms = exp(-physics_model.a_gamma * (1.0 - sample.gamma_e))
    alpha_nav = exp(-physics_model.a_eta * (1.0 - sample.eta_e))
    alpha_trust = exp(-physics_model.a_chi * sample.chi_e)
    alpha_e = alpha_weather * alpha_comms * alpha_nav * alpha_trust
    alpha_e = max(physics_model.minimum_alpha, min(1.0, alpha_e))
    effective_capacity = sample.c_e * alpha_e
    separation = sample.base_separation / max(alpha_e, physics_model.minimum_alpha)
    return DisturbanceResponse(
        alpha_e=alpha_e,
        c_e=effective_capacity,
        s_e=separation,
        alpha_weather=alpha_weather,
        alpha_comms=alpha_comms,
        alpha_nav=alpha_nav,
        alpha_trust=alpha_trust,
    )
