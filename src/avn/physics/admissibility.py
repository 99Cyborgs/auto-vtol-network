from __future__ import annotations

from dataclasses import dataclass

from avn.core.models import AdmissibilityConfig
from avn.physics.disturbance_model import DisturbanceResponse
from avn.physics.state_mapping import PhysicsStateSample


@dataclass(slots=True, frozen=True)
class AdmissibilityResult:
    inside_A: bool
    status: str
    margins: dict[str, float]
    violated_constraints: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "inside_A": self.inside_A,
            "status": self.status,
            "margins": dict(self.margins),
            "violated_constraints": list(self.violated_constraints),
        }


def evaluate_admissibility(
    sample: PhysicsStateSample,
    response: DisturbanceResponse,
    admissibility: AdmissibilityConfig,
) -> AdmissibilityResult:
    margins = {
        "rho_e": admissibility.rho_safe - sample.rho_e,
        "queue": admissibility.queue_safe - sample.queue_e,
        "gamma": sample.gamma_e - admissibility.gamma_min,
        "trust": admissibility.chi_max - sample.chi_e,
        "reserve": sample.reserve_e - admissibility.reserve_min,
        "capacity": response.c_e,
    }
    violated = tuple(key for key, margin in margins.items() if key != "capacity" and margin < 0.0)
    inside_A = not violated
    return AdmissibilityResult(
        inside_A=inside_A,
        status="inside_A" if inside_A else f"outside_A:{','.join(violated)}",
        margins=margins,
        violated_constraints=violated,
    )
