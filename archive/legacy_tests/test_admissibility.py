from avn.core.models import AdmissibilityConfig
from avn.physics.admissibility import evaluate_admissibility
from avn.physics.disturbance_model import DisturbanceResponse
from avn.physics.state_mapping import PhysicsStateSample


def test_admissibility_flags_gamma_and_reserve_violations() -> None:
    sample = PhysicsStateSample(
        time_minute=10,
        rho_e=0.8,
        q_e=1.2,
        lambda_e=1.1,
        c_e=0.9,
        w_e=0.2,
        gamma_e=0.6,
        eta_e=1.0,
        chi_e=0.2,
        queue_e=0.4,
        reserve_e=-1.0,
        base_separation=30.0,
        active_vehicle_count=4,
    )
    admissibility = evaluate_admissibility(
        sample,
        DisturbanceResponse(alpha_e=0.7, c_e=0.63, s_e=42.0),
        AdmissibilityConfig(rho_safe=1.0, queue_safe=0.5, gamma_min=0.8, chi_max=0.6, reserve_min=0.0),
    )

    assert admissibility.inside_A is False
    assert "gamma" in admissibility.violated_constraints
    assert "reserve" in admissibility.violated_constraints
