from avn.physics.admissibility import AdmissibilityResult
from avn.physics.disturbance_model import DisturbanceResponse
from avn.physics.phase_detection import detect_phase_events
from avn.physics.state_mapping import PhysicsStateSample


def test_phase_detection_identifies_synthetic_transitions() -> None:
    samples = [
        PhysicsStateSample(0, 0.5, 0.5, 0.8, 1.0, 0.1, 0.95, 1.0, 0.1, 0.1, 5.0, 30.0, 5),
        PhysicsStateSample(5, 0.8, 0.9, 0.9, 1.0, 0.1, 0.90, 1.0, 0.1, 0.2, 4.5, 30.0, 5),
        PhysicsStateSample(10, 1.1, 0.85, 1.2, 1.0, 0.2, 0.78, 1.0, 0.1, 0.4, 3.0, 30.0, 5, kappa_i=0.0, r_e=3.0, demand_diverts=1.0),
        PhysicsStateSample(15, 1.4, 0.70, 1.3, 1.0, 0.5, 0.72, 1.0, 0.2, 0.7, -0.5, 30.0, 5, kappa_i=0.0, r_e=-0.5, demand_diverts=2.0),
    ]
    responses = [
        DisturbanceResponse(alpha_e=0.95, c_e=0.95, s_e=31.6),
        DisturbanceResponse(alpha_e=0.90, c_e=0.90, s_e=33.3),
        DisturbanceResponse(alpha_e=0.70, c_e=0.70, s_e=42.8),
        DisturbanceResponse(alpha_e=0.55, c_e=0.55, s_e=54.5),
    ]
    admissibility = [
        AdmissibilityResult(True, "inside_A", {}, ()),
        AdmissibilityResult(True, "inside_A", {}, ()),
        AdmissibilityResult(False, "outside_A:gamma", {"gamma": -0.02}, ("gamma",)),
        AdmissibilityResult(False, "outside_A:gamma,reserve", {"gamma": -0.08, "reserve": -0.5}, ("gamma", "reserve")),
    ]

    events = detect_phase_events(samples, responses, admissibility)

    assert events["flow_breakdown"].detected is True
    assert events["queue_divergence"].detected is True
    assert events["admissibility_exit"].detected is True
    assert events["contingency_saturation"].detected is True
    assert events["comms_failure"].detected is True
    assert events["weather_collapse"].detected is True
