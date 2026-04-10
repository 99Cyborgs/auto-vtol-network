from pathlib import Path
import random

from avn.comms import is_state_stale, update_information_age
from avn.contingency import compute_reachable_landing_options
from avn.core.models import (
    CorridorConfig,
    DisturbanceState,
    NodeConfig,
    ScenarioModifiers,
    VehicleConfig,
)
from avn.failure_classification import classify_failure
from avn.network.graph import VTOLNetwork
from avn.trust import can_file_intent, can_receive_reservation
from avn.vehicle.fleet import Vehicle


def test_trust_privileges_change_with_state() -> None:
    assert can_file_intent("trusted")
    assert can_file_intent("unknown")
    assert not can_file_intent("revoked")
    assert can_receive_reservation("trusted", degraded_mode=True)
    assert can_receive_reservation("degraded", degraded_mode=True)
    assert not can_receive_reservation("unknown", degraded_mode=True)
    assert can_receive_reservation("unknown", degraded_mode=False)


def test_information_age_grows_under_drops_and_becomes_stale() -> None:
    vehicle = Vehicle.from_config(
        VehicleConfig(
            vehicle_id="V1",
            mission_class="cargo",
            route=["A", "B"],
            reserve_energy=50.0,
        )
    ).state
    disturbance = DisturbanceState(
        weather_severity=0.0,
        comms_reliability=0.5,
        comms_latency_minutes=2.0,
        message_drop_probability=1.0,
        stale_after_minutes=6.0,
    )

    update_information_age(vehicle, disturbance, 5, random.Random(0))
    update_information_age(vehicle, disturbance, 5, random.Random(0))

    assert vehicle.information_age_minutes == 10.0
    assert is_state_stale(vehicle, disturbance)


def test_contingency_reachability_filters_by_reserve_margin() -> None:
    network = VTOLNetwork.from_config(
        nodes=[
            NodeConfig(node_id="A", node_type="hub", service_rate=10.0, contingency_capacity=4, contingency_landing_slots=0),
            NodeConfig(node_id="PAD", node_type="emergency", service_rate=4.0, contingency_capacity=2, contingency_landing_slots=1),
        ],
        corridors=[
            CorridorConfig(
                corridor_id="A_PAD",
                origin="A",
                destination="PAD",
                length=10.0,
                free_flow_speed=100.0,
                base_capacity=6.0,
                required_separation=40.0,
            )
        ],
        modifiers=ScenarioModifiers(),
    )
    vehicle = Vehicle.from_config(
        VehicleConfig(
            vehicle_id="V1",
            mission_class="cargo",
            route=["A", "PAD"],
            reserve_energy=20.0,
        )
    ).state
    disturbance = DisturbanceState(weather_severity=0.2, comms_reliability=1.0)

    reachable = compute_reachable_landing_options(network, vehicle, disturbance, reserve_multiplier=1.0)

    assert len(reachable) == 1
    assert reachable[0]["node_id"] == "PAD"


def test_failure_classifier_prefers_direct_trust_and_contingency_markers() -> None:
    assert classify_failure(
        "corridor_load_ratio",
        {
            "unsafe_admission_count": 1,
            "quarantine_count": 1,
            "revocation_count": 0,
            "no_admissible_landing_events": 0,
            "stale_state_exposure_minutes": 0.0,
        },
    ) == "trust_breakdown"
    assert classify_failure(
        "corridor_load_ratio",
        {
            "unsafe_admission_count": 0,
            "quarantine_count": 0,
            "revocation_count": 0,
            "no_admissible_landing_events": 2,
            "stale_state_exposure_minutes": 0.0,
        },
    ) == "contingency_unreachable"
