from avn.core.models import (
    CorridorConfig,
    DisturbanceState,
    NodeConfig,
    PhysicsModelConfig,
    ScenarioModifiers,
    VehicleConfig,
)
from avn.network.graph import VTOLNetwork
from avn.physics.disturbance_model import compute_disturbance_response
from avn.physics.state_mapping import map_engine_state
from avn.vehicle.fleet import build_fleet


def test_increasing_weather_reduces_capacity() -> None:
    network = VTOLNetwork.from_config(
        nodes=[
            NodeConfig(node_id="A", node_type="hub", service_rate=60.0, contingency_capacity=4),
            NodeConfig(node_id="B", node_type="hub", service_rate=60.0, contingency_capacity=4),
        ],
        corridors=[
            CorridorConfig(
                corridor_id="AB",
                origin="A",
                destination="B",
                length=20.0,
                free_flow_speed=120.0,
                base_capacity=30.0,
                required_separation=30.0,
            )
        ],
        modifiers=ScenarioModifiers(),
    )
    vehicles = build_fleet(
        [
            VehicleConfig(vehicle_id="V1", mission_class="passenger", route=["A", "B"], reserve_energy=50.0),
            VehicleConfig(vehicle_id="V2", mission_class="passenger", route=["A", "B"], reserve_energy=50.0),
        ],
        demand_multiplier=1.0,
    )
    mild = DisturbanceState(weather_severity=0.1, comms_reliability=0.98)
    severe = DisturbanceState(weather_severity=0.7, comms_reliability=0.98)
    network.prepare_step(mild, 5.0)
    network.finalize_step(5.0)

    mild_sample = map_engine_state(network, vehicles, mild, time_minute=0)
    severe_sample = map_engine_state(network, vehicles, severe, time_minute=0)

    physics_model = PhysicsModelConfig()
    mild_response = compute_disturbance_response(mild_sample, physics_model)
    severe_response = compute_disturbance_response(severe_sample, physics_model)

    assert severe_sample.w_e > mild_sample.w_e
    assert severe_response.alpha_e < mild_response.alpha_e
    assert severe_response.c_e < mild_response.c_e
