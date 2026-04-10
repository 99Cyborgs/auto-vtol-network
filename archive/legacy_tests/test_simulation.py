from pathlib import Path

from avn.core.models import CorridorConfig, DisturbanceState, NodeConfig, SimulationConfig, VehicleConfig
from avn.simulation.engine import SimulationEngine


def test_basic_route_progression(tmp_path: Path) -> None:
    config = SimulationConfig(
        scenario_name="unit_route",
        description="Single vehicle single corridor test",
        time_step_minutes=5,
        duration_minutes=10,
        output_root=tmp_path,
        nodes=[
            NodeConfig(node_id="A", node_type="hub", service_rate=60.0, contingency_capacity=4),
            NodeConfig(node_id="B", node_type="hub", service_rate=60.0, contingency_capacity=4),
        ],
        corridors=[
            CorridorConfig(
                corridor_id="AB",
                origin="A",
                destination="B",
                length=10.0,
                free_flow_speed=120.0,
                base_capacity=60.0,
                required_separation=30.0,
            )
        ],
        vehicles=[
            VehicleConfig(
                vehicle_id="VEH_1",
                mission_class="passenger",
                route=["A", "B"],
                reserve_energy=100.0,
            )
        ],
        disturbance_base=DisturbanceState(weather_severity=0.0, comms_reliability=1.0),
    )

    result = SimulationEngine(config).run()

    assert result.summary["completed_vehicles"] == 1
    assert result.summary["incomplete_vehicles"] == 0
    assert result.metrics_path.exists()
    assert result.event_log_path.exists()
    assert len(result.plot_paths) >= 3

