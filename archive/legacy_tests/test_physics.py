from avn.core.models import DisturbanceState
from avn.physics.dynamics import (
    approximate_reserve_energy_drain,
    disturbance_modified_speed,
    effective_capacity_reduction,
    separation_inflation,
)


def test_capacity_reduction_under_disturbance() -> None:
    mild = DisturbanceState(weather_severity=0.1, comms_reliability=0.98)
    severe = DisturbanceState(weather_severity=0.8, comms_reliability=0.65)

    mild_speed = disturbance_modified_speed(140.0, mild)
    severe_speed = disturbance_modified_speed(140.0, severe)
    mild_sep = separation_inflation(40.0, mild)
    severe_sep = separation_inflation(40.0, severe)

    mild_capacity = effective_capacity_reduction(10.0, 140.0, mild_speed, 40.0, mild_sep, mild)
    severe_capacity = effective_capacity_reduction(10.0, 140.0, severe_speed, 40.0, severe_sep, severe)

    assert severe_speed < mild_speed < 140.0
    assert severe_sep > mild_sep > 40.0
    assert 0.25 <= severe_capacity < mild_capacity < 10.0


def test_reserve_energy_drain_increases_with_distance_and_weather() -> None:
    short_leg = approximate_reserve_energy_drain(10.0, 5.0, 0.1, mission_class="passenger", status="enroute")
    long_bad_weather = approximate_reserve_energy_drain(
        25.0, 5.0, 0.7, mission_class="passenger", status="enroute"
    )

    assert short_leg > 0.0
    assert long_bad_weather > short_leg

