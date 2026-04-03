from avn.physics.dynamics import step_node_queue


def test_queue_stepping_accumulates_fractional_service_credit() -> None:
    served_1, remaining_1, credit_1 = step_node_queue(3, 6.0, 5.0)
    served_2, remaining_2, credit_2 = step_node_queue(
        remaining_1,
        6.0,
        5.0,
        service_credit=credit_1,
    )

    assert served_1 == 0
    assert remaining_1 == 3
    assert 0.49 < credit_1 < 0.51

    assert served_2 == 1
    assert remaining_2 == 2
    assert credit_2 == 0.0

