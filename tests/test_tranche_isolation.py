from pathlib import Path

import pytest

from avn.sweep_tranches import SeedPolicy, SweepAxis, TrancheDefinition, generate_tranche_slices


def test_tranche_isolation_prevents_cross_dominance() -> None:
    with pytest.raises(ValueError):
        TrancheDefinition(
            tranche_name="invalid",
            description="Two varying axes should be rejected.",
            base_config_path=Path("configs/nominal.toml"),
            fixed_params={},
            sweep_axes=(
                SweepAxis("alpha", (0.0, 1.0)),
                SweepAxis("beta", (0.0, 1.0)),
            ),
            expected_metrics=("dominant_failure_mode",),
            expected_failure_modes=("CORRIDOR_CONGESTION",),
            seed_policy=SeedPolicy(base_seed=1),
        )


def test_minimum_slice_count_is_enforced() -> None:
    tranche = TrancheDefinition(
        tranche_name="valid",
        description="Single dominant variable.",
        base_config_path=Path("configs/nominal.toml"),
        fixed_params={},
        sweep_axes=(SweepAxis("alpha", (0.0, 0.5, 1.0)),),
        expected_metrics=("dominant_failure_mode",),
        expected_failure_modes=("CORRIDOR_CONGESTION",),
        seed_policy=SeedPolicy(base_seed=1),
        minimum_slice_count=3,
    )

    with pytest.raises(ValueError):
        generate_tranche_slices(tranche, max_slices=2)
