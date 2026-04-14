from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_nominal_runner_is_retired_with_canonical_replacement() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_nominal.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Legacy config runner retired." in result.stderr
    assert "python -m avn run baseline_flow" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr


def test_weather_runner_is_retired_with_canonical_replacement() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_weather_case.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Legacy config runner retired." in result.stderr
    assert "python -m avn run weather_closure" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr


def test_historical_phase2b_runner_is_retired_with_generic_cli_guidance() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_trust_and_comms_compound.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Legacy config runner retired." in result.stderr
    assert str((ROOT / "archive" / "legacy_runtime" / "configs" / "trust_and_comms_compound.toml").resolve()) in result.stderr
    assert "python -m avn list-scenarios" in result.stderr
    assert "python -m avn adaptive-sweep configs/example_adaptive_sweep_manifest.json" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr
