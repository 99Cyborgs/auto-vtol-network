from pathlib import Path

from avn.core.config import load_simulation_config
from avn.simulation.engine import SimulationEngine
from avn.sweep import run_phase2b_sweep


ROOT = Path(__file__).resolve().parents[1]
LEGACY_CONFIGS = ROOT / "archive" / "legacy_runtime" / "configs"


def _run_named_config(config_name: str, tmp_path: Path, leaf: str):
    config = load_simulation_config(LEGACY_CONFIGS / config_name)
    config.output_root = tmp_path / leaf
    result = SimulationEngine(config).run()
    assert result.metrics_path.exists()
    assert result.event_log_path.exists()
    assert result.run_summary_path.exists()
    assert result.threshold_summary_path.exists()
    return result


def test_legacy_nominal_remains_backward_compatible(tmp_path: Path) -> None:
    result = _run_named_config("nominal.toml", tmp_path, "nominal")

    assert result.summary["completed_vehicles"] == 14
    assert result.summary["incomplete_vehicles"] == 0
    assert result.summary["first_dominant_failure_mechanism"] == "coupled_failure_indeterminate"


def test_stale_state_scenario_produces_freshness_failure(tmp_path: Path) -> None:
    result = _run_named_config("stale_state_nominal_load.toml", tmp_path, "stale")

    assert result.summary["stale_state_exposure_minutes"] > 0
    assert result.summary["reservation_invalidations"] > 0 or result.summary["lost_link_fallback_activations"] > 0
    assert result.summary["first_dominant_failure_mechanism"] == "stale_information_instability"


def test_supplier_compromise_produces_trust_breakdown(tmp_path: Path) -> None:
    result = _run_named_config("supplier_compromise.toml", tmp_path, "supplier")

    assert result.summary["quarantine_count"] > 0
    assert result.summary["revocation_count"] > 0
    assert result.summary["first_dominant_failure_mechanism"] == "trust_breakdown"


def test_spoofed_vehicle_injection_produces_unsafe_admission_signal(tmp_path: Path) -> None:
    result = _run_named_config("spoofed_vehicle_injection.toml", tmp_path, "spoof")

    assert result.summary["unsafe_admission_count"] > 0
    assert result.summary["first_dominant_failure_mechanism"] == "trust_breakdown"


def test_contingency_saturation_produces_no_admissible_landing(tmp_path: Path) -> None:
    result = _run_named_config("weather_plus_contingency_saturation.toml", tmp_path, "contingency")

    assert result.summary["no_admissible_landing_events"] > 0
    assert result.summary["first_dominant_failure_mechanism"] == "contingency_unreachable"
    assert result.summary["dominant_failure_mode"] == "CONTINGENCY_SATURATION"
    assert result.summary["phase_detection"]["contingency_saturation"]["detected"] is True


def test_compound_scenario_is_deterministic_for_fixed_seed(tmp_path: Path) -> None:
    result_a = _run_named_config("trust_and_comms_compound.toml", tmp_path, "compound_a")
    result_b = _run_named_config("trust_and_comms_compound.toml", tmp_path, "compound_b")

    keys = [
        "completed_vehicles",
        "incomplete_vehicles",
        "unsafe_admission_count",
        "revocation_count",
        "stale_state_exposure_minutes",
        "reservation_invalidations",
        "lost_link_fallback_activations",
        "first_dominant_failure_mechanism",
    ]
    assert {key: result_a.summary[key] for key in keys} == {key: result_b.summary[key] for key in keys}


def test_phase2b_sweep_emits_aggregate_table(tmp_path: Path) -> None:
    result = run_phase2b_sweep(LEGACY_CONFIGS / "trust_and_comms_compound.toml", output_root=tmp_path)

    assert result.aggregate_csv_path.exists()
    assert result.summary_json_path.exists()
    assert len(result.rows) == 5
    assert {"label", "demand_multiplier", "first_dominant_failure_mechanism"} <= set(result.rows[0].keys())
