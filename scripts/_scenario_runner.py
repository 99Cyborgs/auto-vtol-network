from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from avn.simulation.engine import run_from_config


def run_named_config(config_name: str) -> None:
    result = run_from_config(ROOT / "configs" / config_name)
    print(f"Scenario: {result.scenario_name}")
    print(f"Run directory: {result.output_dir}")
    print(f"Metrics CSV: {result.metrics_path}")
    print(f"Event log JSON: {result.event_log_path}")
    print(f"Run summary JSON: {result.run_summary_path}")
    print(f"Threshold summary JSON: {result.threshold_summary_path}")
    print(f"Plots: {', '.join(str(path) for path in result.plot_paths)}")
    print(
        "Summary: "
        f"completed={result.summary['completed_vehicles']}, "
        f"incomplete={result.summary['incomplete_vehicles']}, "
        f"final_avg_queue={result.summary['avg_queue_length']:.2f}, "
        f"peak_avg_queue={result.summary['peak_avg_queue_length']:.2f}, "
        f"mean_speed={result.summary['mean_corridor_speed']:.2f} km/h, "
        f"mean_reserve={result.summary['mean_reserve_energy']:.2f}, "
        f"failure={result.summary['first_dominant_failure_mechanism']}"
    )
