from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from avn.sweep import run_phase2b_sweep


def main() -> None:
    result = run_phase2b_sweep(ROOT / "configs" / "trust_and_comms_compound.toml")
    print(f"Sweep directory: {result.output_dir}")
    print(f"Aggregate CSV: {result.aggregate_csv_path}")
    print(f"Summary JSON: {result.summary_json_path}")
    print("Labels: " + ", ".join(str(row["label"]) for row in result.rows))


if __name__ == "__main__":
    main()
