from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from avn.demo_assets import DEMO_SCENARIO_IDS
from avn.sim.runner import run_scenario


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "src" / "avn" / "demo_assets"


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="avn-demo-refresh-") as temp_dir:
        output_root = Path(temp_dir)
        for scenario_id in DEMO_SCENARIO_IDS:
            result = run_scenario(scenario_id, output_root=output_root)
            payload = json.loads(result.replay_path.read_text(encoding="utf-8"))
            target = OUTPUT_DIR / f"{scenario_id}.json"
            target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(f"Refreshed demo replay: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
