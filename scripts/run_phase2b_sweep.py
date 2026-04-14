from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "archive" / "legacy_runtime" / "configs" / "trust_and_comms_compound.toml"
EXAMPLE_MANIFEST = ROOT / "configs" / "example_adaptive_sweep_manifest.json"


def build_retirement_message(config_path: Path) -> str:
    return "\n".join(
        [
            "Historical sweep shim retired.",
            f"Historical fixture retained: {config_path.resolve()}",
            "The live package no longer exposes `avn.sweep`; use the canonical governed sweep surface instead:",
            f"  python -m avn adaptive-sweep {EXAMPLE_MANIFEST.resolve()}",
            "The legacy TOML fixtures remain for historical reference only.",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Retired shim that redirects legacy sweep usage to the canonical governed entrypoint."
    )
    parser.add_argument(
        "config",
        nargs="?",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Historical TOML fixture retained for reference only.",
    )
    args = parser.parse_args(argv)
    print(build_retirement_message(args.config), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
