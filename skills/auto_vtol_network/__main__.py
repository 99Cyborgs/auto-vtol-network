from __future__ import annotations

import argparse
from pathlib import Path

from .enums import EngineType
from .harness import load_request, run_skill_pack


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the governed Auto VTOL Network skill pack.")
    parser.add_argument("--input", required=True, type=Path, help="Path to the JSON request payload.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for emitted artifacts.")
    parser.add_argument(
        "--engine",
        action="append",
        choices=[engine.value for engine in EngineType],
        help="Optional engine selection. Repeat to run multiple engines.",
    )
    args = parser.parse_args(argv)

    request = load_request(args.input)
    selected = None if not args.engine else tuple(EngineType(engine) for engine in args.engine)
    receipt = run_skill_pack(request, output_dir=args.output_dir, selected_engines=selected)
    print(f"Run receipt: {args.output_dir.resolve() / 'run_receipt.json'}")
    print(f"Artifacts emitted: {len(receipt.artifacts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
