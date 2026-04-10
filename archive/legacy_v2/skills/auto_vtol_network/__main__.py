from __future__ import annotations

import argparse
from pathlib import Path

from .enums import EngineType
from .harness import load_request, load_v2_request, run_skill_pack


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the governed Auto VTOL Network skill pack.")
    parser.add_argument("--input", type=Path, help="Path to the JSON request payload.")
    parser.add_argument("--v2-input", type=Path, help="Path to an avn_v2 run directory or report bundle.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for emitted artifacts.")
    parser.add_argument(
        "--engine",
        action="append",
        choices=[engine.value for engine in EngineType],
        help="Optional engine selection. Repeat to run multiple engines.",
    )
    args = parser.parse_args(argv)

    if bool(args.input) == bool(args.v2_input):
        raise SystemExit("Provide exactly one of --input or --v2-input.")
    request = load_request(args.input) if args.input else load_v2_request(args.v2_input)
    selected = None if not args.engine else tuple(EngineType(engine) for engine in args.engine)
    receipt = run_skill_pack(request, output_dir=args.output_dir, selected_engines=selected)
    print(f"Run receipt: {args.output_dir.resolve() / 'run_receipt.json'}")
    print(f"Artifacts emitted: {len(receipt.artifacts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
