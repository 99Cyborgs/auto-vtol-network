from __future__ import annotations

import argparse
import json
from pathlib import Path

from avn.governance.sweep import run_adaptive_sweep
from avn.governance.validation import validate_run_directory
from avn.sim.runner import run_scenario
from avn.sim.scenario_loader import list_scenarios
from avn.ui.api import run_dashboard


def _run_command(args: argparse.Namespace) -> int:
    result = run_scenario(args.scenario, output_root=args.output_root)
    print(f"Scenario: {result.scenario_id}")
    print(f"Run directory: {result.output_dir}")
    print(f"Replay: {result.replay_path}")
    print(f"Summary: {result.summary_path}")
    print(f"Threshold ledger: {result.threshold_ledger_path}")
    print(f"Promotion decisions: {result.promotion_decisions_path}")
    print(f"Validation report: {result.validation_report_path}")
    print(f"Artifact manifest: {result.artifact_manifest_path}")
    print(json.dumps(result.replay.summary, indent=2))
    return 0


def _dashboard_command(args: argparse.Namespace) -> int:
    server = run_dashboard(scenario=args.scenario, host=args.host, port=args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _validate_run_command(args: argparse.Namespace) -> int:
    report = validate_run_directory(args.run_dir)
    print(json.dumps(report.to_dict(), indent=2))
    return 0 if report.status == "passed" else 1


def _adaptive_sweep_command(args: argparse.Namespace) -> int:
    result, paths = run_adaptive_sweep(args.manifest)
    print(f"Sweep: {result.sweep_id}")
    print(f"Adaptive sweep artifact: {paths['adaptive_sweep']}")
    print(f"Validation report: {paths['validation_report']}")
    print(f"Artifact manifest: {paths['artifact_manifest']}")
    print(json.dumps(result.to_dict(), indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Auto-VTOL-Network simulator and dashboard.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a deterministic scenario headlessly.")
    run_parser.add_argument("scenario", help="Built-in scenario id or a path to a scenario JSON file.")
    run_parser.add_argument("--output-root", type=Path, default=None)
    run_parser.set_defaults(handler=_run_command)

    dashboard_parser = subparsers.add_parser("dashboard", help="Launch the operator dashboard.")
    dashboard_parser.add_argument("--scenario", default=None, help="Optional built-in scenario id.")
    dashboard_parser.add_argument("--host", default="127.0.0.1")
    dashboard_parser.add_argument("--port", type=int, default=8000)
    dashboard_parser.set_defaults(handler=_dashboard_command)

    list_parser = subparsers.add_parser("list-scenarios", help="List built-in scenarios.")
    list_parser.set_defaults(handler=lambda _args: print("\n".join(list_scenarios())) or 0)

    demo_parser = subparsers.add_parser("demo", help="Launch the recommended dashboard scenario.")
    demo_parser.add_argument("--host", default="127.0.0.1")
    demo_parser.add_argument("--port", type=int, default=8000)
    demo_parser.set_defaults(
        handler=lambda args: _dashboard_command(
            argparse.Namespace(scenario="weather_closure", host=args.host, port=args.port)
        )
    )

    validate_parser = subparsers.add_parser("validate-run", help="Validate a canonical run artifact directory.")
    validate_parser.add_argument("run_dir", type=Path)
    validate_parser.set_defaults(handler=_validate_run_command)

    sweep_parser = subparsers.add_parser(
        "adaptive-sweep",
        help="Run the canonical governed adaptive sweep from a manifest JSON file.",
    )
    sweep_parser.add_argument("manifest", type=Path)
    sweep_parser.set_defaults(handler=_adaptive_sweep_command)

    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
