from __future__ import annotations

import argparse
import json
from pathlib import Path

from avn.demo_assets import load_demo_replay_payloads
from avn.governance.sweep import run_adaptive_sweep
from avn.governance.validation import validate_batch_directory, validate_run_directory
from avn.sim.batch import run_scenario_batch
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
    try:
        server = run_dashboard(
            scenario=args.scenario,
            scenarios=getattr(args, "scenarios", None),
            replay_path=args.replay,
            replay_payloads=getattr(args, "replay_payloads", None),
            host=args.host,
            port=args.port,
        )
    except ValueError as exc:
        raise SystemExit(f"Dashboard startup failed: {exc}") from exc
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


def _validate_batch_command(args: argparse.Namespace) -> int:
    report = validate_batch_directory(args.batch_dir)
    print(json.dumps(report.to_dict(), indent=2))
    return 0 if report.status == "passed" else 1


def _adaptive_sweep_command(args: argparse.Namespace) -> int:
    try:
        result, paths = run_adaptive_sweep(args.manifest)
    except FileExistsError as exc:
        raise SystemExit(
            f"Adaptive sweep output directory already exists. Remove it or change the manifest sweep_id/output_root: {exc.filename}"
        ) from exc
    print(f"Sweep: {result.sweep_id}")
    print(f"Adaptive sweep artifact: {paths['adaptive_sweep']}")
    print(f"Validation report: {paths['validation_report']}")
    print(f"Artifact manifest: {paths['artifact_manifest']}")
    print(json.dumps(result.to_dict(), indent=2))
    return 0


def _batch_run_command(args: argparse.Namespace) -> int:
    result = run_scenario_batch(
        args.scenarios or None,
        repeat=args.repeat,
        output_root=args.output_root,
        batch_id=args.batch_id,
    )
    print(f"Batch: {result.batch_id}")
    print(f"Output directory: {result.output_dir}")
    print(f"Batch summary: {result.summary_path}")
    print(f"Batch manifest: {result.manifest_path}")
    print(json.dumps(result.summary["suite_statistics"], indent=2))
    return 0


def _demo_command(args: argparse.Namespace) -> int:
    return _dashboard_command(
        argparse.Namespace(
            scenario=None,
            scenarios=None,
            replay=None,
            replay_payloads=load_demo_replay_payloads(),
            host=args.host,
            port=args.port,
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Auto-VTOL-Network simulator and dashboard.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a deterministic scenario headlessly.")
    run_parser.add_argument("scenario", help="Built-in scenario id or a path to a scenario JSON file.")
    run_parser.add_argument("--output-root", type=Path, default=None)
    run_parser.set_defaults(handler=_run_command)

    batch_parser = subparsers.add_parser(
        "batch-run",
        help="Run one or more deterministic scenarios and write aggregate summary statistics.",
    )
    batch_parser.add_argument(
        "scenarios",
        nargs="*",
        help="Optional built-in scenario ids or scenario JSON paths. Defaults to the full built-in suite.",
    )
    batch_parser.add_argument("--repeat", type=int, default=1, help="Number of runs to execute per scenario.")
    batch_parser.add_argument("--output-root", type=Path, default=None)
    batch_parser.add_argument("--batch-id", default=None)
    batch_parser.set_defaults(handler=_batch_run_command)

    dashboard_parser = subparsers.add_parser("dashboard", help="Launch the operator dashboard.")
    dashboard_source = dashboard_parser.add_mutually_exclusive_group()
    dashboard_source.add_argument("--scenario", default=None, help="Optional built-in scenario id.")
    dashboard_source.add_argument(
        "--replay",
        type=Path,
        default=None,
        help="Optional path to a saved replay.json bundle.",
    )
    dashboard_parser.add_argument("--host", default="127.0.0.1")
    dashboard_parser.add_argument("--port", type=int, default=8000)
    dashboard_parser.set_defaults(handler=_dashboard_command)

    list_parser = subparsers.add_parser("list-scenarios", help="List built-in scenarios.")
    list_parser.set_defaults(handler=lambda _args: print("\n".join(list_scenarios())) or 0)

    demo_parser = subparsers.add_parser("demo", help="Launch the curated presentation scenario set.")
    demo_parser.add_argument("--host", default="127.0.0.1")
    demo_parser.add_argument("--port", type=int, default=8000)
    demo_parser.set_defaults(handler=_demo_command)

    validate_parser = subparsers.add_parser("validate-run", help="Validate a canonical run artifact directory.")
    validate_parser.add_argument("run_dir", type=Path)
    validate_parser.set_defaults(handler=_validate_run_command)

    validate_batch_parser = subparsers.add_parser("validate-batch", help="Validate an aggregate batch artifact directory.")
    validate_batch_parser.add_argument("batch_dir", type=Path)
    validate_batch_parser.set_defaults(handler=_validate_batch_command)

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
