from __future__ import annotations

import argparse
import json
from pathlib import Path

from .artifacts import write_json
from .calibration import calibrate_bundle
from .config import load_scenario_config
from .engine import run_scenario
from .experiments import run_experiment
from .ingest import run_ingestion
from .reporting import build_report_from_directory


def _run_command(path: Path) -> int:
    config = load_scenario_config(path)
    result = run_scenario(config)
    print(f"Run directory: {result.output_dir}")
    print(f"Run summary: {result.run_summary_path}")
    print(f"Report bundle: {result.report_bundle_path}")
    return 0


def _experiment_command(path: Path) -> int:
    result = run_experiment(path)
    print(f"Experiment output: {result['output_dir']}")
    print(f"Experiment summary: {result['summary_path']}")
    return 0


def _calibrate_command(path: Path) -> int:
    result = calibrate_bundle(str(path))
    validation_path = path.parent / f"{path.stem}.bundle_validation.v2.json"
    calibration_path = path.parent / f"{path.stem}.calibration_report.v2.json"
    write_json(validation_path, result["bundle_validation"])
    write_json(calibration_path, result["calibration_report"])
    print(f"Bundle validation: {validation_path.resolve()}")
    print(f"Calibration report: {calibration_path.resolve()}")
    return 0


def _ingest_command(path: Path) -> int:
    result = run_ingestion(path)
    print(f"Raw staging: {result['raw_dir']}")
    print(f"Bundle directory: {result['bundle_dir']}")
    print(f"Bundle path: {result['bundle_path']}")
    print(f"Ingestion report: {result['report_path']}")
    return 0 if result["usable_bundle"] else 1


def _report_command(path: Path) -> int:
    report = build_report_from_directory(path)
    output_path = (path / "report_view.v2.json") if path.is_dir() else (path.parent / "report_view.v2.json")
    write_json(output_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"Report view: {output_path.resolve()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Auto-VTOL-Network v2 operational model.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a v2 scenario.")
    run_parser.add_argument("scenario", type=Path)

    experiment_parser = subparsers.add_parser("experiment", help="Run a v2 experiment manifest.")
    experiment_parser.add_argument("manifest", type=Path)

    calibrate_parser = subparsers.add_parser("calibrate", help="Run calibration against a reference bundle.")
    calibrate_parser.add_argument("bundle", type=Path)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest external reference data into a standard v2 bundle.")
    ingest_parser.add_argument("source_manifest", type=Path)

    report_parser = subparsers.add_parser("report", help="Render a report summary from a run or experiment directory.")
    report_parser.add_argument("path", type=Path)

    args = parser.parse_args(argv)
    if args.command == "run":
        return _run_command(args.scenario)
    if args.command == "experiment":
        return _experiment_command(args.manifest)
    if args.command == "calibrate":
        return _calibrate_command(args.bundle)
    if args.command == "ingest":
        return _ingest_command(args.source_manifest)
    if args.command == "report":
        return _report_command(args.path)
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
