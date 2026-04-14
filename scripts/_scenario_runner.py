from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIVE_CONFIGS_DIR = ROOT / "configs"
ARCHIVED_CONFIGS_DIR = ROOT / "archive" / "legacy_runtime" / "configs"

_RUN_REPLACEMENTS = {
    "nominal.toml": "python -m avn run baseline_flow",
    "weather_disruption.toml": "python -m avn run weather_closure",
}


def _resolve_config_path(config_name: str) -> Path:
    live_path = LIVE_CONFIGS_DIR / config_name
    if live_path.exists():
        return live_path
    archived_path = ARCHIVED_CONFIGS_DIR / config_name
    return archived_path


def build_retirement_message(config_name: str) -> str:
    config_path = _resolve_config_path(config_name)
    lines = [
        "Legacy config runner retired.",
        f"Legacy config retained: {config_path.resolve()}",
        "The live package no longer exposes `avn.simulation.engine`; use the canonical `avn` CLI instead.",
    ]

    replacement = _RUN_REPLACEMENTS.get(config_name)
    if replacement is not None:
        lines.extend(
            [
                "Recommended canonical replacement:",
                f"  {replacement}",
                "Other canonical entrypoints:",
                "  python -m avn list-scenarios",
                "  python -m avn adaptive-sweep configs/example_adaptive_sweep_manifest.json",
            ]
        )
    else:
        lines.extend(
            [
                "Recommended canonical entrypoints:",
                "  python -m avn list-scenarios",
                "  python -m avn run <scenario-id>",
                "  python -m avn adaptive-sweep configs/example_adaptive_sweep_manifest.json",
            ]
        )
    return "\n".join(lines)


def run_named_config(config_name: str) -> None:
    print(build_retirement_message(config_name), file=sys.stderr)
    raise SystemExit(1)
