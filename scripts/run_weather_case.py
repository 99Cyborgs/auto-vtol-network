from __future__ import annotations

from _scenario_runner import run_named_config


def main() -> None:
    run_named_config("weather_disruption.toml")


if __name__ == "__main__":
    main()
