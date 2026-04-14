from __future__ import annotations

import json
from importlib import resources


DEMO_SCENARIO_IDS = [
    "incident_diversion_balanced",
    "incident_diversion_avoidant",
    "metro_surge_balanced",
    "metro_surge_throughput_max",
]


def load_demo_replay_payloads() -> list[dict]:
    base = resources.files("avn.demo_assets")
    payloads: list[dict] = []
    for scenario_id in DEMO_SCENARIO_IDS:
        payload = json.loads(base.joinpath(f"{scenario_id}.json").read_text(encoding="utf-8"))
        payloads.append(payload)
    return payloads
