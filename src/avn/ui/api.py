from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from avn.core.policies import DEFAULT_POLICY_ID, get_policy_profile
from avn.sim.engine import SimulationEngine
from avn.sim.replay import load_replay_bundle
from avn.sim.scenario_loader import list_scenarios, load_scenario
from avn.ui.serializers import serialize_replay


def _dashboard_asset(name: str) -> str:
    return resources.files("avn.ui.dashboard").joinpath(name).read_text(encoding="utf-8")


class DashboardHandler(BaseHTTPRequestHandler):
    bundles: dict[str, dict] = {}
    scenario_order: list[str] = []

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html"}:
            self._write_text(_dashboard_asset("index.html"), "text/html; charset=utf-8")
            return
        if parsed.path == "/app.js":
            self._write_text(_dashboard_asset("app.js"), "application/javascript; charset=utf-8")
            return
        if parsed.path == "/styles.css":
            self._write_text(_dashboard_asset("styles.css"), "text/css; charset=utf-8")
            return
        if parsed.path == "/api/scenarios":
            ordered = DashboardHandler.scenario_order or sorted(self.bundles)
            payload = [{"scenario_id": name, "name": self.bundles[name]["name"]} for name in ordered]
            self._write_json(payload)
            return
        if parsed.path == "/api/replay":
            query = parse_qs(parsed.query)
            default_scenario = (DashboardHandler.scenario_order or list(self.bundles))[0]
            scenario_id = query.get("scenario", [default_scenario])[0]
            if scenario_id not in self.bundles:
                self.send_error(404, f"Unknown scenario: {scenario_id}")
                return
            self._write_json(self.bundles[scenario_id])
            return
        self.send_error(404)

    def log_message(self, fmt: str, *args) -> None:
        return

    def _write_json(self, payload) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _write_text(self, payload: str, content_type: str) -> None:
        encoded = payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def _validate_replay_payload(payload: dict) -> dict:
    if "policy" not in payload:
        profile = get_policy_profile(DEFAULT_POLICY_ID)
        payload["policy"] = {
            "policy_id": profile.policy_id,
            "label": profile.label,
            "description": profile.description,
        }

    required_top_level = {"scenario_id", "name", "description", "policy", "summary", "steps", "event_log"}
    missing = sorted(required_top_level - set(payload))
    if missing:
        raise ValueError(f"Replay payload is missing required top-level fields: {', '.join(missing)}")
    if not isinstance(payload["steps"], list) or not payload["steps"]:
        raise ValueError("Replay payload must include a non-empty steps list.")
    if not isinstance(payload["policy"], dict) or {"policy_id", "label", "description"} - set(payload["policy"]):
        raise ValueError("Replay payload must include a policy object with policy_id, label, and description.")

    required_step_fields = {"nodes", "corridors", "vehicles", "metrics", "alerts", "events"}
    missing_step_fields = sorted(required_step_fields - set(payload["steps"][0]))
    if missing_step_fields:
        raise ValueError(
            "Replay payload is missing required step fields: " + ", ".join(missing_step_fields)
        )
    return payload


def _load_dashboard_replay(replay_path: str | Path) -> dict:
    try:
        payload = load_replay_bundle(replay_path)
    except FileNotFoundError as exc:
        raise ValueError(f"Replay file does not exist: {Path(replay_path)}") from exc
    except OSError as exc:
        raise ValueError(f"Replay file could not be read: {Path(replay_path)}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Replay file is not valid JSON: {Path(replay_path)}") from exc

    return _validate_replay_payload(payload)


def run_dashboard(
    *,
    scenario: str | None = None,
    scenarios: list[str] | None = None,
    replay_path: str | Path | None = None,
    replay_payloads: list[dict] | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> ThreadingHTTPServer:
    DashboardHandler.bundles = {}
    DashboardHandler.scenario_order = []
    if replay_payloads is not None:
        for payload in replay_payloads:
            validated = _validate_replay_payload(dict(payload))
            DashboardHandler.bundles[validated["scenario_id"]] = validated
            DashboardHandler.scenario_order.append(validated["scenario_id"])
    elif replay_path is not None:
        payload = _load_dashboard_replay(replay_path)
        DashboardHandler.bundles[payload["scenario_id"]] = payload
        DashboardHandler.scenario_order = [payload["scenario_id"]]
    else:
        scenario_ids = list(scenarios) if scenarios is not None else ([scenario] if scenario else list_scenarios())
        for scenario_id in scenario_ids:
            DashboardHandler.bundles[scenario_id] = serialize_replay(SimulationEngine(load_scenario(scenario_id)).run())
        DashboardHandler.scenario_order = list(scenario_ids)
    if not DashboardHandler.scenario_order:
        raise ValueError("Dashboard requires at least one scenario or replay payload.")
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard listening on http://{host}:{server.server_port}")
    return server
