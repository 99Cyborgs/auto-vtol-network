from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from urllib.parse import parse_qs, urlparse

from avn.sim.event_loop import Simulator
from avn.sim.scenario_loader import list_scenarios, load_scenario
from avn.ui.serializers import serialize_replay


def _dashboard_asset(name: str) -> str:
    return resources.files("avn.ui.dashboard").joinpath(name).read_text(encoding="utf-8")


class DashboardHandler(BaseHTTPRequestHandler):
    bundles: dict[str, dict] = {}

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
            payload = [{"scenario_id": name, "name": self.bundles[name]["name"]} for name in sorted(self.bundles)]
            self._write_json(payload)
            return
        if parsed.path == "/api/replay":
            query = parse_qs(parsed.query)
            scenario_id = query.get("scenario", [next(iter(self.bundles))])[0]
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


def run_dashboard(
    *,
    scenario: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> ThreadingHTTPServer:
    scenario_ids = [scenario] if scenario else list_scenarios()
    DashboardHandler.bundles = {}
    for scenario_id in scenario_ids:
        DashboardHandler.bundles[scenario_id] = serialize_replay(Simulator(load_scenario(scenario_id)).run())
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard listening on http://{host}:{port}")
    return server
