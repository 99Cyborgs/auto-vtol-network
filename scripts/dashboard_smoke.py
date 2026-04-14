from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from avn.demo_assets import DEMO_SCENARIO_IDS  # noqa: E402
from avn.sim.runner import run_scenario  # noqa: E402


OUTPUT_ROOT = REPO_ROOT / "output" / "playwright"
DEMO_ARTIFACT_DIR = OUTPUT_ROOT / "demo_smoke"
REPLAY_ARTIFACT_DIR = OUTPUT_ROOT / "replay_smoke"
SMOKE_REPLAY_NAME = "Replay Smoke Override"
SMOKE_REPLAY_DESCRIPTION = "Replay-backed dashboard validation should render this saved artifact directly."
STATE_EXPR = (
    "({ "
    "scenarioOptions: Array.from(document.querySelectorAll('#scenario-select option')).map((option) => ({ "
    "value: option.value, label: option.textContent.trim() })), "
    "selectedScenario: document.querySelector('#scenario-select')?.value ?? null, "
    "scenarioName: document.querySelector('#scenario-name')?.textContent.trim() ?? '', "
    "scenarioDescription: document.querySelector('#scenario-description')?.textContent.trim() ?? '', "
    "policyText: document.querySelector('#policy-panel')?.textContent.trim() ?? '', "
    "metricCount: document.querySelectorAll('#metric-strip .metric').length, "
    "networkShapeCount: document.querySelectorAll('#network-canvas line, #network-canvas circle').length, "
    "timelineValue: Number(document.querySelector('#timeline')?.value ?? 0), "
    "timelineMax: Number(document.querySelector('#timeline')?.max ?? 0), "
    "frameNote: document.querySelector('#frame-note')?.textContent.trim() ?? '', "
    "playLabel: document.querySelector('#play-toggle')?.textContent.trim() ?? '' "
    "})"
)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd or REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def _server_env() -> dict[str, str]:
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH")
    src_path = str(SRC_ROOT)
    repo_path = str(REPO_ROOT)
    env["PYTHONPATH"] = os.pathsep.join(
        value for value in (src_path, repo_path, current_pythonpath) if value
    )
    return env


def _npx_command() -> list[str]:
    npx = shutil.which("npx.cmd") or shutil.which("npx")
    if not npx:
        raise RuntimeError("npx.cmd was not found on PATH. Install Node.js/npm before running dashboard smoke.")
    return [npx, "--yes", "--package", "@playwright/cli", "playwright-cli"]


def _playwright_command(session: str | None, *args: str) -> list[str]:
    command = _npx_command()
    if session:
        command.append(f"-s={session}")
    command.extend(args)
    return command


def _playwright_cache_root() -> Path:
    local_appdata = os.environ.get("LOCALAPPDATA")
    if local_appdata:
        return Path(local_appdata) / "ms-playwright"
    return Path.home() / ".cache" / "ms-playwright"


def _ensure_browser_installed() -> None:
    if any(_playwright_cache_root().glob("chromium-*")):
        return
    print("Installing Playwright chromium browser...")
    _run(_playwright_command(None, "install-browser", "chromium"), timeout=900)


def _playwright_eval(session: str, expression: str, *, cwd: Path) -> Any:
    result = _run(_playwright_command(session, "eval", expression, "--raw"), cwd=cwd, timeout=180)
    payload = result.stdout.strip()
    if not payload:
        raise RuntimeError(f"Playwright eval returned no output.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Playwright eval returned non-JSON output: {payload}") from exc


def _playwright_run_code(session: str, code: str, *, cwd: Path) -> Any:
    result = _run(_playwright_command(session, "run-code", code, "--raw"), cwd=cwd, timeout=180)
    payload = result.stdout.strip()
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Playwright run-code returned non-JSON output: {payload}") from exc


def _playwright_snapshot(session: str, target: Path, *, cwd: Path) -> None:
    result = _run(_playwright_command(session, "snapshot"), cwd=cwd, timeout=180)
    target.write_text(result.stdout, encoding="utf-8")


def _playwright_screenshot(session: str, target: Path, *, cwd: Path) -> None:
    code = (
        "async (page) => { "
        f"await page.screenshot({{ path: {json.dumps(target.as_posix())} }}); "
        f"return {{ path: {json.dumps(target.as_posix())} }}; "
        "}"
    )
    _playwright_run_code(session, code, cwd=cwd)


def _close_session(session: str, *, cwd: Path) -> None:
    _run(_playwright_command(session, "close"), cwd=cwd)


def _capture_state(session: str, *, cwd: Path) -> dict[str, Any]:
    value = _playwright_eval(session, STATE_EXPR, cwd=cwd)
    _assert(isinstance(value, dict), "Dashboard state probe did not return an object.")
    return value


def _reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_http(url: str, *, timeout: int = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except URLError:
            time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {url}")


@contextmanager
def _dashboard_server(command: list[str], *, label: str, port: int, artifact_dir: Path):
    stdout_path = artifact_dir / f"{label}.stdout.log"
    stderr_path = artifact_dir / f"{label}.stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        env=_server_env(),
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
    )
    try:
        try:
            _wait_for_http(f"http://127.0.0.1:{port}/api/scenarios")
        except Exception:
            stdout_handle.flush()
            stderr_handle.flush()
            raise RuntimeError(
                f"{label} server failed to start. STDOUT:\n{stdout_path.read_text(encoding='utf-8')}\n"
                f"STDERR:\n{stderr_path.read_text(encoding='utf-8')}"
            ) from None
        yield
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        stdout_handle.close()
        stderr_handle.close()


def _wait_for_state(
    session: str,
    predicate,
    *,
    cwd: Path,
    timeout: int = 15,
) -> dict[str, Any]:
    deadline = time.time() + timeout
    last_state: dict[str, Any] | None = None
    while time.time() < deadline:
        last_state = _capture_state(session, cwd=cwd)
        if predicate(last_state):
            return last_state
        time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for dashboard state. Last state: {last_state}")


def _override_replay_bundle(source_path: Path, target_dir: Path) -> tuple[Path, dict[str, Any]]:
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    payload["name"] = SMOKE_REPLAY_NAME
    payload["description"] = SMOKE_REPLAY_DESCRIPTION
    target_path = target_dir / "replay_override.json"
    target_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target_path, payload


def _run_demo_smoke(*, headed: bool) -> None:
    artifact_dir = DEMO_ARTIFACT_DIR
    artifact_dir.mkdir(parents=True, exist_ok=True)
    session = f"avn-demo-smoke-{os.getpid()}"
    port = _reserve_port()
    command = [sys.executable, "-m", "avn", "demo", "--host", "127.0.0.1", "--port", str(port)]
    if headed:
        print("Running demo smoke in headed mode.")
    with _dashboard_server(command, label="demo", port=port, artifact_dir=artifact_dir):
        _close_session(session, cwd=artifact_dir)
        open_command = _playwright_command(session, "open", f"http://127.0.0.1:{port}", "--persistent")
        if headed:
            open_command.append("--headed")
        _run(open_command, cwd=artifact_dir, timeout=180)
        try:
            initial_state = _wait_for_state(
                session,
                lambda state: state["metricCount"] >= 5 and state["networkShapeCount"] > 0,
                cwd=artifact_dir,
            )
            _write_json(artifact_dir / "initial_state.json", initial_state)
            _playwright_snapshot(session, artifact_dir / "initial_snapshot.txt", cwd=artifact_dir)
            _playwright_screenshot(session, artifact_dir / "initial_view.png", cwd=artifact_dir)

            _assert(
                [item["value"] for item in initial_state["scenarioOptions"]] == DEMO_SCENARIO_IDS,
                f"Curated scenario order drifted: {initial_state['scenarioOptions']}",
            )
            _assert(initial_state["scenarioName"], "Demo did not render a scenario title.")
            _assert(initial_state["policyText"], "Demo did not render policy metadata.")
            _assert(initial_state["metricCount"] >= 5, "Demo metric strip did not render expected metrics.")
            _assert(initial_state["networkShapeCount"] > 0, "Demo SVG network did not render shapes.")

            step_state = _playwright_eval(
                session,
                "(() => { "
                "document.querySelector('#step-forward').click(); "
                "return { "
                "timelineValue: Number(document.querySelector('#timeline').value), "
                "minute: Number(document.querySelector('#summary strong').textContent), "
                "frameNote: document.querySelector('#frame-note').textContent.trim() "
                "}; "
                "})()",
                cwd=artifact_dir,
            )
            _write_json(artifact_dir / "after_step.json", step_state)
            _assert(step_state["timelineValue"] > initial_state["timelineValue"], "Step control did not advance the timeline.")

            play_started = _playwright_eval(
                session,
                "(() => { "
                "document.querySelector('#play-toggle').click(); "
                "return { "
                "playLabel: document.querySelector('#play-toggle').textContent.trim(), "
                "timelineValue: Number(document.querySelector('#timeline').value) "
                "}; "
                "})()",
                cwd=artifact_dir,
            )
            time.sleep(2.0)
            play_stopped = _playwright_eval(
                session,
                "(() => { "
                "const duringPlay = { "
                "playLabel: document.querySelector('#play-toggle').textContent.trim(), "
                "timelineValue: Number(document.querySelector('#timeline').value) "
                "}; "
                "document.querySelector('#play-toggle').click(); "
                "return { "
                "duringPlay, "
                "pausedLabel: document.querySelector('#play-toggle').textContent.trim() "
                "}; "
                "})()",
                cwd=artifact_dir,
            )
            _write_json(artifact_dir / "playback_cycle.json", {"started": play_started, "stopped": play_stopped})
            _assert(play_started["playLabel"] == "Pause", "Play button did not switch into pause state.")
            _assert(
                play_stopped["duringPlay"]["timelineValue"] > step_state["timelineValue"],
                "Play control did not advance the timeline.",
            )
            _assert(play_stopped["pausedLabel"] == "Play", "Play button did not return to Play after pausing.")

            target_scenario = DEMO_SCENARIO_IDS[1]
            _playwright_eval(
                session,
                f"(() => {{ "
                "const select = document.querySelector('#scenario-select'); "
                f"select.value = {json.dumps(target_scenario)}; "
                "select.dispatchEvent(new Event('change', { bubbles: true })); "
                "return { selectedScenario: select.value }; "
                "})()",
                cwd=artifact_dir,
            )
            switched_state = _wait_for_state(
                session,
                lambda state: state["selectedScenario"] == target_scenario and "Avoidant" in state["scenarioName"],
                cwd=artifact_dir,
            )
            _write_json(artifact_dir / "after_switch.json", switched_state)
            _assert(switched_state["selectedScenario"] == target_scenario, "Scenario switch did not update the selected scenario.")

            slider_state = _playwright_run_code(
                session,
                "async (page) => { "
                "const slider = page.locator('#timeline'); "
                "await slider.evaluate((element) => { "
                "element.value = element.max; "
                "element.dispatchEvent(new Event('input', { bubbles: true })); "
                "}); "
                "return await page.evaluate(() => ({ "
                "timelineValue: Number(document.querySelector('#timeline').value), "
                "timelineMax: Number(document.querySelector('#timeline').max), "
                "frameNote: document.querySelector('#frame-note').textContent.trim() "
                "})); "
                "}",
                cwd=artifact_dir,
            )
            _write_json(artifact_dir / "after_slider.json", slider_state)
            _assert(slider_state["timelineValue"] == slider_state["timelineMax"], "Timeline slider did not jump to the selected frame.")

            _playwright_screenshot(session, artifact_dir / "final_view.png", cwd=artifact_dir)
        finally:
            _close_session(session, cwd=artifact_dir)


def _run_replay_smoke(*, headed: bool) -> None:
    artifact_dir = REPLAY_ARTIFACT_DIR
    artifact_dir.mkdir(parents=True, exist_ok=True)
    session = f"avn-replay-smoke-{os.getpid()}"
    port = _reserve_port()
    with tempfile.TemporaryDirectory(prefix="avn-replay-smoke-") as temp_dir:
        temp_root = Path(temp_dir)
        result = run_scenario("weather_closure", output_root=temp_root)
        replay_path, replay_payload = _override_replay_bundle(result.replay_path, temp_root)
        _write_json(artifact_dir / "replay_override_preview.json", replay_payload)

        command = [
            sys.executable,
            "-m",
            "avn",
            "dashboard",
            "--replay",
            str(replay_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]
        with _dashboard_server(command, label="replay", port=port, artifact_dir=artifact_dir):
            _close_session(session, cwd=artifact_dir)
            open_command = _playwright_command(session, "open", f"http://127.0.0.1:{port}", "--persistent")
            if headed:
                open_command.append("--headed")
            _run(open_command, cwd=artifact_dir, timeout=180)
            try:
                replay_state = _wait_for_state(
                    session,
                    lambda state: state["scenarioName"] == SMOKE_REPLAY_NAME,
                    cwd=artifact_dir,
                )
                _write_json(artifact_dir / "replay_state.json", replay_state)
                _playwright_snapshot(session, artifact_dir / "replay_snapshot.txt", cwd=artifact_dir)
                _playwright_screenshot(session, artifact_dir / "replay_view.png", cwd=artifact_dir)

                _assert(len(replay_state["scenarioOptions"]) == 1, f"Replay mode exposed unexpected scenarios: {replay_state['scenarioOptions']}")
                _assert(replay_state["scenarioName"] == SMOKE_REPLAY_NAME, "Replay mode did not render the saved replay name.")
                _assert(
                    replay_state["scenarioDescription"] == SMOKE_REPLAY_DESCRIPTION,
                    "Replay mode did not render the saved replay description.",
                )
                _assert(
                    replay_payload["policy"]["label"] in replay_state["policyText"],
                    "Replay mode did not render saved policy metadata.",
                )
                _assert(replay_state["metricCount"] >= 5, "Replay mode did not render the metric strip.")
                _assert(replay_state["networkShapeCount"] > 0, "Replay mode did not render the network SVG.")
            finally:
                _close_session(session, cwd=artifact_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a browser smoke test against the AVN dashboard surfaces.")
    parser.add_argument("--headed", action="store_true", help="Open the browser in headed mode.")
    args = parser.parse_args(argv)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    _ensure_browser_installed()
    _run_demo_smoke(headed=args.headed)
    _run_replay_smoke(headed=args.headed)
    print(f"Dashboard smoke passed. Artifacts: {OUTPUT_ROOT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
