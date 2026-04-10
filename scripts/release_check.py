from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = REPO_ROOT / "dist"
BUILD_DIR = REPO_ROOT / "build"


def _run(command: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd or REPO_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def _clean_artifact_dirs() -> None:
    for path in (BUILD_DIR, DIST_DIR):
        if path.exists():
            shutil.rmtree(path)


def _build_wheel() -> Path:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    _run([sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "--wheel-dir", str(DIST_DIR)])
    wheels = sorted(DIST_DIR.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one wheel in {DIST_DIR}, found {len(wheels)}")
    return wheels[0]


def _inspect_wheel(wheel_path: Path) -> None:
    with zipfile.ZipFile(wheel_path) as archive:
        names = archive.namelist()
    required_entries = {
        "avn/__main__.py",
        "avn/governance/artifacts.py",
    }
    missing = sorted(entry for entry in required_entries if not any(name.endswith(entry) for name in names))
    if missing:
        raise RuntimeError(f"Wheel is missing expected entries: {missing}")
    forbidden = sorted(name for name in names if "skills/auto_vtol_network" in name)
    if forbidden:
        raise RuntimeError(f"Wheel unexpectedly includes deprecated skill-pack content: {forbidden}")


def _venv_python(venv_dir: Path) -> Path:
    scripts_dir = "Scripts" if os.name == "nt" else "bin"
    executable = "python.exe" if os.name == "nt" else "python"
    return venv_dir / scripts_dir / executable


def _smoke_installed_wheel(wheel_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="avn-release-check-") as temp_dir:
        venv_dir = Path(temp_dir) / "venv"
        _run([sys.executable, "-m", "venv", str(venv_dir)])
        python_bin = _venv_python(venv_dir)
        _run([str(python_bin), "-m", "pip", "install", str(wheel_path)])
        _run([str(python_bin), "-m", "avn", "--help"])
        _run([str(python_bin), "-m", "avn", "run", "baseline_flow", "--output-root", str(Path(temp_dir) / "outputs")])
        run_dirs = sorted((Path(temp_dir) / "outputs").glob("baseline_flow_*"))
        if len(run_dirs) != 1:
            raise RuntimeError(f"Expected one run directory, found {len(run_dirs)}")
        report = _run([str(python_bin), "-m", "avn", "validate-run", str(run_dirs[0])])
        payload = json.loads(report.stdout)
        if payload["status"] != "passed":
            raise RuntimeError(f"Installed wheel validation failed: {payload}")


def main() -> int:
    _clean_artifact_dirs()
    wheel_path = _build_wheel()
    _inspect_wheel(wheel_path)
    _smoke_installed_wheel(wheel_path)
    print(f"Release check passed: {wheel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
