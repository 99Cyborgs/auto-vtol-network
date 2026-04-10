from __future__ import annotations

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
WARNING_MARKER = "Package would be ignored"
FORBIDDEN_WHEEL_PREFIX = "skills/auto_vtol_network/tests/"


def _run(command: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd or REPO_ROOT),
        env=env,
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
    result = _run([sys.executable, "-m", "build", "--wheel"])
    combined_output = f"{result.stdout}\n{result.stderr}"
    if WARNING_MARKER in combined_output:
        raise RuntimeError(f"Setuptools emitted package-ambiguity warnings.\n{combined_output}")
    wheels = sorted(DIST_DIR.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"Expected one wheel in {DIST_DIR}, found {len(wheels)}")
    return wheels[0]


def _inspect_wheel(wheel_path: Path) -> None:
    with zipfile.ZipFile(wheel_path) as archive:
        names = archive.namelist()
    forbidden = sorted(name for name in names if name.startswith(FORBIDDEN_WHEEL_PREFIX))
    if forbidden:
        raise RuntimeError(f"Wheel unexpectedly includes skill-pack tests: {forbidden}")


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
        _run([str(python_bin), "-m", "avn_v2", "--help"])
        _run([str(python_bin), "-m", "skills.auto_vtol_network", "--help"])
        _run(
            [
                str(python_bin),
                "-c",
                (
                    "from importlib.resources import files; "
                    "payload = files('skills.auto_vtol_network.templates').joinpath('sample_request.json').read_text(encoding='utf-8'); "
                    "assert 'avn-governed-skill-pack-sample' in payload"
                ),
            ]
        )


def main() -> int:
    _clean_artifact_dirs()
    wheel_path = _build_wheel()
    _inspect_wheel(wheel_path)
    _smoke_installed_wheel(wheel_path)
    print(f"Release check passed: {wheel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
