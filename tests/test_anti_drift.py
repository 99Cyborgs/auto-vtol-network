from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_avn_v2_runtime_root_is_absent() -> None:
    shim_root = REPO_ROOT / "src" / "avn_v2"
    assert not shim_root.exists()


def test_release_gate_targets_canonical_runtime() -> None:
    script = (REPO_ROOT / "scripts" / "release_check.py").read_text(encoding="utf-8")
    assert "-m\", \"avn\"" in script
    assert "avn_v2/__main__.py" not in script
    assert "\"avn_v2\"" not in script
    assert "skills.auto_vtol_network" not in script


def test_makefile_exposes_single_release_check_gate() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "release-check:" in makefile
    assert "python -m avn_v2" not in makefile


def test_no_live_parallel_runtime_roots_remain() -> None:
    src_root = REPO_ROOT / "src"
    runtime_roots = sorted(path.name for path in src_root.iterdir() if path.is_dir())
    assert "avn" in runtime_roots
    assert "avn_v2" not in runtime_roots


def test_replay_writer_delegates_to_canonical_artifact_writer() -> None:
    replay_module = (REPO_ROOT / "src" / "avn" / "sim" / "replay.py").read_text(encoding="utf-8")
    assert "from avn.governance.artifacts import write_replay" in replay_module
    assert "json.dump" not in replay_module
    assert "write_text(" not in replay_module
