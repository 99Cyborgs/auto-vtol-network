from __future__ import annotations

import json
from pathlib import Path

from avn.core.state import ReplayBundle
from avn.governance.artifacts import write_replay


def load_replay_bundle(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_replay_bundle(path: Path, bundle: ReplayBundle) -> Path:
    return write_replay(path, bundle)
