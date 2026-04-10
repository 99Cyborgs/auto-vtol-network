from __future__ import annotations

from avn.core.state import ReplayBundle


def serialize_replay(bundle: ReplayBundle) -> dict:
    return bundle.to_dict()
