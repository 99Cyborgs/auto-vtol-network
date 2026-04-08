from __future__ import annotations

import json
from pathlib import Path

import pytest

from skills.auto_vtol_network.harness import run_skill_pack
from skills.auto_vtol_network.contracts import SkillPackRequest
from skills.auto_vtol_network.writers import ArtifactWriteError, write_artifact


TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "templates" / "sample_request.json"


def _load_request() -> SkillPackRequest:
    payload = json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))
    return SkillPackRequest.from_dict(payload)


def test_writer_is_idempotent_for_same_run(tmp_path: Path) -> None:
    request = _load_request()
    run_skill_pack(request, output_dir=tmp_path)
    first_payload = (tmp_path / "research_intake.json").read_text(encoding="utf-8")
    run_skill_pack(request, output_dir=tmp_path)
    second_payload = (tmp_path / "research_intake.json").read_text(encoding="utf-8")
    assert first_payload == second_payload


def test_writer_rejects_silent_record_drop(tmp_path: Path) -> None:
    request = _load_request()
    run_skill_pack(request, output_dir=tmp_path)

    research_path = tmp_path / "research_intake.json"
    payload = json.loads(research_path.read_text(encoding="utf-8"))
    payload["records"] = payload["records"][:1]

    with pytest.raises(ArtifactWriteError, match="drop ids"):
        write_artifact(research_path, payload)
