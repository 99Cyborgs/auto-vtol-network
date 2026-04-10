from __future__ import annotations

import json
from importlib.resources import files
from pathlib import Path

from skills.auto_vtol_network.contracts import SkillPackRequest
from skills.auto_vtol_network.harness import run_skill_pack


def _load_request() -> SkillPackRequest:
    payload = json.loads(
        files("skills.auto_vtol_network.templates").joinpath("sample_request.json").read_text(encoding="utf-8")
    )
    return SkillPackRequest.from_dict(payload)


def test_end_to_end_pipeline_emits_required_artifacts(tmp_path: Path) -> None:
    request = _load_request()
    receipt = run_skill_pack(request, output_dir=tmp_path)

    required_files = (
        "research_intake.json",
        "architecture_summary.json",
        "threshold_ledger.json",
        "scenario_catalog.json",
        "hazard_ledger.json",
        "deployment_stage_profile.json",
        "promotion_decisions.json",
        "artifact_manifest.json",
        "run_receipt.json",
    )
    for filename in required_files:
        assert (tmp_path / filename).exists(), filename

    research = json.loads((tmp_path / "research_intake.json").read_text(encoding="utf-8"))
    thresholds = json.loads((tmp_path / "threshold_ledger.json").read_text(encoding="utf-8"))
    decisions = json.loads((tmp_path / "promotion_decisions.json").read_text(encoding="utf-8"))

    assert research["type"] == "research_intake"
    assert thresholds["type"] == "threshold_ledger"
    assert decisions["type"] == "promotion_decisions"
    assert len(research["records"]) == 2
    assert len(thresholds["thresholds"]) == 4
    assert len(decisions["decisions"]) == len(decisions["approved_stage_ids"]) + len(decisions["held_stage_ids"])
    assert receipt.status == "completed"
