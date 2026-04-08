from __future__ import annotations

import json
from pathlib import Path

import pytest

from skills.auto_vtol_network.contracts import SkillPackRequest
from skills.auto_vtol_network.validators import SkillPackValidationError, validate_request


TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "templates" / "sample_request.json"


def _load_payload() -> dict:
    return json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))


def test_sample_request_validates() -> None:
    request = SkillPackRequest.from_dict(_load_payload())
    validate_request(request)


def test_missing_provenance_fails() -> None:
    payload = _load_payload()
    payload["provenance"] = {}
    request = SkillPackRequest.from_dict(payload)
    with pytest.raises(SkillPackValidationError, match="provenance"):
        validate_request(request)


def test_threshold_without_metric_linkage_fails() -> None:
    payload = _load_payload()
    payload["thresholds"][0]["metric_key"] = ""
    request = SkillPackRequest.from_dict(payload)
    with pytest.raises(SkillPackValidationError, match="metric linkage"):
        validate_request(request)


def test_hazard_without_mitigation_fails() -> None:
    payload = _load_payload()
    payload["hazards"][0]["mitigations"] = []
    request = SkillPackRequest.from_dict(payload)
    with pytest.raises(SkillPackValidationError, match="mitigation"):
        validate_request(request)


def test_scenario_without_success_criteria_fails() -> None:
    payload = _load_payload()
    payload["scenarios"][0]["success_criteria"] = []
    request = SkillPackRequest.from_dict(payload)
    with pytest.raises(SkillPackValidationError, match="success criteria"):
        validate_request(request)


def test_deployment_without_blockers_fails() -> None:
    payload = _load_payload()
    payload["deployment_stages"][0]["blocker_ids"] = []
    request = SkillPackRequest.from_dict(payload)
    with pytest.raises(SkillPackValidationError, match="blockers"):
        validate_request(request)
