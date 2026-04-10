from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class ArtifactRecord:
    artifact_id: str
    artifact_type: str
    contract_version: int
    path: str
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ArtifactManifest:
    manifest_id: str
    contract_version: int
    artifact_family: str
    artifacts: list[ArtifactRecord]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["artifacts"] = [artifact.to_dict() for artifact in self.artifacts]
        return payload


@dataclass(slots=True)
class ThresholdEvaluation:
    threshold_id: str
    metric_key: str
    comparator: str
    target_value: float
    observed_value: float
    status: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ThresholdLedger:
    ledger_id: str
    contract_version: int
    scenario_id: str
    evaluations: list[ThresholdEvaluation]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evaluations"] = [evaluation.to_dict() for evaluation in self.evaluations]
        return payload


@dataclass(slots=True)
class PromotionDecision:
    decision_id: str
    threshold_id: str
    status: str
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PromotionDecisionSet:
    artifact_id: str
    contract_version: int
    scenario_id: str
    release_status: str
    decisions: list[PromotionDecision]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["decisions"] = [decision.to_dict() for decision in self.decisions]
        return payload


@dataclass(slots=True)
class ValidationCheck:
    check_id: str
    status: str
    detail: str
    category: str = "schema"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ValidationReport:
    report_id: str
    contract_version: int
    status: str
    checks: list[ValidationCheck]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["checks"] = [check.to_dict() for check in self.checks]
        return payload


@dataclass(slots=True, frozen=True)
class SweepAxis:
    path: str
    values: tuple[float | int | str | bool, ...]


@dataclass(slots=True, frozen=True)
class AdaptiveSweepManifest:
    sweep_id: str
    scenario: str
    output_root: str
    metric_key: str
    axis: SweepAxis
    max_iterations: int = 6
    initial_samples: int = 3


@dataclass(slots=True)
class SweepPointResult:
    point_id: str
    axis_value: float | int | str | bool
    run_dir: str
    release_status: str
    observed_metric: float
    threshold_target: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AdaptiveSweepResult:
    sweep_id: str
    contract_version: int
    scenario_id: str
    metric_key: str
    axis_path: str
    stopping_reason: str
    points: list[SweepPointResult]
    thresholds: list[ThresholdEvaluation]
    promotion: PromotionDecisionSet

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["points"] = [point.to_dict() for point in self.points]
        payload["thresholds"] = [threshold.to_dict() for threshold in self.thresholds]
        payload["promotion"] = self.promotion.to_dict()
        return payload
