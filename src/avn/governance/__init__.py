from avn.governance.artifacts import ARTIFACT_CONTRACT_VERSION, write_run_artifacts, write_sweep_artifacts
from avn.governance.models import (
    ArtifactManifest,
    PromotionDecision,
    PromotionDecisionSet,
    ThresholdEvaluation,
    ThresholdLedger,
    ValidationReport,
)

__all__ = [
    "ARTIFACT_CONTRACT_VERSION",
    "ArtifactManifest",
    "PromotionDecision",
    "PromotionDecisionSet",
    "ThresholdEvaluation",
    "ThresholdLedger",
    "ValidationReport",
    "run_adaptive_sweep",
    "validate_batch_directory",
    "validate_run_directory",
    "write_run_artifacts",
    "write_sweep_artifacts",
]


def __getattr__(name: str):
    if name == "run_adaptive_sweep":
        from avn.governance.sweep import run_adaptive_sweep

        return run_adaptive_sweep
    if name == "validate_batch_directory":
        from avn.governance.validation import validate_batch_directory

        return validate_batch_directory
    if name == "validate_run_directory":
        from avn.governance.validation import validate_run_directory

        return validate_run_directory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
