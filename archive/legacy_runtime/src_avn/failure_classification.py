from __future__ import annotations

from avn.failures import classify_legacy_failure


def classify_failure(first_violation_cause: str | None, summary: dict[str, object]) -> str:
    return classify_legacy_failure(first_violation_cause, summary)
