from __future__ import annotations

import csv
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from .artifacts import V2_CONTRACT_VERSION
from .models import ReferenceBundle


class BundleValidationError(ValueError):
    pass


def _timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _file_hash(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _csv_columns(path: Path) -> tuple[str, ...]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(reader.fieldnames or ())


def build_bundle_validation_report(bundle: ReferenceBundle, *, report_id: str | None = None) -> dict[str, Any]:
    provenance = bundle.provenance
    checks: list[dict[str, Any]] = []
    failure_reasons: list[str] = []
    warning_reasons: list[str] = []

    def record_check(check_id: str, passed: bool, note: str, *, category: str = "schema") -> None:
        checks.append(
            {
                "id": check_id,
                "category": category,
                "status": "passed" if passed else "failed",
                "note": note,
            }
        )

    source_files = bundle.source_files
    metric_columns = set(_csv_columns(source_files["metric_targets"]))
    missing_metric_columns = sorted(set(bundle.quality_checks.required_metric_columns) - metric_columns)
    metric_columns_ok = not missing_metric_columns
    record_check(
        "quality.metric_columns",
        metric_columns_ok,
        "Metric targets contain required columns." if metric_columns_ok else f"Missing metric columns: {missing_metric_columns}",
        category="quality",
    )
    if not metric_columns_ok:
        failure_reasons.append("missing_metric_columns")

    if "event_expectations" in source_files:
        event_columns = set(_csv_columns(source_files["event_expectations"]))
        missing_event_columns = sorted(set(bundle.quality_checks.required_event_columns) - event_columns)
        event_columns_ok = not missing_event_columns
        record_check(
            "quality.event_columns",
            event_columns_ok,
            "Event expectations contain required columns." if event_columns_ok else f"Missing event columns: {missing_event_columns}",
            category="quality",
        )
        if not event_columns_ok:
            failure_reasons.append("missing_event_columns")

    if "series_targets" in source_files:
        series_columns = set(_csv_columns(source_files["series_targets"]))
        missing_series_columns = sorted(set(bundle.quality_checks.required_series_columns) - series_columns)
        series_columns_ok = not missing_series_columns
        record_check(
            "quality.series_columns",
            series_columns_ok,
            "Series targets contain required columns." if series_columns_ok else f"Missing series columns: {missing_series_columns}",
            category="quality",
        )
        if not series_columns_ok:
            failure_reasons.append("missing_series_columns")

    reference_sources_ok = (not bundle.quality_checks.require_reference_sources) or bool(bundle.reference_sources)
    record_check(
        "provenance.reference_sources",
        reference_sources_ok,
        "Reference sources are present." if reference_sources_ok else "Reference sources are required but missing.",
        category="provenance",
    )
    if not reference_sources_ok:
        failure_reasons.append("missing_reference_sources")

    maintainer_ok = bool(str(provenance.get("maintainer", "")).strip())
    curation_ok = bool(str(provenance.get("curation_note", provenance.get("note", ""))).strip())
    temporal_ok = bool(str(provenance.get("generated_at", provenance.get("captured_at", ""))).strip())
    record_check("provenance.maintainer", maintainer_ok, "Maintainer is present." if maintainer_ok else "Maintainer is required.", category="provenance")
    record_check("provenance.curation_note", curation_ok, "Curation note is present." if curation_ok else "Curation note is required.", category="provenance")
    record_check("provenance.timestamp", temporal_ok, "Generated/captured timestamp is present." if temporal_ok else "Generated or captured timestamp is required.", category="provenance")
    if not maintainer_ok:
        failure_reasons.append("missing_bundle_maintainer")
    if not curation_ok:
        failure_reasons.append("missing_curation_note")
    if not temporal_ok:
        failure_reasons.append("missing_bundle_timestamp")

    monotonic_windows_ok = True
    if bundle.quality_checks.require_monotonic_windows:
        for target in bundle.series_targets:
            if target.minute_start is not None and target.minute_end is not None and target.minute_start > target.minute_end:
                monotonic_windows_ok = False
                break
        for target in bundle.series_targets:
            if target.minute is not None and (target.minute_start is not None or target.minute_end is not None):
                monotonic_windows_ok = False
                break
    record_check(
        "quality.series_windows",
        monotonic_windows_ok,
        "Series windows are well-formed." if monotonic_windows_ok else "Series targets contain malformed or overlapping point/window declarations.",
        category="quality",
    )
    if not monotonic_windows_ok:
        failure_reasons.append("malformed_series_windows")

    scope_counts = {}
    for target in bundle.series_targets:
        scope_counts[target.scope] = scope_counts.get(target.scope, 0) + 1
    scope_coverage_ok = True
    missing_scopes = []
    if bundle.quality_checks.require_scope_coverage:
        for scope in bundle.coverage_requirements.required_scopes:
            if scope_counts.get(scope, 0) <= 0:
                missing_scopes.append(scope)
        scope_coverage_ok = not missing_scopes
    record_check(
        "quality.scope_coverage",
        scope_coverage_ok,
        "Required scope coverage is present." if scope_coverage_ok else f"Missing required scopes: {missing_scopes}",
        category="quality",
    )
    if not scope_coverage_ok:
        failure_reasons.append("missing_required_scope_coverage")

    bundle_hashes = provenance.get("bundle_hashes", {})
    bundle_hashes_ok = True
    hash_details: dict[str, str] = {}
    if bundle.quality_checks.require_bundle_hashes:
        for key, file_path in source_files.items():
            actual = _file_hash(file_path)
            expected = str(bundle_hashes.get(key, ""))
            hash_details[key] = actual
            if actual != expected:
                bundle_hashes_ok = False
        record_check(
            "provenance.bundle_hashes",
            bundle_hashes_ok,
            "Bundle hashes match source files." if bundle_hashes_ok else "Bundle hashes do not match source files.",
            category="provenance",
        )
        if not bundle_hashes_ok:
            failure_reasons.append("bundle_hash_mismatch")
    else:
        for key, file_path in source_files.items():
            hash_details[key] = _file_hash(file_path)
        warning_reasons.append("bundle_hashes_not_required")
        record_check(
            "provenance.bundle_hashes",
            True,
            "Bundle hashes are optional for this bundle.",
            category="provenance",
        )

    valid = not failure_reasons
    return {
        "id": f"{report_id or bundle.bundle_id}:bundle_validation",
        "artifact_type": "bundle_validation",
        "contract_version": V2_CONTRACT_VERSION,
        "generated_at": _timestamp(),
        "bundle_id": bundle.bundle_id,
        "bundle_version": bundle.version,
        "bundle_contract_version": bundle.contract_version,
        "status": "passed" if valid else "failed",
        "checks": checks,
        "failure_reasons": failure_reasons,
        "warning_reasons": warning_reasons,
        "reference_sources": list(bundle.reference_sources),
        "source_file_hashes": hash_details,
        "scope_counts": dict(sorted(scope_counts.items())),
        "provenance_summary": {
            "maintainer": provenance.get("maintainer"),
            "curation_note": provenance.get("curation_note", provenance.get("note")),
            "generated_at": provenance.get("generated_at"),
            "captured_at": provenance.get("captured_at"),
        },
    }


def ensure_bundle_validation_passes(bundle: ReferenceBundle, *, report_id: str | None = None) -> dict[str, Any]:
    report = build_bundle_validation_report(bundle, report_id=report_id)
    if report["status"] != "passed":
        raise BundleValidationError(
            f"Bundle validation failed for {bundle.bundle_id}: {', '.join(report['failure_reasons'])}"
        )
    return report
