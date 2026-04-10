from __future__ import annotations

import csv
import json
import math
import os
import shutil
from hashlib import sha256
from pathlib import Path
from typing import Any

from .artifacts import V2_CONTRACT_VERSION, write_json
from .config import load_external_source_manifest, load_reference_bundle
from .models import ExternalFieldMapping, ExternalSourceManifest
from .reporting import utc_timestamp
from .validation import build_bundle_validation_report


SUPPORTED_SOURCE_TYPES = {"csv_directory", "json_files", "pre-extracted_archive"}
SUPPORTED_INGESTION_MODES = {"copy"}
CANONICAL_SCOPES = {"network", "node", "corridor"}
CANONICAL_UNITS = {"count", "minutes", "ratio", "percent", "events", "score"}
CANONICAL_FIELDS = {
    "metrics": ("metric_key", "reference_value", "tolerance", "unit", "weight", "objective_group"),
    "event_expectations": (
        "expectation_id",
        "event_type",
        "expected_count",
        "count_tolerance",
        "first_minute",
        "timing_tolerance",
        "weight",
        "objective_group",
    ),
    "series_targets": (
        "target_id",
        "scope",
        "entity_id",
        "metric_key",
        "reference_value",
        "tolerance",
        "weight",
        "minute",
        "minute_start",
        "minute_end",
        "aggregation",
        "objective_group",
    ),
}
REQUIRED_FIELDS = {
    "metrics": ("metric_key", "reference_value", "tolerance"),
    "event_expectations": ("expectation_id", "event_type", "expected_count", "count_tolerance"),
    "series_targets": ("target_id", "scope", "entity_id", "metric_key", "reference_value", "tolerance"),
}


class IngestionError(ValueError):
    pass


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sanitize(value: str) -> str:
    lowered = value.strip().lower()
    return "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in lowered).strip("-")


def _bundle_id(manifest: ExternalSourceManifest) -> str:
    return "-".join(
        part for part in (_sanitize(manifest.bundle_family), _sanitize(manifest.source_id), _sanitize(manifest.version)) if part
    )


def _external_root(manifest: ExternalSourceManifest) -> Path:
    if manifest.output_root is not None:
        return manifest.output_root.resolve()
    return (_repo_root() / "data" / "v2" / "external").resolve()


def _bundle_dir_for_manifest(manifest: ExternalSourceManifest) -> Path:
    return _external_root(manifest) / "bundles" / _bundle_id(manifest)


def _bundle_json_for_source(path: Path) -> Path:
    if path.is_dir():
        return (path / "bundle.json").resolve()
    if path.name == "bundle.json":
        return path.resolve()
    manifest = load_external_source_manifest(path)
    return (_bundle_dir_for_manifest(manifest) / "bundle.json").resolve()


def resolve_ingested_bundle_source(path: str | Path) -> Path:
    bundle_path = _bundle_json_for_source(Path(path).resolve())
    if not bundle_path.exists():
        raise FileNotFoundError(f"Ingested bundle does not exist yet: {bundle_path}")
    return bundle_path


def _hash_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _relative_path(target: Path, base: Path) -> str:
    return Path(os.path.relpath(target, start=base)).as_posix()


def _read_rows(path: Path, *, file_format: str) -> list[dict[str, Any]]:
    normalized_format = file_format.strip().lower()
    if normalized_format == "csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    if normalized_format == "json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [dict(item) for item in payload]
        if isinstance(payload, dict):
            rows = payload.get("rows")
            if isinstance(rows, list):
                return [dict(item) for item in rows]
        raise IngestionError(f"JSON input {path} must be a list of objects or an object with 'rows'.")
    raise IngestionError(f"Unsupported raw input format: {file_format}")


def _coerce_float(value: Any) -> float:
    if value in (None, ""):
        raise IngestionError("missing numeric value")
    return float(value)


def _coerce_int(value: Any) -> int:
    if value in (None, ""):
        raise IngestionError("missing integer value")
    return int(round(float(value)))


def _round_minute(value: float, mode: str) -> int:
    if mode == "floor":
        return math.floor(value)
    if mode == "ceil":
        return math.ceil(value)
    return int(round(value))


def _normalize_time_value(value: Any, manifest: ExternalSourceManifest) -> int | None:
    if value in (None, ""):
        return None
    minutes = float(value) * manifest.normalization.time_multiplier
    return _round_minute(minutes, manifest.normalization.time_rounding)


def _missing_required_mapping(dataset_name: str, mapping: ExternalFieldMapping) -> list[str]:
    failures = []
    for field_name in REQUIRED_FIELDS[dataset_name]:
        if field_name in mapping.fields:
            continue
        if field_name in mapping.defaults:
            continue
        if field_name in mapping.optional_fields:
            continue
        failures.append(f"missing_required_mapping:{dataset_name}:{field_name}")
    return failures


def _validate_manifest(manifest: ExternalSourceManifest) -> list[str]:
    failures: list[str] = []
    if manifest.source_type not in SUPPORTED_SOURCE_TYPES:
        failures.append(f"unsupported_source_type:{manifest.source_type}")
    if manifest.ingestion_mode not in SUPPORTED_INGESTION_MODES:
        failures.append(f"unsupported_ingestion_mode:{manifest.ingestion_mode}")
    if "metrics" not in manifest.raw_inputs:
        failures.append("missing_required_raw_input:metrics")
    for dataset_name, raw_input in manifest.raw_inputs.items():
        if not raw_input.path.exists():
            failures.append(f"missing_raw_input:{dataset_name}")
        if dataset_name not in manifest.field_mapping:
            failures.append(f"missing_field_mapping:{dataset_name}")
            continue
        failures.extend(_missing_required_mapping(dataset_name, manifest.field_mapping[dataset_name]))
    return failures


def _stage_raw_inputs(manifest: ExternalSourceManifest, raw_dir: Path) -> dict[str, Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    staged = {}
    for dataset_name, raw_input in manifest.raw_inputs.items():
        target_name = f"{dataset_name}{raw_input.path.suffix or '.dat'}"
        target_path = raw_dir / target_name
        shutil.copy2(raw_input.path, target_path)
        staged[dataset_name] = target_path
    return staged


def _extract_field(
    dataset_name: str,
    mapping: ExternalFieldMapping,
    row: dict[str, Any],
    field_name: str,
) -> tuple[Any | None, str | None]:
    if field_name in mapping.fields:
        source_key = mapping.fields[field_name]
        value = row.get(source_key)
        if value not in (None, ""):
            return value, None
        if field_name in mapping.defaults:
            return mapping.defaults[field_name], None
        if field_name not in REQUIRED_FIELDS[dataset_name]:
            return None, None
        if field_name in mapping.optional_fields:
            return None, None
        return None, f"missing_field:{dataset_name}:{field_name}"
    if field_name in mapping.defaults:
        return mapping.defaults[field_name], None
    if field_name not in REQUIRED_FIELDS[dataset_name]:
        return None, None
    if field_name in mapping.optional_fields:
        return None, None
    return None, f"missing_field:{dataset_name}:{field_name}"


def _normalize_metric_row(
    row: dict[str, Any],
    *,
    mapping: ExternalFieldMapping,
    manifest: ExternalSourceManifest,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field_name in CANONICAL_FIELDS["metrics"]:
        value, error = _extract_field("metrics", mapping, row, field_name)
        if error is not None:
            raise IngestionError(error)
        normalized[field_name] = value
    unit = "" if normalized["unit"] in (None, "") else str(normalized["unit"])
    unit_mapping = manifest.normalization.metric_unit_mappings.get(unit)
    if unit_mapping is not None:
        multiplier = float(unit_mapping.get("multiplier", 1.0))
        normalized["reference_value"] = _coerce_float(normalized["reference_value"]) * multiplier
        normalized["tolerance"] = _coerce_float(normalized["tolerance"]) * multiplier
        normalized["unit"] = str(unit_mapping.get("target_unit", unit))
    else:
        if unit and unit not in CANONICAL_UNITS:
            raise IngestionError(f"unknown_unit:{unit}")
        normalized["reference_value"] = _coerce_float(normalized["reference_value"])
        normalized["tolerance"] = _coerce_float(normalized["tolerance"])
        normalized["unit"] = unit
    normalized["metric_key"] = str(normalized["metric_key"])
    normalized["weight"] = float(normalized["weight"] if normalized["weight"] not in (None, "") else 1.0)
    normalized["objective_group"] = str(normalized["objective_group"] or "metric")
    return normalized


def _normalize_event_row(
    row: dict[str, Any],
    *,
    mapping: ExternalFieldMapping,
    manifest: ExternalSourceManifest,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field_name in CANONICAL_FIELDS["event_expectations"]:
        value, error = _extract_field("event_expectations", mapping, row, field_name)
        if error is not None:
            raise IngestionError(error)
        normalized[field_name] = value
    event_type = str(normalized["event_type"])
    normalized["event_type"] = manifest.normalization.event_type_mapping.get(event_type, event_type)
    normalized["expectation_id"] = str(normalized["expectation_id"])
    normalized["expected_count"] = _coerce_int(normalized["expected_count"])
    normalized["count_tolerance"] = _coerce_int(normalized["count_tolerance"])
    normalized["first_minute"] = _normalize_time_value(normalized["first_minute"], manifest)
    normalized["timing_tolerance"] = None if normalized["timing_tolerance"] in (None, "") else float(normalized["timing_tolerance"])
    normalized["weight"] = float(normalized["weight"] if normalized["weight"] not in (None, "") else 1.0)
    normalized["objective_group"] = str(normalized["objective_group"] or "event")
    return normalized


def _normalize_series_row(
    row: dict[str, Any],
    *,
    mapping: ExternalFieldMapping,
    manifest: ExternalSourceManifest,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field_name in CANONICAL_FIELDS["series_targets"]:
        value, error = _extract_field("series_targets", mapping, row, field_name)
        if error is not None:
            raise IngestionError(error)
        normalized[field_name] = value
    raw_scope = str(normalized["scope"])
    scope = manifest.normalization.scope_mapping.get(raw_scope, raw_scope)
    if scope not in CANONICAL_SCOPES:
        raise IngestionError(f"unknown_scope:{raw_scope}")
    normalized["scope"] = scope
    normalized["target_id"] = str(normalized["target_id"])
    normalized["entity_id"] = str(normalized["entity_id"])
    normalized["metric_key"] = str(normalized["metric_key"])
    normalized["reference_value"] = _coerce_float(normalized["reference_value"])
    normalized["tolerance"] = _coerce_float(normalized["tolerance"])
    normalized["weight"] = float(normalized["weight"] if normalized["weight"] not in (None, "") else 1.0)
    normalized["minute"] = _normalize_time_value(normalized["minute"], manifest)
    normalized["minute_start"] = _normalize_time_value(normalized["minute_start"], manifest)
    normalized["minute_end"] = _normalize_time_value(normalized["minute_end"], manifest)
    normalized["aggregation"] = str(normalized["aggregation"] or "point")
    normalized["objective_group"] = str(normalized["objective_group"] or "series")
    return normalized


def _normalize_dataset(
    dataset_name: str,
    rows: list[dict[str, Any]],
    *,
    manifest: ExternalSourceManifest,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    mapping = manifest.field_mapping[dataset_name]
    normalized_rows: list[dict[str, Any]] = []
    dropped_records: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        try:
            if dataset_name == "metrics":
                normalized = _normalize_metric_row(row, mapping=mapping, manifest=manifest)
            elif dataset_name == "event_expectations":
                normalized = _normalize_event_row(row, mapping=mapping, manifest=manifest)
            elif dataset_name == "series_targets":
                normalized = _normalize_series_row(row, mapping=mapping, manifest=manifest)
            else:
                raise IngestionError(f"unsupported_dataset:{dataset_name}")
            normalized_rows.append(normalized)
        except (IngestionError, ValueError) as error:
            dropped_records.append({"row_index": index, "reason": str(error)})
    return (
        normalized_rows,
        {
            "input_rows": len(rows),
            "accepted_rows": len(normalized_rows),
            "rejected_rows": len(dropped_records),
            "dropped_records": dropped_records,
            "field_mapping": dict(sorted(mapping.fields.items())),
        },
    )


def _write_csv(path: Path, fieldnames: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _objective_group_weights(normalized_payloads: dict[str, list[dict[str, Any]]]) -> dict[str, float]:
    weights = {"metric": 1.0, "event": 1.0, "series": 1.0}
    for dataset_rows in normalized_payloads.values():
        for row in dataset_rows:
            group = str(row.get("objective_group", "")).strip()
            if group:
                weights.setdefault(group, 1.0)
    return dict(sorted(weights.items()))


def _default_gates() -> dict[str, Any]:
    return {
        "max_total_score": 1.0,
        "require_metric_match": True,
        "require_event_match": True,
        "require_series_match": False,
        "require_calibrated_parameters": True,
        "require_evidence_coverage": True,
    }


def _build_bundle_payload(
    manifest: ExternalSourceManifest,
    *,
    bundle_id: str,
    bundle_dir: Path,
    scenario_path: Path,
    generated_paths: dict[str, Path],
    staged_inputs: dict[str, Path],
    normalized_payloads: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    provenance = {
        "source": "external_ingestion",
        "source_id": manifest.source_id,
        "source_type": manifest.source_type,
        "bundle_family": manifest.bundle_family,
        "maintainer": manifest.provenance_defaults.get("maintainer"),
        "curation_note": manifest.provenance_defaults.get("curation_note"),
        "generated_at": manifest.provenance_defaults.get("generated_at", utc_timestamp()),
    }
    if "captured_at" in manifest.provenance_defaults:
        provenance["captured_at"] = manifest.provenance_defaults["captured_at"]
    provenance["bundle_hashes"] = {
        key: _hash_file(path)
        for key, path in generated_paths.items()
        if key in {"metric_targets", "event_expectations", "series_targets"}
    }
    for key, value in manifest.provenance_defaults.items():
        if key not in provenance and key != "bundle_hashes":
            provenance[key] = value
    backtest = {"metric_targets": _relative_path(generated_paths["metric_targets"], bundle_dir)}
    if "event_expectations" in generated_paths:
        backtest["event_expectations"] = _relative_path(generated_paths["event_expectations"], bundle_dir)
    if "series_targets" in generated_paths:
        backtest["series_targets"] = _relative_path(generated_paths["series_targets"], bundle_dir)
    fit_space = manifest.fit_space_overrides or {"parameters": []}
    if "parameters" not in fit_space:
        fit_space = {"parameters": []}
    return {
        "contract_version": V2_CONTRACT_VERSION,
        "bundle_id": bundle_id,
        "name": f"External {manifest.bundle_family.replace('_', ' ').title()} Bundle",
        "version": manifest.version,
        "scenario": _relative_path(scenario_path, bundle_dir),
        "backtest": backtest,
        "coverage_requirements": {
            "min_metric_targets": manifest.coverage_requirements.min_metric_targets,
            "min_event_expectations": manifest.coverage_requirements.min_event_expectations,
            "min_series_targets": manifest.coverage_requirements.min_series_targets,
            "required_scopes": list(manifest.coverage_requirements.required_scopes),
        },
        "confidence_policy": {
            "high_confidence_score": manifest.confidence_policy.high_confidence_score,
            "medium_confidence_score": manifest.confidence_policy.medium_confidence_score,
            "max_sensitivity_delta": manifest.confidence_policy.max_sensitivity_delta,
            "max_failed_objectives": manifest.confidence_policy.max_failed_objectives,
            "min_coverage_completeness": manifest.confidence_policy.min_coverage_completeness,
        },
        "quality_checks": {
            "require_reference_sources": manifest.quality_checks.require_reference_sources,
            "require_bundle_hashes": manifest.quality_checks.require_bundle_hashes,
            "require_monotonic_windows": manifest.quality_checks.require_monotonic_windows,
            "require_scope_coverage": manifest.quality_checks.require_scope_coverage,
            "required_metric_columns": list(manifest.quality_checks.required_metric_columns),
            "required_event_columns": list(manifest.quality_checks.required_event_columns),
            "required_series_columns": list(manifest.quality_checks.required_series_columns),
        },
        "objective_group_weights": _objective_group_weights(normalized_payloads),
        "fit_space": fit_space,
        "gates": _default_gates(),
        "reference_sources": [str(path.resolve()) for path in staged_inputs.values()],
        "provenance": provenance,
    }


def run_ingestion(manifest_path: str | Path) -> dict[str, Any]:
    manifest = load_external_source_manifest(manifest_path)
    external_root = _external_root(manifest)
    raw_dir = external_root / "raw" / _sanitize(manifest.source_id) / _sanitize(manifest.version)
    bundle_id = _bundle_id(manifest)
    bundle_dir = external_root / "bundles" / bundle_id
    bundle_dir.mkdir(parents=True, exist_ok=True)
    manifest_failures = _validate_manifest(manifest)
    staged_inputs = _stage_raw_inputs(manifest, raw_dir) if not any(reason.startswith("missing_raw_input:") for reason in manifest_failures) else {}

    normalization_summary: dict[str, Any] = {}
    normalized_payloads: dict[str, list[dict[str, Any]]] = {}
    generated_paths: dict[str, Path] = {}
    status = "failed"
    usable_bundle = False
    bundle_validation_report: dict[str, Any] | None = None
    bundle_path = bundle_dir / "bundle.json"
    report_failures = list(manifest_failures)

    if not manifest_failures:
        for dataset_name in ("metrics", "event_expectations", "series_targets"):
            if dataset_name not in staged_inputs:
                continue
            try:
                rows = _read_rows(staged_inputs[dataset_name], file_format=manifest.raw_inputs[dataset_name].format)
                normalized_rows, dataset_summary = _normalize_dataset(dataset_name, rows, manifest=manifest)
                normalized_payloads[dataset_name] = normalized_rows
                normalization_summary[dataset_name] = dataset_summary
            except (IngestionError, ValueError, json.JSONDecodeError) as error:
                normalized_payloads[dataset_name] = []
                normalization_summary[dataset_name] = {
                    "input_rows": 0,
                    "accepted_rows": 0,
                    "rejected_rows": 0,
                    "dropped_records": [],
                    "field_mapping": dict(sorted(manifest.field_mapping[dataset_name].fields.items())),
                    "error": str(error),
                }
                report_failures.append(f"normalization_failed:{dataset_name}")
        if not normalized_payloads.get("metrics"):
            report_failures.append("no_metric_rows_accepted")
        if normalized_payloads.get("metrics"):
            output_file_map = {
                "metrics": ("metric_targets", "reference_metrics.csv", CANONICAL_FIELDS["metrics"]),
                "event_expectations": ("event_expectations", "event_expectations.csv", CANONICAL_FIELDS["event_expectations"]),
                "series_targets": ("series_targets", "series_targets.csv", CANONICAL_FIELDS["series_targets"]),
            }
            for dataset_name, rows in normalized_payloads.items():
                if not rows:
                    continue
                artifact_key, file_name, fieldnames = output_file_map[dataset_name]
                artifact_path = bundle_dir / file_name
                _write_csv(artifact_path, fieldnames, rows)
                generated_paths[artifact_key] = artifact_path
            bundle_payload = _build_bundle_payload(
                manifest,
                bundle_id=bundle_id,
                bundle_dir=bundle_dir,
                scenario_path=manifest.scenario_path.resolve(),
                generated_paths=generated_paths,
                staged_inputs=staged_inputs,
                normalized_payloads=normalized_payloads,
            )
            write_json(bundle_path, bundle_payload)
            bundle = load_reference_bundle(bundle_path)
            bundle_validation_report = build_bundle_validation_report(bundle, report_id=bundle.bundle_id)
            if bundle_validation_report["status"] == "passed" and not report_failures:
                status = "passed"
                usable_bundle = True
            else:
                report_failures.extend(bundle_validation_report["failure_reasons"])
    generated_files = {
        file_path.name: {
            "path": str(file_path.resolve()),
            "sha256": _hash_file(file_path),
        }
        for file_path in [*generated_paths.values(), *([bundle_path] if bundle_path.exists() else [])]
    }
    report_payload = {
        "id": f"{bundle_id}:ingestion_report",
        "artifact_type": "ingestion_report",
        "contract_version": V2_CONTRACT_VERSION,
        "generated_at": utc_timestamp(),
        "status": status,
        "source_id": manifest.source_id,
        "source_type": manifest.source_type,
        "source_version": manifest.version,
        "bundle_family": manifest.bundle_family,
        "bundle_id": bundle_id,
        "raw_dir": str(raw_dir.resolve()),
        "bundle_dir": str(bundle_dir.resolve()),
        "bundle_path": str(bundle_path.resolve()),
        "raw_inputs_used": {
            name: {
                "source_path": str(raw_input.path.resolve()),
                "staged_path": None if name not in staged_inputs else str(staged_inputs[name].resolve()),
                "format": raw_input.format,
            }
            for name, raw_input in sorted(manifest.raw_inputs.items())
        },
        "field_mappings_applied": {
            name: {
                "fields": dict(sorted(mapping.fields.items())),
                "optional_fields": list(mapping.optional_fields),
                "defaults": dict(sorted(mapping.defaults.items())),
            }
            for name, mapping in sorted(manifest.field_mapping.items())
        },
        "normalization_summary": normalization_summary,
        "dropped_records": {
            dataset_name: summary["dropped_records"]
            for dataset_name, summary in normalization_summary.items()
            if summary["dropped_records"]
        },
        "generated_files": generated_files,
        "bundle_validation_id": None if bundle_validation_report is None else bundle_validation_report["id"],
        "bundle_validation_status": None if bundle_validation_report is None else bundle_validation_report["status"],
        "bundle_validation_failures": [] if bundle_validation_report is None else bundle_validation_report["failure_reasons"],
        "failure_reasons": list(dict.fromkeys(report_failures)),
        "usable_bundle": usable_bundle,
    }
    report_path = write_json(bundle_dir / "ingestion_report.v2.json", report_payload)
    return {
        "status": status,
        "usable_bundle": usable_bundle,
        "bundle_path": bundle_path.resolve(),
        "bundle_dir": bundle_dir.resolve(),
        "raw_dir": raw_dir.resolve(),
        "report_path": report_path.resolve(),
        "report": report_payload,
    }
