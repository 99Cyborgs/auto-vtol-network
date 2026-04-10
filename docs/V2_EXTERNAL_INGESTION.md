# AVN V2 External Ingestion

## Purpose

`python -m avn_v2 ingest <source-manifest.json>` is the only supported path for bringing external reference data into v2 tuning. The runtime does not read remote URLs, live APIs, or vendor-native schemas directly.

The ingestion flow is:

1. Load a source manifest.
2. Stage raw files under `data/v2/external/raw/<source_id>/<version>/`.
3. Normalize rows into canonical v2 bundle files.
4. Generate a standard `bundle.json`.
5. Run bundle validation.
6. Emit `ingestion_report.v2.json`.

`calibrate` and `experiment` continue to consume only bundle-shaped local evidence packages.

## Source Manifest Shape

The source manifest is JSON and is separate from the v2 bundle contract. It includes:

- `source_id`
- `source_type`
- `version`
- `scenario`
- `ingestion_mode`
- `bundle_family`
- `raw_inputs`
- `field_mapping`
- `normalization`
- `fit_space_overrides`
- `coverage_requirements`
- `confidence_policy`
- `quality_checks`
- `provenance_defaults`
- optional `output_root`

Supported `source_type` values:

- `csv_directory`
- `json_files`
- `pre-extracted_archive`

Supported `ingestion_mode` values:

- `copy`

## Generated Outputs

The ingester writes:

- staged raw files in `data/v2/external/raw/...`
- normalized `reference_metrics.csv`
- normalized `event_expectations.csv` when present
- normalized `series_targets.csv` when present
- generated `bundle.json`
- `ingestion_report.v2.json`

If bundle validation fails, the generated files remain on disk but the report marks the bundle as unusable.

## Runtime Reuse

`python -m avn_v2 calibrate <bundle.json>` works unchanged against ingested bundles.

Experiment manifests may either:

- set `calibration_bundle = "<local bundle.json>"`
- or set `ingested_bundle_source = "<source-manifest-or-generated-bundle>"`

`ingested_bundle_source` only resolves a previously generated local bundle path. It does not trigger ingestion during experiment execution.
