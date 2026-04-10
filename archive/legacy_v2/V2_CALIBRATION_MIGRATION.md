# AVN V2 Calibration Contract Migration

## Change

`data/v2/.../bundle.json` moved again from the calibration-strength-aware schema to a validated evidence-package contract with explicit confidence and governance inputs.

## Prior Shape

- `contract_version = 3`
- coverage requirements and grouped objective weights
- deterministic search metadata plus confidence tier
- no first-class bundle-validation artifact
- no declared confidence policy
- no experiment-level governance policy surface

## New Shape

- `contract_version = 4`
- `backtest.metric_targets`
- `backtest.event_expectations`
- `backtest.series_targets`
- `coverage_requirements`
- `confidence_policy`
- `quality_checks`
- `reference_sources`
- `objective_group_weights`
- `fit_space.parameters`
- `gates`
- windowed series targets via `minute_start`, `minute_end`, and `aggregation`
- calibration report fields for `confidence_score`, `confidence_components`, `confidence_policy`, and `bundle_validation_id`
- experiment manifest field `governance_policy`
- policy-evaluated promotion artifacts and contradiction metadata
- emitted `bundle_validation.v2.json`

## Consumer Surfaces Updated

- `src/avn_v2/config.py`
- `src/avn_v2/calibration.py`
- `src/avn_v2/fitting.py`
- `src/avn_v2/validation.py`
- `src/avn_v2/policy.py`
- `src/avn_v2/experiments.py`
- `src/avn_v2/reporting.py`
- `skills/auto_vtol_network/v2_adapter.py`
- `tests/test_avn_v2.py`

## Notes

- `python -m avn_v2 calibrate <bundle>` now emits both `bundle_validation.v2.json` and `calibration_report.v2.json`.
- `python -m avn_v2 experiment <manifest>` now applies a declared governance policy and fails closed on invalid bundles unless the policy explicitly waives the blocker category.
- `python -m avn_v2 ingest <source-manifest>` now converts external offline datasets into a normal local `bundle.json` plus `ingestion_report.v2.json`; `calibrate` and `experiment` still consume only bundle-shaped evidence packages.
- Experiment manifests may now point at `ingested_bundle_source` as a convenience alias for a previously generated local bundle path.
- `backtest_trace.v2.json` is now part of the v2 artifact surface.
- Sample bundles now include `reference_baseline`, `queue_pressure_backtest`, and `reroute_contingency_backtest`.
