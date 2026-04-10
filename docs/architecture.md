# Architecture

The repo now has one canonical runtime surface: `avn`.

## Product Surfaces

- `src/avn/core/`
  Graph primitives, routing, weather degradation functions, queue/service logic, metrics, alerts, and canonical replay state models.
- `src/avn/sim/`
  Scenario loading, disturbance injection, deterministic event loop, replay writing, and CLI-facing scenario execution.
- `src/avn/governance/`
  Canonical schema and artifact surfaces for threshold ledgers, promotion decisions, artifact manifests, validation reports, and adaptive sweep outputs.
- `src/avn/ui/`
  A small local HTTP dashboard that renders the same replay bundle emitted by the simulator.

## Execution Model

1. Load a deterministic scenario definition from `src/avn/scenarios/`.
2. Build a directed corridor graph with queue-aware routing weights.
3. Apply active disturbances per time step.
4. Move enroute vehicles, service queued vehicles, reroute around closures, and divert to contingency nodes when no destination route exists.
5. Emit canonical replay state, alerts, metrics, and event log entries.
6. Derive the threshold ledger and promotion decisions from the same replay summary.
7. Write one manifest-governed artifact family and validate it before release use.
8. Reuse the replay bundle in the dashboard without a second UI-side model.

## Why This Is Smaller

- One package surface instead of `avn` plus `avn_v2`.
- One replay contract plus one governed artifact contract instead of separate runtime artifacts and governance bundles.
- One dashboard path that consumes simulator truth directly.
- One live `src/avn/` package surface limited to `core`, `sim`, `governance`, `scenarios`, and `ui`.

## Compatibility Policy

- `python -m avn` is the canonical CLI and packaging surface.
- `src/avn_v2` has been removed from the live package tree.
- All historical `avn_v2` operational-model modules, calibration artifacts, and legacy analytical sweep modules are quarantined under `archive/`.

## Anti-Drift Gates

- `tests/test_anti_drift.py` fails if `src/avn_v2` reappears, if release gates target deprecated surfaces, or if replay writing bypasses the canonical artifact writer.
- `tests/test_governance_artifacts.py` fails if canonical run outputs stop emitting the threshold, promotion, validation, and manifest surfaces.
- `make release-check` builds the wheel, verifies the packaged contents, performs a run smoke test, and validates the emitted run directory.

## Archived Material

Legacy v2 governance, calibration, and skill-pack material was moved under `archive/legacy_v2/`.
Residual pre-consolidation `avn` analytical/runtime modules were moved under `archive/legacy_runtime/`.

See also: [Unification Plan](unification.md)
