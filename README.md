# auto-vtol-network

## Stack Status

- `src/avn/` is the legacy Phase 2C research simulator and remains in place for backward comparison and historical scenario replay.
- `src/avn_v2/` is the new operational-model stack with a separate scenario contract, dispatch/reroute runtime, calibration flow, experiment runner, and versioned evidence artifacts.
- The two stacks are intentionally separate. Runtime modules are not shared across `avn` and `avn_v2`.

## Current State

- The repository is currently centered on a Phase 2C simulator for autonomous VTOL network stress exploration.
- The active implementation supports both fixed scenario execution and deterministic tranche sweeps.
- The built-in tranche surface currently covers `load`, `comms`, `trust`, `contingency`, `weather`, and `coupled` mechanism families.
- The simulator now includes adaptive sweep refinement plus phase-space analysis outputs such as phase maps, transition regions, threshold estimates, threshold ledgers, promotion decisions, and convergence reports.
- Legacy Phase 2B-style scenarios and sweep compatibility remain in the repo and are still exercised by the test suite.
- Current test coverage spans core simulation flow, node and physics behavior, Phase 2B scenario regressions, deterministic tranche execution, and adaptive sweep behavior.
- A new `avn_v2` path now exists for operational-model work with `run`, `experiment`, `calibrate`, and `report` entrypoints plus versioned v2 artifacts.
- V2 calibration now uses a validated evidence-package contract with fit-space parameters, coverage requirements, windowed series targets, explicit confidence policy, bundle validation, and policy-aware promotion artifacts.

## V2 Quick Start

- Run a v2 scenario: `python -m avn_v2 run configs/v2/nominal.toml`
- Run a v2 experiment: `python -m avn_v2 experiment configs/v2/dispatch_conflict_experiment.toml`
- Calibrate against a reference bundle: `python -m avn_v2 calibrate data/v2/bundles/reference_baseline/bundle.json`
  This now emits both `*.bundle_validation.v2.json` and `*.calibration_report.v2.json`.
- Ingest external offline data into a standard v2 bundle: `python -m avn_v2 ingest <source-manifest.json>`
  This stages raw files under `data/v2/external/raw/...`, writes a normalized bundle under `data/v2/external/bundles/...`, and emits `ingestion_report.v2.json`.
- Additional sample bundles: `data/v2/bundles/queue_pressure_backtest/` and `data/v2/bundles/reroute_contingency_backtest/`
- Render a report view from a v2 run directory: `python -m avn_v2 report outputs/v2/<run-dir>`
- Feed a v2 run into the governed skill pack: `python -m skills.auto_vtol_network --v2-input outputs/v2/<run-dir> --output-dir outputs/skill_pack_v2`
- Migration note for the new calibration contract: `docs/V2_CALIBRATION_MIGRATION.md`
- External ingestion contract note: `docs/V2_EXTERNAL_INGESTION.md`

## Packaging / Release Check

- Supported installed entrypoints are `python -m avn_v2` and `python -m skills.auto_vtol_network`.
- Run `make release-check` before release work. This builds a wheel from a clean `build/` and `dist/`, rejects setuptools package-ambiguity warnings, verifies that skill-pack tests are not shipped, and smoke-tests both installed entrypoints plus packaged template access from a fresh virtual environment.

## Project Goal

- Build a reproducible corridor-and-node simulation environment for studying how an autonomous VTOL network degrades under load, weather, communications loss, trust failures, and contingency landing pressure.
- Use the simulator as decision-support infrastructure for identifying when the system leaves its safe operating region, which mechanism dominates first, and how mixed stresses change failure ordering.
- Keep the model intentionally lightweight and interpretable rather than treating it as a production flight-control, certification, or operational command-and-control system.

## Forward Direction

- Continue tightening tranche and adaptive-sweep evidence so phase boundaries and threshold estimates are easier to compare and defend.
- Expand the mechanism-level analysis around coupled failures and cross-tranche promotion logic while preserving deterministic replay and backward compatibility with the existing scenario surface.
- Use the current artifact pipeline to turn tranche results into clearer threshold ledgers, promotion decisions, and cross-tranche comparisons that can guide the next round of architecture and governance questions.
