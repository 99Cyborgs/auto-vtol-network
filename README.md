# Auto-VTOL-Network

Auto-VTOL-Network now has one canonical runtime surface: `src/avn/`.

`src/avn` is the source of truth for:

- runtime models and replay state,
- governed threshold ledgers and promotion decisions,
- artifact writing and validation,
- adaptive sweep execution,
- CLI and packaged entrypoints.

The former `avn_v2` package has been removed. `avn` is the only supported runtime, packaging, and CLI surface.

The repo demonstrates:

- corridor traffic flow,
- node throughput and queue pressure,
- weather-driven degradation,
- reroutes under closures or degraded conditions,
- alerts and contingency handling,
- deterministic replayable scenarios,
- governed threshold and release artifacts,
- canonical adaptive sweep outputs.

## Quick Start

1. Install:

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

2. Run a headless scenario:

```bash
python -m avn run weather_closure
```

3. Launch the dashboard:

```bash
python -m avn dashboard
```

4. Launch the canonical demo scenario directly:

```bash
python -m avn demo
```

5. Run tests:

```bash
python -m pytest
```

6. Run the canonical release gate:

```bash
make release-check
```

Portable fallback when `make` is unavailable:

```bash
python scripts/release_check.py
```

## Mainline Structure

- `src/avn/core/`: graph, routing, weather, queueing, metrics, alerts, and replay state definitions.
- `src/avn/sim/`: scenario loading, disturbance injection, deterministic event loop, run output, and replay IO.
- `src/avn/governance/`: canonical threshold ledger, promotion decision, artifact manifest, validation, and adaptive sweep surfaces.
- `src/avn/scenarios/`: built-in deterministic scenario suite.
- `src/avn/ui/`: local HTTP dashboard and serialized state contract.
- `docs/`: concise product-facing architecture and usage docs.
- `archive/`: superseded v2 assets plus archived pre-consolidation analytical/runtime modules retained for history only.

## Built-In Scenarios

- `baseline_flow`
- `weather_closure`
- `node_saturation`
- `priority_mission`

List them from the CLI with:

```bash
python -m avn list-scenarios
```

## Run Outputs

Each headless run now writes one canonical governed artifact family:

- `replay.json`: deterministic time-stepped state history consumed by the dashboard contract,
- `summary.json`: compact scenario summary and alert counts,
- `threshold_ledger.json`: threshold evaluations derived from the run summary,
- `promotion_decisions.json`: release decisions derived from the threshold ledger,
- `validation_report.json`: schema and consistency validation status,
- `artifact_manifest.json`: artifact inventory plus content hashes.

Default output root: `outputs/avn/`.

Validate a run directory with:

```bash
python -m avn validate-run outputs/avn/<run-dir>
```

Run the canonical adaptive sweep with a manifest JSON file:

```bash
python -m avn adaptive-sweep path/to/manifest.json
```

Compatibility policy:

- `python -m avn` is the only canonical CLI surface.
- archived `avn_v2` modules, legacy sweep code, and skill-pack content are historical references under `archive/`; they are not release surfaces or compatibility targets.

## Documentation

- [Architecture](docs/architecture.md)
- [Unification Plan](docs/unification.md)
- [Simulator](docs/simulator.md)
- [Dashboard Demo](docs/dashboard_demo.md)
- [Roadmap](docs/roadmap.md)
