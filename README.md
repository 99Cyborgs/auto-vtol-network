![Auto-VTOL-Network hero](docs/assets/flight-hero.svg)

# Auto-VTOL-Network

Auto-VTOL-Network is a deterministic simulator for autonomous VTOL traffic moving through a network of hubs, vertiports, corridors, weather disruptions, and dispatch policies.

It is built to answer questions like:

- What happens to flow when a corridor closes mid-run?
- Which routing policy sheds queue pressure better?
- How much reserve energy is left after a diversion?
- Can a run be replayed, validated, and compared without ambiguity?

Every run produces a replayable state history, a small dashboard-friendly artifact bundle, and governed validation outputs that make the result easy to inspect or reuse.

## Why This Project Exists

Auto-VTOL-Network aims to make network-level VTOL behavior understandable without needing a full production control plane.

- It gives you deterministic scenarios instead of opaque live state.
- It keeps routing, queueing, weather, and alert behavior visible step by step.
- It lets you compare policy choices with the same scenario inputs.
- It writes validation and release-style artifacts from the same run output used by the dashboard.

## What You Can Do

- Run built-in scenarios such as weather closure, node saturation, priority dispatch, incident diversion, and metro surge.
- Compare policy modes that trade off speed, congestion, and disruption avoidance.
- Open a local dashboard that replays the exact state emitted by the simulator.
- Validate a run directory or a batch directory from the CLI.
- Run batch comparisons and adaptive sweeps over governed thresholds.

## Quick Start

Install the package:

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

Try the fastest demo path:

```bash
python -m avn demo
```

Run a headless scenario and inspect its artifacts:

```bash
python -m avn run weather_closure
python -m avn validate-run outputs/avn/<run-dir>
```

Launch the dashboard against a saved replay:

```bash
python -m avn dashboard --replay outputs/avn/<run-dir>/replay.json
```

Run the full test suite:

```bash
python -m pytest
```

Run the release gate:

```bash
make release-check
```

Portable fallback when `make` is unavailable:

```bash
python scripts/release_check.py
```

## How It Works

The canonical runtime surface is `src/avn/`.

- `src/avn/core/` holds graph primitives, routing, weather effects, queue logic, metrics, alerts, and replay state definitions.
- `src/avn/sim/` holds scenario loading, the simulation engine, replay helpers, batch execution, and runtime orchestration.
- `src/avn/governance/` holds threshold ledgers, promotion decisions, validation, manifests, and adaptive sweep support.
- `src/avn/scenarios/` holds the built-in deterministic scenario suite.
- `src/avn/ui/` holds the local dashboard that consumes the simulator replay contract directly.

The historical `avn_v2` surface has been removed from the live runtime. Archived material remains under `archive/` for provenance only.

## Built-In Scenarios

- `baseline_flow`
- `weather_closure`
- `node_saturation`
- `priority_mission`
- `incident_diversion_balanced`
- `incident_diversion_avoidant`
- `metro_surge_balanced`
- `metro_surge_throughput_max`

List them from the CLI:

```bash
python -m avn list-scenarios
```

## Policy Modes

- `balanced`: balances travel time against congestion and moderate disruption penalties.
- `throughput_max`: prefers routes that shed queue pressure and corridor crowding, even if they are longer.
- `disruption_avoidant`: avoids degraded weather corridors earlier and will hold instead of dispatching into higher-risk weather.

## Run Outputs

Each headless run writes one governed artifact family under `outputs/avn/`:

- `replay.json`: deterministic time-stepped state history consumed by the dashboard.
- `summary.json`: compact scenario summary and alert counts.
- `threshold_ledger.json`: threshold evaluations derived from the run summary.
- `promotion_decisions.json`: release decisions derived from the threshold ledger.
- `validation_report.json`: schema and consistency validation status.
- `artifact_manifest.json`: artifact inventory plus content hashes.

Validate a batch directory:

```bash
python -m avn batch-run baseline_flow weather_closure --repeat 2
python -m avn validate-batch outputs/avn_batch/<batch-dir>
```

Run an adaptive sweep from a manifest:

```bash
python -m avn adaptive-sweep configs/example_adaptive_sweep_manifest.json
```

Refresh the packaged demo replay bundles after demo-scenario changes:

```bash
python scripts/refresh_demo_replays.py
```

## Roadmap

### Now

- Canonical `avn` runtime and CLI
- Deterministic scenario replay
- Local dashboard replay loading
- Batch execution and validation
- Adaptive sweep support
- Governed run artifacts and release checks

### Next

- Continue pruning historical archive surfaces that are no longer needed for reference or provenance
- Keep tightening the public demo and documentation flow around the canonical runtime

### Explicitly Deferred

- Production control-plane behavior
- Certification-style governance workflows
- External ingestion pipelines
- Policy-heavy evidence packaging

## Documentation

- [Architecture](docs/architecture.md)
- [Simulator](docs/simulator.md)
- [Dashboard Demo](docs/dashboard_demo.md)
- [Unification Plan](docs/unification.md)
- [Roadmap Notes](docs/roadmap.md)
