# auto-vtol-network

`auto-vtol-network` is a Python simulator for a corridor-and-node autonomous VTOL transportation network. Phase 1 is intentionally narrow: it runs a small regional network with discrete time steps, disturbance-aware corridor behavior, simple node queues, simplified vehicle movement, and measurable outputs.

## Why corridor and node architecture

The simulator models the network as directed corridors connecting operational nodes:

- Nodes represent real service and control bottlenecks such as vertiports, hubs, and emergency pads.
- Corridors represent the constrained airspace segments where separation, flow, and capacity interact.
- This split makes disruption effects visible in the right place. Weather and communications degradation affect corridor speed, required separation, and effective capacity, while queues and service rates accumulate at nodes.

That structure is a better fit for regional VTOL operations than a generic agent animation or a pure routing toy, because throughput depends on both corridor constraints and node handling constraints.

## What Phase 1 models

Phase 1 includes:

- A directed `networkx`-backed network with project-owned node and corridor logic.
- Typed dataclass models for node, corridor, vehicle, disturbance, simulation config, and metrics snapshots.
- Three node types: `MicroVertiport`, `HubVertiport`, and `EmergencyPad`.
- Disturbance scalars for weather severity and communications reliability.
- Physics helpers for disturbance-modified speed, separation inflation, effective capacity reduction, node queue stepping, and reserve-energy drain approximation.
- Scenario loading from TOML configs plus CSV sample data.
- A discrete-time simulation engine that loads a scenario, updates disturbances, steps queues, dispatches and moves vehicles, updates corridor state, records events, and records metrics.
- Run outputs: `metrics.csv`, `events.json`, and multiple PNG plots.

Bundled scenarios:

- `configs/nominal.toml`
- `configs/weather_disruption.toml`

Sample network data lives in `data/sample/`.

## What is intentionally deferred

Phase 1 does not try to solve the full research problem. It intentionally defers:

- Detailed vehicle performance and battery electrochemistry.
- Airspace conflict resolution beyond scalar separation inflation and capacity reduction.
- Stochastic demand generation, passenger assignment, maintenance, and dispatch optimization.
- Multi-layer weather fields, terrain, and CFD or rotorcraft aerodynamics.
- A UI, web app, database, or service layer.

The goal here is a runnable base simulator with real state transitions and outputs, not a full operational digital twin.

## Repository layout

- `configs/`: scenario definitions.
- `data/sample/`: sample nodes, corridors, and vehicle manifests.
- `src/avn/`: simulator package code.
- `scripts/`: direct scenario runners.
- `tests/`: unit and small integration tests.

## Quickstart

1. Create and activate a Python 3.11+ environment.
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

3. Run the nominal case:

```bash
python scripts/run_nominal.py
```

4. Run the weather disruption case:

```bash
python scripts/run_weather_case.py
```

5. Run tests:

```bash
python -m pytest
```

You can also run a scenario directly:

```bash
python -m avn configs/nominal.toml
```

## Outputs

Each run creates a timestamped directory under `outputs/` containing:

- `metrics.csv`
- `events.json`
- `completed_vehicles.png`
- `queue_length.png`
- `corridor_performance.png`
- `disturbances.png`

The terminal output also prints a short run summary with completed vehicles, incomplete vehicles, final queue level, peak queue level, run-mean corridor speed, and final mean reserve energy.

## Phase 1 behavior notes

- Service rates are modeled as per-hour dispatch capacity with fractional carry between time steps.
- Corridor flow and density are updated every step from vehicle occupancy and departures.
- Disturbances are scalar but coupled to corridor performance, so the weather case should visibly reduce speed and capacity and increase queue pressure.
- Reserve energy is a simplified proxy quantity used to track mission margin rather than a detailed battery state-of-charge model.
