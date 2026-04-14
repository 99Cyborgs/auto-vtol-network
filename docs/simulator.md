# Simulator

## Core Model

- Nodes represent hubs, vertiports, and emergency pads.
- Corridors represent directed graph edges with base speed, base capacity, and weather exposure.
- Vehicles track route, node or corridor position, progress, reserve energy, reroute count, mission class, and contingency target.

## Behaviors

- Corridor departure credit limits flow by effective capacity.
- Node service credit limits departures and makes queue pressure visible.
- Optional node turnaround time can delay re-dispatch after release or arrival.
- Optional stand capacity reserves arrival space on the next hop and can hold departures when a node is full.
- Weather severity reduces corridor speed and capacity with explicit threshold bands.
- Corridor or node disturbances can degrade or close assets.
- Routing recomputes against live conditions, with policy-specific queue, weather, and occupancy penalties to make reroutes interpretable.
- Disruption-avoidant policy mode can hold queued vehicles instead of dispatching them into higher-risk weather.
- If no route to destination exists, the simulator attempts a contingency diversion to a hub or emergency pad.

## Policy Modes

- `balanced`: balances travel time against congestion and moderate disruption penalties.
- `throughput_max`: prefers routes that shed queue pressure and corridor crowding, even if they are longer.
- `disruption_avoidant`: avoids degraded weather corridors earlier and will hold instead of dispatching into higher-risk weather.

## Scenario Contract

Scenario files are JSON and define:

- `scenario_id`
- `name`
- `description`
- `policy_id`
- `recommended`
- `nodes`
- `corridors`
- `vehicles`
- `disturbances`
- `duration_minutes`
- `time_step_minutes`
- `alert_thresholds`

Built-in scenarios live in `src/avn/scenarios/`.

For programmatic authoring, `avn.sim.validate_scenario_payload()` validates the canonical JSON shape before load, and `avn.sim.scenario_to_payload()` converts a `ScenarioDefinition` back into the canonical authoring payload.

Optional node fields:
- `turnaround_minutes`
- `stand_capacity`

## Headless Run

```bash
python -m avn run weather_closure
```

This writes `replay.json` and `summary.json` under `outputs/avn/<scenario>_<timestamp>/`.

## Adaptive Sweep

Run the governed adaptive sweep surface with a manifest JSON file:

```bash
python -m avn adaptive-sweep configs/example_adaptive_sweep_manifest.json
```

Bundled canonical manifests live under `configs/`:

- `example_adaptive_sweep_manifest.json`: baseline queue-threshold refinement
- `example_adaptive_sweep_node_saturation.json`: queue-threshold refinement under node saturation
- `example_adaptive_sweep_weather_corridor_load.json`: corridor-load refinement under weather closure
- `example_adaptive_sweep_incident_low_reserve.json`: reserve-threshold refinement under incident diversion

Supported override path forms:

- `alert_thresholds.<threshold_key>`
- `nodes.<node_id>.<field>`
- `corridors.<corridor_id>.<field>`
- `vehicles.<vehicle_id>.<field>`
- `disturbances.<disturbance_id>.<field>`
