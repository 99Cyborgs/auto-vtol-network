# Simulator

## Core Model

- Nodes represent hubs, vertiports, and emergency pads.
- Corridors represent directed graph edges with base speed, base capacity, and weather exposure.
- Vehicles track route, node or corridor position, progress, reserve energy, reroute count, mission class, and contingency target.

## Behaviors

- Corridor departure credit limits flow by effective capacity.
- Node service credit limits departures and makes queue pressure visible.
- Weather severity reduces corridor speed and capacity with explicit threshold bands.
- Corridor or node disturbances can degrade or close assets.
- Routing recomputes against live conditions, with queue and weather penalties to make reroutes interpretable.
- If no route to destination exists, the simulator attempts a contingency diversion to a hub or emergency pad.

## Scenario Contract

Scenario files are JSON and define:

- `nodes`
- `corridors`
- `vehicles`
- `disturbances`
- `duration_minutes`
- `time_step_minutes`
- `alert_thresholds`

Built-in scenarios live in `src/avn/scenarios/`.

## Headless Run

```bash
python -m avn run weather_closure
```

This writes `replay.json` and `summary.json` under `outputs/avn/<scenario>_<timestamp>/`.
