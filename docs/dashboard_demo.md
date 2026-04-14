# Dashboard Demo

The dashboard is a thin HTTP surface over canonical simulator state.

## Launch

```bash
python -m avn dashboard
```

Or render a previously saved replay bundle directly:

```bash
python -m avn run weather_closure
python -m avn dashboard --replay outputs/avn/<run-dir>/replay.json
```

Or launch the recommended demo directly:

```bash
python -m avn demo
```

`python -m avn demo` now loads the curated presentation set in this order:

- `incident_diversion_balanced`
- `incident_diversion_avoidant`
- `metro_surge_balanced`
- `metro_surge_throughput_max`

The demo command serves frozen packaged replay bundles for those scenarios instead of re-running the simulator on launch.

Open the printed local URL in a browser.

## Browser Smoke

Run the developer-side browser smoke gate with:

```bash
make dashboard-smoke
```

This launches a Playwright CLI smoke pass against both:

- `python -m avn demo`
- `python -m avn dashboard --replay <saved replay>`

The smoke gate validates scenario ordering, hero/policy/metric rendering, network SVG output, playback controls, scenario switching, and replay-backed rendering from a saved artifact. Artifacts are written under `output/playwright/`.

Use headed mode when you want to watch the flow:

```bash
python scripts/dashboard_smoke.py --headed
```

## What It Shows

- policy identity and policy posture,
- active disruptions with operator notes,
- network graph with live corridor load,
- node queue pressure and degraded status,
- vehicles moving through replay frames,
- corridor closures and weather severity,
- alert feed,
- event log,
- scenario selection,
- deterministic playback controls.

## Recommended Demo Flow

- `incident_diversion_balanced`
- `incident_diversion_avoidant`
- `metro_surge_balanced`
- `metro_surge_throughput_max`

## Contract

The dashboard reads `/api/replay?scenario=<id>`, which is the same replay bundle structure emitted to disk by headless runs. Replay bundles now include policy metadata alongside scenario description, summary, steps, and event log.

When launched with `--replay`, the dashboard serves the saved canonical `replay.json` artifact directly instead of re-running the simulator in process.

Refresh the packaged demo replay set after changing any of the curated demo scenarios with:

```bash
python scripts/refresh_demo_replays.py
```
