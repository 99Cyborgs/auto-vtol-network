# Dashboard Demo

The dashboard is a thin HTTP surface over canonical simulator state.

## Launch

```bash
python -m avn dashboard
```

Or launch the recommended demo directly:

```bash
python -m avn demo
```

Open the printed local URL in a browser.

## What It Shows

- network graph with live corridor load,
- node queue pressure and degraded status,
- vehicles moving through replay frames,
- corridor closures and weather severity,
- alert feed,
- event log,
- scenario selection,
- deterministic playback controls.

## Contract

The dashboard reads `/api/replay?scenario=<id>`, which is the same replay bundle structure emitted to disk by headless runs.
