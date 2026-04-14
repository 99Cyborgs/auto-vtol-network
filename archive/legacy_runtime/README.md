# Legacy Runtime Archive

This folder retains pre-consolidation `avn` modules that no longer define the canonical package surface.

- `src_avn/`: former analytical/runtime modules removed from `src/avn/` during semantic cleanup, including the old sweep, physics, phase-space, network, vehicle, and visualization paths.
- `configs/`: historical Phase 2B TOML fixtures retained for provenance and retired compatibility shims.

The live package surface is now intentionally limited to:

- `src/avn/core/`
- `src/avn/sim/`
- `src/avn/scenarios/`
- `src/avn/ui/`
