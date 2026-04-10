# Auto-VTOL-Network V2 Architecture

## Boundary

- `src/avn/` is the legacy research simulator and remains available for historical comparison.
- `src/avn_v2/` is the new operational-model stack.
- The two stacks do not share runtime modules. Comparison is artifact-level, not internal-code reuse.

## V2 Runtime

- Scenario contract is TOML with explicit `network`, `fleet`, `demand`, `dispatch_policy`, `reservation_policy`, `contingency_policy`, `disruptions`, `calibration`, and `outputs` sections.
- Demand is time-based from request files, not fleet duplication.
- Dispatch is reservation-aware and route selection can reroute or divert when direct service is unavailable.
- Promotion outputs are emitted as versioned v2 artifacts from the same run surface.

## V2 Commands

- `python -m avn_v2 run <scenario>`
- `python -m avn_v2 experiment <manifest>`
- `python -m avn_v2 calibrate <bundle>`
- `python -m avn_v2 report <run-or-experiment>`

## V2 Data

- Curated reference bundles live under `data/v2/`.
- Bundles define provenance, a baseline scenario, and metric tolerances for calibration and promotion gating.
