# auto-vtol-network

## Current State

- The repository is currently centered on a Phase 2C simulator for autonomous VTOL network stress exploration.
- The active implementation supports both fixed scenario execution and deterministic tranche sweeps.
- The built-in tranche surface currently covers `load`, `comms`, `trust`, `contingency`, `weather`, and `coupled` mechanism families.
- The simulator now includes adaptive sweep refinement plus phase-space analysis outputs such as phase maps, transition regions, threshold estimates, threshold ledgers, promotion decisions, and convergence reports.
- Legacy Phase 2B-style scenarios and sweep compatibility remain in the repo and are still exercised by the test suite.
- Current test coverage spans core simulation flow, node and physics behavior, Phase 2B scenario regressions, deterministic tranche execution, and adaptive sweep behavior.

## Project Goal

- Build a reproducible corridor-and-node simulation environment for studying how an autonomous VTOL network degrades under load, weather, communications loss, trust failures, and contingency landing pressure.
- Use the simulator as decision-support infrastructure for identifying when the system leaves its safe operating region, which mechanism dominates first, and how mixed stresses change failure ordering.
- Keep the model intentionally lightweight and interpretable rather than treating it as a production flight-control, certification, or operational command-and-control system.

## Forward Direction

- Continue tightening tranche and adaptive-sweep evidence so phase boundaries and threshold estimates are easier to compare and defend.
- Expand the mechanism-level analysis around coupled failures and cross-tranche promotion logic while preserving deterministic replay and backward compatibility with the existing scenario surface.
- Use the current artifact pipeline to turn tranche results into clearer threshold ledgers, promotion decisions, and cross-tranche comparisons that can guide the next round of architecture and governance questions.
