# Unification Plan

This repo is unified around one canonical runtime: `src/avn/`.

## Canonical Package Choice

- Canonical package: `avn`
- Compatibility package: none
- Historical material: `archive/legacy_v2/`, `archive/legacy_runtime/`

## Duplicated Concepts And Chosen Sources Of Truth

Runtime models and state:
- Canonical source: `src/avn/core/state.py`
- Superseded sources: archived `src_avn_v2/models.py`, archived legacy `src_avn/core/models.py`

Replay and run outputs:
- Canonical source: `src/avn/sim/runner.py`
- Canonical writer: `src/avn/governance/artifacts.py`
- Superseded sources: archived `src_avn_v2/artifacts.py`, prior ad hoc `summary.json` + `replay.json` only flow

Threshold governance and promotion:
- Canonical source: `src/avn/governance/thresholds.py`
- Superseded source: archived `src_avn_v2/policy.py` plus archived reporting/governance outputs

Artifact validation:
- Canonical source: `src/avn/governance/validation.py`
- Superseded source: archived `src_avn_v2/validation.py`

Adaptive sweep:
- Canonical source: `src/avn/governance/sweep.py`
- Superseded sources: archived legacy `src_avn/sweep.py`, `src_avn/sweep_adaptive.py`, `src_avn/sweep_analysis.py`

CLI and packaging:
- Canonical source: `src/avn/__main__.py`

## Adapter And Deprecation Plan

Short-term compatibility:
- none in the live package tree

No coequal runtime support:
- Do not restore `avn_v2` operational modules
- Do not restore skill-pack runtime surfaces under `skills/auto_vtol_network`
- Do not expose a second console script or package namespace as active

## Removal Plan For Duplicate Logic

Already removed or quarantined:
- `src/avn_v2/` entirely removed from the live package tree
- legacy `src/avn` analytical sweep/runtime modules moved out of the live package tree
- v2 docs, configs, bundles, and skill-pack content moved under `archive/`

Fail-closed enforcement:
- `tests/test_anti_drift.py` blocks `src/avn_v2` from returning at all
- `tests/test_governance_artifacts.py` blocks artifact drift in the canonical run surface
- `scripts/release_check.py` blocks deprecated skill-pack content from re-entering the wheel

## Migration Status

Current status:
- Single active runtime: yes
- Canonical schema surface: yes
- Canonical artifact contract: yes
- Canonical CLI/package surface: yes
- Deprecated compatibility shim only: no, removed

Final cleanup still pending:
- none inside the live package tree; remaining cleanup is historical archive pruning only

Historical naming note:
- `phase2b` labels that remain in fixture paths or archived compatibility files are historical identifiers only; they do not indicate a second active runtime phase.
