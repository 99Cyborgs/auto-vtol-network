# Auto VTOL Network Skill Pack

Governed skill pack for deterministic research intake, architecture generation, threshold tracking, scenario planning, hazard ledgers, deployment readiness mapping, and promotion summaries.

## Usage

Run the full pipeline from the repository root:

```bash
python -m skills.auto_vtol_network --input skills/auto_vtol_network/templates/sample_request.json --output-dir outputs/skill_pack_run
```

Run a subset of engines:

```bash
python -m skills.auto_vtol_network --input skills/auto_vtol_network/templates/sample_request.json --output-dir outputs/skill_pack_physics --engine physics_threshold_tracking --engine scenario_stress_planning
```

## Scope

- Corridor and node topology only.
- Cargo and public service stay first in service priority.
- Simulation scope is limited to threshold tracking, failure injections, metric specs, and scenario planning.
- Outputs are typed JSON artifacts with provenance, evidence references, assumptions, uncertainties, and engine tags.

## Known Gaps

- This pack does not build a flight-control or operational command-and-control engine.
- Deployment readiness stops at governed planning and simulation promotion surfaces.
- Prompt files are governance references only; runtime generation is code-driven and deterministic.
