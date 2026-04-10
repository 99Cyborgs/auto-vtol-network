from __future__ import annotations

import json
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

from .contracts import SkillPackRequest


def _load_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_sample_template_payload() -> dict[str, Any]:
    resource = files("skills.auto_vtol_network.templates").joinpath("sample_request.json")
    with as_file(resource) as path:
        return _load_payload(path)


def _report_root(path: Path) -> Path:
    if path.is_dir():
        return path
    return path.parent


def _load_v2_context(root: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    source_root = root
    experiment_summary_path = root / "experiment_summary.v2.json"
    experiment_summary = _load_payload(experiment_summary_path) if experiment_summary_path.exists() else None
    if experiment_summary is not None and not (root / "report_bundle.v2.json").exists():
        best_root = Path(experiment_summary["best_slice"]["report_bundle_path"]).parent
        root = best_root
    report_bundle = _load_payload(root / "report_bundle.v2.json")
    run_summary = _load_payload(root / "run_summary.v2.json")
    threshold_ledger = _load_payload(root / "threshold_ledger.v2.json")
    hazard_ledger = _load_payload(root / "hazard_ledger.v2.json")
    promotion_decisions = _load_payload(root / "promotion_decisions.v2.json")
    contradictions = _load_payload(root / "contradictions.v2.json")
    calibration_report_path = root / "calibration_report.v2.json"
    calibration_report = _load_payload(calibration_report_path) if calibration_report_path.exists() else None
    bundle_validation_path = (source_root / "bundle_validation.v2.json") if (source_root / "bundle_validation.v2.json").exists() else (root / "bundle_validation.v2.json")
    bundle_validation = _load_payload(bundle_validation_path) if bundle_validation_path.exists() else None
    return (
        report_bundle,
        run_summary,
        threshold_ledger,
        hazard_ledger,
        promotion_decisions,
        contradictions,
        calibration_report,
        experiment_summary,
        bundle_validation,
    )


def build_request_from_v2_bundle(path: Path, *, template_path: Path | None = None) -> SkillPackRequest:
    root = _report_root(path)
    (
        report_bundle,
        run_summary,
        threshold_ledger,
        hazard_ledger,
        promotion_decisions,
        contradictions,
        calibration_report,
        experiment_summary,
        bundle_validation,
    ) = _load_v2_context(root)

    template = _load_payload(template_path) if template_path is not None else _load_sample_template_payload()
    run_id = f"{run_summary['run_id']}:skill_pack"
    timestamp = run_summary["generated_at"]
    evidence_refs = [
        {
            "id": "v2-run-summary",
            "source": str((root / "run_summary.v2.json").resolve()),
            "locator": "run_summary",
            "note": "Primary operational evidence for v2 scenario behavior.",
        },
        {
            "id": "v2-threshold-ledger",
            "source": str((root / "threshold_ledger.v2.json").resolve()),
            "locator": "thresholds",
            "note": "Threshold evidence emitted directly by avn_v2.",
        },
        {
            "id": "v2-hazard-ledger",
            "source": str((root / "hazard_ledger.v2.json").resolve()),
            "locator": "hazards",
            "note": "Hazard evidence emitted directly by avn_v2.",
        },
        {
            "id": "v2-promotion",
            "source": str((root / "promotion_decisions.v2.json").resolve()),
            "locator": "promotion_decisions",
            "note": "Promotion gating derived from v2 artifact contracts.",
        },
    ]
    if calibration_report is not None:
        evidence_refs.append(
            {
                "id": "v2-calibration",
                "source": str((root / "calibration_report.v2.json").resolve()),
                "locator": "calibration_report",
                "note": "Calibration evidence for v2 fit quality and backtest alignment.",
            }
        )
    if bundle_validation is not None:
        evidence_refs.append(
            {
                "id": "v2-bundle-validation",
                "source": str((root / "bundle_validation.v2.json").resolve()),
                "locator": "bundle_validation",
                "note": "Bundle provenance and data-shape validation for v2 calibration evidence.",
            }
        )
    if experiment_summary is not None:
        evidence_refs.append(
            {
                "id": "v2-experiment-summary",
                "source": str((_report_root(path) / "experiment_summary.v2.json").resolve()),
                "locator": "experiment_summary",
                "note": "Experiment-level stability and calibration drift evidence.",
            }
        )

    fit_quality_summary = None if calibration_report is None else calibration_report.get("fit_quality_summary")
    calibration_confidence = None if calibration_report is None else calibration_report.get("confidence_tier")
    confidence_score = None if calibration_report is None else calibration_report.get("confidence_score")
    experiment_blockers = {"fit_failure": [], "evidence_insufficiency": [], "operational_breach": []}
    if experiment_summary is not None:
        experiment_blockers = experiment_summary.get("promotion_blockers", experiment_blockers)
    promotion_axes = promotion_decisions.get("decision_axes", {})
    template.update(
        {
            "run_id": run_id,
            "timestamp": timestamp,
            "provenance": {
                "sources": [entry["source"] for entry in evidence_refs],
                "baselines": ["avn_v2", "data_backed_calibration"],
                "generated_by": "skills.auto_vtol_network.v2_adapter",
                "run_id": run_id,
                "lineage": [
                    "avn_v2_contract_ingest",
                    report_bundle["summary"]["dominant_failure_chain"],
                    "experiment_level_ingest" if experiment_summary is not None else "run_level_ingest",
                ],
            },
            "assumptions": [
                "Legacy avn artifacts are not used for this request.",
                "Promotion evidence is derived from avn_v2 contract-versioned outputs.",
                "Cargo and public service remain the leading governed service priorities.",
            ],
            "evidence_refs": evidence_refs,
            "uncertainties": [
                f"[{finding.get('category', 'general')}/{finding.get('severity', 'unknown')}] {finding['note']}"
                for finding in contradictions["findings"]
                if finding["status"] == "open"
            ]
            or ["No open contradictions were recorded by avn_v2."],
            "network_name": f"Auto VTOL Network V2 - {run_summary['scenario_name']}",
            "node_roles": ["hub", "contingency_pad"],
            "corridor_policies": ["reservation_aware_dispatch", "reroute_gating", "contingency_diversion"],
            "governance_controls": ["v2_contract_ledgers", "calibration_required", "calibration_confidence", "promotion_blockers"],
            "research_records": [
                {
                    "id": "research-v2-operational-baseline",
                    "title": "V2 Operational Baseline Intake",
                    "source_kind": "simulation_run",
                    "portfolio_ref": run_summary["run_id"],
                    "summary": f"Scenario {run_summary['scenario_name']} completed {run_summary['completed_requests']} requests with dominant failure chain {run_summary['dominant_failure_chain']}.",
                    "constraints": [
                        "corridor_node_topology_only",
                        "dispatch_and_reroute_realism_first",
                        "promotion_requires_contract_artifacts",
                    ],
                    "architecture_drivers": [
                        "reservation_aware_dispatch",
                        "event_chain_classification",
                        "contingency_diversion_accounting",
                    ],
                    "threshold_hypotheses": [threshold["id"] for threshold in threshold_ledger["thresholds"]],
                    "open_questions": [finding["note"] for finding in contradictions["findings"]],
                },
                {
                    "id": "research-v2-calibration",
                    "title": "V2 Calibration Intake",
                    "source_kind": "calibration_bundle" if calibration_report is not None else "missing_calibration",
                    "portfolio_ref": calibration_report["bundle_id"] if calibration_report is not None else "missing",
                    "summary": (
                        f"Calibration bundle {calibration_report['bundle_id']} reported confidence={calibration_confidence}, score={confidence_score}, promotable={calibration_report['promotable']}, fit quality {fit_quality_summary}."
                        if calibration_report is not None
                        else "Calibration evidence is absent and remains a promotion blocker."
                    ),
                    "constraints": [
                        "data_backed_calibration_required",
                        "promotion_requires_fit_report",
                    ],
                    "architecture_drivers": ["backtest_alignment", "reference_bundle_provenance"],
                    "threshold_hypotheses": [threshold["id"] for threshold in threshold_ledger["thresholds"]],
                    "open_questions": (
                        list(calibration_report.get("failure_reasons", ()))
                        if calibration_report is not None
                        else ["What error band should gate promotion for this scenario family?"]
                    ),
                },
            ],
            "state_variables": [
                {
                    "id": f"state-{threshold['metric_key'].replace('_', '-')}",
                    "name": threshold["metric_key"].replace("_", " ").title(),
                    "symbol": threshold["metric_key"],
                    "metric_key": threshold["metric_key"],
                    "unit": "scalar",
                    "description": f"Governed v2 metric for {threshold['metric_key']}.",
                    "safe_operating_guidance": f"Keep {threshold['metric_key']} within the avn_v2 threshold ledger bounds.",
                    "failure_regime_indicators": [run_summary["dominant_failure_chain"]],
                }
                for threshold in threshold_ledger["thresholds"]
            ],
            "thresholds": [
                {
                    "id": threshold["id"],
                    "variable_id": f"state-{threshold['metric_key'].replace('_', '-')}",
                    "metric_key": threshold["metric_key"],
                    "comparator": "<=",
                    "warning_value": threshold.get("warning_value"),
                    "breach_value": threshold["breach_value"],
                    "rationale": f"Derived from avn_v2 status={threshold['status']} with fit summary {fit_quality_summary} and calibration confidence {calibration_confidence}.",
                    "linked_hazard_ids": [hazard["id"] for hazard in hazard_ledger["hazards"]],
                }
                for threshold in threshold_ledger["thresholds"]
            ],
        }
    )

    scenario_id = "scenario-v2-operational"
    template["scenarios"] = [
        {
            "id": scenario_id,
            "name": f"{run_summary['scenario_name']} governed scenario",
            "stress_family": run_summary["dominant_failure_chain"],
            "description": f"Governed scenario synthesized from avn_v2 run {run_summary['run_id']}.",
            "failure_injections": [
                {
                    "id": f"injection-{hazard['id']}",
                    "mechanism": hazard["mechanism"],
                    "target": "network",
                    "description": hazard["title"],
                    "linked_threshold_ids": [threshold["id"] for threshold in template["thresholds"]],
                }
                for hazard in hazard_ledger["hazards"]
            ],
            "metric_specs": [
                {
                    "id": f"metric-{threshold['metric_key']}",
                    "name": threshold["metric_key"].replace("_", " ").title(),
                    "metric_key": threshold["metric_key"],
                    "unit": "scalar",
                    "comparator": "<=",
                    "target": threshold["breach_value"],
                    "description": f"Governed target imported from avn_v2 threshold {threshold['id']}.",
                }
                for threshold in template["thresholds"]
            ],
            "success_criteria": [
                {
                    "id": "success-completed-requests",
                    "metric_key": "completed_requests",
                    "comparator": ">=",
                    "target": max(1, run_summary["completed_requests"]),
                    "description": "Maintain completed demand throughput at or above the observed baseline.",
                }
            ]
            + (
                []
                if experiment_summary is None
                else [
                    {
                        "id": "success-calibration-gate",
                        "metric_key": "calibration_gate",
                        "comparator": "<=",
                        "target": 0,
                        "description": f"Experiment grouped blockers must be empty: {experiment_blockers}.",
                    }
                ]
            ),
            "linked_threshold_ids": [threshold["id"] for threshold in template["thresholds"]],
            "linked_hazard_ids": [hazard["id"] for hazard in hazard_ledger["hazards"]],
        }
    ]

    template["hazards"] = [
        {
            "id": hazard["id"],
            "title": hazard["title"],
            "severity": hazard["severity"],
            "description": hazard["mitigation"],
            "trigger_conditions": [f"{key}={value}" for key, value in hazard["evidence"].items()],
            "mitigations": [hazard["mitigation"]],
            "linked_threshold_ids": [threshold["id"] for threshold in template["thresholds"]],
            "linked_scenario_ids": [scenario_id],
        }
        for hazard in hazard_ledger["hazards"]
    ]

    blockers = []
    for finding in contradictions["findings"]:
        if finding["status"] != "open":
            continue
        blockers.append(
            {
                "id": finding["id"],
                "title": finding["id"].replace(".", " ").replace("_", " ").title(),
                "severity": "critical" if finding.get("category") == "evidence_insufficiency" else "high",
                "description": finding["note"],
                "affected_surfaces": ["promotion", finding.get("category", "governance")],
                "resolution_path": f"Address the underlying avn_v2 contradiction, currently policy_effect={finding.get('policy_effect', 'unknown')}, and re-run report generation.",
            }
        )
    grouped_imported_blockers = {}
    for blocker_group in {"fit_failure", "evidence_insufficiency", "operational_breach"}:
        grouped_imported_blockers[blocker_group] = list(
            dict.fromkeys(
                list(promotion_decisions.get("promotion_blockers", {}).get(blocker_group, []))
                + list(experiment_blockers.get(blocker_group, []))
            )
        )
    for blocker_group, blocker_ids in grouped_imported_blockers.items():
        for blocker_id in blocker_ids:
            if any(blocker["id"] == blocker_id for blocker in blockers):
                continue
            blockers.append(
                {
                    "id": blocker_id,
                    "title": blocker_id.replace("_", " ").replace(":", " ").title(),
                    "severity": "critical" if blocker_group == "evidence_insufficiency" else "high",
                    "description": f"Promotion blocker imported from avn_v2 category={blocker_group}: {blocker_id}.",
                    "affected_surfaces": ["promotion", blocker_group],
                    "resolution_path": "Resolve the blocker in avn_v2 and regenerate the governed request.",
                }
            )
    if not blockers:
        blockers.append(
            {
                "id": "blocker-none",
                "title": "No Material Blockers",
                "severity": "low",
                "description": "The avn_v2 bundle did not emit open blockers.",
                "affected_surfaces": ["promotion"],
                "resolution_path": "Maintain current evidence density and monitor for regressions.",
            }
        )
    template["blockers"] = blockers
    blocker_ids = [blocker["id"] for blocker in blockers]
    template["deployment_stages"] = [
        {
            "id": "stage-v2-threshold-tracking",
            "stage": "threshold_tracking",
            "readiness_state": "simulation_ready",
            "entry_criteria": ["avn_v2 run summary exists", "threshold ledger exists"],
            "exit_criteria": ["thresholds remain bounded", "hazards are linked to mitigations"],
            "blocker_ids": blocker_ids,
            "allowed_services": ["cargo", "public_service"],
            "notes": ["Stage synthesized directly from avn_v2 artifact contracts."],
        },
        {
            "id": "stage-v2-governed-pilot",
            "stage": "governed_pilot_planning",
            "readiness_state": (
                "promotable"
                if promotion_axes.get("promotion_eligibility", {}).get("decision") == "promote"
                and not any(experiment_blockers.values())
                else "blocked"
            ),
            "entry_criteria": ["calibration report cited", "promotion blockers reviewed"],
            "exit_criteria": ["promotion decision is promote", "open contradictions resolved"],
            "blocker_ids": blocker_ids,
            "allowed_services": ["cargo", "public_service"],
            "notes": [decision["rationale"] for decision in promotion_decisions["decisions"]],
        },
    ]
    template["decision_context"] = [
        f"dominant_failure_chain={run_summary['dominant_failure_chain']}",
        f"completed_requests={run_summary['completed_requests']}",
        f"diversion_count={run_summary['diversion_count']}",
        f"calibration_present={calibration_report is not None}",
        f"calibration_promotable={False if calibration_report is None else calibration_report.get('promotable', False)}",
        f"calibration_confidence={calibration_confidence}",
        f"confidence_score={confidence_score}",
        f"policy_result={promotion_decisions.get('policy_result')}",
        f"bundle_validation_status={None if bundle_validation is None else bundle_validation.get('status')}",
        f"fit_quality_summary={fit_quality_summary}",
        f"experiment_blockers={experiment_blockers}",
    ]
    return SkillPackRequest.from_dict(template)
