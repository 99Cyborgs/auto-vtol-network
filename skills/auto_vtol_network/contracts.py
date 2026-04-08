from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from typing import Any, Mapping, Self, Sequence

from .enums import ArtifactType, BlockerSeverity, Comparator, DeploymentStage, EngineType, ReadinessState


def _serialize(value: Any) -> Any:
    if is_dataclass(value):
        return {field.name: _serialize(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, tuple):
        return [_serialize(item) for item in value]
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if hasattr(value, "value"):
        return value.value
    return value


def _mapping(payload: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return {} if payload is None else payload


def _as_str(value: Any) -> str:
    return str(value).strip()


def _dedupe_strings(values: Sequence[Any]) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _as_str(value)
        if not text or text in seen:
            continue
        ordered.append(text)
        seen.add(text)
    return tuple(ordered)


def _merge_strings(shared: Sequence[str], local: Sequence[Any] | None) -> tuple[str, ...]:
    local_values = [] if local is None else list(local)
    return _dedupe_strings(list(shared) + local_values)


@dataclass(slots=True, frozen=True)
class SerializableModel:
    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)


@dataclass(slots=True, frozen=True)
class Provenance(SerializableModel):
    sources: tuple[str, ...]
    baselines: tuple[str, ...]
    generated_by: str
    run_id: str
    lineage: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        return cls(
            sources=_dedupe_strings(payload.get("sources", ())),
            baselines=_dedupe_strings(payload.get("baselines", ())),
            generated_by=_as_str(payload.get("generated_by", "")),
            run_id=_as_str(payload.get("run_id", "")),
            lineage=_dedupe_strings(payload.get("lineage", ())),
        )


@dataclass(slots=True, frozen=True)
class EvidenceReference(SerializableModel):
    id: str
    source: str
    locator: str
    note: str = ""

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        return cls(
            id=_as_str(payload.get("id", "")),
            source=_as_str(payload.get("source", "")),
            locator=_as_str(payload.get("locator", "")),
            note=_as_str(payload.get("note", "")),
        )


@dataclass(slots=True, frozen=True)
class GovernedMetadata(SerializableModel):
    id: str
    timestamp: str
    provenance: Provenance
    assumptions: tuple[str, ...]
    evidence_refs: tuple[EvidenceReference, ...]
    uncertainties: tuple[str, ...]
    engine_tags: tuple[EngineType, ...]


@dataclass(slots=True, frozen=True)
class MetricSpecification(SerializableModel):
    id: str
    name: str
    metric_key: str
    unit: str
    comparator: Comparator
    target: float
    description: str

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        return cls(
            id=_as_str(payload.get("id", "")),
            name=_as_str(payload.get("name", "")),
            metric_key=_as_str(payload.get("metric_key", "")),
            unit=_as_str(payload.get("unit", "")),
            comparator=Comparator(_as_str(payload.get("comparator", Comparator.GREATER_THAN_OR_EQUAL.value))),
            target=float(payload.get("target", 0.0)),
            description=_as_str(payload.get("description", "")),
        )


@dataclass(slots=True, frozen=True)
class FailureInjection(SerializableModel):
    id: str
    mechanism: str
    target: str
    description: str
    linked_threshold_ids: tuple[str, ...]

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        return cls(
            id=_as_str(payload.get("id", "")),
            mechanism=_as_str(payload.get("mechanism", "")),
            target=_as_str(payload.get("target", "")),
            description=_as_str(payload.get("description", "")),
            linked_threshold_ids=_dedupe_strings(payload.get("linked_threshold_ids", ())),
        )


@dataclass(slots=True, frozen=True)
class SuccessCriterion(SerializableModel):
    id: str
    metric_key: str
    comparator: Comparator
    target: float
    description: str

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        return cls(
            id=_as_str(payload.get("id", "")),
            metric_key=_as_str(payload.get("metric_key", "")),
            comparator=Comparator(_as_str(payload.get("comparator", Comparator.LESS_THAN_OR_EQUAL.value))),
            target=float(payload.get("target", 0.0)),
            description=_as_str(payload.get("description", "")),
        )


def _combined_evidence(
    shared_refs: Sequence[EvidenceReference],
    local_refs: Sequence[Mapping[str, Any]] | None,
) -> tuple[EvidenceReference, ...]:
    local_models = [] if local_refs is None else [EvidenceReference.from_dict(item) for item in local_refs]
    combined = list(shared_refs)
    existing = {item.id for item in shared_refs}
    for item in local_models:
        if item.id not in existing:
            combined.append(item)
            existing.add(item.id)
    return tuple(combined)


def _local_provenance(shared: Provenance, payload: Mapping[str, Any]) -> Provenance:
    local = payload.get("provenance")
    return shared if local is None else Provenance.from_dict(_mapping(local))


def _record_metadata(
    payload: Mapping[str, Any],
    *,
    timestamp: str,
    provenance: Provenance,
    assumptions: Sequence[str],
    evidence_refs: Sequence[EvidenceReference],
    uncertainties: Sequence[str],
    engine_tag: EngineType,
) -> dict[str, Any]:
    return {
        "id": _as_str(payload.get("id", "")),
        "timestamp": _as_str(payload.get("timestamp", timestamp)),
        "provenance": _local_provenance(provenance, payload),
        "assumptions": _merge_strings(assumptions, payload.get("assumptions")),
        "evidence_refs": _combined_evidence(evidence_refs, payload.get("evidence_refs")),
        "uncertainties": _merge_strings(uncertainties, payload.get("uncertainties")),
        "engine_tags": (engine_tag,),
    }


@dataclass(slots=True, frozen=True)
class ResearchIntakeRecord(GovernedMetadata):
    type: str
    title: str
    source_kind: str
    portfolio_ref: str
    summary: str
    constraints: tuple[str, ...]
    architecture_drivers: tuple[str, ...]
    threshold_hypotheses: tuple[str, ...]
    open_questions: tuple[str, ...]

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        timestamp: str,
        provenance: Provenance,
        assumptions: Sequence[str],
        evidence_refs: Sequence[EvidenceReference],
        uncertainties: Sequence[str],
    ) -> Self:
        metadata = _record_metadata(
            payload,
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            engine_tag=EngineType.RESEARCH_INTAKE,
        )
        return cls(
            **metadata,
            type="research_intake_record",
            title=_as_str(payload.get("title", "")),
            source_kind=_as_str(payload.get("source_kind", "")),
            portfolio_ref=_as_str(payload.get("portfolio_ref", "")),
            summary=_as_str(payload.get("summary", "")),
            constraints=_dedupe_strings(payload.get("constraints", ())),
            architecture_drivers=_dedupe_strings(payload.get("architecture_drivers", ())),
            threshold_hypotheses=_dedupe_strings(payload.get("threshold_hypotheses", ())),
            open_questions=_dedupe_strings(payload.get("open_questions", ())),
        )


@dataclass(slots=True, frozen=True)
class StateVariableDefinition(GovernedMetadata):
    type: str
    name: str
    symbol: str
    metric_key: str
    unit: str
    description: str
    safe_operating_guidance: str
    failure_regime_indicators: tuple[str, ...]

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        timestamp: str,
        provenance: Provenance,
        assumptions: Sequence[str],
        evidence_refs: Sequence[EvidenceReference],
        uncertainties: Sequence[str],
        engine_tag: EngineType = EngineType.ARCHITECTURE_ARTIFACT_GENERATION,
    ) -> Self:
        metadata = _record_metadata(
            payload,
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            engine_tag=engine_tag,
        )
        return cls(
            **metadata,
            type="state_variable_definition",
            name=_as_str(payload.get("name", "")),
            symbol=_as_str(payload.get("symbol", "")),
            metric_key=_as_str(payload.get("metric_key", "")),
            unit=_as_str(payload.get("unit", "")),
            description=_as_str(payload.get("description", "")),
            safe_operating_guidance=_as_str(payload.get("safe_operating_guidance", "")),
            failure_regime_indicators=_dedupe_strings(payload.get("failure_regime_indicators", ())),
        )


@dataclass(slots=True, frozen=True)
class ThresholdRecord(GovernedMetadata):
    type: str
    variable_id: str
    metric_key: str
    comparator: Comparator
    warning_value: float | None
    breach_value: float
    rationale: str
    linked_hazard_ids: tuple[str, ...]

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        timestamp: str,
        provenance: Provenance,
        assumptions: Sequence[str],
        evidence_refs: Sequence[EvidenceReference],
        uncertainties: Sequence[str],
    ) -> Self:
        metadata = _record_metadata(
            payload,
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            engine_tag=EngineType.PHYSICS_THRESHOLD_TRACKING,
        )
        warning_value = payload.get("warning_value")
        return cls(
            **metadata,
            type="threshold_record",
            variable_id=_as_str(payload.get("variable_id", "")),
            metric_key=_as_str(payload.get("metric_key", "")),
            comparator=Comparator(_as_str(payload.get("comparator", Comparator.GREATER_THAN_OR_EQUAL.value))),
            warning_value=None if warning_value is None else float(warning_value),
            breach_value=float(payload.get("breach_value", 0.0)),
            rationale=_as_str(payload.get("rationale", "")),
            linked_hazard_ids=_dedupe_strings(payload.get("linked_hazard_ids", ())),
        )


@dataclass(slots=True, frozen=True)
class BlockerRecord(GovernedMetadata):
    type: str
    title: str
    severity: BlockerSeverity
    description: str
    affected_surfaces: tuple[str, ...]
    resolution_path: str

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        timestamp: str,
        provenance: Provenance,
        assumptions: Sequence[str],
        evidence_refs: Sequence[EvidenceReference],
        uncertainties: Sequence[str],
    ) -> Self:
        metadata = _record_metadata(
            payload,
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            engine_tag=EngineType.DEPLOYMENT_READINESS_MAPPING,
        )
        return cls(
            **metadata,
            type="blocker_record",
            title=_as_str(payload.get("title", "")),
            severity=BlockerSeverity(_as_str(payload.get("severity", BlockerSeverity.MEDIUM.value))),
            description=_as_str(payload.get("description", "")),
            affected_surfaces=_dedupe_strings(payload.get("affected_surfaces", ())),
            resolution_path=_as_str(payload.get("resolution_path", "")),
        )


@dataclass(slots=True, frozen=True)
class ScenarioDefinition(GovernedMetadata):
    type: str
    name: str
    stress_family: str
    description: str
    failure_injections: tuple[FailureInjection, ...]
    metric_specs: tuple[MetricSpecification, ...]
    success_criteria: tuple[SuccessCriterion, ...]
    linked_threshold_ids: tuple[str, ...]
    linked_hazard_ids: tuple[str, ...]

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        timestamp: str,
        provenance: Provenance,
        assumptions: Sequence[str],
        evidence_refs: Sequence[EvidenceReference],
        uncertainties: Sequence[str],
    ) -> Self:
        metadata = _record_metadata(
            payload,
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            engine_tag=EngineType.SCENARIO_STRESS_PLANNING,
        )
        return cls(
            **metadata,
            type="scenario_definition",
            name=_as_str(payload.get("name", "")),
            stress_family=_as_str(payload.get("stress_family", "")),
            description=_as_str(payload.get("description", "")),
            failure_injections=tuple(
                FailureInjection.from_dict(_mapping(item)) for item in payload.get("failure_injections", ())
            ),
            metric_specs=tuple(MetricSpecification.from_dict(_mapping(item)) for item in payload.get("metric_specs", ())),
            success_criteria=tuple(
                SuccessCriterion.from_dict(_mapping(item)) for item in payload.get("success_criteria", ())
            ),
            linked_threshold_ids=_dedupe_strings(payload.get("linked_threshold_ids", ())),
            linked_hazard_ids=_dedupe_strings(payload.get("linked_hazard_ids", ())),
        )


@dataclass(slots=True, frozen=True)
class HazardRecord(GovernedMetadata):
    type: str
    title: str
    severity: BlockerSeverity
    description: str
    trigger_conditions: tuple[str, ...]
    mitigations: tuple[str, ...]
    linked_threshold_ids: tuple[str, ...]
    linked_scenario_ids: tuple[str, ...]

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        timestamp: str,
        provenance: Provenance,
        assumptions: Sequence[str],
        evidence_refs: Sequence[EvidenceReference],
        uncertainties: Sequence[str],
    ) -> Self:
        metadata = _record_metadata(
            payload,
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            engine_tag=EngineType.SAFETY_HAZARD_LEDGER,
        )
        return cls(
            **metadata,
            type="hazard_record",
            title=_as_str(payload.get("title", "")),
            severity=BlockerSeverity(_as_str(payload.get("severity", BlockerSeverity.MEDIUM.value))),
            description=_as_str(payload.get("description", "")),
            trigger_conditions=_dedupe_strings(payload.get("trigger_conditions", ())),
            mitigations=_dedupe_strings(payload.get("mitigations", ())),
            linked_threshold_ids=_dedupe_strings(payload.get("linked_threshold_ids", ())),
            linked_scenario_ids=_dedupe_strings(payload.get("linked_scenario_ids", ())),
        )


@dataclass(slots=True, frozen=True)
class DeploymentStageRecord(GovernedMetadata):
    type: str
    stage: DeploymentStage
    readiness_state: ReadinessState
    entry_criteria: tuple[str, ...]
    exit_criteria: tuple[str, ...]
    blockers: tuple[BlockerRecord, ...]
    allowed_services: tuple[str, ...]
    notes: tuple[str, ...]

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        *,
        blockers_by_id: Mapping[str, BlockerRecord],
        timestamp: str,
        provenance: Provenance,
        assumptions: Sequence[str],
        evidence_refs: Sequence[EvidenceReference],
        uncertainties: Sequence[str],
    ) -> Self:
        metadata = _record_metadata(
            payload,
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            engine_tag=EngineType.DEPLOYMENT_READINESS_MAPPING,
        )
        blocker_ids = _dedupe_strings(payload.get("blocker_ids", ()))
        return cls(
            **metadata,
            type="deployment_stage_record",
            stage=DeploymentStage(_as_str(payload.get("stage", DeploymentStage.RESEARCH_BASELINE.value))),
            readiness_state=ReadinessState(_as_str(payload.get("readiness_state", ReadinessState.BLOCKED.value))),
            entry_criteria=_dedupe_strings(payload.get("entry_criteria", ())),
            exit_criteria=_dedupe_strings(payload.get("exit_criteria", ())),
            blockers=tuple(blockers_by_id[blocker_id] for blocker_id in blocker_ids if blocker_id in blockers_by_id),
            allowed_services=_dedupe_strings(payload.get("allowed_services", ())),
            notes=_dedupe_strings(payload.get("notes", ())),
        )


@dataclass(slots=True, frozen=True)
class PromotionDecision(GovernedMetadata):
    type: str
    subject_id: str
    subject_type: str
    target_stage: DeploymentStage | None
    approved: bool
    decision: str
    readiness_state: ReadinessState
    blocker_ids: tuple[str, ...]
    rationale: tuple[str, ...]
    required_actions: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class ResearchIntakeArtifact(GovernedMetadata):
    type: ArtifactType
    records: tuple[ResearchIntakeRecord, ...]
    constraint_summary: tuple[str, ...]
    architecture_driver_summary: tuple[str, ...]
    source_count: int
    open_question_count: int


@dataclass(slots=True, frozen=True)
class ArchitectureSummaryArtifact(GovernedMetadata):
    type: ArtifactType
    network_name: str
    topology_model: str
    service_priority: tuple[str, ...]
    node_roles: tuple[str, ...]
    corridor_policies: tuple[str, ...]
    governance_controls: tuple[str, ...]
    governance_first: bool
    simulation_scope: str
    free_flight_architecture: bool
    passenger_autonomy_assumed: bool
    state_variables: tuple[StateVariableDefinition, ...]
    constraint_links: tuple[str, ...]
    research_driver_summary: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class ThresholdLedgerArtifact(GovernedMetadata):
    type: ArtifactType
    state_variables: tuple[StateVariableDefinition, ...]
    thresholds: tuple[ThresholdRecord, ...]
    tracked_metric_keys: tuple[str, ...]
    failure_regime_focus: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class ScenarioCatalogArtifact(GovernedMetadata):
    type: ArtifactType
    scenarios: tuple[ScenarioDefinition, ...]
    covered_threshold_ids: tuple[str, ...]
    covered_hazard_ids: tuple[str, ...]
    covered_metric_keys: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class HazardLedgerArtifact(GovernedMetadata):
    type: ArtifactType
    hazards: tuple[HazardRecord, ...]
    high_severity_hazard_ids: tuple[str, ...]
    linked_scenario_ids: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class DeploymentStageProfileArtifact(GovernedMetadata):
    type: ArtifactType
    stages: tuple[DeploymentStageRecord, ...]
    blockers: tuple[BlockerRecord, ...]
    highest_ready_stage: str
    highest_readiness_state: ReadinessState


@dataclass(slots=True, frozen=True)
class PromotionDecisionsArtifact(GovernedMetadata):
    type: ArtifactType
    decisions: tuple[PromotionDecision, ...]
    blockers: tuple[BlockerRecord, ...]
    approved_stage_ids: tuple[str, ...]
    held_stage_ids: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class ArtifactManifestEntry(GovernedMetadata):
    type: str
    artifact_id: str
    artifact_type: ArtifactType
    path: str
    sha256: str
    record_count: int


@dataclass(slots=True, frozen=True)
class ArtifactManifestArtifact(GovernedMetadata):
    type: ArtifactType
    entries: tuple[ArtifactManifestEntry, ...]


@dataclass(slots=True, frozen=True)
class RunReceipt(GovernedMetadata):
    type: ArtifactType
    run_id: str
    status: str
    selected_engines: tuple[EngineType, ...]
    artifacts: tuple[ArtifactManifestEntry, ...]
    output_dir: str


@dataclass(slots=True, frozen=True)
class SkillPackRequest(SerializableModel):
    run_id: str
    timestamp: str
    provenance: Provenance
    assumptions: tuple[str, ...]
    evidence_refs: tuple[EvidenceReference, ...]
    uncertainties: tuple[str, ...]
    selected_engines: tuple[EngineType, ...]
    network_name: str
    topology_model: str
    service_priority: tuple[str, ...]
    node_roles: tuple[str, ...]
    corridor_policies: tuple[str, ...]
    governance_controls: tuple[str, ...]
    governance_first: bool
    simulation_scope: str
    free_flight_architecture: bool
    passenger_autonomy_assumed: bool
    research_records: tuple[ResearchIntakeRecord, ...]
    state_variables: tuple[StateVariableDefinition, ...]
    thresholds: tuple[ThresholdRecord, ...]
    scenarios: tuple[ScenarioDefinition, ...]
    hazards: tuple[HazardRecord, ...]
    blockers: tuple[BlockerRecord, ...]
    deployment_stages: tuple[DeploymentStageRecord, ...]
    decision_context: tuple[str, ...]

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        provenance = Provenance.from_dict(_mapping(payload.get("provenance")))
        evidence_refs = tuple(EvidenceReference.from_dict(item) for item in payload.get("evidence_refs", ()))
        assumptions = _dedupe_strings(payload.get("assumptions", ()))
        uncertainties = _dedupe_strings(payload.get("uncertainties", ()))
        timestamp = _as_str(payload.get("timestamp", ""))

        blockers = tuple(
            BlockerRecord.from_dict(
                _mapping(item),
                timestamp=timestamp,
                provenance=provenance,
                assumptions=assumptions,
                evidence_refs=evidence_refs,
                uncertainties=uncertainties,
            )
            for item in payload.get("blockers", ())
        )
        blockers_by_id = {blocker.id: blocker for blocker in blockers}

        return cls(
            run_id=_as_str(payload.get("run_id", "")),
            timestamp=timestamp,
            provenance=provenance,
            assumptions=assumptions,
            evidence_refs=evidence_refs,
            uncertainties=uncertainties,
            selected_engines=tuple(EngineType(_as_str(item)) for item in payload.get("selected_engines", ())),
            network_name=_as_str(payload.get("network_name", "")),
            topology_model=_as_str(payload.get("topology_model", "")),
            service_priority=_dedupe_strings(payload.get("service_priority", ())),
            node_roles=_dedupe_strings(payload.get("node_roles", ())),
            corridor_policies=_dedupe_strings(payload.get("corridor_policies", ())),
            governance_controls=_dedupe_strings(payload.get("governance_controls", ())),
            governance_first=bool(payload.get("governance_first", True)),
            simulation_scope=_as_str(payload.get("simulation_scope", "thresholds_and_failure_regimes")),
            free_flight_architecture=bool(payload.get("free_flight_architecture", False)),
            passenger_autonomy_assumed=bool(payload.get("passenger_autonomy_assumed", False)),
            research_records=tuple(
                ResearchIntakeRecord.from_dict(
                    _mapping(item),
                    timestamp=timestamp,
                    provenance=provenance,
                    assumptions=assumptions,
                    evidence_refs=evidence_refs,
                    uncertainties=uncertainties,
                )
                for item in payload.get("research_records", ())
            ),
            state_variables=tuple(
                StateVariableDefinition.from_dict(
                    _mapping(item),
                    timestamp=timestamp,
                    provenance=provenance,
                    assumptions=assumptions,
                    evidence_refs=evidence_refs,
                    uncertainties=uncertainties,
                )
                for item in payload.get("state_variables", ())
            ),
            thresholds=tuple(
                ThresholdRecord.from_dict(
                    _mapping(item),
                    timestamp=timestamp,
                    provenance=provenance,
                    assumptions=assumptions,
                    evidence_refs=evidence_refs,
                    uncertainties=uncertainties,
                )
                for item in payload.get("thresholds", ())
            ),
            scenarios=tuple(
                ScenarioDefinition.from_dict(
                    _mapping(item),
                    timestamp=timestamp,
                    provenance=provenance,
                    assumptions=assumptions,
                    evidence_refs=evidence_refs,
                    uncertainties=uncertainties,
                )
                for item in payload.get("scenarios", ())
            ),
            hazards=tuple(
                HazardRecord.from_dict(
                    _mapping(item),
                    timestamp=timestamp,
                    provenance=provenance,
                    assumptions=assumptions,
                    evidence_refs=evidence_refs,
                    uncertainties=uncertainties,
                )
                for item in payload.get("hazards", ())
            ),
            blockers=blockers,
            deployment_stages=tuple(
                DeploymentStageRecord.from_dict(
                    _mapping(item),
                    blockers_by_id=blockers_by_id,
                    timestamp=timestamp,
                    provenance=provenance,
                    assumptions=assumptions,
                    evidence_refs=evidence_refs,
                    uncertainties=uncertainties,
                )
                for item in payload.get("deployment_stages", ())
            ),
            decision_context=_dedupe_strings(payload.get("decision_context", ())),
        )
