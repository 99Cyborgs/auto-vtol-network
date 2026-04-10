from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from hashlib import sha256
from itertools import product
from pathlib import Path


ScalarParam = str | int | float | bool
ParamValue = ScalarParam | dict[str, ScalarParam]


@dataclass(slots=True)
class SeedPolicy:
    base_seed: int
    strategy: str = "slice_id_sha256"
    replicates: int = 3

    def to_dict(self) -> dict[str, str | int]:
        return {
            "base_seed": self.base_seed,
            "strategy": self.strategy,
            "replicates": self.replicates,
        }


@dataclass(slots=True)
class SweepAxis:
    name: str
    values: tuple[ScalarParam, ...]

    def __post_init__(self) -> None:
        self.values = tuple(self.values)
        if not self.values:
            raise ValueError(f"Sweep axis '{self.name}' must expose at least one value")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "values": list(self.values),
        }


@dataclass(slots=True)
class TrancheDefinition:
    tranche_name: str
    description: str
    base_config_path: Path
    fixed_params: dict[str, ParamValue]
    sweep_axes: tuple[SweepAxis, ...]
    expected_metrics: tuple[str, ...]
    expected_failure_modes: tuple[str, ...]
    seed_policy: SeedPolicy
    dominant_axis: str | None = None
    minimum_slice_count: int = 1

    def __post_init__(self) -> None:
        self.base_config_path = Path(self.base_config_path).resolve()
        self.sweep_axes = tuple(self.sweep_axes)
        self.expected_metrics = tuple(self.expected_metrics)
        self.expected_failure_modes = tuple(self.expected_failure_modes)
        if len(self.sweep_axes) != 1:
            raise ValueError(
                f"Tranche '{self.tranche_name}' is not isolatable: expected exactly one sweep axis, "
                f"found {len(self.sweep_axes)}"
            )
        if self.dominant_axis is None:
            self.dominant_axis = self.sweep_axes[0].name
        if self.sweep_axes[0].name != self.dominant_axis:
            raise ValueError(
                f"Tranche '{self.tranche_name}' dominant axis '{self.dominant_axis}' does not match "
                f"configured sweep axis '{self.sweep_axes[0].name}'"
            )
        if self.minimum_slice_count <= 0:
            raise ValueError("minimum_slice_count must be positive")

    @property
    def slice_count(self) -> int:
        total = 1
        for axis in self.sweep_axes:
            total *= len(axis.values)
        return total

    def to_dict(self) -> dict[str, object]:
        return {
            "tranche_name": self.tranche_name,
            "description": self.description,
            "base_config_path": str(self.base_config_path),
            "fixed_params": copy.deepcopy(self.fixed_params),
            "sweep_axes": [axis.to_dict() for axis in self.sweep_axes],
            "expected_metrics": list(self.expected_metrics),
            "expected_failure_modes": list(self.expected_failure_modes),
            "seed_policy": self.seed_policy.to_dict(),
            "dominant_axis": self.dominant_axis,
            "minimum_slice_count": self.minimum_slice_count,
            "slice_count": self.slice_count,
        }


@dataclass(slots=True)
class TrancheSlice:
    slice_id: str
    tranche_name: str
    seed: int
    resolved_params: dict[str, ParamValue]
    base_config_path: Path

    def __post_init__(self) -> None:
        self.base_config_path = Path(self.base_config_path).resolve()

    def to_dict(self) -> dict[str, object]:
        return {
            "slice_id": self.slice_id,
            "tranche_name": self.tranche_name,
            "seed": self.seed,
            "resolved_params": copy.deepcopy(self.resolved_params),
            "base_config_path": str(self.base_config_path),
        }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _stable_slice_id(tranche_name: str, resolved_params: dict[str, ParamValue]) -> str:
    canonical_payload = json.dumps(
        {
            "tranche_name": tranche_name,
            "resolved_params": resolved_params,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = sha256(canonical_payload.encode("utf-8")).hexdigest()[:12]
    return f"{tranche_name}_{digest}"


def _stable_seed_from_slice_id(slice_id: str, seed_policy: SeedPolicy) -> int:
    digest = sha256(f"{seed_policy.strategy}:{slice_id}".encode("utf-8")).digest()
    hashed_value = int.from_bytes(digest[:4], "big")
    modulus = 2_147_483_647
    return (seed_policy.base_seed + hashed_value) % modulus


def build_tranche_slice(
    tranche: TrancheDefinition,
    resolved_params: dict[str, ParamValue],
) -> TrancheSlice:
    canonical_params = copy.deepcopy(resolved_params)
    slice_id = _stable_slice_id(tranche.tranche_name, canonical_params)
    seed = _stable_seed_from_slice_id(slice_id, tranche.seed_policy)
    return TrancheSlice(
        slice_id=slice_id,
        tranche_name=tranche.tranche_name,
        seed=seed,
        resolved_params=canonical_params,
        base_config_path=tranche.base_config_path,
    )


def generate_tranche_slices(
    tranche: TrancheDefinition,
    *,
    max_slices: int | None = None,
) -> list[TrancheSlice]:
    if max_slices is not None and max_slices < tranche.minimum_slice_count:
        raise ValueError(
            f"Tranche '{tranche.tranche_name}' requires at least {tranche.minimum_slice_count} slices "
            f"for admissible evidence; received max_slices={max_slices}"
        )
    axes = tranche.sweep_axes
    slices: list[TrancheSlice] = []

    for axis_values in product(*(axis.values for axis in axes)):
        resolved_params: dict[str, ParamValue] = copy.deepcopy(tranche.fixed_params)
        for axis, value in zip(axes, axis_values, strict=True):
            resolved_params[axis.name] = value

        slices.append(build_tranche_slice(tranche, resolved_params))

        if max_slices is not None and len(slices) >= max_slices:
            break

    return slices


def get_built_in_tranches() -> dict[str, TrancheDefinition]:
    configs = _repo_root() / "configs"
    common_metrics = (
        "first_dominant_failure_mechanism",
        "dominant_failure_mode",
        "first_safe_region_exit_time",
        "peak_corridor_load_ratio",
        "peak_node_utilization_ratio",
        "peak_queue_ratio",
        "stale_state_exposure_minutes",
        "unsafe_admission_count",
        "no_admissible_landing_events",
        "phase_detection",
        "physics_summary",
        "admissibility_summary",
    )

    return {
        "load": TrancheDefinition(
            tranche_name="load",
            description="Isolated load and service stress to separate corridor exceedance from node collapse.",
            base_config_path=configs / "dense_weather.toml",
            fixed_params={
                "scenario.clear_trust_events": True,
                "scenario.clear_infrastructure_events": True,
                "scenario.clear_vehicle_injections": True,
                "modifiers.weather_multiplier": 1.0,
                "modifiers.separation_multiplier": 1.0,
                "modifiers.reserve_consumption_multiplier": 1.0,
                "modifiers.comms_reliability_multiplier": 1.0,
                "modifiers.latency_multiplier": 1.0,
                "modifiers.drop_probability_multiplier": 1.0,
                "modifiers.contingency_capacity_multiplier": 1.0,
                "modifiers.closure_probability": 0.0,
                "safe_region.max_corridor_load_ratio": 1.10,
                "disturbance.weather_severity": 0.10,
                "disturbance.comms_reliability": 0.98,
                "disturbance.comms_latency_minutes": 1.0,
                "disturbance.message_drop_probability": 0.0,
                "disturbance.stale_after_minutes": 12.0,
                "disturbance.reroute_delay_minutes": 2.0,
                "disturbance.low_bandwidth_threshold_minutes": 18.0,
                "disturbance.node_service_multiplier": 1.0,
            },
            sweep_axes=(
                SweepAxis("modifiers.demand_multiplier", (0.8, 1.0, 1.2, 1.4, 1.6)),
            ),
            expected_metrics=common_metrics,
            expected_failure_modes=(
                "CORRIDOR_CONGESTION",
                "NODE_SATURATION",
            ),
            seed_policy=SeedPolicy(base_seed=1201, replicates=3),
            dominant_axis="modifiers.demand_multiplier",
            minimum_slice_count=3,
        ),
        "comms": TrancheDefinition(
            tranche_name="comms",
            description="Latency, packet loss, and freshness sweeps that isolate stale-state instability.",
            base_config_path=configs / "stale_state_nominal_load.toml",
            fixed_params={
                "scenario.clear_trust_events": True,
                "scenario.clear_infrastructure_events": True,
                "scenario.clear_vehicle_injections": True,
                "modifiers.demand_multiplier": 0.9,
                "modifiers.weather_multiplier": 1.0,
                "modifiers.corridor_capacity_multiplier": 1.0,
                "modifiers.node_service_multiplier": 1.0,
                "modifiers.separation_multiplier": 1.0,
                "modifiers.reserve_consumption_multiplier": 1.0,
                "modifiers.comms_reliability_multiplier": 1.0,
                "modifiers.latency_multiplier": 1.0,
                "modifiers.drop_probability_multiplier": 1.0,
                "modifiers.contingency_capacity_multiplier": 1.0,
                "modifiers.closure_probability": 0.0,
                "disturbance.weather_severity": 0.10,
                "disturbance.comms_latency_minutes": 6.0,
                "disturbance.message_drop_probability": 0.12,
                "disturbance.reroute_delay_minutes": 8.0,
                "disturbance.low_bandwidth_threshold_minutes": 12.0,
                "disturbance.node_service_multiplier": 1.0,
            },
            sweep_axes=(
                SweepAxis("disturbance.comms_reliability", (0.98, 0.90, 0.82, 0.74, 0.66)),
            ),
            expected_metrics=common_metrics,
            expected_failure_modes=(
                "COMMS_FAILURE",
                "REROUTE_CASCADE",
            ),
            seed_policy=SeedPolicy(base_seed=1202, replicates=3),
            dominant_axis="disturbance.comms_reliability",
            minimum_slice_count=3,
        ),
        "trust": TrancheDefinition(
            tranche_name="trust",
            description="Compromise ratio, revocation delay, and privilege downgrade sweeps focused on trust breakdown.",
            base_config_path=configs / "supplier_compromise.toml",
            fixed_params={
                "scenario.clear_trust_events": True,
                "scenario.clear_infrastructure_events": True,
                "scenario.clear_vehicle_injections": True,
                "modifiers.demand_multiplier": 0.95,
                "modifiers.weather_multiplier": 1.0,
                "modifiers.corridor_capacity_multiplier": 1.0,
                "modifiers.node_service_multiplier": 1.0,
                "modifiers.separation_multiplier": 1.0,
                "modifiers.reserve_consumption_multiplier": 1.0,
                "modifiers.comms_reliability_multiplier": 1.0,
                "modifiers.latency_multiplier": 1.0,
                "modifiers.drop_probability_multiplier": 1.0,
                "modifiers.contingency_capacity_multiplier": 1.0,
                "modifiers.closure_probability": 0.0,
                "disturbance.weather_severity": 0.12,
                "disturbance.comms_reliability": 0.95,
                "disturbance.comms_latency_minutes": 2.0,
                "disturbance.message_drop_probability": 0.03,
                "disturbance.stale_after_minutes": 12.0,
                "disturbance.reroute_delay_minutes": 6.0,
                "disturbance.low_bandwidth_threshold_minutes": 18.0,
                "disturbance.node_service_multiplier": 1.0,
                "trust.revocation_delay_minutes": 30,
                "trust.compromise_state": "degraded",
            },
            sweep_axes=(
                SweepAxis("trust.compromised_participant_ratio", (0.0, 0.10, 0.20, 0.35, 0.50)),
            ),
            expected_metrics=common_metrics,
            expected_failure_modes=(
                "TRUST_FAILURE",
            ),
            seed_policy=SeedPolicy(base_seed=1204, replicates=3),
            dominant_axis="trust.compromised_participant_ratio",
            minimum_slice_count=3,
        ),
        "contingency": TrancheDefinition(
            tranche_name="contingency",
            description="Landing density and reserve-margin sweeps that isolate contingency reachability collapse.",
            base_config_path=configs / "emergency_pad_outage.toml",
            fixed_params={
                "scenario.clear_trust_events": True,
                "scenario.clear_infrastructure_events": True,
                "scenario.clear_vehicle_injections": True,
                "modifiers.demand_multiplier": 0.95,
                "modifiers.weather_multiplier": 1.0,
                "modifiers.corridor_capacity_multiplier": 1.0,
                "modifiers.node_service_multiplier": 1.0,
                "modifiers.separation_multiplier": 1.0,
                "modifiers.reserve_consumption_multiplier": 1.10,
                "modifiers.comms_reliability_multiplier": 1.0,
                "modifiers.latency_multiplier": 1.0,
                "modifiers.drop_probability_multiplier": 1.0,
                "modifiers.closure_probability": 0.0,
                "disturbance.weather_severity": 0.18,
                "disturbance.comms_reliability": 0.96,
                "disturbance.comms_latency_minutes": 2.0,
                "disturbance.message_drop_probability": 0.02,
                "disturbance.stale_after_minutes": 12.0,
                "disturbance.reroute_delay_minutes": 6.0,
                "disturbance.low_bandwidth_threshold_minutes": 18.0,
                "disturbance.node_service_multiplier": 1.0,
                "contingency.slot_capacity_multiplier": 0.60,
                "vehicles.min_contingency_margin": 16.0,
                "contingency.node_impairment_severity": 0.70,
            },
            sweep_axes=(
                SweepAxis("contingency.emergency_pad_density", (1.0, 0.80, 0.60, 0.40, 0.20)),
            ),
            expected_metrics=common_metrics,
            expected_failure_modes=(
                "NODE_SATURATION",
            ),
            seed_policy=SeedPolicy(base_seed=1206, replicates=3),
            dominant_axis="contingency.emergency_pad_density",
            minimum_slice_count=3,
        ),
        "weather": TrancheDefinition(
            tranche_name="weather",
            description="Weather severity and closure sweeps that expose environmental phase transitions.",
            base_config_path=configs / "dense_weather.toml",
            fixed_params={
                "scenario.clear_trust_events": True,
                "scenario.clear_infrastructure_events": True,
                "scenario.clear_vehicle_injections": True,
                "modifiers.demand_multiplier": 0.9,
                "modifiers.node_service_multiplier": 1.0,
                "modifiers.separation_multiplier": 1.0,
                "modifiers.reserve_consumption_multiplier": 1.0,
                "modifiers.comms_reliability_multiplier": 1.0,
                "modifiers.latency_multiplier": 1.0,
                "modifiers.drop_probability_multiplier": 1.0,
                "modifiers.contingency_capacity_multiplier": 1.0,
                "modifiers.closure_probability": 0.15,
                "modifiers.corridor_capacity_multiplier": 0.85,
                "disturbance.comms_reliability": 0.96,
                "disturbance.comms_latency_minutes": 2.0,
                "disturbance.message_drop_probability": 0.03,
                "disturbance.stale_after_minutes": 12.0,
                "disturbance.reroute_delay_minutes": 6.0,
                "disturbance.low_bandwidth_threshold_minutes": 18.0,
                "disturbance.node_service_multiplier": 1.0,
            },
            sweep_axes=(
                SweepAxis("disturbance.weather_severity", (0.10, 0.25, 0.40, 0.55, 0.70)),
            ),
            expected_metrics=common_metrics,
            expected_failure_modes=(
                "WEATHER_COLLAPSE",
                "CORRIDOR_CONGESTION",
            ),
            seed_policy=SeedPolicy(base_seed=1201, replicates=3),
            dominant_axis="disturbance.weather_severity",
            minimum_slice_count=3,
        ),
        "coupled": TrancheDefinition(
            tranche_name="coupled",
            description="Moderate multi-mechanism combinations to compare isolated and mixed-stress failure ordering.",
            base_config_path=configs / "trust_and_comms_compound.toml",
            fixed_params={
                "scenario.clear_trust_events": True,
                "scenario.clear_infrastructure_events": True,
                "scenario.clear_vehicle_injections": True,
                "modifiers.demand_multiplier": 1.0,
                "modifiers.weather_multiplier": 1.0,
                "modifiers.corridor_capacity_multiplier": 1.0,
                "modifiers.node_service_multiplier": 1.0,
                "modifiers.separation_multiplier": 1.0,
                "modifiers.reserve_consumption_multiplier": 1.0,
                "modifiers.comms_reliability_multiplier": 1.0,
                "modifiers.latency_multiplier": 1.0,
                "modifiers.drop_probability_multiplier": 1.0,
                "modifiers.contingency_capacity_multiplier": 1.0,
                "modifiers.closure_probability": 0.0,
                "disturbance.weather_severity": 0.12,
                "disturbance.comms_reliability": 0.96,
                "disturbance.comms_latency_minutes": 2.0,
                "disturbance.message_drop_probability": 0.03,
                "disturbance.stale_after_minutes": 12.0,
                "disturbance.reroute_delay_minutes": 6.0,
                "disturbance.low_bandwidth_threshold_minutes": 18.0,
                "disturbance.node_service_multiplier": 1.0,
            },
            sweep_axes=(
                SweepAxis("coupled.compound_level", (0.0, 0.35, 0.70, 1.0)),
            ),
            expected_metrics=common_metrics,
            expected_failure_modes=(
                "CORRIDOR_CONGESTION",
                "NODE_SATURATION",
                "COMMS_FAILURE",
                "TRUST_FAILURE",
                "WEATHER_COLLAPSE",
                "REROUTE_CASCADE",
            ),
            seed_policy=SeedPolicy(base_seed=1209, replicates=3),
            dominant_axis="coupled.compound_level",
            minimum_slice_count=3,
        ),
    }


BUILT_IN_TRANCHES = get_built_in_tranches()


def list_tranches() -> tuple[TrancheDefinition, ...]:
    return tuple(BUILT_IN_TRANCHES[name] for name in sorted(BUILT_IN_TRANCHES))


def get_tranche(name: str) -> TrancheDefinition:
    try:
        return BUILT_IN_TRANCHES[name]
    except KeyError as exc:
        available = ", ".join(sorted(BUILT_IN_TRANCHES))
        raise KeyError(f"Unknown tranche '{name}'. Available tranches: {available}") from exc
