from __future__ import annotations

import copy
from dataclasses import dataclass
import math
from itertools import product
from typing import TYPE_CHECKING, Callable

from avn.phase_space.convergence import build_convergence_report
from avn.phase_space.models import phase_points_from_slice_results
from avn.phase_space.transitions import detect_transition_regions
from avn.sweep_tranches import SweepAxis, TrancheDefinition, TrancheSlice, build_tranche_slice

if TYPE_CHECKING:
    from avn.sweep_analysis import TrancheSliceResult


SliceExecutor = Callable[[TrancheDefinition, TrancheSlice], "TrancheSliceResult"]


@dataclass(slots=True)
class AdaptiveSweepRun:
    slice_results: list[TrancheSliceResult]
    adaptive_payload: dict[str, object]


def _is_refineable_axis(axis: SweepAxis) -> bool:
    return all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in axis.values)


def _axis_numeric_bounds(axis: SweepAxis) -> tuple[float, float]:
    numeric_values = [float(value) for value in axis.values]
    return min(numeric_values), max(numeric_values)


def _axis_midpoint_value(axis: SweepAxis, lower: float, upper: float) -> object:
    midpoint = round((lower + upper) / 2.0, 10)
    if all(isinstance(value, int) and not isinstance(value, bool) for value in axis.values):
        rounded = round(midpoint)
        if math.isclose(midpoint, rounded):
            return int(rounded)
    return midpoint


def _restore_axis_value(axis: SweepAxis, value: float) -> object:
    if all(isinstance(item, bool) for item in axis.values):
        return bool(round(value))
    if all(isinstance(item, int) and not isinstance(item, bool) for item in axis.values):
        rounded = round(value)
        if math.isclose(value, rounded):
            return int(rounded)
    return round(value, 10)


def _refineable_axes(tranche: TrancheDefinition) -> tuple[SweepAxis, ...]:
    return tuple(axis for axis in tranche.sweep_axes if _is_refineable_axis(axis))


def _discrete_axes(tranche: TrancheDefinition) -> tuple[SweepAxis, ...]:
    return tuple(axis for axis in tranche.sweep_axes if not _is_refineable_axis(axis))


def _coarse_parameter_sets(tranche: TrancheDefinition) -> list[dict[str, object]]:
    refineable_axes = _refineable_axes(tranche)
    discrete_axes = _discrete_axes(tranche)

    value_sets: list[tuple[object, ...]] = []
    for axis in tranche.sweep_axes:
        if _is_refineable_axis(axis):
            lower, upper = _axis_numeric_bounds(axis)
            if math.isclose(lower, upper):
                value_sets.append((_restore_axis_value(axis, lower),))
            else:
                value_sets.append((_restore_axis_value(axis, lower), _restore_axis_value(axis, upper)))
        else:
            value_sets.append(axis.values)

    parameter_sets: list[dict[str, object]] = []
    seen_signatures: set[tuple[tuple[str, object], ...]] = set()
    for combination in product(*value_sets):
        resolved_params = copy.deepcopy(tranche.fixed_params)
        for axis, value in zip(tranche.sweep_axes, combination, strict=True):
            resolved_params[axis.name] = value
        signature = tuple(sorted(resolved_params.items()))
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        parameter_sets.append(resolved_params)

    if refineable_axes:
        discrete_values = [axis.values for axis in discrete_axes]
        discrete_combinations = product(*discrete_values) if discrete_values else [()]
        for discrete_combination in discrete_combinations:
            resolved_params = copy.deepcopy(tranche.fixed_params)
            for axis in refineable_axes:
                lower, upper = _axis_numeric_bounds(axis)
                resolved_params[axis.name] = _axis_midpoint_value(axis, lower, upper)
            for axis, value in zip(discrete_axes, discrete_combination, strict=True):
                resolved_params[axis.name] = value
            signature = tuple(sorted(resolved_params.items()))
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            parameter_sets.append(resolved_params)

    return parameter_sets


def _parameter_set_from_region(tranche: TrancheDefinition, region) -> dict[str, object] | None:
    if region.transition_axis is None:
        return None
    axis_lookup = {axis.name: axis for axis in tranche.sweep_axes}
    if region.transition_axis not in axis_lookup:
        return None
    transition_axis = axis_lookup[region.transition_axis]
    if not _is_refineable_axis(transition_axis):
        return None

    resolved_params = copy.deepcopy(tranche.fixed_params)
    for axis in tranche.sweep_axes:
        if axis.name == region.transition_axis:
            lower, upper = region.bounds[axis.name]
            resolved_params[axis.name] = _axis_midpoint_value(axis, lower, upper)
            continue
        if axis.name in region.fixed_context:
            resolved_params[axis.name] = region.fixed_context[axis.name]
            continue
        bounds = region.bounds.get(axis.name)
        if bounds is None:
            if axis.values:
                resolved_params[axis.name] = axis.values[0]
            continue
        resolved_params[axis.name] = _restore_axis_value(axis, bounds[0])
    return resolved_params


def _refinement_parameter_sets(
    tranche: TrancheDefinition,
    transition_regions,
    *,
    executed_slice_ids: set[str],
    limit: int,
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for region in sorted(
        transition_regions,
        key=lambda item: (
            item.entropy,
            item.local_disagreement,
            item.local_gradient,
            item.transition_axis or "",
        ),
        reverse=True,
    ):
        resolved_params = _parameter_set_from_region(tranche, region)
        if resolved_params is None:
            continue
        candidate_slice = build_tranche_slice(tranche, resolved_params)
        if candidate_slice.slice_id in executed_slice_ids:
            continue
        candidates.append(resolved_params)
        if len(candidates) >= limit:
            break
    return candidates


def adaptive_sweep(
    tranche_spec: TrancheDefinition,
    *,
    execute_slice: SliceExecutor,
    max_iterations: int = 8,
    convergence_threshold: float = 0.3,
    max_slices: int | None = None,
) -> AdaptiveSweepRun:
    pending_params = _coarse_parameter_sets(tranche_spec)
    effective_max_slices = max_slices
    if effective_max_slices is None:
        effective_max_slices = tranche_spec.slice_count
    effective_max_slices = max(effective_max_slices, tranche_spec.minimum_slice_count)
    executed_results: dict[str, TrancheSliceResult] = {}
    iteration_records: list[dict[str, object]] = []
    iteration_regions = []
    cumulative_counts: list[int] = []
    new_counts: list[int] = []
    stopping_reason = "max_iterations_reached"

    for iteration in range(max_iterations):
        new_slices = [
            build_tranche_slice(tranche_spec, resolved_params)
            for resolved_params in pending_params
            if build_tranche_slice(tranche_spec, resolved_params).slice_id not in executed_results
        ]
        if effective_max_slices is not None:
            remaining_budget = effective_max_slices - len(executed_results)
            if remaining_budget <= 0:
                stopping_reason = "max_slices_reached"
                break
            new_slices = new_slices[:remaining_budget]
        if not new_slices:
            stopping_reason = "no_new_slices"
            break

        new_results = [
            execute_slice(tranche_spec, slice_definition)
            for slice_definition in new_slices
        ]
        for result in new_results:
            executed_results[result.slice_id] = result

        cumulative_results = [executed_results[slice_id] for slice_id in sorted(executed_results)]
        phase_points = phase_points_from_slice_results(cumulative_results)
        transition_regions = detect_transition_regions(phase_points)

        iteration_records.append(
            {
                "iteration": iteration,
                "executed_slice_ids": [slice_definition.slice_id for slice_definition in new_slices],
                "transition_region_count": len(transition_regions),
                "transition_region_replay_hashes": [
                    region.replay_hash
                    for region in transition_regions
                    if region.replay_hash is not None
                ],
            }
        )
        iteration_regions.append(transition_regions)
        cumulative_counts.append(len(cumulative_results))
        new_counts.append(len(new_results))

        convergence_report = build_convergence_report(
            iteration_regions,
            convergence_threshold=convergence_threshold,
            iteration_slice_counts=cumulative_counts,
            new_slice_counts=new_counts,
            adaptive_enabled=True,
            max_iterations=max_iterations,
        )
        latest_iteration = convergence_report["iterations"][-1]
        if bool(latest_iteration["converged"]):
            stopping_reason = "converged"
            break

        pending_params = _refinement_parameter_sets(
            tranche_spec,
            transition_regions,
            executed_slice_ids=set(executed_results),
            limit=max(1, len(_refineable_axes(tranche_spec))),
        )
        iteration_records[-1]["selected_refinement_slice_ids"] = [
            build_tranche_slice(tranche_spec, resolved_params).slice_id
            for resolved_params in pending_params
        ]
        if not pending_params:
            stopping_reason = "no_refinement_candidates"
            break

    final_results = [executed_results[slice_id] for slice_id in sorted(executed_results)]
    adaptive_payload = {
        "enabled": True,
        "initial_strategy": "axis_extrema_plus_center",
        "max_iterations": max_iterations,
        "convergence_threshold": convergence_threshold,
        "max_slices": effective_max_slices,
        "stopping_reason": stopping_reason,
        "iterations": iteration_records,
    }
    return AdaptiveSweepRun(
        slice_results=final_results,
        adaptive_payload=adaptive_payload,
    )
