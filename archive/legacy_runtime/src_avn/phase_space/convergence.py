from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from avn.phase_space.models import PhaseRegion


@dataclass(slots=True)
class ConvergenceIteration:
    iteration: int
    new_slice_count: int
    cumulative_slice_count: int
    transition_region_count: int
    max_entropy: float
    boundary_shift: float
    classification_stability: float
    converged: bool

    def to_dict(self) -> dict[str, float | int | bool]:
        return {
            "iteration": self.iteration,
            "new_slice_count": self.new_slice_count,
            "cumulative_slice_count": self.cumulative_slice_count,
            "transition_region_count": self.transition_region_count,
            "max_entropy": round(self.max_entropy, 12),
            "boundary_shift": round(self.boundary_shift, 12),
            "classification_stability": round(self.classification_stability, 12),
            "converged": self.converged,
        }


def _region_signature(region: PhaseRegion) -> tuple[object, ...]:
    fixed_bounds = tuple(
        sorted(
            (axis, lower, upper)
            for axis, (lower, upper) in region.bounds.items()
            if axis != region.transition_axis
        )
    )
    return (
        region.transition_axis,
        fixed_bounds,
        tuple(sorted(region.fixed_context.items())),
    )


def _region_center(region: PhaseRegion) -> float:
    if region.transition_axis is None:
        return 0.0
    if region.estimated_threshold is not None:
        return region.estimated_threshold
    lower, upper = region.bounds[region.transition_axis]
    return (lower + upper) / 2.0


def _compare_regions(previous: Sequence[PhaseRegion], current: Sequence[PhaseRegion]) -> tuple[float, float]:
    if not previous and not current:
        return 0.0, 1.0
    if not previous or not current:
        return 1.0, 0.0

    previous_by_signature = {_region_signature(region): region for region in previous}
    current_by_signature = {_region_signature(region): region for region in current}
    signatures = sorted(set(previous_by_signature) | set(current_by_signature), key=str)

    if not signatures:
        return 0.0, 1.0

    shift_values: list[float] = []
    matched = 0
    stable = 0
    for signature in signatures:
        previous_region = previous_by_signature.get(signature)
        current_region = current_by_signature.get(signature)
        if previous_region is None or current_region is None:
            unmatched_region = current_region or previous_region
            if unmatched_region is None:
                shift_values.append(0.0)
            else:
                shift_values.append(min(1.0, unmatched_region.entropy))
            continue

        matched += 1
        axis_span = max(previous_region.axis_total_span or 0.0, current_region.axis_total_span or 0.0, 1e-9)
        shift_values.append(abs(_region_center(current_region) - _region_center(previous_region)) / axis_span)
        if previous_region.dominant_mechanism == current_region.dominant_mechanism:
            stable += 1

    classification_stability = stable / matched if matched else 0.0
    return max(shift_values, default=0.0), classification_stability


def build_convergence_report(
    iteration_regions: Sequence[Sequence[PhaseRegion]],
    *,
    convergence_threshold: float,
    iteration_slice_counts: Sequence[int],
    new_slice_counts: Sequence[int],
    adaptive_enabled: bool,
    max_iterations: int | None = None,
) -> dict[str, object]:
    reports: list[ConvergenceIteration] = []
    previous_regions: Sequence[PhaseRegion] = ()
    converged = False
    stopping_reason = "insufficient_iterations"

    for index, current_regions in enumerate(iteration_regions):
        max_entropy = max((region.entropy for region in current_regions), default=0.0)
        if index == 0:
            boundary_shift = 1.0 if current_regions else 0.0
            classification_stability = 0.0 if current_regions else 1.0
        else:
            boundary_shift, classification_stability = _compare_regions(previous_regions, current_regions)
        iteration_converged = max_entropy < convergence_threshold and boundary_shift < convergence_threshold
        reports.append(
            ConvergenceIteration(
                iteration=index,
                new_slice_count=new_slice_counts[index],
                cumulative_slice_count=iteration_slice_counts[index],
                transition_region_count=len(current_regions),
                max_entropy=max_entropy,
                boundary_shift=boundary_shift,
                classification_stability=classification_stability,
                converged=iteration_converged,
            )
        )
        previous_regions = current_regions
        if iteration_converged:
            converged = True
            stopping_reason = "converged"
            break

    if not converged and reports:
        final_iteration = reports[-1].iteration
        if max_iterations is not None and final_iteration + 1 >= max_iterations:
            stopping_reason = "max_iterations_reached"
        else:
            stopping_reason = "transition_regions_persist"

    return {
        "adaptive_enabled": adaptive_enabled,
        "converged": converged,
        "convergence_threshold": convergence_threshold,
        "max_iterations": max_iterations,
        "iterations": [report.to_dict() for report in reports],
        "stopping_reason": stopping_reason,
    }
