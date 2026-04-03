from __future__ import annotations

from collections import Counter, defaultdict
from hashlib import sha256
import json
import math
from typing import Iterable, Sequence

from avn.phase_space.models import PhasePoint, PhaseRegion, phase_context


GRADIENT_METRICS = (
    "rho_c",
    "lambda_c",
    "gamma_c",
    "w_c",
    "peak_corridor_load_ratio",
    "peak_node_utilization_ratio",
    "peak_queue_ratio",
    "stale_state_exposure_minutes",
    "trusted_active_fraction",
    "contingency_saturation_duration",
    "rho_proxy",
    "lambda_proxy",
    "gamma_proxy",
    "chi_proxy",
)


def compute_entropy(region: PhaseRegion | dict[str, object] | Iterable[str]) -> float:
    if isinstance(region, PhaseRegion):
        counts = Counter(region.mechanism_counts or {region.dominant_mechanism: 1})
    elif isinstance(region, dict):
        raw_counts = region.get("mechanism_counts")
        if isinstance(raw_counts, dict):
            counts = Counter({str(key): int(value) for key, value in raw_counts.items()})
        else:
            labels = region.get("labels", [])
            counts = Counter(str(label) for label in labels)
    else:
        counts = Counter(str(label) for label in region)

    total = sum(counts.values())
    if total <= 0:
        return 0.0
    return -sum(
        (count / total) * math.log2(count / total)
        for count in counts.values()
        if count > 0
    )


def estimate_local_gradient(points: Sequence[PhasePoint]) -> float:
    if len(points) < 2:
        return 0.0

    gradients: list[float] = []
    for left, right in zip(points, points[1:]):
        shared_axes = sorted(set(left.parameters) & set(right.parameters))
        if not shared_axes:
            continue
        distance = math.sqrt(
            sum((right.parameters[axis] - left.parameters[axis]) ** 2 for axis in shared_axes)
        )
        if distance <= 0.0:
            continue

        metric_deltas: list[float] = []
        for metric in GRADIENT_METRICS:
            left_value = left.metrics.get(metric)
            right_value = right.metrics.get(metric)
            if not isinstance(left_value, (int, float)) or not isinstance(right_value, (int, float)):
                continue
            denominator = max(abs(float(left_value)), abs(float(right_value)), 1.0)
            metric_deltas.append(abs(float(right_value) - float(left_value)) / denominator)
        if not metric_deltas:
            continue
        gradients.append(sum(metric_deltas) / len(metric_deltas) / distance)

    if not gradients:
        return 0.0
    return sum(gradients) / len(gradients)


def _contiguous_support_count(points: Sequence[PhasePoint], *, start_index: int, step: int) -> int:
    if not points:
        return 0
    anchor = points[start_index].mechanism
    count = 0
    index = start_index
    while 0 <= index < len(points) and points[index].mechanism == anchor:
        count += 1
        index += step
    return count


def _phase_consistency(points: Sequence[PhasePoint]) -> float:
    if len(points) < 2:
        return 1.0
    change_count = sum(
        1
        for previous, current in zip(points, points[1:])
        if previous.mechanism != current.mechanism
    )
    if change_count == 0:
        return 1.0
    return 1.0 / change_count


def _support_replay_hash(
    axis: str,
    lower: float,
    upper: float,
    support_points: Sequence[PhasePoint],
    fixed_parameters: dict[str, float],
    fixed_context: dict[str, object],
) -> str:
    return sha256(
        json.dumps(
            {
                "axis": axis,
                "lower": lower,
                "upper": upper,
                "supporting_slice_hashes": [point.replay_hash for point in support_points],
                "fixed_parameters": fixed_parameters,
                "fixed_context": fixed_context,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def detect_transition_regions(points: Sequence[PhasePoint]) -> list[PhaseRegion]:
    if not points:
        return []

    axes = sorted({axis for point in points for axis in point.parameters})
    axis_spans = {
        axis: (
            max(point.parameters[axis] for point in points if axis in point.parameters)
            - min(point.parameters[axis] for point in points if axis in point.parameters)
        )
        for axis in axes
    }

    regions: list[PhaseRegion] = []
    seen_signatures: set[tuple[object, ...]] = set()
    for axis in axes:
        grouped: dict[tuple[object, ...], list[PhasePoint]] = defaultdict(list)
        for point in points:
            if axis not in point.parameters:
                continue
            fixed_parameters = {
                key: point.parameters[key]
                for key in axes
                if key != axis and key in point.parameters
            }
            signature = (
                axis,
                tuple(sorted(fixed_parameters.items())),
                tuple(sorted(phase_context(point).items())),
            )
            grouped[signature].append(point)

        for signature, group in sorted(grouped.items()):
            ordered = sorted(group, key=lambda item: (item.parameters[axis], item.slice_id))
            if len(ordered) < 2:
                continue

            for index, (left, right) in enumerate(zip(ordered, ordered[1:])):
                if left.mechanism == right.mechanism:
                    continue
                lower = min(left.parameters[axis], right.parameters[axis])
                upper = max(left.parameters[axis], right.parameters[axis])
                if math.isclose(lower, upper):
                    continue

                window_start = max(0, index - 1)
                window_end = min(len(ordered), index + 3)
                neighborhood = ordered[window_start:window_end]
                counts = Counter(point.mechanism for point in neighborhood)
                axis_span = axis_spans.get(axis) or (upper - lower)
                bracket_width = upper - lower
                normalized_width = min(1.0, (upper - lower) / max(axis_span, 1e-9))
                local_entropy = compute_entropy(counts.elements()) * normalized_width
                change_count = sum(
                    1
                    for previous, current in zip(neighborhood, neighborhood[1:])
                    if previous.mechanism != current.mechanism
                )
                local_disagreement = change_count / max(len(neighborhood) - 1, 1)
                region_signature = (
                    axis,
                    lower,
                    upper,
                    tuple(sorted(signature[1])),
                    tuple(sorted(signature[2])),
                )
                if region_signature in seen_signatures:
                    continue
                seen_signatures.add(region_signature)

                bounds = {axis: (lower, upper)}
                for fixed_axis, value in signature[1]:
                    bounds[str(fixed_axis)] = (float(value), float(value))

                dominant_mechanism = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
                sample_density = len(neighborhood) / max(upper - lower, 1e-9)
                left_support_count = _contiguous_support_count(ordered, start_index=index, step=-1)
                right_support_count = _contiguous_support_count(ordered, start_index=index + 1, step=1)
                support_count = len(neighborhood)
                refined_depth = 0
                if axis_span > 0.0 and bracket_width > 0.0:
                    refined_depth = max(0, int(math.floor(math.log2(axis_span / bracket_width))))
                fixed_parameters = {str(fixed_axis): float(value) for fixed_axis, value in signature[1]}
                regions.append(
                    PhaseRegion(
                        bounds=bounds,
                        dominant_mechanism=dominant_mechanism,
                        entropy=local_entropy,
                        sample_density=sample_density,
                        transition_axis=axis,
                        fixed_context=dict(signature[2]),
                        mechanism_counts=dict(sorted(counts.items())),
                        local_disagreement=local_disagreement,
                        local_gradient=estimate_local_gradient(neighborhood),
                        representative_slice_ids=tuple(point.slice_id for point in neighborhood),
                        estimated_threshold=(lower + upper) / 2.0,
                        axis_total_span=axis_span,
                        bracket_width=bracket_width,
                        normalized_bracket_width=normalized_width,
                        support_count=support_count,
                        left_support_count=left_support_count,
                        right_support_count=right_support_count,
                        refined_depth=refined_depth,
                        neighbor_agreement=(left_support_count + right_support_count) / max(support_count, 1),
                        phase_consistency=_phase_consistency(neighborhood),
                        replay_hash=_support_replay_hash(
                            axis,
                            lower,
                            upper,
                            neighborhood,
                            fixed_parameters,
                            dict(signature[2]),
                        ),
                    )
                )

    return sorted(
        regions,
        key=lambda region: (
            region.transition_axis or "",
            region.estimated_threshold if region.estimated_threshold is not None else 0.0,
            region.dominant_mechanism,
        ),
    )
