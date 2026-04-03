from __future__ import annotations

from avn.core.models import DisturbanceScheduleEntry, DisturbanceState


class DisturbanceTimeline:
    def __init__(
        self,
        base_state: DisturbanceState,
        schedule: list[DisturbanceScheduleEntry],
    ) -> None:
        self.base_state = base_state
        self.schedule = sorted(schedule, key=lambda entry: entry.start_minute)

    def state_at(self, minute: int) -> DisturbanceState:
        active_state = self.base_state
        for entry in self.schedule:
            if entry.start_minute <= minute:
                active_state = DisturbanceState(
                    weather_severity=entry.weather_severity,
                    comms_reliability=entry.comms_reliability,
                    comms_latency_minutes=entry.comms_latency_minutes,
                    message_drop_probability=entry.message_drop_probability,
                    stale_after_minutes=entry.stale_after_minutes,
                    reroute_delay_minutes=entry.reroute_delay_minutes,
                    low_bandwidth_threshold_minutes=entry.low_bandwidth_threshold_minutes,
                    node_service_multiplier=entry.node_service_multiplier,
                )
            else:
                break
        return active_state
