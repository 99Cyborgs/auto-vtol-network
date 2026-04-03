from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from avn.core.models import MetricsSnapshot


class MetricsRecorder:
    def __init__(self) -> None:
        self.snapshots: list[MetricsSnapshot] = []
        self.events: list[dict[str, Any]] = []

    def record_snapshot(self, snapshot: MetricsSnapshot) -> None:
        self.snapshots.append(snapshot)

    def record_event(self, time_minute: int, event_type: str, **payload: Any) -> None:
        event = {"time_minute": time_minute, "event_type": event_type}
        event.update(payload)
        self.events.append(event)

    def write_metrics_csv(self, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "metrics.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(asdict(self.snapshots[0]).keys()))
            writer.writeheader()
            for snapshot in self.snapshots:
                writer.writerow(asdict(snapshot))
        return path

    def write_event_log(self, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "events.json"
        with path.open("w", encoding="utf-8") as handle:
            json.dump(self.events, handle, indent=2)
        return path

