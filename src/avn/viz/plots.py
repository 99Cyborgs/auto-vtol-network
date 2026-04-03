from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from avn.core.models import MetricsSnapshot


def generate_plots(snapshots: list[MetricsSnapshot], output_dir: Path) -> list[Path]:
    if not snapshots:
        raise ValueError("At least one metrics snapshot is required to generate plots")

    output_dir.mkdir(parents=True, exist_ok=True)
    times = [snapshot.time_minute for snapshot in snapshots]
    completed = [snapshot.completed_vehicles for snapshot in snapshots]
    queues = [snapshot.avg_queue_length for snapshot in snapshots]
    speeds = [snapshot.mean_corridor_speed for snapshot in snapshots]
    capacities = [snapshot.mean_effective_capacity for snapshot in snapshots]
    weather = [snapshot.weather_severity for snapshot in snapshots]
    comms = [snapshot.comms_reliability for snapshot in snapshots]

    plot_paths: list[Path] = []

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(times, completed, color="#1f77b4", linewidth=2)
    ax.set_title("Completed Missions Over Time")
    ax.set_xlabel("Simulation Time (minutes)")
    ax.set_ylabel("Completed Vehicles")
    ax.grid(alpha=0.3)
    path = output_dir / "completed_vehicles.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    plot_paths.append(path)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(times, queues, color="#ff7f0e", linewidth=2)
    ax.set_title("Average Node Queue Length")
    ax.set_xlabel("Simulation Time (minutes)")
    ax.set_ylabel("Vehicles Waiting")
    ax.grid(alpha=0.3)
    path = output_dir / "queue_length.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    plot_paths.append(path)

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax1.plot(times, speeds, color="#2ca02c", linewidth=2)
    ax1.set_xlabel("Simulation Time (minutes)")
    ax1.set_ylabel("Speed (km/h)", color="#2ca02c")
    ax1.tick_params(axis="y", labelcolor="#2ca02c")
    ax1.grid(alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(times, capacities, color="#d62728", linewidth=2)
    ax2.set_ylabel("Capacity (veh/h)", color="#d62728")
    ax2.tick_params(axis="y", labelcolor="#d62728")
    fig.suptitle("Corridor Speed And Capacity")
    path = output_dir / "corridor_performance.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    plot_paths.append(path)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(times, weather, color="#9467bd", linewidth=2, label="Weather Severity")
    ax.plot(times, comms, color="#8c564b", linewidth=2, label="Comms Reliability")
    ax.set_title("Disturbance Timeline")
    ax.set_xlabel("Simulation Time (minutes)")
    ax.set_ylabel("Scalar Value")
    ax.set_ylim(0.0, 1.05)
    ax.grid(alpha=0.3)
    ax.legend()
    path = output_dir / "disturbances.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    plot_paths.append(path)

    return plot_paths

