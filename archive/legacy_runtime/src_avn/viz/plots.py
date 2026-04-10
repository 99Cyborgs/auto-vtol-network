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
    info_age = [snapshot.information_age_mean for snapshot in snapshots]
    trusted_fraction = [snapshot.trusted_active_fraction for snapshot in snapshots]
    unsafe_admissions = [snapshot.unsafe_admission_count for snapshot in snapshots]
    landing_options = [snapshot.reachable_landing_option_mean for snapshot in snapshots]
    contingency_utilization = [snapshot.contingency_node_utilization for snapshot in snapshots]

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

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax1.plot(times, info_age, color="#bcbd22", linewidth=2, label="Mean Information Age")
    ax1.set_xlabel("Simulation Time (minutes)")
    ax1.set_ylabel("Information Age (minutes)", color="#bcbd22")
    ax1.tick_params(axis="y", labelcolor="#bcbd22")
    ax1.grid(alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(times, trusted_fraction, color="#17becf", linewidth=2, label="Trusted Active Fraction")
    ax2.plot(times, unsafe_admissions, color="#7f7f7f", linewidth=1.8, label="Unsafe Admissions")
    ax2.set_ylabel("Trust / Admissions", color="#17becf")
    ax2.tick_params(axis="y", labelcolor="#17becf")
    fig.suptitle("Governance Health")
    path = output_dir / "governance_health.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    plot_paths.append(path)

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax1.plot(times, landing_options, color="#e377c2", linewidth=2)
    ax1.set_xlabel("Simulation Time (minutes)")
    ax1.set_ylabel("Reachable Landing Options", color="#e377c2")
    ax1.tick_params(axis="y", labelcolor="#e377c2")
    ax1.grid(alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(times, contingency_utilization, color="#d62728", linewidth=2)
    ax2.set_ylabel("Contingency Utilization", color="#d62728")
    ax2.tick_params(axis="y", labelcolor="#d62728")
    fig.suptitle("Contingency Risk")
    path = output_dir / "contingency_risk.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    plot_paths.append(path)

    return plot_paths
