const state = { replay: null, frameIndex: 0, timer: null };

async function fetchJson(url) {
  const response = await fetch(url);
  return response.json();
}

function metric(label, value, tone = "normal") {
  return `
    <div class="metric ${tone}">
      <span>${label}</span>
      <strong>${value}</strong>
    </div>
  `;
}

function formatNumber(value) {
  return typeof value === "number" ? value.toFixed(2).replace(/\.00$/, "") : value;
}

function findNode(step, nodeId) {
  return step.nodes.find((node) => node.node_id === nodeId);
}

function activeDisturbances(step) {
  return step.events.filter((event) => event.event_type.endsWith("_disturbance"));
}

function routeBranchLabel(route) {
  if (!route || route.length < 2) {
    return "local";
  }
  if (route.includes("MID_NORTH") || route.includes("NORTH_PORT")) {
    return "north branch";
  }
  if (route.includes("MID_SOUTH") || route.includes("SOUTH_PORT")) {
    return "south branch";
  }
  return "direct";
}

function eventTone(event) {
  if (event.event_type.endsWith("_disturbance")) {
    return event.status === "closed" ? "critical" : event.status === "degraded" || event.weather_severity >= 0.25 ? "warning" : "nominal";
  }
  if (event.event_type === "vehicle_policy_hold" || event.event_type === "vehicle_holding") {
    return "critical";
  }
  if (event.event_type === "vehicle_rerouted" || event.event_type === "vehicle_contingency_divert") {
    return "warning";
  }
  return "normal";
}

function eventTitle(event) {
  switch (event.event_type) {
    case "vehicle_dispatched":
      return `Dispatch ${event.vehicle_id}`;
    case "vehicle_arrived":
      return `Arrival ${event.vehicle_id}`;
    case "vehicle_completed":
      return `Complete ${event.vehicle_id}`;
    case "vehicle_rerouted":
      return `Reroute ${event.vehicle_id}`;
    case "vehicle_policy_hold":
      return `Policy Hold ${event.vehicle_id}`;
    case "vehicle_holding":
      return `Holding ${event.vehicle_id}`;
    case "vehicle_contingency_divert":
      return `Contingency ${event.vehicle_id}`;
    case "corridor_disturbance":
      return `Corridor ${event.target_id}`;
    case "node_disturbance":
      return `Node ${event.target_id}`;
    default:
      return event.event_type.replaceAll("_", " ");
  }
}

function eventDetail(event) {
  switch (event.event_type) {
    case "vehicle_dispatched":
      return `${event.corridor_id} via ${routeBranchLabel(event.route)}`;
    case "vehicle_arrived":
      return `Arrived at ${event.node_id}`;
    case "vehicle_completed":
      return `Completed at ${event.destination}`;
    case "vehicle_rerouted":
      return `New route: ${routeBranchLabel(event.new_route)} (${event.new_route.join(" -> ")})`;
    case "vehicle_policy_hold":
      return `${event.policy_id} held departure on ${event.corridor_id} at severity ${formatNumber(event.weather_severity)}`;
    case "vehicle_holding":
      return `Holding at ${event.node_id} awaiting a route`;
    case "vehicle_contingency_divert":
      return `Diverted to ${event.contingency_target}`;
    case "corridor_disturbance":
    case "node_disturbance":
      return `${event.status} · severity ${formatNumber(event.weather_severity)}${event.note ? ` · ${event.note}` : ""}`;
    default:
      return Object.entries(event)
        .filter(([key]) => !["event_type", "time_minute"].includes(key))
        .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
        .join(" | ");
  }
}

function renderLegend() {
  document.getElementById("legend").innerHTML = `
    <div class="legend-row"><span class="legend-swatch open"></span><span>Open corridor</span></div>
    <div class="legend-row"><span class="legend-swatch degraded"></span><span>Degraded corridor</span></div>
    <div class="legend-row"><span class="legend-swatch closed"></span><span>Closed corridor</span></div>
    <div class="legend-row"><span class="legend-dot passenger"></span><span>Passenger / cargo</span></div>
    <div class="legend-row"><span class="legend-dot ems"></span><span>EMS priority mission</span></div>
    <div class="legend-row"><span class="legend-ring holding"></span><span>Holding vehicle</span></div>
  `;
}

function renderSummary(step) {
  document.getElementById("summary").innerHTML = `
    <h3>Snapshot</h3>
    <div class="summary-grid">
      <div><span>Minute</span><strong>${step.time_minute}</strong></div>
      <div><span>Completed</span><strong>${step.metrics.completed_vehicles}</strong></div>
      <div><span>Holding</span><strong>${step.metrics.holding_vehicles}</strong></div>
      <div><span>Peak Queue</span><strong>${step.metrics.max_queue_length}</strong></div>
      <div><span>Reroutes</span><strong>${step.metrics.reroute_count}</strong></div>
      <div><span>Weather</span><strong>${formatNumber(step.metrics.weather_severity)}</strong></div>
    </div>
  `;
  document.getElementById("metric-strip").innerHTML = [
    metric("Completed", step.metrics.completed_vehicles),
    metric("Active", step.metrics.active_vehicles),
    metric("Queue", step.metrics.max_queue_length, step.metrics.max_queue_length >= 4 ? "warning" : "normal"),
    metric("Load", formatNumber(step.metrics.max_corridor_load_ratio), step.metrics.max_corridor_load_ratio >= 2 ? "warning" : "normal"),
    metric("Weather", formatNumber(step.metrics.weather_severity), step.metrics.weather_severity >= 0.25 ? "warning" : "normal"),
  ].join("");
}

function renderPolicyPanel(step) {
  const policy = state.replay.policy;
  const disturbanceCount = activeDisturbances(step).length;
  document.getElementById("policy-panel").innerHTML = `
    <div class="policy-badge">${policy.policy_id.replaceAll("_", " ")}</div>
    <h3>${policy.label}</h3>
    <p>${policy.description}</p>
    <div class="policy-meta">
      <span>${disturbanceCount} active disruptions</span>
      <span>${step.metrics.reroute_count} cumulative reroutes</span>
    </div>
  `;
}

function renderDisruptions(step) {
  const disruptions = activeDisturbances(step);
  document.getElementById("disruptions").innerHTML = `
    <h3>Active Disruptions</h3>
    <p class="section-note">Current disturbances and route pressure shaping this frame.</p>
    <div class="disruption-list">
      ${disruptions.map((event) => `
        <div class="disruption-card ${eventTone(event)}">
          <div class="disruption-head">
            <strong>${event.target_id}</strong>
            <span>${event.status}</span>
          </div>
          <div class="disruption-meta">${event.event_type.replace("_disturbance", "")} · severity ${formatNumber(event.weather_severity)}</div>
          <p>${event.note || "No operator note."}</p>
        </div>
      `).join("") || `<div class="empty-state">No active disruptions in this frame.</div>`}
    </div>
  `;
}

function renderPosture(step) {
  const policyHolds = step.events.filter((event) => event.event_type === "vehicle_policy_hold").length;
  const reroutes = step.events.filter((event) => event.event_type === "vehicle_rerouted").length;
  const degradedCorridors = step.corridors.filter((corridor) => corridor.status !== "open" || corridor.weather_severity >= 0.25).length;
  const topCorridor = [...step.corridors].sort((left, right) => right.load_ratio - left.load_ratio)[0];
  const note = policyHolds > 0
    ? `${state.replay.policy.label} is intentionally holding departures off higher-risk corridors.`
    : reroutes > 0
      ? `${reroutes} reroute${reroutes === 1 ? "" : "s"} triggered in this frame.`
      : degradedCorridors > 0
        ? `${state.replay.policy.label} is working around ${degradedCorridors} degraded corridor${degradedCorridors === 1 ? "" : "s"}.`
        : "Traffic is flowing nominally with no active route intervention this frame.";

  document.getElementById("posture").innerHTML = `
    <h3>Route Posture</h3>
    <p class="section-note">${note}</p>
    <div class="posture-grid">
      <div><span>Policy Holds</span><strong>${policyHolds}</strong></div>
      <div><span>Frame Reroutes</span><strong>${reroutes}</strong></div>
      <div><span>Degraded Corridors</span><strong>${degradedCorridors}</strong></div>
      <div><span>Hottest Lane</span><strong>${topCorridor ? topCorridor.corridor_id : "n/a"}</strong></div>
    </div>
  `;
}

function renderAlerts(step) {
  document.getElementById("alerts").innerHTML = `
    <h3>Alerts</h3>
    <p class="section-note">Threshold-driven operator alerts derived from the canonical replay contract.</p>
    <div class="list-panel-inner">
      ${step.alerts.map((alert) => `
        <div class="list-item ${alert.severity}">
          <strong>${alert.code}</strong>
          <div>${alert.message}</div>
        </div>
      `).join("") || `<div class="empty-state">No alerts in this frame.</div>`}
    </div>
  `;
}

function renderEvents(step) {
  document.getElementById("events").innerHTML = step.events.map((event) => `
    <div class="list-item event-card ${eventTone(event)}">
      <div class="event-head">
        <strong>${eventTitle(event)}</strong>
        <span class="event-tag">t=${event.time_minute}</span>
      </div>
      <div>${eventDetail(event)}</div>
    </div>
  `).join("") || `<div class="empty-state">No events in this frame.</div>`;
}

function renderNetwork(step) {
  const svg = document.getElementById("network-canvas");
  const corridors = step.corridors.map((corridor) => {
    const origin = findNode(step, corridor.origin);
    const destination = findNode(step, corridor.destination);
    const statusClass = corridor.status === "closed"
      ? "corridor-closed"
      : corridor.status === "degraded" || corridor.weather_severity >= 0.25
        ? "corridor-degraded"
        : "corridor-open";
    return `
      <g class="corridor ${statusClass}">
        <line x1="${origin.x}" y1="${origin.y}" x2="${destination.x}" y2="${destination.y}"
          stroke-width="${4 + corridor.load * 1.4}" stroke-linecap="round" opacity="0.9" />
        <text class="corridor-label" x="${(origin.x + destination.x) / 2}" y="${(origin.y + destination.y) / 2 - 10}">
          ${corridor.corridor_id} · ${formatNumber(corridor.load_ratio)}x
        </text>
      </g>
    `;
  }).join("");
  const nodes = step.nodes.map((node) => {
    const nodeClass = node.status === "closed"
      ? "node-closed"
      : node.status === "degraded" || node.queue_length >= node.available_departures + 2
        ? "node-degraded"
        : "node-open";
    return `
      <g class="node ${nodeClass}">
        <circle cx="${node.x}" cy="${node.y}" r="${26 + node.queue_length * 2.2}" stroke-width="3" />
        <text class="node-label" x="${node.x - 34}" y="${node.y - 38}">${node.label}</text>
        <text class="node-meta" x="${node.x - 30}" y="${node.y + 6}">Q ${node.queue_length}</text>
        <text class="node-meta" x="${node.x - 30}" y="${node.y + 24}">${node.status}</text>
      </g>
    `;
  }).join("");
  const vehicles = step.vehicles.filter((vehicle) => vehicle.status !== "scheduled").map((vehicle) => {
    const vehicleClass = vehicle.mission_class === "ems" ? "vehicle-ems" : "vehicle-standard";
    const holdingClass = vehicle.status === "holding" ? "vehicle-holding" : "";
    return `
      <g class="vehicle ${vehicleClass} ${holdingClass}">
        <circle class="vehicle-marker" cx="${vehicle.x}" cy="${vehicle.y}" r="8" />
        <text class="vehicle-label" x="${vehicle.x + 10}" y="${vehicle.y - 10}">${vehicle.vehicle_id}</text>
      </g>
    `;
  }).join("");
  svg.innerHTML = `${corridors}${nodes}${vehicles}`;
}

function renderFrame(index) {
  state.frameIndex = index;
  const step = state.replay.steps[index];
  document.getElementById("timeline").value = index;
  document.getElementById("frame-note").textContent = `T+${step.time_minute}m · ${activeDisturbances(step).length} active disruptions`;
  renderSummary(step);
  renderPolicyPanel(step);
  renderDisruptions(step);
  renderPosture(step);
  renderNetwork(step);
  renderAlerts(step);
  renderEvents(step);
}

function stopPlayback() {
  if (!state.timer) {
    return;
  }
  clearInterval(state.timer);
  state.timer = null;
  document.getElementById("play-toggle").textContent = "Play";
}

function togglePlayback() {
  const button = document.getElementById("play-toggle");
  if (state.timer) {
    stopPlayback();
    return;
  }
  button.textContent = "Pause";
  state.timer = setInterval(() => {
    renderFrame((state.frameIndex + 1) % state.replay.steps.length);
  }, 900);
}

async function loadScenario(scenarioId) {
  stopPlayback();
  state.replay = await fetchJson(`/api/replay?scenario=${encodeURIComponent(scenarioId)}`);
  document.getElementById("scenario-name").textContent = state.replay.name;
  document.getElementById("scenario-description").textContent = state.replay.description;
  document.getElementById("timeline").max = state.replay.steps.length - 1;
  renderFrame(0);
}

async function init() {
  const scenarios = await fetchJson("/api/scenarios");
  const select = document.getElementById("scenario-select");
  select.innerHTML = scenarios.map((scenario) => `<option value="${scenario.scenario_id}">${scenario.name}</option>`).join("");
  select.addEventListener("change", (event) => loadScenario(event.target.value));
  document.getElementById("play-toggle").addEventListener("click", togglePlayback);
  document.getElementById("step-forward").addEventListener("click", () => renderFrame(Math.min(state.frameIndex + 1, state.replay.steps.length - 1)));
  document.getElementById("timeline").addEventListener("input", (event) => renderFrame(Number(event.target.value)));
  renderLegend();
  await loadScenario(scenarios[0].scenario_id);
}

init();
