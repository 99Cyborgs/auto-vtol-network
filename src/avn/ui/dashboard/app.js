const state = { replay: null, frameIndex: 0, timer: null };

async function fetchJson(url) {
  const response = await fetch(url);
  return response.json();
}

function metric(label, value) {
  return `<div class="metric"><span>${label}</span><strong>${value}</strong></div>`;
}

function renderSummary(step) {
  document.getElementById("summary").innerHTML = `
    <h3>Snapshot</h3>
    <div>Minute: <strong>${step.time_minute}</strong></div>
    <div>Completed: <strong>${step.metrics.completed_vehicles}</strong></div>
    <div>Holding: <strong>${step.metrics.holding_vehicles}</strong></div>
    <div>Peak Queue: <strong>${step.metrics.max_queue_length}</strong></div>
    <div>Reroutes: <strong>${step.metrics.reroute_count}</strong></div>
  `;
  document.getElementById("metric-strip").innerHTML = [
    metric("Completed", step.metrics.completed_vehicles),
    metric("Active", step.metrics.active_vehicles),
    metric("Max Queue", step.metrics.max_queue_length),
    metric("Load Ratio", step.metrics.max_corridor_load_ratio),
    metric("Weather", step.metrics.weather_severity),
  ].join("");
}

function renderList(elementId, items, formatter) {
  document.getElementById(elementId).innerHTML =
    items.map(formatter).join("") || `<div class="list-item">No entries.</div>`;
}

function renderNetwork(step) {
  const svg = document.getElementById("network-canvas");
  const corridors = step.corridors.map((corridor) => {
    const origin = step.nodes.find((node) => node.node_id === corridor.origin);
    const destination = step.nodes.find((node) => node.node_id === corridor.destination);
    const color = corridor.status === "closed" ? "#b23a48" : corridor.weather_severity >= 0.5 ? "#d98524" : "#0d5c63";
    return `
      <g>
        <line x1="${origin.x}" y1="${origin.y}" x2="${destination.x}" y2="${destination.y}"
          stroke="${color}" stroke-width="${4 + corridor.load * 1.3}" stroke-linecap="round" opacity="0.82" />
        <text class="corridor-label" x="${(origin.x + destination.x) / 2}" y="${(origin.y + destination.y) / 2 - 8}">
          ${corridor.corridor_id} ${corridor.load}/${corridor.effective_capacity_per_hour.toFixed(1)}
        </text>
      </g>
    `;
  }).join("");
  const nodes = step.nodes.map((node) => {
    const fill = node.status === "closed" ? "#b23a48" : node.queue_length >= 4 ? "#d98524" : "#f4f0dc";
    return `
      <g>
        <circle cx="${node.x}" cy="${node.y}" r="${26 + node.queue_length * 2}" fill="${fill}" stroke="#18251d" stroke-width="3" />
        <text class="node-label" x="${node.x - 26}" y="${node.y - 36}">${node.label}</text>
        <text x="${node.x - 28}" y="${node.y + 6}">Q ${node.queue_length}</text>
        <text x="${node.x - 28}" y="${node.y + 22}">${node.status}</text>
      </g>
    `;
  }).join("");
  const vehicles = step.vehicles.filter((vehicle) => vehicle.status !== "scheduled").map((vehicle) => {
    const fill = vehicle.mission_class === "ems" ? "#b23a48" : "#0d5c63";
    return `
      <g>
        <circle cx="${vehicle.x}" cy="${vehicle.y}" r="8" fill="${fill}" stroke="#fff" stroke-width="2" />
        <text x="${vehicle.x + 10}" y="${vehicle.y - 10}">${vehicle.vehicle_id}</text>
      </g>
    `;
  }).join("");
  svg.innerHTML = `${corridors}${nodes}${vehicles}`;
}

function renderFrame(index) {
  state.frameIndex = index;
  const step = state.replay.steps[index];
  document.getElementById("timeline").value = index;
  renderSummary(step);
  renderNetwork(step);
  renderList("alerts", step.alerts, (alert) => `
    <div class="list-item ${alert.severity}">
      <strong>${alert.code}</strong><div>${alert.message}</div>
    </div>
  `);
  renderList("events", step.events, (event) => `
    <div class="list-item">
      <strong>${event.event_type}</strong><div>t=${event.time_minute}</div>
      <div>${Object.entries(event).filter(([key]) => !["event_type", "time_minute"].includes(key)).map(([key, value]) => `${key}: ${JSON.stringify(value)}`).join(" | ")}</div>
    </div>
  `);
}

function togglePlayback() {
  const button = document.getElementById("play-toggle");
  if (state.timer) {
    clearInterval(state.timer);
    state.timer = null;
    button.textContent = "Play";
    return;
  }
  button.textContent = "Pause";
  state.timer = setInterval(() => {
    renderFrame((state.frameIndex + 1) % state.replay.steps.length);
  }, 900);
}

async function loadScenario(scenarioId) {
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
  await loadScenario(scenarios[0].scenario_id);
}

init();
