const state = {
  dashboard: { hosts: [], settings: { refresh_interval: 15, history_minutes: 60, ssh_timeout: 10 } },
  nextRefresh: 0,
  timer: null,
  draggingId: null,
  editingHostId: null,
  metricsCursor: 0,
  loading: false,
  renderSignature: "",
};
const MAX_CLIENT_POINTS = 480;

const $ = (selector) => document.querySelector(selector);
const escapeHtml = (value) => String(value ?? "").replace(/[&<>'"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[c]));
const formatTime = (timestamp) => timestamp ? new Date(timestamp * 1000).toLocaleTimeString("zh-CN", { hour12: false }) : "—";
const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
const formatCapacity = (bytes) => {
  if (!bytes) return "—";
  const gib = bytes / (1024 ** 3);
  return `${gib >= 10 || Number.isInteger(gib) ? Math.round(gib) : gib.toFixed(1)}G`;
};
const applyTheme = (theme, persist = true) => {
  const selected = ["dark", "light", "eink"].includes(theme) ? theme : "dark";
  document.documentElement.dataset.theme = selected;
  if (persist) localStorage.setItem("hostwatch-theme", selected);
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
  });
  if (!response.ok) {
    let message = `请求失败 (${response.status})`;
    try {
      const body = await response.json();
      message = Array.isArray(body.detail) ? body.detail.map(x => x.msg).join("；") : body.detail || message;
    } catch (_) {}
    throw new Error(message);
  }
  return response.status === 204 ? null : response.json();
}

function showToast(message, isError = false) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.className = `toast show${isError ? " error" : ""}`;
  clearTimeout(showToast.timeout);
  showToast.timeout = setTimeout(() => toast.className = "toast", 2800);
}

function pathFor(values, maxValue, width = 250, height = 58) {
  if (!values.length) return { line: "", area: "" };
  const points = values.map((value, index) => {
    const x = values.length === 1 ? width : index * width / (values.length - 1);
    const y = height - clamp(value / maxValue, 0, 1) * (height - 5);
    return [x, y];
  });
  const line = points.map((p, index) => `${index ? "L" : "M"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ");
  const area = `${line} L${points.at(-1)[0].toFixed(1)},${height} L${points[0][0].toFixed(1)},${height} Z`;
  return { line, area };
}

function chartSvg(series, options = {}) {
  const allValues = series.flatMap(item => item.values);
  if (!allValues.length) {
    return `<svg class="chart" viewBox="0 0 250 64"><line class="chart-grid" x1="0" y1="31" x2="250" y2="31"/><text class="chart-empty" x="125" y="36" text-anchor="middle">等待采集数据</text></svg>`;
  }
  const maxValue = options.fixedMax || Math.max(options.minMax || 1, ...allValues) * 1.15;
  const lines = series.map((item, index) => {
    const path = pathFor(item.values, maxValue);
    return `${index === 0 ? `<path d="${path.area}" fill="${item.color}" opacity=".08"/>` : ""}<path class="chart-line" style="--chart-color:${item.color}" d="${path.line}"/>`;
  }).join("");
  return `<svg class="chart" viewBox="0 0 250 64" preserveAspectRatio="none" aria-hidden="true"><line class="chart-grid" x1="0" y1="15" x2="250" y2="15"/><line class="chart-grid" x1="0" y1="34" x2="250" y2="34"/><line class="chart-grid" x1="0" y1="53" x2="250" y2="53"/>${lines}</svg>`;
}

function metricCard(key, title, metrics, field, color, unit = "%") {
  const values = metrics.map(metric => metric[field]);
  const latest = values.length ? values.at(-1) : null;
  return `<div class="metric-card" data-metric="${key}"><div class="metric-head"><div><span class="metric-value" style="color:${color}">${latest === null ? "—" : latest.toFixed(1)}</span><span class="metric-unit">${unit}</span></div><span class="metric-meta">${title}</span></div>${chartSvg([{ values, color }], { fixedMax: unit === "%" ? 100 : null })}</div>`;
}

function networkCard(metrics) {
  const rx = metrics.map(metric => metric.network_rx_mbps);
  const tx = metrics.map(metric => metric.network_tx_mbps);
  const latestRx = rx.length ? rx.at(-1) : null;
  const latestTx = tx.length ? tx.at(-1) : null;
  return `<div class="metric-card" data-metric="network"><div class="metric-head"><div><span class="metric-value" style="color:var(--network-rx)">${latestRx === null ? "—" : latestRx.toFixed(2)}</span><span class="metric-unit">Mbps</span></div><span class="metric-meta"><span class="rx">↓ ${latestRx === null ? "—" : latestRx.toFixed(1)}</span>&nbsp; <span class="tx">↑ ${latestTx === null ? "—" : latestTx.toFixed(1)}</span></span></div>${chartSvg([{ values: rx, color: "var(--network-rx)" }, { values: tx, color: "var(--network-tx)" }], { minMax: 1 })}</div>`;
}

function renderHostContent(host, index, count) {
  const metrics = host.metrics || [];
  const status = host.status || { state: "pending" };
  const retrySeconds = status.retry_at ? Math.max(0, Math.ceil(status.retry_at - Date.now() / 1000)) : null;
  const statusText = {
    online: `在线 · ${status.latency_ms ?? "—"} ms`,
    error: retrySeconds === null ? "采集异常" : `采集异常 · ${retrySeconds} 秒后重试`,
    collecting: "正在采集",
    pending: "等待采集",
  }[status.state] || status.state;
  const info = status.system_info;
  const hostname = info?.hostname || "等待 hostname";
  const cpuTitle = info ? `CPU · ${info.cpu_cores}C` : "CPU";
  const memoryTitle = info ? `MEM · ${formatCapacity(info.memory_bytes)}` : "MEM";
  const diskTitle = info ? `DISK / · ${formatCapacity(info.disk_bytes)}` : "DISK /";
  return `<div class="host-cell">
      <span class="drag-handle" title="拖动排序">⠿</span><span class="status-indicator ${escapeHtml(status.state)}" title="${escapeHtml(statusText)}"></span>
      <div class="host-details"><div class="host-name-line"><span class="host-name" title="${escapeHtml(host.name)}">${escapeHtml(host.name)}</span><span class="auth-badge">${host.auth_type === "key" ? "KEY" : "PWD"}</span></div><div class="host-hostname" title="${escapeHtml(hostname)}">${escapeHtml(hostname)}</div><div class="host-address">${escapeHtml(host.username)}@${escapeHtml(host.address)}:${host.port} · ${escapeHtml(statusText)}</div>${status.error ? `<div class="host-error" title="${escapeHtml(status.error)}">${escapeHtml(status.error)}</div>` : ""}</div>
      <div class="host-actions"><button class="mini-button move-up" title="上移" ${index === 0 ? "disabled" : ""}>↑</button><button class="mini-button move-down" title="下移" ${index === count - 1 ? "disabled" : ""}>↓</button><button class="mini-button edit" title="编辑主机">✎</button><button class="mini-button host-refresh" title="立即采集">↻</button><button class="mini-button delete" title="删除主机">×</button></div>
    </div>
    ${metricCard("cpu", cpuTitle, metrics, "cpu_percent", "var(--cpu)")}
    ${metricCard("memory", memoryTitle, metrics, "memory_percent", "var(--memory)")}
    ${networkCard(metrics)}
    ${metricCard("disk", diskTitle, metrics, "disk_percent", "var(--disk)")}`;
}

function renderHost(host, index, count) {
  return `<article class="host-row" draggable="true" data-id="${host.id}">${renderHostContent(host, index, count)}</article>`;
}

function dashboardSignature(hosts) {
  return JSON.stringify(hosts.map(host => [
    host.id, host.name, host.address, host.port, host.username, host.auth_type, host.position,
  ]));
}

function updateSummary() {
  const hosts = state.dashboard.hosts || [];
  $("#emptyState").hidden = hosts.length > 0;
  $("#totalHosts").textContent = hosts.length;
  $("#onlineHosts").textContent = hosts.filter(host => host.status?.state === "online").length;
  $("#errorHosts").textContent = hosts.filter(host => host.status?.state === "error").length;
  $("#lastUpdated").textContent = `页面更新于 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
}

function render() {
  const hosts = state.dashboard.hosts || [];
  $("#hostRows").innerHTML = hosts.map((host, index) => renderHost(host, index, hosts.length)).join("");
  state.renderSignature = dashboardSignature(hosts);
  updateSummary();
  bindRowEvents();
}

function refreshRows(changedHostIds) {
  const hosts = state.dashboard.hosts || [];
  if (state.renderSignature !== dashboardSignature(hosts)) {
    render();
    return;
  }
  hosts.forEach((host, index) => {
    if (!changedHostIds.has(host.id)) return;
    const row = document.querySelector(`.host-row[data-id="${host.id}"]`);
    if (row) row.innerHTML = renderHostContent(host, index, hosts.length);
  });
  updateSummary();
  bindRowEvents();
}

function limitClientMetrics(metrics) {
  if (metrics.length <= MAX_CLIENT_POINTS) return metrics;
  const lastIndex = metrics.length - 1;
  const indexes = new Set(Array.from(
    { length: MAX_CLIENT_POINTS },
    (_, index) => Math.round(index * lastIndex / (MAX_CLIENT_POINTS - 1)),
  ));
  return [...indexes].sort((a, b) => a - b).map(index => metrics[index]);
}

async function loadDashboard({ quiet = false, resetMetrics = false } = {}) {
  if (state.loading) return;
  state.loading = true;
  try {
    const previousHosts = new Map((state.dashboard.hosts || []).map(host => [host.id, host]));
    const dashboard = await api("/api/dashboard");
    const changedHostIds = new Set();
    dashboard.hosts = dashboard.hosts.map(host => {
      const previous = previousHosts.get(host.id);
      if (
        !previous
        || JSON.stringify(previous.status) !== JSON.stringify(host.status)
        || host.status?.state === "error"
      ) {
        changedHostIds.add(host.id);
      }
      return { ...host, metrics: resetMetrics ? [] : (previous?.metrics || []) };
    });
    state.dashboard = dashboard;

    const since = resetMetrics ? 0 : state.metricsCursor;
    const metricPayload = await api(`/api/metrics?since=${since}&max_points=${MAX_CLIENT_POINTS}`);
    const cutoff = Date.now() / 1000 - state.dashboard.settings.history_minutes * 60;
    state.dashboard.hosts.forEach(host => {
      const incoming = metricPayload.metrics[String(host.id)] || [];
      if (resetMetrics || state.metricsCursor === 0) {
        host.metrics = incoming;
      } else {
        const byTimestamp = new Map(host.metrics.map(metric => [metric.timestamp, metric]));
        incoming.forEach(metric => byTimestamp.set(metric.timestamp, metric));
        host.metrics = [...byTimestamp.values()]
          .filter(metric => metric.timestamp >= cutoff)
          .sort((a, b) => a.timestamp - b.timestamp);
      }
      host.metrics = limitClientMetrics(host.metrics);
      if (incoming.length) changedHostIds.add(host.id);
    });
    state.metricsCursor = metricPayload.server_time;
    state.nextRefresh = Date.now() + state.dashboard.settings.refresh_interval * 1000;
    refreshRows(changedHostIds);
  } catch (error) {
    if (!quiet) showToast(error.message, true);
    $("#refreshText").textContent = "同步失败";
  } finally {
    state.loading = false;
  }
}

function beginClock() {
  clearInterval(state.timer);
  state.timer = setInterval(() => {
    const seconds = Math.max(0, Math.ceil((state.nextRefresh - Date.now()) / 1000));
    $("#refreshText").textContent = `${seconds} 秒后刷新`;
    if (seconds <= 0) loadDashboard({ quiet: true });
  }, 1000);
}

async function saveOrder() {
  try {
    await api("/api/hosts/reorder", { method: "POST", body: JSON.stringify({ host_ids: state.dashboard.hosts.map(host => host.id) }) });
  } catch (error) {
    showToast(error.message, true);
    await loadDashboard();
  }
}

function moveHost(id, offset) {
  const index = state.dashboard.hosts.findIndex(host => host.id === id);
  const target = index + offset;
  if (index < 0 || target < 0 || target >= state.dashboard.hosts.length) return;
  [state.dashboard.hosts[index], state.dashboard.hosts[target]] = [state.dashboard.hosts[target], state.dashboard.hosts[index]];
  render();
  saveOrder();
}

function bindRowEvents() {
  document.querySelectorAll(".host-row").forEach(row => {
    const id = Number(row.dataset.id);
    row.querySelector(".move-up").onclick = () => moveHost(id, -1);
    row.querySelector(".move-down").onclick = () => moveHost(id, 1);
    row.querySelector(".edit").onclick = () => {
      openHostDialog(state.dashboard.hosts.find(item => item.id === id));
    };
    row.querySelector(".delete").onclick = async () => {
      const host = state.dashboard.hosts.find(item => item.id === id);
      if (!confirm(`确定删除“${host.name}”及其历史数据吗？`)) return;
      try { await api(`/api/hosts/${id}`, { method: "DELETE" }); showToast("主机已删除"); await loadDashboard(); } catch (error) { showToast(error.message, true); }
    };
    row.querySelector(".host-refresh").onclick = async (event) => {
      event.currentTarget.disabled = true;
      try {
        const result = await api(`/api/hosts/${id}/refresh`, { method: "POST" });
        await loadDashboard();
        showToast(result.state === "online" ? "采集完成" : result.error || "采集失败", result.state !== "online");
      } catch (error) { showToast(error.message, true); await loadDashboard(); }
    };
    row.addEventListener("dragstart", () => { state.draggingId = id; row.classList.add("dragging"); });
    row.addEventListener("dragend", () => { state.draggingId = null; row.classList.remove("dragging"); document.querySelectorAll(".drop-target").forEach(x => x.classList.remove("drop-target")); });
    row.addEventListener("dragover", event => { event.preventDefault(); if (state.draggingId !== id) row.classList.add("drop-target"); });
    row.addEventListener("dragleave", () => row.classList.remove("drop-target"));
    row.addEventListener("drop", event => {
      event.preventDefault();
      const from = state.dashboard.hosts.findIndex(host => host.id === state.draggingId);
      const to = state.dashboard.hosts.findIndex(host => host.id === id);
      if (from >= 0 && to >= 0 && from !== to) {
        const [moved] = state.dashboard.hosts.splice(from, 1);
        state.dashboard.hosts.splice(to, 0, moved);
        render(); saveOrder();
      }
    });
  });
}

function openDialog(selector) { $(selector).showModal(); }
document.querySelectorAll("[data-close]").forEach(button => button.addEventListener("click", () => $(`#${button.dataset.close}`).close()));
function updateAuthFields(authType) {
  const isPassword = authType === "password";
  $("#passwordFields").hidden = !isPassword;
  $("#keyFields").hidden = isPassword;
}

function openHostDialog(host = null) {
  const form = $("#hostForm");
  form.reset();
  state.editingHostId = host?.id ?? null;
  if (host) {
    form.name.value = host.name;
    form.address.value = host.address;
    form.username.value = host.username;
    form.port.value = host.port;
    form.elements.auth_type.value = host.auth_type;
    $("#hostDialogTitle").textContent = "编辑监控主机";
    $("#hostDialogHint").textContent = "凭据留空表示保留原值；填写后将覆盖 config.json 中的明文值";
    $("#saveHost").textContent = "保存修改";
  } else {
    $("#hostDialogTitle").textContent = "添加监控主机";
    $("#hostDialogHint").textContent = "密码和私钥会以明文写入 config.json";
    $("#saveHost").textContent = "保存并开始监控";
  }
  updateAuthFields(host?.auth_type || "password");
  $("#hostFormError").textContent = "";
  openDialog("#hostDialog");
}

$("#openAddHost").onclick = $("#emptyAddHost").onclick = () => openHostDialog();
$("#openSettings").onclick = () => {
  const settings = state.dashboard.settings;
  const form = $("#settingsForm");
  form.refresh_interval.value = settings.refresh_interval;
  form.history_minutes.value = settings.history_minutes;
  form.ssh_timeout.value = settings.ssh_timeout;
  form.theme.value = document.documentElement.dataset.theme || "dark";
  $("#settingsFormError").textContent = "";
  openDialog("#settingsDialog");
};
$("#refreshAll").onclick = () => loadDashboard();

document.querySelectorAll('input[name="auth_type"]').forEach(input => input.addEventListener("change", event => {
  updateAuthFields(event.target.value);
}));

$("#hostForm").addEventListener("submit", async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const data = Object.fromEntries(new FormData(form));
  data.port = Number(data.port);
  const isEditing = state.editingHostId !== null;
  if (isEditing) {
    ["password", "private_key", "passphrase"].forEach(key => { if (!data[key]) delete data[key]; });
  } else {
    data.password ||= null; data.private_key ||= null; data.passphrase ||= null;
  }
  const submit = $("#saveHost");
  submit.disabled = true; submit.textContent = "正在保存…"; $("#hostFormError").textContent = "";
  try {
    const path = isEditing ? `/api/hosts/${state.editingHostId}` : "/api/hosts";
    await api(path, { method: isEditing ? "PATCH" : "POST", body: JSON.stringify(data) });
    $("#hostDialog").close(); showToast(isEditing ? "主机信息已更新" : "主机已添加，正在进行首次采集"); await loadDashboard();
  } catch (error) { $("#hostFormError").textContent = error.message; }
  finally { submit.disabled = false; submit.textContent = isEditing ? "保存修改" : "保存并开始监控"; }
});

$("#settingsForm").addEventListener("submit", async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const data = {
    refresh_interval: Number(form.refresh_interval.value),
    history_minutes: Number(form.history_minutes.value),
    ssh_timeout: Number(form.ssh_timeout.value),
  };
  try {
    state.dashboard.settings = await api("/api/settings", { method: "PUT", body: JSON.stringify(data) });
    applyTheme(form.theme.value);
    state.metricsCursor = 0;
    $("#settingsDialog").close(); state.nextRefresh = Date.now() + data.refresh_interval * 1000; showToast("设置已保存");
    await loadDashboard({ resetMetrics: true });
  } catch (error) { $("#settingsFormError").textContent = error.message; }
});

loadDashboard();
beginClock();
