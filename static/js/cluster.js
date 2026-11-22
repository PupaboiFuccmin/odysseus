// Optional Cluster Tab UI.
// Stays completely dormant (no rail button, sidebar item, modal, or polling)
// unless GET /api/cluster/config returns { enabled: true }.
// Data: same-origin /api/cluster/* (routes/cluster_routes.py).

const API = '/api/cluster';
const COLORS = {
  green: '#3fb950', blue: '#58a6ff', amber: '#e3b341', purple: '#bc8cff',
  red: '#f85149', gray: '#8b949e', teal: '#39c5cf', cyan: '#22d3ee', indigo: '#818cf8',
};
const FLOW_FILTER = {
  task: /task |worker|route/i,
  review: /PASS|FAIL|verdict|fix|check/i,
  monitoring: /health|peer|heartbeat|selfcheck|canary/i,
  alert: /alert|warn|crit|failover|restart/i,
  tuning: /tun|knob|change|power-mode|eco/i,
  physical: /link|ping|rtt|unreach|peer/i,
  tools: /search|memory|ntfy|tool\.|stored/i,
  remote: /remote|controller|session|odysseus cluster/i,
  offload: /offload|frontier/i,
};
const S = {
  open: false, data: null, offload: null, offloadOpen: false, sel: null,
  tab: 'activity', logLines: [], logSince: '', logFilter: null,
  timers: [], metrics: null, changes: [], hours: 24,
};

const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
}[c]));
const num = (v, d = 1) => (v === null || v === undefined || v === '' || Number.isNaN(Number(v)))
  ? '—' : Number(v).toFixed(d).replace(/\.0+$/, '');
const ago = (ts) => {
  if (!ts) return '—';
  const s = (Date.now() - new Date(ts).getTime()) / 1000;
  if (!Number.isFinite(s)) return esc(ts);
  if (s < 60) return `${Math.max(0, Math.round(s))}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86400)}d ago`;
};
const local = (ts) => {
  try {
    return new Date(ts).toLocaleString([], {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
  } catch { return ts; }
};
const dur = (s) => {
  if (!s && s !== 0) return '—';
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  return d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : `${m}m`;
};

async function api(path, opts = {}) {
  const r = await fetch(API + path, {
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
    ...opts,
  });
  let body = null;
  try { body = await r.json(); } catch { /* empty */ }
  if (!r.ok) throw new Error((body && (body.detail || body.error)) || `HTTP ${r.status}`);
  return body;
}

// ---------- simple original inline SVG vendor badges
const LOGOS = {
  nvidia: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="NVIDIA"><rect width="44" height="16" rx="3" fill="#76b900"/><path d="M4 8c2.5-3.6 6.5-4 9-1.5-2.5-1-5-.5-6.8 1.5 1.8 2 4.3 2.5 6.8 1.5C10.5 12 6.5 11.6 4 8z" fill="#fff"/><text x="16" y="11.3" font-size="7.4" font-weight="700" fill="#fff" font-family="Arial,sans-serif">NVIDIA</text></svg>`,
  amd: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="AMD"><rect width="44" height="16" rx="3" fill="#111" stroke="#ed1c24" stroke-width="1"/><path d="M5 4h7v7l-2-2V6H7z M5 6.5l2 2V11h2.5l2 2H5z" fill="#ed1c24"/><text x="16" y="11.5" font-size="8" font-weight="800" fill="#fff" font-family="Arial,sans-serif">AMD</text></svg>`,
  intel: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="Intel"><rect width="44" height="16" rx="3" fill="#0071c5"/><ellipse cx="22" cy="8" rx="17" ry="5.6" fill="none" stroke="#fff" stroke-width=".9"/><text x="12.5" y="10.8" font-size="7.6" font-weight="600" fill="#fff" font-family="Arial,sans-serif">intel</text></svg>`,
  arch: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="Arch Linux"><rect width="44" height="16" rx="3" fill="#1793d1"/><path d="M8.5 2.2c-.6 1.6-1 2.6-1.7 4 .4.4 1 .9 1.9 1.4-1-.4-1.6-.8-2.1-1.2-1 2-2.4 4.6-4.6 8.4 1.8-1 3.2-1.7 4.5-1.9l-.1-.5c0-1.4.9-2.5 2-2.4 1.1.1 1.9 1.3 1.9 2.7v.3c1.3.3 2.6.9 4.3 1.8-.3-.6-.7-1.1-1.1-1.6-.9-.8-1.9-1.2-3-1.5 1.2.2 2 .5 2.6.8C11.5 8 10.4 5.3 8.5 2.2z" fill="#fff"/><text x="18" y="11.2" font-size="7.4" font-weight="700" fill="#fff" font-family="Arial,sans-serif">arch</text></svg>`,
  ubiquiti: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="Ubiquiti"><rect width="44" height="16" rx="3" fill="#006fff"/><path d="M5 3.5v5.2a3.3 3.3 0 0 0 6.6 0V3.5h-1.6v5.2a1.7 1.7 0 0 1-3.4 0V3.5z" fill="#fff"/><text x="14.5" y="11" font-size="6.6" font-weight="700" fill="#fff" font-family="Arial,sans-serif">UBNT</text></svg>`,
  klipper: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="Klipper"><rect width="44" height="16" rx="3" fill="#b12f36"/><text x="5" y="11" font-size="7" font-weight="700" fill="#fff" font-family="Arial,sans-serif">Klipper</text></svg>`,
  octoprint: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="OctoPrint"><rect width="44" height="16" rx="3" fill="#13c100"/><text x="3" y="11" font-size="6.4" font-weight="700" fill="#fff" font-family="Arial,sans-serif">OctoPrint</text></svg>`,
  handheld: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="Handheld"><rect width="44" height="16" rx="3" fill="#1b1b1b"/><rect x="1.5" y="2" width="8" height="12" rx="1.5" fill="#2a2a2a"/><rect x="34.5" y="2" width="8" height="12" rx="1.5" fill="#2a2a2a"/><rect x="11" y="3" width="22" height="10" rx="1.2" fill="#1f6f4a"/><rect x="12.5" y="4.4" width="19" height="7.2" rx="0.8" fill="#0e0e0e"/><circle cx="5.5" cy="8" r="1.6" fill="#c8c8c8"/><circle cx="38.5" cy="8" r="1.6" fill="#c8c8c8"/></svg>`,
  ubuntu: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="Ubuntu"><rect width="44" height="16" rx="3" fill="#e95420"/><text x="5" y="11.2" font-size="7.4" font-weight="700" fill="#fff" font-family="Arial,sans-serif">ubuntu</text></svg>`,
  arm: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="ARM"><rect width="44" height="16" rx="3" fill="#0091bd"/><text x="11" y="11.2" font-size="7.4" font-weight="700" fill="#fff" font-family="Arial,sans-serif">ARM</text></svg>`,
  linux: `<svg class="cl-logo" viewBox="0 0 44 16" aria-label="Linux"><rect width="44" height="16" rx="3" fill="#333"/><text x="9" y="11.2" font-size="7.4" font-weight="700" fill="#fff" font-family="Arial,sans-serif">Linux</text></svg>`,
};
LOGOS.nintendo = LOGOS.handheld;
LOGOS.tegra = LOGOS.arm;

const logo = (v) => LOGOS[(v || '').toLowerCase()] || '';
const ICON = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="2" width="6" height="5" rx="1"/><rect x="2" y="16" width="6" height="5" rx="1"/><rect x="16" y="16" width="6" height="5" rx="1"/><path d="M12 7v4M5 16v-2a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v2"/></svg>`;

// ---------- shell injection
function inject() {
  if (document.getElementById('cluster-modal')) return;
  const railRef = document.getElementById('rail-compare') || document.getElementById('rail-cookbook');
  if (railRef && !document.getElementById('rail-cluster')) {
    const b = document.createElement('button');
    b.className = 'icon-rail-btn';
    b.id = 'rail-cluster';
    b.title = 'Cluster';
    b.innerHTML = ICON;
    b.addEventListener('click', toggle);
    railRef.parentNode.insertBefore(b, railRef);
  }
  const toolRef = document.getElementById('tool-compare-btn') || document.getElementById('tool-cookbook-btn');
  if (toolRef && !document.getElementById('tool-cluster-btn')) {
    const d = document.createElement('div');
    d.className = 'list-item';
    d.id = 'tool-cluster-btn';
    d.innerHTML = `<span style="flex-shrink:0;opacity:.5;display:inline-flex;transform:scale(.9)">${ICON}</span><span class="grow">Cluster</span><span class="cookbook-notif-dot" id="cluster-notif-dot" style="display:none;margin-left:6px;margin-right:4px"></span>`;
    d.addEventListener('click', toggle);
    toolRef.parentNode.insertBefore(d, toolRef);
  }
  const m = document.createElement('div');
  m.id = 'cluster-modal';
  m.className = 'modal hidden';
  m.innerHTML = `
    <div class="modal-content cluster-content" role="dialog" aria-label="Cluster">
      <div class="modal-header">
        <h4 style="margin:0;display:flex;align-items:center;gap:6px">${ICON}Cluster</h4>
        <span id="cl-sources" class="cl-badge">...</span>
        <span id="cl-updated" class="cl-sub" style="margin-right:auto"></span>
        <button class="cl-btn" id="cl-refresh" title="Refresh now">Refresh</button>
        <button class="cl-btn primary" id="cl-add-device" title="Onboard new hardware">Add device</button>
        <button class="close-btn" id="close-cluster-modal" aria-label="Close cluster">&times;</button>
      </div>
      <div class="cl-body">
        <div class="cl-main">
          <div class="cl-map" id="cl-map"></div>
          <div class="cl-offload" id="cl-offload"></div>
          <div class="cl-grid" id="cl-grid"><div class="cl-empty">Loading cluster...</div></div>
        </div>
        <div class="cl-side" id="cl-side"></div>
      </div>
    </div>
    <div id="cl-tip" style="position:fixed;display:none;z-index:400;pointer-events:none;background:var(--panel);color:var(--fg);border:1px solid var(--border);border-radius:6px;padding:6px 8px;font-size:11px;max-width:300px;box-shadow:0 4px 18px #0006"></div>`;
  document.body.appendChild(m);
  m.querySelector('#close-cluster-modal').addEventListener('click', close);
  m.querySelector('#cl-refresh').addEventListener('click', () => refresh(true));
  m.querySelector('#cl-add-device').addEventListener('click', openOnboard);
  m.addEventListener('click', (e) => { if (e.target === m) close(); });
  document.addEventListener('keydown', (e) => { if (S.open && e.key === 'Escape' && !e.defaultPrevented) close(); });
}

function toggle() { if (S.open) close(); else open(); }
function open() {
  inject();
  document.getElementById('cluster-modal').classList.remove('hidden');
  S.open = true;
  document.getElementById('rail-cluster')?.classList.add('active');
  refresh(true);
  S.timers.push(setInterval(() => refresh(false), 5000));
  S.timers.push(setInterval(() => { if (S.sel && S.tab === 'activity') loadLog(false); }, 2500));
}
function close() {
  document.getElementById('cluster-modal')?.classList.add('hidden');
  S.open = false;
  S.timers.forEach(clearInterval);
  S.timers = [];
  document.getElementById('rail-cluster')?.classList.remove('active');
  if (location.pathname === '/cluster') history.replaceState(null, '', '/');
}

async function refresh(force) {
  try {
    const [nodesData, offloadData] = await Promise.all([
      api('/nodes'),
      api('/offload?n=15').catch(() => null),
    ]);
    S.data = nodesData;
    if (offloadData) S.offload = offloadData;
    render();
    if (S.sel === '_onboard') { /* keep the Add device form across polls */ }
    else if (force && S.sel) renderSide();
    else if (S.sel && (S.tab === 'models' || !node())) renderSide();
  } catch (e) {
    const g = document.getElementById('cl-grid');
    if (g) g.innerHTML = `<div class="cl-empty">Cluster API unavailable: ${esc(e.message)}</div>`;
  }
}

function statusClass(st) {
  st = (st || '').toLowerCase();
  if (['ok', 'healthy', 'active', 'busy', 'online'].includes(st)) return 'ok';
  if (['degraded', 'stale', 'warn', 'throttled', 'paused'].includes(st)) return 'warn';
  if (!st || st === 'unknown') return '';
  return 'bad';
}

function render() {
  const d = S.data;
  if (!d) return;
  const src = (d.sources || []).join(' + ') || 'none';
  const sb = document.getElementById('cl-sources');
  sb.textContent = 'data: ' + src;
  sb.className = 'cl-badge ' + ((d.sources || []).includes('mock') ? 'mock' : (d.sources || []).includes('manager') ? 'ok' : '');
  sb.title = d.manager_api ? `manager API: ${d.manager_api}` : 'manager /api/* not reachable or not configured';
  document.getElementById('cl-updated').textContent = 'updated ' + new Date().toLocaleTimeString();
  renderMap();
  renderOffload();
  const cards = (d.nodes || []).map(card).join('') + (d.devices || []).map(deviceCard).join('');
  document.getElementById('cl-grid').innerHTML = cards
    || `<div class="cl-empty">${esc(d.error || 'No cluster nodes configured. Set ODYSSEUS_CLUSTER_API_URL or ODYSSEUS_CLUSTER_MOCK=1.')}</div>`;
  document.querySelectorAll('.cl-card').forEach((el) => el.addEventListener('click', () => select(el.dataset.id)));
}

function card(n) {
  const hw = n.hardware || {}, ol = n.ollama_info || {}, h = n.host || {};
  const st = n.status || (ol.reachable ? 'ok' : 'unknown');
  const loaded = ol.loaded || [];
  const vramUsed = n.vram_used_gb ?? (loaded.reduce((a, m) => a + (m.vram_gb || 0), 0) || null);
  const ram = h.ram_total_gb ? `${num(h.ram_used_gb)}/${num(h.ram_total_gb)} GB` : (hw.ram_gb ? `${hw.ram_gb} GB` : '—');
  const alerts = (n.alerts || []).filter((a) => ['warn', 'crit'].includes(a.level));
  const pct = (a, b) => (a && b) ? Math.min(100, Math.round((a / b) * 100)) : 0;
  return `<div class="cl-card ${S.sel === n.id ? 'selected' : ''}" data-id="${esc(n.id)}">
    <div class="cl-card-head"><span class="cl-dot ${statusClass(st)}" title="${esc(st)}"></span>
      <span class="cl-name">${esc(n.name || n.id)}</span><span class="cl-role">${esc(n.role || '')}</span>
      <span class="cl-sub" style="margin-left:auto">${esc(n.ip || '')}${n.port ? ' · ' + esc(n.port) : ''}</span></div>
    <div class="cl-sub">${esc(n.job || '')}</div>
    <div class="cl-hw">
      <div class="cl-hw-row">${logo(hw.cpu_vendor)}<span>${esc(hw.cpu || '—')}</span></div>
      <div class="cl-hw-row">${logo(hw.gpu_vendor)}<span>${esc(hw.gpu || '—')}</span></div>
      <div class="cl-hw-row">${logo(hw.os_vendor)}<span>${esc(hw.os || '')} · RAM ${esc(ram)} · ${esc(n.backend || '')} · Ollama ${esc(ol.version || '?')}</span></div>
    </div>
    <div class="cl-stats">
      <div class="cl-stat" title="${esc(n.power_note || '')}"><b>${num(n.power_w)}</b><span>Watts</span></div>
      <div class="cl-stat"><b>${num(n.gpu_temp_c, 0)}&deg;</b><span>GPU &deg;C</span></div>
      <div class="cl-stat"><b>${num(n.gpu_util_pct, 0)}%</b><span>GPU util</span></div>
      <div class="cl-stat"><b>${n.queue_depth ?? '—'}</b><span>Queue</span></div>
    </div>
    ${n.vram_total_gb ? `<div class="cl-sub">VRAM ${num(vramUsed, 1)} / ${num(n.vram_total_gb, 1)} GB</div><div class="cl-bar"><i style="width:${pct(vramUsed, n.vram_total_gb)}%"></i></div>` : ''}
    <div class="cl-task" title="${esc(n.current_task || '')}">${n.current_task ? 'Running: ' + esc(n.current_task) : (n.last_task ? `idle · last: ${esc(n.last_task.model)} ${num(n.last_task.tok_s)} tok/s ${ago(n.last_task.ts)}` : 'idle')}</div>
    <div class="cl-chips">${loaded.map((m) => `<span class="cl-chip loaded" title="loaded · ${num(m.vram_gb)} GB VRAM">&bull; ${esc(m.name)}</span>`).join('')}
      ${(ol.installed || []).filter((m) => !loaded.some((l) => l.name === m.name)).slice(0, 6).map((m) => `<span class="cl-chip">${esc(m.name)}</span>`).join('')}</div>
    ${alerts.length ? `<div class="cl-alert ${esc(alerts[alerts.length - 1].level)}">WARN: ${esc(alerts[alerts.length - 1].msg)}</div>` : ''}
    ${n.mock ? '<span class="cl-badge mock">mock data</span>' : ''}
  </div>`;
}

function renderOffload() {
  const el = document.getElementById('cl-offload');
  if (!el) return;
  const o = S.offload;
  if (!o || !o.totals) { el.innerHTML = ''; el.style.display = 'none'; return; }
  el.style.display = '';
  const t = o.totals || {};
  const jobs = o.recent_jobs || [];
  const upCls = o.service_up === true ? 'ok' : (o.service_up === false ? 'bad' : 'warn');
  const upTxt = o.service_up === true ? 'up' : (o.service_up === false ? 'down' : 'journal');
  const fmtTok = (n) => {
    n = Number(n || 0);
    return n >= 1e6 ? (n / 1e6).toFixed(2) + 'M' : n >= 1000 ? (n / 1000).toFixed(1) + 'k' : String(n);
  };
  const pct = t.raw_input_tokens_est ? Math.round((t.frontier_tokens_saved_est / t.raw_input_tokens_est) * 100) : 0;
  const mgrS = t.manager_time_s ?? t.desktop_time_s;
  const wrkS = t.worker_time_s ?? t.mini_time_s;
  el.innerHTML = `
    <div class="cl-offload-head">
      <div style="display:flex;align-items:center;gap:8px">
        <span class="cl-dot ${upCls}"></span>
        <b>Frontier Offload</b>
        <span class="cl-badge ${upCls}">${esc(upTxt)}</span>
        <span class="cl-sub">MCP + REST</span>
      </div>
      <div class="cl-offload-stats">
        <span><b>${t.jobs_total ?? 0}</b> jobs</span>
        <span><b>${fmtTok(t.local_input_tokens)}</b> local in</span>
        <span><b>${fmtTok(t.returned_output_tokens)}</b> returned</span>
        <span style="color:${COLORS.indigo}"><b>${fmtTok(t.frontier_tokens_saved_est)}</b> saved (${pct}%)</span>
        <span><b>${num(mgrS, 1)}s</b> manager &middot; <b>${num(wrkS, 1)}s</b> worker</span>
        ${jobs.length ? `<button class="cl-btn" id="cl-offload-toggle">${S.offloadOpen ? 'Hide jobs' : `Jobs (${jobs.length})`}</button>` : ''}
      </div>
    </div>
    ${S.offloadOpen && jobs.length ? `
      <table class="cl-table" style="margin-top:6px">
        <tr><th>when</th><th>tool</th><th>target</th><th>in &rarr; out</th><th>saved</th><th>time</th><th>status</th></tr>
        ${jobs.slice(0, 12).map((j) => `<tr>
          <td>${esc(ago(j.ts))}</td>
          <td><code>${esc(j.tool)}</code></td>
          <td title="${esc(j.target || '')}">${esc(String(j.target || '').slice(0, 32))}</td>
          <td>${fmtTok(j.raw_input_tokens_est)} &rarr; ${fmtTok(j.returned_output_tokens)}</td>
          <td style="color:${COLORS.indigo}"><b>${fmtTok(j.frontier_tokens_saved_est)}</b></td>
          <td>${num(j.wall_s, 1)}s</td>
          <td><span class="cl-badge ${j.status === 'ok' ? 'ok' : 'bad'}">${esc(j.status)}</span></td>
        </tr>`).join('')}
      </table>` : ''}`;
  const btn = el.querySelector('#cl-offload-toggle');
  if (btn) btn.addEventListener('click', () => { S.offloadOpen = !S.offloadOpen; renderOffload(); });
}

const DEVICE_TEMPLATES = {
  controller: (dv) => {
    const m = dv.metrics || {}, bat = dv.battery || {}, ips = dv.ips || {}, hw = dv.hardware || {}, sess = dv.session || {};
    const pct = m.battery_pct ?? bat.percent;
    const charging = m.charging ?? bat.charging;
    const limit = m.charge_limit_pct ?? bat.charge_limit_percent ?? 85;
    const fill = charging ? COLORS.green : (pct != null && pct < 20 ? COLORS.red : COLORS.cyan);
    const bar = (pct == null) ? '' : `<div class="cl-bar" title="charge limit ${esc(limit)}%"><div style="width:${Math.min(100, Number(pct))}%;height:100%;background:${fill}"></div></div>`;
    return `<div class="cl-hw">
        <div class="cl-hw-row">${logo(dv.vendor || hw.vendor)}${logo(hw.os_vendor)}${logo(hw.cpu_vendor)}<span>${esc(dv.model || hw.system || 'Remote controller')}</span></div>
        <div class="cl-hw-row"><span>${esc(hw.cpu || 'ARM / host')} &middot; ${esc(hw.os || 'Linux')}</span></div></div>
      <div class="cl-stats">
        <div class="cl-stat"><b>${pct == null ? '—' : `${pct}%`}</b><span>${charging ? 'charging' : esc(bat.status || m.battery_status || 'power')} &middot; cap ${esc(limit)}%</span></div>
        <div class="cl-stat"><b>${esc(m.mode || (dv.dock || {}).mode || '—')}</b><span>form</span></div>
        <div class="cl-stat" title="${esc(m.input_detail || m.joycon_detail || '')}"><b>${esc(m.input_state || m.joycon || '—')}</b><span>${esc(m.input_label || 'input')}</span></div>
        <div class="cl-stat"><b>${m.latency_ms == null ? '—' : num(m.latency_ms, 1)}</b><span>ms to manager</span></div></div>
      ${bar}
      <dl class="cl-kv">
        <dt>LAN</dt><dd>${esc(m.lan_ip || ips.wifi || ips.wired || dv.ip || '—')}</dd>
        <dt>Overlay</dt><dd>${esc(ips.tailscale || (m.tailscale_ip !== 'not joined' && m.tailscale_ip) || 'not joined')}</dd>
        <dt>Session</dt><dd>${esc(sess.open ? (sess.detail || m.session_detail || 'Odysseus open') : (m.session || 'idle'))}</dd>
        <dt>Seen</dt><dd>${m.heartbeat_age_s != null ? `${Math.round(m.heartbeat_age_s)}s ago` : 'no heartbeat'}</dd>
      </dl>`;
  },
  router: (dv) => {
    const m = dv.metrics || {};
    return `<div class="cl-hw"><div class="cl-hw-row">${logo(dv.vendor)}<span>${esc(dv.model || '')}</span></div></div>
      <div class="cl-stats">
        <div class="cl-stat"><b>${num(m.latency_ms, 2)}</b><span>ms (TCP)</span></div>
        <div class="cl-stat"><b>${esc(m.reachable || '—')}</b><span>gateways up</span></div>
        <div class="cl-stat"><b>${(dv.interfaces || []).filter((i) => i.subnet).length}</b><span>cluster subnets</span></div>
        <div class="cl-stat"><b>${(dv.interfaces || []).length}</b><span>ports</span></div></div>
      <table class="cl-table"><tr><th>port</th><th>gateway</th><th>subnet</th><th>devices</th></tr>
        ${(dv.interfaces || []).map((i) => `<tr><td>${esc(i.port)}</td><td>${esc(i.ip || '')}</td><td>${esc(i.subnet || i.note || '')}</td><td>${esc((i.devices || []).join(', '))}</td></tr>`).join('')}</table>
      <div class="cl-sub">${esc(dv.probe_note || 'read-only')}</div>`;
  },
};
function genericTemplate(dv) {
  const m = dv.metrics || {};
  const keys = Object.keys(m);
  return `<div class="cl-hw"><div class="cl-hw-row">${logo(dv.vendor)}<span>${esc(dv.model || dv.type)}</span></div></div>
    ${keys.length ? `<dl class="cl-kv">${keys.map((k) => `<dt>${esc(k)}</dt><dd>${esc(typeof m[k] === 'number' ? num(m[k], 2) : m[k])}</dd>`).join('')}</dl>` : '<div class="cl-empty">no metrics (add probe.url / probe.fields in devices.json)</div>'}`;
}
function deviceCard(dv) {
  const tpl = DEVICE_TEMPLATES[dv.type] || genericTemplate;
  return `<div class="cl-card ${S.sel === dv.id ? 'selected' : ''}" data-id="${esc(dv.id)}">
    <div class="cl-card-head"><span class="cl-dot ${statusClass(dv.status)}" title="${esc(dv.status)}"></span>
      <span class="cl-name">${esc(dv.name || dv.id)}</span><span class="cl-role">${esc(dv.role || dv.type)}</span>
      <span class="cl-sub" style="margin-left:auto">${esc(dv.ip || '')}</span></div>
    <div class="cl-sub">${esc(dv.job || '')}</div>${tpl(dv)}</div>`;
}

// ---------- chain-of-command map (hierarchical; scales to N workers)
function renderMap() {
  const d = S.data, el = document.getElementById('cl-map');
  if (!el || !d) return;
  const nodes = d.nodes || [];
  if (!nodes.length && !(d.devices || []).length) { el.innerHTML = ''; return; }
  const managers = nodes.filter((n) => n.role === 'manager' || n.is_host);
  const workers = nodes.filter((n) => !managers.includes(n));
  const controllers = (d.devices || []).filter((x) => x.type === 'controller');
  const W = 1000, H = 340, pos = {};
  managers.forEach((n, i) => { pos[n.id] = { x: (W * (i + 1)) / (managers.length + 1), y: 110 }; });
  workers.forEach((n, i) => { pos[n.id] = { x: (W * (i + 1)) / (workers.length + 1), y: 255 }; });
  controllers.forEach((n, i) => {
    const anchor = (managers[0] && pos[managers[0].id]) || { x: W / 2 };
    pos[n.id] = { x: anchor.x + (i - (controllers.length - 1) / 2) * 170, y: 36 };
  });
  const hasRouter = !!d.router || (d.flows || []).some((f) => f.kind === 'physical' || f.kind === 'network');
  const RID = (d.router && d.router.id) || 'router';
  if (hasRouter) {
    pos[RID] = { x: W - 95, y: 175 };
  }
  const known = new Set(nodes.map((n) => n.id).concat(controllers.map((n) => n.id), hasRouter ? [RID] : []));
  const pseudo = [...new Set((d.flows || []).flatMap((f) => [f.from, f.to]).filter((x) => x && !known.has(x)))];
  const slots = [95, W - 95, 265, W - 265, 180, W - 180];
  pseudo.forEach((id, i) => { pos[id] = { x: slots[i % slots.length], y: 36 + Math.floor(i / slots.length) * 44 }; });
  const kinds = d.flow_kinds || {};
  const pairCount = {};
  let paths = '';
  (d.flows || []).forEach((f, idx) => {
    const a = pos[f.from], b = pos[f.to];
    if (!a || !b || f.from === f.to) return;
    const pk = [f.from, f.to].sort().join('|');
    const k = (pairCount[pk] = (pairCount[pk] || 0) + 1) - 1;
    const col = COLORS[f.color] || COLORS.gray;
    const cls = `cl-flow ${f.status || 'idle'}`;
    let dpath;
    if ((f.kind === 'physical' || f.kind === 'network') && pos[RID]) {
      const m = pos[RID];
      dpath = `M${a.x},${a.y} Q${m.x},${a.y} ${m.x},${m.y} Q${m.x},${b.y} ${b.x},${b.y}`;
    } else {
      const off = (k % 2 ? 1 : -1) * Math.ceil(k / 2) * 9;
      const mx = (a.x + b.x) / 2 + off * 2.2;
      const my = (a.y + b.y) / 2 + (a.y === b.y ? off : 0);
      dpath = `M${a.x + off * 0.25},${a.y} Q${mx},${my} ${b.x + off * 0.25},${b.y}`;
    }
    const twoWay = /(?:⇄|<->)/.test(f.direction || '');
    paths += `<path d="${dpath}" class="${cls}" data-i="${idx}" stroke="${col}" fill="none"
      marker-end="url(#cl-arrow-${f.color})" ${twoWay ? `marker-start="url(#cl-arrow-${f.color}-r)"` : ''}/>
      <path d="${dpath}" class="cl-flow-hit" data-i="${idx}" stroke="transparent" stroke-width="12" fill="none"/>`;
  });
  const markers = Object.entries(COLORS).map(([k, c]) => `
    <marker id="cl-arrow-${k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M0,0L10,5L0,10z" fill="${c}"/></marker>
    <marker id="cl-arrow-${k}-r" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M0,0L10,5L0,10z" fill="${c}"/></marker>`).join('');
  const box = (n, label, sub, st, id) => {
    const p = pos[id]; const w = 150, h = 38;
    return `<g class="cl-mapnode" data-id="${esc(id)}" style="cursor:pointer">
      <rect x="${p.x - w / 2}" y="${p.y - h / 2}" width="${w}" height="${h}" rx="8" fill="var(--bg)" stroke="${S.sel === id ? 'var(--color-accent,#e06c75)' : 'var(--border)'}" stroke-width="${S.sel === id ? 2 : 1}"/>
      <circle cx="${p.x - w / 2 + 12}" cy="${p.y - 6}" r="4" class="cl-mapdot ${statusClass(st)}"/>
      <text x="${p.x - w / 2 + 22}" y="${p.y - 2}" font-weight="600">${esc(label)}</text>
      <text x="${p.x - w / 2 + 12}" y="${p.y + 12}" class="muted">${esc(sub)}</text></g>`;
  };
  const legend = Object.entries(kinds).map(([k, v], i) => `<g transform="translate(${12},${128 + i * 14})"><line x1="0" y1="0" x2="18" y2="0" stroke="${COLORS[v.color] || COLORS.gray}" stroke-width="3"/><text x="24" y="3" class="muted">${esc(v.label)}</text></g>`).join('');
  const pseudoSub = (id) => (
    id === 'clients' ? 'CLI / API / chat'
      : id === 'user' ? 'notifications'
      : id === 'odysseus' ? 'search / memory / notify'
      : id === 'frontier' ? 'Claude / Cursor / API'
      : 'external'
  );
  el.innerHTML = `<svg viewBox="0 0 ${W} ${H + 10}" style="height:auto;max-height:340px" preserveAspectRatio="xMidYMid meet">
    <defs>${markers}</defs>
    <text x="10" y="16" class="muted">CHAIN OF COMMAND — remote / manager / workers &middot; click a line to filter the log</text>
    ${paths}
    ${controllers.map((n) => box(n, n.name || n.id, `${n.role || 'remote'} · ${(n.metrics || {}).mode || n.ip || ''}`, n.status, n.id)).join('')}
    ${managers.map((n) => box(n, n.name || n.id, `${n.role} · ${n.ip || ''}`, n.status, n.id)).join('')}
    ${workers.map((n) => box(n, n.name || n.id, `${n.role} · ${n.ip || ''}`, n.status, n.id)).join('')}
    ${pseudo.map((id) => box(null, id === 'frontier' ? 'frontier agent' : id, pseudoSub(id), 'ok', id)).join('')}
    ${hasRouter ? box(null, (d.router && d.router.name) || 'Router', `${(d.router && d.router.ip) || ''} · ${((d.router && d.router.metrics) || {}).latency_ms ?? '—'} ms`, (d.router && d.router.status) || 'unknown', RID) : ''}
    ${legend}
  </svg>
  <style>
    .cl-flow{stroke-width:2;opacity:.28;transition:opacity .3s}
    .cl-flow.active{opacity:1;stroke-width:2.6;stroke-dasharray:7 6;animation:cl-dash 1s linear infinite;filter:drop-shadow(0 0 3px currentColor)}
    .cl-flow.error{opacity:.95;stroke-dasharray:2 4;stroke:${COLORS.red}!important}
    .cl-flow.hl{opacity:1;stroke-width:3.5}
    .cl-mapdot{fill:var(--color-muted)} .cl-mapdot.ok{fill:${COLORS.green}} .cl-mapdot.warn{fill:${COLORS.amber}} .cl-mapdot.bad{fill:${COLORS.red}}
    @keyframes cl-dash{to{stroke-dashoffset:-13}}
    @media (prefers-reduced-motion: reduce){.cl-flow.active{animation:none}}
  </style>`;
  const tip = document.getElementById('cl-tip');
  el.querySelectorAll('.cl-flow-hit').forEach((p) => {
    const f = d.flows[+p.dataset.i];
    const vis = el.querySelector(`.cl-flow[data-i="${p.dataset.i}"]`);
    p.addEventListener('mousemove', (e) => {
      vis?.classList.add('hl');
      tip.style.display = 'block';
      tip.style.left = (e.clientX + 14) + 'px';
      tip.style.top = (e.clientY + 10) + 'px';
      tip.innerHTML = `<b style="color:${COLORS[f.color]}">${esc(f.label)}</b><br>${esc(f.direction)}<br>
        status: <b>${esc(f.status)}</b> &middot; last: ${ago(f.last)}<br>${esc(f.detail || '')}${f.count != null ? ` &middot; count ${f.count}` : ''}${f.latency_ms != null ? ` &middot; ${num(f.latency_ms)} ms` : ''}`;
    });
    p.addEventListener('mouseleave', () => { tip.style.display = 'none'; vis?.classList.remove('hl'); });
    p.addEventListener('click', () => {
      S.logFilter = f.kind;
      S.tab = 'activity';
      const tgt = [f.to, f.from].find((x) => (S.data.nodes || []).some((n) => n.id === x && n.role !== 'manager')) || f.to;
      select(tgt, true);
    });
  });
  el.querySelectorAll('.cl-mapnode').forEach((g) => {
    if (known.has(g.dataset.id)) g.addEventListener('click', () => select(g.dataset.id));
  });
}

// ---------- side panel
function node() { return (S.data?.nodes || []).find((n) => n.id === S.sel); }
function select(id, keepFilter = false) {
  if (S.sel !== id) { S.logLines = []; S.logSince = ''; S.metrics = null; }
  if (!keepFilter) S.logFilter = null;
  S.sel = id;
  document.getElementById('cl-side').classList.add('open');
  render();
  renderSide();
}

function renderDeviceSide(dv, side) {
  const tpl = DEVICE_TEMPLATES[dv.type] || genericTemplate;
  side.innerHTML = `<div style="display:flex;align-items:center;gap:8px"><span class="cl-dot ${statusClass(dv.status)}"></span><b>${esc(dv.name)}</b><span class="cl-role">${esc(dv.type)}</span><span class="cl-sub">${esc(dv.ip || '')}</span>
      <button class="cl-btn" style="margin-left:auto" id="cl-side-close">&times;</button></div>
    <div class="cl-sub">${esc(dv.job || '')}</div>${tpl(dv)}
    ${(dv.checks || []).length ? `<h5 style="margin:10px 0 4px">Reachability checks</h5><table class="cl-table"><tr><th>target</th><th>port</th><th>RTT</th><th>status</th></tr>
      ${dv.checks.map((c) => `<tr><td>${esc(c.target)}</td><td>${esc(c.port ?? '—')}</td><td>${num(c.rtt_ms, 2)} ms</td><td>${c.reachable ? '<span class="cl-badge ok">up</span>' : '<span class="cl-badge bad">down</span>'}</td></tr>`).join('')}</table>` : ''}
    <h5 style="margin:10px 0 4px">Flows through this device</h5>
    ${(S.data.flows || []).filter((f) => f.via === dv.id || f.from === dv.id || f.to === dv.id).map((f) => `<div class="cl-alert"><b style="color:${COLORS[f.color]}">${esc(f.label)}</b> ${esc(f.direction)} &middot; ${esc(f.status)} &middot; ${esc(f.detail || '')}</div>`).join('') || '<div class="cl-empty">none</div>'}`;
  side.querySelector('#cl-side-close').addEventListener('click', () => { S.sel = null; side.classList.remove('open'); render(); });
}

function renderSide() {
  const n = node(), side = document.getElementById('cl-side');
  if (S.sel === '_onboard') { renderOnboard(side); return; }
  const dv = !n && (S.data?.devices || []).find((x) => x.id === S.sel);
  if (dv) { renderDeviceSide(dv, side); return; }
  if (!n) { side.classList.remove('open'); return; }
  const tabs = [['activity', 'Activity'], ['tasks', 'Tasks'], ['models', 'Models & health'], ['perf', 'Performance']];
  side.innerHTML = `<div style="display:flex;align-items:center;gap:8px"><span class="cl-dot ${statusClass(n.status)}"></span><b>${esc(n.name)}</b><span class="cl-role">${esc(n.role)}</span><span class="cl-sub">${esc(n.ip)}</span>
      <button class="cl-btn" style="margin-left:auto" id="cl-side-close">&times;</button></div>
    <div class="cl-tabs">${tabs.map(([k, l]) => `<button data-t="${k}" class="${S.tab === k ? 'active' : ''}">${l}</button>`).join('')}</div>
    <div id="cl-tab"></div>`;
  side.querySelector('#cl-side-close').addEventListener('click', () => { S.sel = null; side.classList.remove('open'); render(); });
  side.querySelectorAll('.cl-tabs button').forEach((b) => b.addEventListener('click', () => { S.tab = b.dataset.t; renderSide(); }));
  ({ activity: tabActivity, tasks: tabTasks, models: tabModels, perf: tabPerf })[S.tab]?.();
}

function tabActivity() {
  const t = document.getElementById('cl-tab');
  t.innerHTML = `<div class="row" style="display:flex;gap:6px;align-items:center;margin:6px 0">
      <span class="cl-sub">Live log (polling 2.5 s)</span>
      ${S.logFilter ? `<span class="cl-badge" style="border-color:${COLORS[(S.data.flow_kinds[S.logFilter] || {}).color]}">flow: ${esc(S.logFilter)} <a href="#" id="cl-clear-filter">&times;</a></span>` : ''}
    </div>
    <div class="cl-log" id="cl-log"></div>
    <div style="margin-top:8px">${taskForm()}</div>`;
  t.querySelector('#cl-clear-filter')?.addEventListener('click', (e) => { e.preventDefault(); S.logFilter = null; tabActivity(); });
  bindTaskForm();
  drawLog();
  loadLog(true);
}

async function loadLog(reset) {
  if (!S.sel) return;
  try {
    const r = await api(`/nodes/${encodeURIComponent(S.sel)}/log?n=400${!reset && S.logSince ? '&since=' + encodeURIComponent(S.logSince) : ''}`);
    const lines = r.lines || [];
    if (reset) S.logLines = lines; else S.logLines = S.logLines.concat(lines);
    if (r.next_since !== undefined && r.next_since !== null) S.logSince = String(r.next_since);
    else if (S.logLines.length) S.logSince = S.logLines[S.logLines.length - 1].ts || S.logSince;
    if (reset || lines.length) drawLog();
  } catch (e) { const l = document.getElementById('cl-log'); if (l) l.textContent = 'log error: ' + e.message; }
}

function drawLog() {
  const l = document.getElementById('cl-log'); if (!l) return;
  const rx = S.logFilter ? FLOW_FILTER[S.logFilter] : null;
  const rows = S.logLines.filter((r) => !rx || rx.test(`${r.kind || ''} ${r.msg || ''}`));
  l.innerHTML = rows.length ? rows.map((r) => `<div class="${esc(r.level)}"><span class="ts">${esc(local(r.ts))}</span>${r.node && S.sel === 'all' ? ' [' + esc(r.node) + ']' : ''}${r.kind ? ' <span class="ts">' + esc(r.kind) + '</span>' : ''} ${esc(r.msg)}</div>`).join('') : '<span class="ts">no log lines yet</span>';
  l.scrollTop = l.scrollHeight;
}

function taskForm() {
  const opts = (S.data?.nodes || []).map((n) => `<option value="${esc(n.id)}" ${n.id === S.sel ? 'selected' : ''}>${esc(n.name)}</option>`).join('');
  return `<form class="cl-form" id="cl-task-form"><b style="font-size:12px">Send a task</b>
    <textarea name="text" rows="3" placeholder="Describe the task... (manager classifies and routes; reviewer checks the result)"></textarea>
    <div class="row"><label>Target <select name="target"><option value="">auto (manager decides)</option>${opts}</select></label>
      <label>Type <select name="type"><option value="auto">auto</option><option value="code">code</option><option value="text">text</option></select></label>
      <button class="cl-btn primary" type="submit">Submit</button><span class="cl-sub" id="cl-task-msg"></span></div></form>`;
}
function bindTaskForm() {
  const f = document.getElementById('cl-task-form'); if (!f) return;
  f.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = document.getElementById('cl-task-msg');
    const body = { text: f.text.value, target: f.target.value, type: f.type.value };
    if (!body.text.trim()) { msg.textContent = 'enter a task'; return; }
    msg.textContent = 'submitting...';
    try {
      const r = await api('/tasks', { method: 'POST', body: JSON.stringify(body) });
      msg.textContent = `queued ${r.id || r.task_id || ''} (${r.status || 'ok'})`;
      f.text.value = '';
    } catch (err) { msg.textContent = 'Error: ' + err.message; }
  });
}

async function tabTasks() {
  const t = document.getElementById('cl-tab');
  t.innerHTML = `<div style="margin:6px 0">${taskForm()}</div><div id="cl-task-list" class="cl-empty">loading task history...</div>`;
  bindTaskForm();
  try {
    const r = await api(`/tasks?n=40&node=${encodeURIComponent(S.sel)}`);
    const rows = (r.live || []).concat(r.tasks || []);
    document.getElementById('cl-task-list').outerHTML = rows.length ? `<div>${rows.map(taskRow).join('')}</div>` : '<div class="cl-empty">No tasks for this node yet.</div>';
  } catch (e) { document.getElementById('cl-task-list').textContent = 'error: ' + e.message; }
}
function taskRow(t) {
  const at = t.attempts || [];
  const total = at.reduce((a, x) => a + (x.wall_s || 0), 0);
  const st = t.status || '?';
  const cls = /pass|done|ok/.test(st) ? 'ok' : /fail|human|error/.test(st) ? 'bad' : 'warn';
  return `<details class="cl-task-row" style="border-bottom:1px solid var(--border);padding:5px 0"><summary>
      <span class="cl-badge ${cls}">${esc(st)}</span> <b style="font-size:11px">${esc(t.id)}</b>
      <span class="cl-sub">${esc((t.classification || {}).category || t.type || '')} &middot; ${at.length} attempt(s) &middot; ${num((t.timings || {}).total_s ?? total)}s${(t.energy || {}).total_j ? ' &middot; ' + num(t.energy.total_j, 0) + ' J' : ''} &middot; ${ago(t.ts_submit || t.ts)}</span>
      ${t.text_preview ? `<div class="cl-sub" style="margin:2px 0 0 4px">${esc(t.text_preview)}</div>` : ''}</summary>
    <table class="cl-table"><tr><th>#</th><th>phase</th><th>node / model</th><th>tok/s</th><th>J/tok</th><th>wall</th><th>verdict</th></tr>
    ${at.map((a) => `<tr><td>${a.n ?? ''}</td><td>${esc(a.phase || '')}</td><td>${esc(a.node || a.path || '')}<br><span class="cl-sub">${esc(a.model || '')}</span></td>
      <td>${num(a.tok_s)}</td><td>${num(a.j_per_tok, 2)}</td><td>${num(a.wall_s)}s</td>
      <td>${a.verdict ? (a.verdict.pass ? '<span class="cl-badge ok">pass</span>' : `<span class="cl-badge bad">fail</span><div class="cl-sub">${esc((a.verdict.issues || []).join('; ').slice(0, 220))}</div>`) : '—'}</td></tr>`).join('')}
    </table></details>`;
}

function tabModels() {
  const n = node(), ol = n.ollama_info || {}, h = n.host || {};
  const t = document.getElementById('cl-tab');
  const link = (S.data.links || []).find((l) => l.to === n.id || l.from === n.id);
  const kv = (pairs) => `<dl class="cl-kv">${pairs.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join('')}</dl>`;
  t.innerHTML = `
    <h5 style="margin:8px 0 4px">Runtime</h5>${kv([
      ['System', esc([n.hardware?.system, n.hardware?.kernel].filter(Boolean).join(' · ') || '—')],
      ['Ollama', `${esc(ol.version || '—')} ${ol.reachable ? '<span class="cl-badge ok">up</span>' : '<span class="cl-badge bad">unreachable</span>'}`],
      ['Backend', esc(n.backend || '—')],
      ['Uptime', esc(dur(n.uptime_s ?? h.uptime_s)) + (n.service_uptime_s != null ? ` <span class="cl-sub">(service ${esc(dur(n.service_uptime_s))})</span>` : '')],
      ['Busy lock', esc(n.busy || '—')],
      ['Success', n.stats ? `${n.stats.task_success_rate != null ? Math.round(n.stats.task_success_rate * 100) + '% tasks' : '—'} &middot; ${n.stats.attempt_pass_rate != null ? Math.round(n.stats.attempt_pass_rate * 100) + '% attempts' : ''} <span class="cl-sub">(${esc(n.stats.window || '')}, ${esc(n.stats.attempts ?? 0)} attempts)</span>` : '—'],
      ['RAM', h.ram_total_gb ? `${num(h.ram_used_gb)} / ${num(h.ram_total_gb)} GB` : esc(n.ram_used_gb ? `${num(n.ram_used_gb)} / ${num(n.ram_total_gb)} GB` : '—')],
      ['VRAM', n.vram_total_gb ? `${num(n.vram_used_gb, 2)} / ${num(n.vram_total_gb, 1)} GB` : '—'],
      ['Disk', h.disk_total_gb ? `${num(h.disk_used_gb)} / ${num(h.disk_total_gb)} GB` : esc(n.disk_used_gb ? `${num(n.disk_used_gb)} / ${num(n.disk_total_gb)} GB` : '—')],
      ['GPU util / clock', `${num(n.gpu_util_pct, 0)}% / ${n.gpu_clock_mhz ? num(n.gpu_clock_mhz, 0) + ' MHz' : '—'} ${n.gpu_pstate ? '(' + esc(n.gpu_pstate) + ')' : ''}`],
      ['Temps', `GPU ${num(n.gpu_temp_c, 0)}&deg;C &middot; CPU ${num(n.cpu_temp_c, 0)}&deg;C`],
      ['Power', `${num(n.power_w)} W <span class="cl-sub">${esc(n.power_note || '')}</span>`],
      ['Queue depth', esc(n.queue_depth ?? '—')],
      ['Link', (n.link_latency_ms != null) ? `${num(n.link_latency_ms, 1)} ms to peer <span class="cl-sub">${esc(n.link_method || '')}</span>` : link ? `${link.latency_ms != null ? num(link.latency_ms, 2) + ' ms node-to-node' : ''} ${link.probe_rtt_ms != null ? '· probe ' + num(link.probe_rtt_ms) + ' ms' : ''}` : (ol.rtt_ms != null ? `probe ${num(ol.rtt_ms)} ms` : '—')],
      ['Heartbeat', n.heartbeat_age_s != null ? `${num(n.heartbeat_age_s, 0)} s ago` : '—'],
      ['Last benchmark', n.last_benchmark ? `${esc(n.last_benchmark.source || '')} · ${ago(n.last_benchmark.ts)}` : '—'],
      ['Last task', n.last_task ? `${esc(n.last_task.model)} · ${num(n.last_task.tok_s)} tok/s · ${num(n.last_task.j_per_tok, 2)} J/tok · ${ago(n.last_task.ts)}` : '—'],
    ])}
    <h5 style="margin:10px 0 4px">Loaded models</h5>
    ${(ol.loaded || []).length ? `<table class="cl-table"><tr><th>model</th><th>VRAM</th><th>RAM</th><th>ctx</th><th>keep-alive until</th></tr>
      ${ol.loaded.map((m) => `<tr><td>${esc(m.name)}</td><td>${num(m.vram_gb, 2)} GB</td><td>${num(m.ram_gb, 2)} GB</td><td>${esc(m.context_length || '—')}</td><td>${m.expires_at ? esc(local(m.expires_at)) : '—'}</td></tr>`).join('')}</table>` : '<div class="cl-empty">none loaded</div>'}
    <h5 style="margin:10px 0 4px">Installed models</h5>
    ${(ol.installed || []).length ? `<table class="cl-table"><tr><th>model</th><th>size</th><th>params</th><th>quant</th></tr>
      ${ol.installed.map((m) => `<tr><td>${esc(m.name)}</td><td>${num(m.size_gb, 1)} GB</td><td>${esc(m.params || '')}</td><td>${esc(m.quant || '')}</td></tr>`).join('')}</table>` : '<div class="cl-empty">unknown</div>'}
    <h5 style="margin:10px 0 4px">Per-model performance (cluster tasks)</h5>
    ${(n.models_perf || []).length ? `<table class="cl-table"><tr><th>model</th><th>tasks</th><th>success</th><th>first try</th><th>avg tok/s</th><th>avg J/tok</th></tr>
      ${n.models_perf.map((m) => `<tr><td>${esc(m.model)}</td><td>${m.tasks}</td><td>${m.success_rate != null ? Math.round(m.success_rate * 100) + '%' : '—'}</td><td>${m.first_try ?? '—'}</td><td>${num(m.avg_tok_s)}</td><td>${num(m.avg_j_per_tok, 2)}</td></tr>`).join('')}</table>` : '<div class="cl-empty">no completed tasks yet</div>'}
    <h5 style="margin:10px 0 4px">Alerts &amp; health</h5>
    ${(n.problems || []).map((p) => `<div class="cl-alert warn">${esc(p)}</div>`).join('')}
    ${(n.alerts || []).length ? n.alerts.slice().reverse().map((a) => `<div class="cl-alert ${esc(a.level)}"><span class="cl-sub">${esc(local(a.ts))}</span> ${esc(a.msg)}</div>`).join('') : '<div class="cl-empty">no alerts</div>'}`;
}

// ---------- performance charts + change log
const SERIES = [
  ['tok_s', 'Throughput (tok/s)', COLORS.green],
  ['j_per_tok', 'Energy (J/token)', COLORS.amber],
  ['power_w', 'Power (W)', COLORS.purple],
  ['gpu_temp_c', 'GPU temp (&deg;C)', COLORS.red],
  ['util_pct', 'GPU utilization (%)', COLORS.blue],
];
async function tabPerf() {
  const t = document.getElementById('cl-tab');
  t.innerHTML = `<div style="display:flex;gap:6px;align-items:center;margin:6px 0"><span class="cl-sub">Window</span>
      ${[6, 24, 168, 720].map((h) => `<button class="cl-btn ${S.hours === h ? 'primary' : ''}" data-h="${h}">${h < 48 ? h + 'h' : (h / 24) + 'd'}</button>`).join('')}
      <span class="cl-sub" id="cl-perf-src" style="margin-left:auto"></span></div>
    <div id="cl-charts" class="cl-empty">loading...</div>
    <h5 style="margin:12px 0 4px">Change log (tunings, upgrades, fixes) — markers on the charts</h5>
    <div id="cl-changes"></div>
    <form class="cl-form" id="cl-change-form" style="margin-top:8px"><b style="font-size:12px">Log a change</b>
      <div class="row"><input name="title" placeholder="e.g. eco power mode, OLLAMA_FLASH_ATTENTION=1, new fan" style="flex:1"/>
        <select name="kind"><option>tuning</option><option>upgrade</option><option>fix</option><option>hardware</option><option>model</option></select>
        <select name="node"><option value="${esc(S.sel)}">${esc(S.sel)}</option><option value="all">all nodes</option></select></div>
      <div class="row"><input name="before" placeholder="before: tok_s=14.9, j_per_tok=2.36 (blank = computed)" style="flex:1"/><input name="after" placeholder="after: tok_s=15.6, j_per_tok=2.2 (blank = computed)" style="flex:1"/></div>
      <textarea name="detail" rows="2" placeholder="details / why / how to reproduce"></textarea>
      <div class="row"><button class="cl-btn primary" type="submit">Save change</button><span class="cl-sub" id="cl-change-msg"></span></div></form>`;
  t.querySelectorAll('[data-h]').forEach((b) => b.addEventListener('click', () => { S.hours = +b.dataset.h; S.metrics = null; tabPerf(); }));
  t.querySelector('#cl-change-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const f = e.target, msg = document.getElementById('cl-change-msg');
    const body = Object.fromEntries(new FormData(f).entries());
    if (!body.title.trim()) { msg.textContent = 'title required'; return; }
    try {
      const r = await api('/changes', { method: 'POST', body: JSON.stringify(body) });
      msg.textContent = 'saved' + (r.forwarded_to_manager ? ' + sent to manager' : ' (Odysseus store)');
      f.reset();
      tabPerf();
    } catch (err) { msg.textContent = 'Error: ' + err.message; }
  });
  try {
    const [m, c] = await Promise.all([
      api(`/nodes/${encodeURIComponent(S.sel)}/metrics?hours=${S.hours}`),
      api(`/changes?node=${encodeURIComponent(S.sel)}`),
    ]);
    S.metrics = m.series || [];
    S.changes = c.changes || [];
    document.getElementById('cl-perf-src').textContent = 'data: ' + (m.sources || []).join(', ');
    drawCharts();
  } catch (e) { document.getElementById('cl-charts').textContent = 'error: ' + e.message; }
}

const fmtBA = (v) => {
  if (v === null || v === undefined || v === '') return '';
  if (typeof v !== 'object') return esc(v);
  const keys = ['tok_s', 'j_per_tok', 'power_w', 'gpu_temp_c', 'pass_rate', 'note'];
  return keys.filter((k) => v[k] !== undefined && v[k] !== null).map((k) => `${k} ${typeof v[k] === 'number' ? num(v[k], 2) : esc(v[k])}`).join(', ') || '...';
};
const fmtDelta = (d) => d ? Object.entries(d).filter(([, v]) => v !== null && v !== undefined).map(([k, v]) => `${k} ${v > 0 ? '+' : ''}${num(v, 1)}%`).join(', ') : '';
function drawCharts() {
  const box = document.getElementById('cl-charts'); if (!box) return;
  const t1 = Date.now(); let t0 = t1 - S.hours * 3600e3;
  const allTs = (S.metrics || []).map((p) => new Date(p.ts).getTime()).filter((t) => t >= t0);
  if (allTs.length && (t1 - Math.min(...allTs)) < 0.2 * (t1 - t0)) t0 = Math.min(...allTs) - 60e3;
  const changes = S.changes.filter((c) => { const t = new Date(c.ts).getTime(); return t >= t0 && t <= t1; });
  box.className = '';
  box.innerHTML = SERIES.map(([key, label, col]) => {
    const pts = S.metrics.map((p) => [new Date(p.ts).getTime(), p[key]]).filter(([t, v]) => v !== null && v !== undefined && t >= t0);
    if (!pts.length) return `<div class="cl-chart"><h5><span>${label}</span><span>no data</span></h5></div>`;
    const W = 500, H = 90, pad = 4;
    const vs = pts.map((p) => +p[1]);
    let lo = Math.min(...vs), hi = Math.max(...vs);
    if (hi === lo) { hi += 1; lo -= 1; }
    const X = (t) => pad + ((t - t0) / (t1 - t0)) * (W - 2 * pad);
    const Y = (v) => H - 12 - ((v - lo) / (hi - lo)) * (H - 22);
    const line = pts.map(([t, v], i) => `${i ? 'L' : 'M'}${X(t).toFixed(1)},${Y(v).toFixed(1)}`).join(' ');
    const dots = pts.length < 60 ? pts.map(([t, v]) => `<circle cx="${X(t).toFixed(1)}" cy="${Y(v).toFixed(1)}" r="2" fill="${col}"><title>${local(t)}: ${num(v, 2)}</title></circle>`).join('') : '';
    const marks = changes.map((c) => {
      const x = X(new Date(c.ts).getTime());
      return `<line class="marker" x1="${x}" x2="${x}" y1="2" y2="${H - 10}"><title>${esc(c.title)} (${esc(c.kind)}) ${fmtBA(c.before)} &rarr; ${fmtBA(c.after)}</title></line><text x="${x + 2}" y="9">${esc((c.title || '').slice(0, 18))}</text>`;
    }).join('');
    let ba = '';
    if (changes.length) {
      const ct = new Date(changes[changes.length - 1].ts).getTime();
      const b = pts.filter(([t]) => t < ct).map((p) => +p[1]), a = pts.filter(([t]) => t >= ct).map((p) => +p[1]);
      const avg = (x) => x.reduce((s, v) => s + v, 0) / x.length;
      if (b.length && a.length) ba = ` &middot; before ${num(avg(b), 2)} &rarr; after ${num(avg(a), 2)} (${((avg(a) / avg(b)) * 100 - 100).toFixed(1)}%)`;
    }
    return `<div class="cl-chart"><h5><span>${label}</span><span>last ${num(vs[vs.length - 1], 2)} &middot; min ${num(Math.min(...vs), 2)} &middot; max ${num(Math.max(...vs), 2)}${ba}</span></h5>
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"><line class="axis" x1="0" x2="${W}" y1="${H - 11}" y2="${H - 11}"/>${marks}
      <path d="${line}" fill="none" stroke="${col}" stroke-width="1.6" vector-effect="non-scaling-stroke"/>${dots}</svg></div>`;
  }).join('');
  document.getElementById('cl-changes').innerHTML = S.changes.length ? `<table class="cl-table"><tr><th>when</th><th>node</th><th>kind</th><th>change</th><th>before &rarr; after</th></tr>
    ${S.changes.slice().reverse().map((c) => `<tr><td>${esc(local(c.ts))}</td><td>${esc(c.node)}</td><td>${esc(c.kind)}</td><td>${esc(c.title)}${(c.detail || c.description) ? `<div class="cl-sub">${esc(String(c.detail || c.description).slice(0, 200))}</div>` : ''}</td><td>${fmtBA(c.before)} &rarr; ${fmtBA(c.after)}${c.delta_pct ? `<div class="cl-sub">${esc(fmtDelta(c.delta_pct))}</div>` : ''}</td></tr>`).join('')}</table>` : '<div class="cl-empty">No changes logged yet.</div>';
}

function openOnboard() {
  S.sel = '_onboard';
  S.tab = 'onboard';
  const side = document.getElementById('cl-side');
  if (side) side.classList.add('open');
  render();
  renderOnboard(side);
}

function renderMd(src) {
  const lines = String(src || '').replace(/\r\n/g, '\n').split('\n');
  let html = '', inCode = false, code = [], bullets = [], table = [];
  const inline = (s) => esc(s)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  const flushBullets = () => {
    if (!bullets.length) return;
    html += '<ul>' + bullets.map((b) => `<li>${inline(b)}</li>`).join('') + '</ul>';
    bullets = [];
  };
  const flushTable = () => {
    if (!table.length) return;
    const rows = table.filter((r) => !r.every((c) => /^:?-+:?$/.test(c)));
    table = [];
    if (!rows.length) return;
    const [head, ...body] = rows;
    html += '<table class="cl-table"><thead><tr>' + head.map((c) => `<th>${inline(c)}</th>`).join('') + '</tr></thead><tbody>'
      + body.map((r) => '<tr>' + r.map((c) => `<td>${inline(c)}</td>`).join('') + '</tr>').join('') + '</tbody></table>';
  };
  for (const raw of lines) {
    if (raw.startsWith('```')) {
      if (inCode) { html += `<pre class="cl-code"><code>${esc(code.join('\n'))}</code></pre>`; code = []; inCode = false; }
      else { flushBullets(); flushTable(); inCode = true; }
      continue;
    }
    if (inCode) { code.push(raw); continue; }
    if (/^\|/.test(raw)) { flushBullets(); table.push(raw.split('|').slice(1, -1).map((c) => c.trim())); continue; }
    flushTable();
    if (/^\s*[-*]\s+/.test(raw)) { bullets.push(raw.replace(/^\s*[-*]\s+/, '')); continue; }
    flushBullets();
    if (/^###\s+/.test(raw)) html += `<h5>${inline(raw.replace(/^###\s+/, ''))}</h5>`;
    else if (/^##\s+/.test(raw)) html += `<h4>${inline(raw.replace(/^##\s+/, ''))}</h4>`;
    else if (/^#\s+/.test(raw)) html += `<h3>${inline(raw.replace(/^#\s+/, ''))}</h3>`;
    else if (raw.trim() !== '') html += `<p>${inline(raw)}</p>`;
  }
  flushBullets();
  flushTable();
  if (inCode) html += `<pre class="cl-code"><code>${esc(code.join('\n'))}</code></pre>`;
  return html || '<div class="cl-empty">No content.</div>';
}

function renderOnboard(side) {
  side = side || document.getElementById('cl-side');
  if (!side) return;
  side.classList.add('open');
  if (side.dataset.panel === 'onboard' && side.querySelector('#cl-onboard-json')) return;
  const inst = S.onboardInst || {};
  side.innerHTML = `
    <div style="display:flex;align-items:center;gap:8px">
      <b>Add device</b><span class="cl-role">onboard</span>
      <button class="cl-btn" style="margin-left:auto" id="cl-side-close">&times;</button>
    </div>
    <div class="cl-sub">Probe a new machine, paste the JSON, and get a role / model / checklist recommendation. Registering only writes devices.json (no sudo).</div>
    <div class="cl-md" id="cl-onboard-inst">${inst.markdown ? renderMd(inst.markdown) : '<div class="cl-empty">Loading instructions...</div>'}</div>
    <form class="cl-form" id="cl-onboard-form">
      <label>Probe JSON <span class="cl-sub">(paste or upload probe.sh output)</span></label>
      <div class="row">
        <input type="file" id="cl-onboard-file" accept=".json,application/json,text/plain"/>
        <button type="button" class="cl-btn" id="cl-onboard-clear">Clear</button>
      </div>
      <textarea id="cl-onboard-json" rows="10" placeholder='{"schema":"llm-cluster-probe/1", ...}'></textarea>
      <div class="row">
        <button class="cl-btn primary" type="submit" id="cl-onboard-advise">Recommend</button>
        <span class="cl-sub" id="cl-onboard-msg"></span>
      </div>
    </form>
    <div id="cl-onboard-result"></div>`;
  side.dataset.panel = 'onboard';
  side.querySelector('#cl-side-close').addEventListener('click', () => {
    S.sel = null; side.dataset.panel = ''; side.classList.remove('open'); render();
  });
  side.querySelector('#cl-onboard-file').addEventListener('change', (e) => {
    const f = e.target.files && e.target.files[0];
    if (!f) return;
    const r = new FileReader();
    r.onload = () => { side.querySelector('#cl-onboard-json').value = String(r.result || ''); };
    r.readAsText(f);
  });
  side.querySelector('#cl-onboard-clear').addEventListener('click', () => {
    side.querySelector('#cl-onboard-json').value = '';
    side.querySelector('#cl-onboard-result').innerHTML = '';
    side.querySelector('#cl-onboard-msg').textContent = '';
  });
  side.querySelector('#cl-onboard-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = side.querySelector('#cl-onboard-msg');
    const raw = side.querySelector('#cl-onboard-json').value.trim();
    if (!raw) { msg.textContent = 'paste or upload probe JSON first'; return; }
    let probe;
    try { probe = JSON.parse(raw); }
    catch (err) { msg.textContent = 'invalid JSON: ' + err.message; return; }
    msg.textContent = 'asking the advisor...';
    try {
      const r = await api('/onboard/advise', { method: 'POST', body: JSON.stringify({ probe }) });
      S.onboardAdvice = r.advice || r;
      msg.textContent = 'recommendation ready';
      drawOnboardAdvice(side, S.onboardAdvice);
    } catch (err) { msg.textContent = 'Error: ' + err.message; }
  });
  if (!inst.markdown) {
    api('/onboard/instructions').then((r) => {
      S.onboardInst = r;
      const el = document.getElementById('cl-onboard-inst');
      if (el) el.innerHTML = renderMd(r.markdown || '');
    }).catch((err) => {
      const el = document.getElementById('cl-onboard-inst');
      if (el) el.innerHTML = `<div class="cl-empty">Could not load instructions (${esc(err.message)}). Run <code>./probe.sh --pretty</code> on the new machine, then paste the JSON below. Docs: contrib/cluster-network/onboard/ONBOARDING.md</div>`;
    });
  }
}

function drawOnboardAdvice(side, advice) {
  const box = side.querySelector('#cl-onboard-result');
  if (!box || !advice) return;
  const d = advice.device || {};
  const mem = advice.memory || {};
  const models = advice.models || [];
  const flows = advice.flows || [];
  const rec = models.filter((m) => m.recommend);
  box.innerHTML = `
    <div class="cl-card" style="cursor:default">
      <div class="cl-card-head"><span class="cl-name">${esc(d.id || '')}</span><span class="cl-role">${esc(advice.role || '')}</span>
        <span class="cl-badge">${esc(advice.confidence || '')}</span>
        ${advice.already_registered ? '<span class="cl-badge warn">already registered</span>' : ''}
        ${advice.protected ? '<span class="cl-badge bad">protected</span>' : ''}
      </div>
      <div class="cl-sub">${esc(d.job || '')}</div>
      <div class="cl-kv">
        <dt>type</dt><dd>${esc(advice.device_type || d.type || '')}</dd>
        <dt>hardware</dt><dd>${esc([(d.hardware || {}).cpu, (d.hardware || {}).gpu, (d.hardware || {}).ram_gb != null ? (d.hardware || {}).ram_gb + ' GB RAM' : null].filter(Boolean).join(' · '))}</dd>
        <dt>usable</dt><dd>${esc(mem.kind || '')} &middot; VRAM ${num(mem.usable_vram_gb)} GB &middot; RAM ${num(mem.ram_gb)} GB</dd>
        <dt>IP</dt><dd>${esc(d.ip || '—')}</dd>
      </div>
      ${(advice.reasons || []).length ? `<ul class="cl-reasons">${(advice.reasons || []).map((r) => `<li>${esc(r)}</li>`).join('')}</ul>` : ''}
      <p class="cl-sub">${esc(advice.position || '')}</p>
    </div>
    ${rec.length || models.length ? `<h5 style="margin:10px 0 4px">Models</h5>
      <table class="cl-table"><tr><th>model</th><th>quant</th><th>~VRAM</th><th>fits?</th><th>why</th></tr>
      ${models.map((m) => `<tr><td><code>${esc(m.name)}</code></td><td>${esc(m.quant || '')}</td><td>${num(m.vram_gb)} GB</td>
        <td>${m.recommend ? '<span class="cl-badge ok">yes</span>' : '<span class="cl-badge">no</span>'} <span class="cl-sub">${esc(m.fit_reason || '')}</span></td>
        <td>${esc(m.why || '')}</td></tr>`).join('')}</table>` : '<div class="cl-empty">No catalog models recommended for this role.</div>'}
    ${flows.length ? `<h5 style="margin:10px 0 4px">Flows to add</h5>
      <table class="cl-table"><tr><th>id</th><th>from &rarr; to</th><th>type</th><th>job</th></tr>
      ${flows.map((f) => `<tr><td><code>${esc(f.id)}</code></td><td>${esc(f.from)} ${f.direction === 'bidirectional' ? '&harr;' : '&rarr;'} ${esc(f.to)}</td>
        <td>${esc(f.type)}</td><td>${esc(f.label)}</td></tr>`).join('')}</table>` : ''}
    <h5 style="margin:10px 0 4px">Setup checklist</h5>
    <div class="cl-md">${renderMd(advice.checklist_md || '')}</div>
    ${advice.protected || advice.already_registered ? `<div class="cl-sub">This id is already in the registry${advice.protected ? ' and is protected' : ''}. Registration is disabled here.</div>` : `
    <form class="cl-form" id="cl-onboard-apply">
      <label class="row"><input type="checkbox" id="cl-onboard-confirm"/> I have read the checklist. Register this device in devices.json (no sudo).</label>
      <div class="row"><button class="cl-btn primary" type="submit">Register device</button><span class="cl-sub" id="cl-onboard-apply-msg"></span></div>
    </form>`}`;
  const form = box.querySelector('#cl-onboard-apply');
  if (!form) return;
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = box.querySelector('#cl-onboard-apply-msg');
    if (!box.querySelector('#cl-onboard-confirm')?.checked) { msg.textContent = 'tick the confirmation box first'; return; }
    msg.textContent = 'registering...';
    try {
      const r = await api('/onboard/apply', { method: 'POST', body: JSON.stringify({ advice, confirm: true }) });
      msg.textContent = `registered ${r.id || d.id || ''}`;
      refresh(true);
    } catch (err) { msg.textContent = 'Error: ' + err.message; }
  });
}

// ---------- boot (dormant unless ODYSSEUS_CLUSTER_ENABLED=1)
async function boot() {
  try {
    const r = await fetch(API + '/config', { credentials: 'same-origin' });
    if (!r.ok) return;
    const cfg = await r.json();
    if (!cfg || !cfg.enabled) return;
  } catch {
    return;
  }
  inject();
  if (location.pathname.toLowerCase() === '/cluster') {
    document.title = 'Cluster — Odysseus';
    setTimeout(open, 300);
  }
}
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
window.OdysseusCluster = { open, close, toggle };
export { open, close, toggle };
