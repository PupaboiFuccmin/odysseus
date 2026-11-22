// Optional Cluster Network: host badges, model-picker grouping, reply
// "ran on" footers, and a remote-session banner. Models always run on
// cluster machines, never on a remote controller. This file only labels
// that, and stays dormant when /api/cluster-network/hosts reports disabled.

const LS_COLLAPSE = 'odysseus-cluster-host-picker';
const SS_BANNER = 'odysseus-cluster-remote-banner-dismissed';
const CHEVRON = '<svg class="clh-chevron" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="6 9 12 15 18 9"/></svg>';

const VENDOR_DOT = {
  nvidia: '#76b900',
  amd: '#ed1c24',
  intel: '#0071c5',
  tegra: '#76b900',
};

let hosts = null;
let lastClusterMeta = null; // most recent orchestrator "cluster" payload from a chat stream
let rebuilding = false;

function loadCollapse() {
  try { return JSON.parse(localStorage.getItem(LS_COLLAPSE) || '{}') || {}; } catch { return {}; }
}
function saveCollapse(state) {
  try { localStorage.setItem(LS_COLLAPSE, JSON.stringify(state)); } catch { /* quota */ }
}
function isCollapsed(key, defaultCollapsed = false) {
  const st = loadCollapse();
  if (Object.prototype.hasOwnProperty.call(st, key)) return !!st[key];
  return defaultCollapsed;
}
function toggleCollapsed(key, defaultCollapsed = false) {
  const st = loadCollapse();
  st[key] = !isCollapsed(key, defaultCollapsed);
  saveCollapse(st);
  return st[key];
}

async function refreshHosts() {
  try {
    const r = await fetch('/api/cluster-network/hosts', { credentials: 'same-origin' });
    if (!r.ok) return hosts;
    hosts = await r.json();
    return hosts;
  } catch {
    return hosts;
  }
}

function currentRoute() {
  const sm = window.sessionModule;
  if (!sm) return { model: '', endpointId: '', url: '' };
  const sid = sm.getCurrentSessionId && sm.getCurrentSessionId();
  const pending = sm.getPendingChat && sm.getPendingChat();
  if (sid && sm.getSessions) {
    const s = (sm.getSessions() || []).find(x => x.id === sid);
    if (s && s.model) return { model: s.model, endpointId: s.endpoint_id || '', url: s.endpoint_url || '' };
  }
  if (pending && pending.modelId) return { model: pending.modelId, endpointId: pending.endpointId || '', url: pending.url || '' };
  return { model: '', endpointId: '', url: '' };
}

function hostAliases(h) {
  if (!h) return [];
  const out = [h.id, h.name, h.bridge_prefix, h.short_label];
  if (Array.isArray(h.aliases)) out.push(...h.aliases);
  return out.filter(Boolean).map(String);
}

function findHostById(id) {
  if (!hosts || !id) return null;
  const want = String(id);
  if (hosts.manager && hostAliases(hosts.manager).includes(want)) return hosts.manager;
  return (hosts.workers || []).find(w => hostAliases(w).includes(want)) || null;
}

function hostsEnabled() {
  return !!(hosts && hosts.enabled);
}

function resolveHostForModel(modelId) {
  if (!modelId || !hosts) return null;
  const raw = String(modelId);
  const bare = raw.split('/').pop();
  if (raw === (hosts.cluster_network && hosts.cluster_network.model) || raw === 'cluster-network' || raw === 'Cluster Network') {
    return { kind: 'cluster', id: 'cluster-network', label: (hosts.cluster_network && hosts.cluster_network.label) || 'Cluster (routed)', online: !!(hosts.cluster_network && hosts.cluster_network.online), model: 'cluster-network' };
  }
  if (hosts.manager && (hosts.manager.models || []).some(m => m.id === raw || m.route_model === raw || m.id === bare)) {
    const m = hosts.manager.models.find(x => x.id === raw || x.route_model === raw || x.id === bare);
    return { kind: 'manager', id: hosts.manager.id, label: hosts.manager.label, online: hosts.manager.online, model: (m && m.display) || bare, host: hosts.manager };
  }
  for (const w of hosts.workers || []) {
    const m = (w.models || []).find(x => x.id === raw || x.route_model === raw || x.id === bare);
    if (m) return { kind: 'worker', id: w.id, label: w.label, online: w.online, model: m.display || bare, host: w };
  }
  // Prefix fallback: worker-prefix/model, using each host's id, aliases, and bridge_prefix
  const pref = raw.includes('/') ? raw.split('/', 1)[0] : '';
  if (pref) {
    const hit = findHostById(pref);
    if (hit) {
      const kind = (hosts.manager && hit.id === hosts.manager.id) ? 'manager' : 'worker';
      return { kind, id: hit.id, label: hit.label, online: hit.online, model: bare, host: hit };
    }
  }
  return null;
}

function vendorMark(vendor) {
  const color = VENDOR_DOT[(vendor || '').toLowerCase()];
  if (!color) return '';
  const span = document.createElement('span');
  span.className = 'clh-vendor';
  span.style.background = color;
  span.title = vendor;
  span.setAttribute('aria-hidden', 'true');
  return span;
}

function badgeEl(label, { online = true, routed = false } = {}) {
  const el = document.createElement('span');
  el.className = 'clh-badge' + (online ? '' : ' offline') + (routed ? ' routed' : '');
  const dot = document.createElement('span');
  dot.className = 'clh-dot';
  dot.setAttribute('aria-hidden', 'true');
  el.appendChild(dot);
  el.appendChild(document.createTextNode(label));
  return el;
}

function matchesQuery(q, ...parts) {
  if (!q) return true;
  return parts.filter(Boolean).join(' ').toLowerCase().includes(q);
}

function closePicker() {
  const menu = document.getElementById('model-picker-menu');
  const btn = document.getElementById('model-picker-btn');
  if (document.activeElement) document.activeElement.blur();
  if (menu) menu.classList.add('hidden');
  if (btn) btn.setAttribute('aria-expanded', 'false');
}

async function pickModel(m) {
  const sm = window.sessionModule;
  const mid = m.route_model || m.id;
  const url = m.endpoint_url || '';
  const endpointId = m.endpoint_id || '';
  const display = m.display || mid;
  try {
    window.__odysseusLastPickedRoute = {
      model: mid,
      endpoint_url: url,
      endpoint_id: endpointId,
      display,
      picked_at: Date.now(),
      session_id: (sm && sm.getCurrentSessionId && sm.getCurrentSessionId()) || null,
    };
  } catch { /* ignore */ }
  try { document.dispatchEvent(new CustomEvent('odysseus:model-picked', { detail: { mid, url, endpointId, display } })); } catch { /* ignore */ }
  closePicker();
  if (!sm) return;
  const sid = sm.getCurrentSessionId && sm.getCurrentSessionId();
  try {
    if (!sid) {
      if (sm.createDirectChat) await sm.createDirectChat(url, mid, endpointId, { source: 'manual' });
    } else {
      const sessions = sm.getSessions ? sm.getSessions() : [];
      const s = sessions.find(x => x.id === sid);
      if (s) { s.model = mid; s.endpoint_url = url; s.endpoint_id = endpointId; s.endpoint_name = m.endpoint_name || ''; }
      const fd = new FormData();
      fd.append('model', mid);
      fd.append('endpoint_url', url);
      if (endpointId) fd.append('endpoint_id', endpointId);
      const res = await fetch(`/api/session/${sid}`, { method: 'PATCH', body: fd, credentials: 'same-origin' });
      if (!res.ok) {
        if (window.uiModule && window.uiModule.showError) window.uiModule.showError('Failed to set model');
        return;
      }
      if (sm.updateModelPicker) sm.updateModelPicker();
      if (window.refreshChatContextHeader) window.refreshChatContextHeader('model-pick');
    }
    if (window.uiModule && window.uiModule.showToast) window.uiModule.showToast('Using ' + display);
  } catch (e) {
    if (window.uiModule && window.uiModule.showError) window.uiModule.showError('Failed to set model: ' + e);
  }
  enhanceLabel();
}

function addHeader(listEl, { key, title, subtitle, defaultCollapsed, extraClass }) {
  const collapsed = isCollapsed(key, !!defaultCollapsed);
  const header = document.createElement('button');
  header.type = 'button';
  header.className = 'clh-header' + (extraClass ? ' ' + extraClass : '') + (collapsed ? ' collapsed' : '');
  header.setAttribute('aria-expanded', String(!collapsed));
  header.innerHTML = CHEVRON;
  const text = document.createElement('span');
  text.className = 'clh-header-title';
  text.textContent = title;
  header.appendChild(text);
  if (subtitle) {
    const sub = document.createElement('span');
    sub.className = 'clh-header-sub';
    sub.textContent = subtitle;
    header.appendChild(sub);
  }
  header.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleCollapsed(key, !!defaultCollapsed);
    const list = document.getElementById('model-picker-list');
    if (list) rebuildList(list, true);
  });
  listEl.appendChild(header);
  return !collapsed;
}

function addModelRow(listEl, m, host, { routed = false } = {}) {
  const row = document.createElement('div');
  row.className = 'model-switch-item clh-row';
  const mid = m.route_model || m.id;
  row.dataset.model = mid;
  const cur = currentRoute();
  if (cur.model && (cur.model === mid || cur.model === m.id || cur.model.split('/').pop() === m.display)) {
    row.classList.add('clh-selected');
  }
  const online = m.online !== false && (!host || host.online !== false);
  if (!online) {
    row.classList.add('model-switch-stale', 'clh-offline');
    row.title = (host && host.label ? host.label + ' is offline' : 'Host appears offline') + '. You can still try, but the model will not run on this device.';
  } else {
    row.title = (routed ? 'Routes across the cluster. ' : 'Runs on ' + ((host && host.label) || '') + '. ') + 'Never runs on a remote controller.';
  }
  const name = document.createElement('span');
  name.className = 'mp-model-name';
  name.textContent = m.display || m.id;
  row.appendChild(name);
  const meta = [];
  if (m.params) meta.push(m.params);
  if (m.quant) meta.push(m.quant);
  if (meta.length) {
    const info = document.createElement('span');
    info.className = 'clh-meta';
    info.textContent = meta.join(' · ');
    row.appendChild(info);
  }
  const label = routed ? ((hosts && hosts.cluster_network && hosts.cluster_network.label) || 'Cluster (routed)') : (host && host.label) || '';
  if (label) row.appendChild(badgeEl(label, { online, routed }));
  row.addEventListener('click', () => pickModel(m));
  listEl.appendChild(row);
}

function rebuildList(listEl, fromUs) {
  if (!listEl || !hosts) return;
  if (rebuilding) return;
  const search = document.getElementById('model-picker-search');
  const q = (search && search.value || '').trim().toLowerCase();
  rebuilding = true;
  try {
    listEl.dataset.clusterHostLayout = '1';
    listEl.classList.remove('is-loading', 'is-empty');
    listEl.innerHTML = '';

    const cn = hosts.cluster_network || {};
    const mgr = hosts.manager;
    const workers = hosts.workers || [];

    // Cluster Network (top-level, expandable): routed model + manager models
    const mgrModels = ((mgr && mgr.models) || []).filter(m =>
      matchesQuery(q, m.id, m.display, m.route_model, mgr && mgr.label, mgr && mgr.name, mgr && mgr.short_label, 'cluster network'));
    const showRouted = matchesQuery(q, 'cluster network', 'routed', cn.model, cn.label);
    const clusterCount = (showRouted ? 1 : 0) + mgrModels.length;
    if (clusterCount > 0 || !q) {
      const open = addHeader(listEl, {
        key: 'cluster-network',
        title: 'Cluster Network',
        subtitle: mgr ? (mgr.online ? mgr.label : mgr.label + ' · offline') : '',
        extraClass: 'clh-top',
      });
      if (open) {
        const group = document.createElement('div');
        group.className = 'clh-group';
        const tmp = document.createElement('div');
        if (showRouted) {
          addModelRow(tmp, {
            id: cn.model || 'cluster-network',
            display: 'Cluster Network',
            route_model: cn.model || 'cluster-network',
            endpoint_id: cn.endpoint_id,
            endpoint_url: cn.endpoint_url,
            endpoint_name: cn.endpoint_name,
            online: cn.online !== false,
          }, mgr, { routed: true });
        }
        mgrModels.forEach(m => addModelRow(tmp, m, mgr));
        while (tmp.firstChild) group.appendChild(tmp.firstChild);
        if (!group.childElementCount) {
          const empty = document.createElement('div');
          empty.className = 'clh-empty';
          empty.textContent = mgr && !mgr.online ? ((mgr.short_label || mgr.label || 'Manager') + ' is offline') : 'No models on the manager';
          group.appendChild(empty);
        }
        listEl.appendChild(group);
      }
    }

    // Workers (collapsible), with a sub-group per worker machine
    const visibleWorkers = workers.filter(w => {
      if (!q) return true;
      if (matchesQuery(q, w.id, w.name, w.label, w.short_label, 'workers')) return true;
      return (w.models || []).some(m => matchesQuery(q, m.id, m.display, m.route_model, w.label));
    });
    if (visibleWorkers.length || (!q && workers.length === 0)) {
      const total = visibleWorkers.reduce((n, w) => n + ((w.models || []).length), 0);
      const open = addHeader(listEl, {
        key: 'workers',
        title: 'Workers',
        subtitle: visibleWorkers.length ? (visibleWorkers.length + ' machine' + (visibleWorkers.length === 1 ? '' : 's') + ' · ' + total + ' model' + (total === 1 ? '' : 's')) : 'none yet',
        extraClass: 'clh-top',
      });
      if (open) {
        if (!visibleWorkers.length) {
          const empty = document.createElement('div');
          empty.className = 'clh-empty';
          empty.textContent = 'No worker machines yet. They appear here from devices.json.';
          listEl.appendChild(empty);
        }
        visibleWorkers.forEach(w => {
          const models = (w.models || []).filter(m =>
            !q || matchesQuery(q, m.id, m.display, m.route_model, w.label, w.name));
          const wopen = addHeader(listEl, {
            key: 'worker:' + w.id,
            title: w.label || w.name,
            subtitle: w.online ? ((models.length || (w.models || []).length) + ' model' + ((models.length || (w.models || []).length) === 1 ? '' : 's')) : 'offline',
            extraClass: 'clh-sub' + (w.online ? '' : ' offline'),
          });
          if (!wopen) return;
          const group = document.createElement('div');
          group.className = 'clh-group clh-sub-group';
          const tmp = document.createElement('div');
          models.forEach(m => addModelRow(tmp, m, w));
          while (tmp.firstChild) group.appendChild(tmp.firstChild);
          if (!group.childElementCount) {
            const empty = document.createElement('div');
            empty.className = 'clh-empty';
            empty.textContent = w.online ? 'No chat models reported' : 'Offline — models will show when this worker is back';
            group.appendChild(empty);
          }
          listEl.appendChild(group);
        });
      }
    }

    if (q && !listEl.querySelector('.clh-row')) {
      const empty = document.createElement('div');
      empty.className = 'model-switch-empty';
      empty.textContent = 'No matching models';
      listEl.appendChild(empty);
    }
  } finally {
    rebuilding = false;
  }
}

function hookPicker() {
  if (!hostsEnabled()) return;
  const menu = document.getElementById('model-picker-menu');
  const list = document.getElementById('model-picker-list');
  if (!menu || !list || menu.dataset.clusterHostBound === '1') return;
  menu.dataset.clusterHostBound = '1';

  const takeOver = () => {
    if (!hostsEnabled()) return;
    if (list.classList.contains('is-loading')) return;
    if (menu.classList.contains('hidden')) return;
    rebuildList(list);
  };

  new MutationObserver(() => {
    if (rebuilding) return;
    if (list.dataset.clusterHostLayout === '1') return;
    takeOver();
  }).observe(list, { childList: true });

  new MutationObserver(() => {
    if (menu.classList.contains('hidden')) return;
    list.dataset.clusterHostLayout = '';
    refreshHosts().then(takeOver);
  }).observe(menu, { attributes: true, attributeFilter: ['class'] });

  const search = document.getElementById('model-picker-search');
  if (search && search.dataset.clusterHostBound !== '1') {
    search.dataset.clusterHostBound = '1';
    search.addEventListener('input', () => {
      if (list.dataset.clusterHostLayout === '1') rebuildList(list, true);
    });
  }

  if (!menu.classList.contains('hidden')) takeOver();
}

function enhanceLabel() {
  const label = document.getElementById('model-picker-label');
  if (!label || !hostsEnabled()) return;
  const route = currentRoute();
  const toggleOn = !!(window.clusterNetwork && window.clusterNetwork.isOn && window.clusterNetwork.isOn());
  const info = toggleOn
    ? { kind: 'cluster', label: (hosts.cluster_network && hosts.cluster_network.label) || 'Cluster (routed)', online: !!(hosts.cluster_network && hosts.cluster_network.online), model: 'Cluster Network' }
    : resolveHostForModel(route.model);
  if (!info) return;
  if (label.querySelector('.clh-badge') && label.dataset.clhModel === (info.model || '') && label.dataset.clhHost === info.label && label.dataset.clhOn === (toggleOn ? '1' : '0')) return;
  label.dataset.clhModel = info.model || '';
  label.dataset.clhHost = info.label || '';
  label.dataset.clhOn = toggleOn ? '1' : '0';
  const name = document.createElement('span');
  name.className = 'clh-label-name';
  name.textContent = toggleOn ? 'Cluster Network' : (info.model || route.model || '').split('/').pop();
  const badge = badgeEl(info.label, { online: info.online !== false, routed: info.kind === 'cluster' });
  label.replaceChildren(name, badge);
  label.title = toggleOn
    ? 'Cluster Network routes this chat across your cluster. Nothing runs on a remote controller.'
    : ('Runs on ' + info.label + (info.online ? '' : ' (offline)') + ' · ' + (info.model || route.model));
}

const MD_FOOTER = /(?:^|\n)\s*(?:[-—]{2,}\s*)?\*?Cluster Network · ([^\n*]+?)\*?\s*(?=\n|$)/;

function parseMarkdownFooter(text) {
  const m = String(text || '').replace(/\u00a0/g, ' ').match(MD_FOOTER);
  if (!m) return null;
  // "mode · manager-node · gemma4:12b, worker-node · qwen2.5:7b · reviewed 1/1 passed · 12.3 s"
  const parts = m[1].split(' · ').map(s => s.trim()).filter(Boolean);
  return { raw: m[1], parts, text: m[1] };
}

function prettyDevice(id, model) {
  const host = findHostById(id) || null;
  const prefixed = (id && model) ? (String(id) + '/' + model) : (model || id);
  const info = resolveHostForModel(prefixed) || resolveHostForModel(model || id);
  const label = (host && host.label) || (info && info.label) || id || '';
  const modelName = model || (info && info.model) || '';
  return [label, modelName].filter(Boolean).join(' · ');
}

function hideMarkdownFooter(msg) {
  msg.querySelectorAll('em, i, p, li, div, span, hr').forEach((el) => {
    const text = (el.innerText || el.textContent || '').replace(/\u00a0/g, ' ').trim();
    if (!/^\*?Cluster Network · /.test(text) || text.length > 240) return;
    if (el.querySelector && el.querySelector('.cl-ran-on, .msg-footer, .msg-actions')) return;
    el.style.display = 'none';
    const prev = el.previousElementSibling;
    if (prev && prev.tagName === 'HR') prev.style.display = 'none';
  });
}

function messageText(msg) {
  const bits = [];
  msg.querySelectorAll('.content, .msg-content, .markdown-body, .md-body').forEach(el => bits.push(el.innerText || ''));
  bits.push(msg.innerText || '');
  return bits.join('\n');
}

function inferRunInfo(msg) {
  const contentText = messageText(msg);
  const md = parseMarkdownFooter(contentText);
  const role = (msg.querySelector('.role') && msg.querySelector('.role').textContent) || '';
  const toggleOn = !!(window.clusterNetwork && window.clusterNetwork.isOn && window.clusterNetwork.isOn());
  const meta = msg._clusterMeta || lastClusterMeta;

  if (meta && (meta.nodes_used || meta.devices || meta.steps)) {
    const devices = meta.devices || [];
    const used = meta.nodes_used || devices.map(d => (d.id || d.device) + (d.model ? ' · ' + d.model : ''));
    const steps = (meta.subtasks || meta.steps || []).filter(s => s && (s.device || s.node || s.model));
    if (steps.length) {
      const lines = steps.map(s => {
        const dev = s.device || s.node || '';
        const model = s.model || '';
        const title = s.title || s.kind || '';
        return (title ? title + ': ' : '') + [dev, model].filter(Boolean).join(' · ');
      });
      return {
        text: 'ran on ' + (used.map(u => {
          if (u && u.includes(' · ')) { const [d, ...r] = u.split(' · '); return prettyDevice(d, r.join(' · ')); }
          return prettyDevice(u, '');
        }).join(', ') || 'the cluster'),
        title: 'Cluster Network' + (meta.mode ? ' · ' + meta.mode : '') + '\n' + lines.join('\n'),
        cluster: true,
        definitive: true,
      };
    }
    if (used.length) {
      return { text: 'ran on ' + used.map(u => {
      if (u && u.includes(' · ')) { const [d, ...r] = u.split(' · '); return prettyDevice(d, r.join(' · ')); }
      return prettyDevice(u, '');
    }).join(', '), title: 'Cluster Network' + (meta.mode ? ' · ' + meta.mode : ''), cluster: true, definitive: true };
    }
  }

  if (md) {
    // Drop trailing timing / "reviewed N/N passed" from the compact footer; keep them in the title.
    const keep = md.parts.filter(p => !/^\d+(\.\d+)?\s*s$/.test(p) && !/^reviewed\b/i.test(p) && !/^(trivial|direct|plan|tools|utility)$/i.test(p));
    const isHost = (p) => !!findHostById(p);
    const shown = [];
    for (let i = 0; i < keep.length; i++) {
      const p = keep[i];
      if (p.includes(' · ')) {
        const [dev, ...rest] = p.split(' · ');
        shown.push(prettyDevice(dev, rest.join(' · ')));
        continue;
      }
      if (isHost(p) && keep[i + 1] && !isHost(keep[i + 1])) {
        shown.push(prettyDevice(p, keep[i + 1]));
        i++;
        continue;
      }
      shown.push(isHost(p) ? prettyDevice(p, '') : p);
    }
    return { text: 'ran on ' + (shown.length ? shown.join(', ') : md.parts.join(', ')), title: 'Cluster Network · ' + md.raw, cluster: true, definitive: true };
  }

  const route = currentRoute();
  let modelGuess = route.model;
  // Prefer the model name shown in the role header when present
  const roleModel = role.replace(/\s*\(.*\)\s*$/, '').trim();
  if (roleModel && roleModel !== 'Odysseus' && roleModel !== 'You') {
    // role may already be "Cluster Network · cluster-network" or a model id
    const last = roleModel.split('·').pop().trim();
    if (last) modelGuess = last;
  }
  const info = resolveHostForModel(modelGuess) || (toggleOn ? resolveHostForModel('cluster-network') : null);
  if (!info) return null;
  if (info.kind === 'cluster') {
    return { text: 'ran on Cluster Network (routed across your cluster)', title: 'The manager plans and reviews; workers may run subtasks. Nothing runs on this device.', cluster: true };
  }
  return { text: 'ran on ' + info.label + ' · ' + info.model, title: info.label + (info.online ? '' : ' (offline when last checked)'), cluster: false };
}

function streamBusy() {
  try { return !!(window.__odysseusChatBusy || (Date.now() < (window.__odysseusChatBusyUntil || 0))); } catch { return false; }
}

function decorateMessage(msg) {
  if (!msg || !msg.classList || !msg.classList.contains('msg-ai')) return;
  const existing = msg.querySelector('.cl-ran-on');
  const info = inferRunInfo(msg);
  if (!info) return;
  // While a turn is still streaming and we only have a generic "routed" guess,
  // wait for the orchestrator footer or the real msg-footer so we don't flash
  // a placeholder at the top of the bubble.
  if (!info.definitive && streamBusy() && !msg.querySelector('.msg-footer') && !parseMarkdownFooter((msg.querySelector('.content') && msg.querySelector('.content').innerText) || '')) {
    return;
  }
  hideMarkdownFooter(msg);
  if (existing) {
    if (existing.dataset.text === info.text) return;
    existing.textContent = info.text;
    existing.title = info.title || info.text;
    existing.dataset.text = info.text;
    existing.className = 'cl-ran-on' + (info.cluster ? ' cluster' : '');
    return;
  }
  const el = document.createElement('div');
  el.className = 'cl-ran-on' + (info.cluster ? ' cluster' : '');
  el.textContent = info.text;
  el.title = info.title || info.text;
  el.dataset.text = info.text;
  const footer = msg.querySelector('.msg-footer');
  const content = msg.querySelector('.content');
  if (footer) footer.insertBefore(el, footer.firstChild);
  else if (content && content.parentNode) content.parentNode.insertBefore(el, content.nextSibling);
  else msg.appendChild(el);
}

function decorateAll(root) {
  (root || document).querySelectorAll('.msg-ai').forEach(decorateMessage);
}

function installStreamHook() {
  if (window.__clusterHostFetchHooked) return;
  window.__clusterHostFetchHooked = true;
  const orig = window.fetch.bind(window);
  window.fetch = function (input, init) {
    const p = orig(input, init);
    try {
      const url = typeof input === 'string' ? input : (input && input.url) || '';
      const path = url.replace(/^https?:\/\/[^/]+/, '');
      if (/^\/api\/chat_stream(\?|$)/.test(path)) {
        p.then(res => {
          try {
            const clone = res.clone();
            consumeClusterStream(clone);
          } catch { /* ignore */ }
        }).catch(() => {});
      }
    } catch { /* ignore */ }
    return p;
  };
}

async function consumeClusterStream(res) {
  if (!res || !res.body || !res.body.getReader) return;
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  let found = null;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n')) >= 0) {
        let line = buf.slice(0, idx);
        buf = buf.slice(idx + 1);
        if (line.endsWith('\r')) line = line.slice(0, -1);
        if (!line.startsWith('data:')) continue;
        const payload = line.slice(5).trim();
        if (!payload || payload === '[DONE]') continue;
        try {
          const j = JSON.parse(payload);
          if (j && j.cluster) found = j.cluster;
          const delta = j && j.choices && j.choices[0] && j.choices[0].delta;
          if (delta && delta.cluster) found = delta.cluster;
        } catch { /* partial/non-json */ }
      }
    }
  } catch { /* stream aborted */ }
  if (found) {
    lastClusterMeta = found;
    const msgs = document.querySelectorAll('.msg-ai');
    const last = msgs[msgs.length - 1];
    if (last) {
      last._clusterMeta = found;
      decorateMessage(last);
    }
  }
}

function showRemoteBanner() {
  if (!hostsEnabled()) {
    const stale = document.getElementById('clh-remote-banner');
    if (stale) stale.remove();
    return;
  }
  const session = hosts && hosts.session;
  if (!session || !session.remote || !session.label) {
    const old = document.getElementById('clh-remote-banner');
    if (old) old.remove();
    return;
  }
  try { if (sessionStorage.getItem(SS_BANNER) === session.client_device_id || sessionStorage.getItem(SS_BANNER) === '1') return; } catch { /* ignore */ }
  let el = document.getElementById('clh-remote-banner');
  if (!el) {
    el = document.createElement('div');
    el.id = 'clh-remote-banner';
    el.className = 'clh-remote-banner';
    el.setAttribute('role', 'status');
    const text = document.createElement('span');
    text.className = 'clh-remote-text';
    el.appendChild(text);
    const close = document.createElement('button');
    close.type = 'button';
    close.className = 'clh-remote-dismiss';
    close.setAttribute('aria-label', 'Dismiss');
    close.textContent = '×';
    close.addEventListener('click', () => {
      try { sessionStorage.setItem(SS_BANNER, session.client_device_id || '1'); } catch { /* ignore */ }
      el.remove();
    });
    el.appendChild(close);
    placeBanner(el);
  }
  const text = el.querySelector('.clh-remote-text');
  if (text) text.textContent = session.label;
}

function placeBanner(el) {
  const candidates = [
    document.querySelector('.chat-main'),
    document.querySelector('.chat-container'),
    document.getElementById('chat-container'),
    document.querySelector('.chat-column'),
    document.getElementById('chat-history') && document.getElementById('chat-history').parentElement,
    document.body,
  ].filter(Boolean);
  const host = candidates[0];
  if (host && host.id === 'chat-history' || (host && host.classList && host.classList.contains('chat-container'))) {
    host.prepend(el);
  } else if (host) {
    host.insertBefore(el, host.firstChild);
  } else {
    document.body.appendChild(el);
  }
}

function hookMessages() {
  const box = document.getElementById('chat-history') || document.body;
  if (box.dataset.clusterHostBound === '1') return;
  box.dataset.clusterHostBound = '1';
  new MutationObserver(() => {
    const msgs = box.querySelectorAll('.msg-ai');
    for (let i = Math.max(0, msgs.length - 6); i < msgs.length; i++) decorateMessage(msgs[i]);
  }).observe(box, { childList: true, subtree: true, characterData: true });
  decorateAll(box);
}

function hookLabel() {
  const label = document.getElementById('model-picker-label');
  if (!label || label.dataset.clusterHostObs === '1') return;
  label.dataset.clusterHostObs = '1';
  new MutationObserver(() => enhanceLabel()).observe(label, { childList: true, characterData: true, subtree: true });
  document.addEventListener('odysseus:model-picked', enhanceLabel);
  document.addEventListener('odysseus:cluster-network', enhanceLabel);
}

async function init() {
  await refreshHosts();
  if (!hostsEnabled()) return;
  installStreamHook();
  hookPicker();
  hookMessages();
  hookLabel();
  enhanceLabel();
  showRemoteBanner();
  new MutationObserver(() => {
    if (!hostsEnabled()) return;
    hookPicker();
    hookMessages();
    hookLabel();
    if (!document.getElementById('clh-remote-banner')) showRemoteBanner();
  }).observe(document.body, { childList: true, subtree: true });
  setInterval(() => { refreshHosts().then(() => { if (hostsEnabled()) { enhanceLabel(); showRemoteBanner(); } }); }, 60000);
}

window.clusterHost = { refresh: refreshHosts, hosts: () => hosts, resolve: resolveHostForModel };
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
