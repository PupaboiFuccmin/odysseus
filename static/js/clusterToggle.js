// Optional "Use Cluster Network" toggle.
// Stays completely dormant (no DOM, no fetch wrapper) unless
// GET /api/cluster-network/config returns { enabled: true }.

const LS_KEY = 'odysseus-cluster-network-mode';
const PREV_KEY = 'odysseus-cluster-network-prev-routes';
const PREF_KEY = 'cluster_network_mode';
const AGENT_KEY = 'odysseus-cluster-network-agent-tools';
let cfg = null;
let on = true;

function loadJSON(k, d) { try { return JSON.parse(localStorage.getItem(k) || '') ?? d; } catch { return d; } }
function saveJSON(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* quota */ } }

function rewriteChatForm(fd) {
  if (!cfg || !cfg.enabled) return;
  const sid = fd.get('session') || '';
  const prev = loadJSON(PREV_KEY, {});
  const model = fd.get('selected_model') || '';
  if (on) {
    if (model && model !== cfg.model && sid) {
      prev[sid] = {
        model,
        endpoint_url: fd.get('selected_endpoint_url') || '',
        endpoint_id: fd.get('selected_endpoint_id') || '',
        toggled: true,
      };
      saveJSON(PREV_KEY, prev);
    } else if (sid && !prev[sid]) {
      prev[sid] = { toggled: true };
      saveJSON(PREV_KEY, prev);
    }
    fd.set('selected_model', cfg.model);
    fd.set('selected_endpoint_id', cfg.endpoint_id);
    fd.delete('selected_endpoint_url');
    fd.set('thinking_mode', 'on');
    if (fd.get('mode') === 'agent') {
      if (localStorage.getItem(AGENT_KEY) === '1') {
        fd.set('allow_web_search', 'true');
      } else {
        fd.set('mode', 'chat');
        fd.delete('allow_web_search');
      }
    }
    return;
  }
  const p = sid && prev[sid];
  const picked = window.__odysseusLastPickedRoute;
  const explicitPick = picked && picked.model === cfg.model && (picked.session_id || '') === sid;
  if (p && p.toggled && model === cfg.model && !explicitPick && p.model) {
    fd.set('selected_model', p.model);
    if (p.endpoint_id) fd.set('selected_endpoint_id', p.endpoint_id); else fd.delete('selected_endpoint_id');
    if (p.endpoint_url) fd.set('selected_endpoint_url', p.endpoint_url); else fd.delete('selected_endpoint_url');
    delete prev[sid];
    saveJSON(PREV_KEY, prev);
  }
}

function installFetchHook() {
  if (window.__clusterNetFetchHooked) return;
  window.__clusterNetFetchHooked = true;
  const orig = window.fetch.bind(window);
  window.fetch = function (input, init) {
    try {
      const url = typeof input === 'string' ? input : (input && input.url) || '';
      const path = url.replace(/^https?:\/\/[^/]+/, '');
      if (/^\/api\/chat_stream(\?|$)/.test(path) && init && init.body instanceof FormData) rewriteChatForm(init.body);
    } catch (e) { console.warn('[cluster-network] rewrite skipped', e); }
    return orig(input, init);
  };
}

function render() {
  if (!cfg || !cfg.enabled) return;
  let btn = document.getElementById('cluster-net-toggle');
  const top = document.querySelector('.chat-input-top');
  if (!top) return;
  if (!btn) {
    btn = document.createElement('button');
    btn.type = 'button';
    btn.id = 'cluster-net-toggle';
    btn.className = 'cluster-net-toggle';
    btn.setAttribute('role', 'switch');
    btn.innerHTML = '<span class="cnt-dot" aria-hidden="true"></span><span class="cnt-label">Use Cluster Network</span><span class="cnt-switch" aria-hidden="true"><span></span></span>';
    btn.addEventListener('click', () => setOn(!on, true));
    const picker = document.getElementById('model-picker-wrap');
    top.insertBefore(btn, picker || top.firstChild);
  }
  btn.setAttribute('aria-checked', String(on));
  btn.classList.toggle('on', on);
  btn.classList.toggle('unhealthy', !!cfg && !cfg.healthy);
  btn.title = on
    ? `Cluster Network ON: chats go to the cluster (bridge ${cfg && cfg.healthy ? 'online' : 'NOT reachable'}). Click to use a single model.`
    : 'Cluster Network OFF: chats use the model picked below. Click to route chats through the cluster.';
  document.body.classList.toggle('cluster-net-on', on);
  const pbtn = document.getElementById('model-picker-btn');
  if (pbtn) { pbtn.disabled = on; pbtn.setAttribute('aria-disabled', String(on)); }
}

function setOn(v, user) {
  on = !!v;
  try { localStorage.setItem(LS_KEY, on ? '1' : '0'); } catch { /* ignore */ }
  render();
  if (user) {
    fetch('/api/prefs/' + PREF_KEY, {
      method: 'PUT',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value: on }),
    }).catch(() => {});
    document.dispatchEvent(new CustomEvent('odysseus:cluster-network', { detail: { on } }));
  }
}

async function init() {
  try {
    const r = await fetch('/api/cluster-network/config', { credentials: 'same-origin' });
    if (!r.ok) return;
    cfg = await r.json();
  } catch {
    return;
  }
  if (!cfg || !cfg.enabled) return;

  installFetchHook();
  let v = null;
  const ls = localStorage.getItem(LS_KEY);
  if (ls === '1' || ls === '0') v = ls === '1';
  if (v === null) {
    try {
      const pr = await fetch('/api/prefs/' + PREF_KEY, { credentials: 'same-origin' });
      if (pr.ok) {
        const p = await pr.json();
        if (typeof p.value === 'boolean') v = p.value;
      }
    } catch { /* ignore */ }
  }
  setOn(v === null ? cfg.default_on !== false : v, false);

  new MutationObserver(() => { if (!document.getElementById('cluster-net-toggle')) render(); })
    .observe(document.body, { childList: true, subtree: true });
  setInterval(async () => {
    try {
      const r = await fetch('/api/cluster-network/config', { credentials: 'same-origin' });
      if (r.ok) {
        cfg = { ...cfg, ...(await r.json()) };
        render();
      }
    } catch { /* ignore */ }
  }, 60000);
}

window.clusterNetwork = {
  isOn: () => !!(cfg && cfg.enabled && on),
  set: (v) => { if (cfg && cfg.enabled) setOn(v, true); },
  config: () => cfg,
  agentTools: (v) => {
    if (v !== undefined) localStorage.setItem(AGENT_KEY, v ? '1' : '0');
    return localStorage.getItem(AGENT_KEY) === '1';
  },
};
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
