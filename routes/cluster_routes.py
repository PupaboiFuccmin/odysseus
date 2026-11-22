"""Optional Cluster tab backend.

Disabled by default; enable with ``ODYSSEUS_CLUSTER_ENABLED=1``.

Same-origin proxy for the multi-machine LLM cluster dashboard. The browser only
ever talks to ``/api/cluster/*``; this module fans out to:

1. the cluster manager API (``ODYSSEUS_CLUSTER_API_URL``),
2. each node's Ollama (``/api/tags``, ``/api/ps``, ``/api/version``),
3. the manager's journal files mounted read-only at ``ODYSSEUS_CLUSTER_DATA_DIR``
   when the manager API is not reachable,
4. an explicit mock mode (``ODYSSEUS_CLUSTER_MOCK=1``) for UI previews.

Nothing here writes outside ``ODYSSEUS_CLUSTER_LOCAL_DIR`` (``data/cluster/``)
and admin endpoints are gated by Odysseus's existing ``require_admin`` dependency.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from core.middleware import require_admin


def _truthy(val: str | None, default: bool = False) -> bool:
    if val is None or val.strip() == "":
        return default
    return val.strip().lower() not in {"0", "false", "no", "off"}


def is_cluster_enabled() -> bool:
    return _truthy(os.environ.get("ODYSSEUS_CLUSTER_ENABLED"), default=False)


def _api_candidates() -> list[str]:
    raw = (os.environ.get("ODYSSEUS_CLUSTER_API_URL") or "").strip().rstrip("/")
    return [raw] if raw else []


def _data_dir() -> Path:
    return Path(os.environ.get("ODYSSEUS_CLUSTER_DATA_DIR") or "/cluster-data")


def _local_dir() -> Path:
    return Path(os.environ.get("ODYSSEUS_CLUSTER_LOCAL_DIR") or "data/cluster")


def _force_mock() -> bool:
    return _truthy(os.environ.get("ODYSSEUS_CLUSTER_MOCK"), default=False)


def _timeout() -> float:
    try:
        return float(os.environ.get("ODYSSEUS_CLUSTER_TIMEOUT") or 3.0)
    except ValueError:
        return 3.0


def _auth_headers() -> dict[str, str]:
    tok = (os.environ.get("ODYSSEUS_CLUSTER_API_TOKEN") or "").strip()
    if not tok:
        token_file = Path(os.environ.get("ODYSSEUS_CLUSTER_API_TOKEN_FILE") or "/run/secrets/cluster_api_token")
        try:
            tok = token_file.read_text(encoding="utf-8").strip()
        except OSError:
            tok = ""
    return {"Authorization": f"Bearer {tok}"} if tok else {}


# Built-in RFC 5737 example topology used ONLY when ODYSSEUS_CLUSTER_MOCK=1
# and no devices.json / ODYSSEUS_CLUSTER_NODES is supplied.
_MOCK_NODES: list[dict[str, Any]] = [
    {
        "id": "manager-node",
        "name": "manager-node",
        "role": "manager",
        "job": "Plans, routes and reviews tasks; runs larger local models",
        "ip": "192.0.2.10",
        "port": "eth0",
        "is_host": True,
        "ollama": "",
        "backend": "CUDA",
        "hardware": {
            "cpu": "Example Desktop CPU",
            "cpu_vendor": "intel",
            "gpu": "Example Discrete GPU (12 GB)",
            "gpu_vendor": "nvidia",
            "ram_gb": 64,
            "os": "Linux",
            "os_vendor": "arch",
        },
    },
    {
        "id": "worker-node",
        "name": "worker-node",
        "role": "worker",
        "job": "Runs simple tasks on small models; reports back for review",
        "ip": "192.0.2.20",
        "port": "eth0",
        "is_host": False,
        "ollama": "",
        "backend": "Vulkan",
        "hardware": {
            "cpu": "Example Worker APU",
            "cpu_vendor": "amd",
            "gpu": "Example Integrated GPU",
            "gpu_vendor": "amd",
            "ram_gb": 32,
            "os": "Linux",
            "os_vendor": "arch",
        },
    },
]

_MOCK_ROUTER_DEVICE: dict[str, Any] = {
    "id": "cluster-router",
    "name": "Cluster Router",
    "type": "router",
    "role": "network",
    "job": "Gateway / inter-subnet router (read-only view)",
    "vendor": "ubiquiti",
    "model": "Example Managed Router",
    "ip": "192.0.2.1",
    "interfaces": [
        {"port": "eth0", "subnet": "", "note": "WAN"},
        {"port": "eth1", "ip": "192.0.2.1", "subnet": "192.0.2.0/24", "devices": ["manager-node", "worker-node"]},
    ],
    "probe": {"targets": ["192.0.2.1"], "tcp_ports": [443, 80, 22]},
}


def _registry(*, allow_mock_defaults: bool | None = None) -> list[dict[str, Any]]:
    """Device registry: ODYSSEUS_CLUSTER_NODES > <data_dir>/devices.json > mock defaults (if mock enabled)."""
    raw = os.environ.get("ODYSSEUS_CLUSTER_NODES")
    cands: list[Any] = []
    if raw:
        try:
            cands.append(json.loads(raw))
        except json.JSONDecodeError:
            pass
    reg = _data_dir() / "devices.json"
    if reg.exists():
        try:
            cands.append(json.loads(reg.read_text(encoding="utf-8")))
        except Exception:
            pass
    for v in cands:
        v = _unwrap(v, "devices")
        if isinstance(v, list) and v:
            return [dict(d, type=d.get("type") or "compute") for d in v if isinstance(d, dict) and d.get("id")]
    use_mock = _force_mock() if allow_mock_defaults is None else allow_mock_defaults
    if use_mock:
        return [dict(n, type="compute") for n in _MOCK_NODES] + [dict(_MOCK_ROUTER_DEVICE)]
    return []


def _nodes_cfg() -> list[dict[str, Any]]:
    """Compute nodes only (LLM workers/managers)."""
    return [d for d in _registry() if d.get("type", "compute") == "compute"]


def _path_to_node_map(nodes: list[dict[str, Any]] | None = None) -> dict[str, str]:
    nodes = nodes if nodes is not None else _nodes_cfg()
    mgr = next((n["id"] for n in nodes if n.get("role") == "manager" or n.get("is_host")), None)
    worker = next((n["id"] for n in nodes if n["id"] != mgr), None)
    mapping: dict[str, str] = {}
    if mgr:
        mapping["desktop"] = mgr
        mapping["manager"] = mgr
        mapping[mgr] = mgr
    if worker:
        for k in ("mini", "mini-agent", "mini-direct", "worker"):
            mapping[k] = worker
        mapping[worker] = worker
    return mapping


# ---------------------------------------------------------------- device-type probes (plugins)
async def _tcp_rtt(host: str, ports: list[int]) -> tuple[float | None, int | None]:
    for port in ports:
        t0 = time.perf_counter()
        try:
            _, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=1.5)
            w.close()
            try:
                await w.wait_closed()
            except Exception:
                pass
            return round((time.perf_counter() - t0) * 1000, 2), port
        except ConnectionRefusedError:
            return round((time.perf_counter() - t0) * 1000, 2), port
        except Exception:
            continue
    return None, None


async def _probe_router(dev: dict) -> dict:
    """Read-only reachability: TCP connect + immediate close. Never logs in."""
    if _force_mock():
        return {
            "status": "ok",
            "checks": [{"target": dev.get("ip") or "192.0.2.1", "rtt_ms": 0.42, "port": 443, "reachable": True}],
            "metrics": {"latency_ms": 0.42, "reachable": "1/1"},
            "probe_note": "mock router probe",
        }
    pr = dev.get("probe") or {}
    targets = pr.get("targets") or ([dev.get("ip")] if dev.get("ip") else [])
    ports = pr.get("tcp_ports") or [443, 80, 22]
    res = await asyncio.gather(*[_tcp_rtt(t, ports) for t in targets if t])
    checks = [{"target": t, "rtt_ms": r, "port": p, "reachable": r is not None} for t, (r, p) in zip(targets, res)]
    up = [c for c in checks if c["reachable"]]
    return {
        "status": "ok" if len(up) == len(checks) and checks else ("degraded" if up else "unreachable"),
        "checks": checks,
        "metrics": {
            "latency_ms": min((c["rtt_ms"] for c in up), default=None),
            "reachable": f"{len(up)}/{len(checks)}",
        },
        "probe_note": "TCP connect RTT from Odysseus container; read-only, no login",
    }


async def _probe_http_json(dev: dict) -> dict:
    """Generic plugin for peripheral devices exposing a JSON status URL."""
    pr = dev.get("probe") or {}
    url = pr.get("url")
    if not url or _force_mock():
        return {"status": "unknown", "metrics": {}}
    try:
        async with httpx.AsyncClient(timeout=_timeout()) as c:
            t0 = time.perf_counter()
            r = await c.get(url, headers=pr.get("headers") or {})
            rtt = round((time.perf_counter() - t0) * 1000, 1)
            data = r.json()
    except Exception as e:
        return {"status": "unreachable", "metrics": {}, "error": type(e).__name__}
    metrics: dict[str, Any] = {"latency_ms": rtt}
    for label, path in (pr.get("fields") or {}).items():
        cur: Any = data
        for part in str(path).split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
                cur = cur[int(part)]
            else:
                cur = None
        metrics[label] = cur
    return {"status": "ok" if r.status_code < 400 else "degraded", "metrics": metrics}


async def _ping_icmp(host: str) -> float | None:
    """Best-effort ICMP RTT; returns None if ping is missing or the host is down."""
    if not host:
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "ping", "-c", "1", "-W", "1", host,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=2.0)
        if proc.returncode != 0:
            return None
        for line in out.decode("utf-8", "replace").splitlines():
            if "time=" in line:
                return round(float(line.split("time=")[1].split()[0].replace("ms", "")), 2)
    except Exception:
        return None
    return None


async def _probe_controller(dev: dict) -> dict:
    """Remote / controller: signed heartbeat file + ICMP fallback."""
    path = _data_dir() / "state" / "remotes" / f"{dev.get('id')}.json"
    rec: dict[str, Any] = {}
    if path.exists():
        try:
            rec = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            rec = {}
    hb = rec.get("heartbeat") or {}
    ts = hb.get("ts") or rec.get("ts")
    age = round(time.time() - _ts(ts), 1) if ts else None
    fresh = age is not None and age < 90
    ips = hb.get("ips") or {}
    targets = [v for v in (ips.get("wifi"), ips.get("wired"), ips.get("tailscale")) if v]
    for t in (dev.get("probe") or {}).get("targets") or []:
        if t and t not in targets:
            targets.append(t)
    if not targets and dev.get("ip"):
        targets.append(dev["ip"])

    desk = rec.get("desktop_probe") or {}
    checks = list(desk.get("checks") or [])
    live_rtt = desk.get("latency_ms")
    probe_age = (time.time() - _ts(desk["ts"])) if desk.get("ts") else None
    if not _force_mock() and (live_rtt is None or (probe_age is not None and probe_age > 90) or not checks):
        pings = await asyncio.gather(*[_ping_icmp(t) for t in targets[:3]])
        checks = [{"target": t, "rtt_ms": r, "reachable": r is not None, "method": "icmp"}
                  for t, r in zip(targets[:3], pings)]
        live_rtt = min((c["rtt_ms"] for c in checks if c["rtt_ms"] is not None), default=None)

    reachable = any(c.get("reachable") for c in checks) or fresh
    if fresh:
        status = "ok"
    elif reachable:
        status = "degraded"
    else:
        status = "unreachable"

    bat, dck, jc, sess = hb.get("battery") or {}, hb.get("dock") or {}, hb.get("joycon") or {}, hb.get("session") or {}
    lan = ips.get("wifi") or ips.get("wired") or dev.get("ip")
    ts_ip = ips.get("tailscale")
    joy_bits = [f"{s.get('capacity_level') or '?'}{((' ' + s['status']) if s.get('status') else '')}"
                for s in (jc.get("rails_or_serial") or [])]
    metrics = {
        "battery_pct": bat.get("percent"),
        "charging": bat.get("charging"),
        "charge_limit_pct": bat.get("charge_limit_percent"),
        "battery_status": bat.get("status"),
        "mode": dck.get("mode"),
        "joycon": ("attached" if jc.get("attached") else ("not detected" if jc else None)),
        "joycon_detail": ", ".join(joy_bits) or None,
        "session": "Odysseus open" if sess.get("open") else "idle",
        "session_detail": sess.get("detail"),
        "lan_ip": lan,
        "tailscale_ip": ts_ip or "not joined",
        "latency_ms": (hb.get("latency") or {}).get("to_desktop_ms") or live_rtt,
        "heartbeat_age_s": age,
    }
    return {
        "status": status,
        "metrics": metrics,
        "ips": ips or {"wifi": dev.get("ip"), "wired": None, "tailscale": None},
        "battery": bat,
        "dock": dck,
        "joycon": jc,
        "session": sess,
        "checks": checks,
        "last_seen": ts,
        "heartbeat_age_s": age,
        "ip": lan,
        "probe_note": "signed user-level heartbeat + ICMP probe",
    }


_PROBES = {
    "router": _probe_router,
    "controller": _probe_controller,
    "printer": _probe_http_json,
    "camera": _probe_http_json,
    "cooling": _probe_http_json,
    "power-meter": _probe_http_json,
    "smart-plug": _probe_http_json,
    "generic": _probe_http_json,
}


async def _probe_device(dev: dict) -> dict:
    fn = _PROBES.get(dev.get("type") or "generic", _probe_http_json)
    try:
        r = await fn(dev)
    except Exception as e:
        r = {"status": "error", "metrics": {}, "error": str(e)[:200]}
    return {**dev, **r}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts(s: Any) -> float:
    if isinstance(s, (int, float)):
        return float(s)
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _read_jsonl(p: Path, n: int | None = None) -> list[dict]:
    if not p.exists():
        return []
    out = []
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    for ln in lines[-n:] if n else lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return out


def _unwrap(v: Any, key: str) -> Any:
    if isinstance(v, dict) and key in v:
        return v[key]
    return v


# ---------------------------------------------------------------- manager API
class _Mgr:
    base: str | None = None
    checked: float = 0.0
    last_error: str = ""


async def _mgr(method: str, path: str, **kw) -> Any:
    """Call the manager API; returns parsed JSON or None when unavailable."""
    if _force_mock():
        return None
    candidates = _api_candidates()
    if not candidates:
        _Mgr.base = None
        _Mgr.last_error = "ODYSSEUS_CLUSTER_API_URL is not configured"
        return None
    if _Mgr.base is None and time.time() - _Mgr.checked < 30:
        return None
    bases = [_Mgr.base] if (_Mgr.base and _Mgr.base in candidates) else candidates
    async with httpx.AsyncClient(timeout=_timeout(), headers=_auth_headers()) as c:
        for b in bases:
            try:
                r = await c.request(method, b + path, **kw)
            except Exception as e:
                _Mgr.last_error = f"{b}{path}: {type(e).__name__}"
                continue
            if r.status_code in (403, 404, 405) and path.startswith("/api/"):
                _Mgr.last_error = f"{b}{path}: HTTP {r.status_code}"
                continue
            if r.status_code >= 500 or r.status_code in (401, 403, 404):
                _Mgr.last_error = f"{b}{path}: HTTP {r.status_code}"
                continue
            _Mgr.base = b
            _Mgr.last_error = ""
            try:
                return r.json()
            except ValueError:
                return None
    _Mgr.base, _Mgr.checked = None, time.time()
    return None


# ---------------------------------------------------------------- ollama
async def _ollama(node: dict) -> dict:
    url = node.get("ollama")
    out: dict[str, Any] = {"reachable": False}
    if not url or _force_mock():
        return out
    async with httpx.AsyncClient(timeout=_timeout()) as c:
        t0 = time.perf_counter()
        try:
            v = await c.get(url + "/api/version")
            out["rtt_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            out["version"] = v.json().get("version")
            out["reachable"] = True
        except Exception as e:
            out["error"] = type(e).__name__
            return out
        tags, ps = await asyncio.gather(c.get(url + "/api/tags"), c.get(url + "/api/ps"), return_exceptions=True)
    if not isinstance(tags, Exception):
        try:
            out["installed"] = [{
                "name": m.get("name"),
                "size_gb": round((m.get("size") or 0) / 1e9, 2),
                "quant": (m.get("details") or {}).get("quantization_level"),
                "params": (m.get("details") or {}).get("parameter_size"),
                "family": (m.get("details") or {}).get("family"),
                "modified": m.get("modified_at"),
            } for m in tags.json().get("models", [])]
        except Exception:
            pass
    if not isinstance(ps, Exception):
        try:
            out["loaded"] = [{
                "name": m.get("name"),
                "size_gb": round((m.get("size") or 0) / 1e9, 2),
                "vram_gb": round((m.get("size_vram") or 0) / 1e9, 2),
                "ram_gb": round(max(0, (m.get("size") or 0) - (m.get("size_vram") or 0)) / 1e9, 2),
                "expires_at": m.get("expires_at"),
                "context_length": m.get("context_length"),
                "quant": (m.get("details") or {}).get("quantization_level"),
            } for m in ps.json().get("models", [])]
        except Exception:
            pass
    return out


# ---------------------------------------------------------------- journal fallback
def _journal_tasks(n: int = 200) -> list[dict]:
    return _read_jsonl(_data_dir() / "journal" / "tasks.jsonl", n)


def _health(n: int = 2000) -> list[dict]:
    return _read_jsonl(_data_dir() / "journal" / "health.jsonl", n)


def _alerts(n: int = 200) -> list[dict]:
    return _read_jsonl(_data_dir() / "journal" / "alerts.jsonl", n)


def _stats() -> dict:
    p = _data_dir() / "state" / "pipeline_stats.json"
    try:
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    except Exception:
        return {}


def _host_view() -> dict:
    """Host facts visible from inside the container (shared kernel)."""
    out: dict[str, Any] = {}
    try:
        out["uptime_s"] = int(float(Path("/proc/uptime").read_text().split()[0]))
    except Exception:
        pass
    try:
        mi = {}
        for ln in Path("/proc/meminfo").read_text().splitlines():
            k, v = ln.split(":", 1)
            mi[k] = int(v.strip().split()[0])
        out["ram_total_gb"] = round(mi["MemTotal"] / 1048576, 1)
        out["ram_used_gb"] = round((mi["MemTotal"] - mi["MemAvailable"]) / 1048576, 1)
    except Exception:
        pass
    try:
        d = _data_dir()
        st = os.statvfs(d if d.exists() else "/")
        out["disk_total_gb"] = round(st.f_blocks * st.f_frsize / 1e9, 1)
        out["disk_used_gb"] = round((st.f_blocks - st.f_bfree) * st.f_frsize / 1e9, 1)
    except Exception:
        pass
    try:
        out["load1"] = round(os.getloadavg()[0], 2)
    except Exception:
        pass
    return out


def _attempt_rows(tasks: list[dict], path_map: dict[str, str] | None = None) -> list[dict]:
    pm = path_map or _path_to_node_map()
    rows = []
    for t in tasks:
        for a in t.get("attempts") or []:
            node = a.get("node") or pm.get(a.get("path") or a.get("target") or "", "")
            rows.append({**a, "task_id": t.get("id"), "ts": t.get("ts_start") or t.get("ts_submit"), "node": node})
    return rows


def _model_stats(node_id: str) -> list[dict]:
    pm = _path_to_node_map()
    stats = _stats().get("rows") or []
    agg: dict[str, dict] = {}
    for r in stats:
        if pm.get(r.get("path"), r.get("path")) != node_id:
            continue
        m = agg.setdefault(
            r.get("model") or "?",
            {"model": r.get("model"), "tasks": 0, "first_try": 0, "within_rounds": 0,
             "tok_s_sum": 0.0, "jpt_sum": 0.0, "n": 0},
        )
        k = r.get("tasks") or 0
        m["tasks"] += k
        m["first_try"] += r.get("first_try") or 0
        m["within_rounds"] += r.get("within_rounds") or 0
        if r.get("avg_tok_s") is not None:
            m["tok_s_sum"] += (r.get("avg_tok_s") or 0) * k
            m["jpt_sum"] += (r.get("avg_j_per_tok") or 0) * k
            m["n"] += k
    out = []
    for m in agg.values():
        n = m.pop("n") or 0
        out.append({
            **{k: v for k, v in m.items() if not k.endswith("_sum")},
            "avg_tok_s": round(m["tok_s_sum"] / n, 2) if n else None,
            "avg_j_per_tok": round(m["jpt_sum"] / n, 3) if n else None,
            "success_rate": round(m["within_rounds"] / m["tasks"], 3) if m["tasks"] else None,
        })
    return out


def _journal_node(node: dict, tasks: list[dict], health: list[dict], alerts: list[dict]) -> dict:
    nid = node["id"]
    d: dict[str, Any] = {}
    last_h = health[-1] if health else {}
    if node.get("is_host") and last_h:
        g = last_h.get("gpu") or {}
        d.update({
            "power_w": g.get("power_w"),
            "gpu_temp_c": g.get("temp_c"),
            "cpu_temp_c": (last_h.get("cpu") or {}).get("temp_c"),
            "gpu_util_pct": g.get("util_pct"),
            "vram_used_gb": round((g.get("mem_used_mib") or 0) / 1024, 2),
            "vram_total_gb": round((g.get("mem_total_mib") or 0) / 1024, 2),
            "gpu_pstate": g.get("pstate"),
            "gpu_clock_mhz": g.get("clock_mhz") or g.get("sm_clock_mhz"),
            "queue_depth": ((last_h.get("queue") or {}).get("tasks_pending") or 0)
            + ((last_h.get("queue") or {}).get("mini_jobs_pending") or 0),
            "status": "ok" if last_h.get("ok") else "degraded",
            "health_ts": last_h.get("ts"),
            "problems": last_h.get("problems") or [],
        })
        running = (last_h.get("queue") or {}).get("running")
        if running:
            d["current_task"] = running if isinstance(running, str) else json.dumps(running)[:200]
    if not node.get("is_host") and last_h:
        peer = last_h.get("peer") or {}
        d.update({
            "status": peer.get("state") or "unknown",
            "heartbeat_age_s": peer.get("heartbeat_age_s"),
            "health_ts": last_h.get("ts"),
        })
        if peer.get("busy"):
            d["current_task"] = str(peer.get("busy"))[:200]
        d["queue_depth"] = (last_h.get("queue") or {}).get("mini_jobs_pending")
    mine = [a for a in _attempt_rows(tasks) if a.get("node") == nid]
    if mine:
        last = mine[-1]
        d.setdefault("power_w", last.get("avg_w"))
        if d.get("power_w") is None:
            d["power_w"] = last.get("avg_w")
        d["power_note"] = d.get("power_note") or ("live" if node.get("is_host") and last_h else "last task avg")
        d["last_task"] = {
            "id": last.get("task_id"),
            "model": last.get("model"),
            "tok_s": last.get("tok_s"),
            "j_per_tok": last.get("j_per_tok"),
            "ts": last.get("ts"),
        }
    d["alerts"] = [a for a in alerts if a.get("about") in (nid, node.get("name")) or a.get("from") == nid][-8:]
    d["models_perf"] = _model_stats(nid)
    return d


# ---------------------------------------------------------------- mock
def _mock_node(node: dict) -> dict:
    rnd = random.Random(node["id"] + str(int(time.time() // 5)))
    host = node.get("is_host")
    return {
        "status": "ok",
        "power_w": round(rnd.uniform(9, 60) if host else rnd.uniform(5, 32), 1),
        "gpu_temp_c": round(rnd.uniform(32, 60) if host else rnd.uniform(45, 75), 1),
        "cpu_temp_c": round(rnd.uniform(35, 55), 1),
        "gpu_util_pct": round(rnd.uniform(0, 70)),
        "queue_depth": rnd.randint(0, 2),
        "current_task": None,
        "mock": True,
    }


def _mock_metrics(node: dict, hours: float) -> list[dict]:
    rnd = random.Random(node["id"])
    now = time.time()
    pts, steps = [], 72
    base_tok = 52 if node.get("is_host") else 15
    for i in range(steps):
        t = now - hours * 3600 * (1 - i / (steps - 1))
        boost = 1.08 if i > steps * 0.6 else 1.0
        pts.append({
            "ts": datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tok_s": round(base_tok * boost * rnd.uniform(0.93, 1.05), 2),
            "j_per_tok": round((2.4 if node.get("is_host") else 2.3) / boost * rnd.uniform(0.95, 1.06), 3),
            "power_w": round(rnd.uniform(9, 120) if node.get("is_host") else rnd.uniform(5, 35), 1),
            "gpu_temp_c": round(rnd.uniform(32, 70) if node.get("is_host") else rnd.uniform(45, 84), 1),
            "util_pct": round(rnd.uniform(0, 95)),
        })
    return pts


# ---------------------------------------------------------------- changes (local store)
def _changes_path() -> Path:
    return _local_dir() / "changes.jsonl"


async def _noop() -> dict:
    return {"reachable": False, "skipped": "manager provides ollama data"}


def _local_changes() -> list[dict]:
    return _read_jsonl(_changes_path())


# ---------------------------------------------------------------- flows (chain-of-command map)
_FLOW_KINDS = {
    "task": {"label": "Task flow", "color": "green", "dir": "manager->worker"},
    "review": {"label": "Review / fix returns", "color": "blue", "dir": "worker->manager"},
    "monitoring": {"label": "System monitoring", "color": "amber", "dir": "two-way"},
    "tuning": {"label": "Tuning / remote tuning", "color": "purple", "dir": "manager->worker"},
    "alert": {"label": "Alerts / failover", "color": "red", "dir": "two-way"},
    "physical": {"label": "Physical link (router)", "color": "gray", "dir": "two-way"},
    "tools": {"label": "Odysseus tools (search / memory / notify)", "color": "teal", "dir": "manager->odysseus"},
    "remote": {"label": "Remote control / chat", "color": "cyan", "dir": "controller->manager"},
    "offload": {"label": "Frontier agent offload", "color": "indigo", "dir": "frontier<->cluster"},
    "peripheral": {"label": "Peripheral / sensor", "color": "gray", "dir": "device->manager"},
}


def _derive_flows(nodes: list[dict], changes: list[dict], router: dict | None = None) -> list[dict]:
    """Flow states from the journal when the manager has no /api/flows."""
    now = time.time()
    mgr = next((n for n in nodes if n.get("role") == "manager" or n.get("is_host")), nodes[0] if nodes else None)
    if not mgr:
        return []
    tasks, health, alerts = _journal_tasks(500), _health(50), _alerts(200)
    pm = _path_to_node_map(nodes)
    router_id = (router or {}).get("id") or "router"
    out = []
    for w in [n for n in nodes if n is not mgr]:
        wid = w["id"]
        att = [a for a in _attempt_rows(tasks, pm) if a.get("node") == wid]
        sent = [a for a in att if a.get("phase") in (None, "worker")]
        reviewed = [a for a in att if a.get("verdict")]
        fails = [a for a in reviewed if not (a.get("verdict") or {}).get("pass")]
        last_sent = max((_ts(a.get("ts")) for a in sent), default=0)
        live_q = (w.get("queue_depth") or 0) > 0 or bool(w.get("current_task"))

        def st(last: float, window: float = 120, err: bool = False) -> str:
            if err:
                return "error"
            return "active" if last and now - last < window else "idle"

        hb = w.get("heartbeat_age_s")
        mon_last = _ts((health[-1] if health else {}).get("ts"))
        mon_err = w.get("status") not in (None, "ok", "busy")
        w_alerts = [a for a in alerts if a.get("about") == wid or a.get("from") == wid]
        bad_alerts = [a for a in w_alerts if a.get("level") in ("warn", "crit")]
        last_alert = max((_ts(a.get("ts")) for a in w_alerts), default=0)
        last_bad = max((_ts(a.get("ts")) for a in bad_alerts), default=0)
        w_changes = [c for c in changes if c.get("node") in (wid, "all")]
        last_change = max((_ts(c.get("ts")) for c in w_changes), default=0)
        rtt = (w.get("ollama_info") or {}).get("rtt_ms")
        base = {"from": mgr["id"], "to": wid}
        out += [
            {**base, "kind": "task", "status": "active" if live_q else st(last_sent),
             "last": last_sent or None, "count": len(sent), "detail": f"{len(sent)} worker attempts sent"},
            {**base, "kind": "review", "from": wid, "to": mgr["id"],
             "status": st(max((_ts(a.get("ts")) for a in reviewed), default=0)),
             "last": max((_ts(a.get("ts")) for a in reviewed), default=0) or None,
             "count": len(reviewed), "detail": f"{len(reviewed)} checked, {len(fails)} sent back with fixes"},
            {**base, "kind": "monitoring",
             "status": "error" if mon_err else ("active" if hb is not None and hb < 120 else st(mon_last, 180)),
             "last": mon_last or None, "count": len(health),
             "detail": f"heartbeat age {hb}s" if hb is not None else "health polls"},
            {**base, "kind": "alert",
             "status": "error" if last_bad and now - last_bad < 900 else st(last_alert, 900),
             "last": last_alert or None, "count": len(w_alerts),
             "detail": f"{len(bad_alerts)} warn/crit of {len(w_alerts)} alerts"},
            {**base, "kind": "tuning", "status": st(last_change, 3600), "last": last_change or None,
             "count": len(w_changes), "detail": f"{len(w_changes)} logged changes"},
            {**base, "kind": "physical", "via": router_id, "status": "active" if rtt is not None else "error",
             "last": now if rtt is not None else None, "latency_ms": rtt,
             "detail": f"probe RTT {rtt} ms" if rtt is not None else "unreachable"},
        ]
    for f in out:
        f.update({k: v for k, v in _FLOW_KINDS[f["kind"]].items() if k != "dir"})
        f["direction"] = f"{f['from']} -> {f['to']}" if f["kind"] in ("task", "review", "tuning") else f"{f['from']} <-> {f['to']}"
        if isinstance(f.get("last"), float):
            f["last"] = datetime.fromtimestamp(f["last"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return out


# ---------------------------------------------------------------- manager v1 -> UI shape
def _gib(v):
    return round(v, 2) if isinstance(v, (int, float)) else None


def _norm_node(m: dict) -> dict:
    """Map manager API v1 Node onto the flat fields the UI renders."""
    hw = m.get("hardware") or {}
    cpu, gpu, os_ = hw.get("cpu") or {}, hw.get("gpu") or {}, hw.get("os") or {}
    g, t, mem = m.get("gpu") or {}, m.get("temps") or {}, m.get("memory") or {}
    ol = m.get("ollama") or {}
    disk = (m.get("disk") or [{}])[0] if isinstance(m.get("disk"), list) else (m.get("disk") or {})
    ct = m.get("current_task")
    if isinstance(ct, dict):
        ct = f"{ct.get('task_id')} · attempt {ct.get('attempt')} · {ct.get('phase')} · {ct.get('activity')} · {ct.get('model')}"
    stats = m.get("stats") or {}
    out = {
        "id": m.get("id"),
        "name": m.get("name"),
        "role": m.get("role"),
        "job": m.get("job"),
        "ip": (m.get("ips") or {}).get("wired") or m.get("ip"),
        "ips": m.get("ips"),
        "hardware": {
            "cpu": cpu.get("model") if isinstance(cpu, dict) else cpu,
            "cpu_vendor": (hw.get("logo") or {}).get("cpu") or (cpu.get("vendor") if isinstance(cpu, dict) else None),
            "gpu": (gpu.get("model") + (f" ({round(gpu['vram_mib'] / 1024)} GB)" if gpu.get("vram_mib") and gpu.get("kind") == "discrete" else "")) if isinstance(gpu, dict) and gpu.get("model") else None,
            "gpu_vendor": (hw.get("logo") or {}).get("gpu") or (gpu.get("vendor") if isinstance(gpu, dict) else None),
            "ram_gb": hw.get("ram_gib"),
            "os": (os_.get("pretty") or os_.get("name")) if isinstance(os_, dict) else os_,
            "os_vendor": (hw.get("logo") or {}).get("os") or (os_.get("vendor") if isinstance(os_, dict) else None),
            "kernel": os_.get("kernel") if isinstance(os_, dict) else None,
            "system": " ".join(x for x in [(hw.get("system") or {}).get("vendor"), (hw.get("system") or {}).get("board") or (hw.get("system") or {}).get("model")] if x) or None,
        },
        "status": m.get("status"),
        "problems": m.get("status_detail") or [],
        "last_seen": m.get("last_seen"),
        "heartbeat_age_s": m.get("heartbeat_age_s"),
        "uptime_s": (m.get("uptime") or {}).get("system_s"),
        "service_uptime_s": (m.get("uptime") or {}).get("service_s"),
        "power_w": (m.get("power") or {}).get("w"),
        "power_note": (m.get("power") or {}).get("source"),
        "gpu_temp_c": t.get("gpu_c"),
        "cpu_temp_c": t.get("cpu_c"),
        "nvme_temp_c": t.get("nvme_c"),
        "gpu_util_pct": g.get("util_pct"),
        "gpu_clock_mhz": g.get("clock_mhz"),
        "gpu_clock_max_mhz": g.get("clock_max_mhz"),
        "gpu_pstate": g.get("pstate"),
        "vram_used_gb": _gib((g.get("vram_used_mib") or 0) / 1024) if g.get("vram_used_mib") is not None else None,
        "vram_total_gb": _gib((g.get("vram_total_mib") or 0) / 1024) if g.get("vram_total_mib") else None,
        "ram_used_gb": mem.get("ram_used_gib"),
        "ram_total_gb": mem.get("ram_total_gib"),
        "disk_used_gb": disk.get("used_gib"),
        "disk_total_gb": disk.get("total_gib"),
        "disk_mount": disk.get("mount"),
        "backend": (ol.get("backend") or "").upper() or None,
        "ollama_info": {
            "reachable": bool(ol.get("up")),
            "version": ol.get("version"),
            "rtt_ms": ol.get("latency_ms"),
            "url": ol.get("url"),
            "installed": [{"name": x.get("name"), "size_gb": x.get("size_gib"), "quant": x.get("quant"),
                           "params": x.get("params"), "family": x.get("family")} for x in ol.get("installed") or []],
            "loaded": [{"name": x.get("name"),
                        "size_gb": _gib((x.get("size_bytes") or 0) / 1e9),
                        "vram_gb": _gib((x.get("vram_bytes") or 0) / 1e9),
                        "ram_gb": _gib(max(0, (x.get("size_bytes") or 0) - (x.get("vram_bytes") or 0)) / 1e9),
                        "vram_pct": x.get("vram_pct"),
                        "context_length": x.get("context"),
                        "expires_at": x.get("expires_at"),
                        "expires_in_s": x.get("expires_in_s")} for x in ol.get("loaded") or []],
        },
        "link_latency_ms": (m.get("latency") or {}).get("to_peer_ms"),
        "link_method": (m.get("latency") or {}).get("method"),
        "queue_depth": (m.get("queue") or {}).get("depth"),
        "current_task": ct,
        "busy": m.get("busy"),
        "stats": stats,
        "models_perf": [{"model": x.get("model"), "tasks": x.get("attempts"), "success_rate": x.get("pass_rate"),
                         "first_try": None, "avg_tok_s": x.get("avg_tok_s"), "avg_j_per_tok": x.get("avg_j_per_tok"),
                         "avg_score": x.get("avg_score")} for x in stats.get("per_model") or []],
        "last_benchmark": m.get("last_benchmark"),
        "alerts": m.get("alerts_active") or [],
    }
    return {k: v for k, v in out.items() if v is not None}


_FLOW_TYPE_COLOR = {
    "task": "green", "review": "blue", "monitoring": "amber", "monitor": "amber", "health": "amber",
    "tuning": "purple", "alert": "red", "failover": "red", "physical": "gray", "network": "gray",
    "tools": "teal", "remote": "cyan", "offload": "indigo", "peripheral": "gray",
}


def _norm_flow(f: dict) -> dict:
    kind = f.get("type") or f.get("kind") or "task"
    c = f.get("counts") or {}
    d = f.get("direction")
    arrow = "<->" if d in ("both", "two-way", "bidirectional") else "->"
    return {
        **f,
        "kind": kind,
        "color": f.get("color") or _FLOW_TYPE_COLOR.get(kind, "gray"),
        "label": f.get("label") or _FLOW_KINDS.get(kind, {}).get("label", kind),
        "last": f.get("last_activity") or f.get("last"),
        "count": c.get("total", f.get("count")),
        "direction": f"{f.get('from')} {arrow} {f.get('to')}" + (f" ({d})" if d and arrow == "->" else ""),
        "detail": (f"{c.get('ok', 0)} ok / {c.get('error', 0)} error · {c.get('last_1h', 0)} in last 1h" if c else f.get("detail", "")),
        "status": f.get("status") or "idle",
    }


def _require_enabled() -> None:
    if not is_cluster_enabled():
        raise HTTPException(404, "Cluster feature is disabled (set ODYSSEUS_CLUSTER_ENABLED=1 to enable)")


# ---------------------------------------------------------------- router
def setup_cluster_routes() -> APIRouter:
    router = APIRouter(prefix="/api/cluster", tags=["cluster"])

    @router.get("/config")
    async def cluster_config():
        return {
            "enabled": is_cluster_enabled(),
            "mock": _force_mock(),
            "configured": bool(_api_candidates() or (_data_dir() / "devices.json").exists() or os.environ.get("ODYSSEUS_CLUSTER_NODES")),
        }

    @router.get("/nodes", dependencies=[Depends(require_admin)])
    async def nodes():
        _require_enabled()
        cfg = _nodes_cfg()
        sources = set()
        mgr = await _mgr("GET", "/api/nodes")
        raw_nodes = _unwrap(mgr, "nodes") if isinstance(mgr, dict) else None
        mgr_nodes = {n.get("id") or n.get("name"): _norm_node(n) for n in (raw_nodes or []) if isinstance(n, dict)}
        if mgr_nodes:
            sources.add("manager")
            for nid, n in mgr_nodes.items():
                if nid not in {c["id"] for c in cfg}:
                    cfg.append({
                        "id": nid,
                        "name": n.get("name") or nid,
                        "role": n.get("role") or "worker",
                        "is_host": (n.get("role") == "manager"),
                        "ip": n.get("ip"),
                        "hardware": n.get("hardware") or {},
                        "ollama": n.get("ollama"),
                    })
        ollamas = await asyncio.gather(*[
            _ollama(n) if not (mgr_nodes.get(n["id"]) or {}).get("ollama_info", {}).get("installed") else _noop()
            for n in cfg
        ])
        tasks, health, alerts = _journal_tasks(), _health(200), _alerts()
        if tasks or health:
            sources.add("journal")
        out = []
        for node, ol in zip(cfg, ollamas):
            d = dict(node)
            d["ollama_info"] = ol
            if ol.get("reachable"):
                sources.add("ollama")
            j = _journal_node(node, tasks, health, alerts) if (tasks or health) else {}
            d.update({k: v for k, v in j.items() if v is not None})
            if node.get("is_host"):
                d["host"] = _host_view()
            m = mgr_nodes.get(node["id"])
            if m:
                d.update({k: v for k, v in m.items() if v is not None and k not in ("hardware", "ollama_info")})
                if m.get("ollama_info", {}).get("installed") or not ol.get("reachable"):
                    d["ollama_info"] = m.get("ollama_info")
                d.pop("host", None)
                if m.get("hardware"):
                    d["hardware"] = {**node.get("hardware", {}), **m["hardware"]}
            if not (m or j or ol.get("reachable")):
                if _force_mock():
                    d.update(_mock_node(node))
                    sources.add("mock")
                else:
                    d["status"] = "unreachable"
            out.append(d)

        reg = _registry()
        devices = await asyncio.gather(*[_probe_device(d) for d in reg if d.get("type", "compute") != "compute"])
        rdev = next((d for d in devices if d.get("type") == "router"), None)

        links = []
        if len(out) >= 2:
            mgr_node = next((n for n in out if n.get("role") == "manager" or n.get("is_host")), out[0])
            peer = next((n for n in out if n["id"] != mgr_node["id"]), None)
            if peer:
                links.append({
                    "from": mgr_node["id"],
                    "to": peer["id"],
                    "via": (rdev or {}).get("id"),
                    "latency_ms": peer.get("link_latency_ms"),
                    "method": peer.get("link_method"),
                    "probe_rtt_ms": (peer.get("ollama_info") or {}).get("rtt_ms"),
                    "note": "probe = Odysseus container -> node Ollama HTTP round-trip",
                })

        mflows = await _mgr("GET", "/api/flows") if mgr_nodes else None
        flows = _unwrap(mflows, "flows") if isinstance(mflows, dict) else None
        if flows:
            flows = [_norm_flow(f) for f in flows if isinstance(f, dict)]
            sources.add("flows")
            if not any(f["kind"] in ("physical", "network") for f in flows) and rdev:
                flows += [f for f in _derive_flows(out, [], rdev) if f["kind"] == "physical"]
        else:
            flows = _derive_flows(out, _local_changes(), rdev)

        if rdev:
            sources.add("probe")
            for f in flows:
                if f.get("kind") in ("physical", "network"):
                    f["via"] = rdev["id"]
                    lat = (rdev.get("metrics") or {}).get("latency_ms")
                    f["detail"] = (f.get("detail") or "") + (f" · router {lat} ms" if lat is not None else " · router unreachable")

        mgr_id = next((n["id"] for n in out if n.get("role") == "manager" or n.get("is_host")), None)
        for dv in devices:
            if dv.get("type") != "controller" or not mgr_id:
                continue
            sources.add("probe")
            sess = dv.get("session") or {}
            age = dv.get("heartbeat_age_s")
            live = (age is not None and age < 90) or bool(sess.get("open"))
            st = "active" if live else ("error" if dv.get("status") == "unreachable" else "idle")
            bits = []
            if age is not None:
                bits.append(f"heartbeat {age}s ago")
            if sess.get("open"):
                bits.append(sess.get("detail") or "Odysseus session open")
            lat = (dv.get("metrics") or {}).get("latency_ms")
            flows.append(_norm_flow({
                "id": f"remote.{dv['id']}",
                "from": dv["id"],
                "to": mgr_id,
                "type": "remote",
                "kind": "remote",
                "direction": "outbound",
                "color": "cyan",
                "label": "Remote control / chat",
                "status": st,
                "last": dv.get("last_seen"),
                "latency_ms": lat,
                "detail": " · ".join(bits) or "no heartbeat yet",
            }))

        if not sources:
            sources.add("unreachable" if _api_candidates() else "unconfigured")
        return {
            "nodes": out,
            "devices": list(devices),
            "router": rdev,
            "links": links,
            "flows": flows,
            "flow_kinds": _FLOW_KINDS,
            "sources": sorted(sources),
            "ts": _now(),
            "manager_api": _Mgr.base,
            "last_error": _Mgr.last_error or None,
        }

    @router.get("/nodes/{node_id}/log", dependencies=[Depends(require_admin)])
    async def node_log(node_id: str, since: str = "", n: int = 200):
        _require_enabled()
        params: dict[str, Any] = {"n": n}
        if since and since.isdigit():
            params["since"] = since
        mgr = await _mgr("GET", f"/api/nodes/{node_id}/log", params=params)
        if isinstance(mgr, dict) and ("events" in mgr or "lines" in mgr):
            lines = mgr.get("events") or mgr.get("lines") or []
            return {"lines": lines, "next_since": mgr.get("next_since"), "sources": ["manager"]}
        pm = _path_to_node_map()
        lines: list[dict] = []
        for a in _alerts(500):
            if a.get("about") == node_id or a.get("from") == node_id:
                lines.append({"ts": a.get("ts"), "level": a.get("level", "info"), "msg": f"[alert {a.get('key')}] {a.get('msg')}"})
        for t in _journal_tasks(300):
            for a in t.get("attempts") or []:
                nid = a.get("node") or pm.get(a.get("path") or "", "")
                if nid != node_id:
                    continue
                v = a.get("verdict") or {}
                lines.append({
                    "ts": t.get("ts_start") or t.get("ts_submit"),
                    "level": "info" if v.get("pass", True) else "warn",
                    "msg": f"task {t.get('id')} #{a.get('n')} {a.get('phase')} {a.get('model')}: "
                           f"{a.get('eval_tokens')} tok @ {a.get('tok_s')} tok/s, {a.get('j_per_tok')} J/tok, "
                           f"{a.get('wall_s')}s -> {'PASS' if v.get('pass') else 'FAIL' if v else 'done'}"
                           + (f" ({'; '.join(v.get('issues') or [])[:160]})" if v and not v.get("pass") else ""),
                })
        host = next((c for c in _nodes_cfg() if c.get("is_host")), None)
        for h in _health(300):
            if host and node_id == host["id"]:
                g = h.get("gpu") or {}
                lines.append({
                    "ts": h.get("ts"),
                    "level": "info" if h.get("ok") else "warn",
                    "msg": f"health {'ok' if h.get('ok') else 'PROBLEM'}: GPU {g.get('temp_c')}C {g.get('power_w')}W "
                           f"{g.get('util_pct')}% {g.get('pstate')}; queue {h.get('queue')}"
                           + (f"; problems: {h.get('problems')}" if h.get("problems") else ""),
                })
            elif (h.get("peer") or {}).get("state") not in (None, "ok"):
                lines.append({"ts": h.get("ts"), "level": "warn", "msg": f"peer state {(h.get('peer') or {}).get('state')}"})
        if host and node_id == host["id"]:
            for s in _read_jsonl(_data_dir() / "journal" / "selfcheck.jsonl", 100):
                lines.append({"ts": s.get("ts"), "level": "info", "msg": f"selfcheck: {s.get('action')} ({s.get('why')})"})
        lines.sort(key=lambda r: _ts(r.get("ts")))
        if since:
            lines = [r for r in lines if _ts(r.get("ts")) > _ts(since)]
        src = ["journal"] if lines else (["mock"] if _force_mock() else ["none"])
        if not lines and _force_mock():
            lines = [{"ts": _now(), "level": "info", "msg": f"[mock] {node_id} idle - no manager API or journal mounted"}]
        return {"lines": lines[-n:], "sources": src}

    @router.get("/tasks", dependencies=[Depends(require_admin)])
    async def tasks(n: int = 50, node: str = ""):
        _require_enabled()
        mgr = await _mgr("GET", "/api/tasks", params={"n": n, "node": node} if node else {"n": n})
        if isinstance(mgr, dict) and "tasks" in mgr:
            return {"tasks": _unwrap(mgr, "tasks") or [], "live": mgr.get("live") or [], "sources": ["manager"]}
        pm = _path_to_node_map()
        rows = _journal_tasks(n * 3)
        if node:
            rows = [t for t in rows if any((a.get("node") or pm.get(a.get("path") or "", "")) == node
                                          for a in t.get("attempts") or [])]
        return {"tasks": list(reversed(rows))[:n], "live": [], "sources": ["journal"] if rows else ["none"]}

    @router.post("/tasks", dependencies=[Depends(require_admin)])
    async def submit_task(request: Request):
        _require_enabled()
        body = await request.json()
        text = str(body.get("text") or "").strip()
        if not text:
            raise HTTPException(400, "text required")
        payload = {
            "text": text,
            "type": body.get("type") or "auto",
            "target": body.get("target") or "auto",
            "source": "odysseus-cluster-tab",
        }
        if body.get("tests"):
            payload["tests"] = str(body["tests"])[:20000]
        mgr = await _mgr("POST", "/api/tasks", json=payload)
        if isinstance(mgr, dict) and not mgr.get("error"):
            return {**mgr, "sources": ["manager"]}
        if isinstance(mgr, dict):
            raise HTTPException(400, mgr.get("error"))
        if _force_mock():
            return {"id": "mock-" + str(int(time.time())), "status": "queued", "mock": True, "sources": ["mock"]}
        raise HTTPException(503, "Cluster manager API not reachable from Odysseus. Task not submitted.")

    @router.get("/nodes/{node_id}/metrics", dependencies=[Depends(require_admin)])
    async def metrics(node_id: str, hours: float = 24):
        _require_enabled()
        node = next((c for c in _nodes_cfg() if c["id"] == node_id), {"id": node_id})
        mgr = await _mgr("GET", f"/api/nodes/{node_id}/metrics", params={"from": f"-{int(hours * 60)}m"})
        if isinstance(mgr, dict) and isinstance(mgr.get("series"), dict):
            alias = {"gpu_util_pct": "util_pct"}
            pts: dict[float, dict] = {}
            for key, arr in mgr["series"].items():
                for t, v in arr or []:
                    pts.setdefault(t, {"ts": datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})[alias.get(key, key)] = v
            return {
                "series": [pts[k] for k in sorted(pts)],
                "units": mgr.get("units"),
                "step_s": mgr.get("step_s"),
                "resolution": mgr.get("resolution"),
                "sources": ["manager"],
            }
        cutoff = time.time() - hours * 3600
        series: list[dict] = []
        if node.get("is_host"):
            for h in _health():
                if _ts(h.get("ts")) < cutoff:
                    continue
                g = h.get("gpu") or {}
                series.append({
                    "ts": h.get("ts"),
                    "power_w": g.get("power_w"),
                    "gpu_temp_c": g.get("temp_c"),
                    "cpu_temp_c": (h.get("cpu") or {}).get("temp_c"),
                    "util_pct": g.get("util_pct"),
                })
        for a in _attempt_rows(_journal_tasks(1000)):
            if a.get("node") != node_id or _ts(a.get("ts")) < cutoff:
                continue
            series.append({
                "ts": a.get("ts"),
                "tok_s": a.get("tok_s"),
                "j_per_tok": a.get("j_per_tok"),
                "power_w": a.get("avg_w"),
                "model": a.get("model"),
            })
        series.sort(key=lambda r: _ts(r.get("ts")))
        if series:
            return {"series": series, "sources": ["journal"]}
        if _force_mock():
            return {"series": _mock_metrics(node, hours), "sources": ["mock"]}
        return {"series": [], "sources": ["none"]}

    @router.get("/changes", dependencies=[Depends(require_admin)])
    async def changes(node: str = ""):
        _require_enabled()
        mgr = await _mgr("GET", "/api/changes", params={"node": node, "n": 200} if node else {"n": 200})
        rows = list(mgr.get("changes") or []) if isinstance(mgr, dict) else []
        seen = {r.get("id") for r in rows}
        rows += [r for r in _local_changes() if r.get("id") not in seen]
        if node:
            rows = [r for r in rows if r.get("node") in (node, "all", None, "")]
        rows.sort(key=lambda r: _ts(r.get("ts")))
        return {"changes": rows, "sources": (["manager"] if mgr is not None else []) + ["odysseus"]}

    @router.post("/changes", dependencies=[Depends(require_admin)])
    async def add_change(request: Request):
        _require_enabled()
        b = await request.json()
        title = str(b.get("title") or "").strip()
        if not title:
            raise HTTPException(400, "title required")
        rec = {
            "id": "c" + time.strftime("%Y%m%d-%H%M%S") + f"-{random.randint(0, 0xfff):03x}",
            "ts": b.get("ts") or _now(),
            "node": str(b.get("node") or "all")[:64],
            "kind": str(b.get("kind") or "tuning")[:32],
            "title": title[:200],
            "detail": str(b.get("detail") or "")[:4000],
            "before": str(b.get("before") or "")[:200],
            "after": str(b.get("after") or "")[:200],
            "source": "odysseus-cluster-tab",
        }

        def kv(txt: str) -> dict | None:
            txt = (txt or "").strip()
            if not txt:
                return None
            out: dict[str, Any] = {}
            for part in txt.replace(";", ",").split(","):
                if "=" in part:
                    k, v = part.split("=", 1)
                    try:
                        out[k.strip()] = float(v)
                    except ValueError:
                        out[k.strip()] = v.strip()
            return out or {"note": txt}

        mrec = {
            "node": rec["node"],
            "kind": rec["kind"],
            "title": rec["title"],
            "description": rec["detail"],
            "author": "odysseus-ui",
            "window_h": int(b.get("window_h") or 24),
        }
        if b.get("ts"):
            mrec["ts"] = b["ts"]
        for k in ("before", "after"):
            if kv(rec[k]):
                mrec[k] = kv(rec[k])
        fwd = await _mgr("POST", "/api/changes", json=mrec)
        if isinstance(fwd, dict) and fwd.get("id"):
            return {"change": fwd, "forwarded_to_manager": True}
        _local_dir().mkdir(parents=True, exist_ok=True)
        with _changes_path().open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
        return {"change": rec, "forwarded_to_manager": False}

    @router.get("/offload", dependencies=[Depends(require_admin)])
    async def offload(n: int = 20):
        _require_enabled()
        mgr = await _mgr("GET", "/api/offload", params={"n": n})
        if isinstance(mgr, dict) and "totals" in mgr:
            return {**mgr, "sources": ["manager"]}
        rows = _read_jsonl(_data_dir() / "offload" / "jobs.jsonl", 2000)
        cfg_nodes = _nodes_cfg()
        mgr_ids = {c["id"] for c in cfg_nodes if c.get("role") == "manager" or c.get("is_host")} | {"desktop", "manager"}
        worker_ids = {c["id"] for c in cfg_nodes if c["id"] not in mgr_ids} | {"mini", "worker"}
        totals = {
            "jobs_total": len(rows),
            "jobs_ok": sum(1 for r in rows if r.get("status") == "done"),
            "jobs_error": sum(1 for r in rows if r.get("status") == "error"),
            "raw_input_tokens_est": sum(int(r.get("raw_input_tokens_est") or 0) for r in rows),
            "local_input_tokens": sum(int(r.get("local_input_tokens") or 0) for r in rows),
            "local_output_tokens": sum(int(r.get("local_output_tokens") or 0) for r in rows),
            "returned_output_tokens": sum(int(r.get("returned_output_tokens") or 0) for r in rows),
            "frontier_tokens_saved_est": sum(int(r.get("frontier_tokens_saved_est") or 0) for r in rows),
            "desktop_time_s": round(sum(
                sum(float(v or 0.0) for k, v in (r.get("node_time_s") or {}).items() if k in mgr_ids)
                for r in rows
            ), 2),
            "mini_time_s": round(sum(
                sum(float(v or 0.0) for k, v in (r.get("node_time_s") or {}).items() if k in worker_ids)
                for r in rows
            ), 2),
        }
        return {
            "service_up": None,
            "port": 11630,
            "totals": totals,
            "recent_jobs": list(reversed(rows[-max(1, min(n, 100)):])),
            "sources": ["journal"] if rows else ["none"],
        }

    @router.get("/onboard/instructions", dependencies=[Depends(require_admin)])
    async def onboard_instructions():
        _require_enabled()
        mgr = await _mgr("GET", "/api/onboard/instructions")
        if isinstance(mgr, dict) and not mgr.get("error"):
            return {**mgr, "sources": ["manager"]}
        md = (
            "# Add a device to the LLM cluster\n\n"
            "The manager API is not reachable right now, so this is the short version.\n\n"
            "On the new machine, copy `contrib/cluster-network/onboard/probe.sh` and run:\n\n"
            "```bash\n./probe.sh --pretty --manager http://<manager-host>:11600 | tee /tmp/cluster-probe.json\n```\n\n"
            "Paste that JSON below, or on the manager host run `cluster onboard /tmp/cluster-probe.json`. "
            "See `contrib/cluster-network/onboard/ONBOARDING.md`."
        )
        return {"title": "Add a device to the LLM cluster", "markdown": md, "sources": ["odysseus-fallback"]}

    @router.post("/onboard/advise", dependencies=[Depends(require_admin)])
    async def onboard_advise(request: Request):
        _require_enabled()
        body = await request.json()
        mgr = await _mgr("POST", "/api/onboard/advise", json=body)
        if isinstance(mgr, dict) and (mgr.get("advice") or mgr.get("markdown")) and not mgr.get("error"):
            return {**mgr, "sources": ["manager"]}
        detail = (mgr or {}).get("error") if isinstance(mgr, dict) else "Cluster manager API not reachable"
        raise HTTPException(503 if mgr is None else 400, detail)

    @router.post("/onboard/apply", dependencies=[Depends(require_admin)])
    async def onboard_apply(request: Request):
        _require_enabled()
        body = await request.json()
        if not (body.get("confirm") or body.get("yes")):
            raise HTTPException(400, "confirm=true is required")
        mgr = await _mgr("POST", "/api/onboard/apply", json=body)
        if isinstance(mgr, dict) and mgr.get("ok") and not mgr.get("error"):
            return {**mgr, "sources": ["manager"]}
        detail = (mgr or {}).get("error") if isinstance(mgr, dict) else "Cluster manager API not reachable"
        raise HTTPException(503 if mgr is None else 400, detail)

    @router.get("/status", dependencies=[Depends(require_admin)])
    async def status():
        _require_enabled()
        await _mgr("GET", "/api/nodes")
        d = _data_dir()
        return {
            "enabled": is_cluster_enabled(),
            "manager_api": _Mgr.base,
            "last_error": _Mgr.last_error or None,
            "token_present": bool(_auth_headers()),
            "candidates": _api_candidates(),
            "data_dir": str(d),
            "data_dir_mounted": (d / "journal").exists(),
            "mock_forced": _force_mock(),
        }

    return router
