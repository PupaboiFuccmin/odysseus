"""Optional "Use Cluster Network" support.

Disabled by default. Enable by setting ``ODYSSEUS_CLUSTER_ENABLED=1`` (and
optionally ``ODYSSEUS_CLUSTER_NETWORK=1``, which defaults to on when cluster
support is enabled).

* Registers (idempotently) a shared model endpoint **Cluster Network** pointing
  at the cluster bridge (``ODYSSEUS_CLUSTER_BRIDGE_URL``, model
  ``cluster-network``). The bridge runs the chain-of-command orchestrator.
* ``GET /api/cluster-network/config`` tells the chat UI toggle whether the
  feature is enabled, healthy, and which endpoint id/model to route to.
* ``GET /api/cluster-network/hosts`` lists compute hosts and their chat models
  (for the picker grouping and host badges) and describes the current browser
  session (local vs remote).
* ``GET /api/cluster-network/session`` reports whether this browser is on a
  remote controller, so the UI can show that models run on the cluster, not
  on the device you are holding.

Nothing else in Odysseus changes when the feature is off: the hosts and
session routes still exist but return ``enabled: false``, and the picker is
left alone. Disable the feature with ``ODYSSEUS_CLUSTER_ENABLED=0``.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Request

logger = logging.getLogger(__name__)

ENDPOINT_ID = "clusternet"
ENDPOINT_NAME = "Cluster Network"
MODEL_ID = "cluster-network"

_EMBED_RE = re.compile(r"(embed|embedding|nomic-bert)", re.I)
_RTX_RE = re.compile(r"((?:GeForce\s+)?(?:RTX|GTX|RX)\s*\d+\s*(?:SUPER|Ti|XT|S)?)", re.I)
_IGPU_RE = re.compile(r"(\d{2,4}M)\b")

# Synthetic chat models used only when ODYSSEUS_CLUSTER_MOCK=1 and the manager
# API has no live inventory. Names match config.example.json.
_MOCK_CHAT_MODELS = {
    "manager": [
        {"id": "gemma4:12b", "display": "gemma4:12b", "params": "12B"},
        {"id": "qwen3-coder-30b:latest", "display": "qwen3-coder-30b:latest", "params": "30B"},
    ],
    "worker": [
        {"id": "qwen2.5:7b", "display": "qwen2.5:7b", "params": "7B"},
        {"id": "qwen2.5:3b", "display": "qwen2.5:3b", "params": "3B"},
    ],
}


def _truthy(val: str | None, default: bool = False) -> bool:
    if val is None or val.strip() == "":
        return default
    return val.strip().lower() not in ("0", "false", "no", "off")


def _is_enabled() -> bool:
    if not _truthy(os.environ.get("ODYSSEUS_CLUSTER_ENABLED"), default=False):
        return False
    return _truthy(os.environ.get("ODYSSEUS_CLUSTER_NETWORK"), default=True)


def _force_mock() -> bool:
    try:
        from routes.cluster_routes import _force_mock as _mock
        return bool(_mock())
    except Exception:
        return _truthy(os.environ.get("ODYSSEUS_CLUSTER_MOCK"), default=False)


def _bridge_url() -> str:
    return (os.environ.get("ODYSSEUS_CLUSTER_BRIDGE_URL") or "http://host.docker.internal:11610/v1").rstrip("/")


def _manager_url() -> str:
    return (os.environ.get("ODYSSEUS_CLUSTER_API_URL") or "http://host.docker.internal:11600").rstrip("/")


def _data_dir() -> Path:
    return Path(os.environ.get("ODYSSEUS_CLUSTER_DATA_DIR") or "/cluster-data")


def _token() -> str:
    tok = (os.environ.get("ODYSSEUS_CLUSTER_BRIDGE_TOKEN") or "").strip()
    if tok:
        return tok
    token_file = Path(os.environ.get("ODYSSEUS_CLUSTER_BRIDGE_TOKEN_FILE") or "/run/secrets/cluster_bridge_token")
    try:
        return token_file.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _api_headers() -> dict[str, str]:
    tok = (os.environ.get("ODYSSEUS_CLUSTER_API_TOKEN") or "").strip()
    if not tok:
        token_file = Path(os.environ.get("ODYSSEUS_CLUSTER_API_TOKEN_FILE") or "/run/secrets/cluster_api_token")
        try:
            tok = token_file.read_text(encoding="utf-8").strip()
        except OSError:
            tok = ""
    return {"Authorization": f"Bearer {tok}"} if tok else {}


def ensure_cluster_endpoint() -> str | None:
    """Create or refresh the shared Cluster Network endpoint row when enabled."""
    if not _is_enabled():
        return None
    base_url = _bridge_url()
    try:
        from core.database import ModelEndpoint, SessionLocal
    except Exception as e:  # pragma: no cover
        logger.warning("cluster-network: database unavailable: %s", e)
        return None
    tok = _token()
    db = SessionLocal()
    try:
        ep = db.query(ModelEndpoint).filter(ModelEndpoint.id == ENDPOINT_ID).first()
        created = ep is None
        if created:
            ep = ModelEndpoint(id=ENDPOINT_ID, name=ENDPOINT_NAME, base_url=base_url, model_type="llm", owner=None)
            db.add(ep)
        ep.base_url = base_url
        if tok:
            ep.api_key = tok
        ep.is_enabled = True
        ep.endpoint_kind = "local"
        ep.supports_tools = True
        ep.cached_models = json.dumps([MODEL_ID])
        ep.pinned_models = json.dumps([MODEL_ID])
        db.commit()
        logger.info("cluster-network: endpoint %s %s (%s)", ENDPOINT_ID, "created" if created else "refreshed", base_url)
    except Exception as e:
        db.rollback()
        logger.warning("cluster-network: could not register endpoint: %s", e)
        return None
    finally:
        db.close()
    try:
        from routes.model_routes import _invalidate_models_cache
        _invalidate_models_cache()
    except Exception:
        pass
    return ENDPOINT_ID


def _gpu_short(gpu: str | None) -> str:
    if not gpu:
        return ""
    m = _RTX_RE.search(gpu)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).replace("GeForce ", "").strip()
    m = _IGPU_RE.search(gpu)
    if m:
        return m.group(1)
    cleaned = re.sub(r"\s*\(.*?\)", "", gpu)
    cleaned = re.sub(r"^(NVIDIA|AMD|Intel|GeForce|Radeon)\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+iGPU$", "", cleaned, flags=re.I).strip()
    return cleaned or gpu.split("(")[0].strip()


def _name_aliases(dev: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in ("id", "name", "bridge_prefix", "label", "short_label"):
        v = dev.get(key)
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
    for v in dev.get("aliases") or []:
        if isinstance(v, str) and v.strip() and _ip_addr(v.strip()) is None:
            out.append(v.strip())
    seen, uniq = set(), []
    for item in out:
        if item not in seen:
            seen.add(item)
            uniq.append(item)
    return uniq


def _host_label(dev: dict[str, Any], hw: dict[str, Any] | None = None) -> str:
    hw = hw or (dev.get("hardware") or {})
    gpu = _gpu_short(hw.get("gpu") or dev.get("gpu") or "")
    base = (
        (dev.get("label") or dev.get("short_label") or dev.get("name") or dev.get("id") or "Worker")
    )
    if isinstance(base, str):
        base = base.strip()
    else:
        base = "Worker"
    if gpu and gpu.lower() not in base.lower():
        return f"{base} · {gpu}"
    return base


def _short_label(dev: dict[str, Any], label: str) -> str:
    explicit = dev.get("short_label") or dev.get("label")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    if " · " in label:
        return label.split(" · ", 1)[0]
    return label


def _bridge_prefix(dev: dict[str, Any]) -> str:
    raw = dev.get("bridge_prefix")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    for v in dev.get("aliases") or []:
        if isinstance(v, str) and v.strip() and _ip_addr(v.strip()) is None:
            return v.strip()
    return str(dev.get("id") or dev.get("name") or "worker")


def _collect_ips(dev: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in ("ip", "tailscale_ip", "lan_ip", "wifi_ip"):
        v = dev.get(key)
        if isinstance(v, str) and v:
            out.append(v.strip())
    ips = dev.get("ips")
    if isinstance(ips, dict):
        for v in ips.values():
            if isinstance(v, str) and v:
                out.append(v.strip())
    elif isinstance(ips, list):
        for v in ips:
            if isinstance(v, str) and v:
                out.append(v.strip())
    for v in dev.get("ip_aliases") or []:
        if isinstance(v, str) and v:
            out.append(v.strip())
    probe = dev.get("probe") or {}
    for v in probe.get("targets") or []:
        if isinstance(v, str) and v and not v.startswith("http"):
            out.append(v.strip())
    seen, uniq = set(), []
    for ip in out:
        if ip not in seen:
            seen.add(ip)
            uniq.append(ip)
    return uniq


def _registry() -> list[dict[str, Any]]:
    try:
        from routes.cluster_routes import _registry as _reg
        return list(_reg() or [])
    except Exception:
        pass
    p = _data_dir() / "devices.json"
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return list(data.get("devices") or [])
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def _is_chat_model(name: str) -> bool:
    return bool(name) and not _EMBED_RE.search(name)


def _local_ollama_endpoint() -> dict[str, str]:
    fallback = {"id": "", "name": "Local Ollama", "base_url": "http://host.docker.internal:11434/v1"}
    try:
        from core.database import ModelEndpoint, SessionLocal
    except Exception:
        return fallback
    db = SessionLocal()
    try:
        rows = db.query(ModelEndpoint).filter(ModelEndpoint.is_enabled.is_(True)).all()
        for ep in rows:
            if ep.id == ENDPOINT_ID:
                continue
            url = ep.base_url or ""
            if ":11434" in url:
                return {"id": ep.id, "name": ep.name or "Local Ollama", "base_url": url}
    except Exception as e:
        logger.debug("cluster-network: local ollama endpoint lookup failed: %s", e)
    finally:
        db.close()
    return fallback


async def _manager_nodes() -> list[dict[str, Any]]:
    headers = _api_headers()
    try:
        async with httpx.AsyncClient(timeout=3.0) as c:
            r = await c.get(_manager_url() + "/api/nodes", headers=headers)
            r.raise_for_status()
            j = r.json()
            return list(j.get("nodes") or [])
    except Exception as e:
        logger.debug("cluster-network: manager /api/nodes failed: %s", e)
        return []


def _installed_names(node: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    ol = node.get("ollama") or {}
    info = node.get("ollama_info") or {}
    installed = ol.get("installed") or info.get("installed") or node.get("models") or []
    models = []
    for item in installed:
        if isinstance(item, str):
            name = item
            extra: dict[str, Any] = {}
        elif isinstance(item, dict):
            name = item.get("name") or item.get("model") or ""
            extra = item
        else:
            continue
        if not _is_chat_model(name):
            continue
        details = extra.get("details")
        quant = extra.get("quant")
        if not quant and isinstance(details, dict):
            quant = details.get("quantization_level")
        models.append({
            "id": name,
            "display": name.split("/")[-1],
            "params": extra.get("params"),
            "quant": quant,
            "size_gb": extra.get("size_gib") or extra.get("size_gb"),
            "family": extra.get("family"),
        })
    if "up" in ol:
        up = bool(ol.get("up"))
    else:
        up = bool(info.get("reachable", bool(installed)))
    return models, up


def _ip_addr(s: str) -> ipaddress._BaseAddress | None:
    try:
        return ipaddress.ip_address(s.split("%")[0].strip())
    except Exception:
        return None


def _is_loopback_or_docker(ip: str) -> bool:
    addr = _ip_addr(ip)
    if addr is None:
        return ip in {"localhost", "host.docker.internal", "::1"}
    if addr.is_loopback or addr.is_unspecified:
        return True
    # Docker / container bridges commonly land here when Odysseus is published on loopback.
    if isinstance(addr, ipaddress.IPv4Address) and addr in ipaddress.ip_network("172.16.0.0/12"):
        return True
    return False


def _request_ips(request: Request | None) -> list[str]:
    if request is None:
        return []
    found: list[str] = []

    def add(raw: str | None):
        if not raw:
            return
        for part in str(raw).split(","):
            part = part.strip()
            if "for=" in part.lower():
                # RFC 7239 Forwarded: for=1.2.3.4 or for="[::1]"
                m = re.search(r"for\s*=\s*\"?\[?([^;,\s\]\"]+)", part, re.I)
                part = m.group(1) if m else part
            part = part.strip().strip("\"[]")
            if part and part not in found:
                found.append(part)

    # Prefer proxy headers: a serve/reverse-proxy hop often makes request.client
    # the container or loopback address, with the real browser IP forwarded.
    add(request.headers.get("x-forwarded-for"))
    add(request.headers.get("x-real-ip"))
    add(request.headers.get("x-forwarded"))
    add(request.headers.get("forwarded"))
    if request.client and request.client.host:
        add(request.client.host)
    return found


def _manager_device(devices: list[dict[str, Any]]) -> dict[str, Any] | None:
    for dev in devices:
        if dev.get("is_host") or dev.get("role") == "manager":
            return dev
    return None


def _disabled_session() -> dict[str, Any]:
    return {
        "remote": False,
        "client_device_id": None,
        "client_name": "",
        "label": "",
        "client_ip": None,
        "host_header": "",
    }


def _session_from_request(request: Request | None, devices: list[dict[str, Any]]) -> dict[str, Any]:
    ips = _request_ips(request)
    host = ""
    if request is not None:
        host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(":")[0].lower()

    by_ip: dict[str, dict[str, Any]] = {}
    local_ids = set()
    for dev in devices:
        did = dev.get("id") or ""
        if dev.get("is_host") or dev.get("role") == "manager":
            local_ids.add(did)
        for ip in _collect_ips(dev):
            by_ip[ip] = dev

    matched = None
    matched_ip = None
    usable = []
    for ip in ips:
        if _ip_addr(ip) is None and not _is_loopback_or_docker(ip):
            continue
        usable.append(ip)
        if ip in by_ip:
            matched, matched_ip = by_ip[ip], ip
            break
        if _is_loopback_or_docker(ip) and not matched:
            # No forwarded client: treat loopback as the manager host itself.
            matched = _manager_device(devices)
            matched_ip = ip

    is_local = bool(matched and (matched.get("id") in local_ids))
    if not is_local and not matched and all(_is_loopback_or_docker(ip) for ip in (usable or ["127.0.0.1"])):
        is_local = True
        matched = _manager_device(devices)

    if is_local:
        mgr = matched or {}
        return {
            "remote": False,
            "client_device_id": mgr.get("id") or "manager",
            "client_name": mgr.get("name") or mgr.get("label") or mgr.get("id") or "manager",
            "label": "",
            "client_ip": matched_ip or (usable[0] if usable else None),
            "host_header": host,
        }

    if matched:
        name = matched.get("name") or matched.get("label") or matched.get("id") or "remote device"
        return {
            "remote": True,
            "client_device_id": matched.get("id"),
            "client_name": name,
            "label": f"Remote session from {name}: models run on your cluster, not this device",
            "client_ip": matched_ip,
            "host_header": host,
        }

    return {
        "remote": True,
        "client_device_id": None,
        "client_name": "Remote",
        "label": "Remote session: models run on your cluster, not this device",
        "client_ip": usable[0] if usable else None,
        "host_header": host,
    }


def _mock_models(is_mgr: bool) -> list[dict[str, Any]]:
    return [dict(m) for m in _MOCK_CHAT_MODELS["manager" if is_mgr else "worker"]]


async def _hosts_payload(request: Request | None = None) -> dict[str, Any]:
    enabled = _is_enabled()
    base_url = _bridge_url()
    cluster_network = {
        "model": MODEL_ID,
        "endpoint_id": ENDPOINT_ID,
        "endpoint_url": base_url if enabled else None,
        "endpoint_name": ENDPOINT_NAME,
        "label": "Cluster (routed)",
        "online": False,
    }
    if not enabled:
        return {
            "enabled": False,
            "cluster_network": cluster_network,
            "manager": None,
            "workers": [],
            "session": _disabled_session(),
            "healthy": False,
            "checks": None,
        }

    devices = _registry()
    mgr_nodes = {n.get("id") or n.get("name"): n for n in await _manager_nodes() if isinstance(n, dict)}
    local_ep = _local_ollama_endpoint()
    compute = [d for d in devices if (d.get("type") or "compute") == "compute"]
    if not compute:
        compute = [{"id": nid, "name": n.get("name") or nid, "type": "compute",
                    "role": n.get("role"), "is_host": n.get("role") == "manager",
                    "hardware": {}, "ip": n.get("ip")} for nid, n in mgr_nodes.items()]

    def pack(dev: dict[str, Any]) -> dict[str, Any]:
        nid = dev.get("id") or dev.get("name")
        live = mgr_nodes.get(nid) or {}
        # Also accept a live node keyed by an alias or bridge prefix.
        if not live:
            for alias in _name_aliases(dev):
                if alias in mgr_nodes:
                    live = mgr_nodes[alias]
                    break
        hw = dict(dev.get("hardware") or {})
        live_hw = live.get("hardware") or {}
        if isinstance(live_hw, dict):
            if not hw.get("gpu"):
                gpu = live_hw.get("gpu")
                if isinstance(gpu, dict):
                    hw["gpu"] = gpu.get("model")
                elif isinstance(gpu, str):
                    hw["gpu"] = gpu
        models, ollama_up = _installed_names(live)
        is_mgr = bool(dev.get("is_host") or dev.get("role") == "manager")
        if not models and _force_mock():
            models = _mock_models(is_mgr)
            ollama_up = True
        st = str((live or {}).get("status") or "").lower()
        if _force_mock() and not live:
            online = True
        elif not live:
            online = False
        elif st in ("down", "offline", "error", "unreachable"):
            online = False
        else:
            online = bool(ollama_up) or st in ("ok", "online", "up", "healthy", "")
        label = _host_label(dev, hw)
        prefix = _bridge_prefix(dev)
        out_models = []
        for m in models:
            if is_mgr:
                route_model, endpoint_id, endpoint_url, via = m["id"], local_ep["id"], local_ep["base_url"], "local-ollama"
            else:
                route_model = f"{prefix}/{m['id']}"
                endpoint_id, endpoint_url, via = ENDPOINT_ID, base_url, "bridge"
            out_models.append({
                **m,
                "route_model": route_model,
                "endpoint_id": endpoint_id,
                "endpoint_url": endpoint_url,
                "endpoint_name": ENDPOINT_NAME if via == "bridge" else local_ep["name"],
                "via": via,
                "online": online,
            })
        return {
            "id": nid,
            "name": dev.get("name") or nid,
            "role": dev.get("role") or ("manager" if is_mgr else "worker"),
            "job": dev.get("job") or (live.get("job") if isinstance(live.get("job"), str) else None),
            "is_manager": is_mgr,
            "label": label,
            "short_label": _short_label(dev, label),
            "bridge_prefix": prefix,
            "aliases": _name_aliases(dev),
            "hardware": hw,
            "gpu": hw.get("gpu"),
            "gpu_short": _gpu_short(hw.get("gpu")),
            "gpu_vendor": (hw.get("gpu_vendor") or "").lower(),
            "cpu_vendor": (hw.get("cpu_vendor") or "").lower(),
            "online": online,
            "ip": dev.get("ip"),
            "ips": _collect_ips(dev),
            "models": out_models,
        }

    packed = [pack(d) for d in compute]
    manager = next((p for p in packed if p["is_manager"]), packed[0] if packed else None)
    workers = [p for p in packed if not p["is_manager"]]
    healthy, detail = False, None
    try:
        tok = _token()
        headers = {"Authorization": f"Bearer {tok}"} if tok else {}
        async with httpx.AsyncClient(timeout=2.5) as c:
            r = await c.get(base_url.rsplit("/v1", 1)[0] + "/health", headers=headers)
            j = r.json()
            healthy, detail = bool(j.get("ok")), j.get("checks")
    except Exception as e:
        detail = str(e)[:200]
    cluster_network["online"] = healthy

    session = _session_from_request(request, devices)
    return {
        "enabled": True,
        "cluster_network": cluster_network,
        "manager": manager,
        "workers": workers,
        "session": session,
        "healthy": healthy,
        "checks": detail,
    }


def setup_cluster_network_routes() -> APIRouter:
    router = APIRouter(prefix="/api/cluster-network", tags=["cluster-network"])
    if _is_enabled():
        ensure_cluster_endpoint()

    @router.get("/config")
    async def config(request: Request):
        enabled = _is_enabled()
        base_url = _bridge_url()
        healthy, detail = False, None
        if enabled:
            try:
                tok = _token()
                headers = {"Authorization": f"Bearer {tok}"} if tok else {}
                async with httpx.AsyncClient(timeout=2.5) as c:
                    r = await c.get(base_url.rsplit("/v1", 1)[0] + "/health", headers=headers)
                    j = r.json()
                    healthy, detail = bool(j.get("ok")), j.get("checks")
            except Exception as e:
                detail = str(e)[:200]
        session = _session_from_request(request, _registry()) if enabled else _disabled_session()
        return {
            "enabled": enabled,
            "endpoint_id": ENDPOINT_ID,
            "endpoint_name": ENDPOINT_NAME,
            "model": MODEL_ID,
            "base_url": base_url if enabled else None,
            "healthy": healthy,
            "checks": detail,
            "default_on": True,
            "pref_key": "cluster_network_mode",
            "session": session,
        }

    @router.get("/hosts")
    async def hosts(request: Request):
        return await _hosts_payload(request)

    @router.get("/session")
    async def session(request: Request):
        if not _is_enabled():
            return _disabled_session() | {"enabled": False}
        return _session_from_request(request, _registry()) | {"enabled": True}

    return router
