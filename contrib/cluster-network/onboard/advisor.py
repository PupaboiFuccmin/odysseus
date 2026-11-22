#!/usr/bin/env python3
"""Advise a role, models, chain-of-command position, and setup checklist
for a new machine, given probe JSON from onboard/probe.sh.

Used by `cluster onboard` and by the manager's /api/onboard/* endpoints.
Never runs sudo. --apply only writes devices.json and
state/extra_flows.json after explicit confirmation.
"""
from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import sys
import time
from copy import deepcopy
from pathlib import Path

SCHEMA = "llm-cluster-onboard-advice/1"
ROOT = Path(os.path.expanduser(os.environ.get("LLM_CLUSTER_HOME") or str(Path(__file__).resolve().parents[1])))
DEVICES_PATH = ROOT / "devices.json"
CONFIG_PATH = ROOT / "config.json"
EXTRA_FLOWS_PATH = ROOT / "state" / "extra_flows.json"
DEFAULT_MANAGER_ID = "manager-node"
DEFAULT_MANAGER_IP = "192.0.2.10"
DEFAULT_MANAGER_URL = f"http://{DEFAULT_MANAGER_IP}:11600"
PROTECTED_IDS = {"manager-node"}

# --------------------------------------------------------------------------- model catalog
# VRAM figures are conservative "loaded in Ollama" sizes (Q4_K_M unless noted),
# conservative defaults based on model footprint + 10% VRAM margin.
# A model "fits" when usable_vram_gb * 0.90 >= vram_gb (or, on CPU-only,
# ram_gb * 0.45 >= ram_gb_need).
MODELS = [
    {
        "name": "nomic-embed-text",
        "roles": ["embeddings", "storage/memory", "cpu-worker", "gpu-worker", "monitor/sentinel"],
        "params": "137M",
        "quant": "F16",
        "vram_gb": 0.3,
        "ram_gb": 0.6,
        "why": "Already used by the Odysseus bridge for cluster memory (Chroma). Tiny; fits almost anywhere.",
        "existing": True,
        "priority": 10,
    },
    {
        "name": "qwen2.5:3b",
        "roles": ["cpu-worker", "gpu-worker", "monitor/sentinel"],
        "params": "3B",
        "quant": "Q4_K_M",
        "vram_gb": 2.2,
        "ram_gb": 3.5,
        "why": "Fast worker model. Short/simple/summarize work (~30+ tok/s on a 680M-class APU).",
        "existing": True,
        "priority": 20,
    },
    {
        "name": "qwen2.5:7b",
        "roles": ["cpu-worker", "gpu-worker", "reviewer"],
        "params": "7B",
        "quant": "Q4_K_M",
        "vram_gb": 4.7,
        "ram_gb": 7.0,
        "why": "General worker model. Strong quality for 7B-class APUs and entry GPUs (~15 tok/s on Vulkan).",
        "existing": True,
        "priority": 30,
    },
    {
        "name": "qwen2.5-coder:7b",
        "roles": ["cpu-worker", "gpu-worker", "reviewer"],
        "params": "7B",
        "quant": "Q4_K_M",
        "vram_gb": 4.7,
        "ram_gb": 7.0,
        "why": "Code-focused 7B worker model; first-pass edits reviewed by a larger manager/reviewer node.",
        "existing": True,
        "priority": 31,
    },
    {
        "name": "gemma4:12b",
        "roles": ["gpu-worker", "reviewer"],
        "params": "12B",
        "quant": "Q4_K_M",
        "vram_gb": 8.0,
        "ram_gb": 14.0,
        "why": "Balanced manager / router / rubric / synthesizer model for 12 GB-class discrete GPUs.",
        "existing": True,
        "priority": 40,
    },
    {
        "name": "qwen3:14b",
        "roles": ["gpu-worker", "reviewer"],
        "params": "14B",
        "quant": "Q4_K_M",
        "vram_gb": 9.3,
        "ram_gb": 16.0,
        "why": "Optional independent 14B text/plan reviewer for 12 GB+ GPUs.",
        "existing": True,
        "priority": 45,
    },
    {
        "name": "qwen3-coder-30b:latest",
        "roles": ["gpu-worker", "reviewer"],
        "params": "30B",
        "quant": "Q4_K_M",
        "vram_gb": 9.9,
        "ram_gb": 20.0,
        "why": "Code generation and cross-review model (~10 GiB VRAM). Strong code reviewer; not a general router.",
        "existing": True,
        "priority": 50,
    },
]

ROLES = (
    "gpu-worker",
    "cpu-worker",
    "reviewer",
    "embeddings",
    "storage/memory",
    "monitor/sentinel",
    "controller",
    "peripheral",
)

ROLE_JOBS = {
    "gpu-worker": "Runs assigned generate / review work on a GPU (or capable APU)",
    "cpu-worker": "Runs small models on CPU (or a weak iGPU) for simple tasks",
    "reviewer": "Second-pass review of another node's output (independent of the writer)",
    "embeddings": "Always-on embeddings / cluster memory helper (nomic-embed-text)",
    "storage/memory": "Holds models, backups, or Chroma/memory data; light or no generate",
    "monitor/sentinel": "Low-power always-on health, sensors, and alerting",
    "controller": "Wireless / portable Odysseus + Cluster tab client; not a model host",
    "peripheral": "Non-compute hardware (printer, camera, cooling, sensor) observed by the cluster",
}

# devices.json `type` for the Cluster tab
ROLE_DEVICE_TYPE = {
    "gpu-worker": "compute",
    "cpu-worker": "compute",
    "reviewer": "compute",
    "embeddings": "compute",
    "storage/memory": "generic",
    "monitor/sentinel": "generic",
    "controller": "controller",
    "peripheral": "generic",
}


# --------------------------------------------------------------------------- IO helpers
def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def load_config() -> dict:
    return load_json(CONFIG_PATH, {}) or {}


def load_devices() -> dict:
    data = load_json(DEVICES_PATH, {"devices": []}) or {"devices": []}
    if isinstance(data, list):
        data = {"devices": data}
    data.setdefault("devices", [])
    return data


def load_cluster_context() -> dict:
    cfg = load_config()
    devs = load_devices()
    models_cfg = cfg.get("models") or {}
    existing = []
    for d in devs.get("devices") or []:
        existing.append({
            "id": d.get("id"),
            "name": d.get("name") or d.get("id"),
            "type": d.get("type"),
            "role": d.get("role"),
            "ip": d.get("ip"),
            "hardware": d.get("hardware") or {},
        })
    return {
        "manager_id": cfg.get("node") or DEFAULT_MANAGER_ID,
        "manager_ip": DEFAULT_MANAGER_IP,
        "manager_url": (cfg.get("onboard") or {}).get("manager_url") or DEFAULT_MANAGER_URL,
        "peer_id": (cfg.get("peer") or {}).get("name") or "worker-node",
        "peer_ip": (cfg.get("peer") or {}).get("ip") or "192.0.2.20",
        "models": models_cfg,
        "devices": existing,
        "config": cfg,
    }


def _num(v, default=None):
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def primary_gpu(probe: dict) -> dict:
    gpus = probe.get("gpu") or []
    if not gpus:
        return {}
    def key(g):
        vram = _num(g.get("vram_gb"), 0) or 0
        discrete = 0 if g.get("unified_memory") else 1
        return (discrete, vram)
    return max(gpus, key=key)


def usable_memory(probe: dict) -> dict:
    """Estimate the memory a model can actually occupy."""
    mem = probe.get("memory") or {}
    ram_gb = _num(mem.get("ram_gb") or mem.get("ram_total_gb"), 0) or 0
    gpu = primary_gpu(probe)
    vram = _num(gpu.get("vram_gb"), None)
    vendor = (gpu.get("vendor") or "").lower()
    model = (gpu.get("model") or "").lower()
    igpu = vendor in ("tegra",) or any(
        s in model for s in ("680m", "780m", "760m", "880m", "rembrandt", "raphael",
                             "phoenix", "strix", "uhd graphics", "iris", "tegra",
                             "mali", "adreno", "radeon graphics")
    )
    # lspci/sysfs often report a tiny BAR (0.5 GB) for APUs; that is not discrete VRAM.
    tiny_bar = vram is not None and vram < 2
    unified = bool(gpu.get("unified_memory")) or vendor == "tegra" or (igpu and (vram is None or tiny_bar)) or (
        vendor in ("amd", "intel") and tiny_bar
    )
    if unified and tiny_bar:
        vram = None
    if vram is not None and not unified:
        return {
            "kind": "discrete",
            "vram_gb": vram,
            "ram_gb": ram_gb,
            "usable_vram_gb": vram,
            "usable_ram_gb": ram_gb,
            "gpu": gpu,
        }
    # Unified / APU / Tegra: a fraction of system RAM is realistically usable
    # for a model (the rest is OS, desktop, GTT overhead).
    if vendor == "tegra" or ram_gb <= 4:
        frac = 0.35
        cap = min(2.5, ram_gb * frac) if ram_gb else 1.5
    elif ram_gb <= 8:
        frac, cap = 0.40, 3.5
    elif ram_gb <= 16:
        frac, cap = 0.45, 6.5
    else:
        frac, cap = 0.50, 8.0
    usable = round(ram_gb * frac, 2) if ram_gb else 0
    # iGPUs share RAM but practically run 7B-class models (the 680M catalog).
    # Cap so we don't recommend gemma4:12b / qwen3-coder-30b on a 32 GB APU.
    usable = min(usable, cap)
    # If sysfs reported a VRAM window, don't pretend we have more than that
    # unless it's a tiny aperture (< 1 GB) which is usually just the BAR.
    if vram is not None and vram >= 1:
        usable = min(usable or vram, vram)
    return {
        "kind": "unified" if (unified or gpu) else "cpu",
        "vram_gb": vram,
        "ram_gb": ram_gb,
        "usable_vram_gb": usable if gpu else 0,
        "usable_ram_gb": ram_gb,
        "gpu": gpu,
    }


def model_fits(model: dict, mem: dict) -> tuple[bool, str]:
    need_v = _num(model.get("vram_gb"), 0) or 0
    need_r = _num(model.get("ram_gb"), need_v * 1.4) or 0
    vram = _num(mem.get("usable_vram_gb"), 0) or 0
    ram = _num(mem.get("usable_ram_gb"), 0) or 0
    if vram > 0:
        if vram * 0.90 >= need_v:
            return True, f"fits in ~{vram} GB usable VRAM (needs ~{need_v} GB {model.get('quant')})"
        # tight: might run with CPU offload
        if ram >= need_r and vram * 1.4 >= need_v:
            return False, (
                f"tight: needs ~{need_v} GB, usable VRAM ~{vram} GB; "
                f"would spill to system RAM (slow)"
            )
        return False, f"does not fit: needs ~{need_v} GB VRAM, usable ~{vram} GB"
    # CPU only
    if ram * 0.45 >= need_r or ram >= need_r + 4:
        return True, f"CPU-only fit in ~{ram} GB RAM (needs ~{need_r} GB, will be slow)"
    if ram >= need_r:
        return False, f"tight on CPU: ~{ram} GB RAM, model wants ~{need_r} GB"
    return False, f"does not fit on CPU: ~{ram} GB RAM, needs ~{need_r} GB"


def _ip_rank(ip: str) -> int:
    """Prefer RFC1918 LAN addresses over Tailscale CGNAT / other."""
    if ip.startswith("192.168.") or ip.startswith("10.") or ip.startswith("172."):
        # 172.16–31 only is private; coarse is fine for this house.
        return 0
    if ip.startswith("100."):
        return 2
    return 1


def first_ip(probe: dict) -> str | None:
    net = probe.get("network") or {}
    cands = [ip for ip in (net.get("ips") or []) if ip and not ip.startswith("127.") and not ip.startswith("169.254.")]
    if not cands:
        return None
    return sorted(cands, key=_ip_rank)[0]


def suggest_id(probe: dict, role: str) -> str:
    host = (probe.get("hostname") or "").strip().lower()
    host = re.sub(r"[^a-z0-9-]+", "-", host).strip("-")
    if host in {"", "localhost", "archlinux", "ubuntu", "linux"}:
        ip = first_ip(probe) or ""
        tail = ip.split(".")[-1] if ip else "new"
        prefix = {
            "gpu-worker": "gpu",
            "cpu-worker": "cpu",
            "reviewer": "reviewer",
            "embeddings": "embed",
            "storage/memory": "storage",
            "monitor/sentinel": "sentinel",
            "controller": "controller",
            "peripheral": "peripheral",
        }.get(role, "node")
        host = f"{prefix}-{tail}"
    if len(host) < 2:
        host = "node-new"
    return host[:40]


def detect_peripheral_subtype(probe: dict) -> str | None:
    """Look at free-text hints only (not JSON keys — 'chassis' contains 'hass')."""
    hints = probe.get("hints") or {}
    parts = [
        hints.get("product"), hints.get("device_class_guess"), hints.get("chassis"),
        hints.get("note"), probe.get("hostname"),
        (probe.get("cpu") or {}).get("device_tree_model"),
        (probe.get("cpu") or {}).get("model"),
        (probe.get("os") or {}).get("name"),
    ]
    for g in probe.get("gpu") or []:
        parts.append(g.get("model"))
    blob = " ".join(str(x) for x in parts if x).lower()
    if re.search(r"\b(octoprint|moonraker|klipper|voron|printer)\b", blob):
        return "printer"
    if re.search(r"\b(frigate|rtsp|camera|onvif|webcam)\b", blob):
        return "camera"
    if re.search(r"\b(mqtt|cooling|sensor|thermostat|shelly|hass|homeassistant)\b", blob):
        return "cooling" if re.search(r"\b(cool|cooling)\b", blob) else "generic"
    return None


# --------------------------------------------------------------------------- role picker
def pick_role(probe: dict, override: str | None = None) -> dict:
    if override:
        if override not in ROLES:
            raise ValueError(f"unknown role {override!r}; choose one of {', '.join(ROLES)}")
        return {"role": override, "secondary": [], "reasons": [f"role forced to {override}"], "confidence": "manual"}

    mem = usable_memory(probe)
    ram = mem["ram_gb"] or 0
    vram = mem["usable_vram_gb"] or 0
    gpu = mem.get("gpu") or {}
    vendor = (gpu.get("vendor") or "").lower()
    hints = probe.get("hints") or {}
    guess = (hints.get("device_class_guess") or "").lower()
    chassis = (hints.get("chassis") or "").lower()
    arch = (probe.get("arch") or hints.get("arch") or "").lower()
    disk_free = _num(((probe.get("disk") or {}).get("root") or {}).get("free_gb"), 0) or 0
    disk_total = _num(((probe.get("disk") or {}).get("root") or {}).get("total_gb"), 0) or 0
    cores = int((probe.get("cpu") or {}).get("cores") or (probe.get("cpu") or {}).get("threads") or 0)
    battery = bool(hints.get("battery"))
    reasons = []
    subtype = detect_peripheral_subtype(probe)
    if subtype:
        reasons.append(f"probe mentions {subtype}-style hardware")
        return {"role": "peripheral", "secondary": [], "reasons": reasons, "confidence": "high",
                "device_type": subtype, "subtype": subtype}

    # Handheld / very small ARM with little RAM: controller
    dt = ((probe.get("cpu") or {}).get("device_tree_model") or hints.get("product") or "").lower()
    if "nintendo switch" in dt or guess == "controller" or (vendor == "tegra" and ram <= 8 and battery):
        reasons.append("handheld / Tegra-class device with limited RAM — a controller, not a model host")
        return {"role": "controller", "secondary": ["monitor/sentinel"], "reasons": reasons, "confidence": "high"}

    if guess == "sbc" or (arch.startswith("aarch") and ram <= 8 and vram < 2 and not battery):
        reasons.append("low-power SBC / ARM box with modest memory")
        if disk_total >= 2000:
            return {"role": "storage/memory", "secondary": ["monitor/sentinel", "embeddings"], "reasons": reasons, "confidence": "medium"}
        return {"role": "monitor/sentinel", "secondary": ["embeddings"] if ram >= 2 else [], "reasons": reasons, "confidence": "medium"}

    # Lots of disk, weak compute → storage
    if disk_total >= 2000 and vram < 4 and guess in ("nas", "unknown", "cpu-box", ""):
        reasons.append(f"large disk ({disk_total:.0f} GB) and little GPU — storage/memory first")
        secondary = ["embeddings"] if ram >= 8 else ["monitor/sentinel"]
        return {"role": "storage/memory", "secondary": secondary, "reasons": reasons, "confidence": "medium"}

    discrete = mem["kind"] == "discrete"
    if discrete and vram >= 8:
        reasons.append(f"discrete GPU with ~{vram} GB VRAM — gpu-worker")
        secondary = ["reviewer"] if vram >= 9 else []
        if vram >= 16:
            reasons.append("VRAM is at or above the current manager (12 GB); could also host reviewer / future manager-class models")
            secondary = ["reviewer"]
        return {"role": "gpu-worker", "secondary": secondary, "reasons": reasons, "confidence": "high"}

    if discrete and vram >= 4:
        reasons.append(f"discrete GPU with ~{vram} GB VRAM — lighter gpu-worker (7B-class)")
        return {"role": "gpu-worker", "secondary": ["embeddings"], "reasons": reasons, "confidence": "high"}

    # APU / unified
    if mem["kind"] == "unified" and (vram >= 3.5 or ram >= 16):
        reasons.append(
            f"APU / unified memory (~{ram} GB RAM, usable ~{vram} GB) — APU worker class"
        )
        return {"role": "gpu-worker", "secondary": ["embeddings"], "reasons": reasons, "confidence": "high"}

    if mem["kind"] == "unified" and ram >= 8:
        reasons.append(f"modest iGPU / unified memory (~{ram} GB) — small models only")
        return {"role": "cpu-worker", "secondary": ["embeddings"], "reasons": reasons, "confidence": "medium"}

    if ram >= 16 and cores >= 8:
        reasons.append(f"CPU-heavy box ({cores} cores, {ram} GB RAM) with little or no GPU")
        return {"role": "cpu-worker", "secondary": ["embeddings", "reviewer"], "reasons": reasons, "confidence": "medium"}

    if ram >= 8:
        reasons.append(f"{ram} GB RAM and no usable GPU — embeddings or small CPU models")
        return {"role": "embeddings", "secondary": ["cpu-worker"], "reasons": reasons, "confidence": "medium"}

    if battery or chassis in ("laptop", "convertible") or guess == "laptop":
        reasons.append("laptop / battery device with limited headroom — better as a controller than a worker")
        return {"role": "controller", "secondary": ["monitor/sentinel"], "reasons": reasons, "confidence": "medium"}

    reasons.append("limited resources; defaulting to a monitor/sentinel")
    return {"role": "monitor/sentinel", "secondary": [], "reasons": reasons, "confidence": "low"}


def pick_models(probe: dict, role: str, secondary: list[str] | None = None) -> list[dict]:
    mem = usable_memory(probe)
    wanted = [role] + list(secondary or [])
    if role in ("controller", "peripheral"):
        return []
    out = []
    for m in MODELS:
        if not any(r in (m.get("roles") or []) for r in wanted) and role not in (m.get("roles") or []):
            # still mention nomic for storage
            if not (role == "storage/memory" and m["name"] == "nomic-embed-text"):
                continue
        fits, reason = model_fits(m, mem)
        rec = {
            "name": m["name"],
            "quant": m["quant"],
            "params": m["params"],
            "vram_gb": m["vram_gb"],
            "fits": fits,
            "fit_reason": reason,
            "why": m["why"],
            "existing_in_cluster": m["existing"],
            "recommend": fits,
        }
        out.append(rec)
    # Rank: recommended + existing first, then by priority implied by list order
    out.sort(key=lambda r: (0 if r["recommend"] else 1, 0 if r["existing_in_cluster"] else 1))
    # Keep the list tight: all that fit, plus at most one "tight / no" so the
    # operator sees the next model that *doesn't* fit.
    keep = [r for r in out if r["recommend"]]
    rest = [r for r in out if not r["recommend"]]
    if rest:
        keep.append(rest[0])
    # Always mention a no-fit of the big desktop models when this is a small box,
    # so the operator doesn't try to pull qwen3-coder-30b onto 4 GB RAM.
    if role in ("cpu-worker", "embeddings", "monitor/sentinel", "storage/memory"):
        for name in ("gemma4:12b", "qwen3-coder-30b:latest"):
            if not any(r["name"] == name for r in keep):
                for r in out:
                    if r["name"] == name:
                        keep.append(r)
                        break
    return keep


def backend_guess(probe: dict) -> str | None:
    gpu = primary_gpu(probe)
    vendor = (gpu.get("vendor") or "").lower()
    if vendor == "nvidia":
        return "CUDA"
    if vendor == "amd":
        return "Vulkan"  # measured winner on the 680M; ROCm is optional
    if vendor == "intel":
        return "Vulkan"
    if vendor == "tegra":
        return None
    if (probe.get("ollama") or {}).get("present"):
        return "CPU"
    return None


# --------------------------------------------------------------------------- flows / position
def suggested_flows(role: str, device_id: str, ctx: dict) -> list[dict]:
    me = ctx.get("manager_id") or DEFAULT_MANAGER_ID
    if role == "controller":
        # Odysseus already draws remote.<id> from type=controller. Don't duplicate.
        return []
    if role == "peripheral":
        return [{
            "id": f"peripheral.{device_id}",
            "from": device_id,
            "to": me,
            "type": "network",
            "direction": "inbound",
            "label": f"Peripheral status from {device_id}",
            "expected_interval_s": None,
        }]
    if role in ("monitor/sentinel",):
        return [
            {"id": f"monitor.heartbeat-{device_id}", "from": device_id, "to": me, "type": "monitor",
             "direction": "inbound", "label": f"{device_id} heartbeat", "expected_interval_s": None},
            {"id": f"alert.from-{device_id}", "from": device_id, "to": me, "type": "alert",
             "direction": "inbound", "label": f"alerts from {device_id}", "expected_interval_s": None},
        ]
    if role in ("storage/memory", "embeddings"):
        return [
            {"id": f"tools.memory-{device_id}", "from": me, "to": device_id, "type": "tools",
             "direction": "bidirectional", "label": f"memory / embeddings on {device_id}",
             "expected_interval_s": None},
            {"id": f"monitor.heartbeat-{device_id}", "from": device_id, "to": me, "type": "monitor",
             "direction": "inbound", "label": f"{device_id} heartbeat", "expected_interval_s": None},
        ]
    # workers + reviewers
    flows = [
        {"id": f"task.dispatch-{device_id}", "from": me, "to": device_id, "type": "task",
         "direction": "outbound", "label": f"jobs handed to {device_id}", "expected_interval_s": None},
        {"id": f"task.result-{device_id}", "from": device_id, "to": me, "type": "task",
         "direction": "inbound", "label": f"{device_id} returns finished jobs", "expected_interval_s": None},
        {"id": f"review.verdict-{device_id}", "from": me, "to": device_id, "type": "review",
         "direction": "outbound", "label": f"desktop checks {device_id} output", "expected_interval_s": None},
        {"id": f"monitor.heartbeat-{device_id}", "from": device_id, "to": me, "type": "monitor",
         "direction": "inbound", "label": f"{device_id} heartbeat", "expected_interval_s": None},
        {"id": f"monitor.poll-{device_id}", "from": me, "to": device_id, "type": "monitor",
         "direction": "outbound", "label": f"desktop polls {device_id}", "expected_interval_s": None},
        {"id": f"network.link-{device_id}", "from": me, "to": device_id, "type": "network",
         "direction": "bidirectional", "label": f"link {me} ↔ {device_id}", "expected_interval_s": None},
    ]
    if role == "reviewer":
        flows.insert(0, {
            "id": f"review.from-{device_id}", "from": device_id, "to": me, "type": "review",
            "direction": "inbound", "label": f"{device_id} reviews manager / peer output",
            "expected_interval_s": None,
        })
    return flows


def position_text(role: str, device_id: str, ctx: dict) -> str:
    me = ctx.get("manager_id") or DEFAULT_MANAGER_ID
    peer = ctx.get("peer_id") or "worker-node"
    if role == "controller":
        return (
            f"{device_id} sits above {me} on the chain-of-command map as a remote controller, "
            f"controller pattern. Odysseus draws a cyan “Remote control / chat” line "
            f"automatically once the device type is `controller`. It does not run models."
        )
    if role == "peripheral":
        return (
            f"{device_id} is a leaf on the map, off to the side with the router. A gray "
            f"peripheral/status line runs to {me}. The manager does not dispatch LLM tasks to it."
        )
    if role == "monitor/sentinel":
        return (
            f"{device_id} sits beside the existing nodes as a sentinel. Amber monitoring / red "
            f"alert lines go {device_id} → {me}. No green task line unless you later promote it."
        )
    if role in ("storage/memory", "embeddings"):
        return (
            f"{device_id} sits under {me} as a memory/storage helper (teal tools line, plus an "
            f"amber heartbeat). It is not in the default generate chain ({me} ⇄ {peer})."
        )
    if role == "reviewer":
        return (
            f"{device_id} sits next to {me} as an independent reviewer (blue review lines both "
            f"ways). Generate work still flows {me} ⇄ {peer} unless you also assign it worker flows."
        )
    return (
        f"{device_id} sits under {me} as a worker, alongside {peer}. Green task lines go "
        f"{me} → {device_id} → {me}; blue review lines go {me} → {device_id}; amber monitoring "
        f"is two-way. The manager keeps classifying and checking; this node only runs assigned work."
    )


# --------------------------------------------------------------------------- checklist
def _fence(title: str, body: str, undo: str | None = None) -> str:
    parts = [f"```bash", f"# {title}", body.strip(), "```"]
    if undo:
        parts += ["", "Undo:", "```bash", undo.strip(), "```"]
    return "\n".join(parts)


def build_checklist(advice: dict, probe: dict, ctx: dict) -> str:
    role = advice["role"]
    did = advice["device"]["id"]
    ip = advice["device"].get("ip") or first_ip(probe) or "<ip>"
    manager_id = ctx.get("manager_id") or DEFAULT_MANAGER_ID
    manager_url = ctx.get("manager_url") or DEFAULT_MANAGER_URL
    manager_ip = ctx.get("manager_ip") or DEFAULT_MANAGER_IP
    rec_models = [m["name"] for m in advice.get("models") or [] if m.get("recommend")]
    ollama = probe.get("ollama") or {}
    lines = []
    a = lines.append
    a(f"# Onboarding checklist: `{did}` ({role})")
    a("")
    a(f"- Generated {_now()} from probe schema `{(probe.get('schema') or 'unknown')}` on `{probe.get('hostname') or '?'}`")
    a(f"- Suggested position: {advice['position'].split('. ')[0]}.")
    a(f"- Confidence: {advice.get('confidence') or pick_role(probe).get('confidence')}")
    a("")
    a("None of these steps are run for you. Root steps are paste-ready and always come with undo commands. The advisor itself never uses `sudo`.")
    a("")
    a("## 1. Copy the probe (already done if you are reading this)")
    a("")
    a("On the new machine:")
    a("")
    a(_fence("run the probe", f"""chmod +x probe.sh
./probe.sh --pretty --manager {manager_url} | tee /tmp/cluster-probe.json
# then copy the JSON to {manager_id}, e.g.
#   scp /tmp/cluster-probe.json <USER>@{manager_ip}:/tmp/{did}-probe.json
"""))
    a("")
    a(f"On {manager_id}, preview (does not register the device):")
    a("")
    a(_fence("advise only", f"bin/cluster onboard /tmp/{did}-probe.json"))
    a("")

    if role == "peripheral":
        subtype = advice.get("device_type") or "generic"
        a("## 2. Expose a read-only status URL")
        a("")
        a("Do not give the cluster write access to the device. Point `probe.url` at a local status endpoint:")
        a("")
        if subtype == "printer":
            a("- Klipper / Moonraker: `http://<printer>:7125/printer/objects/query?extruder&heater_bed&print_stats`")
            a("- OctoPrint: `http://<printer>/api/printer` with an API key in `probe.headers` (read-only key).")
        elif subtype == "camera":
            a("- Prefer a Frigate (or similar) HTTP API over exposing raw RTSP to the desktop.")
            a("- Store the stream URL in the device record for later; the Cluster tab only needs a JSON status probe today.")
        else:
            a("- MQTT / sensors: a small local bridge that serves JSON (or Home Assistant's REST API, read-only token).")
            a("- Cooling / plugs: a Shelly-style `http://<ip>/rpc/Switch.GetStatus?id=0` works with the generic probe.")
        a("")
        a("## 3. Register (no sudo)")
        a("")
        a("After you are happy with the recommendation:")
        a("")
        a(_fence("register in devices.json", f"bin/cluster onboard --apply --yes /tmp/{did}-probe.json"))
        a("")
        a("The Cluster tab picks the new card up on the next refresh. To remove it later, see § Undo.")
        a("")
        a("## Undo")
        a("")
        a(_fence("unregister", f"bin/cluster onboard --remove {did} --yes"))
        return "\n".join(lines) + "\n"

    if role == "controller":
        a("## 2. Use it as a controller, not a worker")
        a("")
        a("Do **not** install Ollama or a worker agent on this device:")
        a("")
        a("1. Join your tailnet (see `net/TAILSCALE_DESKTOP.sh.example` for Tailscale Serve setup).")
        a("2. Open Odysseus in the device browser (`https://<TAILNET_HOST>` once Tailscale Serve is on).")
        a(f"3. Optionally install a user-level signed reporter that POSTs to `{manager_id}:11620` (see `remote/README.md`). No sudo.")
        a("")
        a("Root step — only if you want Tailscale on this device and it is not installed yet (run it yourself):")
        a("")
        a(_fence(
            "install tailscale (optional, on the new device)",
            """curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
# then log in at the printed URL
""",
            undo="""sudo tailscale down
sudo systemctl disable --now tailscaled
# Debian/Ubuntu:  sudo apt remove tailscale
# Arch:           sudo pacman -Rns tailscale
""",
        ))
        a("")
        a("## 3. Register (no sudo)")
        a("")
        a(_fence("register in devices.json", f"bin/cluster onboard --apply --yes /tmp/{did}-probe.json"))
        a("")
        a("## Undo")
        a("")
        a(_fence("unregister", f"bin/cluster onboard --remove {did} --yes"))
        return "\n".join(lines) + "\n"

    a("## 2. User-level runtime (no sudo required for the agent itself)")
    a("")
    a(f"Create a workspace as the device's normal user (example uses `$LLM_CLUSTER_HOME`):")
    a("")
    a(_fence("workspace", """mkdir -p agent secret
# install the worker agent and HMAC token on the new worker
"""))
    a("")
    if not ollama.get("present"):
        a("## 3. Ollama")
        a("")
        a("Prefer a **user-level** binary if you want to avoid root. The existing cluster nodes run *system* Ollama; either works, as long as the desktop can reach `:11434` from the wired LAN.")
        a("")
        a("User-level (no sudo) — official Linux tarball into `~/.local`:")
        a("")
        a(_fence("user-level ollama", """mkdir -p ~/.local/bin ~/.local/share/ollama
# pick the tarball for this arch (amd64 or arm64):
arch=$(uname -m)
case "$arch" in x86_64) a=amd64;; aarch64|arm64) a=arm64;; *) echo "unsupported $arch"; exit 1;; esac
curl -L "https://github.com/ollama/ollama/releases/latest/download/ollama-linux-${a}.tgz" -o /tmp/ollama.tgz
tar -xzf /tmp/ollama.tgz -C ~/.local --strip-components=0
# if the tarball unpacks a single `ollama` binary:
#   install -m 0755 ollama ~/.local/bin/ollama
export PATH="$HOME/.local/bin:$PATH"
export OLLAMA_HOST=127.0.0.1:11434
ollama serve
"""))
        a("")
        a("Alternative — official system install (uses sudo; paste only if you want it):")
        a("")
        a(_fence(
            "system ollama (optional, uses sudo)",
            "curl -fsSL https://ollama.com/install.sh | sh",
            undo="""sudo systemctl disable --now ollama
sudo rm -f /usr/local/bin/ollama
sudo rm -rf /usr/lib/ollama /usr/share/ollama
sudo rm -f /etc/systemd/system/ollama.service
sudo systemctl daemon-reload
""",
        ))
        a("")
    else:
        a("## 3. Ollama is already present")
        a("")
        a(f"- version: `{ollama.get('version') or ollama.get('api_version') or 'unknown'}`")
        names = [m.get("name") for m in (ollama.get("models") or []) if m.get("name")]
        a(f"- pulled models: {', '.join('`'+n+'`' for n in names) if names else '(none)'}")
        a("")

    if rec_models:
        a("## 4. Pull only the models that fit")
        a("")
        a("On the new device, after `ollama serve` is running:")
        a("")
        pulls = "\n".join(f"ollama pull {m}" for m in rec_models)
        a(_fence("pull recommended models", pulls + "\n# confirm:\nollama list\n"))
        a("")
        a("Undo a pull: `ollama rm <name>`. Nothing else is changed.")
        a("")
    else:
        a("## 4. Models")
        a("")
        a("No cluster catalog model fits this device comfortably. Do not pull gemma4:12b or qwen3-coder-30b here.")
        a("")

    a("## 5. Let the desktop reach Ollama (may need a firewall paste)")
    a("")
    a(f"Ollama should listen on the LAN IP `{ip}:11434` *or* you can ssh-tunnel; the rest of the cluster talks to nodes over the wired LAN. If a firewall is on, allow only the manager:")
    a("")
    a(_fence(
        "optional firewall paste (on the new device, only if ufw is active)",
        f"""# only run this if `ufw status` shows the firewall is active
sudo ufw allow from {manager_ip} to any port 11434 proto tcp comment 'llm-cluster manager'
sudo ufw status
""",
        undo=f"sudo ufw delete allow from {manager_ip} to any port 11434 proto tcp",
    ))
    a("")
    a("If you are not running ufw, skip this. Do not bind Ollama to `0.0.0.0` on a Wi-Fi interface if you can help it; prefer the wired NIC.")
    a("")
    a("## 6. Cluster agent (user systemd, no linger required to test)")
    a("")
    a("Install the outbound-only worker agent with a shared HMAC token, or start with heartbeats only. Keep manager ports restricted to trusted subnets.")
    a("")
    a("If you want the user service to stay up after logout (optional root):")
    a("")
    a(_fence(
        "optional linger (on the new device)",
        "sudo loginctl enable-linger $USER",
        undo="sudo loginctl disable-linger $USER",
    ))
    a("")
    a("## 7. Register on the desktop (no sudo)")
    a("")
    a(f"This writes `devices.json` and `state/extra_flows.json` on {manager_id}, then asks the running manager to load the new flows.")
    a("")
    a(_fence("register", f"bin/cluster onboard --apply --yes /tmp/{did}-probe.json"))
    a("")
    a("Open Odysseus → Cluster and confirm the new card and the color-coded lines. Hover a line to see its job.")
    a("")
    a("## Undo")
    a("")
    a(_fence("unregister (desktop, no sudo)", f"""bin/cluster onboard --remove {did} --yes
# optional: systemctl --user restart llm-cluster-manager
"""))
    a("")
    a("On the new device, stop what you started (user-level, no sudo):")
    a("")
    a(_fence("stop local services", """systemctl --user disable --now llm-cluster-agent.service 2>/dev/null || true
# if you used a user-level ollama:
systemctl --user disable --now ollama.service 2>/dev/null || true
# models you pulled can be removed with: ollama rm <name>
"""))
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- public API
def synthesize_probe_from_device(dev: dict, ctx: dict | None = None) -> dict:
    """Build a probe-like dict from an existing devices.json entry (no live SSH)."""
    hw = dev.get("hardware") or {}
    ram = hw.get("ram_gb")
    gpu = []
    if hw.get("gpu"):
        vendor = (hw.get("gpu_vendor") or "").lower()
        gpu.append({
            "vendor": vendor,
            "model": hw.get("gpu"),
            "vram_gb": None,
            "unified_memory": vendor in ("amd", "tegra", "intel"),
            "source": "devices.json",
        })
    return {
        "schema": "llm-cluster-probe/1",
        "ts": _now(),
        "hostname": dev.get("name") or dev.get("id"),
        "arch": "aarch64" if "arm" in json.dumps(hw).lower() or "tegra" in json.dumps(hw).lower() else "x86_64",
        "os": {"name": hw.get("os"), "id": hw.get("os_vendor"), "version": "", "kernel": ""},
        "cpu": {"model": hw.get("cpu"), "vendor": hw.get("cpu_vendor"), "cores": None, "threads": None,
                "device_tree_model": hw.get("system")},
        "memory": {"ram_gb": ram},
        "disk": {"root": {"path": "/", "free_gb": None, "total_gb": None}, "mounts": []},
        "gpu": gpu,
        "network": {"hostname": dev.get("name"), "ips": [dev["ip"]] if dev.get("ip") else [], "ifaces": []},
        "ollama": {"present": bool(dev.get("ollama")), "reachable": None, "models": []},
        "manager": {"url": (ctx or {}).get("manager_url"), "reachable": None},
        "benchmark": {"skipped": True, "note": "synthesized from devices.json; run probe.sh for live numbers"},
        "hints": {
            "chassis": "handheld" if dev.get("type") == "controller" else "unknown",
            "device_class_guess": "controller" if dev.get("type") == "controller" else "unknown",
            "product": dev.get("model") or hw.get("system"),
            "battery": dev.get("type") == "controller",
            "source": "devices.json",
        },
        "synthesized": True,
        "source_device_id": dev.get("id"),
    }


def build_device_record(probe: dict, role_info: dict, device_id: str, ctx: dict) -> dict:
    mem = usable_memory(probe)
    gpu = mem.get("gpu") or {}
    cpu = probe.get("cpu") or {}
    osinfo = probe.get("os") or {}
    ip = first_ip(probe)
    dtype = role_info.get("device_type") or ROLE_DEVICE_TYPE.get(role_info["role"], "generic")
    rec = {
        "id": device_id,
        "name": device_id,
        "type": dtype,
        "role": role_info["role"],
        "job": ROLE_JOBS.get(role_info["role"], role_info["role"]),
        "ip": ip,
        "hardware": {
            "cpu": cpu.get("model"),
            "cpu_vendor": cpu.get("vendor"),
            "gpu": gpu.get("model"),
            "gpu_vendor": gpu.get("vendor"),
            "ram_gb": mem.get("ram_gb"),
            "os": osinfo.get("name"),
            "os_vendor": osinfo.get("id"),
        },
        "probe": {
            "targets": [ip] if ip else [],
            "note": f"Onboarded from probe {probe.get('ts') or ''} ({probe.get('schema')})",
        },
    }
    if dtype == "compute":
        rec["ollama"] = f"http://{ip}:11434" if ip else None
        rec["backend"] = backend_guess(probe)
    if cpu.get("device_tree_model") or (probe.get("hints") or {}).get("product"):
        rec["model"] = cpu.get("device_tree_model") or (probe.get("hints") or {}).get("product")
    # drop nulls in hardware only (keep explicit nulls off the card)
    rec["hardware"] = {k: v for k, v in rec["hardware"].items() if v not in (None, "")}
    return rec


def advise(probe: dict, role: str | None = None, device_id: str | None = None,
           ctx: dict | None = None) -> dict:
    if not isinstance(probe, dict):
        raise ValueError("probe must be a JSON object")
    ctx = ctx or load_cluster_context()
    role_info = pick_role(probe, role)
    did = device_id or suggest_id(probe, role_info["role"])
    # don't silently reuse a protected id for a *new* machine
    existing_ids = {d.get("id") for d in ctx.get("devices") or []}
    if did in existing_ids and not probe.get("source_device_id"):
        # keep the name but call out the collision; apply will refuse
        pass
    models = pick_models(probe, role_info["role"], role_info.get("secondary") or [])
    dev = build_device_record(probe, role_info, did, ctx)
    flows = suggested_flows(role_info["role"], did, ctx)
    advice = {
        "schema": SCHEMA,
        "ts": _now(),
        "role": role_info["role"],
        "secondary_roles": role_info.get("secondary") or [],
        "confidence": role_info.get("confidence"),
        "reasons": role_info.get("reasons") or [],
        "device_type": dev["type"],
        "device": dev,
        "already_registered": did in existing_ids,
        "protected": did in PROTECTED_IDS,
        "memory": usable_memory(probe),
        "models": models,
        "backend": backend_guess(probe),
        "flows": flows,
        "position": position_text(role_info["role"], did, ctx),
        "cluster": {
            "manager_id": ctx.get("manager_id"),
            "peer_id": ctx.get("peer_id"),
            "existing_devices": [d.get("id") for d in ctx.get("devices") or []],
        },
    }
    advice["checklist_md"] = build_checklist(advice, probe, ctx)
    return advice


def render_advice(advice: dict) -> str:
    d = advice["device"]
    mem = advice.get("memory") or {}
    lines = []
    a = lines.append
    a(f"# Recommendation: `{d.get('id')}` → **{advice['role']}**")
    a("")
    a(f"- Type: `{advice.get('device_type')}` · confidence: {advice.get('confidence') or 'n/a'}")
    if advice.get("secondary_roles"):
        a(f"- Secondary: {', '.join('`'+r+'`' for r in advice['secondary_roles'])}")
    if advice.get("already_registered"):
        note = "this id is already in devices.json"
        if advice.get("protected"):
            note += " (protected; `--apply` will refuse)"
        a(f"- Note: {note}")
    a(f"- Job: {d.get('job')}")
    hw = d.get("hardware") or {}
    hw_bits = [hw.get("cpu"), hw.get("gpu"), f"{hw.get('ram_gb')} GB RAM" if hw.get("ram_gb") is not None else None,
               hw.get("os")]
    a(f"- Hardware: {', '.join(str(x) for x in hw_bits if x)}")
    a(f"- Usable memory: {mem.get('kind')} · VRAM {mem.get('usable_vram_gb') or 0} GB · RAM {mem.get('ram_gb') or 0} GB")
    if d.get("ip"):
        a(f"- IP: `{d.get('ip')}`")
    a("")
    a("## Why this role")
    a("")
    for r in advice.get("reasons") or []:
        a(f"- {r}")
    a("")
    a("## Position in the chain of command")
    a("")
    a(advice.get("position") or "")
    a("")
    if advice.get("flows"):
        a("## Flows to add")
        a("")
        a("| id | from → to | type | job |")
        a("|---|---|---|---|")
        for f in advice["flows"]:
            arrow = "⇄" if f.get("direction") in ("bidirectional", "two-way", "both") else "→"
            a(f"| `{f['id']}` | {f['from']} {arrow} {f['to']} | {f['type']} | {f['label']} |")
        a("")
        a("Flows are registered as event-driven (no expected interval) so the map stays idle, not red, until the new agent actually heartbeats.")
        a("")
    elif advice["role"] == "controller":
        a("## Flows")
        a("")
        a("No extra manager flows. The Cluster tab already draws a cyan remote-control line for `type=controller`.")
        a("")
    a("## Models")
    a("")
    models = advice.get("models") or []
    if not models:
        a("No catalog models recommended (controller / peripheral, or nothing fits).")
        a("")
    else:
        a("| model | quant | ~VRAM | fits? | why |")
        a("|---|---|---|---|---|")
        for m in models:
            mark = "yes" if m.get("recommend") else "no"
            a(f"| `{m['name']}` | {m.get('quant')} | {m.get('vram_gb')} GB | {mark} — {m.get('fit_reason')} | {m.get('why')} |")
        a("")
        a("Prefer models already in the cluster catalog so the manager / orchestrator can reuse names. Quantization is the catalog default (Q4_K_M unless noted).")
        a("")
    a(advice.get("checklist_md") or "")
    return "\n".join(lines).rstrip() + "\n"


def instructions(ctx: dict | None = None) -> dict:
    ctx = ctx or load_cluster_context()
    url = ctx.get("manager_url") or DEFAULT_MANAGER_URL
    md = f"""# Add a device to the LLM cluster

The onboarding path is: **probe → advise → (you confirm) → apply**. Apply never uses sudo; it only writes `devices.json` and `state/extra_flows.json` on the manager node.

## 1. On the new machine (no root, no install)

Copy one file, `onboard/probe.sh` (bash + python3, x86_64 or aarch64 Linux):

```bash
chmod +x probe.sh
./probe.sh --pretty --manager {url} | tee /tmp/cluster-probe.json
```

The probe reads CPU, RAM, GPU / VRAM, disk, OS, Ollama (if present), and latency / bandwidth to the desktop manager. An optional short benchmark times a tiny already-pulled Ollama model, or a CPU matmul if Ollama is absent. It never pulls models and never uses sudo.

## 2. Preview the recommendation

On the manager node:

```bash
bin/cluster onboard /tmp/cluster-probe.json
```

Or paste the JSON into Odysseus → Cluster → **Add device**.

The advisor picks a role (gpu-worker, cpu-worker, reviewer, embeddings, storage/memory, monitor/sentinel, controller, peripheral), the catalog models that fit, the chain-of-command position, and a markdown checklist. Root steps in that checklist are paste-ready and come with undo commands.

## 3. Register, only after you agree

```bash
bin/cluster onboard --apply --yes /tmp/cluster-probe.json
```

`--apply` without `--yes` asks for an interactive `yes`. It refuses to overwrite protected core nodes.

Playbook for device classes (GPU box, mini PC, SBC, laptop, NAS, printers / cameras / cooling): `onboard/ONBOARDING.md`.
"""
    return {
        "title": "Add a device to the LLM cluster",
        "manager_url": url,
        "probe_command": f"./probe.sh --pretty --manager {url} | tee /tmp/cluster-probe.json",
        "markdown": md,
    }


# --------------------------------------------------------------------------- apply / remove
def _lock_and_write(path: Path, data, backup: bool = True) -> Path | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    bak = None
    if backup and path.exists():
        bak_dir = ROOT / "backups"
        bak_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        bak = bak_dir / f"{path.name}.bak-{stamp}"
        shutil.copy2(path, bak)
        os.chmod(bak, 0o600 if path.suffix == ".json" else 0o644)
    tmp = path.with_name(path.name + ".tmp")
    payload = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
    return bak


def apply_advice(advice: dict, *, confirm: bool = False, update: bool = False) -> dict:
    if not confirm:
        raise ValueError("refusing to apply without explicit confirmation (pass confirm=true / --yes)")
    dev = advice.get("device") or {}
    did = dev.get("id")
    if not did or not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,39}", str(did)):
        raise ValueError("device id must be 2-40 chars of lowercase letters, digits, and hyphens")
    if did in PROTECTED_IDS:
        raise ValueError(f"{did} is a protected existing device; --apply will not modify it")
    registry = load_devices()
    existing = next((d for d in registry["devices"] if d.get("id") == did), None)
    if existing and not update:
        raise ValueError(f"{did} is already in devices.json; pass update=true to overwrite that one entry only")
    new_devices = [d for d in registry["devices"] if d.get("id") != did]
    new_devices.append(dev)
    registry = deepcopy(registry)
    registry["devices"] = new_devices
    bak_dev = _lock_and_write(DEVICES_PATH, registry, backup=True)

    flows = advice.get("flows") or []
    extra = load_json(EXTRA_FLOWS_PATH, {"flows": []}) or {"flows": []}
    if isinstance(extra, list):
        extra = {"flows": extra}
    extra.setdefault("flows", [])
    ids = {f.get("id") for f in flows if f.get("id")}
    kept = [f for f in extra["flows"] if f.get("id") not in ids]
    extra["flows"] = kept + [f for f in flows if f.get("id")]
    extra["updated"] = _now()
    bak_fl = _lock_and_write(EXTRA_FLOWS_PATH, extra, backup=True)
    return {
        "ok": True,
        "id": did,
        "devices_path": str(DEVICES_PATH),
        "flows_path": str(EXTRA_FLOWS_PATH),
        "backup_devices": str(bak_dev) if bak_dev else None,
        "backup_flows": str(bak_fl) if bak_fl else None,
        "flows_added": [f.get("id") for f in flows],
        "note": "Manager should be told to reload extra flows (POST /api/onboard/reload, or restart the user service).",
    }


def remove_device(device_id: str, *, confirm: bool = False) -> dict:
    if not confirm:
        raise ValueError("refusing to remove without explicit confirmation")
    if device_id in PROTECTED_IDS:
        raise ValueError(f"{device_id} is protected; not removing")
    registry = load_devices()
    before = len(registry.get("devices") or [])
    registry["devices"] = [d for d in registry.get("devices") or [] if d.get("id") != device_id]
    if len(registry["devices"]) == before:
        raise ValueError(f"{device_id} is not in devices.json")
    bak_dev = _lock_and_write(DEVICES_PATH, registry, backup=True)
    extra = load_json(EXTRA_FLOWS_PATH, {"flows": []}) or {"flows": []}
    if isinstance(extra, list):
        extra = {"flows": extra}
    extra["flows"] = [f for f in extra.get("flows") or [] if device_id not in (f.get("from"), f.get("to"))
                      and not str(f.get("id") or "").endswith("-" + device_id)
                      and not str(f.get("id") or "").endswith("." + device_id)]
    extra["updated"] = _now()
    bak_fl = _lock_and_write(EXTRA_FLOWS_PATH, extra, backup=True)
    return {
        "ok": True,
        "id": device_id,
        "backup_devices": str(bak_dev) if bak_dev else None,
        "backup_flows": str(bak_fl) if bak_fl else None,
    }


def load_probe_arg(arg: str, ctx: dict | None = None) -> dict:
    """Read probe JSON from a path, stdin, or an existing device id / IP / name."""
    ctx = ctx or load_cluster_context()
    if arg in ("-", ""):
        return json.load(sys.stdin)
    path = Path(os.path.expanduser(arg))
    if path.is_file():
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("probe JSON must be an object")
        return data
    # registry lookup
    needle = arg.strip().lower()
    for d in ctx.get("devices") or []:
        if (d.get("id") or "").lower() == needle or (d.get("name") or "").lower() == needle:
            return synthesize_probe_from_device(d, ctx)
        if (d.get("ip") or "") == arg.strip():
            return synthesize_probe_from_device(d, ctx)
    raise FileNotFoundError(
        f"{arg} is not a probe JSON file, and no devices.json entry matches that id/name/IP. "
        f"Run onboard/probe.sh on the machine and pass the JSON file."
    )


if __name__ == "__main__":
    # Lightweight direct invocation: python3 advisor.py probe.json
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("probe", help="probe JSON path, existing device id, or - for stdin")
    ap.add_argument("--role")
    ap.add_argument("--id")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    ctx = load_cluster_context()
    probe = load_probe_arg(args.probe, ctx)
    advice = advise(probe, role=args.role, device_id=args.id, ctx=ctx)
    if args.json:
        print(json.dumps(advice, indent=2, ensure_ascii=False))
    else:
        sys.stdout.write(render_advice(advice))
