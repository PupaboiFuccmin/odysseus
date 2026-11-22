#!/usr/bin/env bash
# llm-cluster hardware probe (portable, no root).
# Works on x86_64 and aarch64 Linux. Missing tools degrade to nulls, not errors.
#
# Usage:
#   ./probe.sh [--manager URL] [--benchmark|--no-benchmark] [--pretty] [--timeout SEC]
#   CLUSTER_MANAGER=http://<MANAGER_IP>:11600 ./probe.sh
#
# Copy this single file to the new machine, run it, and paste the JSON into:
#   cluster onboard probe.json
# or the Odysseus Cluster tab → Add device.
#
# It never uses sudo and never prints secrets.

set -euo pipefail

if ! command -v python3 >/dev/null 2>&1; then
  echo "probe.sh: python3 is required (not found on PATH)" >&2
  exit 1
fi

export CLUSTER_PROBE_MANAGER="${CLUSTER_MANAGER:-http://127.0.0.1:11600}"
export CLUSTER_PROBE_BENCHMARK="${CLUSTER_PROBE_BENCHMARK:-1}"
export CLUSTER_PROBE_TIMEOUT="${CLUSTER_PROBE_TIMEOUT:-8}"
export CLUSTER_PROBE_PRETTY="${CLUSTER_PROBE_PRETTY:-0}"
export CLUSTER_PROBE_BLOB_BYTES="${CLUSTER_PROBE_BLOB_BYTES:-524288}"

while [ $# -gt 0 ]; do
  case "$1" in
    --manager) CLUSTER_PROBE_MANAGER="$2"; shift 2 ;;
    --manager=*) CLUSTER_PROBE_MANAGER="${1#--manager=}"; shift ;;
    --benchmark) CLUSTER_PROBE_BENCHMARK=1; shift ;;
    --no-benchmark) CLUSTER_PROBE_BENCHMARK=0; shift ;;
    --pretty) CLUSTER_PROBE_PRETTY=1; shift ;;
    --timeout) CLUSTER_PROBE_TIMEOUT="$2"; shift 2 ;;
    --timeout=*) CLUSTER_PROBE_TIMEOUT="${1#--timeout=}"; shift ;;
    --blob-bytes) CLUSTER_PROBE_BLOB_BYTES="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "probe.sh: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

exec python3 - << 'PY'
"""Hardware inventory for the llm-cluster onboarding advisor."""
from __future__ import annotations

import json
import os
import platform
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

MANAGER = os.environ.get("CLUSTER_PROBE_MANAGER", "http://127.0.0.1:11600").rstrip("/")
DO_BENCH = os.environ.get("CLUSTER_PROBE_BENCHMARK", "1") not in ("0", "false", "no", "")
TIMEOUT = float(os.environ.get("CLUSTER_PROBE_TIMEOUT", "8") or 8)
PRETTY = os.environ.get("CLUSTER_PROBE_PRETTY", "0") not in ("0", "false", "no", "")
BLOB_BYTES = max(0, min(int(os.environ.get("CLUSTER_PROBE_BLOB_BYTES", "524288") or 0), 1048576))


def sh(argv, timeout=TIMEOUT, input_bytes=None):
    try:
        r = subprocess.run(
            argv, input=input_bytes, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=timeout, check=False,
        )
        out = r.stdout.decode("utf-8", "replace").strip()
        return r.returncode, out
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return 127, ""


def read_text(path, default=""):
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return default


def first_present(*paths):
    for p in paths:
        if p and Path(p).exists():
            return Path(p)
    return None


def iso_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def gb(n, digits=2):
    if n is None:
        return None
    try:
        return round(float(n) / (1024 ** 3), digits)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- OS / CPU / RAM / disk
def probe_os():
    osr = {}
    for line in read_text("/etc/os-release").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            osr[k] = v.strip().strip('"')
    kernel = platform.release()
    return {
        "name": osr.get("PRETTY_NAME") or osr.get("NAME") or platform.system(),
        "id": (osr.get("ID") or "").lower(),
        "version": osr.get("VERSION_ID") or osr.get("VERSION") or "",
        "kernel": kernel,
        "family": platform.system(),
    }


def cpu_vendor_from_name(name, raw=""):
    blob = f"{name} {raw}".lower()
    if "tegra" in blob or "x1" in blob and "nvidia" in blob:
        return "tegra"
    if "apple" in blob:
        return "apple"
    if "amd" in blob or "ryzen" in blob or "authenticamd" in blob:
        return "amd"
    if "intel" in blob or "genuineintel" in blob:
        return "intel"
    if "arm" in blob or "aarch64" in blob or "cortex" in blob:
        return "arm"
    return ""


def probe_cpu():
    info = read_text("/proc/cpuinfo")
    model = ""
    hardware = ""
    implementer = ""
    for line in info.splitlines():
        if ":" not in line:
            continue
        k, v = [x.strip() for x in line.split(":", 1)]
        lk = k.lower()
        if lk in ("model name", "cpu part"):
            model = model or v
        elif lk == "hardware":
            hardware = v
        elif lk == "cpu implementer":
            implementer = v
        elif lk == "processor" and not model and "aarch64" in platform.machine():
            pass
    if not model:
        _, out = sh(["lscpu"])
        m = re.search(r"Model name:\s*(.+)", out)
        if m:
            model = m.group(1).strip()
    dt_model = read_text("/proc/device-tree/model").replace("\x00", "").strip()
    if not model:
        model = dt_model or hardware or platform.processor() or platform.machine()
    threads = os.cpu_count() or 0
    cores = threads
    _, lscpu = sh(["lscpu", "-p=CORE"])
    core_ids = {ln.strip() for ln in lscpu.splitlines() if ln and not ln.startswith("#")}
    if core_ids:
        try:
            cores = len({int(c.split(",")[0]) for c in core_ids})
        except ValueError:
            cores = len(core_ids)
    vendor = cpu_vendor_from_name(model, info + " " + dt_model + " " + hardware)
    if not vendor and implementer.lower() == "0x41":
        vendor = "arm"
    if not vendor and "nvidia" in (dt_model + hardware).lower():
        vendor = "tegra"
    return {
        "model": model,
        "vendor": vendor,
        "cores": cores,
        "threads": threads,
        "hardware": hardware or None,
        "device_tree_model": dt_model or None,
    }


def probe_memory():
    mem, swap = None, None
    for line in read_text("/proc/meminfo").splitlines():
        if line.startswith("MemTotal:"):
            mem = int(line.split()[1]) * 1024
        elif line.startswith("SwapTotal:"):
            swap = int(line.split()[1]) * 1024
    return {"ram_bytes": mem, "ram_gb": gb(mem, 1), "swap_bytes": swap, "swap_gb": gb(swap, 1)}


def probe_disk():
    rows = []
    for path in ("/", os.path.expanduser("~")):
        try:
            u = shutil.disk_usage(path)
        except OSError:
            continue
        rows.append({
            "path": path,
            "total_bytes": u.total,
            "free_bytes": u.free,
            "total_gb": gb(u.total, 1),
            "free_gb": gb(u.free, 1),
        })
    # unique by path
    seen = set()
    out = []
    for r in rows:
        if r["path"] in seen:
            continue
        seen.add(r["path"])
        out.append(r)
    root = out[0] if out else {"path": "/", "free_gb": None, "total_gb": None}
    return {"root": root, "mounts": out}


# --------------------------------------------------------------------------- GPU
PCI_VENDORS = {
    "10de": "nvidia",
    "1002": "amd",
    "1022": "amd",
    "8086": "intel",
    "13b5": "arm",  # ARM Mali
    "1ae0": "google",
}


def pci_ids_name(vendor, device):
    """Best-effort lookup in pci.ids without extra packages."""
    text = read_text("/usr/share/misc/pci.ids") or read_text("/usr/share/hwdata/pci.ids")
    if not text:
        return None
    vendor = vendor.lower()
    device = device.lower()
    in_vendor = False
    vname = ""
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if not line.startswith("\t") and not line.startswith("C "):
            in_vendor = line.lower().startswith(vendor + " ")
            if in_vendor:
                vname = line[4:].strip() if len(line) > 4 else ""
        elif in_vendor and line.startswith("\t") and not line.startswith("\t\t"):
            if line.strip().lower().startswith(device + " "):
                dname = line.strip()[4:].strip() if len(line.strip()) > 4 else line.strip()
                return f"{vname} {dname}".strip()
    return None


def parse_lspci():
    gpus = []
    code, out = sh(["lspci", "-nn"])
    if code != 0 or not out:
        code, out = sh(["lspci"])
    if not out:
        return gpus
    rx = re.compile(
        r"^(?P<slot>\S+)\s+(?P<cls>VGA compatible controller|3D controller|Display controller|Processing accelerators)\s*:\s*(?P<name>.+?)(?:\s*\[(?P<ven>[0-9a-fA-F]{4}):(?P<dev>[0-9a-fA-F]{4})\])?\s*$"
    )
    for line in out.splitlines():
        m = rx.match(line)
        if not m:
            # looser fallback
            if re.search(r"VGA|3D controller|Display controller", line, re.I):
                name = line.split(":", 1)[-1].strip() if ":" in line else line
                v = cpu_vendor_from_name(name)
                if "nvidia" in name.lower():
                    v = "nvidia"
                gpus.append({"vendor": v, "model": name, "vram_gb": None, "source": "lspci"})
            continue
        name = m.group("name").strip()
        ven, dev = (m.group("ven") or "").lower(), (m.group("dev") or "").lower()
        vendor = PCI_VENDORS.get(ven) or cpu_vendor_from_name(name)
        if "nvidia" in name.lower() or vendor == "nvidia":
            if "tegra" in name.lower() or "ga10b" in name.lower() or "x1" in name.lower():
                vendor = "tegra"
            else:
                vendor = "nvidia"
        pretty = pci_ids_name(ven, dev) if ven and dev else None
        gpus.append({
            "vendor": vendor,
            "model": pretty or name,
            "vram_gb": None,
            "pci": m.group("slot"),
            "pci_id": f"{ven}:{dev}" if ven and dev else None,
            "source": "lspci",
        })
    return gpus


def probe_nvidia_smi():
    code, out = sh([
        "nvidia-smi",
        "--query-gpu=name,memory.total,driver_version",
        "--format=csv,noheader,nounits",
    ])
    if code != 0 or not out:
        return []
    rows = []
    for line in out.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if not parts or not parts[0]:
            continue
        vram = None
        if len(parts) > 1:
            try:
                vram = round(float(parts[1]) / 1024.0, 2)  # MiB -> GiB
            except ValueError:
                try:
                    vram = round(float(parts[1]), 2)
                except ValueError:
                    vram = None
        # If the number is already in the 8-48 range it was probably already GiB;
        # nvidia-smi nounits for memory.total is MiB. Values like 12282 are MiB.
        if vram is not None and vram > 256:  # forgot to divide? unlikely with /1024
            vram = round(vram / 1024.0, 2)
        rows.append({
            "vendor": "nvidia",
            "model": parts[0],
            "vram_gb": vram,
            "driver": parts[2] if len(parts) > 2 else None,
            "source": "nvidia-smi",
        })
    return rows


def probe_rocm():
    rows = []
    for cmd in (
        ["rocm-smi", "--showproductname", "--showmeminfo", "vram", "--csv"],
        ["rocm-smi", "--showproductname", "--showmeminfo", "vram"],
    ):
        code, out = sh(cmd)
        if code != 0 or not out:
            continue
        name = None
        vram = None
        for line in out.splitlines():
            if re.search(r"Card series|Card model|GPU\[.*\]\s+:\s+", line, re.I):
                name = line.split(":")[-1].strip() or name
            m = re.search(r"(?:vram|Total Memory).{0,20}?(\d+)\s*(MiB|GiB|MB|GB)?", line, re.I)
            if m:
                n = float(m.group(1))
                unit = (m.group(2) or "MiB").lower()
                vram = round(n / 1024.0, 2) if unit.startswith("m") else n
        if name or vram:
            rows.append({"vendor": "amd", "model": name or "AMD GPU", "vram_gb": vram, "source": "rocm-smi"})
            break
    return rows


def probe_sysfs_drm():
    rows = []
    drm = Path("/sys/class/drm")
    if not drm.is_dir():
        return rows
    seen = set()
    for card in sorted(drm.glob("card[0-9]*")):
        if "-" in card.name:  # card0-DP-1 etc
            continue
        dev = card / "device"
        if not dev.exists():
            continue
        try:
            key = str(dev.resolve())
        except OSError:
            key = str(dev)
        if key in seen:
            continue
        seen.add(key)
        ven = read_text(dev / "vendor").strip()
        device = read_text(dev / "device").strip()
        ven_h = ven.lower().replace("0x", "")
        dev_h = device.lower().replace("0x", "")
        vendor = PCI_VENDORS.get(ven_h, "")
        vram_b = None
        for p in (dev / "mem_info_vram_total", dev / "mem_info_vis_vram_total"):
            raw = read_text(p).strip()
            if raw.isdigit():
                vram_b = int(raw)
                break
        # Some drivers expose VRAM as a file named 'available_vram' etc.
        name = pci_ids_name(ven_h, dev_h) if ven_h and dev_h else None
        if not name:
            name = read_text(card / "device" / "label").strip() or f"DRM {card.name}"
        rows.append({
            "vendor": vendor,
            "model": name,
            "vram_gb": gb(vram_b, 2) if vram_b else None,
            "pci_id": f"{ven_h}:{dev_h}" if ven_h and dev_h else None,
            "source": "sysfs",
        })
    return rows


def merge_gpus(*groups):
    """Prefer richer sources (nvidia-smi / rocm) and fill gaps from lspci/sysfs."""
    out = []
    for g in groups:
        for row in g:
            model = (row.get("model") or "").lower()
            vendor = (row.get("vendor") or "").lower()
            # skip obvious non-GPU "simple" entries later
            matched = None
            for existing in out:
                ev, em = (existing.get("vendor") or "").lower(), (existing.get("model") or "").lower()
                same_vendor = ev and vendor and ev == vendor
                overlap = same_vendor and (
                    ev == vendor and (em in model or model in em or not em or not model)
                )
                if overlap:
                    matched = existing
                    break
            if matched:
                if row.get("vram_gb") and not matched.get("vram_gb"):
                    matched["vram_gb"] = row["vram_gb"]
                if row.get("model") and (not matched.get("model") or len(row["model"]) > len(matched.get("model") or "")):
                    if row.get("source") in ("nvidia-smi", "rocm-smi") or not matched.get("model"):
                        matched["model"] = row["model"]
                srcs = {matched.get("source"), row.get("source")}
                matched["source"] = "+".join(s for s in ("nvidia-smi", "rocm-smi", "lspci", "sysfs") if s in srcs)
                if row.get("driver"):
                    matched["driver"] = row["driver"]
            else:
                out.append(dict(row))
    # Drop intel/amd "audio" leftovers if any slipped through; keep at least one if empty later
    return out


def probe_gpu():
    gpus = merge_gpus(probe_nvidia_smi(), probe_rocm(), parse_lspci(), probe_sysfs_drm())
    dt = read_text("/proc/device-tree/model").replace("\x00", "").strip().lower()
    if not gpus and ("tegra" in dt or "nintendo switch" in dt or "jetson" in dt):
        gpus.append({
            "vendor": "tegra",
            "model": read_text("/proc/device-tree/model").replace("\x00", "").strip() or "NVIDIA Tegra",
            "vram_gb": None,  # unified memory
            "unified_memory": True,
            "source": "device-tree",
        })
    # Tegra / APU: mark unified memory when VRAM is missing or is only a tiny BAR.
    for g in gpus:
        model = (g.get("model") or "").lower()
        vram = g.get("vram_gb")
        igpu = g.get("vendor") in ("tegra",) or any(
            s in model for s in ("radeon", "igpu", "uhd", "iris", "rembrandt", "680m", "780m", "tegra", "mali")
        )
        if igpu and (vram is None or (isinstance(vram, (int, float)) and vram < 2)):
            g["unified_memory"] = True
            if isinstance(vram, (int, float)) and vram < 2:
                g["bar_vram_gb"] = vram
                g["vram_gb"] = None
    return gpus


# --------------------------------------------------------------------------- network / ollama / manager
def probe_network():
    hostname = socket.gethostname()
    ips = []
    _, ip_out = sh(["ip", "-4", "-o", "addr", "show"])
    ifaces = []
    if ip_out:
        for line in ip_out.splitlines():
            # 2: enp5s0    inet 192.0.2.10/24 ...
            m = re.search(r"^\d+:\s+(\S+)\s+inet\s+(\d+\.\d+\.\d+\.\d+)/(\d+)", line)
            if not m:
                continue
            iface, addr, plen = m.group(1), m.group(2), int(m.group(3))
            if addr.startswith("127."):
                continue
            ifaces.append({"iface": iface, "ip": addr, "prefix": plen})
            ips.append(addr)
    if not ips:
        _, hostn = sh(["hostname", "-I"])
        for addr in hostn.split():
            if re.match(r"\d+\.\d+\.\d+\.\d+$", addr) and not addr.startswith("127."):
                ips.append(addr)
    # default route
    _, route = sh(["ip", "route", "show", "default"])
    default_via = None
    m = re.search(r"default via (\d+\.\d+\.\d+\.\d+)", route)
    if m:
        default_via = m.group(1)
    return {"hostname": hostname, "ips": ips, "ifaces": ifaces, "default_via": default_via}


def probe_ollama():
    path = shutil.which("ollama")
    version = None
    if path:
        _, out = sh(["ollama", "--version"])
        version = out.splitlines()[-1].strip() if out else None
    url = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
    if not url.startswith("http"):
        url = "http://" + url
    reachable = False
    api_version = None
    models = []
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/api/version", timeout=min(TIMEOUT, 3)) as r:
            api_version = json.loads(r.read().decode() or "{}")
            reachable = True
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        pass
    if reachable:
        try:
            with urllib.request.urlopen(url.rstrip("/") + "/api/tags", timeout=min(TIMEOUT, 4)) as r:
                data = json.loads(r.read().decode() or "{}")
                for m in data.get("models") or []:
                    models.append({
                        "name": m.get("name") or m.get("model"),
                        "size_gb": gb(m.get("size"), 2),
                        "param_size": (m.get("details") or {}).get("parameter_size"),
                        "quant": (m.get("details") or {}).get("quantization_level"),
                    })
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
            pass
    return {
        "present": bool(path) or reachable,
        "path": path,
        "version": version,
        "url": url,
        "reachable": reachable,
        "api_version": (api_version or {}).get("version") if isinstance(api_version, dict) else None,
        "models": models,
    }


def _manager_host_port(url):
    from urllib.parse import urlparse
    u = urlparse(url if "://" in url else "http://" + url)
    host = u.hostname or "127.0.0.1"
    port = u.port or (443 if u.scheme == "https" else 80)
    # default cluster manager port if the URL has no path-implied port and netloc has none
    if u.port is None and u.scheme in ("http", "") and (u.path in ("", "/") or not u.path):
        # CLUSTER_MANAGER often includes :11600; if not, assume 11600
        if u.netloc and ":" not in u.netloc:
            port = 11600
    return host, port, u.scheme or "http"


def tcp_connect_ms(host, port, timeout=TIMEOUT):
    t0 = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return round((time.perf_counter() - t0) * 1000, 2), None
    except OSError as e:
        return None, str(e)


def icmp_ms(host, timeout=TIMEOUT):
    # ping is usually usable without root for a single echo
    for args in (
        ["ping", "-c", "3", "-W", str(max(1, int(timeout))), host],
        ["ping", "-c", "3", host],
    ):
        code, out = sh(args, timeout=timeout + 2)
        if not out:
            continue
        m = re.search(r"rtt min/avg/max(?:/[^\s]+)?\s*=\s*[\d.]+/([\d.]+)/", out)
        if not m:
            m = re.search(r"min/avg/max[^=]*=\s*[\d.]+/([\d.]+)/", out)
        if m:
            return round(float(m.group(1)), 2), None
        if code != 0:
            err = "ping failed"
            if "Operation not permitted" in out or "not permitted" in out:
                err = "ping not permitted"
            return None, err
    return None, "ping unavailable"


def http_ping(url, timeout=TIMEOUT, blob=0):
    target = url.rstrip("/") + "/onboard/ping"
    if blob:
        target += f"?bytes={int(blob)}"
    t0 = time.perf_counter()
    try:
        req = urllib.request.Request(target, headers={"User-Agent": "llm-cluster-probe/1", "Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
            ms = round((time.perf_counter() - t0) * 1000, 2)
            ctype = r.headers.get("Content-Type", "")
            return {"ok": True, "http_ms": ms, "status": r.status, "bytes": len(data), "content_type": ctype, "error": None}
    except urllib.error.HTTPError as e:
        ms = round((time.perf_counter() - t0) * 1000, 2)
        try:
            body = e.read()
        except OSError:
            body = b""
        return {"ok": False, "http_ms": ms, "status": e.code, "bytes": len(body), "error": f"HTTP {e.code}"}
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return {"ok": False, "http_ms": None, "status": None, "bytes": 0, "error": str(e)}


def probe_manager():
    host, port, scheme = _manager_host_port(MANAGER)
    url = f"{scheme}://{host}:{port}"
    icmp, icmp_err = icmp_ms(host)
    tcp, tcp_err = tcp_connect_ms(host, port)
    ping = http_ping(url, blob=0)
    bandwidth_mbps = None
    blob = None
    if BLOB_BYTES and (ping.get("ok") or ping.get("status") in (200, 403, 401, 404)):
        # Only try the blob if the manager at least accepted a TCP connection
        blob = http_ping(url, timeout=max(TIMEOUT, 15), blob=BLOB_BYTES)
        if blob.get("ok") and blob.get("bytes") and blob.get("http_ms"):
            # bits / seconds / 1e6
            seconds = blob["http_ms"] / 1000.0
            if seconds > 0:
                bandwidth_mbps = round((blob["bytes"] * 8) / seconds / 1_000_000, 2)
    reachable = bool(ping.get("ok") or (tcp is not None) or (icmp is not None))
    return {
        "url": url,
        "host": host,
        "port": port,
        "reachable": reachable,
        "icmp_ms": icmp,
        "tcp_connect_ms": tcp,
        "http_ms": ping.get("http_ms"),
        "http_status": ping.get("status"),
        "bandwidth_mbps": bandwidth_mbps,
        "bandwidth_bytes": (blob or {}).get("bytes") if blob else None,
        "error": None if reachable else (ping.get("error") or tcp_err or icmp_err),
        "notes": [x for x in (
            None if ping.get("ok") else (f"onboard/ping: {ping.get('error')}" if ping.get("error") else None),
            f"icmp: {icmp_err}" if icmp_err and icmp is None else None,
        ) if x],
    }


# --------------------------------------------------------------------------- hints + benchmark
CHASSIS = {
    "1": "other", "2": "unknown", "3": "desktop", "4": "desktop",  # low profile
    "5": "desktop",  # pizza box
    "6": "desktop",  # mini tower? actually 6=mini tower, 7=tower, 8=portable, 9=laptop
    "7": "desktop", "8": "laptop", "9": "laptop", "10": "laptop", "11": "laptop",
    "13": "all-in-one", "14": "laptop", "30": "tablet", "31": "convertible",
    "32": "convertible", "33": "server",  # blade? keep loose
    "34": "sbc",  # embedded?
}


def probe_hints(cpu, mem, gpu, net, osinfo):
    chassis_id = read_text("/sys/class/dmi/id/chassis_type").strip()
    chassis = CHASSIS.get(chassis_id, "")
    product = (
        read_text("/sys/class/dmi/id/product_name").strip()
        or (cpu.get("device_tree_model") or "")
    )
    battery = Path("/sys/class/power_supply").exists() and any(
        (p / "type").exists() and "bat" in read_text(p / "type").lower()
        for p in Path("/sys/class/power_supply").glob("*")
    ) if Path("/sys/class/power_supply").is_dir() else False
    dt = (cpu.get("device_tree_model") or "").lower()
    arch = platform.machine()
    ram = mem.get("ram_gb") or 0
    has_discrete = any(
        (g.get("vendor") in ("nvidia", "amd") and not g.get("unified_memory") and (g.get("vram_gb") or 0) >= 4)
        for g in gpu
    )
    kind = "unknown"
    if "nintendo switch" in dt or "tegra" in dt and ram <= 8:
        kind = "controller" if "switch" in dt else "sbc"
        chassis = chassis or ("handheld" if "switch" in dt else "sbc")
    elif re.search(r"raspberry pi|rockchip|orangepi|odroid|jetson|radxa|banana pi", dt + " " + product.lower()):
        kind = "sbc"
        chassis = chassis or "sbc"
    elif battery and chassis in ("laptop", "convertible", "portable", ""):
        kind = "laptop"
        chassis = chassis or "laptop"
    elif has_discrete and (chassis in ("desktop", "tower", "") or ram >= 32):
        kind = "gpu-box"
        chassis = chassis or "desktop"
    elif "mini" in product.lower() or chassis_id in ("6", "35", "36"):  # mini tower / mini PC types
        kind = "mini-pc"
        chassis = chassis or "mini-pc"
    elif chassis == "desktop" and any("radeon" in (g.get("model") or "").lower() or g.get("unified_memory") for g in gpu):
        kind = "mini-pc"
    elif chassis == "desktop":
        kind = "gpu-box" if has_discrete else "mini-pc"
    elif arch.startswith("aarch") and ram <= 8:
        kind = "sbc"
    elif ram >= 16 and not gpu:
        kind = "nas" if (probe_disk()["root"].get("total_gb") or 0) >= 2000 else "cpu-box"
    return {
        "chassis": chassis or "unknown",
        "chassis_id": chassis_id or None,
        "product": product or None,
        "battery": bool(battery),
        "device_class_guess": kind,
        "arch": arch,
    }


def bench_ollama(ollama):
    """Time a short generate on the smallest already-pulled model. Never pulls."""
    if not ollama.get("reachable"):
        return None
    models = ollama.get("models") or []
    if not models:
        return {"kind": "ollama", "skipped": True, "note": "ollama is up but no models are pulled; not pulling anything"}
    # prefer tiny / known-light models
    def score(m):
        name = (m.get("name") or "").lower()
        size = m.get("size_gb") or 99
        pref = 0
        if "nomic-embed" in name or "embed" in name:
            pref = -20
        if any(t in name for t in (":0.5b", "0.5b", ":1b", "1b", ":1.5b", ":3b", "tiny", "mini")):
            pref = -10
        return (pref, size)
    models_sorted = sorted(models, key=score)
    model = models_sorted[0]["name"]
    payload = json.dumps({
        "model": model,
        "prompt": "Reply with the single word pong.",
        "stream": False,
        "options": {"num_predict": 16, "temperature": 0},
        "keep_alive": "0s",
    }).encode()
    url = ollama["url"].rstrip("/") + "/api/generate"
    t0 = time.perf_counter()
    try:
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=max(TIMEOUT, 45)) as r:
            data = json.loads(r.read().decode() or "{}")
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as e:
        return {"kind": "ollama", "model": model, "ok": False, "error": str(e),
                "seconds": round(time.perf_counter() - t0, 2)}
    seconds = round(time.perf_counter() - t0, 2)
    eval_count = data.get("eval_count")
    eval_ns = data.get("eval_duration")
    tok_s = None
    if eval_count and eval_ns:
        tok_s = round(eval_count / (eval_ns / 1e9), 2)
    elif eval_count and seconds:
        tok_s = round(eval_count / seconds, 2)
    return {
        "kind": "ollama",
        "model": model,
        "ok": True,
        "tok_s": tok_s,
        "eval_count": eval_count,
        "seconds": seconds,
        "note": "short generate on an already-pulled model (nothing was pulled)",
    }


def bench_cpu():
    """A short dense matmul so machines without Ollama still get a number."""
    # 512x512 float64 matmul repeated; report MFlop/s-ish and wall time.
    n = 384
    try:
        # Prefer numpy if present, else pure Python (slower, smaller N).
        import array
        try:
            import numpy as np  # type: ignore
            a = np.random.rand(n, n)
            b = np.random.rand(n, n)
            # warmup
            _ = a @ b
            t0 = time.perf_counter()
            reps = 8
            for _ in range(reps):
                c = a @ b  # noqa: F841
            seconds = time.perf_counter() - t0
            flop = reps * (2 * (n ** 3))
            gflops = round((flop / seconds) / 1e9, 2)
            return {"kind": "cpu-matmul", "ok": True, "impl": "numpy", "n": n, "reps": reps,
                    "seconds": round(seconds, 3), "gflops": gflops,
                    "note": "numpy float64 matmul; not comparable to tok/s"}
        except Exception:
            n = 96
            a = [array.array("d", [((i * 31 + j) % 100) / 100.0 for j in range(n)]) for i in range(n)]
            b = [array.array("d", [((i * 17 + j) % 100) / 100.0 for j in range(n)]) for i in range(n)]
            t0 = time.perf_counter()
            reps = 2
            for _ in range(reps):
                c = [[sum(a[i][k] * b[k][j] for k in range(n)) for j in range(n)] for i in range(n)]
            seconds = time.perf_counter() - t0
            flop = reps * (2 * (n ** 3))
            gflops = round((flop / seconds) / 1e9, 3)
            return {"kind": "cpu-matmul", "ok": True, "impl": "python", "n": n, "reps": reps,
                    "seconds": round(seconds, 3), "gflops": gflops,
                    "note": "pure-python matmul fallback (no numpy); treat as a coarse CPU signal only"}
    except Exception as e:
        return {"kind": "cpu-matmul", "ok": False, "error": str(e)}


def probe_benchmark(ollama):
    if not DO_BENCH:
        return {"skipped": True, "note": "passed --no-benchmark"}
    if ollama.get("reachable"):
        r = bench_ollama(ollama)
        if r:
            return r
    return bench_cpu()


def main():
    osinfo = probe_os()
    cpu = probe_cpu()
    mem = probe_memory()
    disk = probe_disk()
    gpu = probe_gpu()
    net = probe_network()
    ollama = probe_ollama()
    manager = probe_manager()
    hints = probe_hints(cpu, mem, gpu, net, osinfo)
    bench = probe_benchmark(ollama)
    doc = {
        "schema": "llm-cluster-probe/1",
        "ts": iso_now(),
        "hostname": net.get("hostname") or socket.gethostname(),
        "arch": platform.machine(),
        "os": osinfo,
        "cpu": cpu,
        "memory": mem,
        "disk": disk,
        "gpu": gpu,
        "network": net,
        "ollama": ollama,
        "manager": manager,
        "benchmark": bench,
        "hints": hints,
    }
    if PRETTY:
        json.dump(doc, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
    else:
        json.dump(doc, sys.stdout, separators=(",", ":"), ensure_ascii=False)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
PY
