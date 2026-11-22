#!/usr/bin/env python3
"""clusterlib: shared helpers for the manager <-> worker LLM cluster.

Stdlib only (no pip). Deployed identically to manager/clusterlib.py and worker agent/cluster/clusterlib.py.

Contents: HMAC request signing, JSON HTTP client, Ollama chat with timing,
power samplers (NVIDIA nvidia-smi / AMD hwmon power1_input), time-vs-power
score, temperature readers, jsonl helpers.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

SCHEMA_TASK = "cluster-task/1"
MAX_SKEW_S = 90


# ---------------------------------------------------------------- basics
def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def append_jsonl(path: Path, obj: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")


def read_jsonl(path: Path, limit: int | None = None) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out[-limit:] if limit else out


def write_json_atomic(path: Path, obj) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def load_token(path: str | Path) -> str:
    p = Path(os.path.expanduser(str(path)))
    st = p.stat()
    if st.st_mode & 0o077:
        raise PermissionError(f"{p} must be mode 0600 (is {oct(st.st_mode & 0o777)})")
    tok = p.read_text(encoding="utf-8").strip()
    if len(tok) < 32:
        raise ValueError(f"{p}: token too short")
    return tok


def new_token() -> str:
    return secrets.token_hex(32)


# ---------------------------------------------------------------- HMAC
def _body_hash(body: bytes) -> str:
    return hashlib.sha256(body or b"").hexdigest()


def sign_headers(token: str, node: str, method: str, path: str, body: bytes = b"") -> dict:
    ts = str(int(time.time()))
    nonce = secrets.token_hex(8)
    msg = "\n".join([method.upper(), path, ts, nonce, node, _body_hash(body)]).encode()
    sig = hmac.new(token.encode(), msg, hashlib.sha256).hexdigest()
    return {"X-Cluster-Node": node, "X-Cluster-Ts": ts, "X-Cluster-Nonce": nonce, "X-Cluster-Sig": sig}


class NonceCache:
    def __init__(self, ttl: int = MAX_SKEW_S * 2):
        self.ttl = ttl
        self._seen: dict[str, float] = {}
        self._lock = threading.Lock()

    def check_and_add(self, nonce: str) -> bool:
        now = time.time()
        with self._lock:
            for k in [k for k, t in self._seen.items() if now - t > self.ttl]:
                self._seen.pop(k, None)
            if nonce in self._seen:
                return False
            self._seen[nonce] = now
            return True


def verify_headers(token: str, method: str, path: str, body: bytes, headers, nonces: NonceCache | None = None) -> tuple[bool, str]:
    node = headers.get("X-Cluster-Node") or ""
    ts = headers.get("X-Cluster-Ts") or ""
    nonce = headers.get("X-Cluster-Nonce") or ""
    sig = headers.get("X-Cluster-Sig") or ""
    if not (node and ts and nonce and sig):
        return False, "missing signature headers"
    try:
        skew = abs(time.time() - int(ts))
    except ValueError:
        return False, "bad timestamp"
    if skew > MAX_SKEW_S:
        return False, f"timestamp skew {skew:.0f}s"
    msg = "\n".join([method.upper(), path, ts, nonce, node, _body_hash(body)]).encode()
    want = hmac.new(token.encode(), msg, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(want, sig):
        return False, "bad signature"
    if nonces is not None and not nonces.check_and_add(nonce):
        return False, "replayed nonce"
    return True, node


# ---------------------------------------------------------------- HTTP
def http_json(url: str, payload=None, method: str | None = None, timeout: float = 60,
              token: str | None = None, node: str | None = None, headers: dict | None = None):
    """Return (status, obj, elapsed_s). Never raises on HTTP/network errors (status 0 = unreachable)."""
    data = b""
    hdrs = dict(headers or {})
    if payload is not None:
        data = json.dumps(payload).encode()
        hdrs["Content-Type"] = "application/json"
        method = method or "POST"
    method = method or "GET"
    if token:
        from urllib.parse import urlsplit
        sp = urlsplit(url)
        path = sp.path + (("?" + sp.query) if sp.query else "")
        hdrs.update(sign_headers(token, node or "unknown", method, path, data))
    req = urllib.request.Request(url, data=data if payload is not None else None, headers=hdrs, method=method)
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            el = time.monotonic() - t0
            if not raw:
                return resp.status, {}, el
            try:
                return resp.status, json.loads(raw.decode()), el
            except json.JSONDecodeError:
                return resp.status, {"raw": raw.decode("utf-8", "replace")}, el
    except urllib.error.HTTPError as e:
        el = time.monotonic() - t0
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body), el
        except json.JSONDecodeError:
            return e.code, {"error": body[:500]}, el
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return 0, {"error": f"{type(e).__name__}: {e}"}, time.monotonic() - t0


# ---------------------------------------------------------------- power
def find_hwmon(name: str) -> Path | None:
    base = Path("/sys/class/hwmon")
    if not base.exists():
        return None
    for h in sorted(base.iterdir()):
        try:
            if (h / "name").read_text().strip() == name:
                return h
        except OSError:
            continue
    return None


def read_hwmon_temp(name: str, sensor: str = "temp1_input") -> float | None:
    h = find_hwmon(name)
    if not h:
        return None
    try:
        return int((h / sensor).read_text().strip()) / 1000.0
    except (OSError, ValueError):
        return None


def read_coretemp_package() -> float | None:
    h = find_hwmon("coretemp")
    if not h:
        return None
    for lab in sorted(h.glob("temp*_label")):
        try:
            if "Package" in lab.read_text():
                return int((h / lab.name.replace("_label", "_input")).read_text()) / 1000.0
        except (OSError, ValueError):
            continue
    return read_hwmon_temp("coretemp")


class PowerSampler:
    """Integrate power while a block runs.

    kind='nvidia': GPU board power via `nvidia-smi -lms 200` (desktop; CPU not included,
                   RAPL may be root-only on some hosts).
    kind='hwmon':  AMD APU package power from hwmon `amdgpu/power1_input` (APU PPT, CPU+GPU).
    kind='none':   no measurement.
    """

    def __init__(self, kind: str = "none", interval_s: float = 0.2):
        self.kind = kind
        self.interval = interval_s
        self.samples: list[tuple[float, float]] = []
        self._stop = threading.Event()
        self._thr: threading.Thread | None = None
        self._proc: subprocess.Popen | None = None
        self.error: str | None = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *a):
        self.stop()

    def start(self):
        self.samples = []
        self._stop.clear()
        if self.kind == "nvidia":
            field = "power.draw.instant"
            try:
                chk = subprocess.run(["nvidia-smi", f"--query-gpu={field}", "--format=csv,noheader,nounits"],
                                     capture_output=True, text=True, timeout=5)
                if chk.returncode != 0 or "Not Supported" in chk.stdout or "Invalid" in chk.stdout:
                    field = "power.draw"
            except (OSError, subprocess.TimeoutExpired):
                self.error = "nvidia-smi unavailable"
                return
            self._proc = subprocess.Popen(
                ["nvidia-smi", f"--query-gpu={field}", "--format=csv,noheader,nounits", "-lms", str(int(self.interval * 1000))],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            self._thr = threading.Thread(target=self._read_nvidia, daemon=True)
            self._thr.start()
        elif self.kind == "hwmon":
            h = find_hwmon("amdgpu")
            if not h or not (h / "power1_input").exists():
                self.error = "amdgpu power1_input missing"
                return
            self._path = h / "power1_input"
            self._thr = threading.Thread(target=self._read_hwmon, daemon=True)
            self._thr.start()

    def _read_nvidia(self):
        assert self._proc and self._proc.stdout
        for line in self._proc.stdout:
            if self._stop.is_set():
                break
            try:
                self.samples.append((time.monotonic(), float(line.strip().split(",")[0])))
            except ValueError:
                continue

    def _read_hwmon(self):
        while not self._stop.is_set():
            try:
                self.samples.append((time.monotonic(), int(self._path.read_text().strip()) / 1e6))
            except (OSError, ValueError):
                pass
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        if self._thr:
            self._thr.join(timeout=3)

    def joules(self) -> float | None:
        s = self.samples
        if len(s) < 2:
            return None
        j = 0.0
        for (t0, p0), (t1, p1) in zip(s, s[1:]):
            j += (p0 + p1) / 2.0 * (t1 - t0)
        return j

    def avg_w(self) -> float | None:
        if not self.samples:
            return None
        return sum(p for _, p in self.samples) / len(self.samples)


def tv_score(tok_s: float | None, j_per_tok: float | None) -> float | None:
    """Cluster time-vs-power score: tok/s ÷ J/token (higher is better)."""
    if not tok_s or not j_per_tok:
        return None
    return tok_s / j_per_tok


# ---------------------------------------------------------------- Ollama
def ollama_chat(base: str, model: str, messages: list[dict], *, options: dict | None = None,
                keep_alive: str | int | None = None, think: bool | None = None, fmt: str | None = None,
                timeout: float = 900) -> dict:
    payload: dict = {"model": model, "messages": messages, "stream": False}
    if options:
        payload["options"] = options
    if keep_alive is not None:
        payload["keep_alive"] = keep_alive
    if think is not None:
        payload["think"] = think
    if fmt:
        payload["format"] = fmt
    st, obj, el = http_json(base.rstrip("/") + "/api/chat", payload, timeout=timeout)
    if st != 200:
        return {"ok": False, "error": obj.get("error") if isinstance(obj, dict) else str(obj), "status": st, "wall_s": el}
    msg = (obj.get("message") or {})
    ns = 1e9
    return {
        "ok": True,
        "content": msg.get("content") or "",
        "model": obj.get("model") or model,
        "eval_count": obj.get("eval_count") or 0,
        "eval_s": (obj.get("eval_duration") or 0) / ns,
        "prompt_eval_count": obj.get("prompt_eval_count") or 0,
        "prompt_eval_s": (obj.get("prompt_eval_duration") or 0) / ns,
        "load_s": (obj.get("load_duration") or 0) / ns,
        "total_s": (obj.get("total_duration") or 0) / ns,
        "wall_s": el,
    }


def run_measured(base: str, model: str, messages: list[dict], power_kind: str, **kw) -> dict:
    """Ollama chat + power integration. tok/s uses generated tokens over wall time (matches the tuner)."""
    with PowerSampler(power_kind) as ps:
        res = ollama_chat(base, model, messages, **kw)
    res["power_source"] = {"nvidia": "gpu_board(nvidia-smi)", "hwmon": "apu_ppt(amdgpu power1_input)"}.get(power_kind, "none")
    j = ps.joules()
    res["energy_j"] = round(j, 2) if j is not None else None
    res["avg_w"] = round(ps.avg_w(), 2) if ps.avg_w() is not None else None
    n = res.get("eval_count") or 0
    wall = res.get("wall_s") or 0
    res["tok_s"] = round(n / wall, 2) if n and wall else None
    res["decode_tok_s"] = round(n / res["eval_s"], 2) if n and res.get("eval_s") else None
    res["j_per_tok"] = round(j / n, 3) if (j is not None and n) else None
    sc = tv_score(res["tok_s"], res["j_per_tok"])
    res["score"] = round(sc, 3) if sc is not None else None
    return res


# ---------------------------------------------------------------- host introspection (API v1 node profile)
_JUNK = {"", "default string", "to be filled by o.e.m.", "system product name", "system manufacturer", "o.e.m.", "none"}


def _read(p: str) -> str | None:
    try:
        return Path(p).read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None


def _clean(v: str | None) -> str | None:
    return None if v is None or v.strip().lower() in _JUNK else v.strip()


def vendor_key(s: str | None) -> str | None:
    s = (s or "").lower()
    for k, keys in (("nvidia", ("nvidia", "geforce", "rtx")), ("amd", ("amd", "radeon", "ryzen", "authenticamd")),
                    ("intel", ("intel", "genuineintel")), ("apple", ("apple",))):
        if any(x in s for x in keys):
            return k
    return None


def system_profile(overrides: dict | None = None) -> dict:
    """Static hardware/OS profile; cheap, call once at startup."""
    cpu_model, vendor_id, cores = None, None, set()
    phys = core = None
    for line in (_read("/proc/cpuinfo") or "").splitlines():
        k, _, v = line.partition(":")
        k, v = k.strip(), v.strip()
        if k == "model name" and not cpu_model:
            cpu_model = v
        elif k == "vendor_id" and not vendor_id:
            vendor_id = v
        elif k == "physical id":
            phys = v
        elif k == "core id":
            core = v
            cores.add((phys, core))
    osr = {}
    for line in (_read("/etc/os-release") or "").splitlines():
        k, _, v = line.partition("=")
        osr[k] = v.strip().strip('"')
    mem = meminfo()
    prof = {
        "system": {"vendor": _clean(_read("/sys/class/dmi/id/sys_vendor")), "model": _clean(_read("/sys/class/dmi/id/product_name")),
                   "board_vendor": _clean(_read("/sys/class/dmi/id/board_vendor")), "board": _clean(_read("/sys/class/dmi/id/board_name"))},
        "cpu": {"model": cpu_model, "vendor": vendor_key(vendor_id or cpu_model), "cores": len(cores) or None, "threads": os.cpu_count()},
        "gpu": {"model": None, "vendor": None, "vram_mib": None, "kind": None, "driver": None},
        "ram_gib": round(mem.get("total_gib") or 0, 1),
        "os": {"name": osr.get("NAME") or osr.get("PRETTY_NAME"), "pretty": osr.get("PRETTY_NAME"), "kernel": os.uname().release,
               "vendor": osr.get("ID")},
    }
    nv = nvidia_stats()
    if nv and not nv.get("error"):
        prof["gpu"].update(model=nv.get("name"), vendor="nvidia", vram_mib=nv.get("vram_total_mib"), kind="discrete", driver=nv.get("driver"))
    else:
        a = amdgpu_stats()
        if a:
            prof["gpu"].update(vendor="amd", vram_mib=a.get("vram_total_mib"), kind="integrated" if (a.get("vram_total_mib") or 0) <= 8192 else "discrete",
                               driver="amdgpu")
    for k, v in (overrides or {}).items():
        if isinstance(v, dict) and isinstance(prof.get(k), dict):
            prof[k].update({kk: vv for kk, vv in v.items() if vv is not None})
        elif v is not None:
            prof[k] = v
    prof["logo"] = {"os": prof["os"].get("vendor"), "cpu": prof["cpu"].get("vendor"), "gpu": prof["gpu"].get("vendor"),
                    "system": vendor_key(prof["system"].get("vendor")) or ((prof["system"].get("vendor") or "").split() or [None])[0]}
    if prof["logo"]["system"]:
        prof["logo"]["system"] = prof["logo"]["system"].lower().strip(",.")
    return prof


def meminfo() -> dict:
    d = {}
    for line in (_read("/proc/meminfo") or "").splitlines():
        k, _, v = line.partition(":")
        try:
            d[k] = int(v.split()[0]) * 1024
        except (ValueError, IndexError):
            pass
    tot, avail = d.get("MemTotal"), d.get("MemAvailable")
    if not tot:
        return {}
    used = tot - (avail or 0)
    return {"ram_total_gib": round(tot / 2**30, 2), "ram_used_gib": round(used / 2**30, 2), "ram_pct": round(100 * used / tot, 1),
            "total_gib": tot / 2**30}


def disk_usage(paths=("/", os.path.expanduser("~"))) -> list[dict]:
    import shutil
    out, seen = [], set()
    for p in paths:
        try:
            dev = os.stat(p).st_dev
            if dev in seen:
                continue
            seen.add(dev)
            u = shutil.disk_usage(p)
            out.append({"mount": p, "used_gib": round(u.used / 2**30, 1), "total_gib": round(u.total / 2**30, 1),
                        "pct": round(100 * u.used / u.total, 1)})
        except OSError:
            pass
    return out


def uptime_s() -> float | None:
    try:
        return round(float((_read("/proc/uptime") or "").split()[0]))
    except (ValueError, IndexError):
        return None


def nvidia_stats() -> dict | None:
    try:
        p = subprocess.run(["nvidia-smi", "--query-gpu=name,temperature.gpu,power.draw,utilization.gpu,memory.used,memory.total,"
                            "clocks.gr,clocks.mem,clocks.max.gr,pstate,driver_version", "--format=csv,noheader,nounits"],
                           capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired) as e:
        return None if isinstance(e, FileNotFoundError) else {"error": str(e)}
    if p.returncode != 0:
        return {"error": p.stderr.strip()[:200]}
    f = [x.strip() for x in p.stdout.strip().splitlines()[0].split(",")]

    def num(x):
        try:
            return float(x)
        except ValueError:
            return None
    return {"name": f[0], "temp_c": num(f[1]), "power_w": num(f[2]), "util_pct": num(f[3]), "vram_used_mib": num(f[4]),
            "vram_total_mib": num(f[5]), "clock_mhz": num(f[6]), "mem_clock_mhz": num(f[7]), "clock_max_mhz": num(f[8]),
            "pstate": f[9], "driver": f[10]}


def amdgpu_stats() -> dict | None:
    for dev in sorted(Path("/sys/class/drm").glob("card[0-9]*/device")):
        if (_read(str(dev / "vendor")) or "").lower() != "0x1002":
            continue
        out = {}
        try:
            out["util_pct"] = float(_read(str(dev / "gpu_busy_percent")) or "nan")
        except ValueError:
            out["util_pct"] = None
        levels = []
        for line in (_read(str(dev / "pp_dpm_sclk")) or "").splitlines():
            try:
                mhz = float(line.split(":")[1].strip().split()[0].lower().replace("mhz", ""))
            except (IndexError, ValueError):
                continue
            levels.append(mhz)
            if "*" in line:
                out["clock_mhz"] = mhz
        out["clock_max_mhz"] = max(levels) if levels else None
        for k, f in (("vram_used_mib", "mem_info_vram_used"), ("vram_total_mib", "mem_info_vram_total")):
            try:
                out[k] = round(int(_read(str(dev / f)) or "") / 2**20)
            except ValueError:
                out[k] = None
        return out
    return None


def ipv4_by_role() -> dict:
    ips = {"wired": None, "wifi": None, "tailscale": None}
    try:
        p = subprocess.run(["ip", "-4", "-br", "addr"], capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        return ips
    for line in p.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        ifc, addr = parts[0], parts[2].split("/")[0]
        role = "wired" if ifc.startswith(("en", "eth")) else "wifi" if ifc.startswith("wl") else "tailscale" if ifc.startswith("tailscale") else None
        if role and not ips[role]:
            ips[role] = addr
    return ips


def ollama_inventory(base: str, timeout: float = 4) -> dict:
    """Normalized /api/version + /api/tags + /api/ps."""
    st, ver, lat = http_json(base + "/api/version", timeout=timeout)
    out = {"up": st == 200, "version": ver.get("version") if st == 200 else None, "latency_ms": round(lat * 1000, 1),
           "installed": None, "loaded": None}
    if st != 200:
        return out
    st, tags, _ = http_json(base + "/api/tags", timeout=timeout)
    if st == 200:
        out["installed"] = [{"name": m.get("name"), "size_bytes": m.get("size"), "size_gib": round((m.get("size") or 0) / 2**30, 2),
                             "quant": (m.get("details") or {}).get("quantization_level"),
                             "params": (m.get("details") or {}).get("parameter_size"),
                             "family": (m.get("details") or {}).get("family"), "modified_at": m.get("modified_at")}
                            for m in tags.get("models") or []]
    st, ps, _ = http_json(base + "/api/ps", timeout=timeout)
    if st == 200:
        loaded = []
        for m in ps.get("models") or []:
            size, vram = m.get("size") or 0, m.get("size_vram") or 0
            exp = m.get("expires_at")
            exp_in = None
            if exp:
                try:
                    import datetime as _dt
                    import re as _re
                    e = _dt.datetime.fromisoformat(_re.sub(r"(\.\d{6})\d+", r"\1", exp).replace("Z", "+00:00"))
                    exp_in = round(e.timestamp() - time.time())
                    exp = e.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    pass
            loaded.append({"name": m.get("name"), "size_bytes": size, "vram_bytes": vram,
                           "vram_pct": round(100 * vram / size, 1) if size else None, "context": m.get("context_length"),
                           "quant": (m.get("details") or {}).get("quantization_level"), "expires_at": exp, "expires_in_s": exp_in})
        out["loaded"] = loaded
    return out
