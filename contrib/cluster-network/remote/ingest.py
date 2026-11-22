#!/usr/bin/env python3
"""User-level ingest for signed heartbeats from non-agent remote controllers.

Separate from llm-cluster-manager so it does not touch manager.py / api.py.
Listens only on explicit local, cluster-LAN, Wi-Fi, and Tailscale addresses
(never 0.0.0.0). Auth is HMAC-SHA256 with secret/remote.token (0600).
Writes state/remotes/<id>.json under LLM_CLUSTER_HOME for the Odysseus Cluster tab.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(os.path.expanduser(os.environ.get("LLM_CLUSTER_HOME") or str(Path(__file__).resolve().parents[1])))
TOKEN_PATH = ROOT / "secret" / "remote.token"
STATE_DIR = ROOT / "state" / "remotes"
PORT = int(os.environ.get("CLUSTER_REMOTE_INGEST_PORT") or 11620)
ALLOWED_CIDRS = [
    "127.0.0.0/8",
    "192.168.0.0/16",      # LAN subnets (HMAC still required)
    "10.0.0.0/8",          # private RFC1918
    "172.16.0.0/12",       # private / Docker subnets
    "100.64.0.0/10",       # Tailscale CGNAT
]
MAX_SKEW_S = 90
STALE_S = 90

_lock = threading.Lock()
_seen: dict[str, float] = {}
_stop = threading.Event()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_token() -> str:
    st = TOKEN_PATH.stat()
    if st.st_mode & 0o077:
        raise PermissionError(f"{TOKEN_PATH} must be mode 0600")
    tok = TOKEN_PATH.read_text().strip()
    if len(tok) < 32:
        raise ValueError("token too short")
    return tok


def ip_in_cidr(ip: str, cidr: str) -> bool:
    addr, _, bits = cidr.partition("/")
    bits = int(bits or 32)
    def n(s: str) -> int:
        a, b, c, d = (int(x) for x in s.split("."))
        return (a << 24) | (b << 16) | (c << 8) | d
    try:
        mask = (0xFFFFFFFF << (32 - bits)) & 0xFFFFFFFF
        return (n(ip) & mask) == (n(addr) & mask)
    except Exception:
        return False


def allowed(ip: str) -> bool:
    return any(ip_in_cidr(ip, c) for c in ALLOWED_CIDRS)


def verify(token: str, method: str, path: str, body: bytes, headers) -> tuple[bool, str]:
    node = headers.get("X-Cluster-Node") or ""
    ts = headers.get("X-Cluster-Ts") or ""
    nonce = headers.get("X-Cluster-Nonce") or ""
    sig = headers.get("X-Cluster-Sig") or ""
    if not (node and ts and nonce and sig):
        return False, "missing signature headers"
    try:
        if abs(time.time() - int(ts)) > MAX_SKEW_S:
            return False, "timestamp skew"
    except ValueError:
        return False, "bad timestamp"
    msg = "\n".join([method.upper(), path, ts, nonce, node, hashlib.sha256(body or b"").hexdigest()]).encode()
    want = hmac.new(token.encode(), msg, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(want, sig):
        return False, "bad signature"
    now = time.time()
    with _lock:
        for k, t in list(_seen.items()):
            if now - t > MAX_SKEW_S * 2:
                _seen.pop(k, None)
        if nonce in _seen:
            return False, "replayed nonce"
        _seen[nonce] = now
    return True, node


def addrs_to_bind() -> list[str]:
    want = {"127.0.0.1"}
    try:
        out = subprocess.run(["ip", "-4", "-br", "addr"], capture_output=True, text=True, timeout=3)
        for line in out.stdout.splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            ifc, addr = parts[0], parts[2].split("/")[0]
            if ifc == "lo" or addr.startswith("127."):
                want.add("127.0.0.1")
            elif ifc.startswith(("en", "eth")) and not addr.startswith("127."):
                want.add(addr)
            elif ifc.startswith("wl"):
                want.add(addr)
            elif ifc.startswith("tailscale") or addr.startswith("100."):
                want.add(addr)
    except Exception:
        pass
    extra = os.environ.get("CLUSTER_REMOTE_INGEST_BIND")
    if extra:
        want.update(x.strip() for x in extra.split(",") if x.strip())
    return sorted(want)


def _ping_ms(host: str, timeout_s: float = 1.5) -> float | None:
    if not host:
        return None
    try:
        p = subprocess.run(["ping", "-c", "1", "-W", str(max(1, int(timeout_s))), host],
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=timeout_s + 1)
    except Exception:
        return None
    if p.returncode != 0:
        return None
    for line in p.stdout.splitlines():
        if "time=" in line:
            try:
                return round(float(line.split("time=")[1].split()[0].replace("ms", "")), 2)
            except (IndexError, ValueError):
                pass
    return None


def write_state(node: str, payload: dict) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = STATE_DIR / f"{node}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def merge_probe(node: str, targets: list[str]) -> None:
    path = STATE_DIR / f"{node}.json"
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except Exception:
            existing = {}
    checks = []
    for t in targets:
        rtt = _ping_ms(t)
        checks.append({"target": t, "reachable": rtt is not None, "rtt_ms": rtt, "method": "icmp"})
    existing["desktop_probe"] = {"ts": now_iso(), "checks": checks,
                                 "latency_ms": min((c["rtt_ms"] for c in checks if c["rtt_ms"] is not None), default=None)}
    # if heartbeat is stale and no fresh POST, still keep last heartbeat fields
    existing.setdefault("id", node)
    write_state(node, existing)


class Handler(BaseHTTPRequestHandler):
    token = ""

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[remote-ingest] {self.address_string()} {fmt % args}\n")

    def _read(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        if n > 64_000:
            return b""
        return self.rfile.read(n) if n else b""

    def _json(self, code: int, obj: dict):
        raw = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path.rstrip("/") in ("/health", "/remote/health"):
            self._json(200, {"ok": True, "service": "llm-cluster-remote-ingest", "ts": now_iso()})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?")[0]
        if path.rstrip("/") != "/remote/heartbeat":
            self._json(404, {"error": "not found"})
            return
        src = self.client_address[0]
        if not allowed(src):
            self._json(403, {"error": "source not allowed"})
            return
        body = self._read()
        ok, node = verify(self.token, "POST", path, body, self.headers)
        if not ok:
            self._json(401, {"error": node})
            return
        try:
            data = json.loads(body.decode() or "{}")
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid json"})
            return
        if not isinstance(data, dict):
            self._json(400, {"error": "expected object"})
            return
        nid = str(data.get("id") or node)
        if nid != node:
            self._json(401, {"error": "node mismatch"})
            return
        # ping back for desktop→switch latency (best-effort, short)
        targets = []
        ips = data.get("ips") or {}
        for k in ("wifi", "wired", "lan", "tailscale"):
            v = ips.get(k)
            if v:
                targets.append(v)
        for v in data.get("probe_targets") or []:
            if v and v not in targets:
                targets.append(v)
        checks = []
        for t in targets[:4]:
            rtt = _ping_ms(t, 1.2)
            checks.append({"target": t, "reachable": rtt is not None, "rtt_ms": rtt, "method": "icmp"})
        rec = {
            "id": nid,
            "ts": now_iso(),
            "received_from": src,
            "heartbeat": data,
            "desktop_probe": {
                "ts": now_iso(),
                "checks": checks,
                "latency_ms": min((c["rtt_ms"] for c in checks if c["rtt_ms"] is not None), default=None),
            },
        }
        write_state(nid, rec)
        self._json(200, {"ok": True, "id": nid, "ts": rec["ts"]})


class Server(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def probe_loop(node: str, targets: list[str], interval: float = 60):
    while not _stop.wait(interval):
        try:
            path = STATE_DIR / f"{node}.json"
            stale = True
            if path.exists():
                try:
                    rec = json.loads(path.read_text())
                    # compare heartbeat ts
                    ts = (rec.get("heartbeat") or {}).get("ts") or rec.get("ts")
                    if ts:
                        # treat as fresh if < STALE_S; skip extra ping if just written
                        from datetime import datetime, timezone
                        t = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                        stale = (time.time() - t) > STALE_S
                except Exception:
                    stale = True
            if stale:
                merge_probe(node, targets)
        except Exception as e:
            sys.stderr.write(f"[remote-ingest] probe: {e}\n")


def main() -> int:
    token = load_token()
    Handler.token = token
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    binds = addrs_to_bind()
    servers = []
    for addr in binds:
        try:
            srv = Server((addr, PORT), Handler)
        except OSError as e:
            sys.stderr.write(f"[remote-ingest] bind {addr}:{PORT} failed: {e}\n")
            continue
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        servers.append(srv)
        sys.stderr.write(f"[remote-ingest] listening on {addr}:{PORT}\n")
    if not servers:
        sys.stderr.write("[remote-ingest] no bind addresses succeeded\n")
        return 1
    # Probe configured controller devices from devices.json (optional)
    targets = []
    try:
        devs = json.loads((ROOT / "devices.json").read_text()).get("devices") or []
        for d in devs:
            if d.get("type") == "controller" and d.get("ip"):
                targets.append((d["id"], [d["ip"]]))
    except Exception:
        pass
    for rid, ips in targets:
        threading.Thread(target=probe_loop, args=(rid, ips, 60), daemon=True).start()

    def stop(*_a):
        _stop.set()
        for s in servers:
            s.shutdown()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while not _stop.wait(1):
            pass
    finally:
        stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
