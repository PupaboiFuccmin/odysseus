"""UI/JSON API v1 for the LLM cluster manager.

Loaded by manager.py: `api.setup(globals())`, then the HTTP handler forwards /api* to api.handle().
Owns: node view, activity log (+SSE), metrics store (sqlite, 7 d raw / 400 d 15-min rollups), change log, flows.
"""
from __future__ import annotations

import hmac
import ipaddress
import json
import os
import re
import secrets
import sys
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from clusterlib import (append_jsonl, disk_usage, ipv4_by_role, meminfo, now_iso, nvidia_stats, ollama_inventory,
                        read_coretemp_package, read_jsonl, system_profile, uptime_s)

API_VERSION = 1
G: dict = {}            # manager globals (CFG, S, TASKS_JSONL, ...)
START = time.time()
NODE_ALIASES = {"desktop": None, "mini": None}


# ------------------------------------------------------------------ small utils
def iso(ts: float | None) -> str | None:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if ts else None


def parse_iso(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def parse_t(v: str | None, default: float) -> float:
    if v in (None, "", "now"):
        return default
    m = re.fullmatch(r"-(\d+(?:\.\d+)?)([smhdw])", v.strip())
    if m:
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[m.group(2)]
        return time.time() - float(m.group(1)) * mult
    try:
        return float(v)
    except ValueError:
        t = parse_iso(v)
        if t is None:
            raise ValueError(f"bad time {v!r}")
        return t


def avg(xs):
    xs = [x for x in xs if isinstance(x, (int, float))]
    return round(sum(xs) / len(xs), 3) if xs else None


def node_id(raw: str) -> str | None:
    me, peer = G["CFG"]["node"], G["CFG"]["peer"]["name"]
    return {"desktop": me, "mini": peer, me: me, peer: peer, "all": "all"}.get(raw)


# ------------------------------------------------------------------ activity log
class Activity:
    def __init__(self, path: Path):
        self.path = path
        self.buf: deque = deque(maxlen=5000)
        self.cv = threading.Condition()
        self.seq = 0
        for e in read_jsonl(path, 5000):
            self.buf.append(e)
            self.seq = max(self.seq, int(e.get("seq") or 0))

    def emit(self, node, kind, msg, level="info", task_id=None, data=None):
        with self.cv:
            self.seq += 1
            e = {"seq": self.seq, "ts": now_iso(), "node": node, "level": level, "kind": kind, "task_id": task_id,
                 "msg": msg, "data": data}
            self.buf.append(e)
            self.cv.notify_all()
        try:
            append_jsonl(self.path, e)
        except OSError:
            pass
        return e

    def since(self, since: int, node: str = "all", n: int = 200) -> list[dict]:
        with self.cv:
            ev = [e for e in self.buf if e["seq"] > since and (node == "all" or e["node"] == node)]
        return ev[:n]

    def tail(self, node: str, n: int) -> list[dict]:
        with self.cv:
            ev = [e for e in self.buf if node == "all" or e["node"] == node]
        return ev[-n:] if n > 0 else []


# ------------------------------------------------------------------ metrics store
HOST_METRICS = {"power_w": "W", "cpu_temp_c": "°C", "gpu_temp_c": "°C", "gpu_util_pct": "%", "gpu_clock_mhz": "MHz",
                "ram_pct": "%", "vram_used_mib": "MiB", "queue_depth": "tasks", "ollama_latency_ms": "ms", "peer_latency_ms": "ms"}
TASK_METRICS = {"tok_s": "tok/s", "decode_tok_s": "tok/s", "j_per_tok": "J/token", "score": "tok/s per J/token",
                "energy_j": "J", "pass": "0/1"}
RAW_KEEP_S, ROLL_KEEP_S, ROLL_S = 7 * 86400, 400 * 86400, 900


class MetricsDB:
    def __init__(self, path: Path):
        self.lock = threading.Lock()
        self.db = sqlite3.connect(str(path), check_same_thread=False, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS raw(node TEXT, ts INTEGER, metric TEXT, model TEXT DEFAULT '', value REAL);
            CREATE INDEX IF NOT EXISTS raw_i ON raw(node, metric, ts);
            CREATE TABLE IF NOT EXISTS roll(node TEXT, bucket INTEGER, metric TEXT, model TEXT DEFAULT '',
                                            avg REAL, mn REAL, mx REAL, n INTEGER, PRIMARY KEY(node, bucket, metric, model));
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);""")
        self.db.commit()

    def add(self, node: str, values: dict, ts: float | None = None, model: str | None = None) -> None:
        ts = int(ts or time.time())
        rows = [(node, ts, k, model or "", float(v)) for k, v in values.items() if isinstance(v, (int, float))]
        if rows:
            with self.lock:
                self.db.executemany("INSERT INTO raw VALUES (?,?,?,?,?)", rows)
                self.db.commit()

    def count_task_points(self) -> int:
        with self.lock:
            return self.db.execute("SELECT count(*) FROM raw WHERE metric='tok_s'").fetchone()[0] + \
                self.db.execute("SELECT count(*) FROM roll WHERE metric='tok_s'").fetchone()[0]

    def maintain(self) -> None:
        now = int(time.time())
        with self.lock:
            r = self.db.execute("SELECT v FROM meta WHERE k='rolled_until'").fetchone()
            start = int(r[0]) if r else (self.db.execute("SELECT min(ts) FROM raw").fetchone()[0] or now)
            start -= start % ROLL_S
            end = (now - ROLL_S) - (now - ROLL_S) % ROLL_S
            if end > start:
                self.db.execute("""INSERT OR REPLACE INTO roll
                    SELECT node, (ts/?)*?, metric, model, avg(value), min(value), max(value), count(*) FROM raw
                    WHERE ts >= ? AND ts < ? GROUP BY node, (ts/?)*?, metric, model""", (ROLL_S, ROLL_S, start, end, ROLL_S, ROLL_S))
                self.db.execute("INSERT OR REPLACE INTO meta VALUES ('rolled_until', ?)", (str(end),))
            self.db.execute("DELETE FROM raw WHERE ts < ?", (now - RAW_KEEP_S,))
            self.db.execute("DELETE FROM roll WHERE bucket < ?", (now - ROLL_KEEP_S,))
            self.db.commit()

    def query(self, node: str, frm: float, to: float, step: int | None, metrics: list[str], model: str | None) -> dict:
        span = max(60, to - frm)
        use_raw = frm >= time.time() - RAW_KEEP_S + 60
        step = int(step or max(60, span / 500))
        if not use_raw:
            step = max(step, ROLL_S)
        step = max(60, step - step % 60)
        series: dict[str, list] = {m: [] for m in metrics}
        with self.lock:
            for m in metrics:
                mfilter, args = "", [node, m, int(frm), int(to)]
                if model and m in TASK_METRICS:
                    mfilter, args = " AND model=?", args + [model]
                if use_raw:
                    q = f"SELECT (ts/{step})*{step} b, avg(value) FROM raw WHERE node=? AND metric=? AND ts>=? AND ts<=?{mfilter} GROUP BY b ORDER BY b"
                else:
                    q = (f"SELECT (bucket/{step})*{step} b, sum(avg*n)/sum(n) FROM roll WHERE node=? AND metric=? AND bucket>=? AND bucket<=?{mfilter} "
                         f"GROUP BY b ORDER BY b")
                series[m] = [[int(b), round(v, 3)] for b, v in self.db.execute(q, args)]
        return {"step_s": step, "resolution": "raw" if use_raw else "15m", "series": series}

    def summary(self, node: str, frm: float, to: float) -> dict:
        keys = ["tok_s", "decode_tok_s", "j_per_tok", "score", "power_w", "gpu_temp_c", "pass"]
        out = {"source": "metrics", "from": iso(frm), "to": iso(to)}
        with self.lock:
            for k in keys:
                a = self.db.execute("SELECT avg(value), count(*) FROM raw WHERE node=? AND metric=? AND ts>=? AND ts<?", (node, k, frm, to)).fetchone()
                if not a[1]:
                    a = self.db.execute("SELECT sum(avg*n)/sum(n), sum(n) FROM roll WHERE node=? AND metric=? AND bucket>=? AND bucket<?",
                                        (node, k, frm, to)).fetchone()
                val = round(a[0], 3) if a and a[0] is not None else None
                if k == "pass":
                    out["attempts"], out["pass_rate"] = int(a[1] or 0) if a else 0, val
                else:
                    out[k] = val
        return out


# ------------------------------------------------------------------ flows (chain-of-command map)
FLOW_DEFS = [
    # id, from, to, type, direction, label, expected interval s (None = event-driven)
    ("task.submit", "clients", "@me", "task", "inbound", "tasks submitted (CLI / API / OpenAI / Odysseus)", None),
    ("task.local", "@me", "@me", "task", "internal", "complex tasks + escalations run on the desktop model", None),
    ("task.dispatch", "@me", "@peer", "task", "outbound", "simple-task jobs handed to the mini (pulled by its agent)", None),
    ("task.result", "@peer", "@me", "task", "inbound", "mini returns finished jobs", None),
    ("review.verdict", "@me", "@peer", "review", "outbound", "desktop checks mini output; fix requests on failure", None),
    ("review.rootgate", "@peer", "@me", "review", "inbound", "mini asks desktop to second-review a root action", None),
    ("tuning.rootgate", "@me", "@peer", "tuning", "outbound", "desktop proposes allowlisted root actions to mini rootgate", None),
    ("tuning.restart_request", "@peer", "@me", "tuning", "inbound", "mini asks desktop to restart its user-level manager", None),
    ("tuning.changes", "clients", "@me", "tuning", "inbound", "tunings / upgrades logged via /api/changes", None),
    ("monitor.heartbeat", "@peer", "@me", "monitor", "inbound", "mini agent heartbeat (30 s)", 30),
    ("monitor.peer_poll", "@peer", "@me", "monitor", "inbound", "mini polls desktop /health (60 s)", 60),
    ("monitor.poll", "@me", "@peer", "monitor", "outbound", "desktop polls mini Ollama over wired + canary (60 s)", 60),
    ("alert.to_peer", "@me", "@peer", "alert", "outbound", "desktop alerts pulled by the mini", None),
    ("alert.to_me", "@peer", "@me", "alert", "inbound", "mini alerts pushed to the desktop", None),
    ("alert.to_user", "@me", "user", "alert", "outbound", "desktop notifications (notify-send)", None),
    ("network.wired", "@me", "@peer", "network", "bidirectional", "wired cluster link via router/switch", 60),
    # Cluster Network orchestrator (Odysseus chat -> bridge :11610), reported via POST /api/events
    ("orch.plan", "@me", "@me", "task", "internal", "Cluster Network chat: manager model classifies + plans", None),
    ("orch.dispatch", "@me", "@peer", "task", "outbound", "Cluster Network subtasks sent to the mini", None),
    ("orch.result", "@peer", "@me", "task", "inbound", "mini subtask results back to the manager", None),
    ("orch.review", "@me", "@peer", "review", "outbound", "manager reviews subtask results; fix requests", None),
    ("orch.synth", "@me", "clients", "task", "outbound", "manager synthesizes + streams the final answer to Odysseus", None),
    ("tools.search", "@me", "odysseus", "tools", "outbound", "web search via Odysseus SearXNG (bridge)", None),
    ("tools.memory", "@me", "odysseus", "tools", "bidirectional", "cluster memory in Odysseus ChromaDB (bridge)", None),
    ("tools.notify", "@me", "odysseus", "tools", "outbound", "ntfy push via Odysseus ntfy (bridge)", None),
    # Frontier Agent Offload (external agents -> llm-cluster-offload :11630), reported via POST /api/events
    ("offload.ingest", "frontier", "@me", "offload", "inbound", "frontier agent offload requests (MCP/REST on :11630)", None),
    ("offload.map", "@me", "@peer", "offload", "outbound", "parallel map/extract chunks dispatched to worker", None),
    ("offload.reduce", "@peer", "@me", "offload", "inbound", "worker map outputs returned to desktop for reduce/verify", None),
    ("offload.memory", "@me", "odysseus", "tools", "bidirectional", "offload semantic search / embeddings via ChromaDB", None),
    ("offload.return", "@me", "frontier", "offload", "outbound", "compact cited results returned to frontier agent", None),
]
FLOW_IDS = {f[0] for f in FLOW_DEFS}
EVENT_KINDS_EXTRA = ("orch.", "tool.", "bridge.", "offload.")


def _flow_tuple(item):
    """Normalize a FLOW_DEFS tuple or a devices/extra_flows.json object."""
    if isinstance(item, (list, tuple)):
        if len(item) == 6:
            return tuple(item) + (None,)
        return tuple(item[:7])
    if isinstance(item, dict) and item.get("id"):
        return (item["id"], item.get("from") or "@me", item.get("to") or "@peer",
                item.get("type") or "task", item.get("direction") or "outbound",
                item.get("label") or item["id"], item.get("expected_interval_s"))
    raise ValueError(f"bad flow def: {item!r}")


class Flows:
    def __init__(self):
        self.lock = threading.Lock()
        self.extra: list = []
        self.st = {f[0]: self._empty_state() for f in FLOW_DEFS}

    def _empty_state(self):
        return {"total": 0, "ok": 0, "error": 0, "recent": deque(maxlen=2000), "last": None, "last_ok": None,
                "latency_ms": None, "extra": {}}

    def defs(self):
        return list(FLOW_DEFS) + list(self.extra)

    def ensure(self, fid: str) -> None:
        with self.lock:
            if fid not in self.st:
                self.st[fid] = self._empty_state()
            FLOW_IDS.add(fid)

    def adopt(self, items) -> int:
        new = []
        for item in items or []:
            tup = _flow_tuple(item)
            new.append(tup)
            self.ensure(tup[0])
        with self.lock:
            ids = {t[0] for t in new}
            self.extra = [e for e in self.extra if e[0] not in ids] + new
        return len(new)

    def load_extra(self, path: Path) -> int:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return 0
        items = data.get("flows") if isinstance(data, dict) else data
        return self.adopt(items or [])


    def hit(self, fid: str, ok: bool = True, latency_ms: float | None = None, n: int = 1, **extra) -> None:
        with self.lock:
            s = self.st.get(fid)
            if not s:
                return
            now = time.time()
            s["total"] += n
            s["ok" if ok else "error"] += n
            s["recent"].append(now)
            s["last"], s["last_ok"] = now, ok
            if latency_ms is not None:
                s["latency_ms"] = round(latency_ms, 1)
            for k, v in extra.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    s["extra"][k] = s["extra"].get(k, 0) + v
                else:
                    s["extra"][k] = v

    def view(self) -> list[dict]:
        me, peer = G["CFG"]["node"], G["CFG"]["peer"]["name"]
        now = time.time()
        out = []
        with self.lock:
            for fid, frm, to, typ, direction, label, interval in self.defs():
                s = self.st.get(fid) or self._empty_state()
                last = s["last"]
                window = (interval * 2.5) if interval else 120
                if last and s["last_ok"] is False and now - last < max(window, 600):
                    status = "error"
                elif last and now - last < window:
                    status = "active"
                else:
                    status = "idle"
                if interval and (not last or now - last > window) and START < now - window:
                    status = "error"          # a periodic flow that stopped is a fault
                out.append({"id": fid, "from": frm.replace("@me", me).replace("@peer", peer),
                            "to": to.replace("@me", me).replace("@peer", peer), "type": typ, "direction": direction,
                            "label": label, "status": status, "last_activity": iso(last),
                            "counts": {"total": s["total"], "ok": s["ok"], "error": s["error"],
                                       "last_1h": sum(1 for t in s["recent"] if now - t < 3600), **s["extra"]},
                            "latency_ms": s["latency_ms"], "expected_interval_s": interval, "since": iso(START)})
        return out


# ------------------------------------------------------------------ module state
ACT: Activity
DB: MetricsDB
FLOWS = Flows()
CURRENT: dict[str, dict | None] = {}
PROFILE: dict = {}
CACHE: dict = {}
API_TOKEN = ""
ALLOW_NETS: list = []
CHANGES_JSONL: Path


def setup(g: dict) -> None:
    global ACT, DB, PROFILE, API_TOKEN, ALLOW_NETS, CHANGES_JSONL
    G.update(g)
    cfg = G["CFG"]
    ACT = Activity(G["JOURNAL"] / "activity.jsonl")
    DB = MetricsDB(G["STATE"] / "metrics.sqlite")
    CHANGES_JSONL = G["JOURNAL"] / "changes.jsonl"
    PROFILE = system_profile(cfg.get("hardware_overrides") or {})
    ALLOW_NETS = [ipaddress.ip_network(c) for c in cfg.get("api_allow_cidrs", ["172.16.0.0/12"])]
    _tok = Path(os.path.expanduser(cfg.get("api_token_file", "secret/api.token")))
    tok = _tok if _tok.is_absolute() else G["ROOT"] / _tok
    if not tok.exists():
        tok.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(str(tok), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.write(fd, (secrets.token_urlsafe(32) + "\n").encode())
        os.close(fd)
    os.chmod(tok, 0o600)
    API_TOKEN = tok.read_text().strip()
    if DB.count_task_points() == 0:
        backfill()
    ACT.emit(cfg["node"], "service", f"llm-cluster-manager {G['VERSION']} started (API v{API_VERSION})")
    try:
        n = FLOWS.load_extra(G["STATE"] / "extra_flows.json")
        if n:
            ACT.emit(cfg["node"], "service", f"loaded {n} extra onboard flow(s)")
    except Exception:
        pass


def backfill() -> None:
    for r in read_jsonl(G["TASKS_JSONL"]):
        ts = parse_iso(r.get("ts_end")) or time.time()
        for a in r.get("attempts") or []:
            record_attempt_metrics(a, ts)


def record_attempt_metrics(a: dict, ts: float | None = None) -> None:
    if not a.get("node"):
        return
    vals = {k: a.get(k) for k in ("tok_s", "decode_tok_s", "j_per_tok", "score", "energy_j")}
    vals["pass"] = 1.0 if (a.get("verdict") or {}).get("pass") else 0.0
    DB.add(a["node"], vals, ts, a.get("model"))


# ------------------------------------------------------------------ hooks called by manager.py
def emit(node, kind, msg, level="info", task_id=None, data=None):
    try:
        return ACT.emit(node, kind, msg, level, task_id, data)
    except Exception:  # noqa: BLE001
        return None


def flow(fid, ok=True, latency_ms=None, n=1, **extra):
    try:
        FLOWS.hit(fid, ok, latency_ms, n, **extra)
    except Exception:  # noqa: BLE001
        pass


def set_current(node: str, info: dict | None) -> None:
    CURRENT[node] = ({**info, "since": now_iso()} if info else None)


def on_attempt(tid: str, att: dict, phase_info: dict | None = None) -> None:
    me, peer = G["CFG"]["node"], G["CFG"]["peer"]["name"]
    v = att.get("verdict") or {}
    ok = bool(v.get("pass"))
    record_attempt_metrics(att)
    msg = (f"attempt {att['n']} {att['phase']} {att.get('path')} {att.get('model')} {'PASS' if ok else 'FAIL'} [{v.get('method')}"
           f"{(' ' + str(v.get('score'))) if v.get('score') is not None else ''}] {att.get('wall_s')}s "
           f"{att.get('tok_s')} tok/s {att.get('j_per_tok')} J/tok")
    emit(att.get("node") or me, "attempt.done", msg, "info" if ok else "warn", tid,
         {k: att.get(k) for k in ("n", "phase", "path", "model", "wall_s", "tok_s", "j_per_tok", "score", "energy_j", "failover")}
         | {"pass": ok, "method": v.get("method"), "issues": v.get("issues")})
    if att.get("node") == peer:
        flow("review.verdict", True, (v.get("check_s") or 0) * 1000, passed=int(ok), failed=int(not ok))
        if att.get("path") == "mini-agent":
            flow("task.result", bool(att.get("ok")), (att.get("wall_s") or 0) * 1000)
    else:
        flow("task.local", bool(att.get("ok")), (att.get("wall_s") or 0) * 1000)
    if not ok and v.get("issues"):
        emit(me, "task.fix", "fix requests: " + " / ".join(v["issues"])[:400], "info", tid)


def sample() -> None:
    """Called every monitor poll (60 s): host metrics for both nodes + network flow."""
    S, cfg = G["S"], G["CFG"]
    me, peer = cfg["node"], cfg["peer"]["name"]
    nv = nvidia_stats() or {}
    h = S.health_cache or {}
    mem = meminfo()
    d = S.peer.get("direct") or {}
    DB.add(me, {"power_w": nv.get("power_w"), "gpu_temp_c": nv.get("temp_c"), "gpu_util_pct": nv.get("util_pct"),
                "gpu_clock_mhz": nv.get("clock_mhz"), "vram_used_mib": nv.get("vram_used_mib"), "cpu_temp_c": read_coretemp_package(),
                "ram_pct": mem.get("ram_pct"), "queue_depth": S.task_q.qsize() + (1 if S.running else 0),
                "ollama_latency_ms": (h.get("ollama") or {}).get("latency_ms"), "peer_latency_ms": d.get("latency_ms")})
    flow("network.wired", bool(d.get("up")), d.get("latency_ms"))
    flow("monitor.poll", bool(d.get("up")), d.get("latency_ms"))
    hb = S.peer.get("hb") or {}
    age = time.time() - S.peer["hb_at"] if S.peer.get("hb_at") else 1e9
    if hb and age < cfg["peer"]["heartbeat_stale_s"] and hb.get("ts") != CACHE.get("last_hb_sampled"):
        CACHE["last_hb_sampled"] = hb.get("ts")
        host = hb.get("host") or {}
        g = host.get("gpu") or {}
        DB.add(peer, {"power_w": hb.get("power_w"), "cpu_temp_c": (hb.get("temps") or {}).get("cpu_c"),
                      "gpu_temp_c": (hb.get("temps") or {}).get("gpu_c"), "gpu_util_pct": g.get("util_pct"),
                      "gpu_clock_mhz": g.get("clock_mhz"), "vram_used_mib": g.get("vram_used_mib"),
                      "ram_pct": (host.get("memory") or {}).get("ram_pct"), "queue_depth": len(S.mini_jobs),
                      "ollama_latency_ms": (hb.get("ollama") or {}).get("latency_ms"), "peer_latency_ms": hb.get("peer_latency_ms")})
    if time.time() - CACHE.get("maint", 0) > 3600:
        CACHE["maint"] = time.time()
        DB.maintain()


# ------------------------------------------------------------------ node view
def task_rows() -> list[dict]:
    c = CACHE.get("rows")
    p = G["TASKS_JSONL"]
    mt = p.stat().st_mtime if p.exists() else 0
    if c and c[0] == mt:
        return c[1]
    rows = read_jsonl(p)
    CACHE["rows"] = (mt, rows)
    return rows


def excluded() -> set:
    f = G["STATE"] / "findings_exclude.txt"
    return set(f.read_text().split()) if f.exists() else set()


def node_stats(node: str) -> dict:
    ex = excluded()
    cut = time.time() - 7 * 86400
    att7, att_all, fin = [], [], []
    for r in task_rows():
        if r.get("id") in ex:
            continue
        t = parse_iso(r.get("ts_end")) or 0
        atts = r.get("attempts") or []
        mine = [a for a in atts if a.get("node") == node]
        att_all += mine
        if t >= cut:
            att7 += mine
            if atts and atts[-1].get("node") == node:
                fin.append(r)
    per = {}
    for a in att7:
        per.setdefault(a.get("model"), []).append(a)
    pr = lambda xs: round(sum(1 for a in xs if (a.get("verdict") or {}).get("pass")) / len(xs), 3) if xs else None
    return {"window": "7d", "attempts": len(att7), "attempt_pass_rate": pr(att7),
            "tasks_finished_here": len(fin),
            "task_success_rate": round(sum(1 for r in fin if r.get("status") == "passed") / len(fin), 3) if fin else None,
            "per_model": [{"model": m, "attempts": len(xs), "pass_rate": pr(xs), "avg_tok_s": avg([a.get("tok_s") for a in xs]),
                           "avg_decode_tok_s": avg([a.get("decode_tok_s") for a in xs]),
                           "avg_j_per_tok": avg([a.get("j_per_tok") for a in xs]), "avg_score": avg([a.get("score") for a in xs])}
                          for m, xs in sorted(per.items(), key=lambda kv: -len(kv[1]))],
            "all_time": {"attempts": len(att_all), "attempt_pass_rate": pr(att_all)}}


def active_alerts(node: str, problems: list) -> list[dict]:
    out = [{"ts": now_iso(), "level": "warn", "key": "problem", "msg": p, "from": G["CFG"]["node"], "current": True} for p in problems]
    cut = time.time() - 3600
    for a in read_jsonl(G["ALERTS_JSONL"], 300):
        if a.get("about") == node and a.get("level") in ("warn", "crit") and (parse_iso(a.get("ts")) or 0) >= cut:
            out.append({k: a.get(k) for k in ("ts", "level", "key", "msg", "from")} | {"current": False})
    return out[-20:]


def last_benchmark(node: str, reported: dict | None) -> dict | None:
    best = reported if reported and reported.get("ts") else None
    for c in read_jsonl(CHANGES_JSONL):
        if c.get("node") == node and c.get("kind") == "benchmark" and (not best or (c.get("ts") or "") > best["ts"]):
            best = {"ts": c["ts"], "source": f"change {c['id']}: {c.get('title')}"}
    return best


def cached(key: str, ttl: float, fn):
    c = CACHE.get(key)
    if c and time.time() - c[0] < ttl:
        return c[1]
    v = fn()
    CACHE[key] = (time.time(), v)
    return v


def desktop_node() -> dict:
    S, cfg = G["S"], G["CFG"]
    me = cfg["node"]
    h = S.health_cache or G["local_health"]()
    nv = cached("nv", 5, nvidia_stats) or {}
    inv = cached("inv_me", 10, lambda: ollama_inventory(cfg["ollama"]))
    d = S.peer.get("direct") or {}
    mem = meminfo()
    return {
        "id": me, "name": me, "role": "manager",
        "job": "Manager: takes tasks (CLI/API/OpenAI), classifies, runs complex ones on gemma4:12b / qwen3-coder-30b, "
               "dispatches simple ones to the mini, checks every result, monitors the mini",
        "ips": cached("ips", 300, ipv4_by_role), "hardware": PROFILE,
        "status": "ok" if h.get("ok") else ("degraded" if h else "unknown"), "status_detail": h.get("problems") or [],
        "last_seen": h.get("ts"), "heartbeat_age_s": 0.0,
        "uptime": {"system_s": uptime_s(), "service_s": round(time.time() - START), "service": "llm-cluster-manager"},
        "power": {"w": nv.get("power_w"), "source": "nvidia-smi (GPU board only; CPU RAPL needs root)"},
        "temps": {"cpu_c": read_coretemp_package(), "gpu_c": nv.get("temp_c"), "nvme_c": None},
        "gpu": {k: nv.get(k) for k in ("util_pct", "clock_mhz", "clock_max_mhz", "mem_clock_mhz", "vram_used_mib", "vram_total_mib", "pstate")},
        "memory": {k: mem.get(k) for k in ("ram_used_gib", "ram_total_gib", "ram_pct")},
        "disk": cached("disk_me", 300, lambda: disk_usage(("/", os.path.expanduser("~"), "/var/lib/ollama"))),
        "ollama": {**inv, "backend": cfg.get("ollama_backend", "cuda"), "url": cfg["ollama"]},
        "latency": {"to_peer_ms": d.get("latency_ms"), "peer": cfg["peer"]["name"], "method": "HTTP GET mini :11434/api/version over wired"},
        "queue": {"depth": S.task_q.qsize() + (1 if S.running else 0), "running": S.running,
                  "detail": {"tasks_pending": S.task_q.qsize(), "mini_jobs_pending": len(S.mini_jobs)}},
        "current_task": CURRENT.get(me), "busy": None, "user_idle": h.get("user_idle"),
        "stats": cached("stats_me", 20, lambda: node_stats(me)),
        "last_benchmark": last_benchmark(me, None),
        "alerts_active": active_alerts(me, h.get("problems") or []),
    }


def mini_node() -> dict:
    S, cfg = G["S"], G["CFG"]
    peer = cfg["peer"]["name"]
    hb = S.peer.get("hb") or {}
    age = round(time.time() - S.peer["hb_at"], 1) if S.peer.get("hb_at") else None
    fresh = age is not None and age < cfg["peer"]["heartbeat_stale_s"]
    host = hb.get("host") or {}
    g = host.get("gpu") or {}
    d = S.peer.get("direct") or {}
    inv = hb.get("inventory") or {}
    if not fresh and d.get("up"):
        inv = cached("inv_peer", 30, lambda: ollama_inventory(cfg["peer"]["ollama"]))
    prof = hb.get("profile") or {"system": {}, "cpu": {}, "gpu": {}, "os": {}, "logo": {}}
    problems = S.peer.get("problems") or []
    return {
        "id": peer, "name": peer, "role": "worker",
        "job": "Worker: runs fast/general worker tasks via Ollama; reports energy; "
               "monitors the manager; runs the overnight tuner",
        "ips": hb.get("ips") or {"wired": cfg["peer"]["ip"], "wifi": None, "tailscale": None}, "hardware": prof,
        "status": S.peer.get("state") or "unknown", "status_detail": problems,
        "last_seen": iso(S.peer.get("hb_at")), "heartbeat_age_s": age,
        "uptime": {"system_s": host.get("uptime_s"), "service_s": hb.get("service_uptime_s"), "service": "llm-cluster-agent"},
        "power": {"w": hb.get("power_w"), "source": "amdgpu power1_input (APU package PPT, CPU+GPU)"},
        "temps": hb.get("temps") or {},
        "gpu": {k: g.get(k) for k in ("util_pct", "clock_mhz", "clock_max_mhz", "vram_used_mib", "vram_total_mib")} | {"mem_clock_mhz": None},
        "memory": host.get("memory") or {}, "disk": host.get("disk") or [],
        "ollama": {"up": (hb.get("ollama") or {}).get("up") if fresh else d.get("up"),
                   "version": (hb.get("ollama") or {}).get("version") or d.get("version"),
                   "backend": hb.get("ollama_backend") or "vulkan", "url": cfg["peer"]["ollama"],
                   "latency_ms": (hb.get("ollama") or {}).get("latency_ms") if fresh else d.get("latency_ms"),
                   "latency_from_manager_ms": d.get("latency_ms"),
                   "installed": inv.get("installed"), "loaded": inv.get("loaded")},
        "latency": {"to_peer_ms": hb.get("peer_latency_ms"), "peer": cfg["node"], "method": "HTTP GET manager /health over wired (signed)"},
        "queue": {"depth": len(S.mini_jobs) + (1 if CURRENT.get(peer) else 0), "running": (CURRENT.get(peer) or {}).get("task_id"),
                  "detail": {"jobs_pending": len(S.mini_jobs), "jobs_in_flight": len(S.jobs_taken)}},
        "current_task": CURRENT.get(peer), "busy": hb.get("busy"),
        "canary": hb.get("canary") or S.peer.get("canary"),
        "stats": cached("stats_peer", 20, lambda: node_stats(peer)),
        "last_benchmark": last_benchmark(peer, hb.get("last_benchmark")),
        "alerts_active": active_alerts(peer, problems),
        "data_fresh": fresh,
    }


def nodes() -> list[dict]:
    return [desktop_node(), mini_node()]


# ------------------------------------------------------------------ tasks
def summarize_task(r: dict, full: bool = False) -> dict:
    t = r.get("task") or {}
    out = {k: r.get(k) for k in ("id", "ts_submit", "ts_start", "ts_end", "status", "classification", "route_initial", "route_final",
                                 "rounds_total", "first_try_pass", "escalated", "timings", "energy", "error")}
    out["text_preview"] = (t.get("text") or "")[:240]
    out["source"] = t.get("source")
    out["target"] = t.get("route") or "auto"
    out["excluded_from_findings"] = r.get("id") in excluded()
    atts = []
    for a in r.get("attempts") or []:
        x = {k: a.get(k) for k in ("n", "phase", "target", "node", "path", "model", "failover", "ok", "error", "wall_s", "load_s",
                                   "eval_tokens", "tok_s", "decode_tok_s", "energy_j", "avg_w", "j_per_tok", "score", "power_source")}
        x["verdict"] = {k: (a.get("verdict") or {}).get(k) for k in ("pass", "method", "score", "issues", "check_s", "checker_model")}
        if full:
            x["output_preview"], x["fix_requests_in"] = a.get("output_preview"), a.get("fix_requests_in")
        atts.append(x)
    out["attempts"] = atts
    if full:
        out["task"] = t
        out["final_output"] = r.get("final_output")
        out["journal_md"] = str(G["TASKS_MD"] / f"{r.get('id')}.md")
    return out


def find_task(tid: str) -> dict | None:
    rec = G["S"].tasks.get(tid)
    if rec:
        return rec
    for r in reversed(task_rows()):
        if r.get("id") == tid:
            return r
    return None


def list_tasks(q: dict) -> dict:
    node = node_id(q.get("node", "all")) if q.get("node") else "all"
    status, n = q.get("status"), min(int(q.get("n") or 50), 1000)
    since = parse_t(q.get("since_ts"), 0) if q.get("since_ts") else 0
    text = (q.get("q") or "").lower()
    me = G["CFG"]["node"]

    def match(r):
        if status and r.get("status") != status:
            return False
        if since and (parse_iso(r.get("ts_submit")) or 0) < since:
            return False
        if text and text not in ((r.get("task") or {}).get("text") or "").lower():
            return False
        if node and node != "all":
            atts = r.get("attempts") or []
            if atts:
                return any(a.get("node") == node for a in atts)
            tgt = (r.get("task") or {}).get("route") or r.get("route_initial")
            return (tgt == "mini") == (node != me)
        return True
    live = [summarize_task(r) for r in list(G["S"].tasks.values()) if r.get("status") in ("queued", "running") and match(r)]
    done = [summarize_task(r) for r in reversed(task_rows()) if match(r)][:n]
    return {"tasks": done, "live": live}


def submit_task(data: dict):
    tgt = (data.get("target") or data.get("route") or "auto").lower()
    route = {"auto": "auto", "desktop": "desktop", "mini": "mini", G["CFG"]["node"]: "desktop", G["CFG"]["peer"]["name"]: "mini"}.get(tgt)
    if route is None:
        raise ValueError(f"unknown target {tgt!r}")
    task = {k: data.get(k) for k in ("text", "type", "tests", "hidden_tests", "expect", "expect_exact", "complexity", "classify", "max_rounds")
            if data.get(k) is not None}
    task.setdefault("type", "auto")
    task["route"] = route
    return G["submit"](task, data.get("source") or "api-v1")


# ------------------------------------------------------------------ changes
def changes(node: str | None = None, n: int = 100) -> list[dict]:
    out = []
    for c in reversed(read_jsonl(CHANGES_JSONL)):
        if node and node != "all" and c.get("node") != node:
            continue
        out.append(enrich_change(c))
        if len(out) >= n:
            break
    return out


def enrich_change(c: dict) -> dict:
    c = dict(c)
    ts = parse_iso(c.get("ts")) or time.time()
    w = float(c.get("window_h") or 24) * 3600
    if not c.get("before"):
        c["before"] = DB.summary(c["node"], ts - w, ts)
    else:
        c["before"] = {"source": "given", **c["before"]}
    if not c.get("after"):
        end = min(time.time(), ts + w)
        c["after"] = DB.summary(c["node"], ts, end) | {"complete": time.time() >= ts + w}
    else:
        c["after"] = {"source": "given", "complete": True, **c["after"]}
    delta = {}
    for k, b in c["before"].items():
        a = c["after"].get(k)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and b:
            delta[k] = round(100 * (a - b) / abs(b), 1)
    c["delta_pct"] = delta
    return c


def add_change(data: dict) -> dict:
    node = node_id(data.get("node") or "")
    if not node or node == "all":
        raise ValueError(f"node must be {G['CFG']['node']} or {G['CFG']['peer']['name']}")
    if not (data.get("title") or "").strip():
        raise ValueError("title required")
    kind = data.get("kind") or "other"
    if kind not in ("tuning", "upgrade", "model", "config", "hardware", "benchmark", "other"):
        raise ValueError("bad kind")
    ts = data.get("ts")
    if ts:
        ts = iso(parse_t(str(ts), time.time()))
    c = {"id": time.strftime("c%Y%m%d-%H%M%S-") + secrets.token_hex(2), "ts": ts or now_iso(), "node": node, "kind": kind,
         "title": str(data["title"])[:200], "description": str(data.get("description") or "")[:4000],
         "author": str(data.get("author") or "api")[:60], "window_h": float(data.get("window_h") or 24),
         "before": data.get("before") if isinstance(data.get("before"), dict) else None,
         "after": data.get("after") if isinstance(data.get("after"), dict) else None}
    append_jsonl(CHANGES_JSONL, c)
    emit(node, "change", f"{kind}: {c['title']}", "info", None, {"id": c["id"]})
    flow("tuning.changes", True)
    try:
        G["write_findings"]()
    except Exception:  # noqa: BLE001
        pass
    return enrich_change(c)


def changes_markdown_lines() -> list[str]:
    cs = changes(None, 50)
    if not cs:
        return []
    L = ["## Change log (from /api/changes; before/after = metrics over window_h around the change)", "",
         "| when (UTC) | node | kind | change | tok/s before → after | J/tok before → after | pass rate before → after | after complete |",
         "|---|---|---|---|---|---|---|---|"]
    f = lambda d, k: "—" if d.get(k) is None else d.get(k)
    for c in cs:
        b, a = c["before"], c["after"]
        L.append(f"| {c['ts']} | {c['node']} | {c['kind']} | {c['title']} | {f(b,'tok_s')} → {f(a,'tok_s')} | "
                 f"{f(b,'j_per_tok')} → {f(a,'j_per_tok')} | {f(b,'pass_rate')} → {f(a,'pass_rate')} | {a.get('complete')} |")
    return L + [""]



def _advisor():
    root = Path(G.get("ROOT") or Path(__file__).resolve().parents[1])
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import importlib
    return importlib.import_module("onboard.advisor")


def handle_onboard(h, method: str, parts: list, data: dict):
    """GET/POST /api/onboard/* — advise and register new hardware. Never uses sudo."""
    adv = _advisor()
    sub = "/".join(parts) or "instructions"
    if method == "GET" and sub in ("", "instructions"):
        return h._send(200, adv.instructions())
    if method == "POST" and sub == "reload":
        n = FLOWS.load_extra(G["STATE"] / "extra_flows.json")
        emit(G["CFG"]["node"], "onboard.reload", f"reloaded {n} extra onboard flow(s)")
        return h._send(200, {"ok": True, "loaded": n, "ids": [t[0] for t in FLOWS.extra]})
    if method == "POST" and sub == "advise":
        probe = data.get("probe") if isinstance(data.get("probe"), dict) else data
        if not isinstance(probe, dict) or not probe:
            raise ValueError("probe JSON object required")
        advice = adv.advise(probe, role=data.get("role"), device_id=data.get("id"))
        return h._send(200, {"advice": advice, "markdown": adv.render_advice(advice)})
    if method == "POST" and sub == "apply":
        if not (data.get("confirm") or data.get("yes")):
            raise ValueError("confirm=true is required")
        if isinstance(data.get("advice"), dict) and data["advice"].get("device"):
            advice = data["advice"]
        else:
            probe = data.get("probe") if isinstance(data.get("probe"), dict) else None
            if not probe:
                raise ValueError("advice or probe required")
            advice = adv.advise(probe, role=data.get("role"), device_id=data.get("id"))
        result = adv.apply_advice(advice, confirm=True, update=bool(data.get("update")))
        FLOWS.load_extra(G["STATE"] / "extra_flows.json")
        emit(G["CFG"]["node"], "onboard.apply", f"registered device {result['id']}", "info", data=result)
        try:
            add_change({"node": result["id"], "kind": "onboard", "title": f"onboarded {result['id']} as {advice.get('role')}",
                        "description": advice.get("position") or "", "author": "onboard"})
        except Exception:
            pass
        return h._send(201, result)
    if method == "POST" and sub == "remove":
        if not (data.get("confirm") or data.get("yes")):
            raise ValueError("confirm=true is required")
        did = str(data.get("id") or data.get("device_id") or "")
        result = adv.remove_device(did, confirm=True)
        FLOWS.load_extra(G["STATE"] / "extra_flows.json")
        emit(G["CFG"]["node"], "onboard.remove", f"unregistered device {did}", "info", data=result)
        return h._send(200, result)
    return h._send(404, {"error": "unknown onboard endpoint"})


# ------------------------------------------------------------------ HTTP
ENDPOINTS = ["GET /api", "GET /api/health", "GET /api/nodes", "GET /api/nodes/{id}", "GET /api/nodes/{id}/log?since=&n=",
             "GET /api/nodes/{id}/log/stream (SSE)", "GET /api/nodes/{id}/metrics?from=&to=&step=&metrics=&model=",
             "GET /api/tasks?node=&status=&n=&since_ts=&q=", "GET /api/tasks/{id}", "POST /api/tasks",
             "GET /api/changes?node=&n=", "POST /api/changes", "GET /api/alerts?node=&n=&level=", "GET /api/flows",
             "GET /api/offload?n=", "POST /api/events", "GET /api/onboard/instructions", "POST /api/onboard/advise", "POST /api/onboard/apply", "POST /api/onboard/remove", "POST /api/onboard/reload"]


def offload_summary(n: int = 20) -> dict:
    import socket as _sock
    jpath = Path(G.get("ROOT") or Path(__file__).resolve().parents[1]) / "offload" / "journal" / "jobs.jsonl"
    rows = read_jsonl(jpath, 2000)
    up = False
    try:
        with _sock.create_connection(("127.0.0.1", 11630), timeout=0.5):
            up = True
    except OSError:
        up = False
    totals = {
        "jobs_total": len(rows),
        "jobs_ok": sum(1 for r in rows if r.get("status") == "done"),
        "jobs_error": sum(1 for r in rows if r.get("status") == "error"),
        "raw_input_tokens_est": sum(int(r.get("raw_input_tokens_est") or 0) for r in rows),
        "local_input_tokens": sum(int(r.get("local_input_tokens") or 0) for r in rows),
        "local_output_tokens": sum(int(r.get("local_output_tokens") or 0) for r in rows),
        "returned_output_tokens": sum(int(r.get("returned_output_tokens") or 0) for r in rows),
        "frontier_tokens_saved_est": sum(int(r.get("frontier_tokens_saved_est") or 0) for r in rows),
        "manager_time_s": round(sum(float((r.get("node_time_s") or {}).get(G["CFG"]["node"]) or 0.0) for r in rows), 2),
        "worker_time_s": round(sum(float((r.get("node_time_s") or {}).get(G["CFG"]["peer"]["name"]) or 0.0) for r in rows), 2),
    }
    by_tool: dict[str, dict] = {}
    for r in rows:
        t = r.get("tool") or "other"
        bt = by_tool.setdefault(t, {"jobs": 0, "saved_tokens": 0, "local_tokens": 0})
        bt["jobs"] += 1
        bt["saved_tokens"] += int(r.get("frontier_tokens_saved_est") or 0)
        bt["local_tokens"] += int(r.get("local_input_tokens") or 0) + int(r.get("local_output_tokens") or 0)
    return {
        "api_version": API_VERSION,
        "ts": now_iso(),
        "service_up": up,
        "port": 11630,
        "totals": totals,
        "by_tool": by_tool,
        "recent_jobs": list(reversed(rows[-max(1, min(int(n), 100)):])),
    }


def post_event(data: dict) -> dict:
    """External reporters (the Odysseus bridge/orchestrator) add activity events and light flows."""
    nid = node_id(str(data.get("node") or G["CFG"]["node"]))
    if not nid or nid == "all":
        raise ValueError(f"node must be {G['CFG']['node']} or {G['CFG']['peer']['name']}")
    kind = str(data.get("kind") or "bridge.event")[:40]
    if not kind.startswith(EVENT_KINDS_EXTRA):
        raise ValueError("kind must start with orch., tool., bridge. or offload.")
    level = data.get("level") if data.get("level") in ("debug", "info", "warn", "crit") else "info"
    msg = str(data.get("msg") or "")[:500]
    ev = None
    if msg:
        ev = emit(nid, kind, msg, level, data.get("task_id"), data.get("data") if isinstance(data.get("data"), dict) else None)
    lit = []
    for f in data.get("flows") or []:
        fid = f.get("id") if isinstance(f, dict) else f
        if fid in FLOW_IDS:
            extra = {}
            if isinstance(f, dict) and isinstance(f.get("counts"), dict):
                extra = {k: int(v) for k, v in f["counts"].items()
                         if k in ("passed", "failed", "escalated", "unverified") and isinstance(v, (int, bool))}
            flow(fid, bool(f.get("ok", True)) if isinstance(f, dict) else True,
                 f.get("latency_ms") if isinstance(f, dict) else None, **extra)
            lit.append(fid)
    return {"ok": True, "seq": (ev or {}).get("seq") if isinstance(ev, dict) else None, "flows": lit}


def authorized(h) -> bool:
    ip = h.client_address[0]
    if ip in ("127.0.0.1", "::1"):
        return True
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        addr = None
    if not addr or not any(addr in n for n in ALLOW_NETS):
        h._send(403, {"error": "source not allowed for /api"})
        return False
    auth = h.headers.get("Authorization") or ""
    if not (auth.startswith("Bearer ") and hmac.compare_digest(auth[7:].strip(), API_TOKEN)):
        h._send(401, {"error": "bearer token required"})
        return False
    return True


def sse(h, node: str, since: int | None, n: int) -> None:
    h.send_response(200)
    h.send_header("Content-Type", "text/event-stream; charset=utf-8")
    h.send_header("Cache-Control", "no-cache")
    h.send_header("X-Accel-Buffering", "no")
    h.send_header("Connection", "close")
    h.end_headers()
    h.close_connection = True
    pending = ACT.since(since, node, 5000) if since is not None else ACT.tail(node, n)
    last = pending[-1]["seq"] if pending else ACT.seq
    try:
        h.wfile.write(b"retry: 3000\n\n")
        while True:
            for e in pending:
                h.wfile.write(f"id: {e['seq']}\nevent: activity\ndata: {json.dumps(e, ensure_ascii=False, default=str)}\n\n".encode())
                last = e["seq"]
            h.wfile.flush()
            with ACT.cv:
                if ACT.seq <= last:
                    ACT.cv.wait(15)
            pending = ACT.since(last, node, 500)
            if not pending:
                h.wfile.write(b": ping\n\n")
                h.wfile.flush()
    except (BrokenPipeError, ConnectionResetError, OSError):
        return


def handle(h, method: str, path: str, qs: dict, body: bytes) -> None:
    if not authorized(h):
        return
    q = {k: v[0] for k, v in qs.items()}
    parts = [p for p in path.split("/") if p][1:]      # drop "api"
    try:
        if method == "GET":
            if not parts:
                return h._send(200, {"api_version": API_VERSION, "service": "llm-cluster-manager", "version": G["VERSION"],
                                     "node": G["CFG"]["node"], "nodes": [G["CFG"]["node"], G["CFG"]["peer"]["name"]],
                                     "endpoints": ENDPOINTS})
            if parts == ["health"]:
                return h._send(200, G["local_health"]())
            if parts == ["nodes"]:
                return h._send(200, {"api_version": API_VERSION, "ts": now_iso(), "nodes": nodes()})
            if parts[0] == "nodes" and len(parts) >= 2:
                nid = node_id(parts[1])
                if not nid or (nid == "all" and len(parts) == 2):
                    return h._send(404, {"error": "unknown node"})
                if len(parts) == 2:
                    return h._send(200, desktop_node() if nid == G["CFG"]["node"] else mini_node())
                if parts[2] == "log" and len(parts) == 3:
                    since = int(q.get("since") or 0)
                    ev = ACT.since(since, nid, min(int(q.get("n") or 200), 2000))
                    return h._send(200, {"events": ev, "next_since": ev[-1]["seq"] if ev else max(since, 0) if since else ACT.seq})
                if parts[2] == "log" and len(parts) == 4 and parts[3] == "stream":
                    lei = h.headers.get("Last-Event-ID") or q.get("since")
                    return sse(h, nid, int(lei) if lei else None, int(q.get("n") or 50))
                if parts[2] == "metrics" and nid != "all":
                    to = parse_t(q.get("to"), time.time())
                    frm = parse_t(q.get("from"), to - 86400)
                    if frm >= to:
                        return h._send(400, {"error": "from must be before to"})
                    ms = [m for m in (q.get("metrics") or ",".join(list(HOST_METRICS) + list(TASK_METRICS))).split(",") if m]
                    bad = [m for m in ms if m not in HOST_METRICS and m not in TASK_METRICS]
                    if bad:
                        return h._send(400, {"error": f"unknown metrics {bad}", "available": list(HOST_METRICS) + list(TASK_METRICS)})
                    step = q.get("step")
                    r = DB.query(nid, frm, to, int(step) if step and step != "auto" else None, ms, q.get("model"))
                    return h._send(200, {"node": nid, "from": iso(frm), "to": iso(to), **r,
                                         "units": {m: (HOST_METRICS | TASK_METRICS)[m] for m in ms},
                                         "retention": {"raw_s": RAW_KEEP_S, "rollup_s": ROLL_KEEP_S, "rollup_step_s": ROLL_S}})
                return h._send(404, {"error": "not found"})
            if parts == ["tasks"]:
                return h._send(200, list_tasks(q))
            if parts[0] == "tasks" and len(parts) == 2:
                r = find_task(parts[1])
                return h._send(200, summarize_task(r, True)) if r else h._send(404, {"error": "unknown task"})
            if parts == ["changes"]:
                return h._send(200, {"changes": changes(node_id(q["node"]) if q.get("node") else None, int(q.get("n") or 100))})
            if parts == ["alerts"]:
                nid = node_id(q["node"]) if q.get("node") else None
                al = [a for a in reversed(read_jsonl(G["ALERTS_JSONL"], 3000))
                      if (not nid or nid == "all" or a.get("about") == nid or a.get("from") == nid)
                      and (not q.get("level") or a.get("level") == q["level"])]
                return h._send(200, {"alerts": al[:int(q.get("n") or 50)]})
            if parts == ["flows"]:
                return h._send(200, {"api_version": API_VERSION, "ts": now_iso(), "flows": FLOWS.view(),
                                     "nodes": [{"id": G["CFG"]["node"], "role": "manager"}, {"id": G["CFG"]["peer"]["name"], "role": "worker"},
                                               {"id": "clients", "role": "external"}, {"id": "user", "role": "human"},
                                               {"id": "odysseus", "role": "service"}, {"id": "frontier", "role": "external"}]})
            if parts == ["offload"]:
                return h._send(200, offload_summary(int(q.get("n") or 20)))
            if parts[:1] == ["onboard"]:
                return handle_onboard(h, "GET", parts[1:], {})
            return h._send(404, {"error": "not found"})
        if method == "POST":
            try:
                data = json.loads(body.decode() or "{}")
            except json.JSONDecodeError:
                return h._send(400, {"error": "invalid json"})
            if parts == ["tasks"]:
                tid = submit_task(data)
                if data.get("wait"):
                    G["S"].task_events[tid].wait(float(data.get("timeout") or 900))
                    return h._send(200, summarize_task(G["S"].tasks[tid], True))
                return h._send(202, {"id": tid, "status": "queued",
                                     "links": {"self": f"/api/tasks/{tid}", "log": "/api/nodes/all/log/stream"}})
            if parts == ["changes"]:
                return h._send(201, add_change(data))
            if parts == ["events"]:
                return h._send(201, post_event(data))
            if parts[:1] == ["onboard"]:
                return handle_onboard(h, "POST", parts[1:], data)
            return h._send(404, {"error": "not found"})
        return h._send(405, {"error": "method not allowed"})
    except ValueError as e:
        return h._send(400, {"error": str(e)})
