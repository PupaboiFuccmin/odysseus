#!/usr/bin/env python3
"""llm-cluster-manager: user-level cluster manager.

Binds loopback and configured LAN IP (default port 11600).
Stdlib only. Does not touch system Ollama config, Odysseus, Docker, or packages.

Pipeline:  submit (CLI / HTTP / OpenAI-compatible) -> classify (simple|complex)
  simple  -> worker pull queue (worker agent runs models on its GPU/APU and measures energy)
  complex -> local manager Ollama
  -> desktop checks (sandboxed tests / expect / gemma rubric) -> fix loop up to max_rounds
  -> escalate to desktop -> needs_human.  Every task -> journal/tasks.jsonl + journal/tasks/<id>.md

Monitoring: polls mini heartbeat + mini Ollama (:11434, wired), canary when the agent is stale,
alerts both ways, limited remediation (propose `systemctl restart ollama` through mini rootgate).
"""
from __future__ import annotations

import ipaddress
import json
import os
import queue
import re
import secrets
import subprocess
import sys
import threading
import time
import traceback
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
import api  # noqa: E402
import checker  # noqa: E402
from clusterlib import (SCHEMA_TASK, NonceCache, append_jsonl, http_json, load_token, now_iso,  # noqa: E402
                        ollama_chat, read_coretemp_package, read_jsonl, run_measured, verify_headers,
                        write_json_atomic)

VERSION = "1.2.0"
ROOT = Path(os.path.expanduser(os.environ.get("LLM_CLUSTER_HOME") or str(Path(__file__).resolve().parents[1])))
CFG_PATH = ROOT / "config.json"
JOURNAL = ROOT / "journal"
TASKS_JSONL = JOURNAL / "tasks.jsonl"
TASKS_MD = JOURNAL / "tasks"
ALERTS_JSONL = JOURNAL / "alerts.jsonl"
HEALTH_JSONL = JOURNAL / "health.jsonl"
STATE = ROOT / "state"
RESTART_FLAG = STATE / "RESTART_REQUESTED"
NEEDS_HUMAN_MD = JOURNAL / "NEEDS_HUMAN.md"
FINDINGS_MD = ROOT / "FINDINGS-pipeline.md"

DEFAULT_CFG = {
    "node": "manager-node",
    "bind": ["127.0.0.1"],
    "port": 11600,
    "ollama": "http://127.0.0.1:11434",
    "token_file": str(ROOT / "secret/cluster.token"),
    "peer": {"name": "worker-node", "ip": "192.0.2.20", "ollama": "http://192.0.2.20:11434",
             "heartbeat_stale_s": 90},
    "models": {"desktop_text": "gemma4:12b", "desktop_code": "qwen3-coder-30b:latest", "checker": "gemma4:12b",
               "mini_fast": "qwen2.5:3b", "mini_main": "qwen2.5:7b", "mini_code": "qwen2.5-coder:7b"},
    "desktop_options": {"num_ctx": 8192, "temperature": 0.2},
    "mini_options": {"num_ctx": 4096, "temperature": 0.2},
    "keep_alive_desktop": "2m",
    "max_rounds": 3,
    "desktop_rounds": 2,
    "job_timeout_s": 420,
    "poll_interval_s": 60,
    "canary_interval_s": 600,
    "alerts": {"notify_send": True, "min_interval_s": 1800},
    "remediation": {"enabled": True, "ollama_restart_after_failures": 3, "min_interval_s": 1800},
}


def deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items():
        out[k] = deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def load_cfg() -> dict:
    if CFG_PATH.exists():
        return deep_merge(DEFAULT_CFG, json.loads(CFG_PATH.read_text(encoding="utf-8")))
    return dict(DEFAULT_CFG)


CFG = load_cfg()
_tp = Path(os.path.expanduser(CFG["token_file"]))
TOKEN = load_token(str(_tp if _tp.is_absolute() else ROOT / _tp))
NONCES = NonceCache()
LOCAL_IPS = {"127.0.0.1", "::1"}
PEER_IP = CFG["peer"]["ip"]


def log(msg: str) -> None:
    sys.stderr.write(f"[{now_iso()}] {msg}\n")
    sys.stderr.flush()


# ============================================================ state
class State:
    def __init__(self):
        self.lock = threading.RLock()
        self.tasks: dict[str, dict] = {}
        self.task_events: dict[str, threading.Event] = {}
        self.task_q: "queue.Queue[str]" = queue.Queue()
        self.running: str | None = None
        self.mini_jobs: deque = deque()
        self.mini_cv = threading.Condition(self.lock)
        self.job_results: dict[str, dict] = {}
        self.job_events: dict[str, threading.Event] = {}
        self.jobs_taken: set[str] = set()
        self.alerts_for_mini: list[dict] = []
        self.alert_seq = 0
        self.alert_last: dict[str, float] = {}
        self.recent: deque = deque(maxlen=500)     # (ts, status)
        self.peer = {"hb": None, "hb_at": 0.0, "state": "unknown", "direct": {}, "canary": {}, "fail_streak": 0,
                     "last_remediation": 0.0, "last_canary": 0.0}
        self.processor_beat = time.time()
        self.health_cache: dict = {}


S = State()
START_TIME = time.time()


# ============================================================ alerts
def alert(level: str, key: str, msg: str, about: str, notify_mini: bool = True) -> None:
    now = time.time()
    rec = {"ts": now_iso(), "from": CFG["node"], "about": about, "level": level, "key": key, "msg": msg}
    append_jsonl(ALERTS_JSONL, rec)
    log(f"ALERT {level} {key}: {msg}")
    api.emit(about if about in (CFG["node"], CFG["peer"]["name"]) else CFG["node"], "alert", msg, level, None, {"key": key})
    if notify_mini:
        api.flow("alert.to_peer", level != "crit")
    with S.lock:
        if notify_mini:
            S.alert_seq += 1
            S.alerts_for_mini.append({**rec, "seq": S.alert_seq})
            S.alerts_for_mini = S.alerts_for_mini[-200:]
        last = S.alert_last.get(key, 0)
        due = now - last >= CFG["alerts"]["min_interval_s"]
        if due:
            S.alert_last[key] = now
    if due and level in ("crit", "warn") and CFG["alerts"].get("notify_send"):
        try:
            subprocess.run(["notify-send", "-a", "LLM cluster", "-u", "critical" if level == "crit" else "normal",
                            f"LLM cluster: {about}", msg[:300]], timeout=5, capture_output=True)
            api.flow("alert.to_user", True)
        except (OSError, subprocess.TimeoutExpired):
            api.flow("alert.to_user", False)


# ============================================================ health
def nvidia_snapshot() -> dict:
    try:
        p = subprocess.run(["nvidia-smi", "--query-gpu=name,temperature.gpu,power.draw,utilization.gpu,memory.used,memory.total,pstate",
                            "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=5)
        f = [x.strip() for x in p.stdout.strip().split(",")]
        return {"name": f[0], "temp_c": float(f[1]), "power_w": float(f[2]), "util_pct": float(f[3]),
                "mem_used_mib": float(f[4]), "mem_total_mib": float(f[5]), "pstate": f[6]}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


def idle_hint() -> bool | None:
    try:
        sid = subprocess.run(["loginctl", "list-sessions", "--no-legend"], capture_output=True, text=True, timeout=3).stdout
        for line in sid.splitlines():
            parts = line.split()
            if len(parts) >= 4 and parts[2] == os.environ.get("USER", "") and parts[3].startswith("seat"):
                out = subprocess.run(["loginctl", "show-session", parts[0], "-p", "IdleHint", "--value"],
                                     capture_output=True, text=True, timeout=3).stdout.strip()
                return out == "yes"
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def local_health() -> dict:
    st_v, ver, lat = http_json(CFG["ollama"] + "/api/version", timeout=3)
    st_p, ps, _ = http_json(CFG["ollama"] + "/api/ps", timeout=3)
    now = time.time()
    with S.lock:
        window = [s for t, s in S.recent if now - t < 3600]
        fails = sum(1 for s in window if s in ("needs_human", "error"))
        proc_age = now - S.processor_beat
        proc_ok = S.processor_thread.is_alive() if hasattr(S, "processor_thread") else False
        h = {
            "node": CFG["node"], "service": "llm-cluster-manager", "version": VERSION, "ts": now_iso(),
            "ollama": {"up": st_v == 200, "version": ver.get("version"), "latency_ms": round(lat * 1000, 1),
                       "loaded": [m.get("name") for m in (ps.get("models") or [])] if st_p == 200 else None},
            "gpu": nvidia_snapshot(),
            "cpu": {"temp_c": read_coretemp_package()},
            "queue": {"tasks_pending": S.task_q.qsize(), "running": S.running, "mini_jobs_pending": len(S.mini_jobs)},
            "errors": {"window_s": 3600, "tasks": len(window), "failed": fails,
                       "rate": round(fails / len(window), 3) if window else 0.0},
            "processor": {"alive": proc_ok, "beat_age_s": round(proc_age, 1)},
            "user_idle": idle_hint(),
            "peer": peer_summary(),
        }
    h["ok"] = bool(h["ollama"]["up"] and proc_ok)
    h["problems"] = [p for p in (
        None if h["ollama"]["up"] else "desktop ollama down",
        None if proc_ok else "task processor dead",
        "gpu hot" if (h["gpu"].get("temp_c") or 0) >= 83 else None,
    ) if p]
    S.health_cache = h
    return h


def peer_summary() -> dict:
    p = S.peer
    hb = p["hb"] or {}
    age = time.time() - p["hb_at"] if p["hb_at"] else None
    return {"name": CFG["peer"]["name"], "state": p["state"],
            "heartbeat_age_s": round(age, 1) if age is not None else None,
            "agent_ok": bool(hb) and age is not None and age < CFG["peer"]["heartbeat_stale_s"],
            "busy": hb.get("busy"), "ollama_up": (hb.get("ollama") or {}).get("up"),
            "canary": hb.get("canary") or p["canary"], "direct": p["direct"],
            "temps": hb.get("temps"), "power_w": hb.get("power_w"), "errors": hb.get("errors")}


def mini_available() -> tuple[str, str]:
    """Return (path, reason): path in {'mini-agent','mini-direct','none'}."""
    ps = peer_summary()
    if ps["agent_ok"]:
        if ps["busy"]:
            return "none", f"mini busy ({ps['busy']})"
        if ps["ollama_up"] is False:
            return "none", "mini agent reports its ollama down"
        return "mini-agent", "heartbeat fresh"
    d = S.peer["direct"]
    if d.get("up") and not (S.peer["hb"] or {}).get("busy"):
        return "mini-direct", "agent stale; mini ollama reachable on wired :11434 (no energy data)"
    return "none", "mini agent stale and ollama unreachable"


# ============================================================ execution
def desktop_model(cls: dict) -> str:
    return CFG["models"]["desktop_code" if cls["type"] == "code" else "desktop_text"]


def mini_model(cls: dict, task: dict, round_no: int) -> str:
    m = CFG["models"]
    if cls["type"] == "code":
        return m["mini_code"]
    if round_no == 1 and cls["category"] in ("text-transform", "summarize") and len(task["text"]) < 800:
        return m["mini_fast"]
    return m["mini_main"]


def exec_desktop(model: str, msgs: list[dict]) -> dict:
    think = False if model.startswith("gemma4") else None
    r = run_measured(CFG["ollama"], model, msgs, "nvidia", options=CFG["desktop_options"],
                     keep_alive=CFG["keep_alive_desktop"], think=think, timeout=1200)
    r["node"], r["path"] = CFG["node"], "desktop"
    return r


def exec_mini_agent(model: str, msgs: list[dict], task_id: str) -> dict | None:
    jid = "j" + secrets.token_hex(5)
    job = {"id": jid, "kind": "generate", "task_id": task_id, "model": model, "messages": msgs,
           "options": CFG["mini_options"], "created": now_iso()}
    ev = threading.Event()
    with S.mini_cv:
        S.job_events[jid] = ev
        S.mini_jobs.append(job)
        S.mini_cv.notify_all()
    got = ev.wait(CFG["job_timeout_s"])
    with S.lock:
        S.job_events.pop(jid, None)
        if not got:
            try:
                S.mini_jobs.remove(job)
            except ValueError:
                pass
            taken = jid in S.jobs_taken
            S.jobs_taken.discard(jid)
            return {"ok": False, "error": f"mini job timeout after {CFG['job_timeout_s']}s (taken={taken})", "timeout": True}
        S.jobs_taken.discard(jid)
        res = S.job_results.pop(jid)
    res["node"], res["path"] = CFG["peer"]["name"], "mini-agent"
    return res


def exec_mini_direct(model: str, msgs: list[dict]) -> dict:
    r = run_measured(CFG["peer"]["ollama"], model, msgs, "none", options=CFG["mini_options"], timeout=600)
    r["node"], r["path"] = CFG["peer"]["name"], "mini-direct"
    return r


def execute(target: str, task: dict, cls: dict, msgs: list[dict], round_no: int, task_id: str) -> dict:
    failover = None
    if target == "mini":
        path, why = mini_available()
        model = mini_model(cls, task, round_no)
        if path == "mini-agent":
            r = exec_mini_agent(model, msgs, task_id)
            if r and r.get("ok"):
                return r
            failover = f"mini-agent failed: {(r or {}).get('error')}"
            path, why = mini_available()
            if path == "mini-agent":
                path = "mini-direct" if S.peer["direct"].get("up") else "none"
        if path == "mini-direct":
            r = exec_mini_direct(model, msgs)
            if r.get("ok"):
                r["failover"] = failover or why
                return r
            failover = (failover + "; " if failover else "") + f"mini-direct failed: {r.get('error')}"
        else:
            failover = (failover + "; " if failover else "") + why
        alert("warn", "failover-desktop", f"simple task {task_id} ran on desktop: {failover}", CFG["peer"]["name"], notify_mini=False)
    r = exec_desktop(desktop_model(cls), msgs)
    if failover:
        r["failover"] = failover
    return r


# ============================================================ pipeline
def preview(s: str, n: int = 400) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[:n] + f"… [{len(s)} chars]"


def run_task(tid: str) -> None:
    rec = S.tasks[tid]
    task = rec["task"]
    t0 = time.time()
    rec["ts_start"] = now_iso()
    cls = checker.classify(task)
    if task.get("classify") == "llm":
        cls = llm_classify(task, cls)
    route = task.get("route") or "auto"
    initial = route if route in ("mini", "desktop") else ("mini" if cls["complexity"] == "simple" else "desktop")
    rec.update(classification=cls, route_initial=initial, status="running")
    api.emit(CFG["node"], "task.route", f"{cls['complexity']}/{cls['category']} -> {initial} ({cls.get('reason')})", "info", tid)
    attempts: list[dict] = []
    prev, fixes, passed = None, None, False
    max_rounds = int(task.get("max_rounds") or (CFG["max_rounds"] if initial == "mini" else CFG["desktop_rounds"]))

    def one(n: int, phase: str, target: str) -> bool:
        nonlocal prev, fixes
        S.processor_beat = time.time()
        msgs = checker.build_messages(task, cls, prev, fixes)
        tnode = CFG["peer"]["name"] if target == "mini" else CFG["node"]
        tmodel = mini_model(cls, task, n) if target == "mini" else desktop_model(cls)
        api.set_current(tnode, {"task_id": tid, "attempt": len(attempts) + 1, "phase": phase, "activity": "generating", "model": tmodel})
        api.emit(tnode, "attempt.start", f"attempt {len(attempts) + 1} {phase} -> {tmodel}", "info", tid)
        try:
            ex = execute(target, task, cls, msgs, n, tid)
        finally:
            api.set_current(tnode, None)
        S.processor_beat = time.time()
        out = ex.get("content") or ""
        if ex.get("ok"):
            api.set_current(CFG["node"], {"task_id": tid, "attempt": len(attempts) + 1, "phase": phase, "activity": "checking",
                                          "model": CFG["models"]["checker"]})
            try:
                v = checker.check(task, cls, out, CFG["ollama"], CFG["models"]["checker"], CFG["keep_alive_desktop"], "nvidia")
            finally:
                api.set_current(CFG["node"], None)
        else:
            v = {"pass": False, "method": "exec-error", "issues": [f"execution error: {ex.get('error')}"], "check_s": 0}
        att = {"n": len(attempts) + 1, "phase": phase, "target": target, "node": ex.get("node"), "path": ex.get("path"),
               "model": ex.get("model"), "failover": ex.get("failover"), "ok": ex.get("ok"), "error": ex.get("error"),
               "wall_s": round(ex.get("wall_s") or 0, 2), "load_s": round(ex.get("load_s") or 0, 2),
               "eval_tokens": ex.get("eval_count"), "tok_s": ex.get("tok_s"), "decode_tok_s": ex.get("decode_tok_s"),
               "energy_j": ex.get("energy_j"), "avg_w": ex.get("avg_w"), "j_per_tok": ex.get("j_per_tok"),
               "score": ex.get("score"), "power_source": ex.get("power_source"),
               "fix_requests_in": fixes, "output_preview": preview(out),
               "verdict": {k: v.get(k) for k in ("pass", "method", "score", "issues", "notes", "check_s", "check_energy_j", "checker_model") if k in v}}
        attempts.append(att)
        rec["attempts"] = attempts
        log(f"{tid} attempt {att['n']} {phase} {att['path']} {att['model']} -> pass={v.get('pass')} ({v.get('method')})")
        api.on_attempt(tid, att)
        prev, fixes = out, v.get("issues") or ["Improve the answer."]
        rec["final_output"] = out
        return bool(v.get("pass"))

    for n in range(1, max_rounds + 1):
        if one(n, "worker", initial):
            passed = True
            break
    if not passed and initial == "mini":
        rec["escalated"] = True
        api.emit(CFG["node"], "task.escalate", f"mini failed {len(attempts)} rounds; escalating to {desktop_model(cls)}", "warn", tid)
        for n in range(1, CFG["desktop_rounds"] + 1):
            if one(n, "escalation", "desktop"):
                passed = True
                break
    status = "passed" if passed else "needs_human"
    worker_j = sum(a["energy_j"] or 0 for a in attempts)
    check_j = sum((a["verdict"].get("check_energy_j") or 0) for a in attempts)
    rec.update(
        status=status, ts_end=now_iso(), rounds_total=len(attempts),
        route_final=attempts[-1]["path"] if attempts else None,
        first_try_pass=bool(attempts and attempts[0]["verdict"].get("pass")),
        timings={"total_s": round(time.time() - t0, 2),
                 "worker_s": round(sum(a["wall_s"] for a in attempts), 2),
                 "check_s": round(sum(a["verdict"].get("check_s") or 0 for a in attempts), 2)},
        energy={"worker_j": round(worker_j, 1), "check_j": round(check_j, 1), "total_j": round(worker_j + check_j, 1),
                "note": "mini = APU package PPT (CPU+GPU); desktop = GPU board only (RAPL needs root)"},
    )
    api.emit(CFG["node"], "task.done", f"{status.upper()} route {initial} -> {rec['route_final']} rounds {len(attempts)} "
             f"{rec['timings']['total_s']}s {rec['energy']['total_j']} J", "info" if passed else "warn", tid)
    finish(tid, rec)


def llm_classify(task: dict, base: dict) -> dict:
    prompt = ("Classify this task for a two-node LLM cluster. 'simple' = a 3B-7B model can do it reliably "
              "(short transforms, small functions, factual Q&A). 'complex' = needs a stronger model (design, long reasoning, "
              "multi-step code). Return ONLY JSON {\"complexity\":\"simple|complex\",\"reason\":\"...\"}.\n\nTASK:\n" + task["text"][:4000])
    r = ollama_chat(CFG["ollama"], CFG["models"]["checker"], [{"role": "user", "content": prompt}],
                    options={"temperature": 0}, keep_alive=CFG["keep_alive_desktop"], think=False, fmt="json", timeout=300)
    try:
        v = json.loads(r.get("content") or "{}")
        if v.get("complexity") in ("simple", "complex"):
            return {**base, "complexity": v["complexity"], "reason": "llm: " + str(v.get("reason"))[:200], "method": "llm"}
    except json.JSONDecodeError:
        pass
    return base


def finish(tid: str, rec: dict) -> None:
    entry = {"schema": SCHEMA_TASK, **{k: v for k, v in rec.items() if k != "task"},
             "task": {k: rec["task"].get(k) for k in ("text", "type", "route", "complexity", "hidden_tests", "expect", "expect_exact", "source")}
             | {"tests_present": bool(rec["task"].get("tests"))}}
    append_jsonl(TASKS_JSONL, entry)
    TASKS_MD.mkdir(parents=True, exist_ok=True)
    (TASKS_MD / f"{tid}.md").write_text(render_md(entry), encoding="utf-8")
    if rec["status"] == "needs_human":
        with NEEDS_HUMAN_MD.open("a", encoding="utf-8") as f:
            f.write(f"- {now_iso()} `{tid}` — {preview(rec['task']['text'], 160)} — see journal/tasks/{tid}.md\n")
        alert("warn", f"needs-human-{tid}", f"task {tid} failed all rounds; flagged for a human", CFG["node"], notify_mini=False)
    with S.lock:
        S.recent.append((time.time(), rec["status"]))
    try:
        write_findings()
    except Exception as e:  # noqa: BLE001
        log(f"findings write failed: {e}")
    ev = S.task_events.get(tid)
    if ev:
        ev.set()


def render_md(e: dict) -> str:
    c = e.get("classification") or {}
    L = [f"# Task {e['id']} — {e['status'].upper()}", "",
         f"- **Submitted:** {e.get('ts_submit')} via `{e['task'].get('source')}`; finished {e.get('ts_end')}",
         f"- **Classification:** {c.get('complexity')} / {c.get('category')} / {c.get('type')} ({c.get('method')}: {c.get('reason')})",
         f"- **Route:** initial `{e.get('route_initial')}` → final `{e.get('route_final')}`; rounds {e.get('rounds_total')}; "
         f"first-try pass {e.get('first_try_pass')}; escalated {bool(e.get('escalated'))}",
         f"- **Timings:** total {e['timings']['total_s']} s (worker {e['timings']['worker_s']} s, checks {e['timings']['check_s']} s)",
         f"- **Energy:** worker {e['energy']['worker_j']} J, checks {e['energy']['check_j']} J ({e['energy']['note']})",
         "", "## Task", "", "```", e["task"]["text"].strip(), "```", ""]
    if e["task"].get("tests_present"):
        L.append(f"Tests provided ({'hidden from worker' if e['task'].get('hidden_tests') else 'shown to worker'}).\n")
    L += ["## Attempts", "", "| # | phase | path | model | pass | check | wall s | tok/s | J | J/tok | score |",
          "|---|---|---|---|---|---|---|---|---|---|---|"]
    for a in e.get("attempts") or []:
        v = a["verdict"]
        L.append(f"| {a['n']} | {a['phase']} | {a['path']} | {a['model']} | {'✅' if v.get('pass') else '❌'} | {v.get('method')}"
                 f"{(' ' + str(v.get('score'))) if v.get('score') is not None else ''} | {a['wall_s']} | {a['tok_s']} | {a['energy_j']} | {a['j_per_tok']} | {a['score']} |")
    L.append("")
    for a in e.get("attempts") or []:
        v = a["verdict"]
        L.append(f"### Attempt {a['n']} ({a['path']}, {a['model']})")
        if a.get("failover"):
            L.append(f"- Failover: {a['failover']}")
        if a.get("fix_requests_in"):
            L.append("- Fix requests sent: " + " / ".join(a["fix_requests_in"]))
        if v.get("issues"):
            L.append("- Checker issues: " + " / ".join(v["issues"]))
        L += ["", "```", a["output_preview"], "```", ""]
    L += ["## Final output", "", "```", (e.get("final_output") or "").strip()[:6000], "```", ""]
    return "\n".join(L)


def write_findings() -> None:
    rows = read_jsonl(TASKS_JSONL)
    # Tasks invalidated by a known checker bug etc. are listed one id per line in state/findings_exclude.txt
    ex_file = STATE / "findings_exclude.txt"
    excluded = set(ex_file.read_text().split()) if ex_file.exists() else set()
    by: dict[tuple, dict] = {}
    for r in rows:
        if r.get("id") in excluded:
            continue
        atts = r.get("attempts") or []
        if not atts:
            continue
        cat = (r.get("classification") or {}).get("category")
        first = atts[0]
        key = (first.get("path"), first.get("model"), cat)
        b = by.setdefault(key, {"n": 0, "first": 0, "within": 0, "esc": 0, "human": 0, "rounds": 0, "tok_s": [], "jpt": [], "score": []})
        b["n"] += 1
        b["first"] += 1 if first["verdict"].get("pass") else 0
        worker_atts = [a for a in atts if a["phase"] == "worker"]
        b["within"] += 1 if any(a["verdict"].get("pass") for a in worker_atts) else 0
        b["esc"] += 1 if r.get("escalated") else 0
        b["human"] += 1 if r.get("status") == "needs_human" else 0
        b["rounds"] += len(worker_atts)
        for a in worker_atts:
            for k, lst in (("tok_s", b["tok_s"]), ("j_per_tok", b["jpt"]), ("score", b["score"])):
                if a.get(k):
                    lst.append(a[k])
    avg = lambda l: round(sum(l) / len(l), 2) if l else None
    L = ["# Pipeline findings (auto-generated)", "",
         f"Updated {now_iso()} from `journal/tasks.jsonl` ({len(rows)} tasks). Score = tok/s ÷ J/token "
         "(mini: APU PPT; desktop: GPU board only).", "",
         "| first path | model | category | tasks | first-try pass | pass within worker rounds | escalated | needs human | avg rounds | avg tok/s | avg J/tok | avg score |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    stats = []
    for (path, model, cat), b in sorted(by.items(), key=lambda kv: (str(kv[0][0]), str(kv[0][2]))):
        L.append(f"| {path} | {model} | {cat} | {b['n']} | {b['first']}/{b['n']} | {b['within']}/{b['n']} | {b['esc']} | {b['human']} | "
                 f"{round(b['rounds'] / b['n'], 2)} | {avg(b['tok_s'])} | {avg(b['jpt'])} | {avg(b['score'])} |")
        stats.append({"path": path, "model": model, "category": cat, "tasks": b["n"], "first_try": b["first"],
                      "within_rounds": b["within"], "escalated": b["esc"], "needs_human": b["human"],
                      "avg_rounds": round(b["rounds"] / b["n"], 2), "avg_tok_s": avg(b["tok_s"]),
                      "avg_j_per_tok": avg(b["jpt"]), "avg_score": avg(b["score"])})
    L += ["", "Reliability rule of thumb: a category is 'mini-reliable' once it has ≥10 tasks with ≥80% pass within worker rounds.", ""]
    if excluded:
        L += [f"Excluded from stats (state/findings_exclude.txt): {', '.join(sorted(excluded))}", ""]
    try:
        L += api.changes_markdown_lines()
    except Exception as e:  # noqa: BLE001
        log(f"changes markdown failed: {e}")
    FINDINGS_MD.write_text("\n".join(L), encoding="utf-8")
    write_json_atomic(STATE / "pipeline_stats.json", {"updated": now_iso(), "tasks": len(rows), "rows": stats, "markdown": "\n".join(L)})


def processor_loop() -> None:
    while True:
        try:
            tid = S.task_q.get(timeout=30)
        except queue.Empty:
            S.processor_beat = time.time()
            continue
        S.running = tid
        S.processor_beat = time.time()
        try:
            run_task(tid)
        except Exception as e:  # noqa: BLE001
            rec = S.tasks[tid]
            rec.update(status="error", error=f"{type(e).__name__}: {e}", trace=traceback.format_exc()[-2000:], ts_end=now_iso(),
                       timings=rec.get("timings") or {"total_s": 0, "worker_s": 0, "check_s": 0},
                       energy=rec.get("energy") or {"worker_j": 0, "check_j": 0, "total_j": 0, "note": "error"})
            log(f"task {tid} crashed: {e}\n{traceback.format_exc()}")
            try:
                finish(tid, rec)
            except Exception:  # noqa: BLE001
                ev = S.task_events.get(tid)
                if ev:
                    ev.set()
        finally:
            S.running = None
            S.processor_beat = time.time()


def submit(task: dict, source: str) -> str:
    tid = time.strftime("t%Y%m%d-%H%M%S-") + secrets.token_hex(2)
    task = dict(task)
    task["source"] = source
    if not (task.get("text") or "").strip():
        raise ValueError("task text required")
    rec = {"id": tid, "ts_submit": now_iso(), "status": "queued", "task": task, "node": CFG["node"]}
    with S.lock:
        S.tasks[tid] = rec
        S.task_events[tid] = threading.Event()
    S.task_q.put(tid)
    api.emit(CFG["node"], "task.queued", f"queued from {source} (target {task.get('route') or 'auto'}): {preview(task['text'], 120)}", "info", tid)
    api.flow("task.submit", True, source=source)
    return tid


def public_task(rec: dict) -> dict:
    return {k: v for k, v in rec.items() if k not in ("trace",)}


# ============================================================ peer monitor + remediation
def poll_peer() -> None:
    pc = CFG["peer"]
    now = time.time()
    st, ver, lat = http_json(pc["ollama"] + "/api/version", timeout=4)
    d = {"up": st == 200, "version": ver.get("version") if st == 200 else None, "latency_ms": round(lat * 1000, 1), "at": now_iso()}
    if st == 200:
        st2, ps, _ = http_json(pc["ollama"] + "/api/ps", timeout=4)
        d["loaded"] = [m.get("name") for m in (ps.get("models") or [])] if st2 == 200 else None
    S.peer["direct"] = d
    summ = peer_summary()
    # Canary from the desktop only when the mini agent is stale (otherwise the agent reports its own canary).
    if not summ["agent_ok"] and d["up"] and now - S.peer["last_canary"] > CFG["canary_interval_s"]:
        S.peer["last_canary"] = now
        r = ollama_chat(pc["ollama"], CFG["models"]["mini_fast"], [{"role": "user", "content": "Reply with exactly: OK"}],
                        options={"num_predict": 4, "temperature": 0}, keep_alive=0, timeout=60)
        S.peer["canary"] = {"ok": bool(r.get("ok")) and "ok" in (r.get("content") or "").lower(), "ms": round((r.get("wall_s") or 0) * 1000),
                            "model": CFG["models"]["mini_fast"], "by": CFG["node"], "at": now_iso()}
    summ = peer_summary()
    hb = S.peer["hb"] or {}
    if summ["agent_ok"] and summ["ollama_up"] and (summ["canary"] or {}).get("ok", True):
        state = "ok"
    elif summ["agent_ok"] or d["up"]:
        state = "degraded"
    else:
        state = "down"
    problems = []
    if not summ["agent_ok"]:
        problems.append("mini agent heartbeat stale")
    if not d["up"]:
        problems.append("mini ollama unreachable on wired :11434")
    if summ["agent_ok"] and summ["ollama_up"] is False:
        problems.append("mini agent reports ollama down")
    if (summ["canary"] or {}).get("ok") is False:
        problems.append("mini canary failed")
    temps = hb.get("temps") or {}
    if (temps.get("gpu_c") or 0) >= 85 or (temps.get("cpu_c") or 0) >= 90:
        problems.append(f"mini hot {temps}")
    errs = hb.get("errors") or {}
    if (errs.get("rate") or 0) > 0.3 and (errs.get("jobs") or 0) >= 3:
        problems.append(f"mini job error rate {errs.get('rate')}")
    S.peer["problems"] = problems
    prev = S.peer["state"]
    S.peer["state"] = state
    if state != prev:
        api.emit(pc["name"], "peer.state", f"{prev} -> {state}" + (f": {'; '.join(problems)}" if problems else ""),
                 "info" if prev == "unknown" else {"ok": "info", "degraded": "warn", "down": "crit"}[state])
    if state != prev and prev != "unknown":
        lvl = {"ok": "info", "degraded": "warn", "down": "crit"}[state]
        alert(lvl, f"peer-state-{state}", f"{pc['name']} {prev} -> {state}: {'; '.join(problems) or 'recovered'}", pc["name"])
    elif problems and state != "ok" and time.time() - START_TIME > 2 * CFG["peer"]["heartbeat_stale_s"]:
        alert("warn", "peer-problems", "; ".join(problems), pc["name"])
    # Remediation: agent alive but its ollama down/canary failing for N polls -> propose restart via mini rootgate.
    bad_ollama = summ["agent_ok"] and (summ["ollama_up"] is False or (summ["canary"] or {}).get("ok") is False)
    S.peer["fail_streak"] = S.peer["fail_streak"] + 1 if bad_ollama else 0
    rem = CFG["remediation"]
    if (rem["enabled"] and bad_ollama and S.peer["fail_streak"] >= rem["ollama_restart_after_failures"]
            and not hb.get("needs_human") and not hb.get("busy")
            and now - S.peer["last_remediation"] > rem["min_interval_s"]):
        S.peer["last_remediation"] = now
        alert("warn", "remediation", "proposing `systemctl restart ollama` to mini rootgate (desktop proposer)", pc["name"])
        threading.Thread(target=propose_root, args=(["systemctl", "restart", "ollama"],
                         f"desktop monitor: mini ollama unhealthy for {S.peer['fail_streak']} polls"), daemon=True).start()


def propose_root(argv: list[str], reason: str, wait_s: int = 180) -> dict:
    jid = "r" + secrets.token_hex(5)
    job = {"id": jid, "kind": "rootgate", "argv": argv, "reason": reason,
           "proposer": f"remote:{CFG['node']}/{CFG['models']['checker']}", "created": now_iso()}
    ev = threading.Event()
    with S.mini_cv:
        S.job_events[jid] = ev
        S.mini_jobs.append(job)
        S.mini_cv.notify_all()
    got = ev.wait(wait_s)
    with S.lock:
        S.job_events.pop(jid, None)
        S.jobs_taken.discard(jid)
        if not got:
            try:
                S.mini_jobs.remove(job)
            except ValueError:
                pass
            res = {"ok": False, "error": "mini agent did not pick up the rootgate job"}
        else:
            res = S.job_results.pop(jid)
    append_jsonl(JOURNAL / "remote_root.jsonl", {"ts": now_iso(), "job": job, "result": res})
    api.flow("tuning.rootgate", bool(res.get("ok")))
    api.emit(CFG["peer"]["name"], "rootgate", f"{' '.join(argv)} -> ok={res.get('ok')} ({reason})",
             "info" if res.get("ok") else "warn", None, {"argv": argv, "result": {k: res.get(k) for k in ("ok", "rc", "error")}})
    return res


def monitor_loop() -> None:
    while True:
        try:
            poll_peer()
            h = local_health()
            append_jsonl(HEALTH_JSONL, {"ts": h["ts"], "ok": h["ok"], "problems": h["problems"], "gpu": h["gpu"],
                                        "cpu": h["cpu"], "queue": h["queue"], "peer": {k: h["peer"][k] for k in ("state", "heartbeat_age_s", "busy")}})
            for p in h["problems"]:
                alert("warn", f"self-{p}", p, CFG["node"])
            api.sample()
        except Exception as e:  # noqa: BLE001
            log(f"monitor error: {e}\n{traceback.format_exc()}")
        time.sleep(CFG["poll_interval_s"])


# ============================================================ review endpoint (desktop as extra rootgate reviewer)
REVIEW = """You are an additional, independent safety reviewer for a root action on the worker node.
A local model there proposed this action; its local reviewer already approved it. Approve ONLY if it is safe, narrowly
scoped, reversible, matches the stated reason, and cannot break SSH, the firewall, boot, user data, or the network.
Reject anything that widens network exposure, disables security, deletes data, or installs software.
Proposer: {proposer}
Reason: {reason}
Command argv: {argv}
{extra}
Return ONLY JSON: {{"approve": true|false, "reason": "short"}}"""


def review(req: dict) -> dict:
    extra = ""
    if req.get("write_dest"):
        extra = f"Writes file {req['write_dest']} with content:\n{str(req.get('preview') or '')[:1500]}"
    msg = REVIEW.format(proposer=req.get("proposer"), reason=req.get("reason"), argv=json.dumps(req.get("argv")), extra=extra)
    r = ollama_chat(CFG["ollama"], CFG["models"]["checker"], [{"role": "user", "content": msg}],
                    options={"temperature": 0}, keep_alive=CFG["keep_alive_desktop"], think=False, fmt="json", timeout=300)
    out = {"reviewer": f"{CFG['node']}/{CFG['models']['checker']}", "approve": False, "reason": "reviewer error (fail closed)"}
    if r.get("ok"):
        try:
            v = json.loads(r["content"])
            out.update(approve=bool(v.get("approve")), reason=str(v.get("reason"))[:300])
        except json.JSONDecodeError:
            out["reason"] = "reviewer returned non-JSON (fail closed)"
    append_jsonl(JOURNAL / "reviews.jsonl", {"ts": now_iso(), "request": req, "verdict": out})
    return out


# ============================================================ HTTP
def openai_content(messages: list) -> str:
    for m in reversed(messages or []):
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, list):
                return "\n".join(p.get("text", "") for p in c if isinstance(p, dict))
            return str(c or "")
    return ""



# ----- onboard probe ping (LAN / tailnet, rate-limited, no token) -----
_ONBOARD_HITS: dict = {}  # ip -> [window_start, count, bytes]


def _onboard_probe_allowed(ip: str, n_bytes: int = 0) -> tuple[bool, str | None]:
    cfg = CFG.get("onboard") or {}
    if cfg.get("probe_open") is False:
        return False, "probe endpoint disabled"
    cidrs = cfg.get("probe_allow_cidrs") or ["192.168.0.0/16", "100.64.0.0/10", "127.0.0.0/8"]
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False, "bad client address"
    if ip in LOCAL_IPS or ip == PEER_IP:
        ok_net = True
    else:
        ok_net = any(addr in ipaddress.ip_network(c, strict=False) for c in cidrs)
    if not ok_net:
        return False, "source not allowed"
    now = time.time()
    rec = _ONBOARD_HITS.get(ip)
    if not rec or now - rec[0] >= 60:
        rec = [now, 0, 0]
    max_bytes = int(cfg.get("probe_max_bytes") or 1048576)
    if rec[1] >= 8 or rec[2] + n_bytes > max(2 * max_bytes, 2 * 1048576):
        return False, "rate limited"
    rec[1] += 1
    rec[2] += n_bytes
    _ONBOARD_HITS[ip] = rec
    return True, None


class Handler(BaseHTTPRequestHandler):
    server_version = f"llm-cluster-manager/{VERSION}"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        if "/cluster/jobs/next" in (self.path or "") or "/cluster/heartbeat" in (self.path or ""):
            return
        log(f"{self.client_address[0]} {fmt % args}")

    # ---- helpers
    def _body(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _send(self, status: int, obj, ctype="application/json"):
        raw = obj if isinstance(obj, bytes) else json.dumps(obj, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _ip(self) -> str:
        return self.client_address[0]

    def _local(self) -> bool:
        return self._ip() in LOCAL_IPS

    def _peer_auth(self, body: bytes) -> bool:
        if self._ip() not in LOCAL_IPS | {PEER_IP}:
            self._send(403, {"error": "source not allowed"})
            return False
        ok, why = verify_headers(TOKEN, self.command, self.path, body, self.headers, NONCES)
        if not ok:
            self._send(401, {"error": f"auth: {why}"})
            return False
        return True

    def _need_local(self) -> bool:
        if not self._local():
            self._send(403, {"error": "localhost only"})
            return False
        return True

    # ---- GET
    def do_GET(self):
        sp = urlsplit(self.path)
        path, q = sp.path, parse_qs(sp.query)
        if path == "/api" or path.startswith("/api/"):
            return api.handle(self, "GET", path, q, b"")
        if path in ("/health", "/"):
            if not (self._local() or self._ip() == PEER_IP):
                return self._send(403, {"error": "source not allowed"})
            if self._ip() == PEER_IP:
                api.flow("monitor.peer_poll", True, ((S.peer["hb"] or {}).get("peer_latency_ms")))
            return self._send(200, local_health())
        if path == "/v1/models":
            if not self._need_local():
                return
            return self._send(200, {"object": "list", "data": [{"id": m, "object": "model", "owned_by": "llm-cluster"}
                                                               for m in ("cluster", "cluster-mini", "cluster-desktop")]})
        if path.startswith("/task/"):
            if not self._need_local():
                return
            rec = S.tasks.get(path.split("/")[2])
            if not rec:
                for r in reversed(read_jsonl(TASKS_JSONL)):
                    if r.get("id") == path.split("/")[2]:
                        rec = r
                        break
            return self._send(200 if rec else 404, public_task(rec) if rec else {"error": "unknown task"})
        if path == "/tasks":
            if not self._need_local():
                return
            n = int((q.get("n") or ["20"])[0])
            return self._send(200, {"tasks": read_jsonl(TASKS_JSONL, n), "live": [public_task(r) for r in S.tasks.values() if r["status"] in ("queued", "running")]})
        if path == "/alerts":
            if not self._need_local():
                return
            return self._send(200, {"alerts": read_jsonl(ALERTS_JSONL, int((q.get("n") or ["30"])[0]))})
        if path == "/cluster/stats":
            if not (self._local() or self._peer_auth(b"")):
                return
            p = STATE / "pipeline_stats.json"
            return self._send(200, json.loads(p.read_text()) if p.exists() else {"tasks": 0, "rows": []})
        if path == "/cluster/alerts":
            if not self._peer_auth(b""):
                return
            since = int((q.get("since") or ["0"])[0])
            with S.lock:
                return self._send(200, {"alerts": [a for a in S.alerts_for_mini if a["seq"] > since], "seq": S.alert_seq})
        if path == "/cluster/jobs/next":
            if not self._peer_auth(b""):
                return
            wait = min(float((q.get("wait") or ["25"])[0]), 30)
            deadline = time.time() + wait
            with S.mini_cv:
                while not S.mini_jobs and time.time() < deadline:
                    S.mini_cv.wait(timeout=max(0.1, deadline - time.time()))
                if not S.mini_jobs:
                    self.send_response(204)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                job = S.mini_jobs.popleft()
                S.jobs_taken.add(job["id"])
            if job.get("kind") == "generate":
                api.flow("task.dispatch", True)
            api.emit(CFG["peer"]["name"], "job.taken", f"{job.get('kind')} job {job['id']} picked up"
                     + (f" ({job.get('model')})" if job.get("model") else f" ({' '.join(job.get('argv') or [])})"), "info", job.get("task_id"))
            return self._send(200, job)
        if path == "/onboard/ping":
            try:
                want = int((q.get("bytes") or ["0"])[0] or 0)
            except ValueError:
                want = 0
            cap = int((CFG.get("onboard") or {}).get("probe_max_bytes") or 1048576)
            want = max(0, min(want, cap, 1048576))
            ok, why = _onboard_probe_allowed(self._ip(), want)
            if not ok:
                return self._send(403, {"error": why or "forbidden"})
            if want:
                raw = b"\0" * want
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(raw)
                return
            return self._send(200, {"ok": True, "node": CFG["node"], "ts": now_iso(), "service": "llm-cluster-manager"})
        return self._send(404, {"error": "not found"})

    # ---- POST
    def do_POST(self):
        sp = urlsplit(self.path)
        path = sp.path
        body = self._body()
        if path == "/api" or path.startswith("/api/"):
            return api.handle(self, "POST", path, parse_qs(sp.query), body)
        try:
            data = json.loads(body.decode() or "{}")
        except json.JSONDecodeError:
            return self._send(400, {"error": "invalid json"})

        if path == "/task":
            if not self._need_local():
                return
            try:
                tid = submit(data, data.get("source") or "api")
            except ValueError as e:
                return self._send(400, {"error": str(e)})
            if data.get("wait"):
                S.task_events[tid].wait(float(data.get("timeout") or 3600))
            return self._send(200, public_task(S.tasks[tid]))

        if path in ("/v1/chat/completions", "/chat/completions"):
            if not self._need_local():
                return
            model = data.get("model") or "cluster"
            route = {"cluster-mini": "mini", "cluster-desktop": "desktop"}.get(model, "auto")
            text = openai_content(data.get("messages") or [])
            try:
                tid = submit({"text": text, "type": "auto", "route": route}, "openai")
            except ValueError as e:
                return self._send(400, {"error": {"message": str(e)}})
            S.task_events[tid].wait(3600)
            rec = S.tasks[tid]
            content = rec.get("final_output") or ""
            meta = {"task_id": tid, "status": rec.get("status"), "route_final": rec.get("route_final"), "rounds": rec.get("rounds_total")}
            if data.get("stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                chunk = {"id": tid, "object": "chat.completion.chunk", "created": int(time.time()), "model": model,
                         "choices": [{"index": 0, "delta": {"role": "assistant", "content": content}, "finish_reason": None}]}
                end = {"id": tid, "object": "chat.completion.chunk", "created": int(time.time()), "model": model,
                       "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}], "cluster": meta}
                self.wfile.write(b"data: " + json.dumps(chunk).encode() + b"\n\n")
                self.wfile.write(b"data: " + json.dumps(end).encode() + b"\n\ndata: [DONE]\n\n")
                self.close_connection = True
                return
            return self._send(200, {"id": tid, "object": "chat.completion", "created": int(time.time()), "model": model,
                                    "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
                                    "usage": {}, "cluster": meta})

        if path == "/cluster/rootgate/propose":
            if not self._need_local():
                return
            argv = data.get("argv")
            if not isinstance(argv, list) or not argv:
                return self._send(400, {"error": "argv list required"})
            return self._send(200, propose_root([str(a) for a in argv], str(data.get("reason") or "manual desktop proposal")))

        # ---- peer (mini) endpoints: HMAC + source IP
        if path == "/cluster/heartbeat":
            if not self._peer_auth(body):
                return
            prev_age = time.time() - S.peer["hb_at"] if S.peer["hb_at"] else None
            S.peer["hb"], S.peer["hb_at"] = data, time.time()
            api.flow("monitor.heartbeat", True, data.get("peer_latency_ms"))
            if prev_age and prev_age > CFG["peer"]["heartbeat_stale_s"]:
                alert("info", "peer-heartbeat-back", f"mini heartbeat resumed after {prev_age:.0f}s", CFG["peer"]["name"])
                api.emit(CFG["peer"]["name"], "heartbeat.resumed", f"heartbeat resumed after {prev_age:.0f}s")
            return self._send(200, {"ok": True, "manager": CFG["node"], "alert_seq": S.alert_seq})
        if path.startswith("/cluster/jobs/") and path.endswith("/result"):
            if not self._peer_auth(body):
                return
            jid = path.split("/")[3]
            with S.lock:
                ev = S.job_events.get(jid)
                if not ev:
                    return self._send(410, {"error": "job expired"})
                S.job_results[jid] = data
                ev.set()
            return self._send(200, {"ok": True})
        if path == "/cluster/alert":
            if not self._peer_auth(body):
                return
            rec = {"ts": now_iso(), "from": data.get("from") or CFG["peer"]["name"], "about": data.get("about"),
                   "level": data.get("level") or "info", "key": data.get("key"), "msg": str(data.get("msg"))[:1000], "via": "peer"}
            append_jsonl(ALERTS_JSONL, rec)
            api.flow("alert.to_me", True)
            api.emit(rec["from"] if rec["from"] in (CFG["node"], CFG["peer"]["name"]) else CFG["peer"]["name"], "alert",
                     f"[{rec['from']}] {rec['msg']}", rec["level"], None, {"key": rec["key"], "about": rec["about"]})
            if rec["level"] in ("warn", "crit") and CFG["alerts"].get("notify_send"):
                k = "peer-" + str(rec["key"])
                if time.time() - S.alert_last.get(k, 0) > CFG["alerts"]["min_interval_s"]:
                    S.alert_last[k] = time.time()
                    try:
                        subprocess.run(["notify-send", "-a", "LLM cluster", f"LLM cluster ({rec['from']})", rec["msg"][:300]],
                                       timeout=5, capture_output=True)
                    except (OSError, subprocess.TimeoutExpired):
                        pass
            return self._send(200, {"ok": True})
        if path == "/cluster/review":
            if not self._peer_auth(body):
                return
            t0 = time.time()
            rv = review(data)
            api.flow("review.rootgate", True, (time.time() - t0) * 1000, approved=int(rv.get("approve")), denied=int(not rv.get("approve")))
            api.emit(CFG["node"], "rootgate", f"second review for mini: {' '.join(map(str, data.get('argv') or []))} -> "
                     f"{'APPROVE' if rv.get('approve') else 'DENY'} ({rv.get('reason')})", "info")
            return self._send(200, rv)
        if path == "/cluster/restart-request":
            if not self._peer_auth(body):
                return
            STATE.mkdir(parents=True, exist_ok=True)
            api.flow("tuning.restart_request", True)
            api.emit(CFG["node"], "service", f"restart requested by {data.get('from')}: {data.get('reason')}", "warn")
            RESTART_FLAG.write_text(json.dumps({"ts": now_iso(), "from": data.get("from"), "reason": data.get("reason")}))
            append_jsonl(ALERTS_JSONL, {"ts": now_iso(), "from": data.get("from"), "about": CFG["node"], "level": "warn",
                                        "key": "restart-request", "msg": f"restart of llm-cluster-manager requested: {data.get('reason')}"})
            return self._send(202, {"ok": True, "note": "llm-cluster-selfcheck.timer will restart the user service"})
        return self._send(404, {"error": "not found"})


def serve(addr: str, port: int) -> None:
    while True:
        try:
            srv = ThreadingHTTPServer((addr, port), Handler)
            srv.daemon_threads = True
            log(f"listening on {addr}:{port}")
            srv.serve_forever()
        except OSError as e:
            log(f"bind {addr}:{port} failed ({e}); retrying in 60s")
            time.sleep(60)


def main() -> None:
    for d in (JOURNAL, TASKS_MD, STATE):
        d.mkdir(parents=True, exist_ok=True)
    RESTART_FLAG.unlink(missing_ok=True)
    api.setup(globals())
    S.processor_thread = threading.Thread(target=processor_loop, name="processor", daemon=True)
    S.processor_thread.start()
    threading.Thread(target=monitor_loop, name="monitor", daemon=True).start()
    for addr in CFG["bind"][1:]:
        threading.Thread(target=serve, args=(addr, CFG["port"]), daemon=True).start()
    serve(CFG["bind"][0], CFG["port"])


if __name__ == "__main__":
    main()
