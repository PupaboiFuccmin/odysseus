#!/usr/bin/env python3
"""llm-cluster <-> Odysseus bridge (user service, default port 11610).

Odysseus -> cluster : MCP server at /mcp (streamable HTTP) + OpenAI-compatible /v1 (models: cluster*, mini/<m>, desktop/<m>, cluster-agent[/<m>]).
Cluster  -> Odysseus: REST tool endpoints /tools/{search,memory/store,memory/query,notify} backed by Odysseus's SearXNG, ChromaDB, ntfy.
Tool-calling runner : /agent/run runs a model on the mini (or desktop) via Ollama /api/chat with tools; the tools execute HERE on the
                      desktop, so the mini needs no new port and never talks to Odysseus services directly.
Poller              : manager alerts / failed tasks -> ntfy; passed tasks -> ChromaDB collection llm_cluster_tasks.
Binds 127.0.0.1 + 172.17.0.1 (docker0) only. Non-loopback callers must be in allow_cidrs AND present the bridge token.
See contrib/cluster-network/README.md.
"""
from __future__ import annotations

import asyncio, hashlib, hmac, ipaddress, json, logging, os, re, socket, sys, time, uuid
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse, Response

VERSION = "1.1.0"
HOME = Path(os.path.expanduser(os.environ.get("LLM_CLUSTER_HOME") or str(Path(__file__).resolve().parents[1])))
BASE = HOME / "bridge"
CFG = json.loads((BASE / "bridge.json").read_text())
_tf = Path(os.path.expanduser(CFG["token_file"]))
TOKEN = (_tf if _tf.is_absolute() else HOME / _tf).read_text().strip()
MGR_TOKEN_FILE = HOME / "secret" / "api.token"
ALLOW = [ipaddress.ip_network(c) for c in CFG.get("allow_cidrs", [])]
STATE_FILE = BASE / "state" / "poller.json"
RUNS = BASE / "journal" / "runs.jsonl"
EVENTS = BASE / "journal" / "events.jsonl"
log = logging.getLogger("bridge")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)

http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=900.0))
_coll_ids: dict[str, str] = {}


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def jlog(path: Path, rec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def event(kind: str, msg: str, **data) -> None:
    jlog(EVENTS, {"ts": now_iso(), "kind": kind, "msg": msg, "data": data or None})
    log.info("%s %s", kind, msg)

# --------------------------------------------------------------------------- Odysseus service wrappers


def _clean_html(v) -> str:
    """SearXNG titles/snippets sometimes carry stray markup (e.g. '</a>')."""
    import html as _h
    return re.sub(r"\s+", " ", _h.unescape(re.sub(r"<[^>]*>", "", str(v or "")))).strip()

async def web_search(query: str, n: int = 5, categories: str = "general") -> list[dict]:
    r = await http.get(f"{CFG['searxng']}/search", params={"q": query, "format": "json", "categories": categories}, timeout=20)
    r.raise_for_status()
    out = []
    for it in r.json().get("results", [])[: max(1, min(int(n), 20))]:
        out.append({"title": _clean_html(it.get("title")), "url": it.get("url"), "snippet": _clean_html(it.get("content"))[:400],
                    "engine": it.get("engine")})
    return out


async def embed(texts: list[str], kind: str) -> list[list[float]]:
    pre = "search_query: " if kind == "query" else "search_document: "
    e = CFG["embed"]
    r = await http.post(f"{e['url']}/api/embed", json={"model": e["model"], "input": [pre + t[:8000] for t in texts],
                                                       "keep_alive": e.get("keep_alive", "30s")}, timeout=120)
    r.raise_for_status()
    return r.json()["embeddings"]


def _cpath(suffix: str = "") -> str:
    return f"{CFG['chromadb']}/api/v2/tenants/default_tenant/databases/default_database/collections{suffix}"


async def collection_id(name: str) -> str:
    if name not in _coll_ids:
        r = await http.post(_cpath(), json={"name": name, "get_or_create": True,
                                            "metadata": {"hnsw:space": "cosine", "owner": "llm-cluster",
                                                         "embedding_model": CFG["embed"]["model"]}})
        r.raise_for_status()
        _coll_ids[name] = r.json()["id"]
    return _coll_ids[name]


def _coll(which: str | None) -> str:
    cols = CFG["collections"]
    return cols.get(which or "memory", which or cols["memory"])


async def memory_store(text: str, tags: list[str] | None = None, source: str = "cluster", collection: str | None = None,
                       meta: dict | None = None, doc_id: str | None = None) -> dict:
    name = _coll(collection)
    cid = await collection_id(name)
    did = doc_id or ("m" + hashlib.sha1(text.encode()).hexdigest()[:16])
    md = {"source": source, "tags": ",".join(tags or []), "ts": now_iso()}
    for k, v in (meta or {}).items():
        if isinstance(v, (str, int, float, bool)):
            md[k] = v
        elif v is not None:
            md[k] = json.dumps(v)[:500]
    emb = (await embed([text], "doc"))[0]
    r = await http.post(_cpath(f"/{cid}/upsert"), json={"ids": [did], "embeddings": [emb], "documents": [text], "metadatas": [md]})
    r.raise_for_status()
    return {"id": did, "collection": name, "stored_chars": len(text)}


async def memory_query(query: str, n: int = 5, collection: str | None = None) -> list[dict]:
    name = _coll(collection)
    cid = await collection_id(name)
    emb = (await embed([query], "query"))[0]
    r = await http.post(_cpath(f"/{cid}/query"), json={"query_embeddings": [emb], "n_results": max(1, min(int(n), 20)),
                                                       "include": ["documents", "metadatas", "distances"]})
    r.raise_for_status()
    d = r.json()
    out = []
    for i, doc_id in enumerate((d.get("ids") or [[]])[0]):
        out.append({"id": doc_id, "collection": name, "distance": round(d["distances"][0][i], 4),
                    "text": (d["documents"][0][i] or "")[:2000], "meta": d["metadatas"][0][i]})
    return out


async def notify(message: str, title: str = "llm-cluster", priority: int = 3, tags: list[str] | None = None,
                 topic: str | None = None) -> dict:
    headers = {"Title": title.encode("ascii", "replace").decode(), "Priority": str(max(1, min(int(priority), 5)))}
    if tags:
        headers["Tags"] = ",".join(tags)
    t = topic or CFG["ntfy_topic"]
    r = await http.post(f"{CFG['ntfy']}/{t}", content=message.encode(), headers=headers, timeout=10)
    r.raise_for_status()
    j = r.json()
    return {"id": j.get("id"), "topic": t, "time": j.get("time")}

# --------------------------------------------------------------------------- manager client

def _mgr_headers() -> dict:
    return {}  # bridge reaches the manager on loopback, which needs no token


async def mgr(method: str, path: str, body: dict | None = None, timeout: float = 30) -> Any:
    r = await http.request(method, CFG["manager"] + path, json=body, headers=_mgr_headers(), timeout=timeout)
    try:
        j = r.json()
    except Exception:
        j = {"error": r.text[:500]}
    if r.status_code >= 400:
        raise RuntimeError(f"manager {method} {path} -> {r.status_code}: {j}")
    return j


def node_brief(n: dict) -> dict:
    ol = n.get("ollama") or {}
    return {"id": n.get("id"), "role": n.get("role"), "status": n.get("status"), "problems": n.get("status_detail"),
            "job": n.get("job"), "ip": (n.get("ips") or {}).get("wired"), "power_w": (n.get("power") or {}).get("w"),
            "temps": n.get("temps"), "busy": n.get("busy"), "current_task": n.get("current_task"),
            "queue": (n.get("queue") or {}).get("depth"), "ollama_up": ol.get("up"),
            "models_installed": [m.get("name") for m in ol.get("installed") or []],
            "models_loaded": [m.get("name") for m in ol.get("loaded") or []]}

# --------------------------------------------------------------------------- tool-calling runner (models on mini/desktop, tools here)

AGENT_TOOLS = {
    "web_search": {"description": "Search the web (SearXNG). Returns titles, urls, snippets.",
                   "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "n": {"type": "integer"}}, "required": ["query"]}},
    "memory_query": {"description": "Search the cluster's long-term memory (ChromaDB) for related notes, findings and past task results.",
                     "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "n": {"type": "integer"},
                                    "collection": {"type": "string", "enum": ["memory", "tasks"]}}, "required": ["query"]}},
    "memory_store": {"description": "Save a note or finding to the cluster's long-term memory (ChromaDB).",
                     "parameters": {"type": "object", "properties": {"text": {"type": "string"}, "tags": {"type": "array", "items": {"type": "string"}}}, "required": ["text"]}},
    "notify": {"description": "Send a push notification via ntfy. Use sparingly: only for results the operator asked to be told about or problems.",
               "parameters": {"type": "object", "properties": {"message": {"type": "string"}, "title": {"type": "string"}, "priority": {"type": "integer"}}, "required": ["message"]}},
}


async def run_tool(name: str, args: dict, run_id: str) -> Any:
    if name == "web_search":
        return await web_search(str(args.get("query", "")), int(args.get("n") or 5))
    if name == "memory_query":
        return await memory_query(str(args.get("query", "")), int(args.get("n") or 5), args.get("collection"))
    if name == "memory_store":
        return await memory_store(str(args.get("text", "")), args.get("tags") or [], source=f"agent:{run_id}")
    if name == "notify":
        return await notify(str(args.get("message", "")), str(args.get("title") or "llm-cluster agent"), int(args.get("priority") or 3))
    return {"error": f"unknown tool {name}"}


async def node_busy(node: str) -> str | None:
    if node != "mini":
        return None
    try:
        peer_id = CFG.get("peer_id") or CFG.get("nodes", {}).get("mini", {}).get("id") or "worker-node"
        n = await mgr("GET", f"/api/nodes/{peer_id}", timeout=5)
        if n.get("busy"):
            return str(n["busy"])
        if n.get("status") == "down":
            return "worker node is down"
    except Exception as e:  # manager down: don't block, Ollama reachability decides
        log.warning("busy check failed: %s", e)
    return None


async def agent_run(prompt: str, node: str = "mini", model: str | None = None, tools: list[str] | None = None,
                    system: str | None = None, max_steps: int | None = None, store_result: bool = False,
                    notify_done: bool = False, source: str = "api") -> dict:
    peer_id = CFG.get("peer_id") or CFG.get("nodes", {}).get("mini", {}).get("id") or "worker-node"
    mgr_id = CFG.get("manager_id") or CFG.get("nodes", {}).get("desktop", {}).get("id") or "manager-node"
    node = {peer_id: "mini", mgr_id: "desktop"}.get(node, node)
    if node not in CFG["nodes"]:
        raise ValueError("node must be mini or desktop")
    run_id = "r" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
    fell_back = None
    busy = await node_busy(node)
    if busy:
        fell_back = f"mini unavailable ({busy}); ran on desktop"
        node, model = "desktop", None
    nc = CFG["nodes"][node]
    model = model or nc["default_model"]
    tools = [t for t in (tools if tools is not None else ["web_search", "memory_query", "memory_store"]) if t in AGENT_TOOLS]
    schemas = [{"type": "function", "function": {"name": t, **AGENT_TOOLS[t]}} for t in tools]
    sys_msg = system or ("You are a worker in a local LLM cluster. Use the tools when they help: web_search for current facts, "
                         "memory_query for past cluster notes. Cite URLs you used. When done, answer concisely.")
    msgs = [{"role": "system", "content": sys_msg}, {"role": "user", "content": prompt}]
    steps, calls, t0 = 0, [], time.time()
    tok_in = tok_out = 0
    max_steps = max(1, min(int(max_steps or CFG["agent"]["max_steps"]), 12))
    content = ""
    deadline = t0 + CFG["agent"]["timeout_s"]
    event("agent.start", f"{run_id} {node}/{model} tools={tools}", run_id=run_id, source=source)
    while steps < max_steps:
        steps += 1
        body = {"model": model, "messages": msgs, "stream": False, "tools": schemas or None,
                "options": {"num_ctx": nc.get("num_ctx", 4096), "temperature": CFG["agent"]["temperature"]}}
        if steps == max_steps:
            body.pop("tools")  # force a final answer
        r = await http.post(f"{nc['ollama']}/api/chat", json=body, timeout=max(30, deadline - time.time()))
        r.raise_for_status()
        j = r.json()
        tok_in += j.get("prompt_eval_count") or 0
        tok_out += j.get("eval_count") or 0
        m = j.get("message") or {}
        tcs = m.get("tool_calls") or []
        msgs.append({"role": "assistant", "content": m.get("content") or "", **({"tool_calls": tcs} if tcs else {})})
        if not tcs:
            content = m.get("content") or ""
            break
        for tc in tcs:
            fn = tc.get("function") or {}
            name, args = fn.get("name"), fn.get("arguments") or {}
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {}
            ts = time.time()
            try:
                res = await run_tool(name, args, run_id) if name in tools else {"error": f"tool {name} not allowed"}
                ok = "error" not in res if isinstance(res, dict) else True
            except Exception as e:
                res, ok = {"error": str(e)}, False
            calls.append({"step": steps, "tool": name, "args": args, "ok": ok, "s": round(time.time() - ts, 2)})
            msgs.append({"role": "tool", "tool_name": name, "content": json.dumps(res, ensure_ascii=False)[:6000]})
    wall = round(time.time() - t0, 2)
    rec = {"id": run_id, "ts": now_iso(), "source": source, "node": node, "model": model, "prompt": prompt[:2000],
           "tools_offered": tools, "tool_calls": calls, "steps": steps, "wall_s": wall, "prompt_tokens": tok_in,
           "eval_tokens": tok_out, "fallback": fell_back, "answer": content}
    if store_result and content:
        try:
            rec["stored"] = await memory_store(f"Q: {prompt}\nA: {content}", ["agent-run", node], source=f"agent:{run_id}",
                                               collection="tasks", meta={"model": model, "node": node}, doc_id=run_id)
        except Exception as e:
            rec["stored"] = {"error": str(e)}
    if notify_done:
        try:
            rec["notified"] = await notify(f"{node}/{model} finished in {wall}s, {len(calls)} tool calls.\n\n{content[:600]}",
                                           f"Cluster run {run_id} done", 3, ["white_check_mark"])
        except Exception as e:
            rec["notified"] = {"error": str(e)}
    jlog(RUNS, rec)
    event("agent.done", f"{run_id} {node}/{model} {wall}s calls={len(calls)}", run_id=run_id)
    return rec

# --------------------------------------------------------------------------- MCP server (for Odysseus)

mcp = FastMCP("llm-cluster", instructions=(
    "Tools for the local LLM cluster (manager + worker nodes). "
    "Use cluster_submit_task for jobs the cluster should run and check (code with tests, text tasks); "
    "cluster_agent_run to have a cluster model research with web search + cluster memory; cluster_status for node health."),
    stateless_http=True, json_response=True, streamable_http_path="/mcp",
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=True,
        allowed_hosts=["127.0.0.1:*", "localhost:*", "172.17.0.1:*", "host.docker.internal:*"],
        allowed_origins=["http://127.0.0.1:*", "http://localhost:*"]))


@mcp.tool()
async def cluster_status() -> dict:
    """Health of every cluster node: status, problems, power, temps, current task, queue, installed/loaded models."""
    j = await mgr("GET", "/api/nodes")
    return {"ts": j.get("ts"), "nodes": [node_brief(n) for n in j.get("nodes", [])]}


@mcp.tool()
async def cluster_submit_task(text: str, target: str = "auto", type: str = "auto", tests: str | None = None,
                              expect: list[str] | None = None, wait: bool = True, timeout_s: int = 600) -> dict:
    """Submit a task to the cluster manager. It routes simple tasks to the mini PC, has the desktop model check the result,
    sends fix requests up to 3 rounds, then escalates. target: auto|worker|manager. type: auto|code|text.
    tests: optional python test source for code tasks. wait=True blocks until done (up to timeout_s) and returns the result."""
    body: dict[str, Any] = {"text": text, "target": target, "type": type}
    if tests:
        body["tests"] = tests
    if expect:
        body["expect"] = expect
    j = await mgr("POST", "/api/tasks", body)
    tid = j.get("id")
    event("task.submit", f"{tid} via MCP", task_id=tid)
    if not wait:
        return j
    end = time.time() + max(10, min(int(timeout_s), 3600))
    while time.time() < end:
        t = await mgr("GET", f"/api/tasks/{tid}")
        if t.get("status") not in ("queued", "running"):
            return _task_brief(t)
        await asyncio.sleep(3)
    return {"id": tid, "status": "still running", "hint": "call cluster_get_task later"}


def _task_brief(t: dict) -> dict:
    keep = ("id", "status", "route_final", "rounds_total", "first_try_pass", "escalated", "timings", "energy", "classification")
    out = {k: t.get(k) for k in keep}
    out["final_output"] = (t.get("final_output") or "")[:6000]
    out["attempts"] = [{k: a.get(k) for k in ("n", "node", "model", "ok", "tok_s", "j_per_tok")} |
                       {"issues": (a.get("verdict") or {}).get("issues")} for a in t.get("attempts") or []]
    return out


@mcp.tool()
async def cluster_get_task(task_id: str) -> dict:
    """Result and attempt history of one cluster task."""
    return _task_brief(await mgr("GET", f"/api/tasks/{task_id}"))


@mcp.tool()
async def cluster_list_tasks(n: int = 10, status: str | None = None) -> dict:
    """Recent cluster tasks (newest first). status: queued|running|passed|needs_human|error."""
    q = f"/api/tasks?n={int(n)}" + (f"&status={status}" if status else "")
    j = await mgr("GET", q)
    f = lambda t: {k: t.get(k) for k in ("id", "status", "ts_submit", "text_preview", "route_final", "rounds_total")}
    return {"live": [f(t) for t in j.get("live", [])], "tasks": [f(t) for t in j.get("tasks", [])]}


@mcp.tool()
async def cluster_alerts(n: int = 10) -> dict:
    """Recent cluster alerts (both directions, newest first)."""
    return await mgr("GET", f"/api/alerts?n={int(n)}")


@mcp.tool()
async def cluster_agent_run(prompt: str, node: str = "mini", model: str | None = None,
                            tools: list[str] | None = None, store_result: bool = True, notify_done: bool = False) -> dict:
    """Run a prompt on a cluster model WITH tools (web_search, memory_query, memory_store, notify). The model runs on the
    chosen node (mini = mini PC, desktop = this PC); tool calls run on the desktop against Odysseus's SearXNG/ChromaDB/ntfy.
    store_result saves Q+A to cluster memory; notify_done pushes an ntfy notification when finished."""
    r = await agent_run(prompt, node, model, tools, store_result=store_result, notify_done=notify_done, source="odysseus-mcp")
    return {k: r.get(k) for k in ("id", "node", "model", "answer", "tool_calls", "wall_s", "eval_tokens", "fallback", "stored", "notified")}


@mcp.tool()
async def cluster_memory_query(query: str, n: int = 5, collection: str = "memory") -> list[dict]:
    """Semantic search in cluster memory. collection: memory (notes/findings) or tasks (archived task results)."""
    return await memory_query(query, n, collection)


@mcp.tool()
async def cluster_memory_store(text: str, tags: list[str] | None = None) -> dict:
    """Save a note/finding to cluster memory (separate from Odysseus's own chat memory)."""
    return await memory_store(text, tags or [], source="odysseus-mcp")


@mcp.tool()
async def cluster_notify(message: str, title: str = "llm-cluster", priority: int = 3) -> dict:
    """Push a notification to the cluster ntfy topic (priority 1-5)."""
    return await notify(message, title, priority)


@mcp.tool()
async def cluster_models() -> dict:
    """Models available per node and the OpenAI-compatible model ids the bridge serves."""
    return await list_models_payload()

# --------------------------------------------------------------------------- REST: tool endpoints + agent + OpenAI

async def _json(req: Request) -> dict:
    try:
        return await req.json()
    except Exception:
        return {}


def _err(e: Exception, code: int = 500) -> JSONResponse:
    return JSONResponse({"error": str(e)}, status_code=code)


@mcp.custom_route("/health", methods=["GET"])
async def r_health(req: Request):
    checks = {}
    for name, url in (("manager", CFG["manager"] + "/api/health"), ("searxng", CFG["searxng"] + "/healthz"),
                      ("chromadb", CFG["chromadb"] + "/api/v2/heartbeat"), ("ntfy", CFG["ntfy"] + "/v1/health"),
                      ("mini_ollama", CFG["nodes"]["mini"]["ollama"] + "/api/version")):
        try:
            r = await http.get(url, timeout=4)
            checks[name] = r.status_code < 400
        except Exception:
            checks[name] = False
    return JSONResponse({"service": "llm-cluster-odysseus-bridge", "version": VERSION, "ts": now_iso(), "ok": all(checks.values()), "checks": checks})


@mcp.custom_route("/tools", methods=["GET"])
async def r_tools(req: Request):
    return JSONResponse({"tools": [{"name": k, **v} for k, v in AGENT_TOOLS.items()],
                         "endpoints": ["POST /tools/search", "POST /tools/memory/store", "POST /tools/memory/query", "POST /tools/notify",
                                       "POST /agent/run", "GET /agent/runs", "GET /v1/models", "POST /v1/chat/completions", "MCP /mcp"]})


@mcp.custom_route("/tools/search", methods=["POST"])
async def r_search(req: Request):
    b = await _json(req)
    try:
        return JSONResponse({"results": await web_search(b["query"], b.get("n", 5), b.get("categories", "general"))})
    except Exception as e:
        return _err(e)


@mcp.custom_route("/tools/memory/store", methods=["POST"])
async def r_mstore(req: Request):
    b = await _json(req)
    try:
        return JSONResponse(await memory_store(b["text"], b.get("tags"), b.get("source", "rest"), b.get("collection"), b.get("meta"), b.get("id")))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/tools/memory/query", methods=["POST"])
async def r_mquery(req: Request):
    b = await _json(req)
    try:
        return JSONResponse({"results": await memory_query(b["query"], b.get("n", 5), b.get("collection"))})
    except Exception as e:
        return _err(e)


@mcp.custom_route("/tools/notify", methods=["POST"])
async def r_notify(req: Request):
    b = await _json(req)
    try:
        return JSONResponse(await notify(b["message"], b.get("title", "llm-cluster"), b.get("priority", 3), b.get("tags")))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/agent/run", methods=["POST"])
async def r_agent(req: Request):
    b = await _json(req)
    try:
        return JSONResponse(await agent_run(b["prompt"], b.get("node", "mini"), b.get("model"), b.get("tools"), b.get("system"),
                                            b.get("max_steps"), bool(b.get("store_result")), bool(b.get("notify")), b.get("source", "rest")))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/agent/runs", methods=["GET"])
async def r_runs(req: Request):
    n = int(req.query_params.get("n", "20"))
    lines = RUNS.read_text().splitlines()[-n:] if RUNS.exists() else []
    return JSONResponse({"runs": [json.loads(x) for x in reversed(lines)]})


async def list_models_payload() -> dict:
    per_node = {}
    for node, nc in CFG["nodes"].items():
        try:
            r = await http.get(nc["ollama"] + "/api/tags", timeout=5)
            per_node[node] = [m["name"] for m in r.json().get("models", []) if "embed" not in m["name"]]
        except Exception:
            per_node[node] = []
    ids = ["cluster", "cluster-mini", "cluster-desktop", "cluster-agent"]  # cluster-network listed separately
    ids += [f"{n}/{m}" for n, ms in per_node.items() for m in ms]
    ids += [f"cluster-agent/{m}" for m in per_node.get("mini", [])]
    return {"nodes": per_node, "openai_model_ids": ids,
            "notes": {"cluster*": "manager pipeline (route, check, fix loop); slow but reviewed",
                      "mini/<m>, desktop/<m>": "direct passthrough to that node's Ollama",
                      "cluster-agent[/<m>]": "mini model with web_search + cluster memory tools"}}


@mcp.custom_route("/v1/models", methods=["GET"])
async def r_models(req: Request):
    # Default: only the orchestrated "cluster-network" model, so the Odysseus picker shows one Cluster Network entry.
    # ?all=1 also lists the direct ids (cluster, cluster-mini, mini/<m>, desktop/<m>, cluster-agent...).
    ids = ["cluster-network"]
    if req.query_params.get("all") in ("1", "true", "yes"):
        ids += (await list_models_payload())["openai_model_ids"]
    return JSONResponse({"object": "list", "data": [{"id": i, "object": "model", "owned_by": "llm-cluster", "created": 0,
                                                     **({"name": "Cluster Network"} if i == "cluster-network" else {})} for i in ids]})


def _last_user(messages: list) -> str:
    for m in reversed(messages or []):
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, list):
                c = "\n".join(p.get("text", "") for p in c if isinstance(p, dict))
            return str(c or "")
    return ""


def _fake_completion(model: str, content: str, stream: bool, extra: dict | None = None):
    cid = "chatcmpl-" + uuid.uuid4().hex[:12]
    if not stream:
        return JSONResponse({"id": cid, "object": "chat.completion", "created": int(time.time()), "model": model,
                             "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
                             "usage": {}, "cluster": extra or {}})

    async def gen():
        for delta, fin in (({"role": "assistant", "content": content}, None), ({}, "stop")):
            yield "data: " + json.dumps({"id": cid, "object": "chat.completion.chunk", "created": int(time.time()), "model": model,
                                         "choices": [{"index": 0, "delta": delta, "finish_reason": fin}]}) + "\n\n"
        yield "data: [DONE]\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


@mcp.custom_route("/v1/chat/completions", methods=["POST"])
async def r_chat(req: Request):
    b = await _json(req)
    model = str(b.get("model") or "cluster")
    stream = bool(b.get("stream"))
    try:
        if model.lower().replace(" ", "-") in ("cluster-network", "llm-cluster/cluster-network"):
            res = await orchestrator.handle(b, stream)
            if stream:
                return StreamingResponse(res, media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
            return JSONResponse(res)
        if model.startswith("cluster-agent"):
            sub = model.split("/", 1)[1] if "/" in model else None
            rec = await agent_run(_last_user(b.get("messages")), "mini", sub, store_result=True, source="odysseus-openai")
            return _fake_completion(model, rec["answer"], stream, {"run_id": rec["id"], "tool_calls": len(rec["tool_calls"]), "node": rec["node"]})
        if model in ("cluster", "cluster-mini", "cluster-desktop"):
            target, upstream_model = CFG["manager"] + "/v1/chat/completions", model
        elif "/" in model and model.split("/", 1)[0] in CFG["nodes"]:
            node, upstream_model = model.split("/", 1)
            target = CFG["nodes"][node]["ollama"] + "/v1/chat/completions"
        else:
            return JSONResponse({"error": {"message": f"unknown model {model}; see GET /v1/models"}}, status_code=404)
        body = dict(b, model=upstream_model)
        if not stream:
            r = await http.post(target, json=body, timeout=3600)
            return Response(r.content, status_code=r.status_code, media_type="application/json")

        async def gen():
            async with http.stream("POST", target, json=body, timeout=3600) as r:
                async for chunk in r.aiter_raw():
                    yield chunk
        return StreamingResponse(gen(), media_type="text/event-stream")
    except Exception as e:
        return JSONResponse({"error": {"message": str(e)}}, status_code=502)

# --------------------------------------------------------------------------- poller: alerts -> ntfy, tasks -> ChromaDB/ntfy

def _load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _save_state(s: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(s))
    tmp.replace(STATE_FILE)


async def poller() -> None:
    pc = CFG["poller"]
    st = _load_state()
    first = "seen_alerts" not in st
    st.setdefault("seen_alerts", [])
    st.setdefault("seen_tasks", [])
    st.setdefault("last_key_ts", {})
    while True:
        try:
            alerts = (await mgr("GET", "/api/alerts?n=50", timeout=10)).get("alerts", [])
            for a in reversed(alerts):
                sig = hashlib.sha1(json.dumps([a.get("ts"), a.get("key"), a.get("msg")]).encode()).hexdigest()[:16]
                if sig in st["seen_alerts"]:
                    continue
                st["seen_alerts"].append(sig)
                if first or a.get("level") not in pc["notify_levels"]:
                    continue
                key = f"{a.get('about')}:{a.get('key')}"
                if time.time() - st["last_key_ts"].get(key, 0) < pc["min_interval_per_key_s"]:
                    continue
                st["last_key_ts"][key] = time.time()
                await notify(a.get("msg") or "", f"[{a.get('level')}] {a.get('about') or 'cluster'}",
                             5 if a.get("level") == "crit" else 4, ["warning"])
                event("poller.alert", f"ntfy {key}")
            tasks = (await mgr("GET", "/api/tasks?n=20", timeout=10)).get("tasks", [])
            for t in reversed(tasks):
                tid, status = t.get("id"), t.get("status")
                if not tid or tid in st["seen_tasks"] or status in ("queued", "running"):
                    continue
                st["seen_tasks"].append(tid)
                if first:
                    continue
                if status in pc["notify_task_status"] or (pc["notify_on_pass"] and status == "passed"):
                    await notify(f"{t.get('text_preview')}\nstatus={status} route={t.get('route_final')} rounds={t.get('rounds_total')}",
                                 f"Cluster task {status}", 4 if status != "passed" else 2, ["x" if status != "passed" else "white_check_mark"])
                    event("poller.task_notify", f"{tid} {status}", task_id=tid)
                if pc["archive_passed_tasks"] and status == "passed" and not t.get("excluded_from_findings"):
                    full = await mgr("GET", f"/api/tasks/{tid}", timeout=10)
                    txt = f"Task: {(full.get('task') or {}).get('text') or t.get('text_preview')}\nResult: {(full.get('final_output') or '')[:4000]}"
                    await memory_store(txt, ["task", t.get("route_final") or ""], source="manager", collection="tasks",
                                       meta={"task_id": tid, "route_final": t.get("route_final"), "rounds": t.get("rounds_total")}, doc_id=tid)
                    event("poller.archive", f"{tid} -> chroma", task_id=tid)
            st["seen_alerts"] = st["seen_alerts"][-500:]
            st["seen_tasks"] = st["seen_tasks"][-500:]
            first = False
            _save_state(st)
        except Exception as e:
            log.warning("poller: %s", e)
        await asyncio.sleep(pc["interval_s"])

import orchestrator  # noqa: E402  (needs the helpers above)
orchestrator.setup(sys.modules[__name__])

# --------------------------------------------------------------------------- auth wrapper + main

class Guard:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            ip = (scope.get("client") or ("0.0.0.0", 0))[0]
            if ip not in ("127.0.0.1", "::1"):
                ok_net = False
                try:
                    ok_net = any(ipaddress.ip_address(ip) in n for n in ALLOW)
                except ValueError:
                    pass
                hdrs = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
                auth = hdrs.get("authorization", "")
                tok = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
                if not tok:
                    from urllib.parse import parse_qs
                    tok = (parse_qs(scope.get("query_string", b"").decode()).get("key") or [""])[0]
                path = scope.get("path", "")
                # Model-metadata probes (Odysseus context-length detection) carry no key; they reveal nothing sensitive.
                if ok_net and not tok and scope.get("method") == "GET" and path in ("/v1/models", "/slots", "/props", "/health"):
                    return await self.app(scope, receive, send)
                if not ok_net or not hmac.compare_digest(tok, TOKEN):
                    log.warning("rejected %s %s from %s (net_ok=%s, token=%s)", scope.get("method"), path, ip, ok_net, "present" if tok else "missing")
                    resp = JSONResponse({"error": "forbidden: source not allowed or bad token"}, status_code=403 if not ok_net else 401)
                    return await resp(scope, receive, send)
        return await self.app(scope, receive, send)


def make_socket(host: str, port: int) -> socket.socket | None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.IPPROTO_IP, 15, 1)  # IP_FREEBIND: docker0 may come up after us
    try:
        s.bind((host, port))
        s.listen(128)
        s.setblocking(False)
        return s
    except OSError as e:
        log.error("cannot bind %s:%s: %s", host, port, e)
        s.close()
        return None


async def main() -> None:
    inner = mcp.streamable_http_app()
    app = Guard(inner)
    socks = [s for s in (make_socket(h, CFG["port"]) for h in CFG["bind"]) if s]
    if not socks:
        raise SystemExit("no listen sockets")
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on", proxy_headers=False))
    tasks = [asyncio.create_task(server.serve(sockets=socks))]
    if CFG["poller"]["enabled"]:
        tasks.append(asyncio.create_task(poller()))
    event("service", f"bridge {VERSION} listening on {[s.getsockname() for s in socks]}")
    await tasks[0]


if __name__ == "__main__":
    asyncio.run(main())
