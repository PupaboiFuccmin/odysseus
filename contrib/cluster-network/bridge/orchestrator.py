"""Cluster Network orchestrator (model id ``cluster-network``), served by the bridge's /v1/chat/completions.

Chain of command:
  Odysseus chat -> manager model classifies:
    trivial    -> one fast model (mini qwen2.5:3b), streamed, no plan
    tools      -> Odysseus tool turn: passthrough to the manager model WITH Odysseus's tool schemas (Odysseus runs the
                  tools and approvals; results come back as role=tool and are passed through again)
    direct     -> one model streams the answer (simple -> mini qwen2.5:7b, complex -> desktop gemma4:12b)
    plan       -> optional web search / cluster memory (bridge, desktop side) -> subtasks in parallel
                  (simple/code -> mini 7b / coder, complex -> desktop) -> manager reviews each result (search claims are
                  checked against the fetched sources) -> fix requests (<= fix_rounds) -> escalate to desktop
                  -> manager synthesizes and streams the final answer.
Progress streams as `reasoning_content` (Odysseus shows it in the collapsible thinking block = "Cluster activity"),
the answer streams as `content`. Every step is reported to the manager (POST /api/events) so the Cluster tab map lights up.
"""
from __future__ import annotations

import asyncio, json, re, time, uuid
from typing import Any, AsyncIterator, Callable

B: Any = None          # bridge module (set by setup)
OC: dict = {}

TRIVIAL_RE = re.compile(r"^\s*(hi+|hey+|hello+|yo|sup|hiya|howdy|thanks?|thank you|thx|ty|ok(ay)?|cool|nice|great|lol|bye|"
                        r"good (morning|afternoon|evening|night)|gm|gn|test|ping|how are you( doing)?|what'?s up)\b[\s!.?,:)]*$", re.I)


def setup(bridge_module) -> None:
    global B, OC
    B = bridge_module
    OC = {"manager_model": "gemma4:12b", "fast_model": "qwen2.5:3b", "mini_text": "qwen2.5:7b", "mini_code": "qwen2.5-coder:7b",
          "max_subtasks": 4, "fix_rounds": 2, "heartbeat_s": 8, "desktop_ctx": 8192, "mini_ctx": 4096, "search_results": 5,
          "max_sources": 8, "store_runs": True, **(B.CFG.get("orchestrator") or {})}


# ------------------------------------------------------------------ helpers

def _text(c) -> str:
    if isinstance(c, list):
        return "\n".join(p.get("text", "") for p in c if isinstance(p, dict) and p.get("type") in (None, "text"))
    return str(c or "")


def _strip_ctx(t: str) -> str:
    """Remove Odysseus's '[Context — current date/time …]' block (standalone or merged into the user turn)."""
    if not t.lstrip().startswith("[Context"):
        return t
    h = t.find("## Current date and time")
    i = t.find("\n\n", h if h >= 0 else 0)
    return t[i + 2:].strip() if i >= 0 else ""


def _is_ctx(m: dict) -> bool:
    return m.get("role") == "user" and not _strip_ctx(_text(m.get("content"))).strip()


def _last_user(msgs: list) -> str:
    for m in reversed(msgs or []):
        if m.get("role") == "user" and not _is_ctx(m):
            return _strip_ctx(_text(m.get("content"))).strip()
    return ""


_FOOTER_RE = re.compile(r"\n*(---\n)?\*?(<sub>)?Cluster Network · [^\n]*$")


def _history(msgs: list, turns: int = 6, cap: int = 700) -> list[dict]:
    out = [{"role": m["role"], "content": _FOOTER_RE.sub("", _strip_ctx(_text(m.get("content"))))[:cap]} for m in msgs or []
           if m.get("role") in ("user", "assistant") and _text(m.get("content")).strip() and not _is_ctx(m)]
    return out[-turns:]


def _nodes():
    return B.CFG["nodes"]["mini"]["ollama"], B.CFG["nodes"]["desktop"]["ollama"]


def _mgr_id() -> str:
    return (B.CFG.get("nodes", {}).get("desktop", {}).get("id") or "manager-node") if B else "manager-node"


def _wrk_id() -> str:
    return (B.CFG.get("nodes", {}).get("mini", {}).get("id") or "worker-node") if B else "worker-node"


def _ev(kind: str, msg: str, node: str | None = None, flows: list | None = None, level: str = "info", data: dict | None = None):
    async def go():
        try:
            await B.http.post(B.CFG["manager"] + "/api/events", json={"node": node or _mgr_id(), "kind": kind, "msg": msg, "level": level,
                                                                      "flows": flows or [], "data": data}, timeout=3)
        except Exception:
            pass
    asyncio.get_running_loop().create_task(go())


async def _chat(url: str, model: str, messages: list, ctx: int, fmt: str | None = None, temp: float = 0.2,
                timeout: float = 600) -> dict:
    body = {"model": model, "messages": messages, "stream": False, "think": False, "keep_alive": "10m",
            "options": {"num_ctx": ctx, "temperature": temp}}
    if fmt:
        body["format"] = fmt
    t0 = time.time()
    r = await B.http.post(url + "/api/chat", json=body, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    dur = (j.get("eval_duration") or 0) / 1e9
    return {"text": (j.get("message") or {}).get("content") or "", "s": round(time.time() - t0, 2),
            "tokens": j.get("eval_count") or 0, "tok_s": round((j.get("eval_count") or 0) / dur, 1) if dur else None}


async def _stream_chat(url: str, model: str, messages: list, ctx: int, put: Callable, temp: float = 0.3) -> dict:
    body = {"model": model, "messages": messages, "stream": True, "think": False, "keep_alive": "10m",
            "options": {"num_ctx": ctx, "temperature": temp}}
    t0, first, n, out = time.time(), None, 0, []
    async with B.http.stream("POST", url + "/api/chat", json=body, timeout=900) as r:
        r.raise_for_status()
        async for line in r.aiter_lines():
            if not line.strip():
                continue
            j = json.loads(line)
            piece = (j.get("message") or {}).get("content") or ""
            if piece:
                if first is None:
                    first = time.time() - t0
                out.append(piece)
                await put("content", piece)
            if j.get("done"):
                n = j.get("eval_count") or 0
    return {"text": "".join(out), "s": round(time.time() - t0, 2), "ttft_s": round(first or 0, 2), "tokens": n}


def _jparse(s: str) -> dict:
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", s or "")
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return {}


# ------------------------------------------------------------------ planner / reviewer prompts

def _today() -> str:
    return time.strftime("%A %Y-%m-%d %H:%M %Z")


PLAN_SYS = """Today is %(today)s. You are the MANAGER of a local LLM cluster. Nodes: manager (you: reasoning,
judgment, synthesis) and worker (smaller GPU/APU: fast/small models for simple text or code; slower, weaker).
Decide how to handle the user's latest message. Reply with JSON only:
{"mode":"direct|plan|tools","complexity":"simple|complex","reason":"short","search":false,"search_queries":[],
 "memory":false,"notify":false,
 "subtasks":[{"id":"s1","title":"short","prompt":"self-contained instructions for the worker","kind":"simple|code|complex","uses_search":false,"depends_on":[]}]}
Rules:
- direct: one model can answer in one go (chit-chat, a single question, a short rewrite). subtasks=[].
- plan: the request has 2+ separable parts, or needs research then analysis. 2-%(max)d subtasks; each prompt must be
  self-contained (the worker sees nothing else). Give simple/code parts to the mini, judgment/comparison/long reasoning = complex.
  Do NOT add a final "combine/synthesize/summarize everything" subtask: you (the manager) write the final answer yourself.
  If a subtask needs another subtask's output, list it in "depends_on": ["s1"] (it then runs after and receives that output).
- tools: ONLY when the user wants an action inside the Odysseus app (notes, calendar, reminders, email, files, documents,
  settings, images, Odysseus memory) and that needs its tools.%(tools_note)s
- search=true when current or real-world facts (versions, prices, news, specs, dates) are needed; give 1-3 search_queries.
  Mark the subtasks that should use the search results with uses_search=true.
- memory=true when past cluster notes/findings/tasks would help. notify=true only if the user asks to be notified.
"""

REVIEW_SYS = """Today is {today}. Web results dated up to today are normal, not "future". You are the reviewer (manager model) of a home LLM cluster. Check a worker's result for the task.
Reply with JSON only: {"pass":true,"score":0-10,"issues":["..."],"unsupported_claims":["..."]}
- pass=false if it does not do what the task asked, is wrong, or is incomplete.
- If SOURCES are given: check every factual claim (numbers, versions, dates, names) against them. Claims not supported by
  the sources go in unsupported_claims; pass=false if any material claim is unsupported or a citation is wrong.
- Be concise; issues must be concrete fix instructions."""

SYNTH_SYS = """Today is {today}. You are the MANAGER of a local LLM cluster writing the final answer to the user's message, using the
reviewed results of your workers below. Rules: answer the user's message directly and well organized (markdown ok);
only state facts that are in the results or sources; omit anything marked UNVERIFIED; keep [n] citations and end with a
"Sources" list of markdown links for the [n] you used. Do not mention the internal subtasks unless useful."""


def _sources_block(sources: list[dict]) -> str:
    return "\n".join(f"[{i + 1}] {s['title']} — {s['url']}\n    {s['snippet']}" for i, s in enumerate(sources))


# ------------------------------------------------------------------ main entry

async def handle(body: dict, stream: bool) -> AsyncIterator[str] | dict:
    if not stream and not body.get("tools"):
        # Odysseus utility calls (titles, summaries, compaction) are non-streamed and tool-less: one fast manager call.
        _, desk_url = _nodes()
        up = {k: v for k, v in body.items() if k not in ("model", "reasoning_effort", "reasoning")}
        up.update({"model": OC["manager_model"], "stream": False, "reasoning_effort": "none"})
        t0 = time.time()
        r = await B.http.post(desk_url + "/v1/chat/completions", json=up, timeout=300)
        j = r.json()
        j["model"] = body.get("model") or "cluster-network"
        _ev("orch.utility", f"utility call → {OC['manager_model']} ({round(time.time() - t0, 1)} s)", flows=[{"id": "task.local"}])
        return j
    q: asyncio.Queue = asyncio.Queue()
    run = {"id": "o" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4], "t0": time.time(), "steps": []}

    async def put(kind: str, text: str):
        await q.put((kind, text))

    async def act(line: str):
        run["steps"].append(line)
        await put("reasoning", line.rstrip("\n") + "\n")

    async def worker():
        try:
            await _orchestrate(body, put, act, run)
        except Exception as e:  # noqa: BLE001
            B.log.exception("orchestrator failed")
            await act(f"✗ orchestrator error: {e}")
            await put("content", f"\n\n**Cluster Network error:** {e}")
            _ev("orch.error", f"{run['id']} error: {e}", level="warn")
        finally:
            await q.put(("end", ""))

    task = asyncio.get_running_loop().create_task(worker())
    cid = "chatcmpl-" + run["id"]
    model = body.get("model") or "cluster-network"

    def chunk(delta: dict, fin=None) -> str:
        return "data: " + json.dumps({"id": cid, "object": "chat.completion.chunk", "created": int(time.time()), "model": model,
                                      "choices": [{"index": 0, "delta": delta, "finish_reason": fin}]}, ensure_ascii=False) + "\n\n"

    if not stream:
        content, reasoning = [], []
        while True:
            kind, text = await q.get()
            if kind == "end":
                break
            if kind == "content":
                content.append(text)
            elif kind == "reasoning":
                reasoning.append(text)
            elif kind == "raw_json":
                return json.loads(text)
        await task
        return {"id": cid, "object": "chat.completion", "created": int(time.time()), "model": model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "".join(content),
                                                     "reasoning_content": "".join(reasoning)}, "finish_reason": "stop"}],
                "usage": {}, "cluster": {"run_id": run["id"], "mode": run.get("mode")}}

    async def gen():
        yield chunk({"role": "assistant", "content": ""})
        hb = OC["heartbeat_s"]
        last = time.time()
        answering = False   # once answer text streams, keep later activity out of the UI (it would open a 2nd thinking block)
        while True:
            try:
                kind, text = await asyncio.wait_for(q.get(), timeout=hb)
            except asyncio.TimeoutError:
                if not answering:
                    busy = run.get("waiting") or "working"
                    yield chunk({"reasoning_content": f"  … {busy} ({int(time.time() - run['t0'])} s)\n"})
                continue
            if kind == "end":
                break
            if kind == "reasoning":
                if not answering:
                    yield chunk({"reasoning_content": text})
            elif kind == "content":
                if text.strip():
                    answering = True
                yield chunk({"content": text})
            elif kind == "raw":
                yield text if text.endswith("\n\n") else text + "\n\n"
            last = time.time()
        yield chunk({}, "stop")
        yield "data: [DONE]\n\n"
    return gen()


async def _orchestrate(body: dict, put, act, run: dict) -> None:
    msgs = body.get("messages") or []
    tools = body.get("tools") or []
    user = _last_user(msgs)
    hist = _history(msgs)
    mini_url, desk_url = _nodes()
    mm = OC["manager_model"]
    t0 = run["t0"]
    _ev("orch.start", f"{run['id']} Cluster Network chat ({len(user)} chars)", flows=[{"id": "task.submit"}])

    # --- Odysseus tool continuation: hand the turn back to the manager model with Odysseus's tools.
    if msgs and msgs[-1].get("role") == "tool":
        run["mode"] = "tools"
        await act(f"⚙ Odysseus tool result received → manager {mm} continues (Odysseus runs tools + approvals)")
        return await _passthrough(body, put, act, run, desk_url, mm)

    # --- trivial: one fast model, no plan
    if TRIVIAL_RE.match(user) and len(user) <= 40:
        run["mode"] = "trivial"
        busy = await B.node_busy("mini")
        url, model, node = (desk_url, mm, _mgr_id()) if busy else (mini_url, OC["fast_model"], _wrk_id())
        await act(f"▸ trivial message → {node} · {model} (no plan{', mini busy: ' + busy if busy else ''})")
        _ev("orch.trivial", f"{run['id']} trivial → {node} {model}", node=node,
            flows=[{"id": "orch.dispatch" if node == _wrk_id() else "task.local"}])
        r = await _stream_chat(url, model, [{"role": "system", "content": "You are Odysseus, answering via the local LLM cluster. Be brief and friendly."}] + hist,
                               OC["mini_ctx"] if node == _wrk_id() else OC["desktop_ctx"], put)
        await act(f"✓ {node} · {model}: first token {r['ttft_s']} s, {r['tokens']} tok, {r['s']} s")
        if node == _wrk_id():
            _ev("orch.result", f"{run['id']} trivial answered", node=node, flows=[{"id": "orch.result"}])
        return await _finish(run, user, r["text"], put, act, footer=False)

    # --- plan with the manager model
    await act(f"▸ manager {mm} ({_mgr_id()}): classifying + planning…")
    run["waiting"] = f"manager {mm} planning"
    tools_note = " Odysseus tools ARE available for this chat." if tools else " (No Odysseus tools available: never choose tools.)"
    plan_msgs = [{"role": "system", "content": PLAN_SYS % {"max": OC["max_subtasks"], "tools_note": tools_note, "today": _today()}}]
    ctx_hist = hist[:-1]
    if ctx_hist:
        plan_msgs.append({"role": "user", "content": "Conversation so far (for context):\n" +
                          "\n".join(f"{h['role']}: {h['content']}" for h in ctx_hist)})
    plan_msgs.append({"role": "user", "content": f"Latest user message:\n{user}"})
    pr = await _chat(desk_url, mm, plan_msgs, OC["desktop_ctx"], fmt="json", temp=0.1, timeout=120)
    plan = _jparse(pr["text"])
    mode = plan.get("mode") if plan.get("mode") in ("direct", "plan", "tools") else "direct"
    if mode == "tools" and not tools:
        mode = "direct"
    subtasks = [s for s in (plan.get("subtasks") or []) if isinstance(s, dict) and s.get("prompt")][: OC["max_subtasks"]]
    if plan.get("search") and mode == "direct":
        mode, subtasks = "plan", [{"id": "s1", "title": "answer with sources", "prompt": user,
                                   "kind": plan.get("complexity") or "simple", "uses_search": True}]
    if mode == "plan" and not subtasks:
        mode = "direct"
    run["mode"], run["plan"] = mode, plan
    _ev("orch.plan", f"{run['id']} plan: {mode}, {len(subtasks)} subtasks, search={bool(plan.get('search'))} ({pr['s']} s)",
        flows=[{"id": "orch.plan"}], data={"reason": plan.get("reason")})
    await act(f"✓ plan ({pr['s']} s): mode={mode}, complexity={plan.get('complexity')}, "
              f"{len(subtasks)} subtask(s){', web search' if plan.get('search') else ''}{', memory' if plan.get('memory') else ''}"
              f" — {plan.get('reason') or ''}")

    if mode == "tools":
        await act(f"⚙ needs Odysseus app tools → manager {mm} with Odysseus's tool schemas (approvals stay in Odysseus)")
        return await _passthrough(body, put, act, run, desk_url, mm)

    if mode == "direct":
        simple = plan.get("complexity") != "complex"
        busy = await B.node_busy("mini") if simple else None
        if simple and not busy:
            url, model, node, ctx = mini_url, OC["mini_text"], _wrk_id(), OC["mini_ctx"]
        else:
            url, model, node, ctx = desk_url, mm, _mgr_id(), OC["desktop_ctx"]
        await act(f"▸ direct answer → {node} · {model} (streaming)")
        _ev("orch.direct", f"{run['id']} direct → {node} {model}", node=node,
            flows=[{"id": "orch.dispatch" if node == _wrk_id() else "orch.synth"}])
        r = await _stream_chat(url, model, [{"role": "system", "content": "You are Odysseus, answering via the local LLM cluster. Today is " + _today() + ". Answer accurately; say when you are unsure."}] + hist, ctx, put)
        await act(f"✓ {node} · {model}: first token {r['ttft_s']} s, {r['tokens']} tok, {r['s']} s")
        if node == _wrk_id():
            _ev("orch.result", f"{run['id']} direct answered on mini", node=node, flows=[{"id": "orch.result"}])
        return await _finish(run, user, r["text"], put, act, footer=True, nodes_used={f"{node} · {model}"})

    # --- plan: tools first (desktop side, via Odysseus services)
    sources: list[dict] = []
    if plan.get("search"):
        queries = [str(x) for x in (plan.get("search_queries") or [user])][:3]
        run["waiting"] = "web search"
        ts = time.time()
        res = await asyncio.gather(*[B.web_search(qq, OC["search_results"]) for qq in queries], return_exceptions=True)
        seen = set()
        for rr in res:
            if isinstance(rr, Exception):
                continue
            for s in rr:
                if s.get("url") and s["url"] not in seen:
                    seen.add(s["url"])
                    sources.append(s)
        sources = sources[: OC["max_sources"]]
        await act(f"🔎 web search (Odysseus SearXNG, desktop): {len(queries)} quer{'y' if len(queries) == 1 else 'ies'} → {len(sources)} sources "
                  f"({round(time.time() - ts, 1)} s): " + "; ".join(queries))
        _ev("tool.search", f"{run['id']} search {queries} → {len(sources)} sources", flows=[{"id": "tools.search", "ok": bool(sources)}])
    memory_ctx = ""
    if plan.get("memory"):
        try:
            mem = await B.memory_query(user, 3, "memory") + await B.memory_query(user, 2, "tasks")
            mem = [m for m in mem if m["distance"] < 0.6]
            memory_ctx = "\n".join(f"- {m['text'][:400]}" for m in mem)
            await act(f"🧠 cluster memory (Odysseus ChromaDB): {len(mem)} relevant notes")
            _ev("tool.memory", f"{run['id']} memory query → {len(mem)}", flows=[{"id": "tools.memory"}])
        except Exception as e:
            await act(f"🧠 memory unavailable: {e}")

    busy = await B.node_busy("mini")
    if busy:
        await act(f"⚠ worker unavailable ({busy}) → all subtasks on {_mgr_id()}")
    sems = {_wrk_id(): asyncio.Semaphore(2), _mgr_id(): asyncio.Semaphore(2)}
    for i, st in enumerate(subtasks):
        st.setdefault("id", f"s{i + 1}")
    ids = {st["id"] for st in subtasks}
    done: dict[str, dict] = {}
    pending = list(subtasks)
    while pending:
        ready = [st for st in pending if all(d in done or d not in ids for d in (st.get("depends_on") or []))] or pending
        if len(ready) < len(pending):
            await act(f"  ⋯ wave: {', '.join(st['id'] for st in ready)} now; {', '.join(st['id'] for st in pending if st not in ready)} wait for their inputs")
        res = await asyncio.gather(*[_subtask(dict(st, prompt=st["prompt"] + (
            "\n\nInputs from other workers:\n" + "\n\n".join(f"[{done[d]['id']} {done[d]['title']}]\n{done[d]['text']}"
                                                                 for d in (st.get("depends_on") or []) if d in done)
            if any(d in done for d in (st.get("depends_on") or [])) else "")), subtasks.index(st), sources, memory_ctx, busy, sems, act, run)
            for st in ready])
        for st, r in zip(ready, res):
            done[st["id"]] = r
        pending = [st for st in pending if st not in ready]
    results = [done[st["id"]] for st in subtasks]

    # --- synthesis
    used = {f"{r['node']} · {r['model']}" for r in results}
    run["waiting"] = f"manager {mm} synthesizing"
    passed = sum(1 for r in results if r["verdict"].get("pass"))
    if len(results) == 1 and results[0]["verdict"].get("pass") and results[0]["node"] == _mgr_id() and not sources:
        await act("▸ single verified result → streaming it")
        text = results[0]["text"]
        for i in range(0, len(text), 400):
            await put("content", text[i:i + 400])
        return await _finish(run, user, text, put, act, footer=True, nodes_used=used, results=results)
    await act(f"▸ manager {mm} synthesizing {len(results)} result(s) ({passed} passed review) — streaming answer")
    _ev("orch.synth", f"{run['id']} synthesizing {len(results)} results", flows=[{"id": "orch.synth"}])
    blocks = []
    for r in results:
        v = r["verdict"]
        tag = "VERIFIED" if v.get("pass") else "PARTLY UNVERIFIED"
        unsup = ("\nUNVERIFIED claims (omit these): " + "; ".join(v.get("unsupported_claims") or [])) if v.get("unsupported_claims") else ""
        blocks.append(f"### {r['title']} ({tag}, by {r['node']} {r['model']})\n{r['text']}{unsup}")
    synth_user = f"User message:\n{user}\n\nWorker results:\n\n" + "\n\n".join(blocks)
    if sources:
        synth_user += "\n\nSOURCES (cite as [n]):\n" + _sources_block(sources)
    sr = await _stream_chat(desk_url, mm, [{"role": "system", "content": SYNTH_SYS.replace("{today}", _today())}] + hist[:-1] + [{"role": "user", "content": synth_user}],
                            OC["desktop_ctx"], put)
    used.add(f"{_mgr_id()} · {mm}")
    await act(f"✓ synthesis: first token {sr['ttft_s']} s, {sr['tokens']} tok, {sr['s']} s")
    return await _finish(run, user, sr["text"], put, act, footer=True, nodes_used=used, results=results, notify=plan.get("notify"))


async def _subtask(st: dict, i: int, sources: list, memory_ctx: str, busy, sems, act, run) -> dict:
    mini_url, desk_url = _nodes()
    mm = OC["manager_model"]
    kind = st.get("kind") if st.get("kind") in ("simple", "code", "complex") else "simple"
    sid = st.get("id") or f"s{i + 1}"
    title = st.get("title") or sid
    on_mini = kind in ("simple", "code") and not busy
    node = _wrk_id() if on_mini else _mgr_id()
    model = (OC["mini_code"] if kind == "code" else OC["mini_text"]) if on_mini else mm
    use_src = bool(st.get("uses_search") and sources)
    prompt = st["prompt"]
    if use_src:
        prompt += ("\n\nSOURCES (web search results):\n" + _sources_block(sources) +
                   "\n\nUse ONLY these sources for facts, cite them as [n]. If they do not contain something, say it is not in the sources.")
    if memory_ctx:
        prompt += "\n\nCluster notes that may help:\n" + memory_ctx
    messages = [{"role": "system", "content": "You are a worker in a local LLM cluster. Do exactly the task. Be accurate and concise."},
                {"role": "user", "content": prompt}]
    verdict: dict = {}
    r: dict = {}
    rounds = OC["fix_rounds"]
    for attempt in range(1, rounds + 3):
        url = mini_url if node == _wrk_id() else desk_url
        ctx = OC["mini_ctx"] if node == _wrk_id() else OC["desktop_ctx"]
        await act(f"  → {sid} «{title}» [{kind}] → {node} · {model}" + (f" (attempt {attempt})" if attempt > 1 else ""))
        _ev("orch.subtask", f"{run['id']} {sid} → {node} {model} attempt {attempt}", node=node,
            flows=[{"id": "orch.dispatch" if node == _wrk_id() else "task.local"}])
        async with sems[node]:
            run["waiting"] = f"{sid} on {node} {model}"
            r = await _chat(url, model, messages, ctx, timeout=600)
        if node == _wrk_id():
            _ev("orch.result", f"{run['id']} {sid} done on mini ({r['s']} s)", node=node, flows=[{"id": "orch.result"}])
        await act(f"  ← {sid} {node} · {model}: {r['tokens']} tok, {r['tok_s']} tok/s, {r['s']} s")
        need_review = node == _wrk_id() or use_src
        if not need_review:
            verdict = {"pass": True, "score": None, "issues": [], "method": "manager-own"}
            break
        rv_user = f"TASK:\n{st['prompt']}\n\nWORKER RESULT:\n{r['text']}"
        if use_src:
            rv_user += "\n\nSOURCES:\n" + _sources_block(sources)
        async with sems[_mgr_id()]:
            run["waiting"] = f"reviewing {sid}"
            rv = await _chat(desk_url, mm, [{"role": "system", "content": REVIEW_SYS.replace("{today}", _today())}, {"role": "user", "content": rv_user}],
                             OC["desktop_ctx"], fmt="json", temp=0.0, timeout=180)
        verdict = _jparse(rv["text"]) or {"pass": True, "issues": ["reviewer returned no JSON"]}
        verdict["pass"] = bool(verdict.get("pass"))
        ok = verdict["pass"]
        unsup = verdict.get("unsupported_claims") or []
        _ev("orch.review", f"{run['id']} {sid} review {'PASS' if ok else 'FAIL'} score={verdict.get('score')}",
            node=node, flows=[{"id": "orch.review", "ok": True, "counts": {"passed": int(ok), "failed": int(not ok)}}])
        await act(f"  {'✓' if ok else '✗'} {sid} review by {mm} ({rv['s']} s): {'PASS' if ok else 'FAIL'} score {verdict.get('score')}"
                  + (f" · issues: {'; '.join(map(str, verdict.get('issues') or []))[:300]}" if not ok and verdict.get("issues") else "")
                  + (f" · unsupported: {'; '.join(map(str, unsup))[:300]}" if unsup else ""))
        if ok:
            break
        if attempt > rounds:
            await act(f"  ⚠ {sid} still failing after escalation — passing it on marked UNVERIFIED")
            break
        if attempt == rounds and node == _wrk_id():
            node, model = _mgr_id(), mm
            await act(f"  ↑ {sid} escalated to {_mgr_id()} · {mm} after {rounds} failed rounds")
            _ev("orch.escalate", f"{run['id']} {sid} escalated to desktop", flows=[{"id": "task.local"}])
        fix = ("The reviewer rejected your answer. Fix these issues:\n- " + "\n- ".join(map(str, (verdict.get("issues") or []) + unsup))
               + "\nReturn the full corrected answer." + (" Only use the SOURCES." if use_src else ""))
        messages = messages + [{"role": "assistant", "content": r["text"]}, {"role": "user", "content": fix}]
    return {"id": sid, "title": title, "kind": kind, "node": node, "model": model, "text": r.get("text", ""), "verdict": verdict,
            "s": r.get("s"), "tok_s": r.get("tok_s")}


async def _passthrough(body: dict, put, act, run, desk_url: str, mm: str) -> None:
    """Forward the full Odysseus request (incl. tool schemas) to the manager model; stream its deltas back raw."""
    up = {k: v for k, v in body.items() if k not in ("model", "stream", "reasoning_effort", "reasoning", "think")}
    up.update({"model": mm, "stream": True, "reasoning_effort": "none"})
    _ev("orch.tools", f"{run['id']} Odysseus tool turn → {mm}", flows=[{"id": "task.local"}])
    t0 = time.time()
    async with B.http.stream("POST", desk_url + "/v1/chat/completions", json=up, timeout=900) as r:
        if r.status_code >= 400:
            err = (await r.aread()).decode(errors="replace")[:300]
            raise RuntimeError(f"manager model error {r.status_code}: {err}")
        async for line in r.aiter_lines():
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if payload == "[DONE]":
                break
            try:
                j = json.loads(payload)
            except Exception:
                continue
            ch = (j.get("choices") or [{}])[0]
            d = ch.get("delta") or {}
            if d.get("reasoning") or d.get("reasoning_content"):
                d.pop("reasoning", None); d.pop("reasoning_content", None)
            if d.get("content") or d.get("tool_calls"):
                await put("raw", "data: " + json.dumps({**j, "choices": [{**ch, "delta": d, "finish_reason": ch.get("finish_reason")}]}) + "\n\n")
            elif ch.get("finish_reason") == "tool_calls":
                await put("raw", "data: " + json.dumps(j) + "\n\n")
    await act(f"✓ manager {mm} turn done ({round(time.time() - t0, 1)} s)")
    B.jlog(B.RUNS, {"id": run["id"], "ts": B.now_iso(), "source": "cluster-network", "mode": run.get("mode"), "steps": run["steps"],
                    "wall_s": round(time.time() - run["t0"], 2)})


async def _finish(run, user, text, put, act, footer=True, nodes_used=None, results=None, notify=False) -> None:
    wall = round(time.time() - run["t0"], 1)
    if footer:
        rv = [r for r in results or [] if r["verdict"].get("method") != "manager-own"]
        rv_s = f" · reviewed {sum(1 for r in rv if r['verdict'].get('pass'))}/{len(rv)} passed" if rv else ""
        await put("content", f"\n\n---\n*Cluster Network · {run.get('mode')} · {', '.join(sorted(nodes_used or []))}{rv_s} · {wall} s*")
    await act(f"■ done in {wall} s (mode {run.get('mode')})")
    _ev("orch.done", f"{run['id']} done mode={run.get('mode')} {wall} s", flows=[{"id": "orch.synth"}] if run.get("mode") == "plan" else [])
    rec = {"id": run["id"], "ts": B.now_iso(), "source": "cluster-network", "mode": run.get("mode"), "prompt": user[:2000],
           "plan": run.get("plan"), "steps": run["steps"], "wall_s": wall, "answer": text[:4000],
           "subtasks": [{k: r.get(k) for k in ("id", "title", "kind", "node", "model", "s", "tok_s")} | {"pass": r["verdict"].get("pass")}
                        for r in results or []]}
    B.jlog(B.RUNS, rec)
    if OC["store_runs"] and run.get("mode") == "plan" and text:
        async def store():
            try:
                await B.memory_store(f"Q: {user}\nA: {text}", ["cluster-network"], source=f"orchestrator:{run['id']}",
                                     collection="tasks", doc_id=run["id"])
                _ev("tool.memory", f"{run['id']} stored in llm_cluster_tasks", flows=[{"id": "tools.memory"}])
            except Exception:
                pass
        asyncio.get_running_loop().create_task(store())
    if notify:
        try:
            await B.notify(text[:700], "Cluster Network answer ready", 3, ["robot"])
            _ev("tool.notify", f"{run['id']} ntfy sent", flows=[{"id": "tools.notify"}])
            await act("🔔 ntfy notification sent")
        except Exception as e:
            await act(f"🔔 ntfy failed: {e}")
