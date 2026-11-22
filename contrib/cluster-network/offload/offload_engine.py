"""Core offload execution engine for llm-cluster-offload (:11630).

Principle: the cluster reads, chunks, maps, extracts, searches, and condenses;
the frontier model decides and can drill down on cited chunk IDs ([c001], ...).
"""
from __future__ import annotations

import asyncio
import fnmatch
import glob as pyglob
import hashlib
import html as _html
import ipaddress
import json
import logging
import os
import re
import socket
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger("offload.engine")

BASE = Path(__file__).resolve().parent
JOURNAL = BASE / "journal" / "jobs.jsonl"
JOBS_DIR = BASE / "state" / "jobs"
UPLOADS_DIR = BASE / "uploads"

_PRIVATE_NETS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("100.64.0.0/10"),   # CGNAT / Tailscale
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def est_tokens(text: str | Any) -> int:
    if not text:
        return 0
    s = text if isinstance(text, str) else json.dumps(text, ensure_ascii=False)
    return max(1, (len(s) + 3) // 4)


def _jlog(path: Path, rec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


class OffloadEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.limits = cfg.get("limits") or {}
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(20.0, read=600.0))
        self._coll_ids: dict[str, str] = {}
        self._mini_cache: tuple[float, str | None] = (0.0, None)
        self._jobs: dict[str, dict] = {}
        self._job_tasks: dict[str, asyncio.Task] = {}
        self._job_sem = asyncio.Semaphore(int(self.limits.get("max_concurrent_jobs", 4)))
        self._node_sems = {
            "desktop": asyncio.Semaphore(int(cfg["nodes"]["desktop"].get("concurrency", 2))),
            "mini": asyncio.Semaphore(int(cfg["nodes"]["mini"].get("concurrency", 2))),
        }
        JOURNAL.parent.mkdir(parents=True, exist_ok=True)
        JOBS_DIR.mkdir(parents=True, exist_ok=True)
        UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

    async def close(self) -> None:
        await self.http.aclose()

    # ------------------------------------------------------------------ manager events & node checks

    def _mgr_id(self) -> str:
        return (self.cfg.get("nodes") or {}).get("desktop", {}).get("id") or "manager-node"

    def _wrk_id(self) -> str:
        return (self.cfg.get("nodes") or {}).get("mini", {}).get("id") or "worker-node"

    def _emit_ev(self, kind: str, msg: str, node: str | None = None,
                 flows: list[str | dict] | None = None, level: str = "info", data: dict | None = None) -> None:
        async def _send() -> None:
            try:
                await self.http.post(
                    f"{self.cfg['manager']}/api/events",
                    json={"node": node or self._mgr_id(), "kind": kind, "msg": msg[:500], "level": level,
                          "flows": flows or [], "data": data},
                    timeout=3.0,
                )
            except Exception:
                pass
        try:
            asyncio.get_running_loop().create_task(_send())
        except RuntimeError:
            pass

    async def mini_busy_reason(self) -> str | None:
        now = time.time()
        if now - self._mini_cache[0] < 5.0:
            return self._mini_cache[1]
        reason: str | None = None
        try:
            r = await self.http.get(f"{self.cfg['manager']}/api/nodes/{self._wrk_id()}", timeout=3.0)
            if r.status_code < 400:
                n = r.json()
                if n.get("busy"):
                    reason = str(n["busy"])
                elif n.get("status") in ("down", "unreachable"):
                    reason = f"worker is {n.get('status')}"
                elif not (n.get("ollama") or {}).get("up", True):
                    reason = "worker Ollama down"
        except Exception as e:
            # Manager unreachable: probe mini Ollama directly
            try:
                r2 = await self.http.get(f"{self.cfg['nodes']['mini']['ollama']}/api/version", timeout=2.0)
                if r2.status_code >= 400:
                    reason = f"mini Ollama HTTP {r2.status_code}"
            except Exception:
                reason = f"mini unreachable ({type(e).__name__})"
        self._mini_cache = (now, reason)
        return reason

    # ------------------------------------------------------------------ allowlisted paths, uploads, URLs

    def _is_denied(self, p: Path) -> bool:
        sp = str(p)
        for pat in self.cfg.get("path_denylist_globs") or []:
            if fnmatch.fnmatch(sp, pat) or fnmatch.fnmatch(p.name, pat):
                return True
        return False

    def _is_allowed_path(self, p: Path) -> bool:
        try:
            rp = p.resolve(strict=True)
        except OSError:
            return False
        if self._is_denied(rp):
            return False
        for root in self.cfg.get("path_allowlist") or []:
            try:
                rr = Path(os.path.expanduser(root)).resolve(strict=True)
                if rp == rr or rp.is_relative_to(rr):
                    return True
            except OSError:
                continue
        return False

    def _read_local_files(self, paths: list[str] | None, glob_pat: str | None) -> list[dict]:
        candidates: list[Path] = []
        for raw in paths or []:
            p = Path(os.path.expanduser(str(raw)))
            if not self._is_allowed_path(p):
                raise ValueError(f"path not in allowlist or denied: {raw}")
            if p.is_dir():
                for sub in sorted(p.rglob("*")):
                    if sub.is_file() and self._is_allowed_path(sub):
                        candidates.append(sub)
            elif p.is_file():
                candidates.append(p)
        if glob_pat:
            pat_exp = os.path.expanduser(str(glob_pat))
            # Must anchor inside an allowlisted directory
            matched = sorted(Path(m) for m in pyglob.glob(pat_exp, recursive=True))
            if not matched:
                # Try matching relative to allowlist roots
                for root in self.cfg.get("path_allowlist") or []:
                    rr = Path(os.path.expanduser(root))
                    matched.extend(sorted(rr.glob(pat_exp)))
            for m in matched:
                if m.is_file() and self._is_allowed_path(m):
                    candidates.append(m)
        seen: set[Path] = set()
        out: list[dict] = []
        total_chars = 0
        max_chars = int(self.limits.get("max_input_chars", 2_000_000))
        for p in candidates:
            rp = p.resolve()
            if rp in seen:
                continue
            seen.add(rp)
            try:
                raw_bytes = rp.read_bytes()
            except OSError:
                continue
            if b"\x00" in raw_bytes[:4096]:
                continue  # skip binary files
            txt = raw_bytes.decode("utf-8", errors="replace")
            if not txt.strip():
                continue
            if total_chars + len(txt) > max_chars:
                rem = max_chars - total_chars
                if rem <= 200:
                    break
                txt = txt[:rem]
            total_chars += len(txt)
            out.append({"source": str(rp), "content": txt})
            if total_chars >= max_chars:
                break
        return out

    def save_upload(self, files: list[dict]) -> dict:
        self._gc_uploads()
        max_up = int(self.limits.get("max_upload_bytes", 5_242_880))
        uid = "up-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
        udir = UPLOADS_DIR / uid
        udir.mkdir(parents=True, exist_ok=True)
        total = 0
        saved: list[dict] = []
        for idx, item in enumerate(files):
            name = re.sub(r"[^a-zA-Z0-9._/-]+", "_", str(item.get("name") or f"file_{idx}.txt")).lstrip("/")
            name = name.replace("..", "_")
            content = str(item.get("content") or "")
            total += len(content.encode("utf-8"))
            if total > max_up:
                raise ValueError(f"upload exceeds max_upload_bytes ({max_up})")
            dest = (udir / Path(name).name)
            dest.write_text(content, encoding="utf-8")
            saved.append({"name": name, "chars": len(content)})
        (udir / "_manifest.json").write_text(json.dumps({"id": uid, "ts": now_iso(), "files": saved}), encoding="utf-8")
        return {"upload_id": uid, "files": saved, "total_bytes": total}

    def _load_uploads(self, upload_ids: list[str] | None) -> list[dict]:
        out: list[dict] = []
        for uid in upload_ids or []:
            if not re.fullmatch(r"up-[0-9-]+-[a-f0-9]+", str(uid)):
                raise ValueError(f"invalid upload_id: {uid}")
            udir = UPLOADS_DIR / uid
            man = udir / "_manifest.json"
            if not man.exists():
                raise ValueError(f"unknown or expired upload_id: {uid}")
            info = json.loads(man.read_text(encoding="utf-8"))
            for f in info.get("files") or []:
                p = udir / Path(f["name"]).name
                if p.exists():
                    out.append({"source": f"upload:{uid}/{f['name']}", "content": p.read_text(encoding="utf-8")})
        return out

    def _gc_uploads(self) -> None:
        ttl = int(self.limits.get("upload_ttl_s", 86400))
        cutoff = time.time() - ttl
        for d in UPLOADS_DIR.iterdir():
            if d.is_dir() and d.stat().st_mtime < cutoff:
                for f in d.iterdir():
                    f.unlink(missing_ok=True)
                d.rmdir()

    async def _fetch_url(self, url: str) -> dict:
        from urllib.parse import urlparse
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            raise ValueError("url must be http or https with a valid host")
        infos = await asyncio.get_running_loop().getaddrinfo(u.hostname, None)
        for fam, _, _, _, sockaddr in infos:
            ip = ipaddress.ip_address(sockaddr[0].split("%")[0])
            if any(ip in net for net in _PRIVATE_NETS) or not ip.is_global:
                raise ValueError(f"url host resolves to disallowed IP {ip}")
        max_b = int(self.limits.get("url_max_bytes", 2_097_152))
        r = await self.http.get(url, follow_redirects=False, timeout=20.0,
                                headers={"User-Agent": "llm-cluster-offload/1.0"})
        r.raise_for_status()
        raw = r.content[:max_b]
        ct = (r.headers.get("content-type") or "").lower()
        txt = raw.decode("utf-8", errors="replace")
        if "html" in ct:
            txt = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", txt)
            txt = re.sub(r"(?s)<[^>]+>", " ", txt)
            txt = re.sub(r"\s+", " ", _html.unescape(txt)).strip()
        return {"source": url, "content": txt}

    # ------------------------------------------------------------------ chunking

    def _chunk_sources(self, sources: list[dict], chunk_chars: int, overlap: int = 200) -> list[dict]:
        max_chunks = int(self.limits.get("max_chunks", 48))
        max_input = int(self.limits.get("max_input_chars", 2_000_000))
        chunks: list[dict] = []
        total_chars = 0
        cid_num = 0
        for src in sources:
            text = str(src.get("content") or "")
            name = str(src.get("source") or "inline")
            if not text.strip():
                continue
            if total_chars + len(text) > max_input:
                text = text[: max(0, max_input - total_chars)]
            total_chars += len(text)
            if len(text) <= chunk_chars:
                cid_num += 1
                lines = text.count("\n") + 1
                chunks.append({
                    "id": f"c{cid_num:03d}", "source": name,
                    "line_start": 1, "line_end": lines, "chars": len(text), "text": text,
                })
            else:
                lines_list = text.splitlines(keepends=True)
                cur: list[str] = []
                cur_len = 0
                start_line = 1
                cur_line = 1
                for line in lines_list:
                    # Handle giant single lines safely
                    if len(line) > chunk_chars and not cur:
                        for pos in range(0, len(line), chunk_chars - overlap):
                            cid_num += 1
                            seg = line[pos: pos + chunk_chars]
                            chunks.append({
                                "id": f"c{cid_num:03d}", "source": name,
                                "line_start": cur_line, "line_end": cur_line, "chars": len(seg), "text": seg,
                            })
                            if len(chunks) >= max_chunks:
                                return chunks
                        cur_line += 1
                        start_line = cur_line
                        continue
                    cur.append(line)
                    cur_len += len(line)
                    if cur_len >= chunk_chars:
                        cid_num += 1
                        seg = "".join(cur)
                        chunks.append({
                            "id": f"c{cid_num:03d}", "source": name,
                            "line_start": start_line, "line_end": cur_line, "chars": len(seg), "text": seg,
                        })
                        if len(chunks) >= max_chunks:
                            return chunks
                        # Carry tail overlap lines
                        ov: list[str] = []
                        ov_len = 0
                        for prev in reversed(cur):
                            if ov_len + len(prev) > overlap:
                                break
                            ov.insert(0, prev)
                            ov_len += len(prev)
                        cur = ov
                        cur_len = ov_len
                        start_line = max(1, cur_line - len(ov) + 1)
                    cur_line += 1
                if cur:
                    cid_num += 1
                    seg = "".join(cur)
                    chunks.append({
                        "id": f"c{cid_num:03d}", "source": name,
                        "line_start": start_line, "line_end": cur_line - 1, "chars": len(seg), "text": seg,
                    })
            if len(chunks) >= max_chunks:
                break
        return chunks

    async def _collect_chunks(self, params: dict, chunk_chars: int) -> list[dict]:
        sources: list[dict] = []
        if params.get("chunks"):
            raw_chunks = params["chunks"]
            out = []
            max_chunks = int(self.limits.get("max_chunks", 48))
            for i, ch in enumerate(raw_chunks[:max_chunks], 1):
                if isinstance(ch, dict):
                    txt = str(ch.get("content") or ch.get("text") or "")
                    cid = str(ch.get("id") or f"c{i:03d}")
                    src = str(ch.get("source") or f"chunk:{i}")
                else:
                    txt = str(ch)
                    cid = f"c{i:03d}"
                    src = f"chunk:{i}"
                if txt.strip():
                    out.append({"id": cid, "source": src, "line_start": 1, "line_end": txt.count("\n") + 1,
                                "chars": len(txt), "text": txt})
            if out:
                return out
        if params.get("text"):
            sources.append({"source": str(params.get("source") or "inline"), "content": str(params["text"])})
        if params.get("diff"):
            sources.append({"source": str(params.get("source") or "diff"), "content": str(params["diff"])})
        for idx, f in enumerate(params.get("files") or []):
            if isinstance(f, dict) and (f.get("content") is not None):
                sources.append({"source": str(f.get("name") or f.get("path") or f"file_{idx}"),
                                "content": str(f["content"])})
        if params.get("upload_ids"):
            sources.extend(self._load_uploads(params["upload_ids"]))
        if params.get("paths") or params.get("path") or params.get("glob"):
            paths = list(params.get("paths") or [])
            if params.get("path"):
                paths.append(params["path"])
            sources.extend(self._read_local_files(paths, params.get("glob")))
        if params.get("url"):
            sources.append(await self._fetch_url(str(params["url"])))
        if not sources:
            raise ValueError("no input provided (pass text, chunks, files, upload_ids, url, or allowlisted paths/glob)")
        chunks = self._chunk_sources(sources, chunk_chars, int(self.limits.get("chunk_overlap_chars", 200)))
        if not chunks:
            raise ValueError("inputs were empty after filtering")
        return chunks

    # ------------------------------------------------------------------ Ollama calls

    async def _ollama_chat(self, node_key: str, model: str, messages: list[dict],
                           num_predict: int | None = None, fmt: str | None = None,
                           temp: float = 0.2, timeout: float = 300.0) -> dict:
        nc = self.cfg["nodes"][node_key]
        opts: dict[str, Any] = {"num_ctx": nc["num_ctx"], "temperature": temp}
        if num_predict:
            opts["num_predict"] = int(num_predict)
        body: dict[str, Any] = {
            "model": model, "messages": messages, "stream": False,
            "think": False, "keep_alive": "5m", "options": opts,
        }
        if fmt:
            body["format"] = fmt
        async with self._node_sems[node_key]:
            t0 = time.time()
            r = await self.http.post(f"{nc['ollama']}/api/chat", json=body, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            wall = round(time.time() - t0, 3)
            return {
                "node_key": node_key,
                "node_id": nc["id"],
                "model": model,
                "text": (j.get("message") or {}).get("content") or "",
                "prompt_tokens": int(j.get("prompt_eval_count") or 0),
                "eval_tokens": int(j.get("eval_count") or 0),
                "wall_s": wall,
            }

    async def _parallel_map(self, chunks: list[dict], prompt_fn, kind: str = "text",
                            prefer_node: str = "auto", fmt: str | None = None,
                            num_predict: int = 450, progress_cb=None) -> list[dict]:
        """Run per-chunk LLM calls in parallel across desktop + mini (if healthy/free)."""
        mini_busy = await self.mini_busy_reason() if prefer_node != "desktop" else "prefer_node=desktop"
        use_mini = (prefer_node in ("auto", "mini")) and (mini_busy is None)
        use_desktop = prefer_node in ("auto", "desktop") or not use_mini

        q: asyncio.Queue[int] = asyncio.Queue()
        for idx in range(len(chunks)):
            await q.put(idx)

        results: list[dict | None] = [None] * len(chunks)
        done_count = 0
        lock = asyncio.Lock()

        def _model_for(node_key: str) -> str:
            nc = self.cfg["nodes"][node_key]
            if kind == "code":
                return nc["code_model"] if node_key == "mini" else nc["text_model"]  # keep 30B for dedicated code_review
            return nc["text_model"]

        async def worker(node_key: str) -> None:
            nonlocal done_count
            model = _model_for(node_key)
            while True:
                try:
                    idx = q.get_nowait()
                except asyncio.QueueEmpty:
                    return
                ch = chunks[idx]
                msgs = prompt_fn(ch)
                try:
                    if node_key == "mini":
                        self._emit_ev("offload.map", f"chunk {ch['id']} -> {self._wrk_id()} ({model})",
                                      node=self._wrk_id(), flows=["offload.map"])
                    res = await self._ollama_chat(node_key, model, msgs, num_predict=num_predict, fmt=fmt)
                    results[idx] = {"ok": True, "chunk": ch, **res}
                    if node_key == "mini":
                        self._emit_ev("offload.reduce", f"chunk {ch['id']} <- {self._wrk_id()} ({res['wall_s']}s)",
                                      node=self._wrk_id(), flows=[{"id": "offload.reduce", "latency_ms": res["wall_s"] * 1000}])
                except Exception as e:
                    # Fallback once to desktop if mini failed
                    if node_key == "mini":
                        try:
                            fb_model = _model_for("desktop")
                            res = await self._ollama_chat("desktop", fb_model, msgs, num_predict=num_predict, fmt=fmt)
                            results[idx] = {"ok": True, "chunk": ch, "fallback": f"mini failed ({e})", **res}
                        except Exception as e2:
                            results[idx] = {"ok": False, "chunk": ch, "error": str(e2),
                                            "node_key": "desktop", "node_id": self._mgr_id(), "model": fb_model,
                                            "text": "", "prompt_tokens": 0, "eval_tokens": 0, "wall_s": 0.0}
                    else:
                        results[idx] = {"ok": False, "chunk": ch, "error": str(e),
                                        "node_key": node_key, "node_id": self.cfg["nodes"][node_key]["id"], "model": model,
                                        "text": "", "prompt_tokens": 0, "eval_tokens": 0, "wall_s": 0.0}
                finally:
                    async with lock:
                        done_count += 1
                        if progress_cb:
                            progress_cb(done_count, len(chunks))
                    q.task_done()

        workers: list[asyncio.Task] = []
        if use_desktop:
            for _ in range(int(self.cfg["nodes"]["desktop"].get("concurrency", 2))):
                workers.append(asyncio.create_task(worker("desktop")))
        if use_mini:
            for _ in range(int(self.cfg["nodes"]["mini"].get("concurrency", 2))):
                workers.append(asyncio.create_task(worker("mini")))
        await asyncio.gather(*workers)
        return [r for r in results if r is not None]

    # ------------------------------------------------------------------ Accounting & Recording

    def _build_accounting(self, raw_chars: int, calls: list[dict], result_obj: Any, wall_s: float) -> dict:
        raw_in_tok = max(1, (raw_chars + 3) // 4)
        local_in = sum(int(c.get("prompt_tokens") or 0) for c in calls)
        local_out = sum(int(c.get("eval_tokens") or 0) for c in calls)
        ret_tok = est_tokens(result_obj)
        saved = max(0, raw_in_tok - ret_tok)
        savings_pct = round((saved / raw_in_tok) * 100.0, 1) if raw_in_tok else 0.0
        node_time: dict[str, float] = {}
        models: list[str] = []
        for c in calls:
            nid = c.get("node_id") or self._mgr_id()
            node_time[nid] = round(node_time.get(nid, 0.0) + float(c.get("wall_s") or 0.0), 2)
            m = f"{nid}/{c.get('model')}"
            if c.get("model") and m not in models:
                models.append(m)
        return {
            "raw_input_tokens_est": raw_in_tok,
            "local_input_tokens": local_in or raw_in_tok,
            "local_output_tokens": local_out,
            "returned_output_tokens": ret_tok,
            "frontier_tokens_saved_est": saved,
            "savings_pct": savings_pct,
            "wall_s": round(wall_s, 2),
            "node_time_s": node_time,
            "models_used": models,
        }

    def _record_job(self, job_id: str, tool: str, status: str, chunks: list[dict],
                    resp: dict, error: str | None = None) -> None:
        acc = resp.get("accounting") or {}
        cov = resp.get("coverage") or {}
        rec = {
            "job_id": job_id,
            "ts": now_iso(),
            "tool": tool,
            "status": status,
            "chunks_total": cov.get("chunks_total", len(chunks)),
            "chunks_processed": cov.get("chunks_processed", 0),
            "chunks_failed": cov.get("chunks_failed", 0),
            "raw_input_tokens_est": acc.get("raw_input_tokens_est", 0),
            "local_input_tokens": acc.get("local_input_tokens", 0),
            "local_output_tokens": acc.get("local_output_tokens", 0),
            "returned_output_tokens": acc.get("returned_output_tokens", 0),
            "frontier_tokens_saved_est": acc.get("frontier_tokens_saved_est", 0),
            "savings_pct": acc.get("savings_pct", 0.0),
            "wall_s": acc.get("wall_s", 0.0),
            "node_time_s": acc.get("node_time_s", {}),
            "models_used": acc.get("models_used", []),
            "confidence": resp.get("confidence"),
            "error": error,
        }
        _jlog(JOURNAL, rec)
        # Save full state (including raw chunks for drill-down)
        try:
            self._gc_jobs()
            (JOBS_DIR / f"{job_id}.json").write_text(
                json.dumps({"job_id": job_id, "ts": rec["ts"], "tool": tool, "status": status,
                            "response": resp, "chunks": chunks}, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError:
            pass

    def _gc_jobs(self) -> None:
        ttl = int(self.limits.get("job_ttl_s", 86400))
        cutoff = time.time() - ttl
        for f in JOBS_DIR.glob("*.json"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink(missing_ok=True)
            except OSError:
                pass

    # ------------------------------------------------------------------ Tool implementations

    async def summarize(self, params: dict, job_id: str | None = None, progress_cb=None) -> dict:
        t0 = time.time()
        jid = job_id or ("off-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4])
        max_tokens = max(80, min(int(params.get("max_tokens") or 600), 4000))
        focus = str(params.get("focus") or "").strip()
        chunks = await self._collect_chunks(params, int(self.limits.get("chunk_chars_text", 6000)))
        raw_chars = sum(c["chars"] for c in chunks)
        self._emit_ev("offload.start", f"{jid} summarize ({len(chunks)} chunks, {raw_chars} chars)",
                      flows=["offload.ingest"])

        calls: list[dict] = []
        if len(chunks) == 1:
            ch = chunks[0]
            sys_msg = (
                "You condense raw text/logs for a frontier reasoning agent. Be dense, factual, and concise. "
                f"Cite [{ch['id']}] for key claims. Keep output under ~{max_tokens} tokens."
            )
            user_msg = (f"Focus: {focus}\n\n" if focus else "") + f"[{ch['id']} | {ch['source']} L{ch['line_start']}-{ch['line_end']}]\n{ch['text']}"
            r = await self._ollama_chat("desktop", self.cfg["nodes"]["desktop"]["text_model"],
                                        [{"role": "system", "content": sys_msg}, {"role": "user", "content": user_msg}],
                                        num_predict=max_tokens)
            calls.append({**r, "ok": True, "chunk": ch})
            final_text = r["text"].strip()
            failed = 0
        else:
            def map_prompt(ch: dict) -> list[dict]:
                return [
                    {"role": "system",
                     "content": "Extract the essential facts, errors, numbers, decisions, and structure from this chunk. "
                                f"Always tag facts with [{ch['id']}]. Be terse (bullet points)."},
                    {"role": "user",
                     "content": (f"Focus: {focus}\n\n" if focus else "") +
                                f"[{ch['id']} | {ch['source']} L{ch['line_start']}-{ch['line_end']}]\n{ch['text']}"},
                ]
            map_res = await self._parallel_map(chunks, map_prompt, kind="text",
                                               prefer_node=str(params.get("prefer_node") or "auto"),
                                               num_predict=380, progress_cb=progress_cb)
            calls.extend(map_res)
            failed = sum(1 for r in map_res if not r["ok"])
            ok_notes = "\n\n".join(
                f"--- [{r['chunk']['id']}] ({r['chunk']['source']} L{r['chunk']['line_start']}-{r['chunk']['line_end']}) ---\n{r['text'].strip()}"
                for r in map_res if r["ok"] and r["text"].strip()
            )
            if not ok_notes:
                raise RuntimeError("all map chunks failed")
            red_sys = (
                "You are reducing chunk summaries into one compact briefing for a frontier agent. "
                "Preserve [cNNN] chunk citations so the frontier agent can drill down if needed. "
                f"Stay concise (<= {max_tokens} tokens). Do not invent facts outside the chunk notes."
            )
            red_user = (f"Focus: {focus}\n\n" if focus else "") + f"Chunk summaries ({len(chunks) - failed}/{len(chunks)}):\n\n{ok_notes[:24000]}"
            red = await self._ollama_chat("desktop", self.cfg["nodes"]["desktop"]["text_model"],
                                          [{"role": "system", "content": red_sys}, {"role": "user", "content": red_user}],
                                          num_predict=max_tokens)
            calls.append({**red, "ok": True})
            final_text = red["text"].strip()

        wall = time.time() - t0
        cov_pct = round(((len(chunks) - failed) / len(chunks)) * 100.0, 1)
        citations = [{"id": c["id"], "source": c["source"], "lines": f"{c['line_start']}-{c['line_end']}", "chars": c["chars"]}
                     for c in chunks]
        acc = self._build_accounting(raw_chars, calls, final_text, wall)
        resp = {
            "job_id": jid,
            "tool": "summarize",
            "status": "done",
            "result": final_text,
            "citations": citations if params.get("include_all_citations", True) else citations[:20],
            "coverage": {
                "chunks_total": len(chunks),
                "chunks_processed": len(chunks) - failed,
                "chunks_failed": failed,
                "coverage_pct": cov_pct,
                "truncated_to_max_tokens": est_tokens(final_text) >= int(max_tokens * 0.92),
            },
            "confidence": "high" if failed == 0 else ("medium" if cov_pct >= 70 else "low"),
            "confidence_note": f"Processed {len(chunks) - failed}/{len(chunks)} chunks; cite [cNNN] + offload_get_chunks for raw text.",
            "accounting": acc,
        }
        if params.get("include_chunks"):
            resp["chunks"] = [{"id": c["id"], "source": c["source"], "text": c["text"]} for c in chunks]
        self._record_job(jid, "summarize", "done", chunks, resp)
        self._emit_ev("offload.done", f"{jid} summarize saved ~{acc['frontier_tokens_saved_est']} tok in {acc['wall_s']}s",
                      flows=[{"id": "offload.return", "latency_ms": acc["wall_s"] * 1000}])
        return resp

    async def map_reduce(self, params: dict, job_id: str | None = None, progress_cb=None) -> dict:
        t0 = time.time()
        jid = job_id or ("off-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4])
        instruction = str(params.get("instruction") or "").strip()
        if not instruction:
            raise ValueError("instruction is required")
        reduce_inst = str(params.get("reduce_instruction") or "Merge all chunk findings concisely, deduplicating and preserving [cNNN] citations.").strip()
        max_tokens = max(80, min(int(params.get("max_tokens") or 800), 4000))
        chunks = await self._collect_chunks(params, int(self.limits.get("chunk_chars_text", 6000)))
        raw_chars = sum(c["chars"] for c in chunks)
        self._emit_ev("offload.start", f"{jid} map_reduce ({len(chunks)} chunks)", flows=["offload.ingest"])

        def map_prompt(ch: dict) -> list[dict]:
            return [
                {"role": "system",
                 "content": f"Apply the user's instruction ONLY to this chunk. Tag claims with [{ch['id']}]. "
                            "If nothing in this chunk matches the instruction, reply exactly: NONE."},
                {"role": "user",
                 "content": f"Instruction: {instruction}\n\n[{ch['id']} | {ch['source']} L{ch['line_start']}-{ch['line_end']}]\n{ch['text']}"},
            ]

        map_res = await self._parallel_map(chunks, map_prompt, kind=str(params.get("kind") or "text"),
                                           prefer_node=str(params.get("prefer_node") or "auto"),
                                           num_predict=400, progress_cb=progress_cb)
        failed = sum(1 for r in map_res if not r["ok"])
        kept = [r for r in map_res if r["ok"] and r["text"].strip() and r["text"].strip().upper() != "NONE"]
        if kept:
            notes = "\n\n".join(f"[{r['chunk']['id']} | {r['chunk']['source']}]\n{r['text'].strip()}" for r in kept)
            red = await self._ollama_chat(
                "desktop", self.cfg["nodes"]["desktop"]["text_model"],
                [{"role": "system", "content": f"Synthesize these map results for a frontier agent. Keep [cNNN] citations. Max ~{max_tokens} tokens."},
                 {"role": "user", "content": f"Task: {instruction}\nReduce instruction: {reduce_inst}\n\nMap outputs ({len(kept)} relevant of {len(chunks)}):\n\n{notes[:24000]}"}],
                num_predict=max_tokens,
            )
            calls = map_res + [{**red, "ok": True}]
            final_text = red["text"].strip()
        else:
            calls = map_res
            final_text = "No matching findings across the processed chunks."

        wall = time.time() - t0
        cov_pct = round(((len(chunks) - failed) / len(chunks)) * 100.0, 1)
        acc = self._build_accounting(raw_chars, calls, final_text, wall)
        resp = {
            "job_id": jid,
            "tool": "map_reduce",
            "status": "done",
            "result": final_text,
            "map_hits": len(kept),
            "citations": [{"id": c["id"], "source": c["source"], "lines": f"{c['line_start']}-{c['line_end']}", "chars": c["chars"]}
                          for c in chunks],
            "coverage": {
                "chunks_total": len(chunks),
                "chunks_processed": len(chunks) - failed,
                "chunks_failed": failed,
                "relevant_chunks": len(kept),
                "coverage_pct": cov_pct,
                "truncated_to_max_tokens": est_tokens(final_text) >= int(max_tokens * 0.92),
            },
            "confidence": "high" if failed == 0 else ("medium" if cov_pct >= 70 else "low"),
            "confidence_note": f"{len(chunks) - failed}/{len(chunks)} chunks mapped ({len(kept)} matched); reduced on manager.",
            "accounting": acc,
        }
        self._record_job(jid, "map_reduce", "done", chunks, resp)
        self._emit_ev("offload.done", f"{jid} map_reduce saved ~{acc['frontier_tokens_saved_est']} tok in {acc['wall_s']}s",
                      flows=[{"id": "offload.return", "latency_ms": acc["wall_s"] * 1000}])
        return resp

    async def extract(self, params: dict, job_id: str | None = None, progress_cb=None) -> dict:
        t0 = time.time()
        jid = job_id or ("off-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4])
        schema = params.get("schema")
        if not schema:
            raise ValueError("schema is required (JSON schema object or field mapping)")
        schema_str = json.dumps(schema, ensure_ascii=False) if not isinstance(schema, str) else schema
        instruction = str(params.get("instruction") or "Extract matching records according to the schema.").strip()
        max_tokens = max(120, min(int(params.get("max_tokens") or 1000), 4000))
        # Support documents=[...] as alias for chunks
        p2 = dict(params)
        if params.get("documents") and not params.get("chunks"):
            p2["chunks"] = params["documents"]
        chunks = await self._collect_chunks(p2, int(self.limits.get("chunk_chars_text", 6000)))
        raw_chars = sum(c["chars"] for c in chunks)
        self._emit_ev("offload.start", f"{jid} extract ({len(chunks)} docs/chunks)", flows=["offload.ingest"])

        def map_prompt(ch: dict) -> list[dict]:
            return [
                {"role": "system",
                 "content": "Extract structured JSON matching the schema from the document. "
                            "Reply with JSON ONLY: {\"records\": [...], \"notes\": \"...\"}. Use null for missing fields."},
                {"role": "user",
                 "content": f"Instruction: {instruction}\nSchema: {schema_str}\n\n"
                            f"[{ch['id']} | {ch['source']}]\n{ch['text']}"},
            ]

        map_res = await self._parallel_map(chunks, map_prompt, kind="text",
                                           prefer_node=str(params.get("prefer_node") or "auto"),
                                           fmt="json", num_predict=500, progress_cb=progress_cb)
        failed = 0
        parse_ok = 0
        records: list[dict] = []
        for r in map_res:
            if not r["ok"]:
                failed += 1
                continue
            try:
                parsed = json.loads(r["text"])
                parse_ok += 1
                recs = parsed.get("records") if isinstance(parsed, dict) and isinstance(parsed.get("records"), list) else (
                    parsed if isinstance(parsed, list) else [parsed]
                )
                for rec in recs:
                    if isinstance(rec, dict):
                        rec.setdefault("_chunk_id", r["chunk"]["id"])
                        rec.setdefault("_source", r["chunk"]["source"])
                        records.append(rec)
            except Exception:
                failed += 1

        # Trim records if they exceed max_tokens budget
        trimmed = False
        kept_records: list[dict] = []
        budget_chars = max_tokens * 4
        used_chars = 0
        for rec in records:
            s = json.dumps(rec, ensure_ascii=False)
            if used_chars + len(s) > budget_chars and kept_records:
                trimmed = True
                break
            kept_records.append(rec)
            used_chars += len(s)

        result_obj = {"records": kept_records, "total_extracted": len(records), "truncated": trimmed}
        wall = time.time() - t0
        cov_pct = round((parse_ok / len(chunks)) * 100.0, 1)
        acc = self._build_accounting(raw_chars, map_res, result_obj, wall)
        resp = {
            "job_id": jid,
            "tool": "extract",
            "status": "done",
            "result": result_obj,
            "citations": [{"id": c["id"], "source": c["source"], "lines": f"{c['line_start']}-{c['line_end']}", "chars": c["chars"]}
                          for c in chunks],
            "coverage": {
                "chunks_total": len(chunks),
                "chunks_processed": parse_ok,
                "chunks_failed": failed,
                "coverage_pct": cov_pct,
                "truncated_to_max_tokens": trimmed,
            },
            "confidence": "high" if failed == 0 else ("medium" if cov_pct >= 70 else "low"),
            "confidence_note": f"Valid JSON extracted from {parse_ok}/{len(chunks)} chunks ({len(records)} records).",
            "accounting": acc,
        }
        self._record_job(jid, "extract", "done", chunks, resp)
        self._emit_ev("offload.done", f"{jid} extract saved ~{acc['frontier_tokens_saved_est']} tok in {acc['wall_s']}s",
                      flows=[{"id": "offload.return", "latency_ms": acc["wall_s"] * 1000}])
        return resp

    async def code_review(self, params: dict, job_id: str | None = None, progress_cb=None) -> dict:
        t0 = time.time()
        jid = job_id or ("off-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4])
        focus = str(params.get("focus") or "bugs, logic errors, security issues, edge cases, resource leaks").strip()
        max_tokens = max(120, min(int(params.get("max_tokens") or 800), 4000))
        chunks = await self._collect_chunks(params, int(self.limits.get("chunk_chars_code", 10000)))
        raw_chars = sum(c["chars"] for c in chunks)
        self._emit_ev("offload.start", f"{jid} code_review ({len(chunks)} chunks, {raw_chars} chars)", flows=["offload.ingest"])

        coder_model = self.cfg["nodes"]["desktop"]["code_model"]   # qwen3-coder-30b:latest
        checker_model = self.cfg["nodes"]["desktop"]["text_model"] # gemma4:12b

        # First-pass review with qwen3-coder-30b
        first_pass_notes: list[str] = []
        calls: list[dict] = []
        failed = 0
        for idx, ch in enumerate(chunks):
            r = await self._ollama_chat(
                "desktop", coder_model,
                [{"role": "system",
                  "content": f"You are a senior code reviewer performing a first-pass review. Focus on: {focus}. "
                             f"Cite [{ch['id']}] and file/line numbers. Skip trivial style nitpicks. Be concise."},
                 {"role": "user",
                  "content": f"[{ch['id']} | {ch['source']} L{ch['line_start']}-{ch['line_end']}]\n{ch['text']}"}],
                num_predict=600, timeout=420.0,
            )
            calls.append({**r, "ok": True, "chunk": ch})
            first_pass_notes.append(f"=== [{ch['id']}] {ch['source']} ===\n{r['text'].strip()}")
            if progress_cb:
                progress_cb(idx + 1, len(chunks) + 1)

        # Cross-check pass with gemma4:12b
        combined_notes = "\n\n".join(first_pass_notes)
        code_ref = "\n\n".join(f"[{c['id']}] ({c['source']}):\n{c['text'][:4000]}" for c in chunks[:4])
        xcheck = await self._ollama_chat(
            "desktop", checker_model,
            [{"role": "system",
              "content": "You are the second-pass reviewer (gemma4) cross-checking candidate findings from qwen3-coder-30b. "
                         "Filter out false positives, confirm real bugs/risks, assign severity [HIGH|MED|LOW], "
                         f"preserve [cNNN] citations, and output a crisp summary <= {max_tokens} tokens."},
             {"role": "user",
              "content": f"Focus: {focus}\n\nFirst-pass findings (qwen3-coder-30b):\n{combined_notes[:16000]}\n\n"
                         f"Code excerpts for verification:\n{code_ref[:12000]}"}],
            num_predict=max_tokens, timeout=300.0,
        )
        calls.append({**xcheck, "ok": True})
        if progress_cb:
            progress_cb(len(chunks) + 1, len(chunks) + 1)

        final_text = xcheck["text"].strip()
        wall = time.time() - t0
        acc = self._build_accounting(raw_chars, calls, final_text, wall)
        resp = {
            "job_id": jid,
            "tool": "code_review",
            "status": "done",
            "result": final_text,
            "first_pass_preview": combined_notes[:1200] if params.get("include_first_pass") else None,
            "citations": [{"id": c["id"], "source": c["source"], "lines": f"{c['line_start']}-{c['line_end']}", "chars": c["chars"]}
                          for c in chunks],
            "coverage": {
                "chunks_total": len(chunks),
                "chunks_processed": len(chunks) - failed,
                "chunks_failed": failed,
                "coverage_pct": 100.0,
                "truncated_to_max_tokens": est_tokens(final_text) >= int(max_tokens * 0.92),
            },
            "confidence": "high",
            "confidence_note": f"First-pass by {coder_model} across {len(chunks)} chunk(s), cross-checked by {checker_model}.",
            "accounting": acc,
        }
        if resp["first_pass_preview"] is None:
            resp.pop("first_pass_preview", None)
        self._record_job(jid, "code_review", "done", chunks, resp)
        self._emit_ev("offload.done", f"{jid} code_review saved ~{acc['frontier_tokens_saved_est']} tok in {acc['wall_s']}s",
                      flows=[{"id": "offload.return", "latency_ms": acc["wall_s"] * 1000}])
        return resp

    # ------------------------------------------------------------------ Memory & Embeddings

    def _cpath(self, suffix: str = "") -> str:
        return f"{self.cfg['chromadb']}/api/v2/tenants/default_tenant/databases/default_database/collections{suffix}"

    async def _collection_id(self, name: str) -> str:
        if name not in self._coll_ids:
            r = await self.http.post(
                self._cpath(),
                json={"name": name, "get_or_create": True,
                      "metadata": {"hnsw:space": "cosine", "owner": "llm-cluster",
                                   "embedding_model": self.cfg["embed"]["model"]}},
                timeout=15.0,
            )
            r.raise_for_status()
            self._coll_ids[name] = r.json()["id"]
        return self._coll_ids[name]

    async def embed(self, params: dict) -> dict:
        t0 = time.time()
        jid = "off-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
        texts = [str(t)[:8000] for t in (params.get("texts") or [])][:32]
        if not texts:
            raise ValueError("texts (non-empty list of strings) is required")
        kind = "query" if params.get("kind") == "query" else "document"
        pre = "search_query: " if kind == "query" else "search_document: "
        e = self.cfg["embed"]
        self._emit_ev("offload.start", f"{jid} embed ({len(texts)} texts)", flows=["offload.ingest", "offload.memory"])
        r = await self.http.post(
            f"{e['url']}/api/embed",
            json={"model": e["model"], "input": [pre + t for t in texts], "keep_alive": e.get("keep_alive", "30s")},
            timeout=120.0,
        )
        r.raise_for_status()
        j = r.json()
        embs = j.get("embeddings") or []
        wall = time.time() - t0
        raw_chars = sum(len(t) for t in texts)
        in_tok = int(j.get("prompt_eval_count") or max(1, raw_chars // 4))
        resp = {
            "job_id": jid,
            "tool": "embed",
            "status": "done",
            "model": e["model"],
            "dims": len(embs[0]) if embs else 0,
            "count": len(embs),
            "embeddings": embs,
            "accounting": {
                "raw_input_tokens_est": in_tok,
                "local_input_tokens": in_tok,
                "local_output_tokens": 0,
                "returned_output_tokens": 0,
                "frontier_tokens_saved_est": in_tok,
                "savings_pct": 100.0,
                "wall_s": round(wall, 2),
                "node_time_s": {self._mgr_id(): round(wall, 2)},
                "models_used": [f"{self._mgr_id()}/{e['model']}"],
            },
        }
        self._record_job(jid, "embed", "done", [], {**resp, "coverage": {"chunks_total": len(texts), "chunks_processed": len(texts), "chunks_failed": 0}})
        self._emit_ev("offload.done", f"{jid} embed {len(texts)} texts in {round(wall, 2)}s", flows=["offload.return"])
        return resp

    async def search_memory(self, params: dict) -> dict:
        t0 = time.time()
        jid = "off-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
        query = str(params.get("query") or "").strip()
        if not query:
            raise ValueError("query is required")
        n = max(1, min(int(params.get("n") or 5), 20))
        which = str(params.get("collection") or "both")
        max_tokens = max(100, min(int(params.get("max_tokens") or 600), 4000))
        cols_map = self.cfg["collections"]
        targets = [cols_map["memory"], cols_map["tasks"]] if which == "both" else [cols_map.get(which, which)]

        self._emit_ev("offload.start", f"{jid} search_memory ({which})", flows=["offload.ingest", "offload.memory"])
        e = self.cfg["embed"]
        er = await self.http.post(
            f"{e['url']}/api/embed",
            json={"model": e["model"], "input": ["search_query: " + query[:8000]], "keep_alive": e.get("keep_alive", "30s")},
            timeout=60.0,
        )
        er.raise_for_status()
        qemb = er.json()["embeddings"][0]

        hits: list[dict] = []
        for cname in targets:
            try:
                cid = await self._collection_id(cname)
                qr = await self.http.post(
                    self._cpath(f"/{cid}/query"),
                    json={"query_embeddings": [qemb], "n_results": n, "include": ["documents", "metadatas", "distances"]},
                    timeout=15.0,
                )
                qr.raise_for_status()
                d = qr.json()
                for i, doc_id in enumerate((d.get("ids") or [[]])[0]):
                    hits.append({
                        "citation": f"mem:{doc_id}",
                        "id": doc_id,
                        "collection": cname,
                        "distance": round(float(d["distances"][0][i]), 4),
                        "text": (d["documents"][0][i] or "")[:1200],
                        "meta": d["metadatas"][0][i],
                    })
            except Exception as exc:
                log.warning("chroma query %s failed: %s", cname, exc)

        hits.sort(key=lambda x: x["distance"])
        hits = hits[:n]
        raw_chars = sum(len(h["text"]) for h in hits) + len(query)
        wall = time.time() - t0
        result_obj = {"query": query, "hits": hits}
        # Trim hits if needed for max_tokens
        trimmed = False
        while est_tokens(result_obj) > max_tokens and len(hits) > 1:
            hits.pop()
            trimmed = True
        acc = self._build_accounting(max(raw_chars, 400), [], result_obj, wall)
        acc["models_used"] = [f"{self._mgr_id()}/{e['model']}"]
        resp = {
            "job_id": jid,
            "tool": "search_memory",
            "status": "done",
            "result": result_obj,
            "coverage": {"chunks_total": len(targets), "chunks_processed": len(targets), "chunks_failed": 0,
                         "coverage_pct": 100.0, "truncated_to_max_tokens": trimmed},
            "confidence": "high" if hits else "medium",
            "confidence_note": f"Returned {len(hits)} closest matches from {', '.join(targets)}.",
            "accounting": acc,
        }
        self._record_job(jid, "search_memory", "done", [], resp)
        self._emit_ev("offload.done", f"{jid} search_memory ({len(hits)} hits)", flows=["offload.return"])
        return resp

    # ------------------------------------------------------------------ Async jobs & drill-down

    def get_chunks(self, job_id: str, chunk_ids: list[str]) -> dict:
        if not re.fullmatch(r"off-[0-9-]+-[a-f0-9]+", str(job_id)):
            raise ValueError("invalid job_id")
        p = JOBS_DIR / f"{job_id}.json"
        if not p.exists():
            raise ValueError(f"job {job_id} not found or expired")
        data = json.loads(p.read_text(encoding="utf-8"))
        want = set(chunk_ids or [])
        chunks = [c for c in (data.get("chunks") or []) if not want or c.get("id") in want]
        return {"job_id": job_id, "chunks": chunks, "found": len(chunks)}

    def submit_job(self, tool: str, params: dict) -> dict:
        if tool not in ("summarize", "map_reduce", "extract", "code_review"):
            raise ValueError("tool must be summarize|map_reduce|extract|code_review")
        jid = "off-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
        state = {"job_id": jid, "tool": tool, "status": "queued", "ts_submit": now_iso(),
                 "chunks_done": 0, "chunks_total": None, "result": None, "error": None}
        self._jobs[jid] = state

        def prog(done: int, total: int) -> None:
            state["chunks_done"] = done
            state["chunks_total"] = total

        async def _run() -> None:
            async with self._job_sem:
                state["status"] = "running"
                try:
                    fn = getattr(self, tool)
                    res = await fn(params, job_id=jid, progress_cb=prog)
                    state["status"] = "done"
                    state["result"] = res
                except Exception as e:
                    state["status"] = "error"
                    state["error"] = str(e)
                    self._record_job(jid, tool, "error", [], {"accounting": {}, "coverage": {}}, error=str(e))

        self._job_tasks[jid] = asyncio.create_task(_run())
        return {"job_id": jid, "tool": tool, "status": "queued"}

    def poll_job(self, job_id: str) -> dict:
        if job_id in self._jobs:
            s = self._jobs[job_id]
            if s["status"] == "done" and s.get("result"):
                return s["result"]
            return {k: v for k, v in s.items() if k != "result"}
        if re.fullmatch(r"off-[0-9-]+-[a-f0-9]+", str(job_id)):
            p = JOBS_DIR / f"{job_id}.json"
            if p.exists():
                return json.loads(p.read_text(encoding="utf-8")).get("response") or {"job_id": job_id, "status": "done"}
        raise ValueError(f"unknown job_id: {job_id}")

    def read_stats(self, n_recent: int = 20) -> dict:
        rows: list[dict] = []
        if JOURNAL.exists():
            with JOURNAL.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        totals = {
            "jobs_total": len(rows),
            "jobs_ok": sum(1 for r in rows if r.get("status") == "done"),
            "jobs_error": sum(1 for r in rows if r.get("status") == "error"),
            "raw_input_tokens_est": sum(int(r.get("raw_input_tokens_est") or 0) for r in rows),
            "local_input_tokens": sum(int(r.get("local_input_tokens") or 0) for r in rows),
            "local_output_tokens": sum(int(r.get("local_output_tokens") or 0) for r in rows),
            "returned_output_tokens": sum(int(r.get("returned_output_tokens") or 0) for r in rows),
            "frontier_tokens_saved_est": sum(int(r.get("frontier_tokens_saved_est") or 0) for r in rows),
            "manager_time_s": round(sum(float((r.get("node_time_s") or {}).get(self._mgr_id()) or 0.0) for r in rows), 2),
            "worker_time_s": round(sum(float((r.get("node_time_s") or {}).get(self._wrk_id()) or 0.0) for r in rows), 2),
            "active_jobs": sum(1 for s in self._jobs.values() if s.get("status") in ("queued", "running")),
        }
        by_tool: dict[str, dict] = {}
        for r in rows:
            t = r.get("tool") or "other"
            bt = by_tool.setdefault(t, {"jobs": 0, "saved_tokens": 0, "local_tokens": 0})
            bt["jobs"] += 1
            bt["saved_tokens"] += int(r.get("frontier_tokens_saved_est") or 0)
            bt["local_tokens"] += int(r.get("local_input_tokens") or 0) + int(r.get("local_output_tokens") or 0)
        return {
            "ts": now_iso(),
            "totals": totals,
            "by_tool": by_tool,
            "recent_jobs": list(reversed(rows[-max(1, min(n_recent, 100)):])),
        }

    async def status(self, include_recent: int = 10) -> dict:
        nodes_info: list[dict] = []
        try:
            r = await self.http.get(f"{self.cfg['manager']}/api/nodes", timeout=4.0)
            if r.status_code < 400:
                for n in r.json().get("nodes") or []:
                    ol = n.get("ollama") or {}
                    nodes_info.append({
                        "id": n.get("id"), "role": n.get("role"), "status": n.get("status"),
                        "busy": n.get("busy"), "queue": (n.get("queue") or {}).get("depth"),
                        "models_installed": [m.get("name") for m in ol.get("installed") or []],
                        "models_loaded": [m.get("name") for m in ol.get("loaded") or []],
                    })
        except Exception as e:
            nodes_info.append({"error": f"manager unreachable: {e}"})
        stats = self.read_stats(include_recent)
        return {
            "service": "llm-cluster-offload",
            "ts": now_iso(),
            "nodes": nodes_info,
            "mini_busy_reason": await self.mini_busy_reason(),
            "limits": self.limits,
            "path_allowlist": self.cfg.get("path_allowlist") or [],
            "accounting": stats["totals"],
            "by_tool": stats["by_tool"],
            "recent_jobs": stats["recent_jobs"],
        }
