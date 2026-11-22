#!/usr/bin/env python3
"""Frontier Agent Offload server (llm-cluster-offload, user service, :11630).

Exposes:
  - Streamable HTTP MCP at /mcp
  - Matching REST API at /v1/offload/* and /health

Binds 127.0.0.1 and optional Tailscale IP from offload.json.
Requires Bearer token (secret/offload.token) on all routes except GET /health.
Enforces request size limits and per-IP sliding-window rate limits.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import secrets
import socket
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

from offload_engine import OffloadEngine, now_iso

VERSION = "1.0.0"
BASE = Path(__file__).resolve().parent
CFG = json.loads((BASE / "offload.json").read_text(encoding="utf-8"))

_tp = Path(os.path.expanduser(CFG["token_file"]))
TOKEN_PATH = _tp if _tp.is_absolute() else BASE.parent / _tp
if not TOKEN_PATH.exists():
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    TOKEN_PATH.write_text(secrets.token_urlsafe(32) + "\n", encoding="utf-8")
    TOKEN_PATH.chmod(0o600)
TOKEN = TOKEN_PATH.read_text(encoding="utf-8").strip()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("offload")

engine = OffloadEngine(CFG)

mcp = FastMCP(
    "llm-cluster-offload",
    instructions=(
        "Frontier Agent Offload for a local LLM cluster (manager + worker nodes). "
        "Hand token-heavy reading/condensing to the cluster (summarize, map_reduce, extract, code_review, search_memory, embed) "
        "and keep final reasoning/decisions on the frontier model. Citations [cNNN] can be fetched verbatim with offload_get_chunks."
    ),
    stateless_http=True,
    json_response=True,
    streamable_http_path="/mcp",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=CFG.get("allowed_hosts") or ["127.0.0.1:*", "localhost:*"],
        allowed_origins=CFG.get("allowed_origins") or ["http://127.0.0.1:*", "http://localhost:*"],
    ),
)


# --------------------------------------------------------------------------- MCP Tools

@mcp.tool()
async def offload_summarize(
    text: str | None = None,
    files: list[dict[str, str]] | None = None,
    paths: list[str] | None = None,
    url: str | None = None,
    upload_ids: list[str] | None = None,
    focus: str | None = None,
    max_tokens: int = 600,
    prefer_node: str = "auto",
) -> dict:
    """Summarize large text, logs, inline files, uploaded files (upload_ids), public URL, or allowlisted local paths.
    Runs map-reduce across worker and manager nodes when multi-chunk, returning a compact summary with [cNNN] chunk citations."""
    return await engine.summarize({
        "text": text, "files": files, "paths": paths, "url": url,
        "upload_ids": upload_ids, "focus": focus, "max_tokens": max_tokens, "prefer_node": prefer_node,
    })


@mcp.tool()
async def offload_map_reduce(
    instruction: str,
    chunks: list[Any] | None = None,
    files: list[dict[str, str]] | None = None,
    paths: list[str] | None = None,
    glob: str | None = None,
    upload_ids: list[str] | None = None,
    reduce_instruction: str | None = None,
    max_tokens: int = 800,
    prefer_node: str = "auto",
    kind: str = "text",
) -> dict:
    """Run an instruction in parallel across chunks, inline files, upload_ids, or allowlisted repo paths/glob on mini + desktop,
    then merge on the manager node with [cNNN] citations and coverage notes."""
    return await engine.map_reduce({
        "instruction": instruction, "chunks": chunks, "files": files, "paths": paths, "glob": glob,
        "upload_ids": upload_ids, "reduce_instruction": reduce_instruction,
        "max_tokens": max_tokens, "prefer_node": prefer_node, "kind": kind,
    })


@mcp.tool()
async def offload_extract(
    schema: dict[str, Any] | str,
    documents: list[Any] | None = None,
    files: list[dict[str, str]] | None = None,
    paths: list[str] | None = None,
    upload_ids: list[str] | None = None,
    instruction: str | None = None,
    max_tokens: int = 1000,
    prefer_node: str = "auto",
) -> dict:
    """Extract structured JSON matching `schema` across many documents/chunks in parallel across cluster nodes."""
    return await engine.extract({
        "schema": schema, "documents": documents, "files": files, "paths": paths,
        "upload_ids": upload_ids, "instruction": instruction, "max_tokens": max_tokens, "prefer_node": prefer_node,
    })


@mcp.tool()
async def offload_code_review(
    diff: str | None = None,
    files: list[dict[str, str]] | None = None,
    paths: list[str] | None = None,
    upload_ids: list[str] | None = None,
    focus: str | None = None,
    max_tokens: int = 800,
) -> dict:
    """First-pass code review of a diff or files using the configured manager code model, cross-checked by the manager text model to filter false positives."""
    return await engine.code_review({
        "diff": diff, "files": files, "paths": paths, "upload_ids": upload_ids,
        "focus": focus, "max_tokens": max_tokens,
    })


@mcp.tool()
async def offload_search_memory(
    query: str,
    n: int = 5,
    collection: str = "both",
    max_tokens: int = 600,
) -> dict:
    """Semantic search over the cluster's ChromaDB memory (collections: memory, tasks, or both)."""
    return await engine.search_memory({"query": query, "n": n, "collection": collection, "max_tokens": max_tokens})


@mcp.tool()
async def offload_embed(texts: list[str], kind: str = "document") -> dict:
    """Generate embeddings via the configured embedding model on the manager (up to 32 texts, kind='document'|'query')."""
    return await engine.embed({"texts": texts, "kind": kind})


@mcp.tool()
async def offload_get_chunks(job_id: str, chunk_ids: list[str] | None = None) -> dict:
    """Fetch verbatim raw chunk text for [cNNN] citations returned by a previous offload job."""
    return engine.get_chunks(job_id, chunk_ids or [])


@mcp.tool()
async def offload_status(include_recent: int = 10) -> dict:
    """Cluster node health, busy state, rate/size limits, path allowlist, and cumulative token savings accounting."""
    return await engine.status(include_recent)


@mcp.tool()
async def offload_submit_job(tool: str, params: dict[str, Any]) -> dict:
    """Submit a long-running offload job asynchronously (tool: summarize|map_reduce|extract|code_review). Returns job_id to poll."""
    return engine.submit_job(tool, params)


@mcp.tool()
async def offload_poll_job(job_id: str) -> dict:
    """Poll status/progress or retrieve the completed result of an async offload job."""
    return engine.poll_job(job_id)


# --------------------------------------------------------------------------- REST API

async def _json(req: Request) -> dict:
    try:
        return await req.json()
    except Exception:
        return {}


def _err(e: Exception, code: int = 400) -> JSONResponse:
    status = 400 if isinstance(e, ValueError) else 500
    if code != 400:
        status = code
    return JSONResponse({"error": str(e)}, status_code=status)


@mcp.custom_route("/health", methods=["GET"])
async def r_health(req: Request):
    return JSONResponse({"ok": True, "service": "llm-cluster-offload", "version": VERSION, "ts": now_iso()})


@mcp.custom_route("/v1/offload/status", methods=["GET"])
async def r_status(req: Request):
    try:
        n = int(req.query_params.get("recent", "10"))
        return JSONResponse(await engine.status(n))
    except Exception as e:
        return _err(e, 500)


@mcp.custom_route("/v1/offload/stats", methods=["GET"])
async def r_stats(req: Request):
    try:
        n = int(req.query_params.get("n", "20"))
        return JSONResponse(engine.read_stats(n))
    except Exception as e:
        return _err(e, 500)


@mcp.custom_route("/v1/offload/jobs", methods=["GET"])
async def r_list_jobs(req: Request):
    n = int(req.query_params.get("n", "20"))
    return JSONResponse({"jobs": engine.read_stats(n)["recent_jobs"]})


@mcp.custom_route("/v1/offload/jobs", methods=["POST"])
async def r_submit_job(req: Request):
    b = await _json(req)
    try:
        res = engine.submit_job(str(b.get("tool") or ""), b.get("params") or {})
        return JSONResponse(res, status_code=202)
    except Exception as e:
        return _err(e)


@mcp.custom_route("/v1/offload/jobs/{job_id}", methods=["GET"])
async def r_poll_job(req: Request):
    try:
        return JSONResponse(engine.poll_job(req.path_params["job_id"]))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=404)
    except Exception as e:
        return _err(e, 500)


@mcp.custom_route("/v1/offload/jobs/{job_id}/chunks", methods=["GET"])
async def r_job_chunks(req: Request):
    try:
        raw_ids = req.query_params.get("ids", "")
        cids = [x.strip() for x in raw_ids.split(",") if x.strip()]
        return JSONResponse(engine.get_chunks(req.path_params["job_id"], cids))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=404)
    except Exception as e:
        return _err(e, 500)


@mcp.custom_route("/v1/offload/upload", methods=["POST"])
async def r_upload(req: Request):
    try:
        ct = (req.headers.get("content-type") or "").lower()
        files: list[dict] = []
        if "multipart/form-data" in ct:
            form = await req.form()
            for _, val in form.multi_items():
                if hasattr(val, "read"):
                    raw = await val.read()
                    if b"\x00" in raw[:4096]:
                        raise ValueError(f"binary file rejected: {getattr(val, 'filename', 'upload')}")
                    files.append({"name": getattr(val, "filename", None) or "upload.txt",
                                  "content": raw.decode("utf-8", errors="replace")})
        else:
            b = await _json(req)
            files = b.get("files") or ([] if b.get("content") is None else [{"name": b.get("name") or "upload.txt", "content": b["content"]}])
        if not files:
            raise ValueError("no files provided")
        return JSONResponse(engine.save_upload(files), status_code=201)
    except Exception as e:
        return _err(e)


@mcp.custom_route("/v1/offload/summarize", methods=["POST"])
async def r_summarize(req: Request):
    try:
        return JSONResponse(await engine.summarize(await _json(req)))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/v1/offload/map_reduce", methods=["POST"])
async def r_map_reduce(req: Request):
    try:
        return JSONResponse(await engine.map_reduce(await _json(req)))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/v1/offload/extract", methods=["POST"])
async def r_extract(req: Request):
    try:
        return JSONResponse(await engine.extract(await _json(req)))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/v1/offload/code_review", methods=["POST"])
async def r_code_review(req: Request):
    try:
        return JSONResponse(await engine.code_review(await _json(req)))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/v1/offload/search_memory", methods=["POST"])
async def r_search_memory(req: Request):
    try:
        return JSONResponse(await engine.search_memory(await _json(req)))
    except Exception as e:
        return _err(e)


@mcp.custom_route("/v1/offload/embed", methods=["POST"])
async def r_embed(req: Request):
    try:
        return JSONResponse(await engine.embed(await _json(req)))
    except Exception as e:
        return _err(e)


# --------------------------------------------------------------------------- Auth, Size & Rate-Limit Guard

class _BodyTooLarge(Exception):
    pass


class SecurityGuard:
    def __init__(self, app):
        self.app = app
        lim = CFG.get("limits") or {}
        self.max_bytes = int(lim.get("max_request_bytes", 10_485_760))
        self.heavy_rpm = int(lim.get("rate_limit_heavy_rpm", 30))
        self.read_rpm = int(lim.get("rate_limit_read_rpm", 120))
        self._hits_heavy: dict[str, deque[float]] = defaultdict(deque)
        self._hits_read: dict[str, deque[float]] = defaultdict(deque)

    def _rate_ok(self, ip: str, heavy: bool) -> tuple[bool, int]:
        now = time.time()
        bucket = self._hits_heavy[ip] if heavy else self._hits_read[ip]
        limit = self.heavy_rpm if heavy else self.read_rpm
        while bucket and now - bucket[0] > 60.0:
            bucket.popleft()
        if len(bucket) >= limit:
            retry = max(1, int(60.0 - (now - bucket[0])) + 1)
            return False, retry
        bucket.append(now)
        return True, 0

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        method = scope.get("method", "GET").upper()
        path = scope.get("path", "")
        ip = (scope.get("client") or ("0.0.0.0", 0))[0]

        # Unauthenticated liveness probe only
        if method == "GET" and path == "/health":
            return await self.app(scope, receive, send)

        hdrs = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        auth = hdrs.get("authorization", "")
        tok = auth[7:].strip() if auth.lower().startswith("bearer ") else hdrs.get("x-api-key", "").strip()
        if not tok or not hmac.compare_digest(tok, TOKEN):
            log.warning("rejected %s %s from %s (token=%s)", method, path, ip, "present" if tok else "missing")
            resp = JSONResponse({"error": "unauthorized: valid Bearer token required"}, status_code=401)
            return await resp(scope, receive, send)

        # Request size check (Content-Length header)
        cl_raw = hdrs.get("content-length")
        if cl_raw:
            try:
                if int(cl_raw) > self.max_bytes:
                    resp = JSONResponse({"error": f"payload too large (max {self.max_bytes} bytes)"}, status_code=413)
                    return await resp(scope, receive, send)
            except ValueError:
                pass

        # Rate limit
        heavy = (method == "POST") and (path == "/mcp" or path.startswith("/v1/offload/"))
        ok, retry = self._rate_ok(ip, heavy)
        if not ok:
            log.warning("rate-limited %s %s from %s", method, path, ip)
            resp = JSONResponse({"error": "rate limit exceeded", "retry_after_s": retry},
                                status_code=429, headers={"Retry-After": str(retry)})
            return await resp(scope, receive, send)

        # Streaming body byte cap
        seen = 0

        async def limited_receive():
            nonlocal seen
            msg = await receive()
            if msg.get("type") == "http.request":
                seen += len(msg.get("body") or b"")
                if seen > self.max_bytes:
                    raise _BodyTooLarge()
            return msg

        try:
            return await self.app(scope, limited_receive, send)
        except _BodyTooLarge:
            resp = JSONResponse({"error": f"payload too large (max {self.max_bytes} bytes)"}, status_code=413)
            return await resp(scope, receive, send)


def make_socket(host: str, port: int) -> socket.socket | None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.IPPROTO_IP, 15, 1)  # IP_FREEBIND
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
    app = SecurityGuard(inner)
    socks = [s for s in (make_socket(h, int(CFG["port"])) for h in CFG["bind"]) if s]
    if not socks:
        raise SystemExit("no listen sockets for llm-cluster-offload")
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on", proxy_headers=False))
    log.info("llm-cluster-offload %s listening on %s", VERSION, [s.getsockname() for s in socks])
    try:
        await server.serve(sockets=socks)
    finally:
        await engine.close()


if __name__ == "__main__":
    asyncio.run(main())
