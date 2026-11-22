"""Optional "Use Cluster Network" support.

Disabled by default. Enable by setting ``ODYSSEUS_CLUSTER_ENABLED=1`` (and
optionally ``ODYSSEUS_CLUSTER_NETWORK=1``, which defaults to on when cluster
support is enabled).

* Registers (idempotently) a shared model endpoint **Cluster Network** pointing
  at the cluster bridge (``ODYSSEUS_CLUSTER_BRIDGE_URL``, model
  ``cluster-network``). The bridge runs the chain-of-command orchestrator.
* ``GET /api/cluster-network/config`` tells the chat UI toggle whether the
  feature is enabled, healthy, and which endpoint id/model to route to.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import httpx
from fastapi import APIRouter

logger = logging.getLogger(__name__)

ENDPOINT_ID = "clusternet"
ENDPOINT_NAME = "Cluster Network"
MODEL_ID = "cluster-network"


def _truthy(val: str | None, default: bool = False) -> bool:
    if val is None or val.strip() == "":
        return default
    return val.strip().lower() not in ("0", "false", "no", "off")


def _is_enabled() -> bool:
    if not _truthy(os.environ.get("ODYSSEUS_CLUSTER_ENABLED"), default=False):
        return False
    return _truthy(os.environ.get("ODYSSEUS_CLUSTER_NETWORK"), default=True)


def _bridge_url() -> str:
    return (os.environ.get("ODYSSEUS_CLUSTER_BRIDGE_URL") or "http://host.docker.internal:11610/v1").rstrip("/")


def _token() -> str:
    tok = (os.environ.get("ODYSSEUS_CLUSTER_BRIDGE_TOKEN") or "").strip()
    if tok:
        return tok
    token_file = Path(os.environ.get("ODYSSEUS_CLUSTER_BRIDGE_TOKEN_FILE") or "/run/secrets/cluster_bridge_token")
    try:
        return token_file.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def ensure_cluster_endpoint() -> str | None:
    """Create or refresh the shared Cluster Network endpoint row when enabled."""
    if not _is_enabled():
        return None
    base_url = _bridge_url()
    try:
        from core.database import ModelEndpoint, SessionLocal
    except Exception as e:  # pragma: no cover
        logger.warning("cluster-network: database unavailable: %s", e)
        return None
    tok = _token()
    db = SessionLocal()
    try:
        ep = db.query(ModelEndpoint).filter(ModelEndpoint.id == ENDPOINT_ID).first()
        created = ep is None
        if created:
            ep = ModelEndpoint(id=ENDPOINT_ID, name=ENDPOINT_NAME, base_url=base_url, model_type="llm", owner=None)
            db.add(ep)
        ep.base_url = base_url
        if tok:
            ep.api_key = tok
        ep.is_enabled = True
        ep.endpoint_kind = "local"
        ep.supports_tools = True
        ep.cached_models = json.dumps([MODEL_ID])
        ep.pinned_models = json.dumps([MODEL_ID])
        db.commit()
        logger.info("cluster-network: endpoint %s %s (%s)", ENDPOINT_ID, "created" if created else "refreshed", base_url)
    except Exception as e:
        db.rollback()
        logger.warning("cluster-network: could not register endpoint: %s", e)
        return None
    finally:
        db.close()
    try:
        from routes.model_routes import _invalidate_models_cache
        _invalidate_models_cache()
    except Exception:
        pass
    return ENDPOINT_ID


def setup_cluster_network_routes() -> APIRouter:
    router = APIRouter(prefix="/api/cluster-network", tags=["cluster-network"])
    if _is_enabled():
        ensure_cluster_endpoint()

    @router.get("/config")
    async def config():
        enabled = _is_enabled()
        base_url = _bridge_url()
        healthy, detail = False, None
        if enabled:
            try:
                tok = _token()
                headers = {"Authorization": f"Bearer {tok}"} if tok else {}
                async with httpx.AsyncClient(timeout=2.5) as c:
                    r = await c.get(base_url.rsplit("/v1", 1)[0] + "/health", headers=headers)
                    j = r.json()
                    healthy, detail = bool(j.get("ok")), j.get("checks")
            except Exception as e:
                detail = str(e)[:200]
        return {
            "enabled": enabled,
            "endpoint_id": ENDPOINT_ID,
            "endpoint_name": ENDPOINT_NAME,
            "model": MODEL_ID,
            "base_url": base_url if enabled else None,
            "healthy": healthy,
            "checks": detail,
            "default_on": True,
            "pref_key": "cluster_network_mode",
        }

    return router
