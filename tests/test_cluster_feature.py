"""Tests for the optional Cluster Network integration."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.middleware import require_admin
from routes import cluster_network, cluster_routes
from src.agent_runtime import completion


def _client_with_cluster_routes(monkeypatch, *, admin: bool = True) -> TestClient:
    if cluster_network._is_enabled():
        monkeypatch.setattr(cluster_network, "ensure_cluster_endpoint", lambda: cluster_network.ENDPOINT_ID)
    app = FastAPI()
    app.include_router(cluster_routes.setup_cluster_routes())
    app.include_router(cluster_network.setup_cluster_network_routes())
    if admin:
        app.dependency_overrides[require_admin] = lambda: {"username": "admin", "role": "admin"}
    return TestClient(app)


def test_cluster_disabled_by_default(monkeypatch):
    for key in (
        "ODYSSEUS_CLUSTER_ENABLED",
        "ODYSSEUS_CLUSTER_MOCK",
        "ODYSSEUS_CLUSTER_API_URL",
        "ODYSSEUS_CLUSTER_API_TOKEN",
        "ODYSSEUS_CLUSTER_NETWORK",
    ):
        monkeypatch.delenv(key, raising=False)

    assert cluster_routes.is_cluster_enabled() is False
    assert cluster_network._is_enabled() is False
    assert completion._cluster_network_turn({"model": "cluster-network"}) is False

    client = _client_with_cluster_routes(monkeypatch)
    cfg = client.get("/api/cluster/config")
    assert cfg.status_code == 200
    assert cfg.json() == {"enabled": False, "mock": False, "configured": False}

    net_cfg = client.get("/api/cluster-network/config")
    assert net_cfg.status_code == 200
    assert net_cfg.json()["enabled"] is False
    assert net_cfg.json()["base_url"] is None

    nodes = client.get("/api/cluster/nodes")
    assert nodes.status_code == 404


def test_cluster_enabled_mock_mode(monkeypatch):
    monkeypatch.setenv("ODYSSEUS_CLUSTER_ENABLED", "1")
    monkeypatch.setenv("ODYSSEUS_CLUSTER_MOCK", "1")
    monkeypatch.setenv("ODYSSEUS_CLUSTER_NETWORK", "1")

    client = _client_with_cluster_routes(monkeypatch)
    cfg = client.get("/api/cluster/config")
    assert cfg.status_code == 200
    assert cfg.json()["enabled"] is True
    assert cfg.json()["mock"] is True

    nodes_resp = client.get("/api/cluster/nodes")
    assert nodes_resp.status_code == 200
    data = nodes_resp.json()
    assert len(data["nodes"]) >= 2
    serialized = json.dumps(data)
    assert "192.0.2." in serialized

    net_cfg = client.get("/api/cluster-network/config")
    assert net_cfg.status_code == 200
    assert net_cfg.json()["enabled"] is True
    assert net_cfg.json()["default_on"] is True


def test_cluster_network_turn_gating(monkeypatch):
    monkeypatch.setenv("ODYSSEUS_CLUSTER_ENABLED", "0")
    assert completion._cluster_network_turn({"model": "cluster-network"}) is False

    monkeypatch.setenv("ODYSSEUS_CLUSTER_ENABLED", "1")
    monkeypatch.delenv("ODYSSEUS_CLUSTER_NETWORK_GATE", raising=False)
    assert completion._cluster_network_turn({"model": "cluster-network"}) is True
    assert completion._cluster_network_turn({"model": "cluster-network-worker"}) is False

    monkeypatch.setenv("ODYSSEUS_CLUSTER_NETWORK_GATE", "1")
    assert completion._cluster_network_turn({"model": "cluster-network"}) is False


def test_contrib_onboard_advisor_recommendation():
    contrib_dir = ROOT / "contrib" / "cluster-network"
    sys.path.insert(0, str(contrib_dir))
    try:
        from onboard.advisor import advise
    finally:
        sys.path.remove(str(contrib_dir))

    probe = {
        "hostname": "sample-worker",
        "os": {"name": "Linux", "kernel": "6.8", "arch": "x86_64"},
        "arch": "x86_64",
        "cpu": {"model": "Sample CPU", "cores": 8},
        "memory": {"ram_gb": 32.0},
        "gpu": [{"vendor": "nvidia", "model": "Sample GPU", "vram_gb": 12.0}],
        "disk": {"free_gb": 200.0},
        "ollama": {"installed": True, "models": []},
        "network": {"primary_ip": "192.0.2.50", "ips": ["192.0.2.50"]},
    }
    rec = advise(probe, ctx={"devices": [], "manager_id": "manager", "peer_id": "worker"})
    assert rec["role"] in {"gpu-worker", "cpu-worker", "reviewer", "embeddings", "storage/memory", "monitor/sentinel"}
    assert rec["device"]["ip"] == "192.0.2.50"

def test_cluster_hosts_disabled_by_default(monkeypatch):
    for key in (
        "ODYSSEUS_CLUSTER_ENABLED",
        "ODYSSEUS_CLUSTER_MOCK",
        "ODYSSEUS_CLUSTER_API_URL",
        "ODYSSEUS_CLUSTER_API_TOKEN",
        "ODYSSEUS_CLUSTER_NETWORK",
    ):
        monkeypatch.delenv(key, raising=False)

    client = _client_with_cluster_routes(monkeypatch)
    hosts = client.get("/api/cluster-network/hosts")
    assert hosts.status_code == 200
    data = hosts.json()
    assert data["enabled"] is False
    assert data["manager"] is None
    assert data["workers"] == []
    assert data["session"]["remote"] is False

    session = client.get("/api/cluster-network/session")
    assert session.status_code == 200
    assert session.json()["enabled"] is False
    assert session.json()["remote"] is False


def test_cluster_hosts_mock_mode_groups_models(monkeypatch):
    monkeypatch.setenv("ODYSSEUS_CLUSTER_ENABLED", "1")
    monkeypatch.setenv("ODYSSEUS_CLUSTER_MOCK", "1")
    monkeypatch.setenv("ODYSSEUS_CLUSTER_NETWORK", "1")

    client = _client_with_cluster_routes(monkeypatch)
    resp = client.get("/api/cluster-network/hosts")
    assert resp.status_code == 200
    data = resp.json()
    assert data["enabled"] is True
    assert data["manager"]["id"] == "manager-node"
    assert data["manager"]["short_label"] == "Desktop"
    assert data["manager"]["label"].startswith("Desktop")
    assert data["manager"]["is_manager"] is True
    assert any(m["id"] == "gemma4:12b" and m["via"] == "local-ollama" for m in data["manager"]["models"])
    assert len(data["workers"]) == 1
    worker = data["workers"][0]
    assert worker["id"] == "worker-node"
    assert worker["short_label"] == "Mini PC"
    assert worker["bridge_prefix"] == "mini"
    assert any(m["route_model"] == "mini/qwen2.5:7b" and m["via"] == "bridge" for m in worker["models"])
    serialized = json.dumps(data)
    assert "192.0.2." in serialized
    assert "198.51.100." in serialized
    assert "qarch-host" not in serialized
    assert "mini-worker" not in serialized
    assert "192.168." not in serialized
    # Loopback session is local (the manager host), not a remote controller.
    assert data["session"]["remote"] is False
    session = client.get("/api/cluster-network/session")
    assert session.json()["remote"] is False
    # A forwarded controller / worker address is a remote session.
    remote = client.get("/api/cluster-network/session", headers={"X-Forwarded-For": "198.51.100.30"})
    assert remote.status_code == 200
    body = remote.json()
    assert body["remote"] is True
    assert body["client_device_id"] == "remote-controller"
    assert "models run on your cluster" in body["label"]
    worker_ip = client.get("/api/cluster-network/session", headers={"X-Forwarded-For": "192.0.2.20"})
    assert worker_ip.json()["remote"] is True
    assert worker_ip.json()["client_device_id"] == "worker-node"
    # Manager LAN / tailnet addresses stay local.
    local_ts = client.get("/api/cluster-network/session", headers={"X-Forwarded-For": "198.51.100.10"})
    assert local_ts.json()["remote"] is False


def test_cluster_payload_lists_devices_used():
    import sys
    contrib = ROOT / "contrib" / "cluster-network" / "bridge"
    sys.path.insert(0, str(contrib))
    try:
        import orchestrator
    finally:
        sys.path.remove(str(contrib))

    payload = orchestrator._cluster_payload({
        "id": "o-test",
        "mode": "plan",
        "wall_s": 1.2,
        "nodes_used": ["manager-node · gemma4:12b", "worker-node · qwen2.5:7b"],
    })
    assert payload["run_id"] == "o-test"
    assert payload["mode"] == "plan"
    assert payload["devices"] == [
        {"id": "manager-node", "model": "gemma4:12b"},
        {"id": "worker-node", "model": "qwen2.5:7b"},
    ]
    assert "qarch-host" not in json.dumps(payload)
