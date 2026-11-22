#!/usr/bin/env bash
# Non-root installer for Odysseus Cluster Network companion services.
# Usage:
#   ./install.sh [--dir "$HOME/llm-cluster"] [--skip-venv] [--enable-systemd]
set -euo pipefail

if [[ ${EUID} -eq 0 ]]; then
  echo "Do not run install.sh as root/sudo. Run as your normal user." >&2
  exit 1
fi

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${LLM_CLUSTER_HOME:-$HOME/llm-cluster}"
SKIP_VENV=0
ENABLE_SYSTEMD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) TARGET_DIR="$2"; shift 2 ;;
    --skip-venv) SKIP_VENV=1; shift ;;
    --enable-systemd) ENABLE_SYSTEMD=1; shift ;;
    -h|--help)
      sed -n '2,6p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

echo "== Installing companion files to $TARGET_DIR =="
mkdir -p "$TARGET_DIR"/{bin,manager,bridge,offload/journal,onboard,remote,systemd,net,journal,state/remotes,secret,backups}
chmod 700 "$TARGET_DIR/secret"

cp -f "$SRC_DIR/bin/cluster" "$TARGET_DIR/bin/cluster"
chmod +x "$TARGET_DIR/bin/cluster"
cp -f "$SRC_DIR/manager/"*.py "$TARGET_DIR/manager/"
cp -f "$SRC_DIR/bridge/"*.py "$TARGET_DIR/bridge/"
cp -f "$SRC_DIR/offload/"*.py "$TARGET_DIR/offload/"
cp -f "$SRC_DIR/onboard/"{__init__.py,advisor.py,probe.sh,ONBOARDING.md} "$TARGET_DIR/onboard/"
chmod +x "$TARGET_DIR/onboard/probe.sh"
cp -f "$SRC_DIR/remote/"{ingest.py,README.md} "$TARGET_DIR/remote/"
cp -f "$SRC_DIR/net/"* "$TARGET_DIR/net/"

copy_if_missing() {
  local src="$1" dst="$2"
  if [[ ! -f "$dst" ]]; then
    cp "$src" "$dst"
    echo "  created $dst (customize for your network)"
  else
    echo "  kept existing $dst"
  fi
}

copy_if_missing "$SRC_DIR/config.example.json" "$TARGET_DIR/config.json"
copy_if_missing "$SRC_DIR/devices.example.json" "$TARGET_DIR/devices.json"
copy_if_missing "$SRC_DIR/bridge/bridge.example.json" "$TARGET_DIR/bridge/bridge.json"
copy_if_missing "$SRC_DIR/offload/offload.example.json" "$TARGET_DIR/offload/offload.json"

echo "== Generating missing secret tokens (chmod 600) =="
for tok in cluster.token api.token bridge.token offload.token remote.token; do
  tpath="$TARGET_DIR/secret/$tok"
  if [[ ! -f "$tpath" ]]; then
    python3 -c 'import secrets; print(secrets.token_hex(32))' > "$tpath"
    chmod 600 "$tpath"
    echo "  generated $tpath"
  else
    chmod 600 "$tpath"
    echo "  kept existing $tpath"
  fi
done

PYTHON_BIN="$(command -v python3)"
BRIDGE_PYTHON_BIN="$PYTHON_BIN"
if [[ $SKIP_VENV -eq 0 ]]; then
  echo "== Setting up Python virtual environment =="
  if [[ ! -d "$TARGET_DIR/.venv" ]]; then
    python3 -m venv "$TARGET_DIR/.venv"
  fi
  "$TARGET_DIR/.venv/bin/pip" install --quiet --upgrade pip
  "$TARGET_DIR/.venv/bin/pip" install --quiet fastapi uvicorn httpx mcp starlette
  PYTHON_BIN="$TARGET_DIR/.venv/bin/python"
  BRIDGE_PYTHON_BIN="$PYTHON_BIN"
fi

echo "== Rendering systemd --user unit templates =="
for tmpl in "$SRC_DIR/systemd/"*.service.in; do
  base="$(basename "$tmpl" .in)"
  sed \
    -e "s|@INSTALL_DIR@|$TARGET_DIR|g" \
    -e "s|@PYTHON_BIN@|$PYTHON_BIN|g" \
    -e "s|@BRIDGE_PYTHON_BIN@|$BRIDGE_PYTHON_BIN|g" \
    "$tmpl" > "$TARGET_DIR/systemd/$base"
done
cp -f "$SRC_DIR/systemd/llm-cluster-selfcheck.timer" "$TARGET_DIR/systemd/llm-cluster-selfcheck.timer"

if [[ $ENABLE_SYSTEMD -eq 1 ]] && command -v systemctl >/dev/null 2>&1; then
  mkdir -p "$HOME/.config/systemd/user"
  cp -f "$TARGET_DIR/systemd/"*.service "$TARGET_DIR/systemd/"*.timer "$HOME/.config/systemd/user/"
  systemctl --user daemon-reload
  systemctl --user enable --now \
    llm-cluster-manager.service \
    llm-cluster-odysseus-bridge.service \
    llm-cluster-offload.service \
    llm-cluster-remote-ingest.service \
    llm-cluster-selfcheck.timer
  echo "  systemd --user services enabled and started."
fi

cat <<EONOTE

Done! Next steps (no sudo required):
  1. Edit $TARGET_DIR/config.json, devices.json, bridge/bridge.json, and offload/offload.json
     with your hostnames, IPs, and Ollama model names.
  2. Install & start user services (if you didn't pass --enable-systemd):
       mkdir -p ~/.config/systemd/user
       cp $TARGET_DIR/systemd/*.{service,timer} ~/.config/systemd/user/
       systemctl --user daemon-reload
       systemctl --user enable --now llm-cluster-manager.service llm-cluster-odysseus-bridge.service
  3. Enable in Odysseus:
       COMPOSE_FILE=docker-compose.yml:contrib/cluster-network/docker-compose.cluster.yml \\
       ODYSSEUS_CLUSTER_HOME=$TARGET_DIR \\
       docker compose up -d

Optional root steps (firewall / Tailscale Serve / Funnel):
  See $TARGET_DIR/net/README.md for inspectable .example scripts.
EONOTE
