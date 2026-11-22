#!/usr/bin/env python3
"""llm-cluster-selfcheck: restart ONLY the user-level llm-cluster-manager service when
(a) its local /health is unreachable or reports a dead task processor, or
(b) a worker node filed an authenticated restart request (state/RESTART_REQUESTED).
Never touches system services, Ollama config, Docker, or Odysseus."""
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(os.path.expanduser(os.environ.get("LLM_CLUSTER_HOME") or str(Path(__file__).resolve().parents[1])))
FLAG = ROOT / "state" / "RESTART_REQUESTED"
LOG = ROOT / "journal" / "selfcheck.jsonl"
UNIT = "llm-cluster-manager.service"


def logrec(**kw):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a") as f:
        f.write(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **kw}) + "\n")


def main():
    if subprocess.run(["systemctl", "--user", "is-active", "--quiet", UNIT]).returncode != 0:
        logrec(action="none", why="manager not active (stopped by user?)")
        return
    reason = None
    if FLAG.exists():
        reason = "peer restart request: " + FLAG.read_text()[:300]
        FLAG.unlink(missing_ok=True)
    else:
        try:
            with urllib.request.urlopen("http://127.0.0.1:11600/health", timeout=10) as r:
                h = json.loads(r.read())
            if not (h.get("processor") or {}).get("alive", False):
                reason = "task processor dead"
        except Exception as e:  # noqa: BLE001
            reason = f"health unreachable: {e}"
    if reason:
        rc = subprocess.run(["systemctl", "--user", "restart", UNIT]).returncode
        logrec(action="restart", why=reason, rc=rc)
    else:
        logrec(action="none", why="healthy")


if __name__ == "__main__":
    main()
