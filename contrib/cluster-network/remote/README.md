# Remote / controller ingest

Tiny user-level status path so a device **without a cluster agent** can show up on the Odysseus Cluster tab. It does **not** talk to manager `:11600` (that port is locked down by `cluster-guard`, and another worker is editing the manager/bridge).

- **Ingest:** `ingest.py`, systemd user unit `llm-cluster-remote-ingest.service`. Listens on loopback, LAN, and Tailscale interfaces (port **11620**). `POST /remote/heartbeat` must be HMAC-signed with `secret/remote.token` (mode 0600). Successful posts are written to `state/remotes/<id>.json` for the Odysseus Cluster tab.
- **Reporter (on a remote controller):** optional timer (`OnUnitActiveSec=45s`, no sudo) posting signed JSON heartbeats (battery, form factor, IPs, active Odysseus session, ICMP latency) to `http://<MANAGER_IP>:11620/remote/heartbeat`.
- **Cluster tab:** `devices.json` type `controller` → `_probe_controller` + `DEVICE_TEMPLATES.controller`. Cyan map flow `kind: remote`.

```bash
systemctl --user status llm-cluster-remote-ingest
systemctl --user disable --now llm-cluster-remote-ingest   # undo on the desktop
# on the Switch:
systemctl --user disable --now cluster-remote-reporter.timer
```
