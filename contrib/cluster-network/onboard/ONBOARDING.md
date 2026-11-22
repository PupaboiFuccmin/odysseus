# Onboarding new hardware

How to add a machine (or a future peripheral) to a local LLM cluster without guessing a role or a model. The path is **probe → advise → confirm → apply**. Apply never uses `sudo`; it only updates `devices.json` and `state/extra_flows.json` on the manager node.

| Piece | Where |
|---|---|
| Probe | `onboard/probe.sh` (copy this one file to the new machine) |
| Advisor + apply | `bin/cluster onboard …` (implementation: `onboard/advisor.py`) |
| Cluster tab | Odysseus → Cluster → **Add device** (paste or upload the probe JSON) |
| Manager API | `GET /api/onboard/instructions`, `POST /api/onboard/advise`, `POST /api/onboard/apply`, `POST /api/onboard/reload` (API.md) |
| Probe ping | `GET /onboard/ping` on the manager (LAN / tailnet, rate-limited; used only for latency and a short bandwidth blob) |

Constraints, same as the rest of the cluster: never run sudo from the advisor; never delete personal or ambiguous files; never put passwords in the probe or the recommendation; leave the desktop OS and apps alone except Odysseus and `$LLM_CLUSTER_HOME`.

## 1. Probe the new machine

On the new device (x86_64 or aarch64 Linux, python3 required, no root):

```bash
# copy onboard/probe.sh over, then:
chmod +x probe.sh
./probe.sh --pretty --manager http://<MANAGER_IP>:11600 | tee /tmp/cluster-probe.json
```

`--no-benchmark` skips the short timing run. The benchmark will use a tiny *already-pulled* Ollama model if one exists; it will not pull anything. Otherwise it runs a small CPU matmul.

The JSON covers CPU, cores/threads, RAM, GPU vendor/model/VRAM (from `nvidia-smi`, `rocm-smi`, `lspci`, and `/sys`, in that order of preference), disk free, OS, Ollama presence and tags, and latency / bandwidth to the desktop manager (`icmp`, TCP connect to `:11600`, `GET /onboard/ping`, optional 512 KiB blob). Missing tools become `null`s, not a failed probe.

Copy the file to the manager node (scp, USB, paste). Do not commit it if it would include extra host inventory you did not mean to share; the stock probe does not collect secrets or home-directory listings.

## 2. Ask for a recommendation

On the manager node:

```bash
bin/cluster onboard /tmp/cluster-probe.json
bin/cluster onboard --json /tmp/cluster-probe.json   # machine-readable
bin/cluster onboard --role gpu-worker --id lab-box /tmp/cluster-probe.json
```

You can also pass an existing device id (`cluster onboard worker-node`) to re-run the advisor against `devices.json`. That path is for planning only; it will not apply over protected devices.

Or open Odysseus → Cluster → **Add device**, paste or upload the JSON, and read the same recommendation in the side panel.

The advisor maps specs to one primary role and optional secondary roles, then names catalog models that fit (with quantization and a VRAM/RAM fit reason), the chain-of-command position, and the flows to add. Model fit uses conservative loaded sizes common worker catalogs, leaving a 10% VRAM margin.

## 3. Apply, only after you agree

```bash
bin/cluster onboard --apply --yes /tmp/cluster-probe.json
```

`--apply` without `--yes` asks you to type `yes`. It refuses to change protected core nodes (configured in `PROTECTED_IDS`). A timestamped copy of `devices.json` lands in `backups/` first.

To take a device back out:

```bash
bin/cluster onboard --remove <id> --yes
```

## Roles

| Role | Typical hardware | Models (catalog) | Map position |
|---|---|---|---|
| **gpu-worker** | Discrete NVIDIA/AMD GPU with ≥4 GB VRAM, or a capable APU (16 GB+ unified memory) | Whatever fits: 3B/7B on a 680M-class APU; gemma4:12b / qwen3:14b / qwen3-coder-30b on ≥10 GB discrete | Under the manager, alongside existing workers. Green task lines, blue review, amber monitoring, gray link |
| **cpu-worker** | Many CPU cores, little or no GPU, ≥16 GB RAM | `qwen2.5:3b` (maybe `qwen2.5:7b` if RAM allows); `nomic-embed-text` | Same worker slot as above, expecting lower tok/s |
| **reviewer** | Mid or large GPU, used as a *second* opinion, not the writer | `qwen3:14b` (text/plan) or `qwen3-coder-30b:latest` (code). The 2026-11-21 eval is why: gemma4 self-graded its own `csv.reader` bug 10/10; the 30B coder caught it | Next to the manager, blue review lines both ways |
| **embeddings** | Modest RAM or a small always-on box | `nomic-embed-text` only (~0.3 GB) | Teal tools/memory line to the manager |
| **storage/memory** | NAS / big disk, weak compute | `nomic-embed-text` if there is spare RAM; otherwise no generate | Leaf under the manager; backups / Chroma data, not the default generate chain |
| **monitor/sentinel** | Pi-class SBC, always on, little RAM | Usually none; optionally `nomic-embed-text` | Amber heartbeat and red alert lines toward the manager |
| **controller** | Handheld / laptop / Switch-class (e.g. handheld or thin client) | None. Do not install Ollama | Above the manager; cyan remote-control line is drawn automatically from `type=controller` |
| **peripheral** | Printer, camera, cooling, sensor, plug | None | Side of the map with the router; gray status line |

The current catalog, for fit math:

| Model | Quant | ~loaded | Role in this cluster | Evidence |
|---|---|---|---|---|
| `nomic-embed-text` | F16 | 0.3 GB | Bridge / Chroma embeddings | Already used on the manager node |
| `qwen2.5:3b` | Q4_K_M | 2.2 GB | fast worker | ~30+ tok/s on a 680M-class APU (Vulkan) |
| `qwen2.5:7b` | Q4_K_M | 4.7 GB | general worker | ~15 tok/s on a 680M-class APU (Vulkan) |
| `qwen2.5-coder:7b` | Q4_K_M | 4.7 GB | mini_code | Same node, code first pass |
| `gemma4:12b` | Q4_K_M | 8.0 GB | Manager router / rubric / synthesis | Strong router/synthesizer on 12 GB-class discrete GPUs |
| `qwen3:14b` | Q4_K_M | 9.3 GB | Optional independent text/plan reviewer | Eval 16/18, 35.8 tok/s, 2.85 J/tok |
| `qwen3-coder-30b:latest` | Q4_K_M | 9.87 GB | Complex code + code cross-review | Eval 13/18; loaded 98.7% of the 12 GB card |

Do not promote a new box to *manager* just because it has more VRAM than the current manager. Say so in the checklist and leave the switch of `config.json` / orchestrator as a later, explicit change.

## Device classes (playbook)

### GPU box (discrete NVIDIA or AMD)

1. Run the probe. Expect role **gpu-worker**, plus **reviewer** if usable VRAM is ≥9 GB.
2. Install Ollama (user-level tarball preferred; official script if you want system Ollama like the current nodes — that script uses sudo, so it is a paste, not something the advisor runs).
3. Pull only the models the advisor marked as fitting. On a 12 GB card that is the current desktop set; on an 8 GB card drop `qwen3:14b` / the 30B coder.
4. Open `:11434` to `<MANAGER_IP>` only (ufw paste is in the checklist).
5. Install a user-level worker agent (outbound HMAC, outbound-only worker agent). `cluster onboard --apply --yes`.
6. Watch the new green / blue / amber lines on the Cluster tab and run one canary task targeted at the new id.

### Mini PC / APU (Ryzen 680M-class, Intel iGPU, etc.)

Same path as an APU worker. Prefer **Vulkan** on AMD iGPUs (measured winner here). Unified-memory fit is conservative (~40–50% of RAM). Typical models: `qwen2.5:3b` / `7b` / `coder:7b` and `nomic-embed-text`. Do not pull gemma4 or the 30B coder.

### SBC / Raspberry Pi / small ARM

Default role **monitor/sentinel**. Skip Ollama unless the advisor says `nomic-embed-text` fits. A user-level heartbeat into the desktop (the Switch reporter on `:11620` is the template) is enough. If the board is a Jetson with real VRAM, the probe should instead land on **gpu-worker** and you can treat it like a small GPU box.

### Laptop

If it is a daily driver, prefer **controller** (open remote Odysseus; do not fight the user's session for the GPU). If it is a dedicated bench laptop with a discrete GPU and you accept it will sleep, treat it as a **gpu-worker** and document that heartbeats will go stale when the lid shuts.

### NAS / storage box

Role **storage/memory**. Use it for model archives, journal backups, or a Chroma data directory — not for generate. If it has a spare couple of GB of RAM, `nomic-embed-text` can live there and the manager can call it later. Keep the NAS's own OS and shares hands-off except for a dedicated directory you create.

### Future peripherals

These use the generic HTTP-JSON probe already documented in `ODYSSEUS_CLUSTER_TAB.md` ("Adding a device type"). The advisor will tag them **peripheral** if the probe (or `--role peripheral`) looks like a printer, camera, or sensor. Suggested integrations; none of these are installed by onboarding:

| Kind | `devices.json` type | Status endpoint to point `probe.url` at | Notes |
|---|---|---|---|
| 3D printer | `printer` | **Moonraker** `http://<ip>:7125/printer/objects/query?extruder&heater_bed&print_stats`, or **OctoPrint** `/api/printer` with a *read-only* API key in `probe.headers` | Disabled Voron/Klipper example already lives under `_examples_disabled` in `devices.json`. Never give the cluster a print-start key. |
| Camera | `camera` | A **Frigate** (or similar) HTTP API, not a raw **RTSP** URL in the Cluster tab | Store the RTSP URL for a later NVR; the card only needs JSON (cameras up, last event). |
| Cooling / sensors / plugs | `cooling`, `power-meter`, `smart-plug` | Device JSON (Shelly ` /rpc/Switch.GetStatus?id=0`) or a small local **MQTT** bridge that serves JSON | MQTT itself stays on the LAN; the cluster reads the bridge. A disabled Shelly-style example is in `devices.json`. |

After the status URL works from the manager node (`curl` as the user, no sudo), `cluster onboard --apply --yes` will add the card and a gray status line.

## What apply does not do

- It does not install packages, edit systemd system units, open firewall ports, or join Tailscale. Those stay in the checklist as paste-ready blocks with undo commands, for you to run.
- It does not start Ollama or pull models.
- It does not change `config.json` routing, the orchestrator, or existing worker agents.
- It does not overwrite the four protected devices.

## Rollback

```bash
bin/cluster onboard --remove <id> --yes
# or restore a backup:
#   cp backups/devices.json.bak-<timestamp> devices.json
systemctl --user restart llm-cluster-manager   # only needed if flows look stale
```

The Cluster tab rereads `devices.json` on the next refresh (the file is bind-mounted into Odysseus).
