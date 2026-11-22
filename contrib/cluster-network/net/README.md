# Optional Network & Exposure Scripts (Root Steps)

None of the companion services require root to run. The `.example` scripts in
this directory are **optional** system-level helpers that require `sudo` if you
choose to use them. Always inspect them, replace `<PLACEHOLDER>` values, and
test with their matching `UNDO-*` script available.

- `cluster-guard.nft.example` + `APPLY-cluster-guard.sh.example` / `UNDO-cluster-guard.sh.example` —
  Optional `nftables` firewall rule restricting manager port `:11600` to loopback,
  your worker IP, and Docker bridge subnets (`172.16.0.0/12`).
- `TAILSCALE_DESKTOP.sh.example` / `UNDO-tailscale-desktop.sh.example` —
  Exposes Odysseus over HTTPS **inside your Tailscale tailnet only**
  (`tailscale serve`, port 443) so remote devices like a tablet or handheld can
  open Odysseus without exposing it to the public internet.
- `OFFLOAD_FUNNEL.sh.example` / `UNDO-offload-funnel.sh.example` —
  **Opt-in public exposure** of the Frontier Offload server (`:11630`) via
  Tailscale Funnel (`:8443`) for cloud agents (e.g., web-hosted assistants).
  Read the security warnings inside before enabling.
