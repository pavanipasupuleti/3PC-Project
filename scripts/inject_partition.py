#!/usr/bin/env python3
"""
Toxiproxy partition injection for 3PC network fault simulation.

Proxies must be created with `setup` before toggling them.
Each proxy sits between the coordinator/peers and a participant,
so disabling it simulates a network partition to that node.

Usage:
    python3 scripts/inject_partition.py setup          # Create all proxies
    python3 scripts/inject_partition.py status         # Show proxy states
    python3 scripts/inject_partition.py 1 on           # Partition participant1
    python3 scripts/inject_partition.py 1 off          # Restore  participant1
    python3 scripts/inject_partition.py restore        # Remove all partitions
    python3 scripts/inject_partition.py latency 1 200  # Add 200 ms latency

Proxy port mapping:
    participant1 → toxiproxy :5011 → participant1:5001
    participant2 → toxiproxy :5012 → participant2:5002
    participant3 → toxiproxy :5013 → participant3:5003
"""

import sys
import json
import requests

TOXIPROXY_ADMIN = "http://localhost:8474"

# Proxy definitions: participant-id -> config
PROXIES = {
    "1": {
        "name":     "participant-1",
        "listen":   "0.0.0.0:5011",
        "upstream": "participant1:5001",
    },
    "2": {
        "name":     "participant-2",
        "listen":   "0.0.0.0:5012",
        "upstream": "participant2:5002",
    },
    "3": {
        "name":     "participant-3",
        "listen":   "0.0.0.0:5013",
        "upstream": "participant3:5003",
    },
}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _admin(method: str, path: str, **kwargs) -> requests.Response:
    try:
        return getattr(requests, method)(
            f"{TOXIPROXY_ADMIN}{path}", timeout=5, **kwargs
        )
    except requests.exceptions.ConnectionError:
        print(f"[ERR] Cannot reach toxiproxy at {TOXIPROXY_ADMIN}")
        print("      Is the stack running?  Try: make up")
        sys.exit(1)


def _resolve(participant_id: str) -> dict:
    if participant_id not in PROXIES:
        print(f"[ERR] Unknown participant '{participant_id}'. Use 1, 2 or 3.")
        sys.exit(1)
    return PROXIES[participant_id]


# ------------------------------------------------------------------
# Commands
# ------------------------------------------------------------------

def setup_proxies() -> None:
    """Create all three proxies in toxiproxy (idempotent)."""
    for pid, cfg in PROXIES.items():
        resp = _admin(
            "post",
            "/api/proxies",
            json={
                "name":     cfg["name"],
                "listen":   cfg["listen"],
                "upstream": cfg["upstream"],
                "enabled":  True,
            },
        )
        if resp.status_code in (200, 201):
            print(f"[OK]  Created  {cfg['name']:15s}  {cfg['listen']} → {cfg['upstream']}")
        elif resp.status_code == 409:
            print(f"[--]  Exists   {cfg['name']}")
        else:
            print(f"[ERR] {cfg['name']}: {resp.status_code} {resp.text}")


def partition(participant_id: str, block: bool) -> None:
    """
    Block (partition) or restore a participant.

    block=True  → disable proxy → all traffic dropped
    block=False → enable  proxy → traffic flows normally
    """
    cfg = _resolve(participant_id)
    resp = _admin(
        "post",
        f"/api/proxies/{cfg['name']}",
        json={
            "name":     cfg["name"],
            "listen":   cfg["listen"],
            "upstream": cfg["upstream"],
            "enabled":  not block,         # disabled proxy = partition
        },
    )
    if resp.status_code == 200:
        label = "PARTITIONED  (traffic blocked)" if block else "RESTORED     (traffic flowing)"
        print(f"[OK]  participant{participant_id}: {label}")
    else:
        print(f"[ERR] {resp.status_code} {resp.text}")


def add_latency(participant_id: str, latency_ms: int) -> None:
    """Inject one-way latency (ms) to simulate slow network."""
    cfg = _resolve(participant_id)
    resp = _admin(
        "post",
        f"/api/proxies/{cfg['name']}/toxics",
        json={
            "name":       f"latency-{cfg['name']}",
            "type":       "latency",
            "stream":     "downstream",
            "attributes": {"latency": latency_ms, "jitter": 0},
        },
    )
    if resp.status_code in (200, 201):
        print(f"[OK]  participant{participant_id}: {latency_ms} ms latency injected")
    else:
        print(f"[ERR] {resp.status_code} {resp.text}")


def restore_all() -> None:
    """Re-enable all proxies and clear latency toxics."""
    for pid in PROXIES:
        partition(pid, block=False)


def show_status() -> None:
    """Print current proxy states."""
    resp = _admin("get", "/api/proxies")
    proxies: dict = resp.json()

    print(f"\n{'Proxy':<20} {'Listen':<18} {'Upstream':<26} Status")
    print("-" * 72)
    for name, info in proxies.items():
        state = "OK" if info.get("enabled") else "PARTITIONED"
        print(f"{name:<20} {info['listen']:<18} {info['upstream']:<26} {state}")
    print()


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1].lower()

    if cmd == "setup":
        setup_proxies()

    elif cmd == "status":
        show_status()

    elif cmd == "restore":
        restore_all()

    elif cmd == "latency" and len(sys.argv) == 4:
        add_latency(sys.argv[2], int(sys.argv[3]))

    elif cmd in ("1", "2", "3") and len(sys.argv) == 3:
        action = sys.argv[2].lower()
        if action not in ("on", "off"):
            print("[ERR] Action must be 'on' (partition) or 'off' (restore)")
            sys.exit(1)
        partition(cmd, block=(action == "on"))

    else:
        print(__doc__)
        sys.exit(1)
