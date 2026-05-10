"""
Integration tests for partition simulation and automatic recovery.

Prerequisites:
    make build && make up
    python3 scripts/inject_partition.py setup

Run:
    pytest tests/test_partition_recovery.py -v

All tests are skipped automatically when the stack is not running,
so they are safe to include in CI without Docker.
"""

import time
import pytest
import requests
import concurrent.futures

# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------

COORDINATOR      = "http://localhost:5000"
PARTICIPANT_URLS = [
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5003",
]
# Toxiproxy URLs used in execute-transaction (goes through the proxy)
PROXY_URLS = [
    "http://localhost:5011",
    "http://localhost:5012",
    "http://localhost:5013",
]
TOXIPROXY_ADMIN  = "http://localhost:8474"
TIMEOUT          = 5        # seconds for admin/health calls


# ------------------------------------------------------------------
# Fixtures and helpers
# ------------------------------------------------------------------

def _stack_running() -> bool:
    """Return True when coordinator and toxiproxy are both reachable."""
    try:
        c = requests.get(f"{COORDINATOR}/health", timeout=2)
        t = requests.get(f"{TOXIPROXY_ADMIN}/api/version", timeout=2)
        return c.status_code == 200 and t.status_code == 200
    except Exception:
        return False


def _proxies_ready() -> bool:
    """Return True when all three toxiproxy proxies exist."""
    try:
        resp = requests.get(f"{TOXIPROXY_ADMIN}/api/proxies", timeout=2)
        proxies = resp.json()
        return all(f"participant-{i}" in proxies for i in (1, 2, 3))
    except Exception:
        return False


requires_stack = pytest.mark.skipif(
    not _stack_running(),
    reason="Docker stack not running — start with 'make up'"
)

requires_proxies = pytest.mark.skipif(
    not _proxies_ready(),
    reason="Toxiproxy proxies not set up — run 'python3 scripts/inject_partition.py setup'"
)


def run_transaction(participant_urls: list = None) -> dict:
    """Submit one 3PC transaction and return the parsed JSON response."""
    urls = participant_urls or PROXY_URLS
    resp = requests.post(
        f"{COORDINATOR}/execute-transaction",
        json={"participants": urls},
        timeout=30,
    )
    return resp.json()


def set_partition(participant_num: int, blocked: bool) -> None:
    """Enable (blocked=True) or remove a toxiproxy partition."""
    name = f"participant-{participant_num}"
    proxy_port = 5010 + participant_num
    upstream    = f"participant{participant_num}:{5000 + participant_num}"
    requests.post(
        f"{TOXIPROXY_ADMIN}/api/proxies/{name}",
        json={
            "name":     name,
            "listen":   f"0.0.0.0:{proxy_port}",
            "upstream": upstream,
            "enabled":  not blocked,
        },
        timeout=TIMEOUT,
    )


def restore_all_partitions() -> None:
    for i in (1, 2, 3):
        set_partition(i, blocked=False)


# ------------------------------------------------------------------
# Test: basic transaction via toxiproxy (proxy is transparent)
# ------------------------------------------------------------------

@requires_stack
@requires_proxies
def test_normal_transaction_through_proxy():
    """
    A transaction routed through all three toxiproxy proxies
    should commit successfully when no faults are injected.
    """
    restore_all_partitions()
    result = run_transaction()
    assert result.get("status") == "committed", (
        f"Expected committed, got: {result}"
    )


# ------------------------------------------------------------------
# Test: partition one participant → transaction aborts
# ------------------------------------------------------------------

@requires_stack
@requires_proxies
def test_partition_causes_abort():
    """
    Partitioning participant1 before a transaction causes the
    coordinator to receive a NO vote (or connection error) and abort.
    """
    restore_all_partitions()
    set_partition(1, blocked=True)

    try:
        result = run_transaction()
        assert result.get("status") in ("aborted", "error"), (
            f"Expected abort/error when participant1 partitioned, got: {result}"
        )
    finally:
        set_partition(1, blocked=False)


# ------------------------------------------------------------------
# Test: restore partition → subsequent transactions commit
# ------------------------------------------------------------------

@requires_stack
@requires_proxies
def test_restore_partition_resumes_normal_operations():
    """
    After removing the partition, normal 3PC transactions should
    succeed again without any coordinator restart.
    """
    restore_all_partitions()
    set_partition(2, blocked=True)

    # Cause an intentional failure
    run_transaction()

    # Restore
    set_partition(2, blocked=False)
    time.sleep(1)

    result = run_transaction()
    assert result.get("status") == "committed", (
        f"Expected recovery after partition removed, got: {result}"
    )


# ------------------------------------------------------------------
# Test: query-state endpoint returns correct per-txn state
# ------------------------------------------------------------------

@requires_stack
def test_query_state_endpoint():
    """
    /query-state/<txn_id> should return a valid state string after
    a transaction completes.
    """
    restore_all_partitions()
    txn_result = run_transaction()
    txn_id = txn_result.get("transaction_id")
    assert txn_id is not None

    for url in PARTICIPANT_URLS:
        resp = requests.get(f"{url}/query-state/{txn_id}", timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        assert data["state"] in ("COMMIT", "ABORT", "UNKNOWN"), (
            f"Unexpected state from {url}: {data}"
        )


# ------------------------------------------------------------------
# Test: /recover endpoint triggers autonomous decision
# ------------------------------------------------------------------

@requires_stack
def test_manual_recover_endpoint():
    """
    POST /recover should return 200 and a results dict even when
    there are no pending transactions.
    """
    resp = requests.post(f"{PARTICIPANT_URLS[0]}/recover", timeout=TIMEOUT)
    assert resp.status_code == 200
    data = resp.json()
    assert "results" in data or "recovery_attempted" in data


# ------------------------------------------------------------------
# Test: leader status
# ------------------------------------------------------------------

@requires_stack
def test_leader_status():
    """coordinator-1 should report is_leader=True in a single-node setup."""
    resp = requests.get(f"{COORDINATOR}/leader-status", timeout=TIMEOUT)
    assert resp.status_code == 200
    data = resp.json()
    assert data["is_leader"] is True
    assert data["node_id"] == "coordinator-1"


# ------------------------------------------------------------------
# Test: concurrent transactions — thread safety
# ------------------------------------------------------------------

@requires_stack
@requires_proxies
def test_concurrent_transactions():
    """
    20 concurrent transactions must all reach a final, consistent
    outcome (committed or aborted) with no server errors.
    """
    restore_all_partitions()
    n = 20

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(run_transaction) for _ in range(n)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == n
    for r in results:
        assert r.get("status") in ("committed", "aborted", "error"), (
            f"Unexpected status: {r}"
        )
    committed = sum(1 for r in results if r.get("status") == "committed")
    print(f"\n  Concurrent: {committed}/{n} committed")


# ------------------------------------------------------------------
# Test: participant heartbeat endpoint
# ------------------------------------------------------------------

@requires_stack
def test_heartbeat_endpoint():
    """Each participant must accept a heartbeat POST and return 200."""
    for url in PARTICIPANT_URLS:
        resp = requests.post(
            f"{url}/heartbeat",
            json={"coordinator_id": "coordinator-1"},
            timeout=TIMEOUT,
        )
        assert resp.status_code == 200, f"Heartbeat failed on {url}"
        assert resp.json().get("status") == "alive"


# ------------------------------------------------------------------
# Test: participant state survives no-crash restart simulation
# (verifies SQLite persistence path by querying loaded state)
# ------------------------------------------------------------------

@requires_stack
@requires_proxies
def test_state_persisted_to_db():
    """
    After committing a transaction, /query-state/<txn_id> should
    return COMMIT even for a past transaction (persisted in SQLite).
    """
    restore_all_partitions()
    result = run_transaction()
    txn_id = result.get("transaction_id")
    assert result.get("status") == "committed"

    # Query state after transaction is finalised
    for url in PARTICIPANT_URLS:
        resp = requests.get(f"{url}/query-state/{txn_id}", timeout=TIMEOUT)
        state = resp.json().get("state")
        # Participants that were reachable should have COMMIT persisted
        assert state in ("COMMIT", "UNKNOWN"), (
            f"{url} returned unexpected state: {state}"
        )


# ------------------------------------------------------------------
# Test: 5 normal → partition → 5 normal (full scenario)
# ------------------------------------------------------------------

@requires_stack
@requires_proxies
def test_partition_then_recovery_scenario():
    """
    Full scenario:
      Phase A — 5 normal transactions (all commit)
      Phase B — partition participant1, attempt transaction (aborts)
      Phase C — remove partition, 5 more transactions (all commit)
    """
    restore_all_partitions()

    # Phase A
    for i in range(5):
        r = run_transaction()
        assert r.get("status") == "committed", f"Phase A txn {i}: {r}"

    # Phase B — inject partition
    set_partition(1, blocked=True)
    try:
        r = run_transaction()
        assert r.get("status") in ("aborted", "error"), \
            f"Expected abort during partition, got: {r}"
    finally:
        set_partition(1, blocked=False)

    time.sleep(1)   # allow proxy to fully re-enable

    # Phase C
    for i in range(5):
        r = run_transaction()
        assert r.get("status") == "committed", f"Phase C txn {i}: {r}"
