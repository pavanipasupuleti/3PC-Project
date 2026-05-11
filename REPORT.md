# Three-Phase Commit (3PC) Protocol — Project Report

**Subject:** Distributed Systems
**Topic:** Implement Three-Phase Commit (3PC) with Network Partition Simulation and Multi-Coordinator High Availability

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Objective](#2-objective)
3. [Problem Statement](#3-problem-statement)
4. [Why 3PC Over 2PC](#4-why-3pc-over-2pc)
5. [Technology Stack](#5-technology-stack)
6. [Folder Structure](#6-folder-structure)
7. [Key Files Explained](#7-key-files-explained)
8. [Full Transaction Flow](#8-full-transaction-flow)
9. [Coordinator and Participant Communication](#9-coordinator-and-participant-communication)
10. [Leader Election Using etcd](#10-leader-election-using-etcd)
11. [Network Partition Simulation with Toxiproxy](#11-network-partition-simulation-with-toxiproxy)
12. [SQLite Persistence](#12-sqlite-persistence)
13. [Thread Safety](#13-thread-safety)
14. [Testing Strategy](#14-testing-strategy)
15. [Architecture](#15-architecture)
16. [How to Run the Project](#16-how-to-run-the-project)
17. [Example Commands and Expected Outputs](#17-example-commands-and-expected-outputs)
18. [Conclusion](#18-conclusion)

---

## 1. Project Overview

This project is a full working implementation of the **Three-Phase Commit (3PC)** distributed transaction protocol. It is built using Python and Flask, runs inside Docker containers, and simulates real-world distributed system scenarios including network partitions and coordinator failure with automatic leader re-election.

The system consists of:

- **Three Coordinators** — only the etcd leader executes transactions; the other two are hot standbys
- **Three Participants** — nodes that vote, acknowledge, and commit/abort
- **etcd** — distributed key-value store used for leader election across coordinators
- **Toxiproxy** — a tool that blocks network traffic to simulate partitions
- **A Dashboard** — a live web UI showing transaction metrics and charts
- **SQLite databases** — for persisting transaction states across restarts

Every component is coded, running, and testable. Transactions are triggered via HTTP, partitions are injected on demand, and coordinator failover happens automatically via etcd lease expiry.

---

## 2. Objective

1. Implement the complete Three-Phase Commit protocol from scratch in Python
2. Demonstrate high availability using three coordinator nodes with etcd leader election
3. Simulate network partitions using Toxiproxy to verify the system handles failures correctly
4. Persist transaction state to disk so participants survive crashes and restarts
5. Build an observable system with a live dashboard, structured logs, and metrics

---

## 3. Problem Statement

In a distributed system, multiple nodes must agree on whether to commit or abort a shared operation. For example, transferring money between two bank accounts on different servers — both must apply the change or neither should.

The challenge is:

> **What happens if the coordinator crashes in the middle of the process?**

With naive approaches, participants are left in an uncertain state — they do not know whether to commit or abort, and are **blocked** waiting indefinitely for the coordinator to recover.

This project addresses this through two mechanisms:
1. **3PC's PRE_COMMIT phase** — creates a safe intermediate state that eliminates the blocking window
2. **etcd leader election** — if the active coordinator dies, a standby takes over within ~10 seconds

---

## 4. Why 3PC Over 2PC

### Two-Phase Commit (2PC) — The Problem

2PC has two phases:
1. **Phase 1 (Voting):** Coordinator asks all participants "Can you commit?" — they reply YES or NO
2. **Phase 2 (Decision):** If all say YES, coordinator sends COMMIT; otherwise ABORT

**The blocking problem:** A participant that voted YES and is waiting for the coordinator's decision is completely stuck if the coordinator crashes at that point. It cannot commit (maybe another participant voted NO) and cannot abort (maybe the coordinator already sent COMMIT to others). It must wait until the coordinator recovers.

### Three-Phase Commit (3PC) — The Solution

3PC adds a PRE_COMMIT phase in the middle:

| Phase | 2PC | 3PC |
|-------|-----|-----|
| Phase 1 | Voting (CAN_COMMIT) | Voting (CAN_COMMIT) |
| Phase 2 | Decision (COMMIT/ABORT) | Pre-commit (PRE_COMMIT) |
| Phase 3 | — | Final commit (DO_COMMIT) |

Once a participant enters PRE_COMMIT, it knows every other participant voted YES. If the coordinator dies after this point, the new coordinator elected via etcd knows it can safely resume Phase 3 — no participant is in an ambiguous state.

### Summary Table

| Feature | 2PC | 3PC |
|---------|-----|-----|
| Number of phases | 2 | 3 |
| Blocking on coordinator failure | YES | NO (new leader resumes) |
| Communication rounds | 2 | 3 |
| High availability | Manual failover | Automatic via etcd |

---

## 5. Technology Stack

| Technology | Role in This Project |
|------------|---------------------|
| Python 3 | Core programming language for all services |
| Flask | HTTP server for coordinators, participants, and dashboard |
| Docker | Packages each service into an isolated container |
| Docker Compose | Starts and wires all 9 containers together |
| SQLite | Persists transaction state to disk |
| etcd | Distributed leader election across 3 coordinator nodes |
| Toxiproxy | Simulates network partitions by blocking traffic |
| structlog | Structured JSON-style logging across all services |
| threading | Background threads for heartbeats, lease renewal, monitors |
| REST/JSON | Communication protocol between all services |

**Python** — Fast to develop, excellent HTTP library support, no compilation. Ideal for a research implementation.

**Flask** — Lightweight HTTP framework. Each service becomes a small web server focused on protocol logic.

**Docker** — Simulates a real distributed system on one machine by giving each service its own isolated network address and filesystem.

**Docker Compose** — Defines all 9 services in one file, started with `make up`.

**SQLite** — File-based database. No separate server needed. Each participant writes its transaction state to a `.db` file so that if the container restarts, it picks up from where it left off.

**etcd** — Distributed key-value store designed for coordination (used by Kubernetes). Used for **leader election** — only the coordinator holding the etcd lock executes transactions.

**Toxiproxy** — Programmable reverse proxy by Shopify. Routes traffic through a controllable proxy so any participant can be partitioned instantly via the admin API.

**structlog** — Every log line includes key-value pairs (`transaction_id`, `phase`, `state`), making logs machine-readable and easy to trace.

**threading** — Used for: coordinator heartbeat sender, participant heartbeat monitor, etcd lease renewal, and protecting shared data with `threading.Lock`.

**REST/JSON** — All communication is HTTP POST/GET with JSON bodies — simple, language-agnostic, and easy to test with `curl`.

---

## 6. Folder Structure

```
3PC-Project/
|
|-- coordinator/           # The coordinator service (3 instances, same code)
|   |-- __init__.py
|   |-- server.py          # Flask HTTP server + 3PC protocol orchestration
|   |-- state.py           # Coordinator state machine (WAIT, PRE_COMMIT, COMMIT, ABORT)
|   |-- messages.py        # Message types (CAN_COMMIT, PRE_COMMIT, DO_COMMIT, ABORT, etc.)
|   |-- leader_election.py # etcd-based distributed leader election
|   |-- heartbeat.py       # Sends heartbeat pings to all participants
|
|-- participant/           # The participant service (3 instances run the same code)
|   |-- __init__.py
|   |-- server.py          # Flask HTTP server + phase handlers
|   |-- state_manager.py   # Thread-safe multi-transaction state tracker
|   |-- timeout_detector.py# Detects coordinator silence (HeartbeatMonitor)
|
|-- storage/               # Database layer
|   |-- database.py        # Coordinator SQLite store (transactions + events)
|   |-- participant_database.py  # Participant SQLite store (per-txn states)
|
|-- dashboard/             # Live web dashboard
|   |-- app.py             # Flask server serving charts and metrics
|   |-- templates/
|       |-- dashboard.html # Auto-refreshing HTML page with Plotly charts
|
|-- tests/                 # All test files
|   |-- __init__.py
|   |-- test_state.py                 # Unit tests for coordinator state machine
|   |-- test_participant_state.py     # Unit tests for participant state manager
|   |-- test_partition_recovery.py   # Integration tests (requires Docker stack)
|
|-- scripts/               # Utility scripts
|   |-- inject_partition.py # CLI tool to control Toxiproxy partitions
|
|-- metrics/               # Metrics collection
|   |-- collector.py       # In-memory metrics (commit rate, latencies, failures)
|
|-- docs/                  # Protocol diagrams (PNG)
|-- data/                  # SQLite database files (created at runtime)
|-- docker-compose.yml     # Defines all 9 services and their configuration
|-- Dockerfile             # How to build the Python application image
|-- Makefile               # Shortcuts for build, run, test, clean, failover
|-- requirements.txt       # Python package dependencies
```

---

## 7. Key Files Explained

### `coordinator/server.py`

The brain of the coordinator. It:
1. Reads `NODE_ID` from the environment (`coordinator-1`, `coordinator-2`, or `coordinator-3`)
2. Participates in etcd leader election on startup
3. Serves `/execute-transaction` — only processes requests if `election.is_leader` is True (returns 503 otherwise)
4. Runs `execute_3pc_protocol()` which sends CAN_COMMIT → PRE_COMMIT → DO_COMMIT in sequence
5. Records metrics and saves every transaction to SQLite

---

### `coordinator/leader_election.py`

Makes the coordinator cluster highly available.

**How it works:**
1. On startup, each coordinator connects to etcd
2. Tries to write its `NODE_ID` to the key `/3pc/leader` using a **Compare-And-Swap (CAS)** transaction — succeeds only if the key does not already exist
3. The winner gets a **10-second lease** renewed every **5 seconds** by a background thread
4. If the leader crashes, its lease expires in ≤10 seconds and etcd deletes the key
5. A standby coordinator calls `try_become_leader()` and wins the next election

**Graceful fallback:** If etcd itself is unreachable, the coordinator assumes leadership so transactions are never blocked by an infrastructure failure.

---

### `coordinator/heartbeat.py`

A background daemon thread inside each coordinator.

Every 2 seconds, sends `POST /heartbeat` to all registered participant URLs. This resets each participant's internal timeout clock, preventing false "coordinator is dead" alarms while the coordinator is healthy.

---

### `participant/timeout_detector.py` — HeartbeatMonitor

Runs inside each participant as a background daemon thread.

- Checks every 0.5 seconds whether a heartbeat arrived recently
- Only fires when a transaction is **actively in-flight** (prevents false alarms between transactions)
- Default timeout: 5 seconds (configurable via `HEARTBEAT_TIMEOUT` env var)
- When timeout fires, calls `_on_coordinator_timeout()` — which logs the event so the new coordinator can pick up

```
t=0s   Transaction starts
t=2s   Coordinator heartbeat received
t=4s   Coordinator CRASHES
t=9s   5-second silence window expires → timeout fires
t=9s+  New coordinator (etcd winner) resumes Phase 3
```

---

### `participant/state_manager.py` — GlobalStateManager

Manages state for **multiple concurrent transactions** in memory, with every change persisted to SQLite.

Valid state transitions enforced:
```
INIT → READY → PRE_COMMIT → COMMIT
                           → ABORT
INIT → ABORT
READY → ABORT
```

Invalid transitions are rejected and logged. On startup, `_restore_from_db()` reloads all non-final transactions from SQLite so a restarted participant can continue from its last known state.

---

### `storage/database.py` — TransactionStore (Coordinator DB)

The coordinator's SQLite database tracking:

1. **`transactions` table** — one row per transaction: status, participant count, per-phase latencies, timestamps
2. **`transaction_events` table** — full event log (state transitions, phase completions, failures)

This feeds the dashboard charts and provides a complete audit trail.

---

### `storage/participant_database.py` — ParticipantStore

SQLite database for each participant.

Schema:
```sql
CREATE TABLE participant_transactions (
    txn_id         TEXT PRIMARY KEY,
    participant_id TEXT NOT NULL,
    state          TEXT NOT NULL,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP
)
```

Every state transition calls `save_state()` before returning. On startup, `get_pending()` restores in-progress transactions.

---

### `scripts/inject_partition.py`

CLI tool for controlling Toxiproxy via its REST API at `http://localhost:8474`.

```
setup          Create proxies for participant-1, participant-2, participant-3
status         Show which proxies are enabled/disabled
1 on           Disable proxy for participant1 (network partition)
1 off          Re-enable proxy for participant1 (restore connection)
restore        Re-enable all three proxies
latency 1 200  Inject 200ms one-way delay to participant1
```

---

## 8. Full Transaction Flow

### Phase 0: Initialization

```
Coordinator → POST /init-transaction → Participant 1
Coordinator → POST /init-transaction → Participant 2
Coordinator → POST /init-transaction → Participant 3
```

Each participant creates a new transaction entry in state `INIT`.

---

### Phase 1: CAN_COMMIT (Voting)

```
Coordinator (state: WAIT)
    → POST /message {type: CAN_COMMIT} → Participant 1  → YES (→ READY)
    → POST /message {type: CAN_COMMIT} → Participant 2  → YES (→ READY)
    → POST /message {type: CAN_COMMIT} → Participant 3  → YES (→ READY)
```

If **any** participant votes NO or is unreachable, coordinator sends ABORT to all.

---

### Phase 2: PRE_COMMIT (Prepare)

```
Coordinator (state: PRE_COMMIT)
    → POST /message {type: PRE_COMMIT} → Participant 1  → ACK (→ PRE_COMMIT)
    → POST /message {type: PRE_COMMIT} → Participant 2  → ACK (→ PRE_COMMIT)
    → POST /message {type: PRE_COMMIT} → Participant 3  → ACK (→ PRE_COMMIT)
```

**Safety checkpoint.** Once a participant is in PRE_COMMIT, every participant has voted YES. The new coordinator can safely resume from here if the active one dies.

---

### Phase 3: DO_COMMIT (Final Commit)

```
Coordinator (state: COMMIT)
    → POST /message {type: DO_COMMIT} → Participant 1  → COMMITTED
    → POST /message {type: DO_COMMIT} → Participant 2  → COMMITTED
    → POST /message {type: DO_COMMIT} → Participant 3  → COMMITTED
```

Transaction complete. All state saved to SQLite. Dashboard updated.

---

### State Diagram

```
Coordinator:
  INIT → WAIT → PRE_COMMIT → COMMIT
                  ↓               ↓
                ABORT          ABORT (if ACKs fail)

Participant:
  INIT → READY → PRE_COMMIT → COMMIT
    ↓       ↓         ↓
  ABORT   ABORT     ABORT
```

---

## 9. Coordinator and Participant Communication

All communication is HTTP with JSON bodies.

### Message Format

```json
{
    "transaction_id": "txn-abc123",
    "sender": "coordinator",
    "receiver": "http://participant1:5001",
    "message_type": "CAN_COMMIT",
    "state": "WAIT",
    "timestamp": 1700000000.0,
    "data": {}
}
```

### Endpoints

| Service | Endpoint | Purpose |
|---------|----------|---------|
| Coordinator | `POST /execute-transaction` | Start a full 3PC transaction |
| Coordinator | `GET /leader-status` | Check if this node is the etcd leader |
| Coordinator | `GET /health` | Health check |
| Coordinator | `GET /metrics` | Metrics snapshot |
| Participant | `POST /init-transaction` | Register a new transaction |
| Participant | `POST /message` | Receive CAN_COMMIT, PRE_COMMIT, DO_COMMIT, ABORT |
| Participant | `POST /heartbeat` | Receive liveness ping from coordinator |
| Participant | `GET /query-state/<txn_id>` | Return state for a specific transaction |
| Participant | `GET /state` | Return all tracked transaction states |

Protocol messages (`/message`) have **no timeout** — the coordinator waits as long as needed. Only admin calls (heartbeats, health checks) use short timeouts (1–5 seconds).

---

## 10. Leader Election Using etcd

### Why Three Coordinators?

A single coordinator is a single point of failure. With three coordinators, if one dies, a standby is elected within ~10 seconds and begins serving transactions — no manual intervention required.

### How It Works

```
On startup, each coordinator:
    → Connects to etcd at :2379
    → Attempts CAS: write NODE_ID to /3pc/leader only if key does not exist
    → Winner: is_leader = True, starts 10s lease, renews every 5s
    → Losers: is_leader = False, enter standby

On coordinator-1 crash:
    → Heartbeat thread stops
    → etcd lease expires (≤10s)
    → etcd deletes /3pc/leader
    → coordinator-2 or coordinator-3 wins next CAS
    → New leader starts serving transactions
```

### Lease and Heartbeat Constants

| Constant | Value |
|----------|-------|
| `LEASE_TTL` | 10 seconds |
| `HEARTBEAT_INTERVAL` | 5 seconds |
| Max failover time | ~10 seconds |

### Graceful Fallback

If etcd is unreachable, the coordinator assumes leadership so the system continues operating. This is logged as a warning.

---

## 11. Network Partition Simulation with Toxiproxy

### Architecture

```
Without Toxiproxy:
  Coordinator → participant1:5001 (direct)

With Toxiproxy:
  Coordinator → toxiproxy:5011 → participant1:5001

Partition injected:
  Coordinator → toxiproxy:5011 → [CONNECTION DROPPED]
```

| Proxy Name | Toxiproxy Port | Upstream |
|------------|---------------|----------|
| participant-1 | 5011 | participant1:5001 |
| participant-2 | 5012 | participant2:5002 |
| participant-3 | 5013 | participant3:5003 |

### Commands

```bash
python3 scripts/inject_partition.py setup        # Create proxies (run once)
python3 scripts/inject_partition.py 1 on         # Partition participant1
python3 scripts/inject_partition.py 1 off        # Restore participant1
python3 scripts/inject_partition.py latency 2 200 # Add 200ms delay to participant2
python3 scripts/inject_partition.py status        # Show proxy states
```

### Why Toxiproxy vs. Killing a Container?

Killing a container stops the entire service. Toxiproxy gives **selective network control** — the participant process keeps running but its network traffic is blocked. This more accurately simulates a real network partition where a switch or router fails between nodes.

---

## 12. SQLite Persistence

### What Gets Saved

**Participant database** (`data/3pc_participant_N.db`):
```
Table: participant_transactions
  txn_id, participant_id, state, created_at, updated_at
```

Every state transition writes to this table before returning. On restart, `_restore_from_db()` reloads all non-final transactions.

**Coordinator database** (`data/3pc_transactions.db`):
```
Table: transactions
  txn_id, status, num_participants,
  phase1_latency_ms, phase2_latency_ms, phase3_latency_ms, total_latency_ms,
  created_at, completed_at

Table: transaction_events
  txn_id, event_type, phase, timestamp, details
```

### Crash Recovery Flow

```
Participant container restarts
    ↓
GlobalStateManager.__init__()
    ↓
_restore_from_db(): SELECT where state NOT IN ('COMMIT', 'ABORT')
    ↓
Pending transactions reloaded into memory
    ↓
HeartbeatMonitor starts watching for new coordinator heartbeats
```

---

## 13. Thread Safety

Flask handles each HTTP request in a separate thread. Multiple transactions can arrive concurrently. All shared state is protected with explicit locks.

| Location | Protects |
|----------|----------|
| `GlobalStateManager` — `threading.Lock` | `_states` dict (txn_id → state) |
| `CoordinatorHeartbeat` — `threading.Lock` | `_participants` dict |
| `HeartbeatMonitor` — `threading.Lock` | `_last_heartbeat`, `_active_transaction` |
| `LeaderElection` — `threading.Lock` | `is_leader` flag |
| `coordinator/server.py` — `threading.Lock` | `active_transactions` dict |
| `ParticipantStore` — `threading.Lock` | SQLite read/write operations |
| `TransactionStore` — `threading.Lock` | SQLite read/write operations |
| `MetricsCollector` — `threading.Lock` | All counter and latency updates |

---

## 14. Testing Strategy

### Unit Tests (No Docker Needed)

```bash
make test
```

Verifies state machines in isolation:
- Valid transitions succeed
- Invalid transitions are rejected
- Final states (COMMIT, ABORT) cannot transition further

### Integration Tests (Requires Docker + Toxiproxy)

```bash
make up
python3 scripts/inject_partition.py setup
pytest tests/test_partition_recovery.py -v
```

All tests use `@requires_stack` and `@requires_proxies` decorators and automatically skip if Docker is not running.

| Test | What It Checks |
|------|---------------|
| `test_normal_transaction_through_proxy` | Traffic through Toxiproxy works with no partition |
| `test_partition_causes_abort` | Blocking participant1 causes abort |
| `test_restore_partition_resumes_normal_operations` | After healing, transactions commit again |
| `test_query_state_endpoint` | `/query-state/<txn_id>` returns valid state after commit |
| `test_manual_recover_endpoint` | `POST /recover` returns 200 |
| `test_leader_status` | Coordinator reports `is_leader=True` correctly |
| `test_concurrent_transactions` | 20 parallel transactions all reach a consistent final state |
| `test_heartbeat_endpoint` | Each participant accepts heartbeat |
| `test_state_persisted_to_db` | After commit, `/query-state` returns `COMMIT` from SQLite |
| `test_partition_then_recovery_scenario` | Full scenario: 5 normal → partition → 5 normal |

---

## 15. Architecture

### High-Level Architecture

```
+------------------------------------------------------------------+
|                    Docker Network (3pc-network)                  |
|                                                                  |
|   +----------+    CAS /3pc/leader    +-------------+            |
|   |   etcd   | <-------------------> | Coordinator-1 :5000 |    |
|   |  :2379   |                       |  (Leader)   |            |
|   |          | <-------------------> | Coordinator-2 :5010 |    |
|   |          |                       |  (Standby)  |            |
|   |          | <-------------------> | Coordinator-3 :5020 |    |
|   +----------+                       |  (Standby)  |            |
|                                      +------+------+            |
|                                             |                   |
|                              3PC Protocol + Heartbeats          |
|                                             |                   |
|              +------------------------------+------------------+|
|              |                              |                   ||
|   +----------v--+          +---------------v-+   +-----------v+||
|   | Toxiproxy   |          | Toxiproxy       |   | Toxiproxy  |||
|   |   :5011     |          |   :5012         |   |   :5013    |||
|   +----------+--+          +------+----------+   +------+-----+||
|              |                    |                      |      ||
|   +----------v--+          +------v----------+   +------v-----+||
|   |Participant 1|          | Participant 2   |   |Participant 3|||
|   |   :5001     |          |   :5002         |   |   :5003    |||
|   | SQLite DB   |          | SQLite DB       |   | SQLite DB  |||
|   +-------------+          +-----------------+   +------------+||
|                                                                  |
|  +--------------------+     +------------------------------+    |
|  | Dashboard  :8000   |     | Toxiproxy Admin  :8474       |    |
|  | Flask + Plotly     |     | Partition control API        |    |
|  +--------------------+     +------------------------------+    |
+------------------------------------------------------------------+
```

### Component Interaction

```
Client
    |
    | POST /execute-transaction
    v
Active Coordinator (etcd leader)
    | 1. Check election.is_leader (returns 503 if not leader)
    | 2. POST /init-transaction to all participants (through Toxiproxy)
    | 3. POST /message CAN_COMMIT → collect YES/NO votes
    | 4. POST /message PRE_COMMIT → collect ACKs
    | 5. POST /message DO_COMMIT → final commit
    | 6. Save result to SQLite
    | 7. Send heartbeats every 2s (background thread)
    v
Participants (P1, P2, P3) — each independently:
    - Manages state: INIT → READY → PRE_COMMIT → COMMIT/ABORT
    - Saves every state change to SQLite
    - Monitors heartbeat (background thread, 5s timeout)
    v
SQLite databases
    - Coordinator: transaction history + event log
    - Participants: per-participant transaction states
```

---

## 16. How to Run the Project

### Prerequisites

- Docker Desktop installed and running
- Python 3.8+
- `make` utility

### Start

```bash
make build      # Build images
make up         # Start all 9 containers (waits 25s for health)
make health     # Verify everything is running
python3 scripts/inject_partition.py setup  # Set up Toxiproxy proxies (once)
make test-transaction  # Run a transaction
```

Open dashboard: `http://localhost:8000`

### Test Leader Failover

```bash
make kill-leader    # Stop coordinator-1
make test-leader    # coordinator-2 or coordinator-3 should now be leader
make start-leader   # Bring coordinator-1 back as standby
```

### Test Network Partition

```bash
python3 scripts/inject_partition.py 2 on   # Partition participant2
make test-transaction                        # Should abort
python3 scripts/inject_partition.py 2 off  # Restore
make test-transaction                        # Should commit
```

### Stop

```bash
make down    # Stop all services
make clean   # Stop + remove volumes and images
```

---

## 17. Example Commands and Expected Outputs

### Health Check

```bash
$ make health

FULL SYSTEM HEALTH CHECK
======================================

Docker Containers:
3pc-etcd               Up   0.0.0.0:2379->2379/tcp
3pc-toxiproxy          Up   0.0.0.0:8474->8474/tcp
3pc-coordinator        Up   0.0.0.0:5000->5000/tcp
3pc-coordinator-2      Up   0.0.0.0:5010->5010/tcp
3pc-coordinator-3      Up   0.0.0.0:5020->5020/tcp
3pc-participant1       Up   0.0.0.0:5001->5001/tcp
3pc-participant2       Up   0.0.0.0:5002->5002/tcp
3pc-participant3       Up   0.0.0.0:5003->5003/tcp
3pc-dashboard          Up   0.0.0.0:8000->8000/tcp

etcd Status:
http://localhost:2379 is healthy

Leader Status:
{ "is_leader": true, "node_id": "coordinator-1", "current_leader": "coordinator-1" }
```

---

### Successful Transaction

```bash
$ make test-transaction
{
    "status": "committed",
    "transaction_id": "txn-abc123",
    "final_state": "COMMIT",
    "state_history": ["WAIT", "PRE_COMMIT", "COMMIT"],
    "participants": 3,
    "votes":   { "...5011": "YES",  "...5012": "YES",  "...5013": "YES" },
    "acks":    { "...5011": "ACK",  "...5012": "ACK",  "...5013": "ACK" },
    "commits": { "...5011": "COMMITTED", "...5012": "COMMITTED", "...5013": "COMMITTED" }
}
```

---

### Transaction with Partition

```bash
$ python3 scripts/inject_partition.py 1 on
[OK]  participant1: PARTITIONED

$ make test-transaction
{
    "status": "aborted",
    "final_state": "ABORT",
    "reason": "One or more participants voted NO",
    "votes": { "...5011": "NO", "...5012": "YES", "...5013": "YES" }
}

$ python3 scripts/inject_partition.py 1 off
[OK]  participant1: RESTORED
```

---

### Leader Failover

```bash
$ make kill-leader
coordinator-1 stopped. A new leader should be elected within ~10s.

$ make test-leader    # After ~10 seconds
coordinator-1 (port 5000):  not responding
coordinator-2 (port 5010):  { "is_leader": true, "node_id": "coordinator-2" }
coordinator-3 (port 5020):  { "is_leader": false, "node_id": "coordinator-3" }

$ make start-leader
coordinator-1 started. It will rejoin as standby.
```

---

### Load Test

```bash
$ make run-txns
Running 10 transactions...
✓ Transaction 1
✓ Transaction 2
...
✓ Transaction 10

$ make metrics
Total: 10, Committed: 10, Success Rate: 100.0%
```

---

## 18. Conclusion

This project implements the Three-Phase Commit protocol with production-grade high availability features in a fully containerized system.

### What Was Built

- Full 3PC protocol (CAN_COMMIT → PRE_COMMIT → DO_COMMIT) over HTTP/REST
- Three-coordinator cluster with etcd-based leader election and automatic failover
- Network partition simulation using Toxiproxy
- SQLite state persistence for crash recovery
- Thread-safe concurrent transaction handling with explicit locks
- Live metrics dashboard using Flask and Plotly
- Comprehensive integration test suite with concurrent transaction tests

### Key Takeaways

1. **3PC's non-blocking advantage comes from PRE_COMMIT** — it creates a safe window where all participants know every peer voted YES, allowing the new coordinator to resume safely
2. **etcd leader election enables automatic failover** — coordinator failure is tolerated with ≤10 seconds downtime, no manual restart needed
3. **Persistence is essential for recovery** — without SQLite, a restarted participant cannot know its pre-crash state and cannot participate in coordinator-led recovery
4. **Thread safety is non-trivial** — every shared data structure requires explicit locking under concurrent Flask requests
5. **Graceful degradation matters** — if etcd is down, coordinators fall back to assuming leadership so transactions are never blocked by infrastructure failure
