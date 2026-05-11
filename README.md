# Three-Phase Commit (3PC) Implementation

## Project Overview

Implementation of the Three-Phase Commit protocol with multi-coordinator high availability, network partition simulation, and etcd-based leader election.

## Technology Stack

| Technology | Role in This Project |
|------------|---------------------|
| Python 3 | Core programming language for all services |
| Flask | HTTP server for coordinators, participants, and dashboard |
| Docker | Packages each service into an isolated container |
| Docker Compose | Starts and wires all containers together |
| SQLite | Persists transaction state to disk |
| etcd | Distributed leader election across 3 coordinator nodes |
| Toxiproxy | Simulates network partitions by blocking traffic |
| structlog | Structured JSON-style logging across all services |
| threading | Background threads for heartbeats, lease renewal, monitors |
| REST/JSON | Communication protocol between all services |

## Setup

1. Create virtual environment: `python3 -m venv venv`
2. Activate: `source venv/bin/activate`
3. Install: `python3 -m pip install -r requirements.txt`

## How to Run

### Prerequisites

- Docker Desktop installed and running
- Python 3.8+
- `make` utility (comes with Linux/macOS)

### Step 1: Build Docker Images

```bash
make build
```

### Step 2: Start All Services

```bash
make up
```

This starts 9 containers:
- etcd (leader election service)
- toxiproxy (partition simulation)
- coordinator-1 (port 5000) — initial leader
- coordinator-2 (port 5010) — standby
- coordinator-3 (port 5020) — standby
- participant1 (port 5001)
- participant2 (port 5002)
- participant3 (port 5003)
- dashboard (port 8000)

Waits 25 seconds for all services to become healthy.

### Step 3: Verify Everything is Running

```bash
make health
```

Expected: all containers up, coordinator-1 reports `is_leader=true`, dashboard responding.

### Step 4: Set Up Toxiproxy Proxies

```bash
python3 scripts/inject_partition.py setup
```

Must be done once after `make up`.

### Step 5: Run a Transaction

```bash
make test-transaction
```

### Step 6: Open Dashboard

Open browser: `http://localhost:8000`

### Step 7: Run Integration Tests

```bash
pytest tests/test_partition_recovery.py -v
```

### Step 8: Inject a Partition

```bash
# Block participant2
python3 scripts/inject_partition.py 2 on

# Try a transaction (should abort)
make test-transaction

# Restore participant2
python3 scripts/inject_partition.py 2 off
```

### Step 9: Test Leader Failover

```bash
# Kill coordinator-1 (the current leader)
make kill-leader

# Wait ~10 seconds for etcd lease to expire and new leader to be elected
make test-leader

# Bring coordinator-1 back (it rejoins as standby)
make start-leader
```

### Step 10: Stop Everything

```bash
make down
```

### Full Cleanup

```bash
make clean
```

## Make Commands Reference

| Command | Description |
|---------|-------------|
| `make build` | Build all Docker images |
| `make up` | Start all 9 services |
| `make down` | Stop all services |
| `make restart` | Restart all services |
| `make status` | Show status of all containers |
| `make logs` | Show all service logs (live) |
| `make logs-coordinator` | Show coordinator-1 logs (live) |
| `make health` | Full system health check |
| `make test-leader` | Show leader status for all 3 coordinators |
| `make test-etcd` | Check which coordinator holds the etcd lock |
| `make test-transaction` | Run a single 3PC transaction |
| `make run-txns` | Run 10 test transactions |
| `make metrics` | Show current dashboard metrics |
| `make kill-leader` | Stop coordinator-1 to trigger re-election |
| `make start-leader` | Start coordinator-1 (rejoins as standby) |
| `make kill-participant2` | Stop participant2 to simulate node failure |
| `make start-participant2` | Start participant2 after it was stopped |
| `make test` | Run Python unit tests |
| `make clean` | Stop services, remove volumes and images |

## Key Concept: High Availability via etcd Leader Election

The system runs **3 coordinator instances** at all times. Only the one holding the etcd lease at `/3pc/leader` executes transactions. The other two are in standby.

When coordinator-1 dies:
1. Its etcd lease expires after 10 seconds
2. etcd deletes the `/3pc/leader` key automatically
3. coordinator-2 or coordinator-3 wins the next election via atomic compare-and-swap
4. The new leader starts accepting transactions immediately

This means the system tolerates coordinator failure with at most ~10 seconds of downtime — without any manual intervention.
