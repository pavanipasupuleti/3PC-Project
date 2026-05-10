# Three-Phase Commit (3PC) Implementation

## Project Overview
Implementation of Three-Phase Commit protocol with partition simulation and non-blocking demonstration.

## Technology Stack

### Overview Table

| Technology | Role in This Project |
|------------|---------------------|
| Python 3 | Core programming language for all services |
| Flask | HTTP server for coordinator, participants, and dashboard |
| Docker | Packages each service into an isolated container |
| Docker Compose | Starts and wires all containers together |
| SQLite | Persists transaction state to disk |
| etcd | Distributed leader election for the coordinator |
| Toxiproxy | Simulates network partitions by blocking traffic |
| structlog | Structured, readable JSON-style logging |
| threading | Background threads for heartbeats and monitors |
| REST APIs | Communication protocol between all services |

## Setup
1. Create virtual environment: `python3 -m venv venv`
2. Activate: `source venv/bin/activate`
3. Install: `python3 -m pip install -r requirements.txt`


## How to Run the Project

### Prerequisites

- Docker Desktop installed and running
- Python 3.8+
- `make` utility (comes with Linux/macOS; Windows users can use Git Bash)

### Step 1: Clone and Navigate

```bash
cd 3PC-Project
```

### Step 2: Build Docker Images

```bash
make build
```

This builds the Python application image using the `Dockerfile`. All containers (coordinator, 3 participants, dashboard) use this same image.

### Step 3: Start All Services

```bash
make up
```

This starts 7 containers:
- etcd (leader election)
- toxiproxy (partition simulation)
- coordinator (port 5000)
- participant1 (port 5001)
- participant2 (port 5002)
- participant3 (port 5003)
- dashboard (port 8000)

It waits 25 seconds for all services to become healthy.

### Step 4: Verify Everything is Running

```bash
make health
```

Expected output shows all containers healthy, leader status `is_leader=true`, and dashboard responding.

### Step 5: Set Up Toxiproxy Proxies

```bash
python3 scripts/inject_partition.py setup
```

This creates the three proxies in Toxiproxy. Must be done once after `make up`.

### Step 6: Run a Transaction

```bash
make test-transaction
```

### Step 7: Open Dashboard

Open browser: `http://localhost:8000`

### Step 8: Run Integration Tests

```bash
pytest tests/test_partition_recovery.py -v
```

### Step 9: Inject a Partition

```bash
# Block participant 1
python3 scripts/inject_partition.py 1 on

# Try a transaction (should abort)
make test-transaction

# Restore participant 1
python3 scripts/inject_partition.py 1 off

# Transaction should succeed again
make test-transaction
```

### Step 10: Stop Everything

```bash
make down
```

### Full Cleanup (removes images and databases)

```bash
make clean
```

## Architecture Diagrams

### State Machines
- [Coordinator State]
<img width="2650" height="3230" alt="coordinator-state-machine" src="https://github.com/user-attachments/assets/4e41701d-9ce4-415b-82e2-ee785e0e841f" />
- [Participant State]
<img width="3345" height="3435" alt="participant-state-machine" src="https://github.com/user-attachments/assets/c7fa2118-d6be-4ac6-883d-6d91ebf78206" />

### Protocol Flows
- [Successful Commit Flow]
- <img width="4685" height="7040" alt="successful-commit-flow" src="https://github.com/user-attachments/assets/77d26fe4-d965-423c-a626-dce79b6623f0" />

- [Abort Flow]
<img width="4330" height="5940" alt="abort-flow" src="https://github.com/user-attachments/assets/1ff0de00-ead5-43d5-bd43-23f397ddcb69" />

- [Non-Blocking Recovery]
- <img width="4542" height="8191" alt="non-blocking-recovery" src="https://github.com/user-attachments/assets/7759717c-afbf-44ad-9cb1-af7749070629" />

- **Key demonstration**

- [Message Format]
- <img width="2142" height="3400" alt="message-format" src="https://github.com/user-attachments/assets/50ac8b22-947d-49d3-9fe4-317ed5d1f48a" />


## Key Concept: Non-Blocking Property

The critical advantage of 3PC over 2PC is demonstrated in the non-blocking recovery diagram:
- When coordinator fails after PRE_COMMIT phase
- Participants can query each other's states
- If all participants are in PRE_COMMIT, they can safely COMMIT
- In 2PC, participants would be BLOCKED waiting for coordinator recovery

This is achieved because PRE_COMMIT creates a "safe to commit" intermediate state.
