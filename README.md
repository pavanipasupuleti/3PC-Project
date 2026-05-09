# Three-Phase Commit (3PC) Implementation

## Project Overview
Implementation of Three-Phase Commit protocol with partition simulation and non-blocking demonstration.

## Tech Stack 
- **Language:** Python 3.12
- **Communication:** HTTP/REST + JSON (Flask)
- **Storage:** SQLite (WAL)
- **Logging:** structlog (Python)
- **Serialization:** JSON
- **Concurrency:** Python threading + mutex
- **Time:** Local monotonic clock
- **Testing:** Toxiproxy (network fault simulation)
- **Container:** Docker + Docker Compose

## Setup
1. Create virtual environment: `python3 -m venv venv`
2. Activate: `source venv/bin/activate`
3. Install: `python3 -m pip install -r requirements.txt`

## Progress
-  Phase 1: Environment setup 
-  Phase 2: Python environment 
-  Phase 3: Design (state machines, messages)
-  Phase 4: Core 3PC protocol
-  Phase 5: WAL + Recovery
-  Phase 6: Partition simulation
-  Phase 7: Non-blocking demonstration



## Architecture Diagrams

### State Machines
- [Coordinator State Machine](docs/diagrams/coordinator-state-machine.png)
- [Participant State Machine](docs/diagrams/participant-state-machine.png)

### Protocol Flows
- [Successful Commit Flow](docs/diagrams/successful-commit-flow.png)
- [Abort Flow](docs/diagrams/abort-flow.png)
- [Non-Blocking Recovery](docs/diagrams/non-blocking-recovery.png) - **Key demonstration**

### Data Structures
- [Message Format](docs/diagrams/message-format.png)

## Key Concept: Non-Blocking Property

The critical advantage of 3PC over 2PC is demonstrated in the non-blocking recovery diagram:
- When coordinator fails after PRE_COMMIT phase
- Participants can query each other's states
- If all participants are in PRE_COMMIT, they can safely COMMIT
- In 2PC, participants would be BLOCKED waiting for coordinator recovery

This is achieved because PRE_COMMIT creates a "safe to commit" intermediate state.
