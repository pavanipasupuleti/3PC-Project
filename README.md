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
