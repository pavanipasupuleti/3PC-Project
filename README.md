# Three-Phase Commit (3PC) Implementation

## Project Overview
Implementation of Three-Phase Commit protocol with partition simulation and non-blocking demonstration.

## Tech Stack (From Professor's Requirements)
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
- [x] Phase 1: Environment setup ✅
- [x] Phase 2: Python environment ✅
- [ ] Phase 3: Design (state machines, messages)
- [ ] Phase 4: Core 3PC protocol
- [ ] Phase 5: WAL + Recovery
- [ ] Phase 6: Partition simulation
- [ ] Phase 7: Non-blocking demonstration

git status

git commit -m "Phase 1 & 2 complete: Environment and Python setup"
git log --oneline
