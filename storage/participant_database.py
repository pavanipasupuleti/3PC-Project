"""
SQLite persistence for participant transaction states.

Allows participants to survive crash-and-restart and resume
autonomous recovery from exactly the state they were in.
"""

import os
import sqlite3
import threading
from typing import List, Optional
import structlog

logger = structlog.get_logger()

_DB_PATH = os.environ.get("PARTICIPANT_DB_PATH", "data/3pc_participant.db")


class ParticipantStore:
    """Thread-safe SQLite storage for participant transaction states."""

    def __init__(self, db_path: str = _DB_PATH) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()
        logger.info("participant_db_initialized", db_path=db_path)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS participant_transactions (
                    txn_id         TEXT PRIMARY KEY,
                    participant_id TEXT NOT NULL,
                    state          TEXT NOT NULL,
                    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save_state(self, txn_id: str, participant_id: str, state: str) -> None:
        """Persist or update the state for a transaction."""
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO participant_transactions
                        (txn_id, participant_id, state, created_at, updated_at)
                    VALUES (?, ?, ?, datetime('now'), datetime('now'))
                    ON CONFLICT(txn_id) DO UPDATE SET
                        state      = excluded.state,
                        updated_at = datetime('now')
                    """,
                    (txn_id, participant_id, state),
                )
                conn.commit()
        logger.info("participant_state_persisted",
                    txn_id=txn_id[:8], state=state)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load_state(self, txn_id: str) -> Optional[str]:
        """Return the persisted state for txn_id, or None."""
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT state FROM participant_transactions WHERE txn_id = ?",
                    (txn_id,),
                ).fetchone()
                return row[0] if row else None

    def get_pending(self) -> List[str]:
        """Return txn_ids that are not yet in a final state."""
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    "SELECT txn_id FROM participant_transactions"
                    " WHERE state NOT IN ('COMMIT', 'ABORT')"
                ).fetchall()
                return [r[0] for r in rows]

    def get_all(self) -> List[dict]:
        """Return all transaction records, newest first."""
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                rows = conn.execute(
                    "SELECT txn_id, participant_id, state, created_at, updated_at"
                    " FROM participant_transactions ORDER BY created_at DESC"
                ).fetchall()
                return [
                    {
                        "txn_id":         r[0],
                        "participant_id": r[1],
                        "state":          r[2],
                        "created_at":     r[3],
                        "updated_at":     r[4],
                    }
                    for r in rows
                ]


# Module-level singleton used by state_manager and auto_recovery
participant_db = ParticipantStore()
