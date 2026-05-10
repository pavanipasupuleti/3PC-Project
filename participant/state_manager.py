"""
Thread-safe, multi-transaction participant state manager.

Replaces the old single-global `participant_state_manager` with a
proper registry keyed by transaction ID.  State is persisted to
SQLite on every transition so it survives process crashes.
"""

import threading
from typing import Dict, List, Optional
import structlog
from storage.participant_database import participant_db

logger = structlog.get_logger()

# 3PC participant state machine
_VALID_TRANSITIONS: Dict[str, List[str]] = {
    "INIT":       ["READY", "ABORT"],
    "READY":      ["PRE_COMMIT", "ABORT"],
    "PRE_COMMIT": ["COMMIT", "ABORT"],
    "COMMIT":     [],
    "ABORT":      [],
}

_FINAL_STATES = frozenset({"COMMIT", "ABORT"})


class GlobalStateManager:
    """
    Manages participant state for multiple concurrent transactions.

    All public methods acquire an internal lock before reading or
    writing the in-memory state dict, and persist every state change
    to SQLite so the state survives restarts.

    On construction the manager reloads every non-final transaction
    from the database, which is the mechanism that makes automatic
    recovery possible after a crash.
    """

    def __init__(self, participant_id: str) -> None:
        self.participant_id = participant_id
        self._states: Dict[str, str] = {}   # txn_id -> state string
        self._lock = threading.Lock()
        self._restore_from_db()

    # ------------------------------------------------------------------
    # Startup restore
    # ------------------------------------------------------------------

    def _restore_from_db(self) -> None:
        """Reload all non-final transactions from SQLite."""
        pending = participant_db.get_pending()
        for txn_id in pending:
            state = participant_db.load_state(txn_id)
            if state:
                self._states[txn_id] = state
                logger.info(
                    "state_restored_from_db",
                    participant_id=self.participant_id,
                    txn_id=txn_id[:8],
                    state=state,
                )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def initialize(self, txn_id: str) -> bool:
        """
        Register a new transaction in INIT state.

        Returns False if txn_id is already tracked (idempotent guard).
        """
        with self._lock:
            if txn_id in self._states:
                return False
            self._states[txn_id] = "INIT"

        participant_db.save_state(txn_id, self.participant_id, "INIT")
        logger.info(
            "transaction_initialized",
            participant_id=self.participant_id,
            txn_id=txn_id[:8],
        )
        return True

    def get_state(self, txn_id: str) -> Optional[str]:
        """Return the current state string, or None if unknown."""
        with self._lock:
            return self._states.get(txn_id)

    def transition(self, txn_id: str, new_state: str, reason: str = "") -> bool:
        """
        Attempt a state transition.

        Validates the transition against the 3PC state machine.
        Persists the new state to SQLite on success.

        Returns True if the transition was applied, False otherwise.
        """
        with self._lock:
            current = self._states.get(txn_id)
            if current is None:
                logger.error(
                    "transition_unknown_txn",
                    txn_id=txn_id[:8],
                    attempted=new_state,
                )
                return False

            allowed = _VALID_TRANSITIONS.get(current, [])
            if new_state not in allowed:
                logger.error(
                    "invalid_state_transition",
                    participant_id=self.participant_id,
                    txn_id=txn_id[:8],
                    from_state=current,
                    to_state=new_state,
                    reason=reason,
                )
                return False

            self._states[txn_id] = new_state

        participant_db.save_state(txn_id, self.participant_id, new_state)
        logger.info(
            "state_transition",
            participant_id=self.participant_id,
            txn_id=txn_id[:8],
            from_state=current,
            to_state=new_state,
            reason=reason,
        )
        return True

    def can_commit_without_coordinator(self, txn_id: str) -> bool:
        """True only when in PRE_COMMIT — the safe autonomous-commit window."""
        return self.get_state(txn_id) == "PRE_COMMIT"

    def is_final(self, txn_id: str) -> bool:
        return self.get_state(txn_id) in _FINAL_STATES

    def get_pending_txn_ids(self) -> List[str]:
        """Return IDs of all non-final tracked transactions."""
        with self._lock:
            return [
                tid for tid, st in self._states.items()
                if st not in _FINAL_STATES
            ]

    def all_states(self) -> Dict[str, str]:
        """Return a snapshot of all tracked states."""
        with self._lock:
            return dict(self._states)
