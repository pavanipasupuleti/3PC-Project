"""
Automatic non-blocking recovery for 3PC participants.

When the coordinator fails, a participant in PRE_COMMIT can resolve
the transaction autonomously by querying its peers.  This is the
mechanism that gives 3PC its non-blocking property over 2PC.

Decision rules (per Skeen 1981):
  - Empty peer list or all peers unreachable → UNKNOWN (wait, try later)
  - Any reachable peer already COMMITTED     → COMMIT  (must match)
  - Any reachable peer in ABORT              → ABORT
  - Any reachable peer in INIT or READY      → ABORT   (pre-commit not reached)
  - All reachable peers in PRE_COMMIT        → COMMIT  (safe to commit)
"""

import threading
from typing import Dict, List, Optional
import requests
import structlog
from storage.participant_database import participant_db

logger = structlog.get_logger()

# Peer query uses the admin-side 5-second budget (not infinite protocol timeout)
_PEER_QUERY_TIMEOUT: int = 5


class AutoRecovery:
    """
    Autonomous recovery from coordinator failure.

    Instantiate once per participant process.  Call `attempt_recovery()`
    whenever coordinator silence is detected (or manually via /recover).

    Args:
        participant_id: this participant's identifier (for logging)
        peer_urls:      URLs of every OTHER participant
        state_manager:  the GlobalStateManager instance for this process
    """

    def __init__(
        self,
        participant_id: str,
        peer_urls: List[str],
        state_manager,                  # GlobalStateManager (avoid circular import)
    ) -> None:
        self.participant_id = participant_id
        self.peer_urls = peer_urls
        self.state_manager = state_manager
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def attempt_recovery(self) -> Dict[str, str]:
        """
        Try to resolve every pending transaction.

        Returns a dict mapping txn_id → decision
        ("COMMIT" | "ABORT" | "UNKNOWN").

        Thread-safe: only one recovery run executes at a time.
        """
        results: Dict[str, str] = {}

        if not self._lock.acquire(blocking=False):
            logger.info("recovery_already_in_progress",
                        participant_id=self.participant_id)
            return results

        try:
            pending = participant_db.get_pending()

            if not pending:
                logger.info("recovery_no_pending_transactions",
                            participant_id=self.participant_id)
                return results

            logger.info(
                "recovery_started",
                participant_id=self.participant_id,
                pending_count=len(pending),
            )

            for txn_id in pending:
                decision = self._recover_transaction(txn_id)
                results[txn_id] = decision
                logger.info(
                    "recovery_decision",
                    participant_id=self.participant_id,
                    txn_id=txn_id[:8],
                    decision=decision,
                )

        finally:
            self._lock.release()

        return results

    # ------------------------------------------------------------------
    # Per-transaction logic
    # ------------------------------------------------------------------

    def _recover_transaction(self, txn_id: str) -> str:
        """
        Decide what to do with a single pending transaction.

        Returns "COMMIT", "ABORT", or "UNKNOWN".
        """
        current = self.state_manager.get_state(txn_id)

        # Only PRE_COMMIT can commit autonomously
        if current == "PRE_COMMIT":
            peer_states = self._query_peers(txn_id)
            return self._make_decision(txn_id, peer_states)

        # Stuck in INIT or READY: coordinator failed before PRE_COMMIT,
        # so we can never commit — abort immediately.
        if current in ("INIT", "READY"):
            self.state_manager.transition(
                txn_id, "ABORT",
                reason="coordinator failed before PRE_COMMIT phase")
            return "ABORT"

        # Already in a final state — nothing to do
        return "UNKNOWN"

    def _query_peers(self, txn_id: str) -> Dict[str, Optional[str]]:
        """GET /query-state/{txn_id} from every configured peer."""
        peer_states: Dict[str, Optional[str]] = {}

        for peer_url in self.peer_urls:
            try:
                resp = requests.get(
                    f"{peer_url}/query-state/{txn_id}",
                    timeout=_PEER_QUERY_TIMEOUT,
                )
                if resp.status_code == 200:
                    peer_states[peer_url] = resp.json().get("state")
                else:
                    peer_states[peer_url] = "UNREACHABLE"
                    logger.warning("peer_query_bad_status",
                                   peer=peer_url, status=resp.status_code,
                                   txn_id=txn_id[:8])
            except Exception as exc:
                peer_states[peer_url] = "UNREACHABLE"
                logger.warning("peer_query_failed",
                               peer=peer_url, txn_id=txn_id[:8],
                               error=str(exc))

        return peer_states

    def _make_decision(
        self,
        txn_id: str,
        peer_states: Dict[str, Optional[str]],
    ) -> str:
        """Apply 3PC recovery decision rules and mutate state accordingly."""
        all_values = list(peer_states.values())

        # Guard: empty peer list — cannot decide, don't auto-commit
        if not all_values:
            logger.warning("recovery_no_peers_configured", txn_id=txn_id[:8])
            return "UNKNOWN"

        reachable = [s for s in all_values if s != "UNREACHABLE"]

        # All peers unreachable — cannot decide
        if not reachable:
            logger.warning("recovery_all_peers_unreachable", txn_id=txn_id[:8])
            return "UNKNOWN"

        # A peer already committed → we must also commit
        if any(s == "COMMIT" for s in reachable):
            self.state_manager.transition(
                txn_id, "COMMIT",
                reason="peer already committed; matching decision")
            return "COMMIT"

        # A peer aborted → must abort
        if any(s == "ABORT" for s in reachable):
            self.state_manager.transition(
                txn_id, "ABORT",
                reason="peer aborted")
            return "ABORT"

        # A peer never reached PRE_COMMIT → unsafe to commit
        if any(s in ("INIT", "READY") for s in reachable):
            self.state_manager.transition(
                txn_id, "ABORT",
                reason="peer in INIT/READY — coordinator failed before PRE_COMMIT")
            return "ABORT"

        # All reachable peers in PRE_COMMIT → safe to commit autonomously
        if all(s == "PRE_COMMIT" for s in reachable):
            self.state_manager.transition(
                txn_id, "COMMIT",
                reason="all reachable peers in PRE_COMMIT — non-blocking recovery")
            return "COMMIT"

        return "UNKNOWN"
