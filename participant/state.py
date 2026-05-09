"""
Participant state management for 3PC protocol.

Tracks participant's current state and manages transitions.
"""

from enum import Enum
import structlog

logger = structlog.get_logger()


class ParticipantState(Enum):
    """All possible participant states."""
    INIT = "INIT"
    READY = "READY"
    PRE_COMMIT = "PRE_COMMIT"
    COMMIT = "COMMIT"
    ABORT = "ABORT"


class ParticipantStateManager:
    """
    Manages participant state transitions.
    
    Think of this as the participant state machine from your diagram.
    """
    
    def __init__(self, participant_id: str, transaction_id: str):
        self.participant_id = participant_id
        self.transaction_id = transaction_id
        self.current_state = ParticipantState.INIT
        self.state_history = [ParticipantState.INIT]
        logger.info(
            "participant_initialized",
            participant_id=participant_id,
            transaction_id=transaction_id,
            state=self.current_state.value
        )
    
    def transition_to(self, new_state: ParticipantState, reason: str = "") -> bool:
        """
        Safely transition to a new state.
        
        Returns True if transition is valid, False otherwise.
        """
        if not self._is_valid_transition(new_state):
            logger.error(
                "invalid_state_transition",
                participant_id=self.participant_id,
                transaction_id=self.transaction_id,
                from_state=self.current_state.value,
                to_state=new_state.value,
                reason=reason
            )
            return False
        
        old_state = self.current_state
        self.current_state = new_state
        self.state_history.append(new_state)
        
        logger.info(
            "state_transition",
            participant_id=self.participant_id,
            transaction_id=self.transaction_id,
            from_state=old_state.value,
            to_state=new_state.value,
            reason=reason
        )
        return True
    
    def _is_valid_transition(self, new_state: ParticipantState) -> bool:
        """
        Check if transition is allowed based on 3PC protocol.
        
        Valid transitions from your diagram:
        INIT -> READY (voted YES)
        INIT -> ABORT (voted NO)
        READY -> PRE_COMMIT (received PRE_COMMIT from coordinator)
        READY -> ABORT (timeout)
        PRE_COMMIT -> COMMIT (received DO_COMMIT OR non-blocking recovery)
        PRE_COMMIT -> ABORT (received ABORT)
        """
        current = self.current_state
        
        valid_transitions = {
            ParticipantState.INIT: [ParticipantState.READY, ParticipantState.ABORT],
            ParticipantState.READY: [ParticipantState.PRE_COMMIT, ParticipantState.ABORT],
            ParticipantState.PRE_COMMIT: [ParticipantState.COMMIT, ParticipantState.ABORT],
            ParticipantState.COMMIT: [],  # Final state
            ParticipantState.ABORT: []    # Final state
        }
        
        return new_state in valid_transitions.get(current, [])
    
    def is_final_state(self) -> bool:
        """Check if we're in a terminal state."""
        return self.current_state in [ParticipantState.COMMIT, ParticipantState.ABORT]
    
    def can_commit_without_coordinator(self) -> bool:
        """
        Check if participant is in PRE_COMMIT state.
        
        This is the KEY to non-blocking recovery!
        If in PRE_COMMIT, participant can safely commit if other participants are also in PRE_COMMIT.
        """
        return self.current_state == ParticipantState.PRE_COMMIT
    
    def get_state(self) -> ParticipantState:
        """Get current state."""
        return self.current_state
    
    def __repr__(self):
        return f"ParticipantState({self.participant_id}, {self.current_state.value}, txn={self.transaction_id[:8]}...)"