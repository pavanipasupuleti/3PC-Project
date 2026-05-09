"""
Coordinator state management for 3PC protocol.

Tracks coordinator's current state and manages transitions.
"""

from enum import Enum
import structlog

logger = structlog.get_logger()


class CoordinatorState(Enum):
    """All possible coordinator states."""
    INIT = "INIT"
    WAIT = "WAIT"
    PRE_COMMIT = "PRE_COMMIT"
    COMMIT = "COMMIT"
    ABORT = "ABORT"


class CoordinatorStateManager:
    """
    Manages coordinator state transitions.
    
    Think of this as the state machine from your diagram.
    """
    
    def __init__(self, transaction_id: str):
        self.transaction_id = transaction_id
        self.current_state = CoordinatorState.INIT
        self.state_history = [CoordinatorState.INIT]
        logger.info(
            "coordinator_initialized",
            transaction_id=transaction_id,
            state=self.current_state.value
        )
    
    def transition_to(self, new_state: CoordinatorState) -> bool:
        """
        Safely transition to a new state.
        
        Returns True if transition is valid, False otherwise.
        """
        if not self._is_valid_transition(new_state):
            logger.error(
                "invalid_state_transition",
                transaction_id=self.transaction_id,
                from_state=self.current_state.value,
                to_state=new_state.value
            )
            return False
        
        old_state = self.current_state
        self.current_state = new_state
        self.state_history.append(new_state)
        
        logger.info(
            "state_transition",
            transaction_id=self.transaction_id,
            from_state=old_state.value,
            to_state=new_state.value
        )
        return True
    
    def _is_valid_transition(self, new_state: CoordinatorState) -> bool:
        """
        Check if transition is allowed based on 3PC protocol.
        
        Valid transitions from your diagram:
        INIT -> WAIT
        WAIT -> PRE_COMMIT or ABORT
        PRE_COMMIT -> COMMIT or ABORT
        """
        current = self.current_state
        
        valid_transitions = {
            CoordinatorState.INIT: [CoordinatorState.WAIT],
            CoordinatorState.WAIT: [CoordinatorState.PRE_COMMIT, CoordinatorState.ABORT],
            CoordinatorState.PRE_COMMIT: [CoordinatorState.COMMIT, CoordinatorState.ABORT],
            CoordinatorState.COMMIT: [],
            CoordinatorState.ABORT: []
        }
        
        return new_state in valid_transitions.get(current, [])
    
    def is_final_state(self) -> bool:
        """Check if we're in a terminal state."""
        return self.current_state in [CoordinatorState.COMMIT, CoordinatorState.ABORT]
    
    def get_state(self) -> CoordinatorState:
        """Get current state."""
        return self.current_state
    
    def __repr__(self):
        return f"CoordinatorState({self.current_state.value}, txn={self.transaction_id[:8]}...)"