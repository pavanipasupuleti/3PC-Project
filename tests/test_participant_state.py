"""Test participant state management."""

from participant.state import ParticipantStateManager, ParticipantState
from coordinator.messages import create_transaction_id

# Create participant state manager
txn_id = create_transaction_id()
state_mgr = ParticipantStateManager("participant_1", txn_id)

print(f"✓ Initial state: {state_mgr.get_state().value}")

# Valid transition: INIT -> READY (voted YES)
success = state_mgr.transition_to(ParticipantState.READY, reason="voted YES")
print(f"✓ INIT -> READY: {'Success' if success else 'Failed'}")
print(f"  Current state: {state_mgr.get_state().value}")

# Valid transition: READY -> PRE_COMMIT
success = state_mgr.transition_to(ParticipantState.PRE_COMMIT, reason="received PRE_COMMIT from coordinator")
print(f"✓ READY -> PRE_COMMIT: {'Success' if success else 'Failed'}")
print(f"  Current state: {state_mgr.get_state().value}")

# Check non-blocking capability (IMPORTANT!)
can_commit = state_mgr.can_commit_without_coordinator()
print(f"✓ Can commit without coordinator? {can_commit} (should be True in PRE_COMMIT!)")

# INVALID transition: PRE_COMMIT -> READY (can't go backwards!)
success = state_mgr.transition_to(ParticipantState.READY, reason="invalid")
print(f"✓ PRE_COMMIT -> READY: {'Success' if success else 'Failed'} (should fail!)")
print(f"  Current state: {state_mgr.get_state().value}")

# Valid transition: PRE_COMMIT -> COMMIT (non-blocking recovery!)
success = state_mgr.transition_to(ParticipantState.COMMIT, reason="non-blocking recovery - all participants in PRE_COMMIT")
print(f"✓ PRE_COMMIT -> COMMIT: {'Success' if success else 'Failed'}")
print(f"  Current state: {state_mgr.get_state().value}")

# Check if final
print(f"✓ Is final state? {state_mgr.is_final_state()}")
print(f"✓ State history: {[s.value for s in state_mgr.state_history]}")