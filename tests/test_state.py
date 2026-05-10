"""Test coordinator state management."""

from coordinator.state import CoordinatorStateManager, CoordinatorState
from coordinator.messages import create_transaction_id

# Create state manager
txn_id = create_transaction_id()
state_mgr = CoordinatorStateManager(txn_id)

print(f"✓ Initial state: {state_mgr.get_state().value}")

# Valid transition: INIT -> WAIT
success = state_mgr.transition_to(CoordinatorState.WAIT)
print(f"✓ INIT -> WAIT: {'Success' if success else 'Failed'}")
print(f"  Current state: {state_mgr.get_state().value}")

# Valid transition: WAIT -> PRE_COMMIT
success = state_mgr.transition_to(CoordinatorState.PRE_COMMIT)
print(f"✓ WAIT -> PRE_COMMIT: {'Success' if success else 'Failed'}")
print(f"  Current state: {state_mgr.get_state().value}")

# INVALID transition: PRE_COMMIT -> WAIT (can't go backwards!)
success = state_mgr.transition_to(CoordinatorState.WAIT)
print(f"✓ PRE_COMMIT -> WAIT: {'Success' if success else 'Failed'} (should fail!)")
print(f"  Current state: {state_mgr.get_state().value}")

# Valid transition: PRE_COMMIT -> COMMIT
success = state_mgr.transition_to(CoordinatorState.COMMIT)
print(f"✓ PRE_COMMIT -> COMMIT: {'Success' if success else 'Failed'}")
print(f"  Current state: {state_mgr.get_state().value}")

# Check if final
print(f"✓ Is final state? {state_mgr.is_final_state()}")
print(f"✓ State history: {[s.value for s in state_mgr.state_history]}")