"""
Participant Flask server for 3PC protocol.

This is the participant's "mouth and ears" - handles HTTP communication.
"""

from flask import Flask, request, jsonify
import structlog
import requests
from participant.state import ParticipantStateManager, ParticipantState
from coordinator.messages import Message, MessageType

logger = structlog.get_logger()

# Create Flask app
app = Flask(__name__)

# Participant configuration
# Will be set when server starts
participant_id = None
participant_state_manager = None


def attempt_recovery(peer_urls: list) -> dict:
    """
    Attempt to recover from coordinator failure using peer consensus.
    
    This is the core of 3PC's non-blocking property!
    
    Recovery rules:
    1. Can only recover if in PRE_COMMIT state
    2. Query all peer participants for their states
    3. If ALL peers in PRE_COMMIT → safe to COMMIT
    4. Otherwise → ABORT
    
    Args:
        peer_urls: List of peer participant URLs to query
        
    Returns:
        dict with recovery decision and details
    """
    global participant_state_manager
    
    # Can only attempt recovery from PRE_COMMIT state
    if not participant_state_manager.can_commit_without_coordinator():
        current_state = participant_state_manager.get_state().value
        logger.warning("recovery_not_possible",
                      participant_id=participant_id,
                      current_state=current_state,
                      reason="not in PRE_COMMIT state")
        return {
            "can_recover": False,
            "reason": f"Cannot recover from state {current_state}",
            "decision": None
        }
    
    logger.info("recovery_attempt_started",
               participant_id=participant_id,
               peer_count=len(peer_urls))
    
    # Query all peers for their states
    peer_states = {}
    for peer_url in peer_urls:
        try:
            response = requests.get(f"{peer_url}/peer-state", timeout=2)
            if response.status_code == 200:
                data = response.json()
                peer_states[peer_url] = {
                    "state": data['state'],
                    "participant_id": data['participant_id']
                }
                logger.info("peer_state_received",
                          peer=data['participant_id'],
                          state=data['state'])
            else:
                peer_states[peer_url] = {"state": "UNKNOWN", "error": "HTTP error"}
                logger.warning("peer_query_failed",
                             peer=peer_url,
                             status=response.status_code)
        except Exception as e:
            peer_states[peer_url] = {"state": "UNKNOWN", "error": str(e)}
            logger.error("peer_query_exception",
                        peer=peer_url,
                        error=str(e))
    
    # Decision logic: ALL peers must be in PRE_COMMIT
    peer_state_values = [p["state"] for p in peer_states.values()]
    all_in_precommit = all(state == "PRE_COMMIT" for state in peer_state_values)
    
    if all_in_precommit:
        # Safe to commit!
        logger.info("recovery_decision_commit",
                   participant_id=participant_id,
                   reason="all peers in PRE_COMMIT state",
                   peer_states=peer_states)
        
        participant_state_manager.transition_to(
            ParticipantState.COMMIT,
            reason="Non-blocking recovery: coordinator failed, all participants in PRE_COMMIT"
        )
        
        return {
            "can_recover": True,
            "decision": "COMMIT",
            "reason": "All peers in PRE_COMMIT, safe to commit",
            "peer_states": peer_states,
            "final_state": "COMMIT"
        }
    else:
        # Not safe - abort
        logger.info("recovery_decision_abort",
                   participant_id=participant_id,
                   reason="not all peers in PRE_COMMIT",
                   peer_states=peer_states)
        
        participant_state_manager.transition_to(
            ParticipantState.ABORT,
            reason="Recovery failed: inconsistent peer states"
        )
        
        return {
            "can_recover": True,
            "decision": "ABORT",
            "reason": "Not all peers in PRE_COMMIT",
            "peer_states": peer_states,
            "final_state": "ABORT"
        }


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.
    
    Returns OK if participant is running.
    """
    return jsonify({
        "status": "healthy",
        "service": "participant",
        "participant_id": participant_id,
        "message": "Participant is running"
    }), 200


@app.route('/message', methods=['POST'])
def receive_message():
    """
    Receive a message from coordinator.
    
    Now handles different message types and responds appropriately.
    """
    global participant_state_manager
    
    data = request.get_json()
    
    # Convert JSON to Message object
    try:
        message = Message.from_dict(data)
    except Exception as e:
        logger.error("invalid_message", error=str(e))
        return jsonify({
            "status": "error",
            "message": "Invalid message format"
        }), 400
    
    logger.info(
        "message_received",
        participant_id=participant_id,
        from_sender=message.sender,
        message_type=message.message_type.value,
        transaction_id=message.transaction_id
    )
    
    # Check if we have state manager for this transaction
    if participant_state_manager is None:
        logger.error(
            "no_state_manager",
            participant_id=participant_id,
            transaction_id=message.transaction_id
        )
        return jsonify({
            "status": "error",
            "message": "Participant not initialized with transaction"
        }), 400
    
    # Handle different message types
    response_message = None
    
    if message.message_type == MessageType.CAN_COMMIT:
        # Phase 1: Coordinator asking if we can commit
        response_message = handle_can_commit(message)
    
    elif message.message_type == MessageType.PRE_COMMIT:
        # Phase 2: Coordinator telling us to prepare
        response_message = handle_pre_commit(message)
    
    elif message.message_type == MessageType.DO_COMMIT:
        # Phase 3: Coordinator telling us to commit
        response_message = handle_do_commit(message)
    
    elif message.message_type == MessageType.ABORT:
        # Coordinator telling us to abort
        response_message = handle_abort(message)
    
    else:
        logger.warning(
            "unknown_message_type",
            message_type=message.message_type.value
        )
        return jsonify({
            "status": "error",
            "message": f"Unknown message type: {message.message_type.value}"
        }), 400
    
    # Return response
    return jsonify({
        "status": "success",
        "participant_id": participant_id,
        "response": response_message.to_dict() if response_message else None,
        "current_state": participant_state_manager.get_state().value
    }), 200


def handle_can_commit(message: Message) -> Message:
    """
    Handle CAN_COMMIT message from coordinator.
    
    Decision logic: For now, always vote YES.
    Later we can add conditions (e.g., check resources, random failures).
    """
    # Simple decision: always YES for now
    vote = MessageType.YES
    
    # Transition to READY state (voted YES)
    success = participant_state_manager.transition_to(
        ParticipantState.READY,
        reason="voted YES to CAN_COMMIT"
    )
    
    if not success:
        # If transition failed, vote NO instead
        vote = MessageType.NO
        participant_state_manager.transition_to(
            ParticipantState.ABORT,
            reason="invalid state transition, voting NO"
        )
    
    # Create response message
    response = Message(
        transaction_id=message.transaction_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=vote,
        state=participant_state_manager.get_state().value
    )
    
    logger.info(
        "voted",
        participant_id=participant_id,
        vote=vote.value,
        new_state=participant_state_manager.get_state().value
    )
    
    return response


def handle_pre_commit(message: Message) -> Message:
    """
    Handle PRE_COMMIT message from coordinator.
    
    This is Phase 2 - prepare to commit.
    """
    # Transition to PRE_COMMIT state
    success = participant_state_manager.transition_to(
        ParticipantState.PRE_COMMIT,
        reason="received PRE_COMMIT from coordinator"
    )
    
    if not success:
        logger.error(
            "pre_commit_failed",
            participant_id=participant_id,
            current_state=participant_state_manager.get_state().value
        )
    
    # Send ACK back to coordinator
    response = Message(
        transaction_id=message.transaction_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=MessageType.ACK,
        state=participant_state_manager.get_state().value
    )
    
    return response


def handle_do_commit(message: Message) -> Message:
    """
    Handle DO_COMMIT message from coordinator.
    
    This is Phase 3 - final commit.
    """
    # Transition to COMMIT state
    success = participant_state_manager.transition_to(
        ParticipantState.COMMIT,
        reason="received DO_COMMIT from coordinator"
    )
    
    if not success:
        logger.error(
            "commit_failed",
            participant_id=participant_id,
            current_state=participant_state_manager.get_state().value
        )
    
    # Send COMMITTED confirmation
    response = Message(
        transaction_id=message.transaction_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=MessageType.ACK,
        state=participant_state_manager.get_state().value,
        data={"committed": True}
    )
    
    logger.info(
        "transaction_committed",
        participant_id=participant_id,
        transaction_id=message.transaction_id
    )
    
    return response


def handle_abort(message: Message) -> Message:
    """
    Handle ABORT message from coordinator.
    
    Abort the transaction.
    """
    # Transition to ABORT state
    participant_state_manager.transition_to(
        ParticipantState.ABORT,
        reason="received ABORT from coordinator"
    )
    
    # Send ACK back
    response = Message(
        transaction_id=message.transaction_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=MessageType.ACK,
        state=participant_state_manager.get_state().value,
        data={"aborted": True}
    )
    
    logger.info(
        "transaction_aborted",
        participant_id=participant_id,
        transaction_id=message.transaction_id
    )
    
    return response


@app.route('/state', methods=['GET'])
def get_state():
    """
    Get participant's current state.
    
    Purpose: Check what state this participant is in.
    """
    if participant_state_manager is None:
        return jsonify({
            "status": "error",
            "message": "Participant not initialized with transaction"
        }), 400
    
    return jsonify({
        "status": "success",
        "participant_id": participant_id,
        "state": participant_state_manager.get_state().value,
        "is_final": participant_state_manager.is_final_state(),
        "can_commit_without_coordinator": participant_state_manager.can_commit_without_coordinator(),
        "state_history": [s.value for s in participant_state_manager.state_history]
    }), 200


@app.route('/peer-state', methods=['GET'])
def get_peer_state():
    """
    Endpoint for other participants to query this participant's state.
    
    Used during recovery protocol when coordinator has failed.
    Other participants can query this endpoint to determine if it's
    safe to commit without the coordinator.
    
    Returns:
        JSON response with current state and transaction info
    """
    if participant_state_manager is None:
        return jsonify({
            "status": "error",
            "message": "No active transaction"
        }), 400
    
    state = participant_state_manager.get_state()
    can_commit = participant_state_manager.can_commit_without_coordinator()
    
    logger.info("peer_state_requested",
               participant_id=participant_id,
               state=state.value,
               can_commit=can_commit)
    
    return jsonify({
        "participant_id": participant_id,
        "state": state.value,
        "transaction_id": participant_state_manager.transaction_id,
        "can_commit_without_coordinator": can_commit,
        "state_history": [s.value for s in participant_state_manager.state_history]
    }), 200


@app.route('/recover', methods=['POST'])
def trigger_recovery():
    """
    Manually trigger non-blocking recovery protocol.
    
    Used to demonstrate 3PC's advantage over 2PC when coordinator fails.
    
    Request JSON:
        {
            "peers": ["http://host:port", ...]
        }
    
    Returns:
        Recovery result with decision and final state
    """
    data = request.get_json()
    peer_urls = data.get('peers', [])
    
    if not peer_urls:
        return jsonify({
            "status": "error",
            "message": "No peer URLs provided"
        }), 400
    
    logger.info("recovery_triggered",
               participant_id=participant_id,
               peer_count=len(peer_urls))
    
    result = attempt_recovery(peer_urls)
    
    return jsonify({
        "participant_id": participant_id,
        "recovery_attempted": True,
        **result
    }), 200


@app.route('/init-transaction', methods=['POST'])
def init_transaction():
    """
    Initialize participant with a transaction.
    
    This creates the state manager for a new transaction.
    """
    global participant_state_manager
    
    data = request.get_json()
    transaction_id = data.get('transaction_id')
    
    if not transaction_id:
        return jsonify({
            "status": "error",
            "message": "transaction_id required"
        }), 400
    
    # Create state manager for this transaction
    participant_state_manager = ParticipantStateManager(participant_id, transaction_id)
    
    logger.info(
        "transaction_initialized",
        participant_id=participant_id,
        transaction_id=transaction_id
    )
    
    return jsonify({
        "status": "success",
        "participant_id": participant_id,
        "transaction_id": transaction_id,
        "state": participant_state_manager.get_state().value
    }), 201


def run_participant(p_id, host='127.0.0.1', port=5001):
    """
    Run the participant server.
    
    Args:
        p_id: Participant identifier (e.g., "participant_1")
        host: IP address to bind to
        port: Port number to listen on
    """
    global participant_id
    participant_id = p_id
    
    logger.info(
        "participant_starting",
        participant_id=participant_id,
        host=host,
        port=port
    )
    
    app.run(host=host, port=port, debug=True)


if __name__ == '__main__':
    # Run participant_1 on port 5001
    run_participant("participant_1", port=5001)