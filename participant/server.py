"""
Participant Flask server for 3PC protocol.

This is the participant's "mouth and ears" - handles HTTP communication.
"""

from flask import Flask, request, jsonify
import structlog
from participant.state import ParticipantStateManager, ParticipantState
from coordinator.messages import Message, MessageType

logger = structlog.get_logger()

# Create Flask app
app = Flask(__name__)

# Participant configuration
# Will be set when server starts
participant_id = None
participant_state_manager = None


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
    
    This is how coordinator talks to participant.
    For now, just acknowledge receipt.
    Later we'll add voting logic here.
    """
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
    
    # For now, just acknowledge
    # Later we'll add logic to handle different message types
    return jsonify({
        "status": "success",
        "participant_id": participant_id,
        "message": f"Message {message.message_type.value} received",
        "transaction_id": message.transaction_id
    }), 200


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