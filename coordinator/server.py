"""
Coordinator Flask server for 3PC protocol.

This is the coordinator's "mouth and ears" - handles HTTP communication.
"""

from flask import Flask, request, jsonify
import structlog
from coordinator.state import CoordinatorStateManager, CoordinatorState
from coordinator.messages import Message, MessageType, create_transaction_id

logger = structlog.get_logger()

# Create Flask app
app = Flask(__name__)

# Store active transactions (in memory for now)
# In real system, this would be in database
active_transactions = {}


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.
    
    Returns OK if coordinator is running.
    Purpose: Test if server is alive.
    """
    return jsonify({
        "status": "healthy",
        "service": "coordinator",
        "message": "Coordinator is running"
    }), 200


@app.route('/start-transaction', methods=['POST'])
def start_transaction():
    """
    Start a new 3PC transaction.
    
    This is how you initiate a transaction.
    Later we'll add participant coordination here.
    """
    # Create new transaction
    txn_id = create_transaction_id()
    
    # Create state manager for this transaction
    state_mgr = CoordinatorStateManager(txn_id)
    
    # Store it
    active_transactions[txn_id] = {
        "state_manager": state_mgr,
        "participants": []  # Will add participant URLs here
    }
    
    logger.info(
        "transaction_started",
        transaction_id=txn_id,
        state=state_mgr.get_state().value
    )
    
    return jsonify({
        "status": "success",
        "transaction_id": txn_id,
        "state": state_mgr.get_state().value,
        "message": "Transaction started"
    }), 201


@app.route('/transaction/<txn_id>/status', methods=['GET'])
def get_transaction_status(txn_id):
    """
    Get status of a transaction.
    
    Purpose: Check what state a transaction is in.
    """
    if txn_id not in active_transactions:
        return jsonify({
            "status": "error",
            "message": "Transaction not found"
        }), 404
    
    txn = active_transactions[txn_id]
    state_mgr = txn["state_manager"]
    
    return jsonify({
        "status": "success",
        "transaction_id": txn_id,
        "state": state_mgr.get_state().value,
        "is_final": state_mgr.is_final_state(),
        "state_history": [s.value for s in state_mgr.state_history]
    }), 200


def run_coordinator(host='127.0.0.1', port=5000):
    """
    Run the coordinator server.
    
    Args:
        host: IP address to bind to
        port: Port number to listen on
    """
    logger.info(
        "coordinator_starting",
        host=host,
        port=port
    )
    
    app.run(host=host, port=port, debug=True)


if __name__ == '__main__':
    # Run coordinator on default port 5000
    run_coordinator()