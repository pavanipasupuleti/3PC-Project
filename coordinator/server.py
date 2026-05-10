"""
Coordinator Flask server for 3PC protocol.

This is the coordinator's "mouth and ears" - handles HTTP communication.
"""

from flask import Flask, request, jsonify
import structlog
from coordinator.state import CoordinatorStateManager, CoordinatorState
from coordinator.messages import Message, MessageType, create_transaction_id
import requests
from typing import List, Dict

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


@app.route('/execute-transaction', methods=['POST'])
def execute_transaction():
    """
    Execute a full 3PC transaction with participants.
    
    This orchestrates all 3 phases automatically.
    """
    data = request.get_json()
    participant_urls = data.get('participants', [])
    
    if not participant_urls:
        return jsonify({
            "status": "error",
            "message": "No participants provided"
        }), 400
    
    # Create new transaction
    txn_id = create_transaction_id()
    state_mgr = CoordinatorStateManager(txn_id)
    
    # Store transaction
    active_transactions[txn_id] = {
        "state_manager": state_mgr,
        "participants": participant_urls
    }
    
    logger.info(
        "transaction_started",
        transaction_id=txn_id,
        num_participants=len(participant_urls)
    )
    
    # Execute the protocol
    try:
        result = execute_3pc_protocol(txn_id, state_mgr, participant_urls)
        return jsonify(result), 200
    except Exception as e:
        logger.error(
            "transaction_failed",
            transaction_id=txn_id,
            error=str(e)
        )
        return jsonify({
            "status": "error",
            "transaction_id": txn_id,
            "message": str(e)
        }), 500


def execute_3pc_protocol(txn_id: str, state_mgr: CoordinatorStateManager, participant_urls: List[str]) -> Dict:
    """
    Execute the full 3PC protocol.
    
    Phase 1: CAN_COMMIT
    Phase 2: PRE_COMMIT
    Phase 3: DO_COMMIT
    """
    # PHASE 1: CAN_COMMIT
    logger.info("phase_1_start", transaction_id=txn_id, phase="CAN_COMMIT")
    
    # First, initialize all participants with transaction
    for url in participant_urls:
        try:
            response = requests.post(
                f"{url}/init-transaction",
                json={"transaction_id": txn_id},
                timeout=5
            )
            if response.status_code != 201:
                raise Exception(f"Failed to initialize participant {url}")
        except Exception as e:
            logger.error("participant_init_failed", url=url, error=str(e))
            raise
    
    # Transition to WAIT state
    state_mgr.transition_to(CoordinatorState.WAIT)
    
    # Send CAN_COMMIT to all participants
    votes = send_can_commit(txn_id, participant_urls)
    
    # Check votes
    all_yes = all(vote == "YES" for vote in votes.values())
    
    if not all_yes:
        # ABORT: At least one participant voted NO
        logger.info("aborting_transaction", transaction_id=txn_id, reason="not all voted YES")
        state_mgr.transition_to(CoordinatorState.ABORT)
        send_abort(txn_id, participant_urls)
        
        return {
            "status": "aborted",
            "transaction_id": txn_id,
            "final_state": "ABORT",
            "reason": "One or more participants voted NO",
            "votes": votes
        }
    
    # PHASE 2: PRE_COMMIT
    logger.info("phase_2_start", transaction_id=txn_id, phase="PRE_COMMIT")
    
    state_mgr.transition_to(CoordinatorState.PRE_COMMIT)
    
    # Send PRE_COMMIT to all participants
    acks = send_pre_commit(txn_id, participant_urls)
    
    # Check ACKs
    all_acked = all(ack == "ACK" for ack in acks.values())
    
    if not all_acked:
        logger.error("pre_commit_failed", transaction_id=txn_id)
        state_mgr.transition_to(CoordinatorState.ABORT)
        send_abort(txn_id, participant_urls)
        
        return {
            "status": "aborted",
            "transaction_id": txn_id,
            "final_state": "ABORT",
            "reason": "Not all participants acknowledged PRE_COMMIT",
            "acks": acks
        }
    
    # ⚠️ DEMO PAUSE: Coordinator crashes here in demo
    logger.info("DEMO_PAUSE", 
               message="⚠️  ALL PARTICIPANTS IN PRE_COMMIT - Coordinator can crash now!",
               transaction_id=txn_id)
    print("\n" + "="*70)
    print(" DEMO: All participants are in PRE_COMMIT state")
    print(" Press Ctrl+C NOW to simulate coordinator crash")
    print("Or press Enter to complete normally")
    print("="*70 + "\n")
    input("Your choice: ")
    
    # If we reach here, coordinator survived
    logger.info("DEMO_CONTINUE", message="Coordinator survived! Continuing to Phase 3")
    
    # PHASE 3: DO_COMMIT
    logger.info("phase_3_start", transaction_id=txn_id, phase="DO_COMMIT")
    
    state_mgr.transition_to(CoordinatorState.COMMIT)
    
    # Send DO_COMMIT to all participants
    commits = send_do_commit(txn_id, participant_urls)
    
    logger.info("transaction_committed", transaction_id=txn_id)
    
    return {
        "status": "committed",
        "transaction_id": txn_id,
        "final_state": "COMMIT",
        "state_history": [s.value for s in state_mgr.state_history],
        "participants": len(participant_urls),
        "votes": votes,
        "acks": acks,
        "commits": commits
    }


def send_can_commit(txn_id: str, participant_urls: List[str]) -> Dict[str, str]:
    """Send CAN_COMMIT to all participants and collect votes."""
    votes = {}
    
    for url in participant_urls:
        message = Message(
            transaction_id=txn_id,
            sender="coordinator",
            receiver=url,
            message_type=MessageType.CAN_COMMIT,
            state="WAIT"
        )
        
        try:
            response = requests.post(
                f"{url}/message",
                json=message.to_dict(),
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                vote = data.get("response", {}).get("message_type", "NO")
                votes[url] = vote
                logger.info("vote_received", url=url, vote=vote)
            else:
                votes[url] = "NO"
                logger.error("vote_failed", url=url, status=response.status_code)
        
        except Exception as e:
            votes[url] = "NO"
            logger.error("vote_timeout", url=url, error=str(e))
    
    return votes


def send_pre_commit(txn_id: str, participant_urls: List[str]) -> Dict[str, str]:
    """Send PRE_COMMIT to all participants and collect ACKs."""
    acks = {}
    
    for url in participant_urls:
        message = Message(
            transaction_id=txn_id,
            sender="coordinator",
            receiver=url,
            message_type=MessageType.PRE_COMMIT,
            state="PRE_COMMIT"
        )
        
        try:
            response = requests.post(
                f"{url}/message",
                json=message.to_dict(),
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                ack = data.get("response", {}).get("message_type", "FAIL")
                acks[url] = ack
                logger.info("ack_received", url=url, ack=ack)
            else:
                acks[url] = "FAIL"
        
        except Exception as e:
            acks[url] = "FAIL"
            logger.error("ack_timeout", url=url, error=str(e))
    
    return acks


def send_do_commit(txn_id: str, participant_urls: List[str]) -> Dict[str, str]:
    """Send DO_COMMIT to all participants."""
    commits = {}
    
    for url in participant_urls:
        message = Message(
            transaction_id=txn_id,
            sender="coordinator",
            receiver=url,
            message_type=MessageType.DO_COMMIT,
            state="COMMIT"
        )
        
        try:
            response = requests.post(
                f"{url}/message",
                json=message.to_dict(),
                timeout=5
            )
            
            if response.status_code == 200:
                commits[url] = "COMMITTED"
                logger.info("commit_received", url=url)
            else:
                commits[url] = "FAILED"
        
        except Exception as e:
            commits[url] = "FAILED"
            logger.error("commit_timeout", url=url, error=str(e))
    
    return commits


def send_abort(txn_id: str, participant_urls: List[str]):
    """Send ABORT to all participants."""
    for url in participant_urls:
        message = Message(
            transaction_id=txn_id,
            sender="coordinator",
            receiver=url,
            message_type=MessageType.ABORT,
            state="ABORT"
        )
        
        try:
            requests.post(
                f"{url}/message",
                json=message.to_dict(),
                timeout=5
            )
            logger.info("abort_sent", url=url)
        except Exception as e:
            logger.error("abort_failed", url=url, error=str(e))


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