"""
Coordinator Flask server for 3PC protocol.

This is the coordinator's "mouth and ears" - handles HTTP communication.
"""

from flask import Flask, request, jsonify
import structlog
from coordinator.state import CoordinatorStateManager, CoordinatorState
from coordinator.messages import Message, MessageType, create_transaction_id
from coordinator.leader_election import LeaderElection
from coordinator.heartbeat import CoordinatorHeartbeat
import requests
import threading
from typing import List, Dict
import time
from datetime import datetime
from metrics.collector import metrics
from storage.database import db_store

logger = structlog.get_logger()

# Create Flask app
app = Flask(__name__)

# Leader election — only the leader executes transactions
election = LeaderElection(node_id='coordinator-1')
election.try_become_leader()

# Heartbeat sender — keeps participants' timeout clocks from firing
coordinator_heartbeat = CoordinatorHeartbeat(coordinator_id='coordinator-1')

# Active transactions — protected by a lock for thread safety
active_transactions: Dict = {}
active_transactions_lock = threading.Lock()


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


@app.route('/metrics', methods=['GET'])
def get_metrics_endpoint():
    """
    Get metrics snapshot.
    
    Expose metrics to dashboard.
    """
    snapshot = metrics.get_snapshot()
    return jsonify(snapshot), 200


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
    with active_transactions_lock:
        txn = active_transactions.get(txn_id)

    if txn is None:
        return jsonify({
            "status": "error",
            "message": "Transaction not found"
        }), 404
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
    if not election.is_leader:
        return jsonify({
            "error": "Not leader",
            "leader": election.get_current_leader()
        }), 503

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

    # Store transaction (lock protects concurrent transaction starts)
    with active_transactions_lock:
        active_transactions[txn_id] = {
            "state_manager": state_mgr,
            "participants": participant_urls,
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
    # Register participants so heartbeat thread keeps them updated
    coordinator_heartbeat.register_participants(participant_urls)

    # Record transaction start
    metrics.record_transaction_start(txn_id)
    total_start_time = time.monotonic()
    
    # 🆕 Log transaction start in database
    created_at = datetime.now()
    db_store.save_transaction(
        txn_id=txn_id,
        status='STARTED',
        num_participants=len(participant_urls),
        created_at=created_at
    )
    db_store.log_event(
        txn_id,
        'TRANSACTION_STARTED',
        details=f'{len(participant_urls)} participants'
    )
    
    # PHASE 1: CAN_COMMIT
    logger.info("phase_1_start", transaction_id=txn_id, phase="CAN_COMMIT")
    phase1_start = time.monotonic()  # 🆕 Changed to monotonic
    
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
            
            # 🆕 Log failure in database
            db_store.save_transaction(
                txn_id=txn_id,
                status='FAILED',
                num_participants=len(participant_urls),
                created_at=created_at,
                completed_at=datetime.now()
            )
            db_store.log_event(txn_id, 'INITIALIZATION_FAILED', details=str(e))
            raise
    
    # Transition to WAIT state
    state_mgr.transition_to(CoordinatorState.WAIT)
    db_store.log_event(txn_id, 'STATE_TRANSITION', phase='WAIT')
    
    # Send CAN_COMMIT to all participants
    votes = send_can_commit(txn_id, participant_urls)
    
    # Record Phase 1 latency
    phase1_time = (time.monotonic() - phase1_start) * 1000  # 🆕 Changed to monotonic
    metrics.record_phase_latency(1, phase1_time)
    db_store.log_event(txn_id, 'PHASE_1_COMPLETE', phase='CAN_COMMIT', 
                      details=f'Latency: {phase1_time:.2f}ms')
    
    # Check votes
    all_yes = all(vote == "YES" for vote in votes.values())
    
    if not all_yes:
        # ABORT: At least one participant voted NO
        logger.info("aborting_transaction", transaction_id=txn_id, reason="not all voted YES")
        state_mgr.transition_to(CoordinatorState.ABORT)
        send_abort(txn_id, participant_urls)
        
        # Record abort
        metrics.record_abort(txn_id)
        
        # 🆕 Save abort to database
        db_store.save_transaction(
            txn_id=txn_id,
            status='ABORT',
            num_participants=len(participant_urls),
            phase1_latency=phase1_time,
            created_at=created_at,
            completed_at=datetime.now()
        )
        db_store.log_event(txn_id, 'ABORTED', phase='PHASE_1', 
                          details='Not all participants voted YES')
        
        return {
            "status": "aborted",
            "transaction_id": txn_id,
            "final_state": "ABORT",
            "reason": "One or more participants voted NO",
            "votes": votes
        }
    
    # PHASE 2: PRE_COMMIT
    logger.info("phase_2_start", transaction_id=txn_id, phase="PRE_COMMIT")
    phase2_start = time.monotonic()  # 🆕 Changed to monotonic
    
    state_mgr.transition_to(CoordinatorState.PRE_COMMIT)
    db_store.log_event(txn_id, 'STATE_TRANSITION', phase='PRE_COMMIT')
    
    # Send PRE_COMMIT to all participants
    acks = send_pre_commit(txn_id, participant_urls)
    
    # Record Phase 2 latency
    phase2_time = (time.monotonic() - phase2_start) * 1000  # 🆕 Changed to monotonic
    metrics.record_phase_latency(2, phase2_time)
    db_store.log_event(txn_id, 'PHASE_2_COMPLETE', phase='PRE_COMMIT',
                      details=f'Latency: {phase2_time:.2f}ms')
    
    # Check ACKs
    all_acked = all(ack == "ACK" for ack in acks.values())
    
    if not all_acked:
        logger.error("pre_commit_failed", transaction_id=txn_id)
        state_mgr.transition_to(CoordinatorState.ABORT)
        send_abort(txn_id, participant_urls)
        
        # Record abort
        metrics.record_abort(txn_id)
        
        # 🆕 Save abort to database
        db_store.save_transaction(
            txn_id=txn_id,
            status='ABORT',
            num_participants=len(participant_urls),
            phase1_latency=phase1_time,
            phase2_latency=phase2_time,
            created_at=created_at,
            completed_at=datetime.now()
        )
        db_store.log_event(txn_id, 'ABORTED', phase='PHASE_2',
                          details='Not all participants acknowledged PRE_COMMIT')
        
        return {
            "status": "aborted",
            "transaction_id": txn_id,
            "final_state": "ABORT",
            "reason": "Not all participants acknowledged PRE_COMMIT",
            "acks": acks
        }
    
    # PHASE 3: DO_COMMIT
    logger.info("phase_3_start", transaction_id=txn_id, phase="DO_COMMIT")
    phase3_start = time.monotonic()  # 🆕 Changed to monotonic
    
    state_mgr.transition_to(CoordinatorState.COMMIT)
    db_store.log_event(txn_id, 'STATE_TRANSITION', phase='COMMIT')
    
    # Send DO_COMMIT to all participants
    commits = send_do_commit(txn_id, participant_urls)
    
    # Record Phase 3 latency
    phase3_time = (time.monotonic() - phase3_start) * 1000  # 🆕 Changed to monotonic
    metrics.record_phase_latency(3, phase3_time)
    db_store.log_event(txn_id, 'PHASE_3_COMPLETE', phase='DO_COMMIT',
                      details=f'Latency: {phase3_time:.2f}ms')
    
    logger.info("transaction_committed", transaction_id=txn_id)
    
    # Record total latency and commit
    total_time = (time.monotonic() - total_start_time) * 1000  # 🆕 Changed to monotonic
    metrics.record_total_latency(total_time)
    metrics.record_commit(txn_id)
    
    # 🆕 Save successful commit to database
    db_store.save_transaction(
        txn_id=txn_id,
        status='COMMIT',
        num_participants=len(participant_urls),
        phase1_latency=phase1_time,
        phase2_latency=phase2_time,
        phase3_latency=phase3_time,
        total_latency=total_time,
        created_at=created_at,
        completed_at=datetime.now()
    )
    db_store.log_event(txn_id, 'COMMITTED', phase='PHASE_3',
                      details=f'Total latency: {total_time:.2f}ms')
    
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
            logger.error("vote_error", url=url, error=str(e))
            metrics.record_timeout()
    
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
            logger.error("ack_error", url=url, error=str(e))
            metrics.record_timeout()
    
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
            )

            if response.status_code == 200:
                commits[url] = "COMMITTED"
                logger.info("commit_received", url=url)
            else:
                commits[url] = "FAILED"

        except Exception as e:
            commits[url] = "FAILED"
            logger.error("commit_error", url=url, error=str(e))
            metrics.record_timeout()
    
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
            )
            logger.info("abort_sent", url=url)
        except Exception as e:
            logger.error("abort_error", url=url, error=str(e))


@app.route('/leader-status', methods=['GET'])
def leader_status():
    """
    Report this node's leadership status.

    Returns whether this coordinator is the current leader
    and who holds the lock in etcd.
    """
    return jsonify({
        "node_id": election.node_id,
        "is_leader": election.is_leader,
        "current_leader": election.get_current_leader()
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