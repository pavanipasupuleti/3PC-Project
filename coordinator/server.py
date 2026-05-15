"""
Coordinator Flask server for 3PC protocol.

This is the coordinator's "mouth and ears" - handles HTTP communication.
"""

import os
import threading
import time
from datetime import datetime
from typing import List, Dict

import requests
import structlog
from flask import Flask, request, jsonify

from coordinator.state import CoordinatorStateManager, CoordinatorState
from coordinator.messages import Message, MessageType, create_transaction_id
from coordinator.leader_election import LeaderElection
from coordinator.heartbeat import CoordinatorHeartbeat
from metrics.collector import metrics
from storage.database import db_store

# Initialize structured logging
logger = structlog.get_logger()

# Create Flask app
app = Flask(__name__)

# Identify this specific node instance
_NODE_ID = os.environ.get('NODE_ID', 'coordinator-1')

# ------------------------------------------------------------------
# Distributed leader election setup
# ------------------------------------------------------------------

election = LeaderElection(node_id=_NODE_ID)

# Initial election attempt to claim leadership
election.try_become_leader()

# Start continuous monitoring thread for failover retry
election.start_monitoring()

# Heartbeat sender — keeps participants' timeout clocks from firing
coordinator_heartbeat = CoordinatorHeartbeat(coordinator_id=_NODE_ID)

# Active transactions memory store — protected by a lock for thread safety
active_transactions: Dict = {}
active_transactions_lock = threading.Lock()


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.
    Returns OK if coordinator is running.
    """
    return jsonify({
        "status": "healthy",
        "service": "coordinator",
        "message": "Coordinator is running"
    }), 200


@app.route('/metrics', methods=['GET'])
def get_metrics_endpoint():
    """
    Shared DB-backed metrics endpoint.
    Retrieves statistics that survive leader failover due to persistence.
    """
    try:
        stats = db_store.get_statistics()

        snapshot = {
            "transactions": {
                "total": stats.get("total", 0),
                "committed": stats.get("committed", 0),
                "aborted": stats.get("aborted", 0),
                "commit_rate": round(
                    (stats.get("committed", 0) / max(stats.get("total", 1), 1)) * 100, 1
                )
            },
            "latency": {
                "phase1_avg": round(stats.get("avg_phase1", 0), 2),
                "phase2_avg": round(stats.get("avg_phase2", 0), 2),
                "phase3_avg": round(stats.get("avg_phase3", 0), 2),
                "total_avg": round(stats.get("avg_latency", 0), 2),
                "phase1_data": stats.get("phase1_history", []),
                "phase2_data": stats.get("phase2_history", []),
                "phase3_data": stats.get("phase3_history", [])
            },
            "failures": {
                "partitions": 0,
                "timeouts": 0
            },
            "state_transitions": {},
            "timeline": []
        }
        return jsonify(snapshot), 200

    except Exception as e:
        logger.error("metrics_fetch_failed", error=str(e))
        return jsonify({
            "transactions": {"total": 0, "committed": 0, "aborted": 0, "commit_rate": 0},
            "latency": {"phase1_avg": 0, "phase2_avg": 0, "phase3_avg": 0, "total_avg": 0, "phase1_data": [], "phase2_data": [], "phase3_data": []},
            "failures": {"partitions": 0, "timeouts": 0},
            "state_transitions": {},
            "timeline": []
        }), 200


@app.route('/start-transaction', methods=['POST'])
def start_transaction():
    """
    Basic endpoint to manually initiate a transaction state without full execution.
    """
    txn_id = create_transaction_id()
    state_mgr = CoordinatorStateManager(txn_id)
    
    with active_transactions_lock:
        active_transactions[txn_id] = {
            "state_manager": state_mgr,
            "participants": []
        }
    
    logger.info("transaction_started", transaction_id=txn_id, state=state_mgr.get_state().value)
    
    return jsonify({
        "status": "success",
        "transaction_id": txn_id,
        "state": state_mgr.get_state().value,
        "message": "Transaction started"
    }), 201


@app.route('/transaction/<txn_id>/status', methods=['GET'])
def get_transaction_status(txn_id):
    """
    Retrieves the current state and history of a specific transaction.
    """
    with active_transactions_lock:
        txn = active_transactions.get(txn_id)

    if txn is None:
        return jsonify({"status": "error", "message": "Transaction not found"}), 404

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
    The primary orchestration endpoint.
    If this node is not the leader, it forwards the request to the current leader.
    """
    # Forward to leader if necessary
    if not election.is_leader:
        leader = election.get_current_leader()
        leader_ports = {"coordinator-1": 5000, "coordinator-2": 5010, "coordinator-3": 5020}
        port = leader_ports.get(leader)

        if not port:
            return jsonify({"error": "Leader unknown"}), 503

        try:
            logger.info("forwarding_request_to_leader", current_node=_NODE_ID, leader=leader, leader_port=port)
            forward_response = requests.post(
                f"http://{leader}:{port}/execute-transaction",
                json=request.get_json(),
                timeout=60
            )
            return jsonify(forward_response.json()), forward_response.status_code
        except Exception as e:
            logger.error("leader_forward_failed", error=str(e))
            return jsonify({"error": str(e)}), 500

    # Logic for current Leader
    data = request.get_json()
    participant_urls = data.get('participants', [])

    if not participant_urls:
        return jsonify({"status": "error", "message": "No participants provided"}), 400
    
    txn_id = create_transaction_id()
    state_mgr = CoordinatorStateManager(txn_id)

    with active_transactions_lock:
        active_transactions[txn_id] = {
            "state_manager": state_mgr,
            "participants": participant_urls,
        }
    
    logger.info("transaction_started", transaction_id=txn_id, num_participants=len(participant_urls))
    
    try:
        result = execute_3pc_protocol(txn_id, state_mgr, participant_urls)
        return jsonify(result), 200
    except Exception as e:
        logger.error("transaction_failed", transaction_id=txn_id, error=str(e))
        metrics.record_abort(txn_id)
        return jsonify({"status": "error", "transaction_id": txn_id, "message": str(e)}), 500


def execute_3pc_protocol(txn_id: str, state_mgr: CoordinatorStateManager, participant_urls: List[str]) -> Dict:
    """
    Orchestrates the 3-Phase Commit sequence.
    """
    coordinator_heartbeat.register_participants(participant_urls)
    metrics.record_transaction_start(txn_id)
    total_start_time = time.monotonic()
    created_at = datetime.now()
    
    # Persistent logging
    db_store.save_transaction(txn_id=txn_id, status='STARTED', num_participants=len(participant_urls), created_at=created_at)
    db_store.log_event(txn_id, 'TRANSACTION_STARTED', details=f'{len(participant_urls)} participants')
    
    # --- PHASE 1: CAN_COMMIT ---
    logger.info("phase_1_start", transaction_id=txn_id, phase="CAN_COMMIT")
    phase1_start = time.monotonic()
    
    # Initialize participants
    for url in participant_urls:
        try:
            response = requests.post(f"{url}/init-transaction", json={"transaction_id": txn_id}, timeout=5)
            if response.status_code != 201:
                raise Exception(f"Participant {url} refused initialization")
        except Exception as e:
            logger.error("participant_init_failed", url=url, error=str(e))
            metrics.record_abort(txn_id)
            db_store.save_transaction(txn_id=txn_id, status='ABORT', num_participants=len(participant_urls), created_at=created_at, completed_at=datetime.now())
            db_store.log_event(txn_id, 'ABORTED', phase='PHASE_1', details=str(e))
            return {"status": "aborted", "transaction_id": txn_id, "final_state": "ABORT", "reason": str(e)}

    # Collect Votes
    votes = send_can_commit(txn_id, participant_urls)
    phase1_time = (time.monotonic() - phase1_start) * 1000
    metrics.record_phase_latency(1, phase1_time)
    
    if not all(v == "YES" for v in votes.values()):
        state_mgr.transition_to(CoordinatorState.ABORT)
        send_abort(txn_id, participant_urls)
        db_store.save_transaction(txn_id=txn_id, status='ABORT', num_participants=len(participant_urls), phase1_latency=phase1_time, created_at=created_at, completed_at=datetime.now())
        return {"status": "aborted", "transaction_id": txn_id, "final_state": "ABORT", "reason": "Negative vote or timeout in Phase 1", "votes": votes}

    # --- PHASE 2: PRE_COMMIT ---
    logger.info("phase_2_start", transaction_id=txn_id, phase="PRE_COMMIT")
    phase2_start = time.monotonic()
    
    state_mgr.transition_to(CoordinatorState.PRE_COMMIT)
    db_store.log_event(txn_id, 'STATE_TRANSITION', phase='PRE_COMMIT')
    
    acks = send_pre_commit(txn_id, participant_urls)
    phase2_time = (time.monotonic() - phase2_start) * 1000
    metrics.record_phase_latency(2, phase2_time)

    # Simulated delay for failure testing
    time.sleep(3) 
    
    if not all(ack == "ACK" for ack in acks.values()):
        state_mgr.transition_to(CoordinatorState.ABORT)
        send_abort(txn_id, participant_urls)
        metrics.record_abort(txn_id)
        db_store.save_transaction(txn_id=txn_id, status='ABORT', num_participants=len(participant_urls), phase1_latency=phase1_time, phase2_latency=phase2_time, created_at=created_at, completed_at=datetime.now())
        return {"status": "aborted", "transaction_id": txn_id, "final_state": "ABORT", "reason": "Acks missing in Phase 2", "acks": acks}
    
    # --- PHASE 3: DO_COMMIT ---
    logger.info("phase_3_start", transaction_id=txn_id, phase="DO_COMMIT")
    phase3_start = time.monotonic()
    
    state_mgr.transition_to(CoordinatorState.COMMIT)
    db_store.log_event(txn_id, 'STATE_TRANSITION', phase='COMMIT')
    
    commits = send_do_commit(txn_id, participant_urls)
    phase3_time = (time.monotonic() - phase3_start) * 1000
    metrics.record_phase_latency(3, phase3_time)
    
    total_time = (time.monotonic() - total_start_time) * 1000
    metrics.record_total_latency(total_time)
    metrics.record_commit(txn_id)
    
    db_store.save_transaction(
        txn_id=txn_id, status='COMMIT', num_participants=len(participant_urls),
        phase1_latency=phase1_time, phase2_latency=phase2_time, phase3_latency=phase3_time,
        total_latency=total_time, created_at=created_at, completed_at=datetime.now()
    )
    
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
    """Phase 1: Poll participants for readiness."""
    votes = {}
    for url in participant_urls:
        message = Message(transaction_id=txn_id, sender="coordinator", receiver=url, message_type=MessageType.CAN_COMMIT, state="WAIT")
        try:
            response = requests.post(f"{url}/message", json=message.to_dict(), timeout=5)
            if response.status_code == 200:
                vote = response.json().get("response", {}).get("message_type", "NO")
                votes[url] = vote
            else:
                votes[url] = "NO"
        except Exception as e:
            votes[url] = "NO"
            metrics.record_timeout()
    return votes


def send_pre_commit(txn_id: str, participant_urls: List[str]) -> Dict[str, str]:
    """Phase 2: Notify participants that commit is imminent."""
    acks = {}
    for url in participant_urls:
        message = Message(transaction_id=txn_id, sender="coordinator", receiver=url, message_type=MessageType.PRE_COMMIT, state="PRE_COMMIT")
        try:
            response = requests.post(f"{url}/message", json=message.to_dict(), timeout=5)
            if response.status_code == 200:
                ack = response.json().get("response", {}).get("message_type", "FAIL")
                acks[url] = ack
            else:
                acks[url] = "FAIL"
        except Exception as e:
            acks[url] = "FAIL"
            metrics.record_timeout()
    return acks


def send_do_commit(txn_id: str, participant_urls: List[str]) -> Dict[str, str]:
    """Phase 3: Final instruction to commit."""
    commits = {}
    for url in participant_urls:
        message = Message(transaction_id=txn_id, sender="coordinator", receiver=url, message_type=MessageType.DO_COMMIT, state="COMMIT")
        try:
            response = requests.post(f"{url}/message", json=message.to_dict(), timeout=5)
            commits[url] = "COMMITTED" if response.status_code == 200 else "FAILED"
        except Exception as e:
            commits[url] = "FAILED"
            metrics.record_timeout()
    return commits


def send_abort(txn_id: str, participant_urls: List[str]):
    """Send ABORT to all participants if any phase fails."""
    for url in participant_urls:
        message = Message(transaction_id=txn_id, sender="coordinator", receiver=url, message_type=MessageType.ABORT, state="ABORT")
        try:
            requests.post(f"{url}/message", json=message.to_dict(), timeout=2)
        except Exception as e:
            logger.error("abort_error", url=url, error=str(e))


@app.route('/leader-status', methods=['GET'])
def leader_status():
    """Reports leadership status for this node."""
    return jsonify({
        "node_id": election.node_id,
        "is_leader": election.is_leader,
        "current_leader": election.get_current_leader()
    }), 200


def run_coordinator(host='127.0.0.1', port=5000):
    """Initializes and runs the Flask server."""
    logger.info("coordinator_starting", node_id=_NODE_ID, host=host, port=port)
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)


if __name__ == '__main__':
    run_coordinator()