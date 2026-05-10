"""
Participant Flask server for 3PC protocol.

Key changes from the original:
  - GlobalStateManager replaces the single-global state manager,
    supporting multiple concurrent transactions with thread safety.
  - HeartbeatMonitor detects coordinator silence automatically.
  - AutoRecovery resolves pending transactions without coordinator.
  - State is persisted to SQLite; restarts resume from last known state.
  - Peer URLs are read from the PARTICIPANT_PEERS environment variable.
  - Protocol message sends have NO timeout (infinite); only admin
    endpoints (heartbeat, health, peer queries) use a short timeout.
"""

import json
import os
import threading
from typing import Optional

import structlog
import requests
from flask import Flask, jsonify, request

from coordinator.messages import Message, MessageType
from participant.auto_recovery import AutoRecovery
from participant.state_manager import GlobalStateManager
from participant.timeout_detector import HeartbeatMonitor

logger = structlog.get_logger()

app = Flask(__name__)

# ------------------------------------------------------------------
# Module-level globals — initialised in run_participant()
# ------------------------------------------------------------------

participant_id: Optional[str] = None
state_manager: Optional[GlobalStateManager] = None
heartbeat_monitor: Optional[HeartbeatMonitor] = None
auto_recovery: Optional[AutoRecovery] = None

# Lock protecting the participant_id / state_manager pair during
# concurrent init-transaction requests.
_init_lock = threading.Lock()


def _peer_urls() -> list:
    """
    Return peer participant URLs from the PARTICIPANT_PEERS env var.

    The value must be a JSON array of URL strings, e.g.:
        ["http://toxiproxy-server:5012", "http://toxiproxy-server:5013"]
    """
    raw = os.environ.get("PARTICIPANT_PEERS", "[]")
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.error("invalid_participant_peers_env", raw=raw)
        return []


# ------------------------------------------------------------------
# Recovery callback — fired by HeartbeatMonitor on coordinator silence
# ------------------------------------------------------------------

def _on_coordinator_timeout() -> None:
    """Trigger automatic recovery when coordinator goes silent."""
    if auto_recovery is None:
        return
    logger.warning("auto_recovery_triggered", participant_id=participant_id)
    results = auto_recovery.attempt_recovery()
    logger.info("auto_recovery_complete",
                participant_id=participant_id, results=results)


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "participant",
        "participant_id": participant_id,
    }), 200


# ------------------------------------------------------------------
# Coordinator heartbeat endpoint
# ------------------------------------------------------------------

@app.route("/heartbeat", methods=["POST"])
def receive_heartbeat():
    """
    Called by the coordinator every ~2 s.

    Resets the HeartbeatMonitor's clock so it does not fire the
    timeout callback while the coordinator is alive.
    """
    if heartbeat_monitor is not None:
        heartbeat_monitor.update_heartbeat()
    return jsonify({"status": "alive", "participant_id": participant_id}), 200


# ------------------------------------------------------------------
# Transaction initialisation
# ------------------------------------------------------------------

@app.route("/init-transaction", methods=["POST"])
def init_transaction():
    """
    Initialise this participant for a new transaction.

    Called by the coordinator before Phase 1.
    """
    global state_manager

    data = request.get_json()
    txn_id = data.get("transaction_id")

    if not txn_id:
        return jsonify({"status": "error",
                        "message": "transaction_id required"}), 400

    if state_manager is None:
        return jsonify({"status": "error",
                        "message": "Participant not started"}), 503

    with _init_lock:
        state_manager.initialize(txn_id)

    # A new transaction is in-flight; start the heartbeat clock.
    if heartbeat_monitor is not None:
        heartbeat_monitor.mark_transaction_active()

    logger.info("transaction_initialized",
                participant_id=participant_id, txn_id=txn_id[:8])

    return jsonify({
        "status": "success",
        "participant_id": participant_id,
        "transaction_id": txn_id,
        "state": state_manager.get_state(txn_id),
    }), 201


# ------------------------------------------------------------------
# Protocol message dispatch
# ------------------------------------------------------------------

@app.route("/message", methods=["POST"])
def receive_message():
    """
    Receive a protocol message from the coordinator.

    Dispatches to the appropriate phase handler based on message type.
    """
    if state_manager is None:
        return jsonify({"status": "error",
                        "message": "Participant not started"}), 503

    data = request.get_json()
    try:
        message = Message.from_dict(data)
    except Exception as exc:
        logger.error("invalid_message", error=str(exc))
        return jsonify({"status": "error",
                        "message": "Invalid message format"}), 400

    txn_id = message.transaction_id

    logger.info("message_received",
                participant_id=participant_id,
                message_type=message.message_type.value,
                txn_id=txn_id[:8])

    # Every coordinator message resets the heartbeat timer.
    if heartbeat_monitor is not None:
        heartbeat_monitor.update_heartbeat()

    if state_manager.get_state(txn_id) is None:
        return jsonify({"status": "error",
                        "message": "Unknown transaction"}), 400

    dispatch = {
        MessageType.CAN_COMMIT: _handle_can_commit,
        MessageType.PRE_COMMIT: _handle_pre_commit,
        MessageType.DO_COMMIT:  _handle_do_commit,
        MessageType.ABORT:      _handle_abort,
    }
    handler = dispatch.get(message.message_type)

    if handler is None:
        logger.warning("unknown_message_type",
                       message_type=message.message_type.value)
        return jsonify({"status": "error",
                        "message": f"Unknown type: {message.message_type.value}"}), 400

    response_msg = handler(message)

    # If the transaction reached a final state, the clock stops.
    if heartbeat_monitor is not None and state_manager.is_final(txn_id):
        heartbeat_monitor.mark_transaction_done()

    return jsonify({
        "status": "success",
        "participant_id": participant_id,
        "response": response_msg.to_dict() if response_msg else None,
        "current_state": state_manager.get_state(txn_id),
    }), 200


# ------------------------------------------------------------------
# Phase handlers
# ------------------------------------------------------------------

def _handle_can_commit(message: Message) -> Message:
    """Phase 1 — vote YES and move to READY."""
    txn_id = message.transaction_id

    success = state_manager.transition(
        txn_id, "READY", reason="voted YES to CAN_COMMIT")

    if success:
        vote = MessageType.YES
    else:
        # Unexpected state — vote NO and abort defensively
        vote = MessageType.NO
        state_manager.transition(txn_id, "ABORT",
                                 reason="invalid transition; voting NO")

    logger.info("voted", participant_id=participant_id,
                vote=vote.value, txn_id=txn_id[:8])

    return Message(
        transaction_id=txn_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=vote,
        state=state_manager.get_state(txn_id),
    )


def _handle_pre_commit(message: Message) -> Message:
    """Phase 2 — move to PRE_COMMIT and ACK."""
    txn_id = message.transaction_id

    state_manager.transition(
        txn_id, "PRE_COMMIT",
        reason="received PRE_COMMIT from coordinator")

    return Message(
        transaction_id=txn_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=MessageType.ACK,
        state=state_manager.get_state(txn_id),
    )


def _handle_do_commit(message: Message) -> Message:
    """Phase 3 — commit and confirm."""
    txn_id = message.transaction_id

    state_manager.transition(
        txn_id, "COMMIT",
        reason="received DO_COMMIT from coordinator")

    logger.info("transaction_committed",
                participant_id=participant_id, txn_id=txn_id[:8])

    return Message(
        transaction_id=txn_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=MessageType.ACK,
        state=state_manager.get_state(txn_id),
        data={"committed": True},
    )


def _handle_abort(message: Message) -> Message:
    """Abort the transaction on coordinator instruction."""
    txn_id = message.transaction_id

    state_manager.transition(
        txn_id, "ABORT",
        reason="received ABORT from coordinator")

    logger.info("transaction_aborted",
                participant_id=participant_id, txn_id=txn_id[:8])

    return Message(
        transaction_id=txn_id,
        sender=participant_id,
        receiver=message.sender,
        message_type=MessageType.ACK,
        state=state_manager.get_state(txn_id),
        data={"aborted": True},
    )


# ------------------------------------------------------------------
# State inspection endpoints
# ------------------------------------------------------------------

@app.route("/state", methods=["GET"])
def get_state():
    """Return all tracked transaction states for this participant."""
    if state_manager is None:
        return jsonify({"status": "error",
                        "message": "Participant not started"}), 503

    return jsonify({
        "status": "success",
        "participant_id": participant_id,
        "states": state_manager.all_states(),
        "pending": state_manager.get_pending_txn_ids(),
    }), 200


@app.route("/peer-state", methods=["GET"])
def get_peer_state():
    """
    Return state of all pending transactions.

    Used during manual recovery to assess peer readiness.
    Kept for backward compatibility; prefer /query-state/<txn_id>
    for per-transaction queries.
    """
    if state_manager is None:
        return jsonify({"status": "error",
                        "message": "No active transaction"}), 400

    pending = state_manager.get_pending_txn_ids()
    states = {tid: state_manager.get_state(tid) for tid in pending}

    return jsonify({
        "participant_id": participant_id,
        "pending_states": states,
        "peer_urls": _peer_urls(),
    }), 200


@app.route("/query-state/<txn_id>", methods=["GET"])
def query_state(txn_id: str):
    """
    Per-transaction state query used by AutoRecovery on peer participants.

    Returns the current state for `txn_id`, or "UNKNOWN" if not tracked.
    """
    if state_manager is None:
        return jsonify({"txn_id": txn_id, "state": "UNKNOWN"}), 200

    state = state_manager.get_state(txn_id) or "UNKNOWN"
    return jsonify({
        "txn_id":           txn_id,
        "participant_id":   participant_id,
        "state":            state,
    }), 200


# ------------------------------------------------------------------
# Recovery endpoint
# ------------------------------------------------------------------

@app.route("/recover", methods=["POST"])
def trigger_recovery():
    """
    Manually trigger the non-blocking recovery protocol.

    AutoRecovery queries configured peers (PARTICIPANT_PEERS env var)
    and resolves all pending transactions.  No request body required.
    """
    if auto_recovery is None:
        return jsonify({"status": "error",
                        "message": "Participant not started"}), 503

    logger.info("manual_recovery_triggered", participant_id=participant_id)
    results = auto_recovery.attempt_recovery()

    return jsonify({
        "participant_id":    participant_id,
        "recovery_attempted": True,
        "results":           results,
    }), 200


# ------------------------------------------------------------------
# Server startup
# ------------------------------------------------------------------

def run_participant(p_id: str, host: str = "127.0.0.1", port: int = 5001) -> None:
    """
    Initialise globals and start the Flask server.

    Args:
        p_id:  participant identifier, e.g. "participant_1"
        host:  bind address
        port:  listen port
    """
    global participant_id, state_manager, heartbeat_monitor, auto_recovery

    participant_id = p_id

    # State manager restores non-final transactions from SQLite on init
    state_manager = GlobalStateManager(participant_id=p_id)

    # AutoRecovery reads peer URLs from env at startup
    peers = _peer_urls()
    auto_recovery = AutoRecovery(
        participant_id=p_id,
        peer_urls=peers,
        state_manager=state_manager,
    )

    # HeartbeatMonitor fires _on_coordinator_timeout on silence
    heartbeat_monitor = HeartbeatMonitor(
        participant_id=p_id,
        heartbeat_timeout=float(os.environ.get("HEARTBEAT_TIMEOUT", "5")),
        on_timeout_callback=_on_coordinator_timeout,
    )

    logger.info("participant_starting",
                participant_id=p_id, host=host, port=port,
                peers=peers)

    app.run(host=host, port=port, debug=True, use_reloader=False)


if __name__ == "__main__":
    run_participant("participant_1", port=5001)
