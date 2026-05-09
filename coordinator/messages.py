"""
Message format definitions for Three-Phase Commit protocol.

Every message between coordinator and participants follows this structure.
"""

from enum import Enum
from typing import Optional, Dict, Any
import time
import uuid


class MessageType(Enum):
    """All possible message types in 3PC protocol."""
    CAN_COMMIT = "CAN_COMMIT"
    YES = "YES"
    NO = "NO"
    PRE_COMMIT = "PRE_COMMIT"
    ACK = "ACK"
    DO_COMMIT = "DO_COMMIT"
    ABORT = "ABORT"
    STATE_REQUEST = "STATE_REQUEST"
    STATE_RESPONSE = "STATE_RESPONSE"


class Message:
    """
    A 3PC protocol message.
    
    Think of this as an envelope + letter combined.
    """
    
    def __init__(
        self,
        transaction_id: str,
        sender: str,
        receiver: str,
        message_type: MessageType,
        state: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None
    ):
        self.transaction_id = transaction_id
        self.sender = sender
        self.receiver = receiver
        self.message_type = message_type
        self.timestamp = time.time()  # When was this sent?
        self.state = state  # Sender's current state
        self.data = data or {}  # Any extra info
    
    def to_dict(self) -> dict:
        """Convert message to JSON format for sending over HTTP."""
        return {
            "transaction_id": self.transaction_id,
            "sender": self.sender,
            "receiver": self.receiver,
            "message_type": self.message_type.value,
            "timestamp": self.timestamp,
            "state": self.state,
            "data": self.data
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'Message':
        """Create message from received JSON."""
        return Message(
            transaction_id=data["transaction_id"],
            sender=data["sender"],
            receiver=data["receiver"],
            message_type=MessageType(data["message_type"]),
            state=data.get("state"),
            data=data.get("data", {})
        )
    
    def __repr__(self):
        """Pretty print for debugging."""
        return (f"Message({self.message_type.value}: "
                f"{self.sender} -> {self.receiver}, "
                f"txn={self.transaction_id[:8]}...)")


def create_transaction_id() -> str:
    """Generate unique transaction ID."""
    return str(uuid.uuid4())
