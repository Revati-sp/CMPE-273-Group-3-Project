"""Chat event envelope shared by every chat message in EngageHub.

Each message carries the identifiers needed for ordering, replay, and
deduplication during live-chat migration between pods.
"""
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel, Field


class EventType(str, Enum):
    CUSTOMER_MESSAGE = "customer_message"
    AGENT_MESSAGE = "agent_message"
    TYPING = "typing"
    ACK = "ack"


class ChatEvent(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    conversation_id: str  # Kafka partition key
    tenant_id: str
    sequence_number: int  # monotonically increasing per conversation
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any] = Field(default_factory=dict)
