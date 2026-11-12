"""Event and frame schemas for the customer reply stream."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Literal

from engagehub_common.events import new_id
from pydantic import BaseModel, ConfigDict, Field, field_validator

ID_PATTERN = r"^[A-Za-z0-9._:-]{1,64}$"
SenderType = Literal["virtual_agent", "human_agent", "customer", "system"]
_CUSTOMER_MESSAGE_NAMESPACE = uuid.UUID("0b5e0c55-2f43-4ad9-9a26-6c1f6f1f7b11")


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


class VirtualAgentReplyProduced(BaseModel):
    """The event on ``chat.replies``, keyed by conversation ID. It has no sequence number:
    the Chat Consumer assigns it, since a Kafka offset is not a conversation sequence."""

    model_config = ConfigDict(extra="forbid")

    event_type: Literal["VirtualAgentReplyProduced"] = "VirtualAgentReplyProduced"
    message_id: str = Field(pattern=ID_PATTERN)
    conversation_id: str = Field(pattern=ID_PATTERN)
    sender_type: SenderType = "virtual_agent"
    sender_id: str = Field(min_length=1, max_length=128)
    sender_name: str = Field(default="", max_length=128)
    content: str = Field(min_length=1, max_length=4000)
    timestamp: datetime
    correlation_id: str = Field(default_factory=new_id, min_length=1, max_length=64)
    schema_version: int = 1

    _aware_timestamp = field_validator("timestamp")(_aware)

    @property
    def event_id(self) -> str:
        return self.message_id

    @classmethod
    def parse(cls, raw: bytes) -> VirtualAgentReplyProduced:
        return cls.model_validate_json(raw)


class SequencedMessage(BaseModel):
    """A message after sequencing. Published on ``chat.replies.delivery`` for fan-out."""

    model_config = ConfigDict(extra="forbid")

    event_type: Literal["ChatMessageSequenced"] = "ChatMessageSequenced"
    message_id: str
    conversation_id: str
    sequence_number: int = Field(ge=1)
    sender_type: SenderType
    sender_id: str
    sender_name: str = ""
    content: str
    timestamp: datetime
    correlation_id: str = ""

    _aware_timestamp = field_validator("timestamp")(_aware)

    @property
    def event_id(self) -> str:
        return self.message_id

    @classmethod
    def parse(cls, raw: bytes) -> SequencedMessage:
        return cls.model_validate_json(raw)

    def frame(self, *, replayed: bool = False) -> dict:
        return {
            "type": "chat_message",
            "conversation_id": self.conversation_id,
            "message_id": self.message_id,
            "sequence_number": self.sequence_number,
            "sender_type": self.sender_type,
            "sender_id": self.sender_id,
            "sender_name": self.sender_name,
            "content": self.content,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "correlation_id": self.correlation_id,
            "replayed": replayed,
        }


class Cursor(BaseModel):
    last_acknowledged_sequence: int = 0
    last_message_id: str = ""
    updated_at: str = ""


def customer_message_id(conversation_id: str, customer_id: str, client_message_id: str) -> str:
    """Deterministic, so a resent customer message maps to the same log entry."""
    return str(uuid.uuid5(_CUSTOMER_MESSAGE_NAMESPACE, f"{conversation_id}:{customer_id}:{client_message_id}"))


# WebSocket frames from the client
class HelloFrame(BaseModel):
    type: Literal["hello"]
    token: str
    # Used only if Redis is unavailable: the highest sequence the client holds.
    last_received_sequence: int | None = Field(default=None, ge=0)


class AckFrame(BaseModel):
    type: Literal["ack"]
    sequence_number: int = Field(ge=1)
    message_id: str = ""


class CustomerMessageFrame(BaseModel):
    type: Literal["customer_message"]
    client_message_id: str = Field(min_length=8, max_length=64)
    content: str = Field(min_length=1, max_length=4000)


class PingFrame(BaseModel):
    type: Literal["ping"]
