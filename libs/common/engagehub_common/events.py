"""The event envelope and types that form the contract between services.

Every Kafka event is an :class:`EventEnvelope`. ``extra="forbid"`` plus the
validator below mean a malformed event fails schema validation at the consumer
and goes to the dead-letter topic rather than crashing the consumer.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

SCHEMA_VERSION = 1
_EVENT_NAMESPACE = uuid.UUID("6f1d6a59-58f0-4b1e-9d3a-2f7a7c1f0e11")


class EventType(StrEnum):
    # chat.messages: sequenced, user-visible
    CUSTOMER_MESSAGE = "customer_message"
    AGENT_MESSAGE = "agent_message"
    HUMAN_MESSAGE = "human_message"
    SYSTEM_MESSAGE = "system_message"
    # chat.session-events
    CONVERSATION_STARTED = "conversation_started"
    CONVERSATION_RESOLVED = "conversation_resolved"
    SESSION_CONNECTED = "session_connected"
    SESSION_MIGRATED = "session_migrated"
    SESSION_REPLAYED = "session_replayed"
    TYPING = "typing"
    # chat.agent-events and chat.agent-tasks.*
    AGENT_ROUTED = "agent_routed"
    AGENT_TASK = "agent_task"
    AGENT_ASSIGNED = "agent_assigned"
    AGENT_TRANSFERRED = "agent_transferred"
    AGENT_RELEASED = "agent_released"
    # agent.responses: a virtual agent's reply, committed to the customer reply log by chat-service
    AGENT_RESPONSE = "agent_response"
    # chat.escalations
    ESCALATION_REQUESTED = "escalation_requested"
    HANDOFF_REQUESTED = "handoff_requested"
    CONVERSATION_ESCALATED = "conversation_escalated"
    # chat.notifications / chat.analytics / chat.audit-events
    NOTIFICATION_REQUESTED = "notification_requested"
    RATING_SUBMITTED = "rating_submitted"
    AUDIT_RECORDED = "audit_recorded"
    # chat.dead-letter-events
    DEAD_LETTER = "dead_letter"


MESSAGE_EVENT_TYPES = frozenset(
    {
        EventType.CUSTOMER_MESSAGE,
        EventType.AGENT_MESSAGE,
        EventType.HUMAN_MESSAGE,
        EventType.SYSTEM_MESSAGE,
    }
)


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return str(uuid.uuid4())


def derive_event_id(source_event_id: str, purpose: str) -> str:
    """Deterministic ID for an event caused by another event.

    When a consumer crashes after publishing but before committing, redelivery
    recomputes the same ID, so downstream consumers can drop the duplicate.
    """
    return str(uuid.uuid5(_EVENT_NAMESPACE, f"{source_event_id}:{purpose}"))


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False)

    event_id: str = Field(default_factory=new_id, min_length=1)
    event_type: EventType
    conversation_id: str | None = None
    tenant_id: str = Field(min_length=1)
    customer_id: str | None = None
    message_id: str | None = None
    sequence_number: int | None = Field(default=None, ge=1)
    correlation_id: str = Field(default_factory=new_id, min_length=1)
    timestamp: datetime = Field(default_factory=utcnow)
    producer_service: str = Field(min_length=1)
    schema_version: int = SCHEMA_VERSION
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _message_events_are_fully_identified(self) -> EventEnvelope:
        if self.event_type in MESSAGE_EVENT_TYPES:
            missing = [
                name
                for name in ("conversation_id", "customer_id", "message_id", "sequence_number")
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(f"{self.event_type} requires {', '.join(missing)}")
            if not isinstance(self.payload.get("text"), str):
                raise ValueError(f"{self.event_type} requires payload.text")
        return self

    @property
    def partition_key(self) -> str:
        """Conversation ID keeps one conversation ordered within a partition."""
        return self.conversation_id or self.tenant_id

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> EventEnvelope:
        return cls.model_validate_json(raw)


def sender(kind: str, sender_id: str, name: str) -> dict[str, str]:
    return {"type": kind, "id": sender_id, "name": name}


def agent_response_event(
    *,
    event_id: str,
    conversation_id: str,
    tenant_id: str,
    customer_id: str,
    message_id: str,
    text: str,
    sender_id: str,
    sender_name: str,
    producer_service: str,
    correlation_id: str | None = None,
    sender_type: str = "virtual_agent",
    metadata: dict | None = None,
    source_message_id: str | None = None,
) -> EventEnvelope:
    """The record a virtual agent publishes to ``agent.responses``.

    Shared by the producer (virtual-agent-service) and the consumer's tests
    (chat-service), so the two sides cannot drift apart.
    """
    payload: dict[str, Any] = {
        "text": text,
        "sender": sender(sender_type, sender_id, sender_name),
        "metadata": metadata or {},
    }
    if source_message_id:
        payload["source_message_id"] = source_message_id
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.AGENT_RESPONSE,
        conversation_id=conversation_id,
        tenant_id=tenant_id,
        customer_id=customer_id,
        message_id=message_id,
        correlation_id=correlation_id or new_id(),
        producer_service=producer_service,
        payload=payload,
    )
