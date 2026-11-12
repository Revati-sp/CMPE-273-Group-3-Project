"""The Chat Consumer: chat.replies and agent.responses -> sequenced PostgreSQL log -> delivery fan-out."""

from __future__ import annotations

import logging
import re
from typing import get_args

from asyncpg.exceptions import CannotConnectNowError, PostgresConnectionError
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.messaging.bus import EventProducer, PublishError, Record
from engagehub_common.messaging.consumer import NonRetryableError
from engagehub_common.topics import AGENT_RESPONSES, CHAT_REPLIES, CHAT_REPLIES_DELIVERY
from sqlalchemy.exc import InterfaceError, OperationalError

from app.replies.log import ConversationOwnedElsewhere, MessageLog
from app.replies.metrics import DUPLICATES_IGNORED, MESSAGES_APPENDED
from app.replies.schemas import ID_PATTERN, SenderType, SequencedMessage, VirtualAgentReplyProduced

log = logging.getLogger(__name__)

# Outages to wait out. Retrying in place blocks the partition, which keeps each
# conversation in order, and nothing is dead-lettered because of an outage.
TRANSIENT_ERRORS = (
    OperationalError,
    InterfaceError,
    OSError,
    TimeoutError,
    PostgresConnectionError,
    CannotConnectNowError,
    PublishError,
)


async def publish_delivery(producer: EventProducer, message: SequencedMessage) -> None:
    await producer.publish_raw(
        CHAT_REPLIES_DELIVERY,
        message.conversation_id.encode(),
        message.model_dump_json().encode(),
        (("correlation_id", message.correlation_id.encode()),),
    )


async def commit_message(
    message_log: MessageLog,
    producer: EventProducer,
    *,
    source: str,
    conversation_id: str,
    message_id: str,
    sender_type: str,
    sender_id: str,
    sender_name: str,
    content: str,
    timestamp,
    correlation_id: str,
    record: Record,
) -> SequencedMessage:
    """Append to the sequenced log (idempotent by message ID), then fan out to the chat pods."""
    context = {
        "conversation_id": conversation_id,
        "message_id": message_id,
        "source_topic": source,
        "kafka_partition": record.partition,
        "kafka_offset": record.offset,
    }
    log.info("message received from kafka", extra=context)
    result = await message_log.append(
        conversation_id=conversation_id,
        message_id=message_id,
        sender_type=sender_type,
        sender_id=sender_id,
        sender_name=sender_name,
        content=content,
        timestamp=timestamp,
        correlation_id=correlation_id,
    )
    context["sequence_number"] = result.message.sequence_number
    if result.duplicate:
        DUPLICATES_IGNORED.labels("consumer").inc()
        log.info("duplicate message ignored", extra={**context, "stage": "consumer"})
    else:
        MESSAGES_APPENDED.inc()
        log.info("message appended to durable log", extra=context)
    # Republished on duplicates too: if a previous attempt stored the message but crashed
    # before fan-out, this is what delivers it. Sockets drop repeats by sequence number.
    await publish_delivery(producer, result.message)
    return result.message


class ReplyConsumer:
    """chat.replies: VirtualAgentReplyProduced events, for example from the fake producer."""

    def __init__(self, message_log: MessageLog, producer: EventProducer) -> None:
        self.log = message_log
        self.producer = producer

    async def handle(self, event: VirtualAgentReplyProduced, record: Record) -> None:
        await commit_message(
            self.log,
            self.producer,
            source=CHAT_REPLIES,
            conversation_id=event.conversation_id,
            message_id=event.message_id,
            sender_type=event.sender_type,
            sender_id=event.sender_id,
            sender_name=event.sender_name,
            content=event.content,
            timestamp=event.timestamp,
            correlation_id=event.correlation_id,
            record=record,
        )


SENDER_TYPES = set(get_args(SenderType))


class AgentResponseConsumer:
    """agent.responses: real virtual-agent responses, in the platform's standard EventEnvelope.

    Unlike the fake producer's events, these carry the tenant and customer, so a
    conversation the chat log has not seen yet is registered on first sight. A
    conversation that already belongs to another customer or tenant is refused.
    """

    def __init__(self, message_log: MessageLog, producer: EventProducer) -> None:
        self.log = message_log
        self.producer = producer

    async def handle(self, event: EventEnvelope, record: Record) -> None:
        if event.event_type != EventType.AGENT_RESPONSE:
            log.info("ignoring non-response event", extra={"event_type": event.event_type.value})
            return
        sender = event.payload.get("sender") or {}
        text = event.payload.get("text")
        missing = [
            name
            for name, value in (
                ("conversation_id", event.conversation_id),
                ("customer_id", event.customer_id),
                ("message_id", event.message_id),
                ("payload.text", text),
                ("payload.sender.id", sender.get("id")),
            )
            if not value
        ]
        if missing:
            raise NonRetryableError(f"agent response is missing {', '.join(missing)}")
        sender_type = sender.get("type", "virtual_agent")
        if sender_type not in SENDER_TYPES:
            raise NonRetryableError(f"unknown sender type {sender_type!r}")
        if not re.fullmatch(ID_PATTERN, event.conversation_id) or not re.fullmatch(ID_PATTERN, event.message_id):
            raise NonRetryableError("conversation_id or message_id has an invalid format")
        try:
            await self.log.register(event.conversation_id, event.tenant_id, event.customer_id)
        except ConversationOwnedElsewhere as exc:
            raise NonRetryableError("conversation belongs to another customer or tenant") from exc
        await commit_message(
            self.log,
            self.producer,
            source=AGENT_RESPONSES,
            conversation_id=event.conversation_id,
            message_id=event.message_id,
            sender_type=sender_type,
            sender_id=str(sender["id"]),
            sender_name=str(sender.get("name", "")),
            content=str(text)[:4000],
            timestamp=event.timestamp,
            correlation_id=event.correlation_id,
            record=record,
        )
