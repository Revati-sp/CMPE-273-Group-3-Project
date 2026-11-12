"""The Chat Consumer: chat.replies -> sequenced PostgreSQL log -> delivery fan-out."""

from __future__ import annotations

import logging

from asyncpg.exceptions import CannotConnectNowError, PostgresConnectionError
from engagehub_common.messaging.bus import EventProducer, PublishError, Record
from engagehub_common.topics import CHAT_REPLIES_DELIVERY
from sqlalchemy.exc import InterfaceError, OperationalError

from app.replies.log import MessageLog
from app.replies.metrics import DUPLICATES_IGNORED, MESSAGES_APPENDED
from app.replies.schemas import SequencedMessage, VirtualAgentReplyProduced

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


class ReplyConsumer:
    def __init__(self, message_log: MessageLog, producer: EventProducer) -> None:
        self.log = message_log
        self.producer = producer

    async def handle(self, event: VirtualAgentReplyProduced, record: Record) -> None:
        context = {
            "conversation_id": event.conversation_id,
            "message_id": event.message_id,
            "kafka_partition": record.partition,
            "kafka_offset": record.offset,
        }
        log.info("message received from kafka", extra=context)
        result = await self.log.append(
            conversation_id=event.conversation_id,
            message_id=event.message_id,
            sender_type=event.sender_type,
            sender_id=event.sender_id,
            sender_name=event.sender_name,
            content=event.content,
            timestamp=event.timestamp,
            correlation_id=event.correlation_id,
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
        await publish_delivery(self.producer, result.message)
