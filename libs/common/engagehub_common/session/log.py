"""Appending sequenced messages to a conversation: the core write path.

The ordering of the three steps is what provides the delivery guarantee:

1. Reserve a sequence number in Redis under the idempotency key.
2. Publish to Kafka and wait for ``acks=all``.
3. Commit in Redis, and only then acknowledge to the caller.

A failure before step 3 means the caller was never acknowledged, so it retries
with the same key. The reservation returns the original identity, which makes
any second Kafka record an exact duplicate that consumers drop by message ID.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from opentelemetry.trace import SpanKind

from engagehub_common.events import EventEnvelope, EventType, new_id
from engagehub_common.messaging.bus import EventProducer
from engagehub_common.metrics import DUPLICATES_SUPPRESSED, PENDING_RECONCILED
from engagehub_common.session.store import SessionStore
from engagehub_common.telemetry import tracer
from engagehub_common.topics import CHAT_MESSAGES

log = logging.getLogger(__name__)

RestoreCallback = Callable[[str, str], Awaitable[bool]]


class SessionNotFound(Exception):
    pass


class StaleConnection(Exception):
    """A newer connection took over this participant: this socket is fenced off."""


@dataclass(frozen=True)
class AppendResult:
    event: EventEnvelope
    duplicate: bool


class ConversationLog:
    def __init__(
        self,
        store: SessionStore,
        producer: EventProducer,
        service_name: str,
        restore: RestoreCallback | None = None,
    ) -> None:
        self.store = store
        self.producer = producer
        self.service_name = service_name
        self.restore = restore
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    def _lock(self, conversation_id: str) -> asyncio.Lock:
        lock = self._locks.get(conversation_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[conversation_id] = lock
        return lock

    async def append(
        self,
        *,
        tenant_id: str,
        conversation_id: str,
        customer_id: str,
        event_type: EventType,
        payload: dict,
        idempotency_key: str,
        correlation_id: str | None = None,
        participant: str = "",
        epoch: int | None = None,
    ) -> AppendResult:
        template = EventEnvelope(
            event_type=event_type,
            conversation_id=conversation_id,
            tenant_id=tenant_id,
            customer_id=customer_id,
            message_id=new_id(),
            sequence_number=1,  # placeholder: Redis assigns the real number
            correlation_id=correlation_id or new_id(),
            producer_service=self.service_name,
            payload=payload,
        )
        # The lock keeps this process's publish order equal to its sequence order.
        lock = self._lock(conversation_id)
        async with lock:
            with tracer().start_as_current_span(
                "conversation.append",
                kind=SpanKind.PRODUCER,
                attributes={"engagehub.conversation_id": conversation_id, "engagehub.tenant_id": tenant_id},
            ):
                reservation = await self._reserve(
                    tenant_id, conversation_id, idempotency_key, template, participant, epoch
                )
                event = EventEnvelope.model_validate_json(reservation.envelope_json).model_copy(
                    update={"sequence_number": reservation.sequence_number}
                )
                if reservation.status == "DUP":
                    DUPLICATES_SUPPRESSED.labels("append").inc()
                    if not reservation.pending:
                        return AppendResult(event, duplicate=True)
                await self._publish_and_commit(tenant_id, conversation_id, event)
                return AppendResult(event, duplicate=reservation.status == "DUP")

    async def _reserve(self, tenant_id, conversation_id, key, template, participant, epoch):
        json_body = template.model_dump_json()
        reservation = await self.store.reserve(
            tenant_id, conversation_id, key, json_body, participant=participant, epoch=epoch
        )
        if reservation.status == "NO_SESSION" and self.restore:
            if await self.restore(tenant_id, conversation_id):
                reservation = await self.store.reserve(
                    tenant_id, conversation_id, key, json_body, participant=participant, epoch=epoch
                )
        if reservation.status == "NO_SESSION":
            raise SessionNotFound(conversation_id)
        if reservation.status == "STALE":
            raise StaleConnection(conversation_id)
        return reservation

    async def _publish_and_commit(self, tenant_id, conversation_id, event: EventEnvelope) -> None:
        result = await self.producer.publish(CHAT_MESSAGES, event)
        await self.store.commit(
            tenant_id, conversation_id, event.sequence_number, event.model_dump_json(), result.offset
        )

    async def reconcile_pending(
        self, tenant_id: str, conversation_id: str, *, older_than_ms: int, now_ms: int
    ) -> tuple[int, int | None]:
        """Republish reservations abandoned by a failed writer.

        Publishing is safe even if the original writer is alive or had already
        reached Kafka, because the record carries the same event and message IDs.
        Returns (count republished, lowest sequence number still in flight).
        """
        pending = await self.store.pending(tenant_id, conversation_id)
        republished = 0
        in_flight: int | None = None
        for seq in sorted(pending):
            reserved_at, envelope_json = pending[seq]
            if now_ms - reserved_at < older_than_ms:
                in_flight = seq if in_flight is None else min(in_flight, seq)
                continue
            event = EventEnvelope.model_validate_json(envelope_json).model_copy(update={"sequence_number": seq})
            await self._publish_and_commit(tenant_id, conversation_id, event)
            PENDING_RECONCILED.inc()
            republished += 1
        return republished, in_flight
