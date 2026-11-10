"""Materialises the transcript from Kafka and serves replay and session restore."""

from __future__ import annotations

import logging
from datetime import timedelta

from engagehub_common.db import Database
from engagehub_common.events import MESSAGE_EVENT_TYPES, EventEnvelope, EventType, utcnow
from engagehub_common.messaging.bus import EventLogReader, Record
from engagehub_common.messaging.consumer import NonRetryableError
from engagehub_common.metrics import DUPLICATES_SUPPRESSED, SEQUENCE_CONFLICTS
from engagehub_common.session import store as session_store
from engagehub_common.session.store import SessionState, SessionStore
from sqlalchemy import func, select

from app.models import Conversation, TranscriptMessage

log = logging.getLogger(__name__)

_RETENTION_FALLBACK = timedelta(days=14)


class HistoryService:
    def __init__(self, db: Database, store: SessionStore, reader: EventLogReader) -> None:
        self.db = db
        self.store = store
        self.reader = reader

    async def handle_event(self, event: EventEnvelope, record: Record) -> None:
        """Idempotent: redelivery cannot insert a message twice or lose metadata."""
        if event.conversation_id is None:
            return
        if event.event_type in MESSAGE_EVENT_TYPES:
            await self._store_message(event, record)
        else:
            await self._apply_conversation_event(event)

    async def _store_message(self, event: EventEnvelope, record: Record) -> None:
        async with self.db.session() as session:
            await self._ensure_conversation(session, event)
            inserted = await self.db.insert_ignore(
                session,
                TranscriptMessage,
                {
                    "tenant_id": event.tenant_id,
                    "conversation_id": event.conversation_id,
                    "message_id": event.message_id,
                    "sequence_number": event.sequence_number,
                    "event_type": event.event_type.value,
                    "envelope": event.model_dump(mode="json"),
                    "kafka_partition": record.partition,
                    "kafka_offset": record.offset,
                    "created_at": event.timestamp,
                },
            )
            if not inserted:
                existing = await session.scalar(
                    select(TranscriptMessage.message_id).where(
                        TranscriptMessage.conversation_id == event.conversation_id,
                        TranscriptMessage.sequence_number == event.sequence_number,
                    )
                )
                if existing is not None and existing != event.message_id:
                    # Only reachable if Redis lost acknowledged writes and reissued a number.
                    SEQUENCE_CONFLICTS.inc()
                    raise NonRetryableError(f"sequence {event.sequence_number} already held by message {existing}")
                DUPLICATES_SUPPRESSED.labels("transcript").inc()
                return
            conversation = await session.get(Conversation, event.conversation_id)
            conversation.message_count += 1
            conversation.updated_at = utcnow()

    @staticmethod
    async def _ensure_conversation(session, event: EventEnvelope) -> Conversation:
        conversation = await session.get(Conversation, event.conversation_id)
        if conversation is None:
            conversation = Conversation(
                id=event.conversation_id,
                tenant_id=event.tenant_id,
                customer_id=event.customer_id or "",
                started_at=event.timestamp,
            )
            session.add(conversation)
            await session.flush()
        return conversation

    async def _apply_conversation_event(self, event: EventEnvelope) -> None:
        payload = event.payload
        match event.event_type:
            case EventType.CONVERSATION_STARTED:
                changes = {
                    "session_id": payload.get("session_id", ""),
                    "subject": payload.get("subject", ""),
                    "language": payload.get("language", "en"),
                }
            case EventType.CONVERSATION_RESOLVED:
                changes = {"status": "resolved", "workflow_state": session_store.WORKFLOW_RESOLVED}
            case EventType.HANDOFF_REQUESTED:
                changes = {
                    "workflow_state": session_store.WORKFLOW_WAITING_HUMAN,
                    "summary": payload.get("summary", ""),
                }
            case EventType.AGENT_ASSIGNED:
                changes = {
                    "workflow_state": session_store.WORKFLOW_HUMAN,
                    "assigned_agent_type": "human",
                    "assigned_agent_id": payload.get("agent_id", ""),
                    "assigned_agent_name": payload.get("agent_name", ""),
                }
            case EventType.AGENT_RELEASED:
                changes = {
                    "workflow_state": payload.get("workflow_state", session_store.WORKFLOW_VIRTUAL),
                    "assigned_agent_type": "virtual",
                    "assigned_agent_id": "",
                    "assigned_agent_name": "",
                }
            case _:
                return
        async with self.db.session() as session:
            conversation = await self._ensure_conversation(session, event)
            for name, value in changes.items():
                setattr(conversation, name, value)
            conversation.updated_at = utcnow()

    async def messages_after(
        self, tenant_id: str, conversation_id: str, after_seq: int, limit: int = 1000
    ) -> list[EventEnvelope]:
        """Durable messages above ``after_seq``: the transcript plus the Kafka tail.

        The transcript fills asynchronously, so the newest messages may exist only
        in Kafka. Reading from the last materialised offset onward closes that gap.
        """
        async with self.db.session() as session:
            rows = (
                await session.scalars(
                    select(TranscriptMessage)
                    .where(
                        TranscriptMessage.tenant_id == tenant_id,
                        TranscriptMessage.conversation_id == conversation_id,
                        TranscriptMessage.sequence_number > after_seq,
                    )
                    .order_by(TranscriptMessage.sequence_number)
                    .limit(limit)
                )
            ).all()
            position = (
                await session.execute(
                    select(TranscriptMessage.kafka_partition, func.max(TranscriptMessage.kafka_offset))
                    .where(
                        TranscriptMessage.tenant_id == tenant_id,
                        TranscriptMessage.conversation_id == conversation_id,
                    )
                    .group_by(TranscriptMessage.kafka_partition)
                )
            ).first()
            conversation = await session.get(Conversation, conversation_id)

        by_seq = {r.sequence_number: EventEnvelope.model_validate(r.envelope) for r in rows}
        for event in await self._kafka_tail(tenant_id, conversation_id, position, conversation):
            if event.sequence_number > after_seq:
                by_seq.setdefault(event.sequence_number, event)
        return [by_seq[seq] for seq in sorted(by_seq)][:limit]

    async def _kafka_tail(self, tenant_id, conversation_id, position, conversation) -> list[EventEnvelope]:
        if conversation is not None and conversation.tenant_id != tenant_id:
            return []
        started = conversation.started_at if conversation else utcnow() - _RETENTION_FALLBACK
        since_ms = int((started - timedelta(minutes=1)).timestamp() * 1000)
        found = await self.reader.read_conversation(
            tenant_id,
            conversation_id,
            partition=position[0] if position else None,
            start_offset=position[1] + 1 if position else None,
            since_ms=since_ms,
        )
        return [event for event, _ in found if event.event_type in MESSAGE_EVENT_TYPES]

    async def restore_session(self, tenant_id: str, conversation_id: str) -> tuple[bool, int]:
        """Rebuild lost hot state from PostgreSQL and Kafka. Safe to call concurrently."""
        async with self.db.session() as session:
            conversation = await session.get(Conversation, conversation_id)
            if conversation is None or conversation.tenant_id != tenant_id:
                return False, 0
            max_seq = await session.scalar(
                select(func.max(TranscriptMessage.sequence_number)).where(
                    TranscriptMessage.conversation_id == conversation_id
                )
            )
        messages = await self.messages_after(tenant_id, conversation_id, max_seq or 0)
        head = max([max_seq or 0, *(m.sequence_number for m in messages)])
        await self.store.restore(
            SessionState(
                conversation_id=conversation.id,
                session_id=conversation.session_id,
                tenant_id=conversation.tenant_id,
                customer_id=conversation.customer_id,
                seq=head,
                last_committed_seq=head,
                workflow_state=conversation.workflow_state,
                assigned_agent_type=conversation.assigned_agent_type,
                assigned_agent_id=conversation.assigned_agent_id,
                assigned_agent_name=conversation.assigned_agent_name,
                language=conversation.language,
                created_at=conversation.started_at.isoformat(),
            )
        )
        log.info("session restored from durable storage", extra={"head_seq": head})
        return True, head
