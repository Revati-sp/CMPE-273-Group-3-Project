"""Append-only, sequenced message log in PostgreSQL."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from engagehub_common.db import Database
from engagehub_common.messaging.consumer import NonRetryableError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.replies.models import ChatConversation, ChatMessage
from app.replies.schemas import SequencedMessage

log = logging.getLogger(__name__)


class UnknownConversation(NonRetryableError):
    """The event names a conversation that was never registered: retrying cannot help."""


class MessageIdConflict(NonRetryableError):
    """The same message ID was already used in a different conversation."""


class ConversationOwnedElsewhere(Exception):
    pass


@dataclass(frozen=True)
class AppendResult:
    message: SequencedMessage
    duplicate: bool


def _to_message(row: ChatMessage) -> SequencedMessage:
    return SequencedMessage(
        message_id=row.message_id,
        conversation_id=row.conversation_id,
        sequence_number=row.sequence_number,
        sender_type=row.sender_type,
        sender_id=row.sender_id,
        sender_name=row.sender_name,
        content=row.content,
        timestamp=row.timestamp,
        correlation_id=row.correlation_id,
    )


class MessageLog:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def register(self, conversation_id: str, tenant_id: str, customer_id: str) -> tuple[ChatConversation, bool]:
        """Create the conversation, or return it if this customer already owns it."""
        async with self.db.session() as session:
            created = await self.db.insert_ignore(
                session,
                ChatConversation,
                {
                    "conversation_id": conversation_id,
                    "tenant_id": tenant_id,
                    "customer_id": customer_id,
                    "last_sequence": 0,
                },
            )
            row = await session.get(ChatConversation, conversation_id)
            if row.tenant_id != tenant_id or row.customer_id != customer_id:
                raise ConversationOwnedElsewhere(conversation_id)
            return row, created

    async def get_conversation(self, conversation_id: str) -> ChatConversation | None:
        async with self.db.session() as session:
            return await session.get(ChatConversation, conversation_id)

    async def append(
        self,
        *,
        conversation_id: str,
        message_id: str,
        sender_type: str,
        sender_id: str,
        sender_name: str,
        content: str,
        timestamp,
        correlation_id: str,
    ) -> AppendResult:
        """Assign the next sequence number and store the message, exactly once per message ID."""
        try:
            return await self._append_once(
                conversation_id, message_id, sender_type, sender_id, sender_name, content, timestamp, correlation_id
            )
        except IntegrityError:
            # Only possible if two transactions raced on one message ID in different
            # conversations. Same-conversation appends are serialised by the row lock.
            existing = await self._find(message_id)
            if existing is None:
                raise
            if existing.conversation_id != conversation_id:
                raise MessageIdConflict(message_id) from None
            return AppendResult(existing, duplicate=True)

    async def _append_once(
        self, conversation_id, message_id, sender_type, sender_id, sender_name, content, timestamp, correlation_id
    ) -> AppendResult:
        async with self.db.session() as session:
            conversation = await session.scalar(
                select(ChatConversation).where(ChatConversation.conversation_id == conversation_id).with_for_update()
            )
            if conversation is None:
                raise UnknownConversation(conversation_id)
            existing = await session.scalar(select(ChatMessage).where(ChatMessage.message_id == message_id))
            if existing is not None:
                if existing.conversation_id != conversation_id:
                    raise MessageIdConflict(message_id)
                return AppendResult(_to_message(existing), duplicate=True)
            sequence = conversation.last_sequence + 1
            conversation.last_sequence = sequence
            row = ChatMessage(
                message_id=message_id,
                conversation_id=conversation_id,
                sequence_number=sequence,
                sender_type=sender_type,
                sender_id=sender_id,
                sender_name=sender_name,
                content=content,
                correlation_id=correlation_id,
                timestamp=timestamp,
            )
            session.add(row)
            await session.flush()
            return AppendResult(_to_message(row), duplicate=False)

    async def _find(self, message_id: str) -> SequencedMessage | None:
        async with self.db.session() as session:
            row = await session.scalar(select(ChatMessage).where(ChatMessage.message_id == message_id))
            return _to_message(row) if row else None

    async def after(self, conversation_id: str, after_sequence: int, limit: int = 1000) -> list[SequencedMessage]:
        async with self.db.session() as session:
            rows = await session.scalars(
                select(ChatMessage)
                .where(ChatMessage.conversation_id == conversation_id, ChatMessage.sequence_number > after_sequence)
                .order_by(ChatMessage.sequence_number)
                .limit(limit)
            )
            return [_to_message(r) for r in rows]

    async def between(self, conversation_id: str, after_sequence: int, up_to: int) -> list[SequencedMessage]:
        async with self.db.session() as session:
            rows = await session.scalars(
                select(ChatMessage)
                .where(
                    ChatMessage.conversation_id == conversation_id,
                    ChatMessage.sequence_number > after_sequence,
                    ChatMessage.sequence_number <= up_to,
                )
                .order_by(ChatMessage.sequence_number)
            )
            return [_to_message(r) for r in rows]

    async def message_at(self, conversation_id: str, sequence_number: int) -> SequencedMessage | None:
        async with self.db.session() as session:
            row = await session.scalar(
                select(ChatMessage).where(
                    ChatMessage.conversation_id == conversation_id, ChatMessage.sequence_number == sequence_number
                )
            )
            return _to_message(row) if row else None

    async def latest_sequence(self, conversation_id: str) -> int:
        async with self.db.session() as session:
            value = await session.scalar(
                select(func.max(ChatMessage.sequence_number)).where(ChatMessage.conversation_id == conversation_id)
            )
            return int(value or 0)
