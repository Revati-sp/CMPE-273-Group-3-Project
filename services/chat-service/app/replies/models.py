"""The durable, append-only reply log in PostgreSQL. The source of truth for replay."""

from datetime import datetime

from engagehub_common.events import utcnow
from sqlalchemy import BigInteger, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class ReplyBase(DeclarativeBase):
    pass


class ChatConversation(ReplyBase):
    """One row per conversation. ``last_sequence`` is the per-conversation counter.

    The append path locks this row (SELECT ... FOR UPDATE), so two messages for one
    conversation cannot read the same counter value, even on different pods.
    """

    __tablename__ = "chat_conversations"

    conversation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    customer_id: Mapped[str] = mapped_column(String(128), index=True)
    last_sequence: Mapped[int] = mapped_column(BigInteger, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ChatMessage(ReplyBase):
    __tablename__ = "chat_messages"
    __table_args__ = (
        UniqueConstraint("conversation_id", "sequence_number", name="uq_chat_messages_conversation_sequence"),
        UniqueConstraint("message_id", name="uq_chat_messages_message_id"),
        Index("ix_chat_messages_conversation_sequence", "conversation_id", "sequence_number"),
    )

    id: Mapped[int] = mapped_column(Integer().with_variant(BigInteger, "postgresql"), primary_key=True)
    message_id: Mapped[str] = mapped_column(String(64))
    conversation_id: Mapped[str] = mapped_column(String(64))
    tenant_id: Mapped[str] = mapped_column(String(64), default="")
    customer_id: Mapped[str] = mapped_column(String(128), default="")
    sequence_number: Mapped[int] = mapped_column(BigInteger)
    sender_type: Mapped[str] = mapped_column(String(32))
    sender_id: Mapped[str] = mapped_column(String(128))
    sender_name: Mapped[str] = mapped_column(String(128), default="")
    content: Mapped[str] = mapped_column(Text)
    correlation_id: Mapped[str] = mapped_column(String(64), default="")
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


# Columns added after the table first shipped. create_all() does not alter existing tables,
# so on PostgreSQL they are added (idempotently) and backfilled at startup.
_MIGRATIONS = (
    "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) NOT NULL DEFAULT ''",
    "ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS customer_id VARCHAR(128) NOT NULL DEFAULT ''",
    "UPDATE chat_messages AS m SET tenant_id = c.tenant_id, customer_id = c.customer_id "
    "FROM chat_conversations AS c WHERE m.conversation_id = c.conversation_id AND m.tenant_id = ''",
)


async def migrate(db) -> None:
    from sqlalchemy import text

    if db.dialect != "postgresql":
        return
    async with db.engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(7219005)"))  # one replica at a time
        for statement in _MIGRATIONS:
            await conn.execute(text(statement))
