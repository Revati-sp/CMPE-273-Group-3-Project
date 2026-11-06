"""Durable conversation metadata and transcript records."""

from datetime import datetime

from engagehub_common.events import utcnow
from sqlalchemy import JSON, BigInteger, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    customer_id: Mapped[str] = mapped_column(String(128), index=True)
    session_id: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(32), default="open")
    subject: Mapped[str] = mapped_column(String(200), default="")
    language: Mapped[str] = mapped_column(String(8), default="en")
    workflow_state: Mapped[str] = mapped_column(String(32), default="virtual")
    assigned_agent_type: Mapped[str] = mapped_column(String(16), default="virtual")
    assigned_agent_id: Mapped[str] = mapped_column(String(128), default="")
    assigned_agent_name: Mapped[str] = mapped_column(String(128), default="")
    summary: Mapped[str] = mapped_column(Text, default="")
    rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating_comment: Mapped[str] = mapped_column(Text, default="")
    message_count: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class TranscriptMessage(Base):
    __tablename__ = "transcript_messages"
    __table_args__ = (
        UniqueConstraint("conversation_id", "sequence_number", name="uq_transcript_conv_seq"),
        UniqueConstraint("conversation_id", "message_id", name="uq_transcript_conv_message"),
        Index("ix_transcript_tenant_conv", "tenant_id", "conversation_id"),
    )

    id: Mapped[int] = mapped_column(Integer().with_variant(BigInteger, "postgresql"), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64))
    conversation_id: Mapped[str] = mapped_column(String(64))
    message_id: Mapped[str] = mapped_column(String(64))
    sequence_number: Mapped[int] = mapped_column(BigInteger)
    event_type: Mapped[str] = mapped_column(String(32))
    envelope: Mapped[dict] = mapped_column(JSON)
    kafka_partition: Mapped[int] = mapped_column(Integer)
    kafka_offset: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
