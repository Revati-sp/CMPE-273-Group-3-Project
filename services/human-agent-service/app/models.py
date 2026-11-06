from datetime import datetime

from engagehub_common.events import new_id, utcnow
from sqlalchemy import JSON, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

STATUS_AVAILABLE = "available"
STATUS_BUSY = "busy"
STATUS_OFFLINE = "offline"

QUEUE_WAITING = "waiting"
QUEUE_OFFERED = "offered"
QUEUE_ASSIGNED = "assigned"
QUEUE_ESCALATED = "escalated"
QUEUE_RESOLVED = "resolved"
QUEUE_RELEASED = "released"
OPEN_STATES = (QUEUE_WAITING, QUEUE_OFFERED, QUEUE_ASSIGNED, QUEUE_ESCALATED)


class Base(DeclarativeBase):
    pass


class HumanAgent(Base):
    __tablename__ = "human_agents"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16), default=STATUS_OFFLINE)
    languages: Mapped[list] = mapped_column(JSON, default=lambda: ["en"])
    skills: Mapped[list] = mapped_column(JSON, default=list)
    max_concurrent: Mapped[int] = mapped_column(Integer, default=3)
    last_assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class QueueEntry(Base):
    __tablename__ = "queue_entries"
    __table_args__ = (Index("ix_queue_tenant_status", "tenant_id", "status"),)

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64))
    conversation_id: Mapped[str] = mapped_column(String(64), index=True)
    customer_id: Mapped[str] = mapped_column(String(128))
    # The source event's ID: redelivering a handoff cannot enqueue a conversation twice.
    source_event_id: Mapped[str] = mapped_column(String(64), unique=True)
    status: Mapped[str] = mapped_column(String(16), default=QUEUE_WAITING)
    priority: Mapped[int] = mapped_column(Integer, default=1)
    reason: Mapped[str] = mapped_column(String(128), default="")
    intent: Mapped[str] = mapped_column(String(32), default="unknown")
    language: Mapped[str] = mapped_column(String(8), default="en")
    sentiment: Mapped[float] = mapped_column(Float, default=0.0)
    tier: Mapped[str] = mapped_column(String(16), default="standard")
    summary: Mapped[str] = mapped_column(Text, default="")
    offered_to: Mapped[str | None] = mapped_column(String(128), nullable=True)
    offered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    assigned_to: Mapped[str | None] = mapped_column(String(128), nullable=True)
    rejected_by: Mapped[list] = mapped_column(JSON, default=list)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    assignment_count: Mapped[int] = mapped_column(Integer, default=0)


class InternalNote(Base):
    __tablename__ = "internal_notes"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    conversation_id: Mapped[str] = mapped_column(String(64), index=True)
    author_id: Mapped[str] = mapped_column(String(128))
    author_name: Mapped[str] = mapped_column(String(128))
    text: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
