"""Notification service: stores and delivers notifications requested through Kafka.

The event ID is the primary key, so a retried or redelivered event cannot notify
anyone twice. If this service is down, events wait in Kafka at the group's committed
offset and are processed when it returns.
"""

import logging
from datetime import datetime

from engagehub_common.auth import Principal, get_principal
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType, utcnow
from engagehub_common.messaging.bus import Record
from engagehub_common.metrics import DUPLICATES_SUPPRESSED
from engagehub_common.service import Runtime, create_service_app, get_runtime
from engagehub_common.topics import NOTIFICATIONS
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from sqlalchemy import Boolean, DateTime, String, Text, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

log = logging.getLogger(__name__)
router = APIRouter()
GROUP = "notification-service"


class Base(DeclarativeBase):
    pass


class Notification(Base):
    __tablename__ = "notifications"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    recipient_id: Mapped[str] = mapped_column(String(128), index=True)
    conversation_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    kind: Mapped[str] = mapped_column(String(64))
    title: Mapped[str] = mapped_column(String(256))
    body: Mapped[str] = mapped_column(Text)
    read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class NotificationConsumer:
    def __init__(self, rt: Runtime) -> None:
        self.rt = rt

    async def handle_event(self, event: EventEnvelope, _record: Record) -> None:
        if event.event_type != EventType.NOTIFICATION_REQUESTED:
            return
        payload = event.payload
        async with self.rt.db.session() as session:
            created = await self.rt.db.insert_ignore(
                session,
                Notification,
                {
                    "event_id": event.event_id,
                    "tenant_id": event.tenant_id,
                    "recipient_id": payload["recipient_id"],
                    "conversation_id": event.conversation_id,
                    "kind": payload["kind"],
                    "title": payload["title"],
                    "body": payload["body"],
                    "read": False,
                    "created_at": event.timestamp,
                },
            )
        if not created:
            DUPLICATES_SUPPRESSED.labels("notification").inc()
            return
        # Email and push are simulated. A real provider call would use event_id as its idempotency key.
        log.info("notification delivered", extra={"kind": payload["kind"], "recipient": payload["recipient_id"]})


def _public(n: Notification) -> dict:
    return {
        "id": n.event_id,
        "kind": n.kind,
        "title": n.title,
        "body": n.body,
        "read": n.read,
        "conversation_id": n.conversation_id,
        "created_at": n.created_at.isoformat(),
    }


@router.get("/notifications")
async def list_notifications(
    principal: Principal = Depends(get_principal), rt: Runtime = Depends(get_runtime)
) -> list[dict]:
    async with rt.db.session() as session:
        rows = await session.scalars(
            select(Notification)
            .where(Notification.tenant_id == principal.tenant_id, Notification.recipient_id == principal.subject)
            .order_by(Notification.created_at.desc())
            .limit(100)
        )
        return [_public(n) for n in rows]


@router.get("/notifications/unread-count")
async def unread_count(principal: Principal = Depends(get_principal), rt: Runtime = Depends(get_runtime)) -> dict:
    async with rt.db.session() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.tenant_id == principal.tenant_id,
                Notification.recipient_id == principal.subject,
                Notification.read.is_(False),
            )
        )
    return {"unread": count or 0}


@router.post("/notifications/{notification_id}/read")
async def mark_read(
    notification_id: str, principal: Principal = Depends(get_principal), rt: Runtime = Depends(get_runtime)
) -> dict:
    async with rt.db.session() as session:
        notification = await session.get(Notification, notification_id)
        if (
            notification is None
            or notification.tenant_id != principal.tenant_id
            or notification.recipient_id != principal.subject
        ):
            raise HTTPException(404, "notification not found")
        notification.read = True
        return _public(notification)


def create_app(runtime: Runtime | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="notification-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        # The primary key provides idempotency, so no Redis dedupe layer is needed.
        rt.add_consumer([NOTIFICATIONS], GROUP, NotificationConsumer(rt).handle_event, dedupe=False)

    app = create_service_app(
        "EngageHub Notification Service", settings, runtime=runtime, kafka=True, db=True, setup=setup
    )
    app.include_router(router)
    return app
