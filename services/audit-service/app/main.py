"""Audit service: an append-only record of important actions."""

from datetime import datetime

from engagehub_common.auth import ROLE_ADMIN, Principal, require_roles
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.messaging.bus import Record
from engagehub_common.metrics import DUPLICATES_SUPPRESSED
from engagehub_common.service import Runtime, create_service_app, get_runtime
from engagehub_common.topics import AGENT_EVENTS, AUDIT_EVENTS, ESCALATIONS, SESSION_EVENTS
from fastapi import APIRouter, Depends, FastAPI, Query
from sqlalchemy import JSON, DateTime, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

router = APIRouter()
GROUP = "audit-service"
_IGNORED = {EventType.TYPING, EventType.AGENT_ROUTED, EventType.SESSION_CONNECTED}


class Base(DeclarativeBase):
    pass


class AuditRecord(Base):
    __tablename__ = "audit_records"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    conversation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(64), index=True)
    actor_id: Mapped[str] = mapped_column(String(128), default="")
    actor_role: Mapped[str] = mapped_column(String(16), default="")
    resource: Mapped[str] = mapped_column(String(128), default="")
    correlation_id: Mapped[str] = mapped_column(String(64))
    producer_service: Mapped[str] = mapped_column(String(64))
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class AuditConsumer:
    def __init__(self, rt: Runtime) -> None:
        self.rt = rt

    async def handle_event(self, event: EventEnvelope, _record: Record) -> None:
        if event.event_type in _IGNORED:
            return
        payload = event.payload
        is_explicit = event.event_type == EventType.AUDIT_RECORDED
        async with self.rt.db.session() as session:
            created = await self.rt.db.insert_ignore(
                session,
                AuditRecord,
                {
                    "event_id": event.event_id,
                    "tenant_id": event.tenant_id,
                    "conversation_id": event.conversation_id,
                    "action": payload.get("action", "unknown") if is_explicit else event.event_type.value,
                    "actor_id": payload.get("actor_id", "") if is_explicit else event.producer_service,
                    "actor_role": payload.get("actor_role", "") if is_explicit else "system",
                    "resource": payload.get("resource", "") if is_explicit else (event.conversation_id or ""),
                    "correlation_id": event.correlation_id,
                    "producer_service": event.producer_service,
                    # Handoff summaries quote customer text, so they stay out of the audit log.
                    "details": payload.get("details", {})
                    if is_explicit
                    else {k: v for k, v in payload.items() if k != "summary"},
                    "occurred_at": event.timestamp,
                },
            )
        if not created:
            DUPLICATES_SUPPRESSED.labels("audit").inc()


@router.get("/audit")
async def list_audit(
    action: str | None = None,
    conversation_id: str | None = None,
    limit: int = Query(default=100, le=500),
    principal: Principal = Depends(require_roles(ROLE_ADMIN)),
    rt: Runtime = Depends(get_runtime),
) -> list[dict]:
    query = select(AuditRecord).where(AuditRecord.tenant_id == principal.tenant_id)
    if action:
        query = query.where(AuditRecord.action == action)
    if conversation_id:
        query = query.where(AuditRecord.conversation_id == conversation_id)
    async with rt.db.session() as session:
        rows = await session.scalars(query.order_by(AuditRecord.occurred_at.desc()).limit(limit))
        return [
            {
                "id": r.event_id,
                "action": r.action,
                "actor_id": r.actor_id,
                "actor_role": r.actor_role,
                "resource": r.resource,
                "conversation_id": r.conversation_id,
                "correlation_id": r.correlation_id,
                "producer_service": r.producer_service,
                "details": r.details,
                "occurred_at": r.occurred_at.isoformat(),
            }
            for r in rows
        ]


def create_app(runtime: Runtime | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="audit-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        rt.add_consumer(
            [AUDIT_EVENTS, AGENT_EVENTS, ESCALATIONS, SESSION_EVENTS],
            GROUP,
            AuditConsumer(rt).handle_event,
            dedupe=False,
        )

    app = create_service_app("EngageHub Audit Service", settings, runtime=runtime, kafka=True, db=True, setup=setup)
    app.include_router(router)
    return app
