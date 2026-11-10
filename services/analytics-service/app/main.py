"""Analytics service: per-tenant metrics aggregated from Kafka events."""

from datetime import date

from engagehub_common.auth import ROLE_ADMIN, Principal, require_roles
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType, utcnow
from engagehub_common.messaging.bus import Record
from engagehub_common.service import Runtime, create_service_app, get_runtime
from engagehub_common.topics import ANALYTICS, CHAT_MESSAGES, ESCALATIONS, SESSION_EVENTS
from fastapi import APIRouter, Depends, FastAPI, Query
from sqlalchemy import BigInteger, Date, Float, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

router = APIRouter()
GROUP = "analytics-service"


class Base(DeclarativeBase):
    pass


class ProcessedEvent(Base):
    """Written in the same transaction as the counters, so each event counts exactly once."""

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)


class DailyMetric(Base):
    __tablename__ = "daily_metrics"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    day: Mapped[date] = mapped_column(Date, primary_key=True)
    metric: Mapped[str] = mapped_column(String(64), primary_key=True)
    dimension: Mapped[str] = mapped_column(String(128), primary_key=True, default="")
    count: Mapped[int] = mapped_column(BigInteger, default=0)
    total: Mapped[float] = mapped_column(Float, default=0.0)


def metrics_for(event: EventEnvelope) -> list[tuple[str, str, float]]:
    """Map an event to (metric, dimension, value)."""
    payload = event.payload
    match event.event_type:
        case EventType.CONVERSATION_STARTED:
            return [("conversations_started", "", 1)]
        case EventType.CONVERSATION_RESOLVED:
            return [("conversations_resolved", "", 1)]
        case EventType.CUSTOMER_MESSAGE | EventType.AGENT_MESSAGE | EventType.HUMAN_MESSAGE | EventType.SYSTEM_MESSAGE:
            return [("messages", event.event_type.value, 1)]
        case EventType.AGENT_ROUTED:
            out = [("intents", payload.get("intent", "unknown"), 1)]
            if not payload.get("escalated"):
                out.append(("routed_to_agent", payload.get("agent", "unknown"), 1))
            return out
        case EventType.HANDOFF_REQUESTED:
            return [("escalations", payload.get("reason", "unknown").split(":")[0], 1)]
        case EventType.AGENT_ASSIGNED:
            return [
                ("human_assignments", payload.get("agent_name", "unknown"), 1),
                ("human_wait_seconds", "", float(payload.get("wait_seconds", 0))),
            ]
        case EventType.RATING_SUBMITTED:
            return [("ratings", "", float(payload.get("rating", 0)))]
        case EventType.SESSION_MIGRATED:
            return [("session_migrations", "", 1)]
        case EventType.SESSION_REPLAYED:
            return [("messages_replayed", payload.get("source", "unknown"), float(payload.get("count", 0)))]
    return []


class AnalyticsConsumer:
    def __init__(self, rt: Runtime) -> None:
        self.rt = rt

    async def handle_event(self, event: EventEnvelope, _record: Record) -> None:
        metrics = metrics_for(event)
        if not metrics:
            return
        day = event.timestamp.date()
        async with self.rt.db.session() as session:
            await self.rt.db.lock_tenant(session, "analytics", event.tenant_id)
            if not await self.rt.db.insert_ignore(session, ProcessedEvent, {"event_id": event.event_id}):
                return
            for metric, dimension, value in metrics:
                key = {"tenant_id": event.tenant_id, "day": day, "metric": metric, "dimension": dimension}
                row = await session.get(DailyMetric, key)
                if row is None:
                    row = DailyMetric(**key, count=0, total=0.0)
                    session.add(row)
                row.count += 1
                row.total += value


@router.get("/analytics/overview")
async def overview(
    days: int = Query(default=30, ge=1, le=365),
    principal: Principal = Depends(require_roles(ROLE_ADMIN)),
    rt: Runtime = Depends(get_runtime),
) -> dict:
    """The tenant comes from the token: an admin can only ever query their own business."""
    since = date.fromordinal(utcnow().date().toordinal() - days + 1)
    async with rt.db.session() as session:
        rows = (
            await session.scalars(
                select(DailyMetric).where(DailyMetric.tenant_id == principal.tenant_id, DailyMetric.day >= since)
            )
        ).all()

    def total(metric: str) -> float:
        return sum(r.total for r in rows if r.metric == metric)

    def count(metric: str) -> int:
        return sum(r.count for r in rows if r.metric == metric)

    def breakdown(metric: str) -> dict[str, float]:
        out: dict[str, float] = {}
        for r in rows:
            if r.metric == metric:
                out[r.dimension] = out.get(r.dimension, 0) + r.total
        return dict(sorted(out.items(), key=lambda kv: kv[1], reverse=True))

    daily: dict[str, int] = {}
    for r in rows:
        if r.metric == "messages":
            daily[r.day.isoformat()] = daily.get(r.day.isoformat(), 0) + r.count
    started = int(total("conversations_started"))
    escalated = int(total("escalations"))
    assignments = count("human_wait_seconds")
    ratings = count("ratings")
    return {
        "tenant_id": principal.tenant_id,
        "days": days,
        "conversations_started": started,
        "conversations_resolved": int(total("conversations_resolved")),
        "messages_total": int(total("messages")),
        "messages_by_type": breakdown("messages"),
        "escalations": escalated,
        "escalation_rate": round(escalated / started, 3) if started else 0.0,
        "escalations_by_reason": breakdown("escalations"),
        "intents": breakdown("intents"),
        "routed_to_agent": breakdown("routed_to_agent"),
        "human_assignments": breakdown("human_assignments"),
        "average_human_wait_seconds": round(total("human_wait_seconds") / assignments, 1) if assignments else 0.0,
        "ratings_count": ratings,
        "average_rating": round(total("ratings") / ratings, 2) if ratings else None,
        "session_migrations": int(total("session_migrations")),
        "messages_replayed": int(total("messages_replayed")),
        "messages_per_day": dict(sorted(daily.items())),
    }


def create_app(runtime: Runtime | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="analytics-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        # ProcessedEvent gives transactional exactly-once counting, so Redis dedupe is not used.
        rt.add_consumer(
            [ANALYTICS, CHAT_MESSAGES, SESSION_EVENTS, ESCALATIONS],
            GROUP,
            AnalyticsConsumer(rt).handle_event,
            dedupe=False,
        )

    app = create_service_app("EngageHub Analytics Service", settings, runtime=runtime, kafka=True, db=True, setup=setup)
    app.include_router(router)
    return app
