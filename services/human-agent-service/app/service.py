"""The human-agent queue: handoff intake, offers, accept/reject, transfer and resolve."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from engagehub_common.auth import Principal
from engagehub_common.events import EventEnvelope, EventType, derive_event_id, sender, utcnow
from engagehub_common.messaging.bus import Record
from engagehub_common.metrics import HUMAN_QUEUE_DEPTH
from engagehub_common.service import Runtime
from engagehub_common.session.log import ConversationLog
from engagehub_common.session.store import (
    WORKFLOW_HUMAN,
    WORKFLOW_RESOLVED,
    WORKFLOW_VIRTUAL,
    WORKFLOW_WAITING_HUMAN,
    SessionStore,
)
from engagehub_common.topics import AGENT_EVENTS, ANALYTICS, ESCALATIONS, SESSION_EVENTS
from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.assignment import estimate_wait_seconds, pick_agent
from app.models import (
    OPEN_STATES,
    QUEUE_ASSIGNED,
    QUEUE_ESCALATED,
    QUEUE_OFFERED,
    QUEUE_RELEASED,
    QUEUE_RESOLVED,
    QUEUE_WAITING,
    STATUS_OFFLINE,
    HumanAgent,
    QueueEntry,
)

log = logging.getLogger(__name__)


def _aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


class QueueService:
    def __init__(self, runtime: Runtime, store: SessionStore, conversation_log: ConversationLog) -> None:
        self.rt = runtime
        self.db = runtime.db
        self.store = store
        self.log = conversation_log
        self.settings = runtime.settings

    # Handoff intake from Kafka
    async def handle_event(self, event: EventEnvelope, _record: Record) -> None:
        if event.event_type != EventType.HANDOFF_REQUESTED:
            return
        payload = event.payload
        async with self.db.session() as session:
            await self.db.lock_tenant(session, "queue", event.tenant_id)
            created = await self.db.insert_ignore(
                session,
                QueueEntry,
                {
                    "id": event.event_id,
                    "tenant_id": event.tenant_id,
                    "conversation_id": event.conversation_id,
                    "customer_id": event.customer_id,
                    "source_event_id": event.event_id,
                    "status": QUEUE_WAITING,
                    "priority": int(payload.get("priority") or 1),
                    "reason": payload.get("reason", ""),
                    "intent": payload.get("intent", "unknown"),
                    "language": payload.get("language", "en"),
                    "sentiment": float(payload.get("sentiment") or 0.0),
                    "tier": payload.get("tier", "standard"),
                    "summary": payload.get("summary", ""),
                    "rejected_by": [],
                    "enqueued_at": utcnow(),
                    "assignment_count": 0,
                },
            )
            if not created:
                return
            await self._dispatch(session, event.tenant_id)
            position, wait = await self._queue_position(session, event.tenant_id, event.event_id)
        await self._announce_wait(event, position, wait)

    async def _announce_wait(self, event: EventEnvelope, position: int, wait_seconds: int) -> None:
        minutes = max(1, round(wait_seconds / 60))
        await self.log.append(
            tenant_id=event.tenant_id,
            conversation_id=event.conversation_id,
            customer_id=event.customer_id,
            event_type=EventType.SYSTEM_MESSAGE,
            payload={
                "text": f"You're number {position} in line. Estimated wait: about {minutes} minute(s).",
                "sender": sender("system", "queue", "Support Queue"),
                "metadata": {"queue_position": position, "estimated_wait_seconds": wait_seconds},
            },
            idempotency_key=f"queue-wait:{event.event_id}",
            correlation_id=event.correlation_id,
        )

    # Queries and shared helpers
    async def _agents(self, session: AsyncSession, tenant_id: str) -> list[HumanAgent]:
        return list((await session.scalars(select(HumanAgent).where(HumanAgent.tenant_id == tenant_id))).all())

    async def _workload(self, session: AsyncSession, tenant_id: str) -> dict[str, int]:
        workload: dict[str, int] = {}
        rows = await session.execute(
            select(QueueEntry.assigned_to, func.count())
            .where(
                QueueEntry.tenant_id == tenant_id,
                QueueEntry.status.in_((QUEUE_ASSIGNED, QUEUE_ESCALATED)),
                QueueEntry.assigned_to.is_not(None),
            )
            .group_by(QueueEntry.assigned_to)
        )
        for agent_id, count in rows:
            workload[agent_id] = count
        offers = await session.execute(
            select(QueueEntry.offered_to, func.count())
            .where(QueueEntry.tenant_id == tenant_id, QueueEntry.status == QUEUE_OFFERED)
            .group_by(QueueEntry.offered_to)
        )
        for agent_id, count in offers:
            workload[agent_id] = workload.get(agent_id, 0) + count
        return workload

    async def _waiting(self, session: AsyncSession, tenant_id: str) -> list[QueueEntry]:
        rows = await session.scalars(
            select(QueueEntry)
            .where(QueueEntry.tenant_id == tenant_id, QueueEntry.status == QUEUE_WAITING)
            .order_by(QueueEntry.priority.desc(), QueueEntry.enqueued_at)
        )
        return list(rows.all())

    async def _queue_position(self, session, tenant_id: str, entry_id: str) -> tuple[int, int]:
        waiting = await self._waiting(session, tenant_id)
        ids = [e.id for e in waiting]
        position = ids.index(entry_id) + 1 if entry_id in ids else 1
        agents = await self._agents(session, tenant_id)
        return position, estimate_wait_seconds(position, agents, self.settings.average_handle_seconds)

    async def _expire_offers(self, session, tenant_id: str) -> None:
        cutoff = utcnow() - timedelta(seconds=self.settings.offer_timeout_seconds)
        rows = await session.scalars(
            select(QueueEntry).where(QueueEntry.tenant_id == tenant_id, QueueEntry.status == QUEUE_OFFERED)
        )
        for entry in rows.all():
            if _aware(entry.offered_at) and _aware(entry.offered_at) < cutoff:
                # Do not re-offer straight back to the agent who ignored it.
                entry.rejected_by = [*(entry.rejected_by or []), entry.offered_to]
                entry.status, entry.offered_to, entry.offered_at = QUEUE_WAITING, None, None

    async def _dispatch(self, session: AsyncSession, tenant_id: str) -> None:
        """Offer waiting conversations to the best agents, highest priority first."""
        await self._expire_offers(session, tenant_id)
        await session.flush()
        waiting = await self._waiting(session, tenant_id)
        agents = await self._agents(session, tenant_id)
        workload = await self._workload(session, tenant_id)
        for entry in waiting:
            agent = pick_agent(entry, agents, workload)
            if agent is None:
                continue
            entry.status, entry.offered_to, entry.offered_at = QUEUE_OFFERED, agent.id, utcnow()
            workload[agent.id] = workload.get(agent.id, 0) + 1
        HUMAN_QUEUE_DEPTH.labels(tenant_id).set(sum(1 for e in waiting if e.status == QUEUE_WAITING))

    async def _open_entry(self, session, tenant_id: str, conversation_id: str) -> QueueEntry:
        entry = await session.scalar(
            select(QueueEntry)
            .where(
                QueueEntry.tenant_id == tenant_id,
                QueueEntry.conversation_id == conversation_id,
                QueueEntry.status.in_(OPEN_STATES),
            )
            .order_by(QueueEntry.enqueued_at.desc())
            .with_for_update()
        )
        if entry is None:
            raise HTTPException(404, "conversation is not in the queue")
        return entry

    # Agents
    async def get_or_create_agent(self, principal: Principal) -> HumanAgent:
        async with self.db.session() as session:
            agent = await session.get(HumanAgent, principal.subject)
            if agent is None:
                agent = HumanAgent(
                    id=principal.subject,
                    tenant_id=principal.tenant_id,
                    name=principal.name or principal.subject,
                    status=STATUS_OFFLINE,
                    languages=["en"],
                    skills=[],
                )
                session.add(agent)
            return agent

    async def set_status(self, principal: Principal, status: str) -> HumanAgent:
        await self.get_or_create_agent(principal)
        async with self.db.session() as session:
            await self.db.lock_tenant(session, "queue", principal.tenant_id)
            agent = await session.get(HumanAgent, principal.subject)
            agent.status = status
            await session.flush()
            if status == STATUS_OFFLINE:
                await self._return_offers(session, agent)
            await self._dispatch(session, principal.tenant_id)
        await self.rt.publish_audit(principal, "agent.status_changed", resource=principal.subject, status=status)
        return agent

    async def _return_offers(self, session, agent: HumanAgent) -> None:
        rows = await session.scalars(
            select(QueueEntry).where(
                QueueEntry.tenant_id == agent.tenant_id,
                QueueEntry.status == QUEUE_OFFERED,
                QueueEntry.offered_to == agent.id,
            )
        )
        for entry in rows.all():
            entry.status, entry.offered_to, entry.offered_at = QUEUE_WAITING, None, None

    async def list_agents(self, tenant_id: str) -> list[dict]:
        async with self.db.session() as session:
            agents = await self._agents(session, tenant_id)
            workload = await self._workload(session, tenant_id)
        return [
            {
                "id": a.id,
                "name": a.name,
                "status": a.status,
                "languages": a.languages,
                "skills": a.skills,
                "max_concurrent": a.max_concurrent,
                "active_conversations": workload.get(a.id, 0),
            }
            for a in agents
        ]

    async def view_queue(self, principal: Principal) -> dict:
        tenant_id = principal.tenant_id
        async with self.db.session() as session:
            await self.db.lock_tenant(session, "queue", tenant_id)
            await self._dispatch(session, tenant_id)
            await session.flush()
            waiting = await self._waiting(session, tenant_id)
            agents = await self._agents(session, tenant_id)
            mine = await session.scalars(
                select(QueueEntry)
                .where(
                    QueueEntry.tenant_id == tenant_id,
                    QueueEntry.status.in_((QUEUE_ASSIGNED, QUEUE_ESCALATED)),
                    QueueEntry.assigned_to == principal.subject,
                )
                .order_by(QueueEntry.assigned_at.desc())
            )
            offers = await session.scalars(
                select(QueueEntry).where(
                    QueueEntry.tenant_id == tenant_id,
                    QueueEntry.status == QUEUE_OFFERED,
                    QueueEntry.offered_to == principal.subject,
                )
            )
            return {
                "waiting": [
                    {
                        **_entry_dict(e),
                        "position": index + 1,
                        "estimated_wait_seconds": estimate_wait_seconds(
                            index + 1, agents, self.settings.average_handle_seconds
                        ),
                    }
                    for index, e in enumerate(waiting)
                ],
                "offers": [_entry_dict(e) for e in offers.all()],
                "assigned": [_entry_dict(e) for e in mine.all()],
            }

    # Transitions. Hot state and events are written inside the transaction, so a broker or
    # Redis failure rolls back the database change and the operation can simply be retried.
    async def accept(self, principal: Principal, conversation_id: str, correlation_id: str) -> dict:
        tenant_id = principal.tenant_id
        async with self.db.session() as session:
            await self.db.lock_tenant(session, "queue", tenant_id)
            entry = await self._open_entry(session, tenant_id, conversation_id)
            if entry.status not in (QUEUE_WAITING, QUEUE_OFFERED):
                raise HTTPException(409, "conversation was already taken")
            if entry.status == QUEUE_OFFERED and entry.offered_to != principal.subject:
                raise HTTPException(409, "conversation is offered to another agent")
            agent = await session.get(HumanAgent, principal.subject)
            if agent is None or agent.tenant_id != tenant_id:
                raise HTTPException(403, "agent profile not found")
            workload = await self._workload(session, tenant_id)
            own_offer = 1 if entry.offered_to == agent.id else 0
            if workload.get(agent.id, 0) - own_offer >= agent.max_concurrent:
                raise HTTPException(409, "agent is at capacity")
            now = utcnow()
            entry.status, entry.assigned_to, entry.assigned_at = QUEUE_ASSIGNED, agent.id, now
            entry.offered_to, entry.offered_at = None, None
            entry.assignment_count += 1
            agent.last_assigned_at = now
            await self._on_assigned(entry, agent, correlation_id)
            result = _entry_dict(entry)
        await self.rt.publish_audit(
            principal, "conversation.accepted", resource=conversation_id, conversation_id=conversation_id
        )
        return result

    async def _on_assigned(self, entry: QueueEntry, agent: HumanAgent, correlation_id: str) -> None:
        key = f"{entry.id}:assign:{entry.assignment_count}"
        await self.store.update(
            entry.tenant_id,
            entry.conversation_id,
            assigned_agent_type="human",
            assigned_agent_id=agent.id,
            assigned_agent_name=agent.name,
            workflow_state=WORKFLOW_HUMAN,
        )
        await self.log.append(
            tenant_id=entry.tenant_id,
            conversation_id=entry.conversation_id,
            customer_id=entry.customer_id,
            event_type=EventType.SYSTEM_MESSAGE,
            payload={
                "text": f"{agent.name} has joined the conversation.",
                "sender": sender("system", "queue", "Support Queue"),
            },
            idempotency_key=f"joined:{key}",
            correlation_id=correlation_id,
        )
        event = self._event(
            entry,
            EventType.AGENT_ASSIGNED,
            key,
            correlation_id,
            {
                "agent_id": agent.id,
                "agent_name": agent.name,
                "priority": entry.priority,
                "reason": entry.reason,
                "wait_seconds": max(0, int((utcnow() - _aware(entry.enqueued_at)).total_seconds())),
            },
        )
        await self.rt.producer.publish(AGENT_EVENTS, event)
        await self.rt.producer.publish(
            ANALYTICS, event.model_copy(update={"event_id": derive_event_id(event.event_id, "analytics")})
        )
        await self.rt.publish_notification(
            tenant_id=entry.tenant_id,
            recipient_id=entry.customer_id,
            conversation_id=entry.conversation_id,
            customer_id=entry.customer_id,
            kind="agent_assigned",
            title="An agent has joined",
            body=f"{agent.name} is now helping you.",
            event_id=derive_event_id(key, "notification"),
            correlation_id=correlation_id,
        )

    def _event(self, entry: QueueEntry, event_type: EventType, key: str, correlation_id, payload) -> EventEnvelope:
        return EventEnvelope(
            event_id=derive_event_id(key, event_type.value),
            event_type=event_type,
            conversation_id=entry.conversation_id,
            tenant_id=entry.tenant_id,
            customer_id=entry.customer_id,
            correlation_id=correlation_id,
            producer_service=self.settings.service_name,
            payload=payload,
        )

    async def reject(self, principal: Principal, conversation_id: str) -> dict:
        async with self.db.session() as session:
            await self.db.lock_tenant(session, "queue", principal.tenant_id)
            entry = await self._open_entry(session, principal.tenant_id, conversation_id)
            if entry.status != QUEUE_OFFERED or entry.offered_to != principal.subject:
                raise HTTPException(409, "conversation is not offered to you")
            entry.rejected_by = [*(entry.rejected_by or []), principal.subject]
            entry.status, entry.offered_to, entry.offered_at = QUEUE_WAITING, None, None
            await session.flush()
            await self._dispatch(session, principal.tenant_id)
            result = _entry_dict(entry)
        await self.rt.publish_audit(
            principal, "conversation.rejected", resource=conversation_id, conversation_id=conversation_id
        )
        return result

    async def _assigned_entry(self, session, principal: Principal, conversation_id: str) -> QueueEntry:
        entry = await self._open_entry(session, principal.tenant_id, conversation_id)
        if entry.assigned_to != principal.subject:
            raise HTTPException(403, "conversation is not assigned to you")
        return entry

    async def transfer(
        self, principal: Principal, conversation_id: str, target_agent_id: str | None, correlation_id: str
    ) -> dict:
        tenant_id = principal.tenant_id
        async with self.db.session() as session:
            await self.db.lock_tenant(session, "queue", tenant_id)
            entry = await self._assigned_entry(session, principal, conversation_id)
            entry.assignment_count += 1
            key = f"{entry.id}:transfer:{entry.assignment_count}"
            if target_agent_id is None:
                await self._release_to_virtual(entry, key, correlation_id)
            else:
                target = await session.get(HumanAgent, target_agent_id)
                if target is None or target.tenant_id != tenant_id or target.id == principal.subject:
                    raise HTTPException(404, "target agent not found")
                await self._transfer_to_human(entry, target, key, correlation_id)
            result = _entry_dict(entry)
        await self.rt.publish_audit(
            principal,
            "conversation.transferred",
            resource=conversation_id,
            conversation_id=conversation_id,
            target=target_agent_id or "virtual",
        )
        return result

    async def _release_to_virtual(self, entry, key, correlation_id) -> None:
        entry.status, entry.assigned_to = QUEUE_RELEASED, None
        await self.store.update(
            entry.tenant_id,
            entry.conversation_id,
            assigned_agent_type="virtual",
            assigned_agent_id="",
            assigned_agent_name="",
            workflow_state=WORKFLOW_VIRTUAL,
            negative_count=0,
            uncertain_count=0,
        )
        await self.rt.producer.publish(
            AGENT_EVENTS,
            self._event(entry, EventType.AGENT_RELEASED, key, correlation_id, {"workflow_state": WORKFLOW_VIRTUAL}),
        )
        await self._system_message(entry, "You've been transferred back to our virtual assistant.", key, correlation_id)

    async def _transfer_to_human(self, entry, target: HumanAgent, key, correlation_id) -> None:
        entry.status, entry.assigned_to = QUEUE_OFFERED, None
        entry.offered_to, entry.offered_at = target.id, utcnow()
        await self.store.update(
            entry.tenant_id,
            entry.conversation_id,
            assigned_agent_type="virtual",
            assigned_agent_id="",
            assigned_agent_name="",
            workflow_state=WORKFLOW_WAITING_HUMAN,
        )
        await self.rt.producer.publish(
            AGENT_EVENTS,
            self._event(
                entry, EventType.AGENT_TRANSFERRED, key, correlation_id,
                {"target_agent_id": target.id, "target_agent_name": target.name},
            ),
        )  # fmt: skip
        await self._system_message(entry, f"You're being transferred to {target.name}.", key, correlation_id)

    async def _system_message(self, entry, text: str, key: str, correlation_id: str) -> None:
        await self.log.append(
            tenant_id=entry.tenant_id,
            conversation_id=entry.conversation_id,
            customer_id=entry.customer_id,
            event_type=EventType.SYSTEM_MESSAGE,
            payload={"text": text, "sender": sender("system", "queue", "Support Queue")},
            idempotency_key=f"system:{key}",
            correlation_id=correlation_id,
        )

    async def resolve(self, principal: Principal, conversation_id: str, correlation_id: str) -> dict:
        async with self.db.session() as session:
            await self.db.lock_tenant(session, "queue", principal.tenant_id)
            entry = await self._assigned_entry(session, principal, conversation_id)
            key = f"{entry.id}:resolve"
            entry.status, entry.resolved_at = QUEUE_RESOLVED, utcnow()
            await self._system_message(
                entry, "This conversation has been resolved. Thanks for contacting us!", key, correlation_id
            )
            await self.store.update(entry.tenant_id, conversation_id, workflow_state=WORKFLOW_RESOLVED)
            event = self._event(
                entry, EventType.CONVERSATION_RESOLVED, key, correlation_id,
                {"resolved_by": principal.subject, "agent_name": principal.name},
            )  # fmt: skip
            await self.rt.producer.publish(SESSION_EVENTS, event)
            await self.rt.publish_notification(
                tenant_id=entry.tenant_id,
                recipient_id=entry.customer_id,
                conversation_id=conversation_id,
                customer_id=entry.customer_id,
                kind="conversation_resolved",
                title="Your conversation was resolved",
                body="Please rate your support experience.",
                event_id=derive_event_id(key, "notification"),
                correlation_id=correlation_id,
            )
            await session.flush()
            await self._dispatch(session, principal.tenant_id)
            result = _entry_dict(entry)
        await self.rt.publish_audit(
            principal, "conversation.resolved", resource=conversation_id, conversation_id=conversation_id
        )
        return result

    async def escalate(self, principal: Principal, conversation_id: str, reason: str, correlation_id: str) -> dict:
        async with self.db.session() as session:
            entry = await self._assigned_entry(session, principal, conversation_id)
            entry.status = QUEUE_ESCALATED
            entry.priority += 1
            entry.reason = reason or entry.reason
            event = self._event(
                entry, EventType.CONVERSATION_ESCALATED, f"{entry.id}:escalate:{entry.priority}", correlation_id,
                {"escalated_by": principal.subject, "reason": reason},
            )  # fmt: skip
            await self.rt.producer.publish(ESCALATIONS, event)
            result = _entry_dict(entry)
        await self.rt.publish_audit(
            principal,
            "conversation.escalated",
            resource=conversation_id,
            conversation_id=conversation_id,
            reason=reason,
        )
        return result


def _entry_dict(e: QueueEntry) -> dict:
    return {
        "conversation_id": e.conversation_id,
        "customer_id": e.customer_id,
        "status": e.status,
        "priority": e.priority,
        "reason": e.reason,
        "intent": e.intent,
        "language": e.language,
        "sentiment": e.sentiment,
        "tier": e.tier,
        "summary": e.summary,
        "offered_to": e.offered_to,
        "assigned_to": e.assigned_to,
        "enqueued_at": e.enqueued_at.isoformat() if e.enqueued_at else None,
        "assigned_at": e.assigned_at.isoformat() if e.assigned_at else None,
    }
