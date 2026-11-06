"""A Kafka worker hosting one virtual agent, plus the supervisor's handoff workflow."""

from __future__ import annotations

import asyncio
import logging

from engagehub_common.events import EventEnvelope, EventType, derive_event_id, sender
from engagehub_common.llm import LLMProvider
from engagehub_common.messaging.bus import EventProducer, Record
from engagehub_common.service import Runtime
from engagehub_common.session.history import HistoryClient, HistoryUnavailable
from engagehub_common.session.log import ConversationLog
from engagehub_common.session.store import WORKFLOW_VIRTUAL, WORKFLOW_WAITING_HUMAN, SessionStore
from engagehub_common.topics import ESCALATIONS

from app.agents import AgentReply, AgentTask, VirtualAgent
from app.clients import DependencyUnavailable

log = logging.getLogger(__name__)


class AgentWorker:
    def __init__(
        self,
        agent: VirtualAgent,
        runtime: Runtime,
        store: SessionStore,
        producer: EventProducer,
        history: HistoryClient,
        llm: LLMProvider,
    ) -> None:
        self.agent = agent
        self.runtime = runtime
        self.store = store
        self.producer = producer
        self.history = history
        self.llm = llm
        self.service = runtime.settings.service_name
        self.log = ConversationLog(store, producer, self.service, restore=history.restore_session)

    async def handle_task(self, event: EventEnvelope, _record: Record) -> None:
        if event.event_type != EventType.AGENT_TASK:
            return
        state = await self.store.get(event.tenant_id, event.conversation_id)
        if state is not None and state.workflow_state != WORKFLOW_VIRTUAL:
            log.info("skipping stale task: conversation has moved on")
            return
        task = AgentTask(
            tenant_id=event.tenant_id,
            customer_id=event.customer_id,
            text=event.payload["text"],
            source_message_id=event.payload["source_message_id"],
            language=event.payload.get("language", "en"),
        )
        try:
            reply = await self.agent.respond(task)
        except DependencyUnavailable as exc:
            log.warning("agent dependency unavailable", extra={"error": str(exc)})
            reply = AgentReply(
                "I'm having trouble reaching our systems right now, so I'm connecting you with a person.",
                escalate_reason="dependency_unavailable",
            )
        await self._post_reply(event, reply)

    async def _post_reply(self, event: EventEnvelope, reply: AgentReply) -> None:
        # The key is derived from the source message, so a redelivered task reuses the same reply.
        await self.log.append(
            tenant_id=event.tenant_id,
            conversation_id=event.conversation_id,
            customer_id=event.customer_id,
            event_type=EventType.AGENT_MESSAGE,
            payload={
                "text": reply.text,
                "sender": sender("virtual_agent", self.agent.agent_type, self.agent.display_name),
                "metadata": reply.metadata,
                "source_message_id": event.payload["source_message_id"],
            },
            idempotency_key=f"reply:{event.payload['source_message_id']}:{self.agent.agent_type}",
            correlation_id=event.correlation_id,
        )
        if reply.notification:
            await self.runtime.publish_notification(
                tenant_id=event.tenant_id,
                recipient_id=event.customer_id,
                conversation_id=event.conversation_id,
                customer_id=event.customer_id,
                event_id=derive_event_id(event.event_id, "notification"),
                correlation_id=event.correlation_id,
                **reply.notification,
            )
        if reply.escalate_reason:
            await self.producer.publish(
                ESCALATIONS,
                EventEnvelope(
                    event_id=derive_event_id(event.event_id, "agent-escalation"),
                    event_type=EventType.ESCALATION_REQUESTED,
                    conversation_id=event.conversation_id,
                    tenant_id=event.tenant_id,
                    customer_id=event.customer_id,
                    correlation_id=event.correlation_id,
                    producer_service=self.service,
                    payload={
                        **{k: event.payload.get(k) for k in ("intent", "sentiment", "language", "tier", "priority")},
                        "reason": reply.escalate_reason,
                        "agent": self.agent.agent_type,
                    },
                ),
            )

    async def handle_escalation(self, event: EventEnvelope, _record: Record) -> None:
        """Supervisor: summarise the conversation and hand it to the human queue."""
        if event.event_type != EventType.ESCALATION_REQUESTED:
            return
        state = await self.store.get(event.tenant_id, event.conversation_id)
        if state is not None and state.workflow_state != WORKFLOW_VIRTUAL:
            return  # A handoff already happened for an earlier escalation.
        summary = await self._summarize(event)
        await self.store.update(event.tenant_id, event.conversation_id, workflow_state=WORKFLOW_WAITING_HUMAN)
        await self.log.append(
            tenant_id=event.tenant_id,
            conversation_id=event.conversation_id,
            customer_id=event.customer_id,
            event_type=EventType.SYSTEM_MESSAGE,
            payload={
                "text": "I'm connecting you with a human support agent who will have the full context.",
                "sender": sender("virtual_agent", "supervisor", "Supervisor"),
            },
            idempotency_key=f"handoff:{event.event_id}",
            correlation_id=event.correlation_id,
        )
        await self.producer.publish(
            ESCALATIONS,
            EventEnvelope(
                event_id=derive_event_id(event.event_id, "handoff"),
                event_type=EventType.HANDOFF_REQUESTED,
                conversation_id=event.conversation_id,
                tenant_id=event.tenant_id,
                customer_id=event.customer_id,
                correlation_id=event.correlation_id,
                producer_service=self.service,
                payload={
                    "summary": summary,
                    "reason": event.payload.get("reason", "unspecified"),
                    "priority": event.payload.get("priority") or 1,
                    "intent": event.payload.get("intent") or "unknown",
                    "sentiment": event.payload.get("sentiment") or 0.0,
                    "language": event.payload.get("language") or (state.language if state else "en"),
                    "tier": event.payload.get("tier") or "standard",
                },
            ),
        )

    async def _summarize(self, event: EventEnvelope) -> str:
        try:
            messages = await self.history.messages_after(event.tenant_id, event.conversation_id, 0)
        except HistoryUnavailable:
            # Do not block the handoff on the summary: use what is still in the Redis buffer.
            log.warning("history unavailable; summarising from the hot buffer")
            raw = await self.store.hot_log_after(event.tenant_id, event.conversation_id, 0)
            messages = [EventEnvelope.model_validate_json(m) for m in raw]
        transcript = [
            {"role": m.payload.get("sender", {}).get("type", "system"), "text": m.payload.get("text", "")}
            for m in messages
        ]
        return await self.llm.summarize(transcript)


async def heartbeat_loop(store: SessionStore, agent_type: str, instance: str, interval: float) -> None:
    """Publish liveness so the orchestrator can route around an agent with no workers."""
    while True:
        try:
            await store.worker_heartbeat(agent_type, instance)
        except Exception as exc:
            log.warning("heartbeat failed", extra={"error": repr(exc)})
        await asyncio.sleep(interval)
