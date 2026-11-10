"""Consumes customer messages, classifies them and routes to a virtual agent or a human."""

from __future__ import annotations

import logging

import httpx
from engagehub_common.auth import service_token
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType, derive_event_id
from engagehub_common.llm import Classification, LLMProvider
from engagehub_common.messaging.bus import EventProducer, Record
from engagehub_common.metrics import ESCALATIONS as ESCALATION_METRIC
from engagehub_common.metrics import ROUTING_DECISIONS
from engagehub_common.resilience import CircuitBreaker
from engagehub_common.session.store import WORKFLOW_VIRTUAL, SessionState, SessionStore
from engagehub_common.topics import AGENT_EVENTS, ANALYTICS, ESCALATIONS, agent_task_topic

from app.routing import FRUSTRATED, RoutingDecision, decide

log = logging.getLogger(__name__)


class CustomerTierClient:
    """Reads a customer's tier. An outage degrades to standard handling."""

    def __init__(self, settings: ServiceSettings, http: httpx.AsyncClient | None = None) -> None:
        self.settings = settings
        self.http = http or httpx.AsyncClient(base_url=settings.customer_service_url, timeout=2.0)
        self.breaker = CircuitBreaker("customer-service")

    async def tier(self, tenant_id: str, customer_id: str) -> str | None:
        token = service_token(self.settings.jwt_secret, self.settings.service_name, tenant_id, customer_id)

        async def fetch() -> httpx.Response:
            response = await self.http.get(f"/customers/{customer_id}", headers={"Authorization": f"Bearer {token}"})
            if response.status_code >= 500:
                response.raise_for_status()
            return response

        try:
            response = await self.breaker.call(fetch)
        except Exception as exc:
            log.warning("customer tier unavailable", extra={"error": repr(exc)})
            return None
        return response.json().get("tier", "standard") if response.status_code == 200 else "standard"


class Orchestrator:
    def __init__(
        self,
        settings: ServiceSettings,
        store: SessionStore,
        producer: EventProducer,
        llm: LLMProvider,
        tiers: CustomerTierClient,
    ) -> None:
        self.settings = settings
        self.store = store
        self.producer = producer
        self.llm = llm
        self.tiers = tiers

    async def handle_event(self, event: EventEnvelope, _record: Record) -> None:
        if event.event_type != EventType.CUSTOMER_MESSAGE:
            return
        tenant_id, conversation_id = event.tenant_id, event.conversation_id
        state = await self.store.get(tenant_id, conversation_id)
        if state is None or state.workflow_state != WORKFLOW_VIRTUAL:
            # A human owns the conversation, one is pending, or it has been resolved.
            return

        classification = await self.llm.classify(event.payload["text"])
        tier = await self._customer_tier(state)
        negative = state.negative_count + (1 if classification.sentiment <= FRUSTRATED else 0)
        uncertain = state.uncertain_count + (1 if classification.intent == "unknown" else 0)
        await self.store.update(
            tenant_id,
            conversation_id,
            negative_count=negative,
            uncertain_count=uncertain,
            last_intent=classification.intent,
            language=classification.language,
        )
        decision = decide(
            classification,
            tier=tier,
            negative_count=negative,
            uncertain_count=uncertain,
            live_workers=await self.store.live_workers(self.settings.worker_heartbeat_ttl_seconds),
        )
        await self._publish_decision(event, classification, decision, tier)

    async def _customer_tier(self, state: SessionState) -> str:
        if state.customer_tier:
            return state.customer_tier
        tier = await self.tiers.tier(state.tenant_id, state.customer_id)
        if tier is None:
            return "standard"  # do not cache a guess: look again on the next message
        await self.store.update(state.tenant_id, state.conversation_id, customer_tier=tier)
        return tier

    def _derived(self, source: EventEnvelope, purpose: str, event_type: EventType, payload: dict) -> EventEnvelope:
        return EventEnvelope(
            event_id=derive_event_id(source.event_id, purpose),
            event_type=event_type,
            conversation_id=source.conversation_id,
            tenant_id=source.tenant_id,
            customer_id=source.customer_id,
            message_id=source.message_id if event_type == EventType.AGENT_TASK else None,
            correlation_id=source.correlation_id,
            producer_service=self.settings.service_name,
            payload=payload,
        )

    async def _publish_decision(
        self, event: EventEnvelope, c: Classification, decision: RoutingDecision, tier: str
    ) -> None:
        details = {
            "intent": c.intent,
            "confidence": c.confidence,
            "sentiment": c.sentiment,
            "language": c.language,
            "tier": tier,
            "priority": decision.priority,
            "reason": decision.reason,
            "agent": decision.agent,
            "source_sequence": event.sequence_number,
        }
        if decision.escalate:
            ESCALATION_METRIC.labels(event.tenant_id, decision.reason.split(":")[0]).inc()
            await self.producer.publish(
                ESCALATIONS,
                self._derived(event, "escalate", EventType.ESCALATION_REQUESTED, details),
            )
        else:
            await self.producer.publish(
                agent_task_topic(decision.agent),
                self._derived(
                    event,
                    "task",
                    EventType.AGENT_TASK,
                    {**details, "text": event.payload["text"], "source_message_id": event.message_id},
                ),
            )
        ROUTING_DECISIONS.labels(event.tenant_id, decision.agent, c.intent).inc()
        routed = self._derived(event, "routed", EventType.AGENT_ROUTED, {**details, "escalated": decision.escalate})
        await self.producer.publish(AGENT_EVENTS, routed)
        await self.producer.publish(
            ANALYTICS, routed.model_copy(update={"event_id": derive_event_id(event.event_id, "routed-analytics")})
        )
