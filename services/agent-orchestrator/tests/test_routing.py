import fakeredis.aioredis
import pytest
from app.orchestrator import Orchestrator
from app.routing import compute_priority, decide
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType, sender
from engagehub_common.llm import Classification, RuleBasedProvider
from engagehub_common.messaging.bus import Record
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.topics import AGENT_EVENTS, AGENT_TYPES, ANALYTICS, ESCALATIONS, agent_task_topic

ALL_LIVE = dict.fromkeys(AGENT_TYPES, 1)


def classify(**overrides):
    return Classification(**{"intent": "product", "confidence": 0.9, "sentiment": 0.0, **overrides})


def route(c, tier="standard", negative=0, uncertain=0, live=None):
    return decide(c, tier=tier, negative_count=negative, uncertain_count=uncertain, live_workers=live or ALL_LIVE)


@pytest.mark.parametrize(
    "intent,agent",
    [
        ("product", "product_advisor"),
        ("order", "order_tracking"),
        ("returns", "returns_refunds"),
        ("billing", "billing"),
        ("technical", "technical_support"),
        ("faq", "faq"),
        ("complaint", "complaint_management"),
        ("unknown", "supervisor"),
    ],
)
def test_intent_routes_to_the_specialist(intent, agent):
    decision = route(classify(intent=intent))
    assert not decision.escalate and decision.agent == agent


def test_explicit_human_request_escalates():
    decision = route(classify(wants_human=True))
    assert decision.escalate and decision.reason == "customer_request"


def test_sensitive_topic_escalates_with_higher_priority():
    decision = route(classify(sensitive_topic="legal"))
    assert decision.escalate and decision.reason == "sensitive_topic:legal" and decision.priority == 2


def test_first_angry_message_goes_to_complaints_and_second_escalates():
    first = route(classify(sentiment=-0.8), negative=1)
    assert not first.escalate and first.agent == "complaint_management"
    assert route(classify(sentiment=-0.8), negative=2).reason == "repeated_frustration"


def test_frustrated_vip_escalates_immediately_with_top_priority():
    decision = route(classify(sentiment=-0.8), tier="vip", negative=1)
    assert decision.escalate and decision.reason == "vip_frustrated" and decision.priority == 4


def test_repeated_uncertainty_escalates():
    assert route(classify(intent="unknown"), uncertain=1).agent == "supervisor"
    assert route(classify(intent="unknown"), uncertain=2).reason == "uncertainty"


def test_unavailable_virtual_agent_falls_back_to_a_human():
    decision = route(classify(), live={**ALL_LIVE, "product_advisor": 0})
    assert decision.escalate and decision.reason == "agent_unavailable:product_advisor"


def test_priority_combines_signals():
    assert compute_priority("standard", 0, 0, False) == 1
    assert compute_priority("vip", -0.9, 2, True) == 5


class FixedTier:
    def __init__(self, tier="standard"):
        self.tier_value, self.calls = tier, 0

    async def tier(self, tenant, customer):
        self.calls += 1
        return self.tier_value


@pytest.fixture
async def setup():
    kafka = InMemoryKafka()
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = SessionStore(redis)
    await store.create(SessionState(conversation_id="c1", session_id="s", tenant_id="acme", customer_id="u1"))
    for agent in AGENT_TYPES:
        await store.worker_heartbeat(agent, "w1")
    settings = ServiceSettings(service_name="agent-orchestrator", otel_enabled=False)
    tiers = FixedTier()
    orchestrator = Orchestrator(settings, store, kafka.producer(), RuleBasedProvider(), tiers)
    yield orchestrator, kafka, store, tiers
    await redis.aclose()


def message(text, event_id="e1", seq=1):
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.CUSTOMER_MESSAGE,
        conversation_id="c1",
        tenant_id="acme",
        customer_id="u1",
        message_id=f"m{seq}",
        sequence_number=seq,
        producer_service="chat-service",
        correlation_id="corr-1",
        payload={"text": text, "sender": sender("customer", "u1", "D")},
    )


RECORD = Record("chat.messages", 0, 0, b"c1", b"")


async def test_message_becomes_task_for_the_right_agent(setup):
    orchestrator, kafka, _, _ = setup
    await orchestrator.handle_event(message("where is my order ORD-1001?"), RECORD)
    tasks = kafka.events(agent_task_topic("order_tracking"))
    assert len(tasks) == 1
    assert tasks[0].correlation_id == "corr-1" and tasks[0].payload["source_message_id"] == "m1"
    assert len(kafka.events(ANALYTICS)) == 1 and len(kafka.events(AGENT_EVENTS)) == 1


async def test_replayed_input_produces_identical_event_ids(setup):
    orchestrator, kafka, _, _ = setup
    await orchestrator.handle_event(message("recommend a tent"), RECORD)
    await orchestrator.handle_event(message("recommend a tent"), RECORD)
    tasks = kafka.events(agent_task_topic("product_advisor"))
    assert tasks[0].event_id == tasks[1].event_id


async def test_human_request_publishes_escalation(setup):
    orchestrator, kafka, _, _ = setup
    await orchestrator.handle_event(message("I want to talk to a real person"), RECORD)
    escalations = kafka.events(ESCALATIONS)
    assert escalations[0].event_type == EventType.ESCALATION_REQUESTED
    assert escalations[0].payload["reason"] == "customer_request"


@pytest.mark.parametrize("workflow", ["human_active", "waiting_human", "resolved"])
async def test_conversations_not_owned_by_virtual_agents_are_left_alone(setup, workflow):
    orchestrator, kafka, store, _ = setup
    await store.update("acme", "c1", workflow_state=workflow)
    await orchestrator.handle_event(message("where is my order?"), RECORD)
    assert kafka.events(AGENT_EVENTS) == []


async def test_non_customer_messages_are_ignored(setup):
    orchestrator, kafka, _, _ = setup
    reply = message("hello").model_copy(update={"event_type": EventType.AGENT_MESSAGE})
    await orchestrator.handle_event(reply, RECORD)
    assert kafka.events(AGENT_EVENTS) == []


async def test_tier_is_cached_on_the_session(setup):
    orchestrator, _, store, tiers = setup
    await orchestrator.handle_event(message("recommend a tent", "e1", 1), RECORD)
    await orchestrator.handle_event(message("recommend boots", "e2", 2), RECORD)
    assert tiers.calls == 1
    assert (await store.get("acme", "c1")).customer_tier == "standard"
