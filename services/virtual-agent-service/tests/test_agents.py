import fakeredis.aioredis
import pytest
from app.agents import AGENT_CLASSES, AgentReply, AgentTask
from app.clients import DependencyUnavailable
from app.worker import AgentWorker
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.llm import RuleBasedProvider
from engagehub_common.messaging.bus import Record
from engagehub_common.session.history import HistoryUnavailable
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.testing import memory_runtime
from engagehub_common.topics import AGENT_TYPES, CHAT_MESSAGES, ESCALATIONS, NOTIFICATIONS

ORDER = {
    "id": "ORD-1001", "status": "delivered", "total": 120.0, "payment_status": "paid",
    "tracking_number": "TRK1", "estimated_delivery": "2026-01-01",
}  # fmt: skip
PRODUCT = {"id": "p1", "name": "Trail Tent", "price": 150.0, "description": "A tent", "stock": 3}


class FakeDomain:
    def __init__(self, order=ORDER):
        self.order = order
        self.returns = []
        self.failing = False

    def _check(self):
        if self.failing:
            raise DependencyUnavailable("down")

    async def search_products(self, tenant_id, customer_id, *, query="", max_price=None):
        self._check()
        return [PRODUCT] if query == "tent" else []

    async def recommendations(self, tenant_id, customer_id, *, max_price=None):
        self._check()
        return [PRODUCT]

    async def get_order(self, tenant_id, customer_id, order_id):
        self._check()
        return self.order if self.order and order_id == self.order["id"] else None

    async def list_orders(self, tenant_id, customer_id):
        self._check()
        return [self.order] if self.order else []

    async def create_return(self, tenant_id, customer_id, order_id, reason, key):
        self._check()
        self.returns.append(key)
        return {"id": "RET-1"}


def task(text):
    return AgentTask("acme", "u1", text, "m1")


async def ask(agent_type, text, domain=None):
    return await AGENT_CLASSES[agent_type](domain or FakeDomain()).respond(task(text))


def test_all_eight_agents_exist():
    assert set(AGENT_CLASSES) == set(AGENT_TYPES) and len(AGENT_TYPES) == 8


async def test_product_advisor_searches_then_recommends():
    assert "Trail Tent" in (await ask("product_advisor", "looking for a tent under $200")).text
    assert "popular" in (await ask("product_advisor", "something for the zzzz")).text


async def test_order_tracking_describes_an_order():
    reply = await ask("order_tracking", "where is ord-1001?")
    assert "ORD-1001 is delivered" in reply.text and "TRK1" in reply.text
    assert "couldn't find" in (await ask("order_tracking", "where is ORD-9")).text


async def test_returns_agent_opens_return_and_notifies():
    domain = FakeDomain()
    reply = await ask("returns_refunds", "return ORD-1001", domain)
    assert "RET-1" in reply.text and reply.notification["kind"] == "return_created"
    assert domain.returns == ["m1"]  # the source message ID is the idempotency key


async def test_large_refund_requires_human_approval():
    domain = FakeDomain({**ORDER, "total": 900.0})
    reply = await ask("returns_refunds", "refund ORD-1001", domain)
    assert reply.escalate_reason == "refund_approval_required" and domain.returns == []


async def test_undelivered_order_cannot_be_returned():
    assert (
        "can't be returned"
        in (await ask("returns_refunds", "return ORD-1001", FakeDomain({**ORDER, "status": "shipped"}))).text
    )


async def test_billing_dispute_escalates():
    assert (await ask("billing", "I was charged twice")).escalate_reason == "billing_dispute"
    assert "$120.00" in (await ask("billing", "invoice for ORD-1001")).text


async def test_technical_support_guides_then_escalates():
    assert "pair again" in (await ask("technical_support", "wifi keeps dropping")).text
    assert (await ask("technical_support", "that did not work")).escalate_reason == "troubleshooting_exhausted"


async def test_faq_complaint_and_supervisor_replies():
    assert "1-year" in (await ask("faq", "what's the warranty")).text
    assert (await ask("complaint_management", "terrible")).notification["kind"] == "complaint_logged"
    assert "right specialist" in (await ask("supervisor", "zzz")).text


class FakeHistory:
    def __init__(self):
        self.down = False
        self.messages = []

    async def messages_after(self, *_a, **_k):
        if self.down:
            raise HistoryUnavailable("down")
        return self.messages

    async def restore_session(self, *_):
        return False


@pytest.fixture
async def worker_for():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    runtime = memory_runtime("virtual-agent-service", redis=redis, db=False)
    store = SessionStore(redis)
    await store.create(SessionState(conversation_id="c1", session_id="s", tenant_id="acme", customer_id="u1"))

    def build(agent_type, domain=None):
        agent = AGENT_CLASSES[agent_type](domain or FakeDomain())
        return AgentWorker(agent, runtime, store, runtime.producer, FakeHistory(), RuleBasedProvider())

    build.kafka, build.store = runtime.kafka, store
    yield build
    await redis.aclose()


def task_event(text="where is ORD-1001", event_id="e1", message_id="m1"):
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.AGENT_TASK,
        conversation_id="c1",
        tenant_id="acme",
        customer_id="u1",
        message_id=message_id,
        producer_service="agent-orchestrator",
        correlation_id="corr-9",
        payload={"text": text, "source_message_id": message_id, "language": "en", "priority": 1},
    )


RECORD = Record("t", 0, 0, b"c1", b"")


async def test_reply_is_published_with_sequence_and_correlation(worker_for):
    await worker_for("order_tracking").handle_task(task_event(), RECORD)
    replies = worker_for.kafka.events(CHAT_MESSAGES)
    assert len(replies) == 1
    assert replies[0].sequence_number == 1 and replies[0].correlation_id == "corr-9"
    assert replies[0].payload["sender"]["id"] == "order_tracking"


async def test_redelivered_task_does_not_duplicate_reply_or_notification(worker_for):
    worker = worker_for("returns_refunds")
    event = task_event("return ORD-1001")
    await worker.handle_task(event, RECORD)
    await worker.handle_task(event, RECORD)
    assert len(worker_for.kafka.events(CHAT_MESSAGES)) == 1
    notifications = worker_for.kafka.events(NOTIFICATIONS)
    assert len({n.event_id for n in notifications}) == 1  # identical IDs, so the consumer drops the copy


async def test_dependency_outage_degrades_to_human_handoff(worker_for):
    domain = FakeDomain()
    domain.failing = True
    await worker_for("order_tracking", domain).handle_task(task_event(), RECORD)
    assert "connecting you with a person" in worker_for.kafka.events(CHAT_MESSAGES)[0].payload["text"]
    assert worker_for.kafka.events(ESCALATIONS)[0].payload["reason"] == "dependency_unavailable"


async def test_stale_task_is_skipped_after_human_takeover(worker_for):
    await worker_for.store.update("acme", "c1", workflow_state="human_active")
    await worker_for("order_tracking").handle_task(task_event(), RECORD)
    assert worker_for.kafka.events(CHAT_MESSAGES) == []


def escalation(event_id="esc-1", **payload):
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.ESCALATION_REQUESTED,
        conversation_id="c1",
        tenant_id="acme",
        customer_id="u1",
        producer_service="agent-orchestrator",
        payload={"reason": "customer_request", "priority": 3, "language": "es", **payload},
    )


async def test_supervisor_produces_handoff_with_summary(worker_for):
    worker = worker_for("supervisor")
    await worker.handle_escalation(escalation(), RECORD)
    handoffs = [e for e in worker_for.kafka.events(ESCALATIONS) if e.event_type == EventType.HANDOFF_REQUESTED]
    assert len(handoffs) == 1
    assert handoffs[0].payload["priority"] == 3 and handoffs[0].payload["language"] == "es"
    assert handoffs[0].payload["summary"]
    assert (await worker_for.store.get("acme", "c1")).workflow_state == "waiting_human"


async def test_second_escalation_does_not_create_a_second_handoff(worker_for):
    worker = worker_for("supervisor")
    await worker.handle_escalation(escalation("esc-1"), RECORD)
    await worker.handle_escalation(escalation("esc-2"), RECORD)
    handoffs = [e for e in worker_for.kafka.events(ESCALATIONS) if e.event_type == EventType.HANDOFF_REQUESTED]
    assert len(handoffs) == 1


async def test_handoff_proceeds_when_history_is_down(worker_for):
    worker = worker_for("supervisor")
    worker.history.down = True
    await worker.handle_escalation(escalation(), RECORD)
    assert (await worker_for.store.get("acme", "c1")).workflow_state == "waiting_human"


def test_reply_defaults():
    reply = AgentReply("hi")
    assert reply.escalate_reason is None and reply.notification is None


def test_one_process_can_host_several_agents_with_independent_consumer_groups(monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import create_app

    monkeypatch.setenv("AGENT_TYPE", "faq,supervisor")
    runtime = memory_runtime("virtual-agent-service", db=False)
    app = create_app(runtime=runtime, history=FakeHistory(), domain=FakeDomain())
    with TestClient(app):
        assert set(app.state.workers) == {"faq", "supervisor"}
        assert {r.group for r in runtime.runners} == {"va-faq", "va-supervisor", "va-supervisor-escalations"}


def test_unknown_agent_type_is_refused_at_startup(monkeypatch):
    from app.main import create_app

    monkeypatch.setenv("AGENT_TYPE", "faq,nonsense")
    with pytest.raises(ValueError):
        create_app(runtime=memory_runtime("virtual-agent-service", db=False))
