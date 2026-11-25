"""The December 1 script is one conversation, answered from order-service through the human queue."""

import importlib
import re
import sys
import time
from pathlib import Path

import engagehub_common.testconf  # noqa: F401
import fakeredis.aioredis
import httpx
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType, sender
from engagehub_common.llm import RuleBasedProvider
from engagehub_common.messaging.bus import Record
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.testing import auth_headers, memory_runtime
from engagehub_common.topics import AGENT_TYPES, CHAT_MESSAGES, ESCALATIONS, agent_task_topic
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parents[1]
PHRASES = (REPO / "demo" / "phrases.txt").read_text().splitlines()
DANA = "cust-acme-dana"
SERVICES = ("order-service", "agent-orchestrator", "virtual-agent-service", "human-agent-service")
ALICE = auth_headers(subject="agent-acme-alice", role="agent", name="Alice Morgan")
RECORD = Record("chat.messages", 0, 0, b"c1", b"")
LIVE_RETURN = re.compile(r"RET-[0-9A-F]{8}")


class RememberedTier:
    async def tier(self, tenant, customer):
        return "standard"


class NoHistory:
    async def messages_after(self, *_args, **_kwargs):
        return []

    async def restore_session(self, *_args, **_kwargs):
        return False


class SpokenSoFar:
    """Customer lines plus the agent replies already published on the chat log."""

    def __init__(self, kafka: InMemoryKafka) -> None:
        self.kafka = kafka
        self.customers: list[EventEnvelope] = []

    def said(self, event: EventEnvelope) -> None:
        self.customers.append(event)

    async def messages_after(self, *_args, **_kwargs) -> list[EventEnvelope]:
        replies = [event for event in self.kafka.events(CHAT_MESSAGES) if event.event_type == EventType.AGENT_MESSAGE]
        spoken: list[EventEnvelope] = []
        for index, customer in enumerate(self.customers):
            spoken.append(customer)
            if index < len(replies):
                spoken.append(replies[index])
        return spoken

    async def restore_session(self, *_args, **_kwargs) -> bool:
        return False


def wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


def load_modules(service: str, *modules: str):
    """Import modules from one service. These services all use the package name ``app``."""
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    for other in SERVICES:
        root = str(REPO / "services" / other)
        while root in sys.path:
            sys.path.remove(root)
    sys.path.insert(0, str(REPO / "services" / service))
    return [importlib.import_module(f"app.{module}") for module in modules]


def customer_message(text: str, event_id: str, seq: int) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.CUSTOMER_MESSAGE,
        conversation_id="c1",
        tenant_id="acme",
        customer_id=DANA,
        message_id=f"m{seq}",
        sequence_number=seq,
        producer_service="chat-service",
        correlation_id="corr-1",
        payload={"text": text, "sender": sender("customer", DANA, "Dana")},
    )


async def test_december_script_reaches_the_queue_with_the_order_and_return(monkeypatch):
    assert PHRASES == [
        "Where is order 1001?",
        "I also want to return it.",
        "This is unacceptable, get me a manager.",
    ]
    monkeypatch.setenv("AGENT_TYPE", "order_tracking,returns_refunds")
    (order_main,) = load_modules("order-service", "main")
    (orchestrator_mod,) = load_modules("agent-orchestrator", "orchestrator")
    va_main, worker_mod, agents_mod, clients_mod = load_modules(
        "virtual-agent-service", "main", "worker", "agents", "clients"
    )

    wire = memory_runtime("virtual-agent-service", db=False)
    wired = va_main.create_app(runtime=wire, history=NoHistory())
    async with wired.router.lifespan_context(wired):
        live = wired.state.workers["order_tracking"].agent.domain
        assert live._fixture_orders is None
        assert wired.state.workers["returns_refunds"].agent.domain is live
    await live.orders.aclose()
    await live.products.aclose()

    order_app = order_main.create_app(runtime=memory_runtime("order-service"))
    kafka = InMemoryKafka()
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = SessionStore(redis)
    await store.create(SessionState(conversation_id="c1", session_id="s", tenant_id="acme", customer_id=DANA))
    for agent in AGENT_TYPES:
        await store.worker_heartbeat(agent, "w1")
    settings = ServiceSettings(service_name="virtual-agent-service", otel_enabled=False)
    runtime = memory_runtime("virtual-agent-service", kafka=kafka, redis=redis, db=False)
    history = SpokenSoFar(kafka)
    supervisor = worker_mod.AgentWorker(
        agents_mod.SupervisorAgent(None),
        runtime,
        store,
        kafka.producer(),
        history,
        RuleBasedProvider(),
    )

    async with order_app.router.lifespan_context(order_app):
        domain = clients_mod.DomainClient(settings, transport=httpx.ASGITransport(app=order_app))
        try:
            listed = await domain.list_orders("acme", DANA)
            assert {order["id"] for order in listed} == {"ORD-1001", "ORD-1002"}

            orchestrator = orchestrator_mod.Orchestrator(
                ServiceSettings(service_name="agent-orchestrator", otel_enabled=False),
                store,
                kafka.producer(),
                RuleBasedProvider(),
                RememberedTier(),
            )
            workers = {
                name: worker_mod.AgentWorker(
                    agents_mod.AGENT_CLASSES[name](domain),
                    runtime,
                    store,
                    kafka.producer(),
                    history,
                    RuleBasedProvider(),
                )
                for name in ("order_tracking", "returns_refunds")
            }

            where = customer_message(PHRASES[0], "e1", 1)
            history.said(where)
            await orchestrator.handle_event(where, RECORD)
            tasks = kafka.events(agent_task_topic("order_tracking"))
            assert len(tasks) == 1
            await workers["order_tracking"].handle_task(tasks[0], RECORD)
            status = kafka.events(CHAT_MESSAGES)[-1].payload
            assert "ORD-1001 is delivered" in status["text"]
            assert "TRK-ACME-1001" in status["text"]
            assert status["metadata"]["order_id"] == "ORD-1001"

            give_back = customer_message(PHRASES[1], "e2", 2)
            history.said(give_back)
            await orchestrator.handle_event(give_back, RECORD)
            returns = kafka.events(agent_task_topic("returns_refunds"))
            assert len(returns) == 1
            await workers["returns_refunds"].handle_task(returns[0], RECORD)
            opened = kafka.events(CHAT_MESSAGES)[-1].payload
            return_id = opened["metadata"]["return_id"]
            assert opened["text"].startswith(f"Return {return_id} is open for order ORD-1001.")
            assert "$189.00" in opened["text"]
            assert opened["metadata"]["order_id"] == "ORD-1001"
            assert LIVE_RETURN.fullmatch(return_id)
            assert return_id != "RET-1001"

            manager = customer_message(PHRASES[2], "e3", 3)
            history.said(manager)
            await orchestrator.handle_event(manager, RECORD)
            escalations = [
                event for event in kafka.events(ESCALATIONS) if event.event_type == EventType.ESCALATION_REQUESTED
            ]
            assert len(escalations) == 1
            await supervisor.handle_escalation(escalations[0], RECORD)
        finally:
            await domain.orders.aclose()
            await domain.products.aclose()

    (human_main,) = load_modules("human-agent-service", "main")
    human = memory_runtime("human-agent-service", kafka=kafka, redis=redis)
    with TestClient(human_main.create_app(runtime=human, history=history)) as client:
        wait_for(lambda: client.get("/queue", headers=ALICE).json()["waiting"])
        entry = client.get("/queue", headers=ALICE).json()["waiting"][0]

    assert entry["conversation_id"] == "c1"
    assert entry["reason"] == "customer_request"
    assert entry["handler"] == "supervisor"
    assert entry["frustrated"] is True
    assert "ORD-1001" in entry["summary"]
    assert return_id in entry["summary"]
    await redis.aclose()
