"""The December 1 manager phrase has to cross every handoff, not stop at the orchestrator."""

import importlib
import sys
import time
from pathlib import Path

import engagehub_common.testconf  # noqa: F401
import fakeredis.aioredis
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType, sender
from engagehub_common.llm import RuleBasedProvider
from engagehub_common.messaging.bus import Record
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.testing import auth_headers, memory_runtime
from engagehub_common.topics import AGENT_TYPES, ESCALATIONS
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parents[1]
PHRASE = (REPO / "demo" / "phrases.txt").read_text().splitlines()[2]
SERVICES = ("agent-orchestrator", "virtual-agent-service", "human-agent-service")
ALICE = auth_headers(subject="agent-acme-alice", role="agent", name="Alice Morgan")
RECORD = Record("chat.messages", 0, 0, b"c1", b"")


class RememberedTier:
    async def tier(self, tenant, customer):
        return "standard"


class ConversationSoFar:
    def __init__(self, messages):
        self.messages = messages

    async def messages_after(self, *_args, **_kwargs):
        return self.messages

    async def restore_session(self, *_args, **_kwargs):
        return False


def load_modules(service: str, *modules: str):
    """Import modules from one service. The three services all use the package name ``app``."""
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    for other in SERVICES:
        root = str(REPO / "services" / other)
        while root in sys.path:
            sys.path.remove(root)
    sys.path.insert(0, str(REPO / "services" / service))
    return [importlib.import_module(f"app.{module}") for module in modules]


def customer_message(text: str) -> EventEnvelope:
    return EventEnvelope(
        event_id="e1",
        event_type=EventType.CUSTOMER_MESSAGE,
        conversation_id="c1",
        tenant_id="acme",
        customer_id="u1",
        message_id="m1",
        sequence_number=1,
        producer_service="chat-service",
        correlation_id="corr-1",
        payload={"text": text, "sender": sender("customer", "u1", "Dana")},
    )


def wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


async def test_manager_phrase_becomes_a_handoff_the_queue_accepts():
    assert PHRASE == "This is unacceptable, get me a manager."
    (orchestrator_mod,) = load_modules("agent-orchestrator", "orchestrator")
    orchestrator_cls = orchestrator_mod.Orchestrator
    worker_mod, agents_mod = load_modules("virtual-agent-service", "worker", "agents")
    agent_worker_cls, supervisor_cls = worker_mod.AgentWorker, agents_mod.SupervisorAgent
    (human_main,) = load_modules("human-agent-service", "main")
    create_human_app = human_main.create_app

    kafka = InMemoryKafka()
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = SessionStore(redis)
    await store.create(SessionState(conversation_id="c1", session_id="s", tenant_id="acme", customer_id="u1"))
    for agent in AGENT_TYPES:
        await store.worker_heartbeat(agent, "w1")

    message = customer_message(PHRASE)
    orchestrator = orchestrator_cls(
        ServiceSettings(service_name="agent-orchestrator", otel_enabled=False),
        store,
        kafka.producer(),
        RuleBasedProvider(),
        RememberedTier(),
    )
    await orchestrator.handle_event(message, RECORD)
    escalations = [event for event in kafka.events(ESCALATIONS) if event.event_type == EventType.ESCALATION_REQUESTED]
    assert len(escalations) == 1
    assert "unacceptable" in escalations[0].payload["summary"].lower()

    worker = agent_worker_cls(
        supervisor_cls(None),
        memory_runtime("virtual-agent-service", kafka=kafka, redis=redis, db=False),
        store,
        kafka.producer(),
        ConversationSoFar([message]),
        RuleBasedProvider(),
    )
    await worker.handle_escalation(escalations[0], RECORD)
    handoffs = [event for event in kafka.events(ESCALATIONS) if event.event_type == EventType.HANDOFF_REQUESTED]
    assert len(handoffs) == 1
    assert handoffs[0].payload["reason"] == "customer_request"
    assert "unacceptable" in handoffs[0].payload["summary"].lower()

    runtime = memory_runtime("human-agent-service", kafka=kafka, redis=redis)
    with TestClient(create_human_app(runtime=runtime, history=ConversationSoFar([message]))) as client:
        wait_for(lambda: client.get("/queue", headers=ALICE).json()["waiting"])
        entry = client.get("/queue", headers=ALICE).json()["waiting"][0]

    assert entry["conversation_id"] == "c1"
    assert entry["reason"] == "customer_request"
    assert "unacceptable" in entry["summary"].lower()
    await redis.aclose()
