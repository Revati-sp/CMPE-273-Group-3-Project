"""In-process load test: many customers, replicated virtual-agent workers, shared consumer groups.

This runs the real worker, sequencing (Redis Lua via fakeredis) and consumer-runner code
over the in-memory Kafka with consumer-group partition balancing. It is not a test of
real Kafka or network throughput; scripts/load/ has the load test for the running stack.

    ../../.venv/bin/python -m pytest tests/test_scaling_load.py -s     # prints the report
"""

import asyncio
import random
import statistics
import time
from collections import Counter, defaultdict

import fakeredis.aioredis
import pytest
from app.agents import AGENT_CLASSES
from app.worker import AgentWorker
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.llm import RuleBasedProvider
from engagehub_common.messaging.consumer import ConsumerRunner
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.testing import memory_runtime
from engagehub_common.topics import CHAT_MESSAGES, ESCALATIONS, agent_task_topic
from prometheus_client import REGISTRY
from tests.test_agents import FakeDomain, FakeHistory

CUSTOMERS = 50
MESSAGES_PER_CUSTOMER = 4
REPLICAS = {"product_advisor": 3, "order_tracking": 2, "faq": 2, "supervisor": 2}
QUESTIONS = {
    "product_advisor": "Can you recommend a tent under $200?",
    "order_tracking": "Where is my order ORD-1001?",
    "faq": "What is your warranty policy?",
}


class SlowDomain(FakeDomain):
    """Each backend call takes 10 ms, like a REST call to product or order service."""

    async def search_products(self, *a, **k):
        await asyncio.sleep(0.01)
        return await super().search_products(*a, **k)

    async def get_order(self, *a, **k):
        await asyncio.sleep(0.01)
        return await super().get_order(*a, **k)


class Replica:
    def __init__(self, kafka, redis, agent_type, index, handler_name="handle_task", topic=None, group=None):
        self.name = f"{agent_type.replace('_', '-')}-{index}"
        runtime = memory_runtime("virtual-agent-service", kafka=kafka, redis=redis, db=False)
        runtime.settings.pod_name = self.name
        worker = AgentWorker(
            AGENT_CLASSES[agent_type](SlowDomain()), runtime, SessionStore(redis), runtime.producer, FakeHistory(),
            RuleBasedProvider(),
        )  # fmt: skip
        self.handled = 0
        handler = getattr(worker, handler_name)

        async def counted(event, record):
            await handler(event, record)
            self.handled += 1

        group = group or f"va-{agent_type}"
        self.consumer = kafka.consumer([topic or agent_task_topic(agent_type)], group)
        self.runner = ConsumerRunner(
            self.consumer, counted, group=group, producer=kafka.producer(), retry_base_seconds=0
        )
        self.alive = True

    async def run(self, stop: asyncio.Event):
        await self.consumer.start()
        self.runner.started = True
        while self.alive and not stop.is_set():
            await self.runner.process_once()

    def crash(self):
        """Killed mid-run: no commit for its last batch, no clean leave."""
        self.alive = False
        self.consumer.crash()


def task_event(conversation, n, agent_type, text):
    return EventEnvelope(
        event_id=f"task-{conversation}-{n}",
        event_type=EventType.AGENT_TASK,
        conversation_id=conversation,
        tenant_id="acme",
        customer_id=f"cust-{conversation}",
        message_id=f"msg-{conversation}-{n}",
        producer_service="agent-orchestrator",
        payload={"text": text, "source_message_id": f"msg-{conversation}-{n}", "agent": agent_type},
    )


def escalation_event(conversation, n):
    return EventEnvelope(
        event_id=f"esc-{conversation}-{n}",
        event_type=EventType.ESCALATION_REQUESTED,
        conversation_id=conversation,
        tenant_id="acme",
        customer_id=f"cust-{conversation}",
        producer_service="agent-orchestrator",
        payload={"reason": "customer_request", "priority": 1, "language": "en"},
    )


def metric(name, labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


async def test_fifty_customers_are_served_concurrently_by_replicated_workers(redis):
    random.seed(7)
    kafka = InMemoryKafka(partitions=12, group_balancing=True)
    store = SessionStore(redis)
    replicas = [Replica(kafka, redis, a, i) for a, n in REPLICAS.items() if a != "supervisor" for i in range(1, n + 1)]
    replicas += [
        Replica(kafka, redis, "supervisor", i, "handle_escalation", ESCALATIONS, "va-supervisor-escalations")
        for i in range(1, REPLICAS["supervisor"] + 1)
    ]
    processed_before = {
        a: sum(metric("engagehub_agent_tasks_processed_total", {"agent_type": a, "outcome": o})
               for o in ("replied", "escalated", "degraded", "skipped"))
        for a in QUESTIONS
    }  # fmt: skip

    for c in range(CUSTOMERS):
        for conversation in (f"conv-{c}", f"conv-{c}-human"):
            await store.create(SessionState(conversation_id=conversation, session_id="s", tenant_id="acme",
                                            customer_id=f"cust-{conversation}"))  # fmt: skip

    stop = asyncio.Event()
    loops = [asyncio.create_task(r.run(stop)) for r in replicas]
    producer = kafka.producer()
    sent_at: dict[str, float] = {}
    expected_tasks: dict[str, str] = {}  # source message id -> agent type
    escalations = 0

    async def customer(c):
        nonlocal escalations
        conversation = f"conv-{c}"
        for n in range(MESSAGES_PER_CUSTOMER):
            if n == MESSAGES_PER_CUSTOMER - 1 and c % 10 == 0:
                # 10% of customers also ask for a human, in a separate conversation: after a handoff
                # the virtual agents rightly skip that conversation's remaining tasks.
                await producer.publish(ESCALATIONS, escalation_event(f"{conversation}-human", n))
                escalations += 1
            else:
                agent_type = random.choice(list(QUESTIONS))
                event = task_event(conversation, n, agent_type, QUESTIONS[agent_type])
                sent_at[event.payload["source_message_id"]] = time.perf_counter()
                expected_tasks[event.payload["source_message_id"]] = agent_type
                await producer.publish(agent_task_topic(agent_type), event)
            await asyncio.sleep(random.uniform(0, 0.02))  # customers type at different speeds

    started = time.perf_counter()
    await asyncio.gather(*(customer(c) for c in range(CUSTOMERS)))

    # Kill one Product Advisor replica while work is in flight.
    victim = next(r for r in replicas if r.name == "product-advisor-1")
    await asyncio.sleep(0.05)
    victim.runner.commit = False
    await asyncio.sleep(0.05)
    victim.crash()

    def replies():
        out = {}
        for e in kafka.events(CHAT_MESSAGES):
            source = e.payload.get("source_message_id")
            if e.event_type == EventType.AGENT_MESSAGE and source:
                out.setdefault(source, []).append(e)
        return out

    reply_seen_at: dict[str, float] = {}
    deadline = time.perf_counter() + 30
    while time.perf_counter() < deadline:
        current = replies()
        now = time.perf_counter()
        for source in current:
            reply_seen_at.setdefault(source, now)
        handoffs = [e for e in kafka.events(ESCALATIONS) if e.event_type == EventType.HANDOFF_REQUESTED]
        if len(current) == len(expected_tasks) and len(handoffs) == escalations:
            break
        await asyncio.sleep(0.005)
    elapsed = time.perf_counter() - started
    stop.set()
    await asyncio.gather(*loops)

    final = replies()
    # Every task answered; redelivery after the crash republished identical records only.
    assert set(final) == set(expected_tasks)
    assert all(len({e.message_id for e in events}) == 1 for events in final.values())
    redelivered = sum(len(events) - 1 for events in final.values())
    handoffs = {e.conversation_id for e in kafka.events(ESCALATIONS) if e.event_type == EventType.HANDOFF_REQUESTED}
    assert len(handoffs) == escalations

    # Work was spread across the replicas of each agent type, including the one that later died.
    per_replica = {r.name: r.handled for r in replicas}
    for agent_type, count in REPLICAS.items():
        prefix = agent_type.replace("_", "-")
        busy = [n for name, n in per_replica.items() if name.startswith(prefix) and n > 0]
        assert len(busy) == count, (agent_type, per_replica)

    # Per conversation and agent, replies are in the order the tasks were sent.
    by_conversation = defaultdict(list)
    for source, events in final.items():
        conversation, n = source.removeprefix("msg-").rsplit("-", 1)
        by_conversation[(conversation, expected_tasks[source])].append((int(n), events[0].sequence_number))
    for pairs in by_conversation.values():
        assert [seq for _, seq in sorted(pairs)] == sorted(seq for _, seq in pairs)

    latencies = sorted((reply_seen_at[s] - sent_at[s]) * 1000 for s in expected_tasks)
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    processed = {
        a: sum(metric("engagehub_agent_tasks_processed_total", {"agent_type": a, "outcome": o})
               for o in ("replied", "escalated", "degraded", "skipped")) - processed_before[a]
        for a in QUESTIONS
    }  # fmt: skip
    reprocessed = int(sum(processed.values())) - len(expected_tasks)
    report = [
        "",
        "In-process load test (real worker code, in-memory Kafka with consumer-group balancing)",
        f"  customers: {CUSTOMERS}, messages: {CUSTOMERS * MESSAGES_PER_CUSTOMER} "
        f"({len(expected_tasks)} agent tasks, {escalations} escalations)",
        f"  elapsed: {elapsed:.2f} s, throughput: {(len(expected_tasks) + escalations) / elapsed:.0f} tasks/s",
        f"  reply latency: avg {statistics.mean(latencies):.1f} ms, p95 {p95:.1f} ms, max {latencies[-1]:.1f} ms",
        f"  events handled per replica: {dict(sorted(per_replica.items()))}",
        f"  tasks per agent type (metric): {processed}",
        f"  crashed replica: {victim.name}; tasks processed more than once during the crash and rebalance: "
        f"{reprocessed} (absorbed by idempotency keys); duplicate reply records: {redelivered}; tasks lost: 0",
        "  (supervisor counts include the HANDOFF_REQUESTED events they read back from chat.escalations)",
        f"  agent types by task count: {dict(Counter(expected_tasks.values()))}",
    ]
    print("\n".join(report))
    assert sum(processed.values()) >= len(expected_tasks)
