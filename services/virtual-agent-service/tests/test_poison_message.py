"""A valid agent task is answered. A bad one is retried, then dead-lettered."""

import logging
from pathlib import Path

import fakeredis.aioredis
from app.agents import OrderTrackingAgent
from app.worker import AgentWorker
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.llm import RuleBasedProvider
from engagehub_common.messaging.consumer import ConsumerRunner
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.testing import memory_runtime
from engagehub_common.topics import CHAT_MESSAGES, DEAD_LETTER, agent_task_topic

POISON_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "poison_agent_task.json"
TOPIC = agent_task_topic("order_tracking")
GROUP = "va-order-tracking"
MAX_ATTEMPTS = 3

ORDER = {
    "id": "ORD-1001",
    "status": "delivered",
    "total": 189.0,
    "payment_status": "paid",
    "tracking_number": "TRK-ACME-1001",
    "estimated_delivery": "2026-10-02",
}


class Catalog:
    async def get_order(self, _tenant_id, _customer_id, order_id):
        return ORDER if order_id == ORDER["id"] else None

    async def list_orders(self, _tenant_id, _customer_id):
        return [ORDER]


class History:
    async def messages_after(self, *_args, **_kwargs):
        return []

    async def restore_session(self, *_args):
        return False


def agent_task(event_id, correlation_id, payload):
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.AGENT_TASK,
        conversation_id="c1",
        tenant_id="acme",
        customer_id="u1",
        producer_service="agent-orchestrator",
        correlation_id=correlation_id,
        payload=payload,
    )


def records_with(caplog, message):
    return [record for record in caplog.records if record.message == message]


async def test_valid_agent_task_is_answered_and_poison_events_are_dead_lettered(caplog):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    runtime = memory_runtime("virtual-agent-service", redis=redis, db=False)
    store = SessionStore(redis)
    await store.create(SessionState(conversation_id="c1", session_id="s", tenant_id="acme", customer_id="u1"))
    worker = AgentWorker(
        OrderTrackingAgent(Catalog()),
        runtime,
        store,
        runtime.producer,
        History(),
        RuleBasedProvider(),
    )
    calls = []

    async def counting(event, record):
        calls.append(event.event_id)
        await worker.handle_task(event, record)

    kafka = runtime.kafka
    producer = kafka.producer()
    await producer.publish(
        TOPIC,
        agent_task(
            "task-order-1001",
            "corr-order-1001",
            {"text": "Where is order 1001?", "source_message_id": "m1", "language": "en"},
        ),
    )
    await producer.publish(TOPIC, agent_task("poison-payload", "corr-poison-handler", {}))
    kafka.append(TOPIC, b"c1", POISON_FIXTURE.read_bytes())

    consumer = kafka.consumer([TOPIC], GROUP)
    await consumer.start()
    runner = ConsumerRunner(
        consumer,
        counting,
        group=GROUP,
        producer=producer,
        service_name="virtual-agent-service",
        max_attempts=MAX_ATTEMPTS,
        retry_base_seconds=0,
    )
    caplog.set_level(logging.WARNING, logger="engagehub_common.messaging.consumer")

    assert await runner.process_once() == 3

    replies = kafka.events(CHAT_MESSAGES)
    assert len(replies) == 1
    assert replies[0].correlation_id == "corr-order-1001"
    assert "ORD-1001 is delivered" in replies[0].payload["text"]
    assert "TRK-ACME-1001" in replies[0].payload["text"]

    # The valid task runs once. The payload with no text is retried up to the limit.
    # The fixture never reaches the handler.
    assert calls == ["task-order-1001", *["poison-payload"] * MAX_ATTEMPTS]

    retries = records_with(caplog, "handler failed; retrying")
    assert len(retries) == MAX_ATTEMPTS - 1
    assert [record.retry_count for record in retries] == [1, 2]
    assert {record.correlation_id for record in retries} == {"corr-poison-handler"}
    assert {record.event_id for record in retries} == {"poison-payload"}

    schema_logs = records_with(caplog, "schema validation failed")
    assert len(schema_logs) == 1
    assert schema_logs[0].correlation_id == "corr-poison-schema"
    assert schema_logs[0].event_id == "poison-schema-1"
    assert schema_logs[0].retry_count == 0
    assert schema_logs[0].reason.startswith("schema_validation")

    letters = kafka.events(DEAD_LETTER)
    assert len(letters) == 2
    by_reason = {letter.payload["reason"].split(":", 1)[0]: letter for letter in letters}
    poison = by_reason["handler_failed"]
    malformed = by_reason["schema_validation"]
    assert poison.correlation_id == "corr-poison-handler"
    assert poison.tenant_id == "acme" and poison.conversation_id == "c1"
    assert poison.payload["original"]["event_id"] == "poison-payload"
    assert malformed.correlation_id == "corr-poison-schema"
    assert malformed.payload["original"]["event_type"] == "not_a_real_event"
    assert malformed.payload["source_topic"] == TOPIC

    dead_logs = records_with(caplog, "event dead-lettered")
    assert {record.correlation_id for record in dead_logs} == {"corr-poison-handler", "corr-poison-schema"}
    assert {record.dlq for record in dead_logs} == {DEAD_LETTER}
    poison_log = next(record for record in dead_logs if record.correlation_id == "corr-poison-handler")
    assert poison_log.event_id == "poison-payload"
    assert poison_log.retry_count == MAX_ATTEMPTS - 1
    assert poison_log.reason.startswith("handler_failed")

    assert await runner.process_once() == 0
    assert calls == ["task-order-1001", *["poison-payload"] * MAX_ATTEMPTS]
    await redis.aclose()
