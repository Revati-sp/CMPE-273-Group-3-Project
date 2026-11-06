"""The transcript materializer, Kafka-tail replay and session restore."""

import grpc
import pytest
from app.grpc_server import start_grpc_server
from app.models import Conversation, TranscriptMessage
from engagehub_common.auth import service_token
from engagehub_common.events import EventEnvelope, EventType, sender
from engagehub_common.messaging.consumer import ConsumerRunner
from engagehub_common.session.grpc_client import GrpcHistoryClient
from engagehub_common.session.history import HistoryUnavailable
from engagehub_common.testing import TEST_SECRET
from engagehub_common.topics import CHAT_MESSAGES, DEAD_LETTER, SESSION_EVENTS
from sqlalchemy import func, select


def message(seq, message_id=None, conv="c1", tenant="acme", text=None):
    return EventEnvelope(
        event_id=f"evt-{message_id or seq}",
        event_type=EventType.CUSTOMER_MESSAGE,
        conversation_id=conv,
        tenant_id=tenant,
        customer_id="cust-1",
        message_id=message_id or f"msg-{seq}",
        sequence_number=seq,
        producer_service="chat-service",
        payload={"text": text or f"text {seq}", "sender": sender("customer", "cust-1", "Dana")},
    )


async def publish(kafka, *events, topic=CHAT_MESSAGES):
    producer = kafka.producer()
    for event in events:
        await producer.publish(topic, event)


async def materialize(kafka, history):
    consumer = kafka.consumer([CHAT_MESSAGES, SESSION_EVENTS], "session-materializer")
    await consumer.start()
    runner = ConsumerRunner(
        consumer, history.handle_event, group="session-materializer", producer=kafka.producer(), retry_base_seconds=0
    )
    await runner.process_once()


async def transcript_count(db):
    async with db.session() as session:
        return await session.scalar(select(func.count()).select_from(TranscriptMessage))


async def test_duplicate_messages_are_not_inserted_twice(db, kafka, history):
    await publish(kafka, message(1), message(1), message(2), message(1))
    await materialize(kafka, history)
    assert await transcript_count(db) == 2
    async with db.session() as session:
        assert (await session.get(Conversation, "c1")).message_count == 2


async def test_full_redelivery_after_consumer_restart_is_idempotent(db, kafka, history):
    await publish(kafka, message(1), message(2))
    await materialize(kafka, history)
    kafka.committed.clear()  # lost commits: every record is delivered again
    await materialize(kafka, history)
    assert await transcript_count(db) == 2


async def test_out_of_order_arrival_is_stored_and_read_in_sequence_order(db, kafka, history):
    await publish(kafka, message(3), message(1), message(2))
    await materialize(kafka, history)
    assert [e.sequence_number for e in await history.messages_after("acme", "c1", 0)] == [1, 2, 3]


async def test_sequence_conflict_is_dead_lettered_not_overwritten(db, kafka, history):
    await publish(kafka, message(1, "original"), message(1, "imposter"))
    await materialize(kafka, history)
    assert await transcript_count(db) == 1
    letters = kafka.events(DEAD_LETTER)
    assert len(letters) == 1 and "already held" in letters[0].payload["reason"]
    assert (await history.messages_after("acme", "c1", 0))[0].message_id == "original"


async def test_replay_reads_kafka_tail_when_transcript_is_behind(db, kafka, history):
    await publish(kafka, message(1), message(2))
    await materialize(kafka, history)
    await publish(kafka, message(3), message(4))  # acknowledged but not yet materialised
    assert await transcript_count(db) == 2
    assert [e.sequence_number for e in await history.messages_after("acme", "c1", 1)] == [2, 3, 4]


async def test_replay_works_when_nothing_is_materialised_yet(kafka, history):
    await publish(kafka, message(1))
    assert [e.sequence_number for e in await history.messages_after("acme", "c1", 0)] == [1]


async def test_replay_ignores_other_conversations_and_tenants(db, kafka, history):
    await publish(kafka, message(1), message(1, "x", conv="c2"), message(1, "y", tenant="globex", conv="c3"))
    await materialize(kafka, history)
    assert [e.message_id for e in await history.messages_after("acme", "c1", 0)] == ["msg-1"]
    assert await history.messages_after("globex", "c1", 0) == []


async def test_restore_rebuilds_session_with_head_including_kafka_tail(db, kafka, history, store):
    started = EventEnvelope(
        event_type=EventType.CONVERSATION_STARTED,
        conversation_id="c1",
        tenant_id="acme",
        customer_id="cust-1",
        producer_service="chat-service",
        payload={"session_id": "s1", "language": "es"},
    )
    await publish(kafka, started, topic=SESSION_EVENTS)
    await publish(kafka, message(1), message(2))
    await materialize(kafka, history)
    await publish(kafka, message(3))  # exists only in Kafka

    found, head = await history.restore_session("acme", "c1")
    assert (found, head) == (True, 3)
    state = await store.get("acme", "c1")
    assert state.seq == 3 and state.customer_id == "cust-1" and state.language == "es"


async def test_restore_unknown_or_other_tenant_conversation_returns_not_found(db, kafka, history):
    await publish(kafka, message(1))
    await materialize(kafka, history)
    assert await history.restore_session("globex", "c1") == (False, 0)
    assert await history.restore_session("acme", "missing") == (False, 0)


async def test_agent_assignment_is_persisted_for_later_restore(db, kafka, history, store):
    await publish(kafka, message(1))
    await publish(
        kafka,
        EventEnvelope(
            event_type=EventType.AGENT_ASSIGNED,
            conversation_id="c1",
            tenant_id="acme",
            customer_id="cust-1",
            producer_service="human-agent-service",
            payload={"agent_id": "agent-1", "agent_name": "Alice"},
        ),
        topic=SESSION_EVENTS,
    )
    await materialize(kafka, history)
    await history.restore_session("acme", "c1")
    state = await store.get("acme", "c1")
    assert state.assigned_agent_id == "agent-1" and state.workflow_state == "human_active"


@pytest.fixture
async def grpc_server(history):
    server, port = await start_grpc_server(history, TEST_SECRET, "127.0.0.1:0")
    yield port
    await server.stop(grace=0)


async def test_grpc_round_trip(grpc_server, kafka, history):
    await publish(kafka, message(1), message(2))
    await materialize(kafka, history)
    client = GrpcHistoryClient(f"127.0.0.1:{grpc_server}", TEST_SECRET, "chat-service")
    try:
        events = await client.messages_after("acme", "c1", 1)
        assert [e.sequence_number for e in events] == [2]
        assert await client.restore_session("acme", "c1") is True
    finally:
        await client.close()


async def test_grpc_rejects_tokens_for_another_tenant(grpc_server):
    from engagehub_common.proto import session_pb2, session_pb2_grpc

    async with grpc.aio.insecure_channel(f"127.0.0.1:{grpc_server}") as channel:
        stub = session_pb2_grpc.SessionHistoryStub(channel)
        token = service_token(TEST_SECRET, "chat-service", "globex")
        with pytest.raises(grpc.aio.AioRpcError) as denied:
            await stub.GetMessagesAfter(
                session_pb2.GetMessagesRequest(tenant_id="acme", conversation_id="c1"),
                metadata=(("authorization", f"Bearer {token}"),),
            )
        assert denied.value.code() == grpc.StatusCode.PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as anonymous:
            await stub.GetMessagesAfter(session_pb2.GetMessagesRequest(tenant_id="acme", conversation_id="c1"))
        assert anonymous.value.code() == grpc.StatusCode.UNAUTHENTICATED


async def test_grpc_client_reports_unavailable_server():
    client = GrpcHistoryClient("127.0.0.1:1", TEST_SECRET, "chat-service", timeout=0.2)
    with pytest.raises(HistoryUnavailable):
        await client.messages_after("acme", "c1", 0)
    await client.close()
