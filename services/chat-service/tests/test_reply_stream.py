"""Reply stream: chat.replies -> Chat Consumer -> PostgreSQL log -> Redis cursor -> WebSocket.

These run the real ASGI app with in-memory Kafka, fakeredis and SQLite. The same
flow runs against real Kafka, Redis Cluster and PostgreSQL in tests/e2e.
"""

import asyncio
import json
import os
import time
import uuid
from datetime import UTC, datetime

import anyio
import fakeredis.aioredis
import pytest
from app.main import create_app
from app.replies.hub import CustomerConnection, ReplyHub
from app.replies.log import MessageLog
from app.replies.models import ChatMessage, ReplyBase
from engagehub_common.db import Database, ensure_database
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.store import SessionStore
from engagehub_common.testing import auth_headers, memory_runtime, token
from engagehub_common.topics import CHAT_REPLIES, CHAT_REPLIES_DLQ
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from starlette.websockets import WebSocketDisconnect
from tests.conftest import KafkaBackedHistory

CONV = "conversation-123"
DANA = {"subject": "cust-acme-dana", "tenant": "acme"}


class SwitchableRedis:
    """fakeredis that can be taken down, as in a Redis outage."""

    def __init__(self):
        self.inner = fakeredis.aioredis.FakeRedis(decode_responses=True)
        self.down = False

    def __getattr__(self, name):
        attribute = getattr(self.inner, name)
        if not callable(attribute):
            return attribute

        async def call(*args, **kwargs):
            if self.down:
                raise RedisConnectionError("redis unavailable")
            return await attribute(*args, **kwargs)

        return call

    def pipeline(self, *args, **kwargs):
        return self.inner.pipeline(*args, **kwargs)


def _fresh_postgres(url: str) -> Database:
    """Empty reply tables on the PostgreSQL server named by TEST_POSTGRES_URL."""

    async def reset():
        await ensure_database(url)
        db = Database(url)
        async with db.engine.begin() as conn:
            await conn.run_sync(ReplyBase.metadata.drop_all)
        await db.dispose()

    asyncio.run(reset())
    return Database(url)


class Stack:
    """Shared infrastructure, so an app instance can be 'restarted' on the same state.

    Uses SQLite by default. With TEST_POSTGRES_URL set, the whole suite runs on PostgreSQL.
    """

    def __init__(self):
        self.kafka = InMemoryKafka()
        self.redis = SwitchableRedis()
        self.runtime = memory_runtime("chat-service", kafka=self.kafka, redis=self.redis)
        self.postgres_url = os.environ.get("TEST_POSTGRES_URL")
        self.started = 0
        if self.postgres_url:
            self.runtime.db = _fresh_postgres(self.postgres_url)

    def app(self, pod="pod-a", dev=True):
        if self.postgres_url and self.started:
            # A restarted process opens a new connection pool. asyncpg connections are
            # bound to the event loop that created them, so they cannot be carried over.
            self.runtime.db = Database(self.postgres_url)
        self.started += 1
        history = KafkaBackedHistory(self.kafka, SessionStore(self.redis))
        app = create_app(runtime=self.runtime, history=history, pod_name=pod)
        app.state.settings.enable_dev_endpoints = dev
        return app

    def publish(self, content, *, conversation_id=CONV, message_id=None, sender_id="order-tracking-agent", raw=None):
        message_id = message_id or f"message-{uuid.uuid4()}"
        payload = raw or {
            "event_type": "VirtualAgentReplyProduced",
            "message_id": message_id,
            "conversation_id": conversation_id,
            "sender_type": "virtual_agent",
            "sender_id": sender_id,
            "sender_name": "Order Tracking Agent",
            "content": content,
            "timestamp": "2026-11-12T12:00:00Z",
        }
        self.kafka.append(CHAT_REPLIES, conversation_id.encode(), json.dumps(payload).encode())
        return message_id

    def stored(self, client, conversation_id=CONV) -> int:
        return client.portal.call(client.app.state.reply_log.latest_sequence, conversation_id)


@pytest.fixture
def stack():
    return Stack()


@pytest.fixture
def client(stack):
    with TestClient(stack.app()) as test_client:
        yield test_client


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met in time")


async def _receive(stream, timeout):  # noqa: ASYNC109 - anyio.fail_after enforces it
    with anyio.fail_after(timeout):
        return await stream.receive()


def recv(ws, timeout=5.0) -> dict:
    """Like ws.receive_json(), but fails instead of hanging if nothing arrives."""
    message = ws.portal.call(_receive, ws._send_rx, timeout)
    ws._raise_on_close(message)
    return json.loads(message["text"])


def recv_until(ws, frame_type, timeout=5.0) -> list[dict]:
    frames = []
    while True:
        frame = recv(ws, timeout)
        frames.append(frame)
        if frame["type"] == frame_type:
            return frames


def create(client, conversation_id=CONV, **who):
    who = who or DANA
    return client.post("/conversations", json={"conversation_id": conversation_id}, headers=auth_headers(**who))


def hello(ws, last_received=None, **who):
    who = who or DANA
    frame = {"type": "hello", "token": token(**who)}
    if last_received is not None:
        frame["last_received_sequence"] = last_received
    ws.send_json(frame)


def connect(ws, **kwargs) -> tuple[dict, list[dict]]:
    """Handshake. Returns (connection_ack, frames up to and including replay_completed)."""
    hello(ws, **kwargs)
    ack = recv(ws)
    assert ack["type"] == "connection_ack", ack
    return ack, recv_until(ws, "replay_completed")


def seqs(frames):
    return [f["sequence_number"] for f in frames if f["type"] == "chat_message"]


def ack(ws, sequence, message_id=""):
    ws.send_json({"type": "ack", "sequence_number": sequence, "message_id": message_id})
    return recv(ws)


# The scenario from the specification, step by step


def test_replay_after_reconnect_matches_the_specified_scenario(stack, client):
    assert create(client).status_code == 201
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connection_ack, replay = connect(ws)  # 1. connect
        assert connection_ack["reconnected"] is False and seqs(replay) == []
        ids = [stack.publish(f"reply {n}") for n in range(1, 6)]
        live = [recv(ws) for _ in range(5)]  # 2. receive 1 through 5
        assert seqs(live) == [1, 2, 3, 4, 5]
        assert [m["message_id"] for m in live] == ids
        assert all(m["sender_type"] == "virtual_agent" and not m["replayed"] for m in live)
        confirmed = ack(ws, 3, ids[2])  # 3. acknowledge up to 3
        assert confirmed["type"] == "ack_confirmed"
        assert confirmed["cursor"]["last_acknowledged_sequence"] == 3
    # 4. disconnected
    for n in (6, 7, 8):  # 5. publish 6, 7, 8 while disconnected
        stack.publish(f"reply {n}")
    wait_for(lambda: stack.stored(client) == 8)  # 6. stored in the durable log

    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:  # 7. reconnect, same conversation ID
        connection_ack, replay = connect(ws)
        assert connection_ack["reconnected"] is True
        assert connection_ack["cursor"]["last_acknowledged_sequence"] == 3  # 8. read Redis cursor
        started = replay[0]
        assert started == {  # 9. query after the cursor
            "type": "replay_started", "conversation_id": CONV, "from_sequence": 4, "to_sequence": 8, "count": 5,
        }  # fmt: skip
        assert seqs(replay) == [4, 5, 6, 7, 8]  # 10. 4 through 8, in order
        assert [f["content"] for f in replay if f["type"] == "chat_message"] == [f"reply {n}" for n in range(4, 9)]
        assert all(f["replayed"] for f in replay if f["type"] == "chat_message")
        assert replay[-1] == {"type": "replay_completed", "conversation_id": CONV, "last_sequence": 8}
        for frame in [f for f in replay if f["type"] == "chat_message"]:  # 11. acknowledge each
            assert ack(ws, frame["sequence_number"], frame["message_id"])["type"] == "ack_confirmed"

    cursor = client.get(f"/conversations/{CONV}/cursor", headers=auth_headers(**DANA)).json()
    assert cursor["cursor"]["last_acknowledged_sequence"] == 8  # 12. cursor at 8
    assert cursor["key"] == f"chat:cursor:{CONV}:cust-acme-dana"
    # 13. no duplicates: the next reconnect replays nothing.
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        _, replay = connect(ws)
        assert seqs(replay) == []


# Connection and validation


def test_connection_ack_describes_the_session(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connection_ack, _ = connect(ws)
    assert connection_ack["pod"] == "pod-a" and connection_ack["cursor_source"] == "none"
    assert connection_ack["latest_sequence"] == 0 and connection_ack["customer_id"] == "cust-acme-dana"


@pytest.mark.parametrize(
    "who,first_frame",
    [
        (None, {"type": "ack", "sequence_number": 1}),
        (None, {"type": "hello", "token": "not-a-jwt"}),
        ({"subject": "cust-acme-vic", "tenant": "acme"}, None),  # another customer
        ({"subject": "cust-acme-dana", "tenant": "globex"}, None),  # another tenant
        ({"subject": "agent-acme-alice", "tenant": "acme", "role": "agent"}, None),
    ],
)
def test_unauthorized_connections_are_closed(client, who, first_frame):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        ws.send_json(first_frame or {"type": "hello", "token": token(**who)})
        with pytest.raises(WebSocketDisconnect) as closed:
            recv(ws)
    assert closed.value.code in (4401, 4404)


def test_unknown_conversation_is_rejected(client):
    with client.websocket_connect("/ws/customer/never-created") as ws:
        hello(ws)
        with pytest.raises(WebSocketDisconnect) as closed:
            recv(ws)
    assert closed.value.code == 4404


def test_disconnect_is_detected_and_the_socket_forgotten(stack, client):
    create(client)
    hub = client.app.state.reply_hub
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        assert len(hub.connections[CONV]) == 1
    wait_for(lambda: CONV not in hub.connections)
    stack.publish("while nobody is connected")
    wait_for(lambda: stack.stored(client) == 1)  # stored, though nothing was delivered


def test_ack_cannot_exceed_delivered_or_mismatch_or_rewind(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        ids = [stack.publish(f"r{n}") for n in range(1, 3)]
        recv(ws), recv(ws)
        assert ack(ws, 5)["code"] == "invalid_ack"
        assert ack(ws, 1, ids[1])["code"] == "invalid_ack"
        assert ack(ws, 2)["cursor"]["last_acknowledged_sequence"] == 2
        assert ack(ws, 1)["cursor"]["last_acknowledged_sequence"] == 2  # never moves backwards


def test_invalid_frames_get_an_error_without_closing(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        ws.send_json({"type": "bogus"})
        assert recv(ws)["code"] == "invalid_frame"
        ws.send_json({"type": "ack", "sequence_number": 0})
        assert recv(ws)["code"] == "invalid_frame"
        ws.send_json({"type": "ping"})
        assert recv(ws)["type"] == "pong"


# Ordering and duplicates


def test_messages_arrive_in_publish_order_with_contiguous_sequences(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        for n in range(1, 21):
            stack.publish(f"r{n}")
        frames = [recv(ws) for _ in range(20)]
    assert seqs(frames) == list(range(1, 21))
    assert [f["content"] for f in frames] == [f"r{n}" for n in range(1, 21)]


def test_duplicate_kafka_events_create_one_row_and_one_visible_message(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        for _ in range(3):
            stack.publish("only once", message_id="message-dup-1")
        stack.publish("next")
        frames = [recv(ws), recv(ws)]
    assert seqs(frames) == [1, 2]
    assert [f["content"] for f in frames] == ["only once", "next"]

    async def count_rows():
        async with stack.runtime.db.session() as session:
            return await session.scalar(select(func.count()).select_from(ChatMessage))

    assert client.portal.call(count_rows) == 2


def test_message_id_reused_in_another_conversation_is_dead_lettered(stack, client):
    create(client)
    create(client, "conversation-456")
    stack.publish("first", message_id="message-shared")
    stack.publish("imposter", conversation_id="conversation-456", message_id="message-shared")
    wait_for(lambda: stack.kafka.events(CHAT_REPLIES_DLQ))
    assert stack.stored(client) == 1 and stack.stored(client, "conversation-456") == 0
    assert "non_retryable" in stack.kafka.events(CHAT_REPLIES_DLQ)[0].payload["reason"]


def test_unknown_conversation_and_bad_schema_go_to_the_dead_letter_topic(stack, client):
    stack.publish("nobody registered this", conversation_id="ghost")
    stack.publish("x", raw={"event_type": "VirtualAgentReplyProduced", "content": "missing fields"})
    wait_for(lambda: len(stack.kafka.events(CHAT_REPLIES_DLQ)) == 2)
    reasons = sorted(e.payload["reason"].split(":")[0] for e in stack.kafka.events(CHAT_REPLIES_DLQ))
    assert reasons == ["non_retryable", "schema_validation"]
    letter = stack.kafka.events(CHAT_REPLIES_DLQ)[0]
    assert (
        letter.payload["source_topic"] == CHAT_REPLIES and letter.payload["consumer_group"] == "chat-replies-consumer"
    )


async def test_concurrent_appends_get_unique_contiguous_sequences():
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.create_schema(ReplyBase)
    log = MessageLog(db)
    await log.register(CONV, "acme", "cust-1")

    async def append(n):
        return await log.append(
            conversation_id=CONV, message_id=f"m-{n}", sender_type="virtual_agent", sender_id="a", sender_name="",
            content=str(n), timestamp=datetime.now(UTC),
            correlation_id="c",
        )  # fmt: skip

    results = await asyncio.gather(*(append(n) for n in range(50)), *(append(n) for n in range(10)))
    assert sorted(r.message.sequence_number for r in results if not r.duplicate) == list(range(1, 51))
    assert sum(r.duplicate for r in results) == 10
    await db.dispose()


def test_live_fanout_fills_gaps_and_drops_repeats():
    async def scenario():
        db = Database("sqlite+aiosqlite:///:memory:")
        await db.create_schema(ReplyBase)
        log = MessageLog(db)
        await log.register(CONV, "acme", "cust-1")
        stored = []
        for n in range(1, 4):
            result = await log.append(
                conversation_id=CONV, message_id=f"m{n}", sender_type="virtual_agent", sender_id="a",
                sender_name="", content=str(n), timestamp=datetime.now(UTC),
                correlation_id="c",
            )  # fmt: skip
            stored.append(result.message)
        from app.replies.cursor import CursorStore

        hub = ReplyHub(log, CursorStore(fakeredis.aioredis.FakeRedis(decode_responses=True), 60), "pod")
        sent = []

        async def send(frame):
            sent.append(frame)

        async def close(code, reason):
            pass

        conn = CustomerConnection(CONV, "cust-1", send, close, replaying=False)
        hub.connections[CONV].add(conn)
        await hub.deliver(stored[2])  # 3 arrives before 1 and 2
        await hub.deliver(stored[1])  # a late, already-sent message
        await hub.deliver(stored[2])
        await db.dispose()
        return [f["sequence_number"] for f in sent]

    assert asyncio.run(scenario()) == [1, 2, 3]


# Restarts and failures


def test_consumer_restart_after_lost_commit_does_not_duplicate(stack):
    with TestClient(stack.app()) as first:
        create(first)
        for n in range(1, 4):
            stack.publish(f"r{n}")
        wait_for(lambda: stack.stored(first) == 3)
    stack.kafka.committed.clear()  # the commit never reached Kafka: everything is redelivered
    with TestClient(stack.app(pod="pod-b")) as second:
        wait_for(lambda: any(group == "chat-replies-consumer" for group, _ in stack.kafka.committed))
        assert stack.stored(second) == 3
        body = second.get(f"/conversations/{CONV}/messages", headers=auth_headers(**DANA)).json()
        assert [m["content"] for m in body["messages"]] == ["r1", "r2", "r3"]


def test_chat_service_restart_keeps_acknowledged_state_and_replays(stack):
    with TestClient(stack.app(pod="pod-a")) as first:
        create(first)
        with first.websocket_connect(f"/ws/customer/{CONV}") as ws:
            connect(ws)
            for n in range(1, 4):
                stack.publish(f"r{n}")
            frames = [recv(ws) for _ in range(3)]
            ack(ws, 2, frames[1]["message_id"])
    stack.publish("published while the service was down")
    with TestClient(stack.app(pod="pod-b")) as second:  # the replacement pod
        wait_for(lambda: stack.stored(second) == 4)
        with second.websocket_connect(f"/ws/customer/{CONV}") as ws:
            connection_ack, replay = connect(ws)
    assert connection_ack["pod"] == "pod-b" and connection_ack["reconnected"] is True
    assert connection_ack["cursor"]["last_acknowledged_sequence"] == 2
    assert seqs(replay) == [3, 4]


def test_redis_outage_falls_back_to_the_durable_log(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        for n in range(1, 4):
            stack.publish(f"r{n}")
        frames = [recv(ws) for _ in range(3)]
        ack(ws, 1, frames[0]["message_id"])

    stack.redis.down = True
    headers = auth_headers(**DANA)
    assert client.get(f"/conversations/{CONV}/cursor", headers=headers).status_code == 503
    assert client.post(f"/conversations/{CONV}/ack", json={"sequence_number": 1}, headers=headers).status_code == 503
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connection_ack, replay = connect(ws, last_received=2)  # the client says what it holds
        assert connection_ack["cursor_source"] == "client"
        assert seqs(replay) == [3]
        error = ack(ws, 3, replay[1]["message_id"])
        assert error["code"] == "cursor_unavailable" and error["retryable"] is True
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connection_ack, replay = connect(ws)  # nothing known: replay everything, the client deduplicates
        assert connection_ack["cursor_source"] == "unavailable" and seqs(replay) == [1, 2, 3]

    stack.redis.down = False
    cursor = client.get(f"/conversations/{CONV}/cursor", headers=headers).json()["cursor"]
    assert cursor["last_acknowledged_sequence"] == 1  # state from before the outage survived


def test_database_outage_is_retried_by_the_consumer_not_dead_lettered(stack, client, monkeypatch):
    create(client)
    log = client.app.state.reply_log
    real_append = log.append
    failures = {"left": 3}

    async def flaky_append(**kwargs):
        if failures["left"]:
            failures["left"] -= 1
            raise OperationalError("INSERT", {}, ConnectionRefusedError("database down"))
        return await real_append(**kwargs)

    monkeypatch.setattr(log, "append", flaky_append)
    stack.publish("survives the outage")
    wait_for(lambda: stack.stored(client) == 1)
    assert failures["left"] == 0
    assert stack.kafka.events(CHAT_REPLIES_DLQ) == []


def test_database_outage_rejects_new_sockets_and_rest_with_retryable_errors(stack, client, monkeypatch):
    create(client)
    log = client.app.state.reply_log

    async def down(*args, **kwargs):
        raise OperationalError("SELECT", {}, ConnectionRefusedError("database down"))

    monkeypatch.setattr(log, "get_conversation", down)
    assert client.get(f"/conversations/{CONV}/messages", headers=auth_headers(**DANA)).status_code == 503
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        hello(ws)
        error = recv(ws)
        assert error["code"] == "service_unavailable" and error["retryable"] is True
        with pytest.raises(WebSocketDisconnect) as closed:
            recv(ws)
    assert closed.value.code == 1013


def test_shutdown_closes_customer_sockets_with_the_restart_code():
    async def scenario():
        hub = ReplyHub(None, None, "pod")
        codes = []

        async def send(frame):
            pass

        async def close(code, reason):
            codes.append(code)

        conn = CustomerConnection(CONV, "c", send, close)
        hub.connections[CONV].add(conn)
        await hub.shutdown()
        return codes, dict(hub.connections)

    assert asyncio.run(scenario()) == ([1012], {})


# REST API


def test_create_conversation_is_idempotent_for_its_owner(client):
    assert create(client).status_code == 201
    again = create(client)
    assert again.status_code == 200 and again.json()["created"] is False
    assert create(client, subject="cust-acme-vic", tenant="acme").status_code == 409
    generated = client.post("/conversations", json={}, headers=auth_headers(**DANA))
    assert generated.status_code == 201 and generated.json()["conversation_id"]


def test_history_endpoint_supports_after_sequence_and_is_isolated(stack, client):
    create(client)
    for n in range(1, 6):
        stack.publish(f"r{n}")
    wait_for(lambda: stack.stored(client) == 5)
    body = client.get(f"/conversations/{CONV}/messages?after_sequence=3", headers=auth_headers(**DANA)).json()
    assert body["latest_sequence"] == 5 and seqs(body["messages"]) == [4, 5]
    other_customer = auth_headers(subject="cust-acme-vic")
    other_tenant_admin = auth_headers(subject="admin-globex", tenant="globex", role="admin")
    same_tenant_agent = auth_headers(subject="agent-acme-alice", role="agent")
    assert client.get(f"/conversations/{CONV}/messages", headers=other_customer).status_code == 404
    assert client.get(f"/conversations/{CONV}/messages", headers=other_tenant_admin).status_code == 404
    assert client.get(f"/conversations/{CONV}/cursor", headers=other_customer).status_code == 404
    assert client.get(f"/conversations/{CONV}/messages", headers=same_tenant_agent).status_code == 200


def test_rest_ack_is_validated_against_delivered_messages(stack, client):
    create(client)
    headers = auth_headers(**DANA)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        ids = [stack.publish(f"r{n}") for n in range(1, 3)]
        recv(ws), recv(ws)
    stack.publish("stored but never delivered")
    wait_for(lambda: stack.stored(client) == 3)
    assert client.post(f"/conversations/{CONV}/ack", json={"sequence_number": 3}, headers=headers).status_code == 409
    mismatch = {"sequence_number": 2, "message_id": ids[0]}
    assert client.post(f"/conversations/{CONV}/ack", json=mismatch, headers=headers).status_code == 409
    ok = client.post(f"/conversations/{CONV}/ack", json={"sequence_number": 2, "message_id": ids[1]}, headers=headers)
    assert ok.status_code == 200 and ok.json()["cursor"]["last_acknowledged_sequence"] == 2
    cursor = client.get(f"/conversations/{CONV}/cursor", headers=headers).json()
    assert cursor["last_delivered_sequence"] == 2 and cursor["cursor"]["last_message_id"] == ids[1]


def test_customer_messages_join_the_same_sequence_and_are_idempotent(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        stack.publish("agent first")
        assert recv(ws)["sequence_number"] == 1
        frame = {"type": "customer_message", "client_message_id": "client-0001", "content": "thanks"}
        ws.send_json(frame)
        frames = recv_until(ws, "chat_message")
        accepted = next(f for f in frames if f["type"] == "customer_message_accepted")
        assert accepted["sequence_number"] == 2 and accepted["duplicate"] is False
        assert frames[-1]["sender_type"] == "customer" and frames[-1]["sequence_number"] == 2
        ws.send_json(frame)  # resent after a lost ack
        retry = recv(ws)
        assert retry["duplicate"] is True and retry["sequence_number"] == 2


def test_dev_fake_reply_endpoint_publishes_through_kafka(stack):
    with TestClient(stack.app(dev=False)) as disabled:
        create(disabled)
        assert (
            disabled.post(
                f"/dev/fake-replies?conversation_id={CONV}", json={}, headers=auth_headers(**DANA)
            ).status_code
            == 404
        )
    with TestClient(stack.app(dev=True)) as client:
        response = client.post(
            f"/dev/fake-replies?conversation_id={CONV}",
            json={"count": 3, "content": "hi"},
            headers=auth_headers(**DANA),
        )
        assert response.status_code == 202 and len(response.json()["message_ids"]) == 3
        wait_for(lambda: stack.stored(client) == 3)
        other = auth_headers(subject="cust-acme-vic")
        assert client.post(f"/dev/fake-replies?conversation_id={CONV}", json={}, headers=other).status_code == 404


def test_health_aliases(client):
    assert client.get("/health").status_code == 200
    assert client.get("/ready").json()["status"] == "ready"


# Real agent responses on agent.responses


def publish_agent_response(
    stack,
    text,
    *,
    conversation_id=CONV,
    message_id=None,
    customer_id="cust-acme-dana",
    tenant_id="acme",
    overrides=None,
):
    """Publishes exactly what virtual-agent-service publishes (same shared builder)."""
    from engagehub_common.events import agent_response_event
    from engagehub_common.topics import AGENT_RESPONSES

    message_id = message_id or f"message-{uuid.uuid4()}"
    event = agent_response_event(
        event_id=f"evt-{message_id}",
        conversation_id=conversation_id,
        tenant_id=tenant_id,
        customer_id=customer_id,
        message_id=message_id,
        text=text,
        sender_id="order_tracking",
        sender_name="Order Tracking",
        producer_service="virtual-agent-service",
        source_message_id="customer-msg-1",
    )
    if overrides:  # model_copy skips validation, so this can produce malformed records on purpose
        event = event.model_copy(update=overrides)
    stack.kafka.append(AGENT_RESPONSES, conversation_id.encode(), event.to_bytes())
    return message_id


def test_real_agent_response_is_committed_and_delivered(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        message_id = publish_agent_response(stack, "Order ORD-1001 is delivered.")
        frame = recv(ws)
        assert frame["type"] == "chat_message" and frame["sequence_number"] == 1
        assert frame["message_id"] == message_id and frame["content"] == "Order ORD-1001 is delivered."
        assert (frame["sender_type"], frame["sender_id"], frame["sender_name"]) == (
            "virtual_agent",
            "order_tracking",
            "Order Tracking",
        )
        assert ack(ws, 1, message_id)["cursor"]["last_acknowledged_sequence"] == 1
    body = client.get(f"/conversations/{CONV}/messages", headers=auth_headers(**DANA)).json()
    assert [m["message_id"] for m in body["messages"]] == [message_id]


def test_real_and_fake_replies_share_one_sequence_and_replay_together(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        stack.publish("fake 1")
        assert recv(ws)["content"] == "fake 1"
        publish_agent_response(stack, "real 2")
        assert recv(ws)["content"] == "real 2"
        ack(ws, 2)
    stack.publish("fake 3")
    publish_agent_response(stack, "real 4")
    wait_for(lambda: stack.stored(client) == 4)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        _, replay = connect(ws)
    assert seqs(replay) == [3, 4]
    assert sorted(f["content"] for f in replay if f["type"] == "chat_message") == ["fake 3", "real 4"]


def test_redelivered_agent_response_is_committed_once(stack):
    with TestClient(stack.app()) as first:
        create(first)
        for _ in range(3):
            publish_agent_response(stack, "only once", message_id="message-real-1")
        wait_for(lambda: stack.stored(first) == 1)
    stack.kafka.committed.clear()  # lost commits: the whole topic is redelivered
    with TestClient(stack.app(pod="pod-b")) as second:
        wait_for(lambda: any(group == "chat-agent-responses-consumer" for group, _ in stack.kafka.committed))
        assert stack.stored(second) == 1


def test_agent_response_registers_a_conversation_for_its_customer(stack, client):
    publish_agent_response(stack, "hello from an agent", conversation_id="conversation-new")
    wait_for(lambda: stack.stored(client, "conversation-new") == 1)
    with client.websocket_connect("/ws/customer/conversation-new") as ws:
        _, replay = connect(ws)
    assert [f["content"] for f in replay if f["type"] == "chat_message"] == ["hello from an agent"]
    with client.websocket_connect("/ws/customer/conversation-new") as ws:
        hello(ws, subject="cust-acme-vic", tenant="acme")
        with pytest.raises(WebSocketDisconnect) as closed:
            recv(ws)
    assert closed.value.code == 4404


@pytest.mark.parametrize(
    "owner_override",
    [{"customer_id": "cust-acme-vic"}, {"tenant_id": "globex"}],
)
def test_agent_response_cannot_write_into_someone_elses_conversation(stack, client, owner_override):
    create(client)
    publish_agent_response(stack, "misrouted", **owner_override)  # customer_id / tenant_id arguments
    wait_for(lambda: stack.kafka.events(CHAT_REPLIES_DLQ))
    assert stack.stored(client) == 0
    letter = stack.kafka.events(CHAT_REPLIES_DLQ)[0]
    assert "another customer or tenant" in letter.payload["reason"]
    assert letter.payload["source_topic"] == "agent.responses"


def test_malformed_agent_responses_are_dead_lettered_and_other_types_ignored(stack, client):
    from engagehub_common.events import EventEnvelope, EventType
    from engagehub_common.topics import AGENT_RESPONSES

    create(client)
    publish_agent_response(stack, "no id", overrides={"message_id": None})
    publish_agent_response(
        stack, "bad sender", overrides={"payload": {"text": "x", "sender": {"type": "robot", "id": "r"}}}
    )
    publish_agent_response(stack, "no text", overrides={"payload": {"sender": {"type": "virtual_agent", "id": "a"}}})
    stack.kafka.append(AGENT_RESPONSES, CONV.encode(), b"not json")
    other = EventEnvelope(
        event_type=EventType.AGENT_ROUTED, conversation_id=CONV, tenant_id="acme", producer_service="x", payload={}
    )
    stack.kafka.append(AGENT_RESPONSES, CONV.encode(), other.to_bytes())
    publish_agent_response(stack, "valid one after the bad ones")
    wait_for(lambda: stack.stored(client) == 1)
    wait_for(lambda: len(stack.kafka.events(CHAT_REPLIES_DLQ)) == 4)
    reasons = sorted(e.payload["reason"].split(":")[0] for e in stack.kafka.events(CHAT_REPLIES_DLQ))
    assert reasons == ["non_retryable", "non_retryable", "non_retryable", "schema_validation"]
    body = client.get(f"/conversations/{CONV}/messages", headers=auth_headers(**DANA)).json()
    assert [m["content"] for m in body["messages"]] == ["valid one after the bad ones"]
