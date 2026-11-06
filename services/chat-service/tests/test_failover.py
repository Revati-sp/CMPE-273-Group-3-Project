"""Failure tests for live-chat migration, with two pods sharing Redis and Kafka.

"Killing" a pod means abandoning its object with no cleanup, as a crash would.
"""

import pytest
from app.core import ChatCore, ConnectionRejected
from app.protocol import HelloFrame
from engagehub_common.auth import Principal
from engagehub_common.config import ServiceSettings
from engagehub_common.events import EventEnvelope, EventType, sender
from engagehub_common.session.log import ConversationLog
from engagehub_common.topics import CHAT_MESSAGES, SESSION_EVENTS

CUSTOMER = Principal("cust-1", "acme", "customer", "Dana", "cust-1")


class FakeSocket:
    def __init__(self):
        self.frames = []
        self.closed = None

    async def send(self, frame):
        if self.closed:
            raise RuntimeError("socket closed")
        self.frames.append(frame)

    async def close(self, code, reason):
        self.closed = code

    def of(self, kind):
        return [f for f in self.frames if f["type"] == kind]

    @property
    def seqs(self):
        return [f["sequence_number"] for f in self.of("message")]


def make_pod(name, store, kafka, history):
    settings = ServiceSettings(service_name="chat-service", otel_enabled=False)
    return ChatCore(settings, store, kafka.producer(), history, pod_name=name)


async def start(pod):
    state = await pod.create_conversation(CUSTOMER, subject="", language="en", correlation_id="corr")
    return state.conversation_id


async def connect(pod, conversation_id, last_seen=None, principal=CUSTOMER):
    sock = FakeSocket()
    conn = await pod.open_connection(
        principal,
        HelloFrame(type="hello", token="x", conversation_id=conversation_id, last_seen_seq=last_seen),
        sock.send,
        sock.close,
    )
    return conn, sock


async def say(pod, conn, text, client_id):
    await pod.handle_frame(conn, {"type": "message", "client_message_id": client_id, "text": text})


async def agent_reply(store, kafka, conversation_id, text, key):
    log = ConversationLog(store, kafka.producer(), "virtual-agent-service")
    result = await log.append(
        tenant_id="acme",
        conversation_id=conversation_id,
        customer_id="cust-1",
        event_type=EventType.AGENT_MESSAGE,
        payload={"text": text, "sender": sender("virtual_agent", "faq", "FAQ")},
        idempotency_key=key,
    )
    return result.event


@pytest.fixture
def pods(store, kafka, history):
    return make_pod("pod-a", store, kafka, history), make_pod("pod-b", store, kafka, history)


async def test_ack_is_sent_only_after_kafka_confirms(pods, kafka):
    pod_a, _ = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)

    kafka.fail_publishes = 1
    await say(pod_a, conn, "hello", "client-msg-1")
    assert sock.of("message_ack") == []
    assert sock.of("error")[-1]["retryable"] is True

    await say(pod_a, conn, "hello", "client-msg-1")
    assert len(sock.of("message_ack")) == 1
    texts = [e.payload["text"] for e in kafka.events(CHAT_MESSAGES)]
    assert texts.count("hello") == 1


async def test_acknowledged_messages_survive_a_pod_crash(pods, kafka, store):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)
    await say(pod_a, conn, "first", "client-msg-1")
    await say(pod_a, conn, "second", "client-msg-2")
    acked = [f["message"] for f in sock.of("message_ack")]
    assert [m["text"] for m in acked] == ["first", "second"]

    # pod-a crashes with no cleanup. The client lost everything except its ack records.
    del pod_a, conn
    _, new_sock = await connect(pod_b, conv, last_seen=0)

    welcome = new_sock.of("welcome")[0]
    assert welcome["migrated"] is True and welcome["previous_pod"] == "pod-a" and welcome["pod"] == "pod-b"
    replayed = {f["message_id"]: f["text"] for f in new_sock.of("message")}
    for message in acked:
        assert replayed[message["message_id"]] == message["text"]


async def test_only_missing_messages_are_replayed_after_reconnect(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)  # seq 1 is the greeting
    conn, sock = await connect(pod_a, conv, last_seen=0)
    await say(pod_a, conn, "where is my order", "client-msg-1")  # seq 2
    await pod_a.handle_frame(conn, {"type": "ack", "up_to_seq": 2})
    await agent_reply(store, kafka, conv, "It shipped", "reply-1")  # seq 3, missed by the crashed pod
    await agent_reply(store, kafka, conv, "Tracking is 123", "reply-2")  # seq 4

    _, new_sock = await connect(pod_b, conv, last_seen=2)
    assert new_sock.seqs == [3, 4]
    done = new_sock.of("replay_complete")[0]
    assert done["through_seq"] == 4 and done["complete"] and done["source"] == "redis"
    assert all(f["replayed"] for f in new_sock.of("message"))


async def test_resume_point_falls_back_to_the_servers_last_ack(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, _ = await connect(pod_a, conv, last_seen=0)
    await pod_a.handle_frame(conn, {"type": "ack", "up_to_seq": 1, "message_id": "m1"})
    await agent_reply(store, kafka, conv, "reply", "reply-1")

    _, new_sock = await connect(pod_b, conv, last_seen=None)
    welcome = new_sock.of("welcome")[0]
    assert welcome["resume_after_seq"] == 1 and welcome["last_ack_message_id"] == "m1"
    assert new_sock.seqs == [2]


async def test_client_retry_after_crash_does_not_duplicate(pods, kafka):
    """Acknowledged, but the ack frame was lost in the crash. The client resends."""
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)
    await say(pod_a, conn, "only once", "client-msg-1")
    original = sock.of("message_ack")[0]["message"]

    new_conn, new_sock = await connect(pod_b, conv, last_seen=1)
    await say(pod_b, new_conn, "only once", "client-msg-1")
    ack = new_sock.of("message_ack")[0]
    assert ack["duplicate"] is True
    assert ack["message"]["message_id"] == original["message_id"]
    assert ack["message"]["sequence_number"] == original["sequence_number"]
    assert [e.payload["text"] for e in kafka.events(CHAT_MESSAGES)].count("only once") == 1


async def test_writer_crash_between_reserve_and_publish_is_reconciled(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)
    kafka.fail_publishes = 1
    await say(pod_a, conn, "reserved but not published", "client-msg-1")
    assert list(await store.pending("acme", conv)) == [2]
    # Age the reservation past the grace period, as a real crash would by the time a client returns.
    key = store._key("acme", conv, "pending")
    value = await store.redis.hget(key, "2")
    await store.redis.hset(key, "2", "1|" + value.split("|", 1)[1])

    _, new_sock = await connect(pod_b, conv, last_seen=1)
    assert new_sock.seqs == [2]
    assert new_sock.of("message")[0]["text"] == "reserved but not published"
    assert await store.pending("acme", conv) == {}


async def test_young_reservation_is_not_reported_as_settled(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, _ = await connect(pod_a, conv, last_seen=0)
    kafka.fail_publishes = 1
    await say(pod_a, conn, "in flight", "client-msg-1")
    _, new_sock = await connect(pod_b, conv, last_seen=1)
    assert new_sock.seqs == []
    assert new_sock.of("replay_complete")[0]["through_seq"] == 1  # the client must wait for seq 2


async def test_replay_falls_back_to_durable_history_when_buffer_was_trimmed(pods, store, kafka, history):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, _ = await connect(pod_a, conv, last_seen=0)
    await say(pod_a, conn, "hi", "client-msg-1")
    await pod_a.handle_frame(conn, {"type": "ack", "up_to_seq": 2})
    assert await store.unacked_count("acme", conv) == 0

    # A new device holds nothing, so it needs the acknowledged history as well.
    _, new_sock = await connect(pod_b, conv, last_seen=0)
    assert new_sock.seqs == [1, 2]
    assert new_sock.of("replay_complete")[0]["source"] == "durable"
    assert history.calls == 1


async def test_partial_replay_is_reported_when_durable_history_is_down(pods, store, kafka, history):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, _ = await connect(pod_a, conv, last_seen=0)
    await say(pod_a, conn, "hi", "client-msg-1")
    await pod_a.handle_frame(conn, {"type": "ack", "up_to_seq": 2})
    await agent_reply(store, kafka, conv, "buffered", "reply-1")  # seq 3
    history.available = False

    new_conn, new_sock = await connect(pod_b, conv, last_seen=0)
    done = new_sock.of("replay_complete")[0]
    assert done["complete"] is False and done["through_seq"] == 0  # the client must not skip 1 and 2

    history.available = True
    await pod_b.handle_frame(new_conn, {"type": "resync", "after_seq": 0})
    assert sorted(new_sock.seqs) == [1, 2, 3]
    assert new_sock.of("replay_complete")[-1]["complete"] is True


async def test_full_redis_loss_rebuilds_from_durable_storage(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)
    await say(pod_a, conn, "survives redis loss", "client-msg-1")

    await store.delete_all("acme", conv)
    new_conn, new_sock = await connect(pod_b, conv, last_seen=1)
    assert new_sock.seqs == [2]

    await say(pod_b, new_conn, "after recovery", "client-msg-2")
    assert new_sock.of("message_ack")[0]["message"]["sequence_number"] == 3  # no sequence reuse


async def test_old_socket_is_fenced_after_takeover_during_partition(pods, store, kafka):
    """pod-a stayed alive but was partitioned from the client, which moved to pod-b."""
    pod_a, pod_b = pods
    conv = await start(pod_a)
    old_conn, old_sock = await connect(pod_a, conv, last_seen=0)
    new_conn, _ = await connect(pod_b, conv, last_seen=1)

    await say(pod_a, old_conn, "from zombie", "client-msg-1")
    assert old_sock.of("error")[-1]["code"] == "superseded"
    assert old_sock.closed == 4409
    assert "from zombie" not in [e.payload.get("text") for e in kafka.events(CHAT_MESSAGES)]


async def test_takeover_event_closes_old_socket_without_waiting_for_a_write(pods, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    _, old_sock = await connect(pod_a, conv, last_seen=0)
    await connect(pod_b, conv, last_seen=1)

    migrated = [e for e in kafka.events(SESSION_EVENTS) if e.event_type == EventType.SESSION_MIGRATED]
    assert len(migrated) == 1
    await pod_a.dispatch(migrated[0])
    assert old_sock.closed == 4409


async def test_fanout_delivers_live_messages_once(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    _, sock = await connect(pod_b, conv, last_seen=1)
    event = await agent_reply(store, kafka, conv, "hello", "reply-1")
    await pod_b.dispatch(event)
    await pod_b.dispatch(event)  # Kafka redelivery or a republished duplicate
    assert sock.seqs == [2]


async def test_messages_published_during_replay_are_not_lost(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)

    original = pod_b.compute_replay

    async def replay_with_live_event(tenant, conversation, after):
        result = await original(tenant, conversation, after)
        event = await agent_reply(store, kafka, conv, "arrived during replay", "reply-1")
        await pod_b.dispatch(event)
        return result

    pod_b.compute_replay = replay_with_live_event
    _, sock = await connect(pod_b, conv, last_seen=0)
    assert sock.seqs == [1, 2]


async def test_ack_beyond_head_is_rejected(pods):
    pod_a, _ = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)
    await pod_a.handle_frame(conn, {"type": "ack", "up_to_seq": 500})
    assert sock.of("error")[-1]["code"] == "invalid_ack"


async def test_client_claiming_future_position_still_gets_later_messages(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=99)
    assert sock.of("welcome")[0]["resume_after_seq"] == 1
    event = await agent_reply(store, kafka, conv, "next", "reply-1")
    await pod_a.dispatch(event)
    assert sock.seqs == [2]


async def test_invalid_frames_are_rejected_without_closing(pods):
    pod_a, _ = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)
    await pod_a.handle_frame(conn, {"type": "message", "client_message_id": "short", "text": ""})
    await pod_a.handle_frame(conn, {"type": "nonsense"})
    await pod_a.handle_frame(conn, {"type": "message", "client_message_id": "long-enough", "text": "x" * 5000})
    assert [e["code"] for e in sock.of("error")] == ["invalid_frame"] * 3
    await pod_a.handle_frame(conn, {"type": "ping"})
    assert sock.of("pong")


async def test_other_tenant_and_other_customer_cannot_connect(pods):
    pod_a, _ = pods
    conv = await start(pod_a)
    with pytest.raises(ConnectionRejected) as other_tenant:
        await connect(pod_a, conv, principal=Principal("cust-9", "globex", "customer", "X", "cust-9"))
    assert other_tenant.value.code == 4404
    with pytest.raises(ConnectionRejected) as other_customer:
        await connect(pod_a, conv, principal=Principal("cust-9", "acme", "customer", "X", "cust-9"))
    assert other_customer.value.code == 4404


async def test_unassigned_agent_is_read_only_and_assigned_agent_can_send(pods, store):
    pod_a, _ = pods
    conv = await start(pod_a)
    agent = Principal("agent-1", "acme", "agent", "Alice")
    conn, sock = await connect(pod_a, conv, last_seen=0, principal=agent)
    await say(pod_a, conn, "hello", "agent-msg-1")
    assert sock.of("error")[-1]["code"] == "not_assigned"

    await store.update("acme", conv, assigned_agent_id="agent-1", assigned_agent_type="human")
    await say(pod_a, conn, "hello", "agent-msg-1")
    assert sock.of("message_ack")[0]["message"]["event_type"] == "human_message"


async def test_resolved_conversation_rejects_new_messages(pods, store):
    pod_a, _ = pods
    conv = await start(pod_a)
    conn, sock = await connect(pod_a, conv, last_seen=0)
    await store.update("acme", conv, workflow_state="resolved")
    await say(pod_a, conn, "hello?", "client-msg-1")
    assert sock.of("error")[-1] == {
        "type": "error", "code": "conversation_resolved", "retryable": False, "client_message_id": "client-msg-1",
    }  # fmt: skip


async def test_sequence_numbers_are_ordered_across_pods(pods, store, kafka):
    pod_a, pod_b = pods
    conv = await start(pod_a)
    conn_a, _ = await connect(pod_a, conv, last_seen=0)
    agent = Principal("agent-1", "acme", "agent", "Alice")
    await store.update("acme", conv, assigned_agent_id="agent-1", assigned_agent_type="human")
    conn_b, _ = await connect(pod_b, conv, last_seen=0, principal=agent)
    for i in range(10):
        pod, conn = (pod_a, conn_a) if i % 2 == 0 else (pod_b, conn_b)
        await say(pod, conn, f"m{i}", f"client-msg-{i}")
    events = [e for e in kafka.events(CHAT_MESSAGES) if e.conversation_id == conv]
    assert [e.sequence_number for e in events] == list(range(1, 12))
    assert len({e.message_id for e in events}) == 11
    assert len({kafka.partition_for(e.partition_key.encode()) for e in events}) == 1


async def test_shutdown_closes_sockets_with_restart_code(pods):
    pod_a, _ = pods
    conv = await start(pod_a)
    _, sock = await connect(pod_a, conv, last_seen=0)
    await pod_a.shutdown()
    assert sock.closed == 1012


async def test_published_events_validate_against_the_schema(pods, kafka):
    pod_a, _ = pods
    conv = await start(pod_a)
    conn, _ = await connect(pod_a, conv, last_seen=0)
    await say(pod_a, conn, "valid", "client-msg-1")
    for record in kafka.records(CHAT_MESSAGES):
        event = EventEnvelope.from_bytes(record.value)
        assert record.key == conv.encode()
        assert event.tenant_id == "acme" and event.producer_service == "chat-service"
