"""Global session state reconciliation and live-chat migration between Chat Service pods.

Each "pod" is a ReplyHub. All pods share one Redis (fakeredis), one message log and
one Kafka (in memory), as replicas would. A crash means abandoning a hub with no
cleanup. A partition means the old hub stays alive, unaware, while a new hub claims
the session.
"""

import asyncio
import json
from datetime import UTC, datetime

import fakeredis.aioredis
import pytest
from app.replies.cursor import CursorStore
from app.replies.hub import ConnectionFenced, CustomerConnection, ReplyHub
from app.replies.log import MessageLog
from app.replies.models import ReplyBase
from app.replies.session import SessionSnapshot, SessionSnapshotStore, StaleSession
from app.replies.session_state import SessionStatePublisher, SessionStateTable, SessionStateUpdated
from engagehub_common.db import Database
from engagehub_common.messaging.consumer import ConsumerRunner, NonRetryableError
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.topics import SESSION_STATE, SESSION_STATE_DLQ
from prometheus_client import REGISTRY

CONV, TENANT, CUSTOMER = "conversation-123", "acme", "cust-acme-dana"


def metric(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


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
        return [f["sequence_number"] for f in self.of("chat_message")]


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


class Cluster:
    """Shared infrastructure for several pods."""

    def __init__(self, redis, db, kafka, clock):
        self.redis, self.db, self.kafka, self.clock = redis, db, kafka, clock
        self.log = MessageLog(db)
        self.platform = SessionStore(redis)
        self.sequence = 0

    def pod(self, name: str) -> ReplyHub:
        table = SessionStateTable()
        hub = ReplyHub(
            self.log,
            CursorStore(self.redis, 3600),
            name,
            sessions=SessionSnapshotStore(self.redis, 3600, 15, clock=self.clock),
            state_table=table,
            publisher=SessionStatePublisher(self.kafka.producer()),
            platform=self.platform,
        )
        table.on_newer = hub.on_session_state
        return hub

    async def reply(self, text: str, sender_id="product_advisor", sender_type="virtual_agent"):
        self.sequence += 1
        result = await self.log.append(
            conversation_id=CONV, message_id=f"message-{self.sequence}-{text}", sender_type=sender_type,
            sender_id=sender_id, sender_name=sender_id.replace("_", " ").title(), content=text,
            timestamp=datetime.now(UTC), correlation_id="corr",
        )  # fmt: skip
        return result.message

    async def sync_session_state(self, *hubs: ReplyHub) -> None:
        """Deliver chat.session-state records to each pod's table, as its consumer would."""
        for record in self.kafka.records(SESSION_STATE):
            event = SessionStateUpdated.parse(record.value)
            for hub in hubs:
                await hub.state_table.handle(event, record)


@pytest.fixture
async def cluster():
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.create_schema(ReplyBase)
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    c = Cluster(redis, db, InMemoryKafka(), Clock())
    await c.log.register(CONV, TENANT, CUSTOMER)
    yield c
    await redis.aclose()
    await db.dispose()


async def connect(hub: ReplyHub, last_received=None) -> tuple[CustomerConnection, FakeSocket]:
    sock = FakeSocket()
    conn = CustomerConnection(CONV, CUSTOMER, sock.send, sock.close, tenant_id=TENANT)
    await hub.open(conn, last_received)
    return conn, sock


async def receive(hub, conn, *messages):
    for message in messages:
        await hub.deliver(message)


# Session snapshot store


async def test_claim_creates_a_versioned_snapshot_with_identity_and_owner(cluster):
    store = SessionSnapshotStore(cluster.redis, 3600, 15, clock=cluster.clock)
    claim = await store.claim(conversation_id=CONV, tenant_id=TENANT, customer_id=CUSTOMER, pod="pod-1",
                              connection_id="c1", fields={"workflow_state": "ACTIVE"})  # fmt: skip
    snap = claim.snapshot
    assert (snap.session_version, snap.current_chat_pod, snap.connection_id, snap.connected) == (1, "pod-1", "c1", True)
    assert (snap.tenant_id, snap.customer_id, snap.workflow_state) == (TENANT, CUSTOMER, "ACTIVE")
    assert claim.existed is False and claim.handover is False
    assert 0 < await cluster.redis.ttl(store.key(CONV, CUSTOMER)) <= 3600


async def test_writes_bump_the_version_and_lease_renewal_does_not(cluster):
    store = SessionSnapshotStore(cluster.redis, 3600, 15, clock=cluster.clock)
    v = (await store.claim(conversation_id=CONV, tenant_id=TENANT, customer_id=CUSTOMER, pod="p", connection_id="c",
                           fields={})).snapshot.session_version  # fmt: skip
    v2 = await store.update(CONV, CUSTOMER, v, {"typing_status": "customer_typing"})
    assert v2 == v + 1
    cluster.clock.now += 10
    await store.renew_lease(CONV, CUSTOMER, v2)
    snap = await store.get(CONV, CUSTOMER)
    assert snap.session_version == v2 and snap.lease_expires_at == cluster.clock.now + 15


async def test_spec_example_old_pod_write_with_older_version_is_rejected(cluster):
    """Pod 1 holds version 10, pod 2 claims (11), pod 1 writes with 10: rejected."""
    store = SessionSnapshotStore(cluster.redis, 3600, 15, clock=cluster.clock)
    version = (await store.claim(conversation_id=CONV, tenant_id=TENANT, customer_id=CUSTOMER, pod="chat-service-1",
                                 connection_id="c1", fields={})).snapshot.session_version  # fmt: skip
    for _ in range(9):
        version = await store.update(CONV, CUSTOMER, version, {"last_sent_sequence": version})
    assert version == 10
    claim = await store.claim(conversation_id=CONV, tenant_id=TENANT, customer_id=CUSTOMER, pod="chat-service-2",
                              connection_id="c2", fields={})  # fmt: skip
    assert claim.snapshot.session_version == 11 and claim.handover
    with pytest.raises(StaleSession) as rejected:
        await store.update(CONV, CUSTOMER, 10, {"last_sent_sequence": 99})
    assert (rejected.value.stored_version, rejected.value.owner_pod) == (11, "chat-service-2")
    assert (await store.get(CONV, CUSTOMER)).last_sent_sequence != 99


async def test_put_if_newer_only_accepts_newer_versions(cluster):
    store = SessionSnapshotStore(cluster.redis, 3600, 15)
    base = SessionSnapshot(conversation_id=CONV, tenant_id=TENANT, customer_id=CUSTOMER, session_version=5)
    assert await store.put_if_newer(base)
    assert not await store.put_if_newer(base.model_copy(update={"session_version": 5, "typing_status": "x"}))
    assert not await store.put_if_newer(base.model_copy(update={"session_version": 4}))
    assert await store.put_if_newer(base.model_copy(update={"session_version": 6, "workflow_state": "HUMAN_ACTIVE"}))
    assert (await store.get(CONV, CUSTOMER)).workflow_state == "HUMAN_ACTIVE"


async def test_legacy_connection_marker_is_migrated_on_claim(cluster):
    store = SessionSnapshotStore(cluster.redis, 3600, 15)
    await cluster.redis.set(store.key(CONV, CUSTOMER), json.dumps({"pod": "old-pod", "connected_at": "x"}))
    claim = await store.claim(conversation_id=CONV, tenant_id=TENANT, customer_id=CUSTOMER, pod="new-pod",
                              connection_id="c", fields={})  # fmt: skip
    assert claim.previous_pod == "old-pod" and claim.handover
    assert await cluster.redis.type(store.key(CONV, CUSTOMER)) == "hash"


# Compacted session-state topic


def snapshot(version, **fields):
    return SessionSnapshot(conversation_id=CONV, tenant_id=TENANT, customer_id=CUSTOMER, session_version=version,
                           **fields)  # fmt: skip


async def test_state_table_keeps_the_newest_version_and_rejects_stale_events():
    seen = []

    async def on_newer(s):
        seen.append(s.session_version)

    table = SessionStateTable(on_newer)
    for version in (3, 1, 5, 4, 5):
        event = SessionStateUpdated.from_snapshot(snapshot(version), "test")
        await table.handle(event, None)
    assert table.get(CONV).session_version == 5 and seen == [3, 5]


def test_session_state_event_carries_the_required_fields():
    event = SessionStateUpdated.from_snapshot(
        snapshot(12, last_acknowledged_sequence=5, assigned_agent_id="agent-1", workflow_state="ACTIVE"), "ack"
    )
    raw = json.loads(event.model_dump_json())
    required = (
        "event_id", "event_type", "conversation_id", "tenant_id", "customer_id", "session_version",
        "last_acknowledged_sequence", "assigned_agent_id", "workflow_state", "timestamp", "payload",
    )  # fmt: skip
    for name in required:
        assert name in raw
    assert raw["event_type"] == "SessionStateUpdated" and raw["session_version"] == 12


def test_session_state_event_whose_payload_disagrees_is_rejected():
    event = SessionStateUpdated.from_snapshot(snapshot(3), "x")
    event.payload["customer_id"] = "someone-else"
    with pytest.raises(NonRetryableError):
        event.snapshot()


async def test_invalid_session_state_events_are_dead_lettered(cluster):
    kafka = cluster.kafka
    good = SessionStateUpdated.from_snapshot(snapshot(2), "x")
    forged = SessionStateUpdated.from_snapshot(snapshot(3), "x")
    forged.payload["tenant_id"] = "globex"
    kafka.append(SESSION_STATE, CONV.encode(), b'{"event_type": "SessionStateUpdated"}')
    kafka.append(SESSION_STATE, CONV.encode(), forged.model_dump_json().encode())
    kafka.append(SESSION_STATE, CONV.encode(), good.model_dump_json().encode())
    table = SessionStateTable()
    consumer = kafka.consumer([SESSION_STATE], None)
    await consumer.start()
    runner = ConsumerRunner(consumer, table.handle, group="session-state-test", producer=kafka.producer(),
                            parser=SessionStateUpdated.parse, dead_letter_topic=SESSION_STATE_DLQ,
                            retry_base_seconds=0, commit=False)  # fmt: skip
    await runner.process_once()
    assert table.get(CONV).session_version == 2
    assert len(kafka.events(SESSION_STATE_DLQ)) == 2


# Handover between pods


async def test_handover_after_pod_crash_replays_only_unacknowledged_messages(cluster):
    pod_1, pod_2 = cluster.pod("chat-service-1"), cluster.pod("chat-service-2")
    conn_1, sock_1 = await connect(pod_1)
    messages = [await cluster.reply(f"reply {n}") for n in range(1, 6)]
    await receive(pod_1, conn_1, *messages)
    assert sock_1.seqs == [1, 2, 3, 4, 5]
    await pod_1.acknowledge(conn_1, 3, messages[2].message_id)
    version_on_pod_1 = conn_1.session_version

    # chat-service-1 crashes: no cleanup. Two more replies are stored meanwhile.
    late = [await cluster.reply(f"reply {n}") for n in (6, 7)]
    handovers_before = metric("engagehub_handovers_total", {"reason": "takeover_while_owner_alive"})
    conn_2, sock_2 = await connect(pod_2)

    ack = sock_2.of("connection_ack")[0]
    assert ack["handover"] and ack["previous_pod"] == "chat-service-1" and ack["pod"] == "chat-service-2"
    assert ack["session_version"] > version_on_pod_1 and ack["cursor"]["last_acknowledged_sequence"] == 3
    started = sock_2.of("handover_started")[0]
    assert started == {"type": "handover_started", "conversation_id": CONV, "old_pod": "chat-service-1",
                       "new_pod": "chat-service-2", "reason": "takeover_while_owner_alive",
                       "session_version": ack["session_version"]}  # fmt: skip
    assert sock_2.seqs == [4, 5, 6, 7]  # unacknowledged (4, 5) and missed (6, 7), in order
    done = sock_2.of("handover_completed")[0]
    assert (done["replayed_from_sequence"], done["replayed_to_sequence"], done["replayed_count"]) == (4, 7, 4)
    assert done["session_version"] >= ack["session_version"] and done["recovery_ms"] >= 0
    order = [f["type"] for f in sock_2.frames if f["type"] != "chat_message"]
    assert order == ["connection_ack", "handover_started", "replay_started", "replay_completed", "handover_completed"]
    assert metric("engagehub_handovers_total", {"reason": "takeover_while_owner_alive"}) == handovers_before + 1

    # Live delivery continues on the new pod; acknowledged state was kept.
    await receive(pod_2, conn_2, late[-1], await cluster.reply("reply 8"))
    assert sock_2.seqs == [4, 5, 6, 7, 8]
    cursor = await pod_2.acknowledge(conn_2, 8, sock_2.of("chat_message")[-1]["message_id"])
    assert cursor["last_acknowledged_sequence"] == 8 and cursor["session_version"] == conn_2.session_version


async def test_partition_old_pod_is_fenced_on_its_next_write(cluster):
    """chat-service-1 is alive but cut off from the client, which moved to chat-service-2."""
    pod_1, pod_2 = cluster.pod("chat-service-1"), cluster.pod("chat-service-2")
    conn_1, sock_1 = await connect(pod_1)
    await receive(pod_1, conn_1, await cluster.reply("before the partition"))
    conn_2, sock_2 = await connect(pod_2)
    rejected_before = metric("engagehub_stale_pod_updates_rejected_total")

    # The partition heals; the fan-out reaches chat-service-1, which tries to deliver.
    late = await cluster.reply("after the partition")
    await pod_1.deliver(late)
    await pod_2.deliver(late)
    assert sock_1.seqs == [1]  # the old pod did not deliver again
    assert sock_1.closed == 4409 and sock_1.of("session_superseded")[0]["owner_pod"] == "chat-service-2"
    assert sock_2.seqs == [1, 2]
    assert metric("engagehub_stale_pod_updates_rejected_total") == rejected_before + 1
    with pytest.raises(ConnectionFenced):
        await pod_1.acknowledge(conn_1, 1)
    snap = await pod_2.sessions.get(CONV, CUSTOMER)
    assert snap.current_chat_pod == "chat-service-2" and snap.connection_id == conn_2.connection_id


async def test_lease_renewal_fences_a_superseded_connection(cluster):
    pod_1, pod_2 = cluster.pod("chat-service-1"), cluster.pod("chat-service-2")
    _, sock_1 = await connect(pod_1)
    await connect(pod_2)
    await pod_1.renew_leases()
    assert sock_1.closed == 4409


async def test_newer_session_state_event_on_kafka_fences_the_old_socket(cluster):
    """Fencing still works when the old pod can reach Kafka but has nothing to write to Redis."""
    pod_1, pod_2 = cluster.pod("chat-service-1"), cluster.pod("chat-service-2")
    _, sock_1 = await connect(pod_1)
    await connect(pod_2)
    await cluster.sync_session_state(pod_1)
    assert sock_1.closed == 4409


async def test_owner_with_expired_lease_is_reported_as_unreachable(cluster):
    pod_1, pod_2 = cluster.pod("chat-service-1"), cluster.pod("chat-service-2")
    await connect(pod_1)
    cluster.clock.now += 60  # chat-service-1 stopped renewing: its lease ran out
    _, sock_2 = await connect(pod_2)
    assert sock_2.of("handover_started")[0]["reason"] == "owner_unreachable"


async def test_agent_assignment_workflow_and_typing_survive_the_handover(cluster):
    await cluster.platform.create(
        SessionState(
            conversation_id=CONV, session_id="s", tenant_id=TENANT, customer_id=CUSTOMER, workflow_state="waiting_human"
        )  # fmt: skip
    )
    pod_1, pod_2 = cluster.pod("chat-service-1"), cluster.pod("chat-service-2")
    conn_1, _ = await connect(pod_1)
    await receive(pod_1, conn_1, await cluster.reply("Let me check", sender_id="order_tracking"))
    await pod_1.typing(conn_1, True)

    _, sock_2 = await connect(pod_2)
    ack = sock_2.of("connection_ack")[0]
    assert ack["typing_status"] == "customer_typing"
    assert (ack["agent_kind"], ack["assigned_agent_id"]) == ("virtual_agent", "order_tracking")
    assert ack["workflow_state"] == "PENDING_HANDOFF"


async def test_human_assignment_from_support_chat_is_restored(cluster):
    await cluster.platform.create(
        SessionState(
            conversation_id=CONV,
            session_id="s",
            tenant_id=TENANT,
            customer_id=CUSTOMER,
            workflow_state="human_active",
            assigned_agent_type="human",
            assigned_agent_id="agent-acme-alice",
            assigned_agent_name="Alice Morgan",
        )  # fmt: skip
    )
    _, sock = await connect(cluster.pod("chat-service-1"))
    ack = sock.of("connection_ack")[0]
    assert (ack["agent_kind"], ack["assigned_agent_id"], ack["workflow_state"]) == (
        "human_agent", "agent-acme-alice", "HUMAN_ACTIVE",
    )  # fmt: skip


async def test_lost_redis_snapshot_is_restored_from_kafka_without_rewinding_versions(cluster):
    pod_1 = cluster.pod("chat-service-1")
    conn_1, _ = await connect(pod_1)
    messages = [await cluster.reply(f"r{n}") for n in (1, 2)]
    await receive(pod_1, conn_1, *messages)
    await pod_1.acknowledge(conn_1, 2, messages[1].message_id)
    kafka_version = conn_1.session_version

    # Redis loses the snapshot and the cursor (for example a failover that dropped recent writes).
    await cluster.redis.delete(pod_1.sessions.key(CONV, CUSTOMER), CursorStore.cursor_key(CONV, CUSTOMER))
    pod_2 = cluster.pod("chat-service-2")
    await cluster.sync_session_state(pod_2)
    restores_before = metric("engagehub_session_restores_total", {"source": "kafka"})
    _, sock_2 = await connect(pod_2)
    ack = sock_2.of("connection_ack")[0]
    assert ack["session_version"] > kafka_version  # versions keep increasing
    assert ack["cursor"]["last_acknowledged_sequence"] == 2 and ack["cursor_source"] == "session_snapshot"
    assert sock_2.seqs == []  # nothing unacknowledged, nothing replayed
    assert metric("engagehub_session_restores_total", {"source": "kafka"}) == restores_before + 1


async def test_redis_outage_resumes_from_the_kafka_snapshot(cluster):
    pod_1 = cluster.pod("chat-service-1")
    conn_1, _ = await connect(pod_1)
    messages = [await cluster.reply(f"r{n}") for n in (1, 2, 3)]
    await receive(pod_1, conn_1, *messages)
    await pod_1.acknowledge(conn_1, 2, messages[1].message_id)
    pod_2 = cluster.pod("chat-service-2")
    await cluster.sync_session_state(pod_2)

    class Down:
        def __getattr__(self, name):
            async def fail(*args, **kwargs):
                from redis.exceptions import ConnectionError

                raise ConnectionError("down")

            return fail

    pod_2.sessions.redis = Down()
    pod_2.cursors.redis = Down()
    _, sock_2 = await connect(pod_2)
    ack = sock_2.of("connection_ack")[0]
    assert ack["session_source"] == "kafka_session_state" and ack["cursor_source"] == "kafka_session_state"
    assert sock_2.seqs == [3]


async def test_rolling_update_drain_marks_the_session_and_the_next_pod_reports_it(cluster):
    pod_1, pod_2 = cluster.pod("chat-service-1"), cluster.pod("chat-service-2")
    conn_1, sock_1 = await connect(pod_1)
    await receive(pod_1, conn_1, await cluster.reply("r1"))
    pod_1.drain()
    assert pod_1.draining
    await pod_1.shutdown()
    assert sock_1.of("server_draining")[0]["reconnect"] is True and sock_1.closed == 1012
    _, sock_2 = await connect(pod_2)
    assert sock_2.of("handover_started")[0]["reason"] == "server_draining"
    assert sock_2.seqs == [1]  # never acknowledged, so replayed


async def test_clean_disconnect_then_reconnect_on_same_pod_is_not_a_handover(cluster):
    pod = cluster.pod("chat-service-1")
    conn, _ = await connect(pod)
    await pod.disconnect(conn, "client_closed")
    _, sock = await connect(pod)
    ack = sock.of("connection_ack")[0]
    assert ack["reconnected"] and not ack["handover"] and not sock.of("handover_started")


async def test_concurrent_claims_leave_exactly_one_owner(cluster):
    pods = [cluster.pod(f"chat-service-{n}") for n in range(1, 5)]
    connections = await asyncio.gather(*(connect(p) for p in pods))
    versions = sorted(c.session_version for c, _ in connections)
    assert len(set(versions)) == 4  # every claim got its own version
    message = await cluster.reply("who owns this?")
    for pod in pods:
        await pod.deliver(message)
    delivered = [sock for _, sock in connections if sock.seqs == [1]]
    fenced = [sock for _, sock in connections if sock.closed == 4409]
    assert len(delivered) == 1 and len(fenced) == 3


async def test_gap_in_live_fanout_is_filled_from_the_log_and_counted(cluster):
    pod = cluster.pod("chat-service-1")
    conn, sock = await connect(pod)
    messages = [await cluster.reply(f"r{n}") for n in range(1, 5)]
    gaps_before = metric("engagehub_message_gaps_detected_total", {"where": "server"})
    await receive(pod, conn, messages[0], messages[1], messages[3], messages[3], messages[2])
    assert sock.seqs == [1, 2, 3, 4]  # 3 filled before 4; the repeat of 4 and late 3 dropped
    assert metric("engagehub_message_gaps_detected_total", {"where": "server"}) == gaps_before + 1


async def test_acknowledged_messages_are_not_replayed_and_unacknowledged_ones_are(cluster):
    pod_1 = cluster.pod("chat-service-1")
    conn, _ = await connect(pod_1)
    messages = [await cluster.reply(f"r{n}") for n in range(1, 11)]
    await receive(pod_1, conn, *messages)
    await pod_1.acknowledge(conn, 6, messages[5].message_id)
    for n in range(2, 5):  # a chain of failures across pods
        _, sock = await connect(cluster.pod(f"chat-service-{n}"))
        assert sock.seqs == [7, 8, 9, 10]


async def test_recovery_metrics_are_recorded(cluster):
    count_before = metric("engagehub_recovery_seconds_count")
    claims_before = metric("engagehub_session_claims_total", {"kind": "handover"})
    await connect(cluster.pod("chat-service-1"))
    await connect(cluster.pod("chat-service-2"))
    assert metric("engagehub_recovery_seconds_count") == count_before + 1
    assert metric("engagehub_session_claims_total", {"kind": "handover"}) == claims_before + 1


async def test_snapshot_tracks_sent_pending_and_acknowledged_messages(cluster):
    pod = cluster.pod("chat-service-1")
    conn, _ = await connect(pod)
    messages = [await cluster.reply(f"r{n}") for n in (1, 2, 3)]
    await receive(pod, conn, *messages)
    await pod.acknowledge(conn, 1, messages[0].message_id)
    snap = await pod.sessions.get(CONV, CUSTOMER)
    assert snap.last_sent_sequence == 3 and snap.last_acknowledged_sequence == 1
    assert snap.pending_message_ids == [m.message_id for m in messages[1:]]
    published = [SessionStateUpdated.parse(r.value) for r in cluster.kafka.records(SESSION_STATE)]
    assert published[-1].last_acknowledged_sequence == 1 and published[-1].session_version == snap.session_version
