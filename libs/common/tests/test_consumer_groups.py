"""Consumer-group semantics that horizontal scaling relies on.

Replicas of one worker type share a group id. Kafka gives each partition to one member,
so each task is handled by one replica, and a conversation (one key, one partition) is
handled in order. When a replica dies, its partitions move to the survivors, which
resume from the last committed offset: uncommitted work is redone, not lost.
"""

import asyncio

from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.messaging.consumer import ConsumerRunner
from engagehub_common.messaging.memory import InMemoryKafka

TOPIC = "chat.agent-tasks.product_advisor"
GROUP = "va-product_advisor"


def task(conversation: str, n: int) -> EventEnvelope:
    return EventEnvelope(
        event_id=f"{conversation}-{n}",
        event_type=EventType.AGENT_TASK,
        conversation_id=conversation,
        tenant_id="acme",
        producer_service="test",
        payload={"n": n},
    )


async def publish(kafka, conversations=20, per_conversation=5):
    producer = kafka.producer()
    for n in range(per_conversation):
        for c in range(conversations):
            await producer.publish(TOPIC, task(f"conv-{c}", n))


class Replica:
    def __init__(self, kafka, name, group=GROUP, delay=0.0):
        self.name = name
        self.handled: list[tuple[str, int]] = []
        self.delay = delay
        self.consumer = kafka.consumer([TOPIC], group)
        self.runner = ConsumerRunner(self.consumer, self.handle, group=group, producer=kafka.producer(),
                                     retry_base_seconds=0)  # fmt: skip

    async def handle(self, event, _record):
        if self.delay:
            await asyncio.sleep(self.delay)
        self.handled.append((event.conversation_id, event.payload["n"]))

    async def start(self):
        await self.consumer.start()
        self.runner.started = True


async def drain(*replicas, rounds=50):
    for _ in range(rounds):
        counts = await asyncio.gather(*(r.runner.process_once() for r in replicas))
        if not any(counts):
            return


async def test_replicas_in_one_group_split_partitions_and_process_each_task_once():
    kafka = InMemoryKafka(partitions=12, group_balancing=True)
    await publish(kafka)
    replicas = [Replica(kafka, f"product-advisor-{i}") for i in range(3)]
    for r in replicas:
        await r.start()
    assignments = [kafka.assignment(r.consumer) for r in replicas]  # what the coordinator hands out now
    assert all(len(a) == 4 for a in assignments)
    assert set.union(*assignments) == {(TOPIC, p) for p in range(12)}
    assert not (assignments[0] & assignments[1])

    await drain(*replicas)
    handled = [item for r in replicas for item in r.handled]
    assert len(handled) == 100 and len(set(handled)) == 100  # every task, exactly once
    assert all(r.handled for r in replicas)  # the work was spread across replicas


async def test_each_conversation_is_handled_by_one_replica_in_order():
    kafka = InMemoryKafka(partitions=12, group_balancing=True)
    await publish(kafka)
    replicas = [Replica(kafka, f"r{i}") for i in range(3)]
    for r in replicas:
        await r.start()
    await drain(*replicas)
    for c in range(20):
        owners = [r for r in replicas if any(conv == f"conv-{c}" for conv, _ in r.handled)]
        assert len(owners) == 1
        assert [n for conv, n in owners[0].handled if conv == f"conv-{c}"] == list(range(5))


async def test_a_crashed_replica_loses_no_task():
    kafka = InMemoryKafka(partitions=12, group_balancing=True)
    await publish(kafka)
    a, b, c = (Replica(kafka, n) for n in ("a", "b", "c"))
    for r in (a, b, c):
        await r.start()
    a.runner.commit = False  # a processes some tasks but dies before committing them
    await a.runner.process_once()
    processed_by_a = list(a.handled)
    assert processed_by_a
    a.consumer.crash()
    await drain(b, c)
    survivors = set(b.handled) | set(c.handled)
    everything = {(f"conv-{i}", n) for i in range(20) for n in range(5)}
    assert survivors == everything  # a's uncommitted work was redelivered, nothing lost
    assert set(processed_by_a) <= survivors  # redone, which is why handlers are idempotent


async def test_a_new_replica_takes_a_share_of_the_partitions():
    kafka = InMemoryKafka(partitions=12, group_balancing=True)
    a = Replica(kafka, "a")
    await a.start()
    assert len(a.consumer.positions) == 12
    b = Replica(kafka, "b")
    await b.start()
    await a.runner.process_once()  # rebalance takes effect on the next poll
    assert len(a.consumer.positions) == 6 and len(b.consumer.positions) == 6


async def test_different_groups_each_receive_every_task():
    kafka = InMemoryKafka(partitions=12, group_balancing=True)
    await publish(kafka, conversations=4, per_conversation=2)
    workers = Replica(kafka, "worker", group=GROUP)
    analytics = Replica(kafka, "analytics", group="analytics-service")
    for r in (workers, analytics):
        await r.start()
    await drain(workers, analytics)
    assert len(workers.handled) == len(analytics.handled) == 8


async def test_replicas_process_concurrently():
    """Three replicas with a 20 ms task finish about three times faster than one."""
    import time

    async def run(replica_count):
        kafka = InMemoryKafka(partitions=12, group_balancing=True)
        await publish(kafka, conversations=12, per_conversation=2)
        replicas = [Replica(kafka, f"r{i}", delay=0.02) for i in range(replica_count)]
        for r in replicas:
            await r.start()
        started = time.perf_counter()
        await drain(*replicas)
        return time.perf_counter() - started

    one, three = await run(1), await run(3)
    assert three < one * 0.6
