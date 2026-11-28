"""An in-memory Kafka used by tests.

It models what the reliability code depends on: key-based partitioning, per
partition ordering, retained logs, per-group committed offsets, and redelivery
from the committed offset when a consumer restarts.
"""

from __future__ import annotations

import asyncio
import zlib
from collections import defaultdict

from engagehub_common.events import EventEnvelope
from engagehub_common.messaging.bus import PublishError, PublishResult, Record


class InMemoryKafka:
    def __init__(self, partitions: int = 6, *, group_balancing: bool = False) -> None:
        self.partitions = partitions
        self.logs: dict[tuple[str, int], list[Record]] = defaultdict(list)
        self.committed: dict[tuple[str, tuple[str, int]], int] = {}
        self.fail_publishes = 0
        self.publish_attempts = 0
        # With group_balancing, consumers sharing a group id split the partitions between
        # them (round-robin over members, recomputed on every join and leave), as Kafka's
        # group coordinator does. Without it, each consumer instance reads all partitions,
        # which suits single-consumer tests.
        self.group_balancing = group_balancing
        self.members: dict[str, list[InMemoryConsumer]] = defaultdict(list)

    def join(self, consumer: InMemoryConsumer) -> None:
        self.members[consumer.group].append(consumer)

    def leave(self, consumer: InMemoryConsumer) -> None:
        if consumer in self.members.get(consumer.group, []):
            self.members[consumer.group].remove(consumer)

    def assignment(self, consumer: InMemoryConsumer) -> set[tuple[str, int]]:
        everything = {(t, p) for t in consumer.topics for p in range(self.partitions)}
        if not (self.group_balancing and consumer.group):
            return everything
        members = self.members.get(consumer.group, [])
        if consumer not in members:
            return set()
        index = members.index(consumer)
        return {(t, p) for (t, p) in everything if p % len(members) == index}

    def partition_for(self, key: bytes | None) -> int:
        return zlib.crc32(key or b"") % self.partitions

    def append(self, topic: str, key: bytes | None, value: bytes, headers: tuple = ()) -> PublishResult:
        partition = self.partition_for(key)
        log = self.logs[(topic, partition)]
        log.append(Record(topic, partition, len(log), key, value, tuple(headers)))
        return PublishResult(topic, partition, len(log) - 1)

    def events(self, topic: str) -> list[EventEnvelope]:
        """Every event retained on a topic, in per-partition log order."""
        out: list[EventEnvelope] = []
        for (name, _), records in sorted(self.logs.items()):
            if name == topic:
                out.extend(EventEnvelope.from_bytes(r.value) for r in records)
        return out

    def records(self, topic: str) -> list[Record]:
        out: list[Record] = []
        for (name, _), records in sorted(self.logs.items()):
            if name == topic:
                out.extend(records)
        return out

    def producer(self) -> InMemoryProducer:
        return InMemoryProducer(self)

    def consumer(self, topics: list[str], group: str | None, offset_reset="earliest") -> InMemoryConsumer:
        return InMemoryConsumer(self, topics, group, offset_reset)

    def log_reader(self) -> InMemoryLogReader:
        return InMemoryLogReader(self)


class InMemoryProducer:
    def __init__(self, kafka: InMemoryKafka) -> None:
        self.kafka = kafka

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(self, topic: str, event: EventEnvelope) -> PublishResult:
        return await self.publish_raw(topic, event.partition_key.encode(), event.to_bytes())

    async def publish_raw(self, topic: str, key: bytes | None, value: bytes, headers: tuple = ()) -> PublishResult:
        self.kafka.publish_attempts += 1
        if self.kafka.fail_publishes > 0:
            self.kafka.fail_publishes -= 1
            raise PublishError("injected broker failure")
        return self.kafka.append(topic, key, value, headers)


class InMemoryConsumer:
    """One consumer instance. A new instance resumes from the group's committed offsets."""

    def __init__(self, kafka: InMemoryKafka, topics: list[str], group: str | None, offset_reset: str) -> None:
        self.kafka = kafka
        self.topics = topics
        self.group = group
        self.offset_reset = offset_reset
        self.positions: dict[tuple[str, int], int] = {}
        self.started = False

    def _initial_position(self, tp: tuple[str, int]) -> int:
        committed = self.kafka.committed.get((self.group, tp)) if self.group else None
        if committed is not None:
            return committed
        return len(self.kafka.logs[tp]) if self.offset_reset == "latest" else 0

    def _sync_assignment(self) -> None:
        """Pick up partitions gained in a rebalance (from the committed offset), drop lost ones."""
        assigned = self.kafka.assignment(self)
        for tp in list(self.positions):
            if tp not in assigned:
                del self.positions[tp]
        for tp in sorted(assigned):  # deterministic poll order, so test runs are repeatable
            if tp not in self.positions:
                self.positions[tp] = self._initial_position(tp)

    async def start(self) -> None:
        if self.group and self.kafka.group_balancing:
            self.kafka.join(self)
        self.started = True
        self._sync_assignment()

    async def stop(self) -> None:
        """Leave the group: its partitions move to the remaining members."""
        self.kafka.leave(self)
        self.started = False

    def crash(self) -> None:
        """Vanish without committing, as a killed pod does."""
        self.kafka.leave(self)
        self.positions.clear()
        self.started = False

    async def poll(self, timeout_ms: int = 0, max_records: int = 50) -> list[Record]:
        if self.kafka.group_balancing and self.started:
            self._sync_assignment()
        out: list[Record] = []
        for tp, position in self.positions.items():
            available = self.kafka.logs[tp][position : position + max_records - len(out)]
            out.extend(available)
            self.positions[tp] = position + len(available)
            if len(out) >= max_records:
                break
        await asyncio.sleep(0 if out else 0.002)
        return out

    @property
    def lag(self) -> dict[tuple[str, int], int]:
        return {tp: len(self.kafka.logs[tp]) - position for tp, position in self.positions.items()}

    async def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        for tp, offset in offsets.items():
            # A member that lost the partition in a rebalance cannot move its offset any more.
            if not self.kafka.group_balancing or tp in self.positions:
                self.kafka.committed[(self.group, tp)] = offset


class InMemoryLogReader:
    def __init__(self, kafka: InMemoryKafka, topic: str = "chat.messages") -> None:
        self.kafka = kafka
        self.topic = topic

    async def read_conversation(
        self,
        tenant_id: str,
        conversation_id: str,
        *,
        partition: int | None,
        start_offset: int | None,
        since_ms: int,
    ) -> list[tuple[EventEnvelope, PublishResult]]:
        found = []
        for record in self.kafka.records(self.topic):
            if partition is not None and start_offset is not None:
                if record.partition != partition or record.offset < start_offset:
                    continue
            event = EventEnvelope.from_bytes(record.value)
            if event.tenant_id == tenant_id and event.conversation_id == conversation_id:
                found.append((event, PublishResult(record.topic, record.partition, record.offset)))
        return found
