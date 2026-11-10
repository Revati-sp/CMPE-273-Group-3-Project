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
    def __init__(self, partitions: int = 6) -> None:
        self.partitions = partitions
        self.logs: dict[tuple[str, int], list[Record]] = defaultdict(list)
        self.committed: dict[tuple[str, tuple[str, int]], int] = {}
        self.fail_publishes = 0
        self.publish_attempts = 0

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

    async def start(self) -> None:
        for topic in self.topics:
            for partition in range(self.kafka.partitions):
                tp = (topic, partition)
                committed = self.kafka.committed.get((self.group, tp)) if self.group else None
                if committed is not None:
                    self.positions[tp] = committed
                elif self.offset_reset == "latest":
                    self.positions[tp] = len(self.kafka.logs[tp])
                else:
                    self.positions[tp] = 0

    async def stop(self) -> None: ...

    async def poll(self, timeout_ms: int = 0, max_records: int = 50) -> list[Record]:
        out: list[Record] = []
        for tp, position in self.positions.items():
            available = self.kafka.logs[tp][position : position + max_records - len(out)]
            out.extend(available)
            self.positions[tp] = position + len(available)
            if len(out) >= max_records:
                break
        await asyncio.sleep(0 if out else 0.002)
        return out

    async def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        for tp, offset in offsets.items():
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
