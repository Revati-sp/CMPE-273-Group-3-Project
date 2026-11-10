"""aiokafka implementations of the messaging interfaces."""

from __future__ import annotations

import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import KafkaError
from pydantic import ValidationError

from engagehub_common.events import EventEnvelope
from engagehub_common.messaging.bus import PublishError, PublishResult, Record
from engagehub_common.metrics import EVENTS_PUBLISHED
from engagehub_common.telemetry import inject_headers
from engagehub_common.topics import CHAT_MESSAGES

log = logging.getLogger(__name__)


class KafkaEventProducer:
    """Waits for ``acks=all``, with broker-side deduplication of internal retries."""

    def __init__(self, bootstrap_servers: str, client_id: str) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            acks="all",
            enable_idempotence=True,
            linger_ms=2,
            request_timeout_ms=15_000,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, event: EventEnvelope) -> PublishResult:
        headers = (("correlation_id", event.correlation_id.encode()), *inject_headers())
        result = await self.publish_raw(topic, event.partition_key.encode(), event.to_bytes(), headers)
        EVENTS_PUBLISHED.labels(topic, event.event_type.value).inc()
        return result

    async def publish_raw(self, topic: str, key: bytes | None, value: bytes, headers: tuple = ()) -> PublishResult:
        try:
            meta = await self._producer.send_and_wait(topic, value=value, key=key, headers=list(headers))
        except (KafkaError, OSError, TimeoutError) as exc:
            raise PublishError(f"publish to {topic} failed: {exc}") from exc
        return PublishResult(meta.topic, meta.partition, meta.offset)


class KafkaEventConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topics: list[str],
        group_id: str | None,
        client_id: str,
        *,
        offset_reset: str = "earliest",
    ) -> None:
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=client_id,
            enable_auto_commit=False,
            auto_offset_reset=offset_reset,
            session_timeout_ms=15_000,
            heartbeat_interval_ms=3_000,
            max_poll_interval_ms=120_000,
            metadata_max_age_ms=30_000,
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def poll(self, timeout_ms: int = 500, max_records: int = 50) -> list[Record]:
        batches = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        records: list[Record] = []
        for tp, messages in batches.items():
            records.extend(
                Record(tp.topic, tp.partition, m.offset, m.key, m.value, tuple(m.headers or ())) for m in messages
            )
        return records

    async def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        await self._consumer.commit({TopicPartition(t, p): o for (t, p), o in offsets.items()})


class KafkaLogReader:
    """Reads one conversation's events straight from the retained ``chat.messages`` log."""

    def __init__(self, bootstrap_servers: str, topic: str = CHAT_MESSAGES) -> None:
        self._servers = bootstrap_servers
        self._topic = topic

    async def read_conversation(
        self,
        tenant_id: str,
        conversation_id: str,
        *,
        partition: int | None,
        start_offset: int | None,
        since_ms: int,
    ) -> list[tuple[EventEnvelope, PublishResult]]:
        consumer = AIOKafkaConsumer(bootstrap_servers=self._servers, group_id=None, enable_auto_commit=False)
        await consumer.start()
        try:
            partitions = (
                [partition]
                if partition is not None and start_offset is not None
                else sorted(await self._partitions(consumer))
            )
            tps = [TopicPartition(self._topic, p) for p in partitions]
            consumer.assign(tps)
            ends = await consumer.end_offsets(tps)
            starts = await self._start_positions(consumer, tps, ends, start_offset, since_ms)
            key = conversation_id.encode()
            found: list[tuple[EventEnvelope, PublishResult]] = []
            for tp in tps:
                if starts[tp] >= ends[tp]:
                    continue
                consumer.seek(tp, starts[tp])
                while await consumer.position(tp) < ends[tp]:
                    batch = await consumer.getmany(tp, timeout_ms=1000, max_records=500)
                    for message in batch.get(tp, []):
                        if message.offset >= ends[tp] or message.key != key:
                            continue
                        try:
                            event = EventEnvelope.from_bytes(message.value)
                        except ValidationError:
                            continue
                        if event.tenant_id == tenant_id and event.conversation_id == conversation_id:
                            found.append((event, PublishResult(tp.topic, tp.partition, message.offset)))
            return found
        finally:
            await consumer.stop()

    async def _partitions(self, consumer: AIOKafkaConsumer) -> set[int]:
        await consumer._client.force_metadata_update()
        return consumer.partitions_for_topic(self._topic) or set()

    @staticmethod
    async def _start_positions(consumer, tps, ends, start_offset, since_ms) -> dict:
        if start_offset is not None and len(tps) == 1:
            return {tps[0]: start_offset}
        by_time = await consumer.offsets_for_times({tp: since_ms for tp in tps})
        return {tp: (by_time[tp].offset if by_time.get(tp) else ends[tp]) for tp in tps}
