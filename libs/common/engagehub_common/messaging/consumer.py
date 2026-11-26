"""Reliable consumer loop: at-least-once delivery with idempotent handling.

For every record, in partition order:

1. Validate the schema. Invalid records go straight to the dead-letter topic.
2. Skip the record if this consumer group has already processed its event ID.
3. Run the handler with bounded retries and exponential backoff. Retrying in
   place holds back later records, which preserves per-conversation order.
4. After retries run out, publish to the dead-letter topic and move on, so one
   poison event cannot stall a partition.
5. Commit the offset only after the record is handled or dead-lettered.

A crash between steps 3 and 5 redelivers the record. Handlers must therefore be
idempotent, which is achieved with unique constraints and derived event IDs.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry.trace import SpanKind
from pydantic import ValidationError

from engagehub_common.events import EventEnvelope, EventType, derive_event_id, utcnow
from engagehub_common.logging_setup import bind_event_context
from engagehub_common.messaging.bus import EventConsumer, EventProducer, Record
from engagehub_common.metrics import (
    CONSUMER_LAG_SECONDS,
    DUPLICATES_SUPPRESSED,
    EVENT_RETRIES,
    EVENTS_CONSUMED,
)
from engagehub_common.resilience import backoff_delay
from engagehub_common.telemetry import extract_context, tracer
from engagehub_common.topics import DEAD_LETTER

log = logging.getLogger(__name__)

Handler = Callable[[Any, Record], Awaitable[None]]
Parser = Callable[[bytes], Any]


class NonRetryableError(Exception):
    """The handler knows a retry cannot help: dead-letter immediately."""


class _StopRequested(Exception):
    """Raised inside a transient-retry loop when the runner is asked to stop."""


class RedisDedupe:
    """Remembers processed event IDs per consumer group."""

    def __init__(self, redis, group: str, ttl_seconds: int = 7 * 24 * 3600) -> None:
        self._redis = redis
        self._group = group
        self._ttl = ttl_seconds

    def _key(self, event_id: str) -> str:
        return f"eh:dedupe:{self._group}:{event_id}"

    async def seen(self, event_id: str) -> bool:
        return bool(await self._redis.exists(self._key(event_id)))

    async def mark(self, event_id: str) -> None:
        await self._redis.set(self._key(event_id), "1", ex=self._ttl)


class ConsumerRunner:
    def __init__(
        self,
        consumer: EventConsumer,
        handler: Handler,
        *,
        group: str,
        producer: EventProducer | None = None,
        dedupe: RedisDedupe | None = None,
        service_name: str = "engagehub",
        max_attempts: int = 4,
        retry_base_seconds: float = 0.2,
        commit: bool = True,
        restart_delay_seconds: float = 2.0,
        parser: Parser = EventEnvelope.from_bytes,
        dead_letter_topic: str = DEAD_LETTER,
        transient_errors: tuple[type[BaseException], ...] = (),
        transient_backoff_cap_seconds: float = 10.0,
    ) -> None:
        self.consumer = consumer
        self.handler = handler
        self.group = group
        self.producer = producer
        self.dedupe = dedupe
        self.service_name = service_name
        self.max_attempts = max_attempts
        self.retry_base_seconds = retry_base_seconds
        self.commit = commit
        self.restart_delay_seconds = restart_delay_seconds
        # The parser lets a topic use its own schema. Its errors count as schema failures.
        self.parser = parser
        self.dead_letter_topic = dead_letter_topic
        # Infrastructure errors, such as the database being down, are retried until they
        # clear, never dead-lettered: the event is fine, its destination is not.
        self.transient_errors = transient_errors
        self.transient_backoff_cap_seconds = transient_backoff_cap_seconds
        self._stopping = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.started = False

    @property
    def healthy(self) -> bool:
        return self._task is None or not self._task.done()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"consumer-{self.group}")

    async def stop(self) -> None:
        """Finish the in-flight batch, commit it and leave the group cleanly."""
        self._stopping.set()
        if self._task:
            await self._task
        if self.started:
            await self.consumer.stop()

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                if not self.started:
                    await self.consumer.start()
                    self.started = True
                await self.process_once()
            except asyncio.CancelledError:
                raise
            except _StopRequested:
                break
            except Exception:
                log.exception("consumer loop error; restarting", extra={"group": self.group})
                await asyncio.sleep(self.restart_delay_seconds)

    async def process_once(self, timeout_ms: int = 500) -> int:
        records = await self.consumer.poll(timeout_ms=timeout_ms)
        offsets: dict[tuple[str, int], int] = {}
        for record in records:
            await self._process(record)
            offsets[(record.topic, record.partition)] = record.offset + 1
        if offsets and self.commit:
            await self.consumer.commit(offsets)
        return len(records)

    async def _process(self, record: Record) -> None:
        try:
            event = self.parser(record.value)
        except (ValidationError, ValueError) as exc:
            log.warning("schema validation failed", extra={"topic": record.topic, "group": self.group})
            count = exc.error_count() if isinstance(exc, ValidationError) else 1
            await self._dead_letter(record, f"schema_validation: {count} errors", None)
            EVENTS_CONSUMED.labels(self.group, record.topic, "invalid").inc()
            return

        bind_event_context(
            correlation_id=getattr(event, "correlation_id", None),
            tenant_id=getattr(event, "tenant_id", None),
            conversation_id=getattr(event, "conversation_id", None),
        )
        if await self._already_processed(event):
            DUPLICATES_SUPPRESSED.labels(f"consumer:{self.group}").inc()
            EVENTS_CONSUMED.labels(self.group, record.topic, "duplicate").inc()
            return

        created = getattr(event, "timestamp", None)
        if isinstance(created, datetime):
            CONSUMER_LAG_SECONDS.labels(self.group).observe(max(0.0, (utcnow() - created).total_seconds()))
        token = otel_context.attach(extract_context(record.headers))
        try:
            with tracer().start_as_current_span(
                f"consume {record.topic}",
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination.name": record.topic,
                    "messaging.consumer.group.name": self.group,
                    "engagehub.event_type": str(getattr(event.event_type, "value", event.event_type)),
                    "engagehub.correlation_id": getattr(event, "correlation_id", "") or "",
                    "engagehub.tenant_id": getattr(event, "tenant_id", "") or "",
                },
            ):
                await self._handle_with_retries(event, record)
        finally:
            otel_context.detach(token)

    async def _handle_with_retries(self, event, record: Record) -> None:
        attempt = 0
        transient_attempt = 0
        while True:
            try:
                await self.handler(event, record)
            except NonRetryableError as exc:
                await self._dead_letter(record, f"non_retryable: {exc}", event)
                EVENTS_CONSUMED.labels(self.group, record.topic, "dead_letter").inc()
                return
            except self.transient_errors as exc:
                if self._stopping.is_set():
                    raise _StopRequested from exc  # leave the offset uncommitted for the next owner
                EVENT_RETRIES.labels(self.group, record.topic).inc()
                log.warning(
                    "dependency unavailable; retrying until it recovers",
                    extra={"event_id": event.event_id, "attempt": transient_attempt + 1, "error": repr(exc)},
                )
                await asyncio.sleep(
                    backoff_delay(
                        min(transient_attempt, 8), self.retry_base_seconds, self.transient_backoff_cap_seconds
                    )
                )
                transient_attempt += 1
            except Exception as exc:
                if attempt == self.max_attempts - 1:
                    log.exception("handler failed; dead-lettering", extra={"event_id": event.event_id})
                    await self._dead_letter(record, f"handler_failed: {exc!r}", event)
                    EVENTS_CONSUMED.labels(self.group, record.topic, "dead_letter").inc()
                    return
                EVENT_RETRIES.labels(self.group, record.topic).inc()
                log.warning("handler failed; retrying", extra={"event_id": event.event_id, "attempt": attempt + 1})
                await asyncio.sleep(backoff_delay(attempt, self.retry_base_seconds))
                attempt += 1
            else:
                await self._mark_processed(event)
                EVENTS_CONSUMED.labels(self.group, record.topic, "ok").inc()
                return

    async def _already_processed(self, event: EventEnvelope) -> bool:
        if not self.dedupe:
            return False
        try:
            return await self.dedupe.seen(event.event_id)
        except Exception:
            # A Redis outage must not stop consumption. Handlers remain idempotent.
            log.warning("dedupe lookup unavailable", extra={"group": self.group})
            return False

    async def _mark_processed(self, event: EventEnvelope) -> None:
        if self.dedupe:
            try:
                await self.dedupe.mark(event.event_id)
            except Exception:
                log.warning("dedupe mark unavailable", extra={"group": self.group})

    async def _dead_letter(self, record: Record, reason: str, event) -> None:
        if self.producer is None:
            log.error("dropping undeliverable record: no producer for DLQ", extra={"reason": reason})
            return
        coordinates = f"{record.topic}:{record.partition}:{record.offset}:{self.group}"
        letter = EventEnvelope(
            event_id=derive_event_id(coordinates, "dlq"),
            event_type=EventType.DEAD_LETTER,
            conversation_id=getattr(event, "conversation_id", None),
            tenant_id=getattr(event, "tenant_id", None) or "unknown",
            customer_id=getattr(event, "customer_id", None),
            correlation_id=getattr(event, "correlation_id", None) or derive_event_id(coordinates, "corr"),
            producer_service=self.service_name,
            payload={
                "reason": reason,
                "consumer_group": self.group,
                "source_topic": record.topic,
                "source_partition": record.partition,
                "source_offset": record.offset,
                "original": _safe_decode(record.value),
            },
        )
        # A failed DLQ publish propagates, so the offset stays uncommitted and nothing is lost.
        log.error(
            "event dead-lettered",
            extra={"reason": reason, "topic": record.topic, "offset": record.offset, "dlq": self.dead_letter_topic},
        )
        await self.producer.publish(self.dead_letter_topic, letter)


def _safe_decode(raw: bytes):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return raw.decode("utf-8", errors="replace")
