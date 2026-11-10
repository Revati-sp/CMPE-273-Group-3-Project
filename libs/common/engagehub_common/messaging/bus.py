"""Transport-neutral messaging interfaces used by services and the consumer runner."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from engagehub_common.events import EventEnvelope


@dataclass(frozen=True)
class PublishResult:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    headers: tuple[tuple[str, bytes], ...] = field(default=())


class PublishError(RuntimeError):
    """The broker did not acknowledge the event: its durability is unknown."""


class EventProducer(Protocol):
    async def publish(self, topic: str, event: EventEnvelope) -> PublishResult: ...

    async def publish_raw(self, topic: str, key: bytes | None, value: bytes, headers: tuple = ()) -> PublishResult: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class EventConsumer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def poll(self, timeout_ms: int = 500, max_records: int = 50) -> list[Record]: ...

    async def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        """Commit ``{(topic, partition): next_offset}``."""


class EventLogReader(Protocol):
    """Random-access read of the retained Kafka log, used to replay after a failure."""

    async def read_conversation(
        self,
        tenant_id: str,
        conversation_id: str,
        *,
        partition: int | None,
        start_offset: int | None,
        since_ms: int,
    ) -> list[tuple[EventEnvelope, PublishResult]]: ...
