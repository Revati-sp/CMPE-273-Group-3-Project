"""The compacted ``chat.session-state`` topic: the latest session snapshot per conversation.

Kafka keeps (at least) the newest record per key, so a pod that loses Redis can
still learn the last known owner, version, acknowledgement and agent state.
Message history is not read from here: compaction deletes older records. The
ordered message tail comes from the PostgreSQL log.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Literal

from engagehub_common.events import new_id
from engagehub_common.messaging.bus import EventProducer, PublishError, Record
from engagehub_common.messaging.consumer import NonRetryableError
from engagehub_common.telemetry import inject_headers
from engagehub_common.topics import SESSION_STATE
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from app.replies.metrics import SESSION_CONFLICTS, SESSION_STATE_PUBLISH_FAILURES
from app.replies.schemas import ID_PATTERN
from app.replies.session import SessionSnapshot

log = logging.getLogger(__name__)


class SessionStateUpdated(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=new_id, min_length=1)
    event_type: Literal["SessionStateUpdated"] = "SessionStateUpdated"
    conversation_id: str = Field(pattern=ID_PATTERN)
    tenant_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    session_version: int = Field(ge=0)
    last_acknowledged_sequence: int = Field(ge=0)
    assigned_agent_id: str = ""
    workflow_state: str = ""
    current_chat_pod: str = ""
    reason: str = ""
    correlation_id: str = Field(default_factory=new_id)
    timestamp: datetime
    payload: dict

    @field_validator("timestamp")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    @classmethod
    def parse(cls, raw: bytes) -> SessionStateUpdated:
        return cls.model_validate_json(raw)

    @classmethod
    def from_snapshot(cls, snapshot: SessionSnapshot, reason: str) -> SessionStateUpdated:
        return cls(
            conversation_id=snapshot.conversation_id,
            tenant_id=snapshot.tenant_id,
            customer_id=snapshot.customer_id,
            session_version=snapshot.session_version,
            last_acknowledged_sequence=snapshot.last_acknowledged_sequence,
            assigned_agent_id=snapshot.assigned_agent_id,
            workflow_state=snapshot.workflow_state,
            current_chat_pod=snapshot.current_chat_pod,
            reason=reason,
            timestamp=datetime.now(UTC),
            payload=snapshot.model_dump(),
        )

    def snapshot(self) -> SessionSnapshot:
        """The full snapshot. Its identity and version must agree with the envelope."""
        try:
            snapshot = SessionSnapshot.model_validate(self.payload)
        except ValidationError as exc:
            raise NonRetryableError(f"invalid session snapshot payload: {exc.error_count()} errors") from exc
        mismatched = [
            name
            for name in ("conversation_id", "tenant_id", "customer_id", "session_version")
            if getattr(snapshot, name) != getattr(self, name)
        ]
        if mismatched:
            raise NonRetryableError(f"payload disagrees with event on {', '.join(mismatched)}")
        return snapshot


class SessionStatePublisher:
    def __init__(self, producer: EventProducer) -> None:
        self.producer = producer

    async def publish(self, snapshot: SessionSnapshot, reason: str) -> bool:
        """Best effort: Redis holds the live copy. A failure is retried on the next change."""
        event = SessionStateUpdated.from_snapshot(snapshot, reason)
        try:
            await self.producer.publish_raw(
                SESSION_STATE,
                snapshot.conversation_id.encode(),
                event.model_dump_json().encode(),
                (("correlation_id", event.correlation_id.encode()), *inject_headers()),
            )
        except PublishError:
            SESSION_STATE_PUBLISH_FAILURES.inc()
            log.warning(
                "session state not published",
                extra={"conversation_id": snapshot.conversation_id, "session_version": snapshot.session_version},
            )
            return False
        log.info(
            "session snapshot written",
            extra={"conversation_id": snapshot.conversation_id, "session_version": snapshot.session_version,
                   "target": "kafka", "reason": reason},
        )  # fmt: skip
        return True


class SessionStateTable:
    """Each pod's view of the compacted topic: the newest snapshot per conversation.

    Fed by a consumer that reads the topic from the beginning with no consumer group,
    like a materialised table. Events older than what the table already holds are
    rejected, so a redelivered or reordered event cannot roll a session back.
    """

    def __init__(self, on_newer: Callable[[SessionSnapshot], Awaitable[None]] | None = None) -> None:
        self.latest: dict[str, SessionSnapshot] = {}
        self.on_newer = on_newer

    def get(self, conversation_id: str) -> SessionSnapshot | None:
        return self.latest.get(conversation_id)

    def apply(self, snapshot: SessionSnapshot) -> bool:
        current = self.latest.get(snapshot.conversation_id)
        if current is not None and current.session_version >= snapshot.session_version:
            if current.session_version > snapshot.session_version:
                SESSION_CONFLICTS.labels("stale_kafka_event").inc()
                log.info(
                    "stale session event ignored",
                    extra={"conversation_id": snapshot.conversation_id, "event_version": snapshot.session_version,
                           "known_version": current.session_version},
                )  # fmt: skip
            return False
        self.latest[snapshot.conversation_id] = snapshot
        return True

    async def handle(self, event: SessionStateUpdated, _record: Record) -> None:
        snapshot = event.snapshot()
        if self.apply(snapshot) and self.on_newer is not None:
            await self.on_newer(snapshot)
