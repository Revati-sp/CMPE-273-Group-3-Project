"""Interface to the durable history owned by session-service."""

from __future__ import annotations

from typing import Protocol

from engagehub_common.events import EventEnvelope


class HistoryUnavailable(Exception):
    pass


class HistoryClient(Protocol):
    async def messages_after(
        self, tenant_id: str, conversation_id: str, after_seq: int, limit: int = 1000
    ) -> list[EventEnvelope]:
        """Durable messages with sequence number above ``after_seq``, ascending."""

    async def restore_session(self, tenant_id: str, conversation_id: str) -> bool:
        """Rebuild hot state in Redis from durable storage. False if unknown."""
