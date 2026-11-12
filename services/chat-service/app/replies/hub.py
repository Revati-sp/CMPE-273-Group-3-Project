"""Delivers sequenced messages to connected customer sockets: in order, once, with replay.

Each connection tracks ``last_sent``, the highest sequence sent on that socket:

- ``seq <= last_sent``: a duplicate, which is dropped.
- ``seq == last_sent + 1``: sent.
- ``seq > last_sent + 1``: a gap, filled from PostgreSQL before sending.

So a socket always receives a gap-free, strictly increasing stream, whatever order
the live fan-out arrives in.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from engagehub_common.events import utcnow
from engagehub_common.messaging.bus import Record

from app.replies.cursor import CursorStore, CursorUnavailable
from app.replies.log import MessageLog
from app.replies.metrics import (
    CUSTOMER_RECONNECTS,
    CUSTOMER_WS_CONNECTIONS,
    DELIVERY_LATENCY,
    DUPLICATES_IGNORED,
    MESSAGES_REPLAYED,
    RECOVERY_TIME,
    REPLAYS,
)
from app.replies.schemas import SequencedMessage

log = logging.getLogger(__name__)

SEND_TIMEOUT_SECONDS = 5


@dataclass(eq=False)
class CustomerConnection:
    conversation_id: str
    customer_id: str
    send_frame: Callable[[dict], Awaitable[None]]
    close: Callable[[int, str], Awaitable[None]]
    last_sent: int = 0
    sent_ids: dict[int, str] = field(default_factory=dict)
    replaying: bool = True
    held: list[SequencedMessage] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: bool = False

    async def send(self, frame: dict) -> None:
        if not self.closed:
            await asyncio.wait_for(self.send_frame(frame), timeout=SEND_TIMEOUT_SECONDS)


class ReplyHub:
    def __init__(self, message_log: MessageLog, cursors: CursorStore, pod: str) -> None:
        self.log = message_log
        self.cursors = cursors
        self.pod = pod
        self.connections: dict[str, set[CustomerConnection]] = defaultdict(set)

    async def open(self, conn: CustomerConnection, client_last_received: int | None) -> None:
        """Send connection_ack, then replay everything after the customer's cursor."""
        started = time.perf_counter()
        context = {"conversation_id": conn.conversation_id, "customer_id": conn.customer_id}
        previous = None
        try:
            previous = await self.cursors.mark_connected(conn.conversation_id, conn.customer_id, self.pod)
        except CursorUnavailable:
            log.warning("redis unavailable: cannot record session", extra=context)

        cursor_source = "redis"
        cursor_payload: dict = {"last_acknowledged_sequence": 0, "last_message_id": "", "updated_at": ""}
        try:
            cursor = await self.cursors.get(conn.conversation_id, conn.customer_id)
            if cursor is not None:
                cursor_payload = cursor.model_dump()
            else:
                cursor_source = "none"
        except CursorUnavailable:
            # Degrade instead of failing: the database has the messages. Resume from what
            # the client says it holds; if it says nothing, replay everything and let it
            # deduplicate by message ID.
            cursor_source = "client" if client_last_received is not None else "unavailable"
            cursor_payload["last_acknowledged_sequence"] = client_last_received or 0
            log.warning(
                "redis unavailable: replaying from durable log", extra={**context, "cursor_source": cursor_source}
            )

        latest = await self.log.latest_sequence(conn.conversation_id)
        resume_after = min(int(cursor_payload["last_acknowledged_sequence"]), latest)
        conn.last_sent = resume_after
        self.connections[conn.conversation_id].add(conn)
        CUSTOMER_WS_CONNECTIONS.inc()
        reconnected = previous is not None
        if reconnected:
            CUSTOMER_RECONNECTS.inc()
            log.info("customer reconnected", extra={**context, "previous_pod": previous.get("pod"), "pod": self.pod})
        log.info("websocket connected", extra={**context, "pod": self.pod, "resume_after": resume_after})

        await conn.send(
            {
                "type": "connection_ack",
                "conversation_id": conn.conversation_id,
                "customer_id": conn.customer_id,
                "pod": self.pod,
                "reconnected": reconnected,
                "cursor": cursor_payload,
                "cursor_source": cursor_source,
                "latest_sequence": latest,
            }
        )
        await self.replay(conn, resume_after)
        elapsed = time.perf_counter() - started
        if reconnected:
            RECOVERY_TIME.observe(elapsed)
            log.info(
                "session recovery completed",
                extra={**context, "recovery_ms": round(elapsed * 1000, 1), "last_sequence": conn.last_sent},
            )

    async def replay(self, conn: CustomerConnection, after_sequence: int) -> None:
        async with conn.lock:
            conn.replaying = True
            messages = await self.log.after(conn.conversation_id, after_sequence, limit=5000)
            to_sequence = messages[-1].sequence_number if messages else after_sequence
            REPLAYS.inc()
            log.info(
                "replay started",
                extra={"conversation_id": conn.conversation_id, "from_sequence": after_sequence + 1,
                       "to_sequence": to_sequence, "count": len(messages)},
            )  # fmt: skip
            await conn.send(
                {
                    "type": "replay_started",
                    "conversation_id": conn.conversation_id,
                    "from_sequence": after_sequence + 1,
                    "to_sequence": to_sequence,
                    "count": len(messages),
                }
            )
            for message in messages:
                await self._send_message(conn, message, replayed=True)
            MESSAGES_REPLAYED.inc(len(messages))
            await conn.send(
                {"type": "replay_completed", "conversation_id": conn.conversation_id, "last_sequence": conn.last_sent}
            )
            log.info(
                "replay completed", extra={"conversation_id": conn.conversation_id, "last_sequence": conn.last_sent}
            )
            # Release live messages that arrived during the replay. They pass through the
            # same duplicate and gap checks as any other live message.
            conn.replaying = False
            while conn.held:
                await self._push(conn, conn.held.pop(0))

    async def deliver_record(self, message: SequencedMessage, _record: Record | None = None) -> None:
        await self.deliver(message)

    async def deliver(self, message: SequencedMessage) -> None:
        for conn in list(self.connections.get(message.conversation_id, ())):
            if conn.replaying:
                conn.held.append(message)
                continue
            try:
                async with conn.lock:
                    await self._push(conn, message)
            except Exception:
                log.info("dropping unresponsive customer socket", exc_info=True)
                await self.close(conn, 1011, "send failed")

    async def _push(self, conn: CustomerConnection, message: SequencedMessage) -> None:
        if message.sequence_number <= conn.last_sent:
            DUPLICATES_IGNORED.labels("delivery").inc()
            log.info(
                "duplicate message ignored",
                extra={"conversation_id": conn.conversation_id, "message_id": message.message_id,
                       "sequence_number": message.sequence_number, "stage": "delivery"},
            )  # fmt: skip
            return
        if message.sequence_number > conn.last_sent + 1:
            missing = await self.log.between(conn.conversation_id, conn.last_sent, message.sequence_number - 1)
            for earlier in missing:
                await self._send_message(conn, earlier)
        await self._send_message(conn, message, live=True)

    async def _send_message(
        self, conn: CustomerConnection, message: SequencedMessage, *, replayed: bool = False, live: bool = False
    ) -> None:
        await conn.send(message.frame(replayed=replayed))
        conn.last_sent = message.sequence_number
        conn.sent_ids[message.sequence_number] = message.message_id
        if live:
            DELIVERY_LATENCY.observe(max(0.0, (utcnow() - message.timestamp).total_seconds()))
        try:
            await self.cursors.record_delivered(conn.conversation_id, conn.customer_id, message.sequence_number)
        except CursorUnavailable:
            pass  # Only used to validate REST acks. WebSocket acks use last_sent.

    async def acknowledge(self, conn: CustomerConnection, sequence: int, message_id: str = "") -> dict:
        """Advance the cursor, but never past what this socket has actually received."""
        if sequence > conn.last_sent:
            raise ValueError(f"sequence {sequence} has not been delivered (last delivered {conn.last_sent})")
        expected = conn.sent_ids.get(sequence)
        if expected is None:
            stored = await self.log.message_at(conn.conversation_id, sequence)
            expected = stored.message_id if stored else ""
        if message_id and message_id != expected:
            raise ValueError(f"message {message_id} is not sequence {sequence}")
        cursor = await self.cursors.advance(conn.conversation_id, conn.customer_id, sequence, expected)
        log.info(
            "cursor updated",
            extra={"conversation_id": conn.conversation_id, "sequence_number": cursor.last_acknowledged_sequence,
                   "message_id": cursor.last_message_id},
        )  # fmt: skip
        return cursor.model_dump()

    async def close(self, conn: CustomerConnection, code: int, reason: str) -> None:
        if not conn.closed:
            conn.closed = True
            try:
                await conn.close(code, reason)
            except Exception:
                pass
        self.forget(conn)

    def forget(self, conn: CustomerConnection) -> None:
        conns = self.connections.get(conn.conversation_id)
        if conns and conn in conns:
            conns.discard(conn)
            CUSTOMER_WS_CONNECTIONS.dec()
            if not conns:
                del self.connections[conn.conversation_id]
            log.info(
                "websocket disconnected",
                extra={"conversation_id": conn.conversation_id, "customer_id": conn.customer_id,
                       "last_sent": conn.last_sent},
            )  # fmt: skip

    async def shutdown(self) -> None:
        """On SIGTERM, close with 1012 (service restart) so clients reconnect elsewhere at once."""
        for conns in list(self.connections.values()):
            for conn in list(conns):
                await self.close(conn, 1012, "service restarting")
