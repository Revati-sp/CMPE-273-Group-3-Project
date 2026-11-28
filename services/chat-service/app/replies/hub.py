"""Delivers sequenced messages to customer sockets, and moves sessions between pods.

Ordering. Each connection tracks ``last_sent``, the highest sequence sent on that socket:

- ``seq <= last_sent``: a duplicate, which is dropped.
- ``seq == last_sent + 1``: sent.
- ``seq > last_sent + 1``: a gap, filled from PostgreSQL before sending.

Handover. When a customer connects, this pod claims the Redis session snapshot,
which increments ``session_version`` (the fencing token). If the previous owner was a
different pod, the client is told ``handover_started``. Everything after the
customer's last acknowledged message is replayed from PostgreSQL, then
``handover_completed`` is sent. Every later snapshot write carries the connection's
version. A connection whose version has been overtaken, for example an old pod that
survived a network partition, has its write rejected and is closed with 4409.

Guarantees, under the failures exercised by the tests (crash, restart, partition,
rolling update, Redis outage): a message acknowledged by the client is never replayed
from an earlier point than its acknowledgement. A message sent but not acknowledged
is replayed after reconnect. The client drops repeats by message ID. These rely on
PostgreSQL holding the log; they do not cover the loss of PostgreSQL itself.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from engagehub_common.events import new_id, utcnow
from engagehub_common.messaging.bus import Record
from engagehub_common.metrics import MESSAGES_REPLAYED
from engagehub_common.session.store import SessionStore
from engagehub_common.telemetry import tracer

from app.replies.cursor import CursorStore, CursorUnavailable
from app.replies.log import MessageLog
from app.replies.metrics import (
    CUSTOMER_RECONNECTS,
    CUSTOMER_WS_CONNECTIONS,
    DELIVERY_LATENCY,
    DUPLICATES_IGNORED,
    GAPS_DETECTED,
    HANDOVERS,
    HANDOVERS_FAILED,
    RECOVERY_TIME,
    REPLAYS,
    SESSION_CLAIMS,
    SESSION_CONFLICTS,
    SESSION_RESTORES,
    STALE_POD_UPDATES_REJECTED,
)
from app.replies.schemas import SequencedMessage
from app.replies.session import (
    WORKFLOW_FROM_PLATFORM,
    ClaimResult,
    SessionSnapshot,
    SessionSnapshotStore,
    SessionUnavailable,
    StaleSession,
)
from app.replies.session_state import SessionStatePublisher, SessionStateTable

log = logging.getLogger(__name__)

SEND_TIMEOUT_SECONDS = 5
CLOSE_SUPERSEDED = 4409
MAX_PENDING_IDS = 200
AGENT_SENDERS = ("virtual_agent", "human_agent")


class ConnectionFenced(Exception):
    """This connection's session was claimed by a newer connection."""


@dataclass(eq=False)
class CustomerConnection:
    conversation_id: str
    customer_id: str
    send_frame: Callable[[dict], Awaitable[None]]
    close: Callable[[int, str], Awaitable[None]]
    tenant_id: str = ""
    connection_id: str = field(default_factory=new_id)
    last_sent: int = 0
    last_acked: int = 0
    sent_ids: dict[int, str] = field(default_factory=dict)
    replaying: bool = True
    held: list[SequencedMessage] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: bool = False
    fenced: bool = False
    tracked: bool = False  # owns a Redis snapshot; False while Redis is unavailable
    session_version: int = 0

    async def send(self, frame: dict) -> None:
        if not self.closed:
            await asyncio.wait_for(self.send_frame(frame), timeout=SEND_TIMEOUT_SECONDS)

    def pending_ids(self) -> list[str]:
        return [self.sent_ids[s] for s in sorted(self.sent_ids) if s > self.last_acked][-MAX_PENDING_IDS:]


class ReplyHub:
    def __init__(
        self,
        message_log: MessageLog,
        cursors: CursorStore,
        pod: str,
        *,
        sessions: SessionSnapshotStore | None = None,
        state_table: SessionStateTable | None = None,
        publisher: SessionStatePublisher | None = None,
        platform: SessionStore | None = None,
    ) -> None:
        self.log = message_log
        self.cursors = cursors
        self.pod = pod
        self.sessions = sessions
        self.state_table = state_table
        self.publisher = publisher
        self.platform = platform
        self.connections: dict[str, set[CustomerConnection]] = defaultdict(set)
        self.draining = False

    # Connect, claim and hand over

    async def open(self, conn: CustomerConnection, client_last_received: int | None) -> None:
        started = time.perf_counter()
        context = {"conversation_id": conn.conversation_id, "customer_id": conn.customer_id, "pod": self.pod}
        with tracer().start_as_current_span(
            "chat.session.open",
            attributes={"engagehub.conversation_id": conn.conversation_id, "engagehub.pod": self.pod},
        ) as span:
            claim, fallback = await self._claim(conn)
            snapshot = claim.snapshot if claim else fallback
            reconnected = bool(claim and claim.existed) or (claim is None and fallback is not None)
            handover = bool(claim and claim.handover)
            if reconnected:
                CUSTOMER_RECONNECTS.inc()
                log.info(
                    "customer reconnecting", extra={**context, "previous_pod": claim.previous_pod if claim else ""}
                )
            SESSION_CLAIMS.labels("handover" if handover else "reconnect" if reconnected else "new").inc()
            span.set_attribute("engagehub.handover", handover)
            span.set_attribute("engagehub.session_version", conn.session_version)

            resume_after, cursor_payload, cursor_source = await self._resume_point(conn, snapshot, client_last_received)
            conn.last_sent = resume_after
            conn.last_acked = int(cursor_payload["last_acknowledged_sequence"])
            self.connections[conn.conversation_id].add(conn)
            CUSTOMER_WS_CONNECTIONS.inc()
            log.info("websocket connected", extra={**context, "resume_after": resume_after,
                                                   "session_version": conn.session_version})  # fmt: skip
            latest = await self.log.latest_sequence(conn.conversation_id)
            await conn.send(
                {
                    "type": "connection_ack",
                    "conversation_id": conn.conversation_id,
                    "customer_id": conn.customer_id,
                    "pod": self.pod,
                    "connection_id": conn.connection_id,
                    "reconnected": reconnected,
                    "handover": handover,
                    "previous_pod": claim.previous_pod if claim else (fallback.current_chat_pod if fallback else ""),
                    "session_version": conn.session_version,
                    "session_source": "redis" if claim else ("kafka_session_state" if fallback else "none"),
                    "cursor": cursor_payload,
                    "cursor_source": cursor_source,
                    "latest_sequence": latest,
                    **_session_view(snapshot),
                }
            )
            if claim is not None:
                if handover:
                    log.info(
                        "conversation claimed by new pod",
                        extra={**context, "old_pod": claim.previous_pod, "session_version": conn.session_version,
                               "reason": claim.handover_reason},
                    )  # fmt: skip
                    log.info("handover started", extra={**context, "old_pod": claim.previous_pod})
                    await conn.send(
                        {
                            "type": "handover_started",
                            "conversation_id": conn.conversation_id,
                            "old_pod": claim.previous_pod,
                            "new_pod": self.pod,
                            "reason": claim.handover_reason,
                            "session_version": conn.session_version,
                        }
                    )
                # Publishing the claim also fences the old connection if its pod reads Kafka.
                await self._publish(claim.snapshot, "handover" if handover else "claim")

            try:
                replayed = await self.replay(conn, resume_after)
                await self._record_progress(conn, publish=None)
            except Exception:
                if handover:
                    HANDOVERS_FAILED.inc()
                    log.error("handover failed", extra=context, exc_info=True)
                raise
            elapsed = time.perf_counter() - started
            if handover:
                HANDOVERS.labels(claim.handover_reason).inc()
                await conn.send(
                    {
                        "type": "handover_completed",
                        "conversation_id": conn.conversation_id,
                        "old_pod": claim.previous_pod,
                        "new_pod": self.pod,
                        "replayed_from_sequence": resume_after + 1,
                        "replayed_to_sequence": replayed[-1].sequence_number if replayed else resume_after,
                        "replayed_count": len(replayed),
                        "session_version": conn.session_version,
                        "recovery_ms": round(elapsed * 1000, 1),
                    }
                )
                log.info("handover completed", extra={**context, "session_version": conn.session_version,
                                                      "replayed": len(replayed)})  # fmt: skip
            if reconnected:
                RECOVERY_TIME.observe(elapsed)
                log.info(
                    "recovery completed",
                    extra={**context, "recovery_ms": round(elapsed * 1000, 1), "last_sequence": conn.last_sent},
                )

    async def _claim(self, conn: CustomerConnection) -> tuple[ClaimResult | None, SessionSnapshot | None]:
        """Claim the session in Redis, restoring it from Kafka first if Kafka's copy is newer.

        Returns (claim, None) normally, or (None, kafka_snapshot_or_None) if Redis is down.
        """
        if self.sessions is None:
            return None, None
        known = self.state_table.get(conn.conversation_id) if self.state_table else None
        if known is not None and (known.tenant_id != conn.tenant_id or known.customer_id != conn.customer_id):
            known = None  # never adopt another tenant's or customer's state
        try:
            with tracer().start_as_current_span("redis.session.claim"):
                stored = await self.sessions.get(conn.conversation_id, conn.customer_id)
                if known is not None and (stored is None or known.session_version > stored.session_version):
                    # Redis lost writes (failover) or the key expired: Kafka has a newer snapshot.
                    if await self.sessions.put_if_newer(known):
                        SESSION_RESTORES.labels("kafka").inc()
                        log.info(
                            "session snapshot restored",
                            extra={"conversation_id": conn.conversation_id, "source": "kafka",
                                   "session_version": known.session_version},
                        )  # fmt: skip
                elif stored is not None:
                    SESSION_RESTORES.labels("redis").inc()
                claim = await self.sessions.claim(
                    conversation_id=conn.conversation_id,
                    tenant_id=conn.tenant_id,
                    customer_id=conn.customer_id,
                    pod=self.pod,
                    connection_id=conn.connection_id,
                    fields=await self._platform_fields(conn),
                )
        except SessionUnavailable:
            log.warning(
                "redis unavailable: session not claimed; using the latest Kafka session state",
                extra={
                    "conversation_id": conn.conversation_id,
                    "kafka_version": known.session_version if known else None,
                },
            )
            if known is not None:
                SESSION_RESTORES.labels("kafka").inc()
                conn.session_version = known.session_version
            return None, known
        conn.session_version = claim.snapshot.session_version
        conn.tracked = True
        return claim, None

    async def _platform_fields(self, conn: CustomerConnection) -> dict:
        """Agent assignment and workflow from the support-chat session, when there is one."""
        if self.platform is None:
            return {}
        try:
            state = await self.platform.get(conn.tenant_id, conn.conversation_id)
        except Exception:
            return {}
        if state is None or state.customer_id != conn.customer_id:
            return {}
        fields = {"workflow_state": WORKFLOW_FROM_PLATFORM.get(state.workflow_state, state.workflow_state.upper())}
        if state.assigned_agent_type == "human" and state.assigned_agent_id:
            fields.update(
                agent_kind="human_agent",
                assigned_agent_type="human",
                assigned_agent_id=state.assigned_agent_id,
                assigned_agent_name=state.assigned_agent_name,
            )
        return fields

    async def _resume_point(
        self, conn: CustomerConnection, snapshot: SessionSnapshot | None, client_last_received: int | None
    ) -> tuple[int, dict, str]:
        """Where to replay from: the last acknowledged message, from the best source available."""
        payload: dict = {"last_acknowledged_sequence": 0, "last_message_id": "", "session_version": 0, "updated_at": ""}
        snapshot_ack = snapshot.last_acknowledged_sequence if snapshot else 0
        try:
            cursor = await self.cursors.get(conn.conversation_id, conn.customer_id)
        except CursorUnavailable:
            # Redis is down. The client's own position avoids resending what it has, and the
            # Kafka snapshot avoids replaying everything. The client deduplicates either way.
            client = client_last_received or 0
            if client_last_received is not None and client >= snapshot_ack:
                source, resume = "client", client
            elif snapshot is not None:
                source, resume = "kafka_session_state", snapshot_ack
            else:
                source, resume = "unavailable", 0
            payload["last_acknowledged_sequence"] = resume
            log.warning(
                "redis unavailable: replaying from durable log",
                extra={"conversation_id": conn.conversation_id, "cursor_source": source},
            )
        else:
            if cursor is not None:
                payload.update(cursor.model_dump())
            source = "redis" if cursor is not None else "none"
            if snapshot_ack > int(payload["last_acknowledged_sequence"]):
                # The snapshot is written first on ack, so it can lead the cursor after a crash.
                payload["last_acknowledged_sequence"] = snapshot_ack
                payload["last_message_id"] = snapshot.last_acknowledged_message_id
                source = "session_snapshot"
            resume = int(payload["last_acknowledged_sequence"])
        latest = await self.log.latest_sequence(conn.conversation_id)
        return min(resume, latest), payload, source

    # Replay and live delivery

    async def replay(self, conn: CustomerConnection, after_sequence: int) -> list[SequencedMessage]:
        async with conn.lock:
            conn.replaying = True
            with tracer().start_as_current_span(
                "postgres.replay", attributes={"engagehub.after_sequence": after_sequence}
            ):
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
            MESSAGES_REPLAYED.labels("postgres").inc(len(messages))
            await conn.send(
                {"type": "replay_completed", "conversation_id": conn.conversation_id, "last_sequence": conn.last_sent}
            )
            log.info(
                "replay completed", extra={"conversation_id": conn.conversation_id, "last_sequence": conn.last_sent}
            )
            # Release live messages that arrived during the replay, through the same checks.
            conn.replaying = False
            while conn.held:
                await self._push(conn, conn.held.pop(0))
            return messages

    async def resync(self, conn: CustomerConnection, after_sequence: int) -> None:
        """The client saw a gap: resend what this socket sent after ``after_sequence``."""
        GAPS_DETECTED.labels("client").inc()
        log.info("gap detected", extra={"conversation_id": conn.conversation_id, "after_sequence": after_sequence,
                                        "where": "client"})  # fmt: skip
        async with conn.lock:
            for message in await self.log.between(conn.conversation_id, after_sequence, conn.last_sent):
                await conn.send(message.frame(replayed=True))
            await conn.send(
                {"type": "resync_completed", "after_sequence": after_sequence, "last_sequence": conn.last_sent}
            )

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
            except ConnectionFenced:
                pass  # already closed by _fence
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
            GAPS_DETECTED.labels("server").inc()
            log.info(
                "gap detected",
                extra={"conversation_id": conn.conversation_id, "last_sent": conn.last_sent,
                       "sequence_number": message.sequence_number, "where": "server"},
            )  # fmt: skip
            missing = await self.log.between(conn.conversation_id, conn.last_sent, message.sequence_number - 1)
            for earlier in missing:
                await self._send_message(conn, earlier, track=True)
        await self._send_message(conn, message, live=True, track=True)

    async def _send_message(
        self,
        conn: CustomerConnection,
        message: SequencedMessage,
        *,
        replayed: bool = False,
        live: bool = False,
        track: bool = False,
    ) -> None:
        if track:
            # Check ownership before sending: a fenced connection must not deliver more.
            await self._write(conn, _progress_fields(conn, message), required=False)
        await conn.send(message.frame(replayed=replayed))
        conn.last_sent = message.sequence_number
        conn.sent_ids[message.sequence_number] = message.message_id
        if live:
            DELIVERY_LATENCY.observe(max(0.0, (utcnow() - message.timestamp).total_seconds()))
        try:
            await self.cursors.record_delivered(conn.conversation_id, conn.customer_id, message.sequence_number)
        except CursorUnavailable:
            pass  # only used to validate REST acks; WebSocket acks use last_sent

    # Client actions

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
        if sequence > conn.last_acked:
            previous = conn.last_acked
            conn.last_acked = sequence
            try:
                await self._write(
                    conn,
                    {"last_acknowledged_sequence": sequence, "last_acknowledged_message_id": expected,
                     "pending_message_ids": conn.pending_ids()},
                    required=True,
                    publish="ack",
                )  # fmt: skip
            except Exception:
                conn.last_acked = previous
                raise
        cursor = await self.cursors.advance(
            conn.conversation_id, conn.customer_id, sequence, expected, session_version=conn.session_version
        )
        log.info(
            "cursor updated",
            extra={"conversation_id": conn.conversation_id, "sequence_number": cursor.last_acknowledged_sequence,
                   "message_id": cursor.last_message_id, "session_version": conn.session_version},
        )  # fmt: skip
        return cursor.model_dump()

    async def typing(self, conn: CustomerConnection, is_typing: bool) -> None:
        await self._write(conn, {"typing_status": "customer_typing" if is_typing else ""}, publish="typing")

    # Session writes and fencing

    async def _write(self, conn, fields: dict, *, required: bool = False, publish: str | None = None, bump=True):
        """Optimistic snapshot write with this connection's version.

        Raises ConnectionFenced if a newer connection owns the session. If Redis is
        down, raises CursorUnavailable when ``required``, otherwise carries on.
        """
        if self.sessions is None or not conn.tracked or conn.fenced:
            if conn.fenced:
                raise ConnectionFenced(conn.conversation_id)
            if required and self.sessions is not None:
                raise CursorUnavailable("session not claimed: Redis was unavailable at connect")
            return
        async with conn.state_lock:
            try:
                conn.session_version = await self.sessions.update(
                    conn.conversation_id, conn.customer_id, conn.session_version, fields, bump=bump
                )
            except StaleSession as exc:
                await self._fence(conn, exc.stored_version, exc.owner_pod, "redis_cas")
                raise ConnectionFenced(conn.conversation_id) from exc
            except SessionUnavailable as exc:
                log.warning(
                    "session snapshot not written: redis unavailable", extra={"conversation_id": conn.conversation_id}
                )
                if required:
                    raise CursorUnavailable(str(exc)) from exc
                return
        log.debug("session snapshot written", extra={"conversation_id": conn.conversation_id,
                                                     "session_version": conn.session_version})  # fmt: skip
        if publish:
            await self._publish_current(conn, publish)

    async def _record_progress(self, conn: CustomerConnection, publish: str | None) -> None:
        if conn.last_sent in conn.sent_ids:
            fields = {"last_sent_sequence": conn.last_sent, "pending_message_ids": conn.pending_ids()}
            await self._write(conn, fields, publish=publish)

    async def _publish_current(self, conn: CustomerConnection, reason: str) -> None:
        if self.publisher is None or self.sessions is None:
            return
        try:
            snapshot = await self.sessions.get(conn.conversation_id, conn.customer_id)
        except SessionUnavailable:
            return
        if snapshot is not None:
            await self._publish(snapshot, reason)

    async def _publish(self, snapshot: SessionSnapshot, reason: str) -> None:
        if self.publisher is not None:
            with tracer().start_as_current_span("kafka.session_state.publish", attributes={"engagehub.reason": reason}):
                await self.publisher.publish(snapshot, reason)

    async def _fence(self, conn: CustomerConnection, stored_version: int, owner_pod: str, source: str) -> None:
        if conn.fenced:
            return
        conn.fenced = True
        STALE_POD_UPDATES_REJECTED.inc()
        SESSION_CONFLICTS.labels(source).inc()
        context = {"conversation_id": conn.conversation_id, "pod": self.pod, "connection_version": conn.session_version,
                   "current_version": stored_version, "owner_pod": owner_pod, "source": source}  # fmt: skip
        log.warning("stale session update rejected", extra=context)
        log.warning("old pod fenced", extra=context)
        try:
            await conn.send({"type": "session_superseded", "conversation_id": conn.conversation_id,
                             "owner_pod": owner_pod, "session_version": stored_version})  # fmt: skip
        except Exception:
            pass
        await self.close(conn, CLOSE_SUPERSEDED, "session claimed by a newer connection")

    async def on_session_state(self, snapshot: SessionSnapshot) -> None:
        """A newer snapshot arrived on chat.session-state: fence local sockets it supersedes."""
        for conn in list(self.connections.get(snapshot.conversation_id, ())):
            if conn.connection_id != snapshot.connection_id and snapshot.session_version > conn.session_version:
                await self._fence(conn, snapshot.session_version, snapshot.current_chat_pod, "kafka_event")

    async def lease_loop(self, interval: float) -> None:
        """Heartbeat: renew each owned session's lease. A failed version check fences the socket."""
        while True:
            await asyncio.sleep(interval)
            await self.renew_leases()

    async def renew_leases(self) -> None:
        if self.sessions is None:
            return
        for conns in list(self.connections.values()):
            for conn in list(conns):
                if not conn.tracked or conn.fenced or conn.closed:
                    continue
                async with conn.state_lock:
                    try:
                        await self.sessions.renew_lease(conn.conversation_id, conn.customer_id, conn.session_version)
                    except StaleSession as exc:
                        stale = exc
                    except SessionUnavailable:
                        continue
                    else:
                        continue
                await self._fence(conn, stale.stored_version, stale.owner_pod, "lease")

    # Disconnect and shutdown

    async def disconnect(self, conn: CustomerConnection, reason: str) -> None:
        """Record a clean disconnect, so the next connection knows the session ended on purpose."""
        if not conn.fenced and conn.tracked:
            try:
                await self._write(conn, {"connected": False, "typing_status": "", "last_disconnect_reason": reason},
                                  publish="disconnect")  # fmt: skip
            except ConnectionFenced:
                pass
        self.forget(conn)

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

    def drain(self) -> None:
        """Stop being ready, so the load balancer sends new connections elsewhere."""
        self.draining = True
        log.info("draining: readiness now fails", extra={"pod": self.pod})

    async def shutdown(self) -> None:
        """Tell clients to reconnect elsewhere, then close with 1012 (service restart)."""
        self.draining = True
        for conns in list(self.connections.values()):
            for conn in list(conns):
                try:
                    await conn.send({"type": "server_draining", "pod": self.pod, "reconnect": True})
                except Exception:
                    pass
                if conn.tracked and not conn.fenced:
                    try:
                        await self._write(conn, {"connected": False, "last_disconnect_reason": "server_draining"},
                                          publish="draining")  # fmt: skip
                    except Exception:
                        pass
                await self.close(conn, 1012, "service restarting")


def _progress_fields(conn: CustomerConnection, message: SequencedMessage) -> dict:
    fields = {
        "last_sent_sequence": message.sequence_number,
        "pending_message_ids": [*conn.pending_ids(), message.message_id][-MAX_PENDING_IDS:],
    }
    if message.sender_type in AGENT_SENDERS:
        fields.update(
            agent_kind=message.sender_type,
            assigned_agent_type=message.sender_id,
            assigned_agent_id=message.sender_id,
            assigned_agent_name=message.sender_name,
        )
    return fields


def _session_view(snapshot: SessionSnapshot | None) -> dict:
    if snapshot is None:
        return {"workflow_state": "ACTIVE", "assigned_agent_type": "", "assigned_agent_id": "",
                "assigned_agent_name": "", "agent_kind": "", "typing_status": ""}  # fmt: skip
    return {
        "workflow_state": snapshot.workflow_state,
        "assigned_agent_type": snapshot.assigned_agent_type,
        "assigned_agent_id": snapshot.assigned_agent_id,
        "assigned_agent_name": snapshot.assigned_agent_name,
        "agent_kind": snapshot.agent_kind,
        "typing_status": snapshot.typing_status,
    }
