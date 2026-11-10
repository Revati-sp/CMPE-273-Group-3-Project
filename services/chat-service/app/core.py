"""Chat service core, independent of the WebSocket transport so it can be failure-tested.

A pod keeps no state that matters: everything needed to continue a conversation
is in Redis, Kafka or the transcript. Local state is only the socket registry.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from engagehub_common.auth import ROLE_ADMIN, ROLE_AGENT, ROLE_CUSTOMER, Principal
from engagehub_common.config import ServiceSettings
from engagehub_common.events import (
    MESSAGE_EVENT_TYPES,
    EventEnvelope,
    EventType,
    new_id,
    sender,
    utcnow,
)
from engagehub_common.messaging.bus import EventProducer, PublishError, Record
from engagehub_common.metrics import (
    DUPLICATES_SUPPRESSED,
    MESSAGES_REPLAYED,
    RECONNECTS,
    SESSION_MIGRATIONS,
    WS_CONNECTIONS,
)
from engagehub_common.session.history import HistoryClient, HistoryUnavailable
from engagehub_common.session.log import ConversationLog, SessionNotFound, StaleConnection
from engagehub_common.session.store import (
    WORKFLOW_RESOLVED,
    SessionState,
    SessionStore,
)
from engagehub_common.topics import SESSION_EVENTS
from pydantic import ValidationError
from redis.exceptions import RedisError

from app.protocol import (
    CLOSE_FORBIDDEN,
    CLOSE_NOT_FOUND,
    CLOSE_SUPERSEDED,
    AckFrame,
    ClientFrame,
    HelloFrame,
    MessageFrame,
    PingFrame,
    ResyncFrame,
    TypingFrame,
)

log = logging.getLogger(__name__)

SendFunc = Callable[[dict], Awaitable[None]]
CloseFunc = Callable[[int, str], Awaitable[None]]

# A reservation younger than this probably has a live writer, so wait for it.
PENDING_GRACE_MS = 1500
SEND_TIMEOUT_SECONDS = 5
FORWARDED_STATE_EVENTS = {
    EventType.AGENT_ASSIGNED,
    EventType.AGENT_TRANSFERRED,
    EventType.AGENT_RELEASED,
    EventType.HANDOFF_REQUESTED,
    EventType.CONVERSATION_RESOLVED,
    EventType.SESSION_MIGRATED,
}


class ConnectionRejected(Exception):
    def __init__(self, code: int, reason: str, retryable: bool = False) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.retryable = retryable


@dataclass(eq=False)
class Connection:
    principal: Principal
    conversation_id: str
    customer_id: str
    epoch: int
    send_frame: SendFunc
    close: CloseFunc
    sent: set[int] = field(default_factory=set)
    replaying: bool = True
    held_back: list[EventEnvelope] = field(default_factory=list)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: bool = False

    @property
    def participant(self) -> str:
        return self.principal.subject

    @property
    def is_customer(self) -> bool:
        return self.principal.role == ROLE_CUSTOMER

    async def send(self, frame: dict) -> None:
        if self.closed:
            return
        async with self.send_lock:
            await asyncio.wait_for(self.send_frame(frame), timeout=SEND_TIMEOUT_SECONDS)

    async def shut(self, code: int, reason: str) -> None:
        if not self.closed:
            self.closed = True
            try:
                await self.close(code, reason)
            except Exception:
                log.debug("socket already closed", exc_info=True)


@dataclass(frozen=True)
class ReplayResult:
    messages: list[EventEnvelope]
    through_seq: int
    complete: bool
    source: str


def message_frame(event: EventEnvelope, *, replayed: bool = False) -> dict:
    return {
        "type": "message",
        "message_id": event.message_id,
        "sequence_number": event.sequence_number,
        "conversation_id": event.conversation_id,
        "event_type": event.event_type.value,
        "text": event.payload.get("text", ""),
        "sender": event.payload.get("sender", {}),
        "metadata": event.payload.get("metadata", {}),
        "timestamp": event.timestamp.isoformat(),
        "correlation_id": event.correlation_id,
        "replayed": replayed,
    }


class ChatCore:
    def __init__(
        self,
        settings: ServiceSettings,
        store: SessionStore,
        producer: EventProducer,
        history: HistoryClient,
        *,
        pod_name: str | None = None,
    ) -> None:
        self.settings = settings
        self.pod = pod_name or settings.pod_name
        self.store = store
        self.producer = producer
        self.history = history
        self.log = ConversationLog(store, producer, settings.service_name, restore=history.restore_session)
        self.connections: dict[tuple[str, str], set[Connection]] = defaultdict(set)

    # Conversation creation
    async def create_conversation(
        self, principal: Principal, *, subject: str, language: str, correlation_id: str
    ) -> SessionState:
        state = SessionState(
            conversation_id=new_id(),
            session_id=new_id(),
            tenant_id=principal.tenant_id,
            customer_id=principal.subject,
            language=language,
            current_pod=self.pod,
        )
        await self.store.create(state)
        await self.producer.publish(
            SESSION_EVENTS,
            EventEnvelope(
                event_type=EventType.CONVERSATION_STARTED,
                conversation_id=state.conversation_id,
                tenant_id=state.tenant_id,
                customer_id=state.customer_id,
                correlation_id=correlation_id,
                producer_service=self.settings.service_name,
                payload={"session_id": state.session_id, "subject": subject, "language": language},
            ),
        )
        await self.log.append(
            tenant_id=state.tenant_id,
            conversation_id=state.conversation_id,
            customer_id=state.customer_id,
            event_type=EventType.SYSTEM_MESSAGE,
            payload={
                "text": "Hi! You're connected to EngageHub support. How can we help you today?",
                "sender": sender("system", "engagehub", "EngageHub"),
            },
            idempotency_key=f"greeting:{state.conversation_id}",
            correlation_id=correlation_id,
        )
        return state

    # Connection lifecycle
    async def open_connection(
        self, principal: Principal, hello: HelloFrame, send: SendFunc, close: CloseFunc
    ) -> Connection:
        """Authorise the participant, take over the session and replay what is missing."""
        tenant_id, conversation_id = principal.tenant_id, hello.conversation_id
        state = await self._load_session(tenant_id, conversation_id)
        self._authorize_connection(principal, state)

        registered = await self.store.connect(
            tenant_id, conversation_id, principal.subject, self.pod, principal.role == ROLE_CUSTOMER
        )
        if registered is None:
            raise ConnectionRejected(CLOSE_NOT_FOUND, "conversation not found")
        epoch, previous_pod = registered

        conn = Connection(principal, conversation_id, state.customer_id, epoch, send, close)
        # Register before replay, so anything published meanwhile is held back and not missed.
        self.connections[(tenant_id, conversation_id)].add(conn)
        WS_CONNECTIONS.inc()

        migrated = bool(previous_pod) and previous_pod != self.pod
        stored_ack = await self.store.participant_ack(tenant_id, conversation_id, principal.subject)
        after_seq = hello.last_seen_seq if hello.last_seen_seq is not None else stored_ack
        # A client claiming more than exists must not skip messages issued later.
        after_seq = min(after_seq, state.seq)

        if hello.last_seen_seq is not None and hello.last_seen_seq > 0:
            RECONNECTS.labels(tenant_id).inc()
        if migrated:
            SESSION_MIGRATIONS.labels(tenant_id).inc()
        await self._announce_connection(conn, state, previous_pod, migrated)

        await conn.send(
            {
                "type": "welcome",
                "conversation_id": conversation_id,
                "session_id": state.session_id,
                "pod": self.pod,
                "epoch": epoch,
                "migrated": migrated,
                "previous_pod": previous_pod,
                "resume_after_seq": after_seq,
                "last_ack_seq": state.last_ack_seq,
                "last_ack_message_id": state.last_ack_message_id,
                "head_seq": state.seq,
                "workflow_state": state.workflow_state,
                "assigned_agent_type": state.assigned_agent_type,
                "assigned_agent_id": state.assigned_agent_id,
                "assigned_agent_name": state.assigned_agent_name,
                "typing": state.typing,
            }
        )
        await self.replay_to(conn, after_seq, migrated=migrated)
        return conn

    async def _load_session(self, tenant_id: str, conversation_id: str) -> SessionState:
        state = await self.store.get(tenant_id, conversation_id)
        if state is not None:
            return state
        # Hot state is gone, for instance after a full Redis loss: rebuild from durable storage.
        try:
            found = await self.history.restore_session(tenant_id, conversation_id)
        except HistoryUnavailable as exc:
            raise ConnectionRejected(1013, "session temporarily unavailable", retryable=True) from exc
        state = await self.store.get(tenant_id, conversation_id) if found else None
        if state is None:
            raise ConnectionRejected(CLOSE_NOT_FOUND, "conversation not found")
        return state

    @staticmethod
    def _authorize_connection(principal: Principal, state: SessionState) -> None:
        if principal.tenant_id != state.tenant_id:
            raise ConnectionRejected(CLOSE_NOT_FOUND, "conversation not found")
        if principal.role == ROLE_CUSTOMER and principal.subject != state.customer_id:
            raise ConnectionRejected(CLOSE_NOT_FOUND, "conversation not found")
        if principal.role not in (ROLE_CUSTOMER, ROLE_AGENT, ROLE_ADMIN):
            raise ConnectionRejected(CLOSE_FORBIDDEN, "role cannot join chat")

    async def _announce_connection(self, conn, state, previous_pod: str, migrated: bool) -> None:
        event_type = EventType.SESSION_MIGRATED if migrated else EventType.SESSION_CONNECTED
        try:
            await self.producer.publish(
                SESSION_EVENTS,
                EventEnvelope(
                    event_type=event_type,
                    conversation_id=conn.conversation_id,
                    tenant_id=conn.principal.tenant_id,
                    customer_id=conn.customer_id,
                    producer_service=self.settings.service_name,
                    payload={
                        "participant": conn.participant,
                        "epoch": conn.epoch,
                        "pod": self.pod,
                        "from_pod": previous_pod,
                        "session_id": state.session_id,
                    },
                ),
            )
        except PublishError:
            # Only informational (and fencing has a second layer in Redis): do not block a reconnect.
            log.warning("could not announce connection")

    async def close_connection(self, conn: Connection, *, clean: bool) -> None:
        key = (conn.principal.tenant_id, conn.conversation_id)
        if conn in self.connections.get(key, ()):
            self.connections[key].discard(conn)
            WS_CONNECTIONS.dec()
            if not self.connections[key]:
                del self.connections[key]
        conn.closed = True
        if clean:
            try:
                await self.store.disconnect(*key, conn.participant, conn.epoch)
            except (RedisError, OSError):
                log.warning("could not record clean disconnect")

    async def shutdown(self) -> None:
        """On SIGTERM, tell clients to reconnect. Close code 1012 means service restart."""
        for conns in list(self.connections.values()):
            for conn in list(conns):
                await conn.shut(1012, "service restarting")

    # Replay
    async def compute_replay(self, tenant_id: str, conversation_id: str, after_seq: int) -> ReplayResult:
        """Find every durable message above ``after_seq``.

        1. Finish reservations abandoned by a failed writer.
        2. Serve from the Redis buffer of unacknowledged messages if it has no gaps.
        3. Otherwise read the transcript and Kafka tail through session-service.
        """
        now_ms = int(utcnow().timestamp() * 1000)
        in_flight: int | None = None
        try:
            _, in_flight = await self.log.reconcile_pending(
                tenant_id, conversation_id, older_than_ms=PENDING_GRACE_MS, now_ms=now_ms
            )
        except PublishError:
            pending = await self.store.pending(tenant_id, conversation_id)
            in_flight = min(pending) if pending else None
            log.warning("could not reconcile pending messages: broker unavailable")

        state = await self.store.get(tenant_id, conversation_id)
        head = state.seq if state else after_seq
        settled_head = in_flight - 1 if in_flight is not None else head

        hot = await self.store.hot_log_after(tenant_id, conversation_id, after_seq)
        by_seq = {e.sequence_number: e for e in map(EventEnvelope.model_validate_json, hot)}
        source = "redis"
        if not _contiguous(by_seq, after_seq, settled_head):
            try:
                durable = await self.history.messages_after(tenant_id, conversation_id, after_seq)
            except HistoryUnavailable:
                # Report only what is certain. The client retries with a resync.
                log.warning("durable history unavailable; partial replay")
                return ReplayResult(
                    [by_seq[s] for s in sorted(by_seq)],
                    _contiguous_head(by_seq, after_seq),
                    complete=False,
                    source="redis_partial",
                )
            for event in durable:
                by_seq.setdefault(event.sequence_number, event)
            source = "durable"
        return ReplayResult([by_seq[s] for s in sorted(by_seq)], settled_head, complete=True, source=source)

    async def replay_to(self, conn: Connection, after_seq: int, *, migrated: bool = False) -> None:
        conn.replaying = True
        tenant_id = conn.principal.tenant_id
        result = await self.compute_replay(tenant_id, conn.conversation_id, after_seq)
        delivered = 0
        for event in result.messages:
            if event.sequence_number not in conn.sent:
                conn.sent.add(event.sequence_number)
                await conn.send(message_frame(event, replayed=True))
                delivered += 1
        MESSAGES_REPLAYED.labels(result.source).inc(delivered)
        # Flush what the fan-out delivered during replay. Connection.sent removes overlaps.
        conn.replaying = False
        held, conn.held_back = conn.held_back, []
        for event in sorted(held, key=lambda e: e.sequence_number):
            await self._deliver(conn, event)
        await conn.send(
            {
                "type": "replay_complete",
                "after_seq": after_seq,
                "through_seq": result.through_seq,
                "count": delivered,
                "complete": result.complete,
                "source": result.source,
            }
        )
        if delivered and (migrated or after_seq > 0):
            try:
                await self.producer.publish(
                    SESSION_EVENTS,
                    EventEnvelope(
                        event_type=EventType.SESSION_REPLAYED,
                        conversation_id=conn.conversation_id,
                        tenant_id=tenant_id,
                        customer_id=conn.customer_id,
                        producer_service=self.settings.service_name,
                        payload={"count": delivered, "source": result.source, "after_seq": after_seq},
                    ),
                )
            except PublishError:
                log.warning("could not publish replay event")

    # Frames from the client
    async def handle_frame(self, conn: Connection, raw: object) -> None:
        try:
            frame = ClientFrame.validate_python(raw)
        except ValidationError as exc:
            client_id = raw.get("client_message_id") if isinstance(raw, dict) else None
            await conn.send(
                {
                    "type": "error",
                    "code": "invalid_frame",
                    "retryable": False,
                    "client_message_id": client_id,
                    "detail": f"{exc.error_count()} validation error(s)",
                }
            )
            return
        match frame:
            case MessageFrame():
                await self._on_message(conn, frame)
            case AckFrame():
                await self._on_ack(conn, frame)
            case TypingFrame():
                await self._on_typing(conn, frame)
            case ResyncFrame():
                await self.replay_to(conn, frame.after_seq)
            case PingFrame():
                await conn.send({"type": "pong"})

    async def _on_message(self, conn: Connection, frame: MessageFrame) -> None:
        principal = conn.principal
        tenant_id, conversation_id = principal.tenant_id, conn.conversation_id
        try:
            state = await self.store.get(tenant_id, conversation_id)
            rejection = _send_rejection(principal, state)
            if rejection:
                await conn.send(
                    {
                        "type": "error",
                        "code": rejection,
                        "retryable": False,
                        "client_message_id": frame.client_message_id,
                    }
                )
                return
            is_customer = principal.role == ROLE_CUSTOMER
            kind = "customer" if is_customer else "human_agent"
            result = await self.log.append(
                tenant_id=tenant_id,
                conversation_id=conversation_id,
                customer_id=conn.customer_id,
                event_type=EventType.CUSTOMER_MESSAGE if is_customer else EventType.HUMAN_MESSAGE,
                payload={
                    "text": frame.text,
                    "sender": sender(kind, principal.subject, principal.name),
                    "client_message_id": frame.client_message_id,
                },
                idempotency_key=f"client:{principal.subject}:{frame.client_message_id}",
                participant=principal.subject,
                epoch=conn.epoch,
            )
        except StaleConnection:
            await conn.send({"type": "error", "code": "superseded", "retryable": False})
            await conn.shut(CLOSE_SUPERSEDED, "superseded by a newer connection")
            return
        except SessionNotFound:
            await conn.send(
                {
                    "type": "error",
                    "code": "conversation_not_found",
                    "retryable": False,
                    "client_message_id": frame.client_message_id,
                }
            )
            return
        except (PublishError, RedisError, OSError, TimeoutError, HistoryUnavailable) as exc:
            # Not durable, so no ack: the client keeps the message and retries with the same key.
            log.warning("message not accepted", extra={"error": repr(exc)})
            await conn.send(
                {
                    "type": "error",
                    "code": "temporarily_unavailable",
                    "retryable": True,
                    "client_message_id": frame.client_message_id,
                }
            )
            return
        event = result.event
        conn.sent.add(event.sequence_number)
        await conn.send(
            {
                "type": "message_ack",
                "client_message_id": frame.client_message_id,
                "duplicate": result.duplicate,
                "message": message_frame(event),
            }
        )

    async def _on_ack(self, conn: Connection, frame: AckFrame) -> None:
        result = await self.store.record_ack(
            conn.principal.tenant_id,
            conn.conversation_id,
            conn.participant,
            frame.up_to_seq,
            is_customer=conn.is_customer,
            message_id=frame.message_id,
        )
        if result == -1:
            await conn.send({"type": "error", "code": "invalid_ack", "retryable": False})

    async def _on_typing(self, conn: Connection, frame: TypingFrame) -> None:
        tenant_id = conn.principal.tenant_id
        try:
            await self.store.update(
                tenant_id,
                conn.conversation_id,
                typing=f"{conn.principal.role}:{conn.participant}" if frame.is_typing else "",
            )
            await self.producer.publish(
                SESSION_EVENTS,
                EventEnvelope(
                    event_type=EventType.TYPING,
                    conversation_id=conn.conversation_id,
                    tenant_id=tenant_id,
                    customer_id=conn.customer_id,
                    producer_service=self.settings.service_name,
                    payload={
                        "participant": conn.participant,
                        "role": conn.principal.role,
                        "is_typing": frame.is_typing,
                    },
                ),
            )
        except (PublishError, RedisError, OSError):
            log.debug("typing indicator dropped")  # Ephemeral, so safe to lose.

    # Fan-out from Kafka to sockets connected to this pod
    async def dispatch(self, event: EventEnvelope, _record: Record | None = None) -> None:
        conns = self.connections.get((event.tenant_id, event.conversation_id or ""))
        if not conns:
            return
        for conn in list(conns):
            try:
                if event.event_type in MESSAGE_EVENT_TYPES:
                    await self._deliver(conn, event)
                elif event.event_type in (EventType.SESSION_CONNECTED, EventType.SESSION_MIGRATED):
                    await self._apply_takeover(conn, event)
                elif event.event_type == EventType.TYPING:
                    if event.payload.get("participant") != conn.participant:
                        await conn.send({"type": "typing", **event.payload})
                elif event.event_type in FORWARDED_STATE_EVENTS:
                    await conn.send(_state_frame(event))
            except Exception:
                # A bad socket must not stop the fan-out for everyone else.
                log.info("dropping unresponsive connection", exc_info=True)
                await conn.shut(1011, "send failed")

    async def _deliver(self, conn: Connection, event: EventEnvelope) -> None:
        if conn.replaying:
            conn.held_back.append(event)
            return
        if event.sequence_number in conn.sent:
            DUPLICATES_SUPPRESSED.labels("fanout").inc()
            return
        conn.sent.add(event.sequence_number)
        await conn.send(message_frame(event))

    async def _apply_takeover(self, conn: Connection, event: EventEnvelope) -> None:
        """Fence off an old socket when the same participant connected elsewhere."""
        payload = event.payload
        if payload.get("participant") == conn.participant and int(payload.get("epoch", 0)) > conn.epoch:
            await conn.shut(CLOSE_SUPERSEDED, "superseded by a newer connection")
            return
        if event.event_type == EventType.SESSION_MIGRATED and payload.get("participant") != conn.participant:
            await conn.send(_state_frame(event))


def _state_frame(event: EventEnvelope) -> dict:
    return {
        "type": "state",
        "event_type": event.event_type.value,
        "conversation_id": event.conversation_id,
        "payload": {k: v for k, v in event.payload.items() if k != "summary"},
    }


def _send_rejection(principal: Principal, state: SessionState | None) -> str | None:
    if state is None:
        return None  # ConversationLog restores the session or raises SessionNotFound.
    if state.workflow_state == WORKFLOW_RESOLVED:
        return "conversation_resolved"
    if principal.role == ROLE_ADMIN:
        return "read_only"
    if principal.role == ROLE_AGENT and state.assigned_agent_id != principal.subject:
        return "not_assigned"
    return None


def _contiguous(by_seq: dict[int, EventEnvelope], after_seq: int, head: int) -> bool:
    return all(seq in by_seq for seq in range(after_seq + 1, head + 1))


def _contiguous_head(by_seq: dict[int, EventEnvelope], after_seq: int) -> int:
    seq = after_seq
    while seq + 1 in by_seq:
        seq += 1
    return seq
