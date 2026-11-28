"""Chat service: WebSocket connections, sequencing, replay and live fan-out."""

import asyncio
import logging

from engagehub_common.auth import ROLE_CUSTOMER, Principal, TokenError, require_roles, verify_token
from engagehub_common.config import ServiceSettings
from engagehub_common.events import new_id
from engagehub_common.logging_setup import bind_event_context
from engagehub_common.messaging.bus import PublishError
from engagehub_common.service import Runtime, create_service_app
from engagehub_common.session.grpc_client import GrpcHistoryClient
from engagehub_common.session.store import SessionStore
from engagehub_common.topics import (
    AGENT_EVENTS,
    AGENT_RESPONSES,
    CHAT_MESSAGES,
    CHAT_REPLIES,
    CHAT_REPLIES_DELIVERY,
    CHAT_REPLIES_DLQ,
    ESCALATIONS,
    SESSION_EVENTS,
    SESSION_STATE,
    SESSION_STATE_DLQ,
)
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError
from redis.exceptions import RedisError

from app.core import ChatCore, ConnectionRejected
from app.protocol import CLOSE_UNAUTHORIZED, HelloFrame
from app.replies.api import router as replies_router
from app.replies.consumer import TRANSIENT_ERRORS, AgentResponseConsumer, ReplyConsumer
from app.replies.cursor import CursorStore
from app.replies.hub import ReplyHub
from app.replies.log import ConversationOwnedElsewhere, MessageLog
from app.replies.models import ReplyBase
from app.replies.models import migrate as migrate_reply_log
from app.replies.schemas import ID_PATTERN, SequencedMessage, VirtualAgentReplyProduced
from app.replies.session import SessionSnapshotStore
from app.replies.session_state import SessionStatePublisher, SessionStateTable, SessionStateUpdated

log = logging.getLogger(__name__)

HELLO_TIMEOUT_SECONDS = 5
router = APIRouter()


class CreateConversationRequest(BaseModel):
    subject: str = Field(default="", max_length=200)
    language: str = Field(default="en", pattern="^[a-z]{2}$")
    # Optional fixed ID, for example "conversation-123" in the replay demo.
    conversation_id: str | None = Field(default=None, pattern=ID_PATTERN)


@router.post("/conversations", status_code=201)
async def create_conversation(
    body: CreateConversationRequest,
    request: Request,
    response: Response,
    principal: Principal = Depends(require_roles(ROLE_CUSTOMER)),
) -> dict:
    """Create a conversation. Idempotent for the owner, so the same ID can be POSTed again."""
    core: ChatCore = request.app.state.core
    conversation_id = body.conversation_id or new_id()
    try:
        # Register in the durable reply log first: it owns the conversation's identity.
        _, created = await request.app.state.reply_log.register(conversation_id, principal.tenant_id, principal.subject)
    except ConversationOwnedElsewhere as exc:
        raise HTTPException(409, "conversation ID is already in use") from exc
    except TRANSIENT_ERRORS as exc:
        raise HTTPException(503, "could not start conversation, retry shortly") from exc
    try:
        state = await core.store.get(principal.tenant_id, conversation_id)
        if state is None:
            state = await core.create_conversation(
                principal,
                subject=body.subject,
                language=body.language,
                correlation_id=request.state.correlation_id,
                conversation_id=conversation_id,
            )
    except (PublishError, RedisError, OSError) as exc:
        raise HTTPException(503, "could not start conversation, retry shortly") from exc
    response.status_code = 201 if created else 200
    return {
        "conversation_id": state.conversation_id,
        "session_id": state.session_id,
        "tenant_id": state.tenant_id,
        "created": created,
    }


@router.websocket("/ws")
async def chat_socket(websocket: WebSocket) -> None:
    """The first frame must be ``hello``. Tokens are not put in URLs, which get logged."""
    await websocket.accept()
    core: ChatCore = websocket.app.state.core
    settings: ServiceSettings = websocket.app.state.settings
    try:
        raw = await asyncio.wait_for(websocket.receive_json(), timeout=HELLO_TIMEOUT_SECONDS)
        hello = HelloFrame.model_validate(raw)
        principal = verify_token(settings.jwt_secret, hello.token, settings.jwt_algorithm)
    except (TimeoutError, ValidationError, TokenError, ValueError, KeyError, WebSocketDisconnect):
        await _close_quietly(websocket, CLOSE_UNAUTHORIZED, "authentication required")
        return

    bind_event_context(tenant_id=principal.tenant_id, conversation_id=hello.conversation_id)
    try:
        conn = await core.open_connection(principal, hello, websocket.send_json, websocket.close)
    except ConnectionRejected as exc:
        await _close_quietly(websocket, exc.code, exc.reason)
        return
    except (RedisError, OSError) as exc:
        log.warning("cannot open connection", extra={"error": repr(exc)})
        await _close_quietly(websocket, 1013, "try again later")
        return

    clean = False
    try:
        while True:
            await core.handle_frame(conn, await websocket.receive_json())
    except WebSocketDisconnect as exc:
        clean = exc.code in (1000, 1001)
    except (ValueError, RuntimeError):
        await _close_quietly(websocket, 1003, "invalid frame")
    except (RedisError, OSError) as exc:
        log.warning("connection dropped", extra={"error": repr(exc)})
        await _close_quietly(websocket, 1013, "try again later")
    finally:
        await core.close_connection(conn, clean=clean)


async def _close_quietly(websocket: WebSocket, code: int, reason: str) -> None:
    try:
        await websocket.close(code=code, reason=reason)
    except RuntimeError:
        pass


def create_app(runtime: Runtime | None = None, history=None, pod_name: str | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="chat-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(ReplyBase)
        await migrate_reply_log(rt.db)
        store = SessionStore(rt.redis, settings.session_ttl_seconds)
        client = history or GrpcHistoryClient(settings.session_grpc_target, settings.jwt_secret, settings.service_name)
        core = ChatCore(settings, store, rt.producer, client, pod_name=pod_name)
        app.state.core = core
        # Each pod sees every live event but only acts on its own sockets. No consumer
        # group and no commits: a reconnecting client catches up through replay instead.
        rt.add_consumer(
            [CHAT_MESSAGES, SESSION_EVENTS, AGENT_EVENTS, ESCALATIONS],
            None,
            core.dispatch,
            dedupe=False,
            offset_reset="latest",
            commit=False,
        )
        rt.shutdown.append(core.shutdown)
        if history is None:
            rt.shutdown.append(client.close)
        setup_reply_stream(app, rt, pod_name or settings.pod_name, platform_sessions=store)

    app = create_service_app(
        "EngageHub Chat Service", settings, runtime=runtime, kafka=True, redis=True, db=True, setup=setup
    )
    app.include_router(router)
    app.include_router(replies_router)
    return app


def setup_reply_stream(app: FastAPI, rt: Runtime, pod: str, platform_sessions: SessionStore | None = None) -> None:
    """chat.replies and agent.responses -> Chat Consumer -> PostgreSQL log -> fan-out -> customer sockets,
    with versioned session snapshots in Redis, mirrored to the compacted chat.session-state topic."""
    settings = rt.settings
    message_log = MessageLog(rt.db)
    state_table = SessionStateTable()
    hub = ReplyHub(
        message_log,
        CursorStore(rt.redis, settings.cursor_ttl_seconds),
        pod,
        sessions=SessionSnapshotStore(rt.redis, settings.session_snapshot_ttl_seconds, settings.session_lease_seconds),
        state_table=state_table,
        publisher=SessionStatePublisher(rt.producer),
        platform=platform_sessions,
    )
    state_table.on_newer = hub.on_session_state
    app.state.reply_log = message_log
    app.state.reply_hub = hub
    # One consumer group across all pods: each reply is sequenced exactly once.
    # The database's unique constraints make it idempotent, so Redis dedupe is not used.
    rt.add_consumer(
        [CHAT_REPLIES],
        "chat-replies-consumer",
        ReplyConsumer(message_log, rt.producer).handle,
        dedupe=False,
        parser=VirtualAgentReplyProduced.parse,
        dead_letter_topic=CHAT_REPLIES_DLQ,
        transient_errors=TRANSIENT_ERRORS,
    )
    # Real virtual-agent responses. A separate group, so the fake-producer path is unaffected.
    rt.add_consumer(
        [AGENT_RESPONSES],
        "chat-agent-responses-consumer",
        AgentResponseConsumer(message_log, rt.producer).handle,
        dedupe=False,
        dead_letter_topic=CHAT_REPLIES_DLQ,
        transient_errors=TRANSIENT_ERRORS,
    )
    # Fan-out: every pod reads every sequenced message and pushes it to its own sockets.
    rt.add_consumer(
        [CHAT_REPLIES_DELIVERY],
        None,
        hub.deliver_record,
        dedupe=False,
        offset_reset="latest",
        commit=False,
        parser=SequencedMessage.parse,
        dead_letter_topic=CHAT_REPLIES_DLQ,
    )
    # Every pod materialises the compacted session-state topic from the beginning (no group),
    # as the fallback when Redis is down and to fence sockets that a newer claim superseded.
    rt.add_consumer(
        [SESSION_STATE],
        None,
        state_table.handle,
        dedupe=False,
        offset_reset="earliest",
        commit=False,
        parser=SessionStateUpdated.parse,
        dead_letter_topic=SESSION_STATE_DLQ,
    )
    rt.background.append(asyncio.create_task(hub.lease_loop(settings.session_lease_renew_seconds)))

    async def not_draining() -> None:
        if hub.draining:
            raise RuntimeError("draining")

    rt.readiness.append(not_draining)
    # Close customer sockets first on shutdown, so clients start reconnecting at once.
    rt.shutdown.insert(0, hub.shutdown)
