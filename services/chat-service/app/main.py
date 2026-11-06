"""Chat service: WebSocket connections, sequencing, replay and live fan-out."""

import asyncio
import logging

from engagehub_common.auth import ROLE_CUSTOMER, Principal, TokenError, require_roles, verify_token
from engagehub_common.config import ServiceSettings
from engagehub_common.logging_setup import bind_event_context
from engagehub_common.messaging.bus import PublishError
from engagehub_common.service import Runtime, create_service_app
from engagehub_common.session.grpc_client import GrpcHistoryClient
from engagehub_common.session.store import SessionStore
from engagehub_common.topics import AGENT_EVENTS, CHAT_MESSAGES, ESCALATIONS, SESSION_EVENTS
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError
from redis.exceptions import RedisError

from app.core import ChatCore, ConnectionRejected
from app.protocol import CLOSE_UNAUTHORIZED, HelloFrame

log = logging.getLogger(__name__)

HELLO_TIMEOUT_SECONDS = 5
router = APIRouter()


class CreateConversationRequest(BaseModel):
    subject: str = Field(default="", max_length=200)
    language: str = Field(default="en", pattern="^[a-z]{2}$")


@router.post("/conversations", status_code=201)
async def create_conversation(
    body: CreateConversationRequest,
    request: Request,
    principal: Principal = Depends(require_roles(ROLE_CUSTOMER)),
) -> dict:
    core: ChatCore = request.app.state.core
    try:
        state = await core.create_conversation(
            principal,
            subject=body.subject,
            language=body.language,
            correlation_id=request.state.correlation_id,
        )
    except (PublishError, RedisError, OSError) as exc:
        raise HTTPException(503, "could not start conversation, retry shortly") from exc
    return {
        "conversation_id": state.conversation_id,
        "session_id": state.session_id,
        "tenant_id": state.tenant_id,
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

    app = create_service_app("EngageHub Chat Service", settings, runtime=runtime, kafka=True, redis=True, setup=setup)
    app.include_router(router)
    return app
