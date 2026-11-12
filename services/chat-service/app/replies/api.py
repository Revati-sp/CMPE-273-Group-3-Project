"""REST and WebSocket API for the customer reply stream."""

from __future__ import annotations

import asyncio
import logging

from engagehub_common.auth import ROLE_CUSTOMER, Principal, TokenError, get_principal, require_roles, verify_token
from engagehub_common.events import new_id, utcnow
from engagehub_common.logging_setup import bind_event_context
from engagehub_common.messaging.bus import PublishError
from engagehub_common.topics import CHAT_REPLIES
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError

from app.replies.consumer import TRANSIENT_ERRORS, publish_delivery
from app.replies.cursor import CursorUnavailable
from app.replies.hub import CustomerConnection, ReplyHub
from app.replies.log import MessageLog
from app.replies.metrics import CURSOR_UPDATES
from app.replies.schemas import (
    ID_PATTERN,
    AckFrame,
    CustomerMessageFrame,
    HelloFrame,
    PingFrame,
    SenderType,
    VirtualAgentReplyProduced,
    customer_message_id,
)

log = logging.getLogger(__name__)
router = APIRouter()

HELLO_TIMEOUT_SECONDS = 5
CLOSE_UNAUTHORIZED = 4401
CLOSE_NOT_FOUND = 4404
CLOSE_TRY_AGAIN = 1013


class AckRequest(BaseModel):
    sequence_number: int = Field(ge=1)
    message_id: str = ""


class FakeReplyRequest(BaseModel):
    content: str = Field(default="", max_length=4000)
    count: int = Field(default=1, ge=1, le=20)
    sender_type: SenderType = "virtual_agent"
    sender_id: str = Field(default="order-tracking-agent", max_length=128)
    sender_name: str = Field(default="Order Tracking Agent", max_length=128)
    message_id: str | None = Field(default=None, pattern=ID_PATTERN)  # reuse one to demonstrate deduplication


def _log(request_or_ws) -> MessageLog:
    return request_or_ws.app.state.reply_log


def _hub(request_or_ws) -> ReplyHub:
    return request_or_ws.app.state.reply_hub


async def _authorize(message_log: MessageLog, principal: Principal, conversation_id: str):
    """Customers reach only their own conversations, staff only their own tenant's."""
    try:
        conversation = await message_log.get_conversation(conversation_id)
    except TRANSIENT_ERRORS as exc:
        raise HTTPException(503, "message log unavailable, retry shortly") from exc
    if (
        conversation is None
        or conversation.tenant_id != principal.tenant_id
        or (not principal.is_staff and conversation.customer_id != principal.subject)
    ):
        raise HTTPException(404, "conversation not found")
    return conversation


@router.get("/conversations/{conversation_id}/messages")
async def list_messages(
    conversation_id: str,
    request: Request,
    after_sequence: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=5000),
    principal: Principal = Depends(get_principal),
) -> dict:
    await _authorize(_log(request), principal, conversation_id)
    try:
        messages = await _log(request).after(conversation_id, after_sequence, limit)
        latest = await _log(request).latest_sequence(conversation_id)
    except TRANSIENT_ERRORS as exc:
        raise HTTPException(503, "message log unavailable, retry shortly") from exc
    return {
        "conversation_id": conversation_id,
        "after_sequence": after_sequence,
        "latest_sequence": latest,
        "messages": [m.frame() for m in messages],
    }


@router.get("/conversations/{conversation_id}/cursor")
async def get_cursor(conversation_id: str, request: Request, principal: Principal = Depends(get_principal)) -> dict:
    conversation = await _authorize(_log(request), principal, conversation_id)
    cursors = _hub(request).cursors
    try:
        cursor = await cursors.get(conversation_id, conversation.customer_id)
        delivered = await cursors.delivered(conversation_id, conversation.customer_id)
    except CursorUnavailable as exc:
        raise HTTPException(503, "cursor store unavailable, retry shortly") from exc
    return {
        "conversation_id": conversation_id,
        "customer_id": conversation.customer_id,
        "key": cursors.cursor_key(conversation_id, conversation.customer_id),
        "cursor": cursor.model_dump() if cursor else None,
        "last_delivered_sequence": delivered,
    }


@router.post("/conversations/{conversation_id}/ack")
async def acknowledge(
    conversation_id: str,
    body: AckRequest,
    request: Request,
    principal: Principal = Depends(require_roles(ROLE_CUSTOMER)),
) -> dict:
    await _authorize(_log(request), principal, conversation_id)
    cursors = _hub(request).cursors
    try:
        delivered = await cursors.delivered(conversation_id, principal.subject)
        if body.sequence_number > delivered:
            raise HTTPException(
                409, f"sequence {body.sequence_number} has not been delivered (last delivered {delivered})"
            )
        stored = await _log(request).message_at(conversation_id, body.sequence_number)
        if stored is None or (body.message_id and body.message_id != stored.message_id):
            raise HTTPException(409, "message ID does not match the sequence number")
        cursor = await cursors.advance(conversation_id, principal.subject, body.sequence_number, stored.message_id)
    except CursorUnavailable as exc:
        raise HTTPException(503, "cursor store unavailable, retry shortly") from exc
    except TRANSIENT_ERRORS as exc:
        raise HTTPException(503, "message log unavailable, retry shortly") from exc
    CURSOR_UPDATES.inc()
    log.info(
        "cursor updated",
        extra={"conversation_id": conversation_id, "sequence_number": cursor.last_acknowledged_sequence,
               "message_id": cursor.last_message_id, "via": "rest"},
    )  # fmt: skip
    return {"conversation_id": conversation_id, "cursor": cursor.model_dump()}


@router.post("/dev/fake-replies", status_code=202)
async def publish_fake_replies(
    body: FakeReplyRequest,
    request: Request,
    conversation_id: str = Query(pattern=ID_PATTERN),
    principal: Principal = Depends(get_principal),
) -> dict:
    """Demo helper: publishes to Kafka exactly as scripts/fake_reply_producer.py does."""
    if not request.app.state.settings.enable_dev_endpoints:
        raise HTTPException(404, "not found")
    await _authorize(_log(request), principal, conversation_id)
    producer = request.app.state.runtime.producer
    published = []
    for index in range(body.count):
        event = VirtualAgentReplyProduced(
            message_id=body.message_id or new_id(),
            conversation_id=conversation_id,
            sender_type=body.sender_type,
            sender_id=body.sender_id,
            sender_name=body.sender_name,
            content=body.content or f"Fake reply {index + 1} from {body.sender_name}",
            timestamp=utcnow(),
            correlation_id=request.state.correlation_id,
        )
        await producer.publish_raw(CHAT_REPLIES, conversation_id.encode(), event.model_dump_json().encode())
        published.append(event.message_id)
    return {"topic": CHAT_REPLIES, "message_ids": published}


@router.websocket("/ws/customer/{conversation_id}")
async def customer_socket(websocket: WebSocket, conversation_id: str) -> None:
    """First frame: {"type": "hello", "token": "<JWT>"}. Tokens never go in the URL."""
    await websocket.accept()
    settings = websocket.app.state.settings
    hub = _hub(websocket)
    try:
        hello = HelloFrame.model_validate(await asyncio.wait_for(websocket.receive_json(), HELLO_TIMEOUT_SECONDS))
        principal = verify_token(settings.jwt_secret, hello.token, settings.jwt_algorithm)
    except (TimeoutError, ValidationError, TokenError, ValueError, KeyError, WebSocketDisconnect):
        await _close(websocket, CLOSE_UNAUTHORIZED, "authentication required")
        return

    bind_event_context(correlation_id=new_id(), tenant_id=principal.tenant_id, conversation_id=conversation_id)
    try:
        conversation = await hub.log.get_conversation(conversation_id)
    except TRANSIENT_ERRORS:
        await _error(websocket, "service_unavailable", True, "message log unavailable, reconnect shortly")
        await _close(websocket, CLOSE_TRY_AGAIN, "try again later")
        return
    if (
        principal.role != ROLE_CUSTOMER
        or conversation is None
        or conversation.tenant_id != principal.tenant_id
        or conversation.customer_id != principal.subject
    ):
        await _close(websocket, CLOSE_NOT_FOUND, "conversation not found")
        return

    conn = CustomerConnection(conversation_id, principal.subject, websocket.send_json, websocket.close)
    try:
        await hub.open(conn, hello.last_received_sequence)
        while True:
            await _handle_frame(websocket, hub, conn, principal, await websocket.receive_json())
    except WebSocketDisconnect:
        pass
    except TRANSIENT_ERRORS:
        log.warning("dependency failed during session", exc_info=True)
        await _error(websocket, "service_unavailable", True, "service unavailable, reconnect shortly")
        await _close(websocket, CLOSE_TRY_AGAIN, "try again later")
    except (ValueError, RuntimeError):
        await _close(websocket, 1003, "invalid frame")
    finally:
        hub.forget(conn)


async def _handle_frame(websocket, hub: ReplyHub, conn: CustomerConnection, principal: Principal, raw) -> None:
    frame_type = raw.get("type") if isinstance(raw, dict) else None
    try:
        if frame_type == "ack":
            frame = AckFrame.model_validate(raw)
            try:
                cursor = await hub.acknowledge(conn, frame.sequence_number, frame.message_id)
            except ValueError as exc:
                await _error(websocket, "invalid_ack", False, str(exc))
                return
            except CursorUnavailable:
                await _error(websocket, "cursor_unavailable", True, "acknowledgement not saved, resend it")
                return
            CURSOR_UPDATES.inc()
            await conn.send({"type": "ack_confirmed", "conversation_id": conn.conversation_id, "cursor": cursor})
        elif frame_type == "customer_message":
            await _customer_message(websocket, hub, conn, principal, CustomerMessageFrame.model_validate(raw))
        elif frame_type == "ping":
            PingFrame.model_validate(raw)
            await conn.send({"type": "pong"})
        else:
            await _error(websocket, "invalid_frame", False, f"unsupported frame type: {frame_type}")
    except ValidationError as exc:
        await _error(websocket, "invalid_frame", False, f"{exc.error_count()} validation error(s)")


async def _customer_message(websocket, hub: ReplyHub, conn, principal: Principal, frame: CustomerMessageFrame) -> None:
    try:
        result = await hub.log.append(
            conversation_id=conn.conversation_id,
            message_id=customer_message_id(conn.conversation_id, principal.subject, frame.client_message_id),
            sender_type="customer",
            sender_id=principal.subject,
            sender_name=principal.name,
            content=frame.content,
            timestamp=utcnow(),
            correlation_id=new_id(),
        )
    except TRANSIENT_ERRORS:
        await _error(websocket, "temporarily_unavailable", True, "message not saved, resend it",
                     client_message_id=frame.client_message_id)  # fmt: skip
        return
    await conn.send(
        {
            "type": "customer_message_accepted",
            "client_message_id": frame.client_message_id,
            "message_id": result.message.message_id,
            "sequence_number": result.message.sequence_number,
            "duplicate": result.duplicate,
        }
    )
    try:
        await publish_delivery(websocket.app.state.runtime.producer, result.message)
    except PublishError:
        # It is stored. Deliver it on this socket now; other sockets get it when they replay.
        await hub.deliver(result.message)


async def _error(websocket, code: str, retryable: bool, detail: str, **extra) -> None:
    try:
        await websocket.send_json({"type": "error", "code": code, "retryable": retryable, "detail": detail, **extra})
    except Exception:
        pass


async def _close(websocket, code: int, reason: str) -> None:
    try:
        await websocket.close(code=code, reason=reason)
    except RuntimeError:
        pass
