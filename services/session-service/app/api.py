"""REST API: conversation history, ratings and hot session inspection."""

from engagehub_common.auth import (
    ROLE_ADMIN,
    ROLE_AGENT,
    ROLE_SERVICE,
    Principal,
    get_principal,
    require_roles,
)
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.service import Runtime, get_runtime
from engagehub_common.topics import ANALYTICS
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.models import Conversation

router = APIRouter()


class RatingRequest(BaseModel):
    rating: int = Field(ge=1, le=5)
    comment: str = Field(default="", max_length=2000)


def _to_dict(c: Conversation, principal: Principal) -> dict:
    return {
        "id": c.id,
        "tenant_id": c.tenant_id,
        "customer_id": c.customer_id,
        "status": c.status,
        "subject": c.subject,
        "language": c.language,
        "workflow_state": c.workflow_state,
        "assigned_agent_type": c.assigned_agent_type,
        "assigned_agent_id": c.assigned_agent_id,
        "assigned_agent_name": c.assigned_agent_name,
        # The AI handoff summary is an internal staff note.
        "summary": c.summary if principal.is_staff else "",
        "rating": c.rating,
        "message_count": c.message_count,
        "started_at": c.started_at.isoformat(),
        "updated_at": c.updated_at.isoformat(),
    }


async def _load(session, conversation_id: str, principal: Principal) -> Conversation:
    conversation = await session.get(Conversation, conversation_id)
    # Same 404 for "missing" and "someone else's", so IDs cannot be probed.
    if (
        conversation is None
        or conversation.tenant_id != principal.tenant_id
        or (not principal.is_staff and conversation.customer_id != principal.subject)
    ):
        raise HTTPException(404, "conversation not found")
    return conversation


@router.get("/conversations")
async def list_conversations(
    status: str | None = None,
    limit: int = Query(default=50, le=200),
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> list[dict]:
    query = select(Conversation).where(Conversation.tenant_id == principal.tenant_id)
    if not principal.is_staff:
        query = query.where(Conversation.customer_id == principal.subject)
    if status:
        query = query.where(Conversation.status == status)
    async with rt.db.session() as session:
        rows = await session.scalars(query.order_by(Conversation.updated_at.desc()).limit(limit))
        return [_to_dict(c, principal) for c in rows]


@router.get("/conversations/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> dict:
    async with rt.db.session() as session:
        return _to_dict(await _load(session, conversation_id, principal), principal)


@router.get("/conversations/{conversation_id}/messages")
async def get_messages(
    conversation_id: str,
    request: Request,
    after_seq: int = Query(default=0, ge=0),
    limit: int = Query(default=1000, le=5000),
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> list[dict]:
    async with rt.db.session() as session:
        await _load(session, conversation_id, principal)
    events = await request.app.state.history.messages_after(principal.tenant_id, conversation_id, after_seq, limit)
    return [e.model_dump(mode="json") for e in events]


@router.post("/conversations/{conversation_id}/rating", status_code=201)
async def rate_conversation(
    conversation_id: str,
    body: RatingRequest,
    request: Request,
    principal: Principal = Depends(get_principal),
    rt: Runtime = Depends(get_runtime),
) -> dict:
    async with rt.db.session() as session:
        conversation = await _load(session, conversation_id, principal)
        if principal.is_staff:
            raise HTTPException(403, "only the customer can rate a conversation")
        if conversation.rating is not None:
            raise HTTPException(409, "conversation already rated")
        conversation.rating = body.rating
        conversation.rating_comment = body.comment
        # Published inside the transaction: if the broker refuses, the rating rolls back.
        await rt.producer.publish(
            ANALYTICS,
            EventEnvelope(
                event_type=EventType.RATING_SUBMITTED,
                conversation_id=conversation_id,
                tenant_id=principal.tenant_id,
                customer_id=principal.subject,
                correlation_id=request.state.correlation_id,
                producer_service=rt.settings.service_name,
                payload={"rating": body.rating, "agent_id": conversation.assigned_agent_id},
            ),
        )
    return {"conversation_id": conversation_id, "rating": body.rating}


@router.get("/sessions/{conversation_id}")
async def get_hot_session(
    conversation_id: str,
    request: Request,
    principal: Principal = Depends(require_roles(ROLE_AGENT, ROLE_ADMIN, ROLE_SERVICE)),
) -> dict:
    store = request.app.state.store
    state = await store.get(principal.tenant_id, conversation_id)
    if state is None:
        raise HTTPException(404, "no active session")
    return {
        **state.model_dump(),
        "unacknowledged_messages": await store.unacked_count(principal.tenant_id, conversation_id),
    }
