"""Human agent service: agent status, the queue and the workspace API."""

from typing import Literal

import httpx
from engagehub_common.auth import ROLE_ADMIN, ROLE_AGENT, Principal, require_roles
from engagehub_common.config import ServiceSettings
from engagehub_common.service import Runtime, create_service_app
from engagehub_common.session.grpc_client import GrpcHistoryClient
from engagehub_common.session.log import ConversationLog
from engagehub_common.session.store import SessionStore
from engagehub_common.topics import ESCALATIONS
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.models import Base, HumanAgent, InternalNote
from app.service import QueueService

router = APIRouter()
agent_only = require_roles(ROLE_AGENT)
staff = require_roles(ROLE_AGENT, ROLE_ADMIN)

DEMO_AGENTS = (
    ("agent-acme-alice", "acme", "Alice Morgan", ["en"], ["products", "orders", "returns"]),
    ("agent-acme-bob", "acme", "Bob Reyes", ["en", "es"], ["billing", "technical", "complaints"]),
    ("agent-globex-carol", "globex", "Carol Chen", ["en"], ["products", "technical", "orders"]),
    ("agent-globex-dave", "globex", "Dave Okafor", ["en", "es"], ["billing", "returns", "complaints"]),
)


class StatusRequest(BaseModel):
    status: Literal["available", "busy", "offline"]


class NoteRequest(BaseModel):
    text: str = Field(min_length=1, max_length=5000)


class TransferRequest(BaseModel):
    target_agent_id: str | None = None  # None hands the conversation back to the virtual agents


class EscalateRequest(BaseModel):
    reason: str = Field(default="", max_length=500)


def queue(request: Request) -> QueueService:
    return request.app.state.queue


@router.get("/agents")
async def list_agents(principal: Principal = Depends(staff), q: QueueService = Depends(queue)) -> list[dict]:
    return await q.list_agents(principal.tenant_id)


@router.get("/agents/me")
async def me(principal: Principal = Depends(agent_only), q: QueueService = Depends(queue)) -> dict:
    agent = await q.get_or_create_agent(principal)
    return {
        "id": agent.id,
        "name": agent.name,
        "status": agent.status,
        "languages": agent.languages,
        "skills": agent.skills,
    }


@router.put("/agents/me/status")
async def set_status(
    body: StatusRequest, principal: Principal = Depends(agent_only), q: QueueService = Depends(queue)
) -> dict:
    agent = await q.set_status(principal, body.status)
    return {"id": agent.id, "status": agent.status}


@router.get("/queue")
async def view_queue(principal: Principal = Depends(staff), q: QueueService = Depends(queue)) -> dict:
    return await q.view_queue(principal)


@router.post("/queue/{conversation_id}/accept")
async def accept(
    conversation_id: str, request: Request, principal: Principal = Depends(agent_only), q: QueueService = Depends(queue)
) -> dict:
    return await q.accept(principal, conversation_id, request.state.correlation_id)


@router.post("/queue/{conversation_id}/reject")
async def reject(
    conversation_id: str, principal: Principal = Depends(agent_only), q: QueueService = Depends(queue)
) -> dict:
    return await q.reject(principal, conversation_id)


@router.post("/conversations/{conversation_id}/transfer")
async def transfer(
    conversation_id: str,
    body: TransferRequest,
    request: Request,
    principal: Principal = Depends(agent_only),
    q: QueueService = Depends(queue),
) -> dict:
    return await q.transfer(principal, conversation_id, body.target_agent_id, request.state.correlation_id)


@router.post("/conversations/{conversation_id}/resolve")
async def resolve(
    conversation_id: str, request: Request, principal: Principal = Depends(agent_only), q: QueueService = Depends(queue)
) -> dict:
    return await q.resolve(principal, conversation_id, request.state.correlation_id)


@router.post("/conversations/{conversation_id}/escalate")
async def escalate(
    conversation_id: str,
    body: EscalateRequest,
    request: Request,
    principal: Principal = Depends(agent_only),
    q: QueueService = Depends(queue),
) -> dict:
    return await q.escalate(principal, conversation_id, body.reason, request.state.correlation_id)


@router.get("/conversations/{conversation_id}/notes")
async def list_notes(conversation_id: str, request: Request, principal: Principal = Depends(staff)) -> list[dict]:
    rt: Runtime = request.app.state.runtime
    async with rt.db.session() as session:
        rows = await session.scalars(
            select(InternalNote)
            .where(InternalNote.tenant_id == principal.tenant_id, InternalNote.conversation_id == conversation_id)
            .order_by(InternalNote.created_at)
        )
        return [
            {"id": n.id, "author": n.author_name, "text": n.text, "created_at": n.created_at.isoformat()} for n in rows
        ]


@router.post("/conversations/{conversation_id}/notes", status_code=201)
async def add_note(
    conversation_id: str, body: NoteRequest, request: Request, principal: Principal = Depends(staff)
) -> dict:
    rt: Runtime = request.app.state.runtime
    async with rt.db.session() as session:
        note = InternalNote(
            tenant_id=principal.tenant_id,
            conversation_id=conversation_id,
            author_id=principal.subject,
            author_name=principal.name or principal.subject,
            text=body.text,
        )
        session.add(note)
    await rt.publish_audit(principal, "note.added", resource=conversation_id, conversation_id=conversation_id)
    return {"id": note.id, "author": note.author_name, "text": note.text, "created_at": note.created_at.isoformat()}


@router.get("/conversations/{conversation_id}/context")
async def conversation_context(conversation_id: str, request: Request, principal: Principal = Depends(staff)) -> dict:
    """One view for the agent: history, AI summary, customer profile and orders."""
    settings = request.app.state.settings
    headers = {"Authorization": request.headers["authorization"], "X-Correlation-ID": request.state.correlation_id}
    http: httpx.AsyncClient = request.app.state.http

    async def fetch(url: str, params: dict | None = None):
        try:
            response = await http.get(url, headers=headers, params=params)
        except httpx.HTTPError:
            return None  # Show what we can: one failed panel should not blank the whole view.
        return response.json() if response.status_code == 200 else None

    conversation = await fetch(f"{settings.session_service_url}/conversations/{conversation_id}")
    if conversation is None:
        raise HTTPException(404, "conversation not found")
    customer_id = conversation["customer_id"]
    return {
        "conversation": conversation,
        "summary": conversation.get("summary", ""),
        "messages": await fetch(f"{settings.session_service_url}/conversations/{conversation_id}/messages") or [],
        "customer": await fetch(f"{settings.customer_service_url}/customers/{customer_id}"),
        "orders": await fetch(f"{settings.order_service_url}/orders", {"customer_id": customer_id}) or [],
    }


async def seed_agents(rt: Runtime) -> None:
    async with rt.db.session() as session:
        for agent_id, tenant, name, languages, skills in DEMO_AGENTS:
            await rt.db.insert_ignore(
                session,
                HumanAgent,
                {
                    "id": agent_id,
                    "tenant_id": tenant,
                    "name": name,
                    "status": "offline",
                    "languages": languages,
                    "skills": skills,
                    "max_concurrent": 3,
                },
            )


def create_app(runtime: Runtime | None = None, history=None, http: httpx.AsyncClient | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="human-agent-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        if settings.seed_demo_data:
            await seed_agents(rt)
        store = SessionStore(rt.redis, settings.session_ttl_seconds)
        client = history or GrpcHistoryClient(settings.session_grpc_target, settings.jwt_secret, settings.service_name)
        conversation_log = ConversationLog(store, rt.producer, settings.service_name, restore=client.restore_session)
        app.state.queue = QueueService(rt, store, conversation_log)
        app.state.http = http or httpx.AsyncClient(timeout=4.0)
        rt.add_consumer([ESCALATIONS], "human-agent-queue", app.state.queue.handle_event)

    app = create_service_app(
        "EngageHub Human Agent Service", settings, runtime=runtime, kafka=True, redis=True, db=True, setup=setup
    )
    app.include_router(router)
    return app
