"""Agent orchestrator: classification and routing."""

from engagehub_common.auth import ROLE_ADMIN, ROLE_AGENT, Principal, require_roles
from engagehub_common.config import ServiceSettings
from engagehub_common.llm import get_llm_provider
from engagehub_common.service import Runtime, create_service_app
from engagehub_common.session.store import SessionStore
from engagehub_common.topics import AGENT_TYPES, CHAT_MESSAGES
from fastapi import APIRouter, Depends, FastAPI, Request

from app.orchestrator import CustomerTierClient, Orchestrator

router = APIRouter()


@router.get("/agents/availability")
async def availability(request: Request, _: Principal = Depends(require_roles(ROLE_AGENT, ROLE_ADMIN))) -> dict:
    settings = request.app.state.settings
    live = await request.app.state.store.live_workers(settings.worker_heartbeat_ttl_seconds)
    return {agent: {"live_workers": live.get(agent, 0)} for agent in AGENT_TYPES}


def create_app(runtime: Runtime | None = None, tiers=None) -> FastAPI:
    settings = ServiceSettings(service_name="agent-orchestrator")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        store = SessionStore(rt.redis, settings.session_ttl_seconds)
        app.state.store = store
        orchestrator = Orchestrator(
            settings, store, rt.producer, get_llm_provider(settings.llm_provider), tiers or CustomerTierClient(settings)
        )
        app.state.orchestrator = orchestrator
        rt.add_consumer([CHAT_MESSAGES], "agent-orchestrator", orchestrator.handle_event)

    app = create_service_app(
        "EngageHub Agent Orchestrator", settings, runtime=runtime, kafka=True, redis=True, setup=setup
    )
    app.include_router(router)
    return app
