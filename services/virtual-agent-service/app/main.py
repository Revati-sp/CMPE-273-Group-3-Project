"""Virtual agent service: one image, one specialised agent per deployment (AGENT_TYPE)."""

import asyncio

from engagehub_common.config import ServiceSettings
from engagehub_common.llm import get_llm_provider
from engagehub_common.service import Runtime, create_service_app
from engagehub_common.session.grpc_client import GrpcHistoryClient
from engagehub_common.session.store import SessionStore
from engagehub_common.topics import ESCALATIONS, agent_task_topic
from fastapi import FastAPI

from app.agents import AGENT_CLASSES
from app.clients import DomainClient
from app.worker import AgentWorker, heartbeat_loop


def create_app(runtime: Runtime | None = None, history=None, domain=None) -> FastAPI:
    settings = ServiceSettings(service_name="virtual-agent-service")
    # Normally one agent type per deployment. A comma-separated list hosts several in one
    # process to save memory (docker-compose.lite.yml); each keeps its own consumer group.
    agent_types = [name.strip() for name in settings.agent_type.split(",") if name.strip()]
    unknown = [name for name in agent_types if name not in AGENT_CLASSES]
    if not agent_types or unknown:
        raise ValueError(f"AGENT_TYPE must be a list drawn from {sorted(AGENT_CLASSES)}")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        store = SessionStore(rt.redis, settings.session_ttl_seconds)
        client = history or GrpcHistoryClient(settings.session_grpc_target, settings.jwt_secret, settings.service_name)
        shared_domain = domain or DomainClient(settings)
        llm = get_llm_provider(settings.llm_provider)
        app.state.workers = {}
        for agent_type in agent_types:
            worker = AgentWorker(AGENT_CLASSES[agent_type](shared_domain), rt, store, rt.producer, client, llm)
            app.state.workers[agent_type] = worker
            rt.add_consumer([agent_task_topic(agent_type)], f"va-{agent_type}", worker.handle_task)
            if agent_type == "supervisor":
                rt.add_consumer([ESCALATIONS], "va-supervisor-escalations", worker.handle_escalation)
            rt.background.append(
                asyncio.create_task(
                    heartbeat_loop(store, agent_type, settings.pod_name, settings.worker_heartbeat_ttl_seconds / 3)
                )
            )

        async def deregister() -> None:
            for agent_type in agent_types:
                await store.worker_stopped(agent_type, settings.pod_name)

        rt.shutdown.append(deregister)

    return create_service_app(
        f"EngageHub Virtual Agent ({settings.agent_type})",
        settings,
        runtime=runtime,
        kafka=True,
        redis=True,
        setup=setup,
    )
