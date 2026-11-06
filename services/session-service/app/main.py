"""Session service: owns the durable transcript and session recovery."""

import os

from engagehub_common.config import ServiceSettings
from engagehub_common.messaging.kafka import KafkaLogReader
from engagehub_common.service import Runtime, create_service_app
from engagehub_common.session.store import SessionStore
from engagehub_common.topics import AGENT_EVENTS, CHAT_MESSAGES, ESCALATIONS, SESSION_EVENTS
from fastapi import FastAPI

from app.api import router
from app.grpc_server import start_grpc_server
from app.history import HistoryService
from app.models import Base

GROUP = "session-materializer"


def create_app(runtime: Runtime | None = None, log_reader=None, grpc_address: str | None = None) -> FastAPI:
    settings = ServiceSettings(service_name="session-service")

    async def setup(app: FastAPI, rt: Runtime) -> None:
        await rt.db.create_schema(Base)
        store = SessionStore(rt.redis, settings.session_ttl_seconds)
        reader = log_reader or KafkaLogReader(settings.kafka_bootstrap_servers)
        history = HistoryService(rt.db, store, reader)
        app.state.store = store
        app.state.history = history
        # The transcript's unique constraints make this consumer idempotent, so it
        # does not need the Redis dedupe layer and keeps working during a Redis outage.
        rt.add_consumer(
            [CHAT_MESSAGES, SESSION_EVENTS, AGENT_EVENTS, ESCALATIONS], GROUP, history.handle_event, dedupe=False
        )
        address = grpc_address or os.environ.get("GRPC_ADDRESS", "0.0.0.0:50051")
        server, app.state.grpc_port = await start_grpc_server(history, settings.jwt_secret, address)

        async def stop_grpc() -> None:
            await server.stop(grace=5)

        rt.shutdown.append(stop_grpc)

    app = create_service_app(
        "EngageHub Session Service", settings, runtime=runtime, kafka=True, redis=True, db=True, setup=setup
    )
    app.include_router(router)
    return app
