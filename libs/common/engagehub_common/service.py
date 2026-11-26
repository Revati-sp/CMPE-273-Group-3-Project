"""Common FastAPI scaffolding: health, metrics, correlation IDs, tracing, lifecycle."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from engagehub_common.config import ServiceSettings
from engagehub_common.db import Database, ensure_database
from engagehub_common.events import EventEnvelope, EventType, new_id
from engagehub_common.logging_setup import bind_event_context, configure_logging, correlation_id_var
from engagehub_common.messaging.bus import EventProducer, PublishError
from engagehub_common.messaging.consumer import ConsumerRunner, Handler, RedisDedupe
from engagehub_common.messaging.kafka import KafkaEventConsumer, KafkaEventProducer
from engagehub_common.redis_factory import build_redis
from engagehub_common.resilience import retry_async
from engagehub_common.telemetry import configure_telemetry
from engagehub_common.topics import AUDIT_EVENTS, NOTIFICATIONS

log = logging.getLogger(__name__)

HTTP_REQUESTS = Counter("engagehub_http_requests_total", "HTTP requests", ["service", "method", "route", "status"])
HTTP_LATENCY = Histogram("engagehub_http_request_seconds", "HTTP latency", ["service", "route"])


@dataclass
class Runtime:
    """External dependencies for a service. Tests inject in-memory versions."""

    settings: ServiceSettings
    producer: EventProducer | None = None
    redis: object | None = None
    db: Database | None = None
    consumer_factory: Callable | None = None
    runners: list[ConsumerRunner] = field(default_factory=list)
    background: list[asyncio.Task] = field(default_factory=list)
    readiness: list[Callable[[], Awaitable[None]]] = field(default_factory=list)
    shutdown: list[Callable[[], Awaitable[None]]] = field(default_factory=list)
    owns_connections: bool = True

    def add_consumer(
        self,
        topics: list[str],
        group: str | None,
        handler: Handler,
        *,
        dedupe: bool = True,
        offset_reset: str = "earliest",
        commit: bool = True,
        **runner_options,
    ) -> ConsumerRunner:
        consumer = self.consumer_factory(topics, group, offset_reset)
        runner = ConsumerRunner(
            consumer,
            handler,
            group=group or f"{self.settings.service_name}-broadcast",
            producer=self.producer,
            dedupe=RedisDedupe(self.redis, group) if dedupe and self.redis is not None and group else None,
            service_name=self.settings.service_name,
            max_attempts=self.settings.consumer_max_attempts,
            retry_base_seconds=self.settings.consumer_retry_base_seconds,
            commit=commit and group is not None,
            **runner_options,
        )
        self.runners.append(runner)
        return runner

    async def publish_audit(self, principal, action: str, *, resource: str = "", **details) -> None:
        await self.producer.publish(
            AUDIT_EVENTS,
            EventEnvelope(
                event_type=EventType.AUDIT_RECORDED,
                conversation_id=details.pop("conversation_id", None),
                tenant_id=principal.tenant_id,
                customer_id=principal.customer_id,
                correlation_id=correlation_id_var.get() or new_id(),
                producer_service=self.settings.service_name,
                payload={
                    "action": action,
                    "actor_id": principal.subject,
                    "actor_role": principal.role,
                    "resource": resource,
                    "details": details,
                },
            ),
        )

    async def publish_notification(
        self, *, tenant_id, recipient_id, kind, title, body, conversation_id=None, customer_id=None,
        event_id=None, correlation_id=None,
    ) -> None:  # fmt: skip
        await self.producer.publish(
            NOTIFICATIONS,
            EventEnvelope(
                event_id=event_id or new_id(),
                event_type=EventType.NOTIFICATION_REQUESTED,
                conversation_id=conversation_id,
                tenant_id=tenant_id,
                customer_id=customer_id,
                correlation_id=correlation_id or correlation_id_var.get() or new_id(),
                producer_service=self.settings.service_name,
                payload={"recipient_id": recipient_id, "kind": kind, "title": title, "body": body},
            ),
        )


async def connect_runtime(settings: ServiceSettings, *, kafka: bool, redis: bool, db: bool) -> Runtime:
    """Connect to real infrastructure, tolerating dependencies that are still starting."""
    runtime = Runtime(settings)
    client_id = f"{settings.service_name}-{settings.pod_name}"

    if kafka:

        async def start_producer():
            producer = KafkaEventProducer(settings.kafka_bootstrap_servers, client_id)
            await producer.start()
            return producer

        runtime.producer = await retry_async(start_producer, attempts=60)
        runtime.consumer_factory = lambda topics, group, reset: KafkaEventConsumer(
            settings.kafka_bootstrap_servers, topics, group, client_id, offset_reset=reset
        )
    if redis:

        async def start_redis():
            client = build_redis(settings)
            await client.initialize() if hasattr(client, "initialize") else None
            await client.ping()
            return client

        runtime.redis = await retry_async(start_redis, attempts=60)
    if db:
        await retry_async(lambda: ensure_database(settings.database_url), attempts=60)
        runtime.db = Database(settings.database_url)
        await retry_async(runtime.db.ping, attempts=60)
    return runtime


async def _close_runtime(runtime: Runtime) -> None:
    if runtime.producer:
        await runtime.producer.stop()
    if runtime.redis is not None:
        await runtime.redis.aclose()
    if runtime.db:
        await runtime.db.dispose()


def create_service_app(
    title: str,
    settings: ServiceSettings,
    *,
    runtime: Runtime | None = None,
    kafka: bool = False,
    redis: bool = False,
    db: bool = False,
    setup: Callable[[FastAPI, Runtime], Awaitable[None]] | None = None,
) -> FastAPI:
    configure_logging(settings.service_name, settings.pod_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        rt = runtime or await connect_runtime(settings, kafka=kafka, redis=redis, db=db)
        if runtime is not None:
            rt.owns_connections = False
        app.state.runtime = rt
        if setup:
            await setup(app, rt)
        for runner in rt.runners:
            runner.start()
        log.info("service started")
        try:
            yield
        finally:
            log.info("service stopping")
            for task in rt.background:
                task.cancel()
            await asyncio.gather(*rt.background, return_exceptions=True)
            for runner in rt.runners:
                await runner.stop()
            for hook in rt.shutdown:
                await hook()
            # Leave an injected Runtime reusable, so tests can simulate a service restart.
            rt.runners.clear()
            rt.background.clear()
            rt.shutdown.clear()
            rt.readiness.clear()
            if rt.owns_connections:
                await _close_runtime(rt)

    app = FastAPI(title=title, version="1.0.0", lifespan=lifespan)
    app.state.settings = settings
    configure_telemetry(app, settings.service_name, settings.pod_name, settings.otel_endpoint, settings.otel_enabled)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        correlation_id = request.headers.get("x-correlation-id") or new_id()
        bind_event_context(correlation_id=correlation_id)
        request.state.correlation_id = correlation_id
        started = time.perf_counter()
        response = await call_next(request)
        route = getattr(request.scope.get("route"), "path", "unmatched")
        HTTP_REQUESTS.labels(settings.service_name, request.method, route, response.status_code).inc()
        HTTP_LATENCY.labels(settings.service_name, route).observe(time.perf_counter() - started)
        response.headers["x-correlation-id"] = correlation_id
        return response

    @app.exception_handler(PublishError)
    async def broker_unavailable(_: Request, exc: PublishError):
        log.warning("event broker unavailable", extra={"error": str(exc)})
        return JSONResponse({"detail": "event broker unavailable, retry shortly"}, status_code=503)

    @app.get("/health", tags=["health"], include_in_schema=False)
    @app.get("/healthz", tags=["health"])
    async def liveness() -> dict:
        return {"status": "ok", "service": settings.service_name, "pod": settings.pod_name}

    @app.get("/ready", tags=["health"], include_in_schema=False)
    @app.get("/readyz", tags=["health"])
    async def readiness(response: Response) -> dict:
        rt: Runtime = app.state.runtime
        checks: dict[str, str] = {}

        async def probe(name: str, func) -> None:
            try:
                await asyncio.wait_for(func(), timeout=2)
                checks[name] = "ok"
            except Exception as exc:
                checks[name] = f"failed: {type(exc).__name__}"

        if rt.redis is not None:
            await probe("redis", rt.redis.ping)
        if rt.db is not None:
            await probe("database", rt.db.ping)
        for index, extra in enumerate(rt.readiness):
            await probe(f"check_{index}", extra)
        for runner in rt.runners:
            checks[f"consumer:{runner.group}"] = "ok" if runner.healthy else "failed"
        ready = all(value == "ok" for value in checks.values())
        response.status_code = 200 if ready else 503
        return {"status": "ready" if ready else "not_ready", "checks": checks}

    @app.get("/metrics", tags=["health"], include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


def get_runtime(request: Request) -> Runtime:
    return request.app.state.runtime
