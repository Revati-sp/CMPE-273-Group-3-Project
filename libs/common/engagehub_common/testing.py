"""Helpers for service test suites: an in-memory Runtime and token minting."""

from __future__ import annotations

import fakeredis.aioredis

from engagehub_common.auth import issue_token
from engagehub_common.config import ServiceSettings
from engagehub_common.db import Database
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.service import Runtime

TEST_SECRET = "test-secret-test-secret-test-secret-123"


def memory_runtime(
    service_name: str = "test", *, kafka: InMemoryKafka | None = None, redis=None, db: bool = True
) -> Runtime:
    kafka = kafka or InMemoryKafka()
    settings = ServiceSettings(service_name=service_name, jwt_secret=TEST_SECRET, otel_enabled=False)
    runtime = Runtime(
        settings,
        producer=kafka.producer(),
        redis=redis if redis is not None else fakeredis.aioredis.FakeRedis(decode_responses=True),
        db=Database("sqlite+aiosqlite:///:memory:") if db else None,
        consumer_factory=lambda topics, group, reset: kafka.consumer(topics, group, reset),
    )
    runtime.kafka = kafka  # type: ignore[attr-defined]
    return runtime


def token(subject="cust-1", tenant="acme", role="customer", name="Test", **kwargs) -> str:
    if role == "customer":
        kwargs.setdefault("customer_id", subject)
    return issue_token(TEST_SECRET, subject=subject, tenant_id=tenant, role=role, name=name, **kwargs)


def auth_headers(**kwargs) -> dict[str, str]:
    return {"Authorization": f"Bearer {token(**kwargs)}"}
