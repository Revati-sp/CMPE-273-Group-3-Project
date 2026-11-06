import time

import pytest
from app.main import create_app, metrics_for
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.testing import auth_headers, memory_runtime
from engagehub_common.topics import ANALYTICS
from fastapi.testclient import TestClient

ACME_ADMIN = auth_headers(subject="admin-acme", role="admin")
GLOBEX_ADMIN = auth_headers(subject="admin-globex", role="admin", tenant="globex")


def event(event_type, event_id, tenant="acme", **payload):
    return EventEnvelope(
        event_id=event_id,
        event_type=event_type,
        conversation_id="c1",
        tenant_id=tenant,
        producer_service="test",
        payload=payload,
    )


@pytest.fixture
def client():
    runtime = memory_runtime("analytics-service")
    with TestClient(create_app(runtime=runtime)) as test_client:
        test_client.kafka = runtime.kafka
        yield test_client


def publish(client, *events):
    for e in events:
        client.kafka.append(ANALYTICS, e.partition_key.encode(), e.to_bytes())


def overview(client, headers=ACME_ADMIN):
    return client.get("/analytics/overview", headers=headers).json()


def wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


def test_metrics_are_aggregated(client):
    publish(
        client,
        event(EventType.CONVERSATION_STARTED, "e1"),
        event(EventType.CONVERSATION_STARTED, "e2"),
        event(EventType.HANDOFF_REQUESTED, "e3", reason="sensitive_topic:legal"),
        event(EventType.RATING_SUBMITTED, "e4", rating=5),
        event(EventType.RATING_SUBMITTED, "e5", rating=2),
        event(EventType.AGENT_ROUTED, "e6", intent="order", agent="order_tracking", escalated=False),
        event(EventType.AGENT_ASSIGNED, "e7", agent_name="Alice", wait_seconds=30),
        event(EventType.SESSION_MIGRATED, "e8"),
        event(EventType.SESSION_REPLAYED, "e9", count=3, source="redis"),
    )
    wait_for(lambda: overview(client)["messages_replayed"] == 3)
    data = overview(client)
    assert data["conversations_started"] == 2 and data["escalation_rate"] == 0.5
    assert data["escalations_by_reason"] == {"sensitive_topic": 1}
    assert data["average_rating"] == 3.5 and data["ratings_count"] == 2
    assert data["routed_to_agent"] == {"order_tracking": 1} and data["intents"] == {"order": 1}
    assert data["average_human_wait_seconds"] == 30 and data["session_migrations"] == 1


def test_redelivered_events_are_counted_once(client):
    publish(client, *[event(EventType.CONVERSATION_STARTED, "same")] * 3, event(EventType.SESSION_MIGRATED, "last"))
    wait_for(lambda: overview(client)["session_migrations"] == 1)
    assert overview(client)["conversations_started"] == 1


def test_analytics_are_isolated_between_tenants(client):
    publish(
        client,
        event(EventType.CONVERSATION_STARTED, "a1"),
        event(EventType.CONVERSATION_STARTED, "g1", tenant="globex"),
    )
    publish(client, event(EventType.CONVERSATION_STARTED, "g2", tenant="globex"))
    wait_for(lambda: overview(client, GLOBEX_ADMIN)["conversations_started"] == 2)
    assert overview(client)["conversations_started"] == 1
    assert overview(client, GLOBEX_ADMIN)["tenant_id"] == "globex"


def test_only_admins_can_read_analytics(client):
    assert client.get("/analytics/overview", headers=auth_headers(subject="u")).status_code == 403
    assert client.get("/analytics/overview", headers=auth_headers(subject="a", role="agent")).status_code == 403


def test_uninteresting_events_produce_no_metrics():
    assert metrics_for(event(EventType.TYPING, "t")) == []
