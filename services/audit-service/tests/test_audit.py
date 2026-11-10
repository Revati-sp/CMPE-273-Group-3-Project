import time

import pytest
from app.main import create_app
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.testing import auth_headers, memory_runtime
from engagehub_common.topics import AUDIT_EVENTS, ESCALATIONS
from fastapi.testclient import TestClient

ACME_ADMIN = auth_headers(subject="admin-acme", role="admin")
GLOBEX_ADMIN = auth_headers(subject="admin-globex", role="admin", tenant="globex")


def audit_event(event_id="a1", tenant="acme", action="user.login"):
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.AUDIT_RECORDED,
        tenant_id=tenant,
        producer_service="auth-service",
        payload={"action": action, "actor_id": "u1", "actor_role": "customer", "resource": "u1", "details": {"a": 1}},
    )


@pytest.fixture
def client():
    runtime = memory_runtime("audit-service")
    with TestClient(create_app(runtime=runtime)) as test_client:
        test_client.kafka = runtime.kafka
        yield test_client


def publish(client, topic, *events):
    for e in events:
        client.kafka.append(topic, e.partition_key.encode(), e.to_bytes())


def records(client, headers=ACME_ADMIN, query=""):
    return client.get(f"/audit{query}", headers=headers).json()


def wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


def test_redelivered_audit_events_are_recorded_once(client):
    publish(client, AUDIT_EVENTS, audit_event(), audit_event(), audit_event("a2", action="note.added"))
    wait_for(lambda: len(records(client)) == 2)
    time.sleep(0.05)
    assert len(records(client)) == 2
    assert records(client, query="?action=note.added")[0]["id"] == "a2"


def test_handoff_is_audited_without_the_customer_text_summary(client):
    event = EventEnvelope(
        event_id="h1",
        event_type=EventType.HANDOFF_REQUESTED,
        conversation_id="c1",
        tenant_id="acme",
        producer_service="virtual-agent-service",
        payload={"summary": "customer said something private", "reason": "customer_request"},
    )
    publish(client, ESCALATIONS, event)
    wait_for(lambda: len(records(client)) == 1)
    record = records(client)[0]
    assert record["action"] == "handoff_requested" and "summary" not in record["details"]


def test_audit_log_is_isolated_and_admin_only(client):
    publish(client, AUDIT_EVENTS, audit_event("acme-1"), audit_event("globex-1", tenant="globex"))
    wait_for(lambda: len(records(client)) == 1 and len(records(client, GLOBEX_ADMIN)) == 1)
    assert records(client)[0]["id"] == "acme-1"
    assert client.get("/audit", headers=auth_headers(subject="a", role="agent")).status_code == 403
