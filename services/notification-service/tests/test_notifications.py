import time

import pytest
from app.main import GROUP, create_app
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.messaging.memory import InMemoryKafka
from engagehub_common.testing import auth_headers, memory_runtime
from engagehub_common.topics import DEAD_LETTER, NOTIFICATIONS
from fastapi.testclient import TestClient

DANA = auth_headers(subject="cust-acme-dana")


def notification(event_id="n1", recipient="cust-acme-dana", tenant="acme", **payload):
    return EventEnvelope(
        event_id=event_id,
        event_type=EventType.NOTIFICATION_REQUESTED,
        conversation_id="c1",
        tenant_id=tenant,
        producer_service="test",
        payload={"recipient_id": recipient, "kind": "k", "title": "T", "body": "B", **payload},
    )


def publish(kafka, *events):
    for event in events:
        kafka.append(NOTIFICATIONS, event.partition_key.encode(), event.to_bytes())


def wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


@pytest.fixture
def kafka():
    return InMemoryKafka()


@pytest.fixture
def client(kafka):
    with TestClient(create_app(runtime=memory_runtime("notification-service", kafka=kafka))) as test_client:
        yield test_client


def inbox(client, headers=DANA):
    return client.get("/notifications", headers=headers).json()


def test_retried_events_do_not_create_duplicate_notifications(client, kafka):
    publish(kafka, notification("n1"), notification("n1"), notification("n2"), notification("n1"))
    wait_for(lambda: len(inbox(client)) >= 2 and sum(1 for k in kafka.committed.values()) > 0)
    time.sleep(0.1)
    assert sorted(n["id"] for n in inbox(client)) == ["n1", "n2"]
    assert client.get("/notifications/unread-count", headers=DANA).json() == {"unread": 2}


def test_events_published_while_the_service_is_down_are_processed_on_restart(kafka):
    runtime = memory_runtime("notification-service", kafka=kafka)
    with TestClient(create_app(runtime=runtime)) as client:
        publish(kafka, notification("before"))
        wait_for(lambda: len(inbox(client)) == 1)
        wait_for(lambda: any(group == GROUP for group, _ in kafka.committed))

    publish(kafka, notification("during-outage-1"), notification("during-outage-2"))
    assert len(kafka.records(NOTIFICATIONS)) == 3  # retained by Kafka while nobody consumes

    with TestClient(create_app(runtime=runtime)) as client:
        wait_for(lambda: len(inbox(client)) == 3)
        assert {n["id"] for n in inbox(client)} == {"before", "during-outage-1", "during-outage-2"}


def test_redelivery_after_restart_with_lost_commits_is_idempotent(kafka):
    runtime = memory_runtime("notification-service", kafka=kafka)
    with TestClient(create_app(runtime=runtime)) as client:
        publish(kafka, notification("n1"))
        wait_for(lambda: len(inbox(client)) == 1)
    kafka.committed.clear()
    with TestClient(create_app(runtime=runtime)) as client:
        wait_for(lambda: any(group == GROUP for group, _ in kafka.committed))
        assert len(inbox(client)) == 1


def test_malformed_event_is_dead_lettered_and_does_not_block_others(client, kafka):
    broken = notification("bad")
    broken.payload.pop("recipient_id")
    publish(kafka, broken, notification("good"))
    wait_for(lambda: len(inbox(client)) == 1 and kafka.events(DEAD_LETTER))
    assert inbox(client)[0]["id"] == "good"


def test_recipients_and_tenants_are_isolated(client, kafka):
    publish(kafka, notification("mine"), notification("theirs", recipient="cust-acme-vic"))
    publish(kafka, notification("other-tenant", recipient="cust-acme-dana", tenant="globex"))
    wait_for(lambda: len(inbox(client)) == 1 and len(inbox(client, auth_headers(subject="cust-acme-vic"))) == 1)
    time.sleep(0.05)
    assert [n["id"] for n in inbox(client)] == ["mine"]
    assert client.post("/notifications/theirs/read", headers=DANA).status_code == 404


def test_mark_read(client, kafka):
    publish(kafka, notification("n1"))
    wait_for(lambda: len(inbox(client)) == 1)
    assert client.post("/notifications/n1/read", headers=DANA).json()["read"] is True
    assert client.get("/notifications/unread-count", headers=DANA).json() == {"unread": 0}
