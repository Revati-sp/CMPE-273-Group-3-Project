import pytest
from app.main import create_app
from engagehub_common.events import EventEnvelope, EventType, sender
from engagehub_common.testing import auth_headers, memory_runtime
from engagehub_common.topics import ANALYTICS, CHAT_MESSAGES
from fastapi.testclient import TestClient


def message(seq, conv="c1", tenant="acme", customer="cust-1"):
    return EventEnvelope(
        event_type=EventType.CUSTOMER_MESSAGE,
        conversation_id=conv,
        tenant_id=tenant,
        customer_id=customer,
        message_id=f"{conv}-{seq}",
        sequence_number=seq,
        producer_service="chat-service",
        payload={"text": f"t{seq}", "sender": sender("customer", customer, "D")},
    )


@pytest.fixture
def client():
    runtime = memory_runtime("session-service")
    app = create_app(runtime=runtime, log_reader=runtime.kafka.log_reader(), grpc_address="127.0.0.1:0")
    with TestClient(app) as test_client:
        test_client.kafka = runtime.kafka
        yield test_client


def seed(client, *events):
    import time

    for event in events:
        client.kafka.append(CHAT_MESSAGES, event.partition_key.encode(), event.to_bytes())
    deadline = time.time() + 3
    while time.time() < deadline:
        response = client.get(
            f"/conversations/{events[0].conversation_id}",
            headers=auth_headers(subject="a", role="admin", tenant=events[0].tenant_id),
        )
        if response.status_code == 200 and response.json()["message_count"] >= len(events):
            return
        time.sleep(0.02)
    raise AssertionError("events were not materialised")


def test_customer_sees_only_their_own_conversations(client):
    seed(client, message(1, "mine"))
    seed(client, message(1, "theirs", customer="cust-2"))
    mine = client.get("/conversations", headers=auth_headers(subject="cust-1")).json()
    assert [c["id"] for c in mine] == ["mine"]
    assert client.get("/conversations/theirs", headers=auth_headers(subject="cust-1")).status_code == 404
    assert client.get("/conversations/theirs/messages", headers=auth_headers(subject="cust-1")).status_code == 404


def test_other_tenant_staff_cannot_read_conversations(client):
    seed(client, message(1, "acme-conv"))
    globex_admin = auth_headers(subject="admin", role="admin", tenant="globex")
    assert client.get("/conversations", headers=globex_admin).json() == []
    assert client.get("/conversations/acme-conv", headers=globex_admin).status_code == 404
    assert client.get("/conversations/acme-conv/messages", headers=globex_admin).status_code == 404


def test_messages_endpoint_supports_after_seq(client):
    seed(client, message(1), message(2), message(3))
    response = client.get("/conversations/c1/messages?after_seq=1", headers=auth_headers())
    assert [m["sequence_number"] for m in response.json()] == [2, 3]


def test_rating_is_accepted_once_and_published(client):
    seed(client, message(1))
    headers = auth_headers()
    assert client.post("/conversations/c1/rating", json={"rating": 5}, headers=headers).status_code == 201
    assert client.post("/conversations/c1/rating", json={"rating": 1}, headers=headers).status_code == 409
    assert client.post("/conversations/c1/rating", json={"rating": 9}, headers=headers).status_code == 422
    assert [e.payload["rating"] for e in client.kafka.events(ANALYTICS)] == [5]


def test_failed_publish_rolls_back_the_rating(client):
    seed(client, message(1))
    client.kafka.fail_publishes = 1
    headers = auth_headers()
    assert client.post("/conversations/c1/rating", json={"rating": 5}, headers=headers).status_code == 503
    assert client.get("/conversations/c1", headers=headers).json()["rating"] is None
    assert client.post("/conversations/c1/rating", json={"rating": 5}, headers=headers).status_code == 201


def test_hot_session_endpoint_is_staff_only(client):
    assert client.get("/sessions/c1", headers=auth_headers()).status_code == 403
    assert client.get("/sessions/c1", headers=auth_headers(subject="a", role="agent")).status_code == 404
