"""Exercises the real ASGI app over the WebSocket protocol."""

import fakeredis.aioredis
import pytest
from app.main import create_app
from engagehub_common.session.store import SessionStore
from engagehub_common.testing import auth_headers, memory_runtime, token
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from tests.conftest import KafkaBackedHistory


@pytest.fixture
def client():
    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    assert isinstance(redis, fakeredis.aioredis.FakeRedis)
    runtime = memory_runtime("chat-service", redis=redis)
    app = create_app(runtime=runtime, history=KafkaBackedHistory(runtime.kafka, SessionStore(redis)), pod_name="pod-1")
    with TestClient(app) as test_client:
        yield test_client


def create_conversation(client, **kwargs):
    response = client.post("/conversations", json={"subject": "hi"}, headers=auth_headers(**kwargs))
    assert response.status_code == 201
    return response.json()["conversation_id"]


def hello(conversation_id, **kwargs):
    return {
        "type": "hello",
        "token": token(**kwargs),
        "conversation_id": conversation_id,
        "last_seen_seq": 0,
    }


def test_health_endpoints(client):
    assert client.get("/healthz").json()["status"] == "ok"
    ready = client.get("/readyz")
    assert ready.status_code == 200 and ready.json()["status"] == "ready"
    assert b"engagehub_ws_connections" in client.get("/metrics").content


def test_correlation_id_is_echoed(client):
    response = client.get("/healthz", headers={"X-Correlation-ID": "abc-123"})
    assert response.headers["x-correlation-id"] == "abc-123"


def test_create_conversation_requires_a_customer_token(client):
    assert client.post("/conversations", json={}).status_code == 401
    agent = auth_headers(subject="a1", role="agent")
    assert client.post("/conversations", json={}, headers=agent).status_code == 403


def test_full_socket_flow(client):
    conversation = create_conversation(client)
    with client.websocket_connect("/ws") as ws:
        ws.send_json(hello(conversation))
        welcome = ws.receive_json()
        assert welcome["type"] == "welcome" and welcome["pod"] == "pod-1"
        greeting = ws.receive_json()
        assert greeting["type"] == "message" and greeting["sequence_number"] == 1
        assert ws.receive_json()["type"] == "replay_complete"

        ws.send_json({"type": "message", "client_message_id": "client-msg-1", "text": "hello"})
        ack = ws.receive_json()
        assert ack["type"] == "message_ack"
        assert ack["message"]["sequence_number"] == 2 and ack["duplicate"] is False

        ws.send_json({"type": "message", "client_message_id": "client-msg-1", "text": "hello"})
        retry = ws.receive_json()
        assert retry["duplicate"] is True
        assert retry["message"]["message_id"] == ack["message"]["message_id"]

        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


@pytest.mark.parametrize(
    "frame",
    [
        {"type": "message", "text": "not hello"},
        {"type": "hello", "token": "garbage", "conversation_id": "c1"},
    ],
)
def test_unauthenticated_socket_is_closed(client, frame):
    with client.websocket_connect("/ws") as ws:
        ws.send_json(frame)
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
    assert closed.value.code == 4401


def test_other_tenant_gets_not_found(client):
    conversation = create_conversation(client)
    with client.websocket_connect("/ws") as ws:
        ws.send_json(hello(conversation, subject="other", tenant="globex"))
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
    assert closed.value.code == 4404
