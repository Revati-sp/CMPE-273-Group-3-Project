"""Session migration features through the real ASGI app (typing, resync, session view, drain)."""

import pytest
from engagehub_common.testing import auth_headers
from engagehub_common.topics import SESSION_STATE, SESSION_STATE_DLQ
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from tests.test_reply_stream import CONV, DANA, Stack, ack, connect, create, hello, recv, wait_for


@pytest.fixture
def stack():
    return Stack()


@pytest.fixture
def client(stack):
    with TestClient(stack.app()) as test_client:
        yield test_client


def test_connection_ack_carries_session_version_and_view(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        first, _ = connect(ws)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        second, _ = connect(ws)
    assert first["session_version"] >= 1 and second["session_version"] > first["session_version"]
    assert second["reconnected"] and not second["handover"]  # same pod, after a clean close
    for field in ("workflow_state", "agent_kind", "assigned_agent_id", "typing_status", "connection_id"):
        assert field in second


def test_typing_and_ack_are_reflected_in_the_session_snapshot(stack, client):
    create(client)
    headers = auth_headers(**DANA)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        message_id = stack.publish("hello")
        assert recv(ws)["sequence_number"] == 1
        ws.send_json({"type": "typing", "is_typing": True})
        confirmed = ack(ws, 1, message_id)
        assert confirmed["cursor"]["session_version"] >= 1
        session = client.get(f"/conversations/{CONV}/session", headers=headers).json()
    assert session["source"] == "redis" and session["key"] == f"chat:session:{CONV}:cust-acme-dana"
    assert session["typing_status"] == "customer_typing" and session["last_acknowledged_sequence"] == 1
    assert session["current_chat_pod"] == "pod-a" and session["agent_kind"] == "virtual_agent"
    assert (
        client.get(f"/conversations/{CONV}/session", headers=auth_headers(subject="cust-acme-vic")).status_code == 404
    )


def test_session_state_is_published_to_the_compacted_topic(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
    events = stack.kafka.records(SESSION_STATE)
    assert events and all(r.key == CONV.encode() for r in events)


def test_client_resync_resends_what_was_sent_after_the_gap(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
        for n in range(1, 4):
            stack.publish(f"r{n}")
        [recv(ws) for _ in range(3)]
        ws.send_json({"type": "resync", "after_sequence": 1})
        frames = [recv(ws), recv(ws), recv(ws)]
    assert [f.get("sequence_number") for f in frames[:2]] == [2, 3] and all(f["replayed"] for f in frames[:2])
    assert frames[2] == {"type": "resync_completed", "after_sequence": 1, "last_sequence": 3}


def test_drain_fails_readiness_and_sends_new_sockets_elsewhere(stack, client):
    create(client)
    assert client.get("/ready").status_code == 200
    assert client.post("/internal/drain").json() == {"draining": True}
    assert client.get("/ready").status_code == 503
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        hello(ws)
        with pytest.raises(WebSocketDisconnect) as closed:
            recv(ws)
    assert closed.value.code == 1012


def test_invalid_session_state_records_are_dead_lettered_by_the_running_service(stack, client):
    stack.kafka.append(SESSION_STATE, CONV.encode(), b'{"event_type": "SessionStateUpdated", "bogus": 1}')
    wait_for(lambda: stack.kafka.events(SESSION_STATE_DLQ))
    letter = stack.kafka.events(SESSION_STATE_DLQ)[0]
    assert letter.payload["source_topic"] == SESSION_STATE and "schema_validation" in letter.payload["reason"]


def test_session_endpoint_falls_back_to_kafka_when_redis_is_down(stack, client):
    create(client)
    with client.websocket_connect(f"/ws/customer/{CONV}") as ws:
        connect(ws)
    table = client.app.state.reply_hub.state_table
    wait_for(lambda: table.get(CONV) is not None)
    stack.redis.down = True
    body = client.get(f"/conversations/{CONV}/session", headers=auth_headers(**DANA)).json()
    assert body["source"] == "kafka_session_state" and body["conversation_id"] == CONV
