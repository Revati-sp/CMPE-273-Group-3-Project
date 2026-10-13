from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health_endpoints():
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_websocket_assigns_increasing_sequence_numbers():
    with client.websocket_connect("/ws/tenant-a/conv-1") as ws:
        ws.send_text("hello")
        first = ws.receive_json()
        ws.send_text("where is my order?")
        second = ws.receive_json()

    assert first["conversation_id"] == "conv-1"
    assert first["tenant_id"] == "tenant-a"
    assert first["payload"] == {"text": "hello"}
    assert second["sequence_number"] == first["sequence_number"] + 1
    assert first["message_id"] != second["message_id"]
