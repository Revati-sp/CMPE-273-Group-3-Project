"""Chat Gateway Service: accepts customer WebSocket connections.

Initial skeleton. Sequence numbers are kept in memory for now; they will
move to Redis Cluster, and events will be published to Kafka, in later steps.
"""
from collections import defaultdict
from typing import Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from app.models import ChatEvent, EventType

app = FastAPI(title="EngageHub Chat Gateway")

_sequence_numbers: Dict[str, int] = defaultdict(int)


def next_sequence(conversation_id: str) -> int:
    _sequence_numbers[conversation_id] += 1
    return _sequence_numbers[conversation_id]


@app.get("/healthz")
def liveness() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readiness() -> dict:
    return {"status": "ready"}


@app.websocket("/ws/{tenant_id}/{conversation_id}")
async def chat(websocket: WebSocket, tenant_id: str, conversation_id: str) -> None:
    await websocket.accept()
    try:
        while True:
            text = await websocket.receive_text()
            event = ChatEvent(
                conversation_id=conversation_id,
                tenant_id=tenant_id,
                sequence_number=next_sequence(conversation_id),
                event_type=EventType.CUSTOMER_MESSAGE,
                payload={"text": text},
            )
            await websocket.send_text(event.model_dump_json())
    except WebSocketDisconnect:
        pass
