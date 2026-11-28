"""Smoke test for scripts/load/load_test.py: the real script, over real sockets, against a fake server.

It proves the script works (sign-up, conversations, chatting, reconnect and replay,
escalations, statistics), not anything about EngageHub's capacity.
"""

import asyncio
import json
import random
import sys
import uuid
from pathlib import Path

import httpx
import pytest
import websockets

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "load"))
import load_test  # noqa: E402


def test_percentile_and_mix():
    assert load_test.percentile([], 95) == 0
    assert load_test.percentile(list(range(1, 101)), 95) == 95
    rng = random.Random(1)
    picks = [load_test.choose_scenario(rng, load_test.DEFAULT_MIX) for _ in range(4000)]
    for name, weight in load_test.DEFAULT_MIX.items():
        assert abs(picks.count(name) / len(picks) - weight) < 0.03


def fake_http():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path in ("/api/auth/register", "/api/auth/login"):
            return httpx.Response(201, json={"access_token": "token-" + uuid.uuid4().hex})
        if request.url.path == "/api/chat/conversations":
            return httpx.Response(201, json={"conversation_id": uuid.uuid4().hex})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


class FakeChat:
    """Speaks the /ws protocol: welcome, replay, acks, one agent reply per message."""

    def __init__(self):
        self.logs: dict[str, list[dict]] = {}
        self.connections = 0

    async def handle(self, ws):
        self.connections += 1
        hello = json.loads(await ws.recv())
        log = self.logs.setdefault(hello["conversation_id"], [])
        await ws.send(json.dumps({"type": "welcome"}))
        for message in log:
            if message["sequence_number"] > hello["last_seen_seq"]:
                await ws.send(json.dumps({**message, "type": "message", "replayed": True}))
        await ws.send(json.dumps({"type": "replay_complete"}))
        async for raw in ws:
            frame = json.loads(raw)
            customer = self._append(log, frame["text"], "customer")
            await ws.send(json.dumps({"type": "message_ack", "client_message_id": frame["client_message_id"],
                                      "message": customer}))  # fmt: skip
            await asyncio.sleep(0.005)
            reply = self._append(log, "an answer", "virtual_agent")
            await ws.send(json.dumps({"type": "message", **reply}))

    @staticmethod
    def _append(log, text, kind):
        message = {"message_id": uuid.uuid4().hex, "sequence_number": len(log) + 1, "text": text,
                   "sender": {"type": kind}}  # fmt: skip
        log.append(message)
        return message


async def test_the_load_script_runs_end_to_end_against_a_fake_server():
    chat = FakeChat()
    async with websockets.serve(chat.handle, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        args = load_test.parse_args([
            "--gateway", "http://gateway.test", "--ws-url", f"ws://127.0.0.1:{port}/ws", "--customers", "12",
            "--messages-per-customer", "4", "--think-time", "0.01", "--ramp-up", "0.05", "--reconnect-ratio", "0.5",
            "--duration", "20",
        ])  # fmt: skip
        summary = await load_test.run(args, transport=fake_http())
    assert summary["customers"] == 12 and summary["error_rate"] == 0
    assert summary["agent_reply_latency"]["count"] > 12 and summary["agent_reply_latency"]["p95_ms"] > 0
    assert summary["reconnects"] > 0 and summary["recovery_time"]["count"] == summary["reconnects"]
    assert summary["duplicates_shown"] == 0  # replayed messages were recognised, not counted twice
    assert summary["requests_per_second"] > 0
    assert chat.connections == 12 + summary["reconnects"]


@pytest.mark.parametrize("value", ["--customers", "--duration"])
def test_cli_defaults_are_sensible(value):
    args = load_test.parse_args([])
    assert args.customers == 50 and args.ws_url == "ws://localhost:18080/ws"
