#!/usr/bin/env python3
"""Runs the reply-replay failure demonstration against the Docker Compose stack.

    docker compose up -d --build
    python scripts/demo_reply_replay.py                      # all steps, including the restart
    python scripts/demo_reply_replay.py --skip-restart       # steps 1-14 only

It plays the customer screen's part over the same WebSocket protocol (connect, ack,
disconnect, reconnect), so every step can be checked automatically. Each step prints
what happened, and the script exits non-zero if any check fails.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import websockets

sys.path.insert(0, str(Path(__file__).parent))
import fake_reply_producer  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


class DemoFailure(AssertionError):
    pass


def step(number: int | str, text: str) -> None:
    print(f"\n[{number}] {text}", flush=True)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise DemoFailure(message)
    print(f"    ok: {message}", flush=True)


@dataclass
class CustomerSocket:
    """Speaks the same protocol as the customer screen."""

    url: str
    token: str
    socket: websockets.ClientConnection | None = None
    frames: list[dict] = field(default_factory=list)

    async def connect(self) -> tuple[dict, list[dict]]:
        self.socket = await websockets.connect(self.url, open_timeout=10)
        await self.socket.send(json.dumps({"type": "hello", "token": self.token}))
        ack = await self.recv()
        if ack.get("type") != "connection_ack":
            raise DemoFailure(f"expected connection_ack, got {ack}")
        replay = await self.recv_until("replay_completed")
        return ack, replay

    async def recv(self, timeout: float = 10.0) -> dict:  # noqa: ASYNC109
        frame = json.loads(await asyncio.wait_for(self.socket.recv(), timeout))
        self.frames.append(frame)
        return frame

    async def recv_until(self, frame_type: str, timeout: float = 10.0) -> list[dict]:
        frames = []
        while True:
            frames.append(await self.recv(timeout))
            if frames[-1]["type"] == frame_type:
                return frames

    async def messages(self, count: int, timeout: float = 15.0) -> list[dict]:
        out = []
        while len(out) < count:
            frame = await self.recv(timeout)
            if frame["type"] == "chat_message":
                out.append(frame)
        return out

    async def quiet(self, seconds: float) -> list[dict]:
        """Collect anything that arrives within ``seconds`` (expected: nothing)."""
        got = []
        try:
            while True:
                got.append(await self.recv(seconds))
        except TimeoutError:
            return got

    async def ack(self, frame: dict) -> dict:
        await self.socket.send(
            json.dumps({"type": "ack", "sequence_number": frame["sequence_number"], "message_id": frame["message_id"]})
        )
        reply = await self.recv()
        if reply.get("type") != "ack_confirmed":
            raise DemoFailure(f"ack was not confirmed: {reply}")
        return reply["cursor"]

    async def close(self) -> None:
        if self.socket is not None:
            await self.socket.close()
            self.socket = None


def seqs(frames: list[dict]) -> list[int]:
    return [f["sequence_number"] for f in frames if f["type"] == "chat_message"]


async def publish(args, content: str, *, message_id: str | None = None) -> str:
    producer_args = fake_reply_producer.parse_args(
        ["--conversation-id", args.conversation_id, "--content", content,
         "--bootstrap-servers", args.bootstrap_servers]
        + (["--message-id", message_id] if message_id else [])
    )  # fmt: skip
    sent = await fake_reply_producer.publish(producer_args)
    return sent[0]["message_id"]


def compose(*arguments: str) -> None:
    subprocess.run(["docker", "compose", *arguments], cwd=ROOT, check=True, capture_output=True, text=True)


async def wait_until_ready(http: httpx.AsyncClient, headers: dict, conversation_id: str, timeout: float = 180) -> float:
    started = time.time()
    while time.time() - started < timeout:
        try:
            response = await http.post(
                "/api/chat/conversations", json={"conversation_id": conversation_id}, headers=headers
            )
            if response.status_code in (200, 201):
                return time.time() - started
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1)
    raise DemoFailure("chat-service did not become ready in time")


async def run(args) -> dict:
    ws_url = args.gateway.replace("http", "ws", 1) + f"/ws/customer/{args.conversation_id}"
    async with httpx.AsyncClient(base_url=args.gateway, timeout=10) as http:
        step(1, "Stack is running (gateway, auth, chat-service, Kafka, Redis, PostgreSQL)")
        login = await http.post(
            "/api/auth/login", json={"tenant_id": args.tenant, "email": args.email, "password": args.password}
        )
        check(login.status_code == 200, f"logged in as {args.email}")
        token = login.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        step(2, "Customer screen opened (this script speaks the screen's WebSocket protocol)")
        created = await http.post(
            "/api/chat/conversations", json={"conversation_id": args.conversation_id}, headers=headers
        )
        check(created.status_code in (200, 201), f"conversation {args.conversation_id} is ready")

        step(3, f"Connect to {args.conversation_id}")
        customer = CustomerSocket(ws_url, token)
        connection_ack, replay = await customer.connect()
        earlier = [f for f in replay if f["type"] == "chat_message"]
        for frame in earlier:  # clear any earlier run, so the steps below start from a clean cursor
            await customer.ack(frame)
        base = connection_ack["latest_sequence"]
        print(
            f"    pod={connection_ack['pod']} existing messages={base} (acknowledged {len(earlier)} from earlier runs)"
        )

        step("4-5", "Publish fake replies 1 and 2 to Kafka (chat.replies)")
        await publish(args, "Reply 1: your order has shipped.")
        await publish(args, "Reply 2: it is out for delivery.")

        step(6, "Both appear on the customer socket")
        live = await customer.messages(2)
        check(seqs(live) == [base + 1, base + 2], f"received sequences {seqs(live)} in order")

        step(7, "Acknowledge only reply 1")
        cursor = await customer.ack(live[0])
        check(
            cursor["last_acknowledged_sequence"] == base + 1, f"Redis cursor = {cursor['last_acknowledged_sequence']}"
        )

        step(8, "Disconnect the customer WebSocket")
        await customer.close()
        check(True, "socket closed")

        step(9, "Publish fake replies 3, 4 and 5 while disconnected")
        for n in (3, 4, 5):
            await publish(args, f"Reply {n}: published while you were away.")

        step(10, "They are stored but were not delivered")
        stored = {}
        for _ in range(50):
            stored = (await http.get(f"/api/chat/conversations/{args.conversation_id}/messages?after_sequence={base}",
                                     headers=headers)).json()  # fmt: skip
            if stored["latest_sequence"] >= base + 5:
                break
            await asyncio.sleep(0.2)
        check(stored["latest_sequence"] == base + 5, f"PostgreSQL log holds up to sequence {stored['latest_sequence']}")
        state = (await http.get(f"/api/chat/conversations/{args.conversation_id}/cursor", headers=headers)).json()
        check(
            state["last_delivered_sequence"] == base + 2, f"last delivered is still {state['last_delivered_sequence']}"
        )

        step(11, "Reconnect the customer")
        customer = CustomerSocket(ws_url, token)
        started = time.perf_counter()
        connection_ack, replay = await customer.connect()
        recovery_ms = (time.perf_counter() - started) * 1000
        check(connection_ack["reconnected"], f"server recognised the reconnect (pod {connection_ack['pod']})")

        step(12, "Replies 2, 3, 4 and 5 are replayed in order")
        expected = [base + n for n in (2, 3, 4, 5)]
        check(replay[0]["type"] == "replay_started" and replay[0]["from_sequence"] == base + 2,
              f"replay_started from {replay[0]['from_sequence']} to {replay[0]['to_sequence']}")  # fmt: skip
        check(seqs(replay) == expected, f"replayed {seqs(replay)} in {recovery_ms:.0f} ms")
        replayed = [f for f in replay if f["type"] == "chat_message"]
        for frame in replayed:
            cursor = await customer.ack(frame)
        check(cursor["last_acknowledged_sequence"] == base + 5, "acknowledged every replayed message")

        step(13, "Send a duplicate Kafka event (reply 5's message ID again)")
        await publish(args, "Reply 5 again (duplicate)", message_id=replayed[-1]["message_id"])

        step(14, "No duplicate message appears")
        extra = [f for f in await customer.quiet(3) if f["type"] == "chat_message"]
        check(extra == [], "nothing new arrived on the socket")
        after = (await http.get(f"/api/chat/conversations/{args.conversation_id}/messages?after_sequence={base}",
                                headers=headers)).json()  # fmt: skip
        check(after["latest_sequence"] == base + 5, "the log still ends at the same sequence")
        await customer.close()

        result = {"conversation_id": args.conversation_id, "base": base, "recovery_ms": round(recovery_ms, 1)}
        if args.skip_restart:
            return result

        step(15, "Stop the Chat Service (all replicas)")
        compose("stop", "chat-service")
        check(True, "chat-service stopped")
        await publish(args, "Reply 6: sent while the Chat Service was down.")
        print("    published reply 6 while chat-service was stopped (Kafka retains it)")

        step(16, "Restart the Chat Service")
        compose("start", "chat-service")
        waited = await wait_until_ready(http, headers, args.conversation_id)
        check(True, f"chat-service ready after {waited:.1f}s")

        step(17, "Reconnect: replay still works")
        customer = CustomerSocket(ws_url, token)
        connection_ack, replay = None, []
        for _ in range(30):  # the consumer may still be catching up on reply 6
            connection_ack, replay = await customer.connect()
            if seqs(replay):
                break
            await customer.close()
            customer = CustomerSocket(ws_url, token)
            await asyncio.sleep(1)
        check(connection_ack["cursor"]["last_acknowledged_sequence"] == base + 5, "cursor survived the restart")
        check(seqs(replay) == [base + 6], f"replayed {seqs(replay)} after the restart")
        await customer.ack([f for f in replay if f["type"] == "chat_message"][-1])
        await customer.close()
        return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", default="http://localhost:18080")
    parser.add_argument("--conversation-id", default="conversation-123")
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--email", default="dana@acme.example")
    parser.add_argument("--password", default="engagehub-demo")
    parser.add_argument("--bootstrap-servers", default="localhost:19092,localhost:19093,localhost:19094")
    parser.add_argument("--skip-restart", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        result = asyncio.run(run(parse_args(argv)))
    except DemoFailure as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        return 1
    print(f"\nAll steps passed: {json.dumps(result)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
