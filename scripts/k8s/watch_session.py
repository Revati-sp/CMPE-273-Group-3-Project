#!/usr/bin/env python3
"""Hold one customer session open while Chat Service pods roll, and report what happened.

Used by scripts/k8s/rolling_update_demo.sh. It talks to chat-service directly through
`kubectl port-forward` and mints its own customer token from JWT_SECRET, because the
Chat Service manifests do not include auth-service. Publishing needs
ENABLE_DEV_ENDPOINTS=true on the deployment (the script's caller sets it).

It reconnects with backoff on every close, publishes a reply every second, acknowledges
everything it receives, and at the end checks: every published reply was received, in
order, with no duplicates shown, across however many pods it was moved between.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid

import httpx
import jwt
import websockets


def token(secret: str, customer: str, tenant: str) -> str:
    now = int(time.time())
    claims = {"sub": customer, "tenant_id": tenant, "role": "customer", "customer_id": customer,
              "name": "Rolling Update Watcher", "iat": now, "exp": now + 3600}  # fmt: skip
    return jwt.encode(claims, secret, algorithm="HS256")


async def run(args) -> int:
    conversation = f"conversation-rollout-{uuid.uuid4().hex[:8]}"
    bearer = token(args.jwt_secret, args.customer, args.tenant)
    headers = {"Authorization": f"Bearer {bearer}"}
    seen: dict[str, int] = {}
    order: list[int] = []
    pods: list[str] = []
    handovers = closes = 0
    published = 0
    deadline = time.time() + args.duration

    async with httpx.AsyncClient(base_url=args.chat_url, timeout=10) as http:
        (await http.post("/conversations", json={"conversation_id": conversation}, headers=headers)).raise_for_status()

        async def publisher():
            nonlocal published
            while time.time() < deadline:
                try:
                    r = await http.post(f"/dev/fake-replies?conversation_id={conversation}",
                                        json={"content": f"tick {published + 1}"}, headers=headers)  # fmt: skip
                    if r.status_code == 202:
                        published += 1
                except httpx.HTTPError:
                    pass  # the pod behind the port-forward may be restarting
                await asyncio.sleep(1)

        task = asyncio.create_task(publisher())
        attempt = 0
        ws_url = args.chat_url.replace("http", "ws", 1) + f"/ws/customer/{conversation}"
        while time.time() < deadline + 10:
            try:
                async with websockets.connect(ws_url, open_timeout=5) as ws:
                    await ws.send(json.dumps({"type": "hello", "token": bearer}))
                    attempt = 0
                    async for raw in ws:
                        frame = json.loads(raw)
                        kind = frame["type"]
                        if kind == "connection_ack" and (not pods or pods[-1] != frame["pod"]):
                            pods.append(frame["pod"])
                        elif kind == "handover_completed":
                            handovers += 1
                            print(
                                f"handover {frame['old_pod']} -> {frame['new_pod']}: "
                                f"replayed {frame['replayed_count']} in {frame['recovery_ms']} ms",
                                flush=True,
                            )
                        elif kind == "chat_message":
                            if frame["message_id"] not in seen:
                                seen[frame["message_id"]] = frame["sequence_number"]
                                order.append(frame["sequence_number"])
                            await ws.send(json.dumps({"type": "ack", "sequence_number": frame["sequence_number"],
                                                      "message_id": frame["message_id"]}))  # fmt: skip
                        if time.time() > deadline + 10:
                            break
            except (OSError, websockets.WebSocketException, TimeoutError):
                pass
            closes += 1
            attempt += 1
            await asyncio.sleep(min(5, 0.25 * 2**attempt))
        await task
        stored = (await http.get(f"/conversations/{conversation}/messages", headers=headers)).json()

    ok = order == sorted(order) and len(order) == stored["latest_sequence"] == len(set(order))
    print(json.dumps({"conversation": conversation, "pods_seen": pods, "handovers": handovers,
                      "reconnects": closes - 1, "published": published, "stored": stored["latest_sequence"],
                      "received_unique": len(order), "in_order": order == sorted(order), "ok": ok}))  # fmt: skip
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chat-url", default="http://localhost:8000")
    parser.add_argument("--jwt-secret", required=True)
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--customer", default="cust-rollout-watcher")
    parser.add_argument("--duration", type=int, default=90)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
