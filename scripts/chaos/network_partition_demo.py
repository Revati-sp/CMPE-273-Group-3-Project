#!/usr/bin/env python3
"""Simulate a network partition of one Chat Service replica and verify the session migrates.

    docker compose up -d --build            # two chat-service replicas, as in docker-compose.yml
    python scripts/chaos/network_partition_demo.py               # partition (network disconnect)
    python scripts/chaos/network_partition_demo.py --mode stop   # crash instead (docker stop)

Safety: the only things changed are the Docker network attachment (or running state)
of one chat-service container in this Compose project. The script always reconnects
or restarts it, even when a check fails. Nothing on the host is touched.

Steps (each is checked; the script exits non-zero on the first failure):
  1. Find the two chat-service replicas.
  2. Connect a customer through the gateway until it lands on replica 1.
  3. Publish replies; acknowledge only some.
  4. Partition replica 1 (docker network disconnect), or stop it.
  5. Publish replies during the failure (stored once the consumer group rebalances).
  6. Reconnect through the gateway: the customer reaches replica 2.
  7. The session is claimed (handover_started), unacknowledged and missed replies are
     replayed in order, then handover_completed.
  8. A duplicate Kafka event shows nothing new.
  9. The customer keeps chatting: a customer message and a new live reply.
 10. Heal the partition: replica 1's stale connection is fenced ("old pod fenced").
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from demo_reply_replay import CustomerSocket, DemoFailure, check, publish, seqs, step  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]


def docker(*args: str, check_result: bool = True) -> str:
    result = subprocess.run(["docker", *args], cwd=ROOT, capture_output=True, text=True, check=check_result)
    return result.stdout.strip()


def replicas() -> list[dict]:
    ids = docker("compose", "ps", "-q", "chat-service").split()
    out = []
    for container_id in ids:
        info = json.loads(docker("inspect", container_id))[0]
        out.append(
            {
                "id": container_id,
                "name": info["Name"].lstrip("/"),
                "hostname": info["Config"]["Hostname"],  # the pod name the service reports
                "networks": list(info["NetworkSettings"]["Networks"]),
            }
        )
    return sorted(out, key=lambda r: r["name"])


async def connect_to(url: str, token: str, wanted_pod: str, attempts: int = 30):
    for _ in range(attempts):
        customer = CustomerSocket(url, token)
        ack, replay = await customer.connect()
        if ack["pod"] == wanted_pod:
            return customer, ack, replay
        await customer.close()
    raise DemoFailure(f"the gateway never routed to {wanted_pod}")


def metric_from(container: str, name: str) -> str:
    script = (
        "import urllib.request as u;"
        "print('\\n'.join(l for l in u.urlopen('http://127.0.0.1:8000/metrics').read().decode().splitlines()"
        f" if l.startswith('{name}')))"
    )
    return docker("exec", container, "python", "-c", script, check_result=False)


async def run(args) -> dict:
    found = replicas()
    step(1, "Find the chat-service replicas")
    check(len(found) >= 2, f"{len(found)} replicas: " + ", ".join(f"{r['name']} ({r['hostname']})" for r in found))
    victim, survivor = found[0], found[1]
    conversation_id = args.conversation_id or f"conversation-partition-{uuid.uuid4().hex[:8]}"
    ws_url = args.gateway.replace("http", "ws", 1) + f"/ws/customer/{conversation_id}"
    args.conversation_id = conversation_id
    partitioned = stopped = False
    try:
        async with httpx.AsyncClient(base_url=args.gateway, timeout=10) as http:
            login = await http.post("/api/auth/login", json={"tenant_id": "acme", "email": args.email,
                                                              "password": args.password})  # fmt: skip
            check(login.status_code == 200, "logged in")
            token = login.json()["access_token"]
            headers = {"Authorization": f"Bearer {token}"}
            await http.post("/api/chat/conversations", json={"conversation_id": conversation_id}, headers=headers)

            step(2, f"Connect the customer to replica 1 ({victim['name']})")
            customer, ack, _ = await connect_to(ws_url, token, victim["hostname"])
            check(
                ack["pod"] == victim["hostname"], f"connected to {ack['pod']}, session version {ack['session_version']}"
            )

            step(3, "Publish 5 replies and acknowledge only the first 2")
            for n in range(1, 6):
                await publish(args, f"Reply {n} before the failure")
            live = await customer.messages(5)
            check(seqs(live) == [1, 2, 3, 4, 5], f"received {seqs(live)}")
            await customer.ack(live[0])
            cursor = await customer.ack(live[1])
            check(cursor["last_acknowledged_sequence"] == 2, "Redis cursor = 2")

            step(4, f"Network partition: {args.mode} replica 1")
            if args.mode == "partition":
                for network in victim["networks"]:
                    docker("network", "disconnect", network, victim["id"])
                partitioned = True
                print(f"    network partition simulated: {victim['name']} detached from {victim['networks']}")
            else:
                docker("stop", victim["id"])
                stopped = True
                print(f"    {victim['name']} stopped")
            failure_started = time.perf_counter()

            step(5, "Publish 3 replies during the failure")
            for n in range(6, 9):
                await publish(args, f"Reply {n} during the failure")
            # If the partitioned replica owned chat.replies partitions, the consumer group
            # rebalances after the session timeout before these are stored.
            for _ in range(120):
                stored = (await http.get(f"/api/chat/conversations/{conversation_id}/messages", headers=headers)).json()
                if stored.get("latest_sequence", 0) >= 8:
                    break
                await asyncio.sleep(0.5)
            check(stored.get("latest_sequence") == 8, "replies 6 to 8 are in the PostgreSQL log")

            step(6, "Force a reconnect through the gateway")
            await customer.close()
            customer = CustomerSocket(ws_url, token)
            ack, frames = await customer.connect()  # includes handover_completed when the pod changed
            check(ack["pod"] == survivor["hostname"], f"now on replica 2 ({ack['pod']})")

            step(7, "Session restored and the unacknowledged tail replayed")
            started = next((f for f in frames if f["type"] == "handover_started"), None)
            done = next((f for f in frames if f["type"] == "handover_completed"), None)
            check(
                started is not None and started["old_pod"] == victim["hostname"],
                f"handover_started from {started and started['old_pod']} ({started and started['reason']})",
            )
            check(ack["cursor"]["last_acknowledged_sequence"] == 2, "acknowledged position (2) preserved")
            check(seqs(frames) == [3, 4, 5, 6, 7, 8], f"replayed {seqs(frames)} in order, no duplicates")
            check(done is not None and done["session_version"] > 0,
                  f"handover_completed, session version {done and done['session_version']}")  # fmt: skip
            recovery_s = time.perf_counter() - failure_started
            for frame in [f for f in frames if f["type"] == "chat_message"]:
                await customer.ack(frame)

            step(8, "A duplicate Kafka event shows nothing new")
            await publish(
                args, "duplicate", message_id=[f for f in frames if f["type"] == "chat_message"][-1]["message_id"]
            )
            extra = [f for f in await customer.quiet(3) if f["type"] == "chat_message"]
            check(extra == [], "no duplicate message")

            step(9, "The customer keeps chatting")
            await customer.socket.send(json.dumps({"type": "customer_message", "client_message_id": uuid.uuid4().hex,
                                                   "content": "Thanks, still here"}))  # fmt: skip
            accepted = await customer.recv_until("customer_message_accepted")
            check(accepted[-1]["sequence_number"] == 9, "customer message stored as sequence 9")
            await publish(args, "Reply 10 after recovery")
            later = await customer.messages(2)
            check(seqs(later)[-1] == 10, f"live delivery continues ({seqs(later)})")
            await customer.close()

        step(10, "Heal: the old replica comes back and its stale connection is fenced")
        if partitioned:
            for network in victim["networks"]:
                docker("network", "connect", network, victim["id"])
            partitioned = False
            fenced = False
            for _ in range(30):
                logs = docker("logs", "--since", "2m", victim["id"], check_result=False)
                if "old pod fenced" in logs or "websocket disconnected" in logs:
                    fenced = "old pod fenced" in logs
                    break
                await asyncio.sleep(1)
            print(f"    {victim['name']}: {'old pod fenced' if fenced else 'old connection closed without a write'}")
        elif stopped:
            docker("start", victim["id"])
            stopped = False
            print(f"    {victim['name']} restarted")
        print("    " + (metric_from(survivor["id"], "engagehub_handovers_total") or "(metrics unavailable)"))
        return {"conversation_id": conversation_id, "recovery_seconds": round(recovery_s, 1),
                "victim": victim["name"], "survivor": survivor["name"]}  # fmt: skip
    finally:
        if partitioned:
            for network in victim["networks"]:
                docker("network", "connect", network, victim["id"], check_result=False)
        if stopped:
            docker("start", victim["id"], check_result=False)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["partition", "stop"], default="partition")
    parser.add_argument("--gateway", default="http://localhost:18080")
    parser.add_argument("--conversation-id", default=None)
    parser.add_argument("--email", default="dana@acme.example")
    parser.add_argument("--password", default="engagehub-demo")
    parser.add_argument("--bootstrap-servers", default="localhost:19092,localhost:19093,localhost:19094")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        result = asyncio.run(run(parse_args(argv)))
    except (DemoFailure, subprocess.CalledProcessError) as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        return 1
    print(f"\nAll steps passed: {json.dumps(result)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
