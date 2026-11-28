#!/usr/bin/env python3
"""Load test for a running EngageHub (Docker Compose or Kubernetes), through the gateway.

    pip install -r scripts/requirements.txt
    python scripts/load/load_test.py --customers 50 --duration 120
    python scripts/load/load_test.py --customers 50 --prometheus http://localhost:19090 --kubectl-namespace engagehub

Each simulated customer registers, opens a conversation and chats on the support socket
(/ws). Messages are product, order and FAQ questions (answered by virtual agents) and
requests for a human (escalated through the supervisor). Some customers drop the
connection and reconnect mid-conversation, which exercises replay.

It reports requests per second, latency (average and p95) for the server's ack and for
the agent's reply, error rate, reconnects and recovery time, and duplicates shown
(expected 0). With --prometheus it also reports consumer lag and how many replicas of
each agent processed tasks during the run, which shows the work was shared. With
--kubectl-namespace it adds replica and HPA counts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field

import httpx
import websockets

SCENARIOS = {
    "product": ("Can you recommend a tent under $200?", "Looking for hiking boots", "Do you have a backpack?"),
    "order": ("Where is my order ORD-1001?", "Has ORD-1002 shipped yet?", "Track my order please"),
    "faq": ("What is your warranty policy?", "How long does shipping take?", "What are your support hours?"),
    "escalation": ("I want to talk to a real person", "Please let me speak with a human agent"),
}
DEFAULT_MIX = {"product": 0.35, "order": 0.3, "faq": 0.25, "escalation": 0.1}


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile; 0 for an empty list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, round(p / 100 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def choose_scenario(rng: random.Random, mix: dict[str, float]) -> str:
    roll, total = rng.random(), 0.0
    for name, weight in mix.items():
        total += weight
        if roll < total:
            return name
    return next(reversed(mix))


@dataclass
class Stats:
    started: float = field(default_factory=time.perf_counter)
    requests: int = 0
    errors: int = 0
    ack_ms: list[float] = field(default_factory=list)
    reply_ms: list[float] = field(default_factory=list)
    recovery_ms: list[float] = field(default_factory=list)
    reconnects: int = 0
    duplicates_shown: int = 0
    replies_by_scenario: dict[str, int] = field(default_factory=dict)
    escalations_confirmed: int = 0
    error_kinds: dict[str, int] = field(default_factory=dict)

    def error(self, kind: str) -> None:
        self.errors += 1
        self.error_kinds[kind] = self.error_kinds.get(kind, 0) + 1

    def summary(self) -> dict:
        elapsed = time.perf_counter() - self.started

        def lat(values):
            return {"avg_ms": round(statistics.mean(values), 1) if values else 0.0,
                    "p95_ms": round(percentile(values, 95), 1), "count": len(values)}  # fmt: skip

        return {
            "elapsed_s": round(elapsed, 1),
            "requests": self.requests,
            "requests_per_second": round(self.requests / elapsed, 1) if elapsed else 0.0,
            "error_rate": round(self.errors / self.requests, 4) if self.requests else 0.0,
            "errors": self.error_kinds,
            "server_ack_latency": lat(self.ack_ms),
            "agent_reply_latency": lat(self.reply_ms),
            "reconnects": self.reconnects,
            "recovery_time": lat(self.recovery_ms),
            "duplicates_shown": self.duplicates_shown,
            "replies_by_scenario": self.replies_by_scenario,
            "escalations_confirmed": self.escalations_confirmed,
        }


class Customer:
    def __init__(self, index: int, args, stats: Stats, http: httpx.AsyncClient, rng: random.Random):
        self.index, self.args, self.stats, self.http, self.rng = index, args, stats, http, rng
        self.token = ""
        self.conversation_id = ""
        self.seen: dict[str, int] = {}  # message_id -> sequence number
        self.last_seq = 0
        self.ws = None

    async def _http(self, method: str, url: str, **kwargs) -> httpx.Response:
        for attempt in range(6):
            self.stats.requests += 1
            try:
                response = await self.http.request(method, url, **kwargs)
            except httpx.HTTPError:
                self.stats.error("http_connect")
                await asyncio.sleep(0.5 * 2**attempt)
                continue
            if response.status_code in (429, 502, 503):  # rate limit or a restarting pod: back off and retry
                self.stats.error(f"http_{response.status_code}")
                await asyncio.sleep(0.5 * 2**attempt)
                continue
            return response
        raise RuntimeError(f"{method} {url} kept failing")

    async def sign_up(self) -> None:
        email = f"load-{self.args.run_id}-{self.index}@{self.args.tenant}.example"
        body = {
            "tenant_id": self.args.tenant,
            "email": email,
            "name": f"Load {self.index}",
            "password": self.args.password,
        }
        response = await self._http("POST", "/api/auth/register", json=body)
        if response.status_code == 409:
            response = await self._http(
                "POST", "/api/auth/login", json={k: body[k] for k in ("tenant_id", "email", "password")}
            )
        response.raise_for_status()
        self.token = response.json()["access_token"]
        created = await self._http(
            "POST", "/api/chat/conversations", json={}, headers={"Authorization": f"Bearer {self.token}"}
        )
        created.raise_for_status()
        self.conversation_id = created.json()["conversation_id"]

    async def connect(self) -> None:
        started = time.perf_counter()
        self.ws = await websockets.connect(self.args.ws_url, open_timeout=10)
        self.stats.requests += 1
        await self.ws.send(json.dumps({"type": "hello", "token": self.token, "conversation_id": self.conversation_id,
                                       "last_seen_seq": self.last_seq}))  # fmt: skip
        while True:
            frame = await self._recv()
            if frame["type"] == "replay_complete":
                return (time.perf_counter() - started) * 1000

    async def _recv(self, timeout: float = 30) -> dict:  # noqa: ASYNC109
        frame = json.loads(await asyncio.wait_for(self.ws.recv(), timeout))
        if frame.get("type") == "message":
            self._record(frame)
        elif frame.get("type") == "message_ack":
            self._record(frame["message"])
        return frame

    def _record(self, message: dict) -> None:
        message_id, seq = message["message_id"], message["sequence_number"]
        if message_id in self.seen:
            # Replays and echoes repeat messages on the wire by design; a screen shows each once.
            return
        if seq in self.seen.values():
            self.stats.duplicates_shown += 1  # same position, different ID: a real duplicate
        self.seen[message_id] = seq
        self.last_seq = max(self.last_seq, seq)

    async def say(self, scenario: str) -> None:
        text = self.rng.choice(SCENARIOS[scenario])
        client_id = uuid.uuid4().hex
        sent = time.perf_counter()
        await self.ws.send(json.dumps({"type": "message", "client_message_id": client_id, "text": text}))
        self.stats.requests += 1
        acked = False
        deadline = sent + self.args.reply_timeout
        while time.perf_counter() < deadline:
            frame = await self._recv(timeout=deadline - time.perf_counter())
            if frame["type"] == "message_ack" and frame["client_message_id"] == client_id:
                self.stats.ack_ms.append((time.perf_counter() - sent) * 1000)
                acked = True
            elif frame["type"] == "error" and frame.get("client_message_id") == client_id:
                self.stats.error(frame.get("code", "ws_error"))
                return
            elif acked and frame["type"] == "message" and frame.get("sender", {}).get("type") != "customer":
                self.stats.reply_ms.append((time.perf_counter() - sent) * 1000)
                self.stats.replies_by_scenario[scenario] = self.stats.replies_by_scenario.get(scenario, 0) + 1
                if scenario == "escalation":
                    self.stats.escalations_confirmed += 1
                return
        self.stats.error("reply_timeout")

    async def run(self, stop_at: float) -> None:
        await asyncio.sleep(self.rng.uniform(0, self.args.ramp_up))
        try:
            await self.sign_up()
            await self.connect()
            for _turn in range(self.args.messages_per_customer):
                if time.perf_counter() > stop_at:
                    break
                scenario = choose_scenario(self.rng, self.args.mix)
                try:
                    await self.say(scenario)
                except (TimeoutError, websockets.WebSocketException):
                    self.stats.error("socket_dropped")
                    await self.reconnect()
                if scenario == "escalation":
                    break  # a human takes over; the virtual agents stop answering this conversation
                if self.rng.random() < self.args.reconnect_ratio:
                    await self.reconnect()
                await asyncio.sleep(self.rng.uniform(0.5, 1.5) * self.args.think_time)
        except Exception as exc:  # one failed customer must not stop the run
            self.stats.error(type(exc).__name__)
        finally:
            if self.ws is not None:
                await self.ws.close()

    async def reconnect(self) -> None:
        if self.ws is not None:
            await self.ws.close()
        self.stats.reconnects += 1
        for attempt in range(6):
            try:
                self.stats.recovery_ms.append(await self.connect())
                return
            except (OSError, TimeoutError, websockets.WebSocketException):
                self.stats.error("reconnect_failed")
                await asyncio.sleep(0.25 * 2**attempt)
        raise RuntimeError("could not reconnect")


async def prometheus(url: str, query: str) -> list[dict]:
    async with httpx.AsyncClient(base_url=url, timeout=5) as client:
        response = await client.get("/api/v1/query", params={"query": query})
        response.raise_for_status()
        return response.json()["data"]["result"]


async def cluster_report(args, window_s: int) -> dict:
    report: dict = {}
    if args.prometheus:
        try:
            lag = await prometheus(args.prometheus, "sum by (group) (engagehub_kafka_consumer_lag)")
            report["consumer_lag_now"] = {r["metric"].get("group", "?"): float(r["value"][1]) for r in lag}
            per_replica = await prometheus(
                args.prometheus,
                f"sum by (agent_type, instance) (increase(engagehub_agent_tasks_processed_total[{window_s}s]))",
            )
            workers: dict[str, dict[str, float]] = {}
            for r in per_replica:
                if float(r["value"][1]) > 0:
                    workers.setdefault(r["metric"].get("agent_type", "?"), {})[r["metric"].get("instance", "?")] = (
                        round(float(r["value"][1]), 1)
                    )
            report["tasks_processed_per_replica"] = workers
            report["replicas_that_processed_tasks"] = {a: len(v) for a, v in workers.items()}
        except Exception as exc:
            report["prometheus_error"] = repr(exc)
    if args.kubectl_namespace:
        try:
            command = ["kubectl", "-n", args.kubectl_namespace, "get", "deploy,hpa", "-o", "json"]
            completed = await asyncio.to_thread(subprocess.run, command, capture_output=True, text=True, check=True)
            raw = completed.stdout
            items = json.loads(raw)["items"]
            report["replicas"] = {i["metadata"]["name"]: i["status"].get("readyReplicas", 0)
                                  for i in items if i["kind"] == "Deployment"}  # fmt: skip
            report["hpa_desired"] = {i["metadata"]["name"]: i["status"].get("desiredReplicas")
                                     for i in items if i["kind"] == "HorizontalPodAutoscaler"}  # fmt: skip
        except Exception as exc:
            report["kubectl_error"] = repr(exc)
    return report


async def run(args, transport: httpx.AsyncBaseTransport | None = None) -> dict:
    stats = Stats()
    rng = random.Random(args.seed)
    stop_at = time.perf_counter() + args.duration
    limits = httpx.Limits(max_connections=args.customers * 2)
    async with httpx.AsyncClient(base_url=args.gateway, timeout=15, limits=limits, transport=transport) as http:
        customers = [Customer(i, args, stats, http, random.Random(rng.random())) for i in range(args.customers)]
        await asyncio.gather(*(c.run(stop_at) for c in customers))
    summary = stats.summary()
    summary["customers"] = args.customers
    summary["cluster"] = await cluster_report(args, max(60, int(summary["elapsed_s"]) + 30))
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", default="http://localhost:18080")
    parser.add_argument("--ws-url", default=None, help="defaults to the gateway's /ws")
    parser.add_argument("--customers", type=int, default=50)
    parser.add_argument("--duration", type=float, default=120, help="seconds before customers stop starting turns")
    parser.add_argument("--messages-per-customer", type=int, default=6)
    parser.add_argument("--think-time", type=float, default=2.0)
    parser.add_argument("--ramp-up", type=float, default=10.0)
    parser.add_argument("--reconnect-ratio", type=float, default=0.15)
    parser.add_argument("--reply-timeout", type=float, default=30.0)
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--password", default="load-test-password")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--prometheus", default=None)
    parser.add_argument("--kubectl-namespace", default=None)
    parser.add_argument("--report-json", default=None)
    args = parser.parse_args(argv)
    args.mix = DEFAULT_MIX
    args.run_id = uuid.uuid4().hex[:8]
    args.ws_url = args.ws_url or args.gateway.replace("http", "ws", 1) + "/ws"
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    summary = asyncio.run(run(args))
    text = json.dumps(summary, indent=2)
    print(text)
    if args.report_json:
        with open(args.report_json, "w") as fh:
            fh.write(text)
    return 0 if summary["error_rate"] < 0.05 and summary["duplicates_shown"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
