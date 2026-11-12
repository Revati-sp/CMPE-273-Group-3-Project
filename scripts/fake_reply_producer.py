#!/usr/bin/env python3
"""Publish fake virtual-agent replies to the chat.replies Kafka topic.

    python scripts/fake_reply_producer.py --conversation-id conversation-123 \
        --content "Your order is currently out for delivery."

    # five numbered replies, half a second apart
    python scripts/fake_reply_producer.py --conversation-id conversation-123 --count 5 --interval 0.5

    # the same event three times, to show it is stored and shown only once
    python scripts/fake_reply_producer.py --conversation-id conversation-123 \
        --message-id message-dup-1 --repeat 3 --content "Only once"

By default it connects to the brokers exposed by Docker Compose on localhost:19092-19094.
Inside the Compose network, use --bootstrap-servers kafka-1:9092. The only dependency
is aiokafka (pip install -r scripts/requirements.txt).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer

TOPIC = "chat.replies"
SENDERS = {
    "order-tracking-agent": "Order Tracking Agent",
    "product-advisor-agent": "Product Advisor Agent",
    "returns-refunds-agent": "Returns and Refunds Agent",
    "billing-agent": "Billing Agent",
    "technical-support-agent": "Technical Support Agent",
    "faq-agent": "FAQ Agent",
}


def build_event(args: argparse.Namespace, index: int) -> dict:
    content = args.content
    if args.count > 1:
        content = f"{content} ({index + 1}/{args.count})" if content else f"Fake reply {index + 1} of {args.count}"
    return {
        "event_type": "VirtualAgentReplyProduced",
        "message_id": args.message_id or f"message-{uuid.uuid4()}",
        "conversation_id": args.conversation_id,
        "sender_type": args.sender_type,
        "sender_id": args.sender_id,
        "sender_name": SENDERS.get(args.sender_id, args.sender_id),
        "content": content or "Fake reply",
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "correlation_id": str(uuid.uuid4()),
    }


async def publish(args: argparse.Namespace) -> list[dict]:
    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        client_id="fake-reply-producer",
    )
    await producer.start()
    sent = []
    try:
        for index in range(args.count):
            event = build_event(args, index)
            for _ in range(args.repeat):
                # The conversation ID is the key, so one conversation's replies stay ordered in one partition.
                meta = await producer.send_and_wait(
                    TOPIC,
                    key=args.conversation_id.encode(),
                    value=json.dumps(event).encode(),
                    headers=[("correlation_id", event["correlation_id"].encode())],
                )
                sent.append(event)
                print(
                    json.dumps(
                        {
                            "published": event["message_id"],
                            "conversation_id": event["conversation_id"],
                            "partition": meta.partition,
                            "offset": meta.offset,
                            "content": event["content"],
                        }
                    ),
                    flush=True,
                )
            if args.interval and index < args.count - 1:
                await asyncio.sleep(args.interval)
    finally:
        await producer.stop()
    return sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--conversation-id", required=True)
    parser.add_argument("--content", default="")
    parser.add_argument("--count", type=int, default=1, help="number of distinct replies to send")
    parser.add_argument("--interval", type=float, default=0.0, help="seconds between replies")
    parser.add_argument("--repeat", type=int, default=1, help="publish each event this many times (duplicates)")
    parser.add_argument("--message-id", default=None, help="fixed message ID (implies duplicates if count > 1)")
    parser.add_argument("--sender-id", default="order-tracking-agent")
    parser.add_argument("--sender-type", default="virtual_agent", choices=["virtual_agent", "human_agent", "system"])
    parser.add_argument("--bootstrap-servers", default="localhost:19092,localhost:19093,localhost:19094")
    args = parser.parse_args(argv)
    if args.count < 1 or args.repeat < 1:
        parser.error("--count and --repeat must be at least 1")
    return args


def main(argv: list[str] | None = None) -> int:
    try:
        asyncio.run(publish(parse_args(argv)))
    except Exception as exc:  # report broker problems plainly, not as a stack trace
        print(f"error: could not publish to Kafka: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
