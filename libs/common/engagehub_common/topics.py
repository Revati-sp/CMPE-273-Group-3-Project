"""Kafka topic names and their provisioning specification."""

from __future__ import annotations

from dataclasses import dataclass

CHAT_MESSAGES = "chat.messages"
SESSION_EVENTS = "chat.session-events"
AGENT_EVENTS = "chat.agent-events"
ESCALATIONS = "chat.escalations"
NOTIFICATIONS = "chat.notifications"
ANALYTICS = "chat.analytics"
AUDIT_EVENTS = "chat.audit-events"
DEAD_LETTER = "chat.dead-letter-events"
# Customer reply stream: fake or real virtual-agent replies, sequenced into PostgreSQL.
CHAT_REPLIES = "chat.replies"
CHAT_REPLIES_DLQ = "chat.replies.dlq"
CHAT_REPLIES_DELIVERY = "chat.replies.delivery"  # sequenced messages fanned out to every chat pod
# Real virtual-agent responses (EventEnvelope, event_type agent_response), keyed by conversation ID.
AGENT_RESPONSES = "agent.responses"

AGENT_TYPES = (
    "product_advisor",
    "order_tracking",
    "returns_refunds",
    "billing",
    "technical_support",
    "faq",
    "complaint_management",
    "supervisor",
)


def agent_task_topic(agent_type: str) -> str:
    if agent_type not in AGENT_TYPES:
        raise ValueError(f"unknown agent type: {agent_type}")
    return f"chat.agent-tasks.{agent_type}"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int


_DAY_MS = 24 * 3600 * 1000

TOPIC_SPECS: tuple[TopicSpec, ...] = (
    TopicSpec(CHAT_MESSAGES, 12, 14 * _DAY_MS),
    TopicSpec(SESSION_EVENTS, 6, 7 * _DAY_MS),
    TopicSpec(AGENT_EVENTS, 6, 7 * _DAY_MS),
    TopicSpec(ESCALATIONS, 6, 14 * _DAY_MS),
    TopicSpec(NOTIFICATIONS, 6, 7 * _DAY_MS),
    TopicSpec(ANALYTICS, 6, 7 * _DAY_MS),
    TopicSpec(AUDIT_EVENTS, 6, 30 * _DAY_MS),
    TopicSpec(DEAD_LETTER, 3, 30 * _DAY_MS),
    TopicSpec(CHAT_REPLIES, 12, 14 * _DAY_MS),
    TopicSpec(CHAT_REPLIES_DLQ, 3, 30 * _DAY_MS),
    TopicSpec(CHAT_REPLIES_DELIVERY, 12, 1 * _DAY_MS),
    TopicSpec(AGENT_RESPONSES, 12, 14 * _DAY_MS),
    *(TopicSpec(agent_task_topic(a), 6, 3 * _DAY_MS) for a in AGENT_TYPES),
)
