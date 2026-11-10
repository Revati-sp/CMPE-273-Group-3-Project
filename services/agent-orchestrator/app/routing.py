"""Routing policy: a pure function, so every rule is directly testable."""

from __future__ import annotations

from dataclasses import dataclass

from engagehub_common.llm import INTENT_TO_AGENT, Classification

FRUSTRATED = -0.5
MAX_NEGATIVE_MESSAGES = 2
MAX_UNCERTAIN_MESSAGES = 2


@dataclass(frozen=True)
class RoutingDecision:
    escalate: bool
    agent: str
    reason: str
    priority: int


def compute_priority(tier: str, sentiment: float, negative_count: int, sensitive: bool) -> int:
    """Higher runs first in the human queue."""
    priority = 1
    if tier == "vip":
        priority += 2
    if sentiment <= FRUSTRATED or negative_count >= MAX_NEGATIVE_MESSAGES:
        priority += 1
    if sensitive:
        priority += 1
    return priority


def decide(
    classification: Classification,
    *,
    tier: str,
    negative_count: int,
    uncertain_count: int,
    live_workers: dict[str, int],
) -> RoutingDecision:
    c = classification
    priority = compute_priority(tier, c.sentiment, negative_count, c.sensitive_topic is not None)

    def escalate(reason: str) -> RoutingDecision:
        return RoutingDecision(True, "supervisor", reason, priority)

    if c.wants_human:
        return escalate("customer_request")
    if c.sensitive_topic:
        return escalate(f"sensitive_topic:{c.sensitive_topic}")
    if c.sentiment <= FRUSTRATED and tier == "vip":
        return escalate("vip_frustrated")
    if negative_count >= MAX_NEGATIVE_MESSAGES:
        return escalate("repeated_frustration")
    if c.intent == "unknown" and uncertain_count >= MAX_UNCERTAIN_MESSAGES:
        return escalate("uncertainty")

    # One angry message goes to the complaint specialist first.
    intent = "complaint" if c.sentiment <= FRUSTRATED else c.intent
    agent = INTENT_TO_AGENT[intent]
    if live_workers.get(agent, 0) == 0:
        return escalate(f"agent_unavailable:{agent}")
    return RoutingDecision(False, agent, f"intent:{intent}", priority)
