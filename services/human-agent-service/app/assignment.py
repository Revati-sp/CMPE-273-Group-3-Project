"""Pure assignment logic: who gets the next conversation, and how long is the wait."""

from __future__ import annotations

from datetime import UTC, datetime

from app.models import STATUS_AVAILABLE, HumanAgent, QueueEntry

INTENT_SKILLS = {
    "billing": "billing",
    "returns": "returns",
    "technical": "technical",
    "complaint": "complaints",
    "order": "orders",
    "product": "products",
}


def pick_agent(entry: QueueEntry, agents: list[HumanAgent], workload: dict[str, int]) -> HumanAgent | None:
    """Best available agent: language, then specialisation, then load, then longest idle."""
    candidates = [
        a
        for a in agents
        if a.tenant_id == entry.tenant_id
        and a.status == STATUS_AVAILABLE
        and workload.get(a.id, 0) < a.max_concurrent
        and a.id not in (entry.rejected_by or [])
    ]
    if not candidates:
        return None
    skill = INTENT_SKILLS.get(entry.intent)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)

    def rank(agent: HumanAgent):
        idle_since = agent.last_assigned_at or epoch
        if idle_since.tzinfo is None:
            idle_since = idle_since.replace(tzinfo=UTC)
        return (
            0 if entry.language in (agent.languages or []) else 1,
            0 if skill and skill in (agent.skills or []) else 1,
            workload.get(agent.id, 0),
            idle_since,
        )

    return min(candidates, key=rank)


def estimate_wait_seconds(position: int, agents: list[HumanAgent], average_handle_seconds: int) -> int:
    """Rough estimate: the conversations ahead divided by the available parallel capacity."""
    capacity = sum(a.max_concurrent for a in agents if a.status == STATUS_AVAILABLE)
    return int(position * average_handle_seconds / max(capacity, 1))
