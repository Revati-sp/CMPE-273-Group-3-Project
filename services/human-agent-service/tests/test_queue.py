import time
from types import SimpleNamespace

import httpx
import pytest
from app.assignment import estimate_wait_seconds, pick_agent
from app.main import create_app
from app.models import HumanAgent, QueueEntry
from engagehub_common.events import EventEnvelope, EventType
from engagehub_common.session.history import HistoryUnavailable
from engagehub_common.session.store import SessionState, SessionStore
from engagehub_common.testing import auth_headers, memory_runtime
from engagehub_common.topics import AGENT_EVENTS, ANALYTICS, CHAT_MESSAGES, ESCALATIONS, NOTIFICATIONS
from fastapi.testclient import TestClient

ALICE = auth_headers(subject="agent-acme-alice", role="agent", name="Alice Morgan")
BOB = auth_headers(subject="agent-acme-bob", role="agent", name="Bob Reyes")
CAROL = auth_headers(subject="agent-globex-carol", role="agent", tenant="globex", name="Carol")


class NoHistory:
    async def messages_after(self, *_a, **_k):
        raise HistoryUnavailable("unused")

    async def restore_session(self, *_):
        return False


def context_backend(request: httpx.Request) -> httpx.Response:
    if "/customers/" in request.url.path:
        return httpx.Response(200, json={"id": "cust-1", "tier": "vip"})
    if request.url.path.endswith("/orders"):
        return httpx.Response(200, json=[{"id": "ORD-1"}])
    if request.url.path.endswith("/messages"):
        return httpx.Response(200, json=[{"sequence_number": 1}])
    if "/conversations/" in request.url.path:
        return httpx.Response(200, json={"customer_id": "cust-1", "summary": "AI summary"})
    return httpx.Response(404)


@pytest.fixture
def client():
    runtime = memory_runtime("human-agent-service")
    http = httpx.AsyncClient(transport=httpx.MockTransport(context_backend))
    app = create_app(runtime=runtime, history=NoHistory(), http=http)
    with TestClient(app) as test_client:
        test_client.kafka = runtime.kafka
        test_client.store = SessionStore(runtime.redis)
        test_client.portal.call(
            test_client.store.create,
            SessionState(conversation_id="c1", session_id="s", tenant_id="acme", customer_id="cust-1"),
        )
        yield test_client


def handoff(client, conv="c1", event_id=None, tenant="acme", **payload):
    event = EventEnvelope(
        event_id=event_id or f"handoff-{conv}",
        event_type=EventType.HANDOFF_REQUESTED,
        conversation_id=conv,
        tenant_id=tenant,
        customer_id="cust-1",
        producer_service="virtual-agent-service",
        payload={"summary": "AI summary", "reason": "customer_request", "priority": 1, "intent": "billing", **payload},
    )
    client.kafka.append(ESCALATIONS, conv.encode(), event.to_bytes())


def wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


def queue(client, headers=ALICE):
    return client.get("/queue", headers=headers).json()


def set_status(client, headers, status):
    assert client.put("/agents/me/status", json={"status": status}, headers=headers).status_code == 200


def test_handoff_enters_queue_with_wait_estimate_and_summary(client):
    handoff(client)
    wait_for(lambda: queue(client)["waiting"])
    entry = queue(client)["waiting"][0]
    assert entry["summary"] == "AI summary" and entry["position"] == 1 and entry["estimated_wait_seconds"] > 0
    wait_for(lambda: any("in line" in e.payload["text"] for e in client.kafka.events(CHAT_MESSAGES)))


def test_redelivered_handoff_does_not_enqueue_twice(client):
    handoff(client)
    handoff(client)
    wait_for(lambda: queue(client)["waiting"])
    time.sleep(0.1)
    assert len(queue(client)["waiting"]) == 1


def test_available_agent_receives_offer_and_accept_assigns(client):
    set_status(client, ALICE, "available")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    assert client.post("/queue/c1/accept", headers=ALICE).status_code == 200

    view = queue(client)
    assert view["assigned"][0]["conversation_id"] == "c1" and view["offers"] == []
    state = client.portal.call(client.store.get, "acme", "c1")
    assert state.assigned_agent_id == "agent-acme-alice" and state.workflow_state == "human_active"
    assert client.kafka.events(AGENT_EVENTS)[0].event_type == EventType.AGENT_ASSIGNED
    assert client.kafka.events(ANALYTICS)
    assert client.kafka.events(NOTIFICATIONS)[0].payload["kind"] == "agent_assigned"
    assert any("has joined" in e.payload["text"] for e in client.kafka.events(CHAT_MESSAGES))


def test_reject_moves_offer_to_the_next_agent(client):
    set_status(client, ALICE, "available")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    set_status(client, BOB, "available")
    assert client.post("/queue/c1/reject", headers=ALICE).status_code == 200
    assert queue(client, BOB)["offers"][0]["conversation_id"] == "c1"
    assert queue(client, ALICE)["offers"] == []


def test_two_agents_cannot_take_the_same_conversation(client):
    set_status(client, ALICE, "available")
    set_status(client, BOB, "busy")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    assert client.post("/queue/c1/accept", headers=BOB).status_code == 409
    assert client.post("/queue/c1/accept", headers=ALICE).status_code == 200
    assert client.post("/queue/c1/accept", headers=BOB).status_code == 409


def test_transfer_to_colleague_then_accept(client):
    set_status(client, ALICE, "available")
    set_status(client, BOB, "available")
    handoff(client)
    wait_for(lambda: queue(client, ALICE)["offers"] or queue(client, BOB)["offers"])
    # Billing intent matches Bob's skill.
    assert queue(client, BOB)["offers"]
    client.post("/queue/c1/accept", headers=BOB)
    response = client.post("/conversations/c1/transfer", json={"target_agent_id": "agent-acme-alice"}, headers=BOB)
    assert response.status_code == 200
    assert queue(client, ALICE)["offers"][0]["conversation_id"] == "c1"
    assert client.post("/queue/c1/accept", headers=ALICE).status_code == 200


def test_transfer_back_to_virtual_agents(client):
    set_status(client, ALICE, "available")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    client.post("/queue/c1/accept", headers=ALICE)
    assert client.post("/conversations/c1/transfer", json={}, headers=ALICE).status_code == 200
    state = client.portal.call(client.store.get, "acme", "c1")
    assert state.workflow_state == "virtual" and state.assigned_agent_id == ""


def test_resolve_updates_state_and_notifies(client):
    set_status(client, ALICE, "available")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    client.post("/queue/c1/accept", headers=ALICE)
    assert client.post("/conversations/c1/resolve", headers=ALICE).status_code == 200
    assert client.portal.call(client.store.get, "acme", "c1").workflow_state == "resolved"
    assert queue(client)["assigned"] == []
    assert any(n.payload["kind"] == "conversation_resolved" for n in client.kafka.events(NOTIFICATIONS))


def test_only_the_assigned_agent_can_change_a_conversation(client):
    set_status(client, ALICE, "available")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    client.post("/queue/c1/accept", headers=ALICE)
    assert client.post("/conversations/c1/resolve", headers=BOB).status_code == 403
    assert client.post("/conversations/c1/transfer", json={}, headers=BOB).status_code == 403


def test_escalate_raises_priority(client):
    set_status(client, ALICE, "available")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    client.post("/queue/c1/accept", headers=ALICE)
    response = client.post("/conversations/c1/escalate", json={"reason": "legal"}, headers=ALICE)
    assert response.json()["status"] == "escalated" and response.json()["priority"] == 2
    assert any(e.event_type == EventType.CONVERSATION_ESCALATED for e in client.kafka.events(ESCALATIONS))


def test_broker_failure_rolls_back_accept_and_retry_succeeds(client):
    set_status(client, ALICE, "available")
    handoff(client)
    wait_for(lambda: queue(client)["offers"])
    client.kafka.fail_publishes = 1
    assert client.post("/queue/c1/accept", headers=ALICE).status_code == 503
    assert queue(client)["offers"][0]["conversation_id"] == "c1"
    assert client.post("/queue/c1/accept", headers=ALICE).status_code == 200
    joined = [e for e in client.kafka.events(CHAT_MESSAGES) if "has joined" in e.payload["text"]]
    assert len(joined) == 1


def test_notes_and_context_panels(client):
    assert client.post("/conversations/c1/notes", json={"text": "VIP, be careful"}, headers=ALICE).status_code == 201
    assert client.get("/conversations/c1/notes", headers=ALICE).json()[0]["text"] == "VIP, be careful"
    context = client.get("/conversations/c1/context", headers=ALICE).json()
    assert context["summary"] == "AI summary"
    assert context["customer"]["tier"] == "vip" and context["orders"] == [{"id": "ORD-1"}]
    assert context["messages"] == [{"sequence_number": 1}]


def test_tenant_isolation(client):
    client.post("/conversations/c1/notes", json={"text": "secret"}, headers=ALICE)
    handoff(client)
    wait_for(lambda: queue(client)["waiting"])
    assert queue(client, CAROL)["waiting"] == []
    assert client.get("/conversations/c1/notes", headers=CAROL).json() == []
    assert client.post("/queue/c1/accept", headers=CAROL).status_code == 404
    assert {a["id"] for a in client.get("/agents", headers=CAROL).json()} == {"agent-globex-carol", "agent-globex-dave"}


def test_customers_cannot_use_the_agent_api(client):
    customer = auth_headers(subject="cust-1")
    assert client.get("/queue", headers=customer).status_code == 403
    assert client.put("/agents/me/status", json={"status": "busy"}, headers=customer).status_code == 403


def agent(agent_id, status="available", languages=("en",), skills=(), last=None):
    return HumanAgent(
        id=agent_id, tenant_id="acme", name=agent_id, status=status, languages=list(languages),
        skills=list(skills), max_concurrent=2, last_assigned_at=last,
    )  # fmt: skip


def entry(**kwargs):
    return QueueEntry(**{"tenant_id": "acme", "intent": "unknown", "language": "en", "rejected_by": [], **kwargs})


def test_pick_agent_ranking():
    assert pick_agent(entry(language="es"), [agent("a"), agent("b", languages=("en", "es"))], {}).id == "b"
    assert pick_agent(entry(intent="billing"), [agent("a"), agent("b", skills=("billing",))], {}).id == "b"
    assert pick_agent(entry(), [agent("a"), agent("b")], {"a": 1}).id == "b"


def test_pick_agent_excludes_unavailable_full_rejected_and_other_tenants():
    assert pick_agent(entry(), [agent("a", status="busy")], {}) is None
    assert pick_agent(entry(), [agent("a", status="offline")], {}) is None
    assert pick_agent(entry(), [agent("a")], {"a": 2}) is None
    assert pick_agent(entry(rejected_by=["a"]), [agent("a")], {}) is None
    other = agent("x")
    other.tenant_id = "globex"
    assert pick_agent(entry(), [other], {}) is None


def test_wait_estimate_scales_with_capacity():
    available = [SimpleNamespace(status="available", max_concurrent=3)]
    assert estimate_wait_seconds(3, available, 240) == 240
    assert estimate_wait_seconds(3, [], 240) == 720
