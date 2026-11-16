"""Handlers answer from the checked-in fixture, not from order-service."""

import httpx
from app.agents import AgentTask, OrderTrackingAgent, ReturnsRefundsAgent
from app.clients import ORDER_FIXTURE, DomainClient
from engagehub_common.config import ServiceSettings


def _refuse_network(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"order client called the network: {request.method} {request.url}")


def _client() -> DomainClient:
    return DomainClient(
        ServiceSettings(service_name="virtual-agent-service"),
        transport=httpx.MockTransport(_refuse_network),
        order_fixture=ORDER_FIXTURE,
    )


async def _close(client: DomainClient) -> None:
    await client.orders.aclose()
    await client.products.aclose()


async def test_where_is_order_1001_comes_back_delivered_from_the_fixture():
    client = _client()
    try:
        reply = await OrderTrackingAgent(client).respond(
            AgentTask("acme", "cust-acme-dana", "Where is order 1001?", "m1")
        )
    finally:
        await _close(client)

    assert "ORD-1001 is delivered" in reply.text
    assert reply.metadata["order_id"] == "ORD-1001"


async def test_i_also_want_to_return_it_opens_the_fixture_order():
    client = _client()
    try:
        reply = await ReturnsRefundsAgent(client).respond(
            AgentTask("acme", "cust-acme-dana", "I also want to return it.", "m2")
        )
    finally:
        await _close(client)

    assert reply.text.startswith("Return RET-1001 is open for order ORD-1001.")
    assert reply.metadata == {"order_id": "ORD-1001", "return_id": "RET-1001"}
