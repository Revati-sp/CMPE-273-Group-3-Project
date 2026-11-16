"""The order handler answers from the checked-in fixture, not from order-service."""

import httpx
from app.agents import AgentTask, OrderTrackingAgent
from app.clients import ORDER_FIXTURE, DomainClient
from engagehub_common.config import ServiceSettings


def _refuse_network(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"order client called the network: {request.method} {request.url}")


async def test_where_is_order_1001_comes_back_delivered_from_the_fixture():
    client = DomainClient(
        ServiceSettings(service_name="virtual-agent-service"),
        transport=httpx.MockTransport(_refuse_network),
        order_fixture=ORDER_FIXTURE,
    )
    try:
        reply = await OrderTrackingAgent(client).respond(
            AgentTask("acme", "cust-acme-dana", "Where is order 1001?", "m1")
        )
    finally:
        await client.orders.aclose()
        await client.products.aclose()

    assert "ORD-1001 is delivered" in reply.text
    assert reply.metadata["order_id"] == "ORD-1001"
