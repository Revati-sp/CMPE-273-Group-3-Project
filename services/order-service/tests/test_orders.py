import pytest
from app.main import create_app
from engagehub_common.testing import auth_headers, memory_runtime
from fastapi.testclient import TestClient

DANA = auth_headers(subject="cust-acme-dana")
VIC = auth_headers(subject="cust-acme-vic")
ALICE = auth_headers(subject="agent-acme-alice", role="agent")
SAM = auth_headers(subject="cust-globex-sam", tenant="globex")
CAROL = auth_headers(subject="agent-globex-carol", role="agent", tenant="globex")


@pytest.fixture
def client():
    with TestClient(create_app(runtime=memory_runtime("order-service"))) as test_client:
        yield test_client


def service_for(customer, tenant="acme"):
    return auth_headers(subject="service:va", role="service", tenant=tenant, customer_id=customer)


def test_customer_sees_only_their_orders(client):
    assert {o["id"] for o in client.get("/orders", headers=DANA).json()} == {"ORD-1001", "ORD-1002"}
    assert client.get("/orders/ORD-1001", headers=DANA).status_code == 200
    assert client.get("/orders/ORD-1003", headers=DANA).status_code == 404
    assert client.get("/orders?customer_id=cust-acme-vic", headers=DANA).status_code == 404


def test_other_tenants_orders_are_invisible(client):
    assert client.get("/orders/ORD-1001", headers=SAM).status_code == 404
    assert client.get("/orders/ORD-1001", headers=CAROL).status_code == 404
    assert all(o["id"].startswith("ORD-2") for o in client.get("/orders", headers=CAROL).json())


def test_staff_can_read_customer_orders_in_their_tenant(client):
    assert len(client.get("/orders?customer_id=cust-acme-vic", headers=ALICE).json()) == 1


def test_service_token_is_limited_to_the_customer_it_acts_for(client):
    """A virtual agent cannot be talked into reading another customer's order."""
    headers = service_for("cust-acme-dana")
    assert client.get("/orders/ORD-1001", headers=headers).status_code == 200
    assert client.get("/orders/ORD-1003", headers=headers).status_code == 404


def test_return_is_idempotent_per_key(client):
    headers = {**DANA, "Idempotency-Key": "message-1"}
    first = client.post("/orders/ORD-1001/returns", json={"reason": "too small"}, headers=headers)
    retry = client.post("/orders/ORD-1001/returns", json={"reason": "too small"}, headers=headers)
    assert (first.status_code, retry.status_code) == (201, 200)
    assert first.json()["id"] == retry.json()["id"]
    assert len(client.get("/returns", headers=DANA).json()) == 1
    assert first.json()["refund_amount"] == 189.0


def test_return_rules_and_isolation(client):
    key = {"Idempotency-Key": "k1"}
    assert client.post("/orders/ORD-1002/returns", json={}, headers={**DANA, **key}).status_code == 409
    assert client.post("/orders/ORD-1003/returns", json={}, headers={**DANA, **key}).status_code == 404
    assert client.post("/orders/ORD-1001/returns", json={}, headers={**SAM, **key}).status_code == 404
    assert client.post("/orders/ORD-1001/returns", json={}, headers=DANA).status_code == 422
    client.post("/orders/ORD-1001/returns", json={}, headers={**DANA, **key})
    assert client.get("/returns", headers=VIC).json() == []
    assert client.get("/returns", headers=CAROL).json() == []
