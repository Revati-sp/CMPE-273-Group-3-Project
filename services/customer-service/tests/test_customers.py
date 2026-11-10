import pytest
from app.main import create_app
from engagehub_common.testing import auth_headers, memory_runtime
from fastapi.testclient import TestClient

DANA = auth_headers(subject="cust-acme-dana")
ALICE = auth_headers(subject="agent-acme-alice", role="agent")
CAROL = auth_headers(subject="agent-globex-carol", role="agent", tenant="globex")


@pytest.fixture
def client():
    with TestClient(create_app(runtime=memory_runtime("customer-service"))) as test_client:
        yield test_client


def test_customer_reads_own_profile_but_not_others(client):
    assert client.get("/customers/me", headers=DANA).json()["id"] == "cust-acme-dana"
    assert client.get("/customers/cust-acme-dana", headers=DANA).status_code == 200
    assert client.get("/customers/cust-acme-vic", headers=DANA).status_code == 404
    assert client.get("/customers", headers=DANA).status_code == 403


def test_staff_are_confined_to_their_tenant(client):
    assert client.get("/customers/cust-acme-vic", headers=ALICE).json()["tier"] == "vip"
    assert client.get("/customers/cust-acme-vic", headers=CAROL).status_code == 404
    assert {c["tenant_id"] for c in client.get("/customers", headers=CAROL).json()} == {"globex"}


def test_service_token_reads_tier_within_its_tenant_only(client):
    service = auth_headers(subject="service:orchestrator", role="service", customer_id="cust-acme-vic")
    assert client.get("/customers/cust-acme-vic", headers=service).json()["tier"] == "vip"
    other = auth_headers(subject="service:orchestrator", role="service", tenant="globex")
    assert client.get("/customers/cust-acme-vic", headers=other).status_code == 404


def test_new_customer_gets_a_profile_on_first_use(client):
    headers = auth_headers(subject="cust-new", name="New Person")
    profile = client.get("/customers/me", headers=headers).json()
    assert profile["name"] == "New Person" and profile["tier"] == "standard"


def test_customer_can_update_language_but_not_tier(client):
    response = client.patch("/customers/me", json={"language": "es", "tier": "vip"}, headers=DANA)
    assert response.json()["language"] == "es" and response.json()["tier"] == "standard"
