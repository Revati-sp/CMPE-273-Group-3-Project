import pytest
from app.main import create_app
from app.passwords import hash_password, verify_password
from engagehub_common.auth import verify_token
from engagehub_common.testing import TEST_SECRET, memory_runtime
from engagehub_common.topics import AUDIT_EVENTS
from fastapi.testclient import TestClient


async def known_tenant(tenant_id):
    return tenant_id in ("acme", "globex")


@pytest.fixture
def client():
    runtime = memory_runtime("auth-service")
    with TestClient(create_app(runtime=runtime, tenant_exists=known_tenant)) as test_client:
        test_client.kafka = runtime.kafka
        yield test_client


def new_user(**overrides):
    return {"tenant_id": "acme", "email": "new@acme.example", "name": "New", "password": "long-enough-1", **overrides}


def login(client, tenant="acme", email="dana@acme.example", password="engagehub-demo"):
    return client.post("/login", json={"tenant_id": tenant, "email": email, "password": password})


def test_password_hash_verifies_and_is_salted():
    stored = hash_password("secret-value")
    assert verify_password("secret-value", stored) and not verify_password("wrong", stored)
    assert stored != hash_password("secret-value")
    assert not verify_password("x", "malformed")


def test_register_returns_a_customer_token_scoped_to_the_tenant(client):
    response = client.post("/register", json=new_user())
    assert response.status_code == 201
    principal = verify_token(TEST_SECRET, response.json()["access_token"])
    assert principal.role == "customer" and principal.tenant_id == "acme"
    assert principal.customer_id == principal.subject


def test_registration_cannot_choose_a_staff_role(client):
    response = client.post("/register", json=new_user(role="admin"))
    assert verify_token(TEST_SECRET, response.json()["access_token"]).role == "customer"


def test_duplicate_email_is_per_tenant(client):
    assert client.post("/register", json=new_user()).status_code == 201
    assert client.post("/register", json=new_user(email="NEW@acme.example")).status_code == 409
    assert client.post("/register", json=new_user(tenant_id="globex")).status_code == 201


def test_unknown_tenant_and_weak_input_are_rejected(client):
    assert client.post("/register", json=new_user(tenant_id="nope")).status_code == 400
    assert client.post("/register", json=new_user(password="short")).status_code == 422
    assert client.post("/register", json=new_user(email="not-an-email")).status_code == 422


def test_seeded_accounts_have_the_expected_roles(client):
    assert verify_token(TEST_SECRET, login(client).json()["access_token"]).role == "customer"
    agent = login(client, email="alice@acme.example").json()["access_token"]
    assert verify_token(TEST_SECRET, agent).role == "agent"
    admin = login(client, tenant="globex", email="admin@globex.example").json()["access_token"]
    assert verify_token(TEST_SECRET, admin).role == "admin"


def test_login_failures_are_indistinguishable(client):
    wrong_password = login(client, password="incorrect")
    unknown_email = login(client, email="ghost@acme.example")
    assert wrong_password.status_code == unknown_email.status_code == 401
    assert wrong_password.json() == unknown_email.json()


def test_credentials_do_not_work_across_tenants(client):
    assert login(client, tenant="globex").status_code == 401


def test_login_is_audited_and_survives_a_broker_outage(client):
    assert login(client).status_code == 200
    assert client.kafka.events(AUDIT_EVENTS)[-1].payload["action"] == "user.login"
    client.kafka.fail_publishes = 1
    assert login(client).status_code == 200


def test_me_requires_a_valid_token(client):
    assert client.get("/me").status_code == 401
    token = login(client).json()["access_token"]
    assert client.get("/me", headers={"Authorization": f"Bearer {token}"}).json()["tenant_id"] == "acme"
