import pytest
from app.main import create_app
from engagehub_common.testing import auth_headers, memory_runtime
from fastapi.testclient import TestClient

ACME = auth_headers(subject="cust-acme-dana")
GLOBEX = auth_headers(subject="cust-globex-sam", tenant="globex")


@pytest.fixture
def client():
    with TestClient(create_app(runtime=memory_runtime("product-service"))) as test_client:
        yield test_client


def names(response):
    return [p["name"] for p in response.json()]


def test_search_by_keyword_and_price(client):
    assert len(client.get("/products?q=tent", headers=ACME).json()) == 2
    assert names(client.get("/products?q=tent&max_price=200", headers=ACME)) == ["Trailhead 2-Person Tent"]


def test_like_wildcards_are_treated_literally(client):
    assert client.get("/products?q=%25", headers=ACME).json() == []


def test_recommendations_are_in_stock_and_best_rated_first(client):
    products = client.get("/products/recommendations?limit=10", headers=ACME).json()
    assert all(p["stock"] > 0 for p in products)
    assert products[0]["name"] == "Summit Hiking Boots"


def test_catalogues_are_isolated_between_tenants(client):
    assert client.get("/products?q=tent", headers=GLOBEX).json() == []
    assert client.get("/products/acme-tent-2p", headers=GLOBEX).status_code == 404
    assert client.get("/products/acme-tent-2p", headers=ACME).status_code == 200
    assert all(p["id"].startswith("globex") for p in client.get("/products", headers=GLOBEX).json())


def test_authentication_is_required(client):
    assert client.get("/products").status_code == 401
