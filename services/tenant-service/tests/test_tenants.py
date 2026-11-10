from app.main import create_app
from engagehub_common.testing import memory_runtime
from fastapi.testclient import TestClient


def test_two_demo_tenants_are_listed_without_auth():
    with TestClient(create_app(runtime=memory_runtime("tenant-service"))) as client:
        assert {t["id"] for t in client.get("/tenants").json()} == {"acme", "globex"}
        assert client.get("/tenants/acme").json()["name"] == "Acme Outdoors"
        assert client.get("/tenants/nope").status_code == 404
        assert client.get("/readyz").status_code == 200
