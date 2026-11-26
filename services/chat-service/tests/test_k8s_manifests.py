"""Static checks on the Kubernetes manifests (rendered offline with kubectl kustomize)."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

KUSTOMIZE_DIR = Path(__file__).resolve().parents[3] / "deploy" / "k8s" / "chat-service"


@pytest.fixture(scope="module")
def resources():
    if shutil.which("kubectl") is None:
        pytest.skip("kubectl not installed")
    secret = KUSTOMIZE_DIR / "secret.env"
    created = not secret.exists()
    if created:
        secret.write_text((KUSTOMIZE_DIR / "secret.env.example").read_text())
    try:
        rendered = subprocess.run(
            ["kubectl", "kustomize", str(KUSTOMIZE_DIR)], check=True, capture_output=True, text=True
        ).stdout
    finally:
        if created:
            secret.unlink()
    return {(doc["kind"], doc["metadata"]["name"]): doc for doc in yaml.safe_load_all(rendered) if doc}


def test_all_required_kinds_are_present(resources):
    kinds = {kind for kind, _ in resources}
    assert {"Deployment", "Service", "ConfigMap", "Secret", "PodDisruptionBudget",
            "HorizontalPodAutoscaler", "Ingress", "Job", "Namespace"} <= kinds  # fmt: skip


def test_deployment_is_highly_available_and_drains_gracefully(resources):
    deployment = resources[("Deployment", "chat-service")]
    spec = deployment["spec"]
    assert spec["replicas"] >= 2
    assert spec["strategy"]["rollingUpdate"]["maxUnavailable"] == 0
    pod = spec["template"]["spec"]
    container = pod["containers"][0]
    assert container["readinessProbe"]["httpGet"]["path"] == "/ready"
    assert container["livenessProbe"]["httpGet"]["path"] == "/health"
    assert container["resources"]["requests"] and container["resources"]["limits"]
    assert container["lifecycle"]["preStop"]
    assert pod["terminationGracePeriodSeconds"] >= 30
    refs = {next(iter(e.values()))["name"] for e in container["envFrom"]}
    assert "chat-service-config" in refs
    assert any(name.startswith("chat-service-secrets") for name in refs)


def test_autoscaler_and_disruption_budget_keep_two_pods(resources):
    assert resources[("HorizontalPodAutoscaler", "chat-service")]["spec"]["minReplicas"] >= 2
    assert resources[("PodDisruptionBudget", "chat-service")]["spec"]["minAvailable"] >= 1


def test_no_sticky_sessions_are_required(resources):
    assert "sessionAffinity" not in resources[("Service", "chat-service")]["spec"]


def test_configmap_carries_no_secrets(resources):
    data = resources[("ConfigMap", "chat-service-config")]["data"]
    assert not any("SECRET" in key or "PASSWORD" in key or "DATABASE_URL" in key for key in data)
    assert data["ENABLE_DEV_ENDPOINTS"] == "false"
