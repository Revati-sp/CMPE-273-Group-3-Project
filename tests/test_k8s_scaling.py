"""Horizontal-scaling manifests: rendered offline with `kubectl kustomize`, then checked.

These check structure and policy (replicas, probes, autoscaling, consumer groups, no
sticky sessions). They do not prove the manifests run on a cluster: see the README for
what was and was not tested on Kubernetes.
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
K8S = ROOT / "deploy" / "k8s"
sys.path.insert(0, str(ROOT / "libs" / "common"))
from engagehub_common.topics import AGENT_TYPES, TOPIC_SPECS, agent_task_topic  # noqa: E402

SCALED = {  # deployment: (min, max) of its autoscaler
    "api-gateway": (2, 6),
    "chat-service": (2, 6),
    "agent-orchestrator": (2, 6),
    "human-agent-service": (2, 4),
    "notification-service": (2, 6),
    "analytics-service": (2, 6),
    **{f"va-{a.replace('_', '-')}": (2, 10) for a in AGENT_TYPES},
}


def render(path: Path) -> list[dict]:
    if shutil.which("kubectl") is None:
        pytest.skip("kubectl not installed")
    secret = K8S / "base" / "secret.env"
    created = not secret.exists()
    if created:
        secret.write_text((K8S / "base" / "secret.env.example").read_text())
    try:
        out = subprocess.run(["kubectl", "kustomize", str(path)], check=True, capture_output=True, text=True).stdout
    finally:
        if created:
            secret.unlink()
    return [d for d in yaml.safe_load_all(out) if d]


def index(docs):
    return {(d["kind"], d["metadata"]["name"]): d for d in docs}


@pytest.fixture(scope="module")
def base():
    return index(render(K8S))


@pytest.fixture(scope="module")
def keda():
    return index(render(K8S / "overlays" / "keda"))


def deployments(resources):
    return {name: d for (kind, name), d in resources.items() if kind == "Deployment"}


def test_every_scaled_service_has_deployment_service_hpa_and_pdb(base):
    for name, (low, high) in SCALED.items():
        deployment = base[("Deployment", name)]
        assert deployment["spec"]["replicas"] >= 2, name
        hpa = base[("HorizontalPodAutoscaler", name)]["spec"]
        assert (hpa["minReplicas"], hpa["maxReplicas"]) == (low, high), name
        assert hpa["scaleTargetRef"] == {"apiVersion": "apps/v1", "kind": "Deployment", "name": name}
        resources = {m["resource"]["name"] for m in hpa["metrics"]}
        assert resources == {"cpu", "memory"}, name
        assert hpa["behavior"]["scaleDown"]["stabilizationWindowSeconds"] >= 300
        assert base[("PodDisruptionBudget", name)]["spec"]["minAvailable"] == 1
        selector = base[("PodDisruptionBudget", name)]["spec"]["selector"]["matchLabels"]
        assert selector == deployment["spec"]["selector"]["matchLabels"]


def test_every_deployment_has_probes_resources_and_graceful_shutdown(base):
    for name, deployment in deployments(base).items():
        spec = deployment["spec"]
        assert spec["strategy"] == {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}}
        pod = spec["template"]["spec"]
        container = pod["containers"][0]
        for probe in ("livenessProbe", "readinessProbe"):
            assert container[probe]["httpGet"]["port"] == "http", (name, probe)
        assert container["resources"]["requests"] and container["resources"]["limits"]["memory"], name
        if name != "frontend":
            assert container.get("startupProbe"), name
            assert container["lifecycle"]["preStop"], name
            assert pod["terminationGracePeriodSeconds"] >= 30, name
        assert pod["serviceAccountName"] == "engagehub-workload" and pod["automountServiceAccountToken"] is False


def test_python_services_run_non_root_with_a_read_only_filesystem(base):
    for name, deployment in deployments(base).items():
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        if container["image"].startswith("engagehub/") and name not in ("api-gateway", "frontend"):
            assert deployment["spec"]["template"]["spec"]["securityContext"]["runAsNonRoot"] is True, name
            assert container["securityContext"]["readOnlyRootFilesystem"] is True, name


def test_one_deployment_per_agent_type_in_its_own_consumer_group(base):
    seen = set()
    for agent_type in AGENT_TYPES:
        name = f"va-{agent_type.replace('_', '-')}"
        container = base[("Deployment", name)]["spec"]["template"]["spec"]["containers"][0]
        env = {e["name"]: e.get("value") for e in container["env"]}
        assert env["AGENT_TYPE"] == agent_type  # the worker joins group va-<agent_type>
        assert container["image"] == "engagehub/virtual-agent-service:local"
        seen.add(agent_type)
    assert seen == set(AGENT_TYPES)


def test_agent_topics_have_enough_partitions_for_the_maximum_replicas(base):
    partitions = {spec.name: spec.partitions for spec in TOPIC_SPECS}
    for agent_type in AGENT_TYPES:
        hpa = base[("HorizontalPodAutoscaler", f"va-{agent_type.replace('_', '-')}")]["spec"]
        assert partitions[agent_task_topic(agent_type)] >= hpa["maxReplicas"]


def test_no_service_requires_sticky_sessions(base):
    for (kind, name), doc in base.items():
        if kind == "Service":
            assert "sessionAffinity" not in doc["spec"], name


def test_chat_service_drains_before_shutdown(base):
    pod = base[("Deployment", "chat-service")]["spec"]["template"]["spec"]
    command = " ".join(pod["containers"][0]["lifecycle"]["preStop"]["exec"]["command"])
    assert "/internal/drain" in command and pod["terminationGracePeriodSeconds"] >= 23


def test_database_urls_are_built_from_the_secret_per_service(base):
    expected = {
        "chat-service": "eh_chat",
        "human-agent-service": "eh_human",
        "notification-service": "eh_notification",
        "analytics-service": "eh_analytics",
        "auth-service": "eh_auth",
        "session-service": "eh_session",
    }
    for name, db in expected.items():
        env = base[("Deployment", name)]["spec"]["template"]["spec"]["containers"][0]["env"]
        names = [e["name"] for e in env]
        url = next(e["value"] for e in env if e["name"] == "DATABASE_URL")
        assert url.endswith(f"/{db}")
        for var in re.findall(r"\$\((\w+)\)", url):
            assert names.index(var) < names.index("DATABASE_URL")  # defined first, so it expands
    assert base[("Secret", "engagehub-secrets")]["type"] == "Opaque"


def test_configmaps_hold_no_secrets(base):
    for (kind, name), doc in base.items():
        if kind == "ConfigMap":
            assert not any(k in doc["data"] for k in ("JWT_SECRET", "POSTGRES_PASSWORD", "DATABASE_URL")), name


def test_gateway_uses_fully_qualified_upstreams_and_is_the_only_public_pod(base):
    env = {
        e["name"]: e["value"]
        for e in base[("Deployment", "api-gateway")]["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["UPSTREAM_SUFFIX"] == ".engagehub.svc.cluster.local"
    ingress = base[("Ingress", "engagehub")]["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]
    assert ingress["name"] == "api-gateway"
    public = base[("NetworkPolicy", "allow-gateway-public")]["spec"]
    assert public["podSelector"]["matchLabels"] == {"app.kubernetes.io/name": "api-gateway"}
    assert base[("NetworkPolicy", "default-deny-ingress")]["spec"]["policyTypes"] == ["Ingress"]


def test_everything_is_namespaced(base):
    for (kind, name), doc in base.items():
        if kind != "Namespace":
            assert doc["metadata"].get("namespace") == "engagehub", (kind, name)


def test_keda_overlay_scales_agents_on_real_consumer_lag(keda):
    for agent_type in AGENT_TYPES:
        name = f"va-{agent_type.replace('_', '-')}"
        spec = keda[("ScaledObject", name)]["spec"]
        assert spec["scaleTargetRef"]["name"] == name
        assert (spec["minReplicaCount"], spec["maxReplicaCount"]) == (2, 10)
        kafka = next(t for t in spec["triggers"] if t["type"] == "kafka")["metadata"]
        assert kafka["consumerGroup"] == f"va-{agent_type}"  # the group the worker really uses
        assert kafka["topic"] == agent_task_topic(agent_type)
        assert int(kafka["lagThreshold"]) > 0
        assert ("HorizontalPodAutoscaler", name) not in keda  # KEDA owns the HPA; no second one
    assert ("HorizontalPodAutoscaler", "chat-service") in keda  # other services keep CPU scaling


def test_kind_overlay_adds_single_node_infrastructure():
    kind = index(render(K8S / "overlays" / "kind"))
    for name in ("kafka", "redis", "postgres"):
        assert ("Service", name) in kind
    config = kind[("ConfigMap", "engagehub-config")]["data"]
    assert config["REDIS_MODE"] == "standalone" and config["KAFKA_REPLICATION_FACTOR"] == "1"
    assert "eh_chat" in kind[("ConfigMap", "postgres-init")]["data"]["init-databases.sql"]


def test_cloud_overlay_has_tls_registry_images_and_lag_scaling():
    cloud = index(render(K8S / "overlays" / "cloud"))
    ingress = cloud[("Ingress", "engagehub")]["spec"]
    assert ingress["tls"][0]["secretName"] == "engagehub-tls"
    image = cloud[("Deployment", "chat-service")]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image.startswith("registry.example.com/")
    assert ("ScaledObject", "va-faq") in cloud
