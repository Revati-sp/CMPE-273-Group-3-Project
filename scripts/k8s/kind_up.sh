#!/usr/bin/env bash
# Local Kubernetes for EngageHub with kind: builds the images, loads them into the cluster,
# installs metrics-server (HPAs need it) and optionally KEDA, then applies overlays/kind
# (the platform plus single-node Kafka, Redis and PostgreSQL).
#
#   scripts/k8s/kind_up.sh            # CPU and memory HPAs
#   KEDA=1 scripts/k8s/kind_up.sh     # plus KEDA (downloads the release manifest)
#
# Needs: docker, kind, kubectl; roughly 8 GB of memory for Docker and 15 GB of disk.
# The images are the ones docker compose builds (engagehub/<service>:local).
set -euo pipefail
CLUSTER=${CLUSTER:-engagehub}
KEDA_VERSION=${KEDA_VERSION:-2.16.1}
cd "$(dirname "$0")/../.."

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
kubectl config use-context "kind-$CLUSTER"

docker compose build
for image in api-gateway frontend chat-service agent-orchestrator virtual-agent-service human-agent-service \
             notification-service analytics-service auth-service tenant-service customer-service product-service \
             order-service session-service audit-service; do
  kind load docker-image "engagehub/$image:local" --name "$CLUSTER"
done

# HPAs read CPU and memory from metrics-server; kind's kubelets need --kubelet-insecure-tls.
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' || true

[ -f deploy/k8s/base/secret.env ] || {
  cp deploy/k8s/base/secret.env.example deploy/k8s/base/secret.env
  sed -i.bak "s/^JWT_SECRET=.*/JWT_SECRET=$(openssl rand -hex 32)/; s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=kind-local/; s/^DEMO_PASSWORD=.*/DEMO_PASSWORD=engagehub-demo/" deploy/k8s/base/secret.env
  rm -f deploy/k8s/base/secret.env.bak
}

OVERLAY=deploy/k8s/overlays/kind
if [ "${KEDA:-0}" = "1" ]; then
  kubectl apply --server-side -f "https://github.com/kedacore/keda/releases/download/v$KEDA_VERSION/keda-$KEDA_VERSION.yaml"
  kubectl -n keda rollout status deployment/keda-operator --timeout=180s
  kubectl apply -k deploy/k8s/overlays/kind
  # Swap the agents' CPU HPAs for Kafka-lag ScaledObjects.
  kubectl -n engagehub get hpa -o name | grep '/va-' | xargs -r kubectl -n engagehub delete
  kubectl apply -f deploy/k8s/overlays/keda/scaledobjects.yaml -n engagehub
else
  kubectl apply -k "$OVERLAY"
fi
kubectl -n engagehub rollout status deployment/chat-service --timeout=300s
kubectl -n engagehub get pods
echo "Gateway: kubectl -n engagehub port-forward svc/api-gateway 18080:8080"
