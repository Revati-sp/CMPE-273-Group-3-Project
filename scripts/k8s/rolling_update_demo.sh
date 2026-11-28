#!/usr/bin/env bash
# Rolling-update demonstration for Chat Service on Kubernetes.
#
# Prerequisites: a cluster with Kafka, Redis Cluster and PostgreSQL reachable at the
# addresses in deploy/k8s/chat-service/configmap.yaml, the manifests applied
# (kubectl apply -k deploy/k8s/chat-service), and the chat-service image loaded
# (for kind: kind load docker-image engagehub/chat-service:local).
#
#   JWT_SECRET=... scripts/k8s/rolling_update_demo.sh
#
# What it shows: each old pod drains (readiness fails, new sockets go elsewhere), sends
# server_draining and closes customer sockets with 1012; the watcher reconnects to a
# new pod, which claims the session (handover) and replays the unacknowledged tail.
set -euo pipefail
NAMESPACE=${NAMESPACE:-engagehub}
: "${JWT_SECRET:?set JWT_SECRET to the value in deploy/k8s/chat-service/secret.env}"

echo "== Enabling the demo publish endpoint for this test"
kubectl -n "$NAMESPACE" set env deployment/chat-service ENABLE_DEV_ENDPOINTS=true
kubectl -n "$NAMESPACE" rollout status deployment/chat-service --timeout=180s

echo "== Port-forwarding the chat-service Service (not a single pod)"
kubectl -n "$NAMESPACE" port-forward svc/chat-service 8000:8000 >/tmp/engagehub-port-forward.log 2>&1 &
FORWARD=$!
trap 'kill $FORWARD 2>/dev/null || true' EXIT
sleep 3

echo "== Starting a customer session"
python scripts/k8s/watch_session.py --jwt-secret "$JWT_SECRET" --duration 120 &
WATCHER=$!
sleep 10

echo "== Rolling restart"
kubectl -n "$NAMESPACE" rollout restart deployment/chat-service
kubectl -n "$NAMESPACE" rollout status deployment/chat-service --timeout=300s

# kubectl port-forward pins one pod; restart it so the watcher can reach a new pod.
kill $FORWARD 2>/dev/null || true
kubectl -n "$NAMESPACE" port-forward svc/chat-service 8000:8000 >>/tmp/engagehub-port-forward.log 2>&1 &
FORWARD=$!

wait $WATCHER
echo "== Old pods' logs mention:"
kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/name=chat-service --since=5m --tail=-1 \
  | grep -E 'draining|handover completed|old pod fenced|recovery completed' | tail -20 || true

echo "== Restoring ENABLE_DEV_ENDPOINTS=false"
kubectl -n "$NAMESPACE" set env deployment/chat-service ENABLE_DEV_ENDPOINTS=false
