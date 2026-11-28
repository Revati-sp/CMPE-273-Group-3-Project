#!/usr/bin/env bash
# Horizontal-scaling demonstration (16 steps). Assumes scripts/k8s/kind_up.sh (or a cluster
# with the manifests applied). Each step prints what to look for. Watch in another terminal:
#   kubectl get pods -n engagehub -w
set -euo pipefail
NS=engagehub
cd "$(dirname "$0")/../.."
step() { printf '\n== %s\n' "$*"; }

step "1-2. Cluster and EngageHub are up"
kubectl get nodes
kubectl -n $NS get deploy

step "3. Two replicas of every scaled service"
kubectl -n $NS get deploy -o custom-columns=NAME:.metadata.name,READY:.status.readyReplicas,WANT:.spec.replicas
kubectl -n $NS get hpa
kubectl -n $NS get scaledobjects 2>/dev/null || echo "(KEDA not installed: agents scale on CPU and memory)"

step "4. Start the load test (50 customers) through the gateway"
kubectl -n $NS port-forward svc/api-gateway 18080:8080 >/tmp/engagehub-gw.log 2>&1 &
FORWARD=$!
trap 'kill $FORWARD 2>/dev/null || true' EXIT
sleep 3
python scripts/load/load_test.py --customers 50 --duration 240 --kubectl-namespace $NS \
  --report-json /tmp/engagehub-load.json > /tmp/engagehub-load.log 2>&1 &
LOAD=$!

step "5-6. Kafka lag rises; replicas are added (HPA/KEDA). Sampling for 90 s"
for _ in $(seq 1 6); do
  kubectl -n $NS get hpa --no-headers 2>/dev/null | awk '{print "  hpa", $1, "replicas", $(NF-1)}' | grep va- || true
  kubectl -n $NS get scaledobjects --no-headers 2>/dev/null | head -3 || true
  sleep 15
done

step "7. Tasks are spread across Product Advisor replicas"
for pod in $(kubectl -n $NS get pods -l app.kubernetes.io/name=va-product-advisor -o name); do
  printf '  %s processed %s tasks\n' "$pod" "$(kubectl -n $NS logs "$pod" | grep -c 'agent task processed' || true)"
done

step "8-9. Stop one virtual-agent pod; the others keep processing"
VICTIM=$(kubectl -n $NS get pods -l app.kubernetes.io/name=va-product-advisor -o name | head -1)
kubectl -n $NS delete "$VICTIM" --wait=false
sleep 20
kubectl -n $NS get pods -l app.kubernetes.io/name=va-product-advisor

step "10-14. Stop one Chat Service pod; customers reconnect and replay (see the load report)"
CHAT=$(kubectl -n $NS get pods -l app.kubernetes.io/name=chat-service -o name | head -1)
kubectl -n $NS delete "$CHAT" --wait=false
sleep 20
kubectl -n $NS logs -l app.kubernetes.io/name=chat-service --since=2m --tail=-1 \
  | grep -E 'handover completed|recovery completed|old pod fenced' | tail -5 || true

step "15. Wait for the load test to finish"
wait $LOAD || true
cat /tmp/engagehub-load.json

step "16. Replicas scale back down (HPA scale-down window is 5 minutes). Sampling for 6 minutes"
for _ in $(seq 1 12); do kubectl -n $NS get hpa --no-headers | awk '{print "  ", $1, $(NF-1)}' | grep va-product || true; sleep 30; done
