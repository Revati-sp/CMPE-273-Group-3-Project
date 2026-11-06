#!/bin/sh
# Forms the Redis Cluster: 3 primaries with 1 replica each. Safe to run repeatedly.
set -eu
NODES="${REDIS_CLUSTER_NODES:-redis-1 redis-2 redis-3 redis-4 redis-5 redis-6}"

for node in $NODES; do
  until redis-cli -h "$node" ping 2>/dev/null | grep -q PONG; do
    echo "waiting for $node"; sleep 1
  done
done

first=$(echo "$NODES" | awk '{print $1}')
if redis-cli -h "$first" cluster info | grep -q 'cluster_state:ok'; then
  echo "cluster already formed"
  exit 0
fi

# redis-cli --cluster create needs IP addresses.
endpoints=""
for node in $NODES; do
  ip=$(getent hosts "$node" | awk '{print $1}')
  endpoints="$endpoints $ip:6379"
done
# shellcheck disable=SC2086
redis-cli --cluster create $endpoints --cluster-replicas 1 --cluster-yes

until redis-cli -h "$first" cluster info | grep -q 'cluster_state:ok'; do sleep 1; done
echo "cluster ready"
