"""Create a Redis client for either Redis Cluster or a single node."""

from __future__ import annotations

from redis.asyncio import Redis
from redis.asyncio.cluster import ClusterNode, RedisCluster

from engagehub_common.config import ServiceSettings


def build_redis(settings: ServiceSettings):
    nodes = [n.strip().rsplit(":", 1) for n in settings.redis_nodes.split(",") if n.strip()]
    options = {"decode_responses": True, "socket_timeout": 3, "socket_connect_timeout": 3}
    if settings.redis_mode == "cluster":
        return RedisCluster(startup_nodes=[ClusterNode(h, int(p)) for h, p in nodes], **options)
    host, port = nodes[0]
    return Redis(host=host, port=int(port), **options)
