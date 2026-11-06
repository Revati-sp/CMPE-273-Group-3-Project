"""Create the Kafka topics. Safe to run repeatedly (`python -m ...provision`)."""

from __future__ import annotations

import asyncio
import logging
import os

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from engagehub_common.resilience import retry_async
from engagehub_common.topics import TOPIC_SPECS

log = logging.getLogger("kafka-provision")


async def _create_topic(admin: AIOKafkaAdminClient, topic: NewTopic) -> None:
    response = await admin.create_topics([topic])
    for error in response.topic_errors:
        name, code = error[0], error[1]
        if code not in (0, TopicAlreadyExistsError.errno):
            # For example, too few brokers have registered yet: the caller retries.
            raise RuntimeError(f"creating topic {name} failed with error code {code}")


async def provision(bootstrap_servers: str, replication_factor: int, min_insync: int) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="engagehub-provisioner")
    await retry_async(admin.start, attempts=60, base=1.0)
    try:
        for spec in TOPIC_SPECS:
            topic = NewTopic(
                name=spec.name,
                num_partitions=spec.partitions,
                replication_factor=replication_factor,
                topic_configs={
                    "retention.ms": str(spec.retention_ms),
                    "min.insync.replicas": str(min_insync),
                },
            )
            # The brokers accept connections before the controller quorum is ready.
            await retry_async(lambda t=topic: _create_topic(admin, t), attempts=60, base=1.0)
            log.info("topic ready: %s", spec.name)
    finally:
        await admin.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(
        provision(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092"),
            int(os.environ.get("KAFKA_REPLICATION_FACTOR", "3")),
            int(os.environ.get("KAFKA_MIN_INSYNC_REPLICAS", "2")),
        )
    )
